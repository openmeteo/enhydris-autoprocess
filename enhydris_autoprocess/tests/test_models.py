import datetime as dt
import textwrap
from unittest import mock

from django.db import IntegrityError
from django.test import TestCase

import numpy as np
import pandas as pd
from htimeseries import HTimeseries
from model_mommy import mommy

from enhydris.models import Station, Timeseries, TimeseriesGroup, Variable
from enhydris_autoprocess import tasks
from enhydris_autoprocess.models import (
    Aggregation,
    AutoProcess,
    Checks,
    CurveInterpolation,
    CurvePeriod,
    CurvePoint,
    RangeCheck,
)


class AutoProcessTestCase(TestCase):
    def setUp(self):
        self.station = mommy.make(Station)
        self.timeseries_group1 = mommy.make(TimeseriesGroup, gentity=self.station)
        self.timeseries_group2 = mommy.make(TimeseriesGroup, gentity=self.station)

        self.original_execute_auto_process = tasks.execute_auto_process
        tasks.execute_auto_process = mock.MagicMock()

    def tearDown(self):
        tasks.execute_auto_process = self.original_execute_auto_process

    def test_create(self):
        auto_process = Checks(timeseries_group=self.timeseries_group1)
        auto_process.save()
        self.assertEqual(AutoProcess.objects.count(), 1)

    def test_update(self):
        mommy.make(Checks, timeseries_group=self.timeseries_group1)
        auto_process = AutoProcess.objects.first()
        auto_process.timeseries_group = self.timeseries_group2
        auto_process.save()
        self.assertEqual(auto_process.timeseries_group.id, self.timeseries_group2.id)

    def test_delete(self):
        mommy.make(Checks, timeseries_group=self.timeseries_group1)
        auto_process = AutoProcess.objects.first()
        auto_process.delete()
        self.assertEqual(AutoProcess.objects.count(), 0)

    def test_save_triggers_auto_process(self):
        auto_process = mommy.make(Checks, timeseries_group=self.timeseries_group1)
        auto_process.save()
        tasks.execute_auto_process.apply_async.assert_any_call(
            args=[auto_process.id], countdown=1
        )


class AutoProcessExecuteTestCase(TestCase):
    @mock.patch(
        "enhydris_autoprocess.models.Checks.process_timeseries",
        side_effect=lambda self: self.htimeseries,
        autospec=True,
    )
    def setUp(self, m):
        self.mock_execute = m
        station = mommy.make(Station)
        self.timeseries_group = mommy.make(
            TimeseriesGroup,
            gentity=station,
            variable__descr="irrelevant",
            time_zone__utc_offset=120,
        )
        self.checks = mommy.make(Checks, timeseries_group=self.timeseries_group)
        self.range_check = mommy.make(RangeCheck, checks=self.checks)
        self.checks.execute()

    def test_called_once(self):
        self.assertEqual(len(self.mock_execute.mock_calls), 1)

    def test_called_with_empty_content(self):
        self.assertEqual(len(self.checks.htimeseries.data), 0)


class AutoProcessExecuteDealsOnlyWithNewerTimeseriesPartTestCase(TestCase):
    @mock.patch(
        "enhydris_autoprocess.models.Checks.process_timeseries",
        side_effect=lambda self: self.htimeseries,
        autospec=True,
    )
    def setUp(self, m):
        self.mock_process_timeseries = m
        station = mommy.make(Station)
        self.timeseries_group = mommy.make(
            TimeseriesGroup,
            gentity=station,
            time_zone__utc_offset=0,
            variable__descr="h",
        )
        self.source_timeseries = mommy.make(
            Timeseries, timeseries_group=self.timeseries_group, type=Timeseries.RAW
        )
        self.source_timeseries.set_data(
            pd.DataFrame(
                data={"value": [1.0, 2.0, 3.0, 4.0], "flags": ["", "", "", ""]},
                columns=["value", "flags"],
                index=[
                    dt.datetime(2019, 5, 21, 17, 0),
                    dt.datetime(2019, 5, 21, 17, 10),
                    dt.datetime(2019, 5, 21, 17, 20),
                    dt.datetime(2019, 5, 21, 17, 30),
                ],
            )
        )
        self.target_timeseries = mommy.make(
            Timeseries, timeseries_group=self.timeseries_group, type=Timeseries.CHECKED
        )
        self.target_timeseries.set_data(
            pd.DataFrame(
                data={"value": [1.0, 2.0], "flags": ["", ""]},
                columns=["value", "flags"],
                index=[
                    dt.datetime(2019, 5, 21, 17, 0),
                    dt.datetime(2019, 5, 21, 17, 10),
                ],
            )
        )
        self.checks = mommy.make(Checks, timeseries_group=self.timeseries_group)
        self.range_check = mommy.make(RangeCheck, checks=self.checks)

        # We could simply run self.checks.execute() here. However,
        # normally it is the parent's execute() that runs, and it finds the child and
        # runs it. So we also run the parent's execute() to ensure things work
        # correctly.
        self.checks.autoprocess_ptr.execute()

    def test_called_once(self):
        self.assertEqual(len(self.mock_process_timeseries.mock_calls), 1)

    def test_called_with_the_newer_part_of_the_timeseries(self):
        expected_arg = pd.DataFrame(
            data={"value": [3.0, 4.0], "flags": ["", ""]},
            columns=["value", "flags"],
            index=[dt.datetime(2019, 5, 21, 17, 20), dt.datetime(2019, 5, 21, 17, 30)],
        )
        expected_arg.index.name = "date"
        pd.testing.assert_frame_equal(self.checks.htimeseries.data, expected_arg)

    def test_appended_the_data(self):
        expected_result = pd.DataFrame(
            data={"value": [1.0, 2.0, 3.0, 4.0], "flags": ["", "", "", ""]},
            columns=["value", "flags"],
            index=[
                dt.datetime(2019, 5, 21, 17, 0),
                dt.datetime(2019, 5, 21, 17, 10),
                dt.datetime(2019, 5, 21, 17, 20),
                dt.datetime(2019, 5, 21, 17, 30),
            ],
        )
        expected_result.index.name = "date"
        pd.testing.assert_frame_equal(
            self.target_timeseries.get_data().data, expected_result
        )


class ChecksTestCase(TestCase):
    def test_create(self):
        timeseries_group = mommy.make(TimeseriesGroup)
        checks = Checks(timeseries_group=timeseries_group)
        checks.save()
        self.assertEqual(Checks.objects.count(), 1)

    def test_update(self):
        timeseries_group1 = mommy.make(TimeseriesGroup, id=42)
        timeseries_group2 = mommy.make(TimeseriesGroup, id=43)
        checks = mommy.make(Checks, timeseries_group=timeseries_group1)
        checks.timeseries_group = timeseries_group2
        checks.save()
        self.assertEqual(Checks.objects.first().timeseries_group.id, 43)

    def test_delete(self):
        checks = mommy.make(Checks)
        checks.delete()
        self.assertEqual(Checks.objects.count(), 0)

    def test_str(self):
        checks = mommy.make(Checks, timeseries_group__name="Temperature")
        self.assertEqual(str(checks), "Checks for Temperature")

    def test_source_timeseries(self):
        self.timeseries_group = mommy.make(TimeseriesGroup)
        self._make_timeseries(id=42, type=Timeseries.RAW)
        self._make_timeseries(id=41, type=Timeseries.CHECKED)
        checks = mommy.make(Checks, timeseries_group=self.timeseries_group)
        self.assertEqual(checks.source_timeseries.id, 42)

    def _make_timeseries(self, id, type):
        return mommy.make(
            Timeseries, id=id, timeseries_group=self.timeseries_group, type=type
        )

    def test_automatically_creates_source_timeseries(self):
        timeseries_group = mommy.make(TimeseriesGroup)
        checks = mommy.make(Checks, timeseries_group=timeseries_group)
        self.assertFalse(Timeseries.objects.exists())
        checks.source_timeseries.id
        self.assertTrue(Timeseries.objects.exists())

    def test_target_timeseries(self):
        self.timeseries_group = mommy.make(TimeseriesGroup)
        self._make_timeseries(id=42, type=Timeseries.RAW)
        self._make_timeseries(id=41, type=Timeseries.CHECKED)
        checks = mommy.make(Checks, timeseries_group=self.timeseries_group)
        self.assertEqual(checks.target_timeseries.id, 41)

    def test_automatically_creates_target_timeseries(self):
        timeseries_group = mommy.make(TimeseriesGroup)
        checks = mommy.make(Checks, timeseries_group=timeseries_group)
        self.assertFalse(Timeseries.objects.exists())
        checks.target_timeseries.id
        self.assertTrue(Timeseries.objects.exists())

    @mock.patch("enhydris_autoprocess.models.RangeCheck.check_timeseries")
    def test_runs_range_check(self, m):
        station = mommy.make(Station)
        range_check = mommy.make(
            RangeCheck,
            checks__timeseries_group__gentity=station,
            checks__timeseries_group__time_zone__utc_offset=0,
            checks__timeseries_group__variable__descr="Temperature",
        )
        range_check.checks.execute()
        m.assert_called_once_with()


class RangeCheckTestCase(TestCase):
    def _mommy_make_range_check(self):
        return mommy.make(
            RangeCheck, checks__timeseries_group__name="pH", upper_bound=55.0
        )

    def test_create(self):
        checks = mommy.make(Checks)
        range_check = RangeCheck(checks=checks, upper_bound=42.7, lower_bound=-5.2)
        range_check.save()
        self.assertEqual(RangeCheck.objects.count(), 1)

    def test_update(self):
        self._mommy_make_range_check()
        range_check = RangeCheck.objects.first()
        range_check.upper_bound = 1831.7
        range_check.save()
        self.assertAlmostEqual(range_check.upper_bound, 1831.7)

    def test_delete(self):
        self._mommy_make_range_check()
        range_check = RangeCheck.objects.first()
        range_check.delete()
        self.assertEqual(RangeCheck.objects.count(), 0)

    def test_str(self):
        range_check = self._mommy_make_range_check()
        self.assertEqual(str(range_check), "Range check for pH")


class RangeCheckProcessTimeseriesTestCase(TestCase):
    _index = [
        dt.datetime(2019, 5, 21, 10, 20),
        dt.datetime(2019, 5, 21, 10, 30),
        dt.datetime(2019, 5, 21, 10, 40),
        dt.datetime(2019, 5, 21, 10, 50),
        dt.datetime(2019, 5, 21, 11, 00),
        dt.datetime(2019, 5, 21, 11, 10),
        dt.datetime(2019, 5, 21, 11, 20),
    ]

    source_timeseries = pd.DataFrame(
        data={
            "value": [1.5, 2.9, 3.1, np.nan, 3.8, 4.9, 7.2],
            "flags": ["", "", "", "", "FLAG1", "FLAG2", "FLAG3"],
        },
        columns=["value", "flags"],
        index=_index,
    )

    expected_result = pd.DataFrame(
        data={
            "value": [np.nan, 2.9, 3.1, np.nan, 3.8, 4.9, np.nan],
            "flags": [
                "RANGE",
                "SUSPECT",
                "",
                "",
                "FLAG1",
                "FLAG2 SUSPECT",
                "FLAG3 RANGE",
            ],
        },
        columns=["value", "flags"],
        index=_index,
    )

    def test_execute(self):
        self.range_check = mommy.make(
            RangeCheck,
            lower_bound=2,
            upper_bound=5,
            soft_lower_bound=3,
            soft_upper_bound=4,
        )
        self.range_check.checks.htimeseries = HTimeseries(self.source_timeseries)
        result = self.range_check.checks.process_timeseries()
        pd.testing.assert_frame_equal(result, self.expected_result)


class CurveInterpolationTestCase(TestCase):
    def setUp(self):
        self.station = mommy.make(Station)
        self.timeseries_group1 = mommy.make(TimeseriesGroup, gentity=self.station)
        self.timeseries_group2 = mommy.make(
            TimeseriesGroup, gentity=self.station, name="Group 2"
        )

    def test_create(self):
        curve_interpolation = CurveInterpolation(
            timeseries_group=self.timeseries_group1,
            target_timeseries_group=self.timeseries_group2,
        )
        curve_interpolation.save()
        self.assertEqual(CurveInterpolation.objects.count(), 1)

    def test_update(self):
        mommy.make(CurveInterpolation, timeseries_group=self.timeseries_group1)
        curve_interpolation = CurveInterpolation.objects.first()
        curve_interpolation.timeseries_group = self.timeseries_group2
        curve_interpolation.save()
        self.assertEqual(
            curve_interpolation.timeseries_group.id, self.timeseries_group2.id
        )

    def test_delete(self):
        mommy.make(CurveInterpolation, timeseries_group=self.timeseries_group1)
        curve_interpolation = CurveInterpolation.objects.first()
        curve_interpolation.delete()
        self.assertEqual(CurveInterpolation.objects.count(), 0)

    def test_str(self):
        curve_interpolation = mommy.make(
            CurveInterpolation,
            timeseries_group=self.timeseries_group1,
            target_timeseries_group=self.timeseries_group2,
        )
        self.assertEqual(str(curve_interpolation), "=> Group 2")

    def test_source_timeseries(self):
        self._make_timeseries(id=42, timeseries_group_num=1, type=Timeseries.RAW)
        self._make_timeseries(id=41, timeseries_group_num=2, type=Timeseries.PROCESSED)
        ci = mommy.make(
            CurveInterpolation,
            timeseries_group=self.timeseries_group1,
            target_timeseries_group=self.timeseries_group2,
        )
        self.assertEqual(ci.source_timeseries.id, 42)

    def _make_timeseries(self, id, timeseries_group_num, type):
        timeseries_group = getattr(self, f"timeseries_group{timeseries_group_num}")
        return mommy.make(
            Timeseries, id=id, timeseries_group=timeseries_group, type=type
        )

    def test_automatically_creates_source_timeseries(self):
        ci = mommy.make(
            CurveInterpolation,
            timeseries_group=self.timeseries_group1,
            target_timeseries_group=self.timeseries_group2,
        )
        self.assertFalse(Timeseries.objects.exists())
        ci.source_timeseries.id
        self.assertTrue(Timeseries.objects.exists())

    def test_target_timeseries(self):
        self._make_timeseries(id=42, timeseries_group_num=1, type=Timeseries.RAW)
        self._make_timeseries(id=41, timeseries_group_num=2, type=Timeseries.PROCESSED)
        ci = mommy.make(
            CurveInterpolation,
            timeseries_group=self.timeseries_group1,
            target_timeseries_group=self.timeseries_group2,
        )
        self.assertEqual(ci.target_timeseries.id, 41)

    def test_automatically_creates_target_timeseries(self):
        ci = mommy.make(
            CurveInterpolation,
            timeseries_group=self.timeseries_group1,
            target_timeseries_group=self.timeseries_group2,
        )
        self.assertFalse(Timeseries.objects.exists())
        ci.target_timeseries.id
        self.assertTrue(Timeseries.objects.exists())


class CurvePeriodTestCase(TestCase):
    def test_create(self):
        curve_interpolation = mommy.make(CurveInterpolation)
        curve_period = CurvePeriod(
            curve_interpolation=curve_interpolation,
            start_date=dt.date(2019, 9, 3),
            end_date=dt.date(2021, 9, 4),
        )
        curve_period.save()
        self.assertEqual(CurvePeriod.objects.count(), 1)

    def test_update(self):
        mommy.make(CurvePeriod)
        curve_period = CurvePeriod.objects.first()
        curve_period.start_date = dt.date(1963, 1, 1)
        curve_period.end_date = dt.date(1963, 12, 1)
        curve_period.save()
        curve_period = CurvePeriod.objects.first()
        self.assertEqual(curve_period.start_date, dt.date(1963, 1, 1))

    def test_delete(self):
        mommy.make(CurvePeriod)
        curve_period = CurvePeriod.objects.first()
        curve_period.delete()
        self.assertEqual(CurvePeriod.objects.count(), 0)

    def test_str(self):
        curve_period = mommy.make(
            CurvePeriod,
            curve_interpolation__target_timeseries_group__name="Discharge",
            start_date=dt.date(2019, 9, 3),
            end_date=dt.date(2021, 9, 4),
        )
        self.assertEqual(str(curve_period), "=> Discharge: 2019-09-03 - 2021-09-04")


class CurvePointTestCase(TestCase):
    def test_create(self):
        curve_period = mommy.make(CurvePeriod)
        point = CurvePoint(curve_period=curve_period, x=2.718, y=3.141)
        point.save()
        self.assertEqual(CurvePoint.objects.count(), 1)

    def test_update(self):
        mommy.make(CurvePoint)
        point = CurvePoint.objects.first()
        point.x = 2.718
        point.save()
        point = CurvePoint.objects.first()
        self.assertAlmostEqual(point.x, 2.718)

    def test_delete(self):
        mommy.make(CurvePoint)
        point = CurvePoint.objects.first()
        point.delete()
        self.assertEqual(CurvePoint.objects.count(), 0)

    def test_str(self):
        point = mommy.make(
            CurvePoint,
            curve_period__start_date=dt.date(2019, 9, 3),
            curve_period__end_date=dt.date(2021, 9, 4),
            curve_period__curve_interpolation__target_timeseries_group__name="pH",
            x=2.178,
            y=3.141,
        )
        self.assertEqual(
            str(point), "=> pH: 2019-09-03 - 2021-09-04: Point (2.178, 3.141)"
        )


class CurvePeriodSetCurveTestCase(TestCase):
    def setUp(self):
        self.period = mommy.make(
            CurvePeriod, start_date=dt.date(2019, 9, 3), end_date=dt.date(2021, 9, 4)
        )
        point = CurvePoint(curve_period=self.period, x=2.718, y=3.141)
        point.save()

    def test_set_curve(self):
        csv = textwrap.dedent(
            """\
            5,6
            7\t8
            9,10
            """
        )
        self.period.set_curve(csv)
        points = CurvePoint.objects.filter(curve_period=self.period).order_by("x")
        self.assertAlmostEqual(points[0].x, 5)
        self.assertAlmostEqual(points[0].y, 6)
        self.assertAlmostEqual(points[1].x, 7)
        self.assertAlmostEqual(points[1].y, 8)
        self.assertAlmostEqual(points[2].x, 9)
        self.assertAlmostEqual(points[2].y, 10)


class CurveInterpolationProcessTimeseriesTestCase(TestCase):
    _index = [
        dt.datetime(2019, 5, 21, 10, 20),
        dt.datetime(2019, 5, 21, 10, 30),
        dt.datetime(2019, 5, 21, 10, 40),
        dt.datetime(2019, 6, 21, 10, 50),
        dt.datetime(2019, 6, 21, 11, 00),
        dt.datetime(2019, 6, 21, 11, 10),
    ]

    source_timeseries = pd.DataFrame(
        data={
            "value": [2.9, 3.1, np.nan, 3.1, 4.9, 7.2],
            "flags": ["", "", "", "", "FLAG1", "FLAG2"],
        },
        columns=["value", "flags"],
        index=_index,
    )

    expected_result = pd.DataFrame(
        data={
            "value": [np.nan, 105, np.nan, 210, 345, np.nan],
            "flags": ["", "", "", "", "", ""],
        },
        columns=["value", "flags"],
        index=_index,
    )

    def test_execute(self):
        station = mommy.make(Station)
        self.curve_interpolation = mommy.make(
            CurveInterpolation,
            timeseries_group__gentity=station,
            target_timeseries_group__gentity=station,
        )
        self._setup_period1()
        self._setup_period2()
        self.curve_interpolation.htimeseries = HTimeseries(self.source_timeseries)
        result = self.curve_interpolation.process_timeseries()
        pd.testing.assert_frame_equal(result, self.expected_result)

    def _setup_period1(self):
        period1 = self._make_period(dt.date(2019, 5, 1), dt.date(2019, 5, 31))
        mommy.make(CurvePoint, curve_period=period1, x=3, y=100)
        mommy.make(CurvePoint, curve_period=period1, x=4, y=150)
        mommy.make(CurvePoint, curve_period=period1, x=5, y=175)

    def _setup_period2(self):
        period1 = self._make_period(dt.date(2019, 6, 1), dt.date(2019, 6, 30))
        mommy.make(CurvePoint, curve_period=period1, x=3, y=200)
        mommy.make(CurvePoint, curve_period=period1, x=4, y=300)
        mommy.make(CurvePoint, curve_period=period1, x=5, y=350)

    def _make_period(self, start_date, end_date):
        return mommy.make(
            CurvePeriod,
            curve_interpolation=self.curve_interpolation,
            start_date=start_date,
            end_date=end_date,
        )


class AggregationTestCase(TestCase):
    def setUp(self):
        self.station = mommy.make(Station)
        variable = mommy.make(Variable, descr="Irrelevant")
        self.timeseries_group = mommy.make(
            TimeseriesGroup, gentity=self.station, variable=variable
        )

    def test_create(self):
        aggregation = Aggregation(
            timeseries_group=self.timeseries_group, method="sum", max_missing=0
        )
        aggregation.save()
        self.assertEqual(Aggregation.objects.count(), 1)

    def _mommy_make_aggregation(self):
        return mommy.make(Aggregation, timeseries_group=self.timeseries_group)

    def test_update(self):
        self._mommy_make_aggregation()
        aggregation = Aggregation.objects.first()
        aggregation.method = "max"
        aggregation.save()
        self.assertEqual(aggregation.method, "max")

    def test_delete(self):
        self._mommy_make_aggregation()
        aggregation = Aggregation.objects.first()
        aggregation.delete()
        self.assertEqual(Aggregation.objects.count(), 0)

    def test_str(self):
        aggregation = self._mommy_make_aggregation()
        self.assertEqual(
            str(aggregation), "Aggregation for {}".format(self.timeseries_group)
        )

    def test_wrong_resulting_timestamp_offset_1(self):
        aggregation = self._mommy_make_aggregation()
        aggregation.resulting_timestamp_offset = "hello"
        with self.assertRaises(IntegrityError):
            aggregation.save()

    def test_wrong_resulting_timestamp_offset_2(self):
        aggregation = self._mommy_make_aggregation()
        aggregation.resulting_timestamp_offset = "-"
        with self.assertRaises(IntegrityError):
            aggregation.save()

    def test_wrong_resulting_timestamp_offset_3(self):
        aggregation = self._mommy_make_aggregation()
        aggregation.resulting_timestamp_offset = "15"
        with self.assertRaises(IntegrityError):
            aggregation.save()

    def test_wrong_resulting_timestamp_offset_4(self):
        aggregation = self._mommy_make_aggregation()
        aggregation.resulting_timestamp_offset = "-min"
        with self.assertRaises(IntegrityError):
            aggregation.save()

    def test_positive_time_step_without_number(self):
        aggregation = self._mommy_make_aggregation()
        aggregation.resulting_timestamp_offset = "min"
        aggregation.save()

    def test_positive_time_step_with_number(self):
        aggregation = self._mommy_make_aggregation()
        aggregation.resulting_timestamp_offset = "15min"
        aggregation.save()

    def test_negative_time_step(self):
        aggregation = self._mommy_make_aggregation()
        aggregation.resulting_timestamp_offset = "-1min"
        aggregation.save()

    def test_source_timeseries(self):
        self._make_timeseries(id=42, type=Timeseries.RAW)
        self._make_timeseries(id=41, type=Timeseries.AGGREGATED)
        aggregation = mommy.make(Aggregation, timeseries_group=self.timeseries_group)
        self.assertEqual(aggregation.source_timeseries.id, 42)

    def _make_timeseries(self, id, type):
        return mommy.make(
            Timeseries,
            id=id,
            timeseries_group=self.timeseries_group,
            type=type,
            time_step="H",
        )

    def test_automatically_creates_source_timeseries(self):
        aggregation = mommy.make(Aggregation, timeseries_group=self.timeseries_group)
        self.assertFalse(Timeseries.objects.exists())
        aggregation.source_timeseries.id
        self.assertTrue(Timeseries.objects.exists())

    def test_target_timeseries(self):
        self._make_timeseries(id=42, type=Timeseries.RAW)
        self._make_timeseries(id=41, type=Timeseries.AGGREGATED)
        aggregation = mommy.make(
            Aggregation, timeseries_group=self.timeseries_group, target_time_step="H"
        )
        self.assertEqual(aggregation.target_timeseries.id, 41)

    def test_automatically_creates_target_timeseries(self):
        aggregation = mommy.make(
            Aggregation, timeseries_group=self.timeseries_group, target_time_step="H"
        )
        self.assertFalse(Timeseries.objects.exists())
        aggregation.target_timeseries.id
        self.assertTrue(Timeseries.objects.exists())


class AggregationProcessTimeseriesTestCase(TestCase):
    _index = [
        dt.datetime(2019, 5, 21, 10, 00),
        dt.datetime(2019, 5, 21, 10, 10),
        dt.datetime(2019, 5, 21, 10, 21),
        dt.datetime(2019, 5, 21, 10, 31),
        dt.datetime(2019, 5, 21, 10, 40),
        dt.datetime(2019, 5, 21, 10, 50),
        dt.datetime(2019, 5, 21, 11, 00),
        dt.datetime(2019, 5, 21, 11, 10),
        dt.datetime(2019, 5, 21, 11, 20),
        dt.datetime(2019, 5, 21, 11, 30),
        dt.datetime(2019, 5, 21, 11, 40),
        dt.datetime(2019, 5, 21, 11, 50),
        dt.datetime(2019, 5, 21, 12, 00),
        dt.datetime(2019, 5, 21, 12, 10),
        dt.datetime(2019, 5, 21, 12, 20),
        dt.datetime(2019, 5, 21, 12, 30),
        dt.datetime(2019, 5, 21, 12, 40),
    ]
    _values = [2, 3, 5, 7, 11, 13, 17, 19, np.nan, 29, 31, 37, 41, 43, 47, 53, 59]

    source_timeseries = pd.DataFrame(
        data={"value": _values, "flags": 17 * [""]},
        columns=["value", "flags"],
        index=_index,
    )

    expected_result_for_max_missing_zero = pd.DataFrame(
        data={"value": [56.0], "flags": [""]},
        columns=["value", "flags"],
        index=[dt.datetime(2019, 5, 21, 10, 59)],
    )

    expected_result_for_max_missing_one = pd.DataFrame(
        data={"value": [56.0, 157.0], "flags": ["", "MISS"]},
        columns=["value", "flags"],
        index=[dt.datetime(2019, 5, 21, 10, 59), dt.datetime(2019, 5, 21, 11, 59)],
    )

    expected_result_for_max_missing_five = pd.DataFrame(
        data={"value": [2.0, 56.0, 157.0], "flags": ["MISS", "", "MISS"]},
        columns=["value", "flags"],
        index=[
            dt.datetime(2019, 5, 21, 9, 59),
            dt.datetime(2019, 5, 21, 10, 59),
            dt.datetime(2019, 5, 21, 11, 59),
        ],
    )

    def _execute(self, max_missing):
        station = mommy.make(Station)
        self.aggregation = mommy.make(
            Aggregation,
            timeseries_group__gentity=station,
            timeseries_group__variable__descr="Hello",
            target_timeseries_group__gentity=station,
            target_timeseries_group__variable__descr="Hello",
            target_time_step="H",
            method="sum",
            max_missing=max_missing,
            resulting_timestamp_offset="1min",
        )
        self.aggregation.htimeseries = HTimeseries(self.source_timeseries)
        self.aggregation.htimeseries.time_step = "10min"
        return self.aggregation.process_timeseries().data

    def test_execute_for_max_missing_zero(self):
        result = self._execute(max_missing=0)
        pd.testing.assert_frame_equal(result, self.expected_result_for_max_missing_zero)

    def test_execute_for_max_missing_one(self):
        result = self._execute(max_missing=1)
        pd.testing.assert_frame_equal(result, self.expected_result_for_max_missing_one)

    def test_execute_for_max_missing_five(self):
        result = self._execute(max_missing=5)
        pd.testing.assert_frame_equal(result, self.expected_result_for_max_missing_five)

    def test_execute_for_max_missing_too_high(self):
        result = self._execute(max_missing=10000)
        pd.testing.assert_frame_equal(result, self.expected_result_for_max_missing_five)
