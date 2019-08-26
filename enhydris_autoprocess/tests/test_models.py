import datetime as dt
from unittest import mock

from django.db import IntegrityError
from django.test import TestCase

import numpy as np
import pandas as pd
from htimeseries import HTimeseries
from model_mommy import mommy

from enhydris.models import Station, Timeseries
from enhydris.tests import RandomEnhydrisTimeseriesDataDir
from enhydris_autoprocess import tasks
from enhydris_autoprocess.models import (
    AutoProcess,
    CurveInterpolation,
    CurvePoint,
    RangeCheck,
)


class AutoProcessTestCase(TestCase):
    def setUp(self):
        self.station1 = mommy.make(Station)
        self.timeseries1_1 = mommy.make(Timeseries, gentity=self.station1)
        self.timeseries1_2 = mommy.make(Timeseries, gentity=self.station1)
        self.timeseries1_3 = mommy.make(Timeseries, gentity=self.station1)
        self.station2 = mommy.make(Station)
        self.timeseries2_1 = mommy.make(Timeseries, gentity=self.station2)
        self.timeseries2_2 = mommy.make(Timeseries, gentity=self.station2)
        self.timeseries2_3 = mommy.make(Timeseries, gentity=self.station2)

        self.original_execute_auto_process = tasks.execute_auto_process
        tasks.execute_auto_process = mock.MagicMock()

    def tearDown(self):
        tasks.execute_auto_process = self.original_execute_auto_process

    def test_create(self):
        auto_process = AutoProcess(
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        auto_process.save()
        self.assertEqual(AutoProcess.objects.count(), 1)

    def test_update(self):
        mommy.make(
            AutoProcess,
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        auto_process = AutoProcess.objects.first()
        auto_process.target_timeseries = self.timeseries1_3
        auto_process.save()
        self.assertEqual(auto_process.target_timeseries.id, self.timeseries1_3.id)

    def test_delete(self):
        mommy.make(
            AutoProcess,
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        auto_process = AutoProcess.objects.first()
        auto_process.delete()
        self.assertEqual(AutoProcess.objects.count(), 0)

    def test_only_accepts_source_timeseries_from_station(self):
        auto_process = mommy.make(
            AutoProcess,
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        auto_process.source_timeseries = self.timeseries2_1
        with self.assertRaises(IntegrityError):
            auto_process.save()

    def test_only_accepts_target_timeseries_from_station(self):
        auto_process = mommy.make(
            AutoProcess,
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        auto_process.target_timeseries = self.timeseries2_1
        with self.assertRaises(IntegrityError):
            auto_process.save()

    def test_save_triggers_auto_process(self):
        auto_process = AutoProcess(
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        auto_process.save()
        tasks.execute_auto_process.apply_async.assert_any_call(
            args=[auto_process.id], countdown=1
        )


@RandomEnhydrisTimeseriesDataDir()
class AutoProcessExecuteTestCase(TestCase):
    @mock.patch(
        "enhydris_autoprocess.models.RangeCheck.process_timeseries",
        side_effect=lambda x: x,
    )
    def setUp(self, m):
        self.mock_execute = m
        station = mommy.make(Station)
        self.source_timeseries = mommy.make(
            Timeseries, gentity=station, variable__descr="irrelevant"
        )
        self.target_timeseries = mommy.make(
            Timeseries, gentity=station, variable__descr="irrelevant"
        )
        self.range_check = mommy.make(
            RangeCheck,
            station=station,
            source_timeseries=self.source_timeseries,
            target_timeseries=self.target_timeseries,
        )
        self.range_check.execute()

    def test_called_once(self):
        self.assertEqual(len(self.mock_execute.mock_calls), 1)

    def test_called_with_empty_content(self):
        args = self.mock_execute.mock_calls[0][1]
        ahtimeseries = args[0]
        self.assertEqual(len(ahtimeseries.data), 0)


@RandomEnhydrisTimeseriesDataDir()
class AutoProcessExecuteDealsOnlyWithNewerTimeseriesPartTestCase(TestCase):
    @mock.patch(
        "enhydris_autoprocess.models.RangeCheck.process_timeseries",
        side_effect=lambda x: x,
    )
    def setUp(self, m):
        self.mock_execute = m
        station = mommy.make(Station)
        self.source_timeseries = mommy.make(
            Timeseries, gentity=station, time_zone__utc_offset=0, variable__descr="h"
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
            Timeseries, gentity=station, time_zone__utc_offset=0, variable__descr="h"
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
        self.range_check = mommy.make(
            RangeCheck,
            station=station,
            source_timeseries=self.source_timeseries,
            target_timeseries=self.target_timeseries,
        )
        self.range_check.execute()

    def test_called_once(self):
        self.assertEqual(len(self.mock_execute.mock_calls), 1)

    def test_called_with_the_newer_part_of_the_timeseries(self):
        args = self.mock_execute.mock_calls[0][1]
        ahtimeseries = args[0]
        expected_arg = pd.DataFrame(
            data={"value": [3.0, 4.0], "flags": ["", ""]},
            columns=["value", "flags"],
            index=[dt.datetime(2019, 5, 21, 17, 20), dt.datetime(2019, 5, 21, 17, 30)],
        )
        expected_arg.index.name = "date"
        pd.testing.assert_frame_equal(ahtimeseries.data, expected_arg)

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


class RangeCheckTestCase(TestCase):
    def setUp(self):
        self.station = mommy.make(Station)
        self.timeseries1 = mommy.make(Timeseries, gentity=self.station)
        self.timeseries2 = mommy.make(Timeseries, gentity=self.station)

    def _mommy_make_range_check(self):
        return mommy.make(
            RangeCheck,
            station=self.station,
            source_timeseries=self.timeseries1,
            target_timeseries=self.timeseries2,
            upper_bound=55.0,
        )

    def test_create(self):
        range_check = RangeCheck(
            station=self.station,
            source_timeseries=self.timeseries1,
            target_timeseries=self.timeseries2,
            upper_bound=42.7,
            lower_bound=-5.2,
        )
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
        self.assertEqual(
            str(range_check), "Range check for {}".format(self.timeseries1)
        )


class RangeCheckProcessTimeseriesTestCase(TestCase):
    _index = [
        dt.datetime(2019, 5, 21, 10, 20),
        dt.datetime(2019, 5, 21, 10, 30),
        dt.datetime(2019, 5, 21, 10, 40),
        dt.datetime(2019, 5, 21, 10, 50),
        dt.datetime(2019, 5, 21, 11, 00),
    ]

    source_timeseries = pd.DataFrame(
        data={
            "value": [2.9, 3.1, np.nan, 4.9, 7.2],
            "flags": ["", "", "", "FLAG1", "FLAG2"],
        },
        columns=["value", "flags"],
        index=_index,
    )

    expected_result = pd.DataFrame(
        data={
            "value": [np.nan, 3.1, np.nan, 4.9, np.nan],
            "flags": ["RANGE", "", "", "FLAG1", "FLAG2 RANGE"],
        },
        columns=["value", "flags"],
        index=_index,
    )

    def test_execute(self):
        station = mommy.make(Station)
        self.range_check = mommy.make(
            RangeCheck,
            lower_bound=3,
            upper_bound=5,
            station=station,
            source_timeseries__gentity=station,
            target_timeseries__gentity=station,
        )
        htimeseries = HTimeseries(self.source_timeseries)
        result = self.range_check.process_timeseries(htimeseries)
        pd.testing.assert_frame_equal(result, self.expected_result)


class CurveInterpolationTestCase(TestCase):
    def setUp(self):
        self.station = mommy.make(Station)
        self.timeseries1 = mommy.make(Timeseries, gentity=self.station)
        self.timeseries2 = mommy.make(Timeseries, gentity=self.station)

    def test_create(self):
        curve_interpolation = CurveInterpolation(
            station=self.station,
            source_timeseries=self.timeseries1,
            target_timeseries=self.timeseries2,
            name="Stage-discharge",
        )
        curve_interpolation.save()
        self.assertEqual(CurveInterpolation.objects.count(), 1)

    def test_update(self):
        mommy.make(
            CurveInterpolation,
            station=self.station,
            source_timeseries=self.timeseries1,
            target_timeseries=self.timeseries2,
        )
        curve_interpolation = CurveInterpolation.objects.first()
        curve_interpolation.name = "Stage-discharge"
        curve_interpolation.save()
        self.assertEqual(curve_interpolation.name, "Stage-discharge")

    def test_delete(self):
        mommy.make(
            CurveInterpolation,
            station=self.station,
            source_timeseries=self.timeseries1,
            target_timeseries=self.timeseries2,
        )
        curve_interpolation = CurveInterpolation.objects.first()
        curve_interpolation.delete()
        self.assertEqual(CurveInterpolation.objects.count(), 0)

    def test_str(self):
        curve_interpolation = mommy.make(
            CurveInterpolation,
            station=self.station,
            source_timeseries=self.timeseries1,
            target_timeseries=self.timeseries2,
            name="Stage-discharge",
        )
        self.assertEqual(str(curve_interpolation), "Stage-discharge")


class CurvePointTestCase(TestCase):
    def setUp(self):
        station = mommy.make(Station)
        self.curve_interpolation = mommy.make(
            CurveInterpolation,
            station=station,
            source_timeseries__gentity=station,
            target_timeseries__gentity=station,
            name="Stage-discharge",
        )

    def test_create(self):
        point = CurvePoint(
            curve_interpolation=self.curve_interpolation, x=2.718, y=3.141
        )
        point.save()
        self.assertEqual(CurvePoint.objects.count(), 1)

    def test_update(self):
        mommy.make(CurvePoint, curve_interpolation=self.curve_interpolation)
        point = CurvePoint.objects.first()
        point.x = 2.718
        point.save()
        self.assertAlmostEqual(point.x, 2.718)

    def test_delete(self):
        mommy.make(CurvePoint, curve_interpolation=self.curve_interpolation)
        point = CurvePoint.objects.first()
        point.delete()
        self.assertEqual(CurvePoint.objects.count(), 0)

    def test_str(self):
        point = mommy.make(
            CurvePoint, curve_interpolation=self.curve_interpolation, x=2.178, y=3.141
        )
        self.assertEqual(str(point), "Stage-discharge: Point (2.178, 3.141)")


class CurveInterpolationProcessTimeseriesTestCase(TestCase):
    _index = [
        dt.datetime(2019, 5, 21, 10, 20),
        dt.datetime(2019, 5, 21, 10, 30),
        dt.datetime(2019, 5, 21, 10, 40),
        dt.datetime(2019, 5, 21, 10, 50),
        dt.datetime(2019, 5, 21, 11, 00),
    ]

    source_timeseries = pd.DataFrame(
        data={
            "value": [2.9, 3.1, np.nan, 4.9, 7.2],
            "flags": ["", "", "", "FLAG1", "FLAG2"],
        },
        columns=["value", "flags"],
        index=_index,
    )

    expected_result = pd.DataFrame(
        data={"value": [100, 105, np.nan, 172.5, 175], "flags": ["", "", "", "", ""]},
        columns=["value", "flags"],
        index=_index,
    )

    def test_execute(self):
        station = mommy.make(Station)
        ci = mommy.make(
            CurveInterpolation,
            station=station,
            source_timeseries__gentity=station,
            target_timeseries__gentity=station,
        )
        mommy.make(CurvePoint, curve_interpolation=ci, x=3, y=100)
        mommy.make(CurvePoint, curve_interpolation=ci, x=4, y=150)
        mommy.make(CurvePoint, curve_interpolation=ci, x=5, y=175)
        htimeseries = HTimeseries(self.source_timeseries)
        result = ci.process_timeseries(htimeseries)
        pd.testing.assert_frame_equal(result, self.expected_result)
