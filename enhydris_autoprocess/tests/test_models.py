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
from enhydris_autoprocess.models import RangeCheck, Validation


class ValidationTestCase(TestCase):
    def setUp(self):
        self.station1 = mommy.make(Station)
        self.timeseries1_1 = mommy.make(Timeseries, gentity=self.station1)
        self.timeseries1_2 = mommy.make(Timeseries, gentity=self.station1)
        self.timeseries1_3 = mommy.make(Timeseries, gentity=self.station1)
        self.station2 = mommy.make(Station)
        self.timeseries2_1 = mommy.make(Timeseries, gentity=self.station2)
        self.timeseries2_2 = mommy.make(Timeseries, gentity=self.station2)
        self.timeseries2_3 = mommy.make(Timeseries, gentity=self.station2)

        self.original_perform_validation = tasks.perform_validation
        tasks.perform_validation = mock.MagicMock()

    def tearDown(self):
        tasks.perform_validation = self.original_perform_validation

    def test_create(self):
        validation = Validation(
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        validation.save()
        self.assertEqual(Validation.objects.count(), 1)

    def test_update(self):
        mommy.make(
            Validation,
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        validation = Validation.objects.first()
        validation.target_timeseries = self.timeseries1_3
        validation.save()
        self.assertEqual(validation.target_timeseries.id, self.timeseries1_3.id)

    def test_delete(self):
        mommy.make(
            Validation,
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        validation = Validation.objects.first()
        validation.delete()
        self.assertEqual(Validation.objects.count(), 0)

    def test_str(self):
        validation = mommy.make(
            Validation,
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        saved_str = Timeseries.__str__
        try:
            Timeseries.__str__ = lambda self: "hello" + str(self.id)
            self.assertEqual(str(validation), "hello" + str(self.timeseries1_1.id))
        finally:
            Timeseries.__str__ = saved_str

    def test_only_accepts_source_timeseries_from_station(self):
        validation = mommy.make(
            Validation,
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        validation.source_timeseries = self.timeseries2_1
        with self.assertRaises(IntegrityError):
            validation.save()

    def test_only_accepts_target_timeseries_from_station(self):
        validation = mommy.make(
            Validation,
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        validation.target_timeseries = self.timeseries2_1
        with self.assertRaises(IntegrityError):
            validation.save()

    def test_save_triggers_validation(self):
        validation = Validation(
            station=self.station1,
            source_timeseries=self.timeseries1_1,
            target_timeseries=self.timeseries1_2,
        )
        validation.save()
        tasks.perform_validation.delay.assert_any_call(validation.id)


@RandomEnhydrisTimeseriesDataDir()
class ValidationPerformTestCase(TestCase):
    @mock.patch("enhydris_autoprocess.models.RangeCheck.perform")
    def setUp(self, m):
        self.mock_perform = m
        station = mommy.make(Station)
        self.source_timeseries = mommy.make(
            Timeseries, gentity=station, variable__descr="irrelevant"
        )
        self.target_timeseries = mommy.make(
            Timeseries, gentity=station, variable__descr="irrelevant"
        )
        self.validation = mommy.make(
            Validation,
            station=station,
            source_timeseries=self.source_timeseries,
            target_timeseries=self.target_timeseries,
        )
        self.range_check = mommy.make(RangeCheck, validation=self.validation)
        self.validation.perform()

    def test_called_once(self):
        self.assertEqual(len(self.mock_perform.mock_calls), 1)

    def test_called_with_empty_content(self):
        args = self.mock_perform.mock_calls[0][1]
        ahtimeseries = args[0]
        self.assertEqual(len(ahtimeseries.data), 0)


@RandomEnhydrisTimeseriesDataDir()
class ValidationPerformDealsOnlyWithNewerTimeseriesPartTestCase(TestCase):
    @mock.patch("enhydris_autoprocess.models.RangeCheck.perform")
    def setUp(self, m):
        self.mock_perform = m
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
        self.validation = mommy.make(
            Validation,
            station=station,
            source_timeseries=self.source_timeseries,
            target_timeseries=self.target_timeseries,
        )
        self.range_check = mommy.make(RangeCheck, validation=self.validation)
        self.validation.perform()

    def test_called_once(self):
        self.assertEqual(len(self.mock_perform.mock_calls), 1)

    def test_called_with_the_newer_part_of_the_timeseries(self):
        args = self.mock_perform.mock_calls[0][1]
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
        station = mommy.make(Station)
        self.validation = mommy.make(
            Validation,
            station=station,
            source_timeseries__gentity=station,
            target_timeseries__gentity=station,
        )

    def test_create(self):
        range_check = RangeCheck(
            validation=self.validation, upper_bound=42.7, lower_bound=-5.2
        )
        range_check.save()
        self.assertEqual(RangeCheck.objects.count(), 1)

    def test_update(self):
        mommy.make(RangeCheck, validation=self.validation, upper_bound=55.0)
        range_check = RangeCheck.objects.first()
        range_check.upper_bound = 1831.7
        range_check.save()
        self.assertAlmostEqual(range_check.upper_bound, 1831.7)

    def test_delete(self):
        mommy.make(RangeCheck, validation=self.validation)
        range_check = RangeCheck.objects.first()
        range_check.delete()
        self.assertEqual(RangeCheck.objects.count(), 0)

    @mock.patch("enhydris_autoprocess.models.RangeCheck.__str__", return_value="hello")
    def test_str(self, m):
        range_check = mommy.make(RangeCheck, validation=self.validation)
        self.assertEqual(str(range_check), "hello")


class RangeCheckPerformTestCase(TestCase):
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

    def test_perform(self):
        station = mommy.make(Station)
        self.range_check = mommy.make(
            RangeCheck,
            lower_bound=3,
            upper_bound=5,
            validation__station=station,
            validation__source_timeseries__gentity=station,
            validation__target_timeseries__gentity=station,
        )
        htimeseries = HTimeseries(self.source_timeseries)
        self.range_check.perform(htimeseries)
        pd.testing.assert_frame_equal(self.source_timeseries, self.expected_result)
