from unittest import mock

from django.test import TestCase

from model_mommy import mommy

from enhydris.models import Station, Timeseries
from enhydris_autoprocess import tasks
from enhydris_autoprocess.models import Checks


class EnqueueAutoProcessTestCase(TestCase):
    def setUp(self):
        self.original_execute_auto_process = tasks.execute_auto_process
        tasks.execute_auto_process = mock.MagicMock()

    def tearDown(self):
        tasks.execute_auto_process = self.original_execute_auto_process

    def test_enqueues_auto_process(self):
        station = mommy.make(Station)
        auto_process = mommy.make(
            Checks,
            timeseries_group__gentity=station,
            target_timeseries_group__gentity=station,
        )
        timeseries = mommy.make(
            Timeseries, timeseries_group=auto_process.timeseries_group
        )
        timeseries.save()
        tasks.execute_auto_process.delay.assert_any_call(auto_process.id)
