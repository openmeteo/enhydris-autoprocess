from unittest import mock

from django.test import TestCase

from model_mommy import mommy

from enhydris.models import Station
from enhydris_autoprocess import tasks
from enhydris_autoprocess.models import AutoProcess


class EnqueueAutoProcessTestCase(TestCase):
    def setUp(self):
        self.original_execute_auto_process = tasks.execute_auto_process
        tasks.execute_auto_process = mock.MagicMock()

    def tearDown(self):
        tasks.execute_auto_process = self.original_execute_auto_process

    def test_enqueues_auto_process(self):
        station = mommy.make(Station)
        auto_process = mommy.make(
            AutoProcess,
            station=station,
            source_timeseries__gentity=station,
            target_timeseries__gentity=station,
        )
        auto_process.source_timeseries.save()
        tasks.execute_auto_process.delay.assert_any_call(auto_process.id)
