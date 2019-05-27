from unittest import mock

from django.test import TestCase

from model_mommy import mommy

from enhydris_autoprocess import tasks
from enhydris_autoprocess.models import Validation


class EnqueueValidationTestCase(TestCase):
    def setUp(self):
        self.original_perform_validation = tasks.perform_validation
        tasks.perform_validation = mock.MagicMock()

    def tearDown(self):
        tasks.perform_validation = self.original_perform_validation

    def test_enqueues_validation(self):
        validation = mommy.make(Validation)
        validation.source_timeseries.save()
        tasks.perform_validation.delay.assert_any_call(validation)
