from django.apps import AppConfig
from django.db.models.signals import post_save

from . import tasks


def enqueue_validation(sender, *, instance, **kwargs):
    from enhydris.models import Timeseries

    try:
        tasks.perform_validation.delay(instance.validation)
    except Timeseries.validation.RelatedObjectDoesNotExist:
        pass


class AutoprocessConfig(AppConfig):
    name = "enhydris_autoprocess"

    def ready(self):
        post_save.connect(enqueue_validation, sender="enhydris.Timeseries")
