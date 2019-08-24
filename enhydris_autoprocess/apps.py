from django.apps import AppConfig
from django.db.models.signals import post_save

from . import tasks


def enqueue_auto_process(sender, *, instance, **kwargs):
    from enhydris.models import Timeseries

    try:
        tasks.execute_auto_process.delay(instance.auto_process.id)
    except Timeseries.auto_process.RelatedObjectDoesNotExist:
        pass


class AutoprocessConfig(AppConfig):
    name = "enhydris_autoprocess"

    def ready(self):
        post_save.connect(enqueue_auto_process, sender="enhydris.Timeseries")
