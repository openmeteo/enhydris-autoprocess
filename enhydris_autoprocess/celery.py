from celery import Celery

from enhydris import set_django_settings_module

set_django_settings_module()

app = Celery("enhydris_autoprocess")

app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
