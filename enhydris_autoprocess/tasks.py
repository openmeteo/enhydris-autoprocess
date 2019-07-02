from enhydris.celery import app


@app.task
def perform_validation(validation_id):
    from .models import Validation

    Validation.objects.get(id=validation_id).perform()
