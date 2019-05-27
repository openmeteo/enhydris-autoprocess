from .celery import app


@app.task
def perform_validation(validation):
    validation.perform()
