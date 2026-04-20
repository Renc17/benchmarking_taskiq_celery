"""Celery application configured to use SQS as the broker"""

from src.settings import settings

from celery import Celery


app = Celery(
    "benchmark",
    broker=settings.CELERY_CONFIG.broker_url,
)

app.config_from_object(settings.CELERY_CONFIG)
