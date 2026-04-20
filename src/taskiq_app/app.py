"""TaskIQ application configured with the SQS broker via LocalStack."""

from src.taskiq_app.brokers.sqs import SQSBroker
from src.settings import settings

from taskiq_redis import RedisAsyncResultBackend

config = settings.TASKIQ_CONFIG.broker_transport_options

broker = SQSBroker(**config).with_result_backend(RedisAsyncResultBackend(settings.TASKIQ_CONFIG.result_backend))
