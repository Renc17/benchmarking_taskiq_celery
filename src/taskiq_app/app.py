"""TaskIQ application configured with the SQS broker via LocalStack."""

from src.settings import settings

from taskiq_redis import RedisAsyncResultBackend
from taskiq_aio_sqs import SQSBroker

config = settings.TASKIQ_CONFIG.broker_transport_options

broker = SQSBroker(**config).with_result_backend(RedisAsyncResultBackend(settings.TASKIQ_CONFIG.result_backend))
