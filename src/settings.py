
from typing import List

from environs import Env
from pydantic import BaseModel


env = Env()


class CeleryConfig(BaseModel):
    timezone: str = "UTC"
    broker_url: str = env.str("CELERY_BROKER_URL", "sqs://fake:fake@localhost:4566/0")
    accept_content: List[str] = ["json"]
    task_serializer: str = "json"
    result_serializer: str = "json"
    task_time_limit: int = 20 * 60 * 60  # 20 hours
    result_expires: int = 60 * 24 * 7
    result_backend: str = env.str("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
    broker_transport_options: dict = {
        "polling_interval": 10,
        "wait_time_seconds": 10,  # valid values: 0 - 20
        # "endpoint_url": env.str("CELERY_SQS_ENDPOINT_URL", "http://localhost:4566"),
        "predefined_queues": {
            "CeleryQueue": {
                "url": env.str(
                    "CELERY_QUEUE_URL",
                    "http://localhost:4566/000000000000/CeleryQueue",
                ),
            }
        },
    }
    task_default_queue: str = "CeleryQueue"
    broker_transport: str = "sqs"
    worker_send_task_events: bool = True


class TaskIQConfig(BaseModel):
    broker_transport_options: dict = {
        "sqs_queue_name": env.str("TASKIQ_QUEUE_NAME", "TaskIqQueue"),
        "region_name": env.str("TASKIQ_SQS_REGION_NAME", "us-east-1"),
        "endpoint_url": env.str("TASKIQ_SQS_ENDPOINT_URL", "http://localhost:4566"),
        "max_number_of_messages": 10,
    }
    result_backend: str = env.str("TASKIQ_RESULT_BACKEND", "redis://localhost:6379/1")


class Settings(BaseModel):
    CELERY_CONFIG: CeleryConfig = CeleryConfig()
    TASKIQ_CONFIG: TaskIQConfig = TaskIQConfig()


settings = Settings()
