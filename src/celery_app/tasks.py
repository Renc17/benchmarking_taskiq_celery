"""Celery tasks wrapping shared async implementations via async_to_sync."""

from asgiref.sync import async_to_sync

from src.celery_app.app import app
from src.tasks_common import cpu_bound, io_bound, mixed, noop


@app.task(name="celery.cpu_bound")
def cpu_bound_task(n: int = 100_000) -> str:
    return async_to_sync(cpu_bound)(n)


@app.task(name="celery.io_bound")
def io_bound_task(sleep_seconds: float = 0.05) -> str:
    return async_to_sync(io_bound)(sleep_seconds)


@app.task(name="celery.mixed")
def mixed_task(n: int = 50_000, sleep_seconds: float = 0.02) -> str:
    return async_to_sync(mixed)(n, sleep_seconds)


@app.task(name="celery.noop")
def noop_task() -> str:
    return async_to_sync(noop)()
