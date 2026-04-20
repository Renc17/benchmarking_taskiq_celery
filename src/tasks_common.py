"""Shared async task implementations used by both Celery and TaskIQ."""

import asyncio
import hashlib


async def cpu_bound(n: int = 100_000) -> str:
    """Simulate CPU-bound work by hashing repeatedly."""
    data = b"benchmark"
    for _ in range(n):
        data = hashlib.sha256(data).digest()
    return data.hex()


async def io_bound(sleep_seconds: float = 0.05) -> str:
    """Simulate IO-bound work with a short sleep."""
    await asyncio.sleep(sleep_seconds)
    return "done"


async def mixed(n: int = 50_000, sleep_seconds: float = 0.02) -> str:
    """Simulate a task with both CPU and IO work."""
    data = b"benchmark"
    for _ in range(n):
        data = hashlib.sha256(data).digest()
    await asyncio.sleep(sleep_seconds)
    return data.hex()


async def noop() -> str:
    """Minimal overhead task – measures framework overhead."""
    return "ok"
