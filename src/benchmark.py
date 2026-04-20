"""
Benchmark runner – compares Celery and TaskIQ on four task profiles:

  • noop       (pure framework overhead)
  • cpu_bound  (hash iterations)
  • io_bound   (async/sync sleep)
  • mixed      (CPU + IO)

Each profile is run with configurable concurrency levels and task counts.
Results are printed as a formatted table and optionally saved to CSV.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
import traceback

import psutil
from tabulate import tabulate

from taskiq import AsyncTaskiqTask


# ---------------------------------------------------------------------------
# Memory sampler
# ---------------------------------------------------------------------------
class MemorySampler:
    """Periodically samples total RSS of a process tree in a background thread."""

    def __init__(self, process: psutil.Process, interval: float = 0.1) -> None:
        self._process = process
        self._interval = interval
        self._peak_bytes: int = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)

    @property
    def peak_mb(self) -> float:
        return self._peak_bytes / (1024 * 1024)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                rss = self._process.memory_info().rss
                for child in self._process.children(recursive=True):
                    try:
                        rss += child.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        print(f"Warning: Process {child.pid} disappeared while sampling memory.")
                        pass
                if rss > self._peak_bytes:
                    self._peak_bytes = rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                print(f"Warning: Process {self._process.pid} disappeared while sampling memory.")
                break
            self._stop.wait(self._interval)


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------
@dataclass
class BenchmarkResult:
    framework: str
    task_type: str
    num_tasks: int
    concurrency: int
    total_time_s: float
    tasks_per_second: float
    avg_latency_s: float
    peak_memory_mb: float = 0.0
    errors: int = 0


# ---------------------------------------------------------------------------
# Celery benchmarks
# ---------------------------------------------------------------------------

def _start_celery_worker(concurrency: int) -> subprocess.Popen:
    """Spawn a Celery worker as a subprocess."""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "celery",
            "-A", "src.celery_app.tasks",
            "worker",
            "--loglevel=WARNING",
            f"--concurrency={concurrency}",
            "-Q", "CeleryQueue",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Give the worker a moment to boot.
    time.sleep(5)
    return proc


def _stop_celery_worker(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def run_celery_benchmark(
    task_type: str,
    num_tasks: int,
    concurrency: int,
) -> BenchmarkResult:
    """Send *num_tasks* to a Celery worker and measure wall-clock time."""
    from src.celery_app import tasks as celery_tasks  # noqa: E402

    task_map = {
        "noop": celery_tasks.noop_task,
        "cpu_bound": celery_tasks.cpu_bound_task,
        "io_bound": celery_tasks.io_bound_task,
        "mixed": celery_tasks.mixed_task,
    }
    task_fn = task_map[task_type]

    worker = _start_celery_worker(concurrency)
    process = psutil.Process(worker.pid)
    sampler = MemorySampler(process)
    sampler.start()

    try:
        start = time.perf_counter()
        results = [task_fn.delay() for _ in range(num_tasks)]

        errors = 0
        for r in results:
            try:
                r.get(timeout=120)
            except Exception:
                print(f"Error [Celery {task_type}]: {traceback.format_exc()}")
                errors += 1

        elapsed = time.perf_counter() - start

        return BenchmarkResult(
            framework="Celery",
            task_type=task_type,
            num_tasks=num_tasks,
            concurrency=concurrency,
            total_time_s=round(elapsed, 4),
            tasks_per_second=round(num_tasks / elapsed, 2) if elapsed else 0,
            avg_latency_s=round(elapsed / num_tasks, 6) if num_tasks else 0,
            peak_memory_mb=round(sampler.peak_mb, 2),
            errors=errors,
        )
    finally:
        sampler.stop()
        _stop_celery_worker(worker)


# ---------------------------------------------------------------------------
# TaskIQ benchmarks
# ---------------------------------------------------------------------------

def _start_taskiq_worker(concurrency: int) -> subprocess.Popen:
    """Spawn a TaskIQ worker as a subprocess."""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()
    worker = subprocess.Popen(
        [
            sys.executable, "-m", "taskiq",
            "worker",
            "--ack-type", "when_received",
            "--log-level", "WARNING",
            "--workers", str(concurrency),
            "src.taskiq_app.app:broker",
            "src.taskiq_app.tasks",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Give the worker a moment to boot.
    time.sleep(5)
    return worker

def _stop_taskiq_worker(worker: subprocess.Popen) -> None:
    worker.terminate()
    try:
        worker.wait(timeout=10)
    except subprocess.TimeoutExpired:
        worker.kill()

async def _run_taskiq_benchmark_async(
    task_type: str,
    num_tasks: int,
    concurrency: int,
) -> BenchmarkResult:
    """Kick and await *num_tasks* on the TaskIQ InMemoryBroker."""
    from src.taskiq_app.app import broker  # noqa: E402
    from src.taskiq_app import tasks as tq_tasks  # noqa: E402

    task_map: dict[str, AsyncTaskiqTask] = {
        "noop": tq_tasks.noop_task,
        "cpu_bound": tq_tasks.cpu_bound_task,
        "io_bound": tq_tasks.io_bound_task,
        "mixed": tq_tasks.mixed_task,
    }
    task_fn = task_map[task_type]

    await broker.startup()

    worker = _start_taskiq_worker(concurrency)
    process = psutil.Process(worker.pid)
    sampler = MemorySampler(process)
    sampler.start()

    try:
        start = time.perf_counter()
        errors = 0

        results = await asyncio.gather(*[task_fn.kiq() for _ in range(num_tasks)])
        for r in results:
            try:
                await r.wait_result(timeout=120)
            except Exception:
                print(f"Error [TaskIQ {task_type}]: {traceback.format_exc()}")
                errors += 1

        elapsed = time.perf_counter() - start

        return BenchmarkResult(
            framework="TaskIQ",
            task_type=task_type,
            num_tasks=num_tasks,
            concurrency=concurrency,
            total_time_s=round(elapsed, 4),
            tasks_per_second=round(num_tasks / elapsed, 2) if elapsed else 0,
            avg_latency_s=round(elapsed / num_tasks, 6) if num_tasks else 0,
            peak_memory_mb=round(sampler.peak_mb, 2),
            errors=errors,
        )
    finally:
        sampler.stop()
        await broker.shutdown()
        _stop_taskiq_worker(worker)


def run_taskiq_benchmark(
    task_type: str,
    num_tasks: int,
    concurrency: int,
) -> BenchmarkResult:
    return asyncio.run(
        _run_taskiq_benchmark_async(task_type, num_tasks, concurrency)
    )


# ---------------------------------------------------------------------------
# CLI & orchestration
# ---------------------------------------------------------------------------

TASK_TYPES = ["noop", "cpu_bound", "io_bound", "mixed"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark Celery vs TaskIQ",
    )
    parser.add_argument(
        "--tasks",
        type=int,
        default=50,
        help="Number of tasks to dispatch per benchmark (default: 50)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        nargs="+",
        default=[1, 4],
        help="Concurrency levels to test (default: 1 4)",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=TASK_TYPES,
        default=TASK_TYPES,
        help="Task types to benchmark (default: all)",
    )
    parser.add_argument(
        "--frameworks",
        nargs="+",
        choices=["celery", "taskiq"],
        default=["celery", "taskiq"],
        help="Frameworks to benchmark (default: both)",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default=None,
        help="Optional path to write results as CSV",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    results: list[BenchmarkResult] = []

    total_runs = (
        len(args.types)
        * len(args.concurrency)
        * len(args.frameworks)
    )
    current = 0

    for conc in args.concurrency:
        for ttype in args.types:
            if "celery" in args.frameworks:
                current += 1
                print(
                    f"[{current}/{total_runs}] Celery  | {ttype:>10} | "
                    f"concurrency={conc} | tasks={args.tasks}"
                )
                res = run_celery_benchmark(ttype, args.tasks, conc)
                results.append(res)

            if "taskiq" in args.frameworks:
                current += 1
                print(
                    f"[{current}/{total_runs}] TaskIQ  | {ttype:>10} | "
                    f"concurrency={conc} | tasks={args.tasks}"
                )
                res = run_taskiq_benchmark(ttype, args.tasks, conc)
                results.append(res)

    # ----- pretty-print results -----
    headers = [
        "Framework",
        "Task Type",
        "Tasks",
        "Concurrency",
        "Total (s)",
        "Tasks/s",
        "Avg Latency (s)",
        "Peak Mem (MB)",
        "Errors",
    ]
    rows = [
        [
            r.framework,
            r.task_type,
            r.num_tasks,
            r.concurrency,
            r.total_time_s,
            r.tasks_per_second,
            r.avg_latency_s,
            r.peak_memory_mb,
            r.errors,
        ]
        for r in results
    ]

    print("\n" + "=" * 80)
    print(tabulate(rows, headers=headers, tablefmt="github"))
    print("=" * 80)

    # ----- optional CSV export -----
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"\nResults written to {args.csv}")


if __name__ == "__main__":
    main()
