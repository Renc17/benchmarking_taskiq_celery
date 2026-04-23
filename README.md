<div align="center">

# ⚡ Benchmarking: TaskIQ vs Celery

**A head-to-head performance comparison of two Python task frameworks using SQS**

[![Python 3.14+](https://img.shields.io/badge/python-3.14%2B-3776AB?logo=python&logoColor=white)](https://www.python.org)
[![Celery](https://img.shields.io/badge/Celery-5.6-37814A?logo=celery&logoColor=white)](https://docs.celeryq.dev)
[![TaskIQ](https://img.shields.io/badge/TaskIQ-0.12-blue)](https://taskiq-python.github.io)
[![Broker](https://img.shields.io/badge/Broker-Amazon%20SQS-FF9900?logo=amazonsqs&logoColor=white)](https://aws.amazon.com/sqs/)

</div>

---

Both frameworks dispatch tasks to **Amazon SQS** (via [LocalStack](https://localstack.cloud)) and report throughput, latency, and memory usage across four workload profiles.

## 📋 Task Profiles

| Profile | Description | Workload Detail |
|---------|-------------|-----------------|
| `noop` | Empty task — pure framework overhead | Returns immediately |
| `cpu_bound` | Compute-heavy work | 100 000 SHA-256 hash iterations |
| `io_bound` | IO-heavy work | 50 ms `asyncio.sleep` |
| `mixed` | CPU + IO combined | 50 000 SHA-256 iterations + 20 ms sleep |

---

## 🚀 Quick Start

### Prerequisites

- **Docker** (with Compose v2)
- **[uv](https://docs.astral.sh/uv/)** — Python package manager

### 1. Clone & install

```bash
git clone <repo-url> && cd benchmarking_taskiq_celery
uv sync
```

### 2. Start infrastructure

```bash
docker compose up -d
```

This boots **LocalStack** (SQS) and **Redis** (Celery result backend), then automatically creates two SQS queues via CloudFormation:

- `CeleryQueue`
- `TaskIqQueue`

Verify the queues are ready:

```bash
AWS_ACCESS_KEY_ID=FAKE AWS_SECRET_ACCESS_KEY=FAKE \
  aws --endpoint-url=http://localhost:4566 sqs list-queues --region us-east-1
```

### 3. Run benchmarks

```bash
# Default: 50 tasks, concurrency 1 & 4, all profiles, both frameworks
uv run benchmark

# Custom run
uv run benchmark --tasks 500 --concurrency 1 4 8 --types cpu_bound io_bound

# Single framework
uv run benchmark --frameworks taskiq --tasks 200

# Export to CSV
uv run benchmark --csv results.csv
```

### 4. Teardown

```bash
docker compose down
```

### Environment Variables (optional)

The defaults target `localhost:4566`. Override if needed:

```bash
export CELERY_BROKER_URL="sqs://fake:fake@localstack:4566/0"
export CELERY_QUEUE_URL="http://localhost:4566/000000000000/CeleryQueue"
export TASKIQ_QUEUE_NAME="TaskIqQueue"
```

---

## 📊 Benchmark Results

> **500 tasks per profile · Concurrency 1 & 4 · Broker: SQS (LocalStack) · Result backend: Redis**

### Concurrency = 1

| Task Type | Framework | Total (s) | Tasks/s | Avg Latency (s) | Peak Mem (MB) |
|-----------|-----------|----------:|--------:|-----------------:|--------------:|
| noop | Celery | 1.89 | 264.60 | 0.0038 | 126 |
| noop | **TaskIQ** | **1.54** | **325.30** | **0.0031** | 155 |
| cpu_bound | Celery | 11.33 | 44.12 | 0.0227 | 128 |
| cpu_bound | **TaskIQ** | **2.74** | **182.65** | **0.0055** | 155 |
| io_bound | Celery | 28.06 | 17.82 | 0.0561 | 127 |
| io_bound | **TaskIQ** | **1.52** | **328.75** | **0.0030** | 155 |
| mixed | Celery | 17.81 | 28.07 | 0.0356 | 128 |
| mixed | **TaskIQ** | **1.75** | **286.53** | **0.0035** | 155 |

### Concurrency = 4

| Task Type | Framework | Total (s) | Tasks/s | Avg Latency (s) | Peak Mem (MB) |
|-----------|-----------|----------:|--------:|-----------------:|--------------:|
| noop | **Celery** | **1.40** | **357.69** | **0.0028** | 227 |
| noop | TaskIQ | 1.58 | 315.93 | 0.0032 | 403 |
| cpu_bound | Celery | 3.20 | 156.02 | 0.0064 | 230 |
| cpu_bound | **TaskIQ** | **2.30** | **216.99** | **0.0046** | 404 |
| io_bound | Celery | 6.94 | 72.00 | 0.0139 | 228 |
| io_bound | **TaskIQ** | **1.61** | **311.21** | **0.0032** | 403 |
| mixed | Celery | 4.41 | 113.39 | 0.0088 | 230 |
| mixed | **TaskIQ** | **1.96** | **254.60** | **0.0039** | 404 |

### TaskIQ Speedup over Celery

| Task Type | Concurrency 1 | Concurrency 4 |
|-----------|---------------:|---------------:|
| noop | **1.23×** | 0.88× *(Celery wins)* |
| cpu_bound | **4.14×** | **1.39×** |
| io_bound | **18.45×** | **4.32×** |
| mixed | **10.21×** | **2.25×** |

---

## 🔍 Analysis

### 1. TaskIQ dominates IO-heavy workloads

At concurrency 1, TaskIQ is **18.4× faster** on `io_bound` tasks (329 vs 18 tasks/s). TaskIQ workers run an asyncio event loop and can overlap hundreds of concurrent `await asyncio.sleep()` calls, while Celery's prefork worker blocks on each sleep sequentially. Even at concurrency 4, TaskIQ retains a **4.3× advantage**.

### 2. Significant CPU-bound gains

TaskIQ completes CPU work **4.1× faster** at concurrency 1, indicating substantially lower dispatch and result-collection overhead. The gap narrows to **1.4×** at concurrency 4 as Celery's prefork model parallelises CPU work across OS processes.

### 3. Framework overhead (noop)

The `noop` profile isolates pure overhead. TaskIQ is **1.23× faster** at concurrency 1, but at concurrency 4 **Celery edges ahead** (358 vs 316 tasks/s) — the prefork pool avoids asyncio coordination costs when there's no real work to do.

### 4. Memory trade-off

| Concurrency | Celery | TaskIQ | Overhead |
|:-----------:|-------:|-------:|---------:|
| 1 | ~127 MB | ~155 MB | +22% |
| 4 | ~229 MB | ~404 MB | +76% |

Celery is consistently leaner. Its prefork workers share memory via copy-on-write after `fork()`, while TaskIQ spawns full Python processes with independent asyncio event loops.

---

## ✅ Summary

| Dimension | Winner | Detail |
|-----------|--------|--------|
| IO-bound throughput | **TaskIQ** | Up to **18×** faster — native async multiplexes IO within one worker |
| CPU-bound throughput | **TaskIQ** | **1.4–4×** faster depending on concurrency |
| Pure overhead (noop) | **Mixed** | TaskIQ wins at low concurrency; Celery wins at high concurrency |
| Memory efficiency | **Celery** | **22–76%** less peak RSS |
| Reliability | **Tie** | 0 errors across all 8 000 tasks |

> **Bottom line:** TaskIQ delivers substantially higher throughput for IO-bound and mixed workloads thanks to its native asyncio execution model. Celery's prefork architecture is more memory-efficient and competitive at high-concurrency pure overhead, but cannot match TaskIQ's ability to multiplex IO-heavy tasks within a single worker.

---

## 🏗️ Project Structure

```
src/
├── celery_app/
│   ├── app.py              # Celery app configured with SQS broker
│   └── tasks.py            # Celery tasks (async_to_sync wrappers)
├── taskiq_app/
│   ├── app.py              # TaskIQ app with custom SQS broker
│   ├── tasks.py            # TaskIQ tasks (native async)
│   └── brokers/
│       └── sqs.py          # Custom SQS broker implementation
├── settings.py             # Pydantic settings (env-configurable)
├── tasks_common.py         # Shared async task implementations
└── benchmark.py            # CLI benchmark runner & reporter
localstack/
├── cloud-formation/
│   └── localstack-cf.yml   # CloudFormation template for SQS queues
└── scripts/
    └── 0001_initial.sh     # Deploys CF stack on LocalStack boot
docker-compose.yml          # LocalStack + Redis services
pyproject.toml              # Project metadata & dependencies
```
