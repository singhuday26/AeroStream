# AeroStream

**Hyper-Personalization Engine** — Built for **Epsilon TeXpedition 2026** (Theme 01: Hyper-personalization at Scale)

A high-throughput, fully asynchronous AdTech/MarTech backend delivering **sub-millisecond profile resolution** and **speculative execution** at scale — built with FastAPI + Python asyncio.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AeroStream Engine                            │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────┐   │
│  │  FastAPI +    │───▶│  Sharded     │───▶│  Speculative       │   │
│  │  Uvicorn      │    │  Async Cache │    │  Worker Pool       │   │
│  │  (Ingestion)  │    │  (64 shards) │    │  (8 workers)       │   │
│  │              │◀───│  O(1) lookup  │◀───│  asyncio.Queue     │   │
│  └──────────────┘    └──────────────┘    └────────────────────┘   │
│         │                    │                      │              │
│         ▼                    ▼                      ▼              │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │              Request Tracing Middleware                      │  │
│  │         (ns-precision latency + trace IDs)                   │  │
│  └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Benchmark Results (Verified)

| Metric | Result |
|---|---|
| **Single event latency** | **69μs** |
| **Burst throughput** | **8,921 events/sec** |
| **Cache hit rate** | **94.5%** |
| **1000 concurrent events** | **1000/1000 accepted** |
| **Min server latency** | **34μs** |
| **Adversarial QA defense rate** | **100% (52/52 probes)** |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Python 3.x + FastAPI |
| ASGI Server | Uvicorn + httptools |
| Validation | Pydantic V2 (Rust core) |
| Concurrency | asyncio (cooperative multitasking) |
| Cache | 64-shard sharded OrderedDict (O(1) LRU) |
| Workers | asyncio.Queue + 8 consumer coroutines |

---

## Project Structure

```
AeroStream/
├── main.py                    # FastAPI app entry point, lifespan management
├── requirements.txt           # Minimal deps: fastapi, pydantic, uvicorn, httptools
├── fuzz_harness.py            # 52-probe adversarial QA & penetration test harness
├── test_smoke.py              # Functional smoke tests + benchmark
└── aerostream/
    ├── __init__.py
    ├── config.py              # Frozen dataclass configuration
    ├── models.py              # Pydantic V2 schemas (events, profiles, results)
    ├── cache.py               # 64-shard async cache with LRU + TTL eviction
    ├── worker.py              # Speculative execution worker pool
    ├── middleware.py          # Request tracing + CORS middleware
    └── routes.py              # All API endpoints
```

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start the server
uvicorn main:app --host 0.0.0.0 --port 8000

# Open interactive API docs
# http://localhost:8000/docs

# Run smoke tests + benchmark
python test_smoke.py

# Run adversarial QA harness (52 probes)
python fuzz_harness.py --concurrency 32

# Burst simulation (5000 events)
curl -X POST "http://localhost:8000/api/v1/simulate/burst?count=5000"
```

---

## API Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Engine overview + endpoint directory |
| `POST` | `/api/v1/stream-event` | Single event ingestion (hot path) |
| `POST` | `/api/v1/stream-events/batch` | Batch ingestion (up to 500 events) |
| `GET` | `/api/v1/profile/{user_id}` | Cached profile resolution |
| `GET` | `/api/v1/health` | Health check with live metrics |
| `GET` | `/api/v1/metrics/summary` | Performance dashboard |
| `POST` | `/api/v1/simulate/burst?count=N` | Burst simulation for demos |
| `GET` | `/docs` | Interactive Swagger UI |

---

## Key Design Decisions

1. **Sharded Cache (64 shards)** — Lock contention drops to ~1.5% at 1000 concurrent coroutines. Each shard has its own `asyncio.Lock()`.
2. **OrderedDict for O(1) LRU** — `move_to_end()` on access + `popitem(last=False)` for eviction — zero heap overhead.
3. **Bounded asyncio.Queue** — Backpressure via `put_nowait()`. HTTP handler **never** blocks when queue is full.
4. **Fire-and-forget scoring** — `enqueue()` is O(1). Workers consume and score asynchronously, decoupled from HTTP latency.
5. **Pydantic V2 Rust core** — Validation runs at ~10x speed of V1. All schema violations rejected before application logic runs.
6. **Zero blocking I/O** — No `time.sleep()`, no synchronous DB calls, no blocking locks anywhere in the stack.

---

## Adversarial QA Coverage

The `fuzz_harness.py` covers 3 threat classes across 52 probes:

- **Class A (22 probes)** — Schema violations, type injections, SQL/RCE/NoSQL injection strings, enum poisoning
- **Class B (10 probes)** — Volumetric DoS: 1MB strings, 50K float arrays, unicode bombs, deep nesting
- **Class C (20 probes)** — Boundary conditions: off-by-one lengths, whitespace IDs, empty dicts, prototype pollution keys

**Result: 100% defense rate. Engine fully operational post-fuzz.**

---

## Built By

**Team:** uday.23bce7842  
**Hackathon:** Epsilon TeXpedition 2026 — VIT  
**Theme:** 01 — Hyper-personalization at Scale
