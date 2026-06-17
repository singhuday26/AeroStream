"""
AeroStream — Hyper-Personalization Engine
==========================================
Elite asynchronous backend for real-time AdTech/MarTech event processing
with sub-millisecond profile resolution and speculative execution.

Built for: Epsilon TeXpedition Hackathon — Theme 01: Hyper-personalization at Scale

Architecture Overview:
    ┌─────────────────────────────────────────────────────────────────────┐
    │                        AeroStream Engine                           │
    │                                                                     │
    │  ┌──────────────┐    ┌──────────────┐    ┌────────────────────┐   │
    │  │  FastAPI +    │───▶│  Sharded     │───▶│  Speculative       │   │
    │  │  Uvicorn      │    │  Async Cache │    │  Worker Pool       │   │
    │  │  (Ingestion)  │    │  (64 shards) │    │  (8 workers)       │   │
    │  │              │◀───│  O(1) lookup  │◀───│  asyncio.Queue     │   │
    │  └──────────────┘    └──────────────┘    └────────────────────┘   │
    │         │                    │                      │              │
    │         │                    │                      │              │
    │         ▼                    ▼                      ▼              │
    │  ┌─────────────────────────────────────────────────────────────┐  │
    │  │              Request Tracing Middleware                      │  │
    │  │         (ns-precision latency + trace IDs)                   │  │
    │  └─────────────────────────────────────────────────────────────┘  │
    └─────────────────────────────────────────────────────────────────────┘

Performance Targets:
    • Event ingestion:     <500μs per event (typically <200μs)
    • Cache resolution:    <50μs (O(1) sharded lookup)
    • Batch throughput:    >10,000 events/second on single process
    • Worker scoring:      1-15ms simulated ML inference (async, decoupled)

Run:
    uvicorn main:app --host 0.0.0.0 --port 8000 --loop uvloop --http httptools
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from aerostream.cache import CacheManager
from aerostream.config import settings
from aerostream.middleware import (
    BodySizeLimitMiddleware,
    CORSHeadersMiddleware,
    RequestTracingMiddleware,
)
from aerostream.routes import router
from aerostream.worker import WorkerPool

# ─── Logging Configuration ───────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)-22s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("aerostream.main")


# ─── Application Lifespan ───────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager — handles startup and shutdown
    of the cache layer and worker pool.

    Using the lifespan pattern (instead of deprecated @app.on_event)
    ensures resources are properly initialized before any requests
    are served and cleanly torn down on shutdown.
    """
    logger.info("-" * 60)
    logger.info("  AeroStream v%s - Starting Up", settings.app_version)
    logger.info("-" * 60)

    # ── Startup ──
    app.state.start_time = time.monotonic()

    # Initialize the sharded async cache
    cache_manager = CacheManager()
    await cache_manager.start_eviction_loop(interval=30.0)
    app.state.cache_manager = cache_manager
    logger.info("[OK] Cache layer online (%d shards)", settings.cache.num_shards)

    # Initialize and start the worker pool
    worker_pool = WorkerPool()
    worker_pool.set_cache(cache_manager)
    await worker_pool.start()
    app.state.worker_pool = worker_pool
    logger.info("[OK] Worker pool online (%d workers)", settings.worker.num_workers)

    logger.info("[OK] AeroStream ready - accepting connections on %s:%d",
                settings.host, settings.port)
    logger.info("-" * 60)

    yield  # ── Application runs here ──

    # ── Shutdown ──
    logger.info("AeroStream shutting down...")
    await worker_pool.shutdown(timeout=10.0)
    await cache_manager.shutdown()
    logger.info("AeroStream shutdown complete.")


# ─── FastAPI Application ────────────────────────────────────────────────────

app = FastAPI(
    title="AeroStream",
    description=(
        "High-throughput hyper-personalization engine for real-time "
        "AdTech/MarTech event processing. Features sub-millisecond "
        "profile resolution via sharded async cache and speculative "
        "execution background workers for predictive scoring."
    ),
    version=settings.app_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── Middleware Stack (order matters: last added = first executed) ──
# Execution order at runtime: BodySizeLimit → CORS → RequestTracing → Route
# BodySizeLimitMiddleware is a raw ASGI callable — registered via add_middleware
# it becomes the OUTERMOST layer, intercepting raw bytes before Starlette
# BaseHTTPMiddleware wrappers buffer anything into memory.
app.add_middleware(RequestTracingMiddleware)
app.add_middleware(CORSHeadersMiddleware)
app.add_middleware(BodySizeLimitMiddleware)  # PATCH B-07: outermost ASGI firewall

# ── Routes ──
app.include_router(router)


# ── Root Endpoint ──
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def serve_telemetry_dashboard():
    """
    Reads index.html from workspace root and returns it as the entrypoint response.
    Bypasses standard JSON serialization to serve native markup immediately.
    """
    import os
    file_path = "index.html"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content, status_code=200)
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>AeroStream Telemetry System Interface // index.html not found in root</h1>",
            status_code=404
        )


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        workers=settings.uvicorn_workers,
        log_level="info",
        access_log=False,  # We handle tracing in middleware
    )
