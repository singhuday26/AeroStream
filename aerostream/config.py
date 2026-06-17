"""
AeroStream Configuration Module
================================
Centralized configuration for the AeroStream hyper-personalization engine.
All tunable parameters are isolated here for rapid iteration during the hackathon.
"""

from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass(frozen=True, slots=True)
class CacheConfig:
    """Configuration for the async in-memory semantic cache layer."""
    # Maximum number of user profiles to hold in memory before LRU eviction
    max_entries: int = 100_000
    # Default TTL for cache entries in seconds (15 minutes)
    default_ttl_seconds: float = 900.0
    # Number of shards to distribute lock contention across
    # Higher shard count = lower lock contention under concurrent load
    num_shards: int = 64


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Configuration for the speculative execution background workers."""
    # Number of concurrent speculative decoding workers
    num_workers: int = 8
    # Maximum items the internal task queue can hold before backpressure kicks in
    queue_maxsize: int = 10_000
    # Simulated latency range (seconds) for speculative scoring computation
    min_scoring_latency: float = 0.001  # 1ms
    max_scoring_latency: float = 0.015  # 15ms
    # Interval (seconds) between worker heartbeat/metrics flushes
    metrics_flush_interval: float = 5.0


@dataclass(frozen=True, slots=True)
class IngestionConfig:
    """Configuration for the high-throughput event ingestion pipeline."""
    # Maximum payload size in bytes (64KB)
    max_payload_bytes: int = 65_536
    # Rate limit: max events per second per client IP (0 = unlimited)
    rate_limit_per_second: int = 0
    # Enable/disable request tracing for latency diagnostics
    enable_tracing: bool = True


@dataclass(frozen=True, slots=True)
class AeroStreamConfig:
    """Root configuration aggregating all subsystem configs."""
    app_name: str = "AeroStream"
    app_version: str = "1.0.0"
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8000
    # Uvicorn worker count — for the hackathon demo, single-process is fine
    # because asyncio handles concurrency within one event loop
    uvicorn_workers: int = 1

    cache: CacheConfig = field(default_factory=CacheConfig)
    worker: WorkerConfig = field(default_factory=WorkerConfig)
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)


# Singleton config instance — import this everywhere
settings = AeroStreamConfig()
