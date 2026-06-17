"""
AeroStream Speculative Execution Worker
=========================================
Fully decoupled background processing pipeline for multi-agent
speculative decoding and predictive scoring.

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                    WorkerPool                               │
    │                                                             │
    │  ┌────────────┐    ┌───────────────────────────────────┐   │
    │  │ asyncio    │───▶│  Worker 0  │  Worker 1  │  ...    │   │
    │  │ Queue      │    │  (Task)    │  (Task)    │         │   │
    │  │ (bounded)  │    └───────────────────────────────────┘   │
    │  └────────────┘                                             │
    │       ▲                         │                           │
    │       │ enqueue()               ▼ results callback          │
    │       │                   ┌──────────┐                     │
    │       │                   │  Cache   │                     │
    │       │                   │  Update  │                     │
    │       │                   └──────────┘                     │
    └───────┼─────────────────────────────────────────────────────┘
            │
    HTTP Handler (fire-and-forget via enqueue)

Decoupling Strategy:
    The HTTP request handler calls `worker_pool.enqueue(task)` which is a
    non-blocking O(1) operation (asyncio.Queue.put_nowait). The request
    returns IMMEDIATELY with a task_id. Workers consume from the queue
    in a completely separate coroutine context, performing CPU-bound
    scoring simulation (via async sleep to simulate compute) and writing
    results back to the cache layer. This ensures the HTTP response
    latency is NEVER affected by scoring computation time.

Backpressure:
    The asyncio.Queue has a bounded maxsize. If the queue is full,
    enqueue() returns False instead of blocking, allowing the HTTP
    handler to gracefully degrade (return 503 or drop low-priority events).
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, List, Optional

from .config import settings
from .models import PersonalizationStrategy, SpeculativeResult

logger = logging.getLogger("aerostream.worker")


@dataclass
class ScoringTask:
    """A unit of work for the speculative execution pipeline."""
    __slots__ = (
        "task_id", "user_id", "event_type", "strategy",
        "payload", "enqueued_at", "priority",
    )
    task_id: str
    user_id: str
    event_type: str
    strategy: PersonalizationStrategy
    payload: Dict[str, Any]
    enqueued_at: float
    priority: int  # Lower = higher priority


class WorkerMetrics:
    """Real-time observability counters for the worker pool."""

    __slots__ = (
        "tasks_completed", "tasks_failed", "total_compute_time_us",
        "_latencies", "_max_latency_buffer",
    )

    def __init__(self, max_latency_buffer: int = 10_000) -> None:
        self.tasks_completed: int = 0
        self.tasks_failed: int = 0
        self.total_compute_time_us: int = 0
        # Ring buffer for latency percentile calculations
        self._latencies: List[int] = []
        self._max_latency_buffer = max_latency_buffer

    def record_latency(self, latency_us: int) -> None:
        """Record a task completion latency for percentile tracking."""
        self._latencies.append(latency_us)
        # Prevent unbounded memory growth — keep only the last N entries
        if len(self._latencies) > self._max_latency_buffer:
            self._latencies = self._latencies[-self._max_latency_buffer:]
        self.total_compute_time_us += latency_us

    @property
    def avg_latency_us(self) -> float:
        return (
            self.total_compute_time_us / self.tasks_completed
            if self.tasks_completed > 0
            else 0.0
        )

    @property
    def p99_latency_us(self) -> float:
        if not self._latencies:
            return 0.0
        sorted_lat = sorted(self._latencies)
        idx = int(len(sorted_lat) * 0.99)
        return float(sorted_lat[min(idx, len(sorted_lat) - 1)])


class SpeculativeEngine:
    """
    Simulates the multi-agent speculative decoding optimization.

    In production, this would dispatch to multiple ML model replicas
    running different decoding strategies in parallel, then select
    the best candidate via a verifier step (analogous to speculative
    decoding in LLM inference). Here we simulate the compute with
    calibrated async sleep to model realistic latency distributions.
    """

    # Pre-computed scoring dimensions for each strategy
    _SCORING_DIMENSIONS: Dict[PersonalizationStrategy, List[str]] = {
        PersonalizationStrategy.COLLABORATIVE_FILTERING: [
            "item_affinity", "user_similarity", "co_occurrence",
            "temporal_decay", "category_preference",
        ],
        PersonalizationStrategy.CONTEXTUAL_BANDIT: [
            "exploit_score", "explore_bonus", "ucb_confidence",
            "reward_estimate", "context_relevance",
        ],
        PersonalizationStrategy.SEMANTIC_EMBEDDING: [
            "cosine_similarity", "embedding_distance", "cluster_membership",
            "topic_coherence", "intent_alignment",
        ],
        PersonalizationStrategy.HYBRID_ENSEMBLE: [
            "cf_weight", "bandit_weight", "semantic_weight",
            "ensemble_confidence", "diversity_bonus", "recency_boost",
        ],
    }

    _PREDICTED_ACTIONS = [
        "click_ad", "view_product", "add_to_cart", "begin_checkout",
        "subscribe_newsletter", "watch_video", "share_content",
        "compare_products", "read_reviews", "save_for_later",
    ]

    async def execute(self, task: ScoringTask) -> SpeculativeResult:
        """
        Execute speculative scoring for a single task.
        Simulates variable-latency ML inference via calibrated async sleep.
        """
        start = time.perf_counter_ns()

        # Simulate compute — this yields to the event loop, allowing other
        # coroutines to run. The sleep duration models realistic ML inference
        # latency (1-15ms depending on model complexity).
        compute_latency = random.uniform(
            settings.worker.min_scoring_latency,
            settings.worker.max_scoring_latency,
        )
        await asyncio.sleep(compute_latency)

        # Generate simulated scoring dimensions
        dimensions = self._SCORING_DIMENSIONS.get(
            task.strategy,
            self._SCORING_DIMENSIONS[PersonalizationStrategy.HYBRID_ENSEMBLE],
        )

        # Produce deterministic-looking but varied scores
        # Seed with user_id hash for reproducibility in demos
        seed = hash(task.user_id + task.event_type) & 0xFFFFFFFF
        rng = random.Random(seed)
        scores = {dim: round(rng.random(), 4) for dim in dimensions}

        # Predict next user action
        predicted_action = rng.choice(self._PREDICTED_ACTIONS)
        confidence = round(rng.uniform(0.65, 0.98), 4)

        elapsed_us = (time.perf_counter_ns() - start) // 1000

        return SpeculativeResult(
            task_id=task.task_id,
            user_id=task.user_id,
            strategy=task.strategy,
            scores=scores,
            predicted_next_action=predicted_action,
            confidence=confidence,
            computation_time_us=elapsed_us,
        )


class WorkerPool:
    """
    Pool of async workers consuming from a bounded queue.

    Concurrency Model:
        Each worker is a long-lived asyncio.Task that loops on Queue.get().
        asyncio.Queue is the synchronization primitive — it's coroutine-safe
        by design (uses internal asyncio.Lock + asyncio.Event), so multiple
        producer/consumer coroutines can safely interact without external locks.

    Backpressure:
        Queue is bounded (default 10K). When full, enqueue() returns False
        immediately (non-blocking), letting the HTTP handler decide whether
        to retry, drop, or return 503 Service Unavailable.

    Graceful Shutdown:
        Sentinel values (None) are pushed into the queue to signal workers
        to exit. Workers drain remaining tasks before terminating.
    """

    def __init__(self, cache_manager=None, config=None) -> None:
        cfg = config or settings.worker
        self._num_workers = cfg.num_workers
        self._queue: asyncio.Queue[Optional[ScoringTask]] = asyncio.Queue(
            maxsize=cfg.queue_maxsize
        )
        self._engine = SpeculativeEngine()
        self._cache = cache_manager  # Will be injected at startup
        self._workers: List[asyncio.Task] = []
        self.metrics = WorkerMetrics()
        self._running = False

        # Optional callback for result delivery (e.g., webhook, SSE push)
        self._result_callback: Optional[
            Callable[[SpeculativeResult], Coroutine]
        ] = None

        logger.info(
            "WorkerPool initialized: %d workers, queue capacity %d",
            self._num_workers, cfg.queue_maxsize,
        )

    def set_cache(self, cache_manager) -> None:
        """Inject cache manager dependency (avoids circular imports)."""
        self._cache = cache_manager

    def set_result_callback(
        self, callback: Callable[[SpeculativeResult], Coroutine]
    ) -> None:
        """Register an async callback invoked on every completed result."""
        self._result_callback = callback

    async def start(self) -> None:
        """Spawn all worker coroutines as background tasks."""
        if self._running:
            return
        self._running = True
        for i in range(self._num_workers):
            task = asyncio.create_task(
                self._worker_loop(worker_id=i),
                name=f"aerostream-worker-{i}",
            )
            self._workers.append(task)
        logger.info("Started %d speculative execution workers", self._num_workers)

    async def _worker_loop(self, worker_id: int) -> None:
        """
        Main worker loop — continuously dequeues and processes scoring tasks.
        Exits when it receives a None sentinel value.
        """
        logger.debug("Worker %d started", worker_id)
        while True:
            # Queue.get() is a coroutine — it yields to the event loop
            # when the queue is empty, so workers consume zero CPU while idle
            task = await self._queue.get()

            # Sentinel check for graceful shutdown
            if task is None:
                self._queue.task_done()
                logger.debug("Worker %d received shutdown sentinel", worker_id)
                break

            try:
                result = await self._engine.execute(task)
                self.metrics.tasks_completed += 1
                self.metrics.record_latency(result.computation_time_us)

                # Write result back to cache for future profile enrichment
                if self._cache is not None:
                    cache_key = f"profile:{task.user_id}"
                    profile, _ = await self._cache.get_or_create(
                        cache_key,
                        lambda k: {
                            "user_id": task.user_id,
                            "segments": [],
                            "interaction_count": 0,
                            "speculative_predictions": [],
                        },
                    )
                    # Append the speculative result to the user's profile
                    profile["speculative_predictions"] = (
                        profile.get("speculative_predictions", [])[-9:]  # Keep last 10
                        + [result.model_dump()]
                    )
                    profile["interaction_count"] = profile.get("interaction_count", 0) + 1
                    await self._cache.set(cache_key, profile)

                # Fire optional result callback
                if self._result_callback is not None:
                    try:
                        await self._result_callback(result)
                    except Exception as cb_err:
                        logger.warning("Result callback error: %s", cb_err)

            except Exception as exc:
                self.metrics.tasks_failed += 1
                logger.error(
                    "Worker %d failed on task %s: %s",
                    worker_id, task.task_id, exc, exc_info=True,
                )
            finally:
                self._queue.task_done()

    def enqueue(self, task: ScoringTask) -> bool:
        """
        Non-blocking enqueue. Returns True if the task was accepted,
        False if the queue is full (backpressure signal).
        Uses put_nowait() which raises QueueFull instead of blocking,
        ensuring the HTTP handler's latency is NEVER affected by queue depth.
        """
        try:
            self._queue.put_nowait(task)
            return True
        except asyncio.QueueFull:
            logger.warning("Worker queue full — dropping task %s", task.task_id)
            return False

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    @property
    def active_workers(self) -> int:
        return sum(1 for w in self._workers if not w.done())

    async def shutdown(self, timeout: float = 10.0) -> None:
        """
        Graceful shutdown sequence:
        1. Send sentinel (None) to each worker
        2. Wait for all workers to finish with timeout
        3. Cancel any stragglers
        """
        if not self._running:
            return
        self._running = False
        logger.info("Initiating worker pool shutdown...")

        # Send one sentinel per worker
        for _ in range(self._num_workers):
            await self._queue.put(None)

        # Wait for workers to drain
        done, pending = await asyncio.wait(
            self._workers, timeout=timeout
        )

        # Force-cancel any workers that didn't finish in time
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        logger.info(
            "Worker pool shutdown complete. %d tasks completed, %d failed, avg latency: %.0fμs",
            self.metrics.tasks_completed,
            self.metrics.tasks_failed,
            self.metrics.avg_latency_us,
        )
