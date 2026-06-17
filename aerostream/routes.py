"""
AeroStream API Routes
======================
High-throughput ingestion endpoints, profile resolution, health checks,
and speculative result retrieval.

All endpoints are fully async — zero blocking calls anywhere in the stack.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, status

from .models import (
    BatchIngestionAck,
    BatchStreamEvents,
    EventType,
    HealthStatus,
    IngestionAck,
    PersonalizationStrategy,
    ProfileSnapshot,
    StreamEvent,
)
from .worker import ScoringTask

logger = logging.getLogger("aerostream.routes")

# ─── Router Setup ────────────────────────────────────────────────────────────

router = APIRouter(prefix="/api/v1", tags=["AeroStream Core"])


# ─── Dependency Accessors ────────────────────────────────────────────────────

def _get_cache(request: Request):
    """Extract the shared CacheManager from app state."""
    return request.app.state.cache_manager


def _get_worker_pool(request: Request):
    """Extract the shared WorkerPool from app state."""
    return request.app.state.worker_pool


# ─── Event Ingestion ─────────────────────────────────────────────────────────

@router.post(
    "/stream-event",
    response_model=IngestionAck,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a single real-time behavioral event",
    description=(
        "Primary high-throughput ingestion endpoint. Accepts a single "
        "behavioral event, resolves the user profile from cache, and "
        "dispatches a speculative scoring task to the background worker "
        "pool. Returns immediately with an acknowledgment — the scoring "
        "computation is fully decoupled from the HTTP response cycle."
    ),
)
async def ingest_stream_event(event: StreamEvent, request: Request) -> IngestionAck:
    """
    Critical hot path — every microsecond matters here.

    Flow:
    1. Validate event (Pydantic handles this before we enter the function)
    2. Resolve/create user profile in cache (O(1) via sharded lookup)
    3. Enqueue speculative scoring task (O(1) via asyncio.Queue.put_nowait)
    4. Return ACK immediately
    """
    start_ns = time.perf_counter_ns()
    cache = _get_cache(request)
    worker_pool = _get_worker_pool(request)

    # Step 1: Resolve or create user profile from cache
    cache_key = f"profile:{event.user_context.user_id}"

    profile, cache_hit = await cache.get_or_create(
        cache_key,
        factory=lambda k: {
            "user_id": event.user_context.user_id,
            "segments": list(event.user_context.segment_tags),
            "interaction_count": 0,
            "last_event_type": None,
            "last_seen_ms": 0,
            "speculative_predictions": [],
        },
    )

    # Step 2: Update profile with this event's metadata
    profile["interaction_count"] = profile.get("interaction_count", 0) + 1
    profile["last_event_type"] = event.event_type.value
    profile["last_seen_ms"] = event.timestamp_ms

    # Merge new segment tags without duplicates
    existing_segments = set(profile.get("segments", []))
    existing_segments.update(event.user_context.segment_tags)
    profile["segments"] = list(existing_segments)

    await cache.set(cache_key, profile)

    # Step 3: Dispatch speculative scoring to background workers
    strategy = event.personalization_hint or PersonalizationStrategy.HYBRID_ENSEMBLE
    task_id = str(uuid.uuid4())

    scoring_task = ScoringTask(
        task_id=task_id,
        user_id=event.user_context.user_id,
        event_type=event.event_type.value,
        strategy=strategy,
        payload=event.payload,
        enqueued_at=time.monotonic(),
        priority=0,
    )

    enqueued = worker_pool.enqueue(scoring_task)
    if not enqueued:
        # Queue full — we still accept the event (it's cached) but warn
        # that speculative scoring is degraded
        logger.warning(
            "Backpressure: scoring task dropped for user %s (queue full)",
            event.user_context.user_id,
        )
        task_id = None

    # Step 4: Build and return ACK
    elapsed_us = (time.perf_counter_ns() - start_ns) // 1000

    return IngestionAck(
        event_id=event.event_id,
        status="accepted",
        cache_hit=cache_hit,
        profile_resolved=True,
        speculative_task_id=task_id,
        processing_time_us=elapsed_us,
    )


@router.post(
    "/stream-events/batch",
    response_model=BatchIngestionAck,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a batch of behavioral events",
    description=(
        "High-throughput batch endpoint for clients sending event arrays. "
        "Each event is processed concurrently using asyncio.gather for "
        "maximum parallelism within the event loop."
    ),
)
async def ingest_batch_events(
    batch: BatchStreamEvents, request: Request
) -> BatchIngestionAck:
    """
    Batch ingestion processes all events concurrently via asyncio.gather.
    This means N events complete in ~O(1) wall-clock time (limited by the
    slowest individual event, not the sum of all events).
    """
    start_ns = time.perf_counter_ns()

    acks: List[IngestionAck] = []
    rejected = 0

    # Process all events concurrently — asyncio.gather runs them as
    # interleaved coroutines on the same event loop thread
    async def process_one(event: StreamEvent) -> Optional[IngestionAck]:
        try:
            return await ingest_stream_event(event, request)
        except Exception as exc:
            logger.error("Batch event %s failed: %s", event.event_id, exc)
            return None

    results = await asyncio.gather(
        *(process_one(evt) for evt in batch.events),
        return_exceptions=False,
    )

    for result in results:
        if result is not None:
            acks.append(result)
        else:
            rejected += 1

    total_us = (time.perf_counter_ns() - start_ns) // 1000

    return BatchIngestionAck(
        accepted=len(acks),
        rejected=rejected,
        acks=acks,
        total_processing_time_us=total_us,
    )


# ─── Profile Resolution ──────────────────────────────────────────────────────

@router.get(
    "/profile/{user_id}",
    response_model=ProfileSnapshot,
    summary="Retrieve a user's resolved profile from cache",
    description=(
        "O(1) profile lookup from the sharded in-memory cache. Returns the "
        "full profile including accumulated segments, interaction counts, "
        "and any completed speculative prediction results."
    ),
)
async def get_user_profile(user_id: str, request: Request) -> ProfileSnapshot:
    cache = _get_cache(request)
    cache_key = f"profile:{user_id}"

    profile = await cache.get(cache_key)
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No profile found for user '{user_id}'",
        )

    # Extract personalization scores from the latest speculative prediction
    predictions = profile.get("speculative_predictions", [])
    latest_scores: Dict[str, float] = {}
    if predictions:
        latest = predictions[-1]
        latest_scores = latest.get("scores", {})

    return ProfileSnapshot(
        user_id=profile["user_id"],
        segments=profile.get("segments", []),
        interaction_count=profile.get("interaction_count", 0),
        last_event_type=profile.get("last_event_type"),
        last_seen_ms=profile.get("last_seen_ms", 0),
        personalization_scores=latest_scores,
        speculative_predictions=predictions[-5:],  # Return last 5
    )


# ─── Health & Observability ──────────────────────────────────────────────────

@router.api_route(
    "/health",
    methods=["GET", "HEAD"],
    response_model=HealthStatus,
    summary="System health check with live performance metrics",
    description=(
        "Returns comprehensive system health including cache hit rate, "
        "worker queue depth, throughput, and latency percentiles. "
        "Use this to verify sub-millisecond processing under load."
    ),
)
async def health_check(request: Request) -> HealthStatus:
    cache = _get_cache(request)
    worker_pool = _get_worker_pool(request)
    app_state = request.app.state

    uptime = time.monotonic() - app_state.start_time
    total_events = cache.metrics.hits + cache.metrics.misses

    return HealthStatus(
        status="healthy",
        uptime_seconds=round(uptime, 2),
        cache_entries=await cache.total_entries(),
        cache_hit_rate=round(cache.metrics.hit_rate, 4),
        worker_queue_depth=worker_pool.queue_depth,
        active_workers=worker_pool.active_workers,
        events_processed=total_events,
        events_per_second=round(total_events / uptime if uptime > 0 else 0, 2),
        avg_latency_us=round(worker_pool.metrics.avg_latency_us, 2),
        p99_latency_us=round(worker_pool.metrics.p99_latency_us, 2),
    )


# ─── Debug / Demo Endpoints ──────────────────────────────────────────────────

@router.post(
    "/simulate/burst",
    summary="Simulate a burst of events for demo/testing",
    description=(
        "Generates and ingests N synthetic events with randomized user IDs "
        "and event types. Useful for quickly populating the cache and "
        "generating speculative results during the hackathon demo."
    ),
)
async def simulate_burst(
    request: Request,
    count: int = Query(default=100, ge=1, le=10_000, description="Number of events to simulate"),
) -> Dict[str, Any]:
    import random
    start_ns = time.perf_counter_ns()

    user_pool = [f"user_{i:04d}" for i in range(max(1, count // 10))]
    event_types = list(EventType)
    strategies = list(PersonalizationStrategy)

    events = []
    for _ in range(count):
        event = StreamEvent(
            event_type=random.choice(event_types),
            user_context={
                "user_id": random.choice(user_pool),
                "device_type": random.choice(["mobile", "desktop", "tablet"]),
                "segment_tags": random.sample(
                    ["high_value", "returning", "price_sensitive", "brand_loyal",
                     "impulse_buyer", "researcher", "deal_seeker"],
                    k=random.randint(1, 3),
                ),
            },
            payload={
                "campaign_id": f"camp_{random.randint(1000, 9999)}",
                "creative_variant": random.choice(["A", "B", "C", "D"]),
                "bid_amount": round(random.uniform(0.01, 5.00), 2),
            },
            personalization_hint=random.choice(strategies),
        )
        events.append(event)

    # Process all simulated events concurrently
    acks = await asyncio.gather(
        *(ingest_stream_event(evt, request) for evt in events)
    )

    elapsed_us = (time.perf_counter_ns() - start_ns) // 1000
    avg_per_event_us = elapsed_us // count if count > 0 else 0

    return {
        "simulated": count,
        "accepted": len(acks),
        "total_time_us": elapsed_us,
        "avg_per_event_us": avg_per_event_us,
        "throughput_eps": round(count / (elapsed_us / 1_000_000) if elapsed_us > 0 else 0, 0),
        "unique_users": len(user_pool),
    }


@router.get(
    "/metrics/summary",
    summary="Aggregated performance metrics dashboard",
)
async def metrics_summary(request: Request) -> Dict[str, Any]:
    cache = _get_cache(request)
    wp = _get_worker_pool(request)
    uptime = time.monotonic() - request.app.state.start_time

    return {
        "system": {
            "uptime_seconds": round(uptime, 2),
        },
        "cache": {
            "total_entries": await cache.total_entries(),
            "hits": cache.metrics.hits,
            "misses": cache.metrics.misses,
            "hit_rate": round(cache.metrics.hit_rate * 100, 2),
            "sets": cache.metrics.sets,
            "evictions": cache.metrics.evictions,
            "ops_per_second": round(cache.metrics.ops_per_second, 2),
        },
        "workers": {
            "active": wp.active_workers,
            "queue_depth": wp.queue_depth,
            "tasks_completed": wp.metrics.tasks_completed,
            "tasks_failed": wp.metrics.tasks_failed,
            "avg_latency_us": round(wp.metrics.avg_latency_us, 2),
            "p99_latency_us": round(wp.metrics.p99_latency_us, 2),
        },
    }
