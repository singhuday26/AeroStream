"""
AeroStream Pydantic Models
===========================
Strict, zero-copy-overhead request/response schemas for the ingestion pipeline.
Pydantic V2 is used for Rust-accelerated validation (pydantic-core).
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator
import json as _json  # aliased to avoid shadowing any local 'json' usage


# ─── Enums ────────────────────────────────────────────────────────────────────

class EventType(str, Enum):
    """Supported real-time behavioral event types for ad personalization."""
    AD_CLICK = "ad_click"
    AD_IMPRESSION = "ad_impression"
    PAGE_VIEW = "page_view"
    SEARCH_QUERY = "search_query"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"
    SCROLL_DEPTH = "scroll_depth"
    VIDEO_WATCH = "video_watch"
    FORM_SUBMIT = "form_submit"
    HOVER_INTENT = "hover_intent"


class DeviceType(str, Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"
    CTV = "ctv"  # Connected TV
    IOT = "iot"


class PersonalizationStrategy(str, Enum):
    """Which speculative decoding strategy the worker should apply."""
    COLLABORATIVE_FILTERING = "collaborative_filtering"
    CONTEXTUAL_BANDIT = "contextual_bandit"
    SEMANTIC_EMBEDDING = "semantic_embedding"
    HYBRID_ENSEMBLE = "hybrid_ensemble"


# ─── Request Models ──────────────────────────────────────────────────────────

class GeoLocation(BaseModel):
    """Geolocation metadata attached to behavioral events."""
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    city: Optional[str] = None
    country_code: Optional[str] = Field(None, min_length=2, max_length=2)


class UserContext(BaseModel):
    """Rich user context for real-time personalization decisions."""
    user_id: str = Field(..., min_length=1, max_length=128, description="Unique user identifier (hashed PII)")
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()), max_length=64)
    device_type: DeviceType = DeviceType.MOBILE
    browser_fingerprint: Optional[str] = Field(None, max_length=256)
    geo: Optional[GeoLocation] = None
    segment_tags: List[str] = Field(default_factory=list, max_length=50)

    # ── SECURITY PATCH C-02: Whitespace Validation Leak ──────────────────────
    @field_validator("user_id", mode="before")
    @classmethod
    def user_id_must_not_be_blank(cls, v: object) -> object:
        """
        Strip leading/trailing/internal whitespace BEFORE Pydantic's
        min_length=1 constraint fires.

        Why mode='before'?
            Pydantic's built-in min_length operates on the raw input value
            after type coercion but BEFORE custom validators run in 'after'
            mode. A string of pure whitespace (e.g. '   ') has len=3, so
            min_length=1 passes silently — the whitespace-only string leaks
            into the cache layer and pollutes downstream user profiles.
            mode='before' fires FIRST, stripping the value before the
            min_length constraint ever evaluates it.

        Behavior:
            '  hello  ' → 'hello'       (valid, stripped)
            '   '       → ValueError    (blank after strip, rejected)
            ''          → ValueError    (empty, caught by min_length after strip)
            123         → passed through unchanged (non-str handled downstream)
        """
        if isinstance(v, str):
            stripped = v.strip()
            if len(stripped) == 0:
                raise ValueError(
                    "user_id must not be blank or whitespace-only. "
                    "Provide a valid non-empty identifier."
                )
            return stripped
        return v


class StreamEvent(BaseModel):
    """
    Primary ingestion payload representing a single real-time behavioral event.
    This is the atomic unit flowing through the AeroStream pipeline.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Idempotency key")
    event_type: EventType
    timestamp_ms: int = Field(
        default_factory=lambda: int(time.time() * 1000),
        description="Unix epoch milliseconds when event occurred on client"
    )
    user_context: UserContext
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Flexible event-specific metadata (ad_id, campaign_id, creative_variant, etc.)"
    )
    personalization_hint: Optional[PersonalizationStrategy] = None

    @field_validator("payload")
    @classmethod
    def payload_must_not_be_enormous(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """
        Guard against oversized payloads using ACTUAL wire-byte length.

        SECURITY FIX (B-07):
            Original implementation used len(str(v)) which measures Python
            repr character count — NOT the UTF-8 byte count transmitted on
            the wire. A 40K-character CJK string encodes to ~120KB in UTF-8
            but str() measures ~80K characters, bypassing the 64KB guard.

            Fix: serialize to JSON bytes first (json.dumps + .encode('utf-8')),
            then measure len(bytes). This is the exact byte count Uvicorn
            would receive from the network, making the guard byte-accurate.
        """
        try:
            serialized_bytes = len(_json.dumps(v, allow_nan=True).encode("utf-8"))
        except (TypeError, ValueError):
            # Unserializable payload — still measure via repr as fallback
            serialized_bytes = len(str(v).encode("utf-8"))
        if serialized_bytes > 65_536:
            raise ValueError(
                f"Payload exceeds 64KB serialized limit "
                f"({serialized_bytes:,} bytes > 65,536 byte limit)"
            )
        return v


class BatchStreamEvents(BaseModel):
    """Batch ingestion wrapper for high-throughput clients sending event arrays."""
    events: List[StreamEvent] = Field(..., min_length=1, max_length=500)


# ─── Response Models ─────────────────────────────────────────────────────────

class IngestionAck(BaseModel):
    """Acknowledgment returned to the client after successful event ingestion."""
    event_id: str
    status: str = "accepted"
    cache_hit: bool = False
    profile_resolved: bool = False
    speculative_task_id: Optional[str] = None
    processing_time_us: int = Field(..., description="Server-side processing latency in microseconds")
    timestamp_ms: int = Field(default_factory=lambda: int(time.time() * 1000))


class BatchIngestionAck(BaseModel):
    """Batch acknowledgment for multi-event submissions."""
    accepted: int
    rejected: int = 0
    acks: List[IngestionAck]
    total_processing_time_us: int


class HealthStatus(BaseModel):
    """Health check response exposing internal system metrics."""
    status: str = "healthy"
    uptime_seconds: float
    cache_entries: int
    cache_hit_rate: float
    worker_queue_depth: int
    active_workers: int
    events_processed: int
    events_per_second: float
    avg_latency_us: float
    p99_latency_us: float


class ProfileSnapshot(BaseModel):
    """A point-in-time snapshot of a resolved user profile from cache."""
    user_id: str
    segments: List[str]
    interaction_count: int
    last_event_type: Optional[str] = None
    last_seen_ms: int
    personalization_scores: Dict[str, float] = Field(default_factory=dict)
    speculative_predictions: List[Dict[str, Any]] = Field(default_factory=list)


class SpeculativeResult(BaseModel):
    """Result from the speculative execution background worker."""
    task_id: str
    user_id: str
    strategy: PersonalizationStrategy
    scores: Dict[str, float]
    predicted_next_action: Optional[str] = None
    confidence: float = Field(..., ge=0.0, le=1.0)
    computation_time_us: int
