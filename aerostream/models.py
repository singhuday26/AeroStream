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
        """Guard against accidental multi-MB payloads that would choke the pipeline."""
        if len(str(v)) > 65_536:
            raise ValueError("Payload exceeds 64KB serialized limit")
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
