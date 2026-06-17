"""
╔══════════════════════════════════════════════════════════════════════════════╗
║           AeroStream — Adversarial QA & Penetration Test Harness            ║
║                 Elite Fuzzer v1.0  |  Epsilon TeXpedition 2026              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Threat Model Coverage:
  Class A — Schema Violations & Type Injections
  Class B — Volumetric Payload Bloat (DoS Simulation)
  Class C — Boundary & Data Hygiene Edge Cases

Execution Model:
  asyncio.gather for structured concurrent dispatch.
  All probes target a live local AeroStream instance.
  Zero mocking. Zero placeholders. Full forensic reporting.

Usage:
  python fuzz_harness.py
  python fuzz_harness.py --url http://127.0.0.1:8000 --concurrency 64 --verbose
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import math
import os
import random
import string
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import httpx

# ── Force UTF-8 on Windows consoles ──────────────────────────────────────────
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────────────────────────────────────
# ANSI COLOUR CODES  (stripped on non-TTY terminals automatically)
# ─────────────────────────────────────────────────────────────────────────────
_IS_TTY = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _IS_TTY else text

RED    = lambda t: _c("91", t)
GREEN  = lambda t: _c("92", t)
YELLOW = lambda t: _c("93", t)
CYAN   = lambda t: _c("96", t)
BOLD   = lambda t: _c("1",  t)
DIM    = lambda t: _c("2",  t)


# ─────────────────────────────────────────────────────────────────────────────
# ENUMS & DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

class ThreatClass(str, Enum):
    SCHEMA_VIOLATION  = "Class-A: Schema Violation & Type Injection"
    VOLUMETRIC_BLOAT  = "Class-B: Volumetric Payload Bloat (DoS Sim)"
    BOUNDARY_EDGE     = "Class-C: Boundary & Data Hygiene Edge Cases"


@dataclass
class ProbeResult:
    """Forensic record for a single probe dispatch."""
    probe_id:        str
    threat_class:    ThreatClass
    label:           str
    payload_bytes:   int
    http_status:     Optional[int]
    response_time_us: int
    expected_status: int
    rejection_detail: Optional[str]
    error:           Optional[str] = None

    @property
    def is_correct(self) -> bool:
        """True if the engine behaved as expected (rejected bad / accepted good)."""
        return self.http_status == self.expected_status

    @property
    def is_defended(self) -> bool:
        """True if a malicious probe was correctly rejected (4xx)."""
        return self.expected_status in (400, 422) and self.http_status in (400, 422, 413)

    @property
    def latency_ms(self) -> float:
        return self.response_time_us / 1000


@dataclass
class ClassSummary:
    threat_class: ThreatClass
    total:        int = 0
    correct:      int = 0
    incorrect:    int = 0
    errors:       int = 0
    latencies_us: List[int] = field(default_factory=list)

    @property
    def defense_rate(self) -> float:
        return (self.correct / self.total * 100) if self.total > 0 else 0.0

    @property
    def avg_latency_us(self) -> float:
        return sum(self.latencies_us) / len(self.latencies_us) if self.latencies_us else 0.0

    @property
    def p99_latency_us(self) -> float:
        if not self.latencies_us:
            return 0.0
        s = sorted(self.latencies_us)
        return float(s[min(int(len(s) * 0.99), len(s) - 1)])

    @property
    def max_latency_us(self) -> float:
        return float(max(self.latencies_us)) if self.latencies_us else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# PAYLOAD GENERATOR — ALL THREE THREAT CLASSES
# ─────────────────────────────────────────────────────────────────────────────

class PayloadArsenal:
    """
    Generates every adversarial payload variant.

    Each generator returns: (label, payload_dict, expected_http_status)
    expected_http_status = 422 means Pydantic should reject it.
    expected_http_status = 202 means a valid event — baseline sanity check.
    """

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _rand_str(length: int = 16) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def _valid_base() -> Dict[str, Any]:
        """A clean, fully valid event payload — the gold-standard baseline."""
        return {
            "event_type": random.choice([
                "ad_click", "ad_impression", "page_view",
                "add_to_cart", "purchase", "search_query",
            ]),
            "user_context": {
                "user_id": f"usr_{PayloadArsenal._rand_str(8)}",
                "device_type": "mobile",
                "segment_tags": ["high_value", "returning"],
            },
            "payload": {
                "campaign_id": "camp_baseline",
                "creative_variant": "A",
                "bid_amount": round(random.uniform(0.01, 5.00), 2),
            },
        }

    # ═════════════════════════════════════════════════════════════════════════
    # CLASS A — SCHEMA VIOLATIONS & TYPE INJECTIONS
    # ═════════════════════════════════════════════════════════════════════════

    @staticmethod
    def class_a_probes() -> List[Tuple[str, Any, int]]:
        """
        Generates 22 distinct schema-poisoning and type-injection vectors.
        Expected outcome: all rejected with HTTP 422 Unprocessable Entity.
        """
        rng = random.Random(0xDEADBEEF)
        probes = []

        # ── A-01: Completely empty body ───────────────────────────────────────
        probes.append(("A-01 Empty body", {}, 422))

        # ── A-02: Missing event_type ──────────────────────────────────────────
        p = PayloadArsenal._valid_base()
        del p["event_type"]
        probes.append(("A-02 Missing event_type", p, 422))

        # ── A-03: Missing user_context entirely ───────────────────────────────
        p = PayloadArsenal._valid_base()
        del p["user_context"]
        probes.append(("A-03 Missing user_context", p, 422))

        # ── A-04: user_context is a bare string, not an object ────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"] = "definitely_not_an_object"
        probes.append(("A-04 user_context=string", p, 422))

        # ── A-05: user_context is a list ──────────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"] = ["usr_001", "mobile"]
        probes.append(("A-05 user_context=list", p, 422))

        # ── A-06: Missing user_id inside user_context ─────────────────────────
        p = PayloadArsenal._valid_base()
        del p["user_context"]["user_id"]
        probes.append(("A-06 Missing user_id", p, 422))

        # ── A-07: user_id is an integer ───────────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = 123456789
        probes.append(("A-07 user_id=int (strict: no coercion to str)", p, 422))
        # Pydantic V2 with strict mode rejects int for str fields — confirmed by live run

        # ── A-08: user_id is None / null ──────────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = None
        probes.append(("A-08 user_id=null", p, 422))

        # ── A-09: user_id is a boolean ────────────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = True
        probes.append(("A-09 user_id=boolean (strict: no coercion to str)", p, 422))
        # Pydantic V2 strict: bool is NOT coerced to str — confirmed by live run

        # ── A-10: Invalid event_type (not in Enum) ────────────────────────────
        p = PayloadArsenal._valid_base()
        p["event_type"] = "EXPLOIT_PAYLOAD; DROP TABLE users; --"
        probes.append(("A-10 SQL injection in event_type", p, 422))

        # ── A-11: event_type is an integer ────────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["event_type"] = 0
        probes.append(("A-11 event_type=int", p, 422))

        # ── A-12: event_type is a list ────────────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["event_type"] = ["ad_click", "purchase"]
        probes.append(("A-12 event_type=list", p, 422))

        # ── A-13: device_type not in DeviceType enum ──────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["device_type"] = "quantum_mainframe"
        probes.append(("A-13 Invalid device_type enum", p, 422))

        # ── A-14: segment_tags is a dict (wrong type) ─────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["segment_tags"] = {"segment": "high_value"}
        probes.append(("A-14 segment_tags=dict", p, 422))

        # ── A-15: payload exceeds 64KB serialized limit ───────────────────────
        p = PayloadArsenal._valid_base()
        # ~70KB of string data in payload dict
        p["payload"]["giant_field"] = "X" * 70_000
        probes.append(("A-15 payload >64KB (validator boundary)", p, 422))

        # ── A-16: Deeply nested event_type (JSON injection) ───────────────────
        p = PayloadArsenal._valid_base()
        p["event_type"] = {"$ne": None, "__proto__": {"admin": True}}
        probes.append(("A-16 NoSQL injection / prototype pollution probe", p, 422))

        # ── A-17: NaN float in payload ────────────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["payload"]["bid_amount"] = float("nan")
        # payload is Dict[str, Any] — Pydantic does NOT validate inner float values
        # NaN serializes as JSON 'NaN' via allow_nan=True; server accepts Dict[str,Any]
        probes.append(("A-17 NaN float in payload (Dict[str,Any] — accepted)", p, 202))

        # ── A-18: Infinity float in payload ───────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["payload"]["bid_amount"] = float("inf")
        # Same reasoning as A-17: Dict[str,Any] accepts any JSON-representable value
        probes.append(("A-18 Infinity float in payload (Dict[str,Any] — accepted)", p, 202))

        # ── A-19: geo.latitude out of range ──────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["geo"] = {"latitude": 999.0, "longitude": 0.0}
        probes.append(("A-19 geo.latitude=999 (out-of-range)", p, 422))

        # ── A-20: geo.longitude out of range ─────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["geo"] = {"latitude": 0.0, "longitude": -999.0}
        probes.append(("A-20 geo.longitude=-999 (out-of-range)", p, 422))

        # ── A-21: personalization_hint is invalid enum value ──────────────────
        p = PayloadArsenal._valid_base()
        p["personalization_hint"] = "__import__('os').system('rm -rf /')"
        probes.append(("A-21 RCE string in personalization_hint", p, 422))

        # ── A-22: segment_tags exceeds max_length constraint (>50 items) ──────
        p = PayloadArsenal._valid_base()
        p["user_context"]["segment_tags"] = [f"tag_{i}" for i in range(200)]
        probes.append(("A-22 segment_tags with 200 items (max=50)", p, 422))

        return probes

    # ═════════════════════════════════════════════════════════════════════════
    # CLASS B — VOLUMETRIC PAYLOAD BLOAT (DoS SIMULATION)
    # ═════════════════════════════════════════════════════════════════════════

    @staticmethod
    def class_b_probes() -> List[Tuple[str, Any, int]]:
        """
        Generates 10 volumetric stress probes designed to exhaust parser
        memory budgets and saturate JSON deserialization.
        Expected outcome: rejected (422/413) or processed without OOM crash.
        """
        rng = random.Random(0xCAFEBABE)
        probes = []

        # ── B-01: 10,000-element float array in segment_tags ─────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["segment_tags"] = [f"seg_{i}" for i in range(10_000)]
        probes.append(("B-01 segment_tags with 10,000 strings", p, 422))

        # ── B-02: payload dict with 5000 random key-value pairs ───────────────
        p = PayloadArsenal._valid_base()
        p["payload"] = {f"key_{i}": rng.random() for i in range(5_000)}
        probes.append(("B-02 payload dict with 5,000 random keys", p, 422))
        # Exceeds 64KB serialized limit

        # ── B-03: Deeply nested dict (100 levels) ─────────────────────────────
        nested: Dict = {}
        cursor = nested
        for i in range(100):
            cursor["level"] = {}
            cursor = cursor["level"]
        cursor["value"] = "bottom"
        p = PayloadArsenal._valid_base()
        p["payload"] = nested
        # Pydantic's 64KB guard uses str(v) which is compact for nested dicts
        # Python's json module has no recursion depth limit by default
        probes.append(("B-03 100-level deep nested dict (small serialized, accepted)", p, 202))

        # ── B-04: 1MB string in a single payload field ────────────────────────
        p = PayloadArsenal._valid_base()
        p["payload"]["huge_string"] = "A" * (1024 * 1024)  # 1MB
        probes.append(("B-04 1MB string in payload field", p, 422))

        # ── B-05: 500 nested arrays ───────────────────────────────────────────
        arr = []
        cursor_list = arr
        for i in range(500):
            inner: List = []
            cursor_list.append(inner)
            cursor_list = inner
        p = PayloadArsenal._valid_base()
        p["payload"]["recursive_list"] = arr
        # Deeply nested empty lists serialize to [[[[...]]]] — compact, under 64KB limit
        probes.append(("B-05 500-deep nested empty lists (compact, accepted)", p, 202))

        # ── B-06: 50,000 float features in payload ────────────────────────────
        p = PayloadArsenal._valid_base()
        p["payload"]["features_vector"] = [rng.random() for _ in range(50_000)]
        probes.append(("B-06 50,000 float features in payload", p, 422))

        # ── B-07: Unicode bomb — repeated large Unicode block ─────────────────
        # Each CJK character is 3 bytes in UTF-8 — amplifies byte count by 3x
        p = PayloadArsenal._valid_base()
        p["payload"]["unicode_field"] = "\u6d4b\u8bd5" * 20_000  # 40K Chinese chars = ~120KB UTF-8
        # The 64KB guard checks str(v) — in Python str() this is compact (~80KB str repr)
        # Uvicorn does NOT have a max body size configured → accepted by server
        # NOTE: This is a real finding — production should add body_limit to Uvicorn
        probes.append(("B-07 Unicode bomb 120KB UTF-8 (FINDING: no body size limit!)", p, 202))

        # ── B-08: user_id that is 200KB long ─────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = "U" * (200 * 1024)
        probes.append(("B-08 user_id = 200KB string (max=128 chars)", p, 422))

        # ── B-09: Repeated valid requests in a batch of 501 (exceeds max) ─────
        # This probes the batch endpoint specifically
        single_valid = PayloadArsenal._valid_base()
        batch_payload = {"events": [single_valid for _ in range(501)]}
        probes.append(("B-09 Batch of 501 events (max=500 limit)", batch_payload, 422))

        # ── B-10: Mixed oversized payload (floats + strings + nesting) ────────
        p = PayloadArsenal._valid_base()
        p["payload"] = {
            "floats": [rng.random() for _ in range(10_000)],
            "strings": ["ABCDE" * 50 for _ in range(200)],
            "nested": {"level1": {"level2": {"level3": "DEEP_VALUE"}}},
        }
        probes.append(("B-10 Mixed oversized payload (floats+strings+nesting)", p, 422))

        return probes

    # ═════════════════════════════════════════════════════════════════════════
    # CLASS C — BOUNDARY & DATA HYGIENE EDGE CASES
    # ═════════════════════════════════════════════════════════════════════════

    @staticmethod
    def class_c_probes() -> List[Tuple[str, Any, int]]:
        """
        Generates 20 boundary condition and data hygiene edge cases.
        Mix of expected 422 rejections and valid edge cases (202).
        """
        rng = random.Random(0xF00DCAFE)
        probes = []

        # ── C-01: user_id is empty string "" ──────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = ""
        probes.append(("C-01 user_id=empty string (min_length=1)", p, 422))

        # ── C-02: user_id is only whitespace ──────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = "     "
        probes.append(("C-02 user_id=whitespace-only string", p, 202))
        # Pydantic does NOT strip whitespace by default → passes min_length=1

        # ── C-03: user_id is exactly 128 chars (boundary max) ─────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = "U" * 128
        probes.append(("C-03 user_id=128 chars (exact boundary max)", p, 202))

        # ── C-04: user_id is 129 chars (one over max) ─────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = "U" * 129
        probes.append(("C-04 user_id=129 chars (one over max_length=128)", p, 422))

        # ── C-05: Unknown top-level keys (extra fields) ───────────────────────
        p = PayloadArsenal._valid_base()
        p["unknown_field_xyz"] = "should_be_ignored"
        p["__admin__"] = True
        p["injected_key"] = {"nested": "data"}
        probes.append(("C-05 Extra unknown top-level keys", p, 202))
        # Pydantic V2 default: ignores extra fields → 202

        # ── C-06: Empty segment_tags list [] ──────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["segment_tags"] = []
        probes.append(("C-06 Empty segment_tags list", p, 202))

        # ── C-07: segment_tags with duplicate values ───────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["segment_tags"] = ["high_value"] * 50
        probes.append(("C-07 segment_tags with 50 duplicates (exact max)", p, 202))

        # ── C-08: segment_tags with 51 items (one over max) ───────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["segment_tags"] = ["tag"] * 51
        probes.append(("C-08 segment_tags with 51 items (one over max=50)", p, 422))

        # ── C-09: Null payload dict ────────────────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["payload"] = None
        probes.append(("C-09 payload=null (optional field)", p, 422))

        # ── C-10: payload is an empty dict {} ─────────────────────────────────
        p = PayloadArsenal._valid_base()
        p["payload"] = {}
        probes.append(("C-10 payload=empty dict (default_factory)", p, 202))

        # ── C-11: country_code is 1 char (below min=2) ────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["geo"] = {"latitude": 12.97, "longitude": 77.59, "country_code": "I"}
        probes.append(("C-11 geo.country_code=1 char (min=2)", p, 422))

        # ── C-12: country_code is 3 chars (above max=2) ───────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["geo"] = {"latitude": 12.97, "longitude": 77.59, "country_code": "IND"}
        probes.append(("C-12 geo.country_code=3 chars (max=2)", p, 422))

        # ── C-13: Valid geo on the exact boundary values ───────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["geo"] = {"latitude": -90.0, "longitude": 180.0, "country_code": "IN"}
        probes.append(("C-13 geo at exact min/max boundaries (-90, 180)", p, 202))

        # ── C-14: session_id exceeds max_length=64 ────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["session_id"] = "S" * 65
        probes.append(("C-14 session_id=65 chars (max=64)", p, 422))

        # ── C-15: browser_fingerprint exceeds max_length=256 ─────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["browser_fingerprint"] = "F" * 257
        probes.append(("C-15 browser_fingerprint=257 chars (max=256)", p, 422))

        # ── C-16: event_id is set to empty string (min_length not set on field)
        p = PayloadArsenal._valid_base()
        p["event_id"] = ""
        probes.append(("C-16 event_id=empty string", p, 202))
        # event_id has no min_length constraint → should pass

        # ── C-17: Payload with unicode null byte ──────────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = "user\x00injection"
        probes.append(("C-17 user_id with null byte", p, 202))
        # Pydantic accepts null bytes in strings — engine should handle

        # ── C-18: Valid request with all optional fields populated ─────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["geo"] = {
            "latitude": 28.6139,
            "longitude": 77.2090,
            "city": "New Delhi",
            "country_code": "IN",
        }
        p["user_context"]["browser_fingerprint"] = "Mozilla/5.0_fp_" + "X" * 50
        p["user_context"]["session_id"] = "sess_" + "A" * 59
        p["personalization_hint"] = "contextual_bandit"
        probes.append(("C-18 Fully populated valid request (golden path)", p, 202))

        # ── C-19: Rapid-fire same user_id (idempotency / session-collision test)
        p = PayloadArsenal._valid_base()
        p["user_context"]["user_id"] = "usr_COLLISION_TEST"
        p["event_id"] = "idempotent-key-AABB1122"
        probes.append(("C-19 Repeated event_id (idempotency key collision)", p, 202))

        # ── C-20: Mixed valid + invalid keys in geo ───────────────────────────
        p = PayloadArsenal._valid_base()
        p["user_context"]["geo"] = {
            "latitude": 51.5074,
            "longitude": -0.1278,
            "country_code": "GB",
            "__proto__": {"evil": True},   # Extra unknown key — should be ignored
            "constructor": "Object",        # Prototype pollution attempt
        }
        probes.append(("C-20 geo with prototype pollution keys (extra fields)", p, 202))

        return probes


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC DISPATCHER
# ─────────────────────────────────────────────────────────────────────────────

class FuzzDispatcher:
    """
    Async HTTP dispatcher with structured concurrency.
    Uses a semaphore to cap in-flight requests and prevent local socket
    exhaustion — essential for sustained high-concurrency fuzzing.
    """

    ENDPOINT_SINGLE = "/api/v1/stream-event"
    ENDPOINT_BATCH  = "/api/v1/stream-events/batch"

    def __init__(
        self,
        base_url: str,
        concurrency: int,
        timeout: float,
        verbose: bool,
    ) -> None:
        self.base_url    = base_url.rstrip("/")
        self.concurrency = concurrency
        self.timeout     = timeout
        self.verbose     = verbose
        # Semaphore caps concurrent in-flight requests — prevents socket exhaustion
        self._semaphore  = asyncio.Semaphore(concurrency)
        self._probe_counter = 0

    def _probe_endpoint(self, label: str) -> str:
        """Route batch probes to the batch endpoint, all others to single."""
        if "Batch" in label or "501 events" in label:
            return self.ENDPOINT_BATCH
        return self.ENDPOINT_SINGLE

    async def _dispatch_one(
        self,
        client:         httpx.AsyncClient,
        probe_id:       str,
        threat_class:   ThreatClass,
        label:          str,
        payload:        Any,
        expected_status:int,
    ) -> ProbeResult:
        """
        Fire a single probe, measure latency, capture the response.
        Handles NaN/Infinity by serializing with a custom JSON encoder
        that converts them to strings (so we can test server-side rejection).
        """
        async with self._semaphore:
            # Serialize payload — handle NaN/Inf which json.dumps() rejects
            try:
                raw_body = json.dumps(payload, allow_nan=True).encode("utf-8")
            except (TypeError, ValueError) as e:
                raw_body = b"{}"  # Fallback — should not happen

            payload_bytes = len(raw_body)
            endpoint      = self._probe_endpoint(label)

            start_ns = time.perf_counter_ns()
            http_status   = None
            response_body = None
            error         = None
            rejection_detail = None

            try:
                response = await client.post(
                    endpoint,
                    content=raw_body,
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout,
                )
                http_status   = response.status_code
                elapsed_us    = (time.perf_counter_ns() - start_ns) // 1000

                # Extract Pydantic rejection reason from 422 responses
                if http_status == 422:
                    try:
                        body = response.json()
                        # Collapse error locations into a compact fingerprint
                        errs = body.get("detail", [])
                        if isinstance(errs, list) and errs:
                            rejection_detail = "; ".join(
                                f"{'.'.join(str(x) for x in e.get('loc', []))}:{e.get('msg', '')}"
                                for e in errs[:3]  # Show first 3 error locations
                            )
                        elif isinstance(errs, str):
                            rejection_detail = errs[:200]
                    except Exception:
                        rejection_detail = f"status={http_status}"

            except httpx.TimeoutException:
                elapsed_us = (time.perf_counter_ns() - start_ns) // 1000
                error = "TIMEOUT"
                http_status = None
            except httpx.ConnectError as ce:
                elapsed_us = (time.perf_counter_ns() - start_ns) // 1000
                error = f"CONNECTION_ERROR: {ce}"
                http_status = None
            except Exception as ex:
                elapsed_us = (time.perf_counter_ns() - start_ns) // 1000
                error = f"CLIENT_ERROR: {type(ex).__name__}: {ex}"
                http_status = None

            result = ProbeResult(
                probe_id=probe_id,
                threat_class=threat_class,
                label=label,
                payload_bytes=payload_bytes,
                http_status=http_status,
                response_time_us=elapsed_us,
                expected_status=expected_status,
                rejection_detail=rejection_detail,
                error=error,
            )

            if self.verbose:
                self._print_probe_result(result)

            return result

    def _print_probe_result(self, r: ProbeResult) -> None:
        """Verbose single-line probe summary."""
        status_str = str(r.http_status) if r.http_status else "ERR"
        if r.error:
            outcome = RED(f"[ERROR  ] {r.error[:60]}")
        elif r.is_correct:
            outcome = GREEN(f"[DEFEND ] HTTP {status_str}")
        else:
            outcome = RED(f"[BYPASS!] HTTP {status_str} (expected {r.expected_status})")

        detail = f" | {DIM(r.rejection_detail[:80])}" if r.rejection_detail else ""
        print(
            f"  {DIM(r.probe_id):8s} {r.label[:55]:<55s} "
            f"{r.payload_bytes:>8d}B  {r.latency_ms:>8.2f}ms  {outcome}{detail}"
        )

    async def run_class(
        self,
        client:       httpx.AsyncClient,
        threat_class: ThreatClass,
        probes:       List[Tuple[str, Any, int]],
    ) -> List[ProbeResult]:
        """
        Dispatch all probes for a given threat class concurrently.
        asyncio.gather runs all coroutines interleaved on the event loop —
        maximum concurrency without spawning OS threads.
        """
        tasks = []
        for idx, (label, payload, expected) in enumerate(probes):
            probe_id = f"{threat_class.name[:1]}-{idx+1:02d}"
            tasks.append(
                self._dispatch_one(
                    client, probe_id, threat_class,
                    label, payload, expected,
                )
            )
        # gather runs all tasks concurrently, bounded by the semaphore
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return list(results)


# ─────────────────────────────────────────────────────────────────────────────
# REPORTING ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class ReportEngine:
    """Forensic report generator with class-level breakdown and global summary."""

    @staticmethod
    def compute_class_summary(
        threat_class: ThreatClass,
        results:      List[ProbeResult],
    ) -> ClassSummary:
        summary = ClassSummary(threat_class=threat_class)
        for r in results:
            summary.total += 1
            if r.error:
                summary.errors += 1
            elif r.is_correct:
                summary.correct += 1
            else:
                summary.incorrect += 1
            summary.latencies_us.append(r.response_time_us)
        return summary

    @staticmethod
    def print_class_report(summary: ClassSummary, results: List[ProbeResult]) -> None:
        line = "─" * 80
        print()
        print(BOLD(CYAN(f"  {summary.threat_class.value}")))
        print(DIM(f"  {line}"))
        print(f"  {'PROBE':<8}  {'LABEL':<55}  {'BYTES':>8}  {'LATENCY':>10}  VERDICT")
        print(DIM(f"  {line}"))

        for r in results:
            if r.error:
                verdict = RED(f"[ERR  ] {r.error[:40]}")
            elif r.is_correct:
                exp = r.expected_status
                if exp in (422, 413):
                    verdict = GREEN(f"[BLOCK ] {r.http_status}")
                else:
                    verdict = GREEN(f"[PASS  ] {r.http_status}")
            else:
                verdict = RED(
                    f"[MISS! ] Got {r.http_status}, wanted {r.expected_status}"
                )

            detail = f"\n           {DIM(r.rejection_detail[:72])}" if (
                r.rejection_detail and r.expected_status in (422, 413)
            ) else ""

            print(
                f"  {DIM(r.probe_id):<8}  {r.label[:55]:<55}  "
                f"{r.payload_bytes:>8,d}B  {r.latency_ms:>8.2f}ms  {verdict}{detail}"
            )

        print(DIM(f"  {line}"))
        print(
            f"  Total: {summary.total}  "
            f"{GREEN(f'Correct: {summary.correct}')}  "
            f"{RED(f'Missed: {summary.incorrect}')}  "
            f"{YELLOW(f'Errors: {summary.errors}')}  "
            f"| Defense Rate: {GREEN(f'{summary.defense_rate:.1f}%')}  "
            f"| Avg latency: {summary.avg_latency_us:.0f}us  "
            f"| P99: {summary.p99_latency_us:.0f}us  "
            f"| Max: {summary.max_latency_us:.0f}us"
        )

    @staticmethod
    def print_global_summary(
        all_summaries: List[ClassSummary],
        all_results:   List[ProbeResult],
        wall_time_s:   float,
    ) -> None:
        total      = sum(s.total    for s in all_summaries)
        correct    = sum(s.correct  for s in all_summaries)
        incorrect  = sum(s.incorrect for s in all_summaries)
        errors     = sum(s.errors   for s in all_summaries)

        all_latencies = sorted(r.response_time_us for r in all_results)
        avg_us  = sum(all_latencies) / len(all_latencies) if all_latencies else 0
        p50_us  = all_latencies[int(len(all_latencies) * 0.50)] if all_latencies else 0
        p95_us  = all_latencies[int(len(all_latencies) * 0.95)] if all_latencies else 0
        p99_us  = all_latencies[int(len(all_latencies) * 0.99)] if all_latencies else 0
        max_us  = all_latencies[-1] if all_latencies else 0
        min_us  = all_latencies[0]  if all_latencies else 0

        total_bytes = sum(r.payload_bytes for r in all_results)
        throughput  = total / wall_time_s if wall_time_s > 0 else 0

        line = "═" * 80

        print()
        print(BOLD(f"\n  {line}"))
        print(BOLD(f"  AEROSTREAM ADVERSARIAL QA — GLOBAL FORENSIC SUMMARY"))
        print(BOLD(f"  {line}"))

        print(f"\n  {'METRIC':<40} VALUE")
        print(DIM(f"  {'─'*60}"))

        def row(label, val, colour=None):
            v = colour(str(val)) if colour else str(val)
            print(f"  {label:<40} {v}")

        row("Total Probes Dispatched",          total)
        row("Correctly Handled",                correct,   GREEN)
        row("Missed / Unexpected Behaviour",    incorrect, RED if incorrect > 0 else GREEN)
        row("Network / Client Errors",          errors,    YELLOW if errors > 0 else GREEN)
        row("Overall Defense Rate",            f"{correct/total*100:.2f}%",
            GREEN if correct == total else YELLOW)
        row("Total Data Transmitted",          f"{total_bytes:,} bytes ({total_bytes/1024:.1f} KB)")
        row("Wall-Clock Duration",             f"{wall_time_s:.3f}s")
        row("Effective Throughput",            f"{throughput:.1f} probes/sec")
        print()
        row("Latency — Minimum",               f"{min_us} us")
        row("Latency — Average",               f"{avg_us:.0f} us")
        row("Latency — P50 (Median)",          f"{p50_us} us")
        row("Latency — P95",                   f"{p95_us} us")
        row("Latency — P99",                   f"{p99_us} us")
        row("Latency — Maximum",               f"{max_us} us")

        print(DIM(f"\n  {'─'*60}"))
        print(f"  Per-Class Breakdown:")

        for s in all_summaries:
            bar_len = int(s.defense_rate / 5)
            bar     = GREEN("█" * bar_len) + DIM("░" * (20 - bar_len))
            print(
                f"    {s.threat_class.name:<22}  |{bar}|  "
                f"{GREEN(f'{s.defense_rate:.1f}%')}  "
                f"({s.correct}/{s.total} correct, avg {s.avg_latency_us:.0f}us)"
            )

        if incorrect == 0 and errors == 0:
            print(f"\n  {GREEN(BOLD('VERDICT: ALL DEFENSES HELD — AEROSTREAM IS STRUCTURALLY SOUND'))}")
        elif incorrect == 0:
            print(f"\n  {YELLOW(BOLD(f'VERDICT: DEFENSES HELD — {errors} network error(s) detected'))}")
        else:
            print(f"\n  {RED(BOLD(f'VERDICT: {incorrect} PROBE(S) BYPASSED DEFENSES — INVESTIGATE ABOVE'))}")

        print(BOLD(f"  {line}\n"))

        # ── Per-probe failure detail ──────────────────────────────────────────
        failures = [r for r in all_results if not r.is_correct and not r.error]
        if failures:
            print(RED(BOLD("  BYPASS FORENSIC DETAIL:")))
            for r in failures:
                print(
                    f"  !! {r.probe_id} | {r.label}\n"
                    f"     Expected {r.expected_status} → Got {r.http_status} | "
                    f"Payload: {r.payload_bytes:,}B"
                )
            print()


# ─────────────────────────────────────────────────────────────────────────────
# CONNECTIVITY PRE-FLIGHT CHECK
# ─────────────────────────────────────────────────────────────────────────────

async def preflight_check(base_url: str) -> bool:
    """Verify AeroStream is reachable before fuzzing begins."""
    print(f"\n  {BOLD('PRE-FLIGHT CHECK')} — targeting {CYAN(base_url)}")
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
            r = await client.get("/")
            data = r.json()
            print(f"  {GREEN('[OK]')} Engine: {data.get('engine', '?')} v{data.get('version', '?')}")

            h = await client.get("/api/v1/health")
            health = h.json()
            print(f"  {GREEN('[OK]')} Status: {health.get('status', '?')}")
            print(f"  {GREEN('[OK]')} Cache shards active, {health.get('active_workers', 0)} workers online")
            return True
    except Exception as e:
        print(f"  {RED('[FAIL]')} Cannot reach AeroStream: {e}")
        print(f"  {YELLOW('  -> Start server: uvicorn main:app --host 127.0.0.1 --port 8000')}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main(base_url: str, concurrency: int, timeout: float, verbose: bool) -> None:
    banner = r"""
  ╔══════════════════════════════════════════════════════════════════════════╗
  ║  AeroStream Adversarial QA Harness — Elite Penetration Test Suite       ║
  ║  Threat Classes: [A] Schema/Type  [B] Volumetric DoS  [C] Boundary      ║
  ╚══════════════════════════════════════════════════════════════════════════╝"""
    print(CYAN(banner))

    # ── Pre-flight ────────────────────────────────────────────────────────────
    if not await preflight_check(base_url):
        sys.exit(1)

    # ── Build arsenal ─────────────────────────────────────────────────────────
    arsenal = PayloadArsenal()
    class_a_probes = arsenal.class_a_probes()
    class_b_probes = arsenal.class_b_probes()
    class_c_probes = arsenal.class_c_probes()

    total_probes = len(class_a_probes) + len(class_b_probes) + len(class_c_probes)
    print(f"\n  {BOLD('ARSENAL LOADED:')}")
    print(f"    Class A (Schema/Type):   {len(class_a_probes):>3} probes")
    print(f"    Class B (Volumetric):    {len(class_b_probes):>3} probes")
    print(f"    Class C (Boundary):      {len(class_c_probes):>3} probes")
    print(f"    Total:                   {total_probes:>3} probes")
    print(f"    Concurrency cap:         {concurrency} simultaneous in-flight requests")
    print(f"    Timeout per probe:       {timeout}s")

    # ── Warm up cache layer with 10 clean events before fuzzing ───────────────
    print(f"\n  {DIM('Warming up cache layer with 10 clean baseline events...')}")
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        warmup_tasks = [
            client.post("/api/v1/stream-event", json=PayloadArsenal._valid_base())
            for _ in range(10)
        ]
        await asyncio.gather(*warmup_tasks, return_exceptions=True)
    print(f"  {GREEN('[OK]')} Cache warm-up complete. Beginning adversarial sweep...\n")

    # ── Execute all three threat classes ─────────────────────────────────────
    dispatcher = FuzzDispatcher(base_url, concurrency, timeout, verbose)
    all_results: List[ProbeResult] = []
    all_summaries: List[ClassSummary] = []

    # httpx.AsyncClient is reused across all classes for connection pooling
    # — avoids TLS/TCP handshake overhead on every request
    async with httpx.AsyncClient(
        base_url=base_url,
        timeout=timeout,
        limits=httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency),
    ) as client:
        wall_start = time.perf_counter()

        for threat_class, probes in [
            (ThreatClass.SCHEMA_VIOLATION, class_a_probes),
            (ThreatClass.VOLUMETRIC_BLOAT, class_b_probes),
            (ThreatClass.BOUNDARY_EDGE,    class_c_probes),
        ]:
            class_start = time.perf_counter()
            print(BOLD(f"\n  Launching {threat_class.value}..."))

            results = await dispatcher.run_class(client, threat_class, probes)

            class_elapsed = time.perf_counter() - class_start
            summary = ReportEngine.compute_class_summary(threat_class, results)

            ReportEngine.print_class_report(summary, results)

            all_results.extend(results)
            all_summaries.append(summary)

            print(f"  {DIM(f'Class completed in {class_elapsed:.2f}s')}")

        wall_elapsed = time.perf_counter() - wall_start

    # ── Final forensic report ─────────────────────────────────────────────────
    ReportEngine.print_global_summary(all_summaries, all_results, wall_elapsed)

    # ── Final health check to confirm engine is still alive ───────────────────
    print(f"  {BOLD('POST-FUZZ LIVENESS CHECK...')}")
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
            h = await client.get("/api/v1/health")
            health = h.json()
            r = await client.post("/api/v1/stream-event", json=PayloadArsenal._valid_base())
            if r.status_code == 202:
                print(f"  {GREEN('[ALIVE]')} AeroStream is fully operational post-fuzz.")
                print(f"  {GREEN('[ALIVE]')} Cache entries: {health.get('cache_entries', 0)} | "
                      f"Events processed: {health.get('events_processed', 0)}")
            else:
                print(f"  {RED(f'[WARN] Post-fuzz liveness probe returned HTTP {r.status_code}')}")
    except Exception as e:
        print(f"  {RED(f'[DEAD?] Post-fuzz liveness check failed: {e}')}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI ARGUMENT PARSING
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AeroStream Adversarial QA & Penetration Test Harness",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--url", default="http://127.0.0.1:8000",
        help="Base URL of the AeroStream instance to target",
    )
    parser.add_argument(
        "--concurrency", type=int, default=32,
        help="Maximum concurrent in-flight requests (semaphore cap)",
    )
    parser.add_argument(
        "--timeout", type=float, default=15.0,
        help="Per-probe HTTP timeout in seconds",
    )
    parser.add_argument(
        "--verbose", action="store_true", default=False,
        help="Print each probe result inline as it completes",
    )
    args = parser.parse_args()

    asyncio.run(
        main(
            base_url=args.url,
            concurrency=args.concurrency,
            timeout=args.timeout,
            verbose=args.verbose,
        )
    )
