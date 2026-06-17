"""
╔══════════════════════════════════════════════════════════════════════════════╗
║            AeroStream Telemetry Analytics Engine  v1.0                      ║
║            Real-Time Pipeline Performance Instrumentation                   ║
║            Zero-Dependency | Async-Native | Microsecond-Fidelity            ║
╚══════════════════════════════════════════════════════════════════════════════╝

Analytics Profiles Computed:
    [1] High-Fidelity Percentile Distributions  (p50 / p95 / p99 / pMax)
    [2] Segment Throughput Analytics            (grouped by trace pattern)
    [3] Cache-Performance Matrix Correlation    (HIT vs MISS latency delta)

Header Contract (AeroStream HTTP Response Headers):
    X-Processing-Time-Us  → integer  microseconds per event
    X-Trace-Id            → 16-char hex  unique request fingerprint
    X-Cache-Status        → "HIT" | "MISS"

Run:
    python telemetry.py
    python telemetry.py --events 5000 --concurrency 64
"""

from __future__ import annotations

import argparse
import asyncio
import io
import math
import random
import statistics
import string
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterator, List, Optional, Tuple

# ── Force UTF-8 on Windows consoles ──────────────────────────────────────────
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────────────────────────────────────
# ANSI TERMINAL STYLING
# ─────────────────────────────────────────────────────────────────────────────

_TTY = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

def _ansi(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _TTY else text

BOLD   = lambda t: _ansi("1",    t)
DIM    = lambda t: _ansi("2",    t)
CYAN   = lambda t: _ansi("96",   t)
GREEN  = lambda t: _ansi("92",   t)
YELLOW = lambda t: _ansi("93",   t)
RED    = lambda t: _ansi("91",   t)
MAGENTA= lambda t: _ansi("95",   t)
WHITE  = lambda t: _ansi("97",   t)


# ─────────────────────────────────────────────────────────────────────────────
# DATA MODEL
# ─────────────────────────────────────────────────────────────────────────────

class CacheStatus(str, Enum):
    HIT  = "HIT"
    MISS = "MISS"


@dataclass(slots=True)
class TelemetryRecord:
    """
    Atomic telemetry event parsed from AeroStream response headers.
    Represents a single request's complete observability snapshot.
    """
    event_id:          str            # Internal sequence ID
    trace_id:          str            # X-Trace-Id header (16-char hex)
    processing_time_us: int           # X-Processing-Time-Us (microseconds)
    cache_status:      CacheStatus    # X-Cache-Status (HIT | MISS)
    event_type:        str            # Inferred from trace segment
    timestamp_ns:      int            # Wall-clock capture time (nanoseconds)

    @property
    def trace_segment(self) -> str:
        """
        Extract the first 8 characters of the trace ID as the segment key.
        This groups requests by their originating pipeline segment —
        analogous to distributed tracing's 'parent span' prefix.
        """
        return self.trace_id[:8]

    @property
    def latency_ms(self) -> float:
        return self.processing_time_us / 1_000


# ─────────────────────────────────────────────────────────────────────────────
# MOCK TELEMETRY STREAM GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

class TelemetryStreamGenerator:
    """
    Simulates AeroStream's HTTP response header telemetry stream.

    Latency Model:
        Cache HITs   → LogNormal(μ=4.0, σ=0.5)  → median ≈ 55μs
        Cache MISSes → LogNormal(μ=5.2, σ=0.6)  → median ≈ 180μs
        Spike events → 0.3% probability of 5ms–25ms outlier burst
                       simulating speculative worker queue saturation

    Trace Segment Distribution:
        8 distinct pipeline segments with realistic volume weighting
        (segments with higher volume simulate hot user cohorts)
    """

    # 8 pipeline segments — first 8 chars of a trace_id prefix
    _SEGMENTS: List[Tuple[str, float]] = [
        ("a3f1b920", 0.22),  # Segment 0 — mobile ad-click hot path
        ("c7e4d012", 0.18),  # Segment 1 — desktop impression pipeline
        ("9b2a5f8e", 0.15),  # Segment 2 — search-query intent stream
        ("4d8c3a11", 0.13),  # Segment 3 — add-to-cart conversion path
        ("f0a7b6c5", 0.11),  # Segment 4 — CTV viewability tracker
        ("2e9d4b73", 0.09),  # Segment 5 — purchase confirmation stream
        ("8c1f0e6a", 0.07),  # Segment 6 — hover-intent signal processor
        ("5b3e7d9f", 0.05),  # Segment 7 — scroll-depth engagement layer
    ]

    # Hit rate by segment — hot segments have higher cache hit rates
    _SEGMENT_HIT_RATES: Dict[str, float] = {
        "a3f1b920": 0.92,  # Mobile hot path — almost always cached
        "c7e4d012": 0.85,
        "9b2a5f8e": 0.78,
        "4d8c3a11": 0.72,
        "f0a7b6c5": 0.65,
        "2e9d4b73": 0.58,
        "8c1f0e6a": 0.45,
        "5b3e7d9f": 0.38,  # Low-volume segment — frequently cache-cold
    }

    _EVENT_TYPES = [
        "ad_click", "ad_impression", "page_view", "add_to_cart",
        "purchase", "search_query", "scroll_depth", "video_watch",
    ]

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)

    def _sample_latency(self, status: CacheStatus) -> int:
        """
        Sample a realistic latency from a log-normal distribution.
        Log-normal is the empirically correct model for HTTP tail latencies —
        it captures the asymmetric, right-skewed nature of real distributions
        where the bulk of requests are fast but a long tail exists.

        μ and σ are in log-space (natural logarithm units).
        """
        if status == CacheStatus.HIT:
            mu_log, sigma_log = 4.0, 0.50   # median ≈ e^4.0 ≈ 55μs
        else:
            mu_log, sigma_log = 5.2, 0.60   # median ≈ e^5.2 ≈ 181μs

        # Sample from log-normal: exp(Normal(mu, sigma))
        z = self._rng.gauss(0.0, 1.0)
        sample = math.exp(mu_log + sigma_log * z)

        # Spike injection — 0.3% of events simulate queue saturation
        if self._rng.random() < 0.003:
            sample = self._rng.uniform(5_000, 25_000)  # 5ms–25ms spike

        return max(1, int(sample))

    def _generate_trace_id(self, segment_prefix: str) -> str:
        """
        Construct a 16-char trace ID with the segment prefix in the
        first 8 characters — remaining 8 are random hex.
        """
        suffix = "".join(
            self._rng.choice(string.hexdigits[:16]) for _ in range(8)
        )
        return f"{segment_prefix}{suffix}"

    def generate_batch(self, count: int) -> List[TelemetryRecord]:
        """
        Synchronously generate a batch of `count` telemetry records.
        This is intentionally synchronous — generation is CPU-bound and
        will be offloaded to asyncio.to_thread() by the async pipeline.
        """
        segment_names    = [s[0] for s in self._SEGMENTS]
        segment_weights  = [s[1] for s in self._SEGMENTS]
        event_types      = self._EVENT_TYPES

        records: List[TelemetryRecord] = []
        now_ns = time.perf_counter_ns()

        for i in range(count):
            segment  = self._rng.choices(segment_names, weights=segment_weights, k=1)[0]
            hit_rate = self._SEGMENT_HIT_RATES[segment]
            status   = (
                CacheStatus.HIT if self._rng.random() < hit_rate
                else CacheStatus.MISS
            )
            latency_us = self._sample_latency(status)
            trace_id   = self._generate_trace_id(segment)
            event_type = self._rng.choice(event_types)

            records.append(TelemetryRecord(
                event_id           = f"evt_{i:06d}",
                trace_id           = trace_id,
                processing_time_us = latency_us,
                cache_status       = status,
                event_type         = event_type,
                timestamp_ns       = now_ns + (i * 100_000),  # 100μs apart
            ))

        return records


# ─────────────────────────────────────────────────────────────────────────────
# MATHEMATICS ENGINE  (zero-dependency, pure Python)
# ─────────────────────────────────────────────────────────────────────────────

class MathEngine:
    """
    Pure-Python statistical computation engine.
    All methods operate on pre-sorted arrays for O(1) percentile access.

    Why not statistics.quantiles()?
        statistics.quantiles() uses interpolation schemes that differ
        from the 'nearest rank' method used in most APM tools. We implement
        the exact nearest-rank method for consistency with tools like Datadog,
        Prometheus, and the DORA metrics framework.
    """

    @staticmethod
    def percentile(sorted_data: List[float], p: float) -> float:
        """
        Compute the p-th percentile using the exact nearest-rank method.

        Formula: index = ceil(p/100 * N) - 1   (0-indexed)
        This gives the smallest value in the sorted array such that
        at least p% of the data is ≤ that value.

        Args:
            sorted_data: Pre-sorted list of values (ascending).
            p:           Percentile in [0, 100].
        Returns:
            The exact p-th percentile value.
        """
        if not sorted_data:
            return 0.0
        n = len(sorted_data)
        if p <= 0:
            return float(sorted_data[0])
        if p >= 100:
            return float(sorted_data[-1])
        # Nearest-rank formula — ceiling division, converted to 0-based index
        rank = math.ceil(p / 100.0 * n)
        return float(sorted_data[rank - 1])

    @staticmethod
    def mean(data: List[float]) -> float:
        if not data:
            return 0.0
        return sum(data) / len(data)

    @staticmethod
    def stdev(data: List[float]) -> float:
        """Population standard deviation (σ), not sample (s)."""
        if len(data) < 2:
            return 0.0
        mu = MathEngine.mean(data)
        variance = sum((x - mu) ** 2 for x in data) / len(data)
        return math.sqrt(variance)

    @staticmethod
    def coefficient_of_variation(data: List[float]) -> float:
        """CV = σ/μ — normalized dispersion metric, scale-independent."""
        mu = MathEngine.mean(data)
        if mu == 0:
            return 0.0
        return MathEngine.stdev(data) / mu

    @staticmethod
    def iqr(sorted_data: List[float]) -> float:
        """Interquartile Range = Q3 - Q1 (robust spread measure, outlier-resistant)."""
        q1 = MathEngine.percentile(sorted_data, 25.0)
        q3 = MathEngine.percentile(sorted_data, 75.0)
        return q3 - q1

    @staticmethod
    def histogram_bins(
        sorted_data: List[float], num_bins: int = 10
    ) -> List[Tuple[float, float, int]]:
        """
        Compute a linear histogram for sparkline rendering.
        Returns list of (bin_start, bin_end, count) tuples.
        Uses Freedman-Diaconis-inspired bin count but capped at num_bins.
        """
        if len(sorted_data) < 2:
            return []
        lo, hi  = sorted_data[0], sorted_data[-1]
        if lo == hi:
            return [(lo, hi, len(sorted_data))]
        width   = (hi - lo) / num_bins
        bins: List[Tuple[float, float, int]] = []
        for i in range(num_bins):
            b_lo  = lo + i * width
            b_hi  = lo + (i + 1) * width
            count = sum(1 for x in sorted_data if b_lo <= x < b_hi)
            bins.append((b_lo, b_hi, count))
        # Last bin is closed on both ends
        bins[-1] = (bins[-1][0], hi, bins[-1][2] + (1 if sorted_data[-1] == hi else 0))
        return bins

    @staticmethod
    def speedup_ratio(miss_latencies: List[float], hit_latencies: List[float]) -> float:
        """
        Cache speedup ratio = mean(MISS) / mean(HIT).
        Measures how many times faster a cache HIT is vs a cache MISS.
        A ratio of 3.0 means HITs are 3x faster on average.
        """
        miss_mean = MathEngine.mean(miss_latencies)
        hit_mean  = MathEngine.mean(hit_latencies)
        if hit_mean == 0:
            return 0.0
        return miss_mean / hit_mean


# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS PROFILES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PercentileProfile:
    """Complete latency distribution profile for a dataset."""
    label:    str
    count:    int
    p50:      float
    p95:      float
    p99:      float
    p_max:    float
    mean:     float
    stdev:    float
    cv:       float
    iqr:      float
    p25:      float
    p75:      float


@dataclass
class SegmentThroughputProfile:
    """Throughput and latency profile for a single trace segment."""
    segment:       str
    event_count:   int
    total_volume:  int          # Sum of all latencies (proxy for total CPU time)
    hit_count:     int
    miss_count:    int
    hit_rate:      float
    p50_us:        float
    p99_us:        float
    mean_us:       float


@dataclass
class CacheCorrelationProfile:
    """Statistical correlation between cache state and processing latency."""
    hit_count:       int
    miss_count:      int
    hit_mean_us:     float
    miss_mean_us:    float
    hit_p50_us:      float
    miss_p50_us:     float
    hit_p95_us:      float
    miss_p95_us:     float
    hit_p99_us:      float
    miss_p99_us:     float
    hit_max_us:      float
    miss_max_us:     float
    speedup_ratio:   float
    latency_delta_us: float     # mean(MISS) - mean(HIT)
    hit_cv:          float
    miss_cv:         float


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC TELEMETRY PIPELINE
# ─────────────────────────────────────────────────────────────────────────────

class TelemetryPipeline:
    """
    Asynchronous telemetry ingestion and analytics computation engine.

    Concurrency Model:
        - Stream parsing is I/O-bound (simulated) → runs as async coroutines
        - Math computation is CPU-bound → offloaded to asyncio.to_thread()
          so it never blocks the event loop during heavy statistical passes
        - Results are aggregated via asyncio.gather() for max parallelism

    The three analytics profiles are computed CONCURRENTLY on the same dataset —
    total compute time equals the slowest individual analysis, not the sum.
    """

    def __init__(self, records: List[TelemetryRecord]) -> None:
        self._records   = records
        self._math      = MathEngine()

    # ── Async stream parsing (simulates header log file ingestion) ────────────

    async def _parse_stream(
        self, chunk_size: int = 500
    ) -> List[TelemetryRecord]:
        """
        Simulate async log stream ingestion by yielding control between chunks.
        In production this would read from a file handle or Kafka consumer.
        Each chunk yields to the event loop via asyncio.sleep(0), allowing
        other coroutines (e.g. health checks, API calls) to interleave.
        """
        parsed: List[TelemetryRecord] = []
        total = len(self._records)
        for start in range(0, total, chunk_size):
            chunk = self._records[start : start + chunk_size]
            parsed.extend(chunk)
            # Yield to event loop between chunks — cooperative multitasking
            # This is the critical non-blocking contract: no chunk holds the
            # loop for more than O(chunk_size) iterations
            await asyncio.sleep(0)
        return parsed

    # ── Profile 1: Percentile Distribution ───────────────────────────────────

    async def compute_percentile_profile(
        self, records: List[TelemetryRecord]
    ) -> PercentileProfile:
        """
        Offload sort + percentile math to a thread pool.
        Sorting N=5000 records is O(N log N) — non-trivial CPU work that
        would block the event loop for ~2ms at large N. asyncio.to_thread()
        routes this to Python's default ThreadPoolExecutor, keeping the
        event loop free for incoming I/O while math runs concurrently.
        """
        def _compute() -> PercentileProfile:
            latencies = [r.processing_time_us for r in records]
            sorted_lat = sorted(latencies)
            return PercentileProfile(
                label  = "Global",
                count  = len(sorted_lat),
                p50    = MathEngine.percentile(sorted_lat, 50),
                p95    = MathEngine.percentile(sorted_lat, 95),
                p99    = MathEngine.percentile(sorted_lat, 99),
                p_max  = MathEngine.percentile(sorted_lat, 100),
                mean   = MathEngine.mean(latencies),
                stdev  = MathEngine.stdev(latencies),
                cv     = MathEngine.coefficient_of_variation(latencies),
                iqr    = MathEngine.iqr(sorted_lat),
                p25    = MathEngine.percentile(sorted_lat, 25),
                p75    = MathEngine.percentile(sorted_lat, 75),
            )

        return await asyncio.to_thread(_compute)

    # ── Profile 2: Segment Throughput ─────────────────────────────────────────

    async def compute_segment_throughput(
        self, records: List[TelemetryRecord]
    ) -> List[SegmentThroughputProfile]:
        """
        Group records by trace segment (first 8 chars of trace_id),
        then compute per-segment throughput and latency metrics.
        Uses defaultdict for O(1) amortized insertion per record.
        """
        def _compute() -> List[SegmentThroughputProfile]:
            # Partition records by segment — O(N) single pass
            by_segment: Dict[str, List[TelemetryRecord]] = defaultdict(list)
            for rec in records:
                by_segment[rec.trace_segment].append(rec)

            profiles: List[SegmentThroughputProfile] = []
            for segment, seg_records in by_segment.items():
                latencies  = [r.processing_time_us for r in seg_records]
                sorted_lat = sorted(latencies)
                hits       = sum(1 for r in seg_records if r.cache_status == CacheStatus.HIT)
                misses     = len(seg_records) - hits
                profiles.append(SegmentThroughputProfile(
                    segment      = segment,
                    event_count  = len(seg_records),
                    total_volume = sum(latencies),
                    hit_count    = hits,
                    miss_count   = misses,
                    hit_rate     = hits / len(seg_records) if seg_records else 0.0,
                    p50_us       = MathEngine.percentile(sorted_lat, 50),
                    p99_us       = MathEngine.percentile(sorted_lat, 99),
                    mean_us      = MathEngine.mean(latencies),
                ))

            # Sort by event_count descending (busiest segments first)
            return sorted(profiles, key=lambda p: p.event_count, reverse=True)

        return await asyncio.to_thread(_compute)

    # ── Profile 3: Cache Correlation Matrix ───────────────────────────────────

    async def compute_cache_correlation(
        self, records: List[TelemetryRecord]
    ) -> CacheCorrelationProfile:
        """
        Partition records by cache state and compute comparative statistics.
        The speedup_ratio quantifies the infrastructure efficiency gain —
        the ratio by which our in-memory cache layer accelerates execution
        relative to cold-path (MISS) processing.
        """
        def _compute() -> CacheCorrelationProfile:
            hits   = [r.processing_time_us for r in records if r.cache_status == CacheStatus.HIT]
            misses = [r.processing_time_us for r in records if r.cache_status == CacheStatus.MISS]

            sorted_hits   = sorted(hits)
            sorted_misses = sorted(misses)

            return CacheCorrelationProfile(
                hit_count        = len(hits),
                miss_count       = len(misses),
                hit_mean_us      = MathEngine.mean(hits),
                miss_mean_us     = MathEngine.mean(misses),
                hit_p50_us       = MathEngine.percentile(sorted_hits,   50),
                miss_p50_us      = MathEngine.percentile(sorted_misses, 50),
                hit_p95_us       = MathEngine.percentile(sorted_hits,   95),
                miss_p95_us      = MathEngine.percentile(sorted_misses, 95),
                hit_p99_us       = MathEngine.percentile(sorted_hits,   99),
                miss_p99_us      = MathEngine.percentile(sorted_misses, 99),
                hit_max_us       = MathEngine.percentile(sorted_hits,   100),
                miss_max_us      = MathEngine.percentile(sorted_misses, 100),
                speedup_ratio    = MathEngine.speedup_ratio(misses, hits),
                latency_delta_us = MathEngine.mean(misses) - MathEngine.mean(hits),
                hit_cv           = MathEngine.coefficient_of_variation(hits),
                miss_cv          = MathEngine.coefficient_of_variation(misses),
            )

        return await asyncio.to_thread(_compute)

    # ── Histogram for sparkline rendering ─────────────────────────────────────

    async def compute_histogram(
        self, records: List[TelemetryRecord], num_bins: int = 12
    ) -> List[Tuple[float, float, int]]:
        def _compute():
            lats = sorted(r.processing_time_us for r in records)
            return MathEngine.histogram_bins(lats, num_bins)
        return await asyncio.to_thread(_compute)

    # ── Main orchestrator ──────────────────────────────────────────────────────

    async def run(self) -> Tuple[
        PercentileProfile,
        List[SegmentThroughputProfile],
        CacheCorrelationProfile,
        List[Tuple[float, float, int]],
    ]:
        """
        Orchestrate the full analytics pipeline with structured concurrency.
        Stream parsing runs first (sequential dependency), then all three
        analytics profiles are computed CONCURRENTLY via asyncio.gather().
        Total time = parse time + max(profile_1, profile_2, profile_3)
        """
        # Phase 1 — Async stream ingestion
        records = await self._parse_stream(chunk_size=500)

        # Phase 2 — Concurrent analytics on the parsed dataset
        # All three coroutines are dispatched simultaneously to the thread pool
        percentile_profile, segment_profiles, cache_profile, histogram = (
            await asyncio.gather(
                self.compute_percentile_profile(records),
                self.compute_segment_throughput(records),
                self.compute_cache_correlation(records),
                self.compute_histogram(records),
            )
        )

        return percentile_profile, segment_profiles, cache_profile, histogram


# ─────────────────────────────────────────────────────────────────────────────
# TERMINAL RENDERER
# ─────────────────────────────────────────────────────────────────────────────

class TelemetryRenderer:
    """
    Formats and renders all three analytics profiles to stdout.
    Pure presentation layer — zero computation, zero side effects.
    """

    _BLOCK_CHARS = " ▁▂▃▄▅▆▇█"

    @classmethod
    def _sparkline(cls, bins: List[Tuple[float, float, int]], width: int = 40) -> str:
        """
        Render a Unicode block-character histogram sparkline.
        Maps each bin's count to one of 9 block height characters,
        producing a visual latency distribution without any graphics library.
        """
        if not bins:
            return ""
        max_count = max(b[2] for b in bins) or 1
        chars = [
            cls._BLOCK_CHARS[min(8, int(b[2] / max_count * 8))]
            for b in bins
        ]
        return "".join(chars)

    @staticmethod
    def _bar(value: float, max_val: float, width: int = 24, colour=None) -> str:
        """Render a proportional ASCII progress bar."""
        if max_val == 0:
            filled = 0
        else:
            filled = int(value / max_val * width)
        bar = "█" * filled + "░" * (width - filled)
        return colour(bar) if colour else bar

    @staticmethod
    def _fmt_us(us: float) -> str:
        """Human-readable microsecond → ms conversion with unit suffix."""
        if us >= 1_000:
            return f"{us/1_000:.2f}ms"
        return f"{us:.0f}μs"

    # ── Section 1: Banner ─────────────────────────────────────────────────────

    @classmethod
    def render_banner(cls, record_count: int, wall_time_ms: float) -> None:
        print()
        print(CYAN(BOLD("  ╔══════════════════════════════════════════════════════════════════════════╗")))
        print(CYAN(BOLD("  ║       AeroStream Telemetry Analytics Engine  —  Performance Report      ║")))
        print(CYAN(BOLD("  ╚══════════════════════════════════════════════════════════════════════════╝")))
        print(f"  {DIM('Records Processed:')} {BOLD(f'{record_count:,}')}  "
              f"{DIM('|')}  {DIM('Pipeline Wall Time:')} {BOLD(f'{wall_time_ms:.2f}ms')}  "
              f"{DIM('|')}  {DIM('Engine:')} {BOLD('AeroStream v1.0')}")
        print()

    # ── Section 2: Percentile Distribution ───────────────────────────────────

    @classmethod
    def render_percentile_profile(
        cls,
        profile: PercentileProfile,
        histogram: List[Tuple[float, float, int]],
    ) -> None:
        w = "─" * 74
        print(BOLD(CYAN("  ┌─ [1] HIGH-FIDELITY LATENCY PERCENTILE DISTRIBUTION ─────────────────────┐")))
        print()

        # Percentile table
        percentiles = [
            ("p25  (1st Quartile)",  profile.p25,   GREEN),
            ("p50  (Median)",        profile.p50,   GREEN),
            ("p75  (3rd Quartile)",  profile.p75,   YELLOW),
            ("p95  (Tail)",          profile.p95,   YELLOW),
            ("p99  (Far Tail)",      profile.p99,   RED),
            ("pMax (Absolute Peak)", profile.p_max, RED),
        ]

        max_lat = profile.p_max or 1.0
        print(f"  {'PERCENTILE':<26} {'VALUE':>10}   {'DISTRIBUTION':<28}  {'NORMALIZED'}")
        print(DIM(f"  {w}"))

        for label, val, colour in percentiles:
            bar     = cls._bar(val, max_lat, width=26, colour=colour)
            norm_pct = val / max_lat * 100
            print(
                f"  {label:<26} {colour(cls._fmt_us(val)):>18}   {bar}  "
                f"{DIM(f'{norm_pct:5.1f}%')}"
            )

        print(DIM(f"  {w}"))
        print()

        # Descriptive stats
        print(f"  {'Metric':<30} {'Value':>14}   {'Metric':<30} {'Value':>14}")
        print(DIM(f"  {'─'*60}"))
        rows = [
            ("Arithmetic Mean",        cls._fmt_us(profile.mean)),
            ("Population Std Dev (σ)", cls._fmt_us(profile.stdev)),
            ("Coeff. of Variation",    f"{profile.cv:.4f}"),
            ("Interquartile Range",    cls._fmt_us(profile.iqr)),
            ("Sample Count (N)",       f"{profile.count:,}"),
            ("IQR / Median Ratio",     f"{(profile.iqr/profile.p50 if profile.p50 else 0):.4f}"),
        ]
        for i in range(0, len(rows), 2):
            l1, v1 = rows[i]
            l2, v2 = rows[i + 1] if i + 1 < len(rows) else ("", "")
            print(f"  {l1:<30} {GREEN(v1):>22}   {l2:<30} {GREEN(v2):>22}")

        print()

        # Sparkline histogram
        sparkline = cls._sparkline(histogram, width=len(histogram))
        if histogram:
            lo  = cls._fmt_us(histogram[0][0])
            hi  = cls._fmt_us(histogram[-1][1])
            mid = cls._fmt_us((histogram[0][0] + histogram[-1][1]) / 2)
            print(f"  Latency Distribution (sparkline):")
            print(f"  {DIM(lo)} {CYAN(sparkline)} {DIM(hi)}")
            print(f"  {DIM('(Each block = one histogram bin; height ∝ event density)')}")

        print()
        print(BOLD(CYAN("  └──────────────────────────────────────────────────────────────────────────┘")))
        print()

    # ── Section 3: Segment Throughput ─────────────────────────────────────────

    @classmethod
    def render_segment_throughput(
        cls, profiles: List[SegmentThroughputProfile]
    ) -> None:
        print(BOLD(CYAN("  ┌─ [2] SEGMENT THROUGHPUT ANALYTICS ──────────────────────────────────────┐")))
        print()

        max_count  = max(p.event_count for p in profiles) if profiles else 1
        max_volume = max(p.total_volume for p in profiles) if profiles else 1

        print(
            f"  {'SEGMENT':^10}  {'EVENTS':>7}  {'VOL(CPU·μs)':>12}  "
            f"{'HIT%':>6}  {'p50':>8}  {'p99':>10}  {'MEAN':>8}  "
            f"{'THROUGHPUT BAR'}"
        )
        print(DIM(f"  {'─'*90}"))

        for p in profiles:
            hit_pct = p.hit_rate * 100
            colour  = GREEN if hit_pct >= 75 else (YELLOW if hit_pct >= 50 else RED)
            bar     = cls._bar(p.event_count, max_count, width=20, colour=colour)
            print(
                f"  {CYAN(p.segment):^18}  {p.event_count:>7,}  "
                f"{p.total_volume:>12,}  "
                f"{colour(f'{hit_pct:5.1f}%'):>14}  "
                f"{cls._fmt_us(p.p50_us):>8}  "
                f"{RED(cls._fmt_us(p.p99_us)):>18}  "
                f"{cls._fmt_us(p.mean_us):>8}  "
                f"{bar}"
            )

        print(DIM(f"  {'─'*90}"))

        # Aggregate footer
        total_events = sum(p.event_count  for p in profiles)
        total_volume = sum(p.total_volume for p in profiles)
        global_hr    = sum(p.hit_count    for p in profiles) / total_events * 100
        print(
            f"  {'TOTAL / GLOBAL':^18}  {total_events:>7,}  "
            f"{total_volume:>12,}  "
            f"{GREEN(f'{global_hr:5.1f}%'):>14}  "
        )
        print()
        print(BOLD(CYAN("  └──────────────────────────────────────────────────────────────────────────┘")))
        print()

    # ── Section 4: Cache Correlation Matrix ───────────────────────────────────

    @classmethod
    def render_cache_correlation(cls, c: CacheCorrelationProfile) -> None:
        print(BOLD(CYAN("  ┌─ [3] CACHE-PERFORMANCE MATRIX CORRELATION ──────────────────────────────┐")))
        print()

        total = c.hit_count + c.miss_count
        hit_pct  = c.hit_count  / total * 100 if total else 0
        miss_pct = c.miss_count / total * 100 if total else 0

        # Volume distribution
        print(f"  {'CACHE STATE':<14} {'COUNT':>8}  {'SHARE':>8}  {'VOLUME BAR'}")
        print(DIM(f"  {'─'*60}"))
        print(
            f"  {GREEN('HIT'):<22}  {c.hit_count:>8,}  "
            f"{GREEN(f'{hit_pct:.1f}%'):>14}  "
            f"{cls._bar(c.hit_count, total, width=30, colour=GREEN)}"
        )
        print(
            f"  {RED('MISS'):<22}  {c.miss_count:>8,}  "
            f"{RED(f'{miss_pct:.1f}%'):>14}  "
            f"{cls._bar(c.miss_count, total, width=30, colour=RED)}"
        )
        print()

        # Comparative latency table
        print(f"  {'LATENCY METRIC':<30} {'CACHE HIT':>14}  {'CACHE MISS':>14}  {'DELTA':>14}")
        print(DIM(f"  {'─'*76}"))

        rows = [
            ("Mean (μ)",     c.hit_mean_us,  c.miss_mean_us),
            ("Median (p50)", c.hit_p50_us,   c.miss_p50_us),
            ("p95 (Tail)",   c.hit_p95_us,   c.miss_p95_us),
            ("p99 (Far Tail)", c.hit_p99_us, c.miss_p99_us),
            ("Max (Peak)",   c.hit_max_us,   c.miss_max_us),
        ]

        for label, hit_val, miss_val in rows:
            delta = miss_val - hit_val
            delta_str = f"+{cls._fmt_us(delta)}" if delta >= 0 else f"-{cls._fmt_us(abs(delta))}"
            print(
                f"  {label:<30} "
                f"{GREEN(cls._fmt_us(hit_val)):>22}  "
                f"{RED(cls._fmt_us(miss_val)):>22}  "
                f"{YELLOW(delta_str):>22}"
            )

        print(DIM(f"  {'─'*76}"))
        print()

        # Coefficient of variation comparison
        print(f"  {'DISPERSION METRICS':<30} {'CACHE HIT':>14}  {'CACHE MISS':>14}")
        print(DIM(f"  {'─'*60}"))
        print(
            f"  {'Coeff. of Variation (CV)':<30} "
            f"{GREEN(f'{c.hit_cv:.4f}'):>22}  "
            f"{RED(f'{c.miss_cv:.4f}'):>22}"
        )
        print()

        # Key derived metrics — the headline numbers
        print(BOLD(f"  {'KEY EFFICIENCY METRICS':}"))
        print(DIM(f"  {'─'*60}"))
        print(
            f"  {'Cache Speedup Ratio':<38} "
            f"{BOLD(GREEN(f'{c.speedup_ratio:.2f}x'))}"
            f"  {DIM('(MISS mean / HIT mean)')}"
        )
        print(
            f"  {'Mean Latency Delta (MISS - HIT)':<38} "
            f"{BOLD(YELLOW(cls._fmt_us(c.latency_delta_us)))}"
            f"  {DIM('(absolute latency savings per HIT)')}"
        )
        hit_total_saved = c.hit_count * c.latency_delta_us
        print(
            f"  {'Total Pipeline Latency Saved':<38} "
            f"{BOLD(GREEN(cls._fmt_us(hit_total_saved)))}"
            f"  {DIM(f'({c.hit_count:,} HITs × {cls._fmt_us(c.latency_delta_us)} delta)')}"
        )
        efficiency_gain = (c.latency_delta_us / c.miss_mean_us * 100) if c.miss_mean_us else 0
        print(
            f"  {'Cache Efficiency Gain':<38} "
            f"{BOLD(GREEN(f'{efficiency_gain:.1f}%'))}"
            f"  {DIM('(latency reduction % relative to MISS)')}"
        )
        print()
        print(BOLD(CYAN("  └──────────────────────────────────────────────────────────────────────────┘")))
        print()

    # ── Section 5: Final verdict ───────────────────────────────────────────────

    @classmethod
    def render_verdict(
        cls,
        percentile: PercentileProfile,
        cache: CacheCorrelationProfile,
        segments: List[SegmentThroughputProfile],
    ) -> None:
        print(BOLD(CYAN("  ┌─ EXECUTIVE SUMMARY ─────────────────────────────────────────────────────┐")))
        print()

        # SLO compliance check (p99 < 1ms = green, < 5ms = yellow, else red)
        p99_ms = percentile.p99 / 1000
        if p99_ms < 1.0:
            slo_colour, slo_label = GREEN, "COMPLIANT ✓  (p99 < 1ms target)"
        elif p99_ms < 5.0:
            slo_colour, slo_label = YELLOW, "WARNING    (p99 < 5ms — monitor)"
        else:
            slo_colour, slo_label = RED,    "BREACHED   (p99 ≥ 5ms — investigate)"

        print(f"  SLO Status (p99 < 1ms):     {slo_colour(BOLD(slo_label))}")
        print(f"  Global p50:                 {GREEN(BOLD(cls._fmt_us(percentile.p50)))}")
        print(f"  Global p99:                 {(GREEN if p99_ms < 1 else YELLOW)(BOLD(cls._fmt_us(percentile.p99)))}")
        print(f"  Cache Speedup:              {GREEN(BOLD(f'{cache.speedup_ratio:.2f}x'))}")
        print(f"  Global Hit Rate:            {GREEN(BOLD(f'{cache.hit_count/(cache.hit_count+cache.miss_count)*100:.1f}%'))}")
        print(f"  Active Segments:            {BOLD(str(len(segments)))}")
        hottest = segments[0] if segments else None
        if hottest:
            print(f"  Hottest Segment:            {CYAN(BOLD(hottest.segment))}  ({hottest.event_count:,} events)")
        print()
        print(BOLD(CYAN("  └──────────────────────────────────────────────────────────────────────────┘")))
        print()


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main(event_count: int, seed: int) -> None:
    renderer = TelemetryRenderer()

    # ── Step 1: Generate mock telemetry stream ────────────────────────────────
    generator = TelemetryStreamGenerator(seed=seed)

    print(f"\n  {DIM('Generating telemetry stream...')} ", end="", flush=True)
    gen_start = time.perf_counter_ns()

    # CPU-bound generation offloaded to thread pool
    records: List[TelemetryRecord] = await asyncio.to_thread(
        generator.generate_batch, event_count
    )
    gen_ms = (time.perf_counter_ns() - gen_start) / 1_000_000
    print(f"{GREEN('done')}  {DIM(f'({event_count:,} records in {gen_ms:.1f}ms)')}")

    # ── Step 2: Run the async analytics pipeline ──────────────────────────────
    print(f"  {DIM('Running analytics pipeline...')}  ", end="", flush=True)
    pipeline   = TelemetryPipeline(records)
    pipe_start = time.perf_counter_ns()

    percentile_profile, segment_profiles, cache_profile, histogram = (
        await pipeline.run()
    )
    pipe_ms = (time.perf_counter_ns() - pipe_start) / 1_000_000
    print(f"{GREEN('done')}  {DIM(f'(3 profiles × concurrent threads in {pipe_ms:.1f}ms)')}")

    total_ms = gen_ms + pipe_ms

    # ── Step 3: Render all profiles ───────────────────────────────────────────
    renderer.render_banner(len(records), total_ms)
    renderer.render_percentile_profile(percentile_profile, histogram)
    renderer.render_segment_throughput(segment_profiles)
    renderer.render_cache_correlation(cache_profile)
    renderer.render_verdict(percentile_profile, cache_profile, segment_profiles)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AeroStream Telemetry Analytics Engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--events", type=int, default=2_000,
        help="Number of synthetic telemetry records to generate and analyze",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="RNG seed for reproducible mock stream generation",
    )
    args = parser.parse_args()

    asyncio.run(main(event_count=args.events, seed=args.seed))
