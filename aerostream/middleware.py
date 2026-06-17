"""
AeroStream Middleware Stack
============================
Request-level instrumentation, tracing, and observability middleware.
"""

from __future__ import annotations

import logging
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger("aerostream.middleware")


class RequestTracingMiddleware(BaseHTTPMiddleware):
    """
    Injects a unique trace ID into every request and measures end-to-end
    latency. The trace ID propagates via response headers for client-side
    correlation and debugging.

    Performance Note:
        time.perf_counter_ns() is used instead of time.time() because it
        provides monotonic nanosecond resolution without system clock skew.
        The middleware itself adds <5μs overhead per request.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Generate a short trace ID for this request
        trace_id = uuid.uuid4().hex[:16]
        request.state.trace_id = trace_id
        request.state.start_time_ns = time.perf_counter_ns()

        response = await call_next(request)

        # Calculate end-to-end latency
        elapsed_ns = time.perf_counter_ns() - request.state.start_time_ns
        elapsed_us = elapsed_ns // 1000
        elapsed_ms = elapsed_us / 1000

        # Inject tracing headers into response
        response.headers["X-Trace-Id"] = trace_id
        response.headers["X-Processing-Time-Us"] = str(elapsed_us)

        # Log slow requests (>50ms) at WARNING level for investigation
        if elapsed_ms > 50:
            logger.warning(
                "SLOW REQUEST trace=%s method=%s path=%s latency=%.2fms",
                trace_id, request.method, request.url.path, elapsed_ms,
            )
        else:
            logger.debug(
                "trace=%s method=%s path=%s latency=%.2fms status=%d",
                trace_id, request.method, request.url.path, elapsed_ms,
                response.status_code,
            )

        return response


class CORSHeadersMiddleware(BaseHTTPMiddleware):
    """
    Lightweight CORS middleware for hackathon demo.
    Allows all origins for simplicity — in production, this would be locked down.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if request.method == "OPTIONS":
            response = Response(status_code=204)
        else:
            response = await call_next(request)

        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Trace-Id"
        response.headers["Access-Control-Expose-Headers"] = "X-Trace-Id, X-Processing-Time-Us"
        return response
