"""API request metrics with bounded route labels."""

import time

from fastapi import Response
from prometheus_client import Counter, Histogram, CONTENT_TYPE_LATEST, generate_latest

REQUESTS = Counter(
    "commerceradar_http_requests_total",
    "Completed API requests",
    ["method", "route", "status"],
)
LATENCY = Histogram(
    "commerceradar_http_request_duration_seconds",
    "API request duration",
    ["method", "route"],
)


def install_metrics(app):
    @app.middleware("http")
    async def measure(request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)
        started = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            label = getattr(route, "path", "unmatched")
            REQUESTS.labels(request.method, label, str(status)).inc()
            LATENCY.labels(request.method, label).observe(
                time.perf_counter() - started
            )

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
