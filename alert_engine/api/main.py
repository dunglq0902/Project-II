"""
alert-engine/api/main.py
FastAPI application entry point for the Alert Engine API.

Endpoints:
  /api/v1/rules/*          – CRUD for alert rules
  /api/v1/notifications/*  – Dispatch incoming alert events
  /health                  – Health check
  /metrics                 – Prometheus metrics

Run:
    uvicorn alert-engine.api.main:app --host 0.0.0.0 --port 8000 --workers 4
"""

import logging
import os
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from motor.motor_asyncio import AsyncIOMotorClient

from .routes.rules import router as rules_router
from .models import AlertEvent, APIResponse

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
MONGO_URI       = os.getenv("MONGO_URI",        "mongodb://mongodb.storage-system.svc.cluster.local:27017")
MONGO_DB        = os.getenv("MONGO_DB",         "crypto_analytics")
NOTIFICATION_SVC= os.getenv("NOTIFICATION_SVC", "http://localhost:8001")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger("AlertEngineAPI")


# ─────────────────────────────────────────────
# Lifespan: DB connect / disconnect
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Connecting to MongoDB: %s", MONGO_URI)
    client = AsyncIOMotorClient(MONGO_URI)
    app.state.db     = client[MONGO_DB]
    app.state.client = client

    # Create indexes on startup
    await app.state.db["alert_rules"].create_index(
        [("user_id", 1), ("symbol", 1), ("timeframe", 1)]
    )
    await app.state.db["alert_rules"].create_index([("rule_id", 1)], unique=True)
    await app.state.db["alert_rules"].create_index([("is_active", 1)])
    await app.state.db["alert_events"].create_index([("rule_id", 1), ("triggered_at", -1)])
    logger.info("MongoDB connected and indexes ensured.")

    yield

    logger.info("Shutting down – closing MongoDB connection.")
    client.close()


# ─────────────────────────────────────────────
# App
# ─────────────────────────────────────────────
app = FastAPI(
    title="Crypto Alert Engine API",
    description="REST API for managing trading alert rules and dispatching notifications.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(rules_router)


# ─────────────────────────────────────────────
# Lightweight Prometheus Metrics
# ─────────────────────────────────────────────
_request_counts: dict = defaultdict(int)
_alerts_dispatched: int = 0
_alerts_failed: int = 0
_request_durations: list = []


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Track request counts and latency for /metrics."""
    start = time.monotonic()
    response = await call_next(request)
    duration = time.monotonic() - start
    key = f'{request.method} {request.url.path} {response.status_code}'
    _request_counts[key] += 1
    _request_durations.append(duration)
    # Keep only last 1000 durations to avoid memory leak
    if len(_request_durations) > 1000:
        _request_durations.pop(0)
    return response


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics():
    """Expose Prometheus-compatible metrics."""
    lines = [
        "# HELP alert_api_requests_total Total HTTP requests.",
        "# TYPE alert_api_requests_total counter",
    ]
    for key, count in _request_counts.items():
        parts = key.split(" ", 2)
        method, endpoint, status_code = parts[0], parts[1], parts[2]
        lines.append(
            f'alert_api_requests_total{{method="{method}",endpoint="{endpoint}",status="{status_code}"}} {count}'
        )
    lines += [
        "",
        "# HELP alert_api_alerts_dispatched_total Total alerts successfully dispatched.",
        "# TYPE alert_api_alerts_dispatched_total counter",
        f"alert_api_alerts_dispatched_total {_alerts_dispatched}",
        "",
        "# HELP alert_api_alerts_failed_total Total alerts that failed to dispatch.",
        "# TYPE alert_api_alerts_failed_total counter",
        f"alert_api_alerts_failed_total {_alerts_failed}",
    ]
    if _request_durations:
        avg_dur = sum(_request_durations) / len(_request_durations)
        lines += [
            "",
            "# HELP alert_api_request_duration_avg_seconds Average request duration.",
            "# TYPE alert_api_request_duration_avg_seconds gauge",
            f"alert_api_request_duration_avg_seconds {avg_dur:.6f}",
        ]
    return "\n".join(lines) + "\n"


# ─────────────────────────────────────────────
# Notification Dispatch Endpoint
# ─────────────────────────────────────────────
@app.post("/api/v1/notifications/dispatch", response_model=APIResponse)
async def dispatch_notification(event: AlertEvent, request: Request):
    """
    Receive an alert event (from Spark unified job)
    and forward to the appropriate notification channel(s).
    """
    import httpx

    db = request.app.state.db

    # Fetch the rule to get notification channels
    rule_doc = await db["alert_rules"].find_one(
        {"rule_id": event.rule_id}, {"_id": 0}
    )
    if not rule_doc:
        raise HTTPException(status_code=404, detail=f"Rule {event.rule_id} not found.")

    channels = rule_doc.get("notification_channels", [])

    # Record event in MongoDB for audit
    event_doc = event.dict()
    event_doc["dispatched_at"] = datetime.utcnow()
    event_doc["channels"]      = channels
    await db["alert_events"].insert_one(event_doc)

    # Update rule trigger stats
    await db["alert_rules"].update_one(
        {"rule_id": event.rule_id},
        {
            "$set":  {"last_triggered_at": datetime.utcnow()},
            "$inc":  {"trigger_count": 1},
        },
    )

    # Forward to notification service
    async with httpx.AsyncClient(timeout=10) as client:
        for channel in channels:
            try:
                resp = await client.post(
                    f"{NOTIFICATION_SVC}/notify/{channel}",
                    json=jsonable_encoder({"event": event.dict(), "rule": rule_doc}),
                )
                if resp.status_code != 200:
                    logger.warning("Notification failed for channel %s: %s", channel, resp.text)
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Notification error channel=%s: %s", channel, exc)

    logger.info("Alert dispatched: rule=%s symbol=%s action=%s", event.rule_id, event.symbol, event.action)

    global _alerts_dispatched
    _alerts_dispatched += 1

    return APIResponse(success=True, message="Alert dispatched.", data={"alert_id": event.alert_id})


# ─────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────
@app.get("/health")
async def health_check(request: Request):
    """Kubernetes liveness/readiness probe endpoint."""
    try:
        await request.app.state.db.command("ping")
        return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
    except Exception as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "error": str(exc)},
        )


@app.get("/")
async def root():
    return {"service": "Crypto Alert Engine API", "version": "2.0.0", "docs": "/docs"}
