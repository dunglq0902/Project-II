"""
alert-engine/notifier/notification_service.py
Lightweight FastAPI microservice that receives dispatch requests
from the Alert Engine API and routes them to the correct notifier channel.

Run:
    uvicorn alert-engine.notifier.notification_service:app --port 8001
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Path, Body
from pydantic import BaseModel, Field

from .notifiers import get_notifier, EmailNotifier

logger = logging.getLogger("NotificationService")

app = FastAPI(
    title="Crypto Notification Service",
    description="Routes alert events to Email and Webhook channels.",
    version="2.0.0",
)


# ─────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────

class TestEmailRequest(BaseModel):
    """Request body for the test-email endpoint."""
    recipient: str = Field(
        ...,
        description="Email address to send test alert to",
        example="luudungpkt922005@gmail.com",
    )
    symbol: str = Field(default="BTCUSDT", description="Crypto symbol for the fake alert")
    action: str = Field(default="BUY", description="Signal action: BUY, SELL, WATCH")


# ─────────────────────────────────────────────
# Core dispatch endpoint
# ─────────────────────────────────────────────

@app.post("/notify/{channel}")
async def notify(
    channel: str = Path(..., description="Notification channel: email | webhook"),
    body: Dict[str, Any] = None,
):
    """
    Dispatch an alert event to the specified notification channel.
    Expected body: { "event": {...}, "rule": {...} }
    """
    if body is None:
        raise HTTPException(status_code=400, detail="Request body is required.")

    event = body.get("event") or body   # Allow flat event payload too
    rule  = body.get("rule", {})

    notifier = get_notifier(channel)
    if not notifier:
        raise HTTPException(status_code=400, detail=f"Unknown channel: {channel}")

    success = await notifier.send(event, rule)
    if not success:
        raise HTTPException(status_code=502, detail=f"Failed to send via {channel}.")

    return {"status": "sent", "channel": channel, "alert_id": event.get("alert_id")}


# ─────────────────────────────────────────────
# Test email endpoint (for quick verification)
# ─────────────────────────────────────────────

@app.post("/test-email")
async def test_email(req: TestEmailRequest):
    """
    Send a realistic test alert email to the specified recipient.
    Useful for verifying SMTP configuration without triggering a real alert.
    """
    now = datetime.now(tz=timezone.utc).isoformat()

    fake_event = {
        "alert_id":       "test-alert-000000",
        "rule_id":        "test-rule-000000000",
        "user_id":        "test-user",
        "symbol":         req.symbol,
        "timeframe":      "1h",
        "action":         req.action,
        "triggered_at":   now,
        "close_price":    67891.2345,
        "rsi_14":         28.50,
        "macd":           -150.2500,
        "macd_signal":    -120.0000,
        "volume_ratio":   2.35,
        "candle_pattern": "HAMMER",
        "message":        "Test alert from Crypto Analytics Platform",
    }

    fake_rule = {
        "rule_id":       "test-rule-000000000",
        "email_address": req.recipient,
    }

    notifier = EmailNotifier()
    success = await notifier.send(fake_event, fake_rule)

    if not success:
        raise HTTPException(
            status_code=502,
            detail="Failed to send test email. Check SMTP_HOST, SMTP_USER, SMTP_PASSWORD environment variables.",
        )

    return {
        "status":    "sent",
        "recipient": req.recipient,
        "symbol":    req.symbol,
        "action":    req.action,
        "message":   f"Test email sent to {req.recipient}. Check inbox/spam folder.",
    }


# ─────────────────────────────────────────────
# SMTP diagnostic endpoint
# ─────────────────────────────────────────────

@app.get("/smtp-status")
async def smtp_status():
    """Return current SMTP configuration (passwords masked)."""
    return {
        "SMTP_HOST":     os.getenv("SMTP_HOST", "smtp.gmail.com"),
        "SMTP_PORT":     os.getenv("SMTP_PORT", "587"),
        "SMTP_USER":     os.getenv("SMTP_USER", "NOT SET"),
        "SMTP_PASSWORD": "***" if os.getenv("SMTP_PASSWORD") else "NOT SET",
        "EMAIL_FROM":    os.getenv("EMAIL_FROM", "NOT SET"),
    }


# ─────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "healthy"}
