"""
tests/test_alert_api.py
Integration tests for the Alert Engine FastAPI application.
Uses httpx.AsyncClient with TestClient — no running server needed.
"""

import json
import sys
import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Skip if FastAPI / httpx not installed
fastapi = pytest.importorskip("fastapi",  reason="fastapi not installed")
httpx   = pytest.importorskip("httpx",    reason="httpx not installed")
motor   = pytest.importorskip("motor",    reason="motor not installed")

from fastapi.testclient import TestClient


# ─────────────────────────────────────────────
# App setup with mocked DB
# ─────────────────────────────────────────────

@pytest.fixture
def mock_db():
    """Return a mock async MongoDB database object."""
    db = MagicMock()

    async def _find_one(query, projection=None):
        return None

    async def _insert_one(doc):
        return MagicMock(inserted_id="fake-id")

    async def _update_one(query, update):
        return MagicMock(matched_count=1, modified_count=1)

    async def _delete_one(query):
        return MagicMock(deleted_count=1)

    async def _count_documents(query):
        return 0

    class AsyncCursor:
        def __init__(self, items): self._items = iter(items)
        def sort(self, *a, **kw): return self
        def skip(self, *a): return self
        def limit(self, *a): return self
        def __aiter__(self): return self
        async def __anext__(self):
            try:    return next(self._items)
            except StopIteration: raise StopAsyncIteration

    db["alert_rules"].find_one        = _find_one
    db["alert_rules"].insert_one      = _insert_one
    db["alert_rules"].update_one      = _update_one
    db["alert_rules"].delete_one      = _delete_one
    db["alert_rules"].count_documents = _count_documents
    db["alert_rules"].find            = lambda *a, **kw: AsyncCursor([])
    db["alert_rules"].create_index    = AsyncMock()
    db["alert_events"].find           = lambda *a, **kw: AsyncCursor([])
    db["alert_events"].insert_one     = _insert_one
    db["alert_events"].create_index   = AsyncMock()
    return db


@pytest.fixture
def client(mock_db):
    """Create a TestClient with mocked DB injected into app state."""
    from alert_engine.api.main import app

    async def _mock_ping(*args, **kwargs):
        return {"ok": 1}

    mock_db.command = _mock_ping

    mock_motor_client = MagicMock()
    mock_motor_client.__getitem__.return_value = mock_db

    with patch("alert_engine.api.main.AsyncIOMotorClient", return_value=mock_motor_client):
        with TestClient(app, raise_server_exceptions=False) as c:
            # Re-ensure app.state.db points to mock_db just in case
            app.state.db = mock_db
            yield c


# ─────────────────────────────────────────────
# Health endpoint
# ─────────────────────────────────────────────

class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_returns_json(self, client):
        resp = client.get("/health")
        assert resp.headers["content-type"].startswith("application/json")


# ─────────────────────────────────────────────
# Create Rule endpoint
# ─────────────────────────────────────────────

VALID_RULE_PAYLOAD = {
    "symbol":    "BTCUSDT",
    "timeframe": "1h",
    "logic":     "AND",
    "action":    "BUY",
    "conditions": [
        {"field": "rsi_14",       "operator": "<",  "value": 30},
        {"field": "volume_ratio", "operator": ">",  "value": 1.5},
    ],
    "notification_channels": ["email"],
    "email_address": "luudungpkt922005@gmail.com",
    "cooldown_seconds": 300,
}


class TestCreateRule:
    def test_create_rule_returns_201(self, client):
        resp = client.post(
            "/api/v1/rules/",
            json=VALID_RULE_PAYLOAD,
            headers={"X-User-Id": "user-001"},
        )
        assert resp.status_code == 201

    def test_create_rule_returns_rule_id(self, client):
        resp = client.post(
            "/api/v1/rules/",
            json=VALID_RULE_PAYLOAD,
            headers={"X-User-Id": "user-001"},
        )
        data = resp.json()
        assert data["success"] is True
        assert "rule_id" in data.get("data", {})

    def test_create_rule_missing_user_header_returns_422(self, client):
        resp = client.post("/api/v1/rules/", json=VALID_RULE_PAYLOAD)
        assert resp.status_code in (401, 422)

    def test_create_rule_invalid_symbol_normalized(self, client):
        payload = {**VALID_RULE_PAYLOAD, "symbol": "btcusdt"}
        resp = client.post(
            "/api/v1/rules/",
            json=payload,
            headers={"X-User-Id": "user-001"},
        )
        # Should succeed — symbol is uppercased by validator
        assert resp.status_code == 201

    def test_create_rule_invalid_cooldown_returns_422(self, client):
        payload = {**VALID_RULE_PAYLOAD, "cooldown_seconds": 10}   # < 60 min
        resp = client.post(
            "/api/v1/rules/",
            json=payload,
            headers={"X-User-Id": "user-001"},
        )
        assert resp.status_code == 422

    def test_create_rule_empty_conditions_returns_422(self, client):
        payload = {**VALID_RULE_PAYLOAD, "conditions": []}
        resp = client.post(
            "/api/v1/rules/",
            json=payload,
            headers={"X-User-Id": "user-001"},
        )
        assert resp.status_code == 422

    def test_create_rule_email_channel_missing_email_returns_422(self, client):
        payload = {
            **VALID_RULE_PAYLOAD,
            "notification_channels": ["email"],
            "email_address": "",  # Explicitly empty to trigger validator error
        }
        resp = client.post(
            "/api/v1/rules/",
            json=payload,
            headers={"X-User-Id": "user-001"},
        )
        assert resp.status_code == 422


# ─────────────────────────────────────────────
# List Rules endpoint
# ─────────────────────────────────────────────

class TestListRules:
    def test_list_rules_returns_200(self, client):
        resp = client.get(
            "/api/v1/rules/",
            headers={"X-User-Id": "user-001"},
        )
        assert resp.status_code == 200

    def test_list_rules_response_shape(self, client):
        resp = client.get(
            "/api/v1/rules/",
            headers={"X-User-Id": "user-001"},
        )
        data = resp.json()
        assert "total" in data
        assert "items" in data
        assert "page"  in data
        assert "limit" in data

    def test_list_rules_requires_auth(self, client):
        resp = client.get("/api/v1/rules/")
        assert resp.status_code in (401, 422)


# ─────────────────────────────────────────────
# Notification dispatch endpoint
# ─────────────────────────────────────────────

class TestDispatchNotification:
    def test_dispatch_unknown_rule_returns_404(self, client):
        payload = {
            "alert_id":       "alert-001",
            "rule_id":        "nonexistent-rule",
            "user_id":        "user-001",
            "symbol":         "BTCUSDT",
            "timeframe":      "1h",
            "action":         "BUY",
            "triggered_at":   datetime.utcnow().isoformat(),
            "close_price":    37050.0,
            "rsi_14":         28.5,
            "macd":           -50.0,
            "volume_ratio":   2.1,
            "candle_pattern": "HAMMER",
            "message":        "Test alert",
        }
        resp = client.post("/api/v1/notifications/dispatch", json=payload)
        assert resp.status_code == 404
