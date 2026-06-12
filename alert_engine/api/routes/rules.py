"""
alert-engine/api/routes/rules.py
FastAPI router for CRUD operations on Alert Rules.
All routes require a valid user_id header (simplified auth for demo).
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from ..models import (
    AlertRule, AlertRuleCreate, AlertRuleUpdate,
    APIResponse, PaginatedRules,
)

router = APIRouter(prefix="/api/v1/rules", tags=["Alert Rules"])


# ─────────────────────────────────────────────
# Dependency: get DB
# ─────────────────────────────────────────────
async def get_db(request: Request) -> AsyncIOMotorDatabase:
    """Retrieve MongoDB database from app state (set during lifespan)."""
    return request.app.state.db


def get_current_user(x_user_id: str = Header(...)) -> str:
    """Simplified: extract user_id from request header."""
    if not x_user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-User-Id header")
    return x_user_id


# ─────────────────────────────────────────────
# CRUD Endpoints
# ─────────────────────────────────────────────

@router.post("/", response_model=APIResponse, status_code=status.HTTP_201_CREATED)
async def create_rule(
    payload:  AlertRuleCreate,
    user_id:  str = Depends(get_current_user),
    db:       AsyncIOMotorDatabase = Depends(get_db),
):
    """Create a new alert rule for the authenticated user."""
    rule = AlertRule(**payload.dict(), user_id=user_id)
    rule_doc = rule.dict()
    rule_doc["created_at"] = datetime.utcnow()
    rule_doc["updated_at"] = datetime.utcnow()

    await db["alert_rules"].insert_one(rule_doc)
    return APIResponse(success=True, message="Alert rule created.", data={"rule_id": rule.rule_id})


@router.get("/", response_model=PaginatedRules)
async def list_rules(
    user_id:   str = Depends(get_current_user),
    db:        AsyncIOMotorDatabase = Depends(get_db),
    symbol:    Optional[str] = Query(None, description="Filter by symbol"),
    timeframe: Optional[str] = Query(None, description="Filter by timeframe"),
    is_active: Optional[bool]= Query(None, description="Filter by active status"),
    page:      int = Query(1, ge=1),
    limit:     int = Query(20, ge=1, le=100),
):
    """List all alert rules for the authenticated user with optional filters."""
    query: dict = {"user_id": user_id}
    if symbol:
        query["symbol"] = symbol.upper()
    if timeframe:
        query["timeframe"] = timeframe
    if is_active is not None:
        query["is_active"] = is_active

    total = await db["alert_rules"].count_documents(query)
    cursor = (
        db["alert_rules"]
        .find(query, {"_id": 0})
        .sort("created_at", -1)
        .skip((page - 1) * limit)
        .limit(limit)
    )
    items = [AlertRule(**doc) async for doc in cursor]

    return PaginatedRules(total=total, page=page, limit=limit, items=items)


@router.get("/{rule_id}", response_model=AlertRule)
async def get_rule(
    rule_id: str,
    user_id: str = Depends(get_current_user),
    db:      AsyncIOMotorDatabase = Depends(get_db),
):
    """Retrieve a specific alert rule by ID."""
    doc = await db["alert_rules"].find_one(
        {"rule_id": rule_id, "user_id": user_id}, {"_id": 0}
    )
    if not doc:
        raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found.")
    return AlertRule(**doc)


@router.patch("/{rule_id}", response_model=APIResponse)
async def update_rule(
    rule_id:  str,
    payload:  AlertRuleUpdate,
    user_id:  str = Depends(get_current_user),
    db:       AsyncIOMotorDatabase = Depends(get_db),
):
    """Partial update of an existing alert rule."""
    update_data = {k: v for k, v in payload.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No update fields provided.")

    update_data["updated_at"] = datetime.utcnow()

    result = await db["alert_rules"].update_one(
        {"rule_id": rule_id, "user_id": user_id},
        {"$set": update_data},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found.")

    return APIResponse(success=True, message="Rule updated successfully.")


@router.delete("/{rule_id}", response_model=APIResponse)
async def delete_rule(
    rule_id: str,
    user_id: str = Depends(get_current_user),
    db:      AsyncIOMotorDatabase = Depends(get_db),
):
    """Delete an alert rule permanently."""
    result = await db["alert_rules"].delete_one(
        {"rule_id": rule_id, "user_id": user_id}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found.")
    return APIResponse(success=True, message="Rule deleted.")


@router.post("/{rule_id}/toggle", response_model=APIResponse)
async def toggle_rule(
    rule_id: str,
    user_id: str = Depends(get_current_user),
    db:      AsyncIOMotorDatabase = Depends(get_db),
):
    """Toggle the is_active status of an alert rule."""
    doc = await db["alert_rules"].find_one(
        {"rule_id": rule_id, "user_id": user_id}, {"_id": 0, "is_active": 1}
    )
    if not doc:
        raise HTTPException(status_code=404, detail=f"Rule '{rule_id}' not found.")

    new_status = not doc["is_active"]
    await db["alert_rules"].update_one(
        {"rule_id": rule_id},
        {"$set": {"is_active": new_status, "updated_at": datetime.utcnow()}},
    )
    state = "activated" if new_status else "deactivated"
    return APIResponse(success=True, message=f"Rule {state}.", data={"is_active": new_status})


@router.get("/{rule_id}/history", response_model=APIResponse)
async def get_rule_history(
    rule_id: str,
    user_id: str = Depends(get_current_user),
    db:      AsyncIOMotorDatabase = Depends(get_db),
    limit:   int = Query(50, ge=1, le=200),
):
    """Retrieve the last N alert trigger events for a specific rule."""
    cursor = (
        db["alert_events"]
        .find({"rule_id": rule_id, "user_id": user_id}, {"_id": 0})
        .sort("triggered_at", -1)
        .limit(limit)
    )
    events = [doc async for doc in cursor]
    return APIResponse(success=True, message="OK", data=events)
