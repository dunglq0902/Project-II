"""
alert-engine/api/models.py
Pydantic models for Alert Rules, Conditions, and API request/response schemas.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator


# ─────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────

class TimeframeEnum(str, Enum):
    ONE_MIN    = "1m"
    THREE_MIN  = "3m"
    FIVE_MIN   = "5m"
    FIFTEEN_MIN= "15m"
    THIRTY_MIN = "30m"
    ONE_HOUR   = "1h"
    FOUR_HOUR  = "4h"
    ONE_DAY    = "1d"


class ActionEnum(str, Enum):
    BUY     = "BUY"
    SELL    = "SELL"
    WATCH   = "WATCH"


class LogicEnum(str, Enum):
    AND = "AND"
    OR  = "OR"


class OperatorEnum(str, Enum):
    GT            = ">"
    LT            = "<"
    GTE           = ">="
    LTE           = "<="
    EQ            = "=="
    NEQ           = "!="
    CROSSES_ABOVE = "crosses_above"
    CROSSES_BELOW = "crosses_below"


class ConditionFieldEnum(str, Enum):
    CLOSE          = "close"
    OPEN           = "open"
    HIGH           = "high"
    LOW            = "low"
    VOLUME         = "volume"
    RSI_14         = "rsi_14"
    MACD           = "macd"
    MACD_SIGNAL    = "macd_signal"
    MACD_HIST      = "macd_hist"
    MA7            = "ma7"
    MA25           = "ma25"
    MA99           = "ma99"
    BB_UPPER       = "bb_upper"
    BB_LOWER       = "bb_lower"
    ATR_14         = "atr_14"
    VOLUME_RATIO   = "volume_ratio"
    CANDLE_PATTERN = "candle_pattern"
    PRICE_CHANGE   = "price_change_pct"
    VWAP           = "vwap"


class NotificationChannelEnum(str, Enum):
    EMAIL    = "email"
    WEBHOOK  = "webhook"


# ─────────────────────────────────────────────
# Alert Condition
# ─────────────────────────────────────────────

class AlertCondition(BaseModel):
    field:    ConditionFieldEnum = Field(..., description="Indicator or price field to evaluate")
    operator: OperatorEnum       = Field(..., description="Comparison operator")
    value:    Union[float, str]  = Field(..., description="Threshold value or pattern name")

    class Config:
        use_enum_values = True

    @validator("value")
    def validate_value(cls, v, values):
        field = values.get("field")
        # candle_pattern expects a string
        if field == ConditionFieldEnum.CANDLE_PATTERN:
            valid_patterns = {
                "DOJI", "HAMMER", "SHOOTING_STAR",
                "BULLISH_ENGULFING", "BEARISH_ENGULFING",
                "BULLISH", "BEARISH", "NEUTRAL",
            }
            if str(v).upper() not in valid_patterns:
                raise ValueError(f"Unknown candle pattern: {v}. Valid: {valid_patterns}")
        else:
            # Numeric fields must be convertible to float
            try:
                float(v)
            except (ValueError, TypeError):
                raise ValueError(f"Field '{field}' requires a numeric value, got: {v}")
        return v


# ─────────────────────────────────────────────
# Alert Rule
# ─────────────────────────────────────────────

class AlertRuleCreate(BaseModel):
    """Payload for creating a new alert rule."""
    symbol:                str                       = Field(..., example="BTCUSDT")
    timeframe:             TimeframeEnum             = Field(..., example="1h")
    conditions:            List[AlertCondition]      = Field(..., min_items=1, max_items=10)
    logic:                 LogicEnum                 = Field(LogicEnum.AND)
    action:                ActionEnum                = Field(..., example="BUY")
    notification_channels: List[NotificationChannelEnum] = Field(
        default=[NotificationChannelEnum.EMAIL]
    )
    email_address:         Optional[str]             = Field("luudungpkt922005@gmail.com", example="luudungpkt922005@gmail.com")
    webhook_url:           Optional[str]             = Field(None, example="https://hooks.example.com/abc")
    cooldown_seconds:      int                       = Field(default=300, ge=60, le=86400)
    is_active:             bool                      = Field(default=True)

    class Config:
        use_enum_values = True

    @validator("symbol")
    def symbol_uppercase(cls, v):
        return v.upper()

    @validator("email_address", always=True)
    def validate_email(cls, v, values):
        channels = values.get("notification_channels", [])
        normalized = [c.value if isinstance(c, NotificationChannelEnum) else str(c) for c in channels]
        if "email" in normalized:
            if not v or "@" not in v:
                raise ValueError("email_address is required and must be a valid email when 'email' channel is selected.")
        return v

    @validator("webhook_url", always=True)
    def validate_webhook(cls, v, values):
        channels = values.get("notification_channels", [])
        # `channels` may contain Enum members or raw values depending on Pydantic config.
        normalized = [c.value if isinstance(c, NotificationChannelEnum) else str(c) for c in channels]
        if "webhook" in normalized and not v:
            raise ValueError("webhook_url is required when 'webhook' channel is selected.")
        return v


class AlertRuleUpdate(BaseModel):
    """Partial update payload."""
    conditions:            Optional[List[AlertCondition]]       = None
    logic:                 Optional[LogicEnum]                  = None
    action:                Optional[ActionEnum]                 = None
    notification_channels: Optional[List[NotificationChannelEnum]] = None
    email_address:         Optional[str]                        = None
    webhook_url:           Optional[str]                        = None
    cooldown_seconds:      Optional[int]                        = Field(None, ge=60, le=86400)
    is_active:             Optional[bool]                       = None


class AlertRule(AlertRuleCreate):
    """Full alert rule with server-generated fields."""
    rule_id:           str       = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id:           str
    created_at:        datetime  = Field(default_factory=datetime.utcnow)
    updated_at:        datetime  = Field(default_factory=datetime.utcnow)
    last_triggered_at: Optional[datetime] = None
    trigger_count:     int       = 0

    class Config:
        use_enum_values = True


# ─────────────────────────────────────────────
# Alert Event
# ─────────────────────────────────────────────

class AlertEvent(BaseModel):
    alert_id:       str
    rule_id:        str
    user_id:        str
    symbol:         str
    timeframe:      str
    action:         str
    triggered_at:   datetime
    close_price:    Optional[float]
    rsi_14:         Optional[float]
    macd:           Optional[float]
    volume_ratio:   Optional[float]
    candle_pattern: Optional[str]
    message:        Optional[str]


# ─────────────────────────────────────────────
# API Responses
# ─────────────────────────────────────────────

class APIResponse(BaseModel):
    success:   bool
    message:   str
    data:      Optional[Any] = None


class PaginatedRules(BaseModel):
    total:  int
    page:   int
    limit:  int
    items:  List[AlertRule]
