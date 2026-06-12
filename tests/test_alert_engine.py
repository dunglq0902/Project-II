"""
tests/test_alert_engine.py
Unit tests for the alert rule engine, condition evaluator, and notifier models.
"""

import json
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alert_engine.evaluator.rule_engine import (
    evaluate_condition,
    evaluate_rule,
    evaluate_rules_for_row,
)
from alert_engine.api.models import (
    AlertCondition,
    AlertRuleCreate,
    ActionEnum,
    TimeframeEnum,
    LogicEnum,
    OperatorEnum,
    ConditionFieldEnum,
)


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def sample_row():
    return {
        "symbol":         "BTCUSDT",
        "timeframe":      "1h",
        "close":          37050.00,
        "rsi_14":         28.5,
        "macd":           -50.0,
        "macd_signal":    -45.0,
        "volume_ratio":   2.1,
        "candle_pattern": "HAMMER",
        "ma7":            36800.0,
        "ma25":           37200.0,
        "bb_upper":       38000.0,
        "bb_lower":       36000.0,
        "vwap":           37100.0,
    }


@pytest.fixture
def sample_rule():
    return {
        "rule_id":          "rule-001",
        "user_id":          "user-001",
        "symbol":           "BTCUSDT",
        "timeframe":        "1h",
        "logic":            "AND",
        "action":           "BUY",
        "is_active":        True,
        "cooldown_seconds": 300,
        "conditions": [
            {"field": "rsi_14",       "operator": "<",  "value": 30},
            {"field": "volume_ratio", "operator": ">",  "value": 1.5},
            {"field": "candle_pattern", "operator": "==", "value": "HAMMER"},
        ],
    }


# ─────────────────────────────────────────────
# Condition Evaluator Tests
# ─────────────────────────────────────────────

class TestConditionEvaluator:
    def test_greater_than_true(self, sample_row):
        cond = {"field": "volume_ratio", "operator": ">", "value": 1.5}
        passed, desc = evaluate_condition(sample_row, cond)
        assert passed is True
        assert "volume_ratio" in desc

    def test_greater_than_false(self, sample_row):
        cond = {"field": "volume_ratio", "operator": ">", "value": 5.0}
        passed, _ = evaluate_condition(sample_row, cond)
        assert passed is False

    def test_less_than_true(self, sample_row):
        cond = {"field": "rsi_14", "operator": "<", "value": 30}
        passed, _ = evaluate_condition(sample_row, cond)
        assert passed is True

    def test_equal_string(self, sample_row):
        cond = {"field": "candle_pattern", "operator": "==", "value": "HAMMER"}
        passed, _ = evaluate_condition(sample_row, cond)
        assert passed is True

    def test_equal_string_case_insensitive(self, sample_row):
        cond = {"field": "candle_pattern", "operator": "==", "value": "hammer"}
        passed, _ = evaluate_condition(sample_row, cond)
        assert passed is True

    def test_not_equal(self, sample_row):
        cond = {"field": "candle_pattern", "operator": "!=", "value": "DOJI"}
        passed, _ = evaluate_condition(sample_row, cond)
        assert passed is True

    def test_crosses_above_true(self):
        current = {"rsi_14": 31.0}
        prev    = {"rsi_14": 29.0}
        cond    = {"field": "rsi_14", "operator": "crosses_above", "value": 30}
        passed, _ = evaluate_condition(current, cond, prev)
        assert passed is True

    def test_crosses_above_false_already_above(self):
        current = {"rsi_14": 35.0}
        prev    = {"rsi_14": 32.0}
        cond    = {"field": "rsi_14", "operator": "crosses_above", "value": 30}
        passed, _ = evaluate_condition(current, cond, prev)
        assert passed is False

    def test_crosses_below_true(self):
        current = {"rsi_14": 29.0}
        prev    = {"rsi_14": 31.0}
        cond    = {"field": "rsi_14", "operator": "crosses_below", "value": 30}
        passed, _ = evaluate_condition(current, cond, prev)
        assert passed is True

    def test_none_value_returns_false(self):
        row  = {"rsi_14": None}
        cond = {"field": "rsi_14", "operator": "<", "value": 30}
        passed, _ = evaluate_condition(row, cond)
        assert passed is False

    def test_unknown_operator_returns_false(self, sample_row):
        cond = {"field": "rsi_14", "operator": "between", "value": 25}
        passed, _ = evaluate_condition(sample_row, cond)
        assert passed is False


# ─────────────────────────────────────────────
# Rule Evaluator Tests
# ─────────────────────────────────────────────

class TestRuleEvaluator:
    def test_and_logic_all_true(self, sample_rule, sample_row):
        passed, descs = evaluate_rule(sample_rule, sample_row)
        assert passed is True
        assert len(descs) == 3

    def test_and_logic_one_false(self, sample_rule, sample_row):
        row = {**sample_row, "rsi_14": 55.0}   # RSI > 30 → fails
        passed, _ = evaluate_rule(sample_rule, row)
        assert passed is False

    def test_or_logic_one_true_passes(self, sample_rule, sample_row):
        rule = {**sample_rule, "logic": "OR"}
        row  = {**sample_row, "rsi_14": 55.0, "candle_pattern": "DOJI"}
        # volume_ratio 2.1 > 1.5 → True, so OR passes
        passed, _ = evaluate_rule(rule, row)
        assert passed is True

    def test_or_logic_all_false(self, sample_rule, sample_row):
        rule = {**sample_rule, "logic": "OR"}
        row  = {
            **sample_row,
            "rsi_14":         55.0,
            "volume_ratio":   0.5,
            "candle_pattern": "DOJI",
        }
        passed, _ = evaluate_rule(rule, row)
        assert passed is False

    def test_empty_conditions_returns_false(self, sample_row):
        rule = {"rule_id": "x", "conditions": [], "logic": "AND"}
        passed, _ = evaluate_rule(rule, sample_row)
        assert passed is False


# ─────────────────────────────────────────────
# Batch Rule Evaluation Tests
# ─────────────────────────────────────────────

class TestBatchEvaluation:
    def test_only_matching_symbol_timeframe_evaluated(self, sample_rule, sample_row):
        rules = [
            sample_rule,
            {**sample_rule, "symbol": "ETHUSDT", "rule_id": "rule-002"},  # Different symbol
            {**sample_rule, "timeframe": "4h",   "rule_id": "rule-003"},  # Different timeframe
        ]
        triggered = evaluate_rules_for_row(rules, sample_row)
        assert len(triggered) == 1
        assert triggered[0]["rule_id"] == "rule-001"

    def test_inactive_rule_skipped(self, sample_rule, sample_row):
        rule = {**sample_rule, "is_active": False}
        triggered = evaluate_rules_for_row([rule], sample_row)
        assert len(triggered) == 0

    def test_conditions_met_populated(self, sample_rule, sample_row):
        triggered = evaluate_rules_for_row([sample_rule], sample_row)
        assert "conditions_met" in triggered[0]
        assert len(triggered[0]["conditions_met"]) == 3


# ─────────────────────────────────────────────
# Pydantic Model Validation Tests
# ─────────────────────────────────────────────

class TestAlertModels:
    def test_valid_rule_create(self):
        rule = AlertRuleCreate(
            symbol="btcusdt",    # should be uppercased by validator
            timeframe=TimeframeEnum.ONE_HOUR,
            conditions=[
                AlertCondition(
                    field=ConditionFieldEnum.RSI_14,
                    operator=OperatorEnum.LT,
                    value=30,
                )
            ],
            logic=LogicEnum.AND,
            action=ActionEnum.BUY,
        )
        assert rule.symbol == "BTCUSDT"
        assert len(rule.conditions) == 1
        assert rule.email_address == "luudungpkt922005@gmail.com"

    def test_invalid_candle_pattern_raises(self):
        with pytest.raises(Exception):
            AlertCondition(
                field=ConditionFieldEnum.CANDLE_PATTERN,
                operator=OperatorEnum.EQ,
                value="INVALID_PATTERN",
            )

    def test_webhook_required_when_channel_selected(self):
        with pytest.raises(Exception):
            AlertRuleCreate(
                symbol="BTCUSDT",
                timeframe=TimeframeEnum.ONE_HOUR,
                conditions=[
                    AlertCondition(
                        field=ConditionFieldEnum.RSI_14,
                        operator=OperatorEnum.LT,
                        value=30,
                    )
                ],
                action=ActionEnum.BUY,
                notification_channels=["webhook"],  # webhook_url missing → should raise
            )

    def test_email_required_when_channel_selected(self):
        with pytest.raises(Exception):
            AlertRuleCreate(
                symbol="BTCUSDT",
                timeframe=TimeframeEnum.ONE_HOUR,
                conditions=[
                    AlertCondition(
                        field=ConditionFieldEnum.RSI_14,
                        operator=OperatorEnum.LT,
                        value=30,
                    )
                ],
                action=ActionEnum.BUY,
                notification_channels=["email"],
                email_address="",  # Explicitly empty to raise error
            )

    def test_cooldown_range_validation(self):
        with pytest.raises(Exception):
            AlertRuleCreate(
                symbol="BTCUSDT",
                timeframe=TimeframeEnum.ONE_HOUR,
                conditions=[
                    AlertCondition(
                        field=ConditionFieldEnum.RSI_14,
                        operator=OperatorEnum.LT,
                        value=30,
                    )
                ],
                action=ActionEnum.BUY,
                cooldown_seconds=10,   # < 60 minimum → should raise
            )
