"""
alert-engine/evaluator/rule_engine.py
Pure-Python rule evaluation engine.
Mirrors the Spark UDF logic for use in the Python notification service
and for unit testing without Spark.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("RuleEngine")


# ─────────────────────────────────────────────
# Condition Evaluator
# ─────────────────────────────────────────────

def evaluate_condition(
    row_data:  Dict[str, Any],
    condition: Dict[str, Any],
    prev_data: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    """
    Evaluate a single condition dict against a data row.

    Args:
        row_data:  Current candle/indicator data.
        condition: {field, operator, value}
        prev_data: Previous row for crosses_above / crosses_below.

    Returns:
        (passed: bool, description: str)
    """
    field    = condition.get("field", "")
    operator = condition.get("operator", "")
    value    = condition.get("value")

    current = row_data.get(field)

    if current is None:
        return False, f"{field} is None"

    try:
        if operator == ">":
            result = float(current) > float(value)
        elif operator == "<":
            result = float(current) < float(value)
        elif operator == ">=":
            result = float(current) >= float(value)
        elif operator == "<=":
            result = float(current) <= float(value)
        elif operator == "==":
            result = str(current).upper() == str(value).upper()
        elif operator == "!=":
            result = str(current).upper() != str(value).upper()
        elif operator == "crosses_above":
            prev = (prev_data or {}).get(field)
            if prev is None:
                return False, f"No previous data for {field}"
            result = float(prev) <= float(value) and float(current) > float(value)
        elif operator == "crosses_below":
            prev = (prev_data or {}).get(field)
            if prev is None:
                return False, f"No previous data for {field}"
            result = float(prev) >= float(value) and float(current) < float(value)
        else:
            return False, f"Unknown operator: {operator}"
    except (ValueError, TypeError) as exc:
        return False, f"Evaluation error: {exc}"

    desc = f"{field} {operator} {value} → {'✓' if result else '✗'} (current={current})"
    return result, desc


def evaluate_rule(
    rule:      Dict[str, Any],
    row_data:  Dict[str, Any],
    prev_data: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, List[str]]:
    """
    Evaluate all conditions of a rule.

    Returns:
        (all_passed: bool, condition_descriptions: List[str])
    """
    conditions = rule.get("conditions", [])
    logic      = rule.get("logic", "AND").upper()

    if not conditions:
        return False, ["No conditions defined"]

    results = []
    descs   = []

    for cond in conditions:
        passed, desc = evaluate_condition(row_data, cond, prev_data)
        results.append(passed)
        descs.append(desc)

    if logic == "AND":
        final = all(results)
    elif logic == "OR":
        final = any(results)
    else:
        final = False

    return final, descs


# ─────────────────────────────────────────────
# Batch Evaluation
# ─────────────────────────────────────────────

def evaluate_rules_for_row(
    rules:     List[Dict[str, Any]],
    row_data:  Dict[str, Any],
    prev_data: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Evaluate all active rules against a single data row.

    Returns list of triggered rule dicts (with conditions_met populated).
    """
    triggered = []
    for rule in rules:
        if not rule.get("is_active", True):
            continue
        if rule.get("symbol") != row_data.get("symbol"):
            continue
        if rule.get("timeframe") != row_data.get("timeframe"):
            continue

        passed, descs = evaluate_rule(rule, row_data, prev_data)
        if passed:
            triggered.append({
                **rule,
                "conditions_met": descs,
                "triggered_row":  row_data,
            })
            logger.info(
                "Rule triggered | rule_id=%s symbol=%s action=%s",
                rule.get("rule_id"), rule.get("symbol"), rule.get("action"),
            )

    return triggered
