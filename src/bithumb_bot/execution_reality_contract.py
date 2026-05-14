from __future__ import annotations

import hashlib
import json
from typing import Any


EXECUTION_REALITY_CONTRACT_SCHEMA_VERSION = 1
CONTRACT_HASH_FIELD = "execution_contract_hash"
EXECUTION_CONDITION_LINEAGE_FIELDS = frozenset({"calibration_artifact_hash"})

_TIMESTAMP_FIELDS = frozenset(
    {
        "generated_at",
        "created_at",
        "updated_at",
        "observed_at",
        "recorded_at",
        "collected_at",
    }
)

REALITY_ORDER = {
    "candle_close_optimistic": 0,
    "candle_next_open": 1,
    "top_of_book_after_decision": 2,
    "latency_adjusted_top_of_book": 3,
    "paper_immediate_top_of_book": 3,
    "paper_stress_top_of_book": 3,
}


def execution_contract_hash(contract: dict[str, Any]) -> str:
    return execution_condition_contract_hash(contract)


def execution_condition_contract_hash(contract: dict[str, Any]) -> str:
    payload = _canonical_contract_payload(contract)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def attach_execution_contract_hash(contract: dict[str, Any]) -> dict[str, Any]:
    payload = dict(contract)
    payload[CONTRACT_HASH_FIELD] = execution_contract_hash(payload)
    return payload


def contract_hash_matches(contract: dict[str, Any], expected_hash: object | None = None) -> bool:
    observed = str(expected_hash or contract.get(CONTRACT_HASH_FIELD) or "").strip()
    return bool(observed) and execution_condition_contract_hash(contract) == observed


def build_execution_reality_contract(
    *,
    fill_reference_policy: str,
    decision_guard_ms: int = 0,
    max_quote_wait_ms: int = 0,
    missing_quote_policy: str = "warn",
    min_execution_reality_level_for_promotion: str | None = None,
    allow_same_candle_close_fill: bool = False,
    quote_source: str | None = None,
    quote_age_limit_ms: int | None = None,
    top_of_book_required: bool = False,
    top_of_book_is_full_depth: bool = False,
    depth_required: bool = False,
    trade_tick_required: bool = False,
    queue_position_required: bool = False,
    intra_candle_path_available: bool = False,
    latency_model: dict[str, Any] | str | None = None,
    partial_fill_model: dict[str, Any] | str | None = None,
    order_failure_model: dict[str, Any] | str | None = None,
    fee_source: str | None = None,
    slippage_source: str | None = None,
    calibration_required: bool = False,
    calibration_artifact_hash: str | None = None,
    execution_reality_level: str | None = None,
    limitations: list[str] | tuple[str, ...] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    level = execution_reality_level or _level_for_fill_reference_policy(fill_reference_policy)
    payload: dict[str, Any] = {
        "schema_version": EXECUTION_REALITY_CONTRACT_SCHEMA_VERSION,
        "fill_reference_policy": str(fill_reference_policy),
        "decision_guard_ms": int(decision_guard_ms),
        "max_quote_wait_ms": int(max_quote_wait_ms),
        "missing_quote_policy": str(missing_quote_policy),
        "min_execution_reality_level_for_promotion": min_execution_reality_level_for_promotion,
        "allow_same_candle_close_fill": bool(allow_same_candle_close_fill),
        "quote_source": quote_source,
        "quote_age_limit_ms": quote_age_limit_ms,
        "top_of_book_required": bool(top_of_book_required),
        "top_of_book_is_full_depth": bool(top_of_book_is_full_depth),
        "depth_required": bool(depth_required),
        "trade_tick_required": bool(trade_tick_required),
        "queue_position_required": bool(queue_position_required),
        "intra_candle_path_available": bool(intra_candle_path_available),
        "latency_model": latency_model or "none",
        "partial_fill_model": partial_fill_model or "none",
        "order_failure_model": order_failure_model or "none",
        "fee_source": fee_source,
        "slippage_source": slippage_source,
        "calibration_required": bool(calibration_required),
        "calibration_artifact_hash": calibration_artifact_hash,
        "execution_reality_level": level,
        "limitations": sorted({str(item) for item in (limitations or default_execution_limitations())}),
    }
    if extra:
        payload.update(dict(extra))
    return attach_execution_contract_hash(payload)


def default_execution_limitations() -> tuple[str, ...]:
    return (
        "top_of_book_is_quote_evidence_not_liquidity_depth",
        "full_orderbook_depth_unavailable",
        "queue_position_unavailable",
        "trade_ticks_unavailable",
        "market_impact_model_unavailable",
        "intra_candle_path_reconstruction_unavailable",
    )


def unsupported_capability_reasons(contract: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if contract.get("top_of_book_is_full_depth") is True:
        reasons.append("top_of_book_cannot_satisfy_full_depth")
    if contract.get("depth_required") and not contract.get("depth_available", False):
        reasons.append("execution_depth_required_but_unavailable")
    if contract.get("trade_tick_required") and not contract.get("trade_ticks_available", False):
        reasons.append("execution_trade_ticks_required_but_unavailable")
    if contract.get("queue_position_required") and not contract.get("queue_position_available", False):
        reasons.append("execution_queue_position_required_but_unavailable")
    if contract.get("intra_candle_path_required") and not contract.get("intra_candle_path_available", False):
        reasons.append("execution_intra_candle_path_required_but_unavailable")
    return reasons


def execution_contract_mismatch_reasons(
    *,
    expected: dict[str, Any] | None,
    observed: dict[str, Any] | None,
) -> list[dict[str, object]]:
    if not isinstance(expected, dict):
        return [{"field": "execution_reality_contract", "reason": "expected_execution_contract_missing"}]
    if not isinstance(observed, dict):
        return [{"field": "execution_reality_contract", "reason": "observed_execution_contract_missing"}]
    mismatches: list[dict[str, object]] = []
    expected_hash = str(expected.get(CONTRACT_HASH_FIELD) or execution_condition_contract_hash(expected))
    observed_hash = str(observed.get(CONTRACT_HASH_FIELD) or execution_condition_contract_hash(observed))
    if expected_hash != observed_hash:
        mismatches.append(
            {
                "field": CONTRACT_HASH_FIELD,
                "expected": expected_hash,
                "actual": observed_hash,
                "reason": "execution_contract_hash_mismatch",
            }
        )
    for field in sorted(set(expected) | set(observed)):
        if field in _TIMESTAMP_FIELDS:
            continue
        if field == CONTRACT_HASH_FIELD:
            continue
        if field in EXECUTION_CONDITION_LINEAGE_FIELDS:
            continue
        if expected.get(field) != observed.get(field):
            mismatches.append(
                {
                    "field": f"execution_reality_contract.{field}",
                    "expected": expected.get(field),
                    "actual": observed.get(field),
                    "reason": "execution_contract_field_mismatch",
                }
            )
    return mismatches


def _canonical_contract_payload(contract: dict[str, Any]) -> dict[str, Any]:
    return _strip_runtime_only(
        {
            k: v
            for k, v in contract.items()
            if k != CONTRACT_HASH_FIELD and k not in EXECUTION_CONDITION_LINEAGE_FIELDS
        }
    )


def _strip_runtime_only(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _strip_runtime_only(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(key) not in _TIMESTAMP_FIELDS
        }
    if isinstance(value, list):
        return [_strip_runtime_only(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_runtime_only(item) for item in value)
    return value


def _level_for_fill_reference_policy(fill_reference_policy: str) -> str:
    return {
        "candle_close_legacy": "candle_close_optimistic",
        "next_candle_open": "candle_next_open",
        "first_orderbook_after_decision": "top_of_book_after_decision",
        "latency_adjusted_orderbook": "latency_adjusted_top_of_book",
    }.get(str(fill_reference_policy), "unknown")
