from __future__ import annotations

import hashlib
import json
from typing import Any


EXECUTION_REALITY_CONTRACT_SCHEMA_VERSION = 1
EXECUTION_CAPABILITY_CONTRACT_SCHEMA_VERSION = 1
CONTRACT_HASH_FIELD = "execution_contract_hash"
CAPABILITY_CONTRACT_HASH_FIELD = "execution_capability_contract_hash"
EXECUTION_CONDITION_LINEAGE_FIELDS = frozenset({"calibration_artifact_hash"})
EXECUTION_OBSERVED_EVIDENCE_FIELDS = frozenset(
    {
        "quote_evidence_available",
        "depth_available",
        "depth_evidence_available",
        "l2_depth_rows_available",
        "l2_depth_complete_snapshots_available",
        "trade_ticks_available",
        "queue_position_available",
        "market_impact_model_available",
        "intra_candle_path_available",
    }
)
CAPABILITY_OBSERVED_AVAILABILITY_FIELDS = frozenset({"top_of_book"})
L2_DEPTH_SNAPSHOT_MISSING_REASON = "execution_l2_depth_snapshot_required_but_unavailable"

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
    "l2_depth_walk_no_queue": 4,
}

SUPPORTED_EVIDENCE_TIERS = frozenset(
    {
        "candle_close_optimistic",
        "candle_next_open",
        "top_of_book_after_decision",
        "latency_adjusted_top_of_book",
        "l2_depth_walk_no_queue",
    }
)

RESERVED_FUTURE_EVIDENCE_TIERS = frozenset(
    {
        "depth_replay_l2",
        "tick_replay_l2",
        "queue_position_simulated",
        "queue_position_calibrated",
        "impact_model_calibrated",
    }
)


PRODUCTION_EXECUTION_TIMING_REQUIRED_FIELDS = (
    "fill_reference_policy",
    "allow_same_candle_close_fill",
    "min_execution_reality_level_for_promotion",
)

_MIN_REALITY_LEVEL_BY_FILL_POLICY = {
    "next_candle_open": "candle_next_open",
    "first_orderbook_after_decision": "top_of_book_after_decision",
    "latency_adjusted_orderbook": "latency_adjusted_top_of_book",
}


def evaluate_execution_reality_policy(
    *,
    production_bound: bool,
    execution_timing: Any,
    execution_timing_declared: bool,
    execution_timing_declared_fields: set[str] | frozenset[str] | None = None,
    dataset_top_of_book: Any | None = None,
    context: str = "manifest",
) -> dict[str, Any]:
    """Evaluate execution timing invariants at a production trust boundary."""
    declared_fields = set(execution_timing_declared_fields or set())
    observed_fields = {
        field: _field_value(execution_timing, field)
        for field in (
            "fill_reference_policy",
            "missing_quote_policy",
            "allow_same_candle_close_fill",
            "min_execution_reality_level_for_promotion",
            "source",
        )
    }
    result: dict[str, Any] = {
        "status": "PASS",
        "context": context,
        "production_bound": bool(production_bound),
        "reasons": [],
        "required_fields": list(PRODUCTION_EXECUTION_TIMING_REQUIRED_FIELDS),
        "declared_fields": sorted(declared_fields),
        "observed_fields": observed_fields,
    }
    if not production_bound:
        return result

    reasons: list[str] = []
    missing_required_fields = [
        field
        for field in PRODUCTION_EXECUTION_TIMING_REQUIRED_FIELDS
        if field not in declared_fields
    ]
    if not execution_timing_declared or missing_required_fields:
        reasons.append("production_execution_timing_required")
    if _field_value(execution_timing, "source") == "legacy_default":
        reasons.append("production_legacy_execution_timing_not_promotable")

    fill_policy = str(_field_value(execution_timing, "fill_reference_policy") or "")
    min_level = _field_value(execution_timing, "min_execution_reality_level_for_promotion")
    if fill_policy == "candle_close_legacy":
        reasons.append("production_execution_reference_price_candle_close_not_promotable")
    if min_level is None:
        reasons.append("production_min_execution_reality_level_required")
    elif min_level == "candle_close_optimistic":
        reasons.append("production_execution_reality_level_below_required")
    else:
        required_min_level = _MIN_REALITY_LEVEL_BY_FILL_POLICY.get(fill_policy)
        if required_min_level is not None and REALITY_ORDER.get(str(min_level), -1) < REALITY_ORDER[required_min_level]:
            reasons.append("production_execution_reality_level_below_policy_reference")
    if bool(_field_value(execution_timing, "allow_same_candle_close_fill")):
        reasons.append("production_same_candle_close_fill_not_allowed")

    if fill_policy in {"first_orderbook_after_decision", "latency_adjusted_orderbook"}:
        top = dataset_top_of_book
        if top is None or not bool(_field_value(top, "required")):
            reasons.append("production_top_of_book_required")
        if top is not None:
            if _field_value(top, "missing_policy") != "fail":
                reasons.append("production_missing_quote_policy_must_fail")
            if float(_field_value(top, "min_coverage_pct") or 0.0) < 100.0:
                reasons.append("production_top_of_book_min_coverage_must_be_100")
        if _field_value(execution_timing, "missing_quote_policy") != "fail":
            reasons.append("production_missing_quote_policy_must_fail")

    reasons = sorted(set(reasons))
    result["reasons"] = reasons
    if reasons:
        result["status"] = "FAIL"
    return result


def execution_contract_hash(contract: dict[str, Any]) -> str:
    return execution_condition_contract_hash(contract)


def execution_condition_contract_hash(contract: dict[str, Any]) -> str:
    payload = _canonical_contract_payload(contract)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def execution_capability_contract_hash(contract: dict[str, Any]) -> str:
    payload = _canonical_capability_contract_payload(contract)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def attach_execution_capability_contract_hash(contract: dict[str, Any]) -> dict[str, Any]:
    payload = dict(contract)
    payload[CAPABILITY_CONTRACT_HASH_FIELD] = execution_capability_contract_hash(payload)
    return payload


def capability_contract_hash_matches(contract: dict[str, Any], expected_hash: object | None = None) -> bool:
    observed = str(expected_hash or contract.get(CAPABILITY_CONTRACT_HASH_FIELD) or "").strip()
    return bool(observed) and execution_capability_contract_hash(contract) == observed


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
    top_of_book_available: bool | None = None,
    top_of_book_is_full_depth: bool = False,
    depth_required: bool = False,
    trade_tick_required: bool = False,
    queue_position_required: bool = False,
    market_impact_required: bool = False,
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
        "market_impact_required": bool(market_impact_required),
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
    capability_top_of_book_available = (
        bool(top_of_book_available)
        if top_of_book_available is not None
        else bool(payload.get("quote_evidence_available", False))
    )
    capability_contract = build_execution_capability_contract(
        fill_reference_policy=str(fill_reference_policy),
        top_of_book_required=bool(top_of_book_required),
        top_of_book_available=capability_top_of_book_available,
        top_of_book_is_full_depth=bool(top_of_book_is_full_depth),
        l2_depth_snapshot_required=bool(depth_required),
        full_orderbook_depth_required=False,
        trade_ticks_required=bool(trade_tick_required),
        queue_position_required=bool(queue_position_required),
        market_impact_model_required=bool(market_impact_required),
        intra_candle_path_required=bool(payload.get("intra_candle_path_required", False)),
        l2_depth_snapshot_available=bool(payload.get("l2_depth_snapshot_available", payload.get("depth_available", False))),
        full_orderbook_depth_available=bool(payload.get("full_orderbook_depth_available", False)),
        trade_ticks_available=bool(payload.get("trade_ticks_available", False)),
        queue_position_available=bool(payload.get("queue_position_available", False)),
        market_impact_model_available=bool(payload.get("market_impact_model_available", False)),
        intra_candle_path_available=bool(payload.get("intra_candle_path_available", False)),
        evidence_tier=str(payload.get("execution_reality_level") or level),
        limitations=list(payload.get("limitations") or []),
    )
    payload["execution_capability_contract"] = capability_contract
    payload[CAPABILITY_CONTRACT_HASH_FIELD] = capability_contract[CAPABILITY_CONTRACT_HASH_FIELD]
    return attach_execution_contract_hash(payload)


def build_execution_capability_contract(
    *,
    fill_reference_policy: str,
    top_of_book_required: bool = False,
    top_of_book_available: bool = False,
    top_of_book_is_full_depth: bool = False,
    l2_depth_snapshot_required: bool = False,
    full_orderbook_depth_required: bool = False,
    trade_ticks_required: bool = False,
    queue_position_required: bool = False,
    market_impact_model_required: bool = False,
    intra_candle_path_required: bool = False,
    l2_depth_snapshot_available: bool = False,
    full_orderbook_depth_available: bool = False,
    trade_ticks_available: bool = False,
    queue_position_available: bool = False,
    market_impact_model_available: bool = False,
    intra_candle_path_available: bool = False,
    evidence_tier: str | None = None,
    limitations: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    tier = str(evidence_tier or _level_for_fill_reference_policy(fill_reference_policy))
    top_required = bool(top_of_book_required) or str(fill_reference_policy) in {
        "first_orderbook_after_decision",
        "latency_adjusted_orderbook",
        "paper_top_of_book",
    }
    required = {
        "candle_ohlcv": True,
        "top_of_book": top_required,
        "l2_depth_snapshot": bool(l2_depth_snapshot_required),
        "full_orderbook_depth": bool(full_orderbook_depth_required),
        "trade_ticks": bool(trade_ticks_required),
        "queue_position": bool(queue_position_required),
        "market_impact_model": bool(market_impact_model_required),
        "intra_candle_path_reconstruction": bool(intra_candle_path_required),
    }
    if tier == "l2_depth_walk_no_queue":
        required["l2_depth_snapshot"] = True
    available = {
        "candle_ohlcv": True,
        "top_of_book": bool(top_of_book_available),
        "top_of_book_is_full_depth": bool(top_of_book_is_full_depth),
        "l2_depth_snapshot": bool(l2_depth_snapshot_available),
        "full_orderbook_depth": bool(full_orderbook_depth_available),
        "trade_ticks": bool(trade_ticks_available),
        "queue_position": bool(queue_position_available),
        "market_impact_model": bool(market_impact_model_available),
        "intra_candle_path_reconstruction": bool(intra_candle_path_available),
    }
    unavailable = [
        name
        for name, is_required in required.items()
        if is_required and not bool(available.get(name, False))
    ]
    capability_limitations = sorted(
        {
            str(item)
            for item in (
                limitations
                or (
                    "top_of_book_is_quote_evidence_not_liquidity_depth",
                    "full_orderbook_depth_unavailable",
                    "queue_position_unavailable",
                    "trade_ticks_unavailable",
                    "market_impact_model_unavailable",
                    "intra_candle_path_reconstruction_unavailable",
                )
            )
        }
    )
    if tier in RESERVED_FUTURE_EVIDENCE_TIERS:
        capability_limitations.append(f"evidence_tier_reserved_not_implemented:{tier}")
    payload = {
        "schema_version": EXECUTION_CAPABILITY_CONTRACT_SCHEMA_VERSION,
        "strategy_required_capabilities": required,
        "available_capabilities": available,
        "evidence_tier": tier,
        "supported_evidence_tiers": sorted(SUPPORTED_EVIDENCE_TIERS),
        "reserved_future_evidence_tiers": sorted(RESERVED_FUTURE_EVIDENCE_TIERS),
        "promotion_rule": "fail_closed_if_required_capability_unavailable",
        "unavailable_required_capabilities": sorted(unavailable),
        "limitations": sorted(set(capability_limitations)),
    }
    return attach_execution_capability_contract_hash(payload)


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
        reasons.append(L2_DEPTH_SNAPSHOT_MISSING_REASON)
    if contract.get("trade_tick_required") and not contract.get("trade_ticks_available", False):
        reasons.append("execution_trade_ticks_required_but_unavailable")
    if contract.get("queue_position_required") and not contract.get("queue_position_available", False):
        reasons.append("execution_queue_position_required_but_unavailable")
    if contract.get("market_impact_required") and not contract.get("market_impact_model_available", False):
        reasons.append("execution_market_impact_required_but_unavailable")
    if contract.get("intra_candle_path_required") and not contract.get("intra_candle_path_available", False):
        reasons.append("execution_intra_candle_path_required_but_unavailable")
    capability = contract.get("execution_capability_contract")
    if isinstance(capability, dict):
        reasons.extend(validate_execution_capability_contract(capability))
        for name in capability.get("unavailable_required_capabilities") or []:
            if name == "top_of_book":
                reasons.append("execution_top_of_book_required_but_unavailable")
            elif name == "l2_depth_snapshot":
                reasons.append(L2_DEPTH_SNAPSHOT_MISSING_REASON)
            elif name == "full_orderbook_depth":
                reasons.append("execution_depth_required_but_unavailable")
            elif name == "trade_ticks":
                reasons.append("execution_trade_ticks_required_but_unavailable")
            elif name == "queue_position":
                reasons.append("execution_queue_position_required_but_unavailable")
            elif name == "market_impact_model":
                reasons.append("execution_market_impact_required_but_unavailable")
            elif name == "intra_candle_path_reconstruction":
                reasons.append("execution_intra_candle_path_required_but_unavailable")
        if capability.get("unavailable_required_capabilities"):
            reasons.append("execution_capability_required_unavailable")
    return sorted(set(reasons))


def validate_execution_capability_contract(contract: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    tier = str(contract.get("evidence_tier") or "").strip()
    if tier in RESERVED_FUTURE_EVIDENCE_TIERS:
        reasons.append("execution_evidence_tier_reserved_not_implemented")
    elif tier and tier not in SUPPORTED_EVIDENCE_TIERS:
        reasons.append("execution_evidence_tier_unsupported")
    elif not tier:
        reasons.append("execution_evidence_tier_unsupported")
    required = contract.get("strategy_required_capabilities")
    available = contract.get("available_capabilities")
    if isinstance(required, dict) and isinstance(available, dict):
        recomputed_unavailable = sorted(
            str(name)
            for name, is_required in required.items()
            if bool(is_required) and not bool(available.get(name, False))
        )
        declared_unavailable = sorted(str(item) for item in contract.get("unavailable_required_capabilities") or [])
        if recomputed_unavailable:
            reasons.append("execution_capability_required_unavailable")
        if recomputed_unavailable != declared_unavailable:
            reasons.append("execution_capability_unavailable_required_capabilities_mismatch")
    return sorted(set(reasons))


def execution_capability_contract_mismatch_reasons(
    *,
    expected: dict[str, Any] | None,
    observed: dict[str, Any] | None,
    include_hash: bool = True,
) -> list[dict[str, object]]:
    if not isinstance(expected, dict):
        return [{"field": "execution_capability_contract", "reason": "expected_execution_capability_contract_missing"}]
    if not isinstance(observed, dict):
        return [{"field": "execution_capability_contract", "reason": "observed_execution_capability_contract_missing"}]
    mismatches: list[dict[str, object]] = []
    expected_hash = str(expected.get(CAPABILITY_CONTRACT_HASH_FIELD) or execution_capability_contract_hash(expected))
    observed_hash = str(observed.get(CAPABILITY_CONTRACT_HASH_FIELD) or execution_capability_contract_hash(observed))
    if include_hash and expected_hash != observed_hash:
        mismatches.append(
            {
                "field": CAPABILITY_CONTRACT_HASH_FIELD,
                "expected": expected_hash,
                "actual": observed_hash,
                "reason": "execution_capability_contract_hash_mismatch",
            }
        )
    for field in sorted(set(expected) | set(observed)):
        if field in _TIMESTAMP_FIELDS or field == CAPABILITY_CONTRACT_HASH_FIELD:
            continue
        if field == "available_capabilities":
            availability_mismatches = _availability_capability_mismatches(
                expected.get(field),
                observed.get(field),
            )
            mismatches.extend(availability_mismatches)
            continue
        if field == "unavailable_required_capabilities":
            expected_items = _semantic_unavailable_required_capabilities(expected.get(field))
            observed_items = _semantic_unavailable_required_capabilities(observed.get(field))
            if expected_items != observed_items:
                mismatches.append(
                    {
                        "field": "execution_capability_contract.unavailable_required_capabilities",
                        "expected": sorted(expected_items),
                        "actual": sorted(observed_items),
                        "reason": "execution_capability_contract_field_mismatch",
                    }
                )
            continue
        if expected.get(field) != observed.get(field):
            mismatches.append(
                {
                    "field": f"execution_capability_contract.{field}",
                    "expected": expected.get(field),
                    "actual": observed.get(field),
                    "reason": "execution_capability_contract_field_mismatch",
                }
            )
    return mismatches


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
        if field in EXECUTION_OBSERVED_EVIDENCE_FIELDS:
            continue
        if field == "execution_capability_contract":
            mismatches.extend(
                execution_capability_contract_mismatch_reasons(
                    expected=expected.get(field) if isinstance(expected.get(field), dict) else None,
                    observed=observed.get(field) if isinstance(observed.get(field), dict) else None,
                    include_hash=False,
                )
            )
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


def _semantic_unavailable_required_capabilities(value: object) -> set[str]:
    items = value if isinstance(value, (list, tuple, set)) else []
    return {
        str(item)
        for item in items
        if str(item) not in CAPABILITY_OBSERVED_AVAILABILITY_FIELDS
    }


def _canonical_contract_payload(contract: dict[str, Any]) -> dict[str, Any]:
    payload = {
        k: v
        for k, v in contract.items()
        if k != CONTRACT_HASH_FIELD
        and k not in EXECUTION_CONDITION_LINEAGE_FIELDS
        and k not in EXECUTION_OBSERVED_EVIDENCE_FIELDS
    }
    capability = payload.get("execution_capability_contract")
    if isinstance(capability, dict):
        payload["execution_capability_contract"] = _canonical_capability_contract_payload(capability)
    return _strip_runtime_only(payload)


def _canonical_capability_contract_payload(contract: dict[str, Any]) -> dict[str, Any]:
    payload = {
        k: v
        for k, v in contract.items()
        if k != CAPABILITY_CONTRACT_HASH_FIELD
    }
    available = payload.get("available_capabilities")
    if isinstance(available, dict):
        payload["available_capabilities"] = {
            key: value
            for key, value in available.items()
            if key not in CAPABILITY_OBSERVED_AVAILABILITY_FIELDS
        }
    unavailable = payload.get("unavailable_required_capabilities")
    if isinstance(unavailable, list):
        payload["unavailable_required_capabilities"] = [
            item
            for item in unavailable
            if str(item) not in CAPABILITY_OBSERVED_AVAILABILITY_FIELDS
        ]
    return _strip_runtime_only(payload)


def _availability_capability_mismatches(
    expected: object,
    observed: object,
) -> list[dict[str, object]]:
    if not isinstance(expected, dict) or not isinstance(observed, dict):
        if expected == observed:
            return []
        return [
            {
                "field": "execution_capability_contract.available_capabilities",
                "expected": expected,
                "actual": observed,
                "reason": "execution_capability_contract_field_mismatch",
            }
        ]
    mismatches: list[dict[str, object]] = []
    for key in sorted(set(expected) | set(observed)):
        if key in CAPABILITY_OBSERVED_AVAILABILITY_FIELDS:
            continue
        if expected.get(key) != observed.get(key):
            mismatches.append(
                {
                    "field": f"execution_capability_contract.available_capabilities.{key}",
                    "expected": expected.get(key),
                    "actual": observed.get(key),
                    "reason": "execution_capability_contract_field_mismatch",
                }
            )
    return mismatches


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


def _field_value(value: Any, field: str) -> Any:
    if isinstance(value, dict):
        return value.get(field)
    return getattr(value, field, None)


def _level_for_fill_reference_policy(fill_reference_policy: str) -> str:
    return {
        "candle_close_legacy": "candle_close_optimistic",
        "next_candle_open": "candle_next_open",
        "first_orderbook_after_decision": "top_of_book_after_decision",
        "latency_adjusted_orderbook": "latency_adjusted_top_of_book",
    }.get(str(fill_reference_policy), "unknown")
