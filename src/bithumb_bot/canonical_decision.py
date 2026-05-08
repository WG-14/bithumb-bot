from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from typing import Any

from .research.hashing import sha256_prefixed


CANONICAL_DECISION_CONTRACT_VERSION = 1
STRATEGY_CONTRACT_VERSION = "sma_strategy_v1"
CANONICAL_DECISION_SCHEMA_FIELDS = (
    "decision_contract_version",
    "strategy_contract_version",
    "strategy_name",
    "profile_content_hash",
    "candidate_profile_hash",
    "dataset_content_hash",
    "db_data_fingerprint",
    "market",
    "interval",
    "signal_timestamp",
    "candle_ts",
    "through_ts_ms",
    "candle_basis",
    "decision_ts",
    "raw_signal",
    "final_signal",
    "side",
    "blocked",
    "block_reason",
    "blocked_filters",
    "prev_s",
    "prev_l",
    "curr_s",
    "curr_l",
    "feature_hash",
    "gap_ratio",
    "range_ratio",
    "expected_edge_ratio",
    "required_edge_ratio",
    "fee_authority_hash",
    "fee_model_hash",
    "slippage_model_hash",
    "order_rules_hash",
    "market_regime",
    "regime_decision",
    "regime_block_reason",
    "position_state_hash",
    "entry_allowed",
    "exit_allowed",
    "dust_state",
    "effective_flat",
    "normalized_exposure_active",
    "exit_rule",
    "exit_reason",
    "exit_evaluations_hash",
    "execution_timing_policy_hash",
    "replay_fingerprint_hash",
)
PROMOTION_REQUIRED_CANONICAL_FIELDS = (
    "decision_contract_version",
    "strategy_contract_version",
    "strategy_name",
    "profile_content_hash",
    "market",
    "interval",
    "candle_basis",
    "raw_signal",
    "final_signal",
    "side",
    "blocked",
    "fee_model_hash",
    "slippage_model_hash",
    "order_rules_hash",
    "position_state_hash",
    "entry_allowed",
    "exit_allowed",
    "dust_state",
    "effective_flat",
    "normalized_exposure_active",
    "exit_evaluations_hash",
    "execution_timing_policy_hash",
)
PROMOTION_REQUIRED_ONE_OF_CANONICAL_FIELDS = (("signal_timestamp", "candle_ts"),)
EMPTY_ORDER_RULES_HASH = sha256_prefixed({})
CANONICAL_FLAT_POSITION_STATE = {
    "comparison_state": "flat_no_dust_no_position",
    "entry_allowed": True,
    "exit_allowed": False,
    "dust_state": "flat",
    "effective_flat": True,
    "normalized_exposure_active": False,
}


@dataclass(frozen=True)
class CanonicalDecisionEvent:
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return dict(self.payload)


@dataclass(frozen=True)
class CanonicalDecisionValidation:
    canonical_schema_present: bool
    canonical_schema_complete: bool
    promotion_grade: bool
    legacy_shallow_decision: bool
    incomplete_canonical_decision: bool
    missing_fields: tuple[str, ...]
    reason_codes: tuple[str, ...]


def canonical_payload_hash(value: object) -> str:
    return sha256_prefixed(_stable_value(value))


def normalize_canonical_decision(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = {field: _canonical_field_value(field, payload.get(field)) for field in CANONICAL_DECISION_SCHEMA_FIELDS}
    normalized["decision_contract_version"] = int(
        payload.get("decision_contract_version") or CANONICAL_DECISION_CONTRACT_VERSION
    )
    normalized["strategy_contract_version"] = str(
        payload.get("strategy_contract_version") or STRATEGY_CONTRACT_VERSION
    )
    normalized["side"] = str(payload.get("side") or payload.get("final_signal") or "").strip().upper()
    normalized["raw_signal"] = str(payload.get("raw_signal") or "").strip().upper()
    normalized["final_signal"] = str(payload.get("final_signal") or normalized["side"]).strip().upper()
    normalized["blocked"] = bool(payload.get("blocked"))
    normalized["blocked_filters"] = tuple(str(item) for item in payload.get("blocked_filters") or ())
    return normalized


def is_canonical_decision(payload: dict[str, Any]) -> bool:
    return int(payload.get("decision_contract_version") or 0) >= CANONICAL_DECISION_CONTRACT_VERSION


def validate_canonical_decision_payload(
    payload: dict[str, Any],
    *,
    promotion_grade: bool = True,
) -> CanonicalDecisionValidation:
    schema_present = is_canonical_decision(payload)
    if not schema_present:
        return CanonicalDecisionValidation(
            canonical_schema_present=False,
            canonical_schema_complete=False,
            promotion_grade=False,
            legacy_shallow_decision=True,
            incomplete_canonical_decision=False,
            missing_fields=(),
            reason_codes=("canonical_decision_legacy_schema",),
        )
    normalized = normalize_canonical_decision(payload)
    missing: list[str] = []
    for field in PROMOTION_REQUIRED_CANONICAL_FIELDS:
        if _canonical_required_missing(normalized.get(field)):
            missing.append(field)
    for group in PROMOTION_REQUIRED_ONE_OF_CANONICAL_FIELDS:
        if all(_canonical_required_missing(normalized.get(field)) for field in group):
            missing.append("|".join(group))
    reason_codes: list[str] = []
    if missing:
        reason_codes.extend(["canonical_decision_required_field_missing", "canonical_decision_incomplete"])
    if str(normalized.get("order_rules_hash") or "").strip() == EMPTY_ORDER_RULES_HASH:
        missing.append("order_rules_hash")
        reason_codes.extend(
            [
                "canonical_decision_empty_order_rules_hash",
                "canonical_decision_incomplete",
            ]
        )
    complete = not missing
    is_promotion_grade = bool(complete)
    if promotion_grade and not is_promotion_grade:
        reason_codes.append("canonical_decision_not_promotion_grade")
    return CanonicalDecisionValidation(
        canonical_schema_present=True,
        canonical_schema_complete=complete,
        promotion_grade=is_promotion_grade,
        legacy_shallow_decision=False,
        incomplete_canonical_decision=not complete,
        missing_fields=tuple(sorted(set(missing))),
        reason_codes=tuple(sorted(set(reason_codes))),
    )


def runtime_decision_to_canonical_event(
    decision: Any,
    *,
    market: str,
    interval: str,
    profile_content_hash: str = "",
    dataset_content_hash: str = "",
    db_data_fingerprint: str = "",
    through_ts_ms: int | None = None,
    decision_ts: int | None = None,
    candle_basis: str = "runtime_closed_candle",
    execution_timing_policy_hash: str = "",
) -> CanonicalDecisionEvent:
    context = dict(getattr(decision, "context", {}) or {})
    final_signal = str(getattr(decision, "signal", context.get("final_signal", "HOLD")) or "HOLD").upper()
    raw_signal = str(context.get("raw_signal") or context.get("base_signal") or final_signal).upper()
    entry = context.get("entry") if isinstance(context.get("entry"), dict) else {}
    filters = context.get("filters") if isinstance(context.get("filters"), dict) else {}
    cost_edge = filters.get("cost_edge") if isinstance(filters.get("cost_edge"), dict) else {}
    exit_context = context.get("exit") if isinstance(context.get("exit"), dict) else {}
    position_gate = context.get("position_gate") if isinstance(context.get("position_gate"), dict) else {}
    order_rules = position_gate.get("order_rules") or context.get("order_rules") or {}
    market_regime = context.get("market_regime") if isinstance(context.get("market_regime"), dict) else {}
    fee_authority = context.get("fee_authority") if isinstance(context.get("fee_authority"), dict) else {}
    blocked_filters = tuple(str(item) for item in context.get("blocked_filters") or ())
    blocked = bool(final_signal == "HOLD" and raw_signal in {"BUY", "SELL"})
    block_reason = str(
        context.get("entry_block_reason")
        or entry.get("entry_reason")
        or context.get("regime_block_reason")
        or getattr(decision, "reason", "")
        or ""
    )
    comparison_position_state = _runtime_comparison_position_state(
        position_gate=position_gate,
        position_state=context.get("position_state") if isinstance(context.get("position_state"), dict) else {},
    )
    flat_comparison_state = comparison_position_state == CANONICAL_FLAT_POSITION_STATE
    payload = {
        "decision_contract_version": CANONICAL_DECISION_CONTRACT_VERSION,
        "strategy_contract_version": STRATEGY_CONTRACT_VERSION,
        "strategy_name": str(context.get("strategy") or ""),
        "profile_content_hash": profile_content_hash or str(context.get("approved_profile_hash") or ""),
        "candidate_profile_hash": str(context.get("candidate_profile_hash") or ""),
        "dataset_content_hash": dataset_content_hash or str(context.get("dataset_content_hash") or ""),
        "db_data_fingerprint": db_data_fingerprint,
        "market": str(market),
        "interval": str(interval),
        "signal_timestamp": str(context.get("ts") or ""),
        "candle_ts": int(context.get("ts") or 0),
        "through_ts_ms": through_ts_ms,
        "candle_basis": candle_basis,
        "decision_ts": decision_ts,
        "raw_signal": raw_signal,
        "final_signal": final_signal,
        "side": final_signal,
        "blocked": blocked,
        "block_reason": block_reason if blocked else "",
        "blocked_filters": blocked_filters,
        "prev_s": context.get("prev_s"),
        "prev_l": context.get("prev_l"),
        "curr_s": context.get("curr_s"),
        "curr_l": context.get("curr_l"),
        "feature_hash": canonical_payload_hash(context.get("features") or {}),
        "gap_ratio": context.get("gap_ratio"),
        "range_ratio": _range_ratio_from_filters(filters),
        "expected_edge_ratio": cost_edge.get("value"),
        "required_edge_ratio": cost_edge.get("threshold"),
        "fee_authority_hash": canonical_payload_hash(fee_authority),
        "fee_model_hash": canonical_payload_hash({"fee_authority": fee_authority}),
        "slippage_model_hash": canonical_payload_hash(context.get("position_lot_interpretation_costs") or {}),
        "order_rules_hash": canonical_payload_hash(order_rules),
        "market_regime": market_regime.get("composite_regime") or context.get("current_regime") or "",
        "regime_decision": context.get("regime_decision") or "",
        "regime_block_reason": context.get("regime_block_reason") or "",
        "position_state_hash": canonical_payload_hash(comparison_position_state),
        "entry_allowed": position_gate.get("entry_allowed"),
        "exit_allowed": position_gate.get("exit_allowed"),
        "dust_state": "flat"
        if flat_comparison_state
        else position_gate.get("dust_state") or context.get("dust_classification") or "",
        "effective_flat": position_gate.get("effective_flat") if "effective_flat" in position_gate else context.get("effective_flat"),
        "normalized_exposure_active": position_gate.get("normalized_exposure_active")
        if "normalized_exposure_active" in position_gate
        else context.get("normalized_exposure_active"),
        "exit_rule": exit_context.get("rule"),
        "exit_reason": exit_context.get("reason") or "",
        "exit_evaluations_hash": canonical_payload_hash(exit_context.get("evaluations") or ()),
        "execution_timing_policy_hash": execution_timing_policy_hash,
        "replay_fingerprint_hash": canonical_payload_hash(context.get("replay_fingerprint") or {}),
    }
    return CanonicalDecisionEvent(normalize_canonical_decision(payload))


def research_decision_to_canonical_event(
    decision: dict[str, Any],
    *,
    profile_content_hash: str = "",
    dataset_content_hash: str = "",
    execution_timing_policy_hash: str = "",
) -> CanonicalDecisionEvent:
    payload = dict(decision)
    payload.setdefault("decision_contract_version", CANONICAL_DECISION_CONTRACT_VERSION)
    payload.setdefault("strategy_contract_version", STRATEGY_CONTRACT_VERSION)
    payload["profile_content_hash"] = profile_content_hash or str(payload.get("profile_content_hash") or "")
    payload["dataset_content_hash"] = dataset_content_hash or str(payload.get("dataset_content_hash") or "")
    payload["execution_timing_policy_hash"] = execution_timing_policy_hash or str(
        payload.get("execution_timing_policy_hash") or ""
    )
    return CanonicalDecisionEvent(normalize_canonical_decision(payload))


def canonical_flat_position_state_hash() -> str:
    return canonical_payload_hash(CANONICAL_FLAT_POSITION_STATE)


def _runtime_comparison_position_state(
    *,
    position_gate: dict[str, Any],
    position_state: dict[str, Any],
) -> dict[str, Any]:
    if _is_flat_no_dust_position_gate(position_gate):
        return dict(CANONICAL_FLAT_POSITION_STATE)
    return {
        "comparison_state": "runtime_position_state_not_research_comparable",
        "unsupported_reason": _unsupported_position_reason(position_gate),
        "runtime_position_state": _stable_value(position_state),
    }


def _is_flat_no_dust_position_gate(position_gate: dict[str, Any]) -> bool:
    return (
        bool(position_gate.get("entry_allowed")) is True
        and bool(position_gate.get("exit_allowed")) is False
        and str(position_gate.get("dust_state") or "") in {"flat", "no_dust"}
        and bool(position_gate.get("effective_flat")) is True
        and bool(position_gate.get("normalized_exposure_active")) is False
        and int(position_gate.get("open_lot_count") or 0) == 0
        and int(position_gate.get("dust_tracking_lot_count") or 0) == 0
        and int(position_gate.get("sellable_executable_lot_count") or 0) == 0
        and bool(position_gate.get("has_any_position_residue")) is False
    )


def _unsupported_position_reason(position_gate: dict[str, Any]) -> str:
    if str(position_gate.get("dust_state") or "") not in {"", "flat"} or bool(position_gate.get("has_dust_only_remainder")):
        return "research_model_lacks_dust_state"
    if bool(position_gate.get("normalized_exposure_active")) or int(position_gate.get("sellable_executable_lot_count") or 0) > 0:
        return "research_model_lacks_lot_native_authority"
    if bool(position_gate.get("has_any_position_residue")):
        return "research_runtime_state_not_comparable"
    return "research_runtime_state_not_comparable"


def export_runtime_replay_decisions(
    *,
    conn: Any,
    strategy: Any,
    through_ts_list: list[int],
    market: str,
    interval: str,
    profile_content_hash: str = "",
    dataset_content_hash: str = "",
    db_data_fingerprint: str = "",
    candle_basis: str = "runtime_closed_candle",
    execution_timing_policy_hash: str = "",
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for through_ts_ms in through_ts_list:
        decision = strategy.decide(conn, through_ts_ms=int(through_ts_ms))
        if decision is None:
            continue
        events.append(
            runtime_decision_to_canonical_event(
                decision,
                market=market,
                interval=interval,
                profile_content_hash=profile_content_hash,
                dataset_content_hash=dataset_content_hash,
                db_data_fingerprint=db_data_fingerprint,
                through_ts_ms=int(through_ts_ms),
                candle_basis=candle_basis,
                execution_timing_policy_hash=execution_timing_policy_hash,
            ).as_dict()
        )
    return events


def export_research_decisions(
    decisions: list[dict[str, Any]],
    *,
    profile_content_hash: str = "",
    dataset_content_hash: str = "",
    execution_timing_policy_hash: str = "",
) -> list[dict[str, Any]]:
    return [
        research_decision_to_canonical_event(
            item,
            profile_content_hash=profile_content_hash,
            dataset_content_hash=dataset_content_hash,
            execution_timing_policy_hash=execution_timing_policy_hash,
        ).as_dict()
        for item in decisions
    ]


def _range_ratio_from_filters(filters: dict[str, Any]) -> object:
    volatility = filters.get("volatility") if isinstance(filters.get("volatility"), dict) else {}
    return volatility.get("value")


def _canonical_field_value(field: str, value: object) -> object:
    if field in {
        "decision_contract_version",
        "candle_ts",
        "through_ts_ms",
        "decision_ts",
    }:
        if value in (None, ""):
            return None
        return int(value)  # type: ignore[arg-type]
    if field in {"blocked", "entry_allowed", "exit_allowed", "effective_flat", "normalized_exposure_active"}:
        if value is None:
            return None
        return bool(value)
    if field in {"blocked_filters"}:
        return tuple(str(item) for item in (value or ()))  # type: ignore[union-attr]
    if field in {"prev_s", "prev_l", "curr_s", "curr_l", "gap_ratio", "range_ratio", "expected_edge_ratio", "required_edge_ratio"}:
        if value in (None, ""):
            return None
        return float(value)  # type: ignore[arg-type]
    return "" if value is None else str(value)


def _canonical_required_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, (list, tuple, dict)):
        return len(value) == 0
    return str(value).strip() == ""


def order_rules_snapshot_payload(resolution_or_rules: object, *, pair: str = "") -> dict[str, object]:
    rules = getattr(resolution_or_rules, "rules", resolution_or_rules)
    payload = asdict(rules) if hasattr(rules, "__dataclass_fields__") else dict(rules or {})  # type: ignore[arg-type]
    source = getattr(resolution_or_rules, "source", None)
    if isinstance(source, dict):
        payload["rule_source"] = {str(key): str(value) for key, value in sorted(source.items())}
    if pair:
        payload["pair"] = str(pair)
    return _stable_value(payload)  # type: ignore[return-value]


def _stable_value(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _stable_value(item) for key, item in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_stable_value(item) for item in value]
    return value
