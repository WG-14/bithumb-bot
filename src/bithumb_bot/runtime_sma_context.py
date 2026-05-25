from __future__ import annotations

from typing import Any

from .broker.order_rules import get_effective_order_rules
from .config import settings
from .decision_contract import apply_decision_contract
from .fee_authority import (
    FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON,
    FeeAuthoritySnapshot,
    build_fee_authority_snapshot,
)
from .strategy.base import PositionContext, StrategyDecision


def sma(values: list[float], n: int, end: int) -> float:
    return sum(values[end - n : end]) / n


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def resolve_strategy_fee_authority(
    *,
    pair: str,
    config_fallback_fee_rate: float,
) -> FeeAuthoritySnapshot:
    return build_fee_authority_snapshot(
        get_effective_order_rules(pair),
        config_fallback_fee_rate=float(config_fallback_fee_rate),
    )


def fee_authority_context(fee_authority: FeeAuthoritySnapshot) -> dict[str, object]:
    return fee_authority.as_dict()


def live_armed_entry_fee_authority_blocks(fee_authority: FeeAuthoritySnapshot) -> bool:
    # Runtime live safety policy: keep this tied to live settings, not replay/sweep config.
    return bool(
        settings.MODE == "live"
        and not bool(settings.LIVE_DRY_RUN)
        and bool(settings.LIVE_REAL_ORDER_ARMED)
        and not fee_authority.live_entry_allowed()
    )


def build_entry_intent_context(
    *,
    pair: str,
    buy_fraction: float,
    max_order_krw: float,
) -> dict[str, Any]:
    return {
        "pair": str(pair),
        "intent": "enter_open_exposure",
        "budget_model": "cash_fraction_capped_by_max_order_krw",
        "budget_fraction_of_cash": float(buy_fraction),
        "max_budget_krw": float(max_order_krw),
        "requires_execution_sizing": True,
    }


def build_entry_decision_context(
    *,
    pair: str,
    base_signal: str,
    base_reason: str,
    entry_signal: str,
    entry_reason: str,
    buy_fraction: float,
    max_order_krw: float,
) -> dict[str, Any]:
    return {
        "base_signal": base_signal,
        "base_reason": base_reason,
        "entry_signal": entry_signal,
        "entry_reason": entry_reason,
        "allowed": entry_signal == "BUY",
        "intent": build_entry_intent_context(
            pair=pair,
            buy_fraction=buy_fraction,
            max_order_krw=max_order_krw,
        ),
    }


def build_exit_decision_context(
    *,
    exposure: object,
    triggered: bool,
    reason: str,
    rule: str | None,
    evaluations: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "allowed": bool(exposure.exit_allowed),
        "policy": "full" if triggered else "none",
        "triggered": bool(triggered),
        "rule": rule,
        "reason": str(reason),
        "terminal_state": str(exposure.terminal_state),
        "evaluations": evaluations,
    }


def build_position_state_context(position_state: object) -> dict[str, Any]:
    payload = position_state.as_dict()
    return {
        "raw_holdings": payload["raw_holdings"],
        "normalized_exposure": payload["normalized_exposure"],
        "operator_diagnostics": payload["operator_diagnostics"],
        "state_interpretation": payload["state_interpretation"],
        "raw_qty_open": payload["raw_qty_open"],
        "raw_total_asset_qty": payload["raw_total_asset_qty"],
        "effective_flat": payload["effective_flat"],
        "effective_flat_due_to_harmless_dust": payload["effective_flat_due_to_harmless_dust"],
    }


def build_position_gate_context(
    exposure: object,
    *,
    order_rules: dict[str, object] | None = None,
) -> dict[str, Any]:
    return {
        "raw_qty_open": float(exposure.raw_qty_open),
        "raw_total_asset_qty": float(exposure.raw_total_asset_qty),
        "open_exposure_qty": float(exposure.open_exposure_qty),
        "dust_tracking_qty": float(exposure.dust_tracking_qty),
        "open_lot_count": int(exposure.open_lot_count),
        "dust_tracking_lot_count": int(exposure.dust_tracking_lot_count),
        "reserved_exit_lot_count": int(exposure.reserved_exit_lot_count),
        "sellable_executable_lot_count": int(exposure.sellable_executable_lot_count),
        "reserved_exit_qty": float(exposure.reserved_exit_qty),
        "sellable_executable_qty": float(exposure.sellable_executable_qty),
        "dust_classification": str(exposure.dust_classification),
        "dust_state": str(exposure.dust_state),
        "effective_flat": bool(exposure.effective_flat),
        "effective_flat_due_to_harmless_dust": bool(exposure.harmless_dust_effective_flat),
        "entry_allowed": bool(exposure.entry_allowed),
        "entry_block_reason": str(exposure.entry_block_reason),
        "exit_allowed": bool(exposure.exit_allowed),
        "exit_block_reason": str(exposure.exit_block_reason),
        "terminal_state": str(exposure.terminal_state),
        "normalized_exposure_active": bool(exposure.normalized_exposure_active),
        "normalized_exposure_qty": float(exposure.normalized_exposure_qty),
        "has_executable_exposure": bool(exposure.has_executable_exposure),
        "has_any_position_residue": bool(exposure.has_any_position_residue),
        "has_non_executable_residue": bool(exposure.has_non_executable_residue),
        "has_dust_only_remainder": bool(exposure.has_dust_only_remainder),
        "dust_new_orders_allowed": bool(exposure.dust_operator_view.new_orders_allowed),
        "dust_resume_allowed": bool(exposure.dust_operator_view.resume_allowed),
        "dust_treat_as_flat": bool(exposure.dust_operator_view.treat_as_flat),
        "order_rules": dict(order_rules or {}),
    }


def legacy_strategy_decision_from_sma_final_decision(
    *,
    decision: object,
    base_context: dict[str, Any],
    position: PositionContext,
    exposure: object,
    position_state: object,
) -> StrategyDecision:
    context = dict(base_context)
    context["position"] = position.as_dict()
    context["position_gate"] = build_position_gate_context(
        position_state.normalized_exposure,
        order_rules=(
            context.get("order_rules") if isinstance(context.get("order_rules"), dict) else {}
        ),
    )
    context["position_state"] = build_position_state_context(position_state)
    context["raw_signal"] = decision.raw_signal
    context["raw_reason"] = decision.raw_reason
    context["entry_signal"] = decision.entry_signal
    context["entry_reason"] = decision.entry_reason
    context["exit_signal"] = decision.exit_signal
    context["exit_reason_raw"] = decision.exit_reason
    context["final_signal"] = decision.final_signal
    context["entry_blocked"] = bool(decision.entry_blocked)
    context["entry_block_reason"] = decision.entry_block_reason
    context["exit"] = build_exit_decision_context(
        exposure=exposure,
        triggered=bool(decision.exit_rule and decision.final_signal == "SELL"),
        reason=decision.exit_reason,
        rule=decision.exit_rule,
        evaluations=[dict(item) for item in decision.exit_evaluations],
    )
    context["exit_evaluations"] = [dict(item) for item in decision.exit_evaluations]
    context["exit_rule"] = decision.exit_rule
    context["exit_reason"] = decision.exit_reason
    context["protective_exit_overrode_entry"] = bool(decision.protective_exit_overrode_entry)
    context["exit_filter_suppression_prevented"] = bool(decision.exit_filter_suppression_prevented)
    context["execution_intent_v2"] = (
        dict(decision.execution_intent) if decision.execution_intent is not None else None
    )
    context["policy_contract_hash"] = decision.policy_contract_hash
    context["policy_input_hash"] = decision.policy_input_hash
    context["policy_decision_hash"] = decision.policy_decision_hash
    context["pure_policy_hash"] = decision.policy_hash
    context["pure_policy_trace"] = decision.as_trace()
    normalized_state = context["position_state"]["normalized_exposure"]
    state_interpretation = context["position_state"]["state_interpretation"]
    context["dust_classification"] = str(normalized_state["dust_classification"])
    context["entry_allowed"] = bool(normalized_state["entry_allowed"])
    context["effective_flat"] = bool(normalized_state["effective_flat"])
    context["raw_qty_open"] = float(normalized_state["raw_qty_open"])
    context["raw_total_asset_qty"] = float(normalized_state["raw_total_asset_qty"])
    context["normalized_exposure_active"] = bool(normalized_state["normalized_exposure_active"])
    context["has_executable_exposure"] = bool(normalized_state.get("has_executable_exposure", False))
    context["has_any_position_residue"] = bool(normalized_state.get("has_any_position_residue", False))
    context["has_non_executable_residue"] = bool(normalized_state.get("has_non_executable_residue", False))
    context["has_dust_only_remainder"] = bool(normalized_state.get("has_dust_only_remainder", False))
    context["exit_allowed"] = bool(normalized_state["exit_allowed"])
    context["exit_block_reason"] = str(normalized_state["exit_block_reason"])
    context["terminal_state"] = str(normalized_state["terminal_state"])
    context["state_outcome"] = str(state_interpretation["operator_outcome"])
    context["exit_submit_expected"] = bool(state_interpretation["exit_submit_expected"])
    return StrategyDecision(
        signal=decision.final_signal,
        reason=decision.final_reason,
        context=apply_decision_contract(context),
    )


__all__ = [
    "FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON",
    "build_entry_decision_context",
    "build_entry_intent_context",
    "build_exit_decision_context",
    "build_position_gate_context",
    "build_position_state_context",
    "fee_authority_context",
    "legacy_strategy_decision_from_sma_final_decision",
    "live_armed_entry_fee_authority_blocks",
    "resolve_strategy_fee_authority",
    "safe_ratio",
    "sma",
]
