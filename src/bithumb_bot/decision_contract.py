from __future__ import annotations

from typing import Any


DECISION_CONTRACT_VERSION = "decision_v2"

BLOCK_LAYER_PRIORITY = (
    "fee_authority",
    "market_regime",
    "position_gate",
    "strategy_filters",
    "pre_trade_economics",
    "execution_order_rule",
    "performance_gate",
)


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _text(value: object, default: str = "") -> str:
    text = str(value if value is not None else default).strip()
    return text or default


def _candidate(reasons: list[tuple[str, str]], layer: str, reason: object) -> None:
    reason_text = _text(reason)
    if not reason_text or reason_text == "none":
        return
    item = (layer, reason_text)
    if item not in reasons:
        reasons.append(item)


def select_primary_block(reasons: list[tuple[str, str]]) -> tuple[str | None, str | None]:
    if not reasons:
        return None, None
    for layer in BLOCK_LAYER_PRIORITY:
        for candidate_layer, candidate_reason in reasons:
            if candidate_layer == layer:
                return candidate_layer, candidate_reason
    return reasons[0]


def build_replay_fingerprint(
    *,
    strategy_name: str,
    pair: str,
    interval: str,
    candle_ts: int | None,
    through_ts_ms: int | None,
    short_n: int,
    long_n: int,
    thresholds: dict[str, object],
    fee_authority: dict[str, object],
    slippage_bps: float,
    regime_version: str,
    order_sizing: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "strategy_name": str(strategy_name),
        "strategy_version": "sma_with_filter_v1",
        "decision_contract_version": DECISION_CONTRACT_VERSION,
        "pair": str(pair),
        "interval": str(interval),
        "candle_ts": None if candle_ts is None else int(candle_ts),
        "through_ts_ms": None if through_ts_ms is None else int(through_ts_ms),
        "sma_short": int(short_n),
        "sma_long": int(long_n),
        "regime_feature_version": str(regime_version),
        "thresholds": dict(thresholds),
        "fee_authority_source": _text(fee_authority.get("fee_source"), default="unknown"),
        "fee_authority_degraded": bool(fee_authority.get("degraded", False)),
        "slippage_bps": float(slippage_bps),
        "order_sizing": dict(order_sizing or {}),
    }


def build_signal_flow(
    *,
    context: dict[str, object],
    final_action: str | None = None,
    extra_block_reasons: list[tuple[str, str]] | None = None,
) -> dict[str, object]:
    entry = _dict(context.get("entry"))
    base_signal = _text(context.get("base_signal", entry.get("base_signal")), default="HOLD").upper()
    strategy_signal = _text(context.get("entry_signal", entry.get("entry_signal", context.get("raw_signal", base_signal))), default=base_signal).upper()
    final_signal = _text(context.get("final_signal", context.get("signal", strategy_signal)), default=strategy_signal).upper()
    resolved_final_action = _text(final_action or context.get("final_action"), default=final_signal).upper()

    reasons: list[tuple[str, str]] = []
    blocked_filters = context.get("blocked_filters") if isinstance(context.get("blocked_filters"), list) else []
    for raw_filter in blocked_filters:
        filter_name = _text(raw_filter)
        if filter_name == "fee_authority_degraded":
            _candidate(reasons, "fee_authority", "degraded")
        elif filter_name:
            _candidate(reasons, "strategy_filters", filter_name)

    market_regime = _dict(context.get("market_regime"))
    if base_signal == "BUY" and market_regime and not bool(market_regime.get("allows_entry", True)):
        _candidate(reasons, "market_regime", market_regime.get("block_reason", market_regime.get("regime")))

    position_gate = _dict(context.get("position_gate"))
    if base_signal == "BUY" and not bool(position_gate.get("entry_allowed", context.get("entry_allowed", True))):
        _candidate(reasons, "position_gate", position_gate.get("entry_block_reason", context.get("entry_block_reason")))

    economics = _dict(context.get("pre_trade_economics"))
    if economics and bool(economics.get("blocking_enabled", False)) and not bool(economics.get("meaningful_edge", True)):
        _candidate(reasons, "pre_trade_economics", economics.get("reason", "net_edge_below_minimum"))

    if extra_block_reasons:
        for layer, reason in extra_block_reasons:
            _candidate(reasons, layer, reason)

    primary_layer, primary_reason = select_primary_block(reasons)
    all_block_reasons = [f"{layer}.{reason}" for layer, reason in reasons]
    return {
        "base_signal": base_signal,
        "strategy_signal": strategy_signal,
        "final_signal": final_signal,
        "final_action": resolved_final_action,
        "primary_block_layer": primary_layer,
        "primary_block_reason": primary_reason,
        "all_block_reasons": all_block_reasons,
    }


def apply_decision_contract(
    context: dict[str, object],
    *,
    final_action: str | None = None,
    extra_block_reasons: list[tuple[str, str]] | None = None,
) -> dict[str, object]:
    payload = dict(context)
    payload["decision_contract_version"] = DECISION_CONTRACT_VERSION
    signal_flow = build_signal_flow(
        context=payload,
        final_action=final_action,
        extra_block_reasons=extra_block_reasons,
    )
    payload["signal_flow"] = signal_flow
    payload["primary_block_layer"] = signal_flow["primary_block_layer"]
    payload["primary_block_reason"] = signal_flow["primary_block_reason"]
    payload["all_block_reasons"] = list(signal_flow["all_block_reasons"])
    payload.setdefault("strategy_features", payload.get("features") if isinstance(payload.get("features"), dict) else {})
    payload.setdefault("strategy_filters", payload.get("filters") if isinstance(payload.get("filters"), dict) else {})
    return payload
