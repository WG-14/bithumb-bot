from __future__ import annotations

from typing import Any


_CANONICAL_CONTEXT_VERSION = 2


def _as_text(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _as_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "yes", "on"}:
            return True
        if v in {"0", "false", "no", "off", ""}:
            return False
    if value is None:
        return False
    return bool(value)


def _as_filter_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        out: list[str] = []
        for item in raw:
            key = str(item).strip()
            if key and key not in out:
                out.append(key)
        return out
    if isinstance(raw, tuple):
        return _as_filter_list(list(raw))
    if isinstance(raw, str):
        parts = [part.strip() for part in raw.split(",")]
        return [part for part in parts if part]
    return []


def _extract_market_observations(context: dict[str, Any]) -> dict[str, float | None]:
    features = context.get("features") if isinstance(context.get("features"), dict) else {}
    return {
        "gap": _as_float_or_none(context.get("gap_ratio", features.get("sma_gap_ratio"))),
        "volatility": _as_float_or_none(
            context.get("volatility_ratio", features.get("volatility_range_ratio"))
        ),
        "extension": _as_float_or_none(
            context.get("overextended_ratio", features.get("overextended_abs_return_ratio"))
        ),
    }


def normalize_strategy_decision_context(
    *,
    context: dict[str, Any] | None,
    signal: str,
    reason: str,
    strategy_name: str,
    pair: str,
    interval: str,
    decision_ts: int,
    candle_ts: int | None,
    market_price: float | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = dict(context or {})
    entry = payload.get("entry") if isinstance(payload.get("entry"), dict) else {}
    signal_strength = (
        payload.get("signal_strength") if isinstance(payload.get("signal_strength"), dict) else {}
    )
    position_gate = payload.get("position_gate") if isinstance(payload.get("position_gate"), dict) else {}

    base_signal = _as_text(payload.get("base_signal", entry.get("base_signal", signal)), default="HOLD")
    base_reason = _as_text(payload.get("base_reason", entry.get("base_reason", reason)), default=reason)
    entry_reason = _as_text(payload.get("entry_reason", entry.get("entry_reason", reason)), default=reason)
    raw_signal = _as_text(payload.get("raw_signal", base_signal), default=base_signal)
    final_signal = _as_text(payload.get("final_signal", payload.get("signal", signal)), default=signal)
    blocked_filters = _as_filter_list(payload.get("blocked_filters"))

    inferred_filter_blocked = bool(blocked_filters) and base_signal in {"BUY", "SELL"}
    filter_blocked = _as_bool(payload.get("filter_blocked")) or inferred_filter_blocked
    entry_blocked = _as_bool(payload.get("entry_blocked"))
    if not entry_blocked:
        entry_blocked = raw_signal in {"BUY", "SELL"} and final_signal != raw_signal

    entry_block_reason = payload.get("entry_block_reason")
    if entry_block_reason is None:
        if filter_blocked:
            entry_block_reason = payload.get("block_reason", entry_reason or reason)
        elif entry_blocked:
            entry_block_reason = payload.get("reason", reason)
    entry_block_reason_text = _as_text(entry_block_reason, default="")
    if not entry_block_reason_text:
        entry_block_reason_text = None

    signal_strength_label = _as_text(
        payload.get("signal_strength_label", signal_strength.get("label", "unknown")),
        default="unknown",
    )

    decision_type = _as_text(payload.get("decision_type"), default="")
    if not decision_type:
        if filter_blocked and base_signal in {"BUY", "SELL"}:
            decision_type = "BLOCKED_ENTRY"
        elif signal in {"BUY", "SELL", "HOLD"}:
            decision_type = signal
        else:
            decision_type = "HOLD"

    market_observations = _extract_market_observations(payload)

    block_reason_hierarchy: list[str] = []
    for item in blocked_filters:
        if item not in block_reason_hierarchy:
            block_reason_hierarchy.append(item)
    if filter_blocked and entry_reason and entry_reason not in block_reason_hierarchy:
        block_reason_hierarchy.append(entry_reason)
    if filter_blocked and base_reason and base_reason not in block_reason_hierarchy:
        block_reason_hierarchy.append(base_reason)
    if entry_blocked and not filter_blocked and entry_block_reason_text:
        block_reason_hierarchy.append(entry_block_reason_text)

    dust_classification = _as_text(
        payload.get(
            "dust_classification",
            position_gate.get("dust_classification", position_gate.get("dust_state", "")),
        ),
        default="",
    )
    entry_allowed = _as_bool(
        payload.get(
            "entry_allowed",
            position_gate.get(
                "entry_allowed",
                position_gate.get(
                    "effective_flat_due_to_harmless_dust",
                    position_gate.get("dust_treat_as_flat"),
                ),
            ),
        )
    )
    effective_flat = _as_bool(
        payload.get(
            "effective_flat",
            position_gate.get("effective_flat_due_to_harmless_dust", position_gate.get("dust_treat_as_flat")),
        )
    )
    raw_qty_open = _as_float_or_none(
        payload.get("raw_qty_open", position_gate.get("raw_qty_open"))
    )
    if raw_qty_open is None:
        raw_qty_open = 0.0
    normalized_exposure_active = _as_bool(
        payload.get(
            "normalized_exposure_active",
            position_gate.get("normalized_exposure_active", raw_qty_open > 1e-12 and not entry_allowed),
        )
    )
    normalized_exposure_qty = _as_float_or_none(
        payload.get(
            "normalized_exposure_qty",
            position_gate.get(
                "normalized_exposure_qty",
                raw_qty_open if normalized_exposure_active else 0.0,
            ),
        )
    )
    if normalized_exposure_qty is None:
        normalized_exposure_qty = raw_qty_open if normalized_exposure_active else 0.0

    decision_summary = {
        "raw_signal": raw_signal,
        "final_signal": final_signal,
        "entry_blocked": bool(entry_blocked),
        "entry_block_reason": entry_block_reason_text,
        "dust_classification": dust_classification,
        "entry_allowed": bool(entry_allowed),
        "effective_flat": bool(effective_flat),
        "raw_qty_open": float(raw_qty_open),
        "normalized_exposure_active": bool(normalized_exposure_active),
        "normalized_exposure_qty": float(normalized_exposure_qty),
    }

    payload["decision_context_version"] = _CANONICAL_CONTEXT_VERSION
    payload["decision_type"] = decision_type
    payload["base_reason"] = base_reason
    payload["entry_reason"] = entry_reason
    payload["raw_signal"] = raw_signal
    payload["final_signal"] = final_signal
    payload["blocked_filters"] = blocked_filters
    payload["filter_blocked"] = bool(filter_blocked)
    payload["entry_blocked"] = bool(entry_blocked)
    payload["entry_block_reason"] = entry_block_reason_text
    payload["signal_strength_label"] = signal_strength_label
    payload["market_observations"] = market_observations
    payload["dust_classification"] = dust_classification
    payload["entry_allowed"] = bool(entry_allowed)
    payload["effective_flat"] = bool(effective_flat)
    payload["raw_qty_open"] = float(raw_qty_open)
    payload["normalized_exposure_active"] = bool(normalized_exposure_active)
    payload["normalized_exposure_qty"] = float(normalized_exposure_qty)
    payload["decision_summary"] = decision_summary

    payload["strategy_name"] = _as_text(payload.get("strategy_name", strategy_name), default=strategy_name)
    payload["pair"] = _as_text(payload.get("pair", pair), default=pair)
    payload["interval"] = _as_text(payload.get("interval", interval), default=interval)
    payload["decision_ts"] = int(decision_ts)
    payload["candle_ts"] = None if candle_ts is None else int(candle_ts)
    payload["market_price"] = None if market_price is None else float(market_price)
    payload["signal"] = _as_text(payload.get("signal", signal), default=signal)
    payload["reason"] = _as_text(payload.get("reason", reason), default=reason)
    payload["base_signal"] = base_signal
    payload["entry_signal"] = _as_text(payload.get("entry_signal", entry.get("entry_signal", signal)), default=signal)

    payload["blocked_candidate"] = bool(decision_type == "BLOCKED_ENTRY")
    if entry_block_reason_text and entry_block_reason_text not in block_reason_hierarchy:
        block_reason_hierarchy.append(entry_block_reason_text)
    payload["block_reason"] = block_reason_hierarchy[0] if block_reason_hierarchy else entry_block_reason_text
    payload["block_reason_hierarchy"] = block_reason_hierarchy
    if not isinstance(payload.get("position_state"), dict):
        raw_holdings = dict(position_gate)
        raw_holdings.setdefault("classification", dust_classification)
        present = dust_classification not in {"", "no_dust"}
        raw_holdings.setdefault("present", present)
        raw_holdings.setdefault("broker_local_match", bool(position_gate.get("dust_broker_local_match", False)))
        raw_holdings.setdefault("compact_summary", str(position_gate.get("dust_residual_summary") or "none"))
        payload["position_state"] = {
            "raw_holdings": {
                "classification": dust_classification,
                "present": present,
                "broker_qty": float(position_gate.get("dust_broker_qty", 0.0) or 0.0),
                "local_qty": float(position_gate.get("dust_local_qty", 0.0) or 0.0),
                "delta_qty": float(position_gate.get("dust_delta_qty", 0.0) or 0.0),
                "min_qty": float(position_gate.get("dust_min_qty", 0.0) or 0.0),
                "min_notional_krw": float(position_gate.get("dust_min_notional_krw", 0.0) or 0.0),
                "broker_local_match": bool(position_gate.get("dust_broker_local_match", False)),
                "compact_summary": str(position_gate.get("dust_residual_summary") or "none"),
            },
            "normalized_exposure": {
                "raw_qty_open": float(raw_qty_open),
                "dust_classification": dust_classification,
                "dust_state": dust_classification,
                "entry_allowed": bool(entry_allowed),
                "effective_flat": bool(effective_flat),
                "harmless_dust_effective_flat": bool(entry_allowed and dust_classification == "harmless_dust"),
                "normalized_exposure_active": bool(normalized_exposure_active),
                "normalized_exposure_qty": float(normalized_exposure_qty),
            },
            "operator_diagnostics": {
                "state": dust_classification or "no_dust",
                "state_label": (
                    "harmless dust residual"
                    if dust_classification == "harmless_dust"
                    else "blocking dust residual requires manual review"
                    if present
                    else "no dust residual"
                ),
                "operator_action": str(position_gate.get("dust_operator_action") or "-"),
                "operator_message": str(position_gate.get("dust_operator_message") or "-"),
                "broker_local_match": bool(position_gate.get("dust_broker_local_match", False)),
                "new_orders_allowed": bool(position_gate.get("dust_new_orders_allowed", False)),
                "resume_allowed": bool(position_gate.get("dust_resume_allowed_by_policy", False)),
                "treat_as_flat": bool(position_gate.get("dust_treat_as_flat", False)),
            },
        }

    return payload
