from __future__ import annotations

from typing import Any


_CANONICAL_CONTEXT_VERSION = 1


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

    base_signal = _as_text(payload.get("base_signal", entry.get("base_signal", signal)), default="HOLD")
    base_reason = _as_text(payload.get("base_reason", entry.get("base_reason", reason)), default=reason)
    entry_reason = _as_text(payload.get("entry_reason", entry.get("entry_reason", reason)), default=reason)
    blocked_filters = _as_filter_list(payload.get("blocked_filters"))

    inferred_filter_blocked = bool(blocked_filters) and base_signal in {"BUY", "SELL"}
    filter_blocked = _as_bool(payload.get("filter_blocked")) or inferred_filter_blocked

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

    payload["decision_context_version"] = _CANONICAL_CONTEXT_VERSION
    payload["decision_type"] = decision_type
    payload["base_reason"] = base_reason
    payload["entry_reason"] = entry_reason
    payload["blocked_filters"] = blocked_filters
    payload["filter_blocked"] = bool(filter_blocked)
    payload["signal_strength_label"] = signal_strength_label
    payload["market_observations"] = market_observations

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
    payload["block_reason"] = block_reason_hierarchy[0] if block_reason_hierarchy else None
    payload["block_reason_hierarchy"] = block_reason_hierarchy

    return payload
