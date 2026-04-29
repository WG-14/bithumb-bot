from __future__ import annotations

import math
from dataclasses import dataclass, field

from .config import settings


@dataclass(frozen=True)
class ExecutionOrderRules:
    market: str
    min_qty: float | None
    qty_step: float | None
    min_notional_krw: float | None
    bid_min_total_krw: float | None = None
    bid_types: tuple[str, ...] = ()
    source_mode: str = "missing"
    source: str = "missing"
    stale: bool = False
    degraded: bool = False
    fallback_used: bool = False
    field_sources: dict[str, str] = field(default_factory=dict)

    def as_order_rules(self) -> dict[str, object]:
        return {
            "market": self.market,
            "min_qty": self.min_qty,
            "qty_step": self.qty_step,
            "min_notional_krw": self.min_notional_krw,
            "bid_min_total_krw": self.bid_min_total_krw,
            "bid_types": list(self.bid_types),
            "order_rule_authority": self.source,
            "order_rule_authority_source": self.source,
            "order_rule_authority_source_mode": self.source_mode,
            "order_rule_authority_stale": bool(self.stale),
            "order_rule_authority_degraded": bool(self.degraded),
            "order_rule_authority_fallback_used": bool(self.fallback_used),
            "order_rule_min_qty_source": self.field_sources.get("min_qty", self.source),
            "order_rule_qty_step_source": self.field_sources.get("qty_step", self.source),
            "order_rule_min_notional_krw_source": self.field_sources.get(
                "min_notional_krw", self.source
            ),
        }


def _positive_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed <= 0.0:
        return None
    return parsed


def _tuple_str(value: object) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    if value is None:
        return ()
    return (str(value),)


def _payload_rule_value(payload: dict[str, object], primary: str, fallback: str) -> float | None:
    value = _positive_float(payload.get(primary))
    if value is not None:
        return value
    return _positive_float(payload.get(fallback))


def _payload_rules(payload: dict[str, object], *, market: str) -> ExecutionOrderRules | None:
    min_qty = _payload_rule_value(payload, "min_qty", "residual_proof_min_qty")
    min_notional = _payload_rule_value(
        payload,
        "min_notional_krw",
        "residual_proof_min_notional_krw",
    )
    if min_qty is None or min_notional is None:
        return None
    qty_step = _payload_rule_value(payload, "qty_step", "residual_proof_qty_step")
    bid_min_total = _positive_float(payload.get("bid_min_total_krw"))
    return ExecutionOrderRules(
        market=market,
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional,
        bid_min_total_krw=bid_min_total,
        bid_types=_tuple_str(payload.get("bid_types")),
        source_mode="payload",
        source="payload",
        field_sources={
            "min_qty": "payload",
            "qty_step": "payload" if qty_step is not None else "missing",
            "min_notional_krw": "payload",
        },
    )


def _settings_rules(*, market: str) -> ExecutionOrderRules | None:
    min_qty = _positive_float(getattr(settings, "LIVE_MIN_ORDER_QTY", None))
    min_notional = _positive_float(getattr(settings, "MIN_ORDER_NOTIONAL_KRW", None))
    if min_qty is None or min_notional is None:
        return None
    qty_step = _positive_float(getattr(settings, "LIVE_ORDER_QTY_STEP", None))
    return ExecutionOrderRules(
        market=market,
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional,
        source_mode="settings",
        source="settings",
        degraded=True,
        fallback_used=True,
        field_sources={
            "min_qty": "settings.LIVE_MIN_ORDER_QTY",
            "qty_step": "settings.LIVE_ORDER_QTY_STEP" if qty_step is not None else "missing",
            "min_notional_krw": "settings.MIN_ORDER_NOTIONAL_KRW",
        },
    )


def _effective_rules(*, market: str) -> ExecutionOrderRules | None:
    try:
        from .broker.order_rules import get_effective_order_rules, rule_source_for

        resolved = get_effective_order_rules(market)
    except Exception:
        return None
    rules = resolved.rules
    min_qty = _positive_float(getattr(rules, "min_qty", None))
    min_notional = _positive_float(getattr(rules, "min_notional_krw", None))
    if min_qty is None or min_notional is None:
        return None
    source = dict(getattr(resolved, "source", {}) or {})
    source_mode = str(getattr(resolved, "source_mode", "") or "effective_order_rules")
    return ExecutionOrderRules(
        market=market,
        min_qty=min_qty,
        qty_step=_positive_float(getattr(rules, "qty_step", None)),
        min_notional_krw=min_notional,
        bid_min_total_krw=_positive_float(getattr(rules, "bid_min_total_krw", None)),
        bid_types=_tuple_str(getattr(rules, "bid_types", ())),
        source_mode=source_mode,
        source=f"effective_order_rules:{source_mode}",
        stale=bool(getattr(resolved, "stale", False))
        or bool(resolved.is_stale() if hasattr(resolved, "is_stale") else False),
        degraded=bool(getattr(resolved, "fallback_used", False)),
        fallback_used=bool(getattr(resolved, "fallback_used", False)),
        field_sources={
            "min_qty": rule_source_for("min_qty", source),
            "qty_step": rule_source_for("qty_step", source),
            "min_notional_krw": rule_source_for("min_notional_krw", source),
        },
    )


def resolve_execution_order_rules(
    payload: dict[str, object] | None = None,
    *,
    market: str | None = None,
    allow_effective_lookup: bool = True,
) -> ExecutionOrderRules:
    normalized_market = str(market or getattr(settings, "PAIR", "") or "")
    payload_rules = _payload_rules(dict(payload or {}), market=normalized_market)
    if payload_rules is not None:
        return payload_rules
    if allow_effective_lookup:
        effective = _effective_rules(market=normalized_market)
        if effective is not None:
            return effective
    settings_rules = _settings_rules(market=normalized_market)
    if settings_rules is not None:
        return settings_rules
    return ExecutionOrderRules(
        market=normalized_market,
        min_qty=None,
        qty_step=None,
        min_notional_krw=None,
        source_mode="missing",
        source="missing",
        degraded=True,
        field_sources={
            "min_qty": "missing",
            "qty_step": "missing",
            "min_notional_krw": "missing",
        },
    )
