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
    ask_min_total_krw: float | None = None
    bid_price_unit: float | None = None
    ask_price_unit: float | None = None
    order_types: tuple[str, ...] = ()
    bid_types: tuple[str, ...] = ()
    ask_types: tuple[str, ...] = ()
    order_sides: tuple[str, ...] = ()
    bid_fee: float | None = None
    ask_fee: float | None = None
    maker_bid_fee: float | None = None
    maker_ask_fee: float | None = None
    max_qty_decimals: int | None = None
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
            "bid_min_total_krw": _nonnegative_float(self.bid_min_total_krw) or 0.0,
            "ask_min_total_krw": _nonnegative_float(self.ask_min_total_krw) or 0.0,
            "bid_price_unit": _nonnegative_float(self.bid_price_unit) or 0.0,
            "ask_price_unit": _nonnegative_float(self.ask_price_unit) or 0.0,
            "order_types": list(self.order_types),
            "bid_types": list(self.bid_types),
            "ask_types": list(self.ask_types),
            "order_sides": list(self.order_sides),
            "bid_fee": _nonnegative_float(self.bid_fee) or 0.0,
            "ask_fee": _nonnegative_float(self.ask_fee) or 0.0,
            "maker_bid_fee": _nonnegative_float(self.maker_bid_fee) or 0.0,
            "maker_ask_fee": _nonnegative_float(self.maker_ask_fee) or 0.0,
            "max_qty_decimals": _positive_int(self.max_qty_decimals) or 0,
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
            "order_rule_bid_min_total_krw_source": self.field_sources.get(
                "bid_min_total_krw", self.source
            ),
            "order_rule_ask_min_total_krw_source": self.field_sources.get(
                "ask_min_total_krw", self.source
            ),
            "order_rule_bid_price_unit_source": self.field_sources.get(
                "bid_price_unit", self.source
            ),
            "order_rule_ask_price_unit_source": self.field_sources.get(
                "ask_price_unit", self.source
            ),
            "order_rule_order_types_source": self.field_sources.get("order_types", self.source),
            "order_rule_bid_types_source": self.field_sources.get("bid_types", self.source),
            "order_rule_ask_types_source": self.field_sources.get("ask_types", self.source),
            "order_rule_order_sides_source": self.field_sources.get("order_sides", self.source),
            "order_rule_bid_fee_source": self.field_sources.get("bid_fee", self.source),
            "order_rule_ask_fee_source": self.field_sources.get("ask_fee", self.source),
            "order_rule_maker_bid_fee_source": self.field_sources.get(
                "maker_bid_fee", self.source
            ),
            "order_rule_maker_ask_fee_source": self.field_sources.get(
                "maker_ask_fee", self.source
            ),
            "order_rule_max_qty_decimals_source": self.field_sources.get(
                "max_qty_decimals", self.source
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


def _finite_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _nonnegative_float(value: object) -> float | None:
    parsed = _finite_float(value)
    if parsed is None or parsed < 0.0:
        return None
    return parsed


def _positive_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


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
    ask_min_total = _positive_float(payload.get("ask_min_total_krw"))
    bid_price_unit = _nonnegative_float(payload.get("bid_price_unit"))
    ask_price_unit = _nonnegative_float(payload.get("ask_price_unit"))
    max_qty_decimals = _positive_int(payload.get("max_qty_decimals"))
    return ExecutionOrderRules(
        market=market,
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional,
        bid_min_total_krw=bid_min_total,
        ask_min_total_krw=ask_min_total,
        bid_price_unit=bid_price_unit,
        ask_price_unit=ask_price_unit,
        order_types=_tuple_str(payload.get("order_types")),
        bid_types=_tuple_str(payload.get("bid_types")),
        ask_types=_tuple_str(payload.get("ask_types")),
        order_sides=_tuple_str(payload.get("order_sides")),
        bid_fee=_nonnegative_float(payload.get("bid_fee")),
        ask_fee=_nonnegative_float(payload.get("ask_fee")),
        maker_bid_fee=_nonnegative_float(payload.get("maker_bid_fee")),
        maker_ask_fee=_nonnegative_float(payload.get("maker_ask_fee")),
        max_qty_decimals=max_qty_decimals,
        source_mode="payload",
        source="payload",
        field_sources={
            "min_qty": "payload",
            "qty_step": "payload" if qty_step is not None else "missing",
            "min_notional_krw": "payload",
            "bid_min_total_krw": "payload" if bid_min_total is not None else "missing",
            "ask_min_total_krw": "payload" if ask_min_total is not None else "missing",
            "bid_price_unit": "payload" if bid_price_unit is not None else "missing",
            "ask_price_unit": "payload" if ask_price_unit is not None else "missing",
            "order_types": "payload" if payload.get("order_types") is not None else "missing",
            "bid_types": "payload" if payload.get("bid_types") is not None else "missing",
            "ask_types": "payload" if payload.get("ask_types") is not None else "missing",
            "order_sides": "payload" if payload.get("order_sides") is not None else "missing",
            "bid_fee": "payload" if payload.get("bid_fee") is not None else "missing",
            "ask_fee": "payload" if payload.get("ask_fee") is not None else "missing",
            "maker_bid_fee": "payload" if payload.get("maker_bid_fee") is not None else "missing",
            "maker_ask_fee": "payload" if payload.get("maker_ask_fee") is not None else "missing",
            "max_qty_decimals": "payload" if max_qty_decimals is not None else "missing",
        },
    )


def _settings_rules(*, market: str) -> ExecutionOrderRules | None:
    min_qty = _positive_float(getattr(settings, "LIVE_MIN_ORDER_QTY", None))
    min_notional = _positive_float(getattr(settings, "MIN_ORDER_NOTIONAL_KRW", None))
    if min_qty is None or min_notional is None:
        return None
    qty_step = _positive_float(getattr(settings, "LIVE_ORDER_QTY_STEP", None))
    max_qty_decimals = _positive_int(getattr(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", None))
    return ExecutionOrderRules(
        market=market,
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional,
        max_qty_decimals=max_qty_decimals,
        source_mode="settings",
        source="settings",
        degraded=True,
        fallback_used=True,
        field_sources={
            "min_qty": "settings.LIVE_MIN_ORDER_QTY",
            "qty_step": "settings.LIVE_ORDER_QTY_STEP" if qty_step is not None else "missing",
            "min_notional_krw": "settings.MIN_ORDER_NOTIONAL_KRW",
            "max_qty_decimals": (
                "settings.LIVE_ORDER_MAX_QTY_DECIMALS"
                if max_qty_decimals is not None
                else "missing"
            ),
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
        ask_min_total_krw=_positive_float(getattr(rules, "ask_min_total_krw", None)),
        bid_price_unit=_nonnegative_float(getattr(rules, "bid_price_unit", None)),
        ask_price_unit=_nonnegative_float(getattr(rules, "ask_price_unit", None)),
        order_types=_tuple_str(getattr(rules, "order_types", ())),
        bid_types=_tuple_str(getattr(rules, "bid_types", ())),
        ask_types=_tuple_str(getattr(rules, "ask_types", ())),
        order_sides=_tuple_str(getattr(rules, "order_sides", ())),
        bid_fee=_nonnegative_float(getattr(rules, "bid_fee", None)),
        ask_fee=_nonnegative_float(getattr(rules, "ask_fee", None)),
        maker_bid_fee=_nonnegative_float(getattr(rules, "maker_bid_fee", None)),
        maker_ask_fee=_nonnegative_float(getattr(rules, "maker_ask_fee", None)),
        max_qty_decimals=_positive_int(getattr(rules, "max_qty_decimals", None)),
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
            "bid_min_total_krw": rule_source_for("bid_min_total_krw", source),
            "ask_min_total_krw": rule_source_for("ask_min_total_krw", source),
            "bid_price_unit": rule_source_for("bid_price_unit", source),
            "ask_price_unit": rule_source_for("ask_price_unit", source),
            "order_types": rule_source_for("order_types", source),
            "bid_types": rule_source_for("bid_types", source),
            "ask_types": rule_source_for("ask_types", source),
            "order_sides": rule_source_for("order_sides", source),
            "bid_fee": rule_source_for("bid_fee", source),
            "ask_fee": rule_source_for("ask_fee", source),
            "maker_bid_fee": rule_source_for("maker_bid_fee", source),
            "maker_ask_fee": rule_source_for("maker_ask_fee", source),
            "max_qty_decimals": rule_source_for("max_qty_decimals", source),
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
