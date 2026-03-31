from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any

from ..config import settings
from ..notifier import notify
from ..markets import canonical_market_id
from .bithumb import BithumbBroker, classify_private_api_error

_CACHE_TTL_SEC = 300.0
_cached_rules: dict[str, tuple[float, "OrderRules", "OrderRules"]] = {}


@dataclass(frozen=True)
class OrderRules:
    min_qty: float
    qty_step: float
    min_notional_krw: float
    max_qty_decimals: int


@dataclass(frozen=True)
class RuleResolution:
    rules: OrderRules
    source: dict[str, str]


def required_rule_issues(rules: OrderRules) -> list[str]:
    issues: list[str] = []
    if not math.isfinite(float(rules.min_qty)) or float(rules.min_qty) <= 0:
        issues.append(f"min_qty must be > 0 (got {rules.min_qty})")
    if not math.isfinite(float(rules.qty_step)) or float(rules.qty_step) <= 0:
        issues.append(f"qty_step must be > 0 (got {rules.qty_step})")
    if not math.isfinite(float(rules.min_notional_krw)) or float(rules.min_notional_krw) <= 0:
        issues.append(f"min_notional_krw must be > 0 (got {rules.min_notional_krw})")
    if int(rules.max_qty_decimals) <= 0:
        issues.append(f"max_qty_decimals must be > 0 (got {rules.max_qty_decimals})")
    return issues


def _manual_rules() -> OrderRules:
    return OrderRules(
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    )


def _lookup(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    cur: Any = payload
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _pick_float(payload: dict[str, Any], paths: tuple[tuple[str, ...], ...]) -> float | None:
    for path in paths:
        raw = _lookup(payload, path)
        if raw is None or raw == "":
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value) and value > 0:
            return value
    return None


def _pick_int(payload: dict[str, Any], paths: tuple[tuple[str, ...], ...]) -> int | None:
    for path in paths:
        raw = _lookup(payload, path)
        if raw is None or raw == "":
            continue
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def build_order_rules_market(pair: str) -> str:
    return canonical_market_id(pair)


def fetch_exchange_order_rules(pair: str) -> OrderRules:
    market = build_order_rules_market(pair)
    payload = BithumbBroker().get_order_chance(market=market)

    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected order rules payload type: {type(payload).__name__}")

    min_qty = _pick_float(
        payload,
        (
            ("market", "ask", "min_volume"),
            ("market", "bid", "min_volume"),
            ("market", "min_volume"),
            ("ask", "min_volume"),
            ("bid", "min_volume"),
            ("min_order_quantity",),
            ("min_qty",),
        ),
    ) or 0.0
    qty_step = _pick_float(
        payload,
        (
            ("market", "ask", "volume_step"),
            ("market", "bid", "volume_step"),
            ("market", "volume_step"),
            ("ask", "volume_step"),
            ("bid", "volume_step"),
            ("order_sizing", "step"),
            ("qty_step",),
        ),
    ) or 0.0
    min_notional_krw = _pick_float(
        payload,
        (
            ("market", "ask", "min_total"),
            ("market", "bid", "min_total"),
            ("market", "min_total"),
            ("ask", "min_total"),
            ("bid", "min_total"),
            ("min_order_amount",),
            ("min_notional",),
        ),
    ) or 0.0
    max_qty_decimals = _pick_int(
        payload,
        (
            ("market", "ask", "max_decimal_places"),
            ("market", "bid", "max_decimal_places"),
            ("market", "max_decimal_places"),
            ("ask", "max_decimal_places"),
            ("bid", "max_decimal_places"),
            ("qty_unit_scale",),
            ("max_qty_decimals",),
        ),
    ) or 0

    return OrderRules(
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        max_qty_decimals=max_qty_decimals,
    )


def get_effective_order_rules(pair: str) -> RuleResolution:
    now = time.time()
    manual = _manual_rules()

    cached = _cached_rules.get(pair)
    if cached and now - cached[0] < _CACHE_TTL_SEC and cached[2] == manual:
        return RuleResolution(rules=cached[1], source={})
    try:
        auto = fetch_exchange_order_rules(pair)
    except Exception as exc:
        code, summary = classify_private_api_error(exc)
        notify(
            f"[WARN] order rules auto-sync failed for {pair}; using manual config only "
            f"({code}: {summary}; {type(exc).__name__}: {exc})"
        )
        _cached_rules[pair] = (now, manual, manual)
        return RuleResolution(
            rules=manual,
            source={
                "min_qty": "manual",
                "qty_step": "manual",
                "min_notional_krw": "manual",
                "max_qty_decimals": "manual",
            },
        )

    source: dict[str, str] = {}
    min_qty = auto.min_qty if auto.min_qty > 0 else manual.min_qty
    source["min_qty"] = "auto" if auto.min_qty > 0 else "manual"

    qty_step = auto.qty_step if auto.qty_step > 0 else manual.qty_step
    source["qty_step"] = "auto" if auto.qty_step > 0 else "manual"

    min_notional = auto.min_notional_krw if auto.min_notional_krw > 0 else manual.min_notional_krw
    source["min_notional_krw"] = "auto" if auto.min_notional_krw > 0 else "manual"

    max_decimals = auto.max_qty_decimals if auto.max_qty_decimals > 0 else manual.max_qty_decimals
    source["max_qty_decimals"] = "auto" if auto.max_qty_decimals > 0 else "manual"

    merged = OrderRules(
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional,
        max_qty_decimals=max_decimals,
    )

    manual_fields = [name for name, field_source in source.items() if field_source == "manual"]
    if manual_fields:
        notify(f"[WARN] order rules auto-sync partial for {pair}; fallback to manual for: {', '.join(manual_fields)}")

    _cached_rules[pair] = (now, merged, manual)
    return RuleResolution(rules=merged, source=source)
