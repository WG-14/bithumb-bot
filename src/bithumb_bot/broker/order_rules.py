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
_cached_rules: dict[str, tuple[float, "DerivedOrderConstraints", "DerivedOrderConstraints"]] = {}
KNOWN_RULE_SOURCES = frozenset({"chance_doc", "manual_config", "unsupported_by_doc", "missing"})


@dataclass(frozen=True)
class OrderChanceSide:
    price_unit: float
    min_total: float


@dataclass(frozen=True)
class OrderChanceResponse:
    market_id: str
    order_types: tuple[str, ...]
    order_sides: tuple[str, ...]
    bid: OrderChanceSide
    ask: OrderChanceSide
    bid_fee: float
    ask_fee: float
    maker_bid_fee: float
    maker_ask_fee: float


@dataclass(frozen=True)
class DerivedOrderConstraints:
    market_id: str = ""
    bid_min_total_krw: float = 0.0
    ask_min_total_krw: float = 0.0
    bid_price_unit: float = 0.0
    ask_price_unit: float = 0.0
    order_types: tuple[str, ...] = ()
    order_sides: tuple[str, ...] = ()
    bid_fee: float = 0.0
    ask_fee: float = 0.0
    maker_bid_fee: float = 0.0
    maker_ask_fee: float = 0.0
    min_qty: float = 0.0
    qty_step: float = 0.0
    min_notional_krw: float = 0.0
    max_qty_decimals: int = 0


@dataclass(frozen=True)
class RuleResolution:
    rules: DerivedOrderConstraints
    source: dict[str, str]


class OrderChanceSchemaError(RuntimeError):
    """Raised when /v1/orders/chance response violates documented schema."""


def side_min_total_krw(*, rules: DerivedOrderConstraints, side: str) -> float:
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY":
        if float(rules.bid_min_total_krw) > 0:
            return float(rules.bid_min_total_krw)
    elif normalized_side == "SELL":
        if float(rules.ask_min_total_krw) > 0:
            return float(rules.ask_min_total_krw)
    return float(rules.min_notional_krw)


def required_rule_issues(rules: DerivedOrderConstraints) -> list[str]:
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


def rule_source_for(field: str, source: dict[str, str] | None) -> str:
    if not source:
        return "missing"
    normalized = str(source.get(field, "")).strip() or "missing"
    return normalized if normalized in KNOWN_RULE_SOURCES else "missing"


def required_rule_source_issues(source: dict[str, str] | None) -> list[str]:
    issues: list[str] = []
    doc_required_fields = (
        "bid_min_total_krw",
        "ask_min_total_krw",
        "bid_price_unit",
        "ask_price_unit",
    )
    for field in doc_required_fields:
        field_source = rule_source_for(field, source)
        if field_source != "chance_doc":
            issues.append(
                f"{field} source must be chance_doc for MODE=live "
                f"(got {field_source})"
            )
    return issues


def _manual_rules() -> DerivedOrderConstraints:
    return DerivedOrderConstraints(
        market_id="",
        bid_min_total_krw=0.0,
        ask_min_total_krw=0.0,
        bid_price_unit=0.0,
        ask_price_unit=0.0,
        order_types=(),
        order_sides=(),
        bid_fee=0.0,
        ask_fee=0.0,
        maker_bid_fee=0.0,
        maker_ask_fee=0.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    )


def _require_dict(payload: dict[str, Any], key: str, *, where: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be object")
    return value


def _require_non_empty_str(payload: dict[str, Any], key: str, *, where: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be non-empty string")
    return value.strip()


def _require_non_empty_list(payload: dict[str, Any], key: str, *, where: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list) or len(value) == 0:
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be non-empty array")
    return value


def _require_positive_number(payload: dict[str, Any], key: str, *, where: str) -> float:
    raw = payload.get(key)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be numeric") from None
    if not math.isfinite(value) or value <= 0:
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be > 0")
    return value


def parse_order_chance_response(payload: dict[str, Any], *, requested_market: str) -> OrderChanceResponse:
    fee_values = {
        fee_field: _require_positive_number(payload, fee_field, where="response")
        for fee_field in ("bid_fee", "ask_fee", "maker_bid_fee", "maker_ask_fee")
    }

    market = _require_dict(payload, "market", where="response")
    market_id = _require_non_empty_str(market, "id", where="response.market")
    if canonical_market_id(market_id) != canonical_market_id(requested_market):
        raise OrderChanceSchemaError(
            "/v1/orders/chance response.market.id mismatch: "
            f"requested={canonical_market_id(requested_market)} response={canonical_market_id(market_id)}"
        )

    _require_non_empty_list(market, "order_types", where="response.market")
    _require_non_empty_list(market, "order_sides", where="response.market")

    bid = _require_dict(market, "bid", where="response.market")
    ask = _require_dict(market, "ask", where="response.market")

    return OrderChanceResponse(
        market_id=market_id,
        order_types=tuple(str(item) for item in _require_non_empty_list(market, "order_types", where="response.market")),
        order_sides=tuple(str(item) for item in _require_non_empty_list(market, "order_sides", where="response.market")),
        bid=OrderChanceSide(
            price_unit=_require_positive_number(bid, "price_unit", where="response.market.bid"),
            min_total=_require_positive_number(bid, "min_total", where="response.market.bid"),
        ),
        ask=OrderChanceSide(
            price_unit=_require_positive_number(ask, "price_unit", where="response.market.ask"),
            min_total=_require_positive_number(ask, "min_total", where="response.market.ask"),
        ),
        bid_fee=fee_values["bid_fee"],
        ask_fee=fee_values["ask_fee"],
        maker_bid_fee=fee_values["maker_bid_fee"],
        maker_ask_fee=fee_values["maker_ask_fee"],
    )


def derive_order_rules_from_chance(response: OrderChanceResponse) -> DerivedOrderConstraints:
    return DerivedOrderConstraints(
        market_id=response.market_id,
        bid_min_total_krw=response.bid.min_total,
        ask_min_total_krw=response.ask.min_total,
        bid_price_unit=response.bid.price_unit,
        ask_price_unit=response.ask.price_unit,
        order_types=response.order_types,
        order_sides=response.order_sides,
        bid_fee=response.bid_fee,
        ask_fee=response.ask_fee,
        maker_bid_fee=response.maker_bid_fee,
        maker_ask_fee=response.maker_ask_fee,
        min_qty=0.0,
        qty_step=0.0,
        min_notional_krw=response.bid.min_total,
        max_qty_decimals=0,
    )


def build_order_rules_market(pair: str) -> str:
    return canonical_market_id(pair)


def fetch_exchange_order_rules(pair: str) -> DerivedOrderConstraints:
    market = build_order_rules_market(pair)
    payload = BithumbBroker().get_order_chance(market=market)

    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected order rules payload type: {type(payload).__name__}")

    chance = parse_order_chance_response(payload, requested_market=market)
    return derive_order_rules_from_chance(chance)


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
                "min_qty": "manual_config",
                "qty_step": "manual_config",
                "min_notional_krw": "manual_config",
                "max_qty_decimals": "manual_config",
                "market_id": "unsupported_by_doc",
                "bid_min_total_krw": "unsupported_by_doc",
                "ask_min_total_krw": "unsupported_by_doc",
                "bid_price_unit": "unsupported_by_doc",
                "ask_price_unit": "unsupported_by_doc",
                "order_types": "unsupported_by_doc",
                "order_sides": "unsupported_by_doc",
                "bid_fee": "unsupported_by_doc",
                "ask_fee": "unsupported_by_doc",
                "maker_bid_fee": "unsupported_by_doc",
                "maker_ask_fee": "unsupported_by_doc",
            },
        )

    source: dict[str, str] = {}
    min_qty = manual.min_qty
    source["min_qty"] = "manual_config"

    qty_step = manual.qty_step
    source["qty_step"] = "manual_config"

    min_notional = manual.min_notional_krw
    source["min_notional_krw"] = "manual_config"

    max_decimals = manual.max_qty_decimals
    source["max_qty_decimals"] = "manual_config"

    merged = DerivedOrderConstraints(
        market_id=auto.market_id or manual.market_id,
        bid_min_total_krw=auto.bid_min_total_krw if auto.bid_min_total_krw > 0 else manual.bid_min_total_krw,
        ask_min_total_krw=auto.ask_min_total_krw if auto.ask_min_total_krw > 0 else manual.ask_min_total_krw,
        bid_price_unit=auto.bid_price_unit if auto.bid_price_unit > 0 else manual.bid_price_unit,
        ask_price_unit=auto.ask_price_unit if auto.ask_price_unit > 0 else manual.ask_price_unit,
        order_types=auto.order_types or manual.order_types,
        order_sides=auto.order_sides or manual.order_sides,
        bid_fee=auto.bid_fee if auto.bid_fee > 0 else manual.bid_fee,
        ask_fee=auto.ask_fee if auto.ask_fee > 0 else manual.ask_fee,
        maker_bid_fee=auto.maker_bid_fee if auto.maker_bid_fee > 0 else manual.maker_bid_fee,
        maker_ask_fee=auto.maker_ask_fee if auto.maker_ask_fee > 0 else manual.maker_ask_fee,
        min_qty=min_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional,
        max_qty_decimals=max_decimals,
    )
    source["market_id"] = "chance_doc" if auto.market_id else "unsupported_by_doc"
    source["bid_min_total_krw"] = "chance_doc" if auto.bid_min_total_krw > 0 else "unsupported_by_doc"
    source["ask_min_total_krw"] = "chance_doc" if auto.ask_min_total_krw > 0 else "unsupported_by_doc"
    source["bid_price_unit"] = "chance_doc" if auto.bid_price_unit > 0 else "unsupported_by_doc"
    source["ask_price_unit"] = "chance_doc" if auto.ask_price_unit > 0 else "unsupported_by_doc"
    source["order_types"] = "chance_doc" if auto.order_types else "unsupported_by_doc"
    source["order_sides"] = "chance_doc" if auto.order_sides else "unsupported_by_doc"
    source["bid_fee"] = "chance_doc" if auto.bid_fee > 0 else "unsupported_by_doc"
    source["ask_fee"] = "chance_doc" if auto.ask_fee > 0 else "unsupported_by_doc"
    source["maker_bid_fee"] = "chance_doc" if auto.maker_bid_fee > 0 else "unsupported_by_doc"
    source["maker_ask_fee"] = "chance_doc" if auto.maker_ask_fee > 0 else "unsupported_by_doc"

    manual_fields = [name for name, field_source in source.items() if field_source == "manual_config"]
    if manual_fields:
        notify(f"[WARN] order rules auto-sync partial for {pair}; fallback to manual for: {', '.join(manual_fields)}")

    _cached_rules[pair] = (now, merged, manual)
    return RuleResolution(rules=merged, source=source)


# Backward-compatible alias for existing call sites.
OrderRules = DerivedOrderConstraints
