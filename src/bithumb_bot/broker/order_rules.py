from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any

from ..config import settings
from ..markets import ExchangeMarketCodeError, canonical_market_id, parse_documented_market_code
from ..notifier import notify
from .bithumb import BithumbBroker, classify_private_api_error

_CACHE_TTL_SEC = 300.0
_cached_rules: dict[str, tuple[float, "RuleResolution", "DerivedOrderConstraints"]] = {}
KNOWN_RULE_SOURCES = frozenset(
    {
        "chance_doc",
        "local_fallback",
        "merged",
        "unsupported_by_doc",
        "missing",
    }
)


@dataclass(frozen=True)
class OrderChanceSide:
    price_unit: float | None
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
class ExchangeDerivedConstraints:
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


@dataclass(frozen=True)
class LocalFallbackConstraints:
    min_qty: float = 0.0
    qty_step: float = 0.0
    min_notional_krw: float = 0.0
    max_qty_decimals: int = 0


@dataclass(frozen=True)
class RuleResolution:
    rules: DerivedOrderConstraints
    source: dict[str, str]
    fallback_used: bool = False
    fallback_reason_code: str = ""
    fallback_reason_summary: str = ""
    fallback_reason_detail: str = ""
    fallback_risk: str = ""


class OrderChanceSchemaError(RuntimeError):
    """Raised when /v1/orders/chance response violates documented schema."""


class OrderChanceMarketMismatchError(OrderChanceSchemaError):
    """Raised when /v1/orders/chance response market does not match request market."""


def side_min_total_krw(*, rules: DerivedOrderConstraints, side: str) -> float:
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY":
        if float(rules.bid_min_total_krw) > 0:
            return float(rules.bid_min_total_krw)
    elif normalized_side == "SELL":
        if float(rules.ask_min_total_krw) > 0:
            return float(rules.ask_min_total_krw)
    return float(rules.min_notional_krw)


def side_price_unit(*, rules: DerivedOrderConstraints, side: str) -> float:
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY":
        return float(rules.bid_price_unit)
    if normalized_side == "SELL":
        return float(rules.ask_price_unit)
    return 0.0


def normalize_limit_price_for_side(*, price: float, side: str, rules: DerivedOrderConstraints) -> float:
    value = float(price)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"limit price must be > 0 (got {price})")

    unit = side_price_unit(rules=rules, side=side)
    if not math.isfinite(unit) or unit <= 0:
        return value

    steps = value / unit
    epsilon = 1e-12
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "SELL":
        aligned_steps = math.ceil(steps - epsilon)
    else:
        aligned_steps = math.floor(steps + epsilon)
    return float(aligned_steps * unit)


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
    if normalized == "manual_config":
        # legacy source label -> canonical source label
        normalized = "local_fallback"
    return normalized if normalized in KNOWN_RULE_SOURCES else "missing"


def required_rule_source_issues(
    source: dict[str, str] | None, *, require_price_unit_sources: bool = True
) -> list[str]:
    issues: list[str] = []
    doc_required_fields: tuple[str, ...] = (
        "bid_min_total_krw",
        "ask_min_total_krw",
    )
    if require_price_unit_sources:
        doc_required_fields += ("bid_price_unit", "ask_price_unit")
    for field in doc_required_fields:
        field_source = rule_source_for(field, source)
        if field_source != "chance_doc":
            issues.append(
                f"{field} source must be chance_doc for MODE=live "
                f"(got {field_source})"
            )
    return issues


def optional_rule_source_warnings(source: dict[str, str] | None) -> list[str]:
    warnings: list[str] = []
    for field in ("bid_price_unit", "ask_price_unit"):
        field_source = rule_source_for(field, source)
        if field_source != "chance_doc":
            warnings.append(
                f"{field} source is {field_source}; limit price tick normalization may be pass-through"
            )
    return warnings


def _local_fallback_constraints() -> LocalFallbackConstraints:
    return LocalFallbackConstraints(
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    )


def _local_fallback_rules() -> DerivedOrderConstraints:
    fallback = _local_fallback_constraints()
    return DerivedOrderConstraints(
        min_qty=fallback.min_qty,
        qty_step=fallback.qty_step,
        min_notional_krw=fallback.min_notional_krw,
        max_qty_decimals=fallback.max_qty_decimals,
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


def _optional_positive_number(payload: dict[str, Any], key: str, *, where: str) -> float | None:
    if key not in payload or payload.get(key) is None:
        return None
    return _require_positive_number(payload, key, where=where)


def parse_order_chance_response(payload: dict[str, Any], *, requested_market: str) -> OrderChanceResponse:
    fee_values = {
        fee_field: _require_positive_number(payload, fee_field, where="response")
        for fee_field in ("bid_fee", "ask_fee", "maker_bid_fee", "maker_ask_fee")
    }

    market = _require_dict(payload, "market", where="response")
    market_id = parse_documented_market_code(_require_non_empty_str(market, "id", where="response.market"))
    normalized_requested_market = parse_documented_market_code(requested_market)
    if market_id != normalized_requested_market:
        raise OrderChanceMarketMismatchError(
            "/v1/orders/chance response.market.id mismatch: "
            f"requested={normalized_requested_market} response={market_id}"
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
            price_unit=_optional_positive_number(bid, "price_unit", where="response.market.bid"),
            min_total=_require_positive_number(bid, "min_total", where="response.market.bid"),
        ),
        ask=OrderChanceSide(
            price_unit=_optional_positive_number(ask, "price_unit", where="response.market.ask"),
            min_total=_require_positive_number(ask, "min_total", where="response.market.ask"),
        ),
        bid_fee=fee_values["bid_fee"],
        ask_fee=fee_values["ask_fee"],
        maker_bid_fee=fee_values["maker_bid_fee"],
        maker_ask_fee=fee_values["maker_ask_fee"],
    )


def derive_order_rules_from_chance(response: OrderChanceResponse) -> ExchangeDerivedConstraints:
    return ExchangeDerivedConstraints(
        market_id=response.market_id,
        bid_min_total_krw=response.bid.min_total,
        ask_min_total_krw=response.ask.min_total,
        bid_price_unit=float(response.bid.price_unit or 0.0),
        ask_price_unit=float(response.ask.price_unit or 0.0),
        order_types=response.order_types,
        order_sides=response.order_sides,
        bid_fee=response.bid_fee,
        ask_fee=response.ask_fee,
        maker_bid_fee=response.maker_bid_fee,
        maker_ask_fee=response.maker_ask_fee,
    )


def fetch_exchange_order_rules(pair: str) -> ExchangeDerivedConstraints:
    try:
        market = parse_documented_market_code(pair)
    except ExchangeMarketCodeError as exc:
        raise OrderChanceSchemaError(
            f"/v1/orders/chance request market must be canonical QUOTE-BASE: {pair!r}"
        ) from exc
    payload = BithumbBroker().get_order_chance(market=market)

    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected order rules payload type: {type(payload).__name__}")

    chance = parse_order_chance_response(payload, requested_market=market)
    return derive_order_rules_from_chance(chance)


def get_effective_order_rules(pair: str) -> RuleResolution:
    now = time.time()
    fallback = _local_fallback_rules()

    cached = _cached_rules.get(pair)
    if cached and now - cached[0] < _CACHE_TTL_SEC and cached[2] == fallback:
        return cached[1]
    try:
        exchange = fetch_exchange_order_rules(pair)
    except Exception as exc:
        code, summary = classify_private_api_error(exc)
        detail = f"{type(exc).__name__}: {exc}"
        fallback_risk = (
            "order-rule auto-sync unavailable; side minimum totals, fees, and tick-size normalization "
            "may stay on local fallback until /v1/orders/chance succeeds again"
        )
        notify(
            f"[WARN] order rules auto-sync failed for {pair}; using local fallback only "
            f"(reason_code={code}; reason={summary}; detail={detail}; risk={fallback_risk})"
        )
        source = {
            "min_qty": "local_fallback",
            "qty_step": "local_fallback",
            "min_notional_krw": "local_fallback",
            "max_qty_decimals": "local_fallback",
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
            "ruleset": "merged",
        }
        resolution = RuleResolution(
            rules=fallback,
            source=source,
            fallback_used=True,
            fallback_reason_code=code,
            fallback_reason_summary=summary,
            fallback_reason_detail=detail,
            fallback_risk=fallback_risk,
        )
        _cached_rules[pair] = (now, resolution, fallback)
        return resolution

    merged = DerivedOrderConstraints(
        market_id=exchange.market_id,
        bid_min_total_krw=exchange.bid_min_total_krw,
        ask_min_total_krw=exchange.ask_min_total_krw,
        bid_price_unit=exchange.bid_price_unit,
        ask_price_unit=exchange.ask_price_unit,
        order_types=exchange.order_types,
        order_sides=exchange.order_sides,
        bid_fee=exchange.bid_fee,
        ask_fee=exchange.ask_fee,
        maker_bid_fee=exchange.maker_bid_fee,
        maker_ask_fee=exchange.maker_ask_fee,
        min_qty=fallback.min_qty,
        qty_step=fallback.qty_step,
        min_notional_krw=fallback.min_notional_krw,
        max_qty_decimals=fallback.max_qty_decimals,
    )
    source = {
        "market_id": "chance_doc",
        "bid_min_total_krw": "chance_doc",
        "ask_min_total_krw": "chance_doc",
        "bid_price_unit": "chance_doc" if exchange.bid_price_unit > 0 else "missing",
        "ask_price_unit": "chance_doc" if exchange.ask_price_unit > 0 else "missing",
        "order_types": "chance_doc",
        "order_sides": "chance_doc",
        "bid_fee": "chance_doc",
        "ask_fee": "chance_doc",
        "maker_bid_fee": "chance_doc",
        "maker_ask_fee": "chance_doc",
        "min_qty": "local_fallback",
        "qty_step": "local_fallback",
        "min_notional_krw": "local_fallback",
        "max_qty_decimals": "local_fallback",
        "ruleset": "merged",
    }
    resolution = RuleResolution(rules=merged, source=source)
    _cached_rules[pair] = (now, resolution, fallback)
    return resolution

OrderRules = DerivedOrderConstraints
