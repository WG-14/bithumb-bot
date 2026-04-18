from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from ..markets import ExchangeMarketCodeError, parse_documented_market_code
from .bithumb import BithumbBroker


@dataclass(frozen=True)
class OrderChanceSide:
    price_unit: float | None
    min_total: float


@dataclass(frozen=True)
class OrderChanceResponse:
    market_id: str
    order_types: tuple[str, ...]
    bid_types: tuple[str, ...]
    ask_types: tuple[str, ...]
    order_sides: tuple[str, ...]
    bid: OrderChanceSide
    ask: OrderChanceSide
    bid_fee: float
    ask_fee: float
    maker_bid_fee: float
    maker_ask_fee: float


@dataclass(frozen=True)
class ExchangeDerivedConstraints:
    market_id: str = ""
    bid_min_total_krw: float = 0.0
    ask_min_total_krw: float = 0.0
    bid_price_unit: float = 0.0
    ask_price_unit: float = 0.0
    order_types: tuple[str, ...] = ()
    bid_types: tuple[str, ...] = ()
    ask_types: tuple[str, ...] = ()
    order_sides: tuple[str, ...] = ()
    bid_fee: float = 0.0
    ask_fee: float = 0.0
    maker_bid_fee: float = 0.0
    maker_ask_fee: float = 0.0


class OrderChanceSchemaError(RuntimeError):
    """Raised when /v1/orders/chance response violates documented schema."""


class OrderChanceMarketMismatchError(OrderChanceSchemaError):
    """Raised when /v1/orders/chance response market does not match request market."""


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
    _require_non_empty_list(market, "bid_types", where="response.market")
    _require_non_empty_list(market, "ask_types", where="response.market")
    _require_non_empty_list(market, "order_sides", where="response.market")

    bid = _require_dict(market, "bid", where="response.market")
    ask = _require_dict(market, "ask", where="response.market")

    return OrderChanceResponse(
        market_id=market_id,
        order_types=tuple(str(item) for item in _require_non_empty_list(market, "order_types", where="response.market")),
        bid_types=tuple(str(item) for item in _require_non_empty_list(market, "bid_types", where="response.market")),
        ask_types=tuple(str(item) for item in _require_non_empty_list(market, "ask_types", where="response.market")),
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
        bid_types=response.bid_types,
        ask_types=response.ask_types,
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
