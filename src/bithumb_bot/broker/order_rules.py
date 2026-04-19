from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from ..config import settings
from ..markets import ExchangeMarketCodeError, canonical_market_id, canonical_market_with_raw, parse_documented_market_code
from ..notifier import notify
from .base import BrokerRejectError
from .bithumb import BithumbBroker
from .bithumb import classify_private_api_error
from .buy_price_none_policy import (
    BUY_PRICE_NONE_ALIAS_POLICY,
    BuyPriceNoneResolution,
    BuyPriceNoneSubmitContract,
    BuyPriceNoneSubmitPolicy,
    build_buy_price_none_diagnostic_fields,
    build_buy_price_none_submit_contract,
    build_buy_price_none_submit_contract_context,
    buy_price_none_alias_policy,
    buy_price_none_submit_contract_mismatch,
    raw_supported_order_types_for_chance_validation,
    resolve_buy_price_none_resolution,
    resolve_buy_price_none_submit_policy,
    serialize_buy_price_none_submit_contract,
    supported_order_types_for_chance_validation,
    validate_buy_price_none_order_chance_contract,
    validate_buy_price_none_submit_contract,
    validate_order_chance_support,
)
from .order_chance_source import (
    ExchangeDerivedConstraints,
    OrderChanceMarketMismatchError as SourceOrderChanceMarketMismatchError,
    OrderChanceResponse,
    OrderChanceSchemaError as SourceOrderChanceSchemaError,
    OrderChanceSide,
    derive_order_rules_from_chance,
    parse_order_chance_response,
)
from .order_rule_persistence import (
    derived_rules_from_snapshot_payload as persistence_derived_rules_from_snapshot_payload,
    detect_chance_contract_change as persistence_detect_chance_contract_change,
    persist_rule_snapshot_if_possible as persistence_persist_rule_snapshot_if_possible,
    resolution_from_persisted_snapshot as persistence_resolution_from_persisted_snapshot,
    tracked_chance_contract_snapshot_from_payload as persistence_tracked_chance_contract_snapshot_from_payload,
    tracked_chance_contract_snapshot_from_rules as persistence_tracked_chance_contract_snapshot_from_rules,
)
from .rule_policy import (
    KNOWN_RULE_SOURCES,
    normalize_limit_price_for_side,
    optional_rule_source_warnings,
    required_rule_issues,
    required_rule_source_issues,
    rule_source_for,
    side_min_total_krw,
    side_price_unit,
)

_CACHE_TTL_SEC = 300.0
_cached_rules: dict[str, tuple[float, "RuleResolution", "DerivedOrderConstraints"]] = {}
TRACKED_CHANCE_CONTRACT_FIELDS = ("order_types", "bid_types", "ask_types", "order_sides")

@dataclass(frozen=True)
class DerivedOrderConstraints:
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
    min_qty: float = 0.0
    qty_step: float = 0.0
    min_notional_krw: float = 0.0
    max_qty_decimals: int = 0


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
    exchange_source: dict[str, str] = field(default_factory=dict)
    local_fallback_source: dict[str, str] = field(default_factory=dict)
    fallback_used: bool = False
    fallback_reason_code: str = ""
    fallback_reason_summary: str = ""
    fallback_reason_detail: str = ""
    fallback_risk: str = ""
    retrieved_at_sec: float = 0.0
    expires_at_sec: float = 0.0
    stale: bool = False
    source_mode: str = "exchange"
    snapshot_persisted: bool = False
    chance_contract_change: "ChanceContractChange | None" = None

    def is_stale(self, *, now_sec: float | None = None) -> bool:
        if self.stale:
            return True
        if self.expires_at_sec <= 0:
            return False
        current = float(time.time() if now_sec is None else now_sec)
        return current >= float(self.expires_at_sec)


class OrderChanceSchemaError(RuntimeError):
    """Raised when /v1/orders/chance response violates documented schema."""


class OrderChanceMarketMismatchError(OrderChanceSchemaError):
    """Raised when /v1/orders/chance response market does not match request market."""


@dataclass(frozen=True)
class ChanceContractChange:
    detected: bool
    changed_fields: dict[str, dict[str, tuple[str, ...]]] = field(default_factory=dict)
    previous_snapshot: dict[str, tuple[str, ...]] = field(default_factory=dict)
    current_snapshot: dict[str, tuple[str, ...]] = field(default_factory=dict)
    previous_fetched_ts: int = 0


def _local_fallback_constraints() -> LocalFallbackConstraints:
    return LocalFallbackConstraints(
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    )


def fetch_exchange_order_rules(pair: str) -> ExchangeDerivedConstraints:
    from .order_rule_resolution import fetch_exchange_order_rules as resolve_exchange_order_rules

    return resolve_exchange_order_rules(pair)


def _local_fallback_rules() -> DerivedOrderConstraints:
    fallback = _local_fallback_constraints()
    return DerivedOrderConstraints(
        order_types=("limit", "price", "market"),
        bid_types=("price",),
        ask_types=("limit", "market"),
        order_sides=("bid", "ask"),
        min_qty=fallback.min_qty,
        qty_step=fallback.qty_step,
        min_notional_krw=fallback.min_notional_krw,
        max_qty_decimals=fallback.max_qty_decimals,
    )


def _exchange_rule_source_map(exchange: ExchangeDerivedConstraints) -> dict[str, str]:
    return {
        "market_id": "chance_doc",
        "bid_min_total_krw": "chance_doc",
        "ask_min_total_krw": "chance_doc",
        "bid_price_unit": "chance_doc" if exchange.bid_price_unit > 0 else "missing",
        "ask_price_unit": "chance_doc" if exchange.ask_price_unit > 0 else "missing",
        "order_types": "chance_doc",
        "bid_types": "chance_doc",
        "ask_types": "chance_doc",
        "order_sides": "chance_doc",
        "bid_fee": "chance_doc",
        "ask_fee": "chance_doc",
        "maker_bid_fee": "chance_doc",
        "maker_ask_fee": "chance_doc",
    }


def _fallback_rule_source_map() -> dict[str, str]:
    return {
        "min_qty": "local_fallback",
        "qty_step": "local_fallback",
        "min_notional_krw": "local_fallback",
        "max_qty_decimals": "local_fallback",
    }


def _build_fallback_only_rule_resolution(
    *,
    pair: str,
    now: float,
    fallback: DerivedOrderConstraints,
    reason_code: str,
    reason_summary: str,
    reason_detail: str,
    fallback_risk: str,
) -> RuleResolution:
    source = {
        **_fallback_rule_source_map(),
        "market_id": "unsupported_by_doc",
        "bid_min_total_krw": "unsupported_by_doc",
        "ask_min_total_krw": "unsupported_by_doc",
        "bid_price_unit": "unsupported_by_doc",
        "ask_price_unit": "unsupported_by_doc",
        "order_types": "unsupported_by_doc",
        "bid_types": "unsupported_by_doc",
        "ask_types": "unsupported_by_doc",
        "order_sides": "unsupported_by_doc",
        "bid_fee": "unsupported_by_doc",
        "ask_fee": "unsupported_by_doc",
        "maker_bid_fee": "unsupported_by_doc",
        "maker_ask_fee": "unsupported_by_doc",
        "ruleset": "merged",
        "exchange_source_json": json.dumps({}, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        "local_fallback_source_json": json.dumps(
            _fallback_rule_source_map(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    resolution = RuleResolution(
        rules=fallback,
        source=source,
        exchange_source={},
        local_fallback_source=_fallback_rule_source_map(),
        fallback_used=True,
        fallback_reason_code=reason_code,
        fallback_reason_summary=reason_summary,
        fallback_reason_detail=reason_detail,
        fallback_risk=fallback_risk,
        retrieved_at_sec=now,
        expires_at_sec=now + _CACHE_TTL_SEC,
        stale=False,
        source_mode="local_fallback",
    )
    if settings.MODE == "live":
        import logging

        logging.getLogger(__name__).warning(
            "live order rule snapshot fallback engaged pair=%s retrieved_at_sec=%.3f expires_at_sec=%.3f reason_code=%s reason=%s source_min_qty=%s source_qty_step=%s source_min_notional=%s source_max_qty_decimals=%s",
            pair,
            resolution.retrieved_at_sec,
            resolution.expires_at_sec,
            reason_code,
            reason_summary,
            resolution.source.get("min_qty", "missing"),
            resolution.source.get("qty_step", "missing"),
            resolution.source.get("min_notional_krw", "missing"),
            resolution.source.get("max_qty_decimals", "missing"),
        )
    return resolution


def _build_merged_rule_resolution(
    *,
    pair: str,
    now: float,
    exchange: ExchangeDerivedConstraints,
    fallback: DerivedOrderConstraints,
) -> RuleResolution:
    merged = DerivedOrderConstraints(
        market_id=exchange.market_id,
        bid_min_total_krw=exchange.bid_min_total_krw,
        ask_min_total_krw=exchange.ask_min_total_krw,
        bid_price_unit=exchange.bid_price_unit,
        ask_price_unit=exchange.ask_price_unit,
        order_types=exchange.order_types,
        bid_types=exchange.bid_types,
        ask_types=exchange.ask_types,
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
    exchange_source = _exchange_rule_source_map(exchange)
    local_fallback_source = _fallback_rule_source_map()
    source = {
        **exchange_source,
        **local_fallback_source,
        "ruleset": "merged",
        "exchange_source_json": json.dumps(
            exchange_source,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "local_fallback_source_json": json.dumps(
            local_fallback_source,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    resolution = RuleResolution(
        rules=merged,
        source=source,
        exchange_source=exchange_source,
        local_fallback_source=local_fallback_source,
        retrieved_at_sec=now,
        expires_at_sec=now + _CACHE_TTL_SEC,
        stale=False,
        source_mode="merged",
    )
    if settings.MODE == "live":
        import logging

        logging.getLogger(__name__).info(
            "live order rule snapshot refreshed pair=%s retrieved_at_sec=%.3f expires_at_sec=%.3f source=exchange source_min_qty=%s source_qty_step=%s source_min_notional=%s source_max_qty_decimals=%s",
            pair,
            resolution.retrieved_at_sec,
            resolution.expires_at_sec,
            resolution.source.get("min_qty", "missing"),
            resolution.source.get("qty_step", "missing"),
            resolution.source.get("min_notional_krw", "missing"),
            resolution.source.get("max_qty_decimals", "missing"),
        )
    return resolution


def _derived_rules_from_snapshot_payload(payload: dict[str, object]) -> DerivedOrderConstraints:
    return persistence_derived_rules_from_snapshot_payload(
        payload,
        derived_rules_cls=DerivedOrderConstraints,
    )


def _resolution_from_persisted_snapshot(*, pair: str) -> RuleResolution | None:
    return persistence_resolution_from_persisted_snapshot(
        pair=pair,
        derived_rules_cls=DerivedOrderConstraints,
        rule_resolution_cls=RuleResolution,
    )


def get_effective_order_rules(pair: str) -> RuleResolution:
    from .order_rule_resolution import get_effective_order_rules as resolve_effective_order_rules

    return resolve_effective_order_rules(pair)


def get_cached_order_rule_snapshot(pair: str) -> RuleResolution | None:
    cached = _cached_rules.get(pair)
    if not cached:
        return None
    return cached[1]


def _coerce_tracked_contract_tokens(value: object) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    if value is None:
        return ()
    return (str(value),)


def _tracked_chance_contract_snapshot_from_payload(payload: dict[str, object] | None) -> dict[str, tuple[str, ...]]:
    return persistence_tracked_chance_contract_snapshot_from_payload(
        payload,
        tracked_fields=TRACKED_CHANCE_CONTRACT_FIELDS,
    )


def _tracked_chance_contract_snapshot_from_rules(rules: DerivedOrderConstraints) -> dict[str, tuple[str, ...]]:
    return persistence_tracked_chance_contract_snapshot_from_rules(
        rules,
        tracked_fields=TRACKED_CHANCE_CONTRACT_FIELDS,
    )


def _detect_chance_contract_change(
    *,
    previous_rules_payload: dict[str, object] | None,
    current_rules_payload: dict[str, object],
    previous_fetched_ts: int = 0,
) -> ChanceContractChange | None:
    return persistence_detect_chance_contract_change(
        previous_rules_payload=previous_rules_payload,
        current_rules_payload=current_rules_payload,
        previous_fetched_ts=previous_fetched_ts,
        tracked_fields=TRACKED_CHANCE_CONTRACT_FIELDS,
        chance_contract_change_cls=ChanceContractChange,
    )


def _persist_rule_snapshot_if_possible(resolution: RuleResolution) -> RuleResolution:
    return persistence_persist_rule_snapshot_if_possible(
        resolution,
        tracked_fields=TRACKED_CHANCE_CONTRACT_FIELDS,
        chance_contract_change_cls=ChanceContractChange,
        rule_resolution_cls=RuleResolution,
    )

OrderRules = DerivedOrderConstraints
