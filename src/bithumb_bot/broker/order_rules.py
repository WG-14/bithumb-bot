from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from ..config import settings
from ..db_core import ensure_db, fetch_latest_order_rule_snapshot, record_order_rule_snapshot
from ..markets import canonical_market_id, canonical_market_with_raw, parse_documented_market_code
from ..notifier import notify
from .base import BrokerRejectError
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
    OrderChanceMarketMismatchError,
    OrderChanceResponse,
    OrderChanceSchemaError,
    OrderChanceSide,
    derive_order_rules_from_chance,
    fetch_exchange_order_rules,
    parse_order_chance_response,
)

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


def get_effective_order_rules(pair: str) -> RuleResolution:
    normalized_pair, _raw_pair = canonical_market_with_raw(pair)
    now = time.time()
    fallback = _local_fallback_rules()

    cached = _cached_rules.get(normalized_pair)
    if cached and now - cached[0] < _CACHE_TTL_SEC and cached[2] == fallback:
        cached_resolution = cached[1]
        if (
            cached_resolution.fallback_used
            and settings.MODE == "live"
            and not bool(settings.LIVE_DRY_RUN)
            and not bool(settings.LIVE_ALLOW_ORDER_RULE_FALLBACK)
        ):
            raise BrokerRejectError(
                f"live order rule snapshot unavailable for {pair}; cached fallback is disabled"
            )
        return cached_resolution
    try:
        exchange = fetch_exchange_order_rules(normalized_pair)
    except Exception as exc:
        fallback_issues = required_rule_issues(fallback)
        code, summary = classify_private_api_error(exc)
        detail = f"{type(exc).__name__}: {exc}"
        fallback_risk = (
            "order-rule auto-sync unavailable; side minimum totals, fees, and tick-size normalization "
            "may stay on local fallback until /v1/orders/chance succeeds again"
        )
        if fallback_issues and settings.MODE == "live" and not bool(settings.LIVE_DRY_RUN):
            raise BrokerRejectError(
                f"live order rule fallback invalid for {pair}: " + "; ".join(fallback_issues)
            ) from exc
        if settings.MODE == "live" and not bool(settings.LIVE_DRY_RUN) and not bool(settings.LIVE_ALLOW_ORDER_RULE_FALLBACK):
            raise BrokerRejectError(
                f"live order rule snapshot unavailable for {pair}; fallback disabled "
                f"(reason_code={code}; reason={summary}; detail={detail})"
            ) from exc
        notify(
            f"[WARN] order rules auto-sync failed for {pair}; using local fallback only "
            f"(reason_code={code}; reason={summary}; detail={detail}; risk={fallback_risk})"
        )
        resolution = _build_fallback_only_rule_resolution(
            pair=pair,
            now=now,
            fallback=fallback,
            reason_code=code,
            reason_summary=summary,
            reason_detail=detail,
            fallback_risk=fallback_risk,
        )
        resolution = _persist_rule_snapshot_if_possible(resolution)
        _cached_rules[normalized_pair] = (now, resolution, fallback)
        return resolution

    resolution = _build_merged_rule_resolution(
        pair=pair,
        now=now,
        exchange=exchange,
        fallback=fallback,
    )
    resolution = _persist_rule_snapshot_if_possible(resolution)
    _cached_rules[normalized_pair] = (now, resolution, fallback)
    return resolution


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
    source = payload or {}
    return {
        field: _coerce_tracked_contract_tokens(source.get(field))
        for field in TRACKED_CHANCE_CONTRACT_FIELDS
    }


def _tracked_chance_contract_snapshot_from_rules(rules: DerivedOrderConstraints) -> dict[str, tuple[str, ...]]:
    return _tracked_chance_contract_snapshot_from_payload(
        {
            field: getattr(rules, field, ())
            for field in TRACKED_CHANCE_CONTRACT_FIELDS
        }
    )


def _detect_chance_contract_change(
    *,
    previous_rules_payload: dict[str, object] | None,
    current_rules_payload: dict[str, object],
    previous_fetched_ts: int = 0,
) -> ChanceContractChange | None:
    if previous_rules_payload is None:
        return None
    previous_snapshot = _tracked_chance_contract_snapshot_from_payload(previous_rules_payload)
    current_snapshot = _tracked_chance_contract_snapshot_from_payload(current_rules_payload)
    changed_fields = {
        field: {
            "previous": previous_snapshot[field],
            "current": current_snapshot[field],
        }
        for field in TRACKED_CHANCE_CONTRACT_FIELDS
        if previous_snapshot[field] != current_snapshot[field]
    }
    return ChanceContractChange(
        detected=bool(changed_fields),
        changed_fields=changed_fields,
        previous_snapshot=previous_snapshot,
        current_snapshot=current_snapshot,
        previous_fetched_ts=int(previous_fetched_ts or 0),
    )


def _persist_rule_snapshot_if_possible(resolution: RuleResolution) -> RuleResolution:
    fallback_market, _raw_market = canonical_market_with_raw(settings.PAIR)
    market = str(getattr(resolution.rules, "market_id", "") or parse_documented_market_code(fallback_market))
    rules_payload = {
        field: getattr(resolution.rules, field)
        for field in resolution.rules.__dataclass_fields__.keys()
    }
    source_payload = dict(resolution.source)
    source_payload["source_mode"] = str(resolution.source_mode)
    source_payload["exchange_source_json"] = json.dumps(
        resolution.exchange_source,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    source_payload["local_fallback_source_json"] = json.dumps(
        resolution.local_fallback_source,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    conn = None
    try:
        conn = ensure_db()
        previous_snapshot_record = fetch_latest_order_rule_snapshot(conn, market=market)
        previous_rules_payload = None
        if previous_snapshot_record is not None:
            try:
                previous_rules_payload = json.loads(previous_snapshot_record.rules_json)
            except Exception:
                previous_rules_payload = None
        chance_contract_change = _detect_chance_contract_change(
            previous_rules_payload=previous_rules_payload,
            current_rules_payload=rules_payload,
            previous_fetched_ts=(
                previous_snapshot_record.fetched_ts
                if previous_snapshot_record is not None
                else 0
            ),
        )
        record_order_rule_snapshot(
            conn,
            market=market,
            fetched_ts=int(max(0.0, float(resolution.retrieved_at_sec)) * 1000),
            source_mode=str(resolution.source_mode),
            fallback_used=bool(resolution.fallback_used),
            fallback_reason_code=str(resolution.fallback_reason_code or ""),
            fallback_reason_summary=str(resolution.fallback_reason_summary or ""),
            rules_payload=rules_payload,
            source_payload=source_payload,
        )
        conn.commit()
        return RuleResolution(
            rules=resolution.rules,
            source=resolution.source,
            exchange_source=resolution.exchange_source,
            local_fallback_source=resolution.local_fallback_source,
            fallback_used=resolution.fallback_used,
            fallback_reason_code=resolution.fallback_reason_code,
            fallback_reason_summary=resolution.fallback_reason_summary,
            fallback_reason_detail=resolution.fallback_reason_detail,
            fallback_risk=resolution.fallback_risk,
            retrieved_at_sec=resolution.retrieved_at_sec,
            expires_at_sec=resolution.expires_at_sec,
            stale=resolution.stale,
            source_mode=resolution.source_mode,
            snapshot_persisted=True,
            chance_contract_change=chance_contract_change,
        )
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return resolution
    finally:
        if conn is not None:
            conn.close()

OrderRules = DerivedOrderConstraints
