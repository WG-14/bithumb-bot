from __future__ import annotations

import time

from ..config import settings
from ..config import (
    LIVE_ORDER_RULE_FALLBACK_PROFILE_ALLOW_LOCAL_FALLBACK,
    LIVE_ORDER_RULE_FALLBACK_PROFILE_PERSISTED_SNAPSHOT_REQUIRED,
)
from ..markets import ExchangeMarketCodeError, canonical_market_with_raw, parse_documented_market_code
from .base import BrokerRejectError
from .order_chance_source import (
    OrderChanceMarketMismatchError as SourceOrderChanceMarketMismatchError,
    OrderChanceSchemaError as SourceOrderChanceSchemaError,
    derive_order_rules_from_chance,
    parse_order_chance_response,
)


def fetch_exchange_order_rules(pair: str):
    from . import order_rules as rules_module

    try:
        market = parse_documented_market_code(pair)
    except ExchangeMarketCodeError as exc:
        raise rules_module.OrderChanceSchemaError(
            f"/v1/orders/chance request market must be canonical QUOTE-BASE: {pair!r}"
        ) from exc
    payload = rules_module.BithumbBroker().get_order_chance(market=market)
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected order rules payload type: {type(payload).__name__}")
    try:
        chance = parse_order_chance_response(payload, requested_market=market)
    except SourceOrderChanceMarketMismatchError as exc:
        raise rules_module.OrderChanceMarketMismatchError(str(exc)) from exc
    except SourceOrderChanceSchemaError as exc:
        raise rules_module.OrderChanceSchemaError(str(exc)) from exc
    return derive_order_rules_from_chance(chance)


def get_effective_order_rules(pair: str):
    from . import order_rules as rules_module

    normalized_pair, _raw_pair = canonical_market_with_raw(pair)
    now = time.time()
    fallback = rules_module._local_fallback_rules()
    fallback_profile = str(settings.LIVE_ORDER_RULE_FALLBACK_PROFILE or "").strip()

    cached = rules_module._cached_rules.get(normalized_pair)
    if cached and now - cached[0] < rules_module._CACHE_TTL_SEC and cached[2] == fallback:
        return cached[1]
    try:
        exchange = rules_module.fetch_exchange_order_rules(normalized_pair)
    except Exception as exc:
        if (
            settings.MODE == "live"
            and not bool(settings.LIVE_DRY_RUN)
            and fallback_profile == LIVE_ORDER_RULE_FALLBACK_PROFILE_PERSISTED_SNAPSHOT_REQUIRED
        ):
            persisted_resolution = rules_module._resolution_from_persisted_snapshot(pair=normalized_pair)
            if persisted_resolution is not None:
                rules_module._cached_rules[normalized_pair] = (now, persisted_resolution, fallback)
                return persisted_resolution
            fallback_issues = rules_module.required_rule_issues(fallback)
            if fallback_issues:
                raise BrokerRejectError(
                    f"live order rule fallback invalid for {pair}: " + "; ".join(fallback_issues)
                ) from exc
            code, summary = rules_module.classify_private_api_error(exc)
            detail = f"{type(exc).__name__}: {exc}"
            raise BrokerRejectError(
                f"live order rule snapshot unavailable for {pair}; persisted snapshot required "
                f"(reason_code={code}; reason={summary}; detail={detail})"
            ) from exc
        if settings.MODE == "live" and not bool(settings.LIVE_DRY_RUN):
            if fallback_profile != LIVE_ORDER_RULE_FALLBACK_PROFILE_ALLOW_LOCAL_FALLBACK:
                raise BrokerRejectError(
                    "live order rule fallback profile invalid: "
                    f"{fallback_profile!r}"
                ) from exc
        code, summary = rules_module.classify_private_api_error(exc)
        detail = f"{type(exc).__name__}: {exc}"
        fallback_risk = (
            "order-rule auto-sync unavailable; side minimum totals, fees, and tick-size normalization "
            "may stay on local fallback until /v1/orders/chance succeeds again"
        )
        rules_module.notify(
            f"[WARN] order rules auto-sync failed for {pair}; using local fallback only "
            f"(reason_code={code}; reason={summary}; detail={detail}; risk={fallback_risk})"
        )
        resolution = rules_module._build_fallback_only_rule_resolution(
            pair=pair,
            now=now,
            fallback=fallback,
            reason_code=code,
            reason_summary=summary,
            reason_detail=detail,
            fallback_risk=fallback_risk,
        )
        resolution = rules_module._persist_rule_snapshot_if_possible(resolution)
        rules_module._cached_rules[normalized_pair] = (now, resolution, fallback)
        return resolution

    resolution = rules_module._build_merged_rule_resolution(
        pair=pair,
        now=now,
        exchange=exchange,
        fallback=fallback,
    )
    resolution = rules_module._persist_rule_snapshot_if_possible(resolution)
    rules_module._cached_rules[normalized_pair] = (now, resolution, fallback)
    return resolution
