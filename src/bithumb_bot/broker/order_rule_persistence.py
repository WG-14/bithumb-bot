from __future__ import annotations

import json

from ..config import settings
from ..db_core import ensure_db, fetch_latest_order_rule_snapshot, record_order_rule_snapshot
from ..markets import canonical_market_with_raw, parse_documented_market_code


def derived_rules_from_snapshot_payload(payload: dict[str, object], *, derived_rules_cls):
    default_rules = derived_rules_cls()
    sequence_fields = {
        "order_types",
        "bid_types",
        "ask_types",
        "order_sides",
    }
    numeric_float_fields = {
        "bid_min_total_krw",
        "ask_min_total_krw",
        "bid_price_unit",
        "ask_price_unit",
        "bid_fee",
        "ask_fee",
        "maker_bid_fee",
        "maker_ask_fee",
        "min_qty",
        "qty_step",
        "min_notional_krw",
    }
    numeric_int_fields = {"max_qty_decimals"}
    normalized: dict[str, object] = {}
    for field_name in default_rules.__dataclass_fields__.keys():
        raw_value = payload.get(field_name, getattr(default_rules, field_name))
        if field_name in sequence_fields:
            if isinstance(raw_value, (list, tuple)):
                normalized[field_name] = tuple(str(item) for item in raw_value)
            elif raw_value in (None, ""):
                normalized[field_name] = ()
            else:
                normalized[field_name] = (str(raw_value),)
        elif field_name in numeric_float_fields:
            normalized[field_name] = float(raw_value or 0.0)
        elif field_name in numeric_int_fields:
            normalized[field_name] = int(raw_value or 0)
        else:
            normalized[field_name] = str(raw_value or "")
    return derived_rules_cls(**normalized)


def resolution_from_persisted_snapshot(
    *,
    pair: str,
    derived_rules_cls,
    rule_resolution_cls,
):
    conn = None
    try:
        conn = ensure_db()
        record = fetch_latest_order_rule_snapshot(conn, market=pair)
        if record is None:
            return None
        rules_payload = json.loads(record.rules_json)
        source_payload = json.loads(record.source_json)
        exchange_source = {}
        local_fallback_source = {}
        try:
            exchange_source = json.loads(str(source_payload.get("exchange_source_json") or "{}"))
        except Exception:
            exchange_source = {}
        try:
            local_fallback_source = json.loads(str(source_payload.get("local_fallback_source_json") or "{}"))
        except Exception:
            local_fallback_source = {}
        retrieved_at_sec = float(record.fetched_ts) / 1000.0 if int(record.fetched_ts) > 0 else 0.0
        return rule_resolution_cls(
            rules=derived_rules_from_snapshot_payload(rules_payload, derived_rules_cls=derived_rules_cls),
            source=dict(source_payload),
            exchange_source=exchange_source if isinstance(exchange_source, dict) else {},
            local_fallback_source=local_fallback_source if isinstance(local_fallback_source, dict) else {},
            fallback_used=bool(record.fallback_used),
            fallback_reason_code=str(record.fallback_reason_code or ""),
            fallback_reason_summary=str(record.fallback_reason_summary or ""),
            fallback_reason_detail="",
            fallback_risk="",
            retrieved_at_sec=retrieved_at_sec,
            expires_at_sec=0.0,
            stale=False,
            source_mode=str(record.source_mode or "merged"),
            snapshot_persisted=True,
        )
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def coerce_tracked_contract_tokens(value: object) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    if value is None:
        return ()
    return (str(value),)


def tracked_chance_contract_snapshot_from_payload(
    payload: dict[str, object] | None,
    *,
    tracked_fields: tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    source = payload or {}
    return {
        field: coerce_tracked_contract_tokens(source.get(field))
        for field in tracked_fields
    }


def tracked_chance_contract_snapshot_from_rules(rules, *, tracked_fields: tuple[str, ...]) -> dict[str, tuple[str, ...]]:
    return tracked_chance_contract_snapshot_from_payload(
        {
            field: getattr(rules, field, ())
            for field in tracked_fields
        },
        tracked_fields=tracked_fields,
    )


def detect_chance_contract_change(
    *,
    previous_rules_payload: dict[str, object] | None,
    current_rules_payload: dict[str, object],
    previous_fetched_ts: int = 0,
    tracked_fields: tuple[str, ...],
    chance_contract_change_cls,
):
    if previous_rules_payload is None:
        return None
    previous_snapshot = tracked_chance_contract_snapshot_from_payload(
        previous_rules_payload,
        tracked_fields=tracked_fields,
    )
    current_snapshot = tracked_chance_contract_snapshot_from_payload(
        current_rules_payload,
        tracked_fields=tracked_fields,
    )
    changed_fields = {
        field: {
            "previous": previous_snapshot[field],
            "current": current_snapshot[field],
        }
        for field in tracked_fields
        if previous_snapshot[field] != current_snapshot[field]
    }
    return chance_contract_change_cls(
        detected=bool(changed_fields),
        changed_fields=changed_fields,
        previous_snapshot=previous_snapshot,
        current_snapshot=current_snapshot,
        previous_fetched_ts=int(previous_fetched_ts or 0),
    )


def persist_rule_snapshot_if_possible(
    resolution,
    *,
    tracked_fields: tuple[str, ...],
    chance_contract_change_cls,
    rule_resolution_cls,
):
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
        chance_contract_change = detect_chance_contract_change(
            previous_rules_payload=previous_rules_payload,
            current_rules_payload=rules_payload,
            previous_fetched_ts=(
                previous_snapshot_record.fetched_ts
                if previous_snapshot_record is not None
                else 0
            ),
            tracked_fields=tracked_fields,
            chance_contract_change_cls=chance_contract_change_cls,
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
        return rule_resolution_cls(
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
