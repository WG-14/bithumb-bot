from __future__ import annotations

from typing import Iterable, Mapping


INCIDENT_OUT_OF_WINDOW_TARGET_DELTA_ENTRY = "out_of_window_target_delta_entry"


def require_buy_authority_source(payload: Mapping[str, object]) -> str:
    side = str(payload.get("side") or payload.get("target_delta_side") or "").strip().upper()
    if side != "BUY":
        return str(payload.get("authority_source") or payload.get("entry_authority_source") or "not_buy")
    authority = str(
        payload.get("entry_authority_source")
        or payload.get("authority_source")
        or payload.get("entry_authority_reason_code")
        or ""
    ).strip()
    if not authority:
        raise ValueError("buy_order_authority_source_missing")
    if authority == "target_delta" and str(payload.get("entry_authority_status") or "") != "ALLOW":
        raise ValueError("target_delta_buy_without_entry_authority")
    return authority


def classify_h74_live_trade(payload: Mapping[str, object]) -> dict[str, object]:
    side = str(payload.get("side") or payload.get("target_delta_side") or "").strip().upper()
    reason = str(payload.get("decision_reason_code") or payload.get("intent_type") or "").strip()
    authority = require_buy_authority_source(payload) if side == "BUY" else "not_buy"
    kst_hour = payload.get("decision_kst_hour")
    try:
        hour = int(kst_hour) if kst_hour is not None else None
    except (TypeError, ValueError):
        hour = None
    daily_entry = authority in {"daily_participation_entry", "daily_participation_fallback_allowed"}
    in_h74_window = hour is not None and 9 <= hour < 11
    out_of_window_target_delta = side == "BUY" and reason == "target_delta_rebalance" and not daily_entry
    incident_type = INCIDENT_OUT_OF_WINDOW_TARGET_DELTA_ENTRY if out_of_window_target_delta else "none"
    return {
        "live_plumbing_success": bool(payload.get("filled") or payload.get("exchange_order_id")),
        "h74_backtest_validation_sample": bool(side == "BUY" and daily_entry and in_h74_window),
        "incident_type": incident_type,
        "entry_authority_source": authority,
    }


def h74_performance_samples(records: Iterable[Mapping[str, object]]) -> list[Mapping[str, object]]:
    return [
        record
        for record in records
        if bool(classify_h74_live_trade(record).get("h74_backtest_validation_sample"))
    ]
