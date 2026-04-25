from __future__ import annotations

import math
import time
from typing import Any

from .config import settings
from .db_core import (
    compute_accounting_replay,
    existing_fill_fee_complete,
    get_fee_pending_accounting_repair_summary,
    load_matching_accounted_fill,
    normalize_cash_amount,
    normalize_asset_qty,
    record_broker_fill_observation,
    record_fee_pending_accounting_repair,
    set_portfolio_breakdown,
)
from .execution import apply_fill_and_trade, order_fill_tolerance
from .fee_authority import resolve_fee_authority_snapshot
from .lifecycle import rebuild_lifecycle_projections_from_trades, summarize_position_lots
from .oms import set_status


def _clean_text(value: object | None) -> str:
    return str(value or "").strip()


def _load_pending_observations(
    conn,
    *,
    client_order_id: str,
    fill_id: str | None,
    exchange_order_id: str | None,
) -> list[Any]:
    filters = ["client_order_id=?", "accounting_status='fee_pending'"]
    params: list[object] = [client_order_id]
    if fill_id:
        filters.append("fill_id=?")
        params.append(fill_id)
    if exchange_order_id:
        filters.append("exchange_order_id=?")
        params.append(exchange_order_id)
    where = " AND ".join(filters)
    return conn.execute(
        f"""
        SELECT id, event_ts, client_order_id, exchange_order_id, fill_id, fill_ts,
               side, price, qty, fee, fee_status, accounting_status, source,
               parse_warnings, raw_payload
        FROM broker_fill_observations
        WHERE {where}
        ORDER BY event_ts DESC, id DESC
        """,
        tuple(params),
    ).fetchall()


def build_fee_pending_accounting_repair_preview(
    conn,
    *,
    client_order_id: str,
    fill_id: str | None = None,
    exchange_order_id: str | None = None,
    fee: float | None = None,
    fee_provenance: str | None = None,
) -> dict[str, Any]:
    client_order_id_text = _clean_text(client_order_id)
    fill_id_text = _clean_text(fill_id) or None
    exchange_order_id_text = _clean_text(exchange_order_id) or None
    fee_value = None if fee is None else normalize_cash_amount(fee)
    fee_provenance_text = _clean_text(fee_provenance)
    try:
        fee_authority_evidence = resolve_fee_authority_snapshot(settings.PAIR).as_dict()
    except Exception as exc:
        fee_authority_evidence = {
            "unavailable": True,
            "error": f"{type(exc).__name__}: {exc}",
        }

    observations = _load_pending_observations(
        conn,
        client_order_id=client_order_id_text,
        fill_id=fill_id_text,
        exchange_order_id=exchange_order_id_text,
    )
    order = conn.execute(
        """
        SELECT client_order_id, exchange_order_id, status, side, qty_req, qty_filled
        FROM orders
        WHERE client_order_id=?
        """,
        (client_order_id_text,),
    ).fetchone()
    repair_summary = get_fee_pending_accounting_repair_summary(conn)

    reasons: list[str] = []
    if not client_order_id_text:
        reasons.append("client_order_id_required")
    if order is None:
        reasons.append("order_not_found")
    if not observations:
        reasons.append("fee_pending_observation_not_found")
    if len(observations) > 1 and not fill_id_text:
        reasons.append(f"ambiguous_fee_pending_observations={len(observations)}; specify --fill-id")
    if fee_value is None:
        reasons.append("fee_required")
    elif not math.isfinite(float(fee_value)) or float(fee_value) < 0.0:
        reasons.append("fee_must_be_non_negative_finite")
    if not fee_provenance_text:
        reasons.append("fee_provenance_required")

    observation = observations[0] if observations else None
    side = _clean_text(observation["side"]).upper() if observation is not None else ""
    price = float(observation["price"] or 0.0) if observation is not None else 0.0
    qty = float(observation["qty"] or 0.0) if observation is not None else 0.0
    fill_ts = int(observation["fill_ts"] or 0) if observation is not None else 0
    observation_fill_id = _clean_text(observation["fill_id"]) if observation is not None else ""
    observation_exchange_order_id = _clean_text(observation["exchange_order_id"]) if observation is not None else ""
    notional = price * qty if math.isfinite(price) and math.isfinite(qty) else 0.0

    if observation is not None:
        existing_fill = load_matching_accounted_fill(
            conn,
            client_order_id=client_order_id_text,
            fill_id=observation_fill_id or None,
            fill_ts=fill_ts,
            price=price,
            qty=qty,
        )
        if side not in {"BUY", "SELL"}:
            reasons.append(f"invalid_observed_side={side or 'none'}")
        if price <= 0.0:
            reasons.append("invalid_observed_price")
        if qty <= 0.0:
            reasons.append("invalid_observed_qty")
        if existing_fill_fee_complete(existing_fill):
            reasons.append("fill_already_accounted")
    else:
        existing_fill = None
    if order is not None and observation is not None:
        order_side = _clean_text(order["side"]).upper()
        if order_side != side:
            reasons.append(f"order_side_mismatch={order_side}!={side}")
        order_exchange_id = _clean_text(order["exchange_order_id"])
        if exchange_order_id_text and order_exchange_id and order_exchange_id != exchange_order_id_text:
            reasons.append("requested_exchange_order_id_mismatch")
    if (
        settings.MODE == "live"
        and fee_value is not None
        and notional >= max(0.0, float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW))
        and float(fee_value) <= 1e-12
    ):
        reasons.append("material_live_fill_requires_positive_fee")

    safe_to_apply = bool(observation is not None and not reasons)
    eligibility_reason = "fee-pending accounting repair applicable" if safe_to_apply else (
        ", ".join(reasons or ["fee-pending accounting repair not applicable"])
    )

    repair_mode = "complete_existing_fill_fee" if existing_fill is not None else "apply_missing_fill"
    projected_status = "unknown"
    projected_qty_filled = None
    if order is not None and observation is not None:
        projected_qty_filled = float(order["qty_filled"] or 0.0)
        if existing_fill is None:
            projected_qty_filled += qty
        qty_req = float(order["qty_req"] or 0.0)
        projected_status = (
            "FILLED"
            if projected_qty_filled >= qty_req - order_fill_tolerance(qty_req)
            else "PARTIAL"
        )

    return {
        "needs_repair": bool(
            observation is not None
            and (
                existing_fill is None
                or not existing_fill_fee_complete(existing_fill)
            )
        ),
        "safe_to_apply": safe_to_apply,
        "eligibility_reason": eligibility_reason,
        "repair_mode": repair_mode,
        "existing_fill_id": int(existing_fill["id"]) if existing_fill is not None else None,
        "existing_fill_fee": (
            float(existing_fill["fee"])
            if existing_fill is not None and existing_fill["fee"] is not None
            else None
        ),
        "client_order_id": client_order_id_text,
        "exchange_order_id": observation_exchange_order_id or exchange_order_id_text,
        "fill_id": observation_fill_id or None,
        "fill_ts": fill_ts,
        "side": side or "unknown",
        "price": price,
        "qty": qty,
        "notional": notional,
        "fee": fee_value,
        "fee_provenance": fee_provenance_text or None,
        "pending_observation_count": len(observations),
        "observation_id": int(observation["id"]) if observation is not None else None,
        "observation_source": _clean_text(observation["source"]) if observation is not None else None,
        "observation_fee_status": _clean_text(observation["fee_status"]) if observation is not None else None,
        "observation_parse_warnings": _clean_text(observation["parse_warnings"]) if observation is not None else None,
        "raw_payload_present": bool(observation is not None and observation["raw_payload"] is not None),
        "fee_authority": fee_authority_evidence,
        "order_status": _clean_text(order["status"]) if order is not None else None,
        "projected_status": projected_status,
        "projected_qty_filled": projected_qty_filled,
        "fee_pending_accounting_repair_count": int(repair_summary.get("repair_count") or 0),
        "recommended_command": (
            "uv run python bot.py fee-pending-accounting-repair --client-order-id "
            f"{client_order_id_text} --fill-id {observation_fill_id} --fee <fee> --fee-provenance <source> --apply --yes"
            if observation is not None and (fill_id_text or len(observations) == 1)
            else "uv run python bot.py recovery-report --json"
        ),
    }


def apply_fee_pending_accounting_repair(
    conn,
    *,
    client_order_id: str,
    fill_id: str | None = None,
    exchange_order_id: str | None = None,
    fee: float,
    fee_provenance: str,
    note: str | None = None,
) -> dict[str, Any]:
    preview = build_fee_pending_accounting_repair_preview(
        conn,
        client_order_id=client_order_id,
        fill_id=fill_id,
        exchange_order_id=exchange_order_id,
        fee=fee,
        fee_provenance=fee_provenance,
    )
    if not bool(preview["safe_to_apply"]):
        raise RuntimeError(f"fee-pending accounting repair is not safe to apply: {preview['eligibility_reason']}")

    before_lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
    replay = compute_accounting_replay(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=normalize_cash_amount(replay["replay_cash"]),
        cash_locked=0.0,
        asset_available=normalize_asset_qty(replay["replay_qty"]),
        asset_locked=0.0,
    )
    event_ts = int(time.time() * 1000)
    repair_basis = {
        "event_type": "fee_pending_accounting_repair",
        "observation_id": preview["observation_id"],
        "observation_source": preview["observation_source"],
        "observation_fee_status": preview["observation_fee_status"],
        "observation_parse_warnings": preview["observation_parse_warnings"],
        "raw_payload_present": preview["raw_payload_present"],
        "client_order_id": preview["client_order_id"],
        "exchange_order_id": preview["exchange_order_id"],
        "fill_id": preview["fill_id"],
        "fill_ts": preview["fill_ts"],
        "side": preview["side"],
        "price": preview["price"],
        "qty": preview["qty"],
        "notional": preview["notional"],
        "fee": preview["fee"],
        "fee_provenance": preview["fee_provenance"],
        "fee_authority": preview["fee_authority"],
        "order_status_before": preview["order_status"],
        "projected_status": preview["projected_status"],
        "lot_snapshot_before": before_lot_snapshot,
        "accounting_replay_cash_before_apply": replay["replay_cash"],
        "accounting_replay_qty_before_apply": replay["replay_qty"],
    }
    repair = record_fee_pending_accounting_repair(
        conn,
        event_ts=event_ts,
        client_order_id=str(preview["client_order_id"]),
        exchange_order_id=str(preview["exchange_order_id"] or "") or None,
        fill_id=str(preview["fill_id"] or "") or None,
        fill_ts=int(preview["fill_ts"]),
        price=float(preview["price"]),
        qty=float(preview["qty"]),
        fee=float(preview["fee"]),
        source="operator_fee_pending_recovery",
        reason="fee_pending_accounting_repair",
        repair_basis=repair_basis,
        note=note,
    )
    if str(preview.get("repair_mode")) == "complete_existing_fill_fee":
        existing_fill_id = int(preview["existing_fill_id"] or 0)
        if existing_fill_id <= 0:
            raise RuntimeError("existing fill fee repair missing existing_fill_id")
        conn.execute(
            "UPDATE fills SET fee=? WHERE id=?",
            (float(preview["fee"]), existing_fill_id),
        )
        conn.execute(
            """
            UPDATE trades
            SET fee=?
            WHERE client_order_id=?
              AND ts=?
              AND ABS(price-?) < 1e-12
              AND ABS(qty-?) < 1e-12
            """,
            (
                float(preview["fee"]),
                str(preview["client_order_id"]),
                int(preview["fill_ts"]),
                float(preview["price"]),
                float(preview["qty"]),
            ),
        )
        replay_after_fee = compute_accounting_replay(conn)
        set_portfolio_breakdown(
            conn,
            cash_available=normalize_cash_amount(replay_after_fee["replay_cash"]),
            cash_locked=0.0,
            asset_available=normalize_asset_qty(replay_after_fee["replay_qty"]),
            asset_locked=0.0,
        )
        applied = {
            "repair_mode": "complete_existing_fill_fee",
            "existing_fill_id": existing_fill_id,
            "fee": float(preview["fee"]),
        }
    else:
        applied = apply_fill_and_trade(
            conn,
            client_order_id=str(preview["client_order_id"]),
            side=str(preview["side"]),
            fill_id=str(preview["fill_id"] or "") or None,
            fill_ts=int(preview["fill_ts"]),
            price=float(preview["price"]),
            qty=float(preview["qty"]),
            fee=float(preview["fee"]),
            note=f"fee_pending_accounting_repair repair_key={repair['repair_key']}",
            allow_entry_decision_fallback=False,
        )
    set_status(
        str(preview["client_order_id"]),
        str(preview["projected_status"]),
        last_error=None,
        conn=conn,
    )
    record_broker_fill_observation(
        conn,
        event_ts=event_ts,
        client_order_id=str(preview["client_order_id"]),
        exchange_order_id=str(preview["exchange_order_id"] or "") or None,
        fill_id=str(preview["fill_id"] or "") or None,
        fill_ts=int(preview["fill_ts"]),
        side=str(preview["side"]),
        price=float(preview["price"]),
        qty=float(preview["qty"]),
        fee=float(preview["fee"]),
        fee_status="operator_confirmed",
        fee_source="operator_confirmed",
        fee_confidence="authoritative",
        accounting_status="accounting_complete",
        source="fee_pending_accounting_repair",
        fee_provenance=str(preview["fee_provenance"]),
        fee_validation_reason="operator_confirmed",
        fee_validation_checks={"operator_confirmed": True},
        parse_warnings=(
            "operator_fee_provenance="
            f"{str(preview['fee_provenance']).replace(';', ',')}"
        ),
        raw_payload={
            "repair_key": repair["repair_key"],
            "source_observation_id": preview["observation_id"],
            "raw_payload_present": preview["raw_payload_present"],
        },
    )
    projection_replay = rebuild_lifecycle_projections_from_trades(
        conn,
        pair=settings.PAIR,
        allow_entry_decision_fallback=False,
    )
    after_lot_snapshot = summarize_position_lots(conn, pair=settings.PAIR).as_dict()
    return {
        "preview": preview,
        "repair": repair,
        "applied_fill": applied,
        "lot_snapshot_before": before_lot_snapshot,
        "lot_snapshot_after": after_lot_snapshot,
        "projection_replay": projection_replay.as_dict(),
    }
