from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from typing import Any
import sqlite3

from .db_core import ensure_db
from .config import settings


OPEN_ORDER_STATUSES = ("PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN", "RECOVERY_REQUIRED", "CANCEL_REQUESTED")
TERMINAL_ORDER_STATUSES = ("CANCELED", "FILLED", "FAILED", "RECOVERY_REQUIRED")
ORDER_INTENT_DEDUP_RELEASE_STATUSES = {"FAILED", "RELEASED"}


def evaluate_unresolved_order_gate(
    conn: sqlite3.Connection,
    *,
    now_ms: int,
    max_open_order_age_sec: int,
) -> tuple[bool, str, str]:
    """Returns whether unresolved risky order states should block new submissions."""
    state = collect_risky_order_state(conn, now_ms=now_ms, max_open_order_age_sec=max_open_order_age_sec)
    if state["submit_unknown_count"] > 0:
        return True, "SUBMIT_UNKNOWN_PRESENT", "submit-unknown unresolved order exists"

    if state["recovery_required_count"] > 0:
        return True, "RECOVERY_REQUIRED_PRESENT", "recovery-required order exists"

    open_count = int(state["unresolved_open_order_count"])
    if open_count <= 0:
        return False, "OK", "ok"

    age_sec = float(state["oldest_unresolved_open_order_age_sec"] or 0.0)
    max_age_sec = max(1, int(max_open_order_age_sec))
    if age_sec > max_age_sec:
        return True, "STALE_UNRESOLVED_OPEN_ORDER", f"stale unresolved open order exists: age={age_sec:.1f}s > {max_age_sec}s"

    return True, "UNRESOLVED_OPEN_ORDER_PRESENT", "unresolved open order exists"


def collect_risky_order_state(
    conn: sqlite3.Connection,
    *,
    now_ms: int,
    max_open_order_age_sec: int,
) -> dict[str, int | float]:
    """Collects risky local order-state counters used by startup and submit gates."""
    submit_unknown_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM orders WHERE status='SUBMIT_UNKNOWN'"
    ).fetchone()
    recovery_required_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM orders WHERE status='RECOVERY_REQUIRED'"
    ).fetchone()
    submit_unknown_without_exchange_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM orders
        WHERE status='SUBMIT_UNKNOWN'
          AND (exchange_order_id IS NULL OR TRIM(exchange_order_id)='')
        """
    ).fetchone()
    stray_remote_open_row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM orders
        WHERE client_order_id LIKE 'remote_%'
          AND status IN ('PENDING_SUBMIT','NEW','PARTIAL','SUBMIT_UNKNOWN','RECOVERY_REQUIRED')
        """
    ).fetchone()

    placeholders = ",".join("?" for _ in OPEN_ORDER_STATUSES)
    open_row = conn.execute(
        f"""
        SELECT COUNT(*) AS open_count, MIN(created_ts) AS oldest_created_ts
        FROM orders
        WHERE status IN ({placeholders})
        """,
        OPEN_ORDER_STATUSES,
    ).fetchone()
    open_count = int(open_row["open_count"] if hasattr(open_row, "keys") else open_row[0])
    oldest_created_ts = open_row["oldest_created_ts"] if hasattr(open_row, "keys") else open_row[1]

    age_sec = 0.0
    if oldest_created_ts is not None:
        age_sec = max(0.0, (int(now_ms) - int(oldest_created_ts)) / 1000)
    max_age_sec = max(1, int(max_open_order_age_sec))
    return {
        "submit_unknown_count": int(submit_unknown_row["cnt"] if submit_unknown_row else 0),
        "recovery_required_count": int(recovery_required_row["cnt"] if recovery_required_row else 0),
        "unresolved_open_order_count": open_count,
        "oldest_unresolved_open_order_age_sec": age_sec,
        "stale_unresolved_open_order": int(open_count > 0 and age_sec > max_age_sec),
        "submit_unknown_without_exchange_id_count": int(
            submit_unknown_without_exchange_row["cnt"] if submit_unknown_without_exchange_row else 0
        ),
        "stray_remote_open_order_count": int(stray_remote_open_row["cnt"] if stray_remote_open_row else 0),
    }


ALLOWED_STATUS_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "PENDING_SUBMIT": ("PENDING_SUBMIT", "NEW", "PARTIAL", "FILLED", "CANCELED", "FAILED", "SUBMIT_UNKNOWN", "RECOVERY_REQUIRED", "CANCEL_REQUESTED"),
    "NEW": ("NEW", "PARTIAL", "FILLED", "CANCELED", "FAILED", "RECOVERY_REQUIRED", "CANCEL_REQUESTED"),
    "PARTIAL": ("PARTIAL", "FILLED", "CANCELED", "FAILED", "RECOVERY_REQUIRED", "CANCEL_REQUESTED"),
    "SUBMIT_UNKNOWN": ("SUBMIT_UNKNOWN", "NEW", "PARTIAL", "FILLED", "CANCELED", "FAILED", "RECOVERY_REQUIRED", "CANCEL_REQUESTED"),
    "CANCEL_REQUESTED": ("CANCEL_REQUESTED", "CANCELED", "FILLED", "PARTIAL", "RECOVERY_REQUIRED"),
    "RECOVERY_REQUIRED": ("RECOVERY_REQUIRED", "NEW", "PARTIAL", "FILLED", "CANCELED", "FAILED"),
    "FILLED": ("FILLED",),
    "CANCELED": ("CANCELED",),
    "FAILED": ("FAILED",),
}


def validate_status_transition(*, from_status: str, to_status: str) -> tuple[bool, str | None]:
    allowed = ALLOWED_STATUS_TRANSITIONS.get(from_status)
    if allowed is None:
        return False, f"unknown current status: {from_status}"
    if to_status in allowed:
        return True, None
    return False, f"disallowed status transition: {from_status}->{to_status}"


def _record_order_event(
    conn: sqlite3.Connection,
    *,
    client_order_id: str,
    event_type: str,
    event_ts: int | None = None,
    order_status: str | None = None,
    exchange_order_id: str | None = None,
    fill_id: str | None = None,
    qty: float | None = None,
    price: float | None = None,
    message: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    order_type: str | None = None,
    submit_attempt_id: str | None = None,
    mode: str | None = None,
    intent_ts: int | None = None,
    submit_ts: int | None = None,
    payload_fingerprint: str | None = None,
    broker_response_summary: str | None = None,
    submission_reason_code: str | None = None,
    exception_class: str | None = None,
    timeout_flag: bool | None = None,
    submit_phase: str | None = None,
    submit_plan_id: str | None = None,
    signed_request_id: str | None = None,
    submission_id: str | None = None,
    confirmation_id: str | None = None,
    submit_evidence: str | None = None,
    exchange_order_id_obtained: bool | None = None,
    internal_lot_size: float | None = None,
    effective_min_trade_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    final_intended_qty: float | None = None,
    final_submitted_qty: float | None = None,
    decision_reason_code: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO order_events(
            client_order_id,
            event_type,
            event_ts,
            order_status,
            exchange_order_id,
            fill_id,
            qty,
            price,
                message,
                symbol,
                side,
                order_type,
                submit_attempt_id,
                mode,
            intent_ts,
            submit_ts,
            payload_fingerprint,
            broker_response_summary,
            submission_reason_code,
            exception_class,
            timeout_flag,
            submit_phase,
            submit_plan_id,
            signed_request_id,
            submission_id,
            confirmation_id,
            submit_evidence,
            exchange_order_id_obtained,
            internal_lot_size,
            effective_min_trade_qty,
            qty_step,
            min_notional_krw,
            intended_lot_count,
            executable_lot_count,
            final_intended_qty,
            final_submitted_qty,
            decision_reason_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            client_order_id,
            event_type,
            int(event_ts if event_ts is not None else time.time() * 1000),
            order_status,
            exchange_order_id,
            fill_id,
            (float(qty) if qty is not None else None),
            (float(price) if price is not None else None),
            (message[:500] if message else None),
            symbol,
            side,
            order_type,
            submit_attempt_id,
            mode,
            int(intent_ts) if intent_ts is not None else None,
            int(submit_ts) if submit_ts is not None else None,
            payload_fingerprint,
            (broker_response_summary[:500] if broker_response_summary else None),
            submission_reason_code,
            exception_class,
            (1 if timeout_flag else 0) if timeout_flag is not None else None,
            submit_phase,
            submit_plan_id,
            signed_request_id,
            submission_id,
            confirmation_id,
            submit_evidence,
            (1 if exchange_order_id_obtained else 0) if exchange_order_id_obtained is not None else None,
            (float(internal_lot_size) if internal_lot_size is not None else None),
            (float(effective_min_trade_qty) if effective_min_trade_qty is not None else None),
            (float(qty_step) if qty_step is not None else None),
            (float(min_notional_krw) if min_notional_krw is not None else None),
            (int(intended_lot_count) if intended_lot_count is not None else None),
            (int(executable_lot_count) if executable_lot_count is not None else None),
            (float(final_intended_qty) if final_intended_qty is not None else None),
            (float(final_submitted_qty) if final_submitted_qty is not None else None),
            decision_reason_code,
        ),
    )


def payload_fingerprint(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_order_intent_key(
    *,
    symbol: str,
    side: str,
    strategy_context: str,
    intent_ts: int,
    intent_type: str,
    qty: float | None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
) -> str:
    payload = {
        "intent_ts": int(intent_ts),
        "intent_type": str(intent_type),
        "side": str(side).upper(),
        "strategy_context": str(strategy_context),
        "symbol": str(symbol),
    }
    if intended_lot_count is not None or executable_lot_count is not None:
        payload["intended_lot_count"] = int(intended_lot_count or 0)
        payload["executable_lot_count"] = int(executable_lot_count or 0)
        payload["lot_key_basis"] = "lot-native"
    else:
        payload["qty"] = (round(float(qty), 12) if qty is not None and math.isfinite(float(qty)) else None)
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_order_suppression_key(
    *,
    mode: str,
    strategy_context: str,
    strategy_name: str,
    signal: str,
    side: str,
    reason_code: str,
    dust_signature: str | None,
    requested_qty: float | None,
    normalized_qty: float | None,
    market_price: float | None,
) -> str:
    payload = {
        "mode": str(mode),
        "strategy_context": str(strategy_context),
        "strategy_name": str(strategy_name),
        "signal": str(signal).upper(),
        "side": str(side).upper(),
        "reason_code": str(reason_code),
        "dust_signature": str(dust_signature or ""),
        "requested_qty": (
            round(float(requested_qty), 12)
            if requested_qty is not None and math.isfinite(float(requested_qty))
            else None
        ),
        "normalized_qty": (
            round(float(normalized_qty), 12)
            if normalized_qty is not None and math.isfinite(float(normalized_qty))
            else None
        ),
        "market_price": (
            round(float(market_price), 8)
            if market_price is not None and math.isfinite(float(market_price))
            else None
        ),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def claim_order_intent_dedup(
    conn: sqlite3.Connection,
    *,
    intent_key: str,
    client_order_id: str,
    symbol: str,
    side: str,
    strategy_context: str,
    intent_type: str,
    intent_ts: int,
    qty: float | None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    order_status: str,
) -> tuple[bool, sqlite3.Row | None]:
    now_ms = int(time.time() * 1000)
    try:
        conn.execute(
            """
            INSERT INTO order_intent_dedup(
                intent_key,
                symbol,
                side,
                strategy_context,
                intent_type,
                intent_ts,
                qty,
                intended_lot_count,
                executable_lot_count,
                client_order_id,
                order_status,
                created_ts,
                updated_ts,
                last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                intent_key,
                symbol,
                side,
                strategy_context,
                intent_type,
                int(intent_ts),
                (float(qty) if qty is not None else None),
                (int(intended_lot_count) if intended_lot_count is not None else None),
                (int(executable_lot_count) if executable_lot_count is not None else None),
                client_order_id,
                order_status,
                now_ms,
                now_ms,
            ),
        )
        return True, None
    except sqlite3.IntegrityError:
        row = conn.execute(
            """
            SELECT intent_key, symbol, side, strategy_context, intent_type, intent_ts, qty,
                   intended_lot_count, executable_lot_count,
                   client_order_id, order_status, created_ts, updated_ts, last_error
            FROM order_intent_dedup
            WHERE intent_key=?
            """,
            (intent_key,),
        ).fetchone()
        if row is not None:
            conn.execute(
                """
                UPDATE order_intent_dedup
                SET updated_ts=?
                WHERE intent_key=?
                """,
                (now_ms, intent_key),
            )
        return False, row


def update_order_intent_dedup(
    conn: sqlite3.Connection,
    *,
    intent_key: str,
    client_order_id: str,
    order_status: str,
    last_error: str | None = None,
) -> None:
    if order_status in ORDER_INTENT_DEDUP_RELEASE_STATUSES:
        conn.execute("DELETE FROM order_intent_dedup WHERE intent_key=?", (intent_key,))
        return

    now_ms = int(time.time() * 1000)
    conn.execute(
        """
        UPDATE order_intent_dedup
        SET client_order_id=?, order_status=?, updated_ts=?, last_error=?
        WHERE intent_key=?
        """,
        (
            client_order_id,
            order_status,
            now_ms,
            (last_error[:500] if last_error else None),
            intent_key,
        ),
    )


def record_submit_attempt(
    *,
    client_order_id: str,
    symbol: str,
    side: str,
    qty: float,
    price: float | None,
    submit_ts: int,
    payload_fingerprint: str,
    broker_response_summary: str | None,
    submission_reason_code: str,
    exception_class: str | None,
    timeout_flag: bool,
    submit_evidence: str | None,
    exchange_order_id_obtained: bool,
    order_status: str,
    submit_attempt_id: str,
    submit_phase: str | None = None,
    submit_plan_id: str | None = None,
    signed_request_id: str | None = None,
    submission_id: str | None = None,
    confirmation_id: str | None = None,
    event_type: str = "submit_attempt_recorded",
    message: str | None = None,
    order_type: str | None = None,
    internal_lot_size: float | None = None,
    effective_min_trade_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    final_intended_qty: float | None = None,
    final_submitted_qty: float | None = None,
    decision_reason_code: str | None = None,
    conn: sqlite3.Connection,
) -> None:
    _record_order_event(
        conn,
        client_order_id=client_order_id,
        event_type=event_type,
        event_ts=submit_ts,
        order_status=order_status,
        qty=qty,
        price=price,
        symbol=symbol,
        side=side,
        order_type=order_type,
        submit_attempt_id=submit_attempt_id,
        submit_ts=submit_ts,
        payload_fingerprint=payload_fingerprint,
        broker_response_summary=broker_response_summary,
        submission_reason_code=submission_reason_code,
        exception_class=exception_class,
        timeout_flag=timeout_flag,
        submit_phase=submit_phase,
        submit_plan_id=submit_plan_id,
        signed_request_id=signed_request_id,
        submission_id=submission_id,
        confirmation_id=confirmation_id,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=exchange_order_id_obtained,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
        message=message,
    )


def new_client_order_id(prefix: str = "cli") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


MAX_CLIENT_ORDER_ID_LENGTH = 36
_CLIENT_ORDER_ID_TOKEN_FALLBACK = "x"
_CLIENT_ORDER_ID_SUFFIX_LENGTH = 8
_CLIENT_ORDER_ID_HEX = set("0123456789abcdef")


def _client_order_token(value: str, *, fallback: str, max_len: int) -> str:
    token = "".join(ch for ch in str(value).strip().lower() if ch.isalnum())
    if not token:
        token = fallback
    return token[:max_len]


def _client_order_suffix(*, submit_attempt_id: str | None, nonce: str | None) -> str:
    source = str(submit_attempt_id or nonce or uuid.uuid4().hex).strip().lower()
    hex_chars = "".join(ch for ch in source if ch in _CLIENT_ORDER_ID_HEX)
    if not hex_chars:
        hex_chars = uuid.uuid4().hex
    if len(hex_chars) < _CLIENT_ORDER_ID_SUFFIX_LENGTH:
        hex_chars = f"{hex_chars}{uuid.uuid4().hex}"
    return hex_chars[:_CLIENT_ORDER_ID_SUFFIX_LENGTH]


def build_client_order_id(
    *,
    mode: str,
    side: str,
    intent_ts: int,
    submit_attempt_id: str | None = None,
    nonce: str | None = None,
) -> str:
    mode_token = _client_order_token(mode, fallback=_CLIENT_ORDER_ID_TOKEN_FALLBACK, max_len=5)
    side_token = _client_order_token(side, fallback="ord", max_len=4)
    ts = int(intent_ts)
    suffix = _client_order_suffix(submit_attempt_id=submit_attempt_id, nonce=nonce)
    client_order_id = f"{mode_token}_{ts}_{side_token}_{suffix}"
    if len(client_order_id) > MAX_CLIENT_ORDER_ID_LENGTH:
        raise ValueError(
            f"client_order_id length overflow: value={client_order_id} "
            f"len={len(client_order_id)} limit={MAX_CLIENT_ORDER_ID_LENGTH}"
        )
    return client_order_id


def create_order(
    *,
    client_order_id: str,
    submit_attempt_id: str | None = None,
    symbol: str | None = None,
    mode: str | None = None,
    side: str,
    qty_req: float,
    price: float | None,
    strategy_name: str | None = None,
    entry_decision_id: int | None = None,
    exit_decision_id: int | None = None,
    decision_reason: str | None = None,
    exit_rule_name: str | None = None,
    order_type: str | None = None,
    internal_lot_size: float | None = None,
    effective_min_trade_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    final_intended_qty: float | None = None,
    final_submitted_qty: float | None = None,
    decision_reason_code: str | None = None,
    local_intent_state: str | None = None,
    status: str = "NEW",
    ts_ms: int | None = None,
    conn: sqlite3.Connection | None = None,
) -> None:
    ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, submit_attempt_id, exchange_order_id, status, side, order_type, price, qty_req, qty_filled,
                strategy_name, entry_decision_id, exit_decision_id, decision_reason, exit_rule_name,
                internal_lot_size, effective_min_trade_qty, qty_step, min_notional_krw, intended_lot_count,
                executable_lot_count, final_intended_qty, final_submitted_qty, decision_reason_code, local_intent_state,
                created_ts, updated_ts, last_error
            )
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                client_order_id,
                submit_attempt_id,
                status,
                side,
                order_type,
                price,
                float(qty_req),
                strategy_name,
                (int(entry_decision_id) if entry_decision_id is not None else None),
                (int(exit_decision_id) if exit_decision_id is not None else None),
                decision_reason,
                exit_rule_name,
                internal_lot_size,
                effective_min_trade_qty,
                qty_step,
                min_notional_krw,
                intended_lot_count,
                executable_lot_count,
                final_intended_qty,
                final_submitted_qty,
                decision_reason_code,
                local_intent_state,
                ts,
                ts,
            ),
        )
        _record_order_event(
            conn,
                client_order_id=client_order_id,
                event_type="intent_created",
                event_ts=ts,
                order_status=status,
                qty=qty_req,
                price=price,
                symbol=symbol or settings.PAIR,
                side=side,
                order_type=order_type,
                submit_attempt_id=submit_attempt_id,
                mode=mode or settings.MODE,
                intent_ts=ts,
                intended_lot_count=intended_lot_count,
                executable_lot_count=executable_lot_count,
            )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def record_submit_started(
    client_order_id: str,
    conn: sqlite3.Connection | None = None,
    *,
    submit_attempt_id: str | None = None,
    symbol: str | None = None,
    side: str | None = None,
    qty: float | None = None,
    mode: str | None = None,
    message: str | None = None,
) -> None:
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="submit_started",
            event_ts=ts,
            order_status="PENDING_SUBMIT",
            symbol=symbol,
            side=side,
            submit_attempt_id=submit_attempt_id,
            mode=mode,
            qty=qty,
            message=message or "submit intent staged before broker dispatch",
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def record_submit_blocked(
    client_order_id: str,
    *,
    status: str,
    reason: str,
    conn: sqlite3.Connection | None = None,
) -> None:
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="submit_blocked",
            event_ts=ts,
            order_status=status,
            message=reason,
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def record_order_suppression(
    *,
    suppression_key: str,
    event_kind: str,
    mode: str,
    strategy_context: str,
    strategy_name: str,
    signal: str,
    side: str,
    reason_code: str,
    reason: str,
    requested_qty: float | None,
    normalized_qty: float | None,
    market_price: float | None,
    decision_id: int | None = None,
    decision_reason: str | None = None,
    exit_rule_name: str | None = None,
    dust_present: bool = False,
    dust_allow_resume: bool = False,
    dust_effective_flat: bool = False,
    dust_state: str | None = None,
    dust_action: str | None = None,
    dust_signature: str | None = None,
    qty_below_min: bool = False,
    normalized_non_positive: bool = False,
    normalized_below_min: bool = False,
    notional_below_min: bool = False,
    summary: str | None = None,
    context: dict[str, Any] | None = None,
    conn: sqlite3.Connection | None = None,
) -> None:
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    context_json = json.dumps(context or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    try:
        existing = conn.execute(
            """
            SELECT seen_count
            FROM order_suppressions
            WHERE suppression_key=?
            """,
            (suppression_key,),
        ).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO order_suppressions(
                    suppression_key,
                    event_kind,
                    event_ts,
                    mode,
                    strategy_context,
                    strategy_name,
                    signal,
                    side,
                    reason_code,
                    reason,
                    requested_qty,
                    normalized_qty,
                    market_price,
                    decision_id,
                    decision_reason,
                    exit_rule_name,
                    dust_present,
                    dust_allow_resume,
                    dust_effective_flat,
                    dust_state,
                    dust_action,
                    dust_signature,
                    qty_below_min,
                    normalized_non_positive,
                    normalized_below_min,
                    notional_below_min,
                    summary,
                    context_json,
                    created_ts,
                    updated_ts,
                    seen_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    suppression_key,
                    event_kind,
                    ts,
                    mode,
                    strategy_context,
                    strategy_name,
                    signal,
                    side,
                    reason_code,
                    reason[:500],
                    requested_qty,
                    normalized_qty,
                    market_price,
                    decision_id,
                    (decision_reason[:500] if decision_reason else None),
                    (exit_rule_name[:500] if exit_rule_name else None),
                    1 if dust_present else 0,
                    1 if dust_allow_resume else 0,
                    1 if dust_effective_flat else 0,
                    dust_state,
                    dust_action,
                    dust_signature,
                    1 if qty_below_min else 0,
                    1 if normalized_non_positive else 0,
                    1 if normalized_below_min else 0,
                    1 if notional_below_min else 0,
                    (summary[:1000] if summary else None),
                    context_json,
                    ts,
                    ts,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE order_suppressions
                SET event_kind=?,
                    event_ts=?,
                    mode=?,
                    strategy_context=?,
                    strategy_name=?,
                    signal=?,
                    side=?,
                    reason_code=?,
                    reason=?,
                    requested_qty=?,
                    normalized_qty=?,
                    market_price=?,
                    decision_id=?,
                    decision_reason=?,
                    exit_rule_name=?,
                    dust_present=?,
                    dust_allow_resume=?,
                    dust_effective_flat=?,
                    dust_state=?,
                    dust_action=?,
                    dust_signature=?,
                    qty_below_min=?,
                    normalized_non_positive=?,
                    normalized_below_min=?,
                    notional_below_min=?,
                    summary=?,
                    context_json=?,
                    updated_ts=?,
                    seen_count=seen_count + 1
                WHERE suppression_key=?
                """,
                (
                    event_kind,
                    ts,
                    mode,
                    strategy_context,
                    strategy_name,
                    signal,
                    side,
                    reason_code,
                    reason[:500],
                    requested_qty,
                    normalized_qty,
                    market_price,
                    decision_id,
                    (decision_reason[:500] if decision_reason else None),
                    (exit_rule_name[:500] if exit_rule_name else None),
                    1 if dust_present else 0,
                    1 if dust_allow_resume else 0,
                    1 if dust_effective_flat else 0,
                    dust_state,
                    dust_action,
                    dust_signature,
                    1 if qty_below_min else 0,
                    1 if normalized_non_positive else 0,
                    1 if normalized_below_min else 0,
                    1 if notional_below_min else 0,
                    (summary[:1000] if summary else None),
                    context_json,
                    ts,
                    suppression_key,
                ),
            )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def set_exchange_order_id(
    client_order_id: str,
    exchange_order_id: str,
    conn: sqlite3.Connection | None = None,
) -> None:
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        conn.execute(
            "UPDATE orders SET exchange_order_id=?, updated_ts=? WHERE client_order_id=?",
            (exchange_order_id, ts, client_order_id),
        )
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="exchange_order_id_attached",
            event_ts=ts,
            exchange_order_id=exchange_order_id,
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def set_status(
    client_order_id: str,
    status: str,
    last_error: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> None:
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        current = conn.execute(
            "SELECT status FROM orders WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()
        if current is None:
            raise ValueError(f"order not found: {client_order_id}")

        from_status = str(current["status"])
        allowed, reason = validate_status_transition(from_status=from_status, to_status=status)
        if not allowed:
            _record_order_event(
                conn,
                client_order_id=client_order_id,
                event_type="status_transition_blocked",
                event_ts=ts,
                order_status=from_status,
                message=reason,
            )
            if own_conn:
                conn.commit()
            raise ValueError(reason)

        conn.execute(
            "UPDATE orders SET status=?, updated_ts=?, last_error=? WHERE client_order_id=?",
            (status, ts, (last_error[:500] if last_error else None), client_order_id),
        )
        event_type = "status_changed"
        if status == "SUBMIT_UNKNOWN":
            event_type = "submit_timeout"
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type=event_type,
            event_ts=ts,
            order_status=status,
            message=last_error,
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def record_status_transition(
    client_order_id: str,
    *,
    from_status: str,
    to_status: str,
    reason: str,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Record a detailed status transition event for high-risk paths."""
    ts = int(time.time() * 1000)
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="status_transition",
            event_ts=ts,
            order_status=to_status,
            message=f"from={from_status};to={to_status};reason={reason}",
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def _compute_fill_slippage_bps(*, side: str, reference_price: float, fill_price: float) -> float | None:
    if not math.isfinite(float(reference_price)) or float(reference_price) <= 0:
        return None
    if not math.isfinite(float(fill_price)):
        return None
    if side == "BUY":
        delta = float(fill_price) - float(reference_price)
    elif side == "SELL":
        delta = float(reference_price) - float(fill_price)
    else:
        return None
    return (delta / float(reference_price)) * 10_000.0


def add_fill(
    *,
    client_order_id: str,
    fill_id: str | None,
    fill_ts: int,
    price: float,
    qty: float,
    fee: float = 0.0,
    conn: sqlite3.Connection | None = None,
) -> None:
    own_conn = conn is None
    conn = conn or ensure_db()
    try:
        order_row = conn.execute(
            "SELECT side, intended_lot_count, executable_lot_count, internal_lot_size FROM orders WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()
        side = str(order_row["side"]) if order_row and order_row["side"] else ""
        intended_lot_count = (
            int(order_row["intended_lot_count"]) if order_row and order_row["intended_lot_count"] is not None else None
        )
        executable_lot_count = (
            int(order_row["executable_lot_count"]) if order_row and order_row["executable_lot_count"] is not None else None
        )
        internal_lot_size = (
            float(order_row["internal_lot_size"]) if order_row and order_row["internal_lot_size"] is not None else None
        )

        submit_row = conn.execute(
            """
            SELECT price
            FROM order_events
            WHERE client_order_id=?
              AND event_type='submit_attempt_recorded'
              AND price IS NOT NULL
            ORDER BY event_ts DESC, id DESC
            LIMIT 1
            """,
            (client_order_id,),
        ).fetchone()
        if submit_row is None:
            submit_row = conn.execute(
                """
                SELECT price
                FROM order_events
                WHERE client_order_id=?
                  AND event_type='submit_attempt_preflight'
                  AND price IS NOT NULL
                ORDER BY event_ts DESC, id DESC
                LIMIT 1
                """,
                (client_order_id,),
            ).fetchone()

        reference_price = float(submit_row["price"]) if submit_row and submit_row["price"] is not None else None
        slippage_bps = (
            _compute_fill_slippage_bps(side=side, reference_price=reference_price, fill_price=float(price))
            if reference_price is not None
            else None
        )

        conn.execute(
            """
            INSERT INTO fills(
                client_order_id,
                fill_id,
                fill_ts,
                price,
                qty,
                fee,
                reference_price,
                slippage_bps,
                intended_lot_count,
                executable_lot_count,
                internal_lot_size
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_order_id,
                fill_id,
                int(fill_ts),
                float(price),
                float(qty),
                float(fee),
                (float(reference_price) if reference_price is not None else None),
                (float(slippage_bps) if slippage_bps is not None else None),
                intended_lot_count,
                executable_lot_count,
                internal_lot_size,
            ),
        )
        updated_ts = int(time.time() * 1000)
        conn.execute(
            "UPDATE orders SET qty_filled = qty_filled + ?, updated_ts=? WHERE client_order_id=?",
            (float(qty), updated_ts, client_order_id),
        )
        _record_order_event(
            conn,
            client_order_id=client_order_id,
            event_type="fill_applied",
            event_ts=updated_ts,
            fill_id=fill_id,
            qty=qty,
            price=price,
            intended_lot_count=intended_lot_count,
            executable_lot_count=executable_lot_count,
            internal_lot_size=internal_lot_size,
            message=(
                f"fee={float(fee)};reference_price={reference_price};slippage_bps={slippage_bps}"
                if fee or reference_price is not None or slippage_bps is not None
                else None
            ),
        )
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def get_open_orders() -> list[dict[str, Any]]:
    conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in OPEN_ORDER_STATUSES)
        rows = conn.execute(
            f"""
            SELECT client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts
            FROM orders
            WHERE status IN ({placeholders})
            ORDER BY created_ts ASC
            """,
            OPEN_ORDER_STATUSES,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
