from .config import (
    LiveModeValidationError,
    ModeValidationError,
    PATH_MANAGER,
    settings,
    log_live_execution_contract,
    validate_live_mode_preflight,
    validate_live_run_startup_contract,
    validate_mode_or_raise,
)
from .risk import evaluate_buy_guardrails
from .broker.paper import paper_execute
import os
import time
import argparse
import sqlite3
import math
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from .marketdata import cmd_sync, cmd_ticker, cmd_candles
from .db_core import (
    compute_accounting_replay,
    ensure_db,
    get_broker_fill_observation_summary,
    get_external_cash_adjustment_summary,
    get_external_position_adjustment_summary,
    get_fee_gap_accounting_repair_summary,
    get_fee_pending_accounting_repair_summary,
    get_manual_flat_accounting_repair_summary,
    get_position_authority_repair_summary,
    get_portfolio_breakdown,
    init_portfolio,
    normalize_asset_qty,
    normalize_cash_amount,
    portfolio_asset_total,
    portfolio_cash_total,
    replay_fill_portfolio_snapshot,
    record_manual_flat_accounting_repair,
    record_external_cash_adjustment,
    summarize_fill_accounting_incident_projection,
)
from .utils_time import kst_str, parse_interval_sec
from .engine import (
    build_resume_guidance,
    compute_signal,
    evaluate_restart_readiness,
    evaluate_resume_eligibility,
    get_health_status,
    maybe_clear_stale_initial_reconcile_halt,
    perform_panic_stop_cleanup,
)
from .recovery import (
    backfill_broker_order_with_exchange_id,
    cancel_open_orders_with_broker,
    load_recent_order_lifecycle,
    reconcile_with_broker,
    recover_order_with_exchange_id,
)
from .run_lock import read_run_lock_status
from .runtime_state import disable_trading_until, enable_trading, refresh_open_order_health
from .notifier import notify
from .observability import safety_event
from .broker.order_rules import (
    build_buy_price_none_diagnostic_fields,
    get_cached_order_rule_snapshot,
    get_effective_order_rules,
    resolve_buy_price_none_resolution,
    rule_source_for,
)
from .broker.bithumb import build_broker_with_auth_diagnostics
from .broker import bithumb as bithumb_broker_module
from .broker.base import BrokerBalance, BrokerOrder
from . import runtime_state
from .oms import OPEN_ORDER_STATUSES
from .flatten import flatten_btc_position
from .fee_gap_repair import apply_fee_gap_accounting_repair, build_fee_gap_accounting_repair_preview
from .fee_pending_repair import (
    apply_fee_pending_accounting_repair,
    build_fee_pending_accounting_repair_preview,
)
from .position_authority_repair import (
    apply_position_authority_rebuild,
    build_position_authority_rebuild_preview,
)
from .runtime_readiness import compute_runtime_readiness_snapshot
from .lifecycle import summarize_position_lots, summarize_reserved_exit_qty
from .manual_flat_repair import apply_manual_flat_accounting_repair, build_manual_flat_accounting_repair_preview
from .external_position_repair import (
    apply_external_position_accounting_repair,
    build_external_position_accounting_repair_preview,
)
from .markets import canonical_market_with_raw
from .position_state_snapshot import build_canonical_position_snapshot
from .repair_plan import build_recovery_policy_from_report, build_repair_plan_preview_from_report
from .reason_codes import DUST_RESIDUAL_UNSELLABLE
from .dust import build_dust_display_context, build_position_state_model, format_flat_start_reason_with_dust
from .reporting import (
    build_fee_rate_drift_diagnostics,
    cmd_experiment_report,
    cmd_cash_drift_report,
    cmd_decision_telemetry,
    cmd_fee_diagnostics,
    _format_external_cash_adjustment_summary,
    cmd_ops_report,
    cmd_risk_report,
    cmd_strategy_report,
    fetch_attribution_quality_summary,
    fetch_recovery_attribution_signal_summary,
    parse_kst_date_range_to_ts_ms,
)
from .storage_io import write_json_atomic
from .bootstrap import get_last_explicit_env_load_summary

import httpx

MODE = settings.MODE
PAIR = settings.PAIR
MARKET, RAW_SYMBOL = canonical_market_with_raw(PAIR)
INTERVAL = settings.INTERVAL
EVERY = settings.EVERY

SMA_SHORT = settings.SMA_SHORT
SMA_LONG = settings.SMA_LONG
COOLDOWN_MIN = settings.COOLDOWN_MIN
MIN_GAP = settings.MIN_GAP

START_CASH_KRW = settings.START_CASH_KRW
BUY_FRACTION = settings.BUY_FRACTION
FEE_RATE = settings.FEE_RATE

MAX_ORDER_KRW = settings.MAX_ORDER_KRW
MAX_DAILY_LOSS_KRW = settings.MAX_DAILY_LOSS_KRW
MAX_OPEN_POSITIONS = settings.MAX_OPEN_POSITIONS
KILL_SWITCH = settings.KILL_SWITCH
KILL_SWITCH_LIQUIDATE = settings.KILL_SWITCH_LIQUIDATE
DEFAULT_BITHUMB_BROKER_CLASS = bithumb_broker_module.BithumbBroker
LIVE_COMMAND_GUARDS = {
    "run": "startup",
    "cancel-open-orders": "preflight",
    "flatten-position": "preflight",
    "panic-stop": "preflight",
    "recover-order": "preflight",
}


def _format_rule_value_with_source(*, field: str, value: object, source: dict[str, str] | None) -> str:
    return f"{value} (source={rule_source_for(field, source)})"


def _format_chance_contract_change_detail(change: object | None) -> str:
    if change is None:
        return "tracked_fields=order_types,bid_types,ask_types,order_sides previous_snapshot=none change_detected=0"
    detected = bool(getattr(change, "detected", False))
    changed_fields = getattr(change, "changed_fields", {}) or {}
    if not detected:
        return "tracked_fields=order_types,bid_types,ask_types,order_sides previous_snapshot=present change_detected=0"
    rendered = ",".join(
        f"{field}:{list(values.get('previous', ()))}->{list(values.get('current', ()))}"
        for field, values in changed_fields.items()
    ) or "-"
    return (
        "tracked_fields=order_types,bid_types,ask_types,order_sides "
        f"previous_snapshot=present previous_fetched_ts={int(getattr(change, 'previous_fetched_ts', 0) or 0)} "
        f"change_detected=1 changed_fields={rendered}"
    )


def _buy_price_none_readiness_status(*, fields: dict[str, object]) -> str:
    if not bool(fields.get("allowed")):
        return "FAIL"
    if bool(fields.get("alias_used")):
        return "WARN"
    return "PASS"


def _clarify_dust_observational_summary(summary: object | None) -> str:
    text = str(summary or "").strip()
    if not text:
        return "none"
    return (
        text.replace("broker_qty=", "observed_broker_qty=")
        .replace("local_qty=", "observed_local_qty=")
        .replace("broker_notional_krw=", "observed_broker_notional_krw=")
        .replace("local_notional_krw=", "observed_local_notional_krw=")
    )


def _enforce_live_command_guard(command: str | None) -> None:
    """Central live-command guard so new dispatch paths have one policy surface."""
    if settings.MODE != "live":
        return
    guard = LIVE_COMMAND_GUARDS.get(str(command or "ticker"))
    if guard is None:
        return
    try:
        if guard == "startup":
            validate_live_run_startup_contract(settings)
        elif guard == "preflight":
            validate_live_mode_preflight(settings)
        else:
            raise LiveModeValidationError(f"unknown live command guard policy: {guard}")
    except LiveModeValidationError as exc:
        if guard == "startup":
            notify(
                safety_event(
                    "startup_gate_blocked",
                    client_order_id="-",
                    submit_attempt_id="-",
                    exchange_order_id="-",
                    reason_code="LIVE_STARTUP_GUARD",
                    alert_kind="startup_gate",
                    reason=str(exc),
                    state_to="HALTED",
                )
            )
        print(f"[LIVE-COMMAND-GUARD] {exc}")
        raise SystemExit(1) from exc


def _closed_candle_cutoff_ts_ms(*, interval: str, now_ms: int | None = None) -> int | None:
    interval_sec = parse_interval_sec(interval)
    interval_ms = max(1, int(interval_sec)) * 1000
    close_guard_ms = max(2_000, min(30_000, interval_ms // 20))
    current_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    cutoff_ts_ms = current_ms - interval_ms - close_guard_ms
    return cutoff_ts_ms if cutoff_ts_ms >= 0 else None


def load_recent(conn: sqlite3.Connection, need: int):
    through_ts_ms = _closed_candle_cutoff_ts_ms(interval=INTERVAL)
    query = """
        SELECT ts, close
        FROM candles
        WHERE pair=? AND interval=?
    """
    params: list[object] = [PAIR, INTERVAL]
    if through_ts_ms is not None:
        query += " AND ts <= ?"
        params.append(int(through_ts_ms))
    query += """
        ORDER BY ts DESC
        LIMIT ?
    """
    params.append(need)
    rows = conn.execute(query, tuple(params)).fetchall()

    if len(rows) < need:
        return None

    rows = list(reversed(rows))  # ASC
    closes = [float(r[1]) for r in rows]
    return rows, closes


def sma(values, n: int):
    return sum(values[-n:]) / n


def cmd_signal(short_n: int, long_n: int):
    conn = conn = ensure_db()
    r = compute_signal(conn, short_n, long_n)
    conn.close()
    if r is None:
        print(f"[SIGNAL] ?????????? ???????繹먮끍???????????????????嫄????? ???????곗뿨????癲ル슢???? sync?????????????????")
        return

    raw_suffix = f" raw_symbol={RAW_SYMBOL}" if RAW_SYMBOL else ""
    print(f"[SIGNAL {MARKET} {INTERVAL}{raw_suffix}] at {kst_str(r['ts'])}")
    print(f"  SMA(short={short_n}) prev={r['prev_s']:.2f} curr={r['curr_s']:.2f}")
    print(f"  SMA(long ={long_n}) prev={r['prev_l']:.2f} curr={r['curr_l']:.2f}")
    print(f"  last_close={r['last_close']:.2f}")
    print(f"  => {r['signal']}")


def cmd_explain(short_n: int, long_n: int):
    """Print recent signal explanation details."""
    need = long_n + 2
    conn = ensure_db()
    rows_closes = load_recent(conn, need)
    conn.close()

    if rows_closes is None:
        print(f"[EXPLAIN] ?????????? ???????繹먮끍???????????????????嫄????? need={need}")
        return

    rows, closes = rows_closes
    raw_suffix = f" raw_symbol={RAW_SYMBOL}" if RAW_SYMBOL else ""
    print(f"[EXPLAIN {MARKET} {INTERVAL}{raw_suffix}] last {need} closes (???????")
    for (ts, close) in rows:
        print(f"  {kst_str(int(ts))}  close={float(close):.2f}")

    conn = ensure_db()
    r = compute_signal(conn, short_n, long_n)
    conn.close()
    print("")
    print("????????????????????")
    print(f"  prev short SMA = ??????????됰Ŧ?????????{short_n}??close)")
    print(f"  prev long  SMA = ??????????됰Ŧ?????????{long_n}??close)")
    print(f"  curr short SMA = ????????????ш끽維뽳쭩?뱀땡???얩맪??{short_n}??close)")
    print(f"  curr long  SMA = ????????????ш끽維뽳쭩?뱀땡???얩맪??{long_n}??close)")
    print("")
    print(f"  prev_s={r['prev_s']:.2f}, prev_l={r['prev_l']:.2f}")
    print(f"  curr_s={r['curr_s']:.2f}, curr_l={r['curr_l']:.2f}")
    print(f"  => signal={r['signal']}")


def cmd_status():
    conn = ensure_db()
    init_portfolio(conn)
    cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
    cash = cash_available + cash_locked
    qty = asset_available + asset_locked
    reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)

    row = conn.execute(
        "SELECT close, ts FROM candles WHERE pair=? AND interval=? ORDER BY ts DESC LIMIT 1",
        (PAIR, INTERVAL),
    ).fetchone()
    conn.close()

    if row is None:
        print("[STATUS] ??????????????????筌?? ???????곗뿨????癲ル슢???? sync ????????")
        return

    last_close = float(row[0])
    ts = int(row[1])

    equity = cash + qty * last_close
    balance_diag: dict[str, object] = {
        "source": "not_checked",
        "reason": "not_checked",
        "failure_category": "none",
        "stale": None,
        "last_success_ts_ms": None,
        "last_asset_ts_ms": None,
    }
    try:
        broker = DEFAULT_BITHUMB_BROKER_CLASS()
        auth_diag = getattr(broker, "get_auth_runtime_diagnostics", lambda **_kwargs: {})(
            caller="cmd_health",
            env_summary=get_last_explicit_env_load_summary().as_dict(),
        )
        raw_diag = getattr(broker, "get_accounts_validation_diagnostics", lambda: {})()
        if isinstance(raw_diag, dict):
            balance_diag.update(raw_diag)
    except Exception as exc:
        auth_diag = {}
        balance_diag["reason"] = f"diagnostic_probe_failed: {type(exc).__name__}"
    conn = ensure_db()
    try:
        snapshot = build_canonical_position_snapshot(
            conn,
            metadata_raw=runtime_state.snapshot().last_reconcile_metadata,
            pair=settings.PAIR,
            portfolio_asset_qty=qty,
        )
    finally:
        conn.close()
    position_state = snapshot.position_state
    dust_context = snapshot.dust_context
    dust = position_state.raw_holdings
    dust_view = position_state.operator_diagnostics
    if dust_context.classification.present and dust_view.resume_allowed and dust_view.treat_as_flat:
        balance_diag["flat_start_allowed"] = True
        balance_diag["flat_start_reason"] = format_flat_start_reason_with_dust(
            balance_diag.get("flat_start_reason") or "flat_start_safe",
            dust_context,
        )
    raw_suffix = f" raw_symbol={RAW_SYMBOL}" if RAW_SYMBOL else ""
    print(f"[STATUS {MARKET} {INTERVAL}{raw_suffix}] at {kst_str(ts)}")
    print(f"  cash_krw={cash:,.0f} (available={cash_available:,.0f}, locked={cash_locked:,.0f})")
    print(f"  asset_qty={qty:.8f} (available={asset_available:.8f}, locked={asset_locked:.8f})")
    print(f"  last_close={last_close:,.0f}")
    print(f"  equity={equity:,.0f} KRW")
    print(
        "  "
        f"balance_source={balance_diag.get('source') or '-'} "
        f"reason={balance_diag.get('reason') or '-'} "
        f"category={balance_diag.get('failure_category') or '-'} "
        f"stale={balance_diag.get('stale')}"
    )
    print("  [RAW-HOLDINGS]")
    print(
        "    "
        f"state={dust.state} observed_broker_qty={dust.broker_qty:.8f} observed_local_qty={dust.local_qty:.8f} "
        f"delta_qty={dust.delta_qty:.8f} broker_local_match={1 if dust.broker_local_match else 0}"
    )
    print("  [NORMALIZED-EXPOSURE]")
    print(
        "    "
        f"effective_flat={1 if position_state.effective_flat else 0} "
        f"normalized_exposure_active={1 if position_state.normalized_exposure.normalized_exposure_active else 0} "
        f"has_executable_exposure={1 if position_state.normalized_exposure.has_executable_exposure else 0} "
        f"has_dust_only_remainder={1 if position_state.normalized_exposure.has_dust_only_remainder else 0} "
        f"normalized_exposure_qty={position_state.normalized_exposure.normalized_exposure_qty:.8f}"
    )
    print("  [OPERATOR-DIAGNOSTICS]")
    print(
        "    "
        f"state={dust_view.state} action={dust_view.operator_action} "
        f"resume_allowed={1 if dust_view.resume_allowed else 0} "
        f"new_orders_allowed={1 if dust_view.new_orders_allowed else 0} "
        f"treat_as_flat={1 if dust_view.treat_as_flat else 0}"
    )


def cmd_trades(limit: int):
    conn = ensure_db()
    rows = conn.execute(
        """
        SELECT ts, side, price, qty, fee, cash_after, asset_after, note
        FROM trades
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    print(f"[TRADES] last {limit}")
    for ts, side, price, qty, fee, cash_a, asset_a, note in reversed(rows):
        note_s = (note or "")
        print(
            f"  {kst_str(int(ts))} {side:4s} price={float(price):,.0f} qty={float(qty):.8f} "
            f"fee={float(fee):,.0f} cash={float(cash_a):,.0f} asset={float(asset_a):.8f} "
            f"note={note_s}"
        )


def cmd_orders(limit: int = 50):
    conn = ensure_db()
    rows = conn.execute(
        """
        SELECT client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts
        FROM orders
        ORDER BY created_ts DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    print(f"[ORDERS] last {limit}")
    for r in reversed(rows):
        print(dict(r))


def cmd_fills(limit: int = 50):
    conn = ensure_db()
    rows = conn.execute(
        """
        SELECT client_order_id, fill_ts, price, qty, fee
        FROM fills
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    print(f"[FILLS] last {limit}")
    for r in reversed(rows):
        print(dict(r))


def cmd_audit():
    conn = ensure_db()
    init_portfolio(conn)

    errors: list[str] = []
    warnings: list[str] = []

    portfolio = conn.execute("SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1").fetchone()
    if portfolio is None:
        errors.append("portfolio row(id=1) missing")
    else:
        cash_krw = float(portfolio["cash_krw"])
        asset_qty = float(portfolio["asset_qty"])
        cash_available = float(portfolio["cash_available"])
        cash_locked = float(portfolio["cash_locked"])
        asset_available = float(portfolio["asset_available"])
        asset_locked = float(portfolio["asset_locked"])
        if cash_krw < 0:
            errors.append(f"portfolio.cash_krw is negative: {cash_krw}")
        if asset_qty < 0:
            errors.append(f"portfolio.asset_qty is negative: {asset_qty}")
        if cash_available < 0:
            errors.append(f"portfolio.cash_available is negative: {cash_available}")
        if cash_locked < 0:
            errors.append(f"portfolio.cash_locked is negative: {cash_locked}")
        if asset_available < 0:
            errors.append(f"portfolio.asset_available is negative: {asset_available}")
        if asset_locked < 0:
            errors.append(f"portfolio.asset_locked is negative: {asset_locked}")
        if abs(portfolio_cash_total(cash_available=cash_available, cash_locked=cash_locked) - cash_krw) > 1e-8:
            errors.append(
                "portfolio cash split mismatch: "
                f"available+locked={portfolio_cash_total(cash_available=cash_available, cash_locked=cash_locked)} != cash_krw={cash_krw}"
            )
        if abs(portfolio_asset_total(asset_available=asset_available, asset_locked=asset_locked) - asset_qty) > 1e-12:
            errors.append(
                "portfolio asset split mismatch: "
                f"available+locked={portfolio_asset_total(asset_available=asset_available, asset_locked=asset_locked)} != asset_qty={asset_qty}"
            )

    filled_without_qty = conn.execute(
        """
        SELECT client_order_id, qty_filled
        FROM orders
        WHERE status='FILLED' AND qty_filled <= 0
        """
    ).fetchall()
    for row in filled_without_qty:
        errors.append(
            f"order {row['client_order_id']} has FILLED status but qty_filled={float(row['qty_filled'])}"
        )

    terminal_orders_with_pending_local_intent = conn.execute(
        """
        SELECT client_order_id, status, local_intent_state
        FROM orders
        WHERE status IN ('FILLED', 'FAILED', 'CANCELED')
          AND COALESCE(local_intent_state, '')='PENDING_SUBMIT'
        """
    ).fetchall()
    for row in terminal_orders_with_pending_local_intent:
        errors.append(
            "terminal order retained pending local intent state: "
            f"client_order_id={row['client_order_id']} "
            f"status={row['status']} local_intent_state={row['local_intent_state']}"
        )

    orphan_fills = conn.execute(
        """
        SELECT f.id, f.client_order_id
        FROM fills f
        LEFT JOIN orders o ON o.client_order_id = f.client_order_id
        WHERE o.client_order_id IS NULL
        """
    ).fetchall()
    for row in orphan_fills:
        errors.append(f"fill id={row['id']} references missing order {row['client_order_id']}")

    bad_buy_snapshots = conn.execute(
        """
        SELECT id, side, qty, fee, cash_after, asset_after
        FROM trades
        WHERE side='BUY' AND (cash_after + fee < 0 OR asset_after < qty)
        """
    ).fetchall()
    for row in bad_buy_snapshots:
        errors.append(
            "trade id={id} BUY snapshot impossible: cash_after={cash_after}, fee={fee}, asset_after={asset_after}, qty={qty}".format(
                id=row["id"],
                cash_after=float(row["cash_after"]),
                fee=float(row["fee"]),
                asset_after=float(row["asset_after"]),
                qty=float(row["qty"]),
            )
        )

    bad_sell_snapshots = conn.execute(
        """
        SELECT id, side, qty, cash_after, asset_after
        FROM trades
        WHERE side='SELL' AND (cash_after < 0 OR asset_after > qty)
        """
    ).fetchall()
    for row in bad_sell_snapshots:
        errors.append(
            "trade id={id} SELL snapshot impossible: cash_after={cash_after}, asset_after={asset_after}, qty={qty}".format(
                id=row["id"],
                cash_after=float(row["cash_after"]),
                asset_after=float(row["asset_after"]),
                qty=float(row["qty"]),
            )
        )

    replay = compute_accounting_replay(conn)
    projection_cash = float(replay["replay_cash"])
    projection_qty = float(replay["replay_qty"])
    projection_cash_available = float(replay["replay_cash_available"])
    projection_cash_locked = float(replay["replay_cash_locked"])
    projection_asset_available = float(replay["replay_asset_available"])
    projection_asset_locked = float(replay["replay_asset_locked"])
    if portfolio is not None:
        portfolio_cash = portfolio_cash_total(cash_available=cash_available, cash_locked=cash_locked)
        portfolio_asset = portfolio_asset_total(asset_available=asset_available, asset_locked=asset_locked)
        if not math.isclose(projection_cash, portfolio_cash, abs_tol=1e-6):
            errors.append(
                "authoritative accounting projection cash mismatch: "
                f"projection_cash={projection_cash} portfolio_cash={portfolio_cash} "
                f"projection_model={replay['projection_model']}"
            )
        if not math.isclose(projection_qty, portfolio_asset, abs_tol=1e-10):
            errors.append(
                "authoritative accounting projection asset mismatch: "
                f"projection_qty={projection_qty} portfolio_asset={portfolio_asset} "
                f"projection_model={replay['projection_model']}"
            )
        if not math.isclose(projection_cash_available, cash_available, abs_tol=1e-6):
            errors.append(
                "authoritative accounting projection cash_available mismatch: "
                f"projection_cash_available={projection_cash_available} portfolio_cash_available={cash_available}"
            )
        if not math.isclose(projection_cash_locked, cash_locked, abs_tol=1e-6):
            errors.append(
                "authoritative accounting projection cash_locked mismatch: "
                f"projection_cash_locked={projection_cash_locked} portfolio_cash_locked={cash_locked}"
            )
        if not math.isclose(projection_asset_available, asset_available, abs_tol=1e-10):
            errors.append(
                "authoritative accounting projection asset_available mismatch: "
                f"projection_asset_available={projection_asset_available} portfolio_asset_available={asset_available}"
            )
        if not math.isclose(projection_asset_locked, asset_locked, abs_tol=1e-10):
            errors.append(
                "authoritative accounting projection asset_locked mismatch: "
                f"projection_asset_locked={projection_asset_locked} portfolio_asset_locked={asset_locked}"
            )

    last_trade = conn.execute(
        "SELECT id, ts, cash_after, asset_after FROM trades ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if last_trade is not None and portfolio is not None:
        post_trade_event_count = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM external_cash_adjustments WHERE event_ts >= ?) +
                (SELECT COUNT(*) FROM manual_flat_accounting_repairs WHERE event_ts >= ?) +
                (SELECT COUNT(*) FROM external_position_adjustments WHERE event_ts >= ?) AS count
            """,
            (int(last_trade["ts"]), int(last_trade["ts"]), int(last_trade["ts"])),
        ).fetchone()
        post_trade_accounting_events = int(post_trade_event_count["count"] if post_trade_event_count else 0)
        latest_cash_mismatch = abs(float(last_trade["cash_after"]) - portfolio_cash_total(cash_available=cash_available, cash_locked=cash_locked)) > 1e-8
        latest_asset_mismatch = abs(float(last_trade["asset_after"]) - portfolio_asset_total(asset_available=asset_available, asset_locked=asset_locked)) > 1e-12
        if latest_cash_mismatch or latest_asset_mismatch:
            detail = (
                "stale execution snapshot: "
                f"trade_id={int(last_trade['id'])} "
                f"trade_cash_after={float(last_trade['cash_after'])} "
                f"trade_asset_after={float(last_trade['asset_after'])} "
                f"portfolio_cash={portfolio_cash_total(cash_available=cash_available, cash_locked=cash_locked)} "
                f"portfolio_asset={portfolio_asset_total(asset_available=asset_available, asset_locked=asset_locked)} "
                f"post_trade_accounting_event_count={post_trade_accounting_events}"
            )
            if post_trade_accounting_events > 0 and not errors:
                warnings.append(detail)
            else:
                errors.append(detail)

    conn.close()

    if errors:
        print("[AUDIT] FAILED")
        for err in errors:
            print(f"  - {err}")
        raise SystemExit(1)

    for warning in warnings:
        print(f"[AUDIT] WARN {warning}")
    print(
        "[AUDIT] projection "
        f"model={replay['projection_model']} "
        f"included_event_families={','.join(replay['included_event_families'])} "
        f"diagnostic_event_families={','.join(replay['diagnostic_event_families'])} "
        f"unresolved_fee_state={1 if bool(replay['unresolved_fee_state']) else 0}"
    )
    print("[AUDIT] OK")


def _finalize_repair_runtime_policy(
    *,
    reason_code: str,
    metadata: dict[str, object],
) -> bool:
    runtime_state.record_reconcile_result(
        success=True,
        reason_code=reason_code,
        metadata=metadata,
    )
    runtime_state.refresh_open_order_health()
    runtime_state.set_startup_gate_reason(None)
    runtime_state.set_resume_gate(blocked=False, reason=None)
    resume_allowed, _ = evaluate_resume_eligibility()
    if resume_allowed:
        enable_trading()
        return True
    return False


def cmd_run(short_n: int, long_n: int):
    from .engine import run_loop
    from .run_lock import RunLockError, acquire_run_lock

    log_live_execution_contract(
        settings,
        caller="cmd_run",
        env_summary=get_last_explicit_env_load_summary().as_dict(),
    )
    try:
        validate_live_run_startup_contract(settings)
    except LiveModeValidationError as e:
        notify(
            safety_event(
                "startup_gate_blocked",
                client_order_id="-",
                submit_attempt_id="-",
                exchange_order_id="-",
                reason_code="LIVE_STARTUP_GUARD",
                alert_kind="startup_gate",
                reason=str(e),
                state_to="HALTED",
            )
        )
        print(f"[RUN] {e}")
        raise SystemExit(1) from e

    try:
        with acquire_run_lock(Path(settings.RUN_LOCK_PATH)):
            run_loop(short_n, long_n)
    except RunLockError as e:
        run_lock_status = read_run_lock_status(Path(settings.RUN_LOCK_PATH))
        notify(
            safety_event(
                "run_lock_conflict",
                client_order_id="-",
                submit_attempt_id="-",
                exchange_order_id="-",
                reason_code="RUN_LOCK_CONFLICT",
                alert_kind="run_lock_conflict",
                lock_path=str(run_lock_status.lock_path),
                lock_owner_pid=run_lock_status.owner_pid if run_lock_status.owner_pid is not None else "-",
                lock_owner_hostname=run_lock_status.owner_hostname or "-",
                lock_created_at=run_lock_status.created_at or "-",
                lock_age_seconds=run_lock_status.age_seconds if run_lock_status.age_seconds is not None else "-",
                lock_owner_state=run_lock_status.owner_state_text,
                lock_stale_candidate=1 if run_lock_status.is_stale_candidate else 0,
                lock_owner_text=run_lock_status.owner_text or "-",
                lock_human_text=run_lock_status.to_human_text(),
                reason=str(e),
            )
        )
        print(f"[RUN] {e}")
        raise SystemExit(1) from e


def cmd_health() -> None:
    refresh_open_order_health()
    health = get_health_status()
    submit_unknown_count = 0
    attribution_quality = None
    recovery_attribution_signals = None
    external_cash_adjustment_summary = None
    external_position_adjustment_summary = None
    manual_flat_repair_summary = None
    manual_flat_repair_preview = None
    external_position_repair_preview = None
    fee_gap_repair_summary = None
    fee_gap_repair_preview = None
    fee_rate_drift = None
    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS submit_unknown_count FROM orders WHERE status='SUBMIT_UNKNOWN'"
        ).fetchone()
        attribution_quality = fetch_attribution_quality_summary(conn)
        recovery_attribution_signals = fetch_recovery_attribution_signal_summary(
            conn,
            last_reconcile_epoch_sec=(
                float(health["last_reconcile_epoch_sec"])
                if health.get("last_reconcile_epoch_sec") is not None
                else None
            ),
        )
        external_cash_adjustment_summary = get_external_cash_adjustment_summary(conn)
        external_position_adjustment_summary = get_external_position_adjustment_summary(conn)
        manual_flat_repair_summary = get_manual_flat_accounting_repair_summary(conn)
        manual_flat_repair_preview = build_manual_flat_accounting_repair_preview(conn)
        external_position_repair_preview = build_external_position_accounting_repair_preview(conn)
        fee_gap_repair_summary = get_fee_gap_accounting_repair_summary(conn)
        fee_gap_repair_preview = build_fee_gap_accounting_repair_preview(conn)
        fee_rate_drift = _fee_rate_drift_diagnostics(conn)
    finally:
        conn.close()
    if row is not None:
        submit_unknown_count = int(row["submit_unknown_count"] or 0)

    current_halt_reason = "none"
    halt_reason_for_summary = "none"
    if health["halt_reason_code"] or health["last_disable_reason"]:
        halt_reason_for_summary = str(health["halt_reason_code"] or "-")
        current_halt_reason = (
            f"code={health['halt_reason_code'] or '-'} "
            f"reason={health['last_disable_reason'] or '-'}"
        )

    open_order_count = 0
    remote_open_order_count: int | None = None
    position_summary = "flat"
    portfolio_conn = ensure_db()
    try:
        open_order_row = portfolio_conn.execute(
            """
            SELECT COUNT(*) AS open_order_count
            FROM orders
            WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING', 'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
            """
        ).fetchone()
        if open_order_row is not None:
            open_order_count = int(open_order_row["open_order_count"] or 0)
        readiness_snapshot = compute_runtime_readiness_snapshot(portfolio_conn)
    finally:
        portfolio_conn.close()

    reconcile_metadata_raw = health.get("last_reconcile_metadata")
    if reconcile_metadata_raw:
        try:
            reconcile_metadata = json.loads(str(reconcile_metadata_raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            reconcile_metadata = {}
        raw_remote_open_count = reconcile_metadata.get("remote_open_order_found")
        if raw_remote_open_count is not None:
            try:
                remote_open_order_count = max(0, int(raw_remote_open_count))
            except (TypeError, ValueError):
                remote_open_order_count = None

    state_label = "running"
    if bool(health["halt_new_orders_blocked"]):
        state_label = "halted"
    elif not bool(health["trading_enabled"]):
        state_label = "paused"

    dust_context = build_dust_display_context(readiness_snapshot.reconcile_metadata)
    position_state = readiness_snapshot.position_state
    normalized_exposure = position_state.normalized_exposure
    if normalized_exposure.terminal_state == "flat":
        position_summary = "flat"
    elif normalized_exposure.has_executable_exposure:
        position_summary = f"open_exposure_qty={normalized_exposure.open_exposure_qty:.8f}"
    elif normalized_exposure.has_dust_only_remainder:
        position_summary = f"dust_only_qty={normalized_exposure.dust_tracking_qty:.8f}"
    else:
        position_summary = f"non_executable_position_state={normalized_exposure.terminal_state}"
    dust = position_state.raw_holdings
    dust_view = position_state.operator_diagnostics
    resume_allowed, resume_blockers = evaluate_resume_eligibility()
    resume_blocker_codes = [str(blocker.code) for blocker in resume_blockers]
    resume_blocker_reason_codes = [str(getattr(blocker, "reason_code", blocker.code)) for blocker in resume_blockers]
    health = get_health_status()
    state_label = "running"
    if bool(health["halt_new_orders_blocked"]):
        state_label = "halted"
    elif not bool(health["trading_enabled"]):
        state_label = "paused"

    current_halt_reason = "none"
    halt_reason_for_summary = "none"
    if health["halt_reason_code"] or health["last_disable_reason"]:
        halt_reason_for_summary = str(health["halt_reason_code"] or "-")
        current_halt_reason = (
            f"code={health['halt_reason_code'] or '-'} "
            f"reason={health['last_disable_reason'] or '-'}"
        )
    can_resume_label = "true" if bool(resume_allowed) else "false"
    blockers_label = ", ".join(resume_blocker_codes) if resume_blocker_codes else "none"
    blocker_reason_codes_label = ", ".join(resume_blocker_reason_codes) if resume_blocker_reason_codes else "none"
    unsafe_reasons: list[str] = []
    if not bool(resume_allowed):
        for blocker in resume_blockers[:3]:
            unsafe_reasons.append(str(blocker.code))
        if not unsafe_reasons:
            unsafe_reasons.append("RESUME_BLOCKED")
    if unsafe_reasons:
        resume_safety = f"unsafe ({', '.join(unsafe_reasons)})"
    else:
        resume_safety = "safe"

    recommended_commands = "uv run python bot.py recovery-report"
    if health["startup_gate_reason"]:
        halt_reason_for_summary = "STARTUP_SAFETY_GATE"
        recommended_commands = "uv run python bot.py reconcile | uv run python bot.py recovery-report"
    elif bool(health.get("emergency_flatten_blocked")):
        halt_reason_for_summary = "EMERGENCY_FLATTEN_UNRESOLVED"
        recommended_commands = "uv run python bot.py flatten-position | uv run python bot.py recovery-report"
    elif health["recovery_required_count"] > 0:
        recommended_commands = (
            "uv run python bot.py recover-order --client-order-id <id>"
            " | uv run python bot.py recovery-report"
        )
    elif dust.present and not dust_view.resume_allowed:
        halt_reason_for_summary = (
            "HARMLESS_DUST_POLICY_REVIEW_REQUIRED"
            if dust_view.state == "harmless_dust"
            else "BLOCKING_DUST_REVIEW_REQUIRED"
        )
        recommended_commands = "uv run python bot.py recovery-report"
    elif halt_reason_for_summary == "KILL_SWITCH":
        recommended_commands = "uv run python bot.py recovery-report | uv run python bot.py resume"

    has_critical_state = bool(
        health["startup_gate_reason"]
        or health["halt_new_orders_blocked"]
        or bool(health.get("emergency_flatten_blocked"))
        or health["recovery_required_count"] > 0
        or (dust.present and not dust_view.resume_allowed)
    )

    reconcile_latest = "none"
    if health["last_reconcile_status"]:
        reconcile_latest = (
            f"epoch_sec={health['last_reconcile_epoch_sec'] if health['last_reconcile_epoch_sec'] is not None else '-'} "
            f"status={health['last_reconcile_status']} "
            f"reason_code={health['last_reconcile_reason_code'] or '-'}"
        )

    candle_status = str(health.get("last_candle_status") or "unknown")
    candle_age = health.get("last_candle_age_sec")
    candle_observed = health.get("last_candle_sync_epoch_sec")
    candle_ts_ms = health.get("last_candle_ts_ms")
    candle_detail = health.get("last_candle_status_detail")
    balance_diag: dict[str, object] = {
        "source": "not_checked",
        "reason": "not_checked",
        "failure_category": "none",
        "stale": None,
        "last_success_ts_ms": None,
        "last_observed_ts_ms": None,
        "last_asset_ts_ms": None,
    }
    auth_diag: dict[str, object] = {}
    try:
        broker, auth_diag = build_broker_with_auth_diagnostics(
            caller="cmd_health",
            env_summary=get_last_explicit_env_load_summary().as_dict(),
            broker_factory=DEFAULT_BITHUMB_BROKER_CLASS,
        )
        raw_diag = getattr(broker, "get_accounts_validation_diagnostics", lambda: {})()
        if isinstance(raw_diag, dict):
            balance_diag.update(raw_diag)
    except Exception as exc:
        balance_diag["reason"] = f"diagnostic_probe_failed: {type(exc).__name__}"
    if dust_context.classification.present and dust_view.resume_allowed and dust_view.treat_as_flat:
        balance_diag["flat_start_allowed"] = True
        balance_diag["flat_start_reason"] = format_flat_start_reason_with_dust(
            balance_diag.get("flat_start_reason") or "flat_start_safe",
            dust_context,
        )

    print("[HEALTH]")
    print("  [HALT-RECOVERY-STATUS]")
    print(
        "    "
        f"state={state_label} "
        f"trading_enabled={1 if bool(health['trading_enabled']) else 0} "
        f"halt_new_orders_blocked={1 if bool(health['halt_new_orders_blocked']) else 0}"
    )
    print(f"    reason={current_halt_reason}")
    print(
        "    "
        f"unresolved_open_order_count={health['unresolved_open_order_count']} "
        f"open_order_count={open_order_count} "
        f"recovery_required_count={health['recovery_required_count']}"
    )
    if remote_open_order_count is None:
        print("    broker_open_order_count=unknown")
    else:
        print(f"    broker_open_order_count={remote_open_order_count}")
    print(
        "    "
        f"position={position_summary} "
        f"entry_allowed={1 if position_state.normalized_exposure.entry_allowed else 0} "
        f"entry_block_reason={position_state.normalized_exposure.entry_block_reason} "
        f"exit_allowed={1 if position_state.normalized_exposure.exit_allowed else 0} "
        f"exit_block_reason={position_state.normalized_exposure.exit_block_reason} "
        f"effective_flat_due_to_harmless_dust={1 if position_state.effective_flat_due_to_harmless_dust else 0}"
    )
    print(
        "    "
        f"total_holdings_qty={float(position_state.normalized_exposure.raw_total_asset_qty):.8f} "
        f"executable_exposure_qty={float(position_state.normalized_exposure.open_exposure_qty):.8f} "
        f"tracked_dust_qty={float(position_state.normalized_exposure.dust_tracking_qty):.8f} "
        f"reserved_exit_qty={float(position_state.normalized_exposure.reserved_exit_qty):.8f} "
        f"sellable_executable_qty={float(position_state.normalized_exposure.sellable_executable_qty):.8f} "
        f"terminal_state={position_state.normalized_exposure.terminal_state}"
    )
    print(
        "    "
        f"state_outcome={position_state.state_interpretation.operator_outcome} "
        f"exit_submit_expected={1 if position_state.state_interpretation.exit_submit_expected else 0} "
        f"state_message={position_state.state_interpretation.operator_message}"
    )
    print(f"    can_resume={can_resume_label}")
    print(f"    blockers={blockers_label}")
    print(f"    blocker_reason_codes={blocker_reason_codes_label}")
    print(f"    recovery_stage={readiness_snapshot.recovery_stage}")
    print(
        "    recovery_blocker_categories="
        f"{', '.join(readiness_snapshot.blocker_categories) if readiness_snapshot.blocker_categories else 'none'}"
    )
    print(f"    canonical_next_action={readiness_snapshot.operator_next_action}")
    print(
        "    "
        f"canonical_state={readiness_snapshot.canonical_state} "
        f"residual_class={readiness_snapshot.residual_class} "
        f"strategy_tradeability_state={readiness_snapshot.tradeability_operator_fields['strategy_tradeability_state']} "
        f"run_loop_allowed={1 if readiness_snapshot.run_loop_allowed else 0} "
        f"new_entry_allowed={1 if readiness_snapshot.new_entry_allowed else 0} "
        f"closeout_allowed={1 if readiness_snapshot.closeout_allowed else 0} "
        f"execution_flat={1 if readiness_snapshot.execution_flat else 0} "
        f"accounting_flat={1 if readiness_snapshot.accounting_flat else 0} "
        f"effective_flat={1 if readiness_snapshot.effective_flat else 0} "
        f"operator_action_required={1 if readiness_snapshot.operator_action_required else 0} "
        f"why_not={readiness_snapshot.why_not}"
    )
    print(f"    resume_safety={resume_safety}")
    print(
        "    "
        f"dust_state={dust_view.state} "
        f"dust_display_scope={readiness_snapshot.tradeability_operator_fields['dust_display_scope']} "
        f"residue_policy_state={readiness_snapshot.tradeability_operator_fields['residue_policy_state']} "
        "dust_tradeability_consistent="
        f"{1 if readiness_snapshot.tradeability_operator_fields['dust_tradeability_consistent'] else 0} "
        f"dust_action={dust_view.operator_action} "
        f"dust_new_orders_allowed={1 if dust_view.new_orders_allowed else 0} "
        f"dust_resume_allowed={1 if dust_view.resume_allowed else 0} "
        f"dust_treat_as_flat={1 if dust_view.treat_as_flat else 0}"
    )
    print(
        "    "
        f"dust_observed_broker_qty={dust_view.broker_qty:.8f} "
        f"dust_observed_local_qty={dust_view.local_qty:.8f} "
        f"dust_delta_qty={dust_view.delta_qty:.8f} "
        f"dust_min_qty={dust_view.min_qty:.8f} "
        f"dust_min_notional_krw={dust_view.min_notional_krw:.1f} "
        f"dust_broker_local_match={1 if dust_view.broker_local_match else 0} "
        f"dust_qty_below_min={dust_context.qty_below_min_summary} "
        f"dust_notional_below_min={dust_context.notional_below_min_summary}"
    )
    print(
        "    "
        f"tradeability_operator_message={readiness_snapshot.tradeability_operator_fields['tradeability_operator_message']}"
    )
    print(
        "    "
        f"balance_source={balance_diag.get('source') or '-'} "
        f"diag_reason={balance_diag.get('reason') or '-'} "
        f"diag_category={balance_diag.get('failure_category') or '-'} "
        f"stale={balance_diag.get('stale')}"
    )
    print(
        "    "
        f"diag_execution_mode={balance_diag.get('execution_mode') or '-'} "
        f"quote_currency={balance_diag.get('quote_currency') or '-'} "
        f"base_currency={balance_diag.get('base_currency') or '-'} "
        f"base_missing_policy={balance_diag.get('base_currency_missing_policy') or '-'} "
        f"preflight_outcome={balance_diag.get('preflight_outcome') or '-'}"
    )
    if isinstance(auth_diag, dict) and auth_diag:
        env_summary = auth_diag.get("env") if isinstance(auth_diag.get("env"), dict) else {}
        chance_auth = auth_diag.get("chance_auth") if isinstance(auth_diag.get("chance_auth"), dict) else {}
        print(
            "    "
            f"env_source_key={env_summary.get('source_key') or '-'} "
            f"env_file={env_summary.get('env_file') or '-'} "
            f"env_loaded={1 if env_summary.get('loaded') else 0} "
            f"env_override={1 if env_summary.get('override') else 0}"
        )
        print(
            "    "
            f"auth_api_key_present={1 if auth_diag.get('api_key_present') else 0} "
            f"auth_api_key_length={auth_diag.get('api_key_length')} "
            f"auth_api_secret_present={1 if auth_diag.get('api_secret_present') else 0} "
            f"auth_api_secret_length={auth_diag.get('api_secret_length')} "
            f"auth_balance_source={auth_diag.get('balance_source_selected') or '-'} "
            f"auth_ws_myasset_enabled={1 if auth_diag.get('ws_myasset_enabled') else 0}"
        )
        print(
            "    "
            f"auth_preview_endpoint={chance_auth.get('endpoint') or '-'} "
            f"auth_preview_method={chance_auth.get('method') or '-'} "
            f"auth_branch={chance_auth.get('auth_branch') or '-'} "
            f"auth_query_hash_included={1 if chance_auth.get('query_hash_included') else 0} "
            f"auth_query_hash_preview={chance_auth.get('query_hash_preview') or '-'} "
            f"auth_fallback_branch_used={1 if chance_auth.get('fallback_branch_used') else 0}"
        )
    print(
        "    "
        f"recent_external_cash_adjustment={_format_external_cash_adjustment_summary(external_cash_adjustment_summary)}"
    )
    print(
        "    "
        "manual_flat_accounting_repair="
        f"needed={1 if bool(manual_flat_repair_preview and manual_flat_repair_preview.get('needs_repair')) else 0} "
        f"safe_to_apply={1 if bool(manual_flat_repair_preview and manual_flat_repair_preview.get('safe_to_apply')) else 0} "
        f"repair_count={int(manual_flat_repair_summary.get('repair_count') or 0) if isinstance(manual_flat_repair_summary, dict) else 0} "
        f"reason={manual_flat_repair_preview.get('eligibility_reason') if isinstance(manual_flat_repair_preview, dict) else 'none'}"
    )
    print(
        "    "
        "fee_gap_accounting_repair="
        f"incident_kind={fee_gap_repair_preview.get('incident_kind') if isinstance(fee_gap_repair_preview, dict) else 'unknown'} "
        f"incident_scope={fee_gap_repair_preview.get('incident_scope') if isinstance(fee_gap_repair_preview, dict) else 'unknown'} "
        f"resolution_state={fee_gap_repair_preview.get('resolution_state') if isinstance(fee_gap_repair_preview, dict) else 'unknown'} "
        f"active_issue={1 if bool(fee_gap_repair_preview and fee_gap_repair_preview.get('active_issue')) else 0} "
        f"needed={1 if bool(fee_gap_repair_preview and fee_gap_repair_preview.get('needs_repair')) else 0} "
        f"resume_blocking={1 if bool(fee_gap_repair_preview and fee_gap_repair_preview.get('resume_blocking')) else 0} "
        f"closeout_blocking={1 if bool(fee_gap_repair_preview and fee_gap_repair_preview.get('closeout_blocking')) else 0} "
        f"resume_policy={fee_gap_repair_preview.get('resume_policy') if isinstance(fee_gap_repair_preview, dict) else 'none'} "
        f"safe_to_apply={1 if bool(fee_gap_repair_preview and fee_gap_repair_preview.get('safe_to_apply')) else 0} "
        f"repair_count={int(fee_gap_repair_summary.get('repair_count') or 0) if isinstance(fee_gap_repair_summary, dict) else 0} "
        f"reason={fee_gap_repair_preview.get('eligibility_reason') if isinstance(fee_gap_repair_preview, dict) else 'none'}"
    )

    print("  [RISK-SNAPSHOT]")
    print(
        "    "
        f"unresolved_open_order_count={health['unresolved_open_order_count']} "
        f"recovery_required_count={health['recovery_required_count']} "
        f"submit_unknown_count={submit_unknown_count}"
    )
    print(f"    current_halt_reason={current_halt_reason}")
    print(f"    reconcile_latest={reconcile_latest}")
    if attribution_quality is not None:
        print("  [ATTRIBUTION-QUALITY-SNAPSHOT]")
        print(
            "    "
            f"trade_count={attribution_quality.total_trade_count} "
            f"unattributed_trade_count={attribution_quality.unattributed_trade_count} "
            f"ambiguous_linkage_count={attribution_quality.ambiguous_linkage_count} "
            f"recovery_derived_attribution_count={attribution_quality.recovery_derived_attribution_count}"
        )
        print(
            "    "
            "reason_buckets="
            f"missing_decision_id:{attribution_quality.reason_buckets.get('missing_decision_id', 0)},"
            f"multiple_candidate_decisions:{attribution_quality.reason_buckets.get('multiple_candidate_decisions', 0)},"
            f"legacy_incomplete_row:{attribution_quality.reason_buckets.get('legacy_incomplete_row', 0)},"
            f"recovery_unresolved_linkage:{attribution_quality.reason_buckets.get('recovery_unresolved_linkage', 0)}"
        )
        if recovery_attribution_signals is not None:
            print(
                "    "
                f"unresolved_attribution_count={recovery_attribution_signals.unresolved_attribution_count} "
                f"recent_recovery_derived_trade_count={recovery_attribution_signals.recent_recovery_derived_trade_count} "
                "ambiguous_linkage_after_recent_reconcile="
                f"{recovery_attribution_signals.ambiguous_linkage_after_recent_reconcile}"
            )
    if has_critical_state:
        print("  [CRITICAL-OPERATOR-SUMMARY]")
        print(
            "    "
            f"halt_reason={halt_reason_for_summary} "
            f"unresolved_order_count={health['unresolved_open_order_count']} "
            f"open_order_count={open_order_count} "
            f"position={position_summary} "
            f"effective_flat_due_to_harmless_dust={1 if dust_context.effective_flat_due_to_harmless_dust else 0} "
            f"dust_state={dust_view.state}"
        )
        print(f"    next_commands={recommended_commands}")
    print("  [ORDER-RULE-SNAPSHOT]")
    try:
        resolved_rules = get_effective_order_rules(PAIR)
        rules = resolved_rules.rules
        source = resolved_rules.source or {}
        buy_price_none_resolution = resolve_buy_price_none_resolution(rules=rules)
        buy_price_none_fields = build_buy_price_none_diagnostic_fields(
            rules=rules,
            resolution=buy_price_none_resolution,
        )
        if getattr(resolved_rules, "fallback_used", False):
            print(
                "    "
                f"order_rules_autosync=FALLBACK "
                f"reason_code={resolved_rules.fallback_reason_code or '-'} "
                f"reason={resolved_rules.fallback_reason_summary or '-'} "
                f"risk={resolved_rules.fallback_risk or '-'}"
            )
        print(
            "    "
            f"min_qty={_format_rule_value_with_source(field='min_qty', value=rules.min_qty, source=source)} "
            f"qty_step={_format_rule_value_with_source(field='qty_step', value=rules.qty_step, source=source)} "
            f"min_notional_krw={_format_rule_value_with_source(field='min_notional_krw', value=rules.min_notional_krw, source=source)} "
            f"max_qty_decimals={_format_rule_value_with_source(field='max_qty_decimals', value=rules.max_qty_decimals, source=source)}"
        )
        print(
            "    "
            f"BUY(min_total_krw={_format_rule_value_with_source(field='bid_min_total_krw', value=rules.bid_min_total_krw, source=source)}, "
            f"price_unit={_format_rule_value_with_source(field='bid_price_unit', value=rules.bid_price_unit, source=source)}) "
            f"SELL(min_total_krw={_format_rule_value_with_source(field='ask_min_total_krw', value=rules.ask_min_total_krw, source=source)}, "
            f"price_unit={_format_rule_value_with_source(field='ask_price_unit', value=rules.ask_price_unit, source=source)})"
        )
        print(
            "    "
            "buy_price_none_resolution="
            f"raw_bid_types={buy_price_none_fields['raw_bid_types']} "
            f"raw_order_types={buy_price_none_fields['raw_order_types']} "
            f"raw_buy_supported_types={buy_price_none_fields['raw_buy_supported_types']} "
            f"support_source={buy_price_none_fields['support_source']} "
            f"resolved_contract={buy_price_none_fields['resolved_contract']} "
            f"contract_id={buy_price_none_fields['contract_id']} "
            f"resolved_order_type={buy_price_none_fields['resolved_order_type']} "
            f"submit_field={buy_price_none_fields['submit_field']} "
            f"allowed={buy_price_none_fields['allowed']} "
            f"decision_outcome={buy_price_none_fields['decision_outcome']} "
            f"decision_basis={buy_price_none_fields['decision_basis']} "
            f"alias_used={buy_price_none_fields['alias_used']} "
            f"alias_policy={buy_price_none_fields['alias_policy']} "
            f"block_reason={buy_price_none_fields['block_reason']}"
        )
        print(
            "    "
            "chance_contract_canary="
            f"{_format_chance_contract_change_detail(getattr(resolved_rules, 'chance_contract_change', None))}"
        )
    except Exception as exc:
        print(f"    failed_to_load={type(exc).__name__}: {exc}")
    print(
        "  "
        f"last_candle_age_sec={candle_age} "
        f"(status={candle_status}, sync_epoch_sec={candle_observed if candle_observed is not None else '-'}, "
        f"candle_ts_ms={candle_ts_ms if candle_ts_ms is not None else '-'}, "
        f"detail={candle_detail or '-'})"
    )
    print(f"  last_candle_status={candle_status}")
    print(f"  last_candle_sync_epoch_sec={candle_observed}")
    print(f"  last_candle_ts_ms={candle_ts_ms}")
    print(f"  last_candle_status_detail={candle_detail}")
    print(f"  error_count={health['error_count']}")
    print(f"  trading_enabled={health['trading_enabled']}")
    print(f"  retry_at_epoch_sec={health['retry_at_epoch_sec']}")
    print(f"  unresolved_open_order_count={health['unresolved_open_order_count']}")
    print(f"  oldest_unresolved_order_age_sec={health['oldest_unresolved_order_age_sec']}")
    print(f"  recovery_required_count={health['recovery_required_count']}")
    print(f"  last_reconcile_epoch_sec={health['last_reconcile_epoch_sec']}")
    print(f"  last_reconcile_status={health['last_reconcile_status']}")
    print(f"  last_reconcile_error={health['last_reconcile_error']}")
    print(f"  last_reconcile_reason_code={health['last_reconcile_reason_code']}")
    print(f"  last_reconcile_metadata={health['last_reconcile_metadata']}")
    if isinstance(fee_rate_drift, dict):
        observed_fee_bps = fee_rate_drift.get("observed_fee_bps_median")
        observed_fee_bps_text = "-" if observed_fee_bps is None else f"{float(observed_fee_bps):.3f}"
        deviation_bps = fee_rate_drift.get("configured_minus_observed_bps")
        deviation_bps_text = "-" if deviation_bps is None else f"{float(deviation_bps):.3f}"
        deviation_pct = fee_rate_drift.get("fee_rate_deviation_pct")
        deviation_pct_text = "-" if deviation_pct is None else f"{float(deviation_pct):.2f}"
        print(
            "  fee_rate_drift="
            f"configured_fee_rate={float(fee_rate_drift.get('configured_fee_rate') or 0.0):.6f} "
            f"configured_fee_rate_estimate={float(fee_rate_drift.get('configured_fee_rate_estimate') or 0.0):.6f} "
            f"configured_fee_bps={float(fee_rate_drift.get('configured_fee_bps') or 0.0):.3f} "
            f"observed_fee_bps_median={observed_fee_bps_text} "
            f"observed_fee_sample_count={int(fee_rate_drift.get('observed_fee_sample_count') or 0)} "
            f"fee_rate_deviation_pct={deviation_pct_text} "
            f"configured_minus_observed_bps={deviation_bps_text} "
            f"expected_fee_rate_warning_count={int(fee_rate_drift.get('expected_fee_rate_warning_count') or 0)} "
            f"recent_expected_fee_rate_mismatch_count={int(fee_rate_drift.get('recent_expected_fee_rate_mismatch_count') or 0)} "
            f"fee_pending_count={int(fee_rate_drift.get('fee_pending_count') or 0)} "
            f"recent_fee_pending_observation_count={int(fee_rate_drift.get('recent_fee_pending_observation_count') or 0)} "
            f"fee_pending_accounting_repair_count={int(fee_rate_drift.get('fee_pending_accounting_repair_count') or 0)} "
            f"position_authority_repair_count={int(fee_rate_drift.get('position_authority_repair_count') or 0)} "
            f"diagnostic_only_vs_startup_blocking={fee_rate_drift.get('diagnostic_only_vs_startup_blocking') or 'unknown'} "
            f"startup_impact={fee_rate_drift.get('startup_impact') or 'unknown'} "
            f"operator_action={fee_rate_drift.get('operator_action') or 'unknown'} "
            f"recommended_command={fee_rate_drift.get('recommended_command') or 'none'}"
        )
    rule_snapshot = get_cached_order_rule_snapshot(settings.PAIR)
    if rule_snapshot is not None:
        print(
            "  order_rule_snapshot="
            f"source_mode={rule_snapshot.source_mode} "
            f"retrieved_at_sec={rule_snapshot.retrieved_at_sec:.3f} "
            f"expires_at_sec={rule_snapshot.expires_at_sec:.3f} "
            f"stale={1 if rule_snapshot.is_stale() else 0} "
            f"fallback_used={1 if rule_snapshot.fallback_used else 0}"
        )
    else:
        print("  order_rule_snapshot=unknown")
    for key, value in dust_context.fields.items():
        if isinstance(value, bool):
            rendered = 1 if value else 0
        elif isinstance(value, float):
            if key in {"dust_broker_qty", "dust_local_qty", "dust_delta_qty", "dust_min_qty"}:
                rendered = f"{value:.8f}"
            elif key == "dust_min_notional_krw":
                rendered = f"{value:.1f}"
            else:
                rendered = value
        else:
            rendered = value
        print(f"  {key}={rendered}")
    print(f"  dust_qty_below_min={dust_context.qty_below_min_summary}")
    print(f"  dust_notional_below_min={dust_context.notional_below_min_summary}")
    print(f"  recovery_required_present={1 if health['recovery_required_count'] else 0}")
    print(f"  last_reconcile_epoch_sec={health['last_reconcile_epoch_sec']}")
    print(f"  last_reconcile_status={health['last_reconcile_status']}")
    print(f"  last_disable_reason={health['last_disable_reason']}")
    print(f"  halt_new_orders_blocked={health['halt_new_orders_blocked']}")
    print(f"  halt_reason_code={health['halt_reason_code']}")
    print(f"  halt_state_unresolved={health['halt_state_unresolved']}")
    print(f"  run_lock={read_run_lock_status(Path(settings.RUN_LOCK_PATH)).to_human_text()}")
    print(f"  last_cancel_open_orders_epoch_sec={health['last_cancel_open_orders_epoch_sec']}")
    print(f"  last_cancel_open_orders_trigger={health['last_cancel_open_orders_trigger']}")
    print(f"  last_cancel_open_orders_status={health['last_cancel_open_orders_status']}")
    print(f"  last_cancel_open_orders_summary={health['last_cancel_open_orders_summary']}")
    print(f"  emergency_flatten_blocked={health.get('emergency_flatten_blocked')}")
    print(f"  emergency_flatten_block_reason={health.get('emergency_flatten_block_reason')}")
    print(f"  startup_gate_reason={health['startup_gate_reason']}")
    print(f"  balance_source={balance_diag.get('source')}")
    print(f"  balance_source_reason={balance_diag.get('reason')}")
    print(f"  balance_source_failure_category={balance_diag.get('failure_category')}")
    print(f"  balance_source_last_success_ts_ms={balance_diag.get('last_success_ts_ms')}")
    print(f"  balance_source_last_observed_ts_ms={balance_diag.get('last_observed_ts_ms')}")
    print(f"  balance_source_last_asset_ts_ms={balance_diag.get('last_asset_ts_ms')}")
    print(f"  balance_source_stale={balance_diag.get('stale')}")
    print(f"  balance_source_execution_mode={balance_diag.get('execution_mode')}")
    print(f"  balance_source_quote_currency={balance_diag.get('quote_currency')}")
    print(f"  balance_source_base_currency={balance_diag.get('base_currency')}")
    print(f"  balance_source_base_currency_missing_policy={balance_diag.get('base_currency_missing_policy')}")
    print(f"  balance_source_preflight_outcome={balance_diag.get('preflight_outcome')}")
    print(f"  balance_source_flat_start_allowed={balance_diag.get('flat_start_allowed')}")
    print(f"  balance_source_flat_start_reason={balance_diag.get('flat_start_reason')}")
    print(
        "  external_position_accounting_repair_needed="
        f"{1 if bool(external_position_repair_preview and external_position_repair_preview.get('needs_repair')) else 0}"
    )
    print(
        "  external_position_accounting_repair_safe_to_apply="
        f"{1 if bool(external_position_repair_preview and external_position_repair_preview.get('safe_to_apply')) else 0}"
    )
    print(
        "  external_position_accounting_repair_reason="
        f"{external_position_repair_preview.get('eligibility_reason') if isinstance(external_position_repair_preview, dict) else 'none'}"
    )
    print(
        "  manual_flat_accounting_repair_needed="
        f"{1 if bool(manual_flat_repair_preview and manual_flat_repair_preview.get('needs_repair')) else 0}"
    )
    print(
        "  manual_flat_accounting_repair_safe_to_apply="
        f"{1 if bool(manual_flat_repair_preview and manual_flat_repair_preview.get('safe_to_apply')) else 0}"
    )
    print(
        "  manual_flat_accounting_repair_reason="
        f"{manual_flat_repair_preview.get('eligibility_reason') if isinstance(manual_flat_repair_preview, dict) else 'none'}"
    )
    print(
        "  fee_gap_accounting_repair_needed="
        f"{1 if bool(fee_gap_repair_preview and fee_gap_repair_preview.get('needs_repair')) else 0}"
    )
    print(
        "  fee_gap_accounting_repair_incident="
        f"kind={fee_gap_repair_preview.get('incident_kind') if isinstance(fee_gap_repair_preview, dict) else 'unknown'} "
        f"scope={fee_gap_repair_preview.get('incident_scope') if isinstance(fee_gap_repair_preview, dict) else 'unknown'} "
        f"resolution={fee_gap_repair_preview.get('resolution_state') if isinstance(fee_gap_repair_preview, dict) else 'unknown'} "
        f"active_issue={1 if bool(fee_gap_repair_preview and fee_gap_repair_preview.get('active_issue')) else 0}"
    )
    print(
        "  fee_gap_accounting_repair_safe_to_apply="
        f"{1 if bool(fee_gap_repair_preview and fee_gap_repair_preview.get('safe_to_apply')) else 0}"
    )
    print(
        "  fee_gap_accounting_repair_resume_blocking="
        f"{1 if bool(fee_gap_repair_preview and fee_gap_repair_preview.get('resume_blocking')) else 0}"
    )
    print(
        "  fee_gap_accounting_repair_resume_policy="
        f"{fee_gap_repair_preview.get('resume_policy') if isinstance(fee_gap_repair_preview, dict) else 'none'}"
    )
    print(
        "  fee_gap_accounting_repair_reason="
        f"{fee_gap_repair_preview.get('eligibility_reason') if isinstance(fee_gap_repair_preview, dict) else 'none'}"
    )


def _eod_price_for_day(conn: sqlite3.Connection, day: str) -> float | None:
    row = conn.execute(
        """
        SELECT close
        FROM candles
        WHERE pair=? AND interval=? AND strftime('%Y-%m-%d', ts/1000, 'unixepoch', '+9 hours')=?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (PAIR, INTERVAL, day),
    ).fetchone()
    if row is None:
        return None
    return float(row["close"])


def _ledger_replay(conn: sqlite3.Connection) -> dict[str, object]:
    init_portfolio(conn)
    replay = compute_accounting_replay(conn)

    p = conn.execute(
        "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    portfolio_cash = (
        portfolio_cash_total(
            cash_available=float(p["cash_available"]),
            cash_locked=float(p["cash_locked"]),
        )
        if p
        else 0.0
    )
    portfolio_cash_available = float(p["cash_available"]) if p else 0.0
    portfolio_qty = (
        portfolio_asset_total(
            asset_available=float(p["asset_available"]),
            asset_locked=float(p["asset_locked"]),
        )
        if p
        else 0.0
    )
    consistent = (
        math.isclose(float(replay["replay_cash"]), portfolio_cash, abs_tol=1e-6)
        and math.isclose(float(replay["replay_cash_available"]), portfolio_cash_available, abs_tol=1e-6)
        and math.isclose(float(replay["replay_cash_locked"]), float(p["cash_locked"]) if p else 0.0, abs_tol=1e-6)
        and math.isclose(float(replay["replay_qty"]), portfolio_qty, abs_tol=1e-10)
    )

    return {
        "replay_cash": float(replay["replay_cash"]),
        "replay_qty": float(replay["replay_qty"]),
        "portfolio_cash": portfolio_cash,
        "portfolio_cash_available": portfolio_cash_available,
        "portfolio_qty": portfolio_qty,
        "fee_total": float(replay["fee_total"]),
        "external_cash_adjustment_count": int(replay["external_cash_adjustment_count"]),
        "external_cash_adjustment_total": float(replay["external_cash_adjustment_total"]),
        "manual_flat_accounting_repair_count": int(replay["manual_flat_accounting_repair_count"]),
        "manual_flat_accounting_repair_cash_total": float(replay["manual_flat_accounting_repair_cash_total"]),
        "manual_flat_accounting_repair_asset_total": float(replay["manual_flat_accounting_repair_asset_total"]),
        "external_position_adjustment_count": int(replay["external_position_adjustment_count"]),
        "external_position_adjustment_cash_total": float(replay["external_position_adjustment_cash_total"]),
        "external_position_adjustment_asset_total": float(replay["external_position_adjustment_asset_total"]),
        "fee_gap_accounting_repair_count": int(replay["fee_gap_accounting_repair_count"]),
        "broker_fill_observation_count": int(replay["broker_fill_observation_count"]),
        "broker_fill_fee_pending_count": int(replay["broker_fill_fee_pending_count"]),
        "broker_fill_accounting_complete_count": int(replay["broker_fill_accounting_complete_count"]),
        "broker_fill_fee_candidate_order_level_count": int(replay["broker_fill_fee_candidate_order_level_count"]),
        "broker_fill_missing_fee_count": int(replay["broker_fill_missing_fee_count"]),
        "broker_fill_zero_reported_fee_count": int(replay["broker_fill_zero_reported_fee_count"]),
        "broker_fill_invalid_fee_count": int(replay["broker_fill_invalid_fee_count"]),
        "broker_fill_latest_unresolved_fee_pending_count": int(replay["broker_fill_latest_unresolved_fee_pending_count"]),
        "broker_fill_latest_accounting_complete_count": int(replay["broker_fill_latest_accounting_complete_count"]),
        "unresolved_fee_state": bool(replay["unresolved_fee_state"]),
        "fill_accounting_incident_projection": replay["fill_accounting_incident_projection"],
        "fill_accounting_active_issue_count": int(replay["fill_accounting_active_issue_count"]),
        "fill_accounting_already_accounted_observation_stale_count": int(
            replay["fill_accounting_already_accounted_observation_stale_count"]
        ),
        "fill_accounting_repaired_incident_count": int(replay["fill_accounting_repaired_incident_count"]),
        "fee_pending_accounting_repair_count": int(replay["fee_pending_accounting_repair_count"]),
        "position_authority_repair_count": int(replay["position_authority_repair_count"]),
        "dup_fill_count": int(replay["dup_fill_count"]),
        "projection_model": str(replay["projection_model"]),
        "projection_kind": str(replay["projection_kind"]),
        "included_event_families": tuple(replay["included_event_families"]),
        "diagnostic_event_families": tuple(replay["diagnostic_event_families"]),
        "omitted_event_families": tuple(replay["omitted_event_families"]),
        "consistent": consistent,
    }


def _fee_rate_drift_diagnostics(
    conn: sqlite3.Connection,
    *,
    observation_limit: int = 100,
) -> dict[str, object]:
    return build_fee_rate_drift_diagnostics(conn, observation_limit=observation_limit)


def cmd_audit_ledger() -> None:
    conn = ensure_db()
    try:
        replay = _ledger_replay(conn)
        readiness = compute_runtime_readiness_snapshot(conn).as_dict()
        preview = build_position_authority_rebuild_preview(conn)
    finally:
        conn.close()

    print("[AUDIT-LEDGER]")
    print(f"  replay_cash={float(replay['replay_cash']):,.3f}")
    print(f"  replay_qty={float(replay['replay_qty']):.10f}")
    print(f"  portfolio_cash={float(replay['portfolio_cash']):,.3f}")
    print(f"  portfolio_qty={float(replay['portfolio_qty']):.10f}")
    print(f"  fee_total={float(replay['fee_total']):,.3f}")
    print(f"  external_cash_adjustment_count={int(replay['external_cash_adjustment_count'])}")
    print(f"  external_cash_adjustment_total={float(replay['external_cash_adjustment_total']):,.3f}")
    print(f"  manual_flat_accounting_repair_count={int(replay['manual_flat_accounting_repair_count'])}")
    print(f"  manual_flat_accounting_repair_cash_total={float(replay['manual_flat_accounting_repair_cash_total']):,.3f}")
    print(f"  manual_flat_accounting_repair_asset_total={float(replay['manual_flat_accounting_repair_asset_total']):.10f}")
    print(f"  external_position_adjustment_count={int(replay['external_position_adjustment_count'])}")
    print(f"  external_position_adjustment_cash_total={float(replay['external_position_adjustment_cash_total']):,.3f}")
    print(f"  external_position_adjustment_asset_total={float(replay['external_position_adjustment_asset_total']):.10f}")
    print(f"  fee_gap_accounting_repair_count={int(replay['fee_gap_accounting_repair_count'])}")
    print(f"  broker_fill_observation_count={int(replay['broker_fill_observation_count'])}")
    print(f"  broker_fill_fee_pending_count={int(replay['broker_fill_fee_pending_count'])}")
    print(f"  broker_fill_accounting_complete_count={int(replay['broker_fill_accounting_complete_count'])}")
    print(f"  broker_fill_fee_candidate_order_level_count={int(replay['broker_fill_fee_candidate_order_level_count'])}")
    print(f"  broker_fill_missing_fee_count={int(replay['broker_fill_missing_fee_count'])}")
    print(f"  broker_fill_zero_reported_fee_count={int(replay['broker_fill_zero_reported_fee_count'])}")
    print(f"  broker_fill_invalid_fee_count={int(replay['broker_fill_invalid_fee_count'])}")
    print(f"  broker_fill_latest_unresolved_fee_pending_count={int(replay['broker_fill_latest_unresolved_fee_pending_count'])}")
    print(f"  broker_fill_latest_accounting_complete_count={int(replay['broker_fill_latest_accounting_complete_count'])}")
    print(f"  unresolved_fee_state={1 if bool(replay['unresolved_fee_state']) else 0}")
    print(f"  fill_accounting_active_issue_count={int(replay['fill_accounting_active_issue_count'])}")
    print(
        "  "
        f"fill_accounting_already_accounted_observation_stale_count="
        f"{int(replay['fill_accounting_already_accounted_observation_stale_count'])}"
    )
    print(f"  fill_accounting_repaired_incident_count={int(replay['fill_accounting_repaired_incident_count'])}")
    print(f"  fee_pending_accounting_repair_count={int(replay['fee_pending_accounting_repair_count'])}")
    print(f"  position_authority_repair_count={int(replay['position_authority_repair_count'])}")
    print(f"  dup_fill_count={int(replay['dup_fill_count'])}")
    print(f"  projection_model={replay['projection_model']}")
    print(f"  projection_kind={replay['projection_kind']}")
    print(f"  included_event_families={','.join(replay['included_event_families'])}")
    print(f"  diagnostic_event_families={','.join(replay['diagnostic_event_families'])}")
    print(f"  omitted_event_families={','.join(replay['omitted_event_families'])}")
    broker_qty = float(preview.get("broker_qty") or 0.0)
    broker_qty_known = bool(preview.get("broker_qty_known"))
    broker_portfolio_converged = bool(
        broker_qty_known
        and abs(normalize_asset_qty(broker_qty) - normalize_asset_qty(float(replay["portfolio_qty"]))) <= 1e-12
    )
    incident_class = str((readiness.get("position_authority_assessment") or {}).get("incident_class") or "NONE")
    print(f"  accounting_projection_ok={1 if bool(replay['consistent']) else 0}")
    print(f"  broker_portfolio_converged={1 if broker_portfolio_converged else 0}")
    print(f"  lot_projection_converged={1 if bool(readiness.get('projection_converged')) else 0}")
    print(f"  live_ready={1 if bool(readiness.get('resume_ready')) else 0}")
    print(f"  blocking_incident_class={incident_class}")
    print(f"  recommended_next_action={preview.get('recommended_command') or readiness.get('recommended_command') or 'none'}")

    if not bool(replay["consistent"]):
        print("[AUDIT-LEDGER] FAILED: replay result mismatches portfolio")
        raise SystemExit(1)

    print("[AUDIT-LEDGER] OK")



def _execution_quality_summary(conn: sqlite3.Connection, *, start_day: str) -> dict[str, float | int | None]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS fill_count,
            SUM(CASE WHEN reference_price IS NOT NULL AND slippage_bps IS NOT NULL THEN 1 ELSE 0 END) AS measured_count,
            AVG(slippage_bps) AS avg_slippage_bps,
            MAX(slippage_bps) AS worst_slippage_bps,
            MIN(slippage_bps) AS best_slippage_bps
        FROM fills
        WHERE strftime('%Y-%m-%d', fill_ts/1000, 'unixepoch', '+9 hours') >= ?
        """,
        (start_day,),
    ).fetchone()
    return {
        "fill_count": int(row["fill_count"] if row else 0),
        "measured_count": int(row["measured_count"] if row and row["measured_count"] is not None else 0),
        "avg_slippage_bps": (float(row["avg_slippage_bps"]) if row and row["avg_slippage_bps"] is not None else None),
        "worst_slippage_bps": (float(row["worst_slippage_bps"]) if row and row["worst_slippage_bps"] is not None else None),
        "best_slippage_bps": (float(row["best_slippage_bps"]) if row and row["best_slippage_bps"] is not None else None),
    }

def cmd_report(days: int) -> None:
    conn = ensure_db()
    try:
        now_kst = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9))).date()
        start_day = now_kst - timedelta(days=days - 1)
        rows: list[tuple[str, float, float, float, float, float, float]] = []

        prev_end = float(settings.START_CASH_KRW)
        peak = prev_end
        mdd = 0.0
        slippage_bps = float(settings.SLIPPAGE_BPS)

        for i in range(days):
            day = (start_day + timedelta(days=i)).isoformat()
            agg = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN side='SELL' THEN (price * qty) ELSE 0 END), 0) AS sell_notional,
                    COALESCE(SUM(CASE WHEN side='BUY' THEN (price * qty) ELSE 0 END), 0) AS buy_notional,
                    COALESCE(SUM(fee), 0) AS fee_total
                FROM trades
                WHERE strftime('%Y-%m-%d', ts/1000, 'unixepoch', '+9 hours')=?
                """,
                (day,),
            ).fetchone()

            day_fee = float(agg["fee_total"])
            realized = float(agg["sell_notional"]) - float(agg["buy_notional"]) - day_fee
            slippage_cost = (float(agg["sell_notional"]) + float(agg["buy_notional"])) * (slippage_bps / 10000.0)

            eod = conn.execute(
                """
                SELECT cash_after, asset_after
                FROM trades
                WHERE strftime('%Y-%m-%d', ts/1000, 'unixepoch', '+9 hours')=?
                ORDER BY ts DESC, id DESC
                LIMIT 1
                """,
                (day,),
            ).fetchone()

            if eod is None:
                end_equity = prev_end
                unrealized = 0.0
            else:
                eod_price = _eod_price_for_day(conn, day)
                if eod_price is None:
                    eod_price = float(eod["cash_after"]) if float(eod["asset_after"]) == 0 else 0.0
                end_equity = float(eod["cash_after"]) + (float(eod["asset_after"]) * float(eod_price))
                unrealized = end_equity - prev_end - realized

            drawdown = 0.0
            if peak > 0:
                drawdown = (peak - end_equity) / peak
            peak = max(peak, end_equity)
            mdd = max(mdd, drawdown)

            rows.append((day, prev_end, end_equity, realized, unrealized, day_fee, slippage_cost))
            prev_end = end_equity

        expected_bars = int((24 * 60 * 60 / parse_interval_sec(INTERVAL)) * days)
        candle_count = int(
            conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM candles
                WHERE pair=? AND interval=? AND ts >= ?
                """,
                (
                    PAIR,
                    INTERVAL,
                    int(datetime.combine(start_day, datetime.min.time(), tzinfo=timezone(timedelta(hours=9))).timestamp() * 1000),
                ),
            ).fetchone()["c"]
        )
        missing_rate = 1.0 - (candle_count / expected_bars) if expected_bars > 0 else 0.0

        replay = _ledger_replay(conn)
        exec_quality = _execution_quality_summary(conn, start_day=start_day.isoformat())
        dup_orders = int(
            conn.execute(
                "SELECT COALESCE(SUM(cnt-1),0) FROM (SELECT client_order_id, COUNT(*) cnt FROM orders GROUP BY client_order_id HAVING COUNT(*)>1)"
            ).fetchone()[0]
        )
    finally:
        conn.close()

    raw_symbol_detail = f" raw_symbol={RAW_SYMBOL}" if RAW_SYMBOL else ""
    print(f"[REPORT] days={days} market={MARKET}{raw_symbol_detail} interval={INTERVAL}")
    print("day,start_equity,end_equity,realized,unrealized,fee,slippage_est")
    for day, start_eq, end_eq, realized, unrealized, fee_total, slip in rows:
        print(f"{day},{start_eq:.2f},{end_eq:.2f},{realized:.2f},{unrealized:.2f},{fee_total:.2f},{slip:.2f}")

    target_daily = 0.001
    avg_daily = ((rows[-1][2] / rows[0][1]) ** (1 / len(rows)) - 1) if rows and rows[0][1] > 0 else 0.0
    gate_missing = missing_rate <= 0.05
    gate_dup = dup_orders == 0 and int(replay["dup_fill_count"]) == 0
    gate_consistency = bool(replay["consistent"])
    gate_mdd = mdd <= 0.20
    gate_pnl = avg_daily >= target_daily
    gate_pass = gate_missing and gate_dup and gate_consistency and gate_mdd and gate_pnl

    print("[SUMMARY]")
    print(f"  avg_daily_return={avg_daily * 100:.4f}%")
    print(f"  mdd={mdd * 100:.2f}%")
    print(f"  missing_candle_rate={missing_rate * 100:.2f}%")
    print(f"  fee_total={float(replay['fee_total']):,.2f}")
    print(f"  ledger_consistent={replay['consistent']}")
    print(f"  duplicate_orders={dup_orders}, duplicate_fills={int(replay['dup_fill_count'])}")
    print("[EXECUTION-QUALITY]")
    print(f"  fills={int(exec_quality['fill_count'])} measured={int(exec_quality['measured_count'])}")
    if exec_quality['avg_slippage_bps'] is None:
        print("  avg_slippage_bps=NA worst_slippage_bps=NA best_slippage_bps=NA")
    else:
        print(
            "  "
            f"avg_slippage_bps={float(exec_quality['avg_slippage_bps']):.3f} "
            f"worst_slippage_bps={float(exec_quality['worst_slippage_bps']):.3f} "
            f"best_slippage_bps={float(exec_quality['best_slippage_bps']):.3f}"
        )
    print("[GATE]")
    print(f"  data_missing_rate<=5%: {'PASS' if gate_missing else 'FAIL'}")
    print(f"  duplicate_orders/fills==0: {'PASS' if gate_dup else 'FAIL'}")
    print(f"  ledger_consistency: {'PASS' if gate_consistency else 'FAIL'}")
    print(f"  drawdown<=20%: {'PASS' if gate_mdd else 'FAIL'}")
    print(f"  avg_daily_return>=0.10%: {'PASS' if gate_pnl else 'FAIL'}")
    print(f"  => {'PASS' if gate_pass else 'FAIL'}")


def _print_operator_command_contract(
    command: str,
    *,
    precondition: str | None = None,
    warning: str | None = None,
    postcondition: str | None = None,
) -> None:
    if precondition is not None:
        print(f"[{command}] precondition={precondition}")
    if warning is not None:
        print(f"[{command}] warning={warning}")
    if postcondition is not None:
        print(f"[{command}] postcondition={postcondition}")


def _resume_blocker_summary(blockers) -> tuple[str, str]:
    blocker_codes = ", ".join(str(blocker.code) for blocker in blockers) if blockers else "none"
    blocker_reason_codes = (
        ", ".join(str(getattr(blocker, "reason_code", blocker.code)) for blocker in blockers)
        if blockers
        else "none"
    )
    return blocker_codes, blocker_reason_codes


def cmd_cancel_open_orders() -> None:
    if settings.MODE != "live":
        print(f"[CANCEL-OPEN-ORDERS] skipped: MODE={settings.MODE} (live only)")
        return

    if settings.LIVE_DRY_RUN:
        print(
            "[CANCEL-OPEN-ORDERS] refused: LIVE_DRY_RUN=true would only simulate cancel and can corrupt local state"
        )
        raise SystemExit(1)

    try:
        validate_live_mode_preflight(settings)
    except LiveModeValidationError as e:
        print(f"[CANCEL-OPEN-ORDERS] failed preflight: {e}")
        raise SystemExit(1)

    _print_operator_command_contract(
        "CANCEL-OPEN-ORDERS",
        precondition=(
            "MODE=live; LIVE_DRY_RUN=false; live preflight passed; "
            "only broker-matched unresolved orders are eligible"
        ),
        warning=(
            "live write command: every broker-matched open order for unresolved local ids will be canceled; "
            "stray remote orders are skipped and reported"
        ),
    )

    from .broker.bithumb import BithumbBroker

    broker = BithumbBroker()
    summary = cancel_open_orders_with_broker(broker)
    status = "partial" if int(summary["failed_count"]) > 0 else "ok"
    runtime_state.record_cancel_open_orders_result(
        trigger="operator-command",
        status=status,
        summary=summary,
    )
    resume_allowed, resume_blocks = evaluate_resume_eligibility()
    resume_blockers, resume_blocker_reason_codes = _resume_blocker_summary(resume_blocks)

    print("[CANCEL-OPEN-ORDERS]")
    print(f"  remote_open_count={summary['remote_open_count']}")
    print(f"  cancel_accepted_count={summary['cancel_accepted_count']}")
    print(f"  canceled_count={summary['canceled_count']}")
    print(f"  cancel_confirm_pending_count={summary['cancel_confirm_pending_count']}")
    print(f"  matched_local_count={summary['matched_local_count']}")
    print(f"  stray_canceled_count={summary['stray_canceled_count']}")
    print(f"  failed_count={summary['failed_count']}")
    for msg in summary["stray_messages"]:
        print(f"  - {msg}")
    for msg in summary["error_messages"]:
        print(f"  - {msg}")
    if int(summary["failed_count"]) > 0 or int(summary["remote_open_count"]) > int(summary["canceled_count"]):
        print(
            "[CANCEL-OPEN-ORDERS] warning="
            "some remote open orders remain or require manual review; run reconcile and recovery-report next"
        )
    state = runtime_state.snapshot()
    _print_operator_command_contract(
        "CANCEL-OPEN-ORDERS",
        postcondition=(
            f"status={status}; "
            f"remote_open_count={int(summary['remote_open_count'])}; "
            f"canceled_count={int(summary['canceled_count'])}; "
            f"failed_count={int(summary['failed_count'])}; "
            f"resume_gate_blocked={1 if state.resume_gate_blocked else 0}; "
            f"resume_allowed={1 if resume_allowed else 0}; "
            f"resume_blockers={resume_blockers}; "
            f"resume_blocker_reason_codes={resume_blocker_reason_codes}; "
            f"local_state_trigger={state.last_cancel_open_orders_trigger or '-'}"
        ),
    )


def cmd_broker_diagnose() -> None:
    if settings.MODE != "live":
        print(f"[BROKER-DIAGNOSE] failed: MODE={settings.MODE} (live only)")
        raise SystemExit(1)

    from .broker.bithumb import BithumbBroker, classify_private_api_error

    broker = BithumbBroker()
    checks: list[dict[str, str | bool]] = []
    account_validation_reason = "not_checked"
    account_validation_last_failure_reason = "none"

    def add_check(name: str, status: str, detail: str, *, critical: bool) -> None:
        checks.append({"name": name, "status": status, "detail": detail, "critical": critical})

    live_armed = settings.MODE == "live" and not settings.LIVE_DRY_RUN
    add_check(
        "live execution mode",
        "PASS",
        f"MODE={settings.MODE} LIVE_DRY_RUN={settings.LIVE_DRY_RUN} armed={live_armed}",
        critical=True,
    )
    add_check(
        "order submit routing",
        "PASS",
        "price=None => /v2/orders market/price order, price set => /v2/orders limit order",
        critical=True,
    )
    add_check(
        "order lookup path",
        "PASS",
        "get_order reads /v1/order directly; open/recent snapshots use /v1/orders",
        critical=True,
    )

    try:
        validate_live_mode_preflight(settings)
        add_check("config/env loaded", "PASS", "live preflight validation passed", critical=True)
    except LiveModeValidationError as e:
        add_check("config/env loaded", "FAIL", str(e), critical=True)

    balance = None
    try:
        balance = broker.get_balance()
        account_validation_reason = "ok"
        add_check("broker authentication", "PASS", "private API reachable", critical=True)
    except Exception as e:
        code, summary = classify_private_api_error(e)
        if code in {"AUTH_SIGN", "PERMISSION"}:
            account_validation_reason = "auth failure"
        elif code in {"TEMPORARY", "SERVER_INTERNAL_FAILURE"}:
            account_validation_reason = "transport failure"
        elif "missing quote currency row" in str(e).lower() or "missing base currency row" in str(e).lower():
            account_validation_reason = "required currency missing"
        elif "duplicate currency row" in str(e).lower():
            account_validation_reason = "duplicate currency"
        elif "schema mismatch" in str(e).lower():
            account_validation_reason = "schema mismatch"
        detail = f"private API failed [{code}] {summary} ({type(e).__name__}: {e})"
        add_check("broker authentication", "FAIL", detail, critical=True)
        add_check("balance query", "FAIL", detail, critical=True)

    if balance is not None:
        balance_summary = (
            f"cash_available={balance.cash_available:,.0f} cash_locked={balance.cash_locked:,.0f} "
            f"asset_available={balance.asset_available:.8f} asset_locked={balance.asset_locked:.8f}"
        )
        add_check("balance query", "PASS", balance_summary, critical=True)

    try:
        conn = ensure_db()
        try:
            rows = conn.execute(
                """
                SELECT client_order_id, exchange_order_id
                FROM orders
                WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING', 'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
                """
            ).fetchall()
        finally:
            conn.close()
        exchange_order_ids = sorted(
            {
                str(row["exchange_order_id"]).strip()
                for row in rows
                if str(row["exchange_order_id"] or "").strip()
            }
        )
        client_order_ids = sorted(
            {
                str(row["client_order_id"]).strip()
                for row in rows
                if str(row["client_order_id"] or "").strip()
            }
        )
        if exchange_order_ids or client_order_ids:
            open_orders = broker.get_open_orders(
                exchange_order_ids=exchange_order_ids,
                client_order_ids=client_order_ids,
            )
            add_check("open order query", "PASS", f"known_unresolved_count={len(open_orders)}", critical=False)
        else:
            add_check("open order query", "PASS", "known_unresolved_count=0 (no local unresolved ids)", critical=False)
    except Exception as e:
        add_check(
            "open order query",
            "WARN",
            f"identifier-scoped snapshot failed ({type(e).__name__}: {e})",
            critical=False,
        )

    try:
        rr = get_effective_order_rules(PAIR)
        rules = rr.rules
        source = rr.source or {}
        buy_price_none_resolution = resolve_buy_price_none_resolution(rules=rules)
        buy_price_none_fields = build_buy_price_none_diagnostic_fields(
            rules=rules,
            resolution=buy_price_none_resolution,
        )
        add_check(
            "symbol/order rule query",
            "PASS",
            (
                f"min_qty={_format_rule_value_with_source(field='min_qty', value=rules.min_qty, source=source)} "
                f"qty_step={_format_rule_value_with_source(field='qty_step', value=rules.qty_step, source=source)} "
                f"min_notional_krw={_format_rule_value_with_source(field='min_notional_krw', value=rules.min_notional_krw, source=source)} "
                f"max_qty_decimals={_format_rule_value_with_source(field='max_qty_decimals', value=rules.max_qty_decimals, source=source)} "
                f"bid_min_total_krw={_format_rule_value_with_source(field='bid_min_total_krw', value=rules.bid_min_total_krw, source=source)} "
                f"ask_min_total_krw={_format_rule_value_with_source(field='ask_min_total_krw', value=rules.ask_min_total_krw, source=source)} "
                f"bid_price_unit={_format_rule_value_with_source(field='bid_price_unit', value=rules.bid_price_unit, source=source)} "
                f"ask_price_unit={_format_rule_value_with_source(field='ask_price_unit', value=rules.ask_price_unit, source=source)}"
            ),
            critical=False,
        )
        add_check(
            "BUY price=None chance resolution",
            _buy_price_none_readiness_status(fields=buy_price_none_fields),
            (
                f"raw_bid_types={buy_price_none_fields['raw_bid_types']} "
                f"raw_order_types={buy_price_none_fields['raw_order_types']} "
                f"raw_buy_supported_types={buy_price_none_fields['raw_buy_supported_types']} "
                f"support_source={buy_price_none_fields['support_source']} "
                f"resolved_contract={buy_price_none_fields['resolved_contract']} "
                f"contract_id={buy_price_none_fields['contract_id']} "
                f"resolved_order_type={buy_price_none_fields['resolved_order_type']} "
                f"submit_field={buy_price_none_fields['submit_field']} "
                f"allowed={buy_price_none_fields['allowed']} "
                f"decision_outcome={buy_price_none_fields['decision_outcome']} "
                f"decision_basis={buy_price_none_fields['decision_basis']} "
                f"alias_used={buy_price_none_fields['alias_used']} "
                f"alias_policy={buy_price_none_fields['alias_policy']} "
                f"block_reason={buy_price_none_fields['block_reason']}"
            ),
            critical=True,
        )
        contract_change = getattr(rr, "chance_contract_change", None)
        add_check(
            "chance contract drift canary",
            "FAIL" if getattr(contract_change, "detected", False) else "PASS",
            _format_chance_contract_change_detail(contract_change),
            critical=True,
        )
    except Exception as e:
        add_check(
            "symbol/order rule query",
            "WARN",
            f"lookup failed ({type(e).__name__}: {e})",
            critical=False,
        )
        add_check(
            "BUY price=None chance resolution",
            "WARN",
            f"resolution unavailable ({type(e).__name__}: {e})",
            critical=False,
        )
        add_check(
            "chance contract drift canary",
            "WARN",
            f"canary unavailable ({type(e).__name__}: {e})",
            critical=False,
        )

    notifier_enabled = os.getenv("NOTIFIER_ENABLED", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "y",
    }
    has_notifier_target = any(
        [
            os.getenv("NOTIFIER_WEBHOOK_URL", "").strip(),
            os.getenv("SLACK_WEBHOOK_URL", "").strip(),
            os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
            and os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        ]
    )
    if not notifier_enabled:
        add_check("notifier configured", "WARN", "NOTIFIER_ENABLED=false", critical=False)
    elif has_notifier_target:
        add_check("notifier configured", "PASS", "delivery target detected", critical=False)
    else:
        add_check("notifier configured", "WARN", "no webhook/chat target configured", critical=False)

    try:
        conn = ensure_db()
        try:
            conn.execute("SAVEPOINT live_readiness_probe")
            conn.execute(
                "INSERT OR REPLACE INTO daily_risk(day_kst, start_equity) VALUES ('__live_readiness_probe__', 0.0)"
            )
            conn.execute("ROLLBACK TO live_readiness_probe")
            conn.execute("RELEASE live_readiness_probe")
            add_check("DB writable", "PASS", f"path={settings.DB_PATH}", critical=True)
        finally:
            conn.close()
    except Exception as e:
        add_check("DB writable", "FAIL", f"db write probe failed ({type(e).__name__}: {e})", critical=True)

    account_diag_raw = getattr(broker, "get_accounts_validation_diagnostics", lambda: {})()
    if isinstance(account_diag_raw, dict):
        account_validation_reason = str(account_diag_raw.get("reason") or account_validation_reason)
        account_validation_last_failure_reason = str(account_diag_raw.get("last_failure_reason") or "none")
        row_count = int(account_diag_raw.get("row_count") or 0)
        currencies = ",".join(str(item) for item in list(account_diag_raw.get("currencies") or [])[:20]) or "-"
        missing = ",".join(str(item) for item in list(account_diag_raw.get("missing_required_currencies") or [])[:10]) or "-"
        duplicate = ",".join(str(item) for item in list(account_diag_raw.get("duplicate_currencies") or [])[:10]) or "-"
        add_check(
            "accounts snapshot(/v1/accounts) validation diagnostic",
            "PASS" if account_validation_reason == "ok" else "WARN",
            (
                f"reason={account_validation_reason} row_count={row_count} "
                f"currencies={currencies} missing_required_currencies={missing} duplicate_currencies={duplicate} "
                f"execution_mode={account_diag_raw.get('execution_mode') or '-'} "
                f"quote_currency={account_diag_raw.get('quote_currency') or '-'} "
                f"base_currency={account_diag_raw.get('base_currency') or '-'} "
                "base_currency_missing_policy="
                f"{account_diag_raw.get('base_currency_missing_policy') or '-'} "
                f"preflight_outcome={account_diag_raw.get('preflight_outcome') or '-'} "
                f"flat_start_allowed={account_diag_raw.get('flat_start_allowed')} "
                f"flat_start_reason={account_diag_raw.get('flat_start_reason') or '-'} "
                f"last_success={account_diag_raw.get('last_success_reason') or '-'} "
                f"last_failure={account_validation_last_failure_reason}"
            ),
            critical=False,
        )

    fail_count = sum(1 for check in checks if check["status"] == "FAIL")
    warn_count = sum(1 for check in checks if check["status"] == "WARN")
    pass_count = sum(1 for check in checks if check["status"] == "PASS")
    overall_status = "FAIL" if fail_count else ("WARN" if warn_count else "PASS")

    print("[BROKER-READINESS]")
    print(f"  market={MARKET}")
    if RAW_SYMBOL:
        print(f"  raw_symbol={RAW_SYMBOL}")
    print(f"  summary: pass={pass_count} warn={warn_count} fail={fail_count} overall={overall_status}")
    for check in checks:
        print(f"  - [{check['status']}] {check['name']}: {check['detail']}")

    if fail_count:
        raise SystemExit(1)


def _last_reconcile_failed(state) -> bool:
    status = str(getattr(state, "last_reconcile_status", "") or "").upper()
    return status in {"FAILED", "ERROR"}


def _safe_recent_broker_orders_snapshot(*, limit: int = 100) -> tuple[list[object], str | None]:
    if settings.MODE != "live":
        return [], "broker snapshot unavailable in non-live mode"
    conn = ensure_db()
    try:
        local_rows = conn.execute(
            """
            SELECT client_order_id, exchange_order_id
            FROM orders
            WHERE status IN ('PENDING_SUBMIT', 'NEW', 'PARTIAL', 'SUBMIT_UNKNOWN', 'ACCOUNTING_PENDING', 'RECOVERY_REQUIRED', 'CANCEL_REQUESTED')
            ORDER BY updated_ts DESC, created_ts DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    finally:
        conn.close()
    exchange_order_ids = sorted(
        {
            str(row["exchange_order_id"]).strip()
            for row in local_rows
            if str(row["exchange_order_id"] or "").strip()
        }
    )
    client_order_ids = sorted(
        {
            str(row["client_order_id"]).strip()
            for row in local_rows
            if str(row["client_order_id"] or "").strip()
        }
    )
    if not exchange_order_ids and not client_order_ids:
        try:
            from .broker.bithumb import BithumbBroker

            broker = BithumbBroker()
            get_recent_orders_for_recovery = getattr(broker, "get_recent_orders_for_recovery", None)
            if not callable(get_recent_orders_for_recovery):
                return [], "no local unresolved identifiers available for broker snapshot"
            return get_recent_orders_for_recovery(
                limit=limit,
                market=settings.PAIR,
            ), None
        except Exception as e:
            return [], f"failed to load recovery-scoped broker orders: {type(e).__name__}: {e}"
    try:
        from .broker.bithumb import BithumbBroker

        return BithumbBroker().get_recent_orders(
            limit=limit,
            exchange_order_ids=exchange_order_ids,
            client_order_ids=client_order_ids,
        ), None
    except Exception as e:
        return [], f"failed to load identifier-scoped broker orders: {type(e).__name__}: {e}"


def _build_recovery_candidates(*, local_order: dict[str, str | float], recent_orders: list[object]) -> list[dict[str, str | float | int]]:
    side = str(local_order["side"])
    requested_qty = float(local_order["qty_req"])
    lot_basis_qty = float(
        local_order.get("final_submitted_qty")
        if float(local_order.get("final_submitted_qty") or 0.0) > 0.0
        else local_order.get("final_intended_qty")
        if float(local_order.get("final_intended_qty") or 0.0) > 0.0
        else local_order["qty_req"]
    )
    local_price_raw = local_order.get("price")
    local_price = float(local_price_raw) if local_price_raw is not None else None
    created_ts = int(local_order["created_ts"])
    submit_ts = int(local_order.get("submit_evidence_attempted_ts") or created_ts)

    ranked: list[tuple[int, dict[str, str | float | int | str]]] = []
    for remote in recent_orders:
        exchange_order_id = str(getattr(remote, "exchange_order_id", "") or "")
        if not exchange_order_id:
            continue

        remote_side = str(getattr(remote, "side", "") or "")
        remote_qty_req = float(getattr(remote, "qty_req", 0.0) or 0.0)
        remote_price_raw = getattr(remote, "price", None)
        remote_price = float(remote_price_raw) if remote_price_raw is not None else None
        remote_status = str(getattr(remote, "status", "") or "-")
        remote_created_ts = int(getattr(remote, "created_ts", 0) or 0)
        remote_updated_ts = int(getattr(remote, "updated_ts", 0) or 0)

        score = 0
        reasons: list[str] = []
        if str(getattr(remote, "client_order_id", "") or "") == str(local_order["client_order_id"]):
            score += 4
            reasons.append("same client_order_id")
        if remote_side == side:
            score += 3
            reasons.append("same side")
        else:
            reasons.append("side mismatch")

        qty_gap = abs(remote_qty_req - lot_basis_qty)
        qty_gap_pct = (qty_gap / max(lot_basis_qty, 1e-12)) * 100.0
        qty_tolerance = max(1e-12, max(lot_basis_qty, remote_qty_req) * 0.03)
        if qty_gap <= max(1e-12, max(lot_basis_qty, remote_qty_req) * 0.01):
            score += 3
            reasons.append("very close qty")
        elif qty_gap <= qty_tolerance:
            score += 2
            reasons.append("close qty")

        ts_gap_sec = abs(remote_created_ts - submit_ts) / 1000 if remote_created_ts > 0 else float("inf")
        if ts_gap_sec <= 90:
            score += 3
            reasons.append("close submit timestamp")
        elif ts_gap_sec <= 300:
            score += 2
            reasons.append("near submit timestamp")
        elif ts_gap_sec <= 600:
            score += 1
            reasons.append("recent timestamp")

        price_gap_pct: float | None = None
        strong_price_match = False
        if local_price is not None and remote_price is not None and local_price > 0:
            price_gap_pct = abs(remote_price - local_price) / local_price * 100.0
            if price_gap_pct <= 0.2:
                score += 2
                strong_price_match = True
                reasons.append("very close price")
            elif price_gap_pct <= 1.0:
                score += 1
                reasons.append("close price")
            else:
                score -= 2
                reasons.append("price mismatch")

        if remote_status in {"PARTIAL", "FILLED", "NEW"}:
            score += 1
            reasons.append(f"status={remote_status}")

        if score < 4:
            continue

        side_match = remote_side == side
        qty_match = qty_gap <= qty_tolerance
        high_confidence = (
            score >= 8
            and side_match
            and qty_match
            and ts_gap_sec <= 300
            and (local_price is None or remote_price is None or strong_price_match)
        )
        likely_fill_match = float(getattr(remote, "qty_filled", 0.0) or 0.0) > 1e-12 or remote_status in {"PARTIAL", "FILLED"}

        ranked.append(
            (
                score,
                {
                    "exchange_order_id": exchange_order_id,
                    "side": remote_side or "-",
                    "qty": remote_qty_req,
                    "lot_basis_qty": lot_basis_qty,
                    "requested_qty": requested_qty,
                    "filled_qty": float(getattr(remote, "qty_filled", 0.0) or 0.0),
                    "status": remote_status,
                    "price": remote_price,
                    "created_ts": remote_created_ts,
                    "updated_ts": remote_updated_ts,
                    "score": score,
                    "qty_gap_pct": qty_gap_pct,
                    "time_gap_sec": ts_gap_sec,
                    "price_gap_pct": price_gap_pct,
                    "match_reason": " + ".join(reasons),
                    "high_confidence": 1 if high_confidence else 0,
                    "likely_fill_match": 1 if likely_fill_match else 0,
                },
            )
        )

    ranked.sort(key=lambda item: (-item[0], -int(item[1]["updated_ts"]), -int(item[1]["created_ts"])))
    return [item[1] for item in ranked]




def _load_submission_evidence(conn, *, client_order_id: str, submit_attempt_id: str | None) -> dict[str, str | int | bool | None]:
    event = None
    if submit_attempt_id:
        event = conn.execute(
            """
            SELECT submit_ts, event_ts, order_status, submission_reason_code,
                   timeout_flag, exchange_order_id_obtained, exception_class
            FROM order_events
            WHERE client_order_id=?
              AND submit_attempt_id=?
              AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """,
            (client_order_id, submit_attempt_id),
        ).fetchone()

    if event is None:
        event = conn.execute(
            """
            SELECT submit_ts, event_ts, order_status, submission_reason_code,
                   timeout_flag, exchange_order_id_obtained, exception_class
            FROM order_events
            WHERE client_order_id=?
              AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """,
            (client_order_id,),
        ).fetchone()

    intent = conn.execute(
        """
        SELECT intent_ts, event_ts
        FROM order_events
        WHERE client_order_id=?
          AND event_type='intent_created'
        ORDER BY id DESC
        LIMIT 1
        """,
        (client_order_id,),
    ).fetchone()

    attempted_locally = bool(event is not None or intent is not None)
    attempted_ts = None
    if event is not None:
        attempted_ts = int(event["submit_ts"] if event["submit_ts"] is not None else event["event_ts"])
    elif intent is not None:
        attempted_ts = int(intent["intent_ts"] if intent["intent_ts"] is not None else intent["event_ts"])

    request_likely_sent = "unknown"
    if event is not None:
        timeout_flag = bool(event["timeout_flag"]) if event["timeout_flag"] is not None else False
        exchange_id_obtained = bool(event["exchange_order_id_obtained"]) if event["exchange_order_id_obtained"] is not None else False
        if timeout_flag or exchange_id_obtained:
            request_likely_sent = "yes"
        elif str(event["order_status"] or "").strip() in {"NEW", "PARTIAL", "FILLED", "CANCELED", "FAILED", "SUBMIT_UNKNOWN"}:
            request_likely_sent = "yes"

    attempted_desc = "local intent evidence unavailable"
    if event is not None:
        attempted_desc = (
            f"submit_attempt_recorded status={event['order_status'] or '-'} "
            f"reason_code={event['submission_reason_code'] or '-'} "
            f"timeout={1 if bool(event['timeout_flag']) else 0} "
            f"exchange_order_id_obtained={1 if bool(event['exchange_order_id_obtained']) else 0}"
        )
    elif intent is not None:
        attempted_desc = "intent_created recorded but no submit_attempt_recorded evidence"

    return {
        "submit_attempt_id": submit_attempt_id,
        "attempted_locally": attempted_locally,
        "attempted_ts": attempted_ts,
        "attempted_desc": attempted_desc,
        "request_likely_sent": request_likely_sent,
    }
def _load_recovery_report(
    *,
    oldest_limit: int = 5,
) -> dict[str, int | float | str | bool | None | list[dict[str, str | float | bool]]]:
    maybe_clear_stale_initial_reconcile_halt()
    conn = conn = ensure_db()
    try:
        placeholders = ",".join("?" for _ in OPEN_ORDER_STATUSES)
        unresolved_row = conn.execute(
            f"""
            SELECT COUNT(*) AS unresolved_count, MIN(created_ts) AS oldest_created_ts
            FROM orders
            WHERE status IN ({placeholders})
            """,
            OPEN_ORDER_STATUSES,
        ).fetchone()
        recovery_required_row = conn.execute(
            "SELECT COUNT(*) AS recovery_required_count FROM orders WHERE status='RECOVERY_REQUIRED'"
        ).fetchone()
        submit_unknown_row = conn.execute(
            "SELECT COUNT(*) AS submit_unknown_count FROM orders WHERE status='SUBMIT_UNKNOWN'"
        ).fetchone()
        oldest_rows = conn.execute(
            f"""
            SELECT
                client_order_id,
                submit_attempt_id,
                status,
                exchange_order_id,
                side,
                price,
                qty_req,
                qty_filled,
                intended_lot_count,
                executable_lot_count,
                final_intended_qty,
                final_submitted_qty,
                created_ts,
                updated_ts,
                last_error
            FROM orders
            WHERE status IN ({placeholders})
            ORDER BY created_ts ASC
            LIMIT ?
            """,
            (*OPEN_ORDER_STATUSES, oldest_limit),
        ).fetchall()
        recovery_required_rows = conn.execute(
            """
            SELECT client_order_id, status, exchange_order_id, created_ts, last_error
            FROM orders
            WHERE status='RECOVERY_REQUIRED'
            ORDER BY created_ts ASC
            LIMIT ?
            """,
            (oldest_limit,),
        ).fetchall()
        health_row = conn.execute(
            """
            SELECT
                halt_reason_code,
                halt_state_unresolved,
                last_disable_reason,
                last_reconcile_epoch_sec,
                last_reconcile_status,
                last_reconcile_error,
                last_reconcile_reason_code,
                last_reconcile_metadata
            FROM bot_health
            WHERE id=1
            """
        ).fetchone()
        recent_external_cash_adjustment = get_external_cash_adjustment_summary(conn)
        recent_order_lifecycle = load_recent_order_lifecycle(conn, limit=oldest_limit)
        submission_evidence_by_order_id: dict[str, dict[str, str | int | bool | None]] = {}
        for row in oldest_rows:
            submit_attempt_id = str(row["submit_attempt_id"] or "").strip()
            submission_evidence_by_order_id[str(row["client_order_id"])] = _load_submission_evidence(
                conn,
                client_order_id=str(row["client_order_id"]),
                submit_attempt_id=(submit_attempt_id or None),
            )
    finally:
        conn.close()

    unresolved_count = int(unresolved_row["unresolved_count"] if unresolved_row else 0)
    recovery_required_count = int(recovery_required_row["recovery_required_count"] if recovery_required_row else 0)
    submit_unknown_count = int(submit_unknown_row["submit_unknown_count"] if submit_unknown_row else 0)

    oldest_created_ts = unresolved_row["oldest_created_ts"] if unresolved_row else None
    oldest_age_sec = None
    if unresolved_count > 0 and oldest_created_ts is not None:
        oldest_age_sec = max(0.0, (time.time() * 1000 - float(oldest_created_ts)) / 1000)

    now_ms = time.time() * 1000

    oldest_orders: list[dict[str, str | float]] = []
    candidate_local_orders: list[dict[str, str | float]] = []
    for row in oldest_rows:
        last_error = str(row["last_error"] or "").strip()
        evidence = submission_evidence_by_order_id.get(str(row["client_order_id"]), {})
        local_order = {
            "client_order_id": str(row["client_order_id"]),
            "status": str(row["status"]),
            "exchange_order_id": str(row["exchange_order_id"] or "-"),
            "side": str(row["side"] or "-"),
            "price": (float(row["price"]) if row["price"] is not None else None),
            "qty_req": float(row["qty_req"] or 0.0),
            "qty_filled": float(row["qty_filled"] or 0.0),
            "requested_lot_count": int(row["intended_lot_count"] or 0),
            "executable_lot_count": int(row["executable_lot_count"] or 0),
            "final_intended_qty": float(row["final_intended_qty"] or 0.0) if row["final_intended_qty"] is not None else 0.0,
            "final_submitted_qty": float(row["final_submitted_qty"] or 0.0) if row["final_submitted_qty"] is not None else 0.0,
            "created_ts": int(row["created_ts"]),
            "updated_ts": int(row["updated_ts"]),
            "age_sec": max(0.0, (now_ms - float(row["created_ts"])) / 1000),
            "last_error": (last_error[:60] + "...") if len(last_error) > 60 else (last_error or "-"),
            "submit_evidence_attempted_locally": bool(evidence["attempted_locally"]),
            "submit_evidence_attempted_ts": int(evidence["attempted_ts"] or row["created_ts"]),
            "submit_evidence_attempted_desc": str(evidence["attempted_desc"]),
            "submit_evidence_request_likely_sent": str(evidence["request_likely_sent"]),
        }
        oldest_orders.append(
            {
                "client_order_id": str(local_order["client_order_id"]),
                "status": str(local_order["status"]),
                "exchange_order_id": str(local_order["exchange_order_id"]),
                "age_sec": float(local_order["age_sec"]),
                "last_error": str(local_order["last_error"]),
            }
        )
        candidate_local_orders.append(local_order)

    recovery_required_orders: list[dict[str, str | float]] = []
    for row in recovery_required_rows:
        last_error = str(row["last_error"] or "").strip()
        recovery_required_orders.append(
            {
                "client_order_id": str(row["client_order_id"]),
                "status": str(row["status"]),
                "exchange_order_id": str(row["exchange_order_id"] or "-"),
                "age_sec": max(0.0, (now_ms - float(row["created_ts"])) / 1000),
                "last_error": (last_error[:60] + "...") if len(last_error) > 60 else (last_error or "-"),
            }
        )

    last_reconcile_summary = "none"
    if health_row and health_row["last_reconcile_status"]:
        pieces = [
            f"status={health_row['last_reconcile_status']}",
            f"reason_code={health_row['last_reconcile_reason_code'] or '-'}",
        ]
        if health_row["last_reconcile_epoch_sec"] is not None:
            pieces.append(f"epoch_sec={float(health_row['last_reconcile_epoch_sec']):.3f}")
            pieces.append(f"age_sec={max(0.0, time.time() - float(health_row['last_reconcile_epoch_sec'])):.1f}")
        if health_row["last_reconcile_error"]:
            pieces.append(f"error={health_row['last_reconcile_error']}")
        last_reconcile_summary = " ".join(pieces)

    recent_halt_reason = "none"
    if health_row and (health_row["halt_reason_code"] or health_row["last_disable_reason"]):
        recent_halt_reason = (
            f"code={health_row['halt_reason_code'] or '-'} "
            f"reason={health_row['last_disable_reason'] or '-'} "
            f"unresolved={1 if bool(health_row['halt_state_unresolved']) else 0}"
        )

    unprocessed_remote_open_orders = 0
    remote_known_unresolved_verification_summary = "none"
    balance_split_mismatch_summary = "none"
    dust_context = build_dust_display_context(health_row["last_reconcile_metadata"] if health_row else None)
    dust = dust_context.classification
    dust_view = dust_context.operator_view
    dust_fields = dust_context.fields
    if health_row and health_row["last_reconcile_metadata"]:
        try:
            reconcile_meta = json.loads(str(health_row["last_reconcile_metadata"]))
        except json.JSONDecodeError:
            reconcile_meta = {}
        raw_count = reconcile_meta.get("remote_open_order_found", 0)
        try:
            unprocessed_remote_open_orders = max(0, int(raw_count))
        except (TypeError, ValueError):
            unprocessed_remote_open_orders = 0
        raw_mismatch_summary = str(reconcile_meta.get("balance_split_mismatch_summary") or "").strip()
        if raw_mismatch_summary:
            balance_split_mismatch_summary = raw_mismatch_summary
        remote_known_unresolved_verification_summary = (
            "lookup_known_exchange_id={lookup_known_exchange_id} "
            "lookup_known_client_order_id={lookup_known_client_order_id} "
            "lookup_identifier_missing={lookup_identifier_missing} "
            "lookup_not_found={lookup_not_found} "
            "lookup_identifier_mismatch={lookup_identifier_mismatch} "
            "lookup_temporary_broker_error={lookup_temporary_broker_error} "
            "lookup_schema_mismatch={lookup_schema_mismatch}".format(
                lookup_known_exchange_id=int(reconcile_meta.get("lookup_known_exchange_id", 0) or 0),
                lookup_known_client_order_id=int(reconcile_meta.get("lookup_known_client_order_id", 0) or 0),
                lookup_identifier_missing=int(reconcile_meta.get("lookup_identifier_missing", 0) or 0),
                lookup_not_found=int(reconcile_meta.get("lookup_not_found", 0) or 0),
                lookup_identifier_mismatch=int(reconcile_meta.get("lookup_identifier_mismatch", 0) or 0),
                lookup_temporary_broker_error=int(reconcile_meta.get("lookup_temporary_broker_error", 0) or 0),
                lookup_schema_mismatch=int(reconcile_meta.get("lookup_schema_mismatch", 0) or 0),
            )
        )

    resume_allowed, blockers = evaluate_resume_eligibility()
    guidance = build_resume_guidance(
        resume_allowed=bool(resume_allowed),
        blockers=blockers,
        unresolved_count=unresolved_count,
        recovery_required_count=recovery_required_count,
        submit_unknown_count=submit_unknown_count,
    )
    blocker_list = guidance.blockers
    can_resume = bool(resume_allowed)
    blocker_codes = [str(b["code"]) for b in blocker_list]
    blocker_reason_codes = [str(b["reason_code"]) for b in blocker_list]
    non_overridable_blockers = guidance.non_overridable_blockers
    primary_blocker_code = guidance.primary_blocker_code
    primary_blocker_reason_code = guidance.primary_blocker_reason_code
    blocker_summary = guidance.blocker_summary
    operator_next_action = guidance.operator_next_action
    recommended_command = guidance.recommended_command
    recommended_next_action = guidance.recommended_next_action
    resume_blocked_reason = guidance.resume_blocked_reason
    active_blocker_summary = guidance.active_blocker_summary
    risk_level = guidance.risk_level

    state = runtime_state.snapshot()
    blocker_summary_view = guidance.blocker_summary_view

    recent_orders_snapshot, broker_snapshot_error = _safe_recent_broker_orders_snapshot(limit=100)
    recent_dust_unsellable_event = None
    external_cash_adjustment_summary = {
        "adjustment_count": 0,
        "adjustment_total": 0.0,
        "last_event_ts": None,
        "last_currency": None,
        "last_delta_amount": None,
        "last_source": None,
        "last_reason": None,
        "last_broker_snapshot_basis": None,
        "last_correlation_metadata": None,
        "last_note": None,
    }
    manual_flat_repair_summary = {
        "repair_count": 0,
        "asset_qty_total": 0.0,
        "cash_total": 0.0,
        "last_event_ts": None,
        "last_repair_key": None,
        "last_asset_qty_delta": None,
        "last_cash_delta": None,
        "last_source": None,
        "last_reason": None,
        "last_repair_basis": None,
        "last_note": None,
    }
    manual_flat_repair_preview = {
        "needs_repair": False,
        "safe_to_apply": False,
        "eligibility_reason": "not_checked",
        "recommended_command": "uv run python bot.py recovery-report",
    }
    external_position_adjustment_summary = {
        "adjustment_count": 0,
        "asset_qty_total": 0.0,
        "cash_total": 0.0,
        "last_event_ts": None,
        "last_adjustment_key": None,
        "last_asset_qty_delta": None,
        "last_cash_delta": None,
        "last_source": None,
        "last_reason": None,
        "last_adjustment_basis": None,
        "last_note": None,
    }
    external_position_repair_preview = {
        "needs_repair": False,
        "safe_to_apply": False,
        "eligibility_reason": "not_checked",
        "recommended_command": "uv run python bot.py recovery-report",
    }
    fee_gap_repair_summary = {
        "repair_count": 0,
        "last_event_ts": None,
        "last_repair_key": None,
        "last_source": None,
        "last_reason": None,
        "last_repair_basis": None,
        "last_note": None,
    }
    fee_pending_repair_summary = {
        "repair_count": 0,
        "last_event_ts": None,
        "last_repair_key": None,
        "last_client_order_id": None,
        "last_exchange_order_id": None,
        "last_fill_id": None,
        "last_fill_ts": None,
        "last_fee": None,
        "last_source": None,
        "last_reason": None,
        "last_repair_basis": None,
        "last_note": None,
    }
    position_authority_repair_summary = {
        "repair_count": 0,
        "last_event_ts": None,
        "last_repair_key": None,
        "last_source": None,
        "last_reason": None,
        "last_repair_basis": None,
        "last_note": None,
    }
    position_authority_rebuild_preview = {
        "needs_rebuild": False,
        "safe_to_apply": False,
        "eligibility_reason": "not_checked",
        "recommended_command": "uv run python bot.py recovery-report",
    }
    runtime_readiness_snapshot = {
        "recovery_stage": "UNKNOWN",
        "resume_ready": False,
        "resume_blockers": ["NOT_CHECKED"],
        "blocker_categories": ["unknown"],
        "operator_next_action": "review_recovery_report",
        "recommended_command": "uv run python bot.py recovery-report",
    }
    fee_gap_repair_preview = {
        "needs_repair": False,
        "safe_to_apply": False,
        "eligibility_reason": "not_checked",
        "recommended_command": "uv run python bot.py recovery-report",
    }
    broker_fill_observation_summary = {
        "observation_count": 0,
        "fee_pending_count": 0,
        "accounting_complete_count": 0,
        "fee_candidate_order_level_count": 0,
        "expected_fee_rate_mismatch_count": 0,
        "missing_fee_count": 0,
        "zero_reported_fee_count": 0,
        "invalid_fee_count": 0,
        "last_event_ts": None,
        "last_client_order_id": None,
        "last_exchange_order_id": None,
        "last_fill_id": None,
        "last_fee_status": None,
        "last_accounting_status": None,
        "last_source": None,
    }
    fill_accounting_incident_projection = {
        "projection_kind": "fill_accounting_incident_projection",
        "incident_count": 0,
        "active_fee_pending_count": 0,
        "unapplied_principal_pending_count": 0,
        "principal_applied_fee_pending_count": 0,
        "fee_validation_blocked_count": 0,
        "fee_finalized_count": 0,
        "active_issue_count": 0,
        "already_accounted_observation_stale_count": 0,
        "repaired_count": 0,
        "latest_accounting_complete_count": 0,
        "verdicts": [],
    }
    fee_rate_drift_diagnostics = {
        "configured_fee_rate_estimate": float(settings.LIVE_FEE_RATE_ESTIMATE),
        "configured_fee_bps": float(settings.LIVE_FEE_RATE_ESTIMATE) * 10000.0,
        "observed_fee_bps_median": None,
        "observed_material_fee_sample_count": 0,
        "observation_window_count": 0,
        "configured_minus_observed_bps": None,
        "recent_expected_fee_rate_mismatch_count": 0,
        "recent_fee_pending_observation_count": 0,
        "fee_pending_accounting_repair_count": 0,
        "material_notional_threshold_krw": float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW),
    }
    accounting_projection = {
        "consistent": False,
        "replay_qty": 0.0,
        "replay_cash": 0.0,
        "portfolio_cash": 0.0,
        "portfolio_qty": 0.0,
    }
    conn = ensure_db()
    try:
        recent_dust_unsellable_event = conn.execute(
            """
            SELECT
                oe.event_ts,
                oe.client_order_id,
                oe.qty,
                oe.price,
                oe.submission_reason_code,
                oe.message
            FROM order_events oe
            JOIN orders o ON o.client_order_id = oe.client_order_id
            WHERE oe.event_type='submit_attempt_recorded'
              AND oe.submission_reason_code=?
              AND o.side='SELL'
            ORDER BY oe.event_ts DESC, oe.id DESC
            LIMIT 1
            """,
            (DUST_RESIDUAL_UNSELLABLE,),
        ).fetchone()
        external_cash_adjustment_summary = get_external_cash_adjustment_summary(conn)
        external_position_adjustment_summary = get_external_position_adjustment_summary(conn)
        manual_flat_repair_summary = get_manual_flat_accounting_repair_summary(conn)
        manual_flat_repair_preview = build_manual_flat_accounting_repair_preview(conn)
        external_position_repair_preview = build_external_position_accounting_repair_preview(conn)
        fee_gap_repair_summary = get_fee_gap_accounting_repair_summary(conn)
        fee_gap_repair_preview = build_fee_gap_accounting_repair_preview(conn)
        fee_pending_repair_summary = get_fee_pending_accounting_repair_summary(conn)
        position_authority_repair_summary = get_position_authority_repair_summary(conn)
        position_authority_rebuild_preview = build_position_authority_rebuild_preview(conn)
        runtime_readiness_snapshot = compute_runtime_readiness_snapshot(conn).as_dict()
        broker_fill_observation_summary = get_broker_fill_observation_summary(conn)
        fill_accounting_incident_projection = summarize_fill_accounting_incident_projection(conn)
        fee_rate_drift_diagnostics = _fee_rate_drift_diagnostics(conn)
        try:
            accounting_projection = compute_accounting_replay(conn)
            cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
            portfolio_cash = portfolio_cash_total(
                cash_available=float(cash_available),
                cash_locked=float(cash_locked),
            )
            portfolio_qty_local = portfolio_asset_total(
                asset_available=float(asset_available),
                asset_locked=float(asset_locked),
            )
            accounting_projection = {
                **accounting_projection,
                "portfolio_cash": portfolio_cash,
                "portfolio_qty": portfolio_qty_local,
                "consistent": bool(
                    abs(float(accounting_projection.get("replay_cash") or 0.0) - portfolio_cash) <= 1e-8
                    and abs(float(accounting_projection.get("replay_qty") or 0.0) - portfolio_qty_local) <= 1e-12
                ),
            }
        except RuntimeError as exc:
            cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
            accounting_projection = {
                "consistent": False,
                "replay_cash": 0.0,
                "replay_qty": 0.0,
                "portfolio_cash": portfolio_cash_total(
                    cash_available=float(cash_available),
                    cash_locked=float(cash_locked),
                ),
                "portfolio_qty": portfolio_asset_total(
                    asset_available=float(asset_available),
                    asset_locked=float(asset_locked),
                ),
                "error": str(exc),
            }
    finally:
        conn.close()
    candidate_report: list[dict[str, object]] = []
    for local_order in candidate_local_orders:
        candidates = _build_recovery_candidates(local_order=local_order, recent_orders=recent_orders_snapshot)
        plausible_candidates = [c for c in candidates if int(c.get("high_confidence") or 0) == 1]
        likely_candidate = plausible_candidates[0] if len(plausible_candidates) == 1 else None
        local_qty_basis = float(
            local_order["final_submitted_qty"]
            if float(local_order["final_submitted_qty"]) > 0.0
            else local_order["final_intended_qty"]
            if float(local_order["final_intended_qty"]) > 0.0
            else local_order["qty_req"]
        )
        local_qty_source = (
            "final_submitted_qty"
            if float(local_order["final_submitted_qty"]) > 0.0
            else "final_intended_qty"
            if float(local_order["final_intended_qty"]) > 0.0
            else "qty_req"
        )
        if not candidates:
            outcome = "no_candidate"
            next_action = "No likely broker match found. Keep order unresolved, run reconcile, and verify exchange history manually before recover-order."
        elif len(plausible_candidates) > 1:
            outcome = "multiple_plausible_candidates"
            next_action = "Multiple high-confidence broker matches detected. Keep unresolved and perform manual broker/order-event review before recover-order."
        elif len(plausible_candidates) == 1:
            outcome = "single_plausible_candidate"
            next_action = "Exactly one high-confidence broker match found. Operator should validate and run recover-order with the suggested exchange_order_id."
        else:
            outcome = "weak_candidates_only"
            next_action = "Only weak broker candidates found. Keep unresolved and verify order/fill history before manual recover-order."

        candidate_report.append(
            {
                "client_order_id": local_order["client_order_id"],
                "local_status": local_order["status"],
                "local_side": local_order["side"],
                "local_qty": local_qty_basis,
                "local_qty_source": local_qty_source,
                "requested_qty": local_order["qty_req"],
                "requested_lot_count": local_order["requested_lot_count"],
                "executable_lot_count": local_order["executable_lot_count"],
                "final_intended_qty": local_order["final_intended_qty"],
                "final_submitted_qty": local_order["final_submitted_qty"],
                "local_created_ts": local_order["created_ts"],
                "attempted_locally": bool(local_order["submit_evidence_attempted_locally"]),
                "attempted_ts": int(local_order["submit_evidence_attempted_ts"]),
                "attempted_summary": str(local_order["submit_evidence_attempted_desc"]),
                "request_likely_sent": str(local_order["submit_evidence_request_likely_sent"]),
                "candidate_outcome": outcome,
                "plausible_candidate_count": len(plausible_candidates),
                "likely_broker_match": bool(likely_candidate is not None),
                "likely_broker_exchange_order_id": (
                    str(likely_candidate["exchange_order_id"]) if likely_candidate is not None else None
                ),
                "likely_broker_match_kind": (
                    "order_or_fill" if likely_candidate is not None and int(likely_candidate.get("likely_fill_match") or 0) == 1
                    else ("order" if likely_candidate is not None else "none")
                ),
                "candidates": candidates[:5],
                "next_action_hint": next_action,
            }
        )

    runtime_stage = str(runtime_readiness_snapshot.get("recovery_stage") or "")
    readiness_has_deferred_debt = runtime_stage == "RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT"
    report_operator_next_action = (
        str(runtime_readiness_snapshot.get("operator_next_action") or operator_next_action)
        if bool(resume_allowed) and readiness_has_deferred_debt
        else operator_next_action
    )
    report_recommended_next_action = (
        "Resume may manage the open executable position; repair deferred historical fee-gap debt after flatten/closeout."
        if bool(resume_allowed) and readiness_has_deferred_debt
        else recommended_next_action
    )
    lot_projection = runtime_readiness_snapshot.get("projection_convergence") or {}
    broker_qty = float(position_authority_rebuild_preview.get("broker_qty") or 0.0)
    broker_qty_known = bool(position_authority_rebuild_preview.get("broker_qty_known"))
    portfolio_qty = float(lot_projection.get("portfolio_qty") or 0.0)
    broker_portfolio_converged = bool(
        broker_qty_known and abs(normalize_asset_qty(broker_qty) - normalize_asset_qty(portfolio_qty)) <= 1e-12
    )
    lot_projection_converged = bool(lot_projection.get("converged"))
    fill_root_cause = "none"
    fill_root_summary = {
        "root": "none",
        "principal_applied": 0,
        "broker_local_asset_converged": 1 if broker_portfolio_converged else 0,
        "fee_status": broker_fill_observation_summary.get("last_fee_status") or "none",
        "fee_source": broker_fill_observation_summary.get("last_fee_source") or "none",
        "fee_confidence": broker_fill_observation_summary.get("last_fee_confidence") or "none",
        "fee_provenance": broker_fill_observation_summary.get("last_fee_provenance") or "none",
        "latest_fee_validation_reason": broker_fill_observation_summary.get("last_fee_validation_reason") or "none",
        "latest_fee_validation_checks": broker_fill_observation_summary.get("last_fee_validation_checks") or "none",
        "accounting_status_counts": {
            "unapplied_principal_pending": int(
                fill_accounting_incident_projection.get("unapplied_principal_pending_count") or 0
            ),
            "principal_applied_fee_pending": int(
                fill_accounting_incident_projection.get("principal_applied_fee_pending_count") or 0
            ),
            "fee_validation_blocked": int(
                fill_accounting_incident_projection.get("fee_validation_blocked_count") or 0
            ),
            "fee_finalized": int(fill_accounting_incident_projection.get("fee_finalized_count") or 0),
        },
        "operator_action": "none",
        "recommended_action": "none",
        "flatten_as_primary_response": False,
        "derived_blockers": [],
        "root_chain": [],
    }
    if int(fill_accounting_incident_projection.get("unapplied_principal_pending_count") or 0) > 0:
        derived_blockers: list[str] = []
        if not broker_portfolio_converged:
            derived_blockers.append("broker_local_asset_mismatch")
        if not lot_projection_converged:
            derived_blockers.append("position_projection_drift")
        if not bool(runtime_readiness_snapshot.get("resume_ready")):
            derived_blockers.append("resume_blocked")
        fill_root_cause = "unapplied_principal_fill"
        fill_root_summary = {
            "root": fill_root_cause,
            "principal_applied": 0,
            "broker_local_asset_converged": 1 if broker_portfolio_converged else 0,
            "fee_status": broker_fill_observation_summary.get("last_fee_status") or "unknown",
            "fee_source": broker_fill_observation_summary.get("last_fee_source") or "unknown",
            "fee_confidence": broker_fill_observation_summary.get("last_fee_confidence") or "unknown",
            "fee_provenance": broker_fill_observation_summary.get("last_fee_provenance") or "unknown",
            "latest_fee_validation_reason": broker_fill_observation_summary.get("last_fee_validation_reason") or "unknown",
            "latest_fee_validation_checks": broker_fill_observation_summary.get("last_fee_validation_checks") or "none",
            "accounting_status_counts": fill_root_summary["accounting_status_counts"],
            "operator_action": "wait_for_auto_reconcile_or_review_fee_evidence",
            "recommended_action": "wait_for_auto_reconcile_or_forensic_fee_authority_diagnosis",
            "flatten_as_primary_response": False,
            "derived_blockers": derived_blockers,
            "root_chain": [fill_root_cause, *derived_blockers],
        }
    elif int(fill_accounting_incident_projection.get("fee_validation_blocked_count") or 0) > 0:
        derived_blockers = ["resume_blocked"]
        fill_root_cause = "fee_validation_blocked"
        fill_root_summary = {
            "root": fill_root_cause,
            "principal_applied": 1,
            "broker_local_asset_converged": 1 if broker_portfolio_converged else 0,
            "fee_status": broker_fill_observation_summary.get("last_fee_status") or "unknown",
            "fee_source": broker_fill_observation_summary.get("last_fee_source") or "unknown",
            "fee_confidence": broker_fill_observation_summary.get("last_fee_confidence") or "unknown",
            "fee_provenance": broker_fill_observation_summary.get("last_fee_provenance") or "unknown",
            "latest_fee_validation_reason": broker_fill_observation_summary.get("last_fee_validation_reason") or "unknown",
            "latest_fee_validation_checks": broker_fill_observation_summary.get("last_fee_validation_checks") or "none",
            "accounting_status_counts": fill_root_summary["accounting_status_counts"],
            "operator_action": "review_fee_evidence",
            "recommended_action": "forensic_fee_authority_diagnosis",
            "flatten_as_primary_response": False,
            "derived_blockers": derived_blockers,
            "root_chain": [fill_root_cause, *derived_blockers],
        }
    elif int(fill_accounting_incident_projection.get("principal_applied_fee_pending_count") or 0) > 0:
        fill_root_cause = "fee_finalization_pending"
        fill_root_summary = {
            "root": fill_root_cause,
            "principal_applied": 1,
            "broker_local_asset_converged": 1 if broker_portfolio_converged else 0,
            "fee_status": broker_fill_observation_summary.get("last_fee_status") or "unknown",
            "fee_source": broker_fill_observation_summary.get("last_fee_source") or "unknown",
            "fee_confidence": broker_fill_observation_summary.get("last_fee_confidence") or "unknown",
            "fee_provenance": broker_fill_observation_summary.get("last_fee_provenance") or "unknown",
            "latest_fee_validation_reason": broker_fill_observation_summary.get("last_fee_validation_reason") or "unknown",
            "latest_fee_validation_checks": broker_fill_observation_summary.get("last_fee_validation_checks") or "none",
            "accounting_status_counts": fill_root_summary["accounting_status_counts"],
            "operator_action": "none_or_review_fee_evidence",
            "recommended_action": "forensic_fee_authority_diagnosis",
            "flatten_as_primary_response": False,
            "derived_blockers": [],
            "root_chain": [fill_root_cause],
        }
    blocking_incident_class = (
        str((runtime_readiness_snapshot.get("position_authority_assessment") or {}).get("incident_class") or "NONE")
    )
    report_recommended_command = (
        str(runtime_readiness_snapshot.get("recommended_command") or recommended_command)
        if (
            bool(resume_allowed) and readiness_has_deferred_debt
        ) or blocking_incident_class == "HISTORICAL_FRAGMENTATION_PROJECTION_DRIFT"
        else recommended_command
    )

    report = {
        "mode": settings.MODE,
        "unresolved_count": unresolved_count,
        "recovery_required_count": recovery_required_count,
        "submit_unknown_count": submit_unknown_count,
        "oldest_unresolved_age_sec": oldest_age_sec,
        "oldest_orders": oldest_orders,
        "last_reconcile_summary": last_reconcile_summary,
        "recent_halt_reason": recent_halt_reason,
        "unprocessed_remote_open_orders": unprocessed_remote_open_orders,
        "remote_known_unresolved_verification_summary": remote_known_unresolved_verification_summary,
        "balance_split_mismatch_summary": balance_split_mismatch_summary,
        **{key: value for key, value in dust_fields.items() if key != "dust_threshold_basis"},
        **{
            key: value
            for key, value in runtime_readiness_snapshot.items()
            if key
            in {
                "dust_display_scope",
                "broker_dust_signal_state",
                "broker_dust_signal_message",
                "dust_tradeability_consistent",
                "dust_operator_message",
                "residue_policy_scope",
                "residue_policy_state",
                "residue_policy_message",
                "residue_blocks_new_entry",
                "residue_blocks_closeout",
                "strategy_tradeability_state",
                "entry_policy_state",
                "closeout_policy_state",
                "tradeability_operator_message",
            }
        },
        "recent_dust_unsellable_event": (
            {
                "event_ts": int(recent_dust_unsellable_event["event_ts"]),
                "client_order_id": str(recent_dust_unsellable_event["client_order_id"]),
                "qty": float(recent_dust_unsellable_event["qty"] or 0.0),
                "price": (
                    float(recent_dust_unsellable_event["price"])
                    if recent_dust_unsellable_event["price"] is not None
                    else None
                ),
                "reason_code": str(recent_dust_unsellable_event["submission_reason_code"] or "-"),
                "summary": str(recent_dust_unsellable_event["message"] or "-"),
            }
            if recent_dust_unsellable_event is not None
            else None
        ),
        "recent_external_cash_adjustment": external_cash_adjustment_summary,
        "external_position_accounting_repair_preview": external_position_repair_preview,
        "external_position_adjustment_summary": external_position_adjustment_summary,
        "manual_flat_accounting_repair_preview": manual_flat_repair_preview,
        "manual_flat_accounting_repair_summary": manual_flat_repair_summary,
        "fee_gap_accounting_repair_preview": fee_gap_repair_preview,
        "fee_gap_accounting_repair_summary": fee_gap_repair_summary,
        "fee_pending_accounting_repair_summary": fee_pending_repair_summary,
        "position_authority_rebuild_preview": position_authority_rebuild_preview,
        "position_authority_repair_summary": position_authority_repair_summary,
        "runtime_readiness": runtime_readiness_snapshot,
        "pending_fee_count": int(fill_accounting_incident_projection.get("active_fee_pending_count") or 0),
        "auto_recovery_count": int(runtime_readiness_snapshot.get("auto_recovery_count") or 0),
        "operator_review_required_count": recovery_required_count,
        "accounting_projection_ok": bool(accounting_projection.get("consistent")),
        "broker_portfolio_converged": broker_portfolio_converged,
        "broker_qty_known": broker_qty_known,
        "broker_qty": broker_qty,
        "portfolio_qty": portfolio_qty,
        "lot_projection_converged": lot_projection_converged,
        "live_ready": bool(runtime_readiness_snapshot.get("resume_ready")),
        "blocking_incident_class": blocking_incident_class,
        "recovery_stage": runtime_readiness_snapshot.get("recovery_stage"),
        "recovery_blocker_categories": runtime_readiness_snapshot.get("blocker_categories"),
        "broker_fill_observation_summary": broker_fill_observation_summary,
        "fee_rate_drift_diagnostics": fee_rate_drift_diagnostics,
        "fill_accounting_incident_projection": fill_accounting_incident_projection,
        "fill_accounting_root_cause": fill_root_summary,
        "trading_enabled": bool(state.trading_enabled),
        "trading_state": (
            "blocked"
            if blocker_list
            else ("paused" if not bool(state.trading_enabled) else "running")
        ),
        "trading_blocked": bool(blocker_list),
        "hard_halt_reason": recent_halt_reason if bool(state.halt_new_orders_blocked or state.halt_state_unresolved) else "none",
        "emergency_flatten_blocked": bool(state.emergency_flatten_blocked),
        "emergency_flatten_block_reason": state.emergency_flatten_block_reason,
        "resume_allowed": bool(resume_allowed),
        "can_resume": can_resume,
        "resume_blockers": blocker_codes,
        "resume_blocker_reason_codes": blocker_reason_codes,
        "force_resume_allowed": all(bool(b.overridable) for b in blockers),
        "blockers": blocker_list,
        "blocker_summary": blocker_summary,
        "active_blocker_summary": active_blocker_summary,
        "blocker_summary_view": blocker_summary_view,
        "risk_level": risk_level,
        "primary_blocker_code": primary_blocker_code,
        "primary_blocker_reason_code": primary_blocker_reason_code,
        "non_overridable_blockers": non_overridable_blockers,
        "unresolved_summary": oldest_orders,
        "recovery_required_summary": recovery_required_orders,
        "operator_next_action": report_operator_next_action,
        "recommended_next_action": report_recommended_next_action,
        "resume_blocked_reason": resume_blocked_reason,
        "recommended_command": report_recommended_command,
        "recent_order_lifecycle": recent_order_lifecycle,
        "recovery_candidates": candidate_report,
        "broker_recent_orders_snapshot_error": broker_snapshot_error,
        "recent_external_cash_adjustment": recent_external_cash_adjustment,
    }
    report["recovery_policy"] = build_recovery_policy_from_report(report)
    return report


def cmd_recovery_report(*, as_json: bool = False) -> None:
    report = _load_recovery_report()
    write_json_atomic(PATH_MANAGER.recovery_report_path(), report)
    if as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return
    dust_context = build_dust_display_context(report)
    conn = ensure_db()
    try:
        init_portfolio(conn)
        _cash_available, _cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
        portfolio_asset_qty = portfolio_asset_total(
            asset_available=float(asset_available),
            asset_locked=float(asset_locked),
        )
        snapshot = build_canonical_position_snapshot(
            conn,
            metadata_raw=report,
            pair=settings.PAIR,
            portfolio_asset_qty=portfolio_asset_qty,
        )
    finally:
        conn.close()
    position_state = snapshot.position_state
    lot_exposure = position_state.normalized_exposure
    dust_tracking_lot_count = int(lot_exposure.dust_tracking_lot_count)
    if (
        dust_tracking_lot_count <= 0
        and float(lot_exposure.dust_tracking_qty) > 0.0
        and float(lot_exposure.open_exposure_qty) <= 0.0
    ):
        dust_tracking_lot_count = 1

    print("[RECOVERY-REPORT]")
    _print_operator_command_contract(
        "RECOVERY-REPORT",
        precondition=(
            "DB snapshot readable; recovery summary can be derived from current runtime state; "
            "report output is snapshot-only and does not mutate trading state"
        ),
        postcondition=f"recovery_report snapshot written to {PATH_MANAGER.recovery_report_path()}",
    )
    print("  [P0] blocker_summary_view")
    for item in report.get("blocker_summary_view") or []:
        print(
            "    - "
            f"blocker={item['blocker']} "
            f"evidence={item['evidence']} "
            f"recommended_next_action={item['recommended_next_action']}"
        )
        if item.get("delta_krw") is not None or item.get("recent_external_cash_adjustment_present") is not None:
            print(
                "      "
                f"delta_krw={item.get('delta_krw') if item.get('delta_krw') is not None else 'none'} "
                f"recent_external_cash_adjustment_present={1 if bool(item.get('recent_external_cash_adjustment_present')) else 0} "
                f"recent_external_cash_adjustment_count={int(item.get('recent_external_cash_adjustment_count') or 0)}"
            )
    print("  [P1] order_recovery_status")
    print(f"    unresolved_count={report['unresolved_count']}")
    print(f"    recovery_required_count={report['recovery_required_count']}")
    print(f"    submit_unknown_count={report['submit_unknown_count']}")
    print("  [RUN-LOCK]")
    run_lock = report.get("run_lock") or {}
    print(f"    {run_lock.get('human_text') or '-'}")
    print("  [P2] resume_eligibility")
    runtime_readiness = report.get("runtime_readiness") or {}
    print(f"    recovery_stage={runtime_readiness.get('recovery_stage') or 'UNKNOWN'}")
    print(f"    accounting_projection_ok={1 if bool(report.get('accounting_projection_ok')) else 0}")
    print(f"    broker_portfolio_converged={1 if bool(report.get('broker_portfolio_converged')) else 0}")
    print(f"    lot_projection_converged={1 if bool(report.get('lot_projection_converged')) else 0}")
    print(f"    live_ready={1 if bool(report.get('live_ready')) else 0}")
    print(f"    blocking_incident_class={report.get('blocking_incident_class') or 'NONE'}")
    print(
        "    recovery_blocker_categories="
        f"{', '.join(str(x) for x in runtime_readiness.get('blocker_categories') or []) or 'none'}"
    )
    print(f"    canonical_next_action={runtime_readiness.get('operator_next_action') or 'review_recovery_report'}")
    print(f"    resume_allowed={1 if bool(report['resume_allowed']) else 0}")
    print(f"    can_resume={'true' if bool(report['can_resume']) else 'false'}")
    resume_blockers = report.get("resume_blockers") or []
    print(f"    blockers={', '.join(str(b) for b in resume_blockers) if resume_blockers else 'none'}")
    resume_blocker_reason_codes = report.get("resume_blocker_reason_codes") or []
    print(
        "    blocker_reason_codes="
        f"{', '.join(str(b) for b in resume_blocker_reason_codes) if resume_blocker_reason_codes else 'none'}"
    )
    print(f"    force_resume_allowed={1 if bool(report['force_resume_allowed']) else 0}")
    blockers = report.get("blockers") or []
    print(f"    blocker_summary={report['blocker_summary']}")
    print(f"    active_blocker_summary={report['active_blocker_summary']}")
    print(f"    risk_level={report['risk_level']}")
    print(f"    primary_blocker_code={report['primary_blocker_code']}")
    print(f"    primary_blocker_reason_code={report['primary_blocker_reason_code']}")
    print(f"    emergency_flatten_blocked={1 if bool(report.get('emergency_flatten_blocked')) else 0}")
    print(f"    emergency_flatten_block_reason={report.get('emergency_flatten_block_reason') or 'none'}")
    for blocker in blockers:
        print(
            "    - "
            f"code={blocker['code']} "
            f"reason_code={blocker['reason_code']} "
            f"summary={blocker['summary']} "
            f"overridable={1 if bool(blocker['overridable']) else 0} "
            f"detail={blocker['detail']}"
        )
    print("  [P3] balance_mismatch")
    print(f"    summary={report['balance_split_mismatch_summary']}")
    print("  [P3.0] dust_residual")
    print(f"    present={1 if bool(report.get('dust_residual_present')) else 0}")
    print(f"    allow_resume={1 if bool(report.get('dust_residual_allow_resume')) else 0}")
    print(f"    policy_reason={report.get('dust_policy_reason') or 'none'}")
    print(f"    state={report.get('dust_state') or 'none'}")
    print(f"    display_scope={report.get('dust_display_scope') or 'broker_reconcile_signal'}")
    print(f"    residue_policy_state={report.get('residue_policy_state') or 'unknown'}")
    print(f"    strategy_tradeability_state={report.get('strategy_tradeability_state') or 'unknown'}")
    print(f"    dust_tradeability_consistent={1 if bool(report.get('dust_tradeability_consistent', True)) else 0}")
    print(f"    state_label={report.get('dust_state_label') or 'none'}")
    print(f"    operator_action={report.get('dust_operator_action') or 'none'}")
    print(f"    operator_message={report.get('dust_operator_message') or 'none'}")
    print(f"    residue_policy_message={report.get('residue_policy_message') or 'none'}")
    print(
        "    "
        f"observed_broker_qty={float(report.get('dust_broker_qty') or 0.0):.8f} "
        f"observed_local_qty={float(report.get('dust_local_qty') or 0.0):.8f} "
        f"delta_qty={float(report.get('dust_delta_qty') or 0.0):.8f} "
        f"min_qty={float(report.get('dust_min_qty') or 0.0):.8f} "
        f"min_notional_krw={float(report.get('dust_min_notional_krw') or 0.0):.1f}"
    )
    print(
        "    "
        "qty_below_min="
        f"{dust_context.qty_below_min_summary} "
        "notional_below_min="
        f"{dust_context.notional_below_min_summary}"
    )
    print(f"    broker_local_match={1 if bool(report.get('dust_broker_local_match')) else 0}")
    print(f"    new_orders_allowed={1 if bool(report.get('dust_new_orders_allowed')) else 0}")
    print(f"    resume_allowed_by_policy={1 if bool(report.get('dust_resume_allowed_by_policy')) else 0}")
    print(f"    treat_as_flat={1 if bool(report.get('dust_treat_as_flat')) else 0}")
    print(f"    dust_effective_flat={1 if bool(report.get('dust_effective_flat')) else 0}")
    print(f"    entry_allowed={1 if dust_context.effective_flat_due_to_harmless_dust else 0}")
    print(
        "    "
        f"effective_flat_due_to_harmless_dust={1 if dust_context.effective_flat_due_to_harmless_dust else 0}"
    )
    print("  [P3.1] lot_exposure")
    print(
        "    "
        f"raw_total_asset_qty={float(lot_exposure.raw_total_asset_qty):.8f} "
        f"open_exposure_qty={float(lot_exposure.open_exposure_qty):.8f} "
        f"dust_tracking_qty={float(lot_exposure.dust_tracking_qty):.8f}"
    )
    print(
        "    "
        f"open_lot_count={int(lot_exposure.open_lot_count)} "
        f"dust_tracking_lot_count={dust_tracking_lot_count} "
        f"reserved_exit_lot_count={int(lot_exposure.reserved_exit_lot_count)} "
        f"sellable_executable_lot_count={int(lot_exposure.sellable_executable_lot_count)} "
        f"sellable_executable_qty={float(lot_exposure.sellable_executable_qty):.8f} "
        f"terminal_state={lot_exposure.terminal_state or 'none'} "
        f"exit_block_reason={lot_exposure.exit_block_reason or 'none'}"
    )
    print(f"    summary={_clarify_dust_observational_summary(report.get('dust_residual_summary'))}")
    recent_dust_unsellable_event = report.get("recent_dust_unsellable_event")
    print("  [P3.0a] recent_dust_unsellable_event")
    if recent_dust_unsellable_event:
        print(
            "    "
            f"reason_code={recent_dust_unsellable_event.get('reason_code') or '-'} "
            f"client_order_id={recent_dust_unsellable_event.get('client_order_id') or '-'} "
            f"qty={float(recent_dust_unsellable_event.get('qty') or 0.0):.8f} "
            f"price={recent_dust_unsellable_event.get('price') if recent_dust_unsellable_event.get('price') is not None else '-'}"
        )
        print(f"    summary={recent_dust_unsellable_event.get('summary') or 'none'}")
    else:
        print("    none")
    recent_external_cash_adjustment = report.get("recent_external_cash_adjustment") or {}
    print(
        "  [P3.0b] recent_external_cash_adjustment="
        f"{_format_external_cash_adjustment_summary(recent_external_cash_adjustment)}"
    )
    manual_flat_repair_preview = report.get("manual_flat_accounting_repair_preview") or {}
    manual_flat_repair_summary = report.get("manual_flat_accounting_repair_summary") or {}
    external_position_repair_preview = report.get("external_position_accounting_repair_preview") or {}
    external_position_adjustment_summary = report.get("external_position_adjustment_summary") or {}
    print("  [P3.0b] external_position_accounting_repair")
    print(
        "    "
        f"needs_repair={1 if bool(external_position_repair_preview.get('needs_repair')) else 0} "
        f"safe_to_apply={1 if bool(external_position_repair_preview.get('safe_to_apply')) else 0} "
        f"reason={external_position_repair_preview.get('eligibility_reason', 'none')}"
    )
    print(
        "    "
        f"repair_count={int(external_position_adjustment_summary.get('adjustment_count') or 0)} "
        f"asset_qty_total={float(external_position_adjustment_summary.get('asset_qty_total') or 0.0):.10f} "
        f"cash_total={float(external_position_adjustment_summary.get('cash_total') or 0.0):,.3f}"
    )
    print("  [P3.0c] manual_flat_accounting_repair")
    print(
        "    "
        f"needed={1 if bool(manual_flat_repair_preview.get('needs_repair')) else 0} "
        f"safe_to_apply={1 if bool(manual_flat_repair_preview.get('safe_to_apply')) else 0} "
        f"repair_count={int(manual_flat_repair_summary.get('repair_count') or 0)} "
        f"reason={manual_flat_repair_preview.get('eligibility_reason') or 'none'}"
    )
    print(f"    command={manual_flat_repair_preview.get('recommended_command') or 'none'}")
    fee_gap_repair_preview = report.get("fee_gap_accounting_repair_preview") or {}
    fee_gap_repair_summary = report.get("fee_gap_accounting_repair_summary") or {}
    print("  [P3.0d] fee_gap_accounting_repair")
    print(
        "    "
        f"incident_kind={fee_gap_repair_preview.get('incident_kind') or 'unknown'} "
        f"incident_scope={fee_gap_repair_preview.get('incident_scope') or 'unknown'} "
        f"resolution_state={fee_gap_repair_preview.get('resolution_state') or 'unknown'} "
        f"active_issue={1 if bool(fee_gap_repair_preview.get('active_issue')) else 0} "
        f"needed={1 if bool(fee_gap_repair_preview.get('needs_repair')) else 0} "
        f"canonical_state={fee_gap_repair_preview.get('canonical_state') or 'unknown'} "
        f"execution_flat={1 if bool(fee_gap_repair_preview.get('execution_flat')) else 0} "
        f"accounting_flat={1 if bool(fee_gap_repair_preview.get('accounting_flat')) else 0} "
        f"resume_blocking={1 if bool(fee_gap_repair_preview.get('resume_blocking')) else 0} "
        f"closeout_blocking={1 if bool(fee_gap_repair_preview.get('closeout_blocking')) else 0} "
        f"resume_policy={fee_gap_repair_preview.get('resume_policy') or 'none'} "
        f"safe_to_apply={1 if bool(fee_gap_repair_preview.get('safe_to_apply')) else 0} "
        f"repair_count={int(fee_gap_repair_summary.get('repair_count') or 0)} "
        f"reason={fee_gap_repair_preview.get('eligibility_reason') or 'none'}"
    )
    print(f"    next_action={fee_gap_repair_preview.get('next_required_action') or 'none'}")
    print(f"    command={fee_gap_repair_preview.get('recommended_command') or 'none'}")
    fee_pending_repair_summary = report.get("fee_pending_accounting_repair_summary") or {}
    print("  [P3.0d2] fee_pending_accounting_repair")
    print(
        "    "
        f"repair_count={int(fee_pending_repair_summary.get('repair_count') or 0)} "
        f"last_client_order_id={fee_pending_repair_summary.get('last_client_order_id') or 'none'} "
        f"last_fill_id={fee_pending_repair_summary.get('last_fill_id') or 'none'} "
        f"last_fee={fee_pending_repair_summary.get('last_fee') if fee_pending_repair_summary.get('last_fee') is not None else 'none'} "
        f"last_reason={fee_pending_repair_summary.get('last_reason') or 'none'}"
    )
    position_rebuild_preview = report.get("position_authority_rebuild_preview") or {}
    position_repair_summary = report.get("position_authority_repair_summary") or {}
    print("  [P3.0d3] position_authority_rebuild")
    print(
        "    "
        f"needed={1 if bool(position_rebuild_preview.get('needs_rebuild')) else 0} "
        f"safe_to_apply={1 if bool(position_rebuild_preview.get('safe_to_apply')) else 0} "
        f"repair_count={int(position_repair_summary.get('repair_count') or 0)} "
        f"stage={position_rebuild_preview.get('recovery_stage') or 'none'} "
        f"repair_mode={position_rebuild_preview.get('repair_mode') or 'none'} "
        f"reason={position_rebuild_preview.get('eligibility_reason') or 'none'}"
    )
    print(f"    command={position_rebuild_preview.get('recommended_command') or 'none'}")
    broker_fill_observation_summary = report.get("broker_fill_observation_summary") or {}
    print("  [P3.0e] broker_fill_observations")
    print(
        "    "
        f"observation_count={int(broker_fill_observation_summary.get('observation_count') or 0)} "
        f"fee_pending_count={int(broker_fill_observation_summary.get('fee_pending_count') or 0)} "
        f"accounting_complete_count={int(broker_fill_observation_summary.get('accounting_complete_count') or 0)} "
        f"order_level_candidate_count={int(broker_fill_observation_summary.get('fee_candidate_order_level_count') or 0)} "
        f"missing_fee_count={int(broker_fill_observation_summary.get('missing_fee_count') or 0)} "
        f"zero_reported_fee_count={int(broker_fill_observation_summary.get('zero_reported_fee_count') or 0)} "
        f"invalid_fee_count={int(broker_fill_observation_summary.get('invalid_fee_count') or 0)}"
    )
    print(
        "    "
        f"last_client_order_id={broker_fill_observation_summary.get('last_client_order_id') or 'none'} "
        f"last_exchange_order_id={broker_fill_observation_summary.get('last_exchange_order_id') or 'none'} "
        f"last_fill_id={broker_fill_observation_summary.get('last_fill_id') or 'none'} "
        f"last_fee_status={broker_fill_observation_summary.get('last_fee_status') or 'none'} "
        f"last_accounting_status={broker_fill_observation_summary.get('last_accounting_status') or 'none'} "
        f"last_source={broker_fill_observation_summary.get('last_source') or 'none'} "
        f"last_fee_validation_reason={broker_fill_observation_summary.get('last_fee_validation_reason') or 'none'}"
    )
    fee_rate_drift = report.get("fee_rate_drift_diagnostics") or {}
    observed_fee_bps_text = (
        "-"
        if fee_rate_drift.get("observed_fee_bps_median") is None
        else f"{float(fee_rate_drift.get('observed_fee_bps_median')):.3f}"
    )
    deviation_bps_text = (
        "-"
        if fee_rate_drift.get("configured_minus_observed_bps") is None
        else f"{float(fee_rate_drift.get('configured_minus_observed_bps')):.3f}"
    )
    deviation_pct_text = (
        "-"
        if fee_rate_drift.get("fee_rate_deviation_pct") is None
        else f"{float(fee_rate_drift.get('fee_rate_deviation_pct')):.2f}"
    )
    print("  [P3.0e1] fee_rate_drift")
    print(
        "    "
        f"configured_fee_rate={float(fee_rate_drift.get('configured_fee_rate') or 0.0):.6f} "
        f"configured_fee_rate_estimate={float(fee_rate_drift.get('configured_fee_rate_estimate') or 0.0):.6f} "
        f"configured_fee_bps={float(fee_rate_drift.get('configured_fee_bps') or 0.0):.3f} "
        f"observed_fee_bps_median={observed_fee_bps_text} "
        f"observed_fee_sample_count={int(fee_rate_drift.get('observed_fee_sample_count') or 0)} "
        f"fee_rate_deviation_pct={deviation_pct_text} "
        f"configured_minus_observed_bps={deviation_bps_text} "
        f"observed_material_fee_sample_count={int(fee_rate_drift.get('observed_material_fee_sample_count') or 0)} "
        f"observation_window_count={int(fee_rate_drift.get('observation_window_count') or 0)}"
    )
    print(
        "    "
        f"recent_expected_fee_rate_mismatch_count={int(fee_rate_drift.get('recent_expected_fee_rate_mismatch_count') or 0)} "
        f"expected_fee_rate_warning_count={int(fee_rate_drift.get('expected_fee_rate_warning_count') or 0)} "
        f"fee_pending_count={int(fee_rate_drift.get('fee_pending_count') or 0)} "
        f"recent_fee_pending_observation_count={int(fee_rate_drift.get('recent_fee_pending_observation_count') or 0)} "
        f"fee_pending_accounting_repair_count={int(fee_rate_drift.get('fee_pending_accounting_repair_count') or 0)} "
        f"position_authority_repair_count={int(fee_rate_drift.get('position_authority_repair_count') or 0)} "
        f"material_notional_threshold_krw={float(fee_rate_drift.get('material_notional_threshold_krw') or 0.0):.1f} "
        f"diagnostic_only_vs_startup_blocking={fee_rate_drift.get('diagnostic_only_vs_startup_blocking') or 'unknown'} "
        f"startup_impact={fee_rate_drift.get('startup_impact') or 'unknown'} "
        f"operator_action={fee_rate_drift.get('operator_action') or 'unknown'} "
        f"recommended_command={fee_rate_drift.get('recommended_command') or 'none'}"
    )
    fill_accounting_incident_projection = report.get("fill_accounting_incident_projection") or {}
    print("  [P3.0e2] fill_accounting_incidents")
    print(
        "    "
        f"active_issue_count={int(fill_accounting_incident_projection.get('active_issue_count') or 0)} "
        f"active_fee_pending_count={int(fill_accounting_incident_projection.get('active_fee_pending_count') or 0)} "
        f"unapplied_principal_pending_count={int(fill_accounting_incident_projection.get('unapplied_principal_pending_count') or 0)} "
        f"principal_applied_fee_pending_count={int(fill_accounting_incident_projection.get('principal_applied_fee_pending_count') or 0)} "
        f"fee_validation_blocked_count={int(fill_accounting_incident_projection.get('fee_validation_blocked_count') or 0)} "
        f"already_accounted_stale_count="
        f"{int(fill_accounting_incident_projection.get('already_accounted_observation_stale_count') or 0)} "
        f"repaired_count={int(fill_accounting_incident_projection.get('repaired_count') or 0)}"
    )
    fill_root_cause = report.get("fill_accounting_root_cause") or {}
    print("  [P3.0e3] fill_accounting_root_cause")
    print(
        "    "
        f"root={fill_root_cause.get('root') or 'none'} "
        f"principal_applied={int(fill_root_cause.get('principal_applied') or 0)} "
        f"broker_local_asset_converged={int(fill_root_cause.get('broker_local_asset_converged') or 0)} "
        f"fee_status={fill_root_cause.get('fee_status') or 'none'} "
        f"fee_source={fill_root_cause.get('fee_source') or 'none'} "
        f"fee_confidence={fill_root_cause.get('fee_confidence') or 'none'} "
        f"operator_action={fill_root_cause.get('operator_action') or 'none'} "
        f"recommended_action={fill_root_cause.get('recommended_action') or 'none'} "
        f"flatten_as_primary_response={1 if bool(fill_root_cause.get('flatten_as_primary_response')) else 0}"
    )
    print(
        "    "
        "root_chain="
        f"{' -> '.join(str(item) for item in (fill_root_cause.get('root_chain') or [])) or 'none'} "
        f"latest_fee_validation_reason={fill_root_cause.get('latest_fee_validation_reason') or 'none'}"
    )
    recovery_policy = report.get("recovery_policy") or {}
    print("  [P3.0e4] recovery_policy")
    print(
        "    "
        f"primary_incident_class={recovery_policy.get('primary_incident_class') or 'RECOVERY_READINESS'} "
        f"recommended_mode={recovery_policy.get('recommended_mode') or 'recovery'} "
        "accounting_root_cause_unresolved="
        f"{1 if bool(recovery_policy.get('accounting_root_cause_unresolved')) else 0} "
        f"accounting_evidence_reliable={1 if bool(recovery_policy.get('accounting_evidence_reliable')) else 0} "
        f"actual_executable_exposure={1 if bool(recovery_policy.get('actual_executable_exposure')) else 0} "
        f"additional_orders_allowed={1 if bool(recovery_policy.get('additional_orders_allowed')) else 0} "
        "flatten_primary_recommendation="
        f"{1 if bool(recovery_policy.get('flatten_primary_recommendation')) else 0}"
    )
    print(
        "    "
        f"recommended_action={recovery_policy.get('recommended_action') or 'none'} "
        f"recommended_command={recovery_policy.get('recommended_command') or 'none'}"
    )
    print("  [P3.1] remote_known_unresolved_verification")
    print(f"    summary={report['remote_known_unresolved_verification_summary']}")
    print("  [P4] last_reconcile_summary")
    print(f"    {report['last_reconcile_summary']}")
    print("  [P5] recent_halt_reason")
    print(f"    {report['recent_halt_reason']}")
    print("  [P6] operator_next_action")
    print(f"    action={report['operator_next_action']}")
    print(f"    recommended_next_action={report['recommended_next_action']}")
    print(f"    resume_blocked_reason={report['resume_blocked_reason']}")
    print(f"    command={report['recommended_command']}")
    print("    hint=check blocker code then run command")
    print("  [P7] unprocessed_remote_open_orders")
    print(f"    count={report['unprocessed_remote_open_orders']}")
    lifecycle = report.get("recent_order_lifecycle") or []
    if lifecycle:
        print(f"  [P8] recent_order_lifecycle(top {len(lifecycle)}):")
        for item in lifecycle:
            print(
                "    - "
                f"client_order_id={item['client_order_id']} "
                f"intent_ts={item['intent_ts']} "
                f"submit_ts={item['submit_ts']} "
                f"correlation={item['correlation']} "
                f"mapping={item['mapping_status']} "
                f"state={item['state']} "
                f"unresolved={item['unresolved']}"
            )

    if report["oldest_unresolved_age_sec"] is None:
        print("  oldest_unresolved_age_sec=none")
    else:
        print(f"  oldest_unresolved_age_sec={float(report['oldest_unresolved_age_sec']):.1f}")

    oldest_orders = report.get("oldest_orders") or []
    if oldest_orders:
        print(f"  oldest_unresolved_orders(top {len(oldest_orders)}):")
        for item in oldest_orders:
            print(
                "    - "
                f"client_order_id={item['client_order_id']} "
                f"status={item['status']} "
                f"exchange_order_id={item['exchange_order_id']} "
                f"age_sec={float(item['age_sec']):.1f} "
                f"last_error={item['last_error']}"
            )

    recovery_required_orders = report.get("recovery_required_summary") or []
    if recovery_required_orders:
        print(f"  recovery_required_orders(top {len(recovery_required_orders)}):")
        for item in recovery_required_orders:
            print(
                "    - "
                f"client_order_id={item['client_order_id']} "
                f"exchange_order_id={item['exchange_order_id']} "
                f"age_sec={float(item['age_sec']):.1f} "
                f"reason={item['last_error']}"
            )

    print("  [P9] recovery_candidates")
    broker_snapshot_error = str(report.get("broker_recent_orders_snapshot_error") or "")
    if broker_snapshot_error:
        print(f"    broker_snapshot={broker_snapshot_error}")
    recovery_candidates = report.get("recovery_candidates") or []
    for item in recovery_candidates:
        print(
            "    - "
            f"local_order_id={item['client_order_id']} "
            f"local_status={item['local_status']} "
            f"candidate_outcome={item['candidate_outcome']}"
        )
        print(
            "      "
            f"attempted_locally={1 if bool(item.get('attempted_locally')) else 0} "
            f"attempted_ts={item.get('attempted_ts')} "
            f"request_likely_sent={item.get('request_likely_sent')}"
        )
        print(f"      attempted_summary={item.get('attempted_summary')}")
        print(
            "      "
            f"likely_broker_match={1 if bool(item.get('likely_broker_match')) else 0} "
            f"likely_broker_exchange_order_id={item.get('likely_broker_exchange_order_id') or '-'} "
            f"likely_broker_match_kind={item.get('likely_broker_match_kind') or 'none'}"
        )
        candidates = item.get("candidates") or []
        if not candidates:
            print("      candidate_exchange_orders=none")
        else:
            for candidate in candidates:
                print(
                    "      * "
                    f"exchange_order_id={candidate['exchange_order_id']} "
                    f"side={candidate['side']} "
                    f"qty={float(candidate['qty']):.8f} "
                    f"filled_qty={float(candidate['filled_qty']):.8f} "
                    f"status={candidate['status']} "
                    f"created_ts={candidate['created_ts']} "
                    f"updated_ts={candidate['updated_ts']} "
                    f"score={int(candidate['score'])} "
                    f"qty_gap_pct={float(candidate['qty_gap_pct']):.3f} "
                    f"time_gap_sec={float(candidate['time_gap_sec']):.1f} "
                    f"price_gap_pct={("-" if candidate['price_gap_pct'] is None else f'{float(candidate["price_gap_pct"]):.3f}')} "
                    f"reason={candidate['match_reason']}"
                )
        print(f"      next_action={item['next_action_hint']}")


def cmd_repair_plan(*, as_json: bool = False) -> None:
    report = _load_recovery_report()
    plan = build_repair_plan_preview_from_report(report)
    if as_json:
        print(json.dumps(plan, ensure_ascii=False, sort_keys=True))
        return

    print("[REPAIR-PLAN]")
    print(
        "  "
        f"plan_id={plan.get('plan_id') or 'none'} "
        f"mode={plan.get('mode') or settings.MODE} "
        f"primary_incident_class={plan.get('primary_incident_class') or 'RECOVERY_READINESS'} "
        f"recommended_mode={plan.get('recommended_mode') or 'recovery'}"
    )
    print(
        "  "
        "accounting_root_cause_unresolved="
        f"{1 if bool(plan.get('accounting_root_cause_unresolved')) else 0} "
        f"accounting_evidence_reliable={1 if bool(plan.get('accounting_evidence_reliable')) else 0} "
        f"additional_orders_allowed={1 if bool(plan.get('additional_orders_allowed')) else 0} "
        "flatten_primary_recommendation="
        f"{1 if bool(plan.get('flatten_primary_recommendation')) else 0}"
    )
    print(
        "  "
        f"recommended_action={plan.get('recommended_action') or 'none'} "
        f"recommended_command={plan.get('recommended_command') or 'none'}"
    )
    print(
        "  "
        f"canonical_portfolio_qty={float(plan.get('canonical_portfolio_qty') or 0.0):.12f} "
        f"broker_qty={float(plan.get('broker_qty') or 0.0):.12f} "
        f"open_position_lots_projected_qty={float(plan.get('open_position_lots_projected_qty') or 0.0):.12f} "
        f"broker_portfolio_converged={1 if bool(plan.get('broker_portfolio_converged')) else 0} "
        f"projection_converged={1 if bool(plan.get('projection_converged')) else 0}"
    )
    print(
        "  "
        f"source_of_truth={plan.get('source_of_truth') or 'unknown'} "
        f"projection_kind={plan.get('projection_kind') or 'unknown'} "
        f"rebuildable={1 if bool(plan.get('rebuildable')) else 0} "
        f"safe_to_rebuild={1 if bool(plan.get('safe_to_rebuild')) else 0} "
        f"reason={plan.get('reason') or 'none'}"
    )
    print("  candidate_repairs:")
    for candidate in plan.get("candidate_repairs") or []:
        print(
            "    - "
            f"name={candidate.get('name') or 'unknown'} "
            f"needed={1 if bool(candidate.get('needed')) else 0} "
            f"active_issue={1 if bool(candidate.get('active_issue')) else 0} "
            f"safe_to_apply={1 if bool(candidate.get('safe_to_apply')) else 0}"
        )
        print(f"      preconditions={candidate.get('preconditions') or 'none'}")
        print(
            "      touched_tables="
            f"{'|'.join(str(item) for item in (candidate.get('touched_tables') or [])) or 'none'}"
        )
        print(f"      expected_after={candidate.get('expected_after') or 'none'}")
        print(f"      idempotency_key={candidate.get('idempotency_key') or 'none'}")
        print(f"      rollback_or_backup={candidate.get('rollback_or_backup') or 'none'}")
        print(f"      why_safe={candidate.get('why_safe') or 'none'}")
        print(f"      recommended_command={candidate.get('recommended_command') or 'none'}")


def _load_json_object_arg(value: str | None, *, field_name: str, allow_none: bool = False) -> dict[str, object] | None:
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{field_name} is required")
    text = str(value).strip()
    if not text:
        if allow_none:
            return None
        raise ValueError(f"{field_name} is required")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return parsed


def cmd_record_external_cash_adjustment(
    *,
    event_ts: int,
    delta_amount: float,
    source: str,
    reason: str,
    broker_snapshot_basis: str,
    currency: str = "KRW",
    correlation_metadata: str | None = None,
    note: str | None = None,
    adjustment_key: str | None = None,
    yes: bool = False,
) -> None:
    if not yes:
        print("[EXTERNAL-CASH-ADJUSTMENT] confirmation required: re-run with --yes to apply")
        raise SystemExit(1)

    conn = ensure_db()
    try:
        basis = _load_json_object_arg(broker_snapshot_basis, field_name="broker_snapshot_basis")
        correlation = _load_json_object_arg(
            correlation_metadata,
            field_name="correlation_metadata",
            allow_none=True,
        )
        adjustment = record_external_cash_adjustment(
            conn,
            event_ts=int(event_ts),
            currency=str(currency),
            delta_amount=float(delta_amount),
            source=str(source),
            reason=str(reason),
            broker_snapshot_basis=basis,
            correlation_metadata=correlation,
            note=note,
            adjustment_key=adjustment_key,
        )
        summary = get_external_cash_adjustment_summary(conn)
    finally:
        conn.close()

    print("[EXTERNAL-CASH-ADJUSTMENT]")
    print(
        "  "
        f"created={1 if bool(adjustment and adjustment.get('created')) else 0} "
        f"adjustment_key={adjustment['adjustment_key']} "
        f"event_ts={kst_str(int(adjustment['event_ts']))} "
        f"delta={float(adjustment['delta_amount']):,.3f} "
        f"currency={adjustment['currency']} "
        f"source={adjustment['source']} "
        f"reason={adjustment['reason']}"
    )
    print(
        "  "
        f"adjustment_count={int(summary.get('adjustment_count') or 0)} "
        f"adjustment_total={float(summary.get('adjustment_total') or 0.0):,.3f} "
        f"last_event={kst_str(int(summary['last_event_ts'])) if summary.get('last_event_ts') is not None else 'none'}"
    )
    if adjustment.get("note"):
        print(f"  note={adjustment['note']}")


def cmd_manual_flat_accounting_repair(*, apply: bool = False, confirm: bool = False, note: str | None = None) -> None:
    conn = ensure_db()
    try:
        preview = build_manual_flat_accounting_repair_preview(conn)
        readiness_snapshot = compute_runtime_readiness_snapshot(conn)
        repair_summary = get_manual_flat_accounting_repair_summary(conn)
        print("[MANUAL-FLAT-ACCOUNTING-REPAIR] preview")
        print(
            "  "
            f"needs_repair={1 if bool(preview['needs_repair']) else 0} "
            f"safe_to_apply={1 if bool(preview['safe_to_apply']) else 0} "
            f"eligibility_reason={preview['eligibility_reason']}"
        )
        print(
            "  "
            f"replay_cash={float(preview['replay_cash']):,.3f} "
            f"portfolio_cash={float(preview['portfolio_cash']):,.3f} "
            f"cash_delta={float(preview['cash_delta']):,.3f}"
        )
        print(
            "  "
            f"replay_qty={float(preview['replay_qty']):.10f} "
            f"portfolio_qty={float(preview['portfolio_qty']):.10f} "
            f"asset_qty_delta={float(preview['asset_qty_delta']):.10f}"
        )
        print(
            "  "
            f"open_order_count={int(preview['open_order_count'])} "
            f"recovery_required_count={int(preview['recovery_required_count'])} "
            f"open_lot_count={int(preview['open_lot_count'])} "
            f"dust_tracking_lot_count={int(preview['dust_tracking_lot_count'])} "
            f"reserved_exit_qty={float(preview['reserved_exit_qty']):.10f}"
        )
        print(
            "  "
            f"last_reconcile_status={preview['last_reconcile_status']} "
            f"last_reconcile_reason_code={preview['last_reconcile_reason_code']} "
            "existing_manual_flat_accounting_repairs="
            f"{int(repair_summary.get('repair_count') or 0)}"
        )
        print(
            "  "
            f"canonical_state={readiness_snapshot.canonical_state} "
            f"residual_class={readiness_snapshot.residual_class} "
            f"run_loop_allowed={1 if readiness_snapshot.run_loop_allowed else 0} "
            f"new_entry_allowed={1 if readiness_snapshot.new_entry_allowed else 0} "
            f"closeout_allowed={1 if readiness_snapshot.closeout_allowed else 0} "
            f"execution_flat={1 if readiness_snapshot.execution_flat else 0} "
            f"accounting_flat={1 if readiness_snapshot.accounting_flat else 0} "
            f"operator_action_required={1 if readiness_snapshot.operator_action_required else 0}"
        )
        print(
            "  "
            "tradeability_operator_message="
            f"{readiness_snapshot.tradeability_operator_fields['tradeability_operator_message']}"
        )
        print(f"  recommended_command={preview['recommended_command']}")

        if not apply:
            print("[MANUAL-FLAT-ACCOUNTING-REPAIR] dry-run: no changes applied")
            return

        if not bool(preview["safe_to_apply"]):
            print("[MANUAL-FLAT-ACCOUNTING-REPAIR] refused: unsafe repair request")
            raise SystemExit(1)
        if not confirm:
            print("[MANUAL-FLAT-ACCOUNTING-REPAIR] confirmation required: re-run with --apply --yes")
            raise SystemExit(1)

        result = apply_manual_flat_accounting_repair(conn, note=note)
        post_preview = build_manual_flat_accounting_repair_preview(conn)
        repair = result["repair"]
    finally:
        conn.close()

    print("[MANUAL-FLAT-ACCOUNTING-REPAIR] applied")
    print(
        "  "
        f"created={1 if bool(repair.get('created')) else 0} "
        f"repair_key={repair['repair_key']} "
        f"event_ts={kst_str(int(repair['event_ts']))} "
        f"cash_delta={float(repair['cash_delta']):,.3f} "
        f"asset_qty_delta={float(repair['asset_qty_delta']):.10f}"
    )
    print(
        "  "
        f"remaining_needs_repair={1 if bool(post_preview['needs_repair']) else 0} "
        f"safe_to_apply={1 if bool(post_preview['safe_to_apply']) else 0} "
        f"eligibility_reason={post_preview['eligibility_reason']}"
    )


def cmd_external_position_accounting_repair(*, apply: bool = False, confirm: bool = False, note: str | None = None) -> None:
    conn = ensure_db()
    try:
        preview = build_external_position_accounting_repair_preview(conn)
        readiness_snapshot = compute_runtime_readiness_snapshot(conn)
        adjustment_summary = get_external_position_adjustment_summary(conn)
        print("[EXTERNAL-POSITION-ACCOUNTING-REPAIR] preview")
        print(
            "  "
            f"needs_repair={1 if bool(preview['needs_repair']) else 0} "
            f"safe_to_apply={1 if bool(preview['safe_to_apply']) else 0} "
            f"eligibility_reason={preview['eligibility_reason']}"
        )
        print(
            "  "
            f"replay_cash={float(preview['replay_cash']):,.3f} "
            f"portfolio_cash={float(preview['portfolio_cash']):,.3f} "
            f"cash_delta={float(preview['cash_delta']):,.3f}"
        )
        print(
            "  "
            f"replay_qty={float(preview['replay_qty']):.10f} "
            f"portfolio_qty={float(preview['portfolio_qty']):.10f} "
            f"asset_qty_delta={float(preview['asset_qty_delta']):.10f}"
        )
        print(
            "  "
            f"open_order_count={int(preview['open_order_count'])} "
            f"recovery_required_count={int(preview['recovery_required_count'])} "
            f"open_lot_count={int(preview['open_lot_count'])} "
            f"dust_tracking_lot_count={int(preview['dust_tracking_lot_count'])} "
            f"reserved_exit_qty={float(preview['reserved_exit_qty']):.10f}"
        )
        print(
            "  "
            f"last_reconcile_status={preview['last_reconcile_status']} "
            f"last_reconcile_reason_code={preview['last_reconcile_reason_code']} "
            f"balance_observed_ts_ms={int(preview['balance_observed_ts_ms'])} "
            "existing_external_position_adjustments="
            f"{int(adjustment_summary.get('adjustment_count') or 0)}"
        )
        print(
            "  "
            f"canonical_state={readiness_snapshot.canonical_state} "
            f"residual_class={readiness_snapshot.residual_class} "
            f"run_loop_allowed={1 if readiness_snapshot.run_loop_allowed else 0} "
            f"new_entry_allowed={1 if readiness_snapshot.new_entry_allowed else 0} "
            f"closeout_allowed={1 if readiness_snapshot.closeout_allowed else 0}"
        )
        print(f"  recommended_command={preview['recommended_command']}")

        if not apply:
            print("[EXTERNAL-POSITION-ACCOUNTING-REPAIR] dry-run: no changes applied")
            return

        if not bool(preview["safe_to_apply"]):
            print("[EXTERNAL-POSITION-ACCOUNTING-REPAIR] refused: unsafe repair request")
            raise SystemExit(1)
        if not confirm:
            print("[EXTERNAL-POSITION-ACCOUNTING-REPAIR] confirmation required: re-run with --apply --yes")
            raise SystemExit(1)

        result = apply_external_position_accounting_repair(conn, note=note)
        post_preview = build_external_position_accounting_repair_preview(conn)
        adjustment = result["adjustment"]
    finally:
        conn.close()

    print("[EXTERNAL-POSITION-ACCOUNTING-REPAIR] applied")
    print(
        "  "
        f"created={1 if bool(adjustment.get('created')) else 0} "
        f"adjustment_key={adjustment['adjustment_key']} "
        f"event_ts={kst_str(int(adjustment['event_ts']))} "
        f"cash_delta={float(adjustment['cash_delta']):,.3f} "
        f"asset_qty_delta={float(adjustment['asset_qty_delta']):.10f}"
    )
    print(
        "  "
        f"remaining_needs_repair={1 if bool(post_preview['needs_repair']) else 0} "
        f"safe_to_apply={1 if bool(post_preview['safe_to_apply']) else 0} "
        f"eligibility_reason={post_preview['eligibility_reason']}"
    )


def cmd_fee_gap_accounting_repair(*, apply: bool = False, confirm: bool = False, note: str | None = None) -> None:
    conn = ensure_db()
    try:
        preview = build_fee_gap_accounting_repair_preview(conn)
        repair_summary = get_fee_gap_accounting_repair_summary(conn)
        print("[FEE-GAP-ACCOUNTING-REPAIR] preview")
        print(
            "  "
            f"needs_repair={1 if bool(preview['needs_repair']) else 0} "
            f"safe_to_apply={1 if bool(preview['safe_to_apply']) else 0} "
            f"already_repaired={1 if bool(preview['already_repaired']) else 0} "
            f"incident_kind={preview.get('incident_kind') or 'unknown'} "
            f"incident_scope={preview.get('incident_scope') or 'unknown'} "
            f"resolution_state={preview.get('resolution_state') or 'unknown'} "
            f"active_issue={1 if bool(preview.get('active_issue')) else 0} "
            f"canonical_state={preview.get('canonical_state') or 'unknown'} "
            f"execution_flat={1 if bool(preview.get('execution_flat')) else 0} "
            f"accounting_flat={1 if bool(preview.get('accounting_flat')) else 0} "
            f"resume_blocking={1 if bool(preview['resume_blocking']) else 0} "
            f"closeout_blocking={1 if bool(preview['closeout_blocking']) else 0} "
            f"resume_policy={preview['resume_policy']} "
            f"eligibility_reason={preview['eligibility_reason']}"
        )
        print(
            "  "
            f"material_zero_fee_fill_count={int(preview['material_zero_fee_fill_count'])} "
            f"fee_gap_adjustment_count={int(preview['fee_gap_adjustment_count'])} "
            f"fee_gap_adjustment_total_krw={float(preview['fee_gap_adjustment_total_krw']):,.3f}"
        )
        print(
            "  "
            f"open_order_count={int(preview['open_order_count'])} "
            f"recovery_required_count={int(preview['recovery_required_count'])} "
            f"open_lot_count={int(preview['open_lot_count'])} "
            f"dust_tracking_lot_count={int(preview['dust_tracking_lot_count'])} "
            f"reserved_exit_qty={float(preview['reserved_exit_qty']):.10f}"
        )
        print(
            "  "
            f"last_reconcile_status={preview['last_reconcile_status']} "
            f"last_reconcile_reason_code={preview['last_reconcile_reason_code']} "
            "existing_fee_gap_accounting_repairs="
            f"{int(repair_summary.get('repair_count') or 0)}"
        )
        print(f"  recommended_command={preview['recommended_command']}")
        print(f"  next_required_action={preview['next_required_action']}")
        print(f"  policy_reason={preview['fee_gap_policy_reason']}")

        if not apply:
            print("[FEE-GAP-ACCOUNTING-REPAIR] dry-run: no changes applied")
            return

        if not bool(preview["safe_to_apply"]):
            print("[FEE-GAP-ACCOUNTING-REPAIR] refused: unsafe repair request")
            raise SystemExit(1)
        if not confirm:
            print("[FEE-GAP-ACCOUNTING-REPAIR] confirmation required: re-run with --apply --yes")
            raise SystemExit(1)

        result = apply_fee_gap_accounting_repair(conn, note=note)
        post_preview = build_fee_gap_accounting_repair_preview(conn)
        repair = result["repair"]
    finally:
        conn.close()

    print("[FEE-GAP-ACCOUNTING-REPAIR] applied")
    print(
        "  "
        f"created={1 if bool(repair.get('created')) else 0} "
        f"repair_key={repair['repair_key']} "
        f"event_ts={kst_str(int(repair['event_ts']))} "
        f"reason={repair['reason']}"
    )
    print(
        "  "
        f"remaining_needs_repair={1 if bool(post_preview['needs_repair']) else 0} "
        f"safe_to_apply={1 if bool(post_preview['safe_to_apply']) else 0} "
        f"eligibility_reason={post_preview['eligibility_reason']}"
    )


def cmd_fee_pending_accounting_repair(
    *,
    client_order_id: str,
    fill_id: str | None = None,
    exchange_order_id: str | None = None,
    fee: float | None = None,
    fee_provenance: str | None = None,
    apply: bool = False,
    confirm: bool = False,
    note: str | None = None,
) -> None:
    conn = ensure_db()
    try:
        preview = build_fee_pending_accounting_repair_preview(
            conn,
            client_order_id=client_order_id,
            fill_id=fill_id,
            exchange_order_id=exchange_order_id,
            fee=fee,
            fee_provenance=fee_provenance,
        )
        repair_summary = get_fee_pending_accounting_repair_summary(conn)
        print("[FEE-PENDING-ACCOUNTING-REPAIR] preview")
        print(
            "  "
            f"needs_repair={1 if bool(preview['needs_repair']) else 0} "
            f"safe_to_apply={1 if bool(preview['safe_to_apply']) else 0} "
            f"repair_mode={preview.get('repair_mode') or 'unknown'} "
            f"eligibility_reason={preview['eligibility_reason']}"
        )
        print(
            "  "
            f"client_order_id={preview['client_order_id']} "
            f"exchange_order_id={preview.get('exchange_order_id') or 'none'} "
            f"fill_id={preview.get('fill_id') or 'none'} "
            f"side={preview['side']} "
            f"fee_status={preview.get('observation_fee_status') or 'none'}"
        )
        print(
            "  "
            f"price={float(preview['price']):.8f} "
            f"qty={float(preview['qty']):.12f} "
            f"notional={float(preview['notional']):,.3f} "
            f"fee={preview.get('fee') if preview.get('fee') is not None else 'none'} "
            f"fee_provenance={preview.get('fee_provenance') or 'none'}"
        )
        print(
            "  "
            f"order_status={preview.get('order_status') or 'none'} "
            f"projected_status={preview.get('projected_status') or 'none'} "
            f"existing_fill_id={preview.get('existing_fill_id') or 'none'} "
            f"existing_fill_fee={preview.get('existing_fill_fee') if preview.get('existing_fill_fee') is not None else 'none'} "
            f"pending_observation_count={int(preview['pending_observation_count'])} "
            f"existing_fee_pending_accounting_repairs={int(repair_summary.get('repair_count') or 0)}"
        )
        print(f"  recommended_command={preview['recommended_command']}")

        if not apply:
            print("[FEE-PENDING-ACCOUNTING-REPAIR] dry-run: no changes applied")
            return

        if not bool(preview["safe_to_apply"]):
            print("[FEE-PENDING-ACCOUNTING-REPAIR] refused: unsafe repair request")
            raise SystemExit(1)
        if not confirm:
            print("[FEE-PENDING-ACCOUNTING-REPAIR] confirmation required: re-run with --apply --yes")
            raise SystemExit(1)

        result = apply_fee_pending_accounting_repair(
            conn,
            client_order_id=client_order_id,
            fill_id=fill_id,
            exchange_order_id=exchange_order_id,
            fee=float(fee if fee is not None else 0.0),
            fee_provenance=str(fee_provenance or ""),
            note=note,
        )
        repair = result["repair"]
        post_lot_snapshot = result["lot_snapshot_after"]
        conn.commit()
    finally:
        conn.close()
    auto_cleared = _finalize_repair_runtime_policy(
        reason_code="FEE_PENDING_ACCOUNTING_REPAIR_COMPLETED",
        metadata={
            "fee_pending_recovery_required": 0,
            "fee_pending_auto_recovering": 0,
            "fee_pending_fill_count": 0,
            "balance_split_mismatch_count": 0,
            "fee_pending_accounting_repair_count": 1,
        },
    )

    print("[FEE-PENDING-ACCOUNTING-REPAIR] applied")
    print(
        "  "
        f"created={1 if bool(repair.get('created')) else 0} "
        f"repair_key={repair['repair_key']} "
        f"event_ts={kst_str(int(repair['event_ts']))} "
        f"client_order_id={repair['client_order_id']} "
        f"fill_id={repair.get('fill_id') or 'none'}"
    )
    print(
        "  "
        f"fee={float(repair['fee']):.8f} "
        f"repair_mode={(result.get('applied_fill', {}).get('repair_mode') if isinstance(result.get('applied_fill'), dict) else None) or 'apply_missing_fill'} "
        f"open_lot_count={int(post_lot_snapshot.get('open_lot_count') or 0)} "
        f"dust_tracking_lot_count={int(post_lot_snapshot.get('dust_tracking_lot_count') or 0)} "
        f"executable_exposure_qty={float(post_lot_snapshot.get('executable_exposure_qty') or 0.0):.12f}"
    )
    print(f"  trading_auto_cleared={1 if auto_cleared else 0}")


def cmd_rebuild_position_authority(
    *,
    apply: bool = False,
    confirm: bool = False,
    note: str | None = None,
    full_projection_rebuild: bool = False,
) -> None:
    conn = ensure_db()
    try:
        preview = build_position_authority_rebuild_preview(
            conn,
            full_projection_rebuild=bool(full_projection_rebuild),
        )
        repair_summary = get_position_authority_repair_summary(conn)
        print("[REBUILD-POSITION-AUTHORITY] preview")
        print(
            "  "
            f"needs_rebuild={1 if bool(preview['needs_rebuild']) else 0} "
            f"safe_to_apply={1 if bool(preview['safe_to_apply']) else 0} "
            f"stage={preview['recovery_stage']} "
            f"repair_mode={preview.get('repair_mode') or 'unknown'} "
            f"eligibility_reason={preview['eligibility_reason']}"
        )
        print(
            "  "
            f"portfolio_qty={float(preview['portfolio_qty']):.12f} "
            f"accounted_buy_qty={float(preview['accounted_buy_qty']):.12f} "
            f"accounted_buy_fill_count={int(preview['accounted_buy_fill_count'])} "
            f"sell_trade_count={int(preview['sell_trade_count'])}"
        )
        print(
            "  "
            f"open_lot_count={int(preview['open_lot_count'])} "
            f"dust_tracking_lot_count={int(preview['dust_tracking_lot_count'])} "
            f"existing_lot_rows={int(preview['existing_lot_rows'])} "
            f"existing_position_authority_repairs={int(repair_summary.get('repair_count') or 0)}"
        )
        assessment = preview.get("position_authority_assessment") or {}
        print(
            "  "
            f"incident_class={assessment.get('incident_class') or 'NONE'} "
            f"target_lot_provenance_kind={preview.get('target_lot_provenance_kind') or 'unknown'} "
            f"broker_qty_known={1 if bool(preview.get('broker_qty_known')) else 0} "
            f"broker_qty={float(preview.get('broker_qty') or 0.0):.12f} "
            f"remote_open_order_count={int(preview.get('remote_open_order_count') or 0)}"
        )
        if preview.get("portfolio_anchor_missing_evidence") or preview.get("manual_projection_missing_evidence"):
            print(
                "  "
                "provenance_missing_evidence="
                f"{'|'.join(str(item) for item in (preview.get('portfolio_anchor_missing_evidence') or []) + (preview.get('manual_projection_missing_evidence') or [])) or 'none'} "
                f"manual_db_update_unsafe={1 if bool(preview.get('manual_db_update_unsafe')) else 0}"
            )
        if preview.get("repair_mode") == "full_projection_rebuild":
            print(
                "  "
                f"projection_converged={1 if bool(preview.get('projection_converged')) else 0} "
                f"projected_total_qty={float(preview.get('projected_total_qty') or 0.0):.12f} "
                f"portfolio_qty={float(preview.get('portfolio_qty') or 0.0):.12f} "
                f"projected_qty_excess={float(preview.get('projected_qty_excess') or 0.0):.12f} "
                f"lot_row_count={int(preview.get('lot_row_count') or 0)} "
                f"other_active_qty={float(preview.get('other_active_qty') or 0.0):.12f}"
            )
            print(
                "  "
                f"portfolio_projection_publication_present="
                f"{1 if bool(preview.get('portfolio_projection_publication_present')) else 0} "
                "portfolio_projection_repair_event_status="
                f"{preview.get('portfolio_projection_repair_event_status') or 'none'} "
                f"needs_full_projection_rebuild={1 if bool(preview.get('needs_full_projection_rebuild')) else 0}"
            )
            print(
                "  "
                f"accounting_projection_ok={1 if bool(preview.get('accounting_projection_ok')) else 0} "
                f"broker_portfolio_converged={1 if bool(preview.get('broker_portfolio_converged')) else 0} "
                f"unresolved_open_order_count={int(preview.get('unresolved_open_order_count') or 0)} "
                f"pending_submit={int(preview.get('pending_submit_count') or 0)} "
                f"submit_unknown={int(preview.get('submit_unknown_count') or 0)} "
                f"unresolved_fee_pending={1 if bool(preview.get('unresolved_fee_pending')) else 0}"
            )
            gate_report = preview.get("full_projection_rebuild_gate_report") or {}
            print(
                "  "
                "full_projection_rebuild_gate_reasons="
                f"{'|'.join(str(item) for item in gate_report.get('reasons') or []) or 'none'}"
            )
        print(f"  next_required_action={preview['next_required_action']}")
        print(f"  recommended_command={preview['recommended_command']}")

        if not apply:
            print("[REBUILD-POSITION-AUTHORITY] dry-run: no changes applied")
            return
        if not bool(preview["safe_to_apply"]):
            print("[REBUILD-POSITION-AUTHORITY] refused: unsafe rebuild request")
            raise SystemExit(1)
        if not confirm:
            print("[REBUILD-POSITION-AUTHORITY] confirmation required: re-run with --apply --yes")
            raise SystemExit(1)

        result = apply_position_authority_rebuild(
            conn,
            note=note,
            full_projection_rebuild=bool(full_projection_rebuild),
        )
        if bool(result.get("noop")):
            after = result["lot_snapshot_after"]
            repair = None
        else:
            repair = result["repair"]
            after = result["lot_snapshot_after"]
            conn.commit()
    finally:
        conn.close()

    if bool(result.get("noop")):
        print("[REBUILD-POSITION-AUTHORITY] no-op")
        print(
            "  "
            f"projection_converged={1 if bool(result.get('post_repair_projection_convergence', {}).get('converged')) else 0} "
            f"open_lot_count={int(after.get('open_lot_count') or 0)} "
            f"dust_tracking_lot_count={int(after.get('dust_tracking_lot_count') or 0)}"
        )
        return

    auto_cleared = _finalize_repair_runtime_policy(
        reason_code="POSITION_AUTHORITY_REBUILD_COMPLETED",
        metadata={},
    )
    print("[REBUILD-POSITION-AUTHORITY] applied")
    if preview.get("repair_mode") == "full_projection_rebuild":
        publication = result.get("projection_publication") or {}
        before = result.get("lot_snapshot_before") or {}
        convergence = result.get("post_repair_projection_convergence") or {}
        print(
            "  "
            f"created={1 if bool(repair.get('created')) else 0} "
            f"repair_key={repair['repair_key']} "
            f"projection_publication_key={publication.get('publication_key') or 'none'} "
            f"event_ts={kst_str(int(repair['event_ts']))}"
        )
        print(
            "  "
            f"old_projected_total_qty={float((preview.get('position_authority_assessment') or {}).get('projected_total_qty') or 0.0):.12f} "
            f"new_projected_total_qty={float(convergence.get('projected_total_qty') or 0.0):.12f} "
            f"portfolio_qty={float(preview.get('portfolio_qty') or 0.0):.12f} "
            f"broker_qty={float(preview.get('broker_qty') or 0.0):.12f}"
        )
        print(
            "  "
            f"old_lot_row_count={int((preview.get('position_authority_assessment') or {}).get('projection_convergence', {}).get('lot_row_count') or 0)} "
            f"new_lot_row_count={int((convergence.get('lot_row_count') or 0))} "
            f"open_lot_count={int(after.get('open_lot_count') or 0)} "
            f"dust_tracking_lot_count={int(after.get('dust_tracking_lot_count') or 0)} "
            f"post_repair_projection_converged={1 if bool(convergence.get('converged')) else 0}"
        )
        print(f"  trading_auto_cleared={1 if auto_cleared else 0}")
    else:
        print(
            "  "
            f"created={1 if bool(repair.get('created')) else 0} "
            f"repair_key={repair['repair_key']} "
            f"event_ts={kst_str(int(repair['event_ts']))} "
            f"open_lot_count={int(after.get('open_lot_count') or 0)} "
            f"dust_tracking_lot_count={int(after.get('dust_tracking_lot_count') or 0)}"
        )
        print(f"  trading_auto_cleared={1 if auto_cleared else 0}")


def _load_restart_safety_checklist() -> list[tuple[str, bool, str]]:
    return evaluate_restart_readiness()


def cmd_restart_checklist() -> None:
    checklist = _load_restart_safety_checklist()
    blocked = [item for item in checklist if not item[1]]
    readiness_snapshot = compute_runtime_readiness_snapshot()
    tradeability_fields = readiness_snapshot.tradeability_operator_fields
    conn = ensure_db()
    try:
        fee_rate_drift = _fee_rate_drift_diagnostics(conn)
    finally:
        conn.close()
    recovery_policy = (_load_recovery_report().get("recovery_policy") or {})

    print("[RESTART-SAFETY-CHECKLIST]")
    for label, ok, detail in checklist:
        status = "PASS" if ok else "BLOCKED"
        print(f"  - {status:<7} {label}: {detail}")
    print(f"  safe_to_resume={1 if not blocked else 0}")
    print(
        "  "
        "resume_scope=process_loop_only "
        f"run_loop_allowed={1 if readiness_snapshot.run_loop_allowed else 0} "
        f"trading_allowed={1 if tradeability_fields['trading_allowed'] else 0} "
        f"new_entry_allowed={1 if readiness_snapshot.new_entry_allowed else 0} "
        f"closeout_allowed={1 if readiness_snapshot.closeout_allowed else 0} "
        f"operator_action_required={1 if readiness_snapshot.operator_action_required else 0}"
    )
    print(
        "  "
        f"canonical_state={readiness_snapshot.canonical_state} "
        f"residual_class={readiness_snapshot.residual_class} "
        f"strategy_tradeability_state={readiness_snapshot.tradeability_operator_fields['strategy_tradeability_state']} "
        f"trading_block_reason={tradeability_fields['trading_block_reason']}"
    )
    print(
        "  "
        f"tradeability_operator_message={tradeability_fields['tradeability_operator_message']}"
    )
    observed_fee_bps = fee_rate_drift.get("observed_fee_bps_median")
    observed_fee_bps_text = "-" if observed_fee_bps is None else f"{float(observed_fee_bps):.3f}"
    deviation_bps = fee_rate_drift.get("configured_minus_observed_bps")
    deviation_bps_text = "-" if deviation_bps is None else f"{float(deviation_bps):.3f}"
    deviation_pct = fee_rate_drift.get("fee_rate_deviation_pct")
    deviation_pct_text = "-" if deviation_pct is None else f"{float(deviation_pct):.2f}"
    print(
        "  "
        f"configured_fee_rate={float(fee_rate_drift.get('configured_fee_rate') or 0.0):.6f} "
        f"configured_fee_rate_estimate={float(fee_rate_drift.get('configured_fee_rate_estimate') or 0.0):.6f} "
        f"configured_fee_bps={float(fee_rate_drift.get('configured_fee_bps') or 0.0):.3f} "
        f"observed_fee_bps_median={observed_fee_bps_text} "
        f"observed_fee_sample_count={int(fee_rate_drift.get('observed_fee_sample_count') or 0)} "
        f"fee_rate_deviation_pct={deviation_pct_text} "
        f"configured_minus_observed_bps={deviation_bps_text} "
        f"recent_expected_fee_rate_mismatch_count={int(fee_rate_drift.get('recent_expected_fee_rate_mismatch_count') or 0)} "
        f"expected_fee_rate_warning_count={int(fee_rate_drift.get('expected_fee_rate_warning_count') or 0)} "
        f"fee_pending_count={int(fee_rate_drift.get('fee_pending_count') or 0)} "
        f"recent_fee_pending_observation_count={int(fee_rate_drift.get('recent_fee_pending_observation_count') or 0)} "
        f"fee_pending_accounting_repair_count={int(fee_rate_drift.get('fee_pending_accounting_repair_count') or 0)} "
        f"position_authority_repair_count={int(fee_rate_drift.get('position_authority_repair_count') or 0)} "
        f"diagnostic_only_vs_startup_blocking={fee_rate_drift.get('diagnostic_only_vs_startup_blocking') or 'unknown'} "
        f"startup_impact={fee_rate_drift.get('startup_impact') or 'unknown'} "
        f"operator_action={fee_rate_drift.get('operator_action') or 'unknown'} "
        f"recommended_command={fee_rate_drift.get('recommended_command') or 'none'}"
    )
    print(
        "  "
        f"primary_incident_class={recovery_policy.get('primary_incident_class') or 'RECOVERY_READINESS'} "
        f"recommended_mode={recovery_policy.get('recommended_mode') or 'recovery'} "
        "accounting_root_cause_unresolved="
        f"{1 if bool(recovery_policy.get('accounting_root_cause_unresolved')) else 0} "
        f"accounting_evidence_reliable={1 if bool(recovery_policy.get('accounting_evidence_reliable')) else 0} "
        f"additional_orders_allowed={1 if bool(recovery_policy.get('additional_orders_allowed')) else 0} "
        "flatten_primary_recommendation="
        f"{1 if bool(recovery_policy.get('flatten_primary_recommendation')) else 0} "
        f"recommended_action={recovery_policy.get('recommended_action') or 'none'} "
        f"recommended_command={recovery_policy.get('recommended_command') or 'none'}"
    )

def _last_reconcile_failed(state) -> bool:
    status = str(getattr(state, "last_reconcile_status", "") or "").upper()
    return status in {"FAILED", "ERROR"}

def cmd_pause() -> None:
    _print_operator_command_contract(
        "PAUSE",
        precondition="operator requested persistent halt; trading may already be paused",
        warning=(
            "live pause stops new orders only; open orders are not canceled and should be handled with "
            "cancel-open-orders or panic-stop if needed"
        ) if settings.MODE == "live" else None,
    )
    runtime_state.enter_halt(
        reason_code="MANUAL_PAUSE",
        reason="manual operator pause",
        unresolved=False,
    )
    resume_allowed, resume_blocks = evaluate_resume_eligibility()
    resume_blockers, resume_blocker_reason_codes = _resume_blocker_summary(resume_blocks)
    state = runtime_state.snapshot()
    print("[PAUSE] trading disabled via persistent runtime state")
    _print_operator_command_contract(
        "PAUSE",
        postcondition=(
            f"trading_enabled={1 if state.trading_enabled else 0}; "
            f"halt_new_orders_blocked={1 if state.halt_new_orders_blocked else 0}; "
            f"resume_gate_blocked={1 if state.resume_gate_blocked else 0}; "
            f"resume_allowed={1 if resume_allowed else 0}; "
            f"resume_blockers={resume_blockers}; "
            f"resume_blocker_reason_codes={resume_blocker_reason_codes}; "
            f"halt_reason_code={state.halt_reason_code or '-'}"
        ),
    )


def cmd_panic_stop(*, flatten: bool = False) -> None:
    if settings.MODE != "live":
        print(f"[PANIC-STOP] skipped: MODE={settings.MODE} (live only)")
        raise SystemExit(1)

    _print_operator_command_contract(
        "PANIC-STOP",
        precondition=(
            f"MODE={settings.MODE}; LIVE_DRY_RUN={1 if settings.LIVE_DRY_RUN else 0}; "
            f"flatten={1 if flatten else 0}; live preflight required"
        ),
        warning=(
            "new orders are blocked immediately, open orders are canceled, "
            "and flatten is optional but conservative default remains no-flatten"
        ),
    )

    try:
        validate_live_mode_preflight(settings)
    except LiveModeValidationError as e:
        print(f"[PANIC-STOP] failed: {e}")
        raise SystemExit(1)

    runtime_state.disable_trading_until(
        float("inf"),
        reason="panic stop requested by operator; cleanup pending",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=True,
        attempt_flatten=bool(flatten),
    )

    from .broker.bithumb import BithumbBroker

    try:
        broker = BithumbBroker()
        halt_reason, canceled_ok, unresolved = perform_panic_stop_cleanup(
            broker,
            reason_code="KILL_SWITCH",
            reason_detail="panic stop requested by operator",
            cancel_trigger="panic-stop",
            flatten_trigger="panic-stop",
            attempt_flatten=bool(flatten),
        )
    except Exception as exc:
        reason_detail = f"panic stop cleanup failed ({type(exc).__name__}): {exc}"
        runtime_state.disable_trading_until(
            float("inf"),
            reason=reason_detail,
            reason_code="KILL_SWITCH",
            halt_new_orders_blocked=True,
            unresolved=True,
            attempt_flatten=bool(flatten),
        )
        notify(
            safety_event(
                "panic_stop_failed",
                reason_code="KILL_SWITCH",
                state_to="HALTED",
                flatten_requested=1 if flatten else 0,
                reason=reason_detail,
            )
        )
        print(f"[PANIC-STOP] failed: {reason_detail}")
        raise SystemExit(1)

    runtime_state.disable_trading_until(
        float("inf"),
        reason=halt_reason.detail,
        reason_code=halt_reason.code,
        halt_new_orders_blocked=True,
        unresolved=unresolved,
        attempt_flatten=bool(flatten),
    )
    resume_allowed, resume_blocks = evaluate_resume_eligibility()
    resume_blocker_codes = ", ".join(blocker.code for blocker in resume_blocks) if resume_blocks else "none"
    resume_blocker_reason_codes = (
        ", ".join(str(getattr(blocker, "reason_code", blocker.code)) for blocker in resume_blocks)
        if resume_blocks
        else "none"
    )
    resume_precondition = "clear" if resume_allowed else "blocked"
    state = runtime_state.snapshot()
    notify(
        safety_event(
            "panic_stop_completed",
            reason_code=halt_reason.code,
            state_to="HALTED",
            flatten_requested=1 if flatten else 0,
            cancel_accepted=1 if canceled_ok else 0,
            unresolved=1 if unresolved else 0,
            resume_allowed=1 if resume_allowed else 0,
            resume_blockers=resume_blocker_codes,
            resume_blocker_reason_codes=resume_blocker_reason_codes,
            cancel_status=state.last_cancel_open_orders_status or "unknown",
            flatten_status=state.last_flatten_position_status or "skipped",
            auto_liquidate_requested=1 if state.halt_policy_auto_liquidate_positions else 0,
            resume_precondition=resume_precondition,
            reason=halt_reason.detail,
        )
    )

    print("[PANIC-STOP]")
    print(f"  flatten_requested={1 if flatten else 0}")
    print(f"  trading_enabled={state.trading_enabled}")
    print(f"  halt_new_orders_blocked={state.halt_new_orders_blocked}")
    print(f"  halt_policy_auto_liquidate_positions={1 if state.halt_policy_auto_liquidate_positions else 0}")
    print(f"  halt_reason_code={state.halt_reason_code}")
    print(f"  last_disable_reason={state.last_disable_reason}")
    print(f"  last_cancel_open_orders_status={state.last_cancel_open_orders_status}")
    print(f"  last_flatten_position_status={state.last_flatten_position_status}")
    print(f"  unresolved_open_order_count={state.unresolved_open_order_count}")
    print(f"  halt_open_orders_present={state.halt_open_orders_present}")
    print(f"  halt_position_present={state.halt_position_present}")
    print(f"  resume_allowed={1 if resume_allowed else 0}")
    print(f"  resume_blockers={resume_blocker_codes}")
    print(f"  resume_blocker_reason_codes={resume_blocker_reason_codes}")
    print(f"  resume_precondition={resume_precondition}")


def _build_live_broker():
    return bithumb_broker_module.BithumbBroker()


def _running_under_pytest() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ


class _OfflineTestReconcileBroker:
    """Safe broker used by operator-command tests unless they opt into live deps."""

    def get_order(
        self,
        *,
        client_order_id: str,
        exchange_order_id: str | None = None,
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            side="BUY",
            status="NEW",
            price=None,
            qty_req=0.0,
            qty_filled=0.0,
            created_ts=0,
            updated_ts=0,
        )

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> list:
        return []

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list:
        return []

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list:
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=0.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )


def _default_live_reconcile_dependencies(*, broker_factory=None, reconcile_fn=None):
    resolved_reconcile_fn = reconcile_fn or reconcile_with_broker
    if broker_factory is not None:
        return broker_factory, resolved_reconcile_fn

    current_broker_class = bithumb_broker_module.BithumbBroker
    if _running_under_pytest() and current_broker_class is DEFAULT_BITHUMB_BROKER_CLASS:
        return _OfflineTestReconcileBroker, resolved_reconcile_fn

    return _build_live_broker, resolved_reconcile_fn


def _run_live_reconcile(*, broker_factory=None, reconcile_fn=None) -> None:
    resolved_broker_factory, resolved_reconcile_fn = _default_live_reconcile_dependencies(
        broker_factory=broker_factory,
        reconcile_fn=reconcile_fn,
    )
    resolved_reconcile_fn(resolved_broker_factory())


def cmd_resume(
    force: bool = False,
    *,
    broker_factory=None,
    reconcile_fn=None,
) -> None:
    _print_operator_command_contract(
        "RESUME",
        precondition=(
            f"trading paused or halted; force={1 if force else 0}; "
            f"MODE={settings.MODE}; live_reconcile={'yes' if settings.MODE == 'live' else 'no'}"
        ),
        warning=(
            "live resume runs reconciliation before the gate check; "
            "force resume bypasses overridable blockers only and non-overridable safety blockers still refuse"
        ) if settings.MODE == "live" else None,
    )
    if settings.MODE == "live":
        _run_live_reconcile(
            broker_factory=broker_factory,
            reconcile_fn=reconcile_fn,
        )

    eligible, resume_blocks = evaluate_resume_eligibility()

    if (not eligible) and (not force):
        print("[RESUME] refused:")
        print("  blocking_reasons:")
        for blocker in resume_blocks:
            print(
                "  - "
                f"code={blocker.code} "
                f"overridable={1 if bool(blocker.overridable) else 0} "
                f"detail={blocker.detail}"
            )
        print("  run `uv run python bot.py recovery-report` for details")
        print("  or resume explicitly with `uv run python bot.py resume --force`")
        raise SystemExit(1)

    if force and resume_blocks:
        non_overridable_blocks = [b for b in resume_blocks if not bool(b.overridable)]
        if non_overridable_blocks:
            print("[RESUME] refused: force override denied")
            print("  non_overridable_blockers:")
            for blocker in non_overridable_blocks:
                print(
                    "  - "
                    f"code={blocker.code} "
                    f"overridable={1 if bool(blocker.overridable) else 0} "
                    f"detail={blocker.detail}"
                )
            print("  run `uv run python bot.py recovery-report` for details")
            raise SystemExit(1)

    enable_trading()
    if force and resume_blocks:
        block_summary = "; ".join(
            f"{blocker.code}[overridable={1 if bool(blocker.overridable) else 0}]:{blocker.detail}"
            for blocker in resume_blocks
        )
        print(f"[RESUME] forced: trading enabled despite blocks={block_summary}")
        print("[RESUME] override_applied=1 override_reason=operator_force_resume")
    else:
        print("[RESUME] trading enabled")
    state = runtime_state.snapshot()
    resume_blockers, resume_blocker_reason_codes = _resume_blocker_summary(resume_blocks)
    _print_operator_command_contract(
        "RESUME",
        postcondition=(
            f"trading_enabled={1 if state.trading_enabled else 0}; "
            f"resume_gate_blocked={1 if state.resume_gate_blocked else 0}; "
            f"resume_gate_reason={state.resume_gate_reason or 'none'}; "
            f"resume_blockers={resume_blockers}; "
            f"resume_blocker_reason_codes={resume_blocker_reason_codes}; "
            f"force_override={1 if force and bool(resume_blocks) else 0}"
        ),
    )


def cmd_reconcile(*, broker_factory=None, reconcile_fn=None) -> None:
    if settings.MODE != "live":
        print(f"[RECONCILE] skipped: MODE={settings.MODE} (live only)")
        return

    _print_operator_command_contract(
        "RECONCILE",
        precondition="MODE=live; broker snapshot refresh will replay recent exchange state into the local ledger",
        warning=(
            "live recovery command: recent remote orders and fills will be replayed into the local ledger; "
            "run when operator intends a recovery pass"
        ),
    )
    _run_live_reconcile(
        broker_factory=broker_factory,
        reconcile_fn=reconcile_fn,
    )
    print("[RECONCILE] completed one live reconciliation pass")
    resume_allowed, resume_blocks = evaluate_resume_eligibility()
    resume_blockers, resume_blocker_reason_codes = _resume_blocker_summary(resume_blocks)
    state = runtime_state.snapshot()
    _print_operator_command_contract(
        "RECONCILE",
        postcondition=(
            f"last_reconcile_status={state.last_reconcile_status or 'none'}; "
            f"last_reconcile_reason_code={state.last_reconcile_reason_code or 'none'}; "
            f"resume_gate_blocked={1 if state.resume_gate_blocked else 0}; "
            f"resume_allowed={1 if resume_allowed else 0}; "
            f"resume_blockers={resume_blockers}; "
            f"resume_blocker_reason_codes={resume_blocker_reason_codes}; "
            f"unresolved_open_order_count={state.unresolved_open_order_count}; "
            f"recovery_required_count={state.recovery_required_count}"
        ),
    )


def cmd_flatten_position(*, dry_run: bool = False) -> None:
    if settings.MODE != "live":
        print(f"[FLATTEN-POSITION] skipped: MODE={settings.MODE} (live only)")
        raise SystemExit(1)

    if not dry_run:
        try:
            validate_live_mode_preflight(settings)
        except LiveModeValidationError as e:
            print(f"[FLATTEN-POSITION] failed: {e}")
            raise SystemExit(1)

    from .broker.bithumb import BithumbBroker, classify_private_api_error

    broker = BithumbBroker()
    summary = flatten_btc_position(broker=broker, dry_run=dry_run, trigger="operator")
    status = str(summary.get("status") or "")
    qty = float(summary.get("qty") or 0.0)

    if status == "no_position":
        print(
            "[FLATTEN-POSITION] no position to flatten: no executable position "
            f"(sellable_lot_count={int(summary.get('sellable_executable_lot_count') or 0)} "
            f"terminal_state={summary.get('terminal_state') or 'unknown'} "
            f"reason={summary.get('reason') or 'no_position'})"
        )
        if (
            float(summary.get("raw_total_asset_qty") or 0.0) > 0.0
            or float(summary.get("tracked_dust_qty") or 0.0) > 0.0
        ):
            print(
                "  "
                f"raw_total_asset_qty={float(summary.get('raw_total_asset_qty') or 0.0):.10f} "
                f"executable_exposure_qty={float(summary.get('executable_exposure_qty') or 0.0):.10f} "
                f"tracked_dust_qty={float(summary.get('tracked_dust_qty') or 0.0):.10f} "
                f"closeout_allowed={1 if bool(summary.get('closeout_allowed')) else 0}"
            )
        return

    print(f"[FLATTEN-POSITION] target=BTC side=SELL qty={qty:.8f} dry_run={1 if dry_run else 0}")
    if status == "dry_run":
        print("[FLATTEN-POSITION] dry-run: submit skipped")
        return

    if status == "blocked":
        print(
            "[FLATTEN-POSITION] blocked "
            f"reason={str(summary.get('reason') or 'unknown')} "
            f"recovery_stage={str(summary.get('recovery_stage') or 'unknown')} "
            f"recommended_command={str(summary.get('recommended_command') or 'uv run python bot.py recovery-report')}"
        )
        raise SystemExit(1)

    if status == "failed":
        err = str(summary.get("error") or "unknown flatten failure")
        print(f"[FLATTEN-POSITION] failed: {err}")
        raise SystemExit(1)

    print(
        "[FLATTEN-POSITION] submitted "
        f"client_order_id={str(summary.get('client_order_id') or '-')} "
        f"exchange_order_id={str(summary.get('exchange_order_id') or '-')} "
        f"status={str(summary.get('order_status') or '-')}"
    )


_RECOVERABLE_UNRESOLVED_STATUSES = {"PENDING_SUBMIT", "NEW", "PARTIAL", "SUBMIT_UNKNOWN"}
_TERMINAL_REMOTE_ORDER_STATUSES = {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "FAILED"}


def _build_recover_order_preview(
    *,
    client_order_id: str,
    exchange_order_id: str,
    broker=None,
) -> dict[str, object]:
    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT client_order_id, status, exchange_order_id, qty_filled, last_error,
                   side, price, qty_req, created_ts, updated_ts
            FROM orders
            WHERE client_order_id=?
            """,
            (client_order_id,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return {
            "exists": False,
            "safe_to_apply": False,
            "target_client_order_id": client_order_id,
            "target_exchange_order_id": exchange_order_id,
            "current_status": "UNKNOWN",
            "current_exchange_order_id": "-",
            "eligibility_reason": "client_order_id not found",
            "proposed_action": "manual_recover_with_exchange_id",
            "state_changes": ["none (client_order_id not found)"],
        }

    current_status = str(row["status"] or "UNKNOWN")
    current_exchange_order_id = str(row["exchange_order_id"] or "").strip()
    if current_status == "RECOVERY_REQUIRED":
        return {
            "exists": True,
            "safe_to_apply": True,
            "target_client_order_id": client_order_id,
            "target_exchange_order_id": exchange_order_id,
            "current_status": current_status,
            "current_exchange_order_id": (current_exchange_order_id or "-"),
            "eligibility_reason": "status is RECOVERY_REQUIRED",
            "proposed_action": "manual_recover_with_exchange_id",
            "state_changes": [
                f"exchange_order_id -> {exchange_order_id}",
                "fetch remote order + fills and apply missing fills",
                "final order status updated from broker snapshot",
                "trading remains disabled until explicit resume",
            ],
            "last_error": str(row["last_error"] or "-"),
            "qty_filled": float(row["qty_filled"] or 0.0),
        }

    eligibility_reason = "client_order_id must exist and be safely attributable"
    safe_to_apply = False
    plausible_candidate_count = 0
    likely_broker_exchange_order_id = None
    remote_status = "-"
    if current_status in _RECOVERABLE_UNRESOLVED_STATUSES:
        if not exchange_order_id:
            eligibility_reason = "exchange_order_id is required for unresolved local order recovery"
        elif current_exchange_order_id and current_exchange_order_id != exchange_order_id:
            eligibility_reason = "exchange_order_id mismatch with local order mapping"
        elif broker is None:
            eligibility_reason = "broker snapshot required to verify high-confidence unresolved recovery"
        else:
            local_order = {
                "client_order_id": str(row["client_order_id"]),
                "status": current_status,
                "exchange_order_id": (current_exchange_order_id or "-"),
                "side": str(row["side"] or "-"),
                "price": (float(row["price"]) if row["price"] is not None else None),
                "qty_req": float(row["qty_req"] or 0.0),
                "qty_filled": float(row["qty_filled"] or 0.0),
                "created_ts": int(row["created_ts"]),
                "updated_ts": int(row["updated_ts"]),
                "submit_evidence_attempted_ts": int(row["created_ts"]),
            }
            recent_orders = broker.get_recent_orders(
                limit=100,
                exchange_order_ids=[exchange_order_id],
                client_order_ids=[client_order_id],
            )
            candidates = _build_recovery_candidates(local_order=local_order, recent_orders=recent_orders)
            plausible_candidates = [c for c in candidates if int(c.get("high_confidence") or 0) == 1]
            plausible_candidate_count = len(plausible_candidates)
            likely_candidate = plausible_candidates[0] if plausible_candidate_count == 1 else None
            likely_broker_exchange_order_id = (
                str(likely_candidate["exchange_order_id"]) if likely_candidate is not None else None
            )
            if plausible_candidate_count != 1:
                eligibility_reason = "requires exactly one high-confidence broker candidate"
            elif likely_broker_exchange_order_id != exchange_order_id:
                eligibility_reason = "suggested high-confidence candidate does not match requested exchange_order_id"
            else:
                remote = broker.get_order(
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id,
                )
                remote_status = str(remote.status or "").upper()
                if remote_status not in _TERMINAL_REMOTE_ORDER_STATUSES:
                    eligibility_reason = f"remote order is not terminal (status={remote.status})"
                else:
                    safe_to_apply = True
                    eligibility_reason = (
                        "unresolved local order with single high-confidence candidate and terminal remote snapshot"
                    )

    return {
        "exists": True,
        "safe_to_apply": safe_to_apply,
        "target_client_order_id": client_order_id,
        "target_exchange_order_id": exchange_order_id,
        "current_status": current_status,
        "current_exchange_order_id": (current_exchange_order_id or "-"),
        "eligibility_reason": eligibility_reason,
        "plausible_candidate_count": plausible_candidate_count,
        "likely_broker_exchange_order_id": likely_broker_exchange_order_id,
        "remote_terminal_status": remote_status,
        "proposed_action": "manual_recover_with_exchange_id",
        "state_changes": [
            f"exchange_order_id -> {exchange_order_id}",
            "fetch remote order + fills and apply missing fills",
            "final order status updated from broker snapshot",
            "trading remains disabled until explicit resume",
        ],
        "last_error": str(row["last_error"] or "-"),
        "qty_filled": float(row["qty_filled"] or 0.0),
    }


def cmd_recover_order(
    *,
    client_order_id: str,
    exchange_order_id: str,
    dry_run: bool = False,
    confirm: bool = False,
    broker_factory=None,
) -> None:
    if settings.MODE != "live":
        print(f"[RECOVER-ORDER] skipped: MODE={settings.MODE} (live only)")
        raise SystemExit(1)

    broker = None
    preview = _build_recover_order_preview(
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        broker=None,
    )
    if (
        not bool(preview.get("safe_to_apply"))
        and preview.get("eligibility_reason")
        == "broker snapshot required to verify high-confidence unresolved recovery"
    ):
        if broker_factory is not None:
            broker = broker_factory()
        else:
            from .broker.bithumb import BithumbBroker

            broker = BithumbBroker()
        preview = _build_recover_order_preview(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            broker=broker,
        )
    print("[RECOVER-ORDER] preview")
    print(
        "  target_order_id="
        f"{preview['target_client_order_id']} exchange_order_id={preview['target_exchange_order_id']}"
    )
    print(
        "  current_known_state="
        f"status={preview['current_status']} "
        f"exchange_order_id={preview['current_exchange_order_id']}"
    )
    print(f"  eligibility_reason={preview.get('eligibility_reason')}")
    print(f"  proposed_recovery_action={preview['proposed_action']}")
    print("  important_state_changes:")
    for change in preview.get("state_changes", []):
        print(f"    - {change}")

    if dry_run:
        print("[RECOVER-ORDER] dry-run: no changes applied")
        return

    if not bool(preview.get("safe_to_apply")):
        print("[RECOVER-ORDER] refused: unsafe recovery request")
        print(f"  reason={preview.get('eligibility_reason')}")
        raise SystemExit(1)

    if not confirm:
        print("[RECOVER-ORDER] confirmation required: re-run with --yes to apply")
        raise SystemExit(1)

    if broker is None:
        if broker_factory is not None:
            broker = broker_factory()
        else:
            from .broker.bithumb import BithumbBroker

            broker = BithumbBroker()

    disable_trading_until(float("inf"), reason="manual recovery in progress")
    try:
        recover_order_with_exchange_id(
            broker,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
        )
    except Exception as e:
        disable_trading_until(float("inf"), reason="manual recovery failed; resume required")
        print(f"[RECOVER-ORDER] failed: {type(e).__name__}: {e}")
        print("  order remains RECOVERY_REQUIRED; inspect and retry")
        raise SystemExit(1)

    disable_trading_until(float("inf"), reason="manual recovery completed; explicit resume required")
    print("[RECOVER-ORDER] completed")
    print("  trading remains disabled; run `uv run python bot.py resume` when ready")


def cmd_backfill_broker_order(
    *,
    exchange_order_id: str,
    dry_run: bool = False,
    confirm: bool = False,
    broker_factory=None,
) -> None:
    if settings.MODE != "live":
        print(f"[BACKFILL-BROKER-ORDER] skipped: MODE={settings.MODE} (live only)")
        raise SystemExit(1)

    broker = broker_factory() if broker_factory is not None else None
    if broker is None:
        from .broker.bithumb import BithumbBroker

        broker = BithumbBroker()

    remote = broker.get_order(client_order_id=None, exchange_order_id=exchange_order_id)
    print("[BACKFILL-BROKER-ORDER] preview")
    print(f"  exchange_order_id={exchange_order_id}")
    print(f"  broker_client_order_id={remote.client_order_id or '-'}")
    print(f"  broker_status={remote.status}")
    print(f"  side={remote.side} qty_req={float(remote.qty_req or 0.0):.12f} qty_filled={float(remote.qty_filled or 0.0):.12f}")
    print("  proposed_action=create synthetic local OMS lineage and apply recoverable broker fills")

    if dry_run:
        print("[BACKFILL-BROKER-ORDER] dry-run: no changes applied")
        return

    if not confirm:
        print("[BACKFILL-BROKER-ORDER] confirmation required: re-run with --yes to apply")
        raise SystemExit(1)

    disable_trading_until(float("inf"), reason="broker-known backfill in progress")
    try:
        result = backfill_broker_order_with_exchange_id(
            broker,
            exchange_order_id=exchange_order_id,
        )
    except Exception as e:
        disable_trading_until(float("inf"), reason="broker-known backfill failed; resume required")
        print(f"[BACKFILL-BROKER-ORDER] failed: {type(e).__name__}: {e}")
        raise SystemExit(1)

    disable_trading_until(float("inf"), reason="broker-known backfill completed; explicit resume required")
    print("[BACKFILL-BROKER-ORDER] completed")
    print(f"  client_order_id={result['client_order_id']}")
    print(f"  exchange_order_id={result['exchange_order_id']}")
    print(f"  status={result['status']}")
    print(f"  fill_count={result['fill_count']} applied_fill_count={result['applied_fill_count']}")
    print(f"  blocked_reason={result['blocked_reason']}")
    print("  trading remains disabled; run reconcile and recovery-report before resume")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bithumb-bot")
    sub = p.add_subparsers(dest="cmd", required=False)

    sub.add_parser("ticker")

    o = sub.add_parser("orders")
    o.add_argument("--limit", type=int, default=50)

    f = sub.add_parser("fills")
    f.add_argument("--limit", type=int, default=50)

    c = sub.add_parser("candles")
    c.add_argument("--limit", type=int, default=5)

    sub.add_parser("sync")

    s = sub.add_parser("signal")
    s.add_argument("--short", type=int, default=SMA_SHORT)
    s.add_argument("--long", type=int, default=SMA_LONG)

    e = sub.add_parser("explain")
    e.add_argument("--short", type=int, default=SMA_SHORT)
    e.add_argument("--long", type=int, default=SMA_LONG)

    sub.add_parser("status")
    sub.add_parser("audit")
    sub.add_parser("check")
    sub.add_parser(
        "health",
        help="show health summary (staleness/errors/trading state/recovery)",
        description="Show health summary for limited unattended operation checks.",
    )
    sub.add_parser("audit-ledger")
    sub.add_parser(
        "cancel-open-orders",
        help="cancel all remote open orders in live mode",
        description="Cancel all remote open orders (live mode only).",
    )
    sub.add_parser(
        "broker-diagnose",
        help="read-only live broker/API diagnostics",
        description="Run read-only live broker diagnostics (no order create/cancel).",
    )
    sub.add_parser(
        "pause",
        help="persistently pause new trading",
        description="Persistently disable trading until explicit resume.",
    )
    flatten = sub.add_parser(
        "flatten-position",
        help="emergency flatten open position",
        description="Flatten current position for emergency exposure reduction.",
    )
    flatten.add_argument("--dry-run", action="store_true")

    resume = sub.add_parser(
        "resume",
        help="resume trading if safety checks pass",
        description="Resume trading with safety gates; use --force only as a last resort.",
    )
    resume.add_argument("--force", action="store_true")

    sub.add_parser(
        "reconcile",
        help="reconcile local/exchange order state",
        description="Run order-state reconciliation with the broker.",
    )
    recovery_report = sub.add_parser(
        "recovery-report",
        help="show unresolved/recovery-required order report",
        description="Show unresolved/recovery-required orders and resume blockers.",
    )
    recovery_report.add_argument("--json", action="store_true")
    repair_plan = sub.add_parser(
        "repair-plan",
        help="show non-mutating accounting recovery plan preview",
        description="Aggregate existing recovery and repair previews into one operator-oriented plan.",
    )
    repair_plan.add_argument("--json", action="store_true")
    sub.add_parser(
        "restart-checklist",
        help="print restart safety checklist before resume",
        description="Print restart safety checklist for operator restart verification.",
    )
    panic_stop = sub.add_parser(
        "panic-stop",
        help="halt trading, cancel open orders, and optionally flatten the position",
    )
    panic_stop.add_argument(
        "--flatten",
        action="store_true",
        help="attempt an explicit flatten after open-order cancellation",
    )
    recover_order = sub.add_parser("recover-order")
    recover_order.add_argument("--client-order-id", required=True)
    recover_order.add_argument("--exchange-order-id", required=True)
    recover_order.add_argument("--dry-run", action="store_true")
    recover_order.add_argument("--yes", action="store_true")

    backfill_broker_order = sub.add_parser("backfill-broker-order")
    backfill_broker_order.add_argument("--exchange-order-id", required=True)
    backfill_broker_order.add_argument("--dry-run", action="store_true")
    backfill_broker_order.add_argument("--yes", action="store_true")

    report = sub.add_parser("report")
    report.add_argument("--days", type=int, default=30)

    t = sub.add_parser("trades")
    t.add_argument("--limit", type=int, default=20)

    ops = sub.add_parser("ops-report", help="operator observability report")
    ops.add_argument("--limit", type=int, default=20)

    risk_report = sub.add_parser(
        "risk-report",
        help="show daily-loss baseline and recent risk evaluations",
    )
    risk_report.add_argument("--limit", type=int, default=20)
    risk_report.add_argument("--json", action="store_true")

    decision_telemetry = sub.add_parser(
        "decision-telemetry",
        help="summary of HOLD/blocked decision telemetry",
    )
    decision_telemetry.add_argument("--limit", type=int, default=200)

    fee_diag = sub.add_parser(
        "fee-diagnostics",
        help="validate real fee application against recent fills/roundtrips",
    )
    fee_diag.add_argument("--fill-limit", type=int, default=100)
    fee_diag.add_argument("--roundtrip-limit", type=int, default=50)
    fee_diag.add_argument(
        "--estimated-fee-rate",
        type=float,
        default=None,
        help="expected fee rate (default: FEE_RATE setting)",
    )
    fee_diag.add_argument("--json", action="store_true")

    strategy_report = sub.add_parser(
        "strategy-report",
        help="strategy performance comparison report",
        description="Aggregate trade_lifecycles by strategy/exit-rule/date range for experiments.",
    )
    strategy_report.add_argument("--strategy-name")
    strategy_report.add_argument("--exit-rule-name")
    strategy_report.add_argument("--pair")
    strategy_report.add_argument("--from-date", help="KST date (YYYY-MM-DD)")
    strategy_report.add_argument("--to-date", help="KST date (YYYY-MM-DD)")
    strategy_report.add_argument(
        "--group-by",
        default="strategy_name,exit_rule_name",
        help="comma-separated axes: strategy_name,exit_rule_name,pair",
    )
    strategy_report.add_argument(
        "--observation-window-bars",
        type=int,
        default=5,
        help="blocked-entry ?????????몃뱥??????????????????????????, ????????5",
    )
    strategy_report.add_argument(
        "--min-observation-sample",
        type=int,
        default=10,
        help="blocked-entry ?????????몃뱥?????????됰Ŧ?????????????????븐뼐?????????????????⑥レ뿥?????棺堉?뤃??믠뫖夷???????????insufficient sample ??????",
    )
    strategy_report.add_argument("--json", action="store_true")

    experiment_report = sub.add_parser(
        "experiment-report",
        help="expectancy validation report for small live experiments",
        description="Report realized PnL/sample distribution/time-regime bias for experiment interpretation.",
    )
    experiment_report.add_argument("--strategy-name")
    experiment_report.add_argument("--pair")
    experiment_report.add_argument("--from-date", help="KST date (YYYY-MM-DD)")
    experiment_report.add_argument("--to-date", help="KST date (YYYY-MM-DD)")
    experiment_report.add_argument("--sample-threshold", type=int, default=30)
    experiment_report.add_argument("--top-n", type=int, default=3)
    experiment_report.add_argument("--concentration-threshold", type=float, default=0.6)
    experiment_report.add_argument("--regime-skew-threshold", type=float, default=0.7)
    experiment_report.add_argument("--regime-pnl-skew-threshold", type=float, default=0.7)
    experiment_report.add_argument("--json", action="store_true")

    cash_drift_report = sub.add_parser(
        "cash-drift-report",
        help="audit broker cash versus local ledger and recent external cash adjustments",
        description="Read-only cash drift diagnostic for broker/local comparison and adjustment review.",
    )
    cash_drift_report.add_argument("--recent-limit", type=int, default=5)
    cash_drift_report.add_argument("--json", action="store_true")

    fee_gap_accounting_repair = sub.add_parser(
        "fee-gap-accounting-repair",
        help="preview or apply explicit fee-gap accounting recovery",
        description="Record an explicit bounded fee-gap accounting repair for reconcile-detected historical zero-fee fill drift.",
    )
    fee_gap_accounting_repair.add_argument("--apply", action="store_true")
    fee_gap_accounting_repair.add_argument("--yes", action="store_true")
    fee_gap_accounting_repair.add_argument("--note")

    fee_pending_accounting_repair = sub.add_parser(
        "fee-pending-accounting-repair",
        help="finalize a fee-pending observed fill with explicit operator fee evidence",
        description=(
            "Apply a broker_fill_observations fee-pending fill through normal accounting after "
            "the operator supplies explicit fee provenance."
        ),
    )
    fee_pending_accounting_repair.add_argument("--client-order-id", required=True)
    fee_pending_accounting_repair.add_argument("--fill-id")
    fee_pending_accounting_repair.add_argument("--exchange-order-id")
    fee_pending_accounting_repair.add_argument("--fee", type=float)
    fee_pending_accounting_repair.add_argument("--fee-provenance")
    fee_pending_accounting_repair.add_argument("--apply", action="store_true")
    fee_pending_accounting_repair.add_argument("--yes", action="store_true")
    fee_pending_accounting_repair.add_argument("--note")

    rebuild_position_authority = sub.add_parser(
        "rebuild-position-authority",
        help="preview or rebuild canonical lot authority from accounted BUY fill evidence",
        description=(
            "Rebuild missing lot-native position authority only from already-accounted BUY fills "
            "when no open orders, no existing lots, no SELL history, and portfolio quantity match."
        ),
    )
    rebuild_position_authority.add_argument("--full-projection-rebuild", action="store_true")
    rebuild_position_authority.add_argument("--apply", action="store_true")
    rebuild_position_authority.add_argument("--yes", action="store_true")
    rebuild_position_authority.add_argument("--note")

    external_cash_adjustment = sub.add_parser(
        "record-external-cash-adjustment",
        help="record an external cash adjustment event",
        description="Store a manual or broker-driven cash adjustment as a separate accounting event.",
    )
    external_cash_adjustment.add_argument("--event-ts", type=int, required=True)
    external_cash_adjustment.add_argument("--delta-amount", type=float, required=True)
    external_cash_adjustment.add_argument("--source", required=True)
    external_cash_adjustment.add_argument("--reason", required=True)
    external_cash_adjustment.add_argument("--broker-snapshot-basis", required=True)
    external_cash_adjustment.add_argument("--currency", default="KRW")
    external_cash_adjustment.add_argument("--correlation-metadata")
    external_cash_adjustment.add_argument("--note")
    external_cash_adjustment.add_argument("--adjustment-key")
    external_cash_adjustment.add_argument("--yes", action="store_true")

    manual_flat_accounting_repair = sub.add_parser(
        "manual-flat-accounting-repair",
        help="preview or apply a bounded manual-flat accounting repair",
        description="Record an explicit manual-flat accounting repair event after broker/manual flattening and local flat cleanup.",
    )
    manual_flat_accounting_repair.add_argument("--apply", action="store_true")
    manual_flat_accounting_repair.add_argument("--yes", action="store_true")
    manual_flat_accounting_repair.add_argument("--note")
    external_position_accounting_repair = sub.add_parser(
        "external-position-accounting-repair",
        help="preview or apply a replay-compatible external position adjustment",
        description="Record an explicit accounting adjustment after broker/offline position changes have already been reconciled into portfolio truth.",
    )
    external_position_accounting_repair.add_argument("--apply", action="store_true")
    external_position_accounting_repair.add_argument("--yes", action="store_true")
    external_position_accounting_repair.add_argument("--note")

    r = sub.add_parser("run")
    r.add_argument("--short", type=int, default=SMA_SHORT)
    r.add_argument("--long", type=int, default=SMA_LONG)

    args = p.parse_args(argv)

    try:
        validate_mode_or_raise(settings.MODE)
    except ModeValidationError as e:
        print(f"[MODE] {e}")
        raise SystemExit(1) from e
    if settings.MODE == "live":
        log_live_execution_contract(
            settings,
            caller=f"app.main:{args.cmd or 'ticker'}",
            env_summary=get_last_explicit_env_load_summary().as_dict(),
        )
    _enforce_live_command_guard(args.cmd)

    if args.cmd in (None, "ticker"):
        cmd_ticker()
    elif args.cmd == "candles":
        cmd_candles(args.limit)
    elif args.cmd == "sync":
        cmd_sync()
    elif args.cmd == "signal":
        cmd_signal(args.short, args.long)
    elif args.cmd == "explain":
        cmd_explain(args.short, args.long)
    elif args.cmd == "status":
        cmd_status()
    elif args.cmd in ("audit", "check"):
        cmd_audit()
    elif args.cmd == "health":
        cmd_health()
    elif args.cmd == "trades":
        cmd_trades(args.limit)
    elif args.cmd == "orders":
        cmd_orders(args.limit)
    elif args.cmd == "fills":
        cmd_fills(args.limit)
    elif args.cmd == "ops-report":
        cmd_ops_report(limit=max(1, int(args.limit)))
    elif args.cmd == "risk-report":
        cmd_risk_report(limit=max(1, int(args.limit)), as_json=bool(args.json))
    elif args.cmd == "decision-telemetry":
        cmd_decision_telemetry(limit=max(1, int(args.limit)))
    elif args.cmd == "fee-diagnostics":
        cmd_fee_diagnostics(
            fill_limit=max(1, int(args.fill_limit)),
            roundtrip_limit=max(1, int(args.roundtrip_limit)),
            estimated_fee_rate=args.estimated_fee_rate,
            as_json=bool(args.json),
        )
    elif args.cmd == "strategy-report":
        try:
            from_ts_ms, to_ts_ms = parse_kst_date_range_to_ts_ms(
                from_date=args.from_date,
                to_date=args.to_date,
            )
        except ValueError:
            p.error("invalid date format for --from-date/--to-date; expected YYYY-MM-DD")

        if from_ts_ms is not None and to_ts_ms is not None and from_ts_ms > to_ts_ms:
            p.error("--from-date must be earlier than or equal to --to-date")

        group_by = tuple(part.strip() for part in str(args.group_by or "").split(",") if part.strip())
        cmd_strategy_report(
            strategy_name=args.strategy_name,
            exit_rule_name=args.exit_rule_name,
            pair=args.pair,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            group_by=group_by,
            observation_window_bars=max(1, int(args.observation_window_bars)),
            min_observation_sample=max(1, int(args.min_observation_sample)),
            as_json=bool(args.json),
        )
    elif args.cmd == "experiment-report":
        try:
            from_ts_ms, to_ts_ms = parse_kst_date_range_to_ts_ms(
                from_date=args.from_date,
                to_date=args.to_date,
            )
        except ValueError:
            p.error("invalid date format for --from-date/--to-date; expected YYYY-MM-DD")
        if from_ts_ms is not None and to_ts_ms is not None and from_ts_ms > to_ts_ms:
            p.error("--from-date must be earlier than or equal to --to-date")
        cmd_experiment_report(
            strategy_name=args.strategy_name,
            pair=args.pair,
            from_ts_ms=from_ts_ms,
            to_ts_ms=to_ts_ms,
            sample_threshold=max(1, int(args.sample_threshold)),
            top_n=max(1, int(args.top_n)),
            concentration_warn_threshold=max(0.0, float(args.concentration_threshold)),
            regime_skew_warn_threshold=max(0.0, float(args.regime_skew_threshold)),
            regime_pnl_skew_warn_threshold=max(0.0, float(args.regime_pnl_skew_threshold)),
            as_json=bool(args.json),
        )
    elif args.cmd == "cash-drift-report":
        cmd_cash_drift_report(recent_limit=max(1, int(args.recent_limit)), as_json=bool(args.json))
    elif args.cmd == "fee-gap-accounting-repair":
        cmd_fee_gap_accounting_repair(
            apply=bool(args.apply),
            confirm=bool(args.yes),
            note=str(args.note) if args.note is not None else None,
        )
    elif args.cmd == "fee-pending-accounting-repair":
        cmd_fee_pending_accounting_repair(
            client_order_id=str(args.client_order_id),
            fill_id=str(args.fill_id) if args.fill_id is not None else None,
            exchange_order_id=str(args.exchange_order_id) if args.exchange_order_id is not None else None,
            fee=float(args.fee) if args.fee is not None else None,
            fee_provenance=str(args.fee_provenance) if args.fee_provenance is not None else None,
            apply=bool(args.apply),
            confirm=bool(args.yes),
            note=str(args.note) if args.note is not None else None,
        )
    elif args.cmd == "rebuild-position-authority":
        cmd_rebuild_position_authority(
            apply=bool(args.apply),
            confirm=bool(args.yes),
            note=str(args.note) if args.note is not None else None,
            full_projection_rebuild=bool(args.full_projection_rebuild),
        )
    elif args.cmd == "record-external-cash-adjustment":
        cmd_record_external_cash_adjustment(
            event_ts=int(args.event_ts),
            delta_amount=float(args.delta_amount),
            source=str(args.source),
            reason=str(args.reason),
            broker_snapshot_basis=str(args.broker_snapshot_basis),
            currency=str(args.currency),
            correlation_metadata=str(args.correlation_metadata) if args.correlation_metadata is not None else None,
            note=str(args.note) if args.note is not None else None,
            adjustment_key=str(args.adjustment_key) if args.adjustment_key is not None else None,
            yes=bool(args.yes),
        )
    elif args.cmd == "manual-flat-accounting-repair":
        cmd_manual_flat_accounting_repair(
            apply=bool(args.apply),
            confirm=bool(args.yes),
            note=str(args.note) if args.note is not None else None,
        )
    elif args.cmd == "external-position-accounting-repair":
        cmd_external_position_accounting_repair(
            apply=bool(args.apply),
            confirm=bool(args.yes),
            note=str(args.note) if args.note is not None else None,
        )
    elif args.cmd == "report":
        cmd_report(max(1, int(args.days)))
    elif args.cmd == "audit-ledger":
        cmd_audit_ledger()
    elif args.cmd == "cancel-open-orders":
        cmd_cancel_open_orders()
    elif args.cmd == "broker-diagnose":
        cmd_broker_diagnose()
    elif args.cmd == "pause":
        cmd_pause()
    elif args.cmd == "resume":
        cmd_resume(force=bool(args.force))
    elif args.cmd == "flatten-position":
        cmd_flatten_position(dry_run=bool(args.dry_run))
    elif args.cmd == "panic-stop":
        cmd_panic_stop(flatten=bool(args.flatten))
    elif args.cmd == "reconcile":
        cmd_reconcile()
    elif args.cmd == "recovery-report":
        cmd_recovery_report(as_json=bool(args.json))
    elif args.cmd == "repair-plan":
        cmd_repair_plan(as_json=bool(args.json))
    elif args.cmd == "restart-checklist":
        cmd_restart_checklist()
    elif args.cmd == "recover-order":
        cmd_recover_order(
            client_order_id=str(args.client_order_id),
            exchange_order_id=str(args.exchange_order_id),
            dry_run=bool(args.dry_run),
            confirm=bool(args.yes),
        )
    elif args.cmd == "backfill-broker-order":
        cmd_backfill_broker_order(
            exchange_order_id=str(args.exchange_order_id),
            dry_run=bool(args.dry_run),
            confirm=bool(args.yes),
        )
    elif args.cmd == "run":
        cmd_run(args.short, args.long)
    else:
        p.print_help()
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
