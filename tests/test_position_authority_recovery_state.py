from __future__ import annotations

import json

import pytest

from bithumb_bot.app import _load_recovery_report, main as app_main
from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import (
    compute_accounting_replay,
    ensure_db,
    record_broker_fill_observation,
    record_external_cash_adjustment,
    record_external_position_adjustment,
    record_position_authority_projection_publication,
    record_position_authority_repair,
    set_portfolio_breakdown,
)
from bithumb_bot.engine import (
    evaluate_restart_readiness,
    evaluate_resume_eligibility,
    evaluate_startup_safety_gate,
    get_health_status,
)
from bithumb_bot.execution_service import build_execution_decision_summary
from bithumb_bot.execution import apply_fill_and_trade, apply_fill_principal_with_pending_fee, record_order_if_missing
from bithumb_bot.external_position_repair import (
    apply_external_position_accounting_repair,
    build_external_position_accounting_repair_preview,
)
from bithumb_bot.fee_gap_repair import apply_fee_gap_accounting_repair, build_fee_gap_accounting_repair_preview
from bithumb_bot.fee_pending_repair import (
    apply_fee_pending_accounting_repair,
    build_fee_pending_accounting_repair_preview,
)
from bithumb_bot.lifecycle import rebuild_lifecycle_projections_from_trades, summarize_position_lots
from bithumb_bot.oms import set_status
from bithumb_bot.position_authority_repair import (
    _simulate_non_full_position_authority_repair,
    _replace_with_portfolio_anchored_projection,
    apply_flat_stale_lot_projection_repair,
    apply_position_authority_rebuild,
    build_flat_stale_lot_projection_repair_preview,
    build_position_authority_rebuild_preview,
)
from bithumb_bot.position_authority_state import build_position_authority_assessment
from bithumb_bot.position_authority_state import build_lot_projection_convergence
from bithumb_bot.runtime_readiness import compute_runtime_readiness_snapshot


FILL_QTY = 0.00059996
LOT_SIZE = 0.0004
PRICE = 7_050_000.0
PORTFOLIO_DIVERGENCE_BUY_QTY = 0.00059992
PORTFOLIO_DIVERGENCE_QTY = 0.00039988
LIVE_INCIDENT_PORTFOLIO_QTY = 0.00099986
LIVE_INCIDENT_STALE_DUST_QTY = 0.001788
OBSERVED_LIVE_BUY_QTY = 0.00059998
OBSERVED_LIVE_BUY_FEE = 27.91
OBSERVED_LIVE_CASH_KRW = 284_169.87556
EC2_REPRO_PRIOR_BUY_QTY = 0.00059986
EC2_REPRO_LATEST_BUY_QTY = 0.00059999
EC2_REPRO_SELL_QTY = 0.0004
EC2_REPRO_PORTFOLIO_QTY = 0.00039985
RESIDUAL_INCIDENT_PRICE = 13_000_000.0
RESIDUAL_INCIDENT_PRIOR_BUY_QTY = 0.00079982
RESIDUAL_INCIDENT_LATEST_BUY_QTY = 0.00049998
RESIDUAL_INCIDENT_SELL_QTY = 0.0004
RESIDUAL_INCIDENT_PORTFOLIO_QTY = 0.00049980


@pytest.fixture
def recovery_db(tmp_path, monkeypatch):
    original_db_path = settings.DB_PATH
    original_mode = settings.MODE
    original_pair = settings.PAIR
    original_live_min_order_qty = settings.LIVE_MIN_ORDER_QTY
    original_live_order_qty_step = settings.LIVE_ORDER_QTY_STEP
    original_live_order_max_qty_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS
    original_min_order_notional_krw = settings.MIN_ORDER_NOTIONAL_KRW
    db_path = tmp_path / "authority-recovery.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0003)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    ensure_db().close()
    runtime_state.enable_trading()
    runtime_state.set_startup_gate_reason(None)
    runtime_state.record_reconcile_result(success=True, reason_code="RECONCILE_OK", metadata={}, now_epoch_sec=0.0)
    try:
        yield db_path
    finally:
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "PAIR", original_pair)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", original_live_min_order_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_live_order_qty_step)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", original_live_order_max_qty_decimals)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", original_min_order_notional_krw)


def _record_fee_pending_buy(conn, *, client_order_id: str = "incident_buy", fill_id: str = "fill-23") -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side="BUY",
        qty_req=FILL_QTY,
        price=PRICE,
        ts_ms=1_700_000_000_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=0.0002,
        qty_step=0.0001,
        min_notional_krw=0.0,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    record_broker_fill_observation(
        conn,
        event_ts=1_700_000_000_100,
        client_order_id=client_order_id,
        exchange_order_id="ex-61",
        fill_id=fill_id,
        fill_ts=1_700_000_000_050,
        side="BUY",
        price=PRICE,
        qty=FILL_QTY,
        fee=None,
        fee_status="missing",
        accounting_status="fee_pending",
        source="test_reconcile_fee_pending",
        raw_payload={"fixture": "incident"},
    )


def _apply_fee_pending_buy(conn, *, client_order_id: str = "incident_buy", fill_id: str = "fill-23") -> None:
    _record_fee_pending_buy(conn, client_order_id=client_order_id, fill_id=fill_id)
    result = apply_fee_pending_accounting_repair(
        conn,
        client_order_id=client_order_id,
        fill_id=fill_id,
        fee=4.23,
        fee_provenance="operator_fixture",
    )
    assert result["applied_fill"] is not None
    conn.commit()


def _corrupt_latest_buy_lot_as_incident(conn, *, client_order_id: str = "incident_buy") -> None:
    trade = conn.execute(
        "SELECT id, ts FROM trades WHERE client_order_id=? AND side='BUY'",
        (client_order_id,),
    ).fetchone()
    assert trade is not None
    conn.execute("DELETE FROM open_position_lots WHERE entry_trade_id=?", (int(trade["id"]),))
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
            qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
            internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
            lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
            position_state, entry_fee_total
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            int(trade["id"]),
            client_order_id,
            "fill-23",
            int(trade["ts"]),
            PRICE,
            FILL_QTY,
            0,
            1,
            1,
            FILL_QTY,
            0.0003,
            0.0001,
            0.0,
            8,
            "ledger",
            "lot-native",
            "dust_tracking",
            4.23,
        ),
    )
    conn.commit()


def _record_historical_sell_history(conn) -> None:
    record_order_if_missing(
        conn,
        client_order_id="historical_buy",
        side="BUY",
        qty_req=LOT_SIZE,
        price=PRICE,
        ts_ms=1_699_999_000_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="historical_buy",
        side="BUY",
        fill_id="historical-buy-fill",
        fill_ts=1_699_999_000_100,
        price=PRICE,
        qty=LOT_SIZE,
        fee=1.0,
    )
    set_status("historical_buy", "FILLED", conn=conn)
    record_order_if_missing(
        conn,
        client_order_id="historical_sell",
        side="SELL",
        qty_req=LOT_SIZE,
        price=PRICE,
        ts_ms=1_699_999_100_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="historical_sell",
        side="SELL",
        fill_id="historical-sell-fill",
        fill_ts=1_699_999_100_100,
        price=PRICE,
        qty=LOT_SIZE,
        fee=1.0,
    )
    set_status("historical_sell", "FILLED", conn=conn)
    conn.commit()


def _create_portfolio_projection_divergence(conn) -> None:
    record_order_if_missing(
        conn,
        client_order_id="live_1776745440000_buy_ae9d0d6e",
        side="BUY",
        qty_req=PORTFOLIO_DIVERGENCE_BUY_QTY,
        price=PRICE,
        ts_ms=1_776_745_440_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=0.0002,
        qty_step=0.0001,
        min_notional_krw=0.0,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="live_1776745440000_buy_ae9d0d6e",
        side="BUY",
        fill_id="live-fill-1776745440000",
        fill_ts=1_776_745_440_050,
        price=PRICE,
        qty=PORTFOLIO_DIVERGENCE_BUY_QTY,
        fee=4.23,
        allow_entry_decision_fallback=False,
    )
    set_status("live_1776745440000_buy_ae9d0d6e", "FILLED", conn=conn)
    set_portfolio_breakdown(
        conn,
        cash_available=settings.START_CASH_KRW,
        cash_locked=0.0,
        asset_available=PORTFOLIO_DIVERGENCE_QTY,
        asset_locked=0.0,
    )
    conn.commit()


def _record_portfolio_projection_broker_evidence(*, broker_qty: float) -> None:
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_observed_ts_ms": 1_776_745_500_000,
            "balance_asset_ts_ms": 1_776_745_500_000,
            "balance_source": "accounts_v1_rest_snapshot",
            "balance_source_stale": False,
            "balance_source_quote_currency": "KRW",
            "balance_source_base_currency": "BTC",
            "broker_asset_qty": broker_qty,
            "broker_asset_available": broker_qty,
            "broker_asset_locked": 0.0,
            "broker_cash_available": OBSERVED_LIVE_CASH_KRW,
            "broker_cash_locked": 0.0,
            "remote_open_order_found": 0,
            "unresolved_open_order_count": 0,
            "submit_unknown_count": 0,
            "recovery_required_count": 0,
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_state": "harmless_dust",
            "dust_broker_qty": broker_qty,
            "dust_local_qty": broker_qty,
            "dust_delta_qty": 0.0,
            "dust_qty_gap_tolerance": 0.000001,
            "dust_qty_gap_small": 1,
            "dust_min_qty": LOT_SIZE,
        },
        now_epoch_sec=1.0,
    )


def _materialize_flat_stale_projection_fixture(
    conn,
    *,
    broker_qty: float = 0.0,
    portfolio_qty: float = 0.0,
    open_order_status: str | None = None,
    include_terminal_sell: bool = True,
) -> list[int]:
    _apply_filled_order(
        conn,
        client_order_id="live_1777186500000_buy_aee54bab",
        side="BUY",
        qty=0.00039982,
        ts_ms=1_777_186_500_000,
        fill_id="flat-stale-buy-fill-1",
    )
    _apply_filled_order(
        conn,
        client_order_id="live_1777248060000_buy_aee958b7",
        side="BUY",
        qty=0.00009998,
        ts_ms=1_777_248_060_000,
        fill_id="flat-stale-buy-fill-2",
    )
    if include_terminal_sell:
        _apply_filled_order(
            conn,
            client_order_id="live_1777367760000_sell_ae50365f",
            side="SELL",
            qty=0.0004998,
            ts_ms=1_777_367_760_000,
            fill_id="flat-stale-sell-fill",
        )
        conn.execute(
            """
            UPDATE orders
            SET decision_reason_code='target_delta_rebalance', final_submitted_qty=?
            WHERE client_order_id='live_1777367760000_sell_ae50365f'
            """,
            (0.0004998,),
        )
        _insert_target_delta_terminal_flat_evidence(
            conn,
            client_order_id="live_1777367760000_sell_ae50365f",
            ts_ms=1_777_367_760_000,
            submitted_qty=0.0004998,
            open_exposure_qty=0.0,
            dust_tracking_qty=0.0004998,
        )
    conn.execute("DELETE FROM open_position_lots WHERE pair=?", (settings.PAIR,))
    buy_rows = conn.execute(
        """
        SELECT id, client_order_id, ts, price
        FROM trades
        WHERE client_order_id IN (?, ?)
        ORDER BY id ASC
        """,
        ("live_1777186500000_buy_aee54bab", "live_1777248060000_buy_aee958b7"),
    ).fetchall()
    quantities = [0.00039982, 0.00009998]
    stale_ids: list[int] = []
    for idx, row in enumerate(buy_rows):
        cursor = conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                int(row["id"]),
                str(row["client_order_id"]),
                f"flat-stale-buy-fill-{idx + 1}",
                int(row["ts"]),
                float(row["price"]),
                quantities[idx],
                0,
                1,
                1,
                LOT_SIZE,
                LOT_SIZE,
                0.0001,
                0.0,
                8,
                "ledger",
                "lot-native",
                "dust_tracking",
                1.0,
            ),
        )
        stale_ids.append(int(cursor.lastrowid))
    if open_order_status:
        record_order_if_missing(
            conn,
            client_order_id="blocking_open_order",
            side="BUY",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_777_367_900_000,
            status=open_order_status,
            internal_lot_size=LOT_SIZE,
            intended_lot_count=1,
            executable_lot_count=1,
        )
    if abs(portfolio_qty) > 1e-12:
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=portfolio_qty)
    replay = compute_accounting_replay(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=float(replay.get("replay_cash") or settings.START_CASH_KRW),
        cash_locked=0.0,
        asset_available=portfolio_qty,
        asset_locked=0.0,
    )
    conn.commit()
    _record_portfolio_projection_broker_evidence(broker_qty=broker_qty)
    return stale_ids


def _insert_live_incident_stale_dust_projection(conn) -> None:
    dust_quantities = [0.000128] * 13 + [0.000124]
    assert sum(dust_quantities) == pytest.approx(LIVE_INCIDENT_STALE_DUST_QTY)
    _insert_stale_dust_projection_rows(conn, dust_quantities=dust_quantities)


def _insert_stale_dust_projection_rows(conn, *, dust_quantities: list[float]) -> None:
    for idx, qty_open in enumerate(dust_quantities):
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                50_000 + idx,
                f"stale_dust_buy_{idx}",
                f"stale-dust-fill-{idx}",
                1_699_000_000_000 + idx,
                PRICE,
                qty_open,
                0,
                1,
                1,
                qty_open,
                LOT_SIZE,
                0.0001,
                0.0,
                8,
                "legacy_projection_residue",
                "lot-native",
                "dust_tracking",
                0.0,
            ),
        )


def _align_accounting_projection_to_portfolio(conn, *, portfolio_qty: float) -> None:
    replay_qty = float(compute_accounting_replay(conn)["replay_qty"])
    delta_qty = portfolio_qty - replay_qty
    if abs(delta_qty) <= 1e-12:
        return
    record_external_position_adjustment(
        conn,
        event_ts=1_776_745_550_000,
        asset_qty_delta=delta_qty,
        cash_delta=0.0,
        source="test_projection_alignment",
        reason="historical_fragmentation_fixture_alignment",
        adjustment_basis={
            "fixture": "historical_fragmentation_projection_drift",
            "portfolio_qty": portfolio_qty,
            "replay_qty_before": replay_qty,
        },
        adjustment_key=f"historical-fragmentation-alignment:{portfolio_qty:.12f}",
    )


def _align_accounting_cash_to_portfolio(conn, *, portfolio_cash: float) -> None:
    replay_cash = float(compute_accounting_replay(conn)["replay_cash"])
    delta_cash = portfolio_cash - replay_cash
    if abs(delta_cash) <= 1e-12:
        return
    record_external_cash_adjustment(
        conn,
        event_ts=1_776_905_550_100,
        currency="KRW",
        delta_amount=delta_cash,
        source="test_projection_alignment",
        reason="historical_fragmentation_fixture_cash_alignment",
        broker_snapshot_basis={
            "fixture": "historical_fragmentation_projection_drift",
            "portfolio_cash": portfolio_cash,
            "replay_cash_before": replay_cash,
        },
        note="align accounting replay cash to observed portfolio fixture",
        adjustment_key=f"historical-fragmentation-cash-alignment:{portfolio_cash:.8f}",
    )


def _create_observed_live_projection_fragmentation_fixture(conn) -> None:
    client_order_id = "live_1776905580000_buy_ae55843c"
    fill_id = "C0101000000983482756"
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side="BUY",
        qty_req=OBSERVED_LIVE_BUY_QTY,
        price=PRICE,
        ts_ms=1_776_905_580_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=0.0002,
        qty_step=0.0001,
        min_notional_krw=0.0,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    apply_fill_and_trade(
        conn,
        client_order_id=client_order_id,
        side="BUY",
        fill_id=fill_id,
        fill_ts=1_776_905_580_050,
        price=PRICE,
        qty=OBSERVED_LIVE_BUY_QTY,
        fee=OBSERVED_LIVE_BUY_FEE,
        allow_entry_decision_fallback=False,
    )
    set_status(client_order_id, "FILLED", conn=conn)
    target_trade = conn.execute(
        "SELECT id, client_order_id FROM trades WHERE client_order_id=? AND side='BUY'",
        (client_order_id,),
    ).fetchone()
    assert target_trade is not None
    conn.execute("DELETE FROM open_position_lots WHERE entry_trade_id=?", (int(target_trade["id"]),))
    _insert_live_incident_stale_dust_projection(conn)
    _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
    _align_accounting_cash_to_portfolio(conn, portfolio_cash=OBSERVED_LIVE_CASH_KRW)
    set_portfolio_breakdown(
        conn,
        cash_available=OBSERVED_LIVE_CASH_KRW,
        cash_locked=0.0,
        asset_available=LIVE_INCIDENT_PORTFOLIO_QTY,
        asset_locked=0.0,
    )
    record_position_authority_repair(
        conn,
        event_ts=1_776_905_600_000,
        source="test_observed_live_projection_fragmentation",
        reason="portfolio_anchored_authority_projection_repair",
        repair_basis={
            "event_type": "portfolio_anchored_authority_projection_repair",
            "target_trade_id": int(target_trade["id"]),
            "target_client_order_id": str(target_trade["client_order_id"]),
            "target_remainder_qty": 0.0,
            "portfolio_qty": LIVE_INCIDENT_PORTFOLIO_QTY,
            "projected_total_qty": LIVE_INCIDENT_STALE_DUST_QTY,
            "projected_qty_excess": LIVE_INCIDENT_STALE_DUST_QTY - LIVE_INCIDENT_PORTFOLIO_QTY,
        },
        note="historical repair evidence without current-state publication",
    )
    conn.commit()
    _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)


def _set_portfolio_asset_qty_preserving_cash(conn, *, asset_qty: float) -> None:
    row = conn.execute(
        """
        SELECT cash_available, cash_locked
        FROM portfolio
        WHERE id=1
        """
    ).fetchone()
    cash_available = float(row["cash_available"] or 0.0) if row is not None else settings.START_CASH_KRW
    cash_locked = float(row["cash_locked"] or 0.0) if row is not None else 0.0
    set_portfolio_breakdown(
        conn,
        cash_available=cash_available,
        cash_locked=cash_locked,
        asset_available=asset_qty,
        asset_locked=0.0,
    )


def _replace_with_tracked_dust_row(
    conn,
    *,
    residual_qty: float,
    min_qty: float = 0.0002,
    client_order_id: str = "tracked_dust_buy",
) -> None:
    conn.execute("DELETE FROM open_position_lots")
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
            qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
            internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
            lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
            position_state, entry_fee_total
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            999,
            client_order_id,
            "tracked-dust-fill",
            1_700_000_000_000,
            PRICE,
            residual_qty,
            0,
            1,
            1,
            LOT_SIZE,
            min_qty,
            0.0001,
            0.0,
            8,
            "ledger",
            "lot-native",
            "dust_tracking",
            0.0,
        ),
    )
    set_portfolio_breakdown(
        conn,
        cash_available=settings.START_CASH_KRW,
        cash_locked=0.0,
        asset_available=residual_qty,
        asset_locked=0.0,
    )
    conn.commit()


def _replace_with_tracked_dust_rows(
    conn,
    *,
    residual_qty: float,
    row_count: int = 2,
    client_order_id_prefix: str = "tracked_dust_buy",
) -> None:
    conn.execute("DELETE FROM open_position_lots")
    per_row_qty = float(residual_qty) / float(row_count)
    for idx in range(row_count):
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                10_000 + idx,
                f"{client_order_id_prefix}_{idx}",
                f"tracked-dust-fill-{idx}",
                1_700_000_000_000 + idx,
                PRICE,
                per_row_qty,
                0,
                1,
                1,
                LOT_SIZE,
                0.0002,
                0.0001,
                0.0,
                8,
                "ledger",
                "lot-native",
                "dust_tracking",
                0.0,
            ),
        )
    set_portfolio_breakdown(
        conn,
        cash_available=settings.START_CASH_KRW,
        cash_locked=0.0,
        asset_available=residual_qty,
        asset_locked=0.0,
    )
    conn.commit()


def _record_consistent_residue_reconcile_metadata(residual_qty: float) -> None:
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 0,
            "dust_state": "no_dust",
            "dust_policy_reason": "no_dust_residual",
            "dust_broker_qty": float(residual_qty),
            "dust_local_qty": float(residual_qty),
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0002,
            "dust_min_notional_krw": 0.0,
            "dust_broker_qty_is_dust": 0,
            "dust_local_qty_is_dust": 0,
            "dust_qty_gap_small": 1,
        },
        now_epoch_sec=1_700_000_010.0,
    )


def _record_formal_broker_flat_reconcile_metadata() -> None:
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_source": "test_balance_snapshot",
            "balance_observed_ts_ms": 1_777_428_900_000,
            "balance_asset_ts_ms": 1_777_428_900_000,
            "balance_source_base_currency": "BTC",
            "balance_source_quote_currency": "KRW",
            "broker_asset_qty": 0.0,
            "broker_asset_available": 0.0,
            "broker_asset_locked": 0.0,
            "broker_cash_available": 398_728.0,
            "broker_cash_locked": 0.0,
            "remote_open_order_found": 0,
        },
        now_epoch_sec=1_777_428_900.0,
    )


def _insert_target_delta_terminal_flat_evidence(
    conn,
    *,
    client_order_id: str,
    ts_ms: int,
    submitted_qty: float,
    open_exposure_qty: float,
    dust_tracking_qty: float,
) -> None:
    evidence = {
        "source": "target_delta",
        "authority": "target_position_delta",
        "submit_qty_source": "target_position_delta",
        "sell_qty_basis_source": "target_position_delta",
        "decision_reason_code": "target_delta_rebalance",
        "final_submitted_qty": submitted_qty,
        "order_qty": submitted_qty,
        "normalized_qty": submitted_qty,
        "sell_open_exposure_qty": open_exposure_qty,
        "sell_dust_tracking_qty": dust_tracking_qty,
        "raw_total_asset_qty": submitted_qty,
        "observed_position_qty": submitted_qty,
    }
    conn.execute(
        """
        INSERT INTO order_events(
            client_order_id, event_type, event_ts, order_status, qty, side,
            submit_evidence, final_submitted_qty, decision_reason_code, submission_reason_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            client_order_id,
            "submit_attempt_recorded",
            int(ts_ms),
            "FILLED",
            float(submitted_qty),
            "SELL",
            json.dumps(evidence, sort_keys=True),
            float(submitted_qty),
            "target_delta_rebalance",
            "target_delta_rebalance",
        ),
    )


def _create_terminal_flat_stale_dust_incident(conn) -> None:
    base_ts = 1_777_428_420_000
    buy_qty = 0.00059997
    price = 91_040_000.0
    record_order_if_missing(
        conn,
        client_order_id="live_1777428420000_buy_ae5d6ffb",
        side="BUY",
        qty_req=buy_qty,
        price=price,
        ts_ms=base_ts,
        status="FILLED",
        internal_lot_size=0.0004,
        effective_min_trade_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="live_1777428420000_buy_ae5d6ffb",
        side="BUY",
        fill_id="C0101000000984353580",
        fill_ts=base_ts,
        price=price,
        qty=buy_qty,
        fee=27.31,
    )
    record_order_if_missing(
        conn,
        client_order_id="live_1777428840000_sell_ae17f61f",
        side="SELL",
        qty_req=buy_qty,
        price=price,
        ts_ms=base_ts + 420_000,
        status="FILLED",
        internal_lot_size=0.0004,
        effective_min_trade_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5000.0,
        intended_lot_count=1,
        executable_lot_count=1,
        final_submitted_qty=buy_qty,
        decision_reason_code="signal_exit",
    )
    apply_fill_and_trade(
        conn,
        client_order_id="live_1777428840000_sell_ae17f61f",
        side="SELL",
        fill_id="C0101000000984353581",
        fill_ts=base_ts + 420_000,
        price=price,
        qty=buy_qty,
        fee=27.31,
        exit_reason="signal_exit",
    )
    conn.execute(
        """
        UPDATE orders
        SET decision_reason_code='target_delta_rebalance', final_submitted_qty=?
        WHERE client_order_id='live_1777428840000_sell_ae17f61f'
        """,
        (buy_qty,),
    )
    _insert_target_delta_terminal_flat_evidence(
        conn,
        client_order_id="live_1777428840000_sell_ae17f61f",
        ts_ms=base_ts + 420_000,
        submitted_qty=buy_qty,
        open_exposure_qty=0.0004,
        dust_tracking_qty=0.00019997,
    )
    conn.commit()
    _record_formal_broker_flat_reconcile_metadata()


def _apply_fee_pending_sell(conn, *, client_order_id: str = "incident_sell", fill_id: str = "sell-fill-9") -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side="SELL",
        qty_req=LOT_SIZE,
        price=PRICE,
        ts_ms=1_700_000_100_000,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=0.0002,
        qty_step=0.0001,
        min_notional_krw=0.0,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    record_broker_fill_observation(
        conn,
        event_ts=1_700_000_100_100,
        client_order_id=client_order_id,
        exchange_order_id="ex-sell-71",
        fill_id=fill_id,
        fill_ts=1_700_000_100_050,
        side="SELL",
        price=PRICE,
        qty=LOT_SIZE,
        fee=None,
        fee_status="missing",
        accounting_status="fee_pending",
        source="test_reconcile_fee_pending",
        raw_payload={"fixture": "incident-sell"},
    )
    result = apply_fee_pending_accounting_repair(
        conn,
        client_order_id=client_order_id,
        fill_id=fill_id,
        fee=17.73,
        fee_provenance="operator_fixture",
    )
    assert result["applied_fill"] is not None
    conn.commit()


def _apply_filled_order(
    conn,
    *,
    client_order_id: str,
    side: str,
    qty: float,
    ts_ms: int,
    fill_id: str,
    fee: float = 1.0,
    price: float = PRICE,
    min_notional_krw: float = 0.0,
) -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side=side,
        qty_req=qty,
        price=price,
        ts_ms=ts_ms,
        status="NEW",
        internal_lot_size=LOT_SIZE,
        effective_min_trade_qty=0.0002,
        qty_step=0.0001,
        min_notional_krw=min_notional_krw,
        intended_lot_count=1,
        executable_lot_count=1,
    )
    apply_fill_and_trade(
        conn,
        client_order_id=client_order_id,
        side=side,
        fill_id=fill_id,
        fill_ts=ts_ms + 50,
        price=price,
        qty=qty,
        fee=fee,
        allow_entry_decision_fallback=False,
    )
    set_status(client_order_id, "FILLED", conn=conn)


def _materialize_full_rebuild_portfolio_anchor_fixture(
    conn,
    *,
    with_publication: bool,
) -> dict[str, object]:
    _apply_filled_order(
        conn,
        client_order_id="ec2_prior_buy",
        side="BUY",
        qty=EC2_REPRO_PRIOR_BUY_QTY,
        ts_ms=1_777_042_400_000,
        fill_id="ec2-prior-buy-fill",
    )
    _apply_filled_order(
        conn,
        client_order_id="ec2_prior_sell",
        side="SELL",
        qty=EC2_REPRO_SELL_QTY,
        ts_ms=1_777_042_410_000,
        fill_id="ec2-prior-sell-fill",
    )
    _apply_filled_order(
        conn,
        client_order_id="ec2_latest_buy",
        side="BUY",
        qty=EC2_REPRO_LATEST_BUY_QTY,
        ts_ms=1_777_042_500_000,
        fill_id="ec2-latest-buy-fill",
    )
    _apply_filled_order(
        conn,
        client_order_id="ec2_latest_sell",
        side="SELL",
        qty=EC2_REPRO_SELL_QTY,
        ts_ms=1_777_042_510_000,
        fill_id="ec2-latest-sell-fill",
    )
    set_portfolio_breakdown(
        conn,
        cash_available=settings.START_CASH_KRW,
        cash_locked=0.0,
        asset_available=EC2_REPRO_PORTFOLIO_QTY,
        asset_locked=0.0,
    )
    _align_accounting_projection_to_portfolio(conn, portfolio_qty=EC2_REPRO_PORTFOLIO_QTY)
    rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR, allow_entry_decision_fallback=False)
    anchor = _replace_with_portfolio_anchored_projection(
        conn,
        portfolio_qty=EC2_REPRO_PORTFOLIO_QTY,
        broker_qty=EC2_REPRO_PORTFOLIO_QTY,
    )
    if with_publication:
        record_position_authority_projection_publication(
            conn,
            event_ts=1_777_042_520_000,
            pair=settings.PAIR,
            target_trade_id=int(anchor["anchor_trade_id"]),
            source="test_full_projection_rebuild_publish",
            publish_basis={
                "event_type": "full_projection_materialized_rebuild",
                "target_trade_id": int(anchor["anchor_trade_id"]),
                "portfolio_qty": EC2_REPRO_PORTFOLIO_QTY,
                "portfolio_anchor_projection": anchor,
            },
            note="portfolio-anchor attestation fixture",
        )
    conn.commit()
    _record_portfolio_projection_broker_evidence(broker_qty=EC2_REPRO_PORTFOLIO_QTY)
    return anchor


def _materialize_broker_matched_residual_only_fixture(conn) -> dict[str, object]:
    _apply_filled_order(
        conn,
        client_order_id="residual_prior_buy",
        side="BUY",
        qty=RESIDUAL_INCIDENT_PRIOR_BUY_QTY,
        ts_ms=1_777_142_400_000,
        fill_id="residual-prior-buy-fill",
        price=RESIDUAL_INCIDENT_PRICE,
        min_notional_krw=5_000.0,
    )
    _apply_filled_order(
        conn,
        client_order_id="residual_prior_sell",
        side="SELL",
        qty=RESIDUAL_INCIDENT_SELL_QTY,
        ts_ms=1_777_142_410_000,
        fill_id="residual-prior-sell-fill",
        price=RESIDUAL_INCIDENT_PRICE,
        min_notional_krw=5_000.0,
    )
    _apply_filled_order(
        conn,
        client_order_id="residual_latest_buy",
        side="BUY",
        qty=RESIDUAL_INCIDENT_LATEST_BUY_QTY,
        ts_ms=1_777_142_500_000,
        fill_id="residual-latest-buy-fill",
        price=RESIDUAL_INCIDENT_PRICE,
        min_notional_krw=5_000.0,
    )
    _apply_filled_order(
        conn,
        client_order_id="residual_latest_sell",
        side="SELL",
        qty=RESIDUAL_INCIDENT_SELL_QTY,
        ts_ms=1_777_142_510_000,
        fill_id="residual-latest-sell-fill",
        price=RESIDUAL_INCIDENT_PRICE,
        min_notional_krw=5_000.0,
    )
    set_portfolio_breakdown(
        conn,
        cash_available=341_778.0,
        cash_locked=0.0,
        asset_available=RESIDUAL_INCIDENT_PORTFOLIO_QTY,
        asset_locked=0.0,
    )
    _align_accounting_projection_to_portfolio(conn, portfolio_qty=RESIDUAL_INCIDENT_PORTFOLIO_QTY)
    rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR, allow_entry_decision_fallback=False)

    prior_buy_trade = conn.execute(
        "SELECT id FROM trades WHERE client_order_id='residual_prior_buy' AND side='BUY'"
    ).fetchone()
    latest_buy_trade = conn.execute(
        "SELECT id FROM trades WHERE client_order_id='residual_latest_buy' AND side='BUY'"
    ).fetchone()
    assert prior_buy_trade is not None
    assert latest_buy_trade is not None
    conn.execute(
        """
        UPDATE open_position_lots
        SET
            position_state='dust_tracking',
            executable_lot_count=0,
            dust_tracking_lot_count=1,
            internal_lot_size=?,
            lot_min_qty=?,
            lot_qty_step=?,
            lot_min_notional_krw=?,
            lot_max_qty_decimals=?,
            lot_rule_source_mode='full_projection_rebuild_portfolio_anchor',
            entry_decision_linkage='degraded_recovery_unattributed'
        WHERE entry_trade_id=?
        """,
        (LOT_SIZE, 0.0002, 0.0001, 5_000.0, 8, int(prior_buy_trade["id"])),
    )
    conn.execute(
        """
        UPDATE open_position_lots
        SET
            position_state='dust_tracking',
            executable_lot_count=0,
            dust_tracking_lot_count=1,
            internal_lot_size=?,
            lot_min_qty=?,
            lot_qty_step=?,
            lot_min_notional_krw=?,
            lot_max_qty_decimals=?,
            lot_rule_source_mode='ledger',
            entry_decision_linkage='direct'
        WHERE entry_trade_id=?
        """,
        (LOT_SIZE, 0.0002, 0.0001, 5_000.0, 8, int(latest_buy_trade["id"])),
    )
    record_position_authority_projection_publication(
        conn,
        event_ts=1_777_142_520_000,
        pair=settings.PAIR,
        target_trade_id=int(prior_buy_trade["id"]),
        source="test_residual_only_publication",
        publish_basis={
            "event_type": "full_projection_materialized_rebuild",
            "target_trade_id": int(prior_buy_trade["id"]),
            "portfolio_qty": RESIDUAL_INCIDENT_PORTFOLIO_QTY,
        },
        note="residual-only current-state attestation fixture",
    )
    conn.commit()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_observed_ts_ms": 1_777_142_530_000,
            "balance_asset_ts_ms": 1_777_142_530_000,
            "balance_source": "accounts_v1_rest_snapshot",
            "balance_source_stale": False,
            "balance_source_quote_currency": "KRW",
            "balance_source_base_currency": "BTC",
            "broker_asset_qty": RESIDUAL_INCIDENT_PORTFOLIO_QTY,
            "broker_asset_available": RESIDUAL_INCIDENT_PORTFOLIO_QTY,
            "broker_asset_locked": 0.0,
            "broker_cash_available": 341_778.0,
            "broker_cash_locked": 0.0,
            "remote_open_order_found": 0,
            "unresolved_open_order_count": 0,
            "submit_unknown_count": 0,
            "recovery_required_count": 0,
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_state": "harmless_dust",
            "dust_broker_qty": RESIDUAL_INCIDENT_PORTFOLIO_QTY,
            "dust_local_qty": RESIDUAL_INCIDENT_PORTFOLIO_QTY,
            "dust_delta_qty": 0.0,
            "dust_qty_gap_tolerance": 0.000001,
            "dust_qty_gap_small": 1,
            "dust_min_qty": 0.0002,
            "dust_min_notional_krw": 5_000.0,
        },
        now_epoch_sec=1.0,
    )
    return {
        "prior_buy_trade_id": int(prior_buy_trade["id"]),
        "latest_buy_trade_id": int(latest_buy_trade["id"]),
    }


def test_fee_pending_repaired_buy_materializes_consistent_lot_authority(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size
            FROM open_position_lots
            ORDER BY id ASC
            """
        ).fetchall()
        fill = conn.execute("SELECT intended_lot_count, executable_lot_count, internal_lot_size FROM fills").fetchone()
        order = conn.execute("SELECT status, qty_filled, executable_lot_count, internal_lot_size FROM orders").fetchone()
        summary = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    assert fill["executable_lot_count"] == 1
    assert fill["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert order["status"] == "FILLED"
    assert order["qty_filled"] == pytest.approx(FILL_QTY)
    assert rows[0]["position_state"] == "open_exposure"
    assert rows[0]["qty_open"] == pytest.approx(LOT_SIZE)
    assert rows[0]["executable_lot_count"] == 1
    assert rows[1]["position_state"] == "dust_tracking"
    assert rows[1]["qty_open"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert summary.open_lot_count == 1
    assert summary.dust_tracking_lot_count == 1


def test_incident_residual_is_created_at_buy_ingestion_then_left_by_sell_matching(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        after_buy_rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size
            FROM open_position_lots
            WHERE entry_client_order_id='incident_buy'
            ORDER BY id ASC
            """
        ).fetchall()
        after_buy = compute_runtime_readiness_snapshot(conn)

        _apply_fee_pending_sell(conn)
        after_sell_rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count,
                   internal_lot_size, lot_min_qty, lot_qty_step
            FROM open_position_lots
            WHERE entry_client_order_id='incident_buy'
            ORDER BY id ASC
            """
        ).fetchall()
        assessment = build_position_authority_assessment(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        replay = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert len(after_buy_rows) == 2
    assert after_buy_rows[0]["position_state"] == "open_exposure"
    assert after_buy_rows[0]["qty_open"] == pytest.approx(LOT_SIZE)
    assert after_buy_rows[0]["executable_lot_count"] == 1
    assert after_buy_rows[1]["position_state"] == "dust_tracking"
    assert after_buy_rows[1]["qty_open"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert after_buy_rows[1]["dust_tracking_lot_count"] == 1
    assert after_buy.canonical_state == "OPEN_EXECUTABLE"

    assert len(after_sell_rows) == 1
    assert after_sell_rows[0]["position_state"] == "dust_tracking"
    assert after_sell_rows[0]["qty_open"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert after_sell_rows[0]["executable_lot_count"] == 0
    assert after_sell_rows[0]["dust_tracking_lot_count"] == 1
    assert after_sell_rows[0]["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert after_sell_rows[0]["lot_min_qty"] == pytest.approx(0.0002)
    assert after_sell_rows[0]["lot_qty_step"] == pytest.approx(0.0001)
    assert assessment["partial_close_residual_candidate"] is True
    assert assessment["residual_state_converged"] is True
    assert assessment["needs_residual_normalization"] is False
    assert assessment["residual_repair_event_present"] is False
    assert readiness.recovery_stage == "RESUME_READY"
    assert readiness.canonical_state == "DUST_ONLY_TRACKED"
    assert readiness.residual_class == "HARMLESS_DUST_TREAT_AS_FLAT"
    assert readiness.run_loop_allowed is True
    assert readiness.new_entry_allowed is True
    assert readiness.closeout_allowed is False
    assert readiness.operator_action_required is False
    assert (
        readiness.position_state.normalized_exposure.dust_operability_state
        == "below_internal_lot_boundary_tracked_residue_entry_allowed"
    )
    assert readiness.as_dict()["run_loop_scope"] == "process_resume_only"
    assert readiness.as_dict()["trading_permission_scope"] == "new_entry_or_position_management"
    assert readiness.as_dict()["trading_allowed"] is True
    assert readiness.as_dict()["trading_block_reason"] == "closeout_blocked:dust_only_remainder"
    assert readiness.as_dict() == replay.as_dict()


def test_partial_close_residual_uses_target_lifecycle_matched_qty_when_sell_closes_multiple_entries(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    buy_b_qty = 0.00059998
    sell_qty = 0.0008
    try:
        record_order_if_missing(
            conn,
            client_order_id="multi_buy_a",
            side="BUY",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_000_000_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="multi_buy_a",
            side="BUY",
            fill_id="multi-buy-a-fill",
            fill_ts=1_700_000_000_050,
            price=PRICE,
            qty=LOT_SIZE,
            fee=1.0,
            allow_entry_decision_fallback=False,
        )
        set_status("multi_buy_a", "FILLED", conn=conn)

        record_order_if_missing(
            conn,
            client_order_id="multi_buy_b",
            side="BUY",
            qty_req=buy_b_qty,
            price=PRICE,
            ts_ms=1_700_000_100_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="multi_buy_b",
            side="BUY",
            fill_id="multi-buy-b-fill",
            fill_ts=1_700_000_100_050,
            price=PRICE,
            qty=buy_b_qty,
            fee=1.0,
            allow_entry_decision_fallback=False,
        )
        set_status("multi_buy_b", "FILLED", conn=conn)

        record_order_if_missing(
            conn,
            client_order_id="multi_sell_ab",
            side="SELL",
            qty_req=sell_qty,
            price=PRICE,
            ts_ms=1_700_000_200_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=2,
            executable_lot_count=2,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="multi_sell_ab",
            side="SELL",
            fill_id="multi-sell-ab-fill",
            fill_ts=1_700_000_200_050,
            price=PRICE,
            qty=sell_qty,
            fee=1.0,
        )
        set_status("multi_sell_ab", "FILLED", conn=conn)
        conn.commit()

        lifecycles = [
            dict(row)
            for row in conn.execute(
                """
                SELECT entry_client_order_id, exit_client_order_id, matched_qty
                FROM trade_lifecycles
                WHERE exit_client_order_id='multi_sell_ab'
                ORDER BY id ASC
                """
            ).fetchall()
        ]
        assessment = build_position_authority_assessment(conn)
        preview = build_position_authority_rebuild_preview(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert lifecycles == [
        {
            "entry_client_order_id": "multi_buy_a",
            "exit_client_order_id": "multi_sell_ab",
            "matched_qty": pytest.approx(LOT_SIZE),
        },
        {
            "entry_client_order_id": "multi_buy_b",
            "exit_client_order_id": "multi_sell_ab",
            "matched_qty": pytest.approx(LOT_SIZE),
        },
    ]
    assert assessment["sell_after_target_buy_qty"] == pytest.approx(sell_qty)
    assert assessment["target_lifecycle_matched_qty"] == pytest.approx(LOT_SIZE)
    assert assessment["lifecycle_matched_qty_accepted"] is True
    assert assessment["lifecycle_matched_qty_acceptance_reason"] == "matched_qty_from_trade_lifecycles"
    assert assessment["effective_closed_qty"] == pytest.approx(LOT_SIZE)
    assert assessment["expected_residual_qty"] == pytest.approx(buy_b_qty - LOT_SIZE)
    assert assessment["partial_close_residual_candidate"] is True
    assert assessment["residual_state_converged"] is True
    assert assessment["needs_residual_normalization"] is False
    assert assessment["needs_correction"] is False
    assert preview["repair_mode"] == "rebuild"
    assert preview["safe_to_apply"] is False
    assert preview["target_lifecycle_matched_qty"] == pytest.approx(LOT_SIZE)
    assert preview["effective_closed_qty"] == pytest.approx(LOT_SIZE)
    assert readiness.recovery_stage == "RESUME_READY"
    assert readiness.resume_ready is True
    assert readiness.resume_blockers == ()
    assert readiness.run_loop_allowed is True
    assert readiness.new_entry_allowed is True


def test_production_like_partial_close_residual_delta_with_publication_is_resume_ready(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    buy_b_qty = 0.00059998
    sell_qty = 0.0008
    materialized_residual_qty = 0.00019986
    expected_residual_qty = buy_b_qty - LOT_SIZE
    try:
        record_order_if_missing(
            conn,
            client_order_id="prod_like_buy_a",
            side="BUY",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_000_000_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="prod_like_buy_a",
            side="BUY",
            fill_id="prod-like-buy-a-fill",
            fill_ts=1_700_000_000_050,
            price=PRICE,
            qty=LOT_SIZE,
            fee=1.0,
            allow_entry_decision_fallback=False,
        )
        set_status("prod_like_buy_a", "FILLED", conn=conn)

        record_order_if_missing(
            conn,
            client_order_id="prod_like_buy_b",
            side="BUY",
            qty_req=buy_b_qty,
            price=PRICE,
            ts_ms=1_700_000_100_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="prod_like_buy_b",
            side="BUY",
            fill_id="prod-like-buy-b-fill",
            fill_ts=1_700_000_100_050,
            price=PRICE,
            qty=buy_b_qty,
            fee=1.0,
            allow_entry_decision_fallback=False,
        )
        set_status("prod_like_buy_b", "FILLED", conn=conn)

        record_order_if_missing(
            conn,
            client_order_id="prod_like_sell_ab",
            side="SELL",
            qty_req=sell_qty,
            price=PRICE,
            ts_ms=1_700_000_200_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=2,
            executable_lot_count=2,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="prod_like_sell_ab",
            side="SELL",
            fill_id="prod-like-sell-ab-fill",
            fill_ts=1_700_000_200_050,
            price=PRICE,
            qty=sell_qty,
            fee=37.11,
        )
        set_status("prod_like_sell_ab", "FILLED", conn=conn)
        conn.commit()

        target_trade = conn.execute(
            "SELECT id, ts FROM trades WHERE client_order_id='prod_like_buy_b' AND side='BUY'"
        ).fetchone()
        assert target_trade is not None
        conn.execute("DELETE FROM open_position_lots WHERE entry_trade_id=?", (int(target_trade["id"]),))
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                int(target_trade["id"]),
                "prod_like_buy_b",
                "prod-like-buy-b-fill",
                int(target_trade["ts"]),
                PRICE,
                materialized_residual_qty,
                0,
                1,
                1,
                LOT_SIZE,
                0.0002,
                0.0001,
                0.0,
                8,
                "ledger",
                "lot-native",
                "dust_tracking",
                1.0,
            ),
        )
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=materialized_residual_qty)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=materialized_residual_qty)
        record_position_authority_projection_publication(
            conn,
            event_ts=1_700_000_210_000,
            pair=settings.PAIR,
            target_trade_id=int(target_trade["id"]),
            source="test_production_like_partial_close_publication",
            publish_basis={
                "target_trade_id": int(target_trade["id"]),
                "portfolio_qty": materialized_residual_qty,
                "target_remainder_qty": materialized_residual_qty,
                "expected_residual_qty": expected_residual_qty,
                "portfolio_anchor_projection": {
                    "anchor_trade_id": int(target_trade["id"]),
                    "portfolio_qty": materialized_residual_qty,
                },
            },
            note="production-like partial close residual publication",
        )
        conn.commit()
        _record_consistent_residue_reconcile_metadata(materialized_residual_qty)

        assessment = build_position_authority_assessment(conn)
        preview = build_position_authority_rebuild_preview(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert assessment["sell_after_target_buy_qty"] == pytest.approx(sell_qty)
    assert assessment["target_lifecycle_matched_qty"] == pytest.approx(LOT_SIZE)
    assert assessment["lifecycle_matched_qty_accepted"] is True
    assert assessment["effective_closed_qty"] == pytest.approx(LOT_SIZE)
    assert assessment["expected_residual_qty"] == pytest.approx(expected_residual_qty)
    assert assessment["projection_convergence"]["converged"] is True
    assert assessment["portfolio_projection_publication_present"] is True
    assert assessment["sell_after_qty_authority_mode"] == "diagnostic_only"
    assert assessment["target_residual_qty_delta"] == pytest.approx(
        materialized_residual_qty - expected_residual_qty
    )
    assert assessment["residual_qty_tolerance"] > 0.0
    assert assessment["partial_close_residual_candidate"] is True
    assert assessment["residual_state_converged"] is True
    assert assessment["needs_residual_normalization"] is False
    assert assessment["needs_correction"] is False
    assert preview["repair_mode"] == "rebuild"
    assert preview["safe_to_apply"] is False
    assert preview["sell_after_qty_authority_mode"] == "diagnostic_only"
    assert preview["target_residual_qty_delta"] == pytest.approx(
        materialized_residual_qty - expected_residual_qty
    )
    assert preview["residual_qty_tolerance"] == pytest.approx(assessment["residual_qty_tolerance"])
    assert readiness.recovery_stage == "RESUME_READY"
    assert readiness.resume_ready is True
    assert readiness.resume_blockers == ()
    assert readiness.run_loop_allowed is True
    assert readiness.new_entry_allowed is True
    assert readiness.closeout_allowed is False


def test_partial_close_residual_large_delta_remains_correction_blocked(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    buy_b_qty = 0.00059998
    sell_qty = 0.0008
    materialized_residual_qty = 0.00005
    expected_residual_qty = buy_b_qty - LOT_SIZE
    try:
        record_order_if_missing(
            conn,
            client_order_id="unsafe_delta_buy_a",
            side="BUY",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_000_000_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="unsafe_delta_buy_a",
            side="BUY",
            fill_id="unsafe-delta-buy-a-fill",
            fill_ts=1_700_000_000_050,
            price=PRICE,
            qty=LOT_SIZE,
            fee=1.0,
            allow_entry_decision_fallback=False,
        )
        set_status("unsafe_delta_buy_a", "FILLED", conn=conn)

        record_order_if_missing(
            conn,
            client_order_id="unsafe_delta_buy_b",
            side="BUY",
            qty_req=buy_b_qty,
            price=PRICE,
            ts_ms=1_700_000_100_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="unsafe_delta_buy_b",
            side="BUY",
            fill_id="unsafe-delta-buy-b-fill",
            fill_ts=1_700_000_100_050,
            price=PRICE,
            qty=buy_b_qty,
            fee=1.0,
            allow_entry_decision_fallback=False,
        )
        set_status("unsafe_delta_buy_b", "FILLED", conn=conn)

        record_order_if_missing(
            conn,
            client_order_id="unsafe_delta_sell_ab",
            side="SELL",
            qty_req=sell_qty,
            price=PRICE,
            ts_ms=1_700_000_200_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=2,
            executable_lot_count=2,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="unsafe_delta_sell_ab",
            side="SELL",
            fill_id="unsafe-delta-sell-ab-fill",
            fill_ts=1_700_000_200_050,
            price=PRICE,
            qty=sell_qty,
            fee=37.11,
        )
        set_status("unsafe_delta_sell_ab", "FILLED", conn=conn)
        conn.commit()

        target_trade = conn.execute(
            "SELECT id, ts FROM trades WHERE client_order_id='unsafe_delta_buy_b' AND side='BUY'"
        ).fetchone()
        assert target_trade is not None
        conn.execute("DELETE FROM open_position_lots WHERE entry_trade_id=?", (int(target_trade["id"]),))
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                int(target_trade["id"]),
                "unsafe_delta_buy_b",
                "unsafe-delta-buy-b-fill",
                int(target_trade["ts"]),
                PRICE,
                materialized_residual_qty,
                0,
                1,
                1,
                LOT_SIZE,
                0.0002,
                0.0001,
                0.0,
                8,
                "ledger",
                "lot-native",
                "dust_tracking",
                1.0,
            ),
        )
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=materialized_residual_qty)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=materialized_residual_qty)
        record_position_authority_projection_publication(
            conn,
            event_ts=1_700_000_210_000,
            pair=settings.PAIR,
            target_trade_id=int(target_trade["id"]),
            source="test_partial_close_large_delta_publication",
            publish_basis={
                "target_trade_id": int(target_trade["id"]),
                "portfolio_qty": materialized_residual_qty,
                "target_remainder_qty": materialized_residual_qty,
                "expected_residual_qty": expected_residual_qty,
            },
            note="large residual delta remains unsafe",
        )
        conn.commit()
        _record_consistent_residue_reconcile_metadata(materialized_residual_qty)

        assessment = build_position_authority_assessment(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert assessment["lifecycle_matched_qty_accepted"] is True
    assert assessment["sell_after_qty_authority_mode"] == "diagnostic_only"
    assert assessment["target_residual_qty_delta"] == pytest.approx(
        materialized_residual_qty - expected_residual_qty
    )
    assert abs(assessment["target_residual_qty_delta"]) > assessment["residual_qty_tolerance"]
    assert assessment["partial_close_residual_candidate"] is False
    assert assessment["residual_state_converged"] is False
    assert assessment["needs_residual_normalization"] is False
    assert assessment["needs_correction"] is True
    assert readiness.recovery_stage == "AUTHORITY_CORRECTION_PENDING"
    assert readiness.resume_ready is False
    assert readiness.resume_blockers == ("POSITION_AUTHORITY_CORRECTION_REQUIRED",)


def test_sub_min_tracked_dust_paths_converge_to_entry_allowed_operability(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        _apply_fee_pending_sell(conn)
        lifecycle_readiness = compute_runtime_readiness_snapshot(conn)

        _replace_with_tracked_dust_row(
            conn,
            residual_qty=FILL_QTY - LOT_SIZE,
            client_order_id="manual-equivalent-dust",
        )
        equivalent_readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    for readiness in (lifecycle_readiness, equivalent_readiness):
        assert readiness.canonical_state == "DUST_ONLY_TRACKED"
        assert readiness.residual_class == "HARMLESS_DUST_TREAT_AS_FLAT"
        assert readiness.run_loop_allowed is True
        assert readiness.new_entry_allowed is True
        assert readiness.closeout_allowed is False
        assert readiness.execution_flat is True
        assert readiness.accounting_flat is False
        assert readiness.position_state.normalized_exposure.sellable_executable_lot_count == 0
        assert readiness.position_state.normalized_exposure.dust_operability_state == (
            "below_internal_lot_boundary_tracked_residue_entry_allowed"
        )


def test_dust_only_snapshot_preserves_effective_min_trade_qty_from_authoritative_lot_metadata(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _replace_with_tracked_dust_row(conn, residual_qty=0.00039988)
        summary = summarize_position_lots(conn, pair=settings.PAIR)
    finally:
        conn.close()

    assert summary.open_lot_count == 0
    assert summary.dust_tracking_lot_count == 1
    assert summary.effective_min_trade_qty == pytest.approx(0.0002)
    assert summary.exit_non_executable_reason == "dust_only_remainder"
    assert summary.lot_definition is not None
    assert summary.lot_definition.min_qty == pytest.approx(0.0002)
    assert summary.lot_definition.qty_step == pytest.approx(0.0001)


def test_dust_only_snapshot_recovers_lot_definition_from_accounted_buy_evidence_when_lot_rows_are_sparse(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        _apply_fee_pending_sell(conn)
        conn.execute(
            """
            UPDATE open_position_lots
            SET lot_semantic_version=NULL,
                internal_lot_size=NULL,
                lot_min_qty=NULL,
                lot_qty_step=NULL,
                lot_min_notional_krw=NULL,
                lot_max_qty_decimals=NULL,
                lot_rule_source_mode=NULL
            WHERE position_state='dust_tracking'
            """
        )
        conn.commit()

        summary = summarize_position_lots(conn, pair=settings.PAIR)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert summary.lot_definition is not None
    assert summary.lot_definition.source_mode == "accounted_buy_evidence"
    assert summary.lot_definition.internal_lot_size == pytest.approx(LOT_SIZE)
    assert summary.lot_definition.min_qty == pytest.approx(0.0002)
    assert summary.lot_definition.qty_step == pytest.approx(0.0001)
    assert summary.effective_min_trade_qty == pytest.approx(0.0002)
    assert readiness.position_state.normalized_exposure.dust_operability_state == (
        "below_internal_lot_boundary_tracked_residue_entry_allowed"
    )
    assert readiness.position_state.normalized_exposure.entry_allowed is True
    assert readiness.closeout_allowed is False


def test_incident_event_sourced_paths_converge_on_same_lot_contract(recovery_db, tmp_path):
    def _materialize(path, *, corrupt_residual_contract: bool = False, repair: bool = False):
        conn = ensure_db(str(path))
        try:
            _apply_fee_pending_buy(conn)
            _apply_fee_pending_sell(conn)
            if corrupt_residual_contract:
                conn.execute(
                    """
                    UPDATE open_position_lots
                    SET internal_lot_size=qty_open
                    WHERE entry_client_order_id='incident_buy'
                      AND position_state='dust_tracking'
                    """
                )
                conn.commit()
            if repair:
                before = build_position_authority_assessment(conn)
                assert before["needs_residual_normalization"] is True
                apply_position_authority_rebuild(conn)
                conn.commit()
            readiness = compute_runtime_readiness_snapshot(conn)
            rows = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count,
                           internal_lot_size, lot_min_qty, lot_qty_step
                    FROM open_position_lots
                    WHERE entry_client_order_id='incident_buy'
                    ORDER BY id ASC
                    """
                ).fetchall()
            ]
            lifecycles = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT entry_client_order_id, exit_client_order_id, matched_qty
                    FROM trade_lifecycles
                    ORDER BY id ASC
                    """
                ).fetchall()
            ]
            return {
                "rows": rows,
                "lifecycles": lifecycles,
                "canonical_state": readiness.canonical_state,
                "residual_class": readiness.residual_class,
                "execution_flat": readiness.execution_flat,
                "accounting_flat": readiness.accounting_flat,
                "new_entry_allowed": readiness.new_entry_allowed,
                "closeout_allowed": readiness.closeout_allowed,
                "normalized_exposure": readiness.position_state.normalized_exposure.as_dict(),
            }
        finally:
            conn.close()

    normal = _materialize(tmp_path / "normal.sqlite")
    replay = _materialize(tmp_path / "replay.sqlite")
    repaired = _materialize(
        tmp_path / "repair.sqlite",
        corrupt_residual_contract=True,
        repair=True,
    )

    assert normal == replay == repaired
    assert normal["rows"][0]["position_state"] == "dust_tracking"
    assert normal["rows"][0]["qty_open"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert normal["rows"][0]["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert normal["normalized_exposure"]["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert normal["normalized_exposure"]["sellable_executable_lot_count"] == 0


@pytest.mark.parametrize(
    ("residual_qty", "new_entry_allowed", "operability_state"),
    [
        (0.0001, True, "below_internal_lot_boundary_tracked_residue_entry_allowed"),
        (0.00019996, True, "below_internal_lot_boundary_tracked_residue_entry_allowed"),
        (0.0002, True, "below_internal_lot_boundary_tracked_residue_entry_allowed"),
        (0.00039999, True, "below_internal_lot_boundary_tracked_residue_entry_allowed"),
        (0.0004, False, "tracked_dust_operator_review_required"),
    ],
)
def test_tracked_dust_operability_boundary_uses_stored_lot_min_qty(
    recovery_db,
    residual_qty,
    new_entry_allowed,
    operability_state,
):
    conn = ensure_db(str(recovery_db))
    try:
        _replace_with_tracked_dust_row(conn, residual_qty=residual_qty)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert readiness.canonical_state == "DUST_ONLY_TRACKED"
    assert readiness.run_loop_allowed is True
    assert readiness.new_entry_allowed is new_entry_allowed
    assert readiness.closeout_allowed is False
    assert readiness.position_state.normalized_exposure.dust_operability_state == operability_state
    if new_entry_allowed:
        assert readiness.residual_class == "HARMLESS_DUST_TREAT_AS_FLAT"
        assert readiness.operator_action_required is False
    else:
        assert readiness.residual_class == "TRACKED_DUST_BLOCK_NEW_ENTRY"
        assert readiness.operator_action_required is True


def test_ec2_boundary_near_dust_only_residue_allows_reentry_without_sell_authority(recovery_db):
    conn = ensure_db(str(recovery_db))
    residual_qty = 0.00039988
    try:
        _replace_with_tracked_dust_rows(conn, residual_qty=residual_qty, row_count=2)
        _record_consistent_residue_reconcile_metadata(residual_qty)

        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    exposure = readiness.position_state.normalized_exposure
    assert readiness.canonical_state == "DUST_ONLY_TRACKED"
    assert readiness.residual_class == "HARMLESS_DUST_TREAT_AS_FLAT"
    assert readiness.run_loop_allowed is True
    assert readiness.new_entry_allowed is True
    assert readiness.closeout_allowed is False
    assert readiness.execution_flat is True
    assert readiness.accounting_flat is False
    assert readiness.operator_action_required is False
    assert exposure.open_lot_count == 0
    assert exposure.dust_tracking_lot_count == 2
    assert exposure.dust_tracking_qty == pytest.approx(residual_qty)
    assert exposure.internal_lot_size == pytest.approx(LOT_SIZE)
    assert exposure.sellable_executable_lot_count == 0
    assert exposure.sellable_executable_qty == pytest.approx(0.0)
    assert exposure.dust_operability_state == "below_internal_lot_boundary_tracked_residue_entry_allowed"
    assert readiness.tradeability_operator_fields["trading_allowed"] is True
    assert readiness.tradeability_operator_fields["strategy_tradeability_state"] == "reentry_allowed"
    assert readiness.tradeability_operator_fields["entry_policy_state"] == "allowed"
    assert readiness.tradeability_operator_fields["closeout_policy_state"] == "blocked"


@pytest.mark.parametrize(
    ("residual_qty", "expected_allowed", "expected_residual_class", "expected_operability_state"),
    [
        (
            0.00039999,
            True,
            "HARMLESS_DUST_TREAT_AS_FLAT",
            "below_internal_lot_boundary_tracked_residue_entry_allowed",
        ),
        (
            0.0004,
            True,
            "TRACKED_ACCOUNTING_RESIDUE_REENTRY_ALLOWED",
            "boundary_near_tracked_residue_entry_allowed",
        ),
        (
            0.00040001,
            True,
            "TRACKED_ACCOUNTING_RESIDUE_REENTRY_ALLOWED",
            "boundary_near_tracked_residue_entry_allowed",
        ),
        (
            0.00040005,
            False,
            "TRACKED_DUST_BLOCK_NEW_ENTRY",
            "tracked_dust_operator_review_required",
        ),
    ],
)
def test_boundary_near_tracked_residue_requires_consistent_evidence_for_reentry(
    recovery_db,
    residual_qty,
    expected_allowed,
    expected_residual_class,
    expected_operability_state,
):
    conn = ensure_db(str(recovery_db))
    try:
        _replace_with_tracked_dust_rows(conn, residual_qty=residual_qty, row_count=2)
        _record_consistent_residue_reconcile_metadata(residual_qty)

        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    exposure = readiness.position_state.normalized_exposure
    assert readiness.new_entry_allowed is expected_allowed
    assert readiness.closeout_allowed is False
    assert readiness.residual_class == expected_residual_class
    assert exposure.sellable_executable_lot_count == 0
    assert exposure.dust_operability_state == expected_operability_state
    assert exposure.dust_operability_boundary_qty == pytest.approx(LOT_SIZE)
    assert exposure.dust_operability_boundary_tolerance_qty == pytest.approx(LOT_SIZE * 0.0001)
    assert exposure.dust_operability_evidence_consistent is True
    if expected_allowed:
        assert readiness.operator_action_required is False
    else:
        assert readiness.operator_action_required is True


def test_boundary_near_tracked_residue_without_broker_local_evidence_still_blocks(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _replace_with_tracked_dust_rows(conn, residual_qty=LOT_SIZE, row_count=2)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "dust_residual_present": 0,
                "dust_state": "no_dust",
                "dust_policy_reason": "no_dust_residual",
                "dust_broker_qty": 0.0,
                "dust_local_qty": 0.0,
            },
            now_epoch_sec=1_700_000_010.0,
        )

        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    exposure = readiness.position_state.normalized_exposure
    assert readiness.canonical_state == "DUST_ONLY_TRACKED"
    assert readiness.residual_class == "TRACKED_DUST_BLOCK_NEW_ENTRY"
    assert readiness.new_entry_allowed is False
    assert readiness.operator_action_required is True
    assert exposure.dust_operability_state == "tracked_dust_operator_review_required"
    assert exposure.dust_operability_evidence_consistent is False
    assert readiness.tradeability_operator_fields["strategy_tradeability_state"] == "running_not_tradable"
    assert readiness.tradeability_operator_fields["entry_policy_state"] == "blocked"
    assert readiness.tradeability_operator_fields["closeout_policy_state"] == "blocked"


def test_authority_correction_repairs_incident_dust_row_with_historical_sell_history(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _record_historical_sell_history(conn)
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)

        assessment = build_position_authority_assessment(conn)
        preview = build_position_authority_rebuild_preview(conn)

        assert assessment["needs_correction"] is True
        assert assessment["safe_to_correct"] is True
        assert preview["repair_mode"] == "correction"
        assert preview["safe_to_apply"] is True
        assert preview["sell_trade_count"] == 1

        result = apply_position_authority_rebuild(conn)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size
            FROM open_position_lots
            WHERE entry_client_order_id='incident_buy'
            ORDER BY id ASC
            """
        ).fetchall()
        repair = conn.execute("SELECT reason, repair_basis FROM position_authority_repairs").fetchone()
    finally:
        conn.close()

    assert result["repair"]["reason"] == "accounted_buy_fill_authority_correction"
    assert repair["reason"] == "accounted_buy_fill_authority_correction"
    assert json.loads(repair["repair_basis"])["event_type"] == "position_authority_correction"
    assert rows[0]["position_state"] == "open_exposure"
    assert rows[0]["qty_open"] == pytest.approx(LOT_SIZE)
    assert rows[0]["executable_lot_count"] == 1
    assert rows[0]["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert rows[1]["position_state"] == "dust_tracking"
    assert rows[1]["qty_open"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert rows[1]["dust_tracking_lot_count"] == 1
    assert rows[1]["internal_lot_size"] == pytest.approx(LOT_SIZE)


def test_portfolio_projection_divergence_classifies_dead_end_without_broker_evidence(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _create_portfolio_projection_divergence(conn)

        assessment = build_position_authority_assessment(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        preview = build_position_authority_rebuild_preview(conn)
    finally:
        conn.close()

    assert assessment["incident_class"] == "PROJECTION_PORTFOLIO_DIVERGENCE"
    assert assessment["needs_portfolio_projection_repair"] is True
    assert assessment["sell_after_target_buy_count"] == 0
    assert assessment["target_qty"] == pytest.approx(PORTFOLIO_DIVERGENCE_BUY_QTY)
    assert assessment["portfolio_qty"] == pytest.approx(PORTFOLIO_DIVERGENCE_QTY)
    assert readiness.recovery_stage == "AUTHORITY_PROJECTION_PORTFOLIO_DIVERGENCE_PENDING"
    assert readiness.resume_blockers == ("POSITION_AUTHORITY_PROJECTION_REPAIR_REQUIRED",)
    assert preview["repair_mode"] == "portfolio_projection_repair"
    assert preview["safe_to_apply"] is False
    assert "broker_position_qty_evidence_missing" in preview["eligibility_reason"]


def test_portfolio_anchored_projection_repair_removes_false_executable_authority(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _create_portfolio_projection_divergence(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "balance_observed_ts_ms": 1_776_745_500_000,
                "balance_asset_ts_ms": 1_776_745_500_000,
                "balance_source": "accounts_v1_rest_snapshot",
                "balance_source_stale": False,
                "balance_source_quote_currency": "KRW",
                "balance_source_base_currency": "BTC",
                "broker_asset_qty": PORTFOLIO_DIVERGENCE_QTY,
                "broker_asset_available": PORTFOLIO_DIVERGENCE_QTY,
                "broker_asset_locked": 0.0,
                "broker_cash_available": OBSERVED_LIVE_CASH_KRW,
                "broker_cash_locked": 0.0,
                "remote_open_order_found": 0,
                "unresolved_open_order_count": 0,
                "submit_unknown_count": 0,
                "recovery_required_count": 0,
                "dust_residual_present": 1,
                "dust_residual_allow_resume": 1,
                "dust_effective_flat": 1,
                "dust_state": "harmless_dust",
                "dust_broker_qty": PORTFOLIO_DIVERGENCE_QTY,
                "dust_local_qty": PORTFOLIO_DIVERGENCE_QTY,
                "dust_delta_qty": 0.0,
                "dust_qty_gap_tolerance": 0.000001,
                "dust_qty_gap_small": 1,
                "dust_min_qty": LOT_SIZE,
            },
            now_epoch_sec=1.0,
        )

        before = compute_runtime_readiness_snapshot(conn)
        preview = build_position_authority_rebuild_preview(conn)
        result = apply_position_authority_rebuild(conn)
        conn.commit()
        after = compute_runtime_readiness_snapshot(conn)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size
            FROM open_position_lots
            WHERE entry_client_order_id='live_1776745440000_buy_ae9d0d6e'
            ORDER BY id ASC
            """
        ).fetchall()
        repair = conn.execute(
            "SELECT reason, repair_basis FROM position_authority_repairs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        publication = conn.execute(
            """
            SELECT pair, target_trade_id, source, publish_basis
            FROM position_authority_projection_publications
            ORDER BY event_ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        adjustment = conn.execute(
            "SELECT reason, adjustment_basis FROM external_position_adjustments ORDER BY id DESC LIMIT 1"
        ).fetchone()

        replay = rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR)
        conn.commit()
        replay_rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size
            FROM open_position_lots
            WHERE entry_client_order_id='live_1776745440000_buy_ae9d0d6e'
            ORDER BY id ASC
            """
        ).fetchall()
        preview_after = build_external_position_accounting_repair_preview(conn)
    finally:
        conn.close()

    assert before.recovery_stage == "AUTHORITY_PROJECTION_PORTFOLIO_DIVERGENCE_PENDING"
    assert preview["safe_to_apply"] is True
    assert preview["eligibility_reason"] == "portfolio-anchored projection repair applicable"
    assert result["repair"]["reason"] == "portfolio_anchored_authority_projection_repair"
    assert result["projection_publication"]["publication_type"] == "portfolio_projection_publish"
    assert result["external_position_adjustment"]["reason"] == "portfolio_projection_external_position_adjustment"
    assert repair["reason"] == "portfolio_anchored_authority_projection_repair"
    assert publication["pair"] == settings.PAIR
    assert publication["source"] == "manual_portfolio_anchored_authority_projection_publish"
    assert adjustment["reason"] == "portfolio_projection_external_position_adjustment"
    basis = json.loads(repair["repair_basis"])
    assert basis["event_type"] == "portfolio_anchored_authority_projection_repair"
    assert basis["target_remainder_qty"] == pytest.approx(PORTFOLIO_DIVERGENCE_QTY)
    publication_basis = json.loads(publication["publish_basis"])
    assert publication_basis["event_type"] == "portfolio_anchored_authority_projection_repair"
    assert publication_basis["target_remainder_qty"] == pytest.approx(PORTFOLIO_DIVERGENCE_QTY)
    adjustment_basis = json.loads(adjustment["adjustment_basis"])
    assert adjustment_basis["event_type"] == "external_position_adjustment"
    assert adjustment_basis["source_event_type"] == "portfolio_anchored_authority_projection_repair"
    assert len(rows) == 1
    assert rows[0]["position_state"] == "dust_tracking"
    assert rows[0]["qty_open"] == pytest.approx(PORTFOLIO_DIVERGENCE_QTY)
    assert rows[0]["executable_lot_count"] == 0
    assert rows[0]["dust_tracking_lot_count"] == 1
    assert rows[0]["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert after.recovery_stage == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert after.resume_ready is False
    assert after.canonical_state == "DUST_ONLY_TRACKED"
    assert after.position_state.normalized_exposure.sellable_executable_lot_count == 0
    assert after.recommended_command == "uv run bithumb-bot residual-closeout-plan"
    assert preview_after["needs_repair"] is False
    assert replay.replayed_buy_count == 1
    assert len(replay_rows) == 1
    assert replay_rows[0]["position_state"] == "dust_tracking"
    assert replay_rows[0]["qty_open"] == pytest.approx(PORTFOLIO_DIVERGENCE_QTY)
    assert replay_rows[0]["executable_lot_count"] == 0


def test_recorded_projection_repair_event_alone_is_not_replayed_as_current_projection(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _create_portfolio_projection_divergence(conn)
        target_trade = conn.execute(
            """
            SELECT id, client_order_id
            FROM trades
            WHERE client_order_id='live_1776745440000_buy_ae9d0d6e' AND side='BUY'
            """
        ).fetchone()
        record_position_authority_repair(
            conn,
            event_ts=1_776_745_600_000,
            source="test_historical_projection_repair_only",
            reason="portfolio_anchored_authority_projection_repair",
            repair_basis={
                "event_type": "portfolio_anchored_authority_projection_repair",
                "target_trade_id": int(target_trade["id"]),
                "target_client_order_id": str(target_trade["client_order_id"]),
                "target_qty": PORTFOLIO_DIVERGENCE_BUY_QTY,
                "target_remainder_qty": PORTFOLIO_DIVERGENCE_QTY,
                "portfolio_qty": PORTFOLIO_DIVERGENCE_QTY,
                "canonical_internal_lot_size": LOT_SIZE,
            },
        )
        conn.execute("DELETE FROM open_position_lots")
        conn.commit()

        replay = rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count
            FROM open_position_lots
            WHERE entry_client_order_id='live_1776745440000_buy_ae9d0d6e'
            ORDER BY id ASC
            """
        ).fetchall()
        publication_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM position_authority_projection_publications"
        ).fetchone()
    finally:
        conn.close()

    assert replay.replayed_buy_count == 1
    assert publication_count["cnt"] == 0
    assert len(rows) == 2
    assert rows[0]["position_state"] == "open_exposure"
    assert rows[0]["qty_open"] == pytest.approx(LOT_SIZE)
    assert rows[0]["executable_lot_count"] == 1
    assert rows[1]["position_state"] == "dust_tracking"
    assert rows[1]["qty_open"] == pytest.approx(PORTFOLIO_DIVERGENCE_BUY_QTY - LOT_SIZE)
    assert rows[1]["dust_tracking_lot_count"] == 1


def test_missing_fee_incident_projection_repair_refuses_non_converged_stale_dust_projection(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_status("incident_buy", "FILLED", conn=conn)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        _insert_live_incident_stale_dust_projection(conn)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)

        preview = build_position_authority_rebuild_preview(conn)
        before = compute_runtime_readiness_snapshot(conn)
        with pytest.raises(RuntimeError, match="requires explicit full projection rebuild mode"):
            apply_position_authority_rebuild(conn)
        repair_count_before_rollback = conn.execute(
            "SELECT COUNT(*) AS cnt FROM position_authority_repairs"
        ).fetchone()
        conn.rollback()
        repair_count_after_rollback = conn.execute(
            "SELECT COUNT(*) AS cnt FROM position_authority_repairs"
        ).fetchone()
        rows_after_rollback = conn.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COALESCE(SUM(qty_open), 0.0) AS lot_qty,
                COALESCE(SUM(CASE WHEN position_state='dust_tracking' THEN qty_open ELSE 0.0 END), 0.0)
                    AS dust_qty,
                COALESCE(SUM(CASE WHEN position_state='open_exposure' THEN qty_open ELSE 0.0 END), 0.0)
                    AS open_qty
            FROM open_position_lots
            """
        ).fetchone()
    finally:
        conn.close()

    assert preview["repair_mode"] == "full_projection_rebuild"
    assert preview["safe_to_apply"] is True
    assert preview["action_state"] == "safe_to_apply_now"
    assert preview["eligibility_reason"] == "full projection rebuild applicable"
    assert before.recovery_stage == "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING"
    assert before.as_dict()["position_authority_alignment_state"] == "projection_diverged"
    assert "historical_fragmentation" in before.as_dict()["position_authority_diagnostic_flags"]
    assert "unsafe_auto_repair" in before.as_dict()["position_authority_diagnostic_flags"]
    assert repair_count_before_rollback["cnt"] == 0
    assert repair_count_after_rollback["cnt"] == 0
    assert rows_after_rollback["row_count"] == 16
    assert rows_after_rollback["lot_qty"] == pytest.approx(FILL_QTY + LIVE_INCIDENT_STALE_DUST_QTY)
    assert rows_after_rollback["dust_qty"] == pytest.approx(
        (FILL_QTY - LOT_SIZE) + LIVE_INCIDENT_STALE_DUST_QTY
    )
    assert rows_after_rollback["open_qty"] == pytest.approx(LOT_SIZE)


def test_projection_divergence_emits_cross_layer_quantity_contract_diagnostics(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_status("incident_buy", "FILLED", conn=conn)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        _insert_live_incident_stale_dust_projection(conn)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)

        assessment = build_position_authority_assessment(conn)
    finally:
        conn.close()

    authoritative = assessment["authoritative_quantity_contract"]
    projection = assessment["projection_quantity_contract"]

    assert assessment["alignment_state"] == "projection_diverged"
    assert assessment["incident_class"] == "HISTORICAL_FRAGMENTATION_PROJECTION_DRIFT"
    assert assessment["repair_mode"] == "full_projection_rebuild"
    assert "historical_fragmentation" in assessment["diagnostic_flags"]
    assert "unsafe_auto_repair" in assessment["diagnostic_flags"]
    assert assessment["repair_action_state"] == "inspect_only"
    assert assessment["projection_repair_covers_excess"] is False
    assert authoritative["requested_qty"] == pytest.approx(FILL_QTY)
    assert authoritative["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert authoritative["executable_lot_count"] == 1
    assert authoritative["residual_qty"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert projection["requested_qty"] == pytest.approx(FILL_QTY)
    assert projection["residual_reason"] == "dust_tracking_projection"
    assert projection["executable_lot_count"] == 1


def test_historical_fragmentation_full_projection_rebuild_dry_run_and_apply(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_status("incident_buy", "FILLED", conn=conn)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        _insert_live_incident_stale_dust_projection(conn)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)

        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
        result = apply_position_authority_rebuild(conn, full_projection_rebuild=True, note="fixture full rebuild")
        conn.commit()
        convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
        readiness = compute_runtime_readiness_snapshot(conn)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, lot_rule_source_mode
            FROM open_position_lots
            ORDER BY id ASC
            """
        ).fetchall()
        repair = conn.execute(
            """
            SELECT reason, repair_basis
            FROM position_authority_repairs
            ORDER BY event_ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        publication = conn.execute(
            """
            SELECT publication_key, publish_basis
            FROM position_authority_projection_publications
            ORDER BY event_ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert preview["repair_mode"] == "full_projection_rebuild"
    assert preview["safe_to_apply"] is True
    assert preview["position_authority_assessment"]["incident_class"] == "HISTORICAL_FRAGMENTATION_PROJECTION_DRIFT"
    assert result["repair"]["reason"] == "full_projection_materialized_rebuild"
    assert convergence["converged"] is True
    assert convergence["projected_total_qty"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY)
    assert readiness.recovery_stage == "RESUME_READY"
    assert readiness.resume_ready is True
    assert len(rows) == 2
    assert rows[0]["position_state"] == "open_exposure"
    assert rows[0]["qty_open"] == pytest.approx(0.0008)
    assert rows[0]["executable_lot_count"] == 2
    assert rows[0]["lot_rule_source_mode"] == "full_projection_rebuild_portfolio_anchor"
    assert rows[1]["position_state"] == "dust_tracking"
    assert rows[1]["qty_open"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY - 0.0008)
    assert rows[1]["dust_tracking_lot_count"] == 1
    assert publication["publication_key"]
    repair_basis = json.loads(repair["repair_basis"])
    assert repair_basis["portfolio_anchor_projection"]["portfolio_qty"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY)
    publication_basis = json.loads(publication["publish_basis"])
    assert publication_basis["portfolio_anchor_projection"]["broker_qty"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY)


def test_full_projection_rebuild_refuses_broker_portfolio_mismatch(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_status("incident_buy", "FILLED", conn=conn)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        _insert_live_incident_stale_dust_projection(conn)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY + 0.0001)

        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["repair_mode"] == "full_projection_rebuild"
    assert preview["safe_to_apply"] is False
    assert preview["broker_portfolio_converged"] is False
    assert "broker_portfolio_qty_mismatch=" in preview["eligibility_reason"]


def test_full_projection_rebuild_refuses_when_unresolved_orders_exist(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_status("incident_buy", "FILLED", conn=conn)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        _insert_live_incident_stale_dust_projection(conn)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        record_order_if_missing(
            conn,
            client_order_id="unresolved-live-order",
            side="BUY",
            qty_req=0.0004,
            price=PRICE,
            ts_ms=1_776_745_700_000,
            status="PENDING_SUBMIT",
            internal_lot_size=LOT_SIZE,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)

        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["safe_to_apply"] is False
    assert preview["pending_submit_count"] == 1
    assert "pending_submit=1" in preview["eligibility_reason"]


def test_full_projection_rebuild_is_idempotent(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_status("incident_buy", "FILLED", conn=conn)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        _insert_live_incident_stale_dust_projection(conn)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)

        first = apply_position_authority_rebuild(conn, full_projection_rebuild=True, note="first rebuild")
        conn.commit()
        second = apply_position_authority_rebuild(conn, full_projection_rebuild=True, note="second rebuild")
        conn.commit()
        repair_count = conn.execute("SELECT COUNT(*) AS cnt FROM position_authority_repairs").fetchone()
        publication_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM position_authority_projection_publications"
        ).fetchone()
    finally:
        conn.close()

    assert first["post_repair_projection_convergence"]["converged"] is True
    assert second["noop"] is True
    assert repair_count["cnt"] == 1
    assert publication_count["cnt"] == 1


def test_full_projection_rebuild_rolls_back_on_postcondition_failure(recovery_db, monkeypatch):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_status("incident_buy", "FILLED", conn=conn)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        _insert_live_incident_stale_dust_projection(conn)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        before_rows = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(qty_open), 0.0) AS qty FROM open_position_lots"
        ).fetchone()

        monkeypatch.setattr(
            "bithumb_bot.position_authority_repair._assert_post_repair_projection_converged",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("forced postcondition failure")),
        )
        with pytest.raises(RuntimeError, match="forced postcondition failure"):
            apply_position_authority_rebuild(conn, full_projection_rebuild=True)
        after_rows = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(qty_open), 0.0) AS qty FROM open_position_lots"
        ).fetchone()
        repair_count = conn.execute("SELECT COUNT(*) AS cnt FROM position_authority_repairs").fetchone()
        publication_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM position_authority_projection_publications"
        ).fetchone()
    finally:
        conn.close()

    assert after_rows["cnt"] == before_rows["cnt"]
    assert after_rows["qty"] == pytest.approx(before_rows["qty"])
    assert repair_count["cnt"] == 0
    assert publication_count["cnt"] == 0


def test_recovery_report_and_audit_ledger_distinguish_accounting_and_lot_projection_states(
    recovery_db, capsys
):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_status("incident_buy", "FILLED", conn=conn)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        _insert_live_incident_stale_dust_projection(conn)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
    finally:
        conn.close()

    report = _load_recovery_report()
    app_main(["audit-ledger"])
    audit_out = capsys.readouterr().out

    assert report["accounting_projection_ok"] is True
    assert report["broker_portfolio_converged"] is True
    assert report["lot_projection_converged"] is False
    assert report["live_ready"] is False
    assert report["blocking_incident_class"] == "HISTORICAL_FRAGMENTATION_PROJECTION_DRIFT"
    assert report["recommended_command"] == "uv run python bot.py rebuild-position-authority --full-projection-rebuild"
    assert "accounting_projection_ok=1" in audit_out
    assert "broker_portfolio_converged=1" in audit_out
    assert "lot_projection_converged=0" in audit_out
    assert "live_ready=0" in audit_out
    assert "blocking_incident_class=HISTORICAL_FRAGMENTATION_PROJECTION_DRIFT" in audit_out


def test_recorded_projection_repair_event_does_not_replace_aggregate_projection_convergence(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        target_trade = conn.execute(
            "SELECT id, client_order_id FROM trades WHERE client_order_id='incident_buy' AND side='BUY'"
        ).fetchone()
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=LIVE_INCIDENT_PORTFOLIO_QTY,
            asset_locked=0.0,
        )
        _insert_live_incident_stale_dust_projection(conn)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.execute("DELETE FROM open_position_lots WHERE entry_trade_id=?", (int(target_trade["id"]),))
        record_position_authority_repair(
            conn,
            event_ts=1_776_745_600_000,
            source="test_stale_portfolio_projection_repair",
            reason="portfolio_anchored_authority_projection_repair",
            repair_basis={
                "event_type": "portfolio_anchored_authority_projection_repair",
                "target_trade_id": int(target_trade["id"]),
                "target_client_order_id": str(target_trade["client_order_id"]),
                "target_remainder_qty": 0.0,
                "portfolio_qty": LIVE_INCIDENT_PORTFOLIO_QTY,
                "projected_total_qty": LIVE_INCIDENT_STALE_DUST_QTY,
                "projected_qty_excess": LIVE_INCIDENT_STALE_DUST_QTY - LIVE_INCIDENT_PORTFOLIO_QTY,
            },
        )
        conn.commit()

        readiness = compute_runtime_readiness_snapshot(conn)
        projection = readiness.as_dict()["projection_convergence"]
        lot_row = conn.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COALESCE(SUM(qty_open), 0.0) AS lot_qty,
                COALESCE(SUM(CASE WHEN position_state='open_exposure' THEN qty_open ELSE 0.0 END), 0.0)
                    AS open_qty
            FROM open_position_lots
            """
        ).fetchone()
    finally:
        conn.close()

    assert lot_row["row_count"] == 14
    assert lot_row["lot_qty"] == pytest.approx(LIVE_INCIDENT_STALE_DUST_QTY)
    assert lot_row["open_qty"] == pytest.approx(0.0)
    assert projection["converged"] is False
    assert projection["projected_total_qty"] == pytest.approx(LIVE_INCIDENT_STALE_DUST_QTY)
    assert projection["portfolio_qty"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY)
    assert projection["projected_qty_excess"] == pytest.approx(
        LIVE_INCIDENT_STALE_DUST_QTY - LIVE_INCIDENT_PORTFOLIO_QTY
    )
    assert readiness.recovery_stage == "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING"
    assert readiness.resume_ready is False
    assert readiness.resume_blockers == ("POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED",)
    assert readiness.run_loop_allowed is False
    assert readiness.new_entry_allowed is False
    assert readiness.closeout_allowed is False
    assert readiness.tradeability_operator_fields["strategy_tradeability_state"] == "run_loop_blocked"
    assert readiness.tradeability_operator_fields["trading_allowed"] is False


def test_recorded_projection_repair_without_publication_enables_full_rebuild_preview(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)

        assessment = build_position_authority_assessment(conn)
        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert assessment["alignment_state"] == "projection_diverged"
    assert assessment["portfolio_projection_repair_event_status"] == "recorded_but_not_current_state_proof"
    assert assessment["portfolio_projection_publication_present"] is False
    assert assessment["projection_excess_with_materialized_fragmentation"] is True
    assert assessment["needs_full_projection_rebuild"] is True
    assert "materialized_projection_fragmentation" in assessment["diagnostic_flags"]
    assert "historical_fragmentation" in assessment["diagnostic_flags"]
    assert preview["repair_mode"] == "full_projection_rebuild"
    assert preview["safe_to_apply"] is True
    assert preview["action_state"] == "safe_to_apply_now"
    assert preview["eligibility_reason"] == "full projection rebuild applicable"
    assert preview["projected_total_qty"] == pytest.approx(LIVE_INCIDENT_STALE_DUST_QTY)
    assert preview["portfolio_qty"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY)
    assert preview["projected_qty_excess"] == pytest.approx(
        LIVE_INCIDENT_STALE_DUST_QTY - LIVE_INCIDENT_PORTFOLIO_QTY
    )
    assert preview["lot_row_count"] == 14
    assert preview["other_active_qty"] == pytest.approx(LIVE_INCIDENT_STALE_DUST_QTY)
    assert preview["portfolio_projection_publication_present"] is False
    assert preview["portfolio_projection_repair_event_status"] == "recorded_but_not_current_state_proof"
    assert preview["needs_full_projection_rebuild"] is True
    assert preview["full_projection_rebuild_gate_report"]["reasons"] == []


def test_recorded_projection_repair_without_publication_full_rebuild_apply_records_publication(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)

        result = apply_position_authority_rebuild(
            conn,
            full_projection_rebuild=True,
            note="observed live stale fragmented projection",
        )
        conn.commit()
        convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
        readiness = compute_runtime_readiness_snapshot(conn)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, lot_rule_source_mode
            FROM open_position_lots
            ORDER BY id ASC
            """
        ).fetchall()
        publication_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM position_authority_projection_publications"
        ).fetchone()
        repair_count = conn.execute("SELECT COUNT(*) AS cnt FROM position_authority_repairs").fetchone()
    finally:
        conn.close()

    assert result["projection_publication"]["publication_key"]
    assert result["repair"]["reason"] == "full_projection_materialized_rebuild"
    assert publication_count["cnt"] == 1
    assert repair_count["cnt"] == 2
    assert convergence["converged"] is True
    assert convergence["projected_total_qty"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY)
    assert len(rows) == 2
    assert rows[0]["position_state"] == "open_exposure"
    assert rows[0]["qty_open"] == pytest.approx(0.0008)
    assert rows[0]["executable_lot_count"] == 2
    assert rows[1]["position_state"] == "dust_tracking"
    assert rows[1]["qty_open"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY - 0.0008)
    assert rows[1]["dust_tracking_lot_count"] == 1
    assert readiness.as_dict()["projection_converged"] is True
    assert readiness.as_dict()["position_authority_assessment"]["portfolio_projection_publication_present"] is True
    assert readiness.as_dict()["position_authority_assessment"]["portfolio_projection_publication_status"] == (
        "published_current_state_attestation"
    )
    assert readiness.as_dict()["position_authority_assessment"]["needs_full_projection_rebuild"] is False
    assert readiness.recovery_stage == "RESUME_READY"


def test_full_projection_rebuild_portfolio_anchor_does_not_require_fill_qty_match_after_partial_close_with_existing_residual(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        anchor = _materialize_full_rebuild_portfolio_anchor_fixture(conn, with_publication=True)

        assessment = build_position_authority_assessment(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert anchor["portfolio_qty"] == pytest.approx(EC2_REPRO_PORTFOLIO_QTY)
    assert assessment["target_trade_id"] == anchor["anchor_trade_id"]
    assert assessment["target_qty"] == pytest.approx(EC2_REPRO_LATEST_BUY_QTY)
    assert assessment["existing_total_qty"] == pytest.approx(EC2_REPRO_PORTFOLIO_QTY)
    assert assessment["target_lot_provenance_kind"] == "portfolio_anchor_projection_lot"
    assert assessment["target_lot_fill_qty_invariant_applies"] is False
    assert assessment["semantic_contract_check_applicable"] is False
    assert assessment["semantic_contract_check_skipped_reason"] == "portfolio_anchor_projection_lot"
    assert assessment["semantic_contract_check_passed"] is True
    assert assessment["portfolio_anchor_projection_state_converged"] is True
    assert assessment["portfolio_projection_publication_present"] is True
    assert assessment["needs_correction"] is False
    assert not any("target_lot_qty_fill_mismatch=" in blocker for blocker in assessment["blockers"])
    assert readiness.recovery_stage == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert readiness.resume_ready is False
    assert readiness.resume_blockers == ("NON_EXECUTABLE_RESIDUAL_HOLDINGS",)
    assert readiness.recommended_command == "uv run bithumb-bot residual-closeout-plan"
    assert readiness.run_loop_allowed is False
    assert readiness.new_entry_allowed is False
    assert readiness.closeout_allowed is False
    assert readiness.residual_class == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert (
        readiness.as_dict()["position_authority_assessment"]["target_lot_provenance_kind"]
        == "portfolio_anchor_projection_lot"
    )
    assert readiness.as_dict()["residual_inventory"]["exchange_sellable"] is True


def test_broker_matched_residual_only_holdings_block_resume_without_rebuild_or_flatten(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        fixture = _materialize_broker_matched_residual_only_fixture(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    payload = readiness.as_dict()
    residual_inventory = payload["residual_inventory"]
    row_map = {int(row["entry_trade_id"]): row for row in residual_inventory["rows"]}

    assert payload["projection_converged"] is True
    assert payload["broker_position_evidence"]["broker_qty"] == pytest.approx(RESIDUAL_INCIDENT_PORTFOLIO_QTY)
    assert payload["projection_convergence"]["portfolio_qty"] == pytest.approx(RESIDUAL_INCIDENT_PORTFOLIO_QTY)
    assert payload["projection_convergence"]["projected_total_qty"] == pytest.approx(RESIDUAL_INCIDENT_PORTFOLIO_QTY)
    assert payload["normalized_exposure"]["sellable_executable_lot_count"] == 0
    assert residual_inventory["residual_qty"] == pytest.approx(RESIDUAL_INCIDENT_PORTFOLIO_QTY)
    assert readiness.resume_ready is False
    assert readiness.recovery_stage == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert readiness.resume_blockers == ("NON_EXECUTABLE_RESIDUAL_HOLDINGS",)
    assert readiness.run_loop_allowed is False
    assert readiness.new_entry_allowed is False
    assert readiness.closeout_allowed is False
    assert readiness.residual_class == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert readiness.recommended_command == "uv run bithumb-bot residual-closeout-plan"
    assert "rebuild-position-authority" not in readiness.recommended_command
    assert "flatten-position" not in readiness.recommended_command
    assert residual_inventory["exchange_sellable"] is True
    assert residual_inventory["strategy_sellable"] is False
    assert row_map[fixture["prior_buy_trade_id"]]["classes"] == [
        "NEAR_LOT_RESIDUAL",
        "PORTFOLIO_ANCHOR_RESIDUAL",
        "DEGRADED_RECOVERY_RESIDUAL",
    ]
    assert row_map[fixture["latest_buy_trade_id"]]["classes"] == [
        "LEDGER_SPLIT_RESIDUAL",
        "TRUE_DUST",
    ]


def test_broker_matched_residual_only_holdings_track_mode_resume_as_tracked_inventory(
    recovery_db,
):
    original_mode = settings.RESIDUAL_INVENTORY_MODE
    object.__setattr__(settings, "RESIDUAL_INVENTORY_MODE", "track")
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_broker_matched_residual_only_fixture(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()
        object.__setattr__(settings, "RESIDUAL_INVENTORY_MODE", original_mode)

    payload = readiness.as_dict()

    assert readiness.resume_ready is True
    assert readiness.recovery_stage == "RESIDUAL_INVENTORY_TRACKED"
    assert readiness.run_loop_allowed is True
    assert readiness.new_entry_allowed is True
    assert readiness.closeout_allowed is True
    assert readiness.residual_class == "RESIDUAL_INVENTORY_TRACKED"
    assert readiness.recommended_command == "uv run python bot.py resume"
    assert payload["residual_inventory_mode"] == "track"
    assert payload["residual_inventory_state"] == "RESIDUAL_INVENTORY_TRACKED"
    assert payload["tradeability_reason"] == "RESIDUAL_INVENTORY_TRACKED"
    assert payload["tradeability_gate_blocked"] is False
    assert payload["residual_inventory_policy_allows_run"] is True
    assert payload["residual_inventory_policy_allows_buy"] is True
    assert payload["residual_inventory_policy_allows_sell"] is True
    assert payload["residual_sell_candidate_allowed"] is True
    assert payload["residual_sell_candidate"]["source"] == "residual_inventory"
    assert payload["total_effective_exposure_qty"] == pytest.approx(RESIDUAL_INCIDENT_PORTFOLIO_QTY)
    assert payload["unresolved_open_order_count"] == 0
    assert payload["submit_unknown_count"] == 0

    sell_decision = build_execution_decision_summary(
        decision_context={
            "raw_signal": "SELL",
            "final_signal": "HOLD",
            "has_dust_only_remainder": True,
            "exit_allowed": False,
            "exit_block_reason": "dust_only_remainder",
            "sellable_executable_lot_count": 0,
            "sellable_executable_qty": 0.0,
        },
        readiness_payload=payload,
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert sell_decision.final_action == "CLOSE_RESIDUAL_CANDIDATE"
    assert sell_decision.submit_expected is False
    assert sell_decision.pre_submit_proof_status == "passed"
    assert sell_decision.block_reason == "residual_live_sell_mode_telemetry"
    assert sell_decision.residual_submit_plan is not None
    assert sell_decision.residual_sell_candidate is not None
    assert sell_decision.strategy_sell_candidate is None

    buy_decision = build_execution_decision_summary(
        decision_context={
            "raw_signal": "BUY",
            "final_signal": "HOLD",
            "entry_block_reason": "dust_only_remainder",
        },
        readiness_payload=payload,
        raw_signal="BUY",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert buy_decision.current_effective_exposure_krw == pytest.approx(
        payload["total_effective_exposure_notional_krw"]
    )
    assert buy_decision.tracked_residual_exposure_krw == pytest.approx(
        payload["residual_inventory_notional_krw"]
    )


def test_broker_matched_residual_only_holdings_track_mode_still_fails_closed_when_broker_evidence_stale(
    recovery_db,
):
    original_mode = settings.RESIDUAL_INVENTORY_MODE
    object.__setattr__(settings, "RESIDUAL_INVENTORY_MODE", "track")
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_broker_matched_residual_only_fixture(conn)
        metadata = json.loads(str(runtime_state.snapshot().last_reconcile_metadata or "{}"))
        metadata["balance_source_stale"] = True
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata=metadata,
            now_epoch_sec=2.0,
        )
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()
        object.__setattr__(settings, "RESIDUAL_INVENTORY_MODE", original_mode)

    assert readiness.run_loop_allowed is False
    assert readiness.residual_class == "RESIDUAL_INVENTORY_UNRESOLVED"


def test_tracked_residual_inventory_changes_operator_policy_surfaces(
    recovery_db,
    capsys,
):
    original_mode = settings.RESIDUAL_INVENTORY_MODE
    object.__setattr__(settings, "RESIDUAL_INVENTORY_MODE", "track")
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_broker_matched_residual_only_fixture(conn)
    finally:
        conn.close()

    try:
        report = _load_recovery_report()

        assert report["tradeability_reason"] == "RESIDUAL_INVENTORY_TRACKED"
        assert report["recommended_command"] == "uv run python bot.py resume"
        assert report["recovery_policy"]["recommended_mode"] == "residual_inventory_tracked"

        capsys.readouterr()
        app_main(["health"])
        health_out = capsys.readouterr().out
        assert "run_loop_can_resume=true" in health_out
        assert "tradeability_reason=RESIDUAL_INVENTORY_TRACKED" in health_out
        assert "tradeability_gate_blocked=0" in health_out

        app_main(["restart-checklist"])
        checklist_out = capsys.readouterr().out
        assert "run_loop_allowed=1" in checklist_out
        assert "recommended_mode=residual_inventory_tracked" in checklist_out
    finally:
        object.__setattr__(settings, "RESIDUAL_INVENTORY_MODE", original_mode)


def test_repair_plan_and_residual_closeout_plan_classify_residual_only_holdings_as_tradeability_policy(
    recovery_db,
    capsys,
):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_broker_matched_residual_only_fixture(conn)
    finally:
        conn.close()

    report = _load_recovery_report()

    assert report["blocking_incident_class"] == "TRADEABILITY_POLICY"
    assert report["recommended_command"] == "uv run bithumb-bot residual-closeout-plan"
    assert report["runtime_readiness"]["recommended_command"] == "uv run bithumb-bot residual-closeout-plan"
    assert report["recovery_policy"]["primary_incident_class"] == "TRADEABILITY_POLICY"
    assert report["recovery_policy"]["recommended_mode"] == "residual_policy_review"
    assert report["recovery_policy"]["recommended_command"] == "uv run bithumb-bot residual-closeout-plan"

    capsys.readouterr()
    app_main(["repair-plan", "--json"])
    plan = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert plan["primary_incident_class"] == "TRADEABILITY_POLICY"
    assert plan["recommended_mode"] == "residual_policy_review"
    assert plan["recommended_command"] == "uv run bithumb-bot residual-closeout-plan"
    assert plan["flatten_primary_recommendation"] is False
    assert plan["accounting_root_cause_unresolved"] is False
    rebuild_candidate = next(
        candidate for candidate in plan["candidate_repairs"] if candidate["name"] == "rebuild-position-authority"
    )
    assert rebuild_candidate["needed"] is False

    app_main(["residual-closeout-plan", "--json"])
    closeout_plan = json.loads(capsys.readouterr().out)
    assert closeout_plan["reason_code"] == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert closeout_plan["strategy_closeout_allowed"] is False
    assert closeout_plan["operator_closeout_possible"] is True
    assert closeout_plan["recommended_command"] == "uv run bithumb-bot residual-closeout-plan"


def test_residual_only_holdings_scope_resume_gates_and_reasons_across_operator_surfaces(
    recovery_db,
    capsys,
):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_broker_matched_residual_only_fixture(conn)
    finally:
        conn.close()

    report = _load_recovery_report()

    assert report["can_resume"] is True
    assert report["halt_recovery_can_resume"] is True
    assert report["run_loop_can_resume"] is False
    assert report["startup_recovery_gate_blocked"] is False
    assert report["tradeability_gate_blocked"] is True
    assert report["projection_reason"] == "converged"
    assert report["tradeability_reason"] == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert report["primary_reason"] == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert report["operator_next_action"] == "residual_policy_review"
    assert report["resume_blocked_reason"] == "run loop blocked by non-executable residual holdings policy"
    assert report["tradeability_resume_safety"] == "policy_blocked (NON_EXECUTABLE_RESIDUAL_HOLDINGS)"
    assert report["recommended_command"] == "uv run bithumb-bot residual-closeout-plan"

    capsys.readouterr()
    app_main(["health"])
    health_out = capsys.readouterr().out
    assert "can_resume=true" in health_out
    assert "halt_recovery_can_resume=true" in health_out
    assert "run_loop_can_resume=false" in health_out
    assert "startup_recovery_gate_blocked=0 tradeability_gate_blocked=1" in health_out
    assert "tradeability_reason=NON_EXECUTABLE_RESIDUAL_HOLDINGS" in health_out
    assert "projection_reason=converged" in health_out
    assert "resume_safety=scoped_safe_halt_recovery_only (NON_EXECUTABLE_RESIDUAL_HOLDINGS)" in health_out
    assert "tradeability_resume_safety=policy_blocked (NON_EXECUTABLE_RESIDUAL_HOLDINGS)" in health_out
    assert "next_commands=uv run bithumb-bot residual-closeout-plan" in health_out

    app_main(["restart-checklist"])
    checklist_out = capsys.readouterr().out
    assert "resume_scope=process_loop_only" in checklist_out
    assert "startup_recovery_gate_blocked=0 tradeability_gate_blocked=1" in checklist_out
    assert "halt_recovery_can_resume=1 run_loop_can_resume=0" in checklist_out
    assert "run_loop_allowed=0" in checklist_out
    assert "tradeability_reason=NON_EXECUTABLE_RESIDUAL_HOLDINGS" in checklist_out
    assert "tradeability_resume_safety=policy_blocked (NON_EXECUTABLE_RESIDUAL_HOLDINGS)" in checklist_out


def test_residual_only_repair_plan_inactive_candidates_do_not_carry_recommended_commands(
    recovery_db,
    capsys,
):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_broker_matched_residual_only_fixture(conn)
    finally:
        conn.close()

    capsys.readouterr()
    app_main(["repair-plan", "--json"])
    plan = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert plan["reason"] == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert plan["primary_reason"] == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert plan["projection_reason"] == "converged"
    assert plan["tradeability_reason"] == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"

    for candidate in plan["candidate_repairs"]:
        if candidate["needed"]:
            continue
        assert candidate["recommended_command"] is None
        assert candidate["command_applicable"] is False
        assert candidate["not_recommended_reason"] == (
            "current broker/portfolio/projection are converged; residual-only tradeability policy applies"
        )

    rebuild_candidate = next(
        candidate for candidate in plan["candidate_repairs"] if candidate["name"] == "rebuild-position-authority"
    )
    assert rebuild_candidate["needed"] is False
    assert rebuild_candidate["recommended_command"] is None
    assert rebuild_candidate["command_applicable"] is False


def test_flat_stale_lot_projection_detector_identifies_ec2_style_case(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_flat_stale_projection_fixture(conn)
        preview = build_flat_stale_lot_projection_repair_preview(conn)
    finally:
        conn.close()

    assert preview["needed"] is True
    assert preview["safe_to_apply"] is True
    assert preview["repair_mode"] == "flat_stale_projection_repair"
    assert preview["stale_lot_row_count"] == 2
    assert preview["stale_lot_qty_total"] == pytest.approx(0.0004998)
    assert preview["latest_sell_client_order_id"] == "live_1777367760000_sell_ae50365f"
    assert preview["recommended_command"] == (
        "uv run bithumb-bot rebuild-position-authority --flat-stale-projection-repair --apply --yes"
    )


def test_repair_plan_reports_target_delta_terminal_flat_stale_dust_clearly(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _create_terminal_flat_stale_dust_incident(conn)
        preview = build_flat_stale_lot_projection_repair_preview(conn)
        rebuild_preview = build_position_authority_rebuild_preview(conn)
    finally:
        conn.close()

    assert preview["needed"] is True
    assert preview["safe_to_apply"] is True
    assert preview["terminal_flat_sell_detected"] is True
    assert preview["safe_to_apply_terminal_flat_projection_repair"] is True
    assert preview["current_broker_qty"] == pytest.approx(0.0)
    assert preview["current_portfolio_qty"] == pytest.approx(0.0)
    assert preview["latest_trade_asset_after"] == pytest.approx(0.0)
    assert preview["materialized_lot_projection_qty"] == pytest.approx(0.00019997)
    assert preview["stale_dust_rows_to_clear"]
    assert preview["terminal_flat_quantity_authority"]["source"] == "target_delta"
    assert preview["terminal_flat_quantity_authority"]["submitted_qty"] == pytest.approx(0.00059997)
    assert preview["terminal_flat_authority_open_exposure_qty"] == pytest.approx(0.0004)
    assert preview["terminal_flat_authority_dust_tracking_qty"] == pytest.approx(0.00019997)

    assert rebuild_preview["repair_mode"] == "flat_stale_projection_repair"
    assert rebuild_preview["safe_to_apply"] is True
    assert rebuild_preview["terminal_flat_sell_detected"] is True
    assert rebuild_preview["stale_dust_rows_to_clear"] == preview["stale_dust_rows_to_clear"]
    assert rebuild_preview["current_broker_qty"] == pytest.approx(0.0)
    assert rebuild_preview["current_portfolio_qty"] == pytest.approx(0.0)
    assert rebuild_preview["materialized_lot_projection_qty"] == pytest.approx(0.00019997)
    assert rebuild_preview["recommended_command"] == (
        "uv run bithumb-bot rebuild-position-authority --flat-stale-projection-repair --apply --yes"
    )


def test_flat_stale_lot_projection_apply_clears_projection_and_records_audit(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        stale_ids = _materialize_flat_stale_projection_fixture(conn)
        before_count = conn.execute("SELECT COUNT(*) AS cnt FROM position_authority_repairs").fetchone()["cnt"]
        result = apply_flat_stale_lot_projection_repair(conn, note="test flat stale repair")
        conn.commit()
        convergence = build_lot_projection_convergence(conn, pair=settings.PAIR)
        after_count = conn.execute("SELECT COUNT(*) AS cnt FROM position_authority_repairs").fetchone()["cnt"]
        repair_row = conn.execute(
            "SELECT repair_basis FROM position_authority_repairs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert convergence["projected_total_qty"] == pytest.approx(0.0)
    assert convergence["portfolio_qty"] == pytest.approx(0.0)
    assert convergence["converged"] is True
    assert after_count == before_count + 1
    assert result["projection_publication"]["created"] is True
    basis = json.loads(repair_row["repair_basis"])
    assert basis["latest_sell_client_order_id"] == "live_1777367760000_sell_ae50365f"
    assert basis["latest_sell_qty"] == pytest.approx(0.0004998)
    assert [row["id"] for row in basis["open_position_lots_before_repair"]] == stale_ids
    assert basis["post_repair_projection_convergence"]["converged"] is True


def test_flat_stale_lot_projection_recovery_report_clears_position_authority_blocker(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_flat_stale_projection_fixture(conn)
    finally:
        conn.close()

    before = _load_recovery_report()
    assert before["lot_projection_converged"] is False
    assert before["position_authority_rebuild_preview"]["repair_mode"] == "flat_stale_projection_repair"
    assert before["position_authority_rebuild_preview"]["safe_to_apply"] is True

    conn = ensure_db(str(recovery_db))
    try:
        apply_flat_stale_lot_projection_repair(conn)
        conn.commit()
    finally:
        conn.close()

    after = _load_recovery_report()
    assert after["lot_projection_converged"] is True
    assert after["runtime_readiness"]["projection_converged"] is True
    assert "POSITION_AUTHORITY_CORRECTION_REQUIRED" not in after["runtime_readiness"]["resume_blockers"]
    assert "AUTHORITY_PROJECTION_NON_CONVERGED" not in after["runtime_readiness"]["resume_blockers"]
    assert after["can_resume"] is True


@pytest.mark.parametrize(
    ("broker_qty", "portfolio_qty", "expected_blocker"),
    [
        (0.0001, 0.0, "broker_not_flat"),
        (0.0, 0.0001, "portfolio_not_flat"),
    ],
)
def test_flat_stale_lot_projection_unsafe_when_broker_or_portfolio_not_flat(
    recovery_db,
    broker_qty,
    portfolio_qty,
    expected_blocker,
):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_flat_stale_projection_fixture(
            conn,
            broker_qty=broker_qty,
            portfolio_qty=portfolio_qty,
        )
        preview = build_flat_stale_lot_projection_repair_preview(conn)
        count_before = conn.execute("SELECT COUNT(*) AS cnt FROM open_position_lots").fetchone()["cnt"]
        with pytest.raises(RuntimeError):
            apply_flat_stale_lot_projection_repair(conn)
        count_after = conn.execute("SELECT COUNT(*) AS cnt FROM open_position_lots").fetchone()["cnt"]
    finally:
        conn.close()

    assert preview["safe_to_apply"] is False
    assert expected_blocker in preview["blockers"]
    assert count_after == count_before


def test_flat_stale_lot_projection_unsafe_when_open_orders_exist(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_flat_stale_projection_fixture(conn, open_order_status="NEW")
        preview = build_flat_stale_lot_projection_repair_preview(conn)
        with pytest.raises(RuntimeError):
            apply_flat_stale_lot_projection_repair(conn)
    finally:
        conn.close()

    assert preview["safe_to_apply"] is False
    assert preview["terminal_flat_sell_detected"] is True
    assert preview["safe_to_apply_terminal_flat_projection_repair"] is False
    assert "open_orders_present" in preview["blockers"]


def test_flat_stale_lot_projection_unsafe_without_terminal_sell_evidence(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_flat_stale_projection_fixture(conn, include_terminal_sell=False)
        preview = build_flat_stale_lot_projection_repair_preview(conn)
        with pytest.raises(RuntimeError):
            apply_flat_stale_lot_projection_repair(conn)
    finally:
        conn.close()

    assert preview["safe_to_apply"] is False
    assert "missing_terminal_flat_sell_evidence" in preview["blockers"]


def test_flat_stale_lot_projection_repair_is_idempotent(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_flat_stale_projection_fixture(conn)
        first = apply_flat_stale_lot_projection_repair(conn)
        conn.commit()
        second_preview = build_flat_stale_lot_projection_repair_preview(conn)
        second = apply_flat_stale_lot_projection_repair(conn)
        repair_count = conn.execute("SELECT COUNT(*) AS cnt FROM position_authority_repairs").fetchone()["cnt"]
    finally:
        conn.close()

    assert first["repair"]["created"] is True
    assert second_preview["needed"] is False
    assert second["noop"] is True
    assert repair_count == 1


def test_flat_stale_lot_projection_operator_commands_and_repair_plan(recovery_db, capsys):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_flat_stale_projection_fixture(conn)
    finally:
        conn.close()

    capsys.readouterr()
    app_main(["repair-plan", "--json"])
    plan = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    candidate = next(
        item for item in plan["candidate_repairs"] if item["name"] == "flat-stale-lot-projection-repair"
    )
    assert candidate["needed"] is True
    assert candidate["active_issue"] is True
    assert candidate["safe_to_apply"] is True
    assert candidate["final_safe_to_apply"] is True
    assert candidate["recommended_command"] == (
        "uv run bithumb-bot rebuild-position-authority --flat-stale-projection-repair --apply --yes"
    )

    app_main(["rebuild-position-authority", "--flat-stale-projection-repair"])
    preview_out = capsys.readouterr().out
    assert "repair_mode=flat_stale_projection_repair" in preview_out
    assert "safe_to_apply=1" in preview_out
    assert "stale_lot_qty_total=0.000499800000" in preview_out

    app_main(["rebuild-position-authority", "--flat-stale-projection-repair", "--apply", "--yes"])
    apply_out = capsys.readouterr().out
    assert "[REBUILD-POSITION-AUTHORITY] applied" in apply_out
    assert "new_projected_total_qty=0.000000000000" in apply_out
    assert "projection_converged_after=1" in apply_out


def test_diagnose_fill_trade_linkage_reports_matchable_and_unmatchable_rows(
    recovery_db,
    capsys,
):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_filled_order(
            conn,
            client_order_id="linkable_buy",
            side="BUY",
            qty=0.0004,
            ts_ms=1_777_242_400_000,
            fill_id="linkable-fill",
            price=RESIDUAL_INCIDENT_PRICE,
        )
        conn.execute("UPDATE fills SET trade_id=NULL WHERE client_order_id='linkable_buy'")
        record_order_if_missing(
            conn,
            client_order_id="unmatched_fill",
            side="BUY",
            qty_req=0.00012345,
            price=RESIDUAL_INCIDENT_PRICE,
            ts_ms=1_777_242_500_000,
            status="FILLED",
        )
        conn.execute(
            """
            INSERT INTO fills(client_order_id, fill_id, fill_ts, price, qty, fee, trade_id)
            VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                "unmatched_fill",
                "unmatched-fill-id",
                1_777_242_500_000,
                RESIDUAL_INCIDENT_PRICE,
                0.00012345,
                0.11,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    capsys.readouterr()
    app_main(["diagnose-fill-trade-linkage", "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert payload["fills_missing_trade_id"] == 2
    assert payload["missing_but_safely_matchable"] == 1
    assert payload["ambiguous"] == 0
    assert payload["unmatchable"] == 1


def test_portfolio_anchor_projection_still_blocks_without_current_publication_attestation(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_full_rebuild_portfolio_anchor_fixture(conn, with_publication=False)

        assessment = build_position_authority_assessment(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert assessment["target_lot_provenance_kind"] == "portfolio_anchor_projection_lot"
    assert assessment["portfolio_anchor_projection_state_converged"] is False
    assert assessment["portfolio_projection_publication_present"] is False
    assert assessment["needs_correction"] is True
    assert "portfolio_anchor_projection_attestation_missing" in assessment["blockers"]
    assert not any("target_lot_qty_fill_mismatch=" in blocker for blocker in assessment["blockers"])
    assert readiness.recovery_stage == "AUTHORITY_CORRECTION_PENDING"
    assert readiness.resume_blockers == ("POSITION_AUTHORITY_CORRECTION_REQUIRED",)
    assert "portfolio_anchor_projection_attestation_missing" in readiness.structured_blockers[0]["detail"]


def test_portfolio_anchor_projection_still_blocks_when_projection_or_portfolio_not_converged(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _materialize_full_rebuild_portfolio_anchor_fixture(conn, with_publication=True)
        conn.execute(
            """
            UPDATE open_position_lots
            SET qty_open=?
            WHERE lot_rule_source_mode='full_projection_rebuild_portfolio_anchor'
            """,
            (0.00039980,),
        )
        conn.commit()

        assessment = build_position_authority_assessment(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert assessment["target_lot_provenance_kind"] == "portfolio_anchor_projection_lot"
    assert assessment["projection_state_converged"] is False
    assert assessment["portfolio_anchor_projection_state_converged"] is False
    assert assessment["needs_correction"] is True
    assert any("projection_convergence_required=" in blocker for blocker in assessment["blockers"])
    assert readiness.recovery_stage == "AUTHORITY_CORRECTION_PENDING"
    assert readiness.resume_ready is False


def test_fill_native_lot_qty_mismatch_still_blocks(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        trade = conn.execute(
            "SELECT id, ts FROM trades WHERE client_order_id='incident_buy' AND side='BUY'"
        ).fetchone()
        assert trade is not None
        conn.execute("DELETE FROM open_position_lots WHERE entry_trade_id=?", (int(trade["id"]),))
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                int(trade["id"]),
                "incident_buy",
                "fill-23",
                int(trade["ts"]),
                PRICE,
                EC2_REPRO_PORTFOLIO_QTY,
                0,
                1,
                1,
                LOT_SIZE,
                0.0002,
                0.0001,
                0.0,
                8,
                "ledger",
                "lot-native",
                "dust_tracking",
                4.23,
            ),
        )
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=EC2_REPRO_PORTFOLIO_QTY,
            asset_locked=0.0,
        )
        conn.commit()

        assessment = build_position_authority_assessment(conn)
    finally:
        conn.close()

    assert assessment["target_lot_provenance_kind"] == "fill_native_lot"
    assert assessment["target_lot_fill_qty_invariant_applies"] is True
    assert assessment["semantic_contract_check_applicable"] is True
    assert assessment["semantic_contract_check_skipped_reason"] is None
    assert assessment["semantic_contract_check_passed"] is False
    assert assessment["needs_correction"] is True
    assert any("target_lot_qty_fill_mismatch=" in blocker for blocker in assessment["blockers"])


def test_full_projection_rebuild_preview_reports_final_post_publish_state(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_filled_order(
            conn,
            client_order_id="ec2_latest_buy",
            side="BUY",
            qty=EC2_REPRO_LATEST_BUY_QTY,
            ts_ms=1_777_042_500_000,
            fill_id="ec2-latest-buy-fill",
        )
        conn.execute("DELETE FROM open_position_lots")
        stale_quantities = [0.000136748125] * 16
        assert sum(stale_quantities) == pytest.approx(0.00218797)
        _insert_stale_dust_projection_rows(conn, dust_quantities=stale_quantities)
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=EC2_REPRO_PORTFOLIO_QTY,
            asset_locked=0.0,
        )
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=EC2_REPRO_PORTFOLIO_QTY)
        record_position_authority_repair(
            conn,
            event_ts=1_777_042_600_000,
            source="test_ec2_projection_fragmentation_preview",
            reason="portfolio_anchored_authority_projection_repair",
            repair_basis={
                "event_type": "portfolio_anchored_authority_projection_repair",
                "target_trade_id": int(
                    conn.execute(
                        "SELECT id FROM trades WHERE client_order_id='ec2_latest_buy' AND side='BUY'"
                    ).fetchone()["id"]
                ),
                "target_client_order_id": "ec2_latest_buy",
                "target_remainder_qty": 0.0,
                "portfolio_qty": EC2_REPRO_PORTFOLIO_QTY,
                "projected_total_qty": 0.00218797,
                "projected_qty_excess": 0.00178812,
            },
            note="ec2 fragmented projection fixture",
        )
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=EC2_REPRO_PORTFOLIO_QTY)

        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["safe_to_apply"] is True
    assert preview["pre_gate_passed"] is True
    assert preview["final_safe_to_apply"] is True
    assert preview["truth_source"] == "broker_portfolio_anchor"
    assert preview["pre_projected_total_qty"] == pytest.approx(0.00218797)
    assert preview["post_publish_projected_total_qty"] == pytest.approx(EC2_REPRO_PORTFOLIO_QTY)
    assert preview["projection_converged_before"] is False
    assert preview["projection_converged_after_publish"] is True
    assert preview["post_publish_projection_converged"] is True
    assert preview["source_mode_of_new_rows"] == ["full_projection_rebuild_portfolio_anchor"]
    assert preview["target_lot_provenance_kind"] == "portfolio_anchor_projection_lot"
    assert preview["target_lot_fill_qty_invariant_applies"] is False
    assert preview["semantic_contract_check_applicable"] is False
    assert preview["semantic_contract_check_skipped_reason"] == "portfolio_anchor_projection_lot"
    assert preview["recommended_command"] == (
        "uv run python bot.py rebuild-position-authority --full-projection-rebuild --apply --yes"
    )
    assert preview["full_projection_rebuild_post_state_preview"]["final_gate_failures"] == []


def test_multi_lot_target_remainder_uses_other_active_qty_when_projection_is_supported(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_filled_order(
            conn,
            client_order_id="other_active_buy",
            side="BUY",
            qty=0.00039982,
            ts_ms=1_777_042_300_000,
            fill_id="other-active-fill",
        )
        _apply_filled_order(
            conn,
            client_order_id="target_buy",
            side="BUY",
            qty=0.00009998,
            ts_ms=1_777_042_400_000,
            fill_id="target-fill",
        )
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=0.00049980,
            asset_locked=0.0,
        )
        conn.commit()

        assessment = build_position_authority_assessment(conn)
    finally:
        conn.close()

    assert assessment["other_active_qty"] == pytest.approx(0.00039982)
    assert assessment["other_active_qty_supported"] is True
    assert assessment["portfolio_target_remainder_qty"] == pytest.approx(0.00009998)
    assert not any("portfolio_target_qty_mismatch=" in blocker for blocker in assessment["blockers"])


def test_multi_lot_target_remainder_fails_closed_when_other_active_qty_is_unsupported(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_filled_order(
            conn,
            client_order_id="other_active_buy",
            side="BUY",
            qty=0.00039982,
            ts_ms=1_777_042_300_000,
            fill_id="other-active-fill",
        )
        _apply_filled_order(
            conn,
            client_order_id="target_buy",
            side="BUY",
            qty=0.00009998,
            ts_ms=1_777_042_400_000,
            fill_id="target-fill",
        )
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=0.00009998,
            asset_locked=0.0,
        )
        conn.commit()

        assessment = build_position_authority_assessment(conn)
    finally:
        conn.close()

    assert assessment["other_active_qty"] == pytest.approx(0.00039982)
    assert assessment["other_active_qty_supported"] is False
    assert assessment["safe_to_correct"] is False
    assert any("other_active_qty_evidence_required=" in blocker for blocker in assessment["blockers"])


def test_full_projection_rebuild_preview_refuses_when_final_post_publish_state_fails(recovery_db, monkeypatch):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)

        def _broken_anchor(_conn, *, portfolio_qty: float, broker_qty: float) -> dict[str, object]:
            anchor = _replace_with_portfolio_anchored_projection(
                _conn,
                portfolio_qty=portfolio_qty,
                broker_qty=broker_qty,
            )
            _conn.execute(
                """
                INSERT INTO open_position_lots(
                    pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                    qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                    internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                    lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                    position_state, entry_fee_total
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    settings.PAIR,
                    int(anchor["anchor_trade_id"]),
                    str(anchor["anchor_client_order_id"]),
                    anchor["anchor_fill_id"],
                    int(anchor["anchor_fill_ts"]),
                    PRICE,
                    0.0008,
                    2,
                    0,
                    1,
                    LOT_SIZE,
                    0.0002,
                    0.0001,
                    0.0,
                    8,
                    "full_projection_rebuild_portfolio_anchor",
                    "lot-native",
                    "open_exposure",
                    0.0,
                ),
            )
            return anchor

        monkeypatch.setattr(
            "bithumb_bot.position_authority_repair._replace_with_portfolio_anchored_projection",
            _broken_anchor,
        )
        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["safe_to_apply"] is False
    assert preview["pre_gate_passed"] is True
    assert preview["final_safe_to_apply"] is False
    assert preview["recommended_command"] is None
    assert preview["preview_command"] == "uv run python bot.py rebuild-position-authority --full-projection-rebuild"
    assert preview["next_required_action"] == "review_rebuild_replay"
    assert any(
        str(item).startswith("post_rebuild_projection_converged=0")
        or str(item).startswith("post_rebuild_projected_total_qty_mismatch=")
        for item in (preview["full_projection_rebuild_post_state_preview"]["final_gate_failures"])
    )


def test_correction_preview_uses_simulated_final_post_state_before_recommending_apply(
    recovery_db, monkeypatch
):
    conn = ensure_db(str(recovery_db))
    try:
        _record_historical_sell_history(conn)
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)

        target_trade = conn.execute(
            "SELECT id, ts FROM trades WHERE client_order_id='incident_buy' AND side='BUY'"
        ).fetchone()
        assert target_trade is not None

        original_simulator = _simulate_non_full_position_authority_repair

        def _broken_simulator(_conn, *, preview, note=None):
            result = original_simulator(_conn, preview=preview, note=note)
            _conn.execute(
                """
                INSERT INTO open_position_lots(
                    pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                    qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                    internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                    lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                    position_state, entry_fee_total
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    settings.PAIR,
                    int(target_trade["id"]),
                    "incident_buy",
                    "fill-23",
                    int(target_trade["ts"]),
                    PRICE,
                    0.0004,
                    1,
                    0,
                    1,
                    LOT_SIZE,
                    0.0002,
                    0.0001,
                    0.0,
                    8,
                    "ledger",
                    "lot-native",
                    "open_exposure",
                    0.0,
                ),
            )
            return result

        monkeypatch.setattr(
            "bithumb_bot.position_authority_repair._simulate_non_full_position_authority_repair",
            _broken_simulator,
        )

        preview = build_position_authority_rebuild_preview(conn)
    finally:
        conn.close()

    assert preview["repair_mode"] == "correction"
    assert preview["safe_to_apply"] is False
    assert preview["final_safe_to_apply"] is False
    assert preview["recommended_command"] is None
    assert preview["operator_next_action"] == "review_position_authority_evidence"
    assert any(
        str(item).startswith("post_repair_projected_total_qty_mismatch=")
        for item in (preview["post_state_preview"]["final_gate_failures"] or [])
    )


def test_projection_divergence_is_reported_as_projection_invalid_not_normal_dust(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        report = _load_recovery_report()
    finally:
        conn.close()

    assert readiness.canonical_state == "PROJECTION_INVALID"
    assert readiness.residual_class == "AUTHORITY_PROJECTION_NON_CONVERGED"
    assert readiness.run_loop_allowed is False
    assert readiness.new_entry_allowed is False
    assert readiness.closeout_allowed is False
    assert readiness.operator_next_action == "review_position_authority_evidence"
    assert readiness.tradeability_operator_fields["residue_policy_state"] == "AUTHORITY_PROJECTION_NON_CONVERGED"
    assert readiness.tradeability_operator_fields["strategy_tradeability_state"] == "run_loop_blocked"
    assert report["runtime_readiness"]["canonical_state"] == "PROJECTION_INVALID"
    assert report["runtime_readiness"]["residual_class"] == "AUTHORITY_PROJECTION_NON_CONVERGED"


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        (
            lambda conn: _record_portfolio_projection_broker_evidence(
                broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY + 0.0001
            ),
            "broker_portfolio_qty_mismatch=",
        ),
        (
            lambda conn: record_order_if_missing(
                conn,
                client_order_id="unresolved-open-blocker",
                side="BUY",
                qty_req=0.0004,
                price=PRICE,
                ts_ms=1_776_905_700_050,
                status="NEW",
                internal_lot_size=LOT_SIZE,
                intended_lot_count=1,
                executable_lot_count=1,
            ),
            "unresolved_open_orders=1",
        ),
        (
            lambda conn: record_order_if_missing(
                conn,
                client_order_id="pending-submit-blocker",
                side="BUY",
                qty_req=0.0004,
                price=PRICE,
                ts_ms=1_776_905_700_000,
                status="PENDING_SUBMIT",
                internal_lot_size=LOT_SIZE,
                intended_lot_count=1,
                executable_lot_count=1,
            ),
            "pending_submit=1",
        ),
        (
            lambda conn: record_order_if_missing(
                conn,
                client_order_id="submit-unknown-blocker",
                side="BUY",
                qty_req=0.0004,
                price=PRICE,
                ts_ms=1_776_905_700_100,
                status="SUBMIT_UNKNOWN",
                internal_lot_size=LOT_SIZE,
                intended_lot_count=1,
                executable_lot_count=1,
            ),
            "submit_unknown=1",
        ),
        (
            lambda conn: record_broker_fill_observation(
                conn,
                event_ts=1_776_905_700_200,
                client_order_id="observed_fee_pending",
                exchange_order_id="observed-fee-pending-ex",
                fill_id="observed-fee-pending-fill",
                fill_ts=1_776_905_700_150,
                side="BUY",
                price=PRICE,
                qty=0.0001,
                fee=None,
                fee_status="missing",
                accounting_status="fee_pending",
                source="test_observed_projection_fragmentation",
                raw_payload={"fixture": "fee_pending_gate"},
            ),
            "fee_pending_count=1",
        ),
    ],
)
def test_recorded_projection_repair_without_publication_full_rebuild_gates_remain_strict(
    recovery_db, mutation, expected_reason
):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)
        mutation(conn)
        conn.commit()
        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["repair_mode"] == "full_projection_rebuild"
    assert preview["safe_to_apply"] is False
    assert expected_reason in preview["eligibility_reason"]


def test_live_fee_pending_fill_keeps_run_loop_alive_but_blocks_new_entries(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _record_fee_pending_buy(conn, client_order_id="auto_recovering_buy", fill_id="auto-recovering-fill")
        set_status("auto_recovering_buy", "ACCOUNTING_PENDING", conn=conn)
        conn.commit()
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert readiness.recovery_stage == "UNAPPLIED_PRINCIPAL_PENDING"
    assert readiness.resume_ready is False
    assert readiness.run_loop_allowed is True
    assert readiness.new_entry_allowed is False
    assert readiness.closeout_allowed is False
    assert readiness.fee_pending_count == 1
    assert readiness.auto_recovery_count >= 1
    assert readiness.operator_next_action == "wait_for_auto_reconcile_or_review_fee_evidence"


def test_recorded_projection_repair_without_publication_full_rebuild_refuses_remote_open_orders(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "balance_observed_ts_ms": 1_776_905_800_000,
                "remote_open_order_found": 1,
                "unresolved_open_order_count": 0,
                "submit_unknown_count": 0,
                "recovery_required_count": 0,
                "dust_residual_present": 1,
                "dust_residual_allow_resume": 1,
                "dust_effective_flat": 1,
                "dust_state": "harmless_dust",
                "dust_broker_qty": LIVE_INCIDENT_PORTFOLIO_QTY,
                "dust_local_qty": LIVE_INCIDENT_PORTFOLIO_QTY,
                "dust_delta_qty": 0.0,
                "dust_qty_gap_tolerance": 0.000001,
                "dust_qty_gap_small": 1,
                "dust_min_qty": LOT_SIZE,
            },
            now_epoch_sec=1.0,
        )
        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["safe_to_apply"] is False
    assert "remote_open_orders=1" in preview["eligibility_reason"]


def test_full_projection_rebuild_keeps_fallback_broker_qty_non_authoritative(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "balance_source": "accounts_v1_rest_snapshot",
                "balance_observed_ts_ms": 1_777_191_428_883,
                "remote_open_order_found": 0,
                "unresolved_open_order_count": 0,
                "submit_unknown_count": 0,
                "recovery_required_count": 0,
                "dust_broker_qty": 0.00079982,
                "dust_local_qty": 0.00079982,
                "dust_delta_qty": 0.0,
            },
            now_epoch_sec=1.0,
        )
        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["broker_qty"] == pytest.approx(0.00079982)
    assert preview["broker_qty_known"] is False
    assert preview["broker_qty_value_source"] == "dust_broker_qty_fallback"
    assert preview["balance_snapshot_available_for_health"] is True
    assert preview["balance_snapshot_available_for_position_rebuild"] is False
    assert preview["safe_to_apply"] is False
    assert "base_currency" in preview["missing_evidence_fields"]
    assert "quote_currency" in preview["missing_evidence_fields"]
    assert "broker_asset_qty" in preview["missing_evidence_fields"]
    assert "broker_asset_available" in preview["missing_evidence_fields"]
    assert "broker_asset_locked" in preview["missing_evidence_fields"]
    assert "broker_position_qty_evidence_missing" in preview["eligibility_reason"]


def test_full_projection_rebuild_refuses_locked_broker_asset_even_with_formal_snapshot(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "balance_observed_ts_ms": 1_776_745_500_000,
                "balance_asset_ts_ms": 1_776_745_500_000,
                "balance_source": "accounts_v1_rest_snapshot",
                "balance_source_stale": False,
                "balance_source_quote_currency": "KRW",
                "balance_source_base_currency": "BTC",
                "broker_asset_qty": LIVE_INCIDENT_PORTFOLIO_QTY,
                "broker_asset_available": LIVE_INCIDENT_PORTFOLIO_QTY - 0.0001,
                "broker_asset_locked": 0.0001,
                "broker_cash_available": OBSERVED_LIVE_CASH_KRW,
                "broker_cash_locked": 0.0,
                "remote_open_order_found": 0,
                "unresolved_open_order_count": 0,
                "submit_unknown_count": 0,
                "recovery_required_count": 0,
                "dust_broker_qty": LIVE_INCIDENT_PORTFOLIO_QTY,
                "dust_local_qty": LIVE_INCIDENT_PORTFOLIO_QTY,
                "dust_delta_qty": 0.0,
            },
            now_epoch_sec=1.0,
        )
        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["broker_qty_known"] is True
    assert preview["balance_snapshot_available_for_position_rebuild"] is False
    assert preview["position_rebuild_blockers"] == ["broker_asset_locked_nonzero"]
    assert preview["asset_locked"] == pytest.approx(0.0001)
    assert preview["safe_to_apply"] is False
    assert "broker_asset_locked_nonzero" in preview["eligibility_reason"]


def test_recorded_projection_repair_without_publication_full_rebuild_refuses_accounting_mismatch(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)
        conn.execute("DELETE FROM external_position_adjustments")
        conn.commit()
        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["safe_to_apply"] is False
    assert "accounting_projection_mismatch=" in preview["eligibility_reason"]


def test_recorded_projection_repair_without_publication_full_rebuild_refuses_recovery_required_orders(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _create_observed_live_projection_fragmentation_fixture(conn)
        record_order_if_missing(
            conn,
            client_order_id="recovery-required-blocker",
            side="SELL",
            qty_req=0.0004,
            price=PRICE,
            ts_ms=1_776_905_700_300,
            status="RECOVERY_REQUIRED",
            internal_lot_size=LOT_SIZE,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        conn.commit()
        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert preview["safe_to_apply"] is False
    assert "recovery_required_orders=1" in preview["eligibility_reason"]


def test_projection_diverged_without_fragmented_materialized_excess_does_not_require_full_rebuild(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_status("incident_buy", "FILLED", conn=conn)
        _replace_with_tracked_dust_row(conn, residual_qty=FILL_QTY - LOT_SIZE)
        _set_portfolio_asset_qty_preserving_cash(conn, asset_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)

        assessment = build_position_authority_assessment(conn)
        preview = build_position_authority_rebuild_preview(conn, full_projection_rebuild=True)
    finally:
        conn.close()

    assert assessment["projection_state_converged"] is False
    assert assessment["projection_excess_with_materialized_fragmentation"] is False
    assert assessment["needs_full_projection_rebuild"] is False
    assert "materialized_projection_fragmentation" not in assessment["diagnostic_flags"]
    assert preview["repair_mode"] == "full_projection_rebuild"
    assert preview["safe_to_apply"] is False
    assert preview["eligibility_reason"] == "full_projection_rebuild_not_required"


def test_projection_non_convergence_is_consistent_across_readiness_resume_and_reports(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=LIVE_INCIDENT_PORTFOLIO_QTY,
            asset_locked=0.0,
        )
        _insert_live_incident_stale_dust_projection(conn)
        _align_accounting_projection_to_portfolio(conn, portfolio_qty=LIVE_INCIDENT_PORTFOLIO_QTY)
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)

        readiness = compute_runtime_readiness_snapshot(conn)
        startup_reason = evaluate_startup_safety_gate()
        resume_allowed, resume_blockers = evaluate_resume_eligibility()
        restart = evaluate_restart_readiness()
        report = _load_recovery_report()
    finally:
        conn.close()

    truth_model = readiness.as_dict()["authority_truth_model"]
    structured_blocker = readiness.as_dict()["structured_blockers"][0]

    assert readiness.recovery_stage == "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING"
    assert readiness.inspect_only_mode is True
    assert truth_model["projection_truth_source"] == "open_position_lots_materialized_projection"
    assert truth_model["projection_role"] == "rebuildable_materialized_view"
    assert truth_model["repair_event_role"] == "historical_evidence_not_current_state_proof"
    assert truth_model["portfolio_asset_qty"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY)
    expected_projected_qty = LIVE_INCIDENT_STALE_DUST_QTY + FILL_QTY
    assert truth_model["projected_total_qty"] == pytest.approx(expected_projected_qty)
    assert truth_model["projection_delta_qty"] == pytest.approx(
        expected_projected_qty - LIVE_INCIDENT_PORTFOLIO_QTY
    )
    assert truth_model["inspect_only"] is True
    assert structured_blocker["reason_code"] == "POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED"
    assert structured_blocker["inspect_only"] is True
    assert structured_blocker["canonical_asset_qty"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY)
    assert structured_blocker["projected_lot_qty"] == pytest.approx(expected_projected_qty)
    assert structured_blocker["divergence_delta_qty"] == pytest.approx(
        expected_projected_qty - LIVE_INCIDENT_PORTFOLIO_QTY
    )
    assert "position_authority_projection_convergence_required=" in str(startup_reason)
    assert resume_allowed is False
    assert any(
        blocker.reason_code == "POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED"
        for blocker in resume_blockers
    )
    normalized_position_item = next(item for item in restart if item[0] == "normalized position state")
    assert normalized_position_item[1] is False
    assert report["runtime_readiness"]["recovery_stage"] == "AUTHORITY_PROJECTION_NON_CONVERGED_PENDING"
    assert report["runtime_readiness"]["inspect_only_mode"] is True
    assert report["runtime_readiness"]["structured_blockers"][0]["reason_code"] == (
        "POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED"
    )
    assert report["runtime_readiness"]["authority_truth_model"]["projection_delta_qty"] == pytest.approx(
        expected_projected_qty - LIVE_INCIDENT_PORTFOLIO_QTY
    )
    assert report["resume_allowed"] is False
    assert report["can_resume"] is False
    assert report["resume_blocked_reason"] == "resume blocked by non-converged lot projection"
    assert report["operator_next_action"] == "position_authority_projection_convergence_required"


def test_external_position_accounting_repair_blocks_resume_until_recorded_for_historical_split(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _create_portfolio_projection_divergence(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "balance_observed_ts_ms": 1_776_745_500_000,
                "balance_asset_ts_ms": 1_776_745_500_000,
                "balance_source": "accounts_v1_rest_snapshot",
                "balance_source_stale": False,
                "balance_source_quote_currency": "KRW",
                "balance_source_base_currency": "BTC",
                "broker_asset_qty": PORTFOLIO_DIVERGENCE_QTY,
                "broker_asset_available": PORTFOLIO_DIVERGENCE_QTY,
                "broker_asset_locked": 0.0,
                "broker_cash_available": OBSERVED_LIVE_CASH_KRW,
                "broker_cash_locked": 0.0,
                "remote_open_order_found": 0,
                "unresolved_open_order_count": 0,
                "submit_unknown_count": 0,
                "recovery_required_count": 0,
                "dust_residual_present": 1,
                "dust_residual_allow_resume": 1,
                "dust_effective_flat": 1,
                "dust_state": "harmless_dust",
                "dust_broker_qty": PORTFOLIO_DIVERGENCE_QTY,
                "dust_local_qty": PORTFOLIO_DIVERGENCE_QTY,
                "dust_delta_qty": 0.0,
                "dust_qty_gap_tolerance": 0.000001,
                "dust_qty_gap_small": 1,
                "dust_min_qty": LOT_SIZE,
            },
            now_epoch_sec=1.0,
        )
        apply_position_authority_rebuild(conn)
        conn.execute("DELETE FROM external_position_adjustments")
        conn.commit()

        before = compute_runtime_readiness_snapshot(conn)
        preview = build_external_position_accounting_repair_preview(conn)
        result = apply_external_position_accounting_repair(conn, note="historical off-bot reduction")
        conn.commit()
        after = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert before.recovery_stage == "ACCOUNTING_EXTERNAL_POSITION_REPAIR_PENDING"
    assert before.resume_blockers == ("EXTERNAL_POSITION_ACCOUNTING_REPAIR_REQUIRED",)
    assert preview["needs_repair"] is True
    assert preview["safe_to_apply"] is True
    assert preview["asset_qty_delta"] == pytest.approx(-0.00020004)
    assert result["adjustment"]["reason"] == "external_position_accounting_repair"
    assert after.recovery_stage == "NON_EXECUTABLE_RESIDUAL_HOLDINGS"
    assert after.resume_ready is False
    assert after.recommended_command == "uv run bithumb-bot residual-closeout-plan"


def test_fee_gap_deadlock_reports_authority_correction_as_next_stage(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _record_historical_sell_history(conn)
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={
                "fee_gap_recovery_required": 1,
                "material_zero_fee_fill_count": 1,
                "material_zero_fee_fill_latest_ts": 1_699_999_000_100,
                "fee_gap_adjustment_count": 1,
                "fee_gap_adjustment_total_krw": 4.23,
                "fee_gap_adjustment_latest_event_ts": 1_700_000_010_000,
                "external_cash_adjustment_reason": "reconcile_fee_gap_cash_drift",
            },
            now_epoch_sec=1.0,
        )

        readiness = compute_runtime_readiness_snapshot(conn)
        fee_gap = build_fee_gap_accounting_repair_preview(conn)
        startup_reason = evaluate_startup_safety_gate()
        resume_allowed, resume_blockers = evaluate_resume_eligibility()
    finally:
        conn.close()

    assert readiness.recovery_stage == "AUTHORITY_CORRECTION_PENDING"
    assert readiness.resume_blockers == ("POSITION_AUTHORITY_CORRECTION_REQUIRED",)
    assert fee_gap["needs_repair"] is True
    assert fee_gap["safe_to_apply"] is False
    assert fee_gap["blocked_by_authority_correction"] is True
    assert fee_gap["next_required_action"] == "rebuild_position_authority"
    assert "position_authority_correction_required=" in str(startup_reason)
    assert resume_allowed is False
    assert any(blocker.reason_code == "POSITION_AUTHORITY_CORRECTION_REQUIRED" for blocker in resume_blockers)


def test_fee_pending_and_authority_repair_resume_open_position_with_deferred_fee_gap(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _record_historical_sell_history(conn)
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="FEE_GAP_RECOVERY_REQUIRED",
            metadata={
                "fee_gap_recovery_required": 1,
                "material_zero_fee_fill_count": 1,
                "material_zero_fee_fill_latest_ts": 1_699_999_000_100,
                "fee_gap_adjustment_count": 1,
                "fee_gap_adjustment_total_krw": 4.23,
                "fee_gap_adjustment_latest_event_ts": 1_700_000_010_000,
                "external_cash_adjustment_reason": "reconcile_fee_gap_cash_drift",
            },
            now_epoch_sec=1.0,
        )

        before = compute_runtime_readiness_snapshot(conn)
        repair = apply_position_authority_rebuild(conn)
        conn.commit()
        after = compute_runtime_readiness_snapshot(conn)
        fee_gap = build_fee_gap_accounting_repair_preview(conn)
        lots = summarize_position_lots(conn, pair=settings.PAIR)
        startup_reason = evaluate_startup_safety_gate()
        resume_allowed, resume_blockers = evaluate_resume_eligibility()
    finally:
        conn.close()

    assert before.recovery_stage == "AUTHORITY_CORRECTION_PENDING"
    assert repair["repair"]["reason"] == "accounted_buy_fill_authority_correction"
    assert lots.open_lot_count >= 1
    assert lots.dust_tracking_lot_count == 1
    assert after.recovery_stage == "RESUME_READY_WITH_DEFERRED_HISTORICAL_DEBT"
    assert after.resume_ready is True
    assert after.resume_blockers == ()
    assert after.blocker_categories == ("advisory_historical_debt",)
    assert fee_gap["needs_repair"] is True
    assert fee_gap["safe_to_apply"] is False
    assert fee_gap["repair_eligibility_state"] == "blocked_until_flattened"
    assert fee_gap["resume_policy"] == "defer_for_open_position_management"
    assert fee_gap["resume_blocking"] is False
    assert fee_gap["closeout_blocking"] is True
    assert fee_gap["blocked_by_open_exposure"] is True
    assert fee_gap["blocked_by_dust_residue"] is True
    assert startup_reason is None
    assert resume_allowed is True
    assert resume_blockers == []


def test_partial_close_residual_normalization_replays_buy_and_sell_authority(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)
        apply_position_authority_rebuild(conn)
        _apply_fee_pending_sell(conn)
        conn.execute("DELETE FROM trade_lifecycles WHERE exit_client_order_id='incident_sell'")
        conn.commit()

        before = compute_runtime_readiness_snapshot(conn)
        conn.commit()
        assessment = build_position_authority_assessment(conn)
        preview = build_position_authority_rebuild_preview(conn)
        result = apply_position_authority_rebuild(conn)
        conn.commit()
        after = compute_runtime_readiness_snapshot(conn)
        rows = conn.execute(
            """
            SELECT position_state, qty_open, executable_lot_count, dust_tracking_lot_count, internal_lot_size
            FROM open_position_lots
            WHERE entry_client_order_id='incident_buy'
            ORDER BY id ASC
            """
        ).fetchall()
        repair = conn.execute(
            """
            SELECT reason, repair_basis
            FROM position_authority_repairs
            ORDER BY event_ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        startup_reason = evaluate_startup_safety_gate()
        resume_allowed, resume_blockers = evaluate_resume_eligibility()
        restart = evaluate_restart_readiness()
        health = get_health_status()
    finally:
        conn.close()

    assert before.recovery_stage == "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING"
    assert assessment["needs_residual_normalization"] is True
    assert assessment["safe_to_normalize_residual"] is True
    assert assessment["expected_residual_qty"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert preview["repair_mode"] == "residual_normalization"
    assert preview["safe_to_apply"] is True
    assert result["repair"]["reason"] == "partial_close_residual_authority_normalization"
    assert repair["reason"] == "partial_close_residual_authority_normalization"
    basis = json.loads(repair["repair_basis"])
    assert basis["event_type"] == "partial_close_residual_authority_normalization"
    assert basis["target_trade_id"] == assessment["target_trade_id"]
    assert basis["expected_residual_qty"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert len(rows) == 1
    assert rows[0]["position_state"] == "dust_tracking"
    assert rows[0]["qty_open"] == pytest.approx(FILL_QTY - LOT_SIZE)
    assert rows[0]["executable_lot_count"] == 0
    assert rows[0]["dust_tracking_lot_count"] == 1
    assert after.recovery_stage == "RESUME_READY"
    assert after.resume_ready is True
    assert startup_reason is None
    assert resume_allowed is True
    assert resume_blockers == []
    assert health["startup_gate_reason"] is None
    assert health["resume_gate_blocked"] is False
    assert all(ok for _label, ok, _detail in restart)


def test_partial_close_residual_repair_event_does_not_replace_state_convergence(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        _apply_fee_pending_sell(conn)
        sell_ids = [
            int(row["id"])
            for row in conn.execute(
                "SELECT id FROM trades WHERE side='SELL' ORDER BY id ASC"
            ).fetchall()
        ]
        target_trade = conn.execute(
            "SELECT id FROM trades WHERE client_order_id='incident_buy' AND side='BUY'"
        ).fetchone()
        conn.execute("DELETE FROM trade_lifecycles WHERE exit_client_order_id='incident_sell'")
        record_position_authority_repair(
            conn,
            event_ts=1_700_000_200_000,
            source="test_stale_repair_event",
            reason="partial_close_residual_authority_normalization",
            repair_basis={
                "event_type": "partial_close_residual_authority_normalization",
                "target_trade_id": int(target_trade["id"]),
                "sell_trade_ids": sell_ids,
                "expected_residual_qty": FILL_QTY - LOT_SIZE,
            },
        )
        conn.commit()

        assessment = build_position_authority_assessment(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert assessment["partial_close_residual_candidate"] is True
    assert assessment["residual_repair_event_present"] is True
    assert assessment["residual_state_converged"] is False
    assert assessment["needs_residual_normalization"] is True
    assert readiness.recovery_stage == "AUTHORITY_RESIDUAL_NORMALIZATION_PENDING"
    assert readiness.resume_blockers == ("POSITION_AUTHORITY_RESIDUAL_NORMALIZATION_REQUIRED",)


def test_fee_pending_existing_sell_fee_repair_replays_lifecycle_projection(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="fee_buy",
            side="BUY",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_000_000_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="fee_buy",
            side="BUY",
            fill_id="fee-buy-fill",
            fill_ts=1_700_000_000_050,
            price=PRICE,
            qty=LOT_SIZE,
            fee=1.0,
            allow_entry_decision_fallback=False,
        )
        record_order_if_missing(
            conn,
            client_order_id="fee_sell",
            side="SELL",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_000_100_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_principal_with_pending_fee(
            conn,
            client_order_id="fee_sell",
            side="SELL",
            fill_id="fee-sell-fill",
            fill_ts=1_700_000_100_050,
            price=PRICE,
            qty=LOT_SIZE,
            fee=0.0,
            fee_status="zero_reported",
            allow_entry_decision_fallback=False,
        )
        record_broker_fill_observation(
            conn,
            event_ts=1_700_000_100_100,
            client_order_id="fee_sell",
            exchange_order_id="fee-sell-ex",
            fill_id="fee-sell-fill",
            fill_ts=1_700_000_100_050,
            side="SELL",
            price=PRICE,
            qty=LOT_SIZE,
            fee=None,
            fee_status="order_level_candidate",
            accounting_status="fee_pending",
            source="test_fee_pending_existing_fill",
            parse_warnings=("missing_fee_field", "order_level_fee_candidate:paid_fee"),
            raw_payload={"trade": {"uuid": "fee-sell-fill"}, "order_fee_fields": {"paid_fee": "17.73"}},
        )
        conn.commit()

        before_lifecycle = conn.execute(
            "SELECT fee_total, net_pnl FROM trade_lifecycles WHERE exit_client_order_id='fee_sell'"
        ).fetchone()
        result = apply_fee_pending_accounting_repair(
            conn,
            client_order_id="fee_sell",
            fill_id="fee-sell-fill",
            fee=17.73,
            fee_provenance="order_level_paid_fee",
        )
        conn.commit()
        after_lifecycle = conn.execute(
            "SELECT fee_total, net_pnl FROM trade_lifecycles WHERE exit_client_order_id='fee_sell'"
        ).fetchone()
        sell_fill = conn.execute("SELECT fee FROM fills WHERE client_order_id='fee_sell'").fetchone()
        sell_trade = conn.execute("SELECT fee FROM trades WHERE client_order_id='fee_sell'").fetchone()
    finally:
        conn.close()

    assert before_lifecycle["fee_total"] == pytest.approx(1.0)
    assert result["applied_fill"]["repair_mode"] == "complete_existing_fill_fee"
    assert result["projection_replay"]["replayed_buy_count"] == 1
    assert result["projection_replay"]["replayed_sell_count"] == 1
    assert sell_fill["fee"] == pytest.approx(17.73)
    assert sell_trade["fee"] == pytest.approx(17.73)
    assert after_lifecycle["fee_total"] == pytest.approx(18.73)
    assert after_lifecycle["net_pnl"] == pytest.approx(-18.73)


def test_projection_replay_removes_stale_latest_buy_open_projection(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _record_historical_sell_history(conn)
        record_order_if_missing(
            conn,
            client_order_id="latest_buy",
            side="BUY",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_000_000_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="latest_buy",
            side="BUY",
            fill_id="latest-buy-fill",
            fill_ts=1_700_000_000_050,
            price=PRICE,
            qty=LOT_SIZE,
            fee=1.0,
            allow_entry_decision_fallback=False,
        )
        record_order_if_missing(
            conn,
            client_order_id="latest_sell",
            side="SELL",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_000_100_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            effective_min_trade_qty=0.0002,
            qty_step=0.0001,
            min_notional_krw=0.0,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="latest_sell",
            side="SELL",
            fill_id="latest-sell-fill",
            fill_ts=1_700_000_100_050,
            price=PRICE,
            qty=LOT_SIZE,
            fee=1.0,
            allow_entry_decision_fallback=False,
        )
        conn.execute("DELETE FROM trade_lifecycles WHERE exit_client_order_id='latest_sell'")
        latest_buy_trade = conn.execute(
            "SELECT id, ts FROM trades WHERE client_order_id='latest_buy'"
        ).fetchone()
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
                qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
                internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
                lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
                position_state, entry_fee_total
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                settings.PAIR,
                int(latest_buy_trade["id"]),
                "latest_buy",
                "latest-buy-fill",
                int(latest_buy_trade["ts"]),
                PRICE,
                LOT_SIZE,
                1,
                0,
                1,
                LOT_SIZE,
                0.0002,
                0.0001,
                0.0,
                8,
                "ledger",
                "lot-native",
                "open_exposure",
                1.0,
            ),
        )
        conn.commit()

        stale = summarize_position_lots(conn, pair=settings.PAIR)
        replay = rebuild_lifecycle_projections_from_trades(conn, pair=settings.PAIR)
        conn.commit()
        repaired = summarize_position_lots(conn, pair=settings.PAIR)
        latest_lifecycle = conn.execute(
            "SELECT COUNT(*) AS cnt FROM trade_lifecycles WHERE exit_client_order_id='latest_sell'"
        ).fetchone()
        latest_lot = conn.execute(
            "SELECT COUNT(*) AS cnt FROM open_position_lots WHERE entry_client_order_id='latest_buy'"
        ).fetchone()
    finally:
        conn.close()

    assert stale.open_lot_count == 1
    assert replay.replayed_buy_count == 2
    assert replay.replayed_sell_count == 2
    assert repaired.open_lot_count == 0
    assert repaired.raw_total_asset_qty == pytest.approx(0.0)
    assert latest_lifecycle["cnt"] == 1
    assert latest_lot["cnt"] == 0


def test_dust_only_fee_gap_deadlock_converges_through_canonical_execution_flat_state(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn)
        _corrupt_latest_buy_lot_as_incident(conn)
        apply_position_authority_rebuild(conn)
        _apply_fee_pending_sell(conn)
        conn.commit()
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="FEE_GAP_RECOVERY_REQUIRED",
            metadata={
                "fee_gap_recovery_required": 1,
                "material_zero_fee_fill_count": 1,
                "material_zero_fee_fill_latest_ts": 1_700_000_100_050,
                "fee_gap_adjustment_count": 1,
                "fee_gap_adjustment_total_krw": 17.73,
                "fee_gap_adjustment_latest_event_ts": 1_700_000_200_000,
                "external_cash_adjustment_reason": "reconcile_fee_gap_cash_drift",
            },
            now_epoch_sec=1.0,
        )

        readiness = compute_runtime_readiness_snapshot(conn)
        fee_gap = build_fee_gap_accounting_repair_preview(conn)
        resume_allowed_before, resume_blockers_before = evaluate_resume_eligibility()
        repair = apply_fee_gap_accounting_repair(conn)
        conn.commit()
        after = compute_runtime_readiness_snapshot(conn)
        fee_gap_after = build_fee_gap_accounting_repair_preview(conn)
    finally:
        conn.close()

    assert readiness.canonical_state == "DUST_ONLY_TRACKED"
    assert readiness.execution_flat is True
    assert readiness.accounting_flat is False
    assert readiness.recovery_stage == "HISTORICAL_FEE_GAP_PENDING"
    assert fee_gap["canonical_state"] == "DUST_ONLY_TRACKED"
    assert fee_gap["execution_flat"] is True
    assert fee_gap["accounting_flat"] is False
    assert fee_gap["needs_repair"] is True
    assert fee_gap["safe_to_apply"] is True
    assert fee_gap["repair_eligibility_state"] == "safe_to_apply_with_tracked_dust"
    assert fee_gap["next_required_action"] == "apply_fee_gap_accounting_repair"
    assert resume_allowed_before is False
    assert any(blocker.reason_code == "FEE_GAP_RECOVERY_REQUIRED" for blocker in resume_blockers_before)
    assert repair["repair"]["created"] is True
    assert after.recovery_stage == "RESUME_READY"
    assert after.resume_ready is True
    assert after.canonical_state == "DUST_ONLY_TRACKED"
    assert after.residual_class == "HARMLESS_DUST_TREAT_AS_FLAT"
    assert after.run_loop_allowed is True
    assert after.new_entry_allowed is True
    assert after.closeout_allowed is False
    assert after.execution_flat is True
    assert after.accounting_flat is False
    assert after.operator_action_required is False
    assert after.why_not == "closeout_blocked:dust_only_remainder"
    assert fee_gap_after["needs_repair"] is False
    assert fee_gap_after["already_repaired"] is True


def test_recovery_policy_cross_module_consistency_for_representative_states(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        flat = compute_runtime_readiness_snapshot(conn)
        flat_fee_gap = build_fee_gap_accounting_repair_preview(conn)

        _apply_fee_pending_buy(conn, client_order_id="open_buy", fill_id="open-fill")
        open_readiness = compute_runtime_readiness_snapshot(conn)
        open_fee_gap = build_fee_gap_accounting_repair_preview(conn)

        _apply_fee_pending_sell(conn, client_order_id="open_sell", fill_id="open-sell-fill")
        conn.commit()
        dust_readiness = compute_runtime_readiness_snapshot(conn)
        dust_fee_gap = build_fee_gap_accounting_repair_preview(conn)

        conn.execute("DELETE FROM open_position_lots")
        set_portfolio_breakdown(
            conn,
            cash_available=settings.START_CASH_KRW,
            cash_locked=0.0,
            asset_available=0.123,
            asset_locked=0.0,
        )
        conn.commit()
        non_exec_readiness = compute_runtime_readiness_snapshot(conn)
        non_exec_fee_gap = build_fee_gap_accounting_repair_preview(conn)
    finally:
        conn.close()

    cases = [
        (flat, flat_fee_gap, "FLAT", "NONE", False, True, True, True, False),
        (
            open_readiness,
            open_fee_gap,
            "OPEN_EXECUTABLE",
            "EXECUTABLE_OPEN_EXPOSURE",
            True,
            False,
            False,
            False,
            False,
        ),
        (
            dust_readiness,
            dust_fee_gap,
            "DUST_ONLY_TRACKED",
            "HARMLESS_DUST_TREAT_AS_FLAT",
            False,
            True,
            False,
            True,
            False,
        ),
        (
            non_exec_readiness,
            non_exec_fee_gap,
            "AUTHORITY_MISSING",
            "NON_EXECUTABLE_RESIDUE_REQUIRES_OPERATOR_ACTION",
            False,
            False,
            False,
            False,
            True,
        ),
    ]
    for (
        readiness,
        fee_gap,
        canonical_state,
        residual_class,
        position_management_allowed,
        execution_flat,
        accounting_flat,
        new_entry_allowed,
        operator_action_required,
    ) in cases:
        assert readiness.canonical_state == canonical_state
        assert readiness.residual_class == residual_class
        if readiness.recovery_stage == "ACCOUNTING_AUTO_RECOVERING":
            assert readiness.run_loop_allowed is True
            assert readiness.resume_ready is False
        else:
            assert readiness.run_loop_allowed is readiness.resume_ready
        assert readiness.position_management_allowed is position_management_allowed
        assert readiness.new_entry_allowed is new_entry_allowed
        assert readiness.operator_action_required is operator_action_required
        assert readiness.execution_flat is execution_flat
        assert readiness.accounting_flat is accounting_flat
        assert readiness.tradeability.as_dict()["residual_class"] == residual_class
        assert fee_gap["canonical_state"] == canonical_state
        assert fee_gap["execution_flat"] is execution_flat
        assert fee_gap["accounting_flat"] is accounting_flat


def test_canonical_open_exposure_clears_stale_risk_mismatch_and_resumes_position_management(
    recovery_db, monkeypatch, capsys
):
    monkeypatch.setattr("bithumb_bot.app.write_json_atomic", lambda *_args, **_kwargs: None)
    conn = ensure_db(str(recovery_db))
    try:
        _apply_fee_pending_buy(conn, client_order_id="ec2_carry_buy", fill_id="ec2-carry-fill")
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="POSITION_AUTHORITY_REBUILD_COMPLETED",
        metadata={
            "balance_split_mismatch_count": 0,
            "balance_source": "accounts_v1_rest_snapshot",
            "balance_observed_ts_ms": 1_777_104_360_500,
            "balance_asset_ts_ms": 1_777_104_360_500,
            "balance_source_base_currency": "BTC",
            "balance_source_quote_currency": "KRW",
            "broker_asset_qty": FILL_QTY,
            "broker_asset_available": FILL_QTY,
            "broker_asset_locked": 0.0,
            "broker_cash_available": 0.0,
            "broker_cash_locked": 0.0,
        },
        now_epoch_sec=1.0,
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="RISK_STATE_MISMATCH cash_delta=69623.301030 asset_delta=-0.000599990000",
        reason_code="RISK_STATE_MISMATCH",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    resume_allowed, blockers = evaluate_resume_eligibility()
    state = runtime_state.snapshot()
    report = _load_recovery_report()

    assert resume_allowed is True
    assert blockers == []
    assert state.halt_new_orders_blocked is False
    assert state.halt_state_unresolved is False
    assert report["resume_blockers"] == []
    assert "HALT_RISK_OPEN_POSITION" not in report["resume_blockers"]
    assert report["runtime_readiness"]["canonical_state"] == "OPEN_EXECUTABLE"
    assert report["runtime_readiness"]["run_loop_allowed"] is True
    assert report["runtime_readiness"]["position_management_allowed"] is True
    assert report["runtime_readiness"]["new_entry_allowed"] is False
    assert report["runtime_readiness"]["closeout_allowed"] is True
    assert report["stale_halt_clear_diagnostics"]["stale_halt_clear_candidate"] is True
    assert report["stale_halt_clear_diagnostics"]["stale_halt_clear_allowed"] is True
    assert report["stale_halt_clear_diagnostics"]["stale_halt_clear_current_evidence_converged"] is True
    assert report["stale_halt_clear_diagnostics"]["halt_reason_current_evidence"] == "stale"
    assert report["stale_halt_clear_diagnostics"]["stale_halt_clear_blockers"] == []
    assert report["recovery_policy"]["primary_incident_class"] == "CANONICAL_OPEN_POSITION"
    assert report["recovery_policy"]["recommended_mode"] == "position_management"
    assert report["recovery_policy"]["position_management_allowed"] is True
    assert report["recovery_policy"]["flatten_primary_recommendation"] is False
    assert report["operator_next_action"] == "resume_position_management"
    assert report["recommended_command"] == "uv run python bot.py resume"

    app_main(["restart-checklist"])
    checklist_out = capsys.readouterr().out
    assert (
        "safe_to_resume=1" in checklist_out
        or "position_management_allowed=1" in checklist_out
    )
    assert "run_loop_allowed=1" in checklist_out
    assert "position_management_allowed=1" in checklist_out
    assert "new_entry_allowed=0" in checklist_out
    assert "closeout_allowed=1" in checklist_out
    assert "stale_halt_clear_candidate=1" in checklist_out
    assert "stale_halt_clear_allowed=1" in checklist_out
    assert "halt_reason_current_evidence=stale" in checklist_out
    assert "stale_halt_clear_blockers=none" in checklist_out
    assert "primary_incident_class=CANONICAL_OPEN_POSITION" in checklist_out
    assert "recommended_mode=position_management" in checklist_out
    assert "recommended_action=resume_position_management" in checklist_out
    assert "recommended_command=uv run python bot.py resume" in checklist_out
    assert "flatten_primary_recommendation=0" in checklist_out

    app_main(["recovery-report"])
    report_out = capsys.readouterr().out
    assert "recommended_action=resume_position_management" in report_out
    assert "recommended_command=uv run python bot.py resume" in report_out
    assert "flatten_primary_recommendation=0" in report_out
    assert "stale_halt_clear_candidate=1" in report_out
    assert "stale_halt_clear_allowed=1" in report_out
    assert "halt_reason_current_evidence=stale" in report_out
    assert "stale_halt_clear_blockers=none" in report_out

    app_main(["repair-plan"])
    repair_plan_out = capsys.readouterr().out
    assert "primary_incident_class=CANONICAL_OPEN_POSITION" in repair_plan_out
    assert "recommended_mode=position_management" in repair_plan_out
    assert "position_management_allowed=1" in repair_plan_out
    assert "recommended_action=resume_position_management" in repair_plan_out
    assert "recommended_command=uv run python bot.py resume" in repair_plan_out
    assert "flatten_primary_recommendation=0" in repair_plan_out

    app_main(["health"])
    health_out = capsys.readouterr().out
    assert "run_loop_allowed=1" in health_out
    assert "position_management_allowed=1" in health_out
    assert "new_entry_allowed=0" in health_out
    assert "closeout_allowed=1" in health_out
    assert "stale_halt_clear_candidate=1" in health_out
    assert "stale_halt_clear_allowed=1" in health_out
    assert "halt_reason_current_evidence=stale" in health_out
    assert "stale_halt_clear_blockers=none" in health_out
    assert "recommended_action=resume_position_management" in health_out
    assert "recommended_command=uv run python bot.py resume" in health_out


def test_fee_pending_repair_remains_applicable_when_fill_exists_but_fee_incomplete(recovery_db):
    conn = ensure_db(str(recovery_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="fee_incomplete_existing_fill",
            side="BUY",
            qty_req=LOT_SIZE,
            price=PRICE,
            ts_ms=1_700_002_000_000,
            status="NEW",
            internal_lot_size=LOT_SIZE,
            intended_lot_count=1,
            executable_lot_count=1,
        )
        apply_fill_principal_with_pending_fee(
            conn,
            client_order_id="fee_incomplete_existing_fill",
            side="BUY",
            fill_id="fee-incomplete-fill",
            fill_ts=1_700_002_000_100,
            price=PRICE,
            qty=LOT_SIZE,
            fee=None,
            fee_status="missing",
        )
        set_status("fee_incomplete_existing_fill", "FILLED", conn=conn)
        record_broker_fill_observation(
            conn,
            event_ts=1_700_002_000_200,
            client_order_id="fee_incomplete_existing_fill",
            exchange_order_id="ex-fee-incomplete",
            fill_id="fee-incomplete-fill",
            fill_ts=1_700_002_000_100,
            side="BUY",
            price=PRICE,
            qty=LOT_SIZE,
            fee=None,
            fee_status="missing",
            accounting_status="fee_pending",
            source="test_existing_fill_fee_pending",
            raw_payload={"fixture": "existing-fill-fee-pending"},
        )
        conn.commit()
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="FILL_FEE_PENDING_RECOVERY_REQUIRED",
            metadata={"fee_pending_auto_recovering": 1},
            now_epoch_sec=1.0,
        )

        readiness = compute_runtime_readiness_snapshot(conn)
        preview = build_fee_pending_accounting_repair_preview(
            conn,
            client_order_id="fee_incomplete_existing_fill",
            fill_id="fee-incomplete-fill",
            fee=3.21,
            fee_provenance="operator_checked_bithumb_trade_history",
        )
        result = apply_fee_pending_accounting_repair(
            conn,
            client_order_id="fee_incomplete_existing_fill",
            fill_id="fee-incomplete-fill",
            fee=3.21,
            fee_provenance="operator_checked_bithumb_trade_history",
        )
        conn.commit()
        fill_count = conn.execute(
            "SELECT COUNT(*) AS cnt, SUM(fee) AS fee_total FROM fills WHERE client_order_id='fee_incomplete_existing_fill'"
        ).fetchone()
        complete_observation = conn.execute(
            """
            SELECT fee_status, accounting_status
            FROM broker_fill_observations
            WHERE client_order_id='fee_incomplete_existing_fill'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert readiness.fee_pending_count == 0
    assert readiness.recovery_stage == "FEE_FINALIZATION_PENDING"
    assert readiness.fill_accounting_incident_summary["principal_applied_fee_pending_count"] == 1
    assert preview["needs_repair"] is True
    assert preview["safe_to_apply"] is True
    assert preview["repair_mode"] == "complete_existing_fill_fee"
    assert "fill_already_accounted" not in preview["eligibility_reason"]
    assert result["applied_fill"]["repair_mode"] == "complete_existing_fill_fee"
    assert fill_count["cnt"] == 1
    assert fill_count["fee_total"] == pytest.approx(3.21)
    assert complete_observation["fee_status"] == "operator_confirmed"
    assert complete_observation["accounting_status"] == "accounting_complete"
