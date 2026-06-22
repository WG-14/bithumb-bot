from __future__ import annotations

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, init_portfolio, set_portfolio_breakdown
from bithumb_bot.lifecycle import DUST_TRACKING_STATE, summarize_non_executable_residuals
from bithumb_bot.runtime_readiness import compute_runtime_readiness_snapshot, evaluate_clean_account_gate


RESIDUAL_QTY = 0.00009996
PRICE = 96_688_675.47018808
MIN_QTY = 0.0001
MIN_NOTIONAL = 5000.0


@pytest.fixture
def residual_db(tmp_path, monkeypatch):
    original = {
        "DB_PATH": settings.DB_PATH,
        "MODE": settings.MODE,
        "PAIR": settings.PAIR,
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
        "RESIDUAL_INVENTORY_MODE": settings.RESIDUAL_INVENTORY_MODE,
    }
    db_path = tmp_path / "residual-readiness.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", MIN_QTY)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", MIN_QTY)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", MIN_NOTIONAL)
    object.__setattr__(settings, "RESIDUAL_INVENTORY_MODE", "track")
    runtime_state.enable_trading()
    try:
        yield db_path
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)
        runtime_state.enable_trading()


def _seed_tracked_residual(conn, *, broker_qty: float = RESIDUAL_QTY, portfolio_qty: float = RESIDUAL_QTY) -> None:
    init_portfolio(conn)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=portfolio_qty,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts, entry_price,
            qty_open, executable_lot_count, dust_tracking_lot_count, lot_semantic_version,
            internal_lot_size, lot_min_qty, lot_qty_step, lot_min_notional_krw,
            lot_max_qty_decimals, lot_rule_source_mode, position_semantic_basis,
            position_state, entry_fee_total, entry_decision_linkage
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            1,
            "incident_buy",
            "incident_fill",
            1_700_000_000_000,
            PRICE,
            RESIDUAL_QTY,
            0,
            1,
            1,
            0.0003,
            MIN_QTY,
            MIN_QTY,
            MIN_NOTIONAL,
            8,
            "ledger",
            "lot-native",
            DUST_TRACKING_STATE,
            0.0,
            "direct",
        ),
    )
    conn.commit()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_source": "accounts_v1_rest_snapshot",
            "balance_observed_ts_ms": 1_700_000_000_100,
            "base_currency": "BTC",
            "quote_currency": "KRW",
            "broker_asset_qty": broker_qty,
            "broker_asset_available": broker_qty,
            "broker_asset_locked": 0.0,
        },
        now_epoch_sec=0.0,
    )


def test_sub_min_qty_high_notional_residual_is_not_clean_account_sellable(residual_db):
    conn = ensure_db(str(residual_db))
    try:
        _seed_tracked_residual(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        gate = evaluate_clean_account_gate(readiness)
    finally:
        conn.close()

    assert readiness.residual_inventory.exchange_sellable is False
    assert readiness.residual_inventory.residual_notional_krw == pytest.approx(9665.0)
    assert readiness.residual_disposition.disposition == "TRACKED_NON_EXECUTABLE"
    assert gate.allowed is True
    assert gate.reason_code != "sellable_residual_clean_account_required"
    assert gate.sellable_residual_qty == pytest.approx(0.0)
    assert "flatten-position" not in str(gate.recommended_command)


def test_true_dust_ledger_split_residual_is_trackable_when_converged(residual_db):
    conn = ensure_db(str(residual_db))
    try:
        _seed_tracked_residual(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert readiness.residual_inventory_state == "RESIDUAL_INVENTORY_TRACKED"
    assert readiness.residual_disposition.disposition == "TRACKED_NON_EXECUTABLE"
    assert readiness.run_loop_allowed is True
    assert readiness.residual_inventory_policy_allows_buy is True


def test_true_dust_does_not_become_exchange_sellable(residual_db):
    conn = ensure_db(str(residual_db))
    try:
        _seed_tracked_residual(conn)
        residual = summarize_non_executable_residuals(conn, pair=settings.PAIR)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert "TRUE_DUST" in residual.residual_classes
    assert residual.exchange_sellable is False
    assert readiness.residual_disposition.exchange_sellable is False
    assert readiness.residual_disposition.sell_allowed is False


def test_true_dust_with_projection_mismatch_blocks_tracking(residual_db):
    conn = ensure_db(str(residual_db))
    try:
        _seed_tracked_residual(conn, broker_qty=RESIDUAL_QTY + 0.0001)
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert readiness.residual_inventory_state != "RESIDUAL_INVENTORY_TRACKED"
    assert readiness.residual_disposition.disposition == "BLOCKING_INCONSISTENT"
    assert readiness.residual_disposition.run_allowed is False
