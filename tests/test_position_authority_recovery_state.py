from __future__ import annotations

import json

import pytest

from bithumb_bot.app import _load_recovery_report
from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import (
    ensure_db,
    record_broker_fill_observation,
    record_position_authority_repair,
    set_portfolio_breakdown,
)
from bithumb_bot.engine import (
    evaluate_restart_readiness,
    evaluate_resume_eligibility,
    evaluate_startup_safety_gate,
    get_health_status,
)
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
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
    apply_position_authority_rebuild,
    build_position_authority_rebuild_preview,
)
from bithumb_bot.position_authority_state import build_position_authority_assessment
from bithumb_bot.runtime_readiness import compute_runtime_readiness_snapshot


FILL_QTY = 0.00059996
LOT_SIZE = 0.0004
PRICE = 7_050_000.0
PORTFOLIO_DIVERGENCE_BUY_QTY = 0.00059992
PORTFOLIO_DIVERGENCE_QTY = 0.00039988
LIVE_INCIDENT_PORTFOLIO_QTY = 0.00099986
LIVE_INCIDENT_STALE_DUST_QTY = 0.001788


@pytest.fixture
def recovery_db(tmp_path, monkeypatch):
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
    return db_path


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


def _insert_live_incident_stale_dust_projection(conn) -> None:
    dust_quantities = [0.000128] * 13 + [0.000124]
    assert sum(dust_quantities) == pytest.approx(LIVE_INCIDENT_STALE_DUST_QTY)
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
    assert readiness.as_dict()["trading_permission_scope"] == "new_entry_or_closeout"
    assert readiness.as_dict()["trading_allowed"] is True
    assert readiness.as_dict()["trading_block_reason"] == "closeout_blocked:dust_only_remainder"
    assert readiness.as_dict() == replay.as_dict()


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
    assert result["external_position_adjustment"]["reason"] == "portfolio_projection_external_position_adjustment"
    assert repair["reason"] == "portfolio_anchored_authority_projection_repair"
    assert adjustment["reason"] == "portfolio_projection_external_position_adjustment"
    basis = json.loads(repair["repair_basis"])
    assert basis["event_type"] == "portfolio_anchored_authority_projection_repair"
    assert basis["target_remainder_qty"] == pytest.approx(PORTFOLIO_DIVERGENCE_QTY)
    adjustment_basis = json.loads(adjustment["adjustment_basis"])
    assert adjustment_basis["event_type"] == "external_position_adjustment"
    assert adjustment_basis["source_event_type"] == "portfolio_anchored_authority_projection_repair"
    assert len(rows) == 1
    assert rows[0]["position_state"] == "dust_tracking"
    assert rows[0]["qty_open"] == pytest.approx(PORTFOLIO_DIVERGENCE_QTY)
    assert rows[0]["executable_lot_count"] == 0
    assert rows[0]["dust_tracking_lot_count"] == 1
    assert rows[0]["internal_lot_size"] == pytest.approx(LOT_SIZE)
    assert after.recovery_stage == "RESUME_READY"
    assert after.resume_ready is True
    assert after.canonical_state == "DUST_ONLY_TRACKED"
    assert after.position_state.normalized_exposure.sellable_executable_lot_count == 0
    assert preview_after["needs_repair"] is False
    assert replay.replayed_buy_count == 1
    assert len(replay_rows) == 1
    assert replay_rows[0]["position_state"] == "dust_tracking"
    assert replay_rows[0]["qty_open"] == pytest.approx(PORTFOLIO_DIVERGENCE_QTY)
    assert replay_rows[0]["executable_lot_count"] == 0


def test_missing_fee_incident_projection_repair_refuses_non_converged_stale_dust_projection(
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
        conn.commit()
        _record_portfolio_projection_broker_evidence(broker_qty=LIVE_INCIDENT_PORTFOLIO_QTY)

        preview = build_position_authority_rebuild_preview(conn)
        before = compute_runtime_readiness_snapshot(conn)
        with pytest.raises(RuntimeError, match="position authority rebuild is not safe to apply"):
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

    assert preview["repair_mode"] == "portfolio_projection_repair"
    assert preview["safe_to_apply"] is False
    assert preview["action_state"] == "inspect_only"
    assert "projection_excess_outside_target=" in preview["eligibility_reason"]
    assert before.recovery_stage == "AUTHORITY_PROJECTION_PORTFOLIO_DIVERGENCE_PENDING"
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

        assessment = build_position_authority_assessment(conn)
    finally:
        conn.close()

    authoritative = assessment["authoritative_quantity_contract"]
    projection = assessment["projection_quantity_contract"]

    assert assessment["alignment_state"] == "projection_diverged"
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


def test_projection_non_convergence_is_consistent_across_readiness_resume_and_reports(
    recovery_db,
):
    conn = ensure_db(str(recovery_db))
    try:
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
    assert truth_model["projected_total_qty"] == pytest.approx(LIVE_INCIDENT_STALE_DUST_QTY)
    assert truth_model["projection_delta_qty"] == pytest.approx(
        LIVE_INCIDENT_STALE_DUST_QTY - LIVE_INCIDENT_PORTFOLIO_QTY
    )
    assert truth_model["inspect_only"] is True
    assert structured_blocker["reason_code"] == "POSITION_AUTHORITY_PROJECTION_CONVERGENCE_REQUIRED"
    assert structured_blocker["inspect_only"] is True
    assert structured_blocker["canonical_asset_qty"] == pytest.approx(LIVE_INCIDENT_PORTFOLIO_QTY)
    assert structured_blocker["projected_lot_qty"] == pytest.approx(LIVE_INCIDENT_STALE_DUST_QTY)
    assert structured_blocker["divergence_delta_qty"] == pytest.approx(
        LIVE_INCIDENT_STALE_DUST_QTY - LIVE_INCIDENT_PORTFOLIO_QTY
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
        LIVE_INCIDENT_STALE_DUST_QTY - LIVE_INCIDENT_PORTFOLIO_QTY
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
    assert after.recovery_stage == "RESUME_READY"
    assert after.resume_ready is True


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
        apply_fill_and_trade(
            conn,
            client_order_id="fee_sell",
            side="SELL",
            fill_id="fee-sell-fill",
            fill_ts=1_700_000_100_050,
            price=PRICE,
            qty=LOT_SIZE,
            fee=0.0,
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
        (flat, flat_fee_gap, "FLAT", "NONE", True, True, True, False),
        (
            open_readiness,
            open_fee_gap,
            "OPEN_EXECUTABLE",
            "EXECUTABLE_OPEN_EXPOSURE",
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
            True,
        ),
    ]
    for (
        readiness,
        fee_gap,
        canonical_state,
        residual_class,
        execution_flat,
        accounting_flat,
        new_entry_allowed,
        operator_action_required,
    ) in cases:
        assert readiness.canonical_state == canonical_state
        assert readiness.residual_class == residual_class
        assert readiness.run_loop_allowed is readiness.resume_ready
        assert readiness.new_entry_allowed is new_entry_allowed
        assert readiness.operator_action_required is operator_action_required
        assert readiness.execution_flat is execution_flat
        assert readiness.accounting_flat is accounting_flat
        assert readiness.tradeability.as_dict()["residual_class"] == residual_class
        assert fee_gap["canonical_state"] == canonical_state
        assert fee_gap["execution_flat"] is execution_flat
        assert fee_gap["accounting_flat"] is accounting_flat


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
        apply_fill_and_trade(
            conn,
            client_order_id="fee_incomplete_existing_fill",
            side="BUY",
            fill_id="fee-incomplete-fill",
            fill_ts=1_700_002_000_100,
            price=PRICE,
            qty=LOT_SIZE,
            fee=0.0,
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
            metadata={"fee_pending_recovery_required": 1},
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

    assert readiness.fee_pending_count == 1
    assert preview["needs_repair"] is True
    assert preview["safe_to_apply"] is True
    assert preview["repair_mode"] == "complete_existing_fill_fee"
    assert "fill_already_accounted" not in preview["eligibility_reason"]
    assert result["applied_fill"]["repair_mode"] == "complete_existing_fill_fee"
    assert fill_count["cnt"] == 1
    assert fill_count["fee_total"] == pytest.approx(3.21)
    assert complete_observation["fee_status"] == "operator_confirmed"
    assert complete_observation["accounting_status"] == "accounting_complete"
