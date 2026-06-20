from __future__ import annotations

import json

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, init_portfolio, record_broker_fill_observation
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.lifecycle import summarize_position_lots
from bithumb_bot.order_settlement import OrderSettlementCoordinator, SettlementBarrierConfig


def _record_event(
    conn,
    *,
    client_order_id: str,
    side: str,
    qty: float,
    ts: int,
    internal_lot_size: float,
) -> None:
    submit_evidence = None
    if side == "SELL":
        open_exposure_qty = 0.0 if float(qty) + 1e-12 < float(internal_lot_size) else float(qty)
        dust_tracking_qty = float(qty) if open_exposure_qty <= 1e-12 else 0.0
        submit_evidence = json.dumps(
            {
                "source": "target_delta",
                "authority": "target_position_delta",
                "decision_reason_code": "target_delta_rebalance",
                "final_submitted_qty": qty,
                "order_qty": qty,
                "normalized_qty": qty,
                "sell_open_exposure_qty": open_exposure_qty,
                "sell_dust_tracking_qty": dust_tracking_qty,
                "raw_total_asset_qty": qty,
                "observed_position_qty": qty,
                "clean_account_after_sell": True,
            },
            sort_keys=True,
        )
    conn.execute(
        """
        INSERT INTO order_events(
            client_order_id, event_type, event_ts, order_status, qty, side,
            submit_evidence, final_submitted_qty, decision_reason_code, submission_reason_code
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            client_order_id,
            "scripted_broker_response",
            int(ts),
            "FILLED",
            float(qty),
            side,
            submit_evidence,
            float(qty),
            "target_delta_rebalance" if side == "SELL" else None,
            "target_delta_rebalance" if side == "SELL" else None,
        ),
    )


def _apply_scripted_fill(
    conn,
    *,
    client_order_id: str,
    side: str,
    qty: float,
    price: float,
    fee: float,
    ts: int,
    internal_lot_size: float = 0.0001,
) -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side=side,
        qty_req=qty,
        price=price,
        ts_ms=ts,
        status="FILLED",
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=0.0001,
        qty_step=0.00000001,
        min_notional_krw=5000.0,
        intended_lot_count=int(qty / internal_lot_size) if internal_lot_size > 0 else 0,
        executable_lot_count=int(qty / internal_lot_size) if internal_lot_size > 0 else 0,
    )
    _record_event(
        conn,
        client_order_id=client_order_id,
        side=side,
        qty=qty,
        ts=ts,
        internal_lot_size=internal_lot_size,
    )
    record_broker_fill_observation(
        conn,
        event_ts=ts,
        client_order_id=client_order_id,
        exchange_order_id=f"ex_{client_order_id}",
        fill_id=f"fill_{client_order_id}",
        fill_ts=ts,
        side=side,
        price=price,
        qty=qty,
        fee=fee,
        fee_status="validated_order_level_paid_fee",
        fee_source="order_level_paid_fee",
        fee_confidence="validated",
        accounting_status="accounting_complete",
        source="scripted_recorded_fixture",
        fee_provenance="order_level_paid_fee_validated_single_fill",
        fee_validation_reason="order_level_paid_fee_validated_single_fill",
        fee_validation_checks={"paid_fee_present": True, "complete_fill_set_available": True},
        raw_payload={"paid_fee": str(fee), "executed_volume": str(qty), "executed_funds": str(qty * price)},
    )
    apply_fill_and_trade(
        conn,
        client_order_id=client_order_id,
        side=side,
        fill_id=f"fill_{client_order_id}",
        fill_ts=ts,
        price=price,
        qty=qty,
        fee=fee,
        strategy_name="target_delta",
        entry_decision_id=ts if side == "BUY" else None,
        exit_decision_id=ts if side == "SELL" else None,
        exit_reason="scripted_close" if side == "SELL" else None,
        exit_rule_name="target_delta" if side == "SELL" else None,
    )


def _settle(client_order_id: str, *, qty: float):
    coordinator = OrderSettlementCoordinator(
        SettlementBarrierConfig(max_attempts=2, poll_intervals_ms=(0,), deadline_ms=100),
        sleeper=lambda _seconds: None,
    )
    return coordinator.settle(
        client_order_id=client_order_id,
        exchange_order_id=f"ex_{client_order_id}",
        observe=lambda _attempt: {
            "order_state": "FILLED",
            "fill_count": 1,
            "fill_set_complete": True,
            "paid_fee_present": True,
            "order_level_paid_fee_present": True,
            "complete_fill_set_available": True,
            "fee_state": "finalized",
            "principal_applied": True,
            "accounting_finalized": True,
            "projection_applied": True,
            "projected_total_qty": qty,
            "portfolio_qty": qty,
            "broker_qty": qty,
            "broker_local_converged": True,
        },
    )


def test_recorded_single_fill_delayed_paid_fee_settles_without_manual_repair(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "recorded_single.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    conn = ensure_db(str(db_path))
    try:
        init_portfolio(conn)
        _apply_scripted_fill(
            conn,
            client_order_id="recorded_buy",
            side="BUY",
            qty=0.0002,
            price=100_000_000.0,
            fee=10.0,
            ts=1_800_000_000_000,
        )
        result = _settle("recorded_buy", qty=0.0002)
        assert result.settled is True
        assert conn.execute("SELECT COUNT(*) FROM fee_pending_accounting_repairs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM broker_fill_observations").fetchone()[0] == 1
    finally:
        conn.close()


def test_recorded_all_dust_terminal_close_projects_flat(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "recorded_dust.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    conn = ensure_db(str(db_path))
    try:
        init_portfolio(conn)
        _apply_scripted_fill(
            conn,
            client_order_id="recorded_dust_buy",
            side="BUY",
            qty=0.00019998,
            price=96_933_000.0,
            fee=10.0,
            ts=1_800_000_000_000,
            internal_lot_size=0.0004,
        )
        assert summarize_position_lots(conn, pair=str(settings.PAIR)).dust_tracking_qty > 0.0
        _apply_scripted_fill(
            conn,
            client_order_id="recorded_dust_sell",
            side="SELL",
            qty=0.00019998,
            price=96_933_000.0,
            fee=10.0,
            ts=1_800_000_001_000,
            internal_lot_size=0.0004,
        )
        result = _settle("recorded_dust_sell", qty=0.0)
        assert result.settled is True
        assert summarize_position_lots(conn, pair=str(settings.PAIR)).raw_total_asset_qty == 0.0
    finally:
        conn.close()


def test_recorded_smoke_five_round_trips_touches_orders_fills_trades_portfolio_lots(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "recorded_smoke.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    conn = ensure_db(str(db_path))
    try:
        init_portfolio(conn)
        qty = 0.0002
        price = 100_000_000.0
        for index in range(5):
            buy_id = f"recorded_smoke_buy_{index}"
            sell_id = f"recorded_smoke_sell_{index}"
            _apply_scripted_fill(
                conn,
                client_order_id=buy_id,
                side="BUY",
                qty=qty,
                price=price,
                fee=10.0,
                ts=1_800_000_000_000 + index * 2_000,
            )
            assert _settle(buy_id, qty=qty).settled is True
            _apply_scripted_fill(
                conn,
                client_order_id=sell_id,
                side="SELL",
                qty=qty,
                price=price,
                fee=10.0,
                ts=1_800_000_001_000 + index * 2_000,
            )
            assert _settle(sell_id, qty=0.0).settled is True

        assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 10
        assert conn.execute("SELECT COUNT(*) FROM order_events").fetchone()[0] >= 10
        assert conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 10
        assert conn.execute("SELECT COUNT(*) FROM broker_fill_observations").fetchone()[0] == 10
        assert conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 10
        assert conn.execute("SELECT COUNT(*) FROM portfolio").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM open_position_lots").fetchone()[0] >= 0
        assert conn.execute("SELECT COUNT(*) FROM trade_lifecycles").fetchone()[0] >= 5
        assert conn.execute("SELECT COUNT(*) FROM fee_pending_accounting_repairs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM position_authority_repairs").fetchone()[0] == 0
        assert summarize_position_lots(conn, pair=str(settings.PAIR)).raw_total_asset_qty == 0.0
    finally:
        conn.close()
