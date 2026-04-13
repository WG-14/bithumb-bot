from __future__ import annotations

from bithumb_bot import runtime_state
from bithumb_bot.broker import paper
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, init_portfolio, record_strategy_decision, set_portfolio
from bithumb_bot.dust import classify_dust_residual, dust_qty_gap_tolerance


def _insert_dust_tracking_lot(conn, *, qty_open: float, entry_trade_id: int) -> None:
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            settings.PAIR,
            int(entry_trade_id),
            f"dust_entry_{entry_trade_id}",
            1_700_000_000_000 + int(entry_trade_id),
            100_000_000.0,
            float(qty_open),
            0,
            1,
            "lot-native",
            "dust_tracking",
        ),
    )


def test_paper_execute_buy_survives_harmless_dust_strategy_decision(tmp_path, monkeypatch):
    db_path = str(tmp_path / "paper-harmless-dust.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    previous_db_path = settings.DB_PATH
    previous_mode = settings.MODE
    previous_pair = settings.PAIR
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 50_000.0)
    object.__setattr__(settings, "PAPER_FEE_RATE", 0.0)
    object.__setattr__(settings, "SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 1_000.0)
    object.__setattr__(settings, "MAX_OPEN_POSITIONS", 1)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)

    monkeypatch.setattr(paper, "_get_fill_price", lambda _signal: 100_000_000.0)

    dust = classify_dust_residual(
        broker_qty=0.00009629,
        local_qty=0.00009629,
        min_qty=0.0001,
        min_notional_krw=5_000.0,
        latest_price=40_000_000.0,
        partial_flatten_recent=False,
        partial_flatten_reason="not_recent",
        qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
        matched_harmless_resume_allowed=True,
    )
    runtime_state.record_reconcile_result(success=True, metadata=dust.to_metadata())

    conn = ensure_db(db_path)
    try:
        init_portfolio(conn)
        set_portfolio(conn, 1_000_000.0, 0.00009629)
        decision_id = record_strategy_decision(
            conn,
            decision_ts=1_700_000_000_000,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1_699_999_940_000,
            market_price=100_000_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "entry_allowed": True,
                "effective_flat": True,
                "normalized_exposure_active": True,
                "has_executable_exposure": False,
                "has_any_position_residue": True,
                "has_non_executable_residue": True,
                "has_dust_only_remainder": True,
                "normalized_exposure_qty": 0.00009629,
                "raw_qty_open": 0.00009629,
                "open_exposure_qty": 0.00009629,
                "dust_tracking_qty": 0.00009629,
                "position_gate": {
                    "entry_allowed": True,
                    "effective_flat_due_to_harmless_dust": True,
                    "normalized_exposure_active": True,
                    "has_executable_exposure": False,
                    "has_any_position_residue": True,
                    "has_non_executable_residue": True,
                    "has_dust_only_remainder": True,
                    "raw_qty_open": 0.00009629,
                },
                "position_state": {
                    "normalized_exposure": {
                        "entry_allowed": True,
                        "effective_flat": True,
                        "normalized_exposure_active": True,
                        "has_executable_exposure": False,
                        "has_any_position_residue": True,
                        "has_non_executable_residue": True,
                        "has_dust_only_remainder": True,
                        "normalized_exposure_qty": 0.00009629,
                        "raw_qty_open": 0.00009629,
                        "open_exposure_qty": 0.00009629,
                        "dust_tracking_qty": 0.00009629,
                    }
                },
            },
        )
        conn.commit()

        trade = paper.paper_execute(
            "BUY",
            1_700_000_000_000,
            100_000_000.0,
            strategy_name="sma_with_filter",
            decision_id=decision_id,
            decision_reason="sma golden cross",
        )

        assert trade is not None
        assert trade["side"] == "BUY"
        assert trade["qty"] > 0.0
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", previous_db_path)
        object.__setattr__(settings, "MODE", previous_mode)
        object.__setattr__(settings, "PAIR", previous_pair)


def test_paper_execute_sell_uses_lot_snapshot_authority_for_dust_only_state(tmp_path, monkeypatch):
    db_path = str(tmp_path / "paper-sell-dust-only.sqlite")
    previous_db_path = settings.DB_PATH
    previous_mode = settings.MODE
    previous_pair = settings.PAIR
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "PAPER_FEE_RATE", 0.0)
    object.__setattr__(settings, "SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 1_000.0)

    monkeypatch.setattr(paper, "_get_fill_price", lambda _signal: 100_000_000.0)

    conn = ensure_db(db_path)
    try:
        init_portfolio(conn)
        set_portfolio(conn, 1_000_000.0, 0.00005)
        _insert_dust_tracking_lot(conn, qty_open=0.00005, entry_trade_id=1)
        conn.commit()

        trade = paper.paper_execute(
            "SELL",
            1_700_000_100_000,
            100_000_000.0,
            strategy_name="sma_with_filter",
            decision_reason="dust-only sell should stay suppressed",
        )

        sell_orders = conn.execute("SELECT COUNT(*) AS n FROM orders WHERE side='SELL'").fetchone()
        assert trade is None
        assert sell_orders is not None
        assert int(sell_orders["n"]) == 0
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", previous_db_path)
        object.__setattr__(settings, "MODE", previous_mode)
        object.__setattr__(settings, "PAIR", previous_pair)


def test_paper_execute_effective_flat_harmless_dust_allows_buy_but_not_sell(tmp_path, monkeypatch):
    db_path = str(tmp_path / "paper-effective-flat-vs-sell-authority.sqlite")
    previous_db_path = settings.DB_PATH
    previous_mode = settings.MODE
    previous_pair = settings.PAIR
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 50_000.0)
    object.__setattr__(settings, "PAPER_FEE_RATE", 0.0)
    object.__setattr__(settings, "SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 1_000.0)
    object.__setattr__(settings, "MAX_OPEN_POSITIONS", 1)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)

    monkeypatch.setattr(paper, "_get_fill_price", lambda _signal: 100_000_000.0)

    dust_qty = 0.00009629
    dust = classify_dust_residual(
        broker_qty=dust_qty,
        local_qty=dust_qty,
        min_qty=0.0001,
        min_notional_krw=5_000.0,
        latest_price=40_000_000.0,
        partial_flatten_recent=False,
        partial_flatten_reason="not_recent",
        qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
        matched_harmless_resume_allowed=True,
    )
    runtime_state.record_reconcile_result(success=True, metadata=dust.to_metadata())

    conn = ensure_db(db_path)
    try:
        init_portfolio(conn)
        set_portfolio(conn, 1_000_000.0, dust_qty)
        _insert_dust_tracking_lot(conn, qty_open=dust_qty, entry_trade_id=2)
        sell_decision_id = record_strategy_decision(
            conn,
            decision_ts=1_700_000_200_000,
            strategy_name="sma_with_filter",
            signal="SELL",
            reason="effective flat dust remains non-sellable",
            candle_ts=1_700_000_140_000,
            market_price=100_000_000.0,
            context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "entry_allowed": True,
                "effective_flat": True,
                "has_dust_only_remainder": True,
                "position_state": {
                    "normalized_exposure": {
                        "entry_allowed": True,
                        "effective_flat": True,
                        "has_dust_only_remainder": True,
                        "open_exposure_qty": 0.0,
                        "dust_tracking_qty": dust_qty,
                        "sellable_executable_lot_count": 0,
                        "sellable_executable_qty": 0.0,
                        "exit_allowed": False,
                        "exit_block_reason": "dust_only_remainder",
                        "terminal_state": "dust_only",
                    }
                },
            },
        )
        buy_decision_id = record_strategy_decision(
            conn,
            decision_ts=1_700_000_201_000,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="effective flat harmless dust allows re-entry",
            candle_ts=1_700_000_140_000,
            market_price=100_000_000.0,
            context={
                "base_signal": "BUY",
                "final_signal": "BUY",
                "entry_allowed": True,
                "effective_flat": True,
                "normalized_exposure_active": True,
                "has_executable_exposure": False,
                "has_any_position_residue": True,
                "has_non_executable_residue": True,
                "has_dust_only_remainder": True,
                "normalized_exposure_qty": dust_qty,
                "raw_qty_open": dust_qty,
                "open_exposure_qty": 0.0,
                "dust_tracking_qty": dust_qty,
                "position_state": {
                    "normalized_exposure": {
                        "entry_allowed": True,
                        "effective_flat": True,
                        "normalized_exposure_active": True,
                        "has_executable_exposure": False,
                        "has_any_position_residue": True,
                        "has_non_executable_residue": True,
                        "has_dust_only_remainder": True,
                        "normalized_exposure_qty": 0.0,
                        "raw_qty_open": 0.0,
                        "open_exposure_qty": 0.0,
                        "dust_tracking_qty": dust_qty,
                    }
                },
            },
        )
        conn.commit()

        sell_trade = paper.paper_execute(
            "SELL",
            1_700_000_200_000,
            100_000_000.0,
            strategy_name="sma_with_filter",
            decision_id=sell_decision_id,
            decision_reason="effective flat dust remains non-sellable",
        )
        buy_trade = paper.paper_execute(
            "BUY",
            1_700_000_201_000,
            100_000_000.0,
            strategy_name="sma_with_filter",
            decision_id=buy_decision_id,
            decision_reason="effective flat harmless dust allows re-entry",
        )

        sell_orders = conn.execute("SELECT COUNT(*) AS n FROM orders WHERE side='SELL'").fetchone()
        assert sell_trade is None
        assert sell_orders is not None
        assert int(sell_orders["n"]) == 0
        assert buy_trade is not None
        assert buy_trade["side"] == "BUY"
        assert buy_trade["qty"] > 0.0
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", previous_db_path)
        object.__setattr__(settings, "MODE", previous_mode)
        object.__setattr__(settings, "PAIR", previous_pair)
