from __future__ import annotations

from bithumb_bot import runtime_state
from bithumb_bot.broker import paper
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, init_portfolio, record_strategy_decision, set_portfolio
from bithumb_bot.dust import classify_dust_residual, dust_qty_gap_tolerance


def test_paper_execute_buy_survives_harmless_dust_strategy_decision(tmp_path, monkeypatch):
    db_path = str(tmp_path / "paper-harmless-dust.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
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
                "normalized_exposure_qty": 0.00009629,
                "raw_qty_open": 0.00009629,
                "open_exposure_qty": 0.00009629,
                "dust_tracking_qty": 0.00009629,
                "position_gate": {
                    "entry_allowed": True,
                    "effective_flat_due_to_harmless_dust": True,
                    "normalized_exposure_active": True,
                    "raw_qty_open": 0.00009629,
                },
                "position_state": {
                    "normalized_exposure": {
                        "entry_allowed": True,
                        "effective_flat": True,
                        "normalized_exposure_active": True,
                        "normalized_exposure_qty": 0.00009629,
                        "raw_qty_open": 0.00009629,
                        "open_exposure_qty": 0.00009629,
                        "dust_tracking_qty": 0.00009629,
                    }
                },
            },
        )
        conn.commit()
    finally:
        conn.close()

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
