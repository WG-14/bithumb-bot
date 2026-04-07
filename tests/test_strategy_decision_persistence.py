from __future__ import annotations

import json

import pytest

from bithumb_bot.db_core import ensure_db, record_strategy_decision


def test_strategy_decision_schema_bootstrap(tmp_path):
    conn = ensure_db(str(tmp_path / "decision_schema.sqlite"))
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(strategy_decisions)").fetchall()}
    conn.close()

    assert "decision_ts" in cols
    assert "strategy_name" in cols
    assert "signal" in cols
    assert "reason" in cols
    assert "candle_ts" in cols
    assert "market_price" in cols
    assert "context_json" in cols
    assert "confidence" in cols


def test_record_strategy_decisions_for_trade_and_non_trade_cases(tmp_path):
    conn = ensure_db(str(tmp_path / "decision_persistence.sqlite"))
    try:
        record_strategy_decision(
            conn,
            decision_ts=1_700_000_000_100,
            strategy_name="sma_cross",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1_700_000_000_000,
            market_price=105_000_000.0,
            confidence=0.87,
            context={
                "features": {"prev_s": 101.0, "prev_l": 100.0, "curr_s": 103.0, "curr_l": 101.0},
                "position_open": False,
            },
        )
        record_strategy_decision(
            conn,
            decision_ts=1_700_000_060_100,
            strategy_name="sma_cross",
            signal="HOLD",
            reason="sma no crossover",
            candle_ts=1_700_000_060_000,
            market_price=105_100_000.0,
            confidence=None,
            context={
                "features": {"prev_s": 102.0, "prev_l": 101.0, "curr_s": 102.0, "curr_l": 101.5},
                "position_open": True,
            },
        )
        conn.commit()

        rows = conn.execute(
            """
            SELECT strategy_name, signal, reason, context_json, confidence
            FROM strategy_decisions
            ORDER BY decision_ts ASC
            """
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 2
    assert rows[0]["strategy_name"] == "sma_cross"
    assert rows[0]["signal"] == "BUY"
    assert rows[0]["reason"] == "sma golden cross"
    buy_context = json.loads(str(rows[0]["context_json"]))
    hold_context = json.loads(str(rows[1]["context_json"]))
    assert buy_context["features"]["curr_s"] == 103.0
    assert hold_context["position_open"] is True


def test_record_strategy_decision_preserves_buy_to_hold_explanation_fields(tmp_path):
    conn = ensure_db(str(tmp_path / "decision_buy_to_hold.sqlite"))
    try:
        record_strategy_decision(
            conn,
            decision_ts=1_710_000_120_000,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="position held: no exit rule triggered",
            candle_ts=1_710_000_060_000,
            market_price=102_500_000.0,
            confidence=None,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "raw_qty_open": 0.00009629,
                "normalized_exposure_active": True,
                "normalized_exposure_qty": 0.00009629,
                "effective_flat": False,
                "dust_classification": "harmless_dust",
                "position_gate": {
                    "dust_state": "harmless_dust",
                    "effective_flat_due_to_harmless_dust": False,
                    "raw_qty_open": 0.00009629,
                },
            },
        )
        conn.commit()
        row = conn.execute(
            "SELECT context_json FROM strategy_decisions ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    ctx = json.loads(str(row["context_json"]))
    assert ctx["raw_signal"] == "BUY"
    assert ctx["final_signal"] == "HOLD"
    assert ctx["entry_blocked"] is True
    assert ctx["entry_block_reason"] == "position held: no exit rule triggered"
    assert ctx["dust_classification"] == "harmless_dust"
    assert ctx["effective_flat"] is False
    assert ctx["raw_qty_open"] == pytest.approx(0.00009629)
    assert ctx["normalized_exposure_active"] is True
    assert ctx["normalized_exposure_qty"] == pytest.approx(0.00009629)
    assert ctx["position_state"]["raw_holdings"]["classification"] == "harmless_dust"
    assert ctx["position_state"]["normalized_exposure"]["normalized_exposure_active"] is True
    assert ctx["position_state"]["operator_diagnostics"]["state"] == "harmless_dust"
    assert ctx["decision_summary"]["raw_signal"] == "BUY"
    assert ctx["decision_summary"]["final_signal"] == "HOLD"
