from __future__ import annotations

import json

from bithumb_bot.app import main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, record_strategy_decision


def test_record_strategy_decision_normalizes_blocked_entry_context(tmp_path, monkeypatch):
    db_path = str(tmp_path / "decision-normalize.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("PAIR", "KRW-BTC")
    monkeypatch.setenv("INTERVAL", "1m")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "INTERVAL", "1m")

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=1_710_000_000_000,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="filtered entry: gap, volatility",
            candle_ts=1_709_999_940_000,
            market_price=102_000_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "filtered entry: gap, volatility",
                "blocked_filters": ["gap", "volatility"],
                "signal_strength": {"label": "weak"},
                "features": {
                    "sma_gap_ratio": 0.001,
                    "volatility_range_ratio": 0.0009,
                    "overextended_abs_return_ratio": 0.01,
                },
            },
        )
        conn.commit()
        row = conn.execute(
            "SELECT signal, strategy_name, context_json FROM strategy_decisions ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    ctx = json.loads(str(row["context_json"]))
    assert ctx["decision_type"] == "BLOCKED_ENTRY"
    assert ctx["base_reason"] == "sma golden cross"
    assert ctx["entry_reason"].startswith("filtered entry")
    assert ctx["filter_blocked"] is True
    assert ctx["blocked_filters"] == ["gap", "volatility"]
    assert ctx["blocked_candidate"] is True
    assert ctx["signal_strength_label"] == "weak"
    assert ctx["market_observations"]["gap"] == 0.001
    assert ctx["market_observations"]["volatility"] == 0.0009
    assert ctx["market_observations"]["extension"] == 0.01
    assert ctx["strategy_name"] == "sma_with_filter"
    assert ctx["pair"] == "KRW-BTC"
    assert ctx["interval"] == "1m"


def test_record_strategy_decision_normalizes_hold_context_without_filter_block(tmp_path, monkeypatch):
    db_path = str(tmp_path / "decision-hold.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=1_710_000_060_000,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="position held: no exit rule triggered",
            candle_ts=1_710_000_000_000,
            market_price=102_100_000.0,
            context={
                "base_signal": "HOLD",
                "base_reason": "sma no crossover",
                "entry_reason": "position held: no exit rule triggered",
                "signal_strength_label": "neutral",
            },
        )
        conn.commit()
        row = conn.execute("SELECT context_json FROM strategy_decisions ORDER BY id DESC LIMIT 1").fetchone()
    finally:
        conn.close()

    assert row is not None
    ctx = json.loads(str(row["context_json"]))
    assert ctx["decision_type"] == "HOLD"
    assert ctx["filter_blocked"] is False
    assert ctx["blocked_filters"] == []
    assert ctx["blocked_candidate"] is False
    assert ctx["base_reason"] == "sma no crossover"
    assert ctx["signal_strength_label"] == "neutral"


def test_decision_telemetry_cli_groups_blocked_hold_and_executed(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "decision-cli.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=1,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="filtered entry: gap",
            candle_ts=1,
            market_price=1.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "filtered entry: gap",
                "blocked_filters": ["gap"],
                "filter_blocked": True,
            },
        )
        record_strategy_decision(
            conn,
            decision_ts=2,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=2,
            market_price=1.0,
            context={"base_signal": "BUY", "base_reason": "sma golden cross", "entry_reason": "sma golden cross"},
        )
        record_strategy_decision(
            conn,
            decision_ts=3,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="position held: no exit rule triggered",
            candle_ts=3,
            market_price=1.0,
            context={"base_signal": "HOLD", "base_reason": "sma no crossover", "entry_reason": "position held: no exit rule triggered"},
        )
        conn.commit()
    finally:
        conn.close()

    rc = main(["decision-telemetry", "--limit", "20"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "[DECISION-TELEMETRY]" in out
    assert "BLOCKED_ENTRY" in out
    assert "BUY,sma_with_filter" in out
    assert "HOLD,sma_with_filter" in out


def test_record_strategy_decision_keeps_cost_edge_block_reason(tmp_path, monkeypatch):
    db_path = str(tmp_path / "decision-cost-edge.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=10,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="filtered entry: cost_edge",
            candle_ts=10,
            market_price=1.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "filtered entry: cost_edge",
                "blocked_filters": ["cost_edge"],
                "filters": {"cost_edge": {"enabled": True, "passed": False, "threshold": 0.04, "value": 0.03}},
                "filter_blocked": True,
            },
        )
        conn.commit()
        row = conn.execute("SELECT context_json FROM strategy_decisions ORDER BY id DESC LIMIT 1").fetchone()
    finally:
        conn.close()

    assert row is not None
    ctx = json.loads(str(row["context_json"]))
    assert ctx["decision_type"] == "BLOCKED_ENTRY"
    assert ctx["blocked_filters"] == ["cost_edge"]
    assert ctx["entry_reason"] == "filtered entry: cost_edge"
