from __future__ import annotations

import json

import pytest

from bithumb_bot.app import main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, record_strategy_decision
from bithumb_bot.reporting import fetch_decision_telemetry_summary


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
    assert ctx["entry_allowed_truth_source"] == "fallback:flat_zero_holdings"
    assert ctx["effective_flat_truth_source"] == "fallback:flat_zero_holdings"
    assert ctx["decision_truth_sources"]["entry_allowed"] == "fallback:flat_zero_holdings"
    assert ctx["decision_truth_sources"]["raw_qty_open"] == "default:0.0"


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


def test_decision_telemetry_cli_exposes_sell_failure_category_fields(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "decision-sell-failure-cli.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=1_710_000_120_000,
            strategy_name="sma_with_filter",
            signal="SELL",
            reason="sell blocked by qty step",
            candle_ts=1_710_000_060_000,
            market_price=102_300_000.0,
            context={
                "base_signal": "SELL",
                "base_reason": "sma dead cross",
                "entry_reason": "sma dead cross",
                "final_signal": "SELL",
                "sell_qty_boundary_kind": "qty_step",
                "raw_qty_open": 0.0002,
                "raw_total_asset_qty": 0.00029193,
                "open_exposure_qty": 0.0002,
                "dust_tracking_qty": 0.00009193,
                "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
                "position_state_source": "context.raw_qty_open",
                "normalized_exposure_active": True,
                "normalized_exposure_qty": 0.0002,
                "effective_flat": False,
                "dust_classification": "blocking_dust",
                "position_gate": {
                    "dust_state": "blocking_dust",
                    "effective_flat_due_to_harmless_dust": False,
                    "raw_qty_open": 0.0002,
                },
            },
        )
        conn.commit()
    finally:
        conn.close()

    rc = main(["decision-telemetry", "--limit", "20"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "sell_failure_category,sell_failure_detail" in out
    assert "qty_step_mismatch" in out
    assert ",qty_step," in out


def test_decision_telemetry_cli_exposes_buy_to_hold_reason_fields(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "decision-buy-to-hold-cli.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=1,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="position held: no exit rule triggered",
            candle_ts=1,
            market_price=1.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "raw_qty_open": 0.00009629,
                "raw_total_asset_qty": 0.00019192,
                "open_exposure_qty": 0.00009629,
                "dust_tracking_qty": 0.00009563,
                "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
                "position_state_source": "context.raw_qty_open",
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
    finally:
        conn.close()

    rc = main(["decision-telemetry", "--limit", "20"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "[DECISION-TELEMETRY]" in out
    assert "base_signal,decision_type,raw_signal,final_signal,buy_flow_state,entry_blocked,entry_allowed" in out
    assert "raw_total_asset_qty" in out
    assert "position_qty" in out
    assert "submit_payload_qty" in out
    assert "open_exposure_qty" in out
    assert "dust_tracking_qty" in out
    assert "submit_qty_source" in out
    assert "sell_submit_qty_source" in out
    assert "sell_normalized_exposure_qty" in out
    assert "sell_open_exposure_qty" in out
    assert "sell_dust_tracking_qty" in out
    assert "BUY,HOLD,BUY,HOLD,BUY_BLOCKED,1,0,position held: no exit rule triggered" in out
    assert "harmless_dust" in out
    assert "0.00009629" in out
    assert "position_gate.effective_flat_due_to_harmless_dust" in out


def test_record_strategy_decision_prefers_entry_allowed_truth_source(tmp_path, monkeypatch):
    db_path = str(tmp_path / "decision-entry-allowed.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=1,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1,
            market_price=1.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "raw_qty_open": 0.00009629,
                "raw_total_asset_qty": 0.00019192,
                "open_exposure_qty": 0.00009629,
                "dust_tracking_qty": 0.00009563,
                "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
                "position_state_source": "context.raw_qty_open",
                "position_gate": {
                    "entry_allowed": True,
                    "effective_flat_due_to_harmless_dust": True,
                    "raw_qty_open": 0.00009629,
                },
            },
        )
        conn.commit()
        row = conn.execute("SELECT context_json FROM strategy_decisions ORDER BY id DESC LIMIT 1").fetchone()
    finally:
        conn.close()

    assert row is not None
    ctx = json.loads(str(row["context_json"]))
    assert ctx["entry_allowed"] is True
    assert ctx["effective_flat"] is True
    assert ctx["normalized_exposure_active"] is True
    assert ctx["raw_total_asset_qty"] == 0.00019192
    assert ctx["position_qty"] == 0.00009629
    assert ctx["submit_payload_qty"] == pytest.approx(0.00009629)
    assert ctx["open_exposure_qty"] == 0.00009629
    assert ctx["dust_tracking_qty"] == 0.00009563
    assert ctx["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert ctx["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert ctx["sell_qty_basis_qty"] == pytest.approx(0.00009629)
    assert ctx["sell_qty_basis_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert ctx["sell_qty_boundary_kind"] == "none"
    assert ctx["sell_normalized_exposure_qty"] == pytest.approx(0.00009629)
    assert ctx["sell_open_exposure_qty"] == pytest.approx(0.00009629)
    assert ctx["sell_dust_tracking_qty"] == pytest.approx(0.00009563)
    assert ctx["sell_submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert ctx["sell_normalized_exposure_qty_truth_source"] == "fallback:raw_qty_open_or_zero"
    assert ctx["sell_open_exposure_qty_truth_source"] == "context.open_exposure_qty"
    assert ctx["sell_dust_tracking_qty_truth_source"] == "context.dust_tracking_qty"
    assert ctx["position_state_source"] == "context.raw_qty_open"
    assert ctx["entry_allowed_truth_source"] == "position_gate.entry_allowed"
    assert ctx["effective_flat_truth_source"] == "position_gate.effective_flat_due_to_harmless_dust"
    assert ctx["decision_truth_sources"]["normalized_exposure_active"] == "fallback:open_exposure_qty"
    assert ctx["position_state"]["normalized_exposure"]["entry_allowed"] is True
    assert ctx["position_state"]["normalized_exposure"]["normalized_exposure_active"] is True


def test_record_strategy_decision_canonicalizes_sell_basis_to_open_exposure(tmp_path, monkeypatch):
    db_path = str(tmp_path / "decision-sell-basis-canonical.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=1,
            strategy_name="sma_with_filter",
            signal="SELL",
            reason="sma dead cross",
            candle_ts=1,
            market_price=1.0,
            context={
                "base_signal": "SELL",
                "base_reason": "sma dead cross",
                "entry_reason": "sma dead cross",
                "raw_qty_open": 0.00009999,
                "raw_total_asset_qty": 0.00019192,
                "dust_tracking_qty": 0.00009193,
                "submit_qty_source": "position_state.raw_total_asset_qty",
                "position_state_source": "context.raw_qty_open",
                "position_gate": {
                    "dust_state": "harmless_dust",
                    "effective_flat_due_to_harmless_dust": False,
                    "raw_qty_open": 0.00009999,
                },
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.00009999,
                        "raw_total_asset_qty": 0.00019192,
                        "open_exposure_qty": 0.00009999,
                        "dust_tracking_qty": 0.00009193,
                    }
                },
            },
        )
        conn.commit()
        row = conn.execute("SELECT context_json FROM strategy_decisions ORDER BY id DESC LIMIT 1").fetchone()
    finally:
        conn.close()

    assert row is not None
    ctx = json.loads(str(row["context_json"]))
    assert ctx["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert ctx["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert ctx["sell_qty_basis_qty"] == pytest.approx(0.00009999)
    assert ctx["sell_qty_basis_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert ctx["sell_submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert ctx["sell_qty_basis_qty_truth_source"] == "position_state.normalized_exposure.sellable_executable_qty"


def test_record_strategy_decision_merges_top_level_position_state_fallbacks(tmp_path, monkeypatch):
    db_path = str(tmp_path / "decision-position-state-top-level.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=2,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=2,
            market_price=1.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "position_state": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.0,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.0,
                    "position_state_source": "position_state.raw_qty_open",
                    "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
                },
            },
        )
        conn.commit()
        row = conn.execute("SELECT context_json FROM strategy_decisions ORDER BY id DESC LIMIT 1").fetchone()
    finally:
        conn.close()

    assert row is not None
    ctx = json.loads(str(row["context_json"]))
    assert ctx["raw_total_asset_qty"] == 0.0
    assert ctx["open_exposure_qty"] == 0.0
    assert ctx["dust_tracking_qty"] == 0.0
    assert ctx["raw_total_asset_qty_truth_source"] == "position_state.raw_total_asset_qty"
    assert ctx["open_exposure_qty_truth_source"] == "position_state.open_exposure_qty"
    assert ctx["dust_tracking_qty_truth_source"] == "position_state.dust_tracking_qty"
    assert ctx["position_state_source"] == "position_state.raw_qty_open"
    assert ctx["position_state_source_truth_source"] == "context.position_state_source"
    assert ctx["position_state"]["normalized_exposure"]["raw_total_asset_qty"] == 0.0
    assert ctx["position_state"]["normalized_exposure"]["open_exposure_qty"] == 0.0
    assert ctx["position_state"]["normalized_exposure"]["dust_tracking_qty"] == 0.0


def test_decision_telemetry_prefers_normalized_position_state_over_shadow_top_level_values(
    tmp_path,
    monkeypatch,
):
    db_path = str(tmp_path / "decision-normalized-state-authority.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=3,
            strategy_name="sma_with_filter",
            signal="SELL",
            reason="sma dead cross",
            candle_ts=3,
            market_price=1.0,
            context={
                "base_signal": "SELL",
                "base_reason": "sma dead cross",
                "entry_reason": "sma dead cross",
                "raw_qty_open": 9.9,
                "raw_total_asset_qty": 9.9,
                "open_exposure_qty": 9.9,
                "dust_tracking_qty": 8.8,
                "normalized_exposure_qty": 9.9,
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.3,
                        "raw_total_asset_qty": 0.35,
                        "open_exposure_qty": 0.25,
                        "dust_tracking_qty": 0.1,
                        "normalized_exposure_qty": 0.25,
                        "entry_allowed": False,
                        "effective_flat": False,
                        "normalized_exposure_active": True,
                        "sellable_executable_qty": 0.25,
                        "exit_allowed": True,
                        "exit_block_reason": "none",
                        "terminal_state": "open_exposure",
                    }
                },
            },
        )
        conn.commit()
        rows = fetch_decision_telemetry_summary(conn, limit=20)
    finally:
        conn.close()

    assert len(rows) == 1
    row = rows[0]
    assert row.raw_qty_open == pytest.approx(0.3)
    assert row.raw_total_asset_qty == pytest.approx(0.35)
    assert row.open_exposure_qty == pytest.approx(0.25)
    assert row.dust_tracking_qty == pytest.approx(0.1)
    assert row.position_qty == pytest.approx(0.25)
    assert row.normalized_exposure_qty == pytest.approx(0.25)


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
    assert "BUY,BLOCKED_ENTRY,BUY,HOLD,BUY_BLOCKED,1,1,filtered entry: gap" in out
    assert "BUY,BUY,BUY,BUY,BUY_SUBMIT,0,1,sma golden cross" in out
    assert "HOLD,HOLD,HOLD,HOLD,HOLD,0,1,position held: no exit rule triggered" in out


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
