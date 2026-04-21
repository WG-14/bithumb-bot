from __future__ import annotations

import json

import pytest

from bithumb_bot.app import main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, record_strategy_decision
from bithumb_bot.decision_context import resolve_canonical_position_exposure_snapshot
from bithumb_bot.reporting import fetch_decision_telemetry_summary, fetch_recent_decision_flow


def _collect_residue_paths(value, path: str = "") -> list[str]:
    if isinstance(value, dict):
        found: list[str] = []
        for key, item in value.items():
            key_text = str(key)
            next_path = f"{path}.{key_text}" if path else key_text
            if (
                key_text == "decision_compatibility_residue"
                or key_text.endswith("_source")
                or key_text.endswith("_truth_source")
                or key_text.endswith("_compatibility_residue")
            ):
                found.append(next_path)
            found.extend(_collect_residue_paths(item, next_path))
        return found
    if isinstance(value, list):
        found: list[str] = []
        for index, item in enumerate(value):
            next_path = f"{path}[{index}]" if path else f"[{index}]"
            found.extend(_collect_residue_paths(item, next_path))
        return found
    return []


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
    assert _collect_residue_paths(ctx) == []


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
                "open_lot_count": 1,
                "dust_tracking_lot_count": 1,
                "reserved_exit_lot_count": 0,
                "sellable_executable_lot_count": 1,
                "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
                "position_state_source": "context.raw_qty_open",
                "normalized_exposure_active": True,
                "has_executable_exposure": True,
                "has_any_position_residue": True,
                "has_non_executable_residue": False,
                "has_dust_only_remainder": False,
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
                "open_lot_count": 1,
                "dust_tracking_lot_count": 1,
                "reserved_exit_lot_count": 0,
                "sellable_executable_lot_count": 1,
                "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
                "position_state_source": "context.raw_qty_open",
                "normalized_exposure_active": True,
                "has_executable_exposure": False,
                "has_any_position_residue": True,
                "has_non_executable_residue": True,
                "has_dust_only_remainder": True,
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
    assert "observed_position_qty" in out
    assert "observed_submit_payload_qty" in out
    assert "open_exposure_qty" in out
    assert "dust_tracking_qty" in out
    assert "observed_sell_qty_basis_qty" in out
    assert "sell_normalized_exposure_qty" in out
    assert "sell_open_exposure_qty" in out
    assert "sell_dust_tracking_qty" in out
    assert "BUY,HOLD,BUY,HOLD,BUY_BLOCKED,1,0,position held: no exit rule triggered" in out
    assert "harmless_dust" in out
    assert "0.00009629" in out
    assert "_source" not in out
    assert "_truth_source" not in out


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
    assert ctx["entry_gate_effective_flat"] is True
    assert ctx["holding_authority_state"] == "dust_only"
    assert ctx["normalized_exposure_active"] is False
    assert ctx["has_executable_exposure"] is False
    assert ctx["has_any_position_residue"] is True
    assert ctx["has_non_executable_residue"] is True
    assert ctx["has_dust_only_remainder"] is True
    assert ctx["raw_total_asset_qty"] == 0.00019192
    assert ctx["position_qty"] == 0.0
    assert ctx["submit_payload_qty"] == pytest.approx(0.0)
    assert ctx["open_exposure_qty"] == 0.0
    assert ctx["dust_tracking_qty"] == 0.00009563
    assert ctx["open_lot_count"] == 0
    assert ctx["sellable_executable_lot_count"] == 0
    assert ctx["exit_allowed"] is False
    assert ctx["exit_block_reason"] == "dust_only_remainder"
    assert ctx["holding_authority_state"] == "dust_only"
    assert ctx["submit_lot_count"] == 0
    assert ctx["sell_qty_basis_qty"] == pytest.approx(0.0)
    assert ctx["sell_qty_boundary_kind"] == "none"
    assert ctx["sell_normalized_exposure_qty"] == pytest.approx(0.0)
    assert ctx["sell_open_exposure_qty"] == pytest.approx(0.0)
    assert ctx["sell_dust_tracking_qty"] == pytest.approx(0.00009563)
    assert ctx["position_state"]["normalized_exposure"]["entry_allowed"] is True
    assert ctx["position_state"]["normalized_exposure"]["entry_gate_effective_flat"] is True
    assert ctx["position_state"]["normalized_exposure"]["holding_authority_state"] == "dust_only"
    assert ctx["position_state"]["normalized_exposure"]["normalized_exposure_active"] is False
    assert ctx["position_state"]["normalized_exposure"]["has_executable_exposure"] is False
    assert ctx["position_state"]["normalized_exposure"]["effective_flat"] is True
    assert ctx["position_state"]["normalized_exposure"]["exit_allowed"] is False
    assert ctx["position_state"]["normalized_exposure"]["exit_block_reason"] == "dust_only_remainder"
    assert _collect_residue_paths(ctx) == []


def test_record_strategy_decision_ignores_stale_position_entry_block_reason_when_canonical_allows_entry(
    tmp_path,
    monkeypatch,
):
    db_path = str(tmp_path / "decision-stale-entry-block.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=1,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="dust_only_remainder",
            candle_ts=1,
            market_price=100_000_000.0,
            context={
                "base_signal": "BUY",
                "raw_signal": "BUY",
                "final_signal": "HOLD",
                "entry_blocked": True,
                "entry_block_reason": "dust_only_remainder",
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.0,
                        "raw_total_asset_qty": 0.00019996,
                        "open_exposure_qty": 0.0,
                        "dust_tracking_qty": 0.00019996,
                        "open_lot_count": 0,
                        "dust_tracking_lot_count": 1,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 0,
                        "reserved_exit_qty": 0.0,
                        "sellable_executable_qty": 0.0,
                        "entry_allowed": True,
                        "entry_block_reason": "none",
                        "effective_flat": True,
                        "entry_gate_effective_flat": True,
                        "has_executable_exposure": False,
                        "has_any_position_residue": True,
                        "has_non_executable_residue": True,
                        "has_dust_only_remainder": True,
                        "exit_allowed": False,
                        "exit_block_reason": "dust_only_remainder",
                        "terminal_state": "dust_only",
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
    snapshot = resolve_canonical_position_exposure_snapshot(ctx)
    assert snapshot.entry_allowed is True
    assert snapshot.entry_block_reason == "none"
    assert ctx["entry_allowed"] is True
    assert ctx["entry_block_reason"] == "none"
    assert ctx["position_state"]["normalized_exposure"]["entry_block_reason"] == "none"
    assert "stale_position_entry_block_reason_ignored" in ctx["authority_anomalies"]


def test_decision_telemetry_summary_prefers_canonical_normalized_exposure_snapshot(tmp_path, monkeypatch):
    db_path = str(tmp_path / "decision-telemetry-canonical.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=1,
            strategy_name="sma_with_filter",
            signal="SELL",
            reason="dust only",
            candle_ts=1,
            market_price=1.0,
            context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "raw_qty_open": 0.5,
                "raw_total_asset_qty": 0.5,
                "open_exposure_qty": 0.5,
                "normalized_exposure_qty": 0.5,
                "sellable_executable_lot_count": 5,
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.5,
                        "raw_total_asset_qty": 0.5,
                        "entry_allowed": False,
                        "effective_flat": False,
                        "normalized_exposure_active": False,
                        "normalized_exposure_qty": 0.0,
                        "has_executable_exposure": False,
                        "has_any_position_residue": True,
                        "has_non_executable_residue": True,
                        "has_dust_only_remainder": True,
                        "open_exposure_qty": 0.0,
                        "dust_tracking_qty": 0.5,
                        "open_lot_count": 0,
                        "dust_tracking_lot_count": 5,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 0,
                        "reserved_exit_qty": 0.0,
                        "sellable_executable_qty": 0.0,
                        "exit_allowed": False,
                        "exit_block_reason": "dust_only_remainder",
                        "sell_qty_boundary_kind": "none",
                    }
                },
            },
        )
        conn.commit()
        summary = fetch_decision_telemetry_summary(conn, limit=10)
    finally:
        conn.close()

    assert len(summary) == 1
    row = summary[0]
    assert row.entry_allowed is False
    assert row.normalized_exposure_active is False
    assert row.normalized_exposure_qty == pytest.approx(0.0)
    assert row.position_qty == pytest.approx(0.0)
    assert row.open_exposure_qty == pytest.approx(0.0)
    assert row.dust_tracking_qty == pytest.approx(0.5)
    assert row.sell_submit_lot_count == 0
    assert row.sell_normalized_exposure_qty == pytest.approx(0.0)


@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_decision_telemetry_summary_treats_qty_only_context_as_diagnostic_only(
    tmp_path,
    monkeypatch,
):
    db_path = str(tmp_path / "decision-telemetry-qty-only.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        record_strategy_decision(
            conn,
            decision_ts=4,
            strategy_name="sma_with_filter",
            signal="SELL",
            reason="legacy qty-only residue",
            candle_ts=4,
            market_price=1.0,
            context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "raw_total_asset_qty": 0.5,
                "open_exposure_qty": 0.5,
                "sellable_executable_qty": 0.5,
                "sellable_executable_lot_count": 5,
                "exit_allowed": True,
            },
        )
        conn.commit()
        rows = fetch_decision_telemetry_summary(conn, limit=20)
    finally:
        conn.close()

    assert len(rows) == 1
    row = rows[0]
    assert row.raw_total_asset_qty == pytest.approx(0.5)
    assert row.position_qty == pytest.approx(0.0)
    assert row.open_exposure_qty == pytest.approx(0.0)
    assert row.sell_submit_lot_count == 0
    assert row.sell_normalized_exposure_qty == pytest.approx(0.0)


@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_recent_decision_flow_does_not_reconstruct_sell_basis_from_observational_qty(
    tmp_path,
    monkeypatch,
):
    db_path = str(tmp_path / "decision-flow-observational-basis.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO strategy_decisions(
                decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                5,
                "sma_with_filter",
                "SELL",
                "legacy observational basis only",
                5,
                1.0,
                None,
                json.dumps(
                    {
                        "base_signal": "SELL",
                        "final_signal": "SELL",
                        "sell_open_exposure_qty": 0.0004,
                        "sell_normalized_exposure_qty": 0.0004,
                        "open_exposure_qty": 0.0004,
                        "raw_total_asset_qty": 0.0004,
                    }
                ),
            ),
        )
        conn.commit()
        rows = fetch_recent_decision_flow(conn, limit=10)
    finally:
        conn.close()

    assert len(rows) == 1
    row = rows[0]
    assert row.sell_open_exposure_qty == pytest.approx(0.0)
    assert row.sell_normalized_exposure_qty == pytest.approx(0.0)
    assert row.sell_qty_basis_qty == pytest.approx(0.0)
    assert row.sell_submit_lot_count == 0


@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_recent_decision_flow_prefers_canonical_normalized_exposure_over_shadow_qty_fields(
    tmp_path,
    monkeypatch,
):
    db_path = str(tmp_path / "decision-flow-canonical-shadow.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO strategy_decisions(
                decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                6,
                "sma_with_filter",
                "SELL",
                "shadow qty residue",
                6,
                1.0,
                None,
                json.dumps(
                    {
                        "base_signal": "SELL",
                        "final_signal": "SELL",
                        "raw_qty_open": 9.9,
                        "raw_total_asset_qty": 9.9,
                        "open_exposure_qty": 9.9,
                        "normalized_exposure_qty": 9.9,
                        "sellable_executable_lot_count": 99,
                        "sellable_executable_qty": 9.9,
                        "position_state": {
                            "normalized_exposure": {
                                "raw_qty_open": 0.3,
                                "raw_total_asset_qty": 0.35,
                                "open_exposure_qty": 0.25,
                                "dust_tracking_qty": 0.1,
                                "open_lot_count": 1,
                                "dust_tracking_lot_count": 1,
                                "reserved_exit_lot_count": 0,
                                "sellable_executable_lot_count": 1,
                                "sellable_executable_qty": 0.25,
                                "normalized_exposure_qty": 0.25,
                                "normalized_exposure_active": True,
                                "entry_allowed": False,
                                "effective_flat": False,
                                "exit_allowed": True,
                                "exit_block_reason": "none",
                            }
                        },
                    }
                ),
            ),
        )
        conn.commit()
        rows = fetch_recent_decision_flow(conn, limit=10)
    finally:
        conn.close()

    assert len(rows) == 1
    row = rows[0]
    assert row.raw_qty_open == pytest.approx(0.3)
    assert row.raw_total_asset_qty == pytest.approx(0.35)
    assert row.position_qty == pytest.approx(0.25)
    assert row.open_exposure_qty == pytest.approx(0.25)
    assert row.dust_tracking_qty == pytest.approx(0.1)
    assert row.sell_submit_lot_count == 1
    assert row.sell_normalized_exposure_qty == pytest.approx(0.25)


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
                "submit_lot_count": 1,
                "submit_qty_source": "position_state.raw_total_asset_qty",
                "position_state_source": "context.raw_qty_open",
                "has_executable_exposure": True,
                "has_any_position_residue": True,
                "has_non_executable_residue": False,
                "has_dust_only_remainder": False,
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
                        "open_lot_count": 1,
                        "dust_tracking_lot_count": 1,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 1,
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
    assert ctx["submit_lot_count"] == 1
    assert ctx["sell_qty_basis_qty"] == pytest.approx(0.00009999)
    assert _collect_residue_paths(ctx) == []


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
    assert ctx["submit_lot_count"] == 0
    assert ctx["position_state"]["normalized_exposure"]["raw_total_asset_qty"] == 0.0
    assert ctx["position_state"]["normalized_exposure"]["open_exposure_qty"] == 0.0
    assert ctx["position_state"]["normalized_exposure"]["dust_tracking_qty"] == 0.0
    assert _collect_residue_paths(ctx) == []


@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_decision_telemetry_prefers_normalized_position_state_over_shadow_top_level_values(
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
                        "open_lot_count": 1,
                        "dust_tracking_lot_count": 1,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 1,
                        "normalized_exposure_qty": 0.25,
                        "entry_allowed": False,
                        "effective_flat": False,
                        "normalized_exposure_active": True,
                        "has_executable_exposure": True,
                        "has_any_position_residue": True,
                        "has_non_executable_residue": False,
                        "has_dust_only_remainder": False,
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


@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_canonical_exposure_snapshot_ignores_shadow_top_level_sell_authority_fields() -> None:
    snapshot = resolve_canonical_position_exposure_snapshot(
        {
            "raw_total_asset_qty": 9.9,
            "open_exposure_qty": 9.9,
            "sellable_executable_lot_count": 99,
            "sellable_executable_qty": 9.9,
            "exit_allowed": True,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.3,
                    "raw_total_asset_qty": 0.35,
                    "open_exposure_qty": 0.25,
                    "dust_tracking_qty": 0.1,
                    "open_lot_count": 1,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_lot_count": 1,
                    "reserved_exit_qty": 0.0,
                    "sellable_executable_qty": 0.25,
                    "exit_allowed": True,
                    "exit_block_reason": "none",
                    "normalized_exposure_qty": 0.25,
                    "normalized_exposure_active": True,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        }
    )

    assert snapshot.raw_total_asset_qty == pytest.approx(0.35)
    assert snapshot.open_exposure_qty == pytest.approx(0.25)
    assert snapshot.sellable_executable_lot_count == 1
    assert snapshot.sellable_executable_qty == pytest.approx(0.25)
    assert snapshot.exit_allowed is True


@pytest.mark.lot_native_regression_gate
def test_lot_native_gate_canonical_exposure_snapshot_keeps_reserved_exit_and_dust_mix_non_sellable() -> None:
    snapshot = resolve_canonical_position_exposure_snapshot(
        {
            "raw_total_asset_qty": 0.0005,
            "open_exposure_qty": 0.0005,
            "sellable_executable_qty": 0.0005,
            "sellable_executable_lot_count": 5,
            "exit_allowed": True,
            "exit_block_reason": "none",
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0004,
                    "raw_total_asset_qty": 0.0005,
                    "open_exposure_qty": 0.0004,
                    "dust_tracking_qty": 0.0001,
                    "open_lot_count": 4,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_qty": 0.0004,
                    "reserved_exit_lot_count": 4,
                    "sellable_executable_qty": 0.0,
                    "sellable_executable_lot_count": 0,
                    "exit_allowed": False,
                    "exit_block_reason": "no_executable_exit_lot",
                    "normalized_exposure_qty": 0.0,
                    "normalized_exposure_active": True,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        }
    )

    assert snapshot.raw_total_asset_qty == pytest.approx(0.0005)
    assert snapshot.open_exposure_qty == pytest.approx(0.0004)
    assert snapshot.dust_tracking_qty == pytest.approx(0.0001)
    assert snapshot.reserved_exit_qty == pytest.approx(0.0004)
    assert snapshot.reserved_exit_lot_count == 4
    assert snapshot.sellable_executable_lot_count == 0
    assert snapshot.sellable_executable_qty == pytest.approx(0.0)
    assert snapshot.sell_qty_basis_qty == pytest.approx(0.0)
    assert snapshot.sell_submit_lot_count == 0
    assert snapshot.exit_allowed is False


def test_canonical_exposure_snapshot_surfaces_recovery_block_on_same_authority_object() -> None:
    snapshot = resolve_canonical_position_exposure_snapshot(
        {
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.00009,
                    "raw_total_asset_qty": 0.00009,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.00009,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_lot_count": 0,
                    "reserved_exit_qty": 0.0,
                    "sellable_executable_qty": 0.0,
                    "exit_allowed": False,
                    "exit_block_reason": "dust_only_remainder",
                    "normalized_exposure_qty": 0.0,
                    "normalized_exposure_active": False,
                    "entry_allowed": False,
                    "effective_flat": False,
                    "has_executable_exposure": False,
                    "has_any_position_residue": True,
                    "has_non_executable_residue": True,
                    "has_dust_only_remainder": True,
                    "unresolved_order_count": 1,
                    "recovery_required_count": 1,
                }
            }
        }
    )

    assert snapshot.has_dust_only_remainder is True
    assert snapshot.has_executable_exposure is False
    assert snapshot.recovery_blocked is True
    assert snapshot.recovery_block_reason == "recovery_required_and_unresolved_orders_present"
    assert snapshot.unresolved_order_count == 1
    assert snapshot.recovery_required_count == 1


@pytest.mark.lot_native_regression_gate
def test_canonical_exposure_snapshot_fail_closes_qty_only_exit_context() -> None:
    snapshot = resolve_canonical_position_exposure_snapshot(
        {
            "raw_total_asset_qty": 0.0002,
            "open_exposure_qty": 0.0002,
            "sellable_executable_qty": 0.0002,
            "sellable_executable_lot_count": 2,
            "exit_allowed": True,
            "exit_block_reason": "none",
        }
    )

    assert snapshot.raw_total_asset_qty == pytest.approx(0.0002)
    assert snapshot.open_exposure_qty == pytest.approx(0.0)
    assert snapshot.sellable_executable_lot_count == 0
    assert snapshot.sellable_executable_qty == pytest.approx(0.0)
    assert snapshot.exit_allowed is False


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
