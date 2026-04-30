from __future__ import annotations

import json

import pytest

from bithumb_bot.db_core import ensure_db, record_strategy_decision


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
    assert buy_context["experiment_id"]
    assert buy_context["experiment_fingerprint"] == buy_context["experiment_id"]
    assert buy_context["experiment_fingerprint_version"] == "experiment_fingerprint_v1"
    assert buy_context["experiment_fingerprint_inputs"]["strategy_name"] == "sma_cross"


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
    assert ctx["decision_entry_block_reason"] == "position held: no exit rule triggered"
    assert ctx["entry_block_reason"] == "legacy_lot_metadata_missing"
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


def test_record_strategy_decision_prefers_position_state_normalized_exposure_truth(tmp_path):
    conn = ensure_db(str(tmp_path / "decision_normalized_truth.sqlite"))
    try:
        record_strategy_decision(
            conn,
            decision_ts=1_710_000_240_000,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1_710_000_180_000,
            market_price=102_500_000.0,
            confidence=None,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "raw_qty_open": 0.25,
                "normalized_exposure_active": False,
                "normalized_exposure_qty": 0.0,
                "effective_flat": True,
                "entry_allowed": True,
                "dust_classification": "harmless_dust",
                "position_gate": {
                    "dust_state": "harmless_dust",
                    "effective_flat_due_to_harmless_dust": True,
                    "raw_qty_open": 0.25,
                },
                "position_state": {
                    "raw_holdings": {
                        "classification": "harmless_dust",
                        "present": True,
                        "broker_qty": 0.00009629,
                        "local_qty": 0.00009629,
                        "delta_qty": 0.0,
                        "min_qty": 0.0001,
                        "min_notional_krw": 5000.0,
                        "broker_local_match": True,
                        "compact_summary": "state=harmless_dust",
                    },
                    "normalized_exposure": {
                        "raw_qty_open": 0.00009629,
                        "raw_total_asset_qty": 0.00009629,
                        "dust_classification": "harmless_dust",
                        "dust_state": "harmless_dust",
                        "entry_allowed": True,
                        "entry_block_reason": "none",
                        "effective_flat": True,
                        "effective_flat_due_to_harmless_dust": True,
                        "normalized_exposure_active": False,
                        "normalized_exposure_qty": 0.0,
                        "open_exposure_qty": 0.0,
                        "dust_tracking_qty": 0.00009629,
                        "reserved_exit_qty": 0.0,
                        "sellable_executable_qty": 0.0,
                        "exit_allowed": False,
                        "exit_block_reason": "dust_only_remainder",
                        "terminal_state": "dust_only",
                    },
                    "operator_diagnostics": {
                        "state": "harmless_dust",
                        "state_label": "harmless dust residual",
                        "operator_action": "harmless_dust_tracked_resume_allowed",
                        "operator_message": "ok",
                        "broker_local_match": True,
                        "new_orders_allowed": True,
                        "resume_allowed": True,
                        "treat_as_flat": True,
                    },
                    "state_interpretation": {
                        "lifecycle_state": "dust_only",
                        "lifecycle_label": "tracked unsellable residual",
                        "operator_outcome": "tracked_unsellable_residual",
                        "operator_message": "Residual holdings are tracked as dust at the state layer.",
                        "entry_status": "allowed",
                        "exit_status": "blocked:dust_only_remainder",
                        "exit_submit_expected": False,
                    },
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
    assert ctx["raw_qty_open"] == pytest.approx(0.00009629)
    assert ctx["entry_allowed"] is True
    assert ctx["effective_flat"] is True
    assert ctx["normalized_exposure_active"] is False
    assert ctx["normalized_exposure_qty"] == pytest.approx(0.0)
    assert ctx["open_exposure_qty"] == pytest.approx(0.0)
    assert ctx["dust_tracking_qty"] == pytest.approx(0.00009629)
    assert ctx["reserved_exit_qty"] == pytest.approx(0.0)
    assert ctx["sellable_executable_qty"] == pytest.approx(0.0)
    assert ctx["exit_allowed"] is False
    assert ctx["exit_block_reason"] == "dust_only_remainder"
    assert ctx["sell_submit_lot_count"] == 0
    assert ctx["submit_lot_count"] == 0
    assert ctx["sell_normalized_exposure_qty"] == pytest.approx(0.0)
    assert ctx["position_state"]["state_interpretation"]["operator_outcome"] == "tracked_unsellable_residual"
    assert ctx["open_lot_count"] == 0
    assert ctx["sellable_executable_lot_count"] == 0
    assert "submit_payload_qty" not in ctx["position_state"]["normalized_exposure"]
    assert _collect_residue_paths(ctx) == []
