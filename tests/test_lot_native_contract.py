"""Contract-gate tests for the current PASS baseline and full declaration closure."""

from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.decision_context import normalize_strategy_decision_context
from bithumb_bot.dust import (
    DUST_TRACKING_LOT_STATE,
    OPEN_EXPOSURE_LOT_STATE,
    build_position_state_model,
    classify_dust_residual,
    dust_qty_gap_tolerance,
    lot_state_quantity_contract,
)
from bithumb_bot.lifecycle import summarize_position_lots
from bithumb_bot.order_sizing import build_sell_execution_sizing


pytestmark = pytest.mark.fast_regression


# Current contract PASS baseline.


def test_sell_execution_sizing_derives_final_qty_from_canonical_sellable_lot_count() -> None:
    plan = build_sell_execution_sizing(
        pair="BTC_KRW",
        market_price=20_000_000.0,
        sellable_qty=0.0,
        sellable_lot_count=2,
        exit_allowed=True,
        exit_block_reason="none",
    )

    expected_qty = pytest.approx(plan.internal_lot_size * 2)

    assert plan.side == "SELL"
    assert plan.allowed is True
    assert plan.qty_source == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert plan.requested_qty == expected_qty
    assert plan.executable_qty == expected_qty
    assert plan.intended_lot_count == 2
    assert plan.executable_lot_count == 2
    assert plan.block_reason == "none"
    assert plan.decision_reason_code == "none"


@pytest.mark.parametrize(
    "exit_block_reason",
    [
        "dust_only_remainder",
        "boundary_below_min",
        "no_executable_exit_lot",
    ],
)
def test_sell_suppression_categories_remain_normal_suppression_outcomes(
    exit_block_reason: str,
) -> None:
    plan = build_sell_execution_sizing(
        pair="BTC_KRW",
        market_price=20_000_000.0,
        sellable_qty=0.0,
        sellable_lot_count=0,
        exit_allowed=False,
        exit_block_reason=exit_block_reason,
    )

    assert plan.side == "SELL"
    assert plan.allowed is False
    assert plan.requested_qty == pytest.approx(0.0)
    assert plan.executable_qty == pytest.approx(0.0)
    assert plan.intended_lot_count == 0
    assert plan.executable_lot_count == 0
    assert plan.block_reason == exit_block_reason
    assert plan.non_executable_reason == exit_block_reason
    assert plan.qty_source == "position_state.normalized_exposure.sellable_executable_lot_count"


def test_lot_state_quantity_contract_keeps_open_exposure_and_dust_tracking_separate() -> None:
    contract = lot_state_quantity_contract()

    assert contract[OPEN_EXPOSURE_LOT_STATE]["strategy_qty_source"] == "open_exposure_qty"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["strategy_lot_source"] == "open_lot_count"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["sell_submit_lot_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["sell_submit_includes_dust_tracking"] is False
    assert contract[OPEN_EXPOSURE_LOT_STATE]["operator_tracking_only"] is False
    assert contract[DUST_TRACKING_LOT_STATE]["strategy_qty_source"] == "dust_tracking_qty"
    assert contract[DUST_TRACKING_LOT_STATE]["strategy_lot_source"] == "dust_tracking_lot_count"
    assert contract[DUST_TRACKING_LOT_STATE]["sell_submit_qty_source"] == "excluded_from_sell_qty"
    assert contract[DUST_TRACKING_LOT_STATE]["sell_submit_lot_source"] == "excluded_from_sell_lot_count"
    assert contract[DUST_TRACKING_LOT_STATE]["sell_submit_includes_dust_tracking"] is False
    assert contract[DUST_TRACKING_LOT_STATE]["operator_tracking_only"] is True


def test_position_state_model_bases_exitability_and_flatness_on_lot_state_not_qty_aggregation() -> None:
    dust = classify_dust_residual(
        broker_qty=0.00049193,
        local_qty=0.00049193,
        min_qty=0.0001,
        min_notional_krw=5000.0,
        latest_price=40_000_000.0,
        partial_flatten_recent=False,
        partial_flatten_reason="not_recent",
        qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
        matched_harmless_resume_allowed=True,
    )

    model = build_position_state_model(
        raw_qty_open=0.00049193,
        metadata_raw=dust,
        raw_total_asset_qty=0.00049193,
        open_exposure_qty=0.0,
        dust_tracking_qty=0.00049193,
        open_lot_count=0,
        dust_tracking_lot_count=1,
        market_price=40_000_000.0,
        min_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=0.0,
        max_qty_decimals=8,
    )

    assert model.normalized_exposure.open_exposure_qty == pytest.approx(0.0)
    assert model.normalized_exposure.dust_tracking_qty == pytest.approx(0.00049193)
    assert model.normalized_exposure.sellable_executable_qty == pytest.approx(0.0)
    assert model.normalized_exposure.open_lot_count == 0
    assert model.normalized_exposure.dust_tracking_lot_count == 1
    assert model.normalized_exposure.sellable_executable_lot_count == 0
    assert model.normalized_exposure.has_executable_exposure is False
    assert model.normalized_exposure.has_any_position_residue is True
    assert model.normalized_exposure.has_non_executable_residue is True
    assert model.normalized_exposure.has_dust_only_remainder is True
    assert model.normalized_exposure.exit_allowed is False
    assert model.normalized_exposure.exit_block_reason == "dust_only_remainder"
    assert model.normalized_exposure.terminal_state == "dust_only"
    assert model.state_interpretation.operator_outcome == "tracked_unsellable_residual"
    assert model.state_interpretation.exit_submit_expected is False


def test_recovery_lifecycle_keeps_qty_only_legacy_rows_non_authoritative_without_legacy_semantic_marker() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE open_position_lots (
            id INTEGER PRIMARY KEY,
            pair TEXT NOT NULL,
            qty_open REAL NOT NULL,
            position_state TEXT NOT NULL,
            executable_lot_count INTEGER NOT NULL DEFAULT 0,
            dust_tracking_lot_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        INSERT INTO open_position_lots (pair, qty_open, position_state, executable_lot_count, dust_tracking_lot_count)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("BTC_KRW", 0.0004, OPEN_EXPOSURE_LOT_STATE, 0, 0),
    )

    snapshot = summarize_position_lots(conn, pair="BTC_KRW")

    assert snapshot.raw_open_exposure_qty == pytest.approx(0.0)
    assert snapshot.executable_open_exposure_qty == pytest.approx(0.0)
    assert snapshot.open_lot_count == 0
    assert snapshot.dust_tracking_lot_count == 0
    assert snapshot.exit_non_executable_reason == "no_executable_open_lots"
    assert snapshot.position_semantic_basis == "lot-native"
    assert "legacy_lot_metadata_missing" not in snapshot.as_dict().values()


# Full lot-native declaration closure.


def test_decision_context_no_longer_emits_compatibility_fallback_or_provenance_layer() -> None:
    ctx = normalize_strategy_decision_context(
        context={
            "base_signal": "SELL",
            "base_reason": "legacy residue",
            "entry_reason": "legacy residue",
            "raw_qty_open": 0.0004,
            "raw_total_asset_qty": 0.0004,
            "open_exposure_qty": 0.0004,
            "dust_tracking_qty": 0.0,
            "open_lot_count": 0,
            "dust_tracking_lot_count": 0,
            "reserved_exit_lot_count": 0,
            "sellable_executable_lot_count": 0,
            "position_state": {
                "semantic_basis": "legacy",
                "normalized_exposure": {
                    "raw_qty_open": 0.0004,
                    "raw_total_asset_qty": 0.0004,
                    "open_exposure_qty": 0.0004,
                    "dust_tracking_qty": 0.0,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 0,
                    "sellable_executable_lot_count": 0,
                },
            },
        },
        signal="SELL",
        reason="legacy residue",
        strategy_name="contract_gate",
        pair="BTC_KRW",
        interval="1m",
        decision_ts=1,
        candle_ts=1,
        market_price=100_000_000.0,
    )

    assert ctx["open_exposure_qty"] == pytest.approx(0.0)
    assert ctx["sellable_executable_lot_count"] == 0
    assert ctx["submit_lot_count"] == 0
    assert ctx["sell_qty_basis_qty"] == pytest.approx(0.0)
    residue_keys = sorted(
        key
        for key in ctx
        if key == "decision_compatibility_residue"
        or key.endswith("_source")
        or key.endswith("_truth_source")
        or key.endswith("_compatibility_residue")
    )

    assert residue_keys == []


def test_reporting_no_longer_preserves_truth_source_or_provenance_primary_fields() -> None:
    ctx = normalize_strategy_decision_context(
        context={
            "base_signal": "SELL",
            "base_reason": "legacy residue",
            "entry_reason": "legacy residue",
            "raw_qty_open": 0.0004,
            "raw_total_asset_qty": 0.0004,
            "open_exposure_qty": 0.0004,
            "dust_tracking_qty": 0.0,
            "open_lot_count": 0,
            "dust_tracking_lot_count": 0,
            "reserved_exit_lot_count": 0,
            "sellable_executable_lot_count": 0,
            "position_state_source": "context.raw_qty_open",
            "position_state": {
                "semantic_basis": "legacy",
                "normalized_exposure": {
                    "raw_qty_open": 0.0004,
                    "raw_total_asset_qty": 0.0004,
                    "open_exposure_qty": 0.0004,
                    "dust_tracking_qty": 0.0,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 0,
                    "sellable_executable_lot_count": 0,
                },
            },
        },
        signal="SELL",
        reason="legacy residue",
        strategy_name="contract_gate",
        pair="BTC_KRW",
        interval="1m",
        decision_ts=2,
        candle_ts=2,
        market_price=100_000_000.0,
    )

    assert ctx["submit_lot_count"] == 0
    assert ctx["sell_submit_lot_count"] == 0
    primary_provenance_keys = sorted(
        key
        for key in ctx
        if key.endswith("_source")
        or key.endswith("_truth_source")
        or key.endswith("_compatibility_residue")
    )

    assert primary_provenance_keys == []


def test_full_declaration_closure_keeps_current_pass_baseline_and_removes_residue_layers() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE open_position_lots (
            id INTEGER PRIMARY KEY,
            pair TEXT NOT NULL,
            qty_open REAL NOT NULL,
            position_state TEXT NOT NULL,
            executable_lot_count INTEGER NOT NULL DEFAULT 0,
            dust_tracking_lot_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        INSERT INTO open_position_lots (pair, qty_open, position_state, executable_lot_count, dust_tracking_lot_count)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("BTC_KRW", 0.0004, OPEN_EXPOSURE_LOT_STATE, 0, 0),
    )
    snapshot = summarize_position_lots(conn, pair="BTC_KRW")
    conn.close()

    ctx = normalize_strategy_decision_context(
        context={
            "base_signal": "SELL",
            "base_reason": "lot-native closed",
            "entry_reason": "lot-native closed",
            "raw_qty_open": 0.0004,
            "raw_total_asset_qty": 0.0004,
            "open_exposure_qty": 0.0,
            "dust_tracking_qty": 0.0,
            "open_lot_count": 0,
            "dust_tracking_lot_count": 0,
            "reserved_exit_lot_count": 0,
            "sellable_executable_lot_count": 0,
            "position_state_source": "context.raw_qty_open",
            "position_state": {
                "semantic_basis": "lot-native",
                "normalized_exposure": {
                    "raw_qty_open": 0.0004,
                    "raw_total_asset_qty": 0.0004,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.0,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 0,
                    "sellable_executable_lot_count": 0,
                },
            },
        },
        signal="SELL",
        reason="lot-native closed",
        strategy_name="contract_gate",
        pair="BTC_KRW",
        interval="1m",
        decision_ts=3,
        candle_ts=3,
        market_price=100_000_000.0,
    )

    residue_keys = sorted(
        key
        for key in ctx
        if key.endswith("_source")
        or key.endswith("_truth_source")
        or key.endswith("_compatibility_residue")
    )

    assert snapshot.exit_non_executable_reason == "no_executable_open_lots"
    assert snapshot.position_semantic_basis == "lot-native"
    assert ctx["open_exposure_qty"] == pytest.approx(0.0)
    assert ctx["sellable_executable_lot_count"] == 0
    assert ctx["submit_lot_count"] == 0
    assert ctx["sell_qty_basis_qty"] == pytest.approx(0.0)
    assert residue_keys == []


def test_current_contract_pass_still_holds_once_the_declaration_residue_is_gone() -> None:
    ctx = normalize_strategy_decision_context(
        context={
            "base_signal": "SELL",
            "base_reason": "explicit residue bucket closed",
            "entry_reason": "explicit residue bucket closed",
            "raw_qty_open": 0.0004,
            "raw_total_asset_qty": 0.0004,
            "open_exposure_qty": 0.0,
            "dust_tracking_qty": 0.0,
            "open_lot_count": 0,
            "dust_tracking_lot_count": 0,
            "reserved_exit_lot_count": 0,
            "sellable_executable_lot_count": 0,
            "position_state_source": "context.raw_qty_open",
            "position_state": {
                "semantic_basis": "lot-native",
                "normalized_exposure": {
                    "raw_qty_open": 0.0004,
                    "raw_total_asset_qty": 0.0004,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.0,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 0,
                    "sellable_executable_lot_count": 0,
                },
            },
        },
        signal="SELL",
        reason="explicit residue bucket closed",
        strategy_name="contract_gate",
        pair="BTC_KRW",
        interval="1m",
        decision_ts=4,
        candle_ts=4,
        market_price=100_000_000.0,
    )

    current_contract_pass = (
        ctx["open_exposure_qty"] == pytest.approx(0.0)
        and ctx["sellable_executable_lot_count"] == 0
        and ctx["submit_lot_count"] == 0
        and ctx["sell_qty_basis_qty"] == pytest.approx(0.0)
    )
    strict_final_closure_complete = bool(
        not any(
            key.endswith("_source")
            or key.endswith("_truth_source")
            or key.endswith("_compatibility_residue")
            for key in ctx
        )
    )

    assert current_contract_pass is True
    assert strict_final_closure_complete is True
