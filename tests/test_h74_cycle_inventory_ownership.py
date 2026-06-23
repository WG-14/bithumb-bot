from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.db_core import ensure_schema
from bithumb_bot.h74_cycle_state import (
    ensure_h74_cycle_schema,
    load_h74_cycle_inventory,
    load_open_h74_cycle_inventories,
    lock_h74_cycle_exit_qty,
    upsert_h74_cycle_fill,
)
from bithumb_bot.run_loop_execution_planner import _inject_h74_cycle_inventory
from bithumb_bot.target_position import TargetPositionSettings, build_target_position_decision
from bithumb_bot.experiment_execution_contract import POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    ensure_h74_cycle_schema(conn)
    return conn


def test_h74_exit_sells_only_cycle_owned_qty_when_external_btc_exists() -> None:
    decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=100_000.0,
        current_position_snapshot=None,
        readiness_payload={
            "broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0028},
            "projection_converged": True,
            "projection_convergence": {"converged": True},
            "h74_cycle_id": "cycle-1",
            "remaining_cycle_qty": 0.0008,
        },
        order_rules={"min_qty": 0.0001, "qty_step": 0.0001, "min_notional_krw": 5000.0},
        reference_price=100_000_000.0,
        settings=TargetPositionSettings(execution_engine="target_delta", position_mode=POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT),
    )

    assert decision.submit_qty == pytest.approx(0.0008)


def test_h74_partial_entry_fills_accumulate_cycle_acquired_qty() -> None:
    conn = _conn()
    upsert_h74_cycle_fill(conn, cycle_id="cycle-1", authority_hash="sha256:a", strategy_instance_id="h74", pair="KRW-BTC", side="BUY", qty=0.0003, client_order_id="entry", fill_ts=1)
    upsert_h74_cycle_fill(conn, cycle_id="cycle-1", authority_hash="sha256:a", strategy_instance_id="h74", pair="KRW-BTC", side="BUY", qty=0.0005, client_order_id="entry", fill_ts=2)

    inventory = load_h74_cycle_inventory(conn, cycle_id="cycle-1")

    assert inventory is not None
    assert inventory.acquired_qty == pytest.approx(0.0008)
    assert inventory.remaining_cycle_qty == pytest.approx(0.0008)


def test_h74_partial_exit_fills_reduce_remaining_cycle_qty() -> None:
    conn = _conn()
    upsert_h74_cycle_fill(conn, cycle_id="cycle-1", authority_hash="sha256:a", strategy_instance_id="h74", pair="KRW-BTC", side="BUY", qty=0.0008, client_order_id="entry", fill_ts=1)
    upsert_h74_cycle_fill(conn, cycle_id="cycle-1", authority_hash="sha256:a", strategy_instance_id="h74", pair="KRW-BTC", side="SELL", qty=0.0003, client_order_id="exit", fill_ts=2)

    inventory = load_h74_cycle_inventory(conn, cycle_id="cycle-1")

    assert inventory is not None
    assert inventory.sold_qty == pytest.approx(0.0003)
    assert inventory.remaining_cycle_qty == pytest.approx(0.0005)


def test_h74_cycle_id_required_for_live_exit_submit_plan() -> None:
    decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=100_000.0,
        current_position_snapshot=None,
        readiness_payload={"remaining_cycle_qty": 0.0008},
        order_rules={"min_qty": 0.0001, "qty_step": 0.0001, "min_notional_krw": 5000.0},
        reference_price=100_000_000.0,
        settings=TargetPositionSettings(execution_engine="target_delta", position_mode=POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT),
    )

    assert decision.would_submit is False
    assert decision.block_reason == "h74_cycle_id_required_for_exit"


def test_h74_live_exit_planner_loads_remaining_cycle_qty_from_db() -> None:
    conn = _conn()
    upsert_h74_cycle_fill(
        conn,
        cycle_id="cycle-1",
        authority_hash="sha256:a",
        strategy_instance_id="h74",
        pair="KRW-BTC",
        side="BUY",
        qty=0.0008,
        client_order_id="entry",
        fill_ts=1,
    )
    conn.execute(
        "UPDATE h74_cycle_state SET sold_qty=0.0002, locked_exit_qty=0.0001 WHERE cycle_id='cycle-1'"
    )

    loaded = _inject_h74_cycle_inventory(
        conn,
        readiness_payload={
            "broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0028},
            "projection_converged": True,
            "projection_convergence": {"converged": True},
            "h74_cycle_id": "cycle-1",
        },
        planning_context={},
    )
    decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=100_000.0,
        current_position_snapshot=None,
        readiness_payload=loaded,
        order_rules={"min_qty": 0.0001, "qty_step": 0.0001, "min_notional_krw": 5000.0},
        reference_price=100_000_000.0,
        settings=TargetPositionSettings(
            execution_engine="target_delta",
            position_mode=POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
        ),
    )

    assert loaded["remaining_cycle_qty"] == pytest.approx(0.0005)
    assert decision.submit_qty == pytest.approx(0.0005)
    assert decision.submit_qty != pytest.approx(0.0028)


def test_h74_sell_planner_discovers_open_cycle_when_payload_has_no_cycle_id() -> None:
    conn = _conn()
    upsert_h74_cycle_fill(
        conn,
        cycle_id="cycle-1",
        authority_hash="sha256:a",
        strategy_instance_id="h74",
        pair="KRW-BTC",
        side="BUY",
        qty=0.0008,
        client_order_id="entry",
        fill_ts=1,
    )
    conn.execute(
        "UPDATE h74_cycle_state SET sold_qty=0.0002, locked_exit_qty=0.0001 WHERE cycle_id='cycle-1'"
    )

    loaded = _inject_h74_cycle_inventory(
        conn,
        readiness_payload={
            "broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0028},
            "projection_converged": True,
            "projection_convergence": {"converged": True},
            "authority_hash": "sha256:a",
            "strategy_instance_id": "h74",
        },
        planning_context={"runtime_pair": "KRW-BTC"},
    )
    decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=100_000.0,
        current_position_snapshot=None,
        readiness_payload=loaded,
        order_rules={"min_qty": 0.0001, "qty_step": 0.0001, "min_notional_krw": 5000.0},
        reference_price=100_000_000.0,
        settings=TargetPositionSettings(
            execution_engine="target_delta",
            position_mode=POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
        ),
    )

    assert (
        len(
            load_open_h74_cycle_inventories(
                conn,
                strategy_instance_id="h74",
                authority_hash="sha256:a",
                pair="KRW-BTC",
            )
        )
        == 1
    )
    assert loaded["h74_cycle_id"] == "cycle-1"
    assert loaded["remaining_cycle_qty"] == pytest.approx(0.0005)
    assert decision.submit_qty == pytest.approx(0.0005)
    assert decision.block_reason != "h74_cycle_id_required_for_exit"
    assert decision.submit_qty != pytest.approx(0.0028)


def test_h74_sell_planner_blocks_when_multiple_open_cycles_are_ambiguous() -> None:
    conn = _conn()
    for cycle_id in ("cycle-1", "cycle-2"):
        upsert_h74_cycle_fill(
            conn,
            cycle_id=cycle_id,
            authority_hash="sha256:a",
            strategy_instance_id="h74",
            pair="KRW-BTC",
            side="BUY",
            qty=0.0008,
            client_order_id=f"entry-{cycle_id}",
            fill_ts=1,
        )

    loaded = _inject_h74_cycle_inventory(
        conn,
        readiness_payload={"authority_hash": "sha256:a", "strategy_instance_id": "h74"},
        planning_context={"runtime_pair": "KRW-BTC"},
    )
    decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=100_000.0,
        current_position_snapshot=None,
        readiness_payload=loaded,
        order_rules={"min_qty": 0.0001, "qty_step": 0.0001, "min_notional_krw": 5000.0},
        reference_price=100_000_000.0,
        settings=TargetPositionSettings(
            execution_engine="target_delta",
            position_mode=POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
        ),
    )

    assert loaded["h74_cycle_inventory_error"] == "multiple_open_h74_cycles"
    assert loaded["h74_open_cycle_count"] == 2
    assert "h74_cycle_id" not in loaded
    assert decision.would_submit is False
    assert decision.block_reason == "multiple_open_h74_cycles"


def test_h74_exit_submit_locks_pending_exit_qty() -> None:
    conn = _conn()
    upsert_h74_cycle_fill(
        conn,
        cycle_id="cycle-1",
        authority_hash="sha256:a",
        strategy_instance_id="h74",
        pair="KRW-BTC",
        side="BUY",
        qty=0.0008,
        client_order_id="entry",
        fill_ts=1,
    )

    lock_h74_cycle_exit_qty(
        conn,
        cycle_id="cycle-1",
        exit_client_order_id="exit-pending",
        qty=0.0008,
        updated_ts=2,
    )
    inventory = load_h74_cycle_inventory(conn, cycle_id="cycle-1")

    assert inventory is not None
    assert inventory.locked_exit_qty == pytest.approx(0.0008)
    assert inventory.remaining_cycle_qty == pytest.approx(0.0)


def test_h74_exit_submit_updates_locked_exit_qty_in_cycle_state() -> None:
    conn = _conn()
    upsert_h74_cycle_fill(
        conn,
        cycle_id="cycle-1",
        authority_hash="sha256:a",
        strategy_instance_id="h74",
        pair="KRW-BTC",
        side="BUY",
        qty=0.0008,
        client_order_id="entry",
        fill_ts=1,
    )

    lock_h74_cycle_exit_qty(
        conn,
        cycle_id="cycle-1",
        exit_client_order_id="exit-submit-pending",
        qty=0.0005,
        updated_ts=2,
    )
    inventory = load_h74_cycle_inventory(conn, cycle_id="cycle-1")

    assert inventory is not None
    assert inventory.locked_exit_qty == pytest.approx(0.0005)
    assert inventory.remaining_cycle_qty == pytest.approx(0.0003)
