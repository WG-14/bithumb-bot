from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.db_core import ensure_schema, init_portfolio
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.h74_cycle_state import ensure_h74_cycle_schema, load_h74_cycle_inventory
from bithumb_bot.h74_position_ownership import h74_position_ownership_contract_from_payload


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    ensure_h74_cycle_schema(conn)
    init_portfolio(conn)
    return conn


def _ownership_contract(cycle_id: str):
    return h74_position_ownership_contract_from_payload(
        {
            "cycle_id": cycle_id,
            "h74_cycle_id": cycle_id,
            "authority_hash": "sha256:a",
            "strategy_instance_id": "h74-source-observation",
            "probe_run_id": "probe-run-1",
            "pair": "KRW-BTC",
            "entry_side": "BUY",
            "entry_plan_id": "h74-buy",
            "position_mode": "fixed_fill_qty_until_exit",
            "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
        }
    )


def _order(conn: sqlite3.Connection, *, cycle_id: str | None = "cycle-1") -> None:
    contract_hash = None
    if cycle_id:
        contract_hash = _ownership_contract(cycle_id).contract_hash
    record_order_if_missing(
        conn,
        client_order_id="h74-buy",
        side="BUY",
        qty_req=0.0008,
        price=100_000_000.0,
        strategy_name="daily_participation_sma",
        strategy_instance_id="h74-source-observation",
        cycle_id=cycle_id,
        authority_hash="sha256:a",
        h74_position_ownership_contract_hash=contract_hash,
        probe_run_id="probe-run-1",
        status="NEW",
    )


def test_h74_buy_fill_creates_cycle_state() -> None:
    conn = _conn()
    _order(conn)

    result = apply_fill_and_trade(
        conn,
        client_order_id="h74-buy",
        side="BUY",
        fill_id="fill-1",
        fill_ts=1,
        price=100_000_000.0,
        qty=0.0008,
        fee=32.0,
        strategy_name="daily_participation_sma",
        pair="KRW-BTC",
    )

    inventory = load_h74_cycle_inventory(conn, cycle_id="cycle-1")
    assert result is not None
    assert result["h74_cycle_ownership_created"] == 1
    assert result["h74_exit_authority_ready"] == 1
    assert inventory is not None
    assert inventory.acquired_qty == pytest.approx(0.0008)
    assert conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()["n"] == 1
    assert conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"] == 1


def test_h74_partial_buy_fills_accumulate_same_cycle() -> None:
    conn = _conn()
    _order(conn)

    for fill_id, qty, ts in (("fill-1", 0.0003, 1), ("fill-2", 0.0005, 2)):
        apply_fill_and_trade(
            conn,
            client_order_id="h74-buy",
            side="BUY",
            fill_id=fill_id,
            fill_ts=ts,
            price=100_000_000.0,
            qty=qty,
            fee=12.0,
            strategy_name="daily_participation_sma",
            pair="KRW-BTC",
        )

    inventory = load_h74_cycle_inventory(conn, cycle_id="cycle-1")
    assert inventory is not None
    assert inventory.acquired_qty == pytest.approx(0.0008)
    assert inventory.remaining_cycle_qty == pytest.approx(0.0008)
    assert conn.execute("SELECT COUNT(*) AS n FROM h74_cycle_state").fetchone()["n"] == 1


def test_h74_buy_fill_links_contract_identity_to_remaining_cycle_qty() -> None:
    conn = _conn()
    contract = _ownership_contract("cycle-1")
    _order(conn)

    apply_fill_and_trade(
        conn,
        client_order_id="h74-buy",
        side="BUY",
        fill_id="fill-1",
        fill_ts=1,
        price=100_000_000.0,
        qty=0.0008,
        fee=32.0,
        strategy_name="daily_participation_sma",
        pair="KRW-BTC",
    )

    inventory = load_h74_cycle_inventory(conn, cycle_id=contract.cycle_id)
    row = conn.execute(
        """
        SELECT cycle_id, acquired_qty, sold_qty, locked_exit_qty, contract_hash
        FROM h74_cycle_state
        WHERE cycle_id=?
        """,
        (contract.cycle_id,),
    ).fetchone()
    assert inventory is not None
    assert row is not None
    expected_remaining = float(row["acquired_qty"]) - float(row["sold_qty"]) - float(row["locked_exit_qty"])
    assert contract.cycle_id == inventory.cycle_id
    assert row["cycle_id"] == contract.cycle_id
    assert inventory.contract_hash == contract.contract_hash
    assert row["contract_hash"] == contract.contract_hash
    assert inventory.remaining_cycle_qty == pytest.approx(expected_remaining)
    assert inventory.remaining_cycle_qty == pytest.approx(0.0008)


def test_h74_buy_fill_without_cycle_id_fails_closed() -> None:
    conn = _conn()
    _order(conn, cycle_id=None)

    with pytest.raises(RuntimeError, match="h74_cycle_ownership_incomplete"):
        apply_fill_and_trade(
            conn,
            client_order_id="h74-buy",
            side="BUY",
            fill_id="fill-1",
            fill_ts=1,
            price=100_000_000.0,
            qty=0.0008,
            fee=32.0,
            strategy_name="daily_participation_sma",
            pair="KRW-BTC",
        )

    assert conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM h74_cycle_state").fetchone()["n"] == 0


def test_h74_buy_fill_rejects_order_contract_hash_mismatch() -> None:
    conn = _conn()
    _order(conn)
    conn.execute(
        """
        UPDATE orders
        SET h74_position_ownership_contract_hash=?
        WHERE client_order_id=?
        """,
        ("sha256:mismatch", "h74-buy"),
    )

    with pytest.raises(RuntimeError, match="h74_cycle_ownership_contract_hash_mismatch"):
        apply_fill_and_trade(
            conn,
            client_order_id="h74-buy",
            side="BUY",
            fill_id="fill-1",
            fill_ts=1,
            price=100_000_000.0,
            qty=0.0008,
            fee=32.0,
            strategy_name="daily_participation_sma",
            pair="KRW-BTC",
        )

    assert conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM h74_cycle_state").fetchone()["n"] == 0


def test_h74_cycle_persistence_failure_rolls_back_fill_trade_and_portfolio(monkeypatch) -> None:
    conn = _conn()
    _order(conn)
    before = conn.execute(
        "SELECT asset_available + asset_locked AS asset_qty FROM portfolio WHERE id=1"
    ).fetchone()["asset_qty"]

    def _fail_upsert(*_args, **_kwargs):
        raise RuntimeError("forced_cycle_persistence_failure")

    monkeypatch.setattr("bithumb_bot.h74_cycle_state.upsert_h74_cycle_fill", _fail_upsert)

    with pytest.raises(RuntimeError, match="forced_cycle_persistence_failure"):
        apply_fill_and_trade(
            conn,
            client_order_id="h74-buy",
            side="BUY",
            fill_id="fill-1",
            fill_ts=1,
            price=100_000_000.0,
            qty=0.0008,
            fee=32.0,
            strategy_name="daily_participation_sma",
            pair="KRW-BTC",
        )

    after = conn.execute(
        "SELECT asset_available + asset_locked AS asset_qty FROM portfolio WHERE id=1"
    ).fetchone()["asset_qty"]
    assert conn.execute("SELECT COUNT(*) AS n FROM fills").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"] == 0
    assert conn.execute("SELECT COUNT(*) AS n FROM h74_cycle_state").fetchone()["n"] == 0
    assert after == pytest.approx(before)
