from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.db_core import ensure_schema, record_execution_plan, record_strategy_decision
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.h74_execution_path_probe import generate_h74_execution_path_probe_report
from bithumb_bot.h74_position_ownership import h74_position_ownership_contract_from_payload
from bithumb_bot.research.hashing import sha256_prefixed


pytestmark = pytest.mark.fast_regression


class _FakeSubmitPlan:
    submit_expected = True
    final_action = "REBALANCE_TO_TARGET"
    block_reason = "none"

    def __init__(self, *, side: str, run_id: str) -> None:
        self.side = side
        self.run_id = run_id

    def as_dict(self) -> dict[str, object]:
        return {
            "side": self.side,
            "qty": 0.001,
            "notional_krw": 100_000.0,
            "idempotency_key": f"{self.run_id}-{self.side.lower()}",
            "source": "h74_source_observation",
            "authority": "h74_entry_submit_semantics_v1",
            "submit_expected": True,
            "final_action": self.final_action,
            "block_reason": self.block_reason,
            "h74_execution_path_probe_run_id": self.run_id,
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


class _FakeExecutionPlanBundle:
    status = "planned"

    def __init__(self, *, side: str, run_id: str) -> None:
        self.submit_plan = _FakeSubmitPlan(side=side, run_id=run_id)
        self.persistence_context: dict[str, object] = {}
        self._side = side
        self._run_id = run_id

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_type": "fake_h74_execution_plan_bundle",
            "side": self._side,
            "h74_execution_path_probe_run_id": self._run_id,
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    return conn


def _allocation(conn: sqlite3.Connection, suffix: str) -> int:
    row = conn.execute(
        """
        INSERT INTO portfolio_allocation_decision(
            bundle_id, allocation_decision_hash, allocation_input_hash,
            allocator_config_hash, strategy_contribution_hash, selected_signal,
            authoritative, primary_block_reason, reason, conflict_resolution_json,
            allocation_decision_json
        ) VALUES (?, ?, ?, ?, ?, ?, 1, 'none', 'none', '{}', '{}')
        """,
        (
            1,
            f"sha256:allocation-{suffix}",
            f"sha256:input-{suffix}",
            f"sha256:config-{suffix}",
            f"sha256:contribution-{suffix}",
            suffix,
        ),
    )
    return int(row.lastrowid)


def _record_plan(conn: sqlite3.Connection, *, side: str, run_id: str) -> int:
    refs = record_execution_plan(
        conn,
        allocation_id=_allocation(conn, f"{run_id}-{side}"),
        portfolio_target_hash=None,
        execution_plan_bundle=_FakeExecutionPlanBundle(side=side, run_id=run_id),
    )
    return int(refs["execution_plan_id"])


def test_ensure_schema_adds_queryable_probe_run_columns() -> None:
    conn = _conn()

    for table in (
        "strategy_decisions",
        "execution_plan",
        "orders",
        "order_events",
        "fills",
        "trades",
        "open_position_lots",
        "trade_lifecycles",
        "portfolio",
    ):
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        assert "probe_run_id" in columns


def test_h74_submit_plan_probe_run_id_persists_to_execution_plan() -> None:
    conn = _conn()

    plan_id = _record_plan(conn, side="BUY", run_id="probe-persist-1")

    row = conn.execute(
        "SELECT probe_run_id, submit_plan_side, execution_submit_plan_json FROM execution_plan WHERE id=?",
        (plan_id,),
    ).fetchone()
    assert row["probe_run_id"] == "probe-persist-1"
    assert row["submit_plan_side"] == "BUY"
    assert "h74_execution_path_probe_run_id" in row["execution_submit_plan_json"]


def test_probe_submit_path_persists_queryable_evidence_and_reports_pass() -> None:
    conn = _conn()
    run_id = "probe-integrated-1"
    base_ts = 1_700_000_000_000

    buy_plan_id = _record_plan(conn, side="BUY", run_id=run_id)
    sell_plan_id = _record_plan(conn, side="SELL", run_id=run_id)
    cycle_id = "probe-integrated-cycle-1"
    authority_hash = "sha256:" + "a" * 64
    strategy_instance_id = "h74-source-observation"
    ownership_contract = h74_position_ownership_contract_from_payload(
        {
            "cycle_id": cycle_id,
            "h74_cycle_id": cycle_id,
            "authority_hash": authority_hash,
            "strategy_instance_id": strategy_instance_id,
            "probe_run_id": run_id,
            "pair": "KRW-BTC",
            "entry_side": "BUY",
            "entry_plan_id": "probe-buy",
            "position_mode": "fixed_fill_qty_until_exit",
            "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
        }
    )
    buy_decision_id = record_strategy_decision(
        conn,
        decision_ts=base_ts,
        strategy_name="daily_participation_sma",
        signal="BUY",
        reason="h74_probe_buy",
        candle_ts=base_ts,
        market_price=100_000_000.0,
        context={"h74_execution_path_probe_run_id": run_id},
        execution_plan_id=buy_plan_id,
    )
    sell_decision_id = record_strategy_decision(
        conn,
        decision_ts=base_ts + 60_000,
        strategy_name="daily_participation_sma",
        signal="SELL",
        reason="h74_probe_sell",
        candle_ts=base_ts + 60_000,
        market_price=100_000_000.0,
        context={"h74_execution_path_probe_run_id": run_id},
        execution_plan_id=sell_plan_id,
    )

    record_order_if_missing(
        conn,
        client_order_id="probe-buy",
        side="BUY",
        qty_req=0.001,
        price=None,
        symbol="KRW-BTC",
        strategy_name="daily_participation_sma",
        strategy_instance_id=strategy_instance_id,
        cycle_id=cycle_id,
        authority_hash=authority_hash,
        h74_entry_plan_client_order_id=ownership_contract.entry_plan_id,
        h74_position_ownership_contract_hash=ownership_contract.contract_hash,
        h74_position_ownership_contract=ownership_contract.as_dict(),
        entry_decision_id=buy_decision_id,
        probe_run_id=run_id,
        ts_ms=base_ts,
        status="PENDING_SUBMIT",
    )
    conn.execute(
        """
        INSERT INTO order_events(probe_run_id, client_order_id, event_type, event_ts, side, symbol)
        VALUES (?, 'probe-buy', 'submit_confirmed', ?, 'BUY', 'KRW-BTC')
        """,
        (run_id, base_ts),
    )
    apply_fill_and_trade(
        conn,
        client_order_id="probe-buy",
        side="BUY",
        fill_id="probe-buy-fill",
        fill_ts=base_ts + 1_000,
        price=100_000_000.0,
        qty=0.001,
        fee=0.0,
        strategy_name="daily_participation_sma",
        entry_decision_id=buy_decision_id,
        pair="KRW-BTC",
    )

    record_order_if_missing(
        conn,
        client_order_id="probe-sell",
        side="SELL",
        qty_req=0.001,
        price=None,
        symbol="KRW-BTC",
        strategy_name="daily_participation_sma",
        strategy_instance_id=strategy_instance_id,
        cycle_id=cycle_id,
        authority_hash=authority_hash,
        h74_entry_plan_client_order_id=ownership_contract.entry_plan_id,
        h74_position_ownership_contract_hash=ownership_contract.contract_hash,
        h74_position_ownership_contract=ownership_contract.as_dict(),
        exit_decision_id=sell_decision_id,
        probe_run_id=run_id,
        ts_ms=base_ts + 60_000,
        status="PENDING_SUBMIT",
    )
    conn.execute(
        """
        INSERT INTO order_events(probe_run_id, client_order_id, event_type, event_ts, side, symbol)
        VALUES (?, 'probe-sell', 'submit_confirmed', ?, 'SELL', 'KRW-BTC')
        """,
        (run_id, base_ts + 60_000),
    )
    apply_fill_and_trade(
        conn,
        client_order_id="probe-sell",
        side="SELL",
        fill_id="probe-sell-fill",
        fill_ts=base_ts + 61_000,
        price=100_000_000.0,
        qty=0.001,
        fee=0.0,
        strategy_name="daily_participation_sma",
        exit_decision_id=sell_decision_id,
        exit_reason="h74_probe_exit",
        exit_rule_name="h74_probe_roundtrip",
        pair="KRW-BTC",
    )

    report = generate_h74_execution_path_probe_report(conn, probe_run_id=run_id)

    assert report["execution_path_probe_status"] == "PASS"
    assert conn.execute("SELECT probe_run_id FROM orders WHERE client_order_id='probe-buy'").fetchone()[0] == run_id
    assert conn.execute("SELECT probe_run_id FROM fills WHERE client_order_id='probe-buy'").fetchone()[0] == run_id
    assert conn.execute("SELECT probe_run_id FROM trades WHERE client_order_id='probe-buy'").fetchone()[0] == run_id
    assert conn.execute("SELECT probe_run_id FROM open_position_lots LIMIT 1").fetchone()[0] == run_id
    assert conn.execute("SELECT probe_run_id FROM trade_lifecycles LIMIT 1").fetchone()[0] == run_id
    assert conn.execute("SELECT probe_run_id FROM portfolio WHERE id=1").fetchone()[0] == run_id


def test_normal_non_probe_execution_does_not_require_probe_run_id() -> None:
    conn = _conn()

    record_order_if_missing(
        conn,
        client_order_id="normal-buy",
        side="BUY",
        qty_req=0.001,
        price=None,
        symbol="KRW-BTC",
        ts_ms=1_700_000_000_000,
        status="PENDING_SUBMIT",
    )
    apply_fill_and_trade(
        conn,
        client_order_id="normal-buy",
        side="BUY",
        fill_id="normal-buy-fill",
        fill_ts=1_700_000_001_000,
        price=100_000_000.0,
        qty=0.001,
        fee=0.0,
        pair="KRW-BTC",
    )

    assert conn.execute("SELECT probe_run_id FROM orders WHERE client_order_id='normal-buy'").fetchone()[0] is None
    assert conn.execute("SELECT probe_run_id FROM fills WHERE client_order_id='normal-buy'").fetchone()[0] is None
    assert conn.execute("SELECT probe_run_id FROM trades WHERE client_order_id='normal-buy'").fetchone()[0] is None
