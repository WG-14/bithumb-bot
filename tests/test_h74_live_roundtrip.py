from __future__ import annotations

import sqlite3
import json
from pathlib import Path

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.broker.base import BrokerFill, BrokerOrder
from bithumb_bot.config import settings
from bithumb_bot.decision_equivalence import sha256_prefixed
from bithumb_bot.db_core import ensure_db, init_portfolio, record_broker_fill_observation
from bithumb_bot.execution import apply_fill_and_trade, apply_fill_principal_with_pending_fee, record_order_if_missing
from bithumb_bot.h74_cycle_state import build_h74_cycle_id, load_h74_cycle_inventory
from bithumb_bot.h74_live_rehearsal import H74LiveRehearsalConfig, run_h74_live_rehearsal
from bithumb_bot.run_loop_execution_planner import _inject_h74_cycle_inventory
from bithumb_bot.oms import set_status
from bithumb_bot.order_settlement import OrderSettlementCoordinator, SettlementBarrierConfig
from bithumb_bot.runtime.daily_participation_claims import (
    DailyParticipationClaimKey,
)
from bithumb_bot.runtime.daily_participation_count_provider import build_runtime_daily_count_snapshot_from_sqlite
from bithumb_bot.runtime.live_order_settlement import LiveOrderSettlementWrapper, _order_fill_evidence
from bithumb_bot.runtime_readiness import compute_runtime_readiness_snapshot
from bithumb_bot.strategy.daily_participation_policy import (
    DailyParticipationPolicyConfig,
    DailyParticipationStateSnapshot,
    evaluate_daily_participation_policy,
)


FIXTURE = Path(__file__).parent / "fixtures" / "bithumb" / "live_paid_fee_single_fill_buy_2026_04_24.json"


def _source_artifact(tmp_path) -> str:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "runtime_base_cost_assumption": {"fee_rate": 0.0004, "slippage_bps": 10},
                "candle_timing": "closed_candle_kst",
                "position_mode": "fixed_fill_qty_until_exit",
                "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
                "residual_inventory_mode": "terminal_dust_reported_not_reused_without_authority",
                "initial_position_policy": "flat_start_required",
                "partial_fill_policy": "accumulate_cycle_acquired_qty",
                "fee_application_policy": "repository_observed_fee_fields",
            }
        ),
        encoding="utf-8",
    )
    return str(source)


def _claim_key() -> DailyParticipationClaimKey:
    return DailyParticipationClaimKey(
        strategy_instance_id="h74-source-observation",
        pair="KRW-BTC",
        kst_day="2026-06-22",
        participation_policy_hash=_participation_config().policy_hash(),
    )


def _participation_config() -> DailyParticipationPolicyConfig:
    return DailyParticipationPolicyConfig(
        enabled=True,
        timezone="Asia/Seoul",
        count_basis="filled",
        window_start_hour=10,
        window_end_hour=11,
        buy_fraction=0.05,
        max_order_krw=100_000.0,
        fallback_mode="unconditional_participation",
    )


@pytest.fixture
def roundtrip_db(tmp_path, monkeypatch):
    original_db_path = settings.DB_PATH
    original_mode = settings.MODE
    original_pair = settings.PAIR
    original_interval = settings.INTERVAL
    original_start_cash = settings.START_CASH_KRW
    original_fee_min_notional = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    original_fee_ratio_min = settings.LIVE_FILL_FEE_RATIO_MIN
    original_fee_ratio_max = settings.LIVE_FILL_FEE_RATIO_MAX
    original_min_qty = settings.LIVE_MIN_ORDER_QTY
    original_qty_step = settings.LIVE_ORDER_QTY_STEP
    db_path = tmp_path / "h74-roundtrip.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "INTERVAL", "1m")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 1.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_RATIO_MIN", 0.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_RATIO_MAX", 0.01)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    conn = ensure_db(str(db_path))
    init_portfolio(conn)
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="roundtrip_fixture_initial_flat",
        metadata={
            "broker_qty_known": True,
            "broker_asset_qty": 0.0,
            "broker_asset_available": 0.0,
            "broker_asset_locked": 0.0,
            "base_currency": "BTC",
            "quote_currency": "KRW",
            "balance_observed_ts_ms": 1_777_048_623_000,
            "balance_source": "h74_roundtrip_fixture",
            "balance_source_stale": False,
        },
        now_epoch_sec=1.0,
    )
    try:
        yield db_path
    finally:
        conn.close()
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="roundtrip_fixture_reset",
            metadata={},
            now_epoch_sec=1.0,
        )
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "PAIR", original_pair)
        object.__setattr__(settings, "INTERVAL", original_interval)
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)
        object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", original_fee_min_notional)
        object.__setattr__(settings, "LIVE_FILL_FEE_RATIO_MIN", original_fee_ratio_min)
        object.__setattr__(settings, "LIVE_FILL_FEE_RATIO_MAX", original_fee_ratio_max)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", original_min_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_qty_step)


def _recorded_broker_roundtrip() -> tuple[BrokerOrder, list[BrokerFill]]:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    trade = payload["trade"]
    fee = float(payload["order_fee_fields"]["paid_fee"])
    client_order_id = "h74-buy-1"
    exchange_order_id = str(trade["uuid"])
    qty = float(trade["volume"])
    price = float(trade["price"])
    ts_ms = 1_777_048_623_000
    return (
        BrokerOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            side="BUY",
            status="FILLED",
            price=price,
            qty_req=qty,
            qty_filled=qty,
            created_ts=ts_ms,
            updated_ts=ts_ms,
            raw=payload,
        ),
        [
            BrokerFill(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                fill_id=exchange_order_id,
                fill_ts=ts_ms,
                price=price,
                qty=qty,
                fee=fee,
                fee_status="complete",
                fee_source="order_level_paid_fee",
                fee_confidence="authoritative",
                fee_provenance=str(FIXTURE),
                raw=payload,
            )
        ],
    )


class _RecordedBroker:
    def __init__(self, order: BrokerOrder, fills: list[BrokerFill]) -> None:
        self.order = order
        self.fills = fills

    def get_order(self, **_kwargs):
        return self.order

    def get_fills(self, **_kwargs):
        return list(self.fills)


def _conn(db_path: Path) -> sqlite3.Connection:
    return ensure_db(str(db_path))


def _runtime_settlement(order: BrokerOrder, fills: list[BrokerFill], db_path: Path):
    return LiveOrderSettlementWrapper(
        broker=_RecordedBroker(order, fills),
        db_factory=lambda: _conn(db_path),
        coordinator=OrderSettlementCoordinator(
            SettlementBarrierConfig(max_attempts=1, poll_intervals_ms=(0,), deadline_ms=100),
            sleeper=lambda _seconds: None,
        ),
    )(
        {
            "client_order_id": order.client_order_id,
            "exchange_order_id": order.exchange_order_id,
            "side": order.side,
            "filled_qty": order.qty_filled,
        }
    )


def _record_h74_buy_intent(conn: sqlite3.Connection, order: BrokerOrder, fill: BrokerFill) -> None:
    key = _claim_key()
    authority_hash = "sha256:h74-roundtrip-authority"
    cycle_id = build_h74_cycle_id(
        strategy_instance_id=key.strategy_instance_id,
        entry_client_order_id=order.client_order_id,
        authority_hash=authority_hash,
    )
    record_order_if_missing(
        conn,
        client_order_id=order.client_order_id,
        side="BUY",
        qty_req=float(fill.qty),
        price=float(fill.price),
        symbol=key.pair,
        strategy_name="daily_participation_sma",
        strategy_instance_id=key.strategy_instance_id,
        cycle_id=cycle_id,
        authority_hash=authority_hash,
        daily_participation_policy_hash=key.participation_policy_hash,
        daily_count_snapshot_hash=sha256_prefixed({"h74": "daily-count"}),
        participation_decision_hash=sha256_prefixed({"h74": "participation-decision"}),
        daily_participation_kst_day=key.kst_day,
        daily_participation_fallback_mode="unconditional_participation",
        internal_lot_size=0.0001,
        effective_min_trade_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5_000.0,
        intended_lot_count=max(1, int(float(fill.qty) / 0.0001)),
        executable_lot_count=max(1, int(float(fill.qty) / 0.0001)),
        final_intended_qty=float(fill.qty),
        final_submitted_qty=float(fill.qty),
        decision_reason_code="daily_participation_fallback_allowed",
        local_intent_state="submitted",
        ts_ms=int(fill.fill_ts),
        status="NEW",
    )


def _apply_recorded_buy_fill(conn: sqlite3.Connection, order: BrokerOrder, fill: BrokerFill) -> None:
    _record_h74_buy_intent(conn, order, fill)
    apply_fill_and_trade(
        conn,
        client_order_id=order.client_order_id,
        side="BUY",
        fill_id=fill.fill_id,
        fill_ts=int(fill.fill_ts),
        price=float(fill.price),
        qty=float(fill.qty),
        fee=float(fill.fee),
        strategy_name="daily_participation_sma",
        pair="KRW-BTC",
        signal_ts=int(fill.fill_ts),
        note=f"recorded broker fixture {FIXTURE}",
    )
    set_status(order.client_order_id, "FILLED", conn=conn)
    conn.commit()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="roundtrip_recorded_fill_applied",
        metadata={
            "broker_qty_known": True,
            "broker_asset_qty": float(fill.qty),
            "broker_asset_available": float(fill.qty),
            "broker_asset_locked": 0.0,
            "base_currency": "BTC",
            "quote_currency": "KRW",
            "balance_observed_ts_ms": int(fill.fill_ts),
            "balance_source": "h74_roundtrip_recorded_broker_fill_fixture",
            "balance_source_stale": False,
        },
        now_epoch_sec=2.0,
    )


def test_h74_buy_fill_marks_daily_claim_fulfilled(tmp_path, roundtrip_db) -> None:
    rehearsal = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))
    assert rehearsal["broker_submit_reached"] is True
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        settlement = _runtime_settlement(order, fills, roundtrip_db)
        row = conn.execute("SELECT status FROM daily_participation_claims").fetchone()
        order_row = conn.execute(
            "SELECT cycle_id, authority_hash, strategy_instance_id FROM orders WHERE client_order_id=?",
            (order.client_order_id,),
        ).fetchone()
        inventory = load_h74_cycle_inventory(conn, cycle_id=str(order_row["cycle_id"]))
        discovered = _inject_h74_cycle_inventory(
            conn,
            readiness_payload={
                "authority_hash": str(order_row["authority_hash"]),
                "strategy_instance_id": str(order_row["strategy_instance_id"]),
            },
            planning_context={"runtime_pair": "KRW-BTC"},
        )
    finally:
        conn.close()

    assert row["status"] == "fulfilled"
    assert inventory is not None
    assert inventory.acquired_qty == pytest.approx(float(fills[0].qty))
    assert inventory.remaining_cycle_qty == pytest.approx(float(fills[0].qty))
    assert discovered["h74_cycle_id"] == str(order_row["cycle_id"])
    assert discovered["h74_remaining_cycle_qty"] == pytest.approx(float(fills[0].qty))
    assert settlement.settled is True
    assert settlement.fee_state == "finalized"
    assert settlement.principal_applied is True
    assert settlement.projection_applied is True
    assert settlement.broker_local_converged is True
    assert rehearsal["would_submit_plan"]["side"] == "BUY"
    assert rehearsal["would_submit_plan"]["source"] == "target_delta"


def test_next_cycle_same_kst_day_does_not_submit_second_buy(tmp_path, roundtrip_db) -> None:
    rehearsal = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))
    assert rehearsal["broker_submit_reached"] is True
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        snapshot = build_runtime_daily_count_snapshot_from_sqlite(
            conn=conn,
            config=_participation_config(),
            decision_ts=int(fills[0].fill_ts) + 60_000,
            pair="KRW-BTC",
            strategy_instance_id="h74-source-observation",
            strategy_name="daily_participation_sma",
        )
    finally:
        conn.close()

    assert snapshot.count_for_kst_day == 1
    assert snapshot.pending_claim_count == 0
    decision = evaluate_daily_participation_policy(
        config=_participation_config(),
        state=DailyParticipationStateSnapshot(
            decision_ts=int(fills[0].fill_ts) + 60_000,
            count_for_kst_day=snapshot.count_for_kst_day,
            position_open=False,
            daily_count_snapshot_hash=snapshot.snapshot_hash,
            pending_claim_count=snapshot.pending_claim_count,
        ),
    )
    assert decision.allowed is False
    assert decision.reason_code == "daily_participation_already_counted"
    assert rehearsal["would_submit_plan"]["side"] == "BUY"


def test_fee_missing_blocks_or_marks_recovery_required(roundtrip_db) -> None:
    order, fills = _recorded_broker_roundtrip()
    missing_fee_fill = BrokerFill(
        client_order_id=order.client_order_id,
        exchange_order_id=order.exchange_order_id,
        fill_id=fills[0].fill_id,
        fill_ts=fills[0].fill_ts,
        price=fills[0].price,
        qty=fills[0].qty,
        fee=None,
        fee_status="missing",
        fee_source="missing",
        fee_confidence="unknown",
        fee_provenance=f"{FIXTURE}:paid_fee_removed",
        raw=fills[0].raw,
    )
    conn = _conn(roundtrip_db)
    try:
        _record_h74_buy_intent(conn, order, missing_fee_fill)
        apply_fill_principal_with_pending_fee(
            conn,
            client_order_id=order.client_order_id,
            side="BUY",
            fill_id=missing_fee_fill.fill_id,
            fill_ts=int(missing_fee_fill.fill_ts),
            price=float(missing_fee_fill.price),
            qty=float(missing_fee_fill.qty),
            fee=None,
            fee_status="missing",
            fee_source="missing",
            fee_confidence="unknown",
            fee_provenance=str(missing_fee_fill.fee_provenance),
            strategy_name="daily_participation_sma",
            pair="KRW-BTC",
            signal_ts=int(missing_fee_fill.fill_ts),
        )
        record_broker_fill_observation(
            conn,
            event_ts=int(missing_fee_fill.fill_ts),
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            fill_id=missing_fee_fill.fill_id,
            fill_ts=int(missing_fee_fill.fill_ts),
            side="BUY",
            price=float(missing_fee_fill.price),
            qty=float(missing_fee_fill.qty),
            fee=None,
            fee_status="missing",
            accounting_status="fee_pending",
            source="h74_roundtrip_missing_fee_fixture",
            parse_warnings="missing_fee",
            raw_payload=dict(missing_fee_fill.raw or {}),
        )
        conn.commit()
        readiness = compute_runtime_readiness_snapshot(conn)
        settlement = _runtime_settlement(order, [missing_fee_fill], roundtrip_db)
    finally:
        conn.close()

    assert settlement.settled is False
    assert settlement.fee_state in {"pending", "blocked"}
    assert readiness.new_entry_fee_blocker is True
    assert readiness.new_entry_allowed is False
    assert readiness.active_fill_accounting_blocker is True


def test_projection_mismatch_blocks_resume(roundtrip_db) -> None:
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="roundtrip_forced_projection_mismatch",
            metadata={
                "broker_qty_known": True,
                "broker_asset_qty": 0.0,
                "broker_asset_available": 0.0,
                "broker_asset_locked": 0.0,
                "base_currency": "BTC",
                "quote_currency": "KRW",
                "balance_observed_ts_ms": int(fills[0].fill_ts),
                "balance_source": "h74_roundtrip_projection_mismatch_fixture",
                "balance_source_stale": False,
            },
            now_epoch_sec=3.0,
        )
        readiness = compute_runtime_readiness_snapshot(conn)
        settlement = _runtime_settlement(order, fills, roundtrip_db)
    finally:
        conn.close()

    assert readiness.run_loop_allowed is False
    assert readiness.broker_position_evidence["broker_qty"] == 0.0
    assert readiness.projection_convergence["portfolio_qty"] > 0.0
    assert settlement.settled is False
    assert settlement.broker_local_converged is False


def test_h74_roundtrip_uses_recorded_broker_fill_fixture(tmp_path, roundtrip_db) -> None:
    rehearsal = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        settlement = _runtime_settlement(order, fills, roundtrip_db)
        evidence = _order_fill_evidence(order=order, fills=fills)
    finally:
        conn.close()

    assert FIXTURE.exists()
    assert rehearsal["would_submit_plan"]["source"] == "target_delta"
    assert order.raw is not None
    assert fills[0].fee == 27.86
    assert evidence["fill_set_complete"] is True
    assert evidence["fee_state"] == "finalized"
    assert settlement.settled is True
    assert settlement.evidence["order_level_paid_fee_present"] is True


def test_h74_roundtrip_verifies_sell_closeout_path(tmp_path, roundtrip_db) -> None:
    rehearsal = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        settlement = _runtime_settlement(order, fills, roundtrip_db)
    finally:
        conn.close()
    sell_closeout = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            source_artifact_path=_source_artifact(tmp_path),
            closeout_existing_qty=max(float(fills[0].qty) * 3.0, 0.002),
        )
    )

    assert settlement.settled is True
    assert rehearsal["would_submit_plan"]["side"] == "BUY"
    assert sell_closeout["would_submit_plan"]["side"] == "SELL"
    assert sell_closeout["would_submit_plan"]["source"] == "target_delta"
    assert sell_closeout["would_submit_plan"]["final_action"] == "REBALANCE_TO_TARGET"
