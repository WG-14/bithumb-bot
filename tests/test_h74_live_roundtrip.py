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
from bithumb_bot.execution_service import ExecutionSubmitPlan
from bithumb_bot.execution import apply_fill_and_trade, apply_fill_principal_with_pending_fee, record_order_if_missing
from bithumb_bot.h74_cycle_state import build_h74_cycle_id, load_h74_cycle_inventory
from bithumb_bot.h74_live_rehearsal import H74LiveRehearsalConfig, run_h74_live_rehearsal
from bithumb_bot.h74_position_ownership import h74_position_ownership_contract_from_payload
from bithumb_bot.h74_probe_acceptance import evaluate_h74_execution_path_probe_acceptance
from bithumb_bot.h74_probe_report import build_h74_execution_path_probe_report
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
from bithumb_bot.target_position import TargetPositionSettings, build_target_position_decision
from tests.test_h74_live_submit_ownership import _request as _live_submit_request
from bithumb_bot.broker.live_submit_orchestrator import _build_context, _plan_submit_attempt, _validate_explicit_submit_plan


FIXTURE = Path(__file__).parent / "fixtures" / "bithumb" / "live_paid_fee_single_fill_buy_2026_04_24.json"


def _source_artifact(tmp_path) -> str:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "runtime_base_cost_assumption": {"fee_rate": 0.0004, "slippage_bps": 10},
                "candle_timing": "closed_candle_kst",
                "behavior_contract": {
                    "position_mode": "fixed_fill_qty_until_exit",
                    "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
                    "residual_inventory_mode": "terminal_dust_reported_not_reused_without_authority",
                    "initial_position_policy": "flat_start_required",
                    "partial_fill_policy": "accumulate_cycle_acquired_qty",
                    "fee_application_policy": "repository_observed_fee_fields",
                },
                "entry_submit_semantics": {
                    "schema_version": 1,
                    "entry_order_type": "price",
                    "entry_submit_field": "price",
                    "entry_quote_notional_krw": 100_000,
                    "entry_volume_forbidden": True,
                    "entry_qty_preview_authoritative": False,
                    "entry_fill_qty_authority": "broker_fills",
                },
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


def _record_h74_buy_intent(
    conn: sqlite3.Connection,
    order: BrokerOrder,
    fill: BrokerFill,
    *,
    requested_qty: float | None = None,
) -> None:
    key = _claim_key()
    authority_hash = "sha256:h74-roundtrip-authority"
    submit_qty = float(fill.qty if requested_qty is None else requested_qty)
    cycle_id = build_h74_cycle_id(
        strategy_instance_id=key.strategy_instance_id,
        entry_client_order_id=order.client_order_id,
        authority_hash=authority_hash,
    )
    contract_hash = h74_position_ownership_contract_from_payload(
        {
            "cycle_id": cycle_id,
            "h74_cycle_id": cycle_id,
            "authority_hash": authority_hash,
            "strategy_instance_id": key.strategy_instance_id,
            "probe_run_id": "probe-run-1",
            "pair": key.pair,
            "entry_side": "BUY",
            "entry_plan_id": order.client_order_id,
            "position_mode": "fixed_fill_qty_until_exit",
            "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
        }
    ).contract_hash
    record_order_if_missing(
        conn,
        client_order_id=order.client_order_id,
        side="BUY",
        qty_req=submit_qty,
        price=float(fill.price),
        symbol=key.pair,
        strategy_name="daily_participation_sma",
        strategy_instance_id=key.strategy_instance_id,
        cycle_id=cycle_id,
        authority_hash=authority_hash,
        entry_decision_id=1,
        h74_position_ownership_contract_hash=contract_hash,
        daily_participation_policy_hash=key.participation_policy_hash,
        daily_count_snapshot_hash=sha256_prefixed({"h74": "daily-count"}),
        participation_decision_hash=sha256_prefixed({"h74": "participation-decision"}),
        daily_participation_kst_day=key.kst_day,
        daily_participation_fallback_mode="unconditional_participation",
        probe_run_id="probe-run-1",
        internal_lot_size=0.0001,
        effective_min_trade_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5_000.0,
        intended_lot_count=max(1, int(submit_qty / 0.0001)),
        executable_lot_count=max(1, int(submit_qty / 0.0001)),
        final_intended_qty=submit_qty,
        final_submitted_qty=submit_qty,
        decision_reason_code="daily_participation_fallback_allowed",
        local_intent_state="submitted",
        ts_ms=int(fill.fill_ts),
        status="NEW",
    )


def _record_h74_sell_intent(conn: sqlite3.Connection, *, cycle_id: str, contract_hash: str) -> None:
    record_order_if_missing(
        conn,
        client_order_id="h74-sell",
        side="SELL",
        qty_req=0.0008,
        price=100_000_000.0,
        symbol="KRW-BTC",
        strategy_name="daily_participation_sma",
        strategy_instance_id="h74-source-observation",
        cycle_id=cycle_id,
        authority_hash="sha256:h74-roundtrip-authority",
        h74_position_ownership_contract_hash=contract_hash,
        exit_decision_id=2,
        decision_reason="max_holding_time",
        exit_rule_name="max_holding_time",
        probe_run_id="probe-run-1",
        internal_lot_size=0.0001,
        effective_min_trade_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5_000.0,
        intended_lot_count=8,
        executable_lot_count=8,
        final_intended_qty=0.0008,
        final_submitted_qty=0.0008,
        ts_ms=2,
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
    assert rehearsal["would_submit_plan"]["source"] == "h74_source_observation"


def test_h74_buy_order_persists_cycle_metadata(roundtrip_db) -> None:
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _record_h74_buy_intent(conn, order, fills[0])
        row = conn.execute(
            """
            SELECT cycle_id, strategy_instance_id, authority_hash, probe_run_id,
                   h74_position_ownership_contract_hash
            FROM orders
            WHERE client_order_id=?
            """,
            (order.client_order_id,),
        ).fetchone()
    finally:
        conn.close()

    assert row["cycle_id"]
    assert row["strategy_instance_id"] == "h74-source-observation"
    assert row["authority_hash"] == "sha256:h74-roundtrip-authority"
    assert row["probe_run_id"] == "probe-run-1"
    assert row["h74_position_ownership_contract_hash"].startswith("sha256:")


def test_h74_roundtrip_contract_inventory_boundary_is_explicit(roundtrip_db) -> None:
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        order_row = conn.execute(
            """
            SELECT cycle_id, strategy_instance_id, authority_hash, probe_run_id,
                   h74_position_ownership_contract_hash
            FROM orders
            WHERE client_order_id=?
            """,
            (order.client_order_id,),
        ).fetchone()
        cycle_row = conn.execute(
            """
            SELECT cycle_id, acquired_qty, sold_qty, locked_exit_qty, contract_hash
            FROM h74_cycle_state
            WHERE cycle_id=?
            """,
            (str(order_row["cycle_id"]),),
        ).fetchone()
        inventory = load_h74_cycle_inventory(conn, cycle_id=str(order_row["cycle_id"]))
    finally:
        conn.close()

    contract = h74_position_ownership_contract_from_payload(
        {
            "cycle_id": str(order_row["cycle_id"]),
            "h74_cycle_id": str(order_row["cycle_id"]),
            "authority_hash": str(order_row["authority_hash"]),
            "strategy_instance_id": str(order_row["strategy_instance_id"]),
            "probe_run_id": str(order_row["probe_run_id"]),
            "pair": "KRW-BTC",
            "entry_side": "BUY",
            "entry_plan_id": order.client_order_id,
            "position_mode": "fixed_fill_qty_until_exit",
            "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
        }
    )
    expected_remaining = (
        float(cycle_row["acquired_qty"])
        - float(cycle_row["sold_qty"])
        - float(cycle_row["locked_exit_qty"])
    )

    assert inventory is not None
    assert contract.cycle_id == inventory.cycle_id
    assert contract.contract_hash == order_row["h74_position_ownership_contract_hash"]
    assert inventory.contract_hash == contract.contract_hash
    assert cycle_row["contract_hash"] == contract.contract_hash
    assert inventory.remaining_cycle_qty == pytest.approx(expected_remaining)
    assert inventory.remaining_cycle_qty == pytest.approx(float(fills[0].qty))


def test_h74_ownership_metadata_survives_plan_to_order(roundtrip_db) -> None:
    conn = _conn(roundtrip_db)
    request = _live_submit_request(conn)
    plan = ExecutionSubmitPlan(
        side="BUY",
        source="h74_source_observation",
        authority="typed_execution_submit_plan",
        final_action="BUY",
        qty=None,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=True,
        pre_submit_proof_status="PASS",
        block_reason="none",
        idempotency_key="h74-live-buy",
        pair="KRW-BTC",
        extra_payload={
            "cycle_id": request.cycle_id,
            "h74_cycle_id": request.h74_cycle_id,
            "strategy_instance_id": request.strategy_instance_id,
            "authority_hash": request.authority_hash,
            "h74_execution_path_probe_run_id": request.probe_run_id,
            "h74_position_ownership_contract_hash": request.h74_position_ownership_contract_hash,
        },
    )
    plan_payload = plan.as_dict()
    try:
        assert request.cycle_id == plan_payload["cycle_id"]
        assert request.h74_cycle_id == plan_payload["h74_cycle_id"]
        assert request.strategy_instance_id == plan_payload["strategy_instance_id"]
        assert request.authority_hash == plan_payload["authority_hash"]
        assert request.probe_run_id == plan_payload["h74_execution_path_probe_run_id"]
        assert (
            request.h74_position_ownership_contract_hash
            == plan_payload["h74_position_ownership_contract_hash"]
        )
        record_order_if_missing(
            conn,
            client_order_id=request.client_order_id,
            submit_attempt_id=request.submit_attempt_id,
            side=request.side,
            qty_req=request.qty,
            price=None,
            symbol="KRW-BTC",
            strategy_name=request.strategy_name,
            strategy_instance_id=request.strategy_instance_id,
            cycle_id=request.cycle_id,
            authority_hash=request.authority_hash,
            h74_position_ownership_contract_hash=request.h74_position_ownership_contract_hash,
            probe_run_id=request.probe_run_id,
            status="PENDING_SUBMIT",
        )
        row = conn.execute(
            """
            SELECT cycle_id, strategy_instance_id, authority_hash, probe_run_id,
                   h74_position_ownership_contract_hash
            FROM orders
            WHERE client_order_id=?
            """,
            (request.client_order_id,),
        ).fetchone()
    finally:
        conn.close()

    assert row["cycle_id"] == plan_payload["cycle_id"]
    assert row["strategy_instance_id"] == plan_payload["strategy_instance_id"]
    assert row["authority_hash"] == plan_payload["authority_hash"]
    assert row["probe_run_id"] == plan_payload["h74_execution_path_probe_run_id"]
    assert row["h74_position_ownership_contract_hash"] == plan_payload["h74_position_ownership_contract_hash"]


def test_h74_order_event_submit_evidence_contains_ownership_contract_hash(roundtrip_db) -> None:
    conn = _conn(roundtrip_db)
    request = _live_submit_request(conn)
    try:
        context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
        _plan_submit_attempt(context=context)
        event_row = conn.execute(
            """
            SELECT submit_evidence FROM order_events
            WHERE client_order_id=? AND submit_phase='planning'
            ORDER BY id DESC LIMIT 1
            """,
            (request.client_order_id,),
        ).fetchone()
    finally:
        conn.close()

    evidence = json.loads(event_row["submit_evidence"])
    assert evidence["h74_position_ownership_contract_hash"] == request.h74_position_ownership_contract_hash


def _acceptance_report(**overrides: object) -> dict[str, object]:
    report = {
        "artifact_type": "h74_execution_path_probe_report",
        "probe_run_id": "probe-1",
        "execution_path_probe_status": "PASS",
        "buy_order_filled": True,
        "h74_cycle_ownership_created": True,
        "h74_cycle_id": "cycle-1",
        "h74_remaining_cycle_qty_before_sell": 0.0008,
        "sell_order_submitted": True,
        "sell_order_filled": True,
        "h74_cycle_state_closed": True,
        "portfolio_flat": True,
        "accounting_flat": True,
        "manual_intervention": False,
        "h74_exit_authority_ready": 1,
        "h74_remaining_cycle_qty": 0.0008,
        "h74_cycle_contract_hash": "sha256:contract",
        "h74_exit_authority_not_ready_reason": "none",
        "buy_decision_id": 1,
        "buy_execution_plan_id": 2,
        "buy_order_id": 3,
        "buy_client_order_id": "buy-1",
        "buy_fill_id": 4,
        "open_lot_id": 5,
        "sell_decision_id": 6,
        "sell_execution_plan_id": 7,
        "sell_order_id": 8,
        "sell_client_order_id": "sell-1",
        "sell_fill_id": 9,
        "lifecycle_id": 10,
        "buy_leg": {
            "decision_id": 1,
            "execution_plan_id": 2,
            "order_id": 3,
            "client_order_id": "buy-1",
            "fill_id": 4,
            "open_lot_id": 5,
        },
        "sell_leg": {
            "decision_id": 6,
            "execution_plan_id": 7,
            "order_id": 8,
            "client_order_id": "sell-1",
            "fill_id": 9,
            "lifecycle_id": 10,
        },
        "accounting": {"validated": True},
        "final_flat_or_documented_dust": True,
    }
    report.update(overrides)
    return report


def test_h74_roundtrip_acceptance_requires_buy_cycle_sell_and_flat() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(_acceptance_report())

    assert result["execution_path_probe_status"] == "PASS"
    assert result["buy_order_filled"] is True
    assert result["h74_cycle_ownership_created"] is True
    assert result["sell_order_submitted"] is True
    assert result["sell_order_filled"] is True
    assert result["h74_cycle_state_closed"] is True
    assert result["portfolio_flat"] is True
    assert result["accounting_flat"] is True


def test_h74_buy_only_does_not_pass_roundtrip_acceptance() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(
        _acceptance_report(
            sell_order_submitted=False,
            sell_order_filled=False,
            sell_order_id=None,
            sell_fill_id=None,
            lifecycle_id=None,
            sell_leg={
                "decision_id": None,
                "execution_plan_id": None,
                "order_id": None,
                "client_order_id": None,
                "fill_id": None,
                "lifecycle_id": None,
            },
        )
    )

    assert result["execution_path_probe_status"] in {"PARTIAL_PASS", "INCOMPLETE"}
    assert "sell_order_submitted" in result["missing_evidence"]
    assert "sell_order_filled" in result["missing_evidence"]


def test_h74_manual_sell_does_not_count_as_automated_sell_success() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(
        _acceptance_report(manual_intervention=True, manual_sell=True)
    )

    assert result["execution_path_probe_status"] != "PASS"
    assert result["manual_intervention"] is True
    assert "automated_sell_required" in result["missing_evidence"]


def test_h74_roundtrip_artifact_contains_exit_authority_fields() -> None:
    result = evaluate_h74_execution_path_probe_acceptance(_acceptance_report())

    assert result["h74_exit_authority_ready"] == 1
    assert result["h74_cycle_id"] == "cycle-1"
    assert result["h74_remaining_cycle_qty"] == pytest.approx(0.0008)
    assert result["h74_cycle_contract_hash"] == "sha256:contract"


def test_h74_probe_report_builder_requires_buy_cycle_sell_and_flat(roundtrip_db) -> None:
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        cycle = conn.execute(
            "SELECT cycle_id, contract_hash FROM h74_cycle_state WHERE entry_client_order_id=?",
            (order.client_order_id,),
        ).fetchone()
        _record_h74_sell_intent(
            conn,
            cycle_id=str(cycle["cycle_id"]),
            contract_hash=str(cycle["contract_hash"]),
        )
        apply_fill_and_trade(
            conn,
            client_order_id="h74-sell",
            side="SELL",
            fill_id="sell-fill",
            fill_ts=int(fills[0].fill_ts) + 1,
            price=float(fills[0].price),
            qty=float(fills[0].qty),
            fee=float(fills[0].fee),
            strategy_name="daily_participation_sma",
            pair="KRW-BTC",
            signal_ts=int(fills[0].fill_ts) + 1,
            exit_reason="max_holding_time",
            exit_rule_name="max_holding_time",
        )
        set_status("h74-sell", "FILLED", conn=conn)
        report = build_h74_execution_path_probe_report(conn, "probe-run-1")
    finally:
        conn.close()

    assert report["execution_path_probe_status"] == "PASS"
    assert report["sell_order_filled"] is True
    assert report["h74_cycle_state_closed"] is True
    assert report["portfolio_flat"] is True
    assert report["accounting_flat"] is True
    assert evaluate_h74_execution_path_probe_acceptance(report)["execution_path_probe_status"] == "PASS"


def test_h74_probe_report_builder_buy_only_is_partial_pass(roundtrip_db) -> None:
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        report = build_h74_execution_path_probe_report(conn, "probe-run-1")
        acceptance = evaluate_h74_execution_path_probe_acceptance(report)
    finally:
        conn.close()

    assert report["execution_path_probe_status"] != "PASS"
    assert report["buy_order_filled"] is True
    assert report["sell_order_submitted"] is False
    assert acceptance["execution_path_probe_status"] != "PASS"
    assert "sell_order_submitted" in acceptance["missing_evidence"]


def test_h74_probe_report_builder_manual_sell_is_not_automated_success(roundtrip_db) -> None:
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        cycle = conn.execute(
            "SELECT cycle_id, contract_hash FROM h74_cycle_state WHERE entry_client_order_id=?",
            (order.client_order_id,),
        ).fetchone()
        record_order_if_missing(
            conn,
            client_order_id="h74-manual-sell",
            side="SELL",
            qty_req=float(fills[0].qty),
            price=float(fills[0].price),
            symbol="KRW-BTC",
            strategy_name="daily_participation_sma",
            strategy_instance_id="h74-source-observation",
            cycle_id=str(cycle["cycle_id"]),
            authority_hash="sha256:h74-roundtrip-authority",
            h74_position_ownership_contract_hash=str(cycle["contract_hash"]),
            decision_reason="operator_closeout",
            probe_run_id="probe-run-1",
            status="FILLED",
        )
        report = build_h74_execution_path_probe_report(conn, "probe-run-1")
        acceptance = evaluate_h74_execution_path_probe_acceptance(report)
    finally:
        conn.close()

    assert report["manual_intervention"] is True
    assert report["execution_path_probe_status"] != "PASS"
    assert acceptance["execution_path_probe_status"] != "PASS"
    assert "automated_sell_required" in acceptance["missing_evidence"]


def test_h74_buy_quote_notional_records_acquired_qty_from_broker_fill(roundtrip_db) -> None:
    order, fills = _recorded_broker_roundtrip()
    broker_fill = BrokerFill(
        client_order_id=fills[0].client_order_id,
        exchange_order_id=fills[0].exchange_order_id,
        fill_id=fills[0].fill_id,
        fill_ts=fills[0].fill_ts,
        price=fills[0].price,
        qty=0.000997,
        fee=fills[0].fee,
        fee_status=fills[0].fee_status,
        fee_source=fills[0].fee_source,
        fee_confidence=fills[0].fee_confidence,
        fee_provenance=fills[0].fee_provenance,
        raw=fills[0].raw,
    )
    preview_qty = 0.0009
    conn = _conn(roundtrip_db)
    try:
        _record_h74_buy_intent(conn, order, broker_fill, requested_qty=preview_qty)
        apply_fill_and_trade(
            conn,
            client_order_id=order.client_order_id,
            side="BUY",
            fill_id=broker_fill.fill_id,
            fill_ts=int(broker_fill.fill_ts),
            price=float(broker_fill.price),
            qty=float(broker_fill.qty),
            fee=float(broker_fill.fee or 0.0),
            strategy_name="daily_participation_sma",
            pair="KRW-BTC",
            signal_ts=int(broker_fill.fill_ts),
            note="quote-notional broker fill authority regression",
        )
        order_row = conn.execute(
            "SELECT cycle_id, qty_req FROM orders WHERE client_order_id=?",
            (order.client_order_id,),
        ).fetchone()
        inventory = load_h74_cycle_inventory(conn, cycle_id=str(order_row["cycle_id"]))
    finally:
        conn.close()

    assert order_row["qty_req"] == pytest.approx(preview_qty)
    assert inventory is not None
    assert inventory.acquired_qty == pytest.approx(0.000997)
    assert inventory.remaining_cycle_qty == pytest.approx(0.000997)


def test_h74_buy_preview_qty_is_not_cycle_authority(tmp_path) -> None:
    rehearsal = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))
    plan = rehearsal["would_submit_plan"]

    assert plan["exchange_submit_notional_krw"] == pytest.approx(100_000.0)
    assert plan["exchange_submit_qty"] is None
    assert plan["submit_qty_authority"] == "non_authoritative_preview"
    assert plan["entry_qty_preview_authoritative"] is False
    assert plan["entry_fill_qty_authority"] == "broker_fills"


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
    assert rehearsal["would_submit_plan"]["source"] == "h74_source_observation"
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


def test_h74_sell_uses_remaining_cycle_qty_after_quote_buy_fill(tmp_path, roundtrip_db) -> None:
    order, fills = _recorded_broker_roundtrip()
    conn = _conn(roundtrip_db)
    try:
        _apply_recorded_buy_fill(conn, order, fills[0])
        order_row = conn.execute(
            "SELECT cycle_id FROM orders WHERE client_order_id=?",
            (order.client_order_id,),
        ).fetchone()
        inventory = load_h74_cycle_inventory(conn, cycle_id=str(order_row["cycle_id"]))
        remaining_qty = float(inventory.remaining_cycle_qty)
    finally:
        conn.close()

    sell_decision = build_target_position_decision(
        raw_signal="SELL",
        previous_target_exposure_krw=100_000.0,
        current_position_snapshot={},
        readiness_payload={
            "h74_cycle_id": str(order_row["cycle_id"]),
            "remaining_cycle_qty": remaining_qty,
        },
        order_rules={"min_qty": 0.0001, "qty_step": 0.0001, "min_notional_krw": 5000.0},
        reference_price=100_000_000.0,
        settings=TargetPositionSettings(
            execution_engine="target_delta",
            target_exposure_krw=100_000.0,
            max_order_krw=100_000.0,
            position_mode="fixed_fill_qty_until_exit",
        ),
    )

    assert sell_decision.delta_side == "SELL"
    assert sell_decision.submit_qty == pytest.approx(remaining_qty)
    assert sell_decision.h74_remaining_cycle_qty == pytest.approx(remaining_qty)
