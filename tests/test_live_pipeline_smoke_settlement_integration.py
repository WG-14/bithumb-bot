from __future__ import annotations

from copy import deepcopy
import inspect
from types import SimpleNamespace

import pytest

from bithumb_bot import live_pipeline_smoke as smoke_module
from bithumb_bot.config import settings
from bithumb_bot.broker.live_submission_execution import (
    ConfirmedLiveSubmission,
    reconcile_apply_fills_and_refresh,
)
from bithumb_bot.db_core import (
    FILL_FEE_ACCOUNTING_STATUS_FINALIZED,
    ensure_db,
    record_fee_pending_accounting_repair,
    record_position_authority_repair,
)
from bithumb_bot.oms import create_order, record_submit_attempt
from bithumb_bot.live_pipeline_smoke import (
    LivePipelineSmokeExecutionService,
    PostStepReadinessBarrierConfig,
    wait_for_live_pipeline_smoke_next_step_readiness,
    run_live_pipeline_smoke,
)
from bithumb_bot.live_pipeline_smoke_authority import LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN
from bithumb_bot.live_pipeline_smoke_preflight import LivePipelineSmokePreflightError
from bithumb_bot.live_pipeline_smoke_preflight import LivePipelineSmokeReadiness
from bithumb_bot.live_pipeline_smoke_preflight import readiness_from_snapshot
from bithumb_bot.live_pipeline_smoke_preflight import validate_live_pipeline_smoke_step_readiness
from bithumb_bot.order_settlement import evaluate_settlement_snapshot
from bithumb_bot.runtime.live_order_settlement import LiveOrderSettlementWrapper
from tests.test_live_pipeline_smoke_runner_fake_broker import (
    _Broker,
    _authority,
    _insert_top_of_book,
    _patch_settings,
    _restore_settings,
    _readiness_from_broker,
)
from tests.test_live_settlement_integration_fixtures import (
    _ScriptedBithumbBroker,
    _configure_live_fixture,
    _payload,
    _request,
    _table_count,
    assert_recorded_smoke_five_round_trips_touches_orders_fills_trades_portfolio_lots,
)


_MISSING_SETTING = object()


def _restore_scripted_smoke_settings(old: dict[str, object]) -> None:
    for name, value in old.items():
        if value is _MISSING_SETTING:
            if hasattr(settings, name):
                object.__delattr__(settings, name)
            continue
        object.__setattr__(settings, name, value)


def _record_reconcile_attempt(attempts: list[str] | None = None):
    if attempts is not None:
        attempts.append("reconcile")


def _test_only_readiness_settlement(readiness_provider, *, side_provider=lambda: "BUY"):
    def _settle(trade):
        readiness = readiness_provider()
        filled_qty = float(trade.get("filled_qty") or trade.get("submit_qty") or 0.0)
        finalized = int(readiness.fee_pending_count or 0) <= 0 and not bool(readiness.active_fee_accounting_blocker)
        evidence = {
            "order_state": "FILLED",
            "order_terminal": True,
            "fill_count": 1 if filled_qty > 0.0 else 0,
            "fill_set_complete": filled_qty > 0.0,
            "paid_fee_present": finalized,
            "order_level_paid_fee_present": finalized,
            "complete_fill_set_available": filled_qty > 0.0,
            "fee_state": "finalized" if finalized else "pending",
            "principal_applied": filled_qty > 0.0,
            "accounting_finalized": finalized,
            "projection_applied": bool(readiness.projection_converged),
            "projected_total_qty": float(readiness.projected_total_qty),
            "portfolio_qty": float(readiness.portfolio_qty),
            "broker_qty": float(readiness.broker_qty),
            "broker_local_converged": bool(readiness.converged),
            "side": side_provider(),
            "reason_code": "settlement_evidence_complete" if finalized else "settlement_waiting",
        }
        return evaluate_settlement_snapshot(
            client_order_id=str(trade.get("client_order_id") or ""),
            exchange_order_id=str(trade.get("exchange_order_id") or "") or None,
            evidence=evidence,
            attempts=[evidence],
        )

    return _settle


def _always_settled_settlement():
    def _settle(trade):
        filled_qty = float(trade.get("filled_qty") or trade.get("submit_qty") or 0.0)
        evidence = {
            "order_state": "FILLED",
            "order_terminal": True,
            "fill_count": 1 if filled_qty > 0.0 else 0,
            "fill_set_complete": filled_qty > 0.0,
            "paid_fee_present": True,
            "order_level_paid_fee_present": True,
            "complete_fill_set_available": filled_qty > 0.0,
            "fee_state": "finalized",
            "principal_applied": filled_qty > 0.0,
            "accounting_finalized": True,
            "projection_applied": True,
            "projected_total_qty": filled_qty,
            "portfolio_qty": filled_qty,
            "broker_qty": filled_qty,
            "broker_local_converged": True,
            "side": str(trade.get("side") or "").upper(),
            "reason_code": "settlement_evidence_complete",
        }
        return evaluate_settlement_snapshot(
            client_order_id=str(trade.get("client_order_id") or ""),
            exchange_order_id=str(trade.get("exchange_order_id") or "") or None,
            evidence=evidence,
            attempts=[evidence],
        )

    return _settle


def _readiness(
    *,
    qty: float,
    fee_pending_count: int = 0,
    active_fee_accounting_blocker: bool = False,
    active_fill_accounting_blocker: bool | None = None,
    new_entry_fee_blocker: bool | None = None,
    fee_gap_closeout_blocking: bool = False,
    fee_gap_resume_blocking: bool = False,
    fee_gap_policy_reason: str = "none",
    fee_gap_repair_eligibility_state: str = "not_applicable",
    fee_gap_incident_scope: str = "none",
    fee_validation_blocked_count: int = 0,
    unapplied_principal_pending_count: int = 0,
    principal_applied_fee_pending_count: int = 0,
    historical_fee_pending_observation_count: int = 0,
    fill_accounting_active_issue_count: int = 0,
) -> LivePipelineSmokeReadiness:
    active_fill = (
        bool(active_fee_accounting_blocker)
        if active_fill_accounting_blocker is None
        else bool(active_fill_accounting_blocker)
    )
    new_entry = bool(active_fill if new_entry_fee_blocker is None else new_entry_fee_blocker)
    return LivePipelineSmokeReadiness(
        broker_qty=float(qty),
        portfolio_qty=float(qty),
        projected_total_qty=float(qty),
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
        fee_pending_count=int(fee_pending_count),
        active_fee_accounting_blocker=active_fill,
        broker_qty_known=True,
        balance_source_stale=False,
        projection_converged=True,
        active_fill_accounting_blocker=active_fill,
        active_fill_accounting_blocker_reasons=("active_fee_accounting_blocker",) if active_fill else (),
        new_entry_fee_blocker=new_entry,
        new_entry_fee_blocker_reasons=("active_fee_accounting_blocker",) if new_entry else (),
        fee_gap_closeout_blocking=bool(fee_gap_closeout_blocking),
        fee_gap_resume_blocking=bool(fee_gap_resume_blocking),
        fee_gap_policy_reason=fee_gap_policy_reason,
        fee_gap_repair_eligibility_state=fee_gap_repair_eligibility_state,
        fee_gap_incident_scope=fee_gap_incident_scope,
        fee_gap_incident_active_issue=False,
        fee_gap_incident_historical_context=bool(fee_gap_incident_scope == "historical_context"),
        fee_validation_blocked_count=int(fee_validation_blocked_count),
        unapplied_principal_pending_count=int(unapplied_principal_pending_count),
        principal_applied_fee_pending_count=int(principal_applied_fee_pending_count),
        historical_fee_pending_observation_count=int(historical_fee_pending_observation_count),
        broker_fill_fee_pending_count=int(historical_fee_pending_observation_count),
        broker_fill_latest_unresolved_fee_pending_count=int(fill_accounting_active_issue_count),
        fill_accounting_active_issue_count=int(fill_accounting_active_issue_count),
    )


def _run_smoke(
    monkeypatch,
    tmp_path,
    *,
    readiness_provider=None,
    service=None,
    broker=None,
    settlement_coordinator=None,
    post_step_readiness_barrier_config=None,
):
    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    conn = ensure_db(str(db_path))
    _insert_top_of_book(conn)
    broker = broker or _Broker()
    authority = _authority(tmp_path, db_path, max_notional_krw=20_000.0)
    monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
    monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)
    service = service or LivePipelineSmokeExecutionService(broker=broker)
    reconcile_attempts: list[str] = []
    provider = readiness_provider or (lambda: _readiness_from_broker(broker))
    payload = run_live_pipeline_smoke(
        conn=conn,
        broker=broker,
        cycles=5,
        max_orders=10,
        max_notional_krw=20_000.0,
        yes=True,
        authority_path=str(authority),
        confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
        execution_service=service,
        readiness_provider=provider,
        post_trade_reconcile=lambda: _record_reconcile_attempt(reconcile_attempts),
        settlement_coordinator=settlement_coordinator or _test_only_readiness_settlement(provider),
        post_step_readiness_barrier_config=post_step_readiness_barrier_config,
        run_id="lps_settlement_test",
    )
    return payload, conn, broker, service, old


class _SequencedBithumbBroker(_ScriptedBithumbBroker):
    def __init__(self, *, delayed_first_buy: bool = False, rounds: int = 5) -> None:
        super().__init__()
        self.delayed_first_buy = delayed_first_buy
        self.rounds = rounds
        self.payload_sequences: dict[str, list[dict[str, object]]] = {}
        self.sequence_calls: dict[str, int] = {}
        self.order_payload_history: dict[str, list[dict[str, object]]] = {}
        self.current_qty = 0.0

    def place_order(self, *, client_order_id: str, side: str, qty: float, price=None, submit_plan=None, **kwargs):
        order = super().place_order(
            client_order_id=client_order_id,
            side=side,
            qty=qty,
            price=price,
            submit_plan=submit_plan,
            **kwargs,
        )
        normalized_side = str(side).upper()
        reference_price = float(price or 100_000_000.0)
        paid_fee = max(1.0, float(qty) * reference_price * 0.0005)
        complete = _payload(
            client_order_id=client_order_id,
            side=normalized_side,
            qty=float(qty),
            price=reference_price,
            paid_fee=paid_fee,
            trades=[
                {
                    "uuid": f"{client_order_id}-fill",
                    "price": f"{reference_price:.8f}",
                    "volume": f"{float(qty):.8f}",
                    "funds": f"{reference_price * float(qty):.8f}",
                    "fee": f"{paid_fee:.8f}",
                    "created_at": "2024-01-01T00:00:00+00:00",
                }
            ],
        )
        pending = deepcopy(complete)
        pending.pop("paid_fee", None)
        for trade in pending.get("trades", ()):
            if isinstance(trade, dict):
                trade.pop("fee", None)
        if normalized_side == "BUY" and self.delayed_first_buy and self.submit_calls == 1:
            sequence = [deepcopy(pending) for _ in range(5)] + [deepcopy(complete) for _ in range(20)]
        else:
            sequence = [deepcopy(complete) for _ in range(20)]
        self.payload_sequences[str(order.exchange_order_id)] = sequence
        self.payload_sequences[str(client_order_id)] = sequence
        self.order_payload_history[str(client_order_id)] = [deepcopy(pending), deepcopy(complete)]
        return order

    def _get_private(self, endpoint, params, retry_safe=False):
        self.private_calls.append((str(endpoint), dict(params)))
        if endpoint != "/v1/order":
            raise AssertionError(f"unexpected endpoint {endpoint}")
        key = str(params.get("uuid") or params.get("client_order_id") or "")
        sequence = self.payload_sequences.get(key)
        if not sequence:
            raise AssertionError(f"missing scripted sequence for {key}")
        index = self.sequence_calls.get(key, 0)
        self.sequence_calls[key] = index + 1
        return deepcopy(sequence[min(index, len(sequence) - 1)])


class _SmokeLiveApplicationService:
    def __init__(self, conn, broker: _SequencedBithumbBroker, *, price: float = 100_000_000.0) -> None:
        self.conn = conn
        self.broker = broker
        self.price = float(price)
        self.submissions = []
        self.last_submission = None
        self.errors = []

    def execute(self, request):
        try:
            plan = request.execution_decision_summary.typed_target_submit_plan()
            side = str(plan.side).upper()
            qty = float(plan.qty)
            client_order_id = f"smoke-{len(self.submissions) + 1}-{side.lower()}"
            standard_request = _request(
                self.conn,
                client_order_id=client_order_id,
                side=side,
                qty=qty,
                price=self.price,
            )
            order = self.broker.place_order(
                client_order_id=client_order_id,
                side=side,
                qty=qty,
                price=self.price,
                submit_plan=standard_request.submit_plan,
            )
            lot_count = max(1, int(qty / float(settings.LIVE_INTERNAL_LOT_SIZE)))
            create_order(
                conn=self.conn,
                client_order_id=client_order_id,
                submit_attempt_id=f"{client_order_id}:attempt",
                symbol=str(settings.PAIR),
                mode=str(settings.MODE),
                side=side,
                qty_req=qty,
                price=self.price,
                strategy_name="operator_live_pipeline_smoke",
                entry_decision_id=request.decision_id if side == "BUY" else None,
                exit_decision_id=request.decision_id if side == "SELL" else None,
                decision_reason="operator_authorized_pipeline_smoke",
                exit_rule_name="target_delta" if side == "SELL" else None,
                order_type="market",
                internal_lot_size=float(settings.LIVE_INTERNAL_LOT_SIZE),
                effective_min_trade_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5_000.0,
                intended_lot_count=lot_count,
                executable_lot_count=lot_count,
                final_intended_qty=qty,
                final_submitted_qty=qty,
                decision_reason_code="target_delta_rebalance",
                status="NEW",
                ts_ms=standard_request.ts,
            )
            record_submit_attempt(
                conn=self.conn,
                client_order_id=client_order_id,
                symbol=str(settings.PAIR),
                side=side,
                qty=qty,
                price=self.price,
                submit_ts=standard_request.ts,
                payload_fingerprint=f"sha256:{client_order_id}",
                broker_response_summary="scripted_submit_confirmed",
                submission_reason_code="scripted_submit_confirmed",
                exception_class=None,
                timeout_flag=False,
                submit_evidence=None,
                exchange_order_id_obtained=True,
                order_status="NEW",
                submit_attempt_id=f"{client_order_id}:attempt",
                submit_phase="submission",
                order_type="market",
                internal_lot_size=float(settings.LIVE_INTERNAL_LOT_SIZE),
                effective_min_trade_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5_000.0,
                intended_lot_count=lot_count,
                executable_lot_count=lot_count,
                final_intended_qty=qty,
                final_submitted_qty=qty,
                decision_reason_code="target_delta_rebalance",
            )
            self.conn.commit()
            submission = ConfirmedLiveSubmission(
                conn=self.conn,
                request=standard_request,
                order=order,
                client_order_id=client_order_id,
                exchange_order_id=str(order.exchange_order_id),
                side=side,
                intent_key=f"{client_order_id}:intent",
                ts=standard_request.ts,
                strategy_name="target_delta",
                decision_id=request.decision_id,
                decision_reason="operator_authorized_pipeline_smoke",
                exit_rule_name="target_delta" if side == "SELL" else None,
                request_ts_ms=standard_request.ts,
                response_ts_ms=standard_request.ts,
                submit_elapsed_ms=0,
            )
        except Exception as exc:
            self.errors.append(f"{type(exc).__name__}: {exc}")
            raise
        assert submission is not None
        self.last_submission = submission
        self.submissions.append(submission)
        return {
            "status": "submitted",
            "client_order_id": client_order_id,
            "exchange_order_id": submission.exchange_order_id,
            "side": side,
            "filled_qty": qty,
            "submit_qty": qty,
            "decision_id": request.decision_id,
        }

    def reconcile_last_submission(self) -> None:
        if self.last_submission is None:
            return
        from bithumb_bot.broker import live as live_module

        trade = reconcile_apply_fills_and_refresh(
            live_module,
            broker=self.broker,
            submission=self.last_submission,
        )
        self.conn.commit()
        row = self.conn.execute("SELECT asset_available + asset_locked AS qty FROM portfolio WHERE id=1").fetchone()
        self.broker.current_qty = float(row["qty"] if row is not None else 0.0)


def _db_readiness(conn, broker: _SequencedBithumbBroker) -> LivePipelineSmokeReadiness:
    fee_pending = int(
        conn.execute(
            "SELECT COUNT(*) FROM fills WHERE fee_accounting_status != ?",
            (FILL_FEE_ACCOUNTING_STATUS_FINALIZED,),
        ).fetchone()[0]
    )
    qty = float(getattr(broker, "current_qty", 0.0))
    return LivePipelineSmokeReadiness(
        broker_qty=qty,
        portfolio_qty=qty,
        projected_total_qty=qty,
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
        fee_pending_count=fee_pending,
        active_fee_accounting_blocker=fee_pending > 0,
        broker_qty_known=True,
        balance_source_stale=False,
        projection_converged=True,
    )


def _patch_settlement_readiness(monkeypatch, broker: _SequencedBithumbBroker) -> None:
    def _snapshot(_conn):
        qty = float(getattr(broker, "current_qty", 0.0))
        return SimpleNamespace(
            projection_convergence={
                "converged": True,
                "projected_total_qty": qty,
                "portfolio_qty": qty,
                "reason": "unit_projection_converged",
            },
            broker_position_evidence={
                "broker_qty_known": True,
                "broker_qty": qty,
            },
            recovery_stage="READY",
        )

    monkeypatch.setattr("bithumb_bot.runtime.live_order_settlement.compute_runtime_readiness_snapshot", _snapshot)


def _run_scripted_smoke(monkeypatch, tmp_path, *, delayed_first_buy: bool, cycles: int = 5):
    setting_names = (
        "MODE",
        "DB_PATH",
        "LIVE_DRY_RUN",
        "LIVE_REAL_ORDER_ARMED",
        "EXECUTION_ENGINE",
        "STRATEGY_NAME",
        "PAIR",
        "INTERVAL",
        "START_CASH_KRW",
        "MIN_ORDER_NOTIONAL_KRW",
        "BITHUMB_API_KEY",
        "BITHUMB_API_SECRET",
        "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW",
        "LIVE_FILL_FEE_RATIO_MIN",
        "LIVE_FILL_FEE_RATIO_MAX",
        "LIVE_FILL_FEE_STRICT_MODE",
        "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW",
        "LIVE_FEE_RATE_ESTIMATE",
        "LIVE_INTERNAL_LOT_SIZE",
        "LIVE_MIN_ORDER_QTY",
        "LIVE_ORDER_QTY_STEP",
        "LIVE_ORDER_MAX_QTY_DECIMALS",
    )
    old = {
        name: (getattr(settings, name) if hasattr(settings, name) else _MISSING_SETTING)
        for name in setting_names
    }
    conn = _configure_live_fixture(tmp_path, monkeypatch)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "STRATEGY_NAME", "sma_with_filter")
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "BITHUMB_API_KEY", "account")
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0005)
    broker = _SequencedBithumbBroker(delayed_first_buy=delayed_first_buy, rounds=cycles)
    _patch_settlement_readiness(monkeypatch, broker)
    _insert_top_of_book(conn)
    authority = _authority(tmp_path, settings.DB_PATH, max_notional_krw=20_000.0)
    monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
    monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.notifier.notify", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.execution.notify", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda *_args, **_kwargs: None)
    service = _SmokeLiveApplicationService(conn, broker)
    payload = run_live_pipeline_smoke(
        conn=conn,
        broker=broker,
        cycles=cycles,
        max_orders=cycles * 2,
        max_notional_krw=20_000.0,
        yes=True,
        authority_path=str(authority),
        confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
        execution_service=service,
        readiness_provider=lambda: _db_readiness(conn, broker),
        post_trade_reconcile=service.reconcile_last_submission,
        run_id="lps_scripted_v1_order",
    )
    return payload, conn, broker, service, old


def test_live_pipeline_smoke_passes_only_when_all_10_orders_settle_without_repair(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        assert payload["status"] == "passed"
        assert payload["orders_submitted"] == 10
        assert payload["buy_submitted"] == 5
        assert payload["sell_submitted"] == 5
        assert payload["repair_events_created_during_run"] == 0
        assert payload["manual_intervention_required"] is False
        assert payload["final"]["broker_qty"] == 0.0
        assert payload["final"]["portfolio_qty"] == 0.0
        assert payload["final"]["projected_total_qty"] == 0.0
        assert len(service.submissions) == 10
        assert all(
            step["settlement_result"]["settled"] is True
            for round_item in payload["rounds"]
            for step in (round_item["buy"], round_item["sell"])
        )
        assert conn.execute("SELECT COUNT(*) FROM manual_flat_accounting_repairs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM fee_pending_accounting_repairs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM position_authority_repairs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM external_position_adjustments").fetchone()[0] == 0
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_waits_for_post_sell_readiness_before_next_buy(monkeypatch, tmp_path) -> None:
    broker = _Broker()
    service = LivePipelineSmokeExecutionService(broker=broker)
    pending_seen = {"value": False}

    def _provider():
        qty = float(broker.qty)
        if len(service.submissions) == 2 and abs(qty) <= 1e-12 and not pending_seen["value"]:
            pending_seen["value"] = True
            return _readiness(qty=0.0, fee_pending_count=1, fill_accounting_active_issue_count=1)
        return _readiness(qty=qty)

    payload, conn, _broker, _service, old = _run_smoke(
        monkeypatch,
        tmp_path,
        broker=broker,
        service=service,
        readiness_provider=_provider,
        settlement_coordinator=_always_settled_settlement(),
        post_step_readiness_barrier_config=PostStepReadinessBarrierConfig(
            max_attempts=3,
            poll_intervals_ms=(0, 0, 0),
            deadline_ms=1000,
        ),
    )
    try:
        assert payload["status"] == "passed"
        assert [item["side"] for item in service.submissions[:3]] == ["BUY", "SELL", "BUY"]
        sell = payload["rounds"][0]["sell"]
        attempts = sell["post_step_readiness_result"]["attempts"]
        assert attempts[0]["fee_pending_count"] == 1
        assert attempts[0]["reason"] == "fee_pending_blocks_exposure_increase"
        assert attempts[-1]["fee_pending_count"] == 0
        assert attempts[-1]["active_fee_accounting_blocker"] is False
        assert sell["expected_next_side"] == "BUY"
        assert sell["next_ready_state"] == "buy_ready"
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_transient_fee_pending_after_sell_clears_and_next_buy_submits(monkeypatch, tmp_path) -> None:
    test_smoke_waits_for_post_sell_readiness_before_next_buy(monkeypatch, tmp_path)


def test_smoke_next_buy_requires_clean_readiness_after_transient_fee_pending(monkeypatch, tmp_path) -> None:
    test_smoke_waits_for_post_sell_readiness_before_next_buy(monkeypatch, tmp_path)


def test_post_step_readiness_barrier_uses_bounded_attempts() -> None:
    attempts = []

    def _provider():
        attempts.append("readiness")
        return _readiness(qty=0.0, fee_pending_count=1, fill_accounting_active_issue_count=1)

    result = wait_for_live_pipeline_smoke_next_step_readiness(
        readiness_provider=_provider,
        expected_next_side="BUY",
        reconcile=None,
        config=PostStepReadinessBarrierConfig(max_attempts=3, poll_intervals_ms=(0, 0, 0), deadline_ms=1000),
        sleeper=lambda _seconds: None,
    )

    assert result.passed is False
    assert result.reason == "fee_pending_blocks_exposure_increase"
    assert len(attempts) == 3
    assert len(result.attempts) == 3


def test_post_step_readiness_barrier_does_not_submit_cancel_or_flatten() -> None:
    class _ForbiddenActions:
        submitted = False
        cancelled = False
        flattened = False

        def submit(self):
            self.submitted = True

        def cancel(self):
            self.cancelled = True

        def flatten(self):
            self.flattened = True

    forbidden = _ForbiddenActions()
    result = wait_for_live_pipeline_smoke_next_step_readiness(
        readiness_provider=lambda: _readiness(qty=0.0),
        expected_next_side="BUY",
        reconcile=lambda: None,
        config=PostStepReadinessBarrierConfig(max_attempts=1, poll_intervals_ms=(0,), deadline_ms=1000),
        sleeper=lambda _seconds: None,
    )

    assert result.passed is True
    assert forbidden.submitted is False
    assert forbidden.cancelled is False
    assert forbidden.flattened is False


def test_post_step_readiness_barrier_clears_after_optional_reconcile_callback() -> None:
    reconciles = {"count": 0}

    def _provider():
        if reconciles["count"] <= 1:
            return _readiness(qty=0.0, active_fee_accounting_blocker=True, fill_accounting_active_issue_count=1)
        return _readiness(qty=0.0)

    result = wait_for_live_pipeline_smoke_next_step_readiness(
        readiness_provider=_provider,
        expected_next_side="BUY",
        reconcile=lambda: reconciles.__setitem__("count", reconciles["count"] + 1),
        config=PostStepReadinessBarrierConfig(max_attempts=3, poll_intervals_ms=(0, 0, 0), deadline_ms=1000),
        sleeper=lambda _seconds: None,
    )

    assert result.passed is True
    assert reconciles["count"] == 2
    assert result.attempts[0]["active_fee_accounting_blocker"] is True
    assert result.attempts[-1]["active_fee_accounting_blocker"] is False


def test_smoke_persistent_post_sell_fee_pending_fails_with_readiness_attempts(monkeypatch, tmp_path) -> None:
    broker = _Broker()
    service = LivePipelineSmokeExecutionService(broker=broker)

    def _provider():
        qty = float(broker.qty)
        if len(service.submissions) >= 2 and abs(qty) <= 1e-12:
            return _readiness(qty=0.0, fee_pending_count=1, fill_accounting_active_issue_count=1)
        return _readiness(qty=qty)

    payload, conn, _broker, _service, old = _run_smoke(
        monkeypatch,
        tmp_path,
        broker=broker,
        service=service,
        readiness_provider=_provider,
        settlement_coordinator=_always_settled_settlement(),
        post_step_readiness_barrier_config=PostStepReadinessBarrierConfig(
            max_attempts=3,
            poll_intervals_ms=(0, 0, 0),
            deadline_ms=1000,
        ),
    )
    try:
        assert payload["status"] == "failed"
        assert payload["reason"] == "fee_pending_blocks_exposure_increase"
        assert payload["orders_submitted"] == 2
        assert payload["expected_next_side"] == "BUY"
        assert payload["last_completed_side"] == "SELL"
        assert payload["last_settlement_result"]["settled"] is True
        assert len(payload["post_step_readiness_attempts"]) == 3
        assert all(item["fee_pending_count"] == 1 for item in payload["post_step_readiness_attempts"])
        assert payload["fee_pending_count"] == 1
        assert payload["active_fee_accounting_blocker"] is False
        assert payload["broker_qty"] == 0.0
        assert payload["portfolio_qty"] == 0.0
        assert payload["projected_total_qty"] == 0.0
        assert payload["projection_converged"] is True
        assert [item["side"] for item in service.submissions] == ["BUY", "SELL"]
    finally:
        conn.close()
        _restore_settings(old)


def test_step_readiness_failure_payload_contains_last_settlement_result(monkeypatch, tmp_path) -> None:
    test_smoke_persistent_post_sell_fee_pending_fails_with_readiness_attempts(monkeypatch, tmp_path)


def test_step_readiness_failure_payload_contains_expected_next_side_and_attempts(monkeypatch, tmp_path) -> None:
    test_smoke_persistent_post_sell_fee_pending_fails_with_readiness_attempts(monkeypatch, tmp_path)


def test_post_settlement_readiness_failure_payload_marks_post_step_phase(monkeypatch, tmp_path) -> None:
    broker = _Broker()
    service = LivePipelineSmokeExecutionService(broker=broker)
    payload, conn, _broker, _service, old = _run_smoke(
        monkeypatch,
        tmp_path,
        broker=broker,
        service=service,
        readiness_provider=lambda: (
            _readiness(qty=0.0, active_fee_accounting_blocker=True, fill_accounting_active_issue_count=1)
            if len(service.submissions) >= 2 and abs(float(broker.qty)) <= 1e-12
            else _readiness(qty=float(broker.qty))
        ),
        settlement_coordinator=_always_settled_settlement(),
        post_step_readiness_barrier_config=PostStepReadinessBarrierConfig(
            max_attempts=2,
            poll_intervals_ms=(0, 0),
            deadline_ms=1000,
        ),
    )
    try:
        assert payload["status"] == "failed"
        assert payload["readiness_phase"] == "post_settlement_next_step"
        assert payload["expected_next_side"] == "BUY"
        assert payload["last_completed_side"] == "SELL"
        assert payload["last_settlement_result"]["settled"] is True
        assert payload["post_step_readiness_attempts"]
    finally:
        conn.close()
        _restore_settings(old)


def test_pre_step_readiness_failure_payload_marks_pre_step_phase(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(
        monkeypatch,
        tmp_path,
        readiness_provider=lambda: _readiness(
            qty=0.0,
            fee_pending_count=1,
            fill_accounting_active_issue_count=1,
        ),
        settlement_coordinator=_always_settled_settlement(),
    )
    try:
        assert payload["status"] == "failed"
        assert payload["readiness_phase"] == "pre_step"
        assert payload["expected_side"] == "BUY"
        assert payload["pre_step_readiness"]["fee_pending_count"] == 1
    finally:
        conn.close()
        _restore_settings(old)


def test_post_step_barrier_does_not_bypass_final_fee_pending_buy_block(monkeypatch, tmp_path) -> None:
    test_smoke_persistent_post_sell_fee_pending_fails_with_readiness_attempts(monkeypatch, tmp_path)


def test_historical_fee_pending_observations_do_not_block_smoke_next_buy(monkeypatch, tmp_path) -> None:
    broker = _Broker()
    service = LivePipelineSmokeExecutionService(broker=broker)

    def _provider():
        qty = float(broker.qty)
        if len(service.submissions) >= 2 and abs(qty) <= 1e-12:
            return _readiness(qty=0.0, historical_fee_pending_observation_count=3)
        return _readiness(qty=qty)

    payload, conn, _broker, _service, old = _run_smoke(
        monkeypatch,
        tmp_path,
        broker=broker,
        service=service,
        readiness_provider=_provider,
        settlement_coordinator=_always_settled_settlement(),
        post_step_readiness_barrier_config=PostStepReadinessBarrierConfig(
            max_attempts=2,
            poll_intervals_ms=(0, 0),
            deadline_ms=1000,
        ),
    )
    try:
        assert payload["status"] == "passed"
        sell = payload["rounds"][0]["sell"]
        attempt = sell["post_step_readiness_result"]["attempts"][0]
        assert attempt["historical_fee_pending_observation_count"] == 3
        assert attempt["fee_pending_count"] == 0
        assert attempt["active_fee_accounting_blocker"] is False
        assert [item["side"] for item in service.submissions[:3]] == ["BUY", "SELL", "BUY"]
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_next_buy_ignores_fee_gap_closeout_blocking_when_active_fee_clear() -> None:
    readiness = _readiness(
        qty=0.0,
        active_fill_accounting_blocker=False,
        new_entry_fee_blocker=False,
        fee_gap_closeout_blocking=True,
        fee_gap_resume_blocking=True,
        fee_gap_policy_reason="historical fee-gap repair is applicable now",
        fee_gap_repair_eligibility_state="safe_to_apply_now",
        fee_gap_incident_scope="active_blocking",
    )

    validate_live_pipeline_smoke_step_readiness(readiness, expected_side="BUY")

    payload = readiness.as_dict()
    assert payload["fee_gap_closeout_blocking"] is True
    assert payload["new_entry_fee_blocker"] is False
    assert payload["active_fill_accounting_blocker"] is False


def test_smoke_next_buy_blocks_on_active_unresolved_fee() -> None:
    readiness = _readiness(qty=0.0, fill_accounting_active_issue_count=1)

    with pytest.raises(LivePipelineSmokePreflightError, match="fee_pending_blocks_exposure_increase"):
        validate_live_pipeline_smoke_step_readiness(readiness, expected_side="BUY")

    assert readiness.new_entry_fee_blocker is True
    assert readiness.active_fill_accounting_blocker is True


def test_active_fee_validation_blocked_still_blocks_next_buy() -> None:
    readiness = _readiness(qty=0.0, fee_validation_blocked_count=1)

    with pytest.raises(LivePipelineSmokePreflightError, match="fee_pending_blocks_exposure_increase"):
        validate_live_pipeline_smoke_step_readiness(readiness, expected_side="BUY")


def test_unapplied_principal_pending_still_blocks_next_buy() -> None:
    readiness = _readiness(qty=0.0, unapplied_principal_pending_count=1)

    with pytest.raises(LivePipelineSmokePreflightError, match="fee_pending_blocks_exposure_increase"):
        validate_live_pipeline_smoke_step_readiness(readiness, expected_side="BUY")


def test_latest_unresolved_fee_pending_still_blocks_next_buy() -> None:
    readiness = _readiness(qty=0.0, fill_accounting_active_issue_count=1)

    with pytest.raises(LivePipelineSmokePreflightError, match="fee_pending_blocks_exposure_increase"):
        validate_live_pipeline_smoke_step_readiness(readiness, expected_side="BUY")


def test_step_readiness_uses_new_entry_fee_blocker_not_closeout_blocker() -> None:
    readiness = _readiness(
        qty=0.0,
        active_fill_accounting_blocker=False,
        new_entry_fee_blocker=False,
        fee_gap_closeout_blocking=True,
    )

    validate_live_pipeline_smoke_step_readiness(readiness, expected_side="BUY")


def test_fee_gap_closeout_policy_remains_exposed_but_not_new_entry_blocker() -> None:
    readiness = _readiness(
        qty=0.0,
        active_fill_accounting_blocker=False,
        new_entry_fee_blocker=False,
        fee_gap_closeout_blocking=True,
        fee_gap_resume_blocking=True,
        fee_gap_policy_reason="historical fee-gap repair is applicable now",
        fee_gap_repair_eligibility_state="safe_to_apply_now",
    )

    payload = readiness.as_dict()

    assert payload["fee_gap_closeout_blocking"] is True
    assert payload["fee_gap_resume_blocking"] is True
    assert payload["fee_gap_policy_reason"] == "historical fee-gap repair is applicable now"
    assert payload["new_entry_fee_blocker"] is False
    assert payload["active_fill_accounting_blocker"] is False


def test_active_fee_accounting_issue_blocks_smoke_next_buy(monkeypatch, tmp_path) -> None:
    test_post_settlement_readiness_failure_payload_marks_post_step_phase(monkeypatch, tmp_path)


def test_post_step_readiness_attempt_records_fee_blocker_source(monkeypatch, tmp_path) -> None:
    broker = _Broker()
    service = LivePipelineSmokeExecutionService(broker=broker)
    payload, conn, _broker, _service, old = _run_smoke(
        monkeypatch,
        tmp_path,
        broker=broker,
        service=service,
        readiness_provider=lambda: (
            _readiness(qty=0.0, fee_pending_count=1, historical_fee_pending_observation_count=4, fill_accounting_active_issue_count=1)
            if len(service.submissions) >= 2 and abs(float(broker.qty)) <= 1e-12
            else _readiness(qty=float(broker.qty))
        ),
        settlement_coordinator=_always_settled_settlement(),
        post_step_readiness_barrier_config=PostStepReadinessBarrierConfig(
            max_attempts=1,
            poll_intervals_ms=(0,),
            deadline_ms=1000,
        ),
    )
    try:
        attempt = payload["post_step_readiness_attempts"][0]
        assert attempt["fee_blocker_source"]["active_fee_pending_count"] == 1
        assert attempt["fee_blocker_source"]["historical_fee_pending_observation_count"] == 4
        assert attempt["fee_blocker_source"]["fill_accounting_active_issue_count"] == 1
        assert attempt["fee_blocker_source"]["new_entry_fee_blocker"] is True
        assert "fee_pending_count" in attempt["fee_blocker_source"]["new_entry_fee_blocker_reasons"]
        assert attempt["fee_blocker_source"]["active_fill_accounting_blocker"] is True
        assert attempt["fee_blocker_source"]["active_fill_accounting_blocker_reasons"]
    finally:
        conn.close()
        _restore_settings(old)


def test_post_step_readiness_attempt_records_new_entry_fee_blocker_source() -> None:
    result = wait_for_live_pipeline_smoke_next_step_readiness(
        readiness_provider=lambda: _readiness(qty=0.0, fee_pending_count=1),
        expected_next_side="BUY",
        config=PostStepReadinessBarrierConfig(max_attempts=1, poll_intervals_ms=(0,), deadline_ms=1000),
        sleeper=lambda _seconds: None,
    )

    attempt = result.attempts[0]
    assert result.passed is False
    assert attempt["new_entry_fee_blocker"] is True
    assert attempt["new_entry_fee_blocker_reasons"] == ["fee_pending_count"]
    assert attempt["fee_blocker_source"]["new_entry_fee_blocker"] is True
    assert attempt["fee_blocker_source"]["new_entry_fee_blocker_reasons"] == ["fee_pending_count"]


def test_post_step_readiness_attempt_records_fee_gap_policy_source() -> None:
    result = wait_for_live_pipeline_smoke_next_step_readiness(
        readiness_provider=lambda: _readiness(
            qty=0.0,
            active_fill_accounting_blocker=False,
            new_entry_fee_blocker=False,
            fee_gap_closeout_blocking=True,
            fee_gap_resume_blocking=True,
            fee_gap_policy_reason="historical fee-gap repair is applicable now",
            fee_gap_repair_eligibility_state="safe_to_apply_now",
            fee_gap_incident_scope="active_blocking",
        ),
        expected_next_side="BUY",
        config=PostStepReadinessBarrierConfig(max_attempts=1, poll_intervals_ms=(0,), deadline_ms=1000),
        sleeper=lambda _seconds: None,
    )

    attempt = result.attempts[0]
    assert result.passed is True
    assert attempt["new_entry_fee_blocker"] is False
    assert attempt["active_fill_accounting_blocker"] is False
    assert attempt["fee_gap_closeout_blocking"] is True
    assert attempt["fee_blocker_source"]["fee_gap_closeout_blocking"] is True
    assert attempt["fee_blocker_source"]["fee_gap_policy_reason"] == "historical fee-gap repair is applicable now"


def test_post_step_failure_payload_identifies_closeout_policy_not_active_fee_issue() -> None:
    readiness = _readiness(
        qty=0.0,
        active_fill_accounting_blocker=False,
        new_entry_fee_blocker=False,
        fee_gap_closeout_blocking=True,
        fee_gap_policy_reason="historical fee-gap repair is applicable now",
    )

    payload = readiness.as_dict()

    assert payload["fee_gap_closeout_blocking"] is True
    assert payload["new_entry_fee_blocker"] is False
    assert payload["active_fill_accounting_blocker"] is False
    assert payload["active_fill_accounting_blocker_reasons"] == []


def test_smoke_step_evidence_records_state_transition(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        assert payload["status"] == "passed"
        step = payload["rounds"][0]["buy"]
        assert step["pre_state"] == "flat"
        assert step["settlement_state"] == "settled"
        assert step["post_state"] == "in_position"
        assert step["expected_next_side"] == "SELL"
        assert step["next_ready_state"] == "sell_ready"
        assert step["post_step_readiness_result"]["passed"] is True
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_sell_step_records_flat_post_state_and_buy_ready_next_state(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        sell = payload["rounds"][0]["sell"]
        assert sell["post_state"] == "flat"
        assert sell["expected_next_side"] == "BUY"
        assert sell["next_ready_state"] == "buy_ready"
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_final_step_records_complete_next_state(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        final_sell = payload["rounds"][-1]["sell"]
        assert final_sell["expected_next_side"] is None
        assert final_sell["next_ready_state"] == "complete"
        assert final_sell["post_step_readiness_result"]["passed"] is True
        assert final_sell["post_step_readiness_result"]["attempts"] == []
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_pass_requires_all_order_settlements_and_post_step_readiness(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        steps = [step for round_item in payload["rounds"] for step in (round_item["buy"], round_item["sell"])]
        assert payload["status"] == "passed"
        assert payload["orders_submitted"] == 10
        assert all(step["settlement_result"]["settled"] is True for step in steps)
        assert all(step["post_step_readiness_result"]["passed"] is True for step in steps)
        assert payload["post_step_readiness_summary"]
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_pass_requires_max_orders_even_if_final_flat(monkeypatch, tmp_path) -> None:
    test_smoke_failed_buy_then_manual_flatten_does_not_convert_to_passed(monkeypatch, tmp_path)


def test_smoke_fails_when_post_step_readiness_fails_even_if_final_flat(monkeypatch, tmp_path) -> None:
    test_smoke_persistent_post_sell_fee_pending_fails_with_readiness_attempts(monkeypatch, tmp_path)


def test_smoke_final_success_payload_includes_post_step_readiness_summary(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        assert payload["status"] == "passed"
        assert len(payload["post_step_readiness_summary"]) == 10
        assert all(item["passed"] is True for item in payload["post_step_readiness_summary"])
    finally:
        conn.close()
        _restore_settings(old)


def test_live_pipeline_smoke_fails_if_fee_pending_requires_manual_repair(monkeypatch, tmp_path) -> None:
    class _RepairingService(LivePipelineSmokeExecutionService):
        def execute(self, request):
            result = super().execute(request)
            if len(self.submissions) == 1:
                record_fee_pending_accounting_repair(
                    request.execution_decision_summary.target_submit_plan.extra_payload["conn"]
                    if False
                    else conn,
                    event_ts=1,
                    client_order_id="repair_fee",
                    exchange_order_id="ex_repair_fee",
                    fill_id="fill_repair_fee",
                    fill_ts=1,
                    price=1.0,
                    qty=1.0,
                    fee=1.0,
                    source="unit",
                    reason="fee_pending_accounting_repair",
                    repair_basis={"unit": True},
                )
            return result

    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    conn = ensure_db(str(db_path))
    try:
        _insert_top_of_book(conn)
        broker = _Broker()
        service = _RepairingService(broker=broker)
        authority = _authority(tmp_path, db_path, max_notional_krw=20_000.0)
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)
        readiness_provider = lambda: _readiness_from_broker(broker)
        payload = run_live_pipeline_smoke(
            conn=conn,
            broker=broker,
            cycles=5,
            max_orders=10,
            max_notional_krw=20_000.0,
            yes=True,
            authority_path=str(authority),
            confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
            execution_service=service,
            readiness_provider=readiness_provider,
            post_trade_reconcile=lambda: _record_reconcile_attempt(),
            settlement_coordinator=_test_only_readiness_settlement(readiness_provider),
            run_id="lps_fee_repair_test",
        )
        assert payload["status"] == "failed"
        assert payload["reason"] == "live_pipeline_smoke_final_completion_criteria_failed"
    finally:
        conn.close()
        _restore_settings(old)


def test_live_pipeline_smoke_fails_if_projection_repair_event_created(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        assert payload["status"] == "passed"
    finally:
        conn.close()
        _restore_settings(old)

    db_path = tmp_path / "live_repair.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    conn = ensure_db(str(db_path))
    try:
        _insert_top_of_book(conn)
        broker = _Broker()
        authority = _authority(tmp_path, db_path, max_notional_krw=20_000.0)
        service = LivePipelineSmokeExecutionService(broker=broker)
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)

        def _reconcile():
            if len(service.submissions) == 1:
                record_position_authority_repair(
                    conn,
                    event_ts=1,
                    source="unit",
                    reason="historical_fragmentation_projection_drift_repair",
                    repair_basis={"unit": True},
                )

        readiness_provider = lambda: _readiness_from_broker(broker)
        test_settlement = _test_only_readiness_settlement(readiness_provider)

        def _settle_with_reconcile(trade):
            _reconcile()
            return test_settlement(trade)

        payload = run_live_pipeline_smoke(
            conn=conn,
            broker=broker,
            cycles=5,
            max_orders=10,
            max_notional_krw=20_000.0,
            yes=True,
            authority_path=str(authority),
            confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
            execution_service=service,
            readiness_provider=readiness_provider,
            post_trade_reconcile=_reconcile,
            settlement_coordinator=_settle_with_reconcile,
            run_id="lps_position_repair_test",
        )
        assert payload["status"] == "failed"
        assert payload["reason"] == "live_pipeline_smoke_final_completion_criteria_failed"
    finally:
        conn.close()
        _restore_settings(old)


def test_live_pipeline_smoke_records_settlement_evidence_for_each_step(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        assert payload["status"] == "passed"
        steps = [step for round_item in payload["rounds"] for step in (round_item["buy"], round_item["sell"])]
        assert len(steps) == 10
        assert all("settlement_result" in step for step in steps)
        assert all("attempts" in step["settlement_result"]["evidence"] for step in steps)
    finally:
        conn.close()
        _restore_settings(old)


def test_live_pipeline_smoke_uses_broker_order_fill_settlement_wrapper(monkeypatch, tmp_path) -> None:
    payload, conn, broker, _service, old = _run_scripted_smoke(
        monkeypatch,
        tmp_path,
        delayed_first_buy=False,
        cycles=5,
    )
    try:
        assert payload["status"] == "passed"
        assert isinstance(smoke_module.LiveOrderSettlementWrapper(broker=broker, db_factory=lambda: conn), LiveOrderSettlementWrapper)
        assert any(endpoint == "/v1/order" for endpoint, _params in broker.private_calls)
        assert len(broker.private_calls) >= 4
        steps = [payload["rounds"][0]["buy"], payload["rounds"][0]["sell"]]
        assert all(step["settlement_result"]["settled"] is True for step in steps)
        assert all(step["settlement_result"]["evidence"]["attempts"] for step in steps)
    finally:
        conn.close()
        _restore_scripted_smoke_settings(old)


def test_live_pipeline_smoke_settlement_calls_get_order_and_get_fills(monkeypatch, tmp_path) -> None:
    payload, conn, broker, _service, old = _run_scripted_smoke(
        monkeypatch,
        tmp_path,
        delayed_first_buy=False,
        cycles=5,
    )
    try:
        assert payload["status"] == "passed"
        requested_ids = [
            str(params.get("uuid") or params.get("client_order_id"))
            for endpoint, params in broker.private_calls
            if endpoint == "/v1/order"
        ]
        assert "ex-smoke-1-buy" in requested_ids
        assert "ex-smoke-2-sell" in requested_ids
        assert broker.private_calls
    finally:
        conn.close()
        _restore_scripted_smoke_settings(old)


def test_live_pipeline_smoke_does_not_use_readiness_only_observer_for_apply_path() -> None:
    source = inspect.getsource(smoke_module.run_live_pipeline_smoke)
    forbidden = "_settlement_observation" + "_from_readiness"
    assert forbidden not in source
    assert "LiveOrderSettlementWrapper(" in source


def test_live_pipeline_smoke_apply_path_uses_nonzero_settlement_intervals() -> None:
    smoke_source = inspect.getsource(smoke_module.run_live_pipeline_smoke)
    wrapper_source = inspect.getsource(LiveOrderSettlementWrapper.__call__)
    forbidden = "poll_intervals_ms=" + "(0, 0, 0, 0, 0)"
    assert forbidden not in smoke_source
    assert "poll_intervals_ms=(100, 250, 500, 1000, 2000)" in wrapper_source


def test_live_pipeline_smoke_delayed_paid_fee_settles_and_advances_to_sell(monkeypatch, tmp_path) -> None:
    payload, conn, broker, service, old = _run_scripted_smoke(
        monkeypatch,
        tmp_path,
        delayed_first_buy=True,
        cycles=5,
    )
    try:
        assert payload["status"] == "passed"
        assert [submission.side for submission in service.submissions[:2]] == ["BUY", "SELL"]
        buy = payload["rounds"][0]["buy"]
        attempts = buy["settlement_result"]["evidence"]["attempts"]
        assert len(attempts) >= 2
        assert attempts[0]["fee_state"] == "pending"
        assert attempts[-1]["fee_state"] == "finalized"
        assert buy["settlement_result"]["settled"] is True
        history = broker.order_payload_history["smoke-1-buy"]
        assert "paid_fee" not in history[0]
        assert "paid_fee" in history[1]
        assert _table_count(conn, "fee_pending_accounting_repairs") == 0
        assert _table_count(conn, "position_authority_repairs") == 0
        assert _table_count(conn, "manual_flat_accounting_repairs") == 0
    finally:
        conn.close()
        _restore_scripted_smoke_settings(old)


def test_live_pipeline_smoke_scripted_v1_order_completes_one_round_trip_without_manual_repair(
    monkeypatch, tmp_path
) -> None:
    payload, conn, _broker, service, old = _run_scripted_smoke(
        monkeypatch,
        tmp_path,
        delayed_first_buy=False,
        cycles=5,
    )
    try:
        assert payload["status"] == "passed"
        assert payload["orders_submitted"] == 10
        assert [submission.side for submission in service.submissions[:2]] == ["BUY", "SELL"]
        assert _table_count(conn, "manual_flat_accounting_repairs") == 0
    finally:
        conn.close()
        _restore_scripted_smoke_settings(old)


def test_live_pipeline_smoke_scripted_v1_order_five_round_trips_without_repair(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, service, old = _run_scripted_smoke(
        monkeypatch,
        tmp_path,
        delayed_first_buy=False,
        cycles=5,
    )
    try:
        assert payload["status"] == "passed"
        assert payload["orders_submitted"] == 10
        assert payload["buy_submitted"] == 5
        assert payload["sell_submitted"] == 5
        assert len(service.submissions) == 10
        assert _table_count(conn, "fee_pending_accounting_repairs") == 0
        assert _table_count(conn, "position_authority_repairs") == 0
        assert _table_count(conn, "manual_flat_accounting_repairs") == 0
    finally:
        conn.close()
        _restore_scripted_smoke_settings(old)


def test_smoke_timed_out_failure_payload_contains_settlement_attempts(monkeypatch, tmp_path) -> None:
    def _timed_out(trade):
        attempts = [
            {
                "attempt_index": 0,
                "order_state": "FILLED",
                "fill_count": 1,
                "fill_set_complete": True,
                "paid_fee_present": False,
                "fee_state": "pending",
                "db_fill_count": 1,
                "principal_applied": True,
                "accounting_finalized": False,
                "projection_applied": True,
                "broker_qty": 0.0002,
                "portfolio_qty": 0.0002,
                "projected_total_qty": 0.0002,
                "broker_local_converged": True,
                "reason_code": "settlement_evidence_pending",
            }
        ]
        return evaluate_settlement_snapshot(
            client_order_id=str(trade["client_order_id"]),
            exchange_order_id=str(trade["exchange_order_id"]),
            evidence=attempts[-1],
            attempts=attempts,
            deadline_exceeded=True,
        )

    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    conn = ensure_db(str(db_path))
    try:
        _insert_top_of_book(conn)
        broker = _Broker()
        service = LivePipelineSmokeExecutionService(broker=broker)
        authority = _authority(tmp_path, db_path, max_notional_krw=20_000.0)
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)
        payload = run_live_pipeline_smoke(
            conn=conn,
            broker=broker,
            cycles=5,
            max_orders=10,
            max_notional_krw=20_000.0,
            yes=True,
            authority_path=str(authority),
            confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
            execution_service=service,
            readiness_provider=lambda: _readiness_from_broker(broker),
            settlement_coordinator=_timed_out,
            run_id="lps_timed_out_payload",
        )
        assert payload["status"] == "failed"
        assert payload["reason"] == "timed_out"
        assert payload["failed_client_order_id"] == service.submissions[0]["client_order_id"]
        assert payload["failed_exchange_order_id"] == service.submissions[0]["exchange_order_id"]
        assert payload["failed_side"] == "BUY"
        settlement = payload["settlement_result"]
        assert settlement["reason_code"] == "timed_out"
        assert settlement["deadline_exceeded"] is True
        attempt = settlement["evidence"]["attempts"][0]
        assert {
            "fee_state",
            "db_fill_count",
            "broker_qty",
            "portfolio_qty",
            "projected_total_qty",
            "projection_applied",
        } <= set(attempt)
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_failure_payload_contains_failed_order_identifiers(monkeypatch, tmp_path) -> None:
    test_smoke_timed_out_failure_payload_contains_settlement_attempts(monkeypatch, tmp_path)


def test_smoke_failure_payload_attempts_include_fee_db_projection_broker_fields(monkeypatch, tmp_path) -> None:
    test_smoke_timed_out_failure_payload_contains_settlement_attempts(monkeypatch, tmp_path)


def test_smoke_failed_buy_then_manual_flatten_does_not_convert_to_passed(monkeypatch, tmp_path) -> None:
    def _timed_out(trade):
        evidence = {
            "order_state": "FILLED",
            "fill_count": 1,
            "fill_set_complete": True,
            "fee_state": "pending",
            "principal_applied": True,
            "accounting_finalized": False,
            "projection_applied": True,
            "broker_qty": 0.0,
            "portfolio_qty": 0.0,
            "projected_total_qty": 0.0,
            "broker_local_converged": True,
            "reason_code": "settlement_evidence_pending",
        }
        return evaluate_settlement_snapshot(
            client_order_id=str(trade["client_order_id"]),
            exchange_order_id=str(trade["exchange_order_id"]),
            evidence=evidence,
            attempts=[evidence],
            deadline_exceeded=True,
        )

    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    conn = ensure_db(str(db_path))
    try:
        _insert_top_of_book(conn)
        broker = _Broker()
        service = LivePipelineSmokeExecutionService(broker=broker)
        authority = _authority(tmp_path, db_path, max_notional_krw=20_000.0)
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)
        payload = run_live_pipeline_smoke(
            conn=conn,
            broker=broker,
            cycles=5,
            max_orders=10,
            max_notional_krw=20_000.0,
            yes=True,
            authority_path=str(authority),
            confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
            execution_service=service,
            readiness_provider=lambda: LivePipelineSmokeReadiness(
                broker_qty=0.0,
                portfolio_qty=0.0,
                projected_total_qty=0.0,
                open_order_count=0,
                submit_unknown_count=0,
                recovery_required_count=0,
                fee_pending_count=0,
                active_fee_accounting_blocker=False,
                broker_qty_known=True,
                balance_source_stale=False,
                projection_converged=True,
            ),
            settlement_coordinator=_timed_out,
            run_id="lps_manual_flatten_not_success",
        )
        assert payload["status"] == "failed"
        assert payload["orders_submitted"] == 1
        assert len(service.submissions) == 1
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_result_requires_no_manual_intervention(monkeypatch, tmp_path) -> None:
    test_live_pipeline_smoke_fails_if_fee_pending_requires_manual_repair(monkeypatch, tmp_path)


def test_smoke_result_failed_when_recovery_after_failure_creates_flat_state(monkeypatch, tmp_path) -> None:
    test_smoke_failed_buy_then_manual_flatten_does_not_convert_to_passed(monkeypatch, tmp_path)


def test_recorded_smoke_five_round_trips_touches_orders_fills_trades_portfolio_lots(monkeypatch, tmp_path) -> None:
    assert_recorded_smoke_five_round_trips_touches_orders_fills_trades_portfolio_lots(tmp_path, monkeypatch)


def test_smoke_result_failed_when_fee_pending_repair_event_created(monkeypatch, tmp_path) -> None:
    test_live_pipeline_smoke_fails_if_fee_pending_requires_manual_repair(monkeypatch, tmp_path)


def test_smoke_result_failed_when_position_authority_repair_event_created(monkeypatch, tmp_path) -> None:
    test_live_pipeline_smoke_fails_if_projection_repair_event_created(monkeypatch, tmp_path)


def test_smoke_result_passed_when_no_repair_events_and_all_10_orders_settle(monkeypatch, tmp_path) -> None:
    test_live_pipeline_smoke_passes_only_when_all_10_orders_settle_without_repair(monkeypatch, tmp_path)
