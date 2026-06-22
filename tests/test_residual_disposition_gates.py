from __future__ import annotations

import json
from types import SimpleNamespace

from bithumb_bot.runtime_recovery_gate import RuntimeRecoveryGateService
from bithumb_bot.runtime_recovery_services import StartupSafetyGateService
from bithumb_bot.runtime_resume_services import RuntimeResumeService
from bithumb_bot.residual_disposition import build_residual_disposition


def _verdict(*, run_allowed: bool = True, mismatch: bool = False):
    qty = 0.00009996
    return build_residual_disposition(
        residual_inventory=SimpleNamespace(residual_qty=qty, exchange_sellable=False),
        residual_inventory_state="RESIDUAL_INVENTORY_TRACKED",
        residual_policy_allows_run=run_allowed,
        residual_policy_allows_buy=run_allowed,
        residual_policy_allows_sell=False,
        position_state=SimpleNamespace(
            normalized_exposure=SimpleNamespace(
                has_executable_exposure=False,
                sellable_executable_lot_count=0,
            )
        ),
        authority_assessment={},
        projection_convergence={
            "converged": not mismatch,
            "portfolio_qty": qty,
            "projected_total_qty": qty if not mismatch else qty + 0.0001,
        },
        broker_position_evidence={"broker_qty_known": True, "broker_qty": qty},
        lot_definition=SimpleNamespace(
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            max_qty_decimals=8,
            source_mode="ledger",
        ),
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
    )


def test_startup_resume_and_reconcile_halt_agree_for_tracked_sub_min_residual():
    verdict = _verdict()

    assert verdict.disposition == "TRACKED_NON_EXECUTABLE"
    assert verdict.run_allowed is True
    assert verdict.buy_allowed is True
    assert verdict.flatten_allowed is False


def test_resume_does_not_block_when_residual_disposition_allows_run():
    verdict = _verdict()

    assert verdict.run_allowed is True
    assert verdict.reason_codes == ("sub_min_qty_residual_tracked",)


def test_reconcile_halt_clears_only_when_residual_disposition_allows_run():
    allowed = _verdict()
    blocked = _verdict(mismatch=True)

    assert allowed.run_allowed is True
    assert blocked.disposition == "BLOCKING_INCONSISTENT"
    assert blocked.run_allowed is False


class _Cursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self) -> None:
        self.closed = False

    def execute(self, sql: str, *_args):
        if "FROM orders" in sql and "PENDING_SUBMIT" in sql:
            return _Cursor(
                {
                    "pending_submit_count": 0,
                    "accounting_pending_count": 0,
                    "submit_unknown_count": 0,
                    "recovery_required_count": 0,
                    "stale_new_partial_count": 0,
                    "unresolved_count": 0,
                }
            )
        if "FROM orders" in sql:
            return _Cursor({"unresolved_count": 0, "recovery_required_count": 0})
        if "FROM portfolio" in sql:
            return _Cursor({"asset_qty": 0.00009996})
        return _Cursor({})

    def close(self) -> None:
        self.closed = True


def _readiness_from_verdict(verdict):
    normalized = SimpleNamespace(
        terminal_state="dust_only",
        open_exposure_qty=0.0,
        raw_total_asset_qty=0.00009996,
        dust_tracking_qty=0.00009996,
        open_lot_count=0,
        dust_tracking_lot_count=1,
        sellable_executable_lot_count=0,
        sellable_executable_qty=0.0,
        exit_block_reason="no_executable_exit_lot",
        has_executable_exposure=False,
        has_dust_only_remainder=True,
        effective_flat=True,
        authority_gap_reason=None,
    )
    return SimpleNamespace(
        residual_disposition=verdict,
        residual_inventory=SimpleNamespace(residual_qty=0.00009996),
        residual_inventory_state="RESIDUAL_INVENTORY_TRACKED",
        position_state=SimpleNamespace(normalized_exposure=normalized),
        recovery_stage="READY",
        position_authority_assessment={},
        projection_convergence={"projected_total_qty": 0.00009996, "portfolio_qty": 0.00009996},
        fee_pending_count=0,
        fee_gap_incident=SimpleNamespace(active_issue=False, policy=SimpleNamespace(resume_blocking=False)),
        recovery_required_count=0,
        open_order_count=0,
        fill_accounting_incident_summary={},
    )


def test_startup_gate_allows_tracked_non_executable_residual_from_readiness(monkeypatch):
    verdict = _verdict(run_allowed=True)
    state = SimpleNamespace(
        last_reconcile_metadata=None,
        unresolved_open_order_count=0,
        recovery_required_count=0,
    )
    gates: list[str | None] = []

    monkeypatch.setattr("bithumb_bot.runtime_recovery_services.ensure_db", lambda *_, **__: _Conn())
    monkeypatch.setattr(
        "bithumb_bot.runtime_recovery_services.collect_risky_order_state",
        lambda *_args, **_kwargs: {
            "submit_unknown_without_exchange_id_count": 0,
            "stray_remote_open_order_count": 0,
        },
    )
    monkeypatch.setattr(
        "bithumb_bot.runtime_recovery_services.compute_runtime_readiness_snapshot",
        lambda _conn: _readiness_from_verdict(verdict),
    )

    reason = StartupSafetyGateService(
        state_snapshot=lambda: state,
        refresh_open_order_health=lambda: None,
        emergency_flatten_blocker=lambda: None,
        set_startup_gate_reason=gates.append,
        balance_split_mismatch_counter=lambda _state: 0,
    ).evaluate()

    assert reason is None
    assert gates == [None]
    assert "clean_account_gate" not in str(reason)


def test_resume_service_uses_residual_disposition_before_dust_metadata(monkeypatch):
    verdict = _verdict(run_allowed=True)
    state = SimpleNamespace(
        last_reconcile_status="ok",
        last_reconcile_reason_code=None,
        last_reconcile_error=None,
        last_reconcile_metadata=json.dumps(
            {
                "dust_residual_present": 1,
                "dust_residual_allow_resume": 0,
                "dust_residual_effective_flat": 0,
                "dust_residual_classification": "dangerous_dust",
            }
        ),
        halt_new_orders_blocked=False,
        halt_state_unresolved=False,
        halt_reason_code=None,
        last_disable_reason=None,
        unresolved_open_order_count=0,
        recovery_required_count=0,
        emergency_flatten_blocked=False,
        emergency_flatten_block_reason=None,
        last_flatten_position_status=None,
        halt_open_orders_present=False,
        halt_position_present=False,
    )
    gates: list[tuple[bool, str | None]] = []
    monkeypatch.setattr(
        "bithumb_bot.runtime_resume_services.compute_runtime_readiness_snapshot",
        lambda _conn: _readiness_from_verdict(verdict),
    )

    recovery_gate = RuntimeRecoveryGateService(
        startup_gate_evaluator=lambda: None,
        initial_reconcile_halt_evaluator=lambda **_kwargs: False,
        live_execution_broker_halt_evaluator=lambda **_kwargs: False,
        risk_state_mismatch_halt_evaluator=lambda **_kwargs: False,
        state_snapshot=lambda: state,
    )
    service = RuntimeResumeService(
        recovery_gate_factory=lambda: recovery_gate,
        state_snapshot=lambda: state,
        set_resume_gate=lambda *, blocked, reason: gates.append((blocked, reason)),
        db_factory=lambda: _Conn(),
        repair_service=SimpleNamespace(
            manual_flat_accounting_preview=lambda _conn: {"safe_to_apply": False},
            fee_gap_accounting_preview=lambda _conn: {"needs_repair": False},
        ),
    )

    allowed, blockers = service.evaluate_resume_eligibility()

    assert allowed is True
    assert [blocker.code for blocker in blockers] == []
    assert gates == [(False, None)]


def test_reconcile_halt_clear_uses_residual_disposition_run_allowed(monkeypatch):
    import bithumb_bot.recovery as recovery

    verdict = _verdict(run_allowed=True)
    state = SimpleNamespace(
        halt_new_orders_blocked=True,
        halt_reason_code="DAILY_LOSS_LIMIT",
        halt_state_unresolved=True,
    )
    cleared: list[dict[str, object]] = []

    monkeypatch.setattr(recovery.runtime_state, "snapshot", lambda: state)
    monkeypatch.setattr(recovery.runtime_state, "disable_trading_until", lambda *args, **kwargs: cleared.append(kwargs))
    monkeypatch.setattr(recovery.runtime_state, "set_resume_gate", lambda **_kwargs: None)
    monkeypatch.setattr(recovery, "compute_runtime_readiness_snapshot", lambda _conn: _readiness_from_verdict(verdict))
    monkeypatch.setattr(
        recovery,
        "summarize_position_lots",
        lambda *_args, **_kwargs: SimpleNamespace(
            lot_definition=SimpleNamespace(
                internal_lot_size=0.0001,
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=5000.0,
                max_qty_decimals=8,
            ),
            raw_total_asset_qty=0.00009996,
            raw_open_exposure_qty=0.0,
            dust_tracking_qty=0.00009996,
            open_lot_count=0,
            dust_tracking_lot_count=1,
        ),
    )
    monkeypatch.setattr(
        recovery,
        "build_position_state_model",
        lambda **_kwargs: _readiness_from_verdict(verdict).position_state,
    )

    recovery._clear_reconcile_halt_if_safe(
        conn=_Conn(),
        reason_code="RECONCILE_OK",
        metadata={"dust_residual_allow_resume": 0},
        broker_open_order_count=0,
    )

    assert cleared
    assert cleared[-1]["halt_new_orders_blocked"] is False
