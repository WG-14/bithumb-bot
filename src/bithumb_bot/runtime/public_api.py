from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .. import runtime_state
from ..db_core import ensure_db
from ..runtime_gate_api import RuntimeGateApi
from ..runtime_readiness import compute_runtime_readiness_snapshot
from .app_container import create_default_runtime_app


@dataclass(frozen=True)
class RuntimeHealthQuery:
    state_snapshot: Callable[[], object] = runtime_state.snapshot
    app_factory: Callable[[], Any] = create_default_runtime_app

    def get_status(self) -> dict[str, float | int | bool | str | None]:
        app = self.app_factory()
        startup_gate_reason = app.runtime_gate_api.startup_safety_gate()
        RuntimeRecoveryCommand(self.app_factory).try_clear_stale_halts(
            startup_gate_reason=startup_gate_reason,
        )
        state = self.state_snapshot()
        residual_fields: dict[str, str | bool | None] = {
            "residual_disposition": None,
            "residual_reason_code": None,
            "manual_exchange_action_required": None,
            "quantity_rule_authority": None,
            "broker_local_projection_state": None,
        }
        conn = ensure_db()
        try:
            readiness = compute_runtime_readiness_snapshot(conn).as_dict()
            disposition = readiness.get("residual_disposition")
            residual_fields.update(
                {
                    "residual_disposition": (
                        str(disposition.get("disposition"))
                        if isinstance(disposition, dict) and disposition.get("disposition") is not None
                        else None
                    ),
                    "residual_reason_code": (
                        str(readiness.get("residual_reason_code"))
                        if readiness.get("residual_reason_code") is not None
                        else None
                    ),
                    "manual_exchange_action_required": bool(
                        readiness.get("manual_exchange_action_required")
                    ),
                    "quantity_rule_authority": (
                        str(readiness.get("quantity_rule_authority"))
                        if readiness.get("quantity_rule_authority") is not None
                        else None
                    ),
                    "broker_local_projection_state": (
                        str(readiness.get("broker_local_projection_state"))
                        if readiness.get("broker_local_projection_state") is not None
                        else None
                    ),
                }
            )
        finally:
            conn.close()
        return {
            "last_candle_age_sec": state.last_candle_age_sec,
            "last_candle_status": state.last_candle_status,
            "last_candle_sync_epoch_sec": state.last_candle_sync_epoch_sec,
            "last_candle_ts_ms": state.last_candle_ts_ms,
            "last_processed_candle_ts_ms": state.last_processed_candle_ts_ms,
            "last_candle_status_detail": state.last_candle_status_detail,
            "error_count": state.error_count,
            "trading_enabled": state.trading_enabled,
            "retry_at_epoch_sec": state.retry_at_epoch_sec,
            "last_disable_reason": state.last_disable_reason,
            "halt_new_orders_blocked": state.halt_new_orders_blocked,
            "halt_reason_code": state.halt_reason_code,
            "halt_state_unresolved": state.halt_state_unresolved,
            "halt_policy_stage": state.halt_policy_stage,
            "halt_policy_block_new_orders": state.halt_policy_block_new_orders,
            "halt_policy_attempt_cancel_open_orders": state.halt_policy_attempt_cancel_open_orders,
            "halt_policy_auto_liquidate_positions": state.halt_policy_auto_liquidate_positions,
            "halt_position_present": state.halt_position_present,
            "halt_open_orders_present": state.halt_open_orders_present,
            "halt_operator_action_required": state.halt_operator_action_required,
            "unresolved_open_order_count": state.unresolved_open_order_count,
            "oldest_unresolved_order_age_sec": state.oldest_unresolved_order_age_sec,
            "recovery_required_count": state.recovery_required_count,
            "last_reconcile_epoch_sec": state.last_reconcile_epoch_sec,
            "last_reconcile_status": state.last_reconcile_status,
            "last_reconcile_error": state.last_reconcile_error,
            "last_reconcile_reason_code": state.last_reconcile_reason_code,
            "last_reconcile_metadata": state.last_reconcile_metadata,
            "last_cancel_open_orders_epoch_sec": state.last_cancel_open_orders_epoch_sec,
            "last_cancel_open_orders_trigger": state.last_cancel_open_orders_trigger,
            "last_cancel_open_orders_status": state.last_cancel_open_orders_status,
            "last_cancel_open_orders_summary": state.last_cancel_open_orders_summary,
            "emergency_flatten_blocked": state.emergency_flatten_blocked,
            "emergency_flatten_block_reason": state.emergency_flatten_block_reason,
            "startup_gate_reason": state.startup_gate_reason,
            "resume_gate_blocked": state.resume_gate_blocked,
            "resume_gate_reason": state.resume_gate_reason,
            **residual_fields,
        }


@dataclass(frozen=True)
class RuntimeResumeQuery:
    runtime_gate_api_factory: Callable[[], RuntimeGateApi]

    def evaluate_eligibility(self):
        runtime_gate_api = self.runtime_gate_api_factory()
        startup_gate = getattr(runtime_gate_api, "startup_safety_gate", None)
        RuntimeRecoveryCommand().try_clear_stale_halts(
            startup_gate_reason=startup_gate() if callable(startup_gate) else None,
        )
        return runtime_gate_api.resume_eligibility()


@dataclass(frozen=True)
class RuntimeRecoveryCommand:
    app_factory: Callable[[], Any] = create_default_runtime_app

    def try_clear_stale_halts(self, *, startup_gate_reason: str | None = None) -> bool:
        app = self.app_factory()
        changed = False
        for clearance_type in (
            "initial_reconcile",
            "live_execution_broker",
            "risk_state_mismatch",
        ):
            clearance = app.recovery_controller.evaluate_clearance(
                snapshot=runtime_state.snapshot(),
                startup_gate_reason=startup_gate_reason,
                clearance_type=clearance_type,
            )
            if clearance.allowed:
                app.recovery_controller.apply_clearance(clearance)
                changed = True
        return changed


@dataclass(frozen=True)
class RuntimeResumeCommand:
    recovery_command: RuntimeRecoveryCommand = RuntimeRecoveryCommand()

    def apply_clearance(self, *, startup_gate_reason: str | None = None) -> bool:
        return self.recovery_command.try_clear_stale_halts(
            startup_gate_reason=startup_gate_reason,
        )


def get_health_status() -> dict[str, float | int | bool | str | None]:
    return RuntimeHealthQuery().get_status()


def evaluate_resume_eligibility():
    return RuntimeResumeQuery(lambda: create_default_runtime_app().runtime_gate_api).evaluate_eligibility()


def evaluate_startup_safety_gate() -> str | None:
    return create_default_runtime_app().runtime_gate_api.startup_safety_gate()


def perform_panic_stop_cleanup(*args: object, **kwargs: object):
    return create_default_runtime_app().safety_controller.attempt_cleanup_with_optional_flatten(
        *args,
        **kwargs,
    )


__all__ = [
    "RuntimeHealthQuery",
    "RuntimeRecoveryCommand",
    "RuntimeResumeCommand",
    "RuntimeResumeQuery",
    "evaluate_resume_eligibility",
    "evaluate_startup_safety_gate",
    "get_health_status",
    "perform_panic_stop_cleanup",
]
