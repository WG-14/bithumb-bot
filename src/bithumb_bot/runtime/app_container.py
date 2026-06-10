from __future__ import annotations

import importlib
import json
import os
import subprocess
import time
from dataclasses import dataclass, replace
from typing import Any, Callable, Protocol

from ..bootstrap import get_last_explicit_env_load_summary
from ..broker.bithumb import BithumbBroker, build_broker_with_auth_diagnostics
from ..config import (
    MarketPreflightValidationError,
    settings,
    validate_live_mode_preflight,
    validate_market_preflight,
    validate_market_runtime,
    validate_runtime_strategy_set_selection,
)
from ..db_core import (
    ensure_db,
    record_runtime_dependency_manifest,
    record_runtime_strategy_set_manifest,
)
from ..decision_equivalence import sha256_prefixed
from ..execution_service import (
    build_signal_execution_service,
    live_execute_signal,
    paper_execute,
    record_harmless_dust_exit_suppression,
)
from ..marketdata import cmd_sync
from ..runtime_data_access import (
    count_open_orders as _count_open_orders,
    latest_order_identifiers as _latest_order_identifiers,
    mark_open_orders_recovery_required as _mark_open_orders_recovery_required,
    open_order_snapshot as _get_open_order_snapshot,
    portfolio_cash_qty_with_position_state,
    position_summary as _position_summary,
    select_latest_candle,
    select_latest_closed_candle as _runtime_select_latest_closed_candle,
)
from ..runtime_gate_api import RuntimeGateApi
from ..runtime_readiness import compute_runtime_readiness_snapshot
from ..runtime_recovery_gate import (
    RuntimeRecoveryGateService,
    classify_startup_gate_reason,
)
from ..runtime_recovery_services import StaleRiskStateMismatchHaltService
from ..runtime_resume_services import (
    RestartReadinessService,
    ResumeGuidanceService,
    RuntimeResumeService,
    classify_balance_split_blocker,
    classify_dust_resume_blocker,
    default_ledger_external_cash_adjustment_summary,
    dust_residual_resume_blocker,
    extract_balance_split_delta_krw,
    last_reconcile_fee_pending_recovery_required,
    reconcile_balance_split_mismatch_count,
    reconcile_dust_context,
)
from ..runtime_service_factories import (
    operator_flatten_service,
    operator_notification_service,
    operator_repair_service,
    run_loop_execution_planner,
)
from ..runtime_strategy_set import (
    active_runtime_strategy_set,
    normalized_runtime_strategy_set_manifest,
)
from ..utils_time import parse_interval_sec
from .. import runtime_state
from .cleanup_revalidation import build_default_cleanup_revalidation_service
from .decision_coordinator import DecisionCoordinator
from .execution_coordinator import ExecutionCoordinator
from .lifecycle_artifacts import RuntimeCycleArtifact, RuntimeDependencyManifest
from .notification_adapter import NotificationAdapter
from .operator_event_composer import recommended_operator_commands
from .recovery_controller import RecoveryController, ReconcileClearEvidence
from .safety_controller import HaltReason, SafetyController
from .startup_controller import StartupController
from .state_store import RuntimeStateStore


class Scheduler(Protocol):
    def sleep(self, seconds: float) -> None: ...


@dataclass(frozen=True)
class TimeScheduler:
    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


def _close_guard_ms(interval_sec: int) -> int:
    interval_ms = max(1, int(interval_sec)) * 1000
    return max(2_000, min(30_000, interval_ms // 20))


def _is_closed_candle(*, candle_ts_ms: int, now_ms: int, interval_sec: int) -> bool:
    interval_ms = max(1, int(interval_sec)) * 1000
    close_ready_ts_ms = int(candle_ts_ms) + interval_ms + _close_guard_ms(interval_sec)
    return int(now_ms) >= close_ready_ts_ms


def _select_latest_closed_candle(conn, **kwargs):
    return _runtime_select_latest_closed_candle(
        conn,
        **kwargs,
        is_closed_candle=_is_closed_candle,
    )


def _identity(value: object) -> str:
    if hasattr(value, "__module__") and hasattr(value, "__qualname__"):
        return f"{getattr(value, '__module__')}.{getattr(value, '__qualname__')}"
    cls = value.__class__
    return f"{cls.__module__}.{cls.__qualname__}"


def _source_revision() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=os.getcwd(),
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None


def _settings_hash(settings_obj: object) -> str:
    payload = {
        name: getattr(settings_obj, name)
        for name in (
            "MODE",
            "PAIR",
            "INTERVAL",
            "STRATEGY_NAME",
            "EXECUTION_ENGINE",
            "LIVE_DRY_RUN",
            "LIVE_REAL_ORDER_ARMED",
            "MAX_ORDER_KRW",
            "MAX_DAILY_LOSS_KRW",
            "MAX_DAILY_ORDER_COUNT",
        )
        if hasattr(settings_obj, name)
    }
    return sha256_prefixed(payload)


def _env_summary_hash() -> tuple[str | None, str | None]:
    summary = get_last_explicit_env_load_summary().as_dict()
    return sha256_prefixed(summary), str(summary.get("selected_path") or summary.get("source") or "")


@dataclass
class RuntimeAppContainer:
    settings_obj: object
    db_factory: Callable[[], Any]
    clock: Callable[[], float]
    scheduler: Scheduler
    broker_factory: Callable[[], Any]
    market_sync: Callable[..., Any]
    candle_reader: Callable[..., Any]
    closed_candle_selector: Callable[..., Any]
    runtime_state_store: RuntimeStateStore
    decision_coordinator: DecisionCoordinator
    execution_coordinator: ExecutionCoordinator
    safety_controller: SafetyController
    startup_controller: StartupController
    notification_service: Any
    notification_adapter: NotificationAdapter
    artifact_sink: Callable[..., RuntimeCycleArtifact]
    execution_service_factory: Callable[..., Any]
    recovery_controller: RecoveryController
    reconcile_with_broker: Callable[[Any], None]
    runtime_strategy_set_provider: Callable[[], Any]
    runtime_strategy_set_manifest_provider: Callable[..., dict[str, object]]
    runtime_dependency_manifest_provider: Callable[..., RuntimeDependencyManifest]
    runtime_gate_api: RuntimeGateApi
    validate_market_preflight: Callable[[Any], None]
    validate_runtime_strategy_set_selection: Callable[[Any], None]
    validate_live_mode_preflight: Callable[[Any], None]
    validate_market_runtime: Callable[[Any], None]
    market_validation_error_type: type[BaseException]
    interval_parser: Callable[[str], int]
    execution_engine_name: str
    live_executor: Callable[..., Any]
    paper_executor: Callable[..., Any]
    harmless_dust_recorder: Callable[..., Any]
    open_order_snapshot: Callable[[int], tuple[int, float | None]]
    mark_open_orders_recovery_required: Callable[[str, int], int]
    portfolio_cash_qty_with_position_state: Callable[..., Any]
    runtime_strategy_set: Any | None = None
    runtime_strategy_set_manifest: dict[str, object] | None = None
    runtime_strategy_set_manifest_id: int | None = None
    runtime_strategy_set_manifest_hash: str | None = None
    runtime_dependency_manifest: RuntimeDependencyManifest | None = None
    runtime_dependency_manifest_id: int | None = None
    runtime_dependency_manifest_hash: str | None = None
    runtime_cycle_pipeline_factory: Callable[[Any], Any] | None = None

    @property
    def runner(self):
        from .runner import Runner

        return Runner(self)

    def with_run_start_manifests(
        self,
        *,
        runtime_strategy_set: Any,
        runtime_strategy_set_manifest: dict[str, object],
        runtime_strategy_set_manifest_id: int,
        runtime_strategy_set_manifest_hash: str,
        runtime_dependency_manifest: RuntimeDependencyManifest,
        runtime_dependency_manifest_id: int,
        runtime_dependency_manifest_hash: str,
    ) -> "RuntimeAppContainer":
        decision = replace(
            self.decision_coordinator,
            run_start_manifest_payload=runtime_strategy_set_manifest,
            run_start_manifest_id=runtime_strategy_set_manifest_id,
            run_start_manifest_hash=runtime_strategy_set_manifest_hash,
        )
        return replace(
            self,
            runtime_strategy_set=runtime_strategy_set,
            runtime_strategy_set_manifest=runtime_strategy_set_manifest,
            runtime_strategy_set_manifest_id=runtime_strategy_set_manifest_id,
            runtime_strategy_set_manifest_hash=runtime_strategy_set_manifest_hash,
            runtime_dependency_manifest=runtime_dependency_manifest,
            runtime_dependency_manifest_id=runtime_dependency_manifest_id,
            runtime_dependency_manifest_hash=runtime_dependency_manifest_hash,
            decision_coordinator=decision,
        )


def build_runtime_dependency_manifest(
    *,
    settings_obj: object,
    broker_factory: object,
    decision_gateway: object,
    execution_service_factory: object,
    notification_service: object,
    flatten_service: object,
    clock: object,
    scheduler: object,
    runtime_strategy_set_manifest_hash: str | None,
) -> RuntimeDependencyManifest:
    env_hash, env_source = _env_summary_hash()
    return RuntimeDependencyManifest(
        schema_version=1,
        source_revision=_source_revision(),
        settings_hash=_settings_hash(settings_obj),
        env_summary_hash=env_hash,
        env_file_source=env_source,
        mode=str(getattr(settings_obj, "MODE", "")),
        live_dry_run=bool(getattr(settings_obj, "LIVE_DRY_RUN", False)),
        live_real_order_armed=bool(getattr(settings_obj, "LIVE_REAL_ORDER_ARMED", False)),
        execution_engine=str(getattr(settings_obj, "EXECUTION_ENGINE", "lot_native") or "lot_native"),
        broker_factory_identity=_identity(broker_factory),
        private_api_submit_boundary_identity="bithumb_bot.execution_service.ExecutionSubmitPlan.as_final_payload",
        decision_gateway_identity=_identity(decision_gateway),
        execution_service_identity=_identity(execution_service_factory),
        notification_service_identity=_identity(notification_service),
        flatten_service_identity=_identity(flatten_service),
        clock_identity=_identity(clock),
        scheduler_identity=_identity(scheduler),
        compat_override_enabled=False,
        legacy_patch_points_enabled=False,
        runtime_strategy_set_manifest_hash=runtime_strategy_set_manifest_hash,
    )


def create_default_runtime_app(settings_obj=settings) -> RuntimeAppContainer:
    from ..recovery import cancel_open_orders_with_broker, reconcile_with_broker
    from ..runtime_decision_service import RuntimeDecisionGateway

    state_store = RuntimeStateStore(runtime_state.snapshot)

    def now_ms() -> int:
        return int(time.time() * 1000)

    def exposure_snapshot(now_ms_value: int) -> tuple[bool, bool]:
        open_count, _ = _get_open_order_snapshot(now_ms_value)
        conn = ensure_db()
        try:
            snapshot = compute_runtime_readiness_snapshot(conn)
        finally:
            conn.close()
        return (
            open_count > 0,
            snapshot.position_state.normalized_exposure.has_any_position_residue,
        )

    def startup_gate() -> str | None:
        return runtime_gate_api.startup_safety_gate()

    def resume_eligibility():
        return runtime_gate_api.resume_eligibility()

    def reconcile_clear_evidence(state) -> ReconcileClearEvidence:
        open_orders_present, position_present = exposure_snapshot(now_ms())
        dust_context = reconcile_dust_context(state.last_reconcile_metadata)
        return ReconcileClearEvidence(
            open_orders_present=open_orders_present,
            position_present=position_present,
            mismatch_count=reconcile_balance_split_mismatch_count(state.last_reconcile_metadata),
            dust_effective_flat=bool(dust_context["effective_flat"]),
        )

    recovery_controller = RecoveryController(
        state_snapshot=runtime_state.snapshot,
        refresh_open_order_health=runtime_state.refresh_open_order_health,
        startup_gate_evaluator=startup_gate,
        reconcile_clear_evidence=reconcile_clear_evidence,
        risk_state_clear_allowed=lambda *, state, startup_gate_reason: bool(
            StaleRiskStateMismatchHaltService(
                balance_split_mismatch_counter=reconcile_balance_split_mismatch_count,
            ).evaluate(state=state, startup_gate_reason=startup_gate_reason)["stale_halt_clear_allowed"]
        ),
        auto_recovery_allowed=lambda *, state, startup_gate_reason: _startup_gate_allows_process_auto_recovery(
            state=state,
            startup_gate_reason=startup_gate_reason,
        ),
        enable_trading=runtime_state.enable_trading,
        disable_trading_until=runtime_state.disable_trading_until,
        set_resume_gate=runtime_state.set_resume_gate,
    )

    def evaluate_clearance(clearance_type: str, *, startup_gate_reason: str | None = None):
        snapshot = runtime_state.snapshot()
        gate_reason = startup_gate_reason if startup_gate_reason is not None else startup_gate()
        return recovery_controller.evaluate_clearance(
            snapshot=snapshot,
            startup_gate_reason=gate_reason,
            clearance_type=clearance_type,
        )

    def initial_reconcile_clearance(*, startup_gate_reason: str | None = None):
        snapshot = runtime_state.snapshot()
        if (
            bool(getattr(snapshot, "halt_new_orders_blocked", False))
            and bool(getattr(snapshot, "halt_state_unresolved", False))
            and getattr(snapshot, "halt_reason_code", None) == "STARTUP_SAFETY_GATE"
        ):
            return evaluate_clearance(
                "startup_gate_auto_recovery_continue",
                startup_gate_reason=startup_gate_reason,
            )
        return evaluate_clearance("initial_reconcile", startup_gate_reason=startup_gate_reason)

    runtime_gate_api = RuntimeGateApi(
        initial_reconcile_halt_evaluator=initial_reconcile_clearance,
        live_execution_broker_halt_evaluator=lambda *, startup_gate_reason=None: evaluate_clearance(
            "live_execution_broker",
            startup_gate_reason=startup_gate_reason,
        ),
        risk_state_mismatch_halt_evaluator=lambda *, startup_gate_reason=None: evaluate_clearance(
            "risk_state_mismatch",
            startup_gate_reason=startup_gate_reason,
        ),
        exposure_snapshot=exposure_snapshot,
    )

    notification_service = _operator_notification_proxy(operator_notification_service())
    flatten_service = operator_flatten_service()
    scheduler = TimeScheduler()
    clock = time.time
    broker_factory = lambda: build_broker_with_auth_diagnostics(
        caller="run_loop",
        env_summary=get_last_explicit_env_load_summary().as_dict(),
        broker_factory=BithumbBroker,
    )
    decision_gateway_factory = RuntimeDecisionGateway
    decision_coordinator = DecisionCoordinator(
        settings_obj=settings_obj,
        db_factory=ensure_db,
        decision_gateway_factory=decision_gateway_factory,
        planner_factory=run_loop_execution_planner,
    )
    execution_engine_name = str(getattr(settings_obj, "EXECUTION_ENGINE", "lot_native") or "lot_native")
    execution_coordinator = ExecutionCoordinator(execution_engine_name)
    safety_controller = SafetyController(
        symbol=settings_obj.PAIR,
        state_snapshot=runtime_state.snapshot,
        enter_halt=state_store.enter_halt,
        resume_evaluator=resume_eligibility,
        latest_order_identifiers=_latest_order_identifiers,
        count_open_orders=_count_open_orders,
        position_summary=_position_summary,
        cancel_open_orders_with_broker=cancel_open_orders_with_broker,
        record_cancel_open_orders_result=runtime_state.record_cancel_open_orders_result,
        flatten_position=flatten_service.flatten_position,
        record_flatten_position_result=runtime_state.record_flatten_position_result,
        exposure_snapshot=exposure_snapshot,
        cleanup_revalidator=build_default_cleanup_revalidation_service().evaluate,
        now_ms=now_ms,
        live_dry_run=lambda: bool(settings_obj.LIVE_DRY_RUN),
        legacy_cancel_open_orders=None,
    )
    startup_controller = StartupController(
        symbol=settings_obj.PAIR,
        startup_gate_evaluator=startup_gate,
        state_snapshot=runtime_state.snapshot,
        latest_order_identifiers=_latest_order_identifiers,
        count_open_orders=_count_open_orders,
        position_summary=_position_summary,
        recommended_commands=recommended_operator_commands,
        auto_recovery_allowed=_startup_gate_allows_process_auto_recovery,
        broker_factory=broker_factory,
        initial_reconcile=reconcile_with_broker,
        halt_on_startup_failure=lambda *, reason_code, reason, unresolved: safety_controller.apply(
            safety_controller.evaluate_halt(
                HaltReason(reason_code, reason),
                unresolved=bool(unresolved),
            )
        ),
        enable_trading=runtime_state.enable_trading,
        set_resume_gate=runtime_state.set_resume_gate,
        recovery_clearance_evaluators=(
            lambda *, startup_gate_reason=None: initial_reconcile_clearance(
                startup_gate_reason=startup_gate_reason
            ),
            lambda *, startup_gate_reason=None: evaluate_clearance(
                "live_execution_broker",
                startup_gate_reason=startup_gate_reason,
            ),
            lambda *, startup_gate_reason=None: evaluate_clearance(
                "risk_state_mismatch",
                startup_gate_reason=startup_gate_reason,
            ),
        ),
        recovery_clearance_applier=recovery_controller.apply_clearance,
    )

    return RuntimeAppContainer(
        settings_obj=settings_obj,
        db_factory=ensure_db,
        clock=clock,
        scheduler=scheduler,
        broker_factory=broker_factory,
        market_sync=cmd_sync,
        candle_reader=select_latest_candle,
        closed_candle_selector=_select_latest_closed_candle,
        runtime_state_store=state_store,
        decision_coordinator=decision_coordinator,
        execution_coordinator=execution_coordinator,
        safety_controller=safety_controller,
        startup_controller=startup_controller,
        notification_service=notification_service,
        notification_adapter=NotificationAdapter(notification_service),
        artifact_sink=lambda **kwargs: RuntimeCycleArtifact(**kwargs),
        execution_service_factory=build_signal_execution_service,
        recovery_controller=recovery_controller,
        reconcile_with_broker=reconcile_with_broker,
        runtime_strategy_set_provider=active_runtime_strategy_set,
        runtime_strategy_set_manifest_provider=normalized_runtime_strategy_set_manifest,
        runtime_dependency_manifest_provider=build_runtime_dependency_manifest,
        runtime_gate_api=runtime_gate_api,
        validate_market_preflight=validate_market_preflight,
        validate_runtime_strategy_set_selection=validate_runtime_strategy_set_selection,
        validate_live_mode_preflight=validate_live_mode_preflight,
        validate_market_runtime=validate_market_runtime,
        market_validation_error_type=MarketPreflightValidationError,
        interval_parser=parse_interval_sec,
        execution_engine_name=execution_engine_name,
        live_executor=live_execute_signal,
        paper_executor=paper_execute,
        harmless_dust_recorder=record_harmless_dust_exit_suppression,
        open_order_snapshot=_get_open_order_snapshot,
        mark_open_orders_recovery_required=_mark_open_orders_recovery_required,
        portfolio_cash_qty_with_position_state=portfolio_cash_qty_with_position_state,
    )


def persist_run_start_manifests(container: RuntimeAppContainer, *, created_ts: int) -> RuntimeAppContainer:
    runtime_strategy_set = container.runtime_strategy_set_provider()
    strategy_manifest = container.runtime_strategy_set_manifest_provider(
        strategy_set=runtime_strategy_set,
        settings_obj=container.settings_obj,
    )
    conn = container.db_factory()
    try:
        strategy_refs = record_runtime_strategy_set_manifest(
            conn,
            strategy_set=runtime_strategy_set,
            manifest_payload=strategy_manifest,
            settings_obj=container.settings_obj,
            created_ts=created_ts,
        )
        strategy_hash = str(strategy_refs["runtime_strategy_set_manifest_hash"])
        dependency_manifest = container.runtime_dependency_manifest_provider(
            settings_obj=container.settings_obj,
            broker_factory=container.broker_factory,
            decision_gateway=container.decision_coordinator.decision_gateway_factory,
            execution_service_factory=container.execution_service_factory,
            notification_service=container.notification_service,
            flatten_service=operator_flatten_service,
            clock=container.clock,
            scheduler=container.scheduler,
            runtime_strategy_set_manifest_hash=strategy_hash,
        )
        dependency_refs = record_runtime_dependency_manifest(
            conn,
            manifest_payload=dependency_manifest.as_dict(),
            created_ts=created_ts,
        )
        conn.commit()
    finally:
        conn.close()
    return container.with_run_start_manifests(
        runtime_strategy_set=runtime_strategy_set,
        runtime_strategy_set_manifest=strategy_manifest,
        runtime_strategy_set_manifest_id=int(strategy_refs["runtime_strategy_set_manifest_id"]),
        runtime_strategy_set_manifest_hash=strategy_hash,
        runtime_dependency_manifest=dependency_manifest,
        runtime_dependency_manifest_id=int(dependency_refs["runtime_dependency_manifest_id"]),
        runtime_dependency_manifest_hash=str(dependency_refs["runtime_dependency_manifest_hash"]),
    )


def _operator_notification_proxy(service: Any) -> Any:
    class _NotificationProxy:
        def send_event(self, event_name: str, /, **fields: object) -> Any:
            return service.send_event(event_name, **fields)

        def send_message(self, message: str) -> Any:
            return service.send_message(message)

    return _NotificationProxy()


def _startup_gate_allows_process_auto_recovery(*, state: object, startup_gate_reason: str | None) -> bool:
    blocker_code, _ = classify_startup_gate_reason(startup_gate_reason, state=state)
    if blocker_code != "FEE_PENDING_AUTO_RECOVERING":
        return False
    try:
        metadata = json.loads(str(getattr(state, "last_reconcile_metadata", None) or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        metadata = {}
    conn = ensure_db()
    try:
        readiness_snapshot = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()
    return bool(
        getattr(state, "last_reconcile_status", None) == "ok"
        and (
            int(metadata.get("fee_pending_auto_recovering", 0) or 0) > 0
            or (
                readiness_snapshot.recovery_stage
                in {"UNAPPLIED_PRINCIPAL_PENDING", "FEE_FINALIZATION_PENDING"}
                and int(readiness_snapshot.auto_recovery_count or 0) > 0
            )
        )
        and int(readiness_snapshot.recovery_required_count or 0) == 0
    )


__all__ = [
    "RuntimeAppContainer",
    "Scheduler",
    "TimeScheduler",
    "build_runtime_dependency_manifest",
    "create_default_runtime_app",
    "persist_run_start_manifests",
]
