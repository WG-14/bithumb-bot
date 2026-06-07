from __future__ import annotations

import logging
import math
import time
import json
import os
import importlib
from dataclasses import replace
from functools import partial

from ..config import (
    MarketPreflightValidationError,
    settings,
    validate_live_mode_preflight,
    validate_market_preflight,
    validate_market_runtime,
    validate_runtime_strategy_set_selection,
)
from ..marketdata import cmd_sync
from ..runtime_decision_service import (
    compute_strategy_decision_snapshot,
    get_runtime_decision_adapter,
    legacy_db_strategy_fallback_allowed,
)
from ..runtime_strategy_set import (
    active_runtime_strategy_set,
    normalized_runtime_strategy_set_manifest,
)
from ..broker.bithumb import BithumbBroker, build_broker_with_auth_diagnostics
from ..db_core import ensure_db, record_runtime_strategy_set_manifest
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
from ..runtime_readiness import compute_runtime_readiness_snapshot
from ..runtime_recovery_gate import (
    ResumeBlocker,
    RuntimeRecoveryGateService,
    classify_startup_gate_reason,
    resume_blocker,
)
from ..runtime_recovery_services import StaleRiskStateMismatchHaltService
from ..runtime_gate_api import RuntimeGateApi
from ..runtime_resume_services import (
    RestartReadinessService,
    ResumeGuidance,
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
from ..utils_time import kst_str, parse_interval_sec
from ..observability import configure_runtime_logging, format_log_kv
from ..bootstrap import get_last_explicit_env_load_summary
from .. import runtime_state
from ..risk import RISK_STATE_MISMATCH
from ..run_loop_execution_planner import (
    load_previous_target_exposure_for_run_loop,
    prepare_strategy_decision_persistence_context as _planner_prepare_strategy_decision_persistence_context,
    resolve_target_position_state_for_run_loop,
    run_loop_uses_target_delta,
)
from ..runtime_service_factories import (
    operator_flatten_service,
    operator_notification_service,
    operator_repair_service,
    run_loop_execution_planner,
)
from .execution_coordinator import ExecutionCoordinator
from .cleanup_revalidation import build_default_cleanup_revalidation_service
from .decision_coordinator import (
    DecisionCoordinator,
    persist_target_position_state_for_run_loop,
)
from .execution_coordinator import authoritative_execution_signal_for_trade
from .lifecycle_artifacts import RuntimeCycleArtifact
from .runtime_checkpoint import RuntimeCheckpoint
from .state_store import RuntimeStateStore, pause_trading_until
from .notification_adapter import NotificationAdapter
from .operator_event_composer import (
    RuntimeOperatorEventComposer,
    recommended_operator_commands,
)
from .recovery_controller import (
    RecoveryController,
    ReconcileClearEvidence,
)
from .safety_controller import (
    HaltReason,
    SafetyController,
)
from .startup_controller import StartupController
from .app_container import RuntimeAppContainer, persist_run_start_manifests

FAILSAFE_RETRY_DELAY_SEC = 180
RUN_LOG = logging.getLogger("bithumb_bot.run")
_LEGACY_ENGINE_ATTEMPT_OPEN_ORDER_CANCELLATION = None
_ORIGINAL_OPERATOR_NOTIFICATION_SERVICE_FACTORY = operator_notification_service


def __getattr__(name: str) -> object:
    compat_request_builder = "build_signal_execution_" + "request"
    if name == compat_request_builder:
        from . import execution_coordinator as _execution_coordinator

        return getattr(_execution_coordinator, compat_request_builder)
    raise AttributeError(name)


class Runner:
    def __init__(self, container: RuntimeAppContainer):
        self.container = container
        self.fail_count = 0
        self.max_fails = 5
        self.last_open_order_reconcile_at: float | None = None
        self.last_market_runtime_check_at: float | None = None
        self.broker = None
        self.runtime_strategy_set = None
        self.execution_service = None
        self.runtime_checkpoint: RuntimeCheckpoint | None = None
        self.runtime_events: RuntimeOperatorEventComposer | None = None
        self._started = False
        self._blocked = False

    def _record_artifact(self, cycle_id: str, **kwargs: object) -> RuntimeCycleArtifact:
        artifact = self.container.artifact_sink(
            cycle_id=cycle_id,
            runtime_dependency_manifest_hash=self.container.runtime_dependency_manifest_hash,
            **kwargs,
        )
        RUN_LOG.info(format_log_kv("[RUN] runtime_cycle_artifact", **artifact.as_dict()))
        return artifact

    def _prepare_runtime_start(self) -> None:
        c = self.container
        configure_runtime_logging()
        if c.settings_obj.MODE != "live":
            c.validate_market_preflight(c.settings_obj)
        c.validate_runtime_strategy_set_selection(c.settings_obj)
        c.validate_live_mode_preflight(c.settings_obj)
        started = persist_run_start_manifests(c, created_ts=int(c.clock() * 1000))
        self.container = started
        c = self.container
        self.runtime_strategy_set = c.runtime_strategy_set
        startup_result = c.startup_controller.prepare_runtime_start(
            live_mode=c.settings_obj.MODE == "live"
        )
        startup_notification_hashes = list(startup_result.as_dict().get("operator_event_hashes", []))
        if startup_result.operator_event:
            c.notification_adapter.send_event(startup_result.operator_event)
        halt_transition = startup_result.as_dict().get("halt_transition", {})
        halt_evidence = halt_transition.get("evidence", {}) if isinstance(halt_transition, dict) else {}
        operator_event = halt_evidence.get("operator_event", {}) if isinstance(halt_evidence, dict) else {}
        if isinstance(operator_event, dict) and operator_event:
            c.notification_adapter.send_event(operator_event)
            if operator_event.get("event_hash"):
                startup_notification_hashes.append(str(operator_event.get("event_hash")))
        self._record_artifact(
            "startup",
            candle_ts=None,
            startup_state=startup_result.status,
            readiness_hash=startup_result.as_dict()["decision_hash"],
            notification_event_hashes=startup_notification_hashes,
            state_transition_hash=(
                startup_result.as_dict().get("halt_transition", {}).get("decision_hash")
                if isinstance(startup_result.as_dict().get("halt_transition"), dict)
                else None
            ),
        )
        if startup_result.status == "BLOCKED":
            _log_loop_event(
                logging.WARNING,
                "[RUN] startup_blocked",
                symbol=c.settings_obj.PAIR,
                interval=c.settings_obj.INTERVAL,
                reason=startup_result.startup_gate_reason or startup_result.reason_code or "startup blocked",
                startup_result_hash=startup_result.as_dict()["decision_hash"],
            )
            self._blocked = True
            self._started = True
            return
        self.broker = startup_result.broker
        self.runtime_checkpoint = RuntimeCheckpoint(symbol=c.settings_obj.PAIR, interval=c.settings_obj.INTERVAL)
        self.runtime_events = RuntimeOperatorEventComposer(c.settings_obj.PAIR)
        self.execution_service = c.execution_service_factory(
            mode=c.settings_obj.MODE,
            broker=self.broker,
            paper_executor=c.paper_executor,
            live_executor=c.live_executor,
            harmless_dust_recorder=c.harmless_dust_recorder,
        )
        _log_loop_event(
            logging.INFO,
            "[RUN] loop_start",
            symbol=c.settings_obj.PAIR,
            interval=c.settings_obj.INTERVAL,
            every_sec=c.interval_parser(c.settings_obj.INTERVAL),
            strategy=c.settings_obj.STRATEGY_NAME,
            runtime_strategy_set_source=getattr(self.runtime_strategy_set, "source", "-"),
            runtime_strategy_set_hash=c.runtime_strategy_set_manifest_hash,
            runtime_dependency_manifest_hash=c.runtime_dependency_manifest_hash,
        )
        self._started = True

    def run_forever(self) -> None:
        if not self._started:
            self._prepare_runtime_start()
        if self._blocked:
            return
        sec = self.container.interval_parser(self.container.settings_obj.INTERVAL)
        try:
            while True:
                tick_now = self.container.clock()
                self.container.scheduler.sleep(sec - (tick_now % sec) + 2)
                self.run_one_cycle()
        except KeyboardInterrupt:
            _log_loop_event(
                logging.INFO,
                "[RUN] stopped",
                symbol=self.container.settings_obj.PAIR,
                interval=self.container.settings_obj.INTERVAL,
                reason="stopped by user (Ctrl+C)",
            )

    def run_one_cycle(self) -> RuntimeCycleArtifact | None:
        if not self._started:
            self._prepare_runtime_start()
        if self._blocked:
            return None
        from .cycle_pipeline import RuntimeCyclePipeline

        factory = self.container.runtime_cycle_pipeline_factory
        pipeline = factory(self) if factory is not None else RuntimeCyclePipeline(self)
        return pipeline.run_once()


def _record_runtime_cycle_artifact(
    cycle_id: str,
    *,
    candle_ts: int | None = None,
    startup_state: str | None = None,
    readiness_hash: str | None = None,
    strategy_decision_hash: str | None = None,
    runtime_strategy_decision_bundle_id: int | None = None,
    runtime_strategy_decision_bundle_hash: str | None = None,
    portfolio_allocation_decision_id: int | None = None,
    portfolio_allocation_decision_hash: str | None = None,
    portfolio_target_id: int | None = None,
    portfolio_target_hash: str | None = None,
    strategy_contribution_hash: str | None = None,
    execution_plan_id: int | None = None,
    execution_plan_bundle_hash: str | None = None,
    execution_submit_plan_hash: str | None = None,
    strategy_virtual_lifecycle_transition_hashes: object = (),
    strategy_risk_decision_hash: str | None = None,
    strategy_risk_policy_hash: str | None = None,
    strategy_risk_input_hash: str | None = None,
    strategy_risk_evidence_hash: str | None = None,
    strategy_risk_state_source: str | None = None,
    strategy_risk_status: str | None = None,
    strategy_risk_reason_code: str | None = None,
    portfolio_risk_decision_hash: str | None = None,
    portfolio_risk_policy_hash: str | None = None,
    portfolio_risk_input_hash: str | None = None,
    portfolio_risk_evidence_hash: str | None = None,
    portfolio_risk_state_source: str | None = None,
    portfolio_risk_status: str | None = None,
    portfolio_risk_reason_code: str | None = None,
    pre_submit_risk_decision_hash: str | None = None,
    pre_submit_risk_policy_hash: str | None = None,
    pre_submit_risk_input_hash: str | None = None,
    pre_submit_risk_evidence_hash: str | None = None,
    pre_submit_risk_plan_hash: str | None = None,
    pre_submit_risk_state_source: str | None = None,
    pre_submit_risk_status: str | None = None,
    pre_submit_risk_reason_code: str | None = None,
    execution_result_hash: str | None = None,
    safety_decision_hash: str | None = None,
    recovery_decision_hash: str | None = None,
    state_transition_hash: str | None = None,
    notification_event_hashes: object = (),
) -> RuntimeCycleArtifact:
    hashes = (
        list(notification_event_hashes)
        if isinstance(notification_event_hashes, (list, tuple))
        else []
    )
    virtual_lifecycle_hashes = (
        list(strategy_virtual_lifecycle_transition_hashes)
        if isinstance(strategy_virtual_lifecycle_transition_hashes, (list, tuple))
        else []
    )
    artifact = RuntimeCycleArtifact(
        cycle_id=cycle_id,
        candle_ts=candle_ts,
        startup_state=startup_state,
        readiness_hash=readiness_hash,
        strategy_decision_hash=strategy_decision_hash,
        runtime_strategy_decision_bundle_id=runtime_strategy_decision_bundle_id,
        runtime_strategy_decision_bundle_hash=runtime_strategy_decision_bundle_hash,
        portfolio_allocation_decision_id=portfolio_allocation_decision_id,
        portfolio_allocation_decision_hash=portfolio_allocation_decision_hash,
        portfolio_target_id=portfolio_target_id,
        portfolio_target_hash=portfolio_target_hash,
        strategy_contribution_hash=strategy_contribution_hash,
        execution_plan_id=execution_plan_id,
        execution_plan_bundle_hash=execution_plan_bundle_hash,
        execution_submit_plan_hash=execution_submit_plan_hash,
        strategy_virtual_lifecycle_transition_hashes=[
            str(item) for item in virtual_lifecycle_hashes
        ],
        strategy_risk_decision_hash=strategy_risk_decision_hash,
        strategy_risk_policy_hash=strategy_risk_policy_hash,
        strategy_risk_input_hash=strategy_risk_input_hash,
        strategy_risk_evidence_hash=strategy_risk_evidence_hash,
        strategy_risk_state_source=strategy_risk_state_source,
        strategy_risk_status=strategy_risk_status,
        strategy_risk_reason_code=strategy_risk_reason_code,
        portfolio_risk_decision_hash=portfolio_risk_decision_hash,
        portfolio_risk_policy_hash=portfolio_risk_policy_hash,
        portfolio_risk_input_hash=portfolio_risk_input_hash,
        portfolio_risk_evidence_hash=portfolio_risk_evidence_hash,
        portfolio_risk_state_source=portfolio_risk_state_source,
        portfolio_risk_status=portfolio_risk_status,
        portfolio_risk_reason_code=portfolio_risk_reason_code,
        pre_submit_risk_decision_hash=pre_submit_risk_decision_hash,
        pre_submit_risk_policy_hash=pre_submit_risk_policy_hash,
        pre_submit_risk_input_hash=pre_submit_risk_input_hash,
        pre_submit_risk_evidence_hash=pre_submit_risk_evidence_hash,
        pre_submit_risk_plan_hash=pre_submit_risk_plan_hash,
        pre_submit_risk_state_source=pre_submit_risk_state_source,
        pre_submit_risk_status=pre_submit_risk_status,
        pre_submit_risk_reason_code=pre_submit_risk_reason_code,
        execution_result_hash=execution_result_hash,
        safety_decision_hash=safety_decision_hash,
        recovery_decision_hash=recovery_decision_hash,
        state_transition_hash=state_transition_hash,
        notification_event_hashes=[str(item) for item in hashes],
    )
    RUN_LOG.info(format_log_kv("[RUN] runtime_cycle_artifact", **artifact.as_dict()))
    return artifact


def _artifact_hash(value: object) -> str | None:
    content_hash = getattr(value, "content_hash", None)
    if callable(content_hash):
        return str(content_hash())
    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        payload = as_dict()
        if isinstance(payload, dict):
            decision_hash = payload.get("decision_hash")
            if decision_hash is not None:
                return str(decision_hash)
    if isinstance(value, dict):
        decision_hash = value.get("decision_hash") or value.get("event_hash")
        if decision_hash is not None:
            return str(decision_hash)
    return None


def _runtime_recovery_gate_service() -> RuntimeRecoveryGateService:
    return _runtime_gate_api().recovery_gate_service()


def _runtime_gate_api() -> RuntimeGateApi:
    return RuntimeGateApi(
        initial_reconcile_halt_evaluator=evaluate_initial_reconcile_halt_clearance,
        live_execution_broker_halt_evaluator=evaluate_live_execution_broker_halt_clearance,
        risk_state_mismatch_halt_evaluator=evaluate_risk_state_mismatch_halt_clearance,
        exposure_snapshot=_get_exposure_snapshot,
        logger=RUN_LOG,
    )


def _operator_repair_service():
    return operator_repair_service()


def _operator_notification_service():
    service = operator_notification_service()

    class _NotificationProxy:
        def send_event(self, event_name: str, /, **fields: object) -> None:
            service.send_event(event_name, **fields)

        def send_message(self, message: str) -> None:
            service.send_message(message)

    return _NotificationProxy()


def _operator_flatten_service():
    return operator_flatten_service()


def _notify_operator(message: str) -> None:
    importlib.import_module("bithumb_bot.notifier").notify(message)


def _flatten_position_compat(*args: object, **kwargs: object) -> dict[str, object]:
    return _operator_flatten_service().flatten_position(*args, **kwargs)


def _runtime_resume_service() -> RuntimeResumeService:
    return RuntimeResumeService(
        recovery_gate_factory=_runtime_recovery_gate_service,
        exposure_snapshot=_get_exposure_snapshot,
        ledger_external_cash_adjustment_summary=default_ledger_external_cash_adjustment_summary,
        repair_service=_operator_repair_service(),
    )


def _resume_guidance_service() -> ResumeGuidanceService:
    return ResumeGuidanceService(
        last_reconcile_fee_pending_recovery_required=last_reconcile_fee_pending_recovery_required,
    )


def _restart_readiness_service() -> RestartReadinessService:
    return RestartReadinessService(
        resume_evaluator=evaluate_resume_eligibility,
        repair_service=_operator_repair_service(),
    )


def _run_loop_uses_target_delta() -> bool:
    return run_loop_uses_target_delta()


def _persist_run_start_runtime_strategy_set_manifest(
    *,
    runtime_strategy_set: object,
    manifest_payload: dict[str, object],
    created_ts: int,
) -> dict[str, object]:
    conn = ensure_db()
    try:
        refs = record_runtime_strategy_set_manifest(
            conn,
            strategy_set=runtime_strategy_set,
            manifest_payload=manifest_payload,
            settings_obj=settings,
            created_ts=created_ts,
        )
        conn.commit()
        return refs
    finally:
        conn.close()


def _load_previous_target_exposure_for_run_loop(conn) -> float | None:
    return load_previous_target_exposure_for_run_loop(conn)


def _resolve_target_position_state_for_run_loop(
    conn,
    *,
    readiness_payload: dict[str, object],
    reference_price: float | None,
    raw_signal: str,
    updated_ts: int,
) -> dict[str, object]:
    return resolve_target_position_state_for_run_loop(
        conn,
        readiness_payload=readiness_payload,
        reference_price=reference_price,
        raw_signal=raw_signal,
        updated_ts=updated_ts,
    )


def _persist_target_position_state_for_run_loop(
    conn,
    *,
    execution_decision: dict[str, object],
    signal: str,
    decision_id: int | None,
    updated_ts: int,
) -> bool:
    return persist_target_position_state_for_run_loop(
        conn,
        execution_decision=execution_decision,
        signal=signal,
        decision_id=decision_id,
        updated_ts=int(updated_ts),
    )


def _evaluate_stale_risk_state_mismatch_halt(
    *,
    state,
    startup_gate_reason: str | None,
) -> dict[str, object]:
    return StaleRiskStateMismatchHaltService(
        balance_split_mismatch_counter=_reconcile_balance_split_mismatch_count,
    ).evaluate(state=state, startup_gate_reason=startup_gate_reason)


def get_stale_risk_state_mismatch_halt_diagnostics(
    *,
    startup_gate_reason: str | None = None,
) -> dict[str, object]:
    gate_reason = startup_gate_reason if startup_gate_reason is not None else evaluate_startup_safety_gate()
    return _evaluate_stale_risk_state_mismatch_halt(
        state=runtime_state.snapshot(),
        startup_gate_reason=gate_reason,
    )
def _promotion_grade_typed_runtime_decision_required(*, selected_strategy_name: str) -> bool:
    return True


def _typed_runtime_handoff_failure_reason(
    signal_handoff: object,
    *,
    selected_strategy_name: str,
) -> str | None:
    del signal_handoff
    if get_runtime_decision_adapter(selected_strategy_name) is None:
        return "runtime_decision_adapter_not_registered"
    return "typed_runtime_decision_required"


def _legacy_db_strategy_fallback_allowed(*, selected_strategy_name: str) -> bool:
    return legacy_db_strategy_fallback_allowed(selected_strategy_name=selected_strategy_name)


READINESS_CONTEXT_KEYS = (
    "residual_inventory_mode",
    "residual_inventory_state",
    "residual_inventory_policy_allows_run",
    "residual_inventory_policy_allows_buy",
    "residual_inventory_policy_allows_sell",
    "residual_inventory_qty",
    "residual_inventory_notional_krw",
    "residual_inventory_exchange_sellable",
    "total_effective_exposure_qty",
    "total_effective_exposure_notional_krw",
    "residual_sell_candidate",
    "unresolved_open_order_count",
    "submit_unknown_count",
    "target_policy_action",
    "target_origin",
    "target_adoption_reason",
    "target_adopted_broker_qty",
    "target_adopted_exposure_krw",
    "target_startup_policy_state",
    "target_existing_state_present",
    "target_missing_state_resolution",
    "target_closeout_requested",
    "target_strategy_signal_source",
)


def prepare_strategy_decision_persistence_context(
    *,
    decision_context: dict[str, object],
    execution_decision_summary: object,
    readiness_payload: dict[str, object],
    target_policy_metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    return _planner_prepare_strategy_decision_persistence_context(
        decision_context=decision_context,
        execution_decision_summary=execution_decision_summary,
        readiness_payload=readiness_payload,
        target_policy_metadata=target_policy_metadata,
    )


def _halt_reason(code: str, detail: str) -> HaltReason:
    return HaltReason(code=code, detail=detail)


def _resume_blocker(
    *,
    code: str,
    detail: str,
    overridable: bool,
    reason_code: str | None = None,
    summary: str | None = None,
    balance_delta_krw: float | None = None,
    recent_external_cash_adjustment_present: bool | None = None,
    recent_external_cash_adjustment_count: int | None = None,
) -> ResumeBlocker:
    return resume_blocker(
        code=code,
        detail=detail,
        reason_code=str(reason_code or code),
        summary=str(summary or detail),
        overridable=overridable,
        balance_delta_krw=balance_delta_krw,
        recent_external_cash_adjustment_present=recent_external_cash_adjustment_present,
        recent_external_cash_adjustment_count=recent_external_cash_adjustment_count,
    )


def _classify_startup_gate_reason(startup_gate_reason: str | None, *, state) -> tuple[str, str]:
    return classify_startup_gate_reason(startup_gate_reason, state=state)


def _classify_dust_resume_blocker(dust_context: dict[str, object]) -> tuple[str, str]:
    return classify_dust_resume_blocker(dust_context)


def _extract_balance_split_delta_krw(summary: str) -> float | None:
    return extract_balance_split_delta_krw(summary)


def _ledger_external_cash_adjustment_summary() -> dict[str, object] | None:
    return default_ledger_external_cash_adjustment_summary()


def _classify_balance_split_blocker(metadata: dict[str, object]) -> ResumeBlocker | None:
    return classify_balance_split_blocker(metadata)


def _classify_fee_gap_recovery_blocker(metadata: dict[str, object]) -> ResumeBlocker | None:
    return _runtime_resume_service().classify_fee_gap_recovery_blocker(metadata)


def _last_reconcile_fee_pending_recovery_required() -> bool:
    return last_reconcile_fee_pending_recovery_required()


def _reconcile_balance_split_mismatch_count(metadata_raw: str | None) -> int:
    return reconcile_balance_split_mismatch_count(metadata_raw)


def _reconcile_dust_context(metadata_raw: str | None) -> dict[str, object]:
    return reconcile_dust_context(metadata_raw)


def _dust_residual_resume_blocker(dust_context: dict[str, object]) -> tuple[str, str] | None:
    return dust_residual_resume_blocker(dust_context)


def _log_loop_event(level: int, prefix: str, /, **fields: object) -> None:
    RUN_LOG.log(level, format_log_kv(prefix, mode=settings.MODE, **fields))


def _close_guard_ms(interval_sec: int) -> int:
    interval_ms = max(1, int(interval_sec)) * 1000
    return max(2_000, min(30_000, interval_ms // 20))


def _candle_close_ts_ms(*, candle_start_ts_ms: int, interval_sec: int) -> int:
    interval_ms = max(1, int(interval_sec)) * 1000
    return int(candle_start_ts_ms) + interval_ms


def _is_closed_candle(*, candle_ts_ms: int, now_ms: int, interval_sec: int) -> bool:
    """
    Return True when candle identified by DB key `candles.ts` is safely closed.

    `candles.ts` is the candle bucket start timestamp (UTC epoch ms), not the
    exchange payload's per-trade snapshot timestamp. Closedness is therefore
    judged from candle-start + interval (+ guard), not from raw payload
    `timestamp`.
    """
    interval_ms = max(1, int(interval_sec)) * 1000
    close_ready_ts_ms = _candle_close_ts_ms(
        candle_start_ts_ms=candle_ts_ms,
        interval_sec=interval_sec,
    ) + _close_guard_ms(interval_sec)
    return now_ms >= close_ready_ts_ms


_select_latest_closed_candle = partial(
    _runtime_select_latest_closed_candle,
    is_closed_candle=_is_closed_candle,
)


def _get_exposure_snapshot(now_ms: int) -> tuple[bool, bool]:
    open_count, _ = _get_open_order_snapshot(now_ms)
    conn = ensure_db()
    try:
        try:
            snapshot = compute_runtime_readiness_snapshot(conn)
        except Exception as exc:
            # Halt-clearing checks must fail closed when lot-native exposure
            # cannot be reconstructed from local state.
            RUN_LOG.warning(
                format_log_kv(
                    "[STARTUP_GATE] exposure snapshot unavailable",
                    reason=f"{type(exc).__name__}: {exc}",
                    open_orders_present=1 if open_count > 0 else 0,
                )
            )
            return open_count > 0, True
    finally:
        conn.close()
    return open_count > 0, snapshot.position_state.normalized_exposure.has_any_position_residue


def _can_clear_reconcile_failure_halt(*, state, startup_gate_reason: str | None) -> bool:
    clearance = _recovery_controller().evaluate_clearance(
        snapshot=state,
        startup_gate_reason=startup_gate_reason,
        clearance_type="initial_reconcile",
    )
    return bool(clearance.allowed)


def _startup_gate_allows_process_auto_recovery(*, state, startup_gate_reason: str | None) -> bool:
    blocker_code, _ = _classify_startup_gate_reason(startup_gate_reason, state=state)
    if blocker_code != "FEE_PENDING_AUTO_RECOVERING":
        return False
    try:
        reconcile_metadata = json.loads(str(getattr(state, "last_reconcile_metadata", None) or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        reconcile_metadata = {}
    conn = ensure_db()
    try:
        readiness_snapshot = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()
    return bool(
        state.last_reconcile_status == "ok"
        and (
            int(reconcile_metadata.get("fee_pending_auto_recovering", 0) or 0) > 0
            or (
                readiness_snapshot.recovery_stage
                in {"UNAPPLIED_PRINCIPAL_PENDING", "FEE_FINALIZATION_PENDING"}
                and int(readiness_snapshot.auto_recovery_count or 0) > 0
            )
        )
        and int(readiness_snapshot.recovery_required_count or 0) == 0
    )


def _reconcile_clear_evidence(state) -> ReconcileClearEvidence:
    open_orders_present, position_present = _get_exposure_snapshot(int(time.time() * 1000))
    dust_context = _reconcile_dust_context(state.last_reconcile_metadata)
    return ReconcileClearEvidence(
        open_orders_present=open_orders_present,
        position_present=position_present,
        mismatch_count=_reconcile_balance_split_mismatch_count(state.last_reconcile_metadata),
        dust_effective_flat=bool(dust_context["effective_flat"]),
    )


def _recovery_controller() -> RecoveryController:
    return RecoveryController(
        state_snapshot=runtime_state.snapshot,
        refresh_open_order_health=runtime_state.refresh_open_order_health,
        startup_gate_evaluator=evaluate_startup_safety_gate,
        reconcile_clear_evidence=_reconcile_clear_evidence,
        risk_state_clear_allowed=_can_clear_stale_risk_state_mismatch_halt,
        auto_recovery_allowed=_startup_gate_allows_process_auto_recovery,
        enable_trading=runtime_state.enable_trading,
        disable_trading_until=runtime_state.disable_trading_until,
        set_resume_gate=runtime_state.set_resume_gate,
    )


def _evaluate_recovery_clearance(*, clearance_type: str, startup_gate_reason: str | None = None):
    snapshot = runtime_state.snapshot()
    gate_reason = startup_gate_reason if startup_gate_reason is not None else evaluate_startup_safety_gate()
    return _recovery_controller().evaluate_clearance(
        snapshot=snapshot,
        startup_gate_reason=gate_reason,
        clearance_type=clearance_type,
    )


def evaluate_initial_reconcile_halt_clearance(*, startup_gate_reason: str | None = None):
    snapshot = runtime_state.snapshot()
    if (
        bool(getattr(snapshot, "halt_new_orders_blocked", False))
        and bool(getattr(snapshot, "halt_state_unresolved", False))
        and getattr(snapshot, "halt_reason_code", None) == "STARTUP_SAFETY_GATE"
    ):
        return _evaluate_recovery_clearance(
            clearance_type="startup_gate_auto_recovery_continue",
            startup_gate_reason=startup_gate_reason,
        )
    return _evaluate_recovery_clearance(
        clearance_type="initial_reconcile",
        startup_gate_reason=startup_gate_reason,
    )


def evaluate_live_execution_broker_halt_clearance(*, startup_gate_reason: str | None = None):
    return _evaluate_recovery_clearance(
        clearance_type="live_execution_broker",
        startup_gate_reason=startup_gate_reason,
    )


def evaluate_risk_state_mismatch_halt_clearance(*, startup_gate_reason: str | None = None):
    return _evaluate_recovery_clearance(
        clearance_type="risk_state_mismatch",
        startup_gate_reason=startup_gate_reason,
    )


def _evaluate_initial_reconcile_halt_clearance_after_observation_refresh(*, startup_gate_reason: str | None = None):
    runtime_state.refresh_open_order_health()
    return evaluate_initial_reconcile_halt_clearance(startup_gate_reason=startup_gate_reason)


def _evaluate_live_execution_broker_halt_clearance_after_observation_refresh(
    *,
    startup_gate_reason: str | None = None,
):
    runtime_state.refresh_open_order_health()
    return evaluate_live_execution_broker_halt_clearance(startup_gate_reason=startup_gate_reason)


def _evaluate_risk_state_mismatch_halt_clearance_after_observation_refresh(
    *,
    startup_gate_reason: str | None = None,
):
    runtime_state.refresh_open_order_health()
    return evaluate_risk_state_mismatch_halt_clearance(startup_gate_reason=startup_gate_reason)


def maybe_clear_stale_initial_reconcile_halt() -> bool:
    runtime_state.refresh_open_order_health()
    state = runtime_state.snapshot()
    try:
        metadata = json.loads(str(state.last_reconcile_metadata or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        metadata = {}
    fee_pending_auto_recovering = int(metadata.get("fee_pending_auto_recovering", 0) or 0) > 0
    if (
        int(getattr(state, "unresolved_open_order_count", 0) or 0) > 0
        or int(getattr(state, "recovery_required_count", 0) or 0) > 0
    ) and not (
        state.halt_reason_code == "STARTUP_SAFETY_GATE"
        and fee_pending_auto_recovering
    ):
        return False
    clearance = evaluate_initial_reconcile_halt_clearance()
    if not clearance.allowed:
        return False
    transition = _recovery_controller().apply_clearance(clearance)
    _record_runtime_cycle_artifact(
        "recovery:initial_reconcile_clear",
        startup_state="RECOVERY",
        recovery_decision_hash=clearance.as_dict()["decision_hash"],
        state_transition_hash=transition.as_dict()["decision_hash"],
        notification_event_hashes=clearance.as_dict().get("operator_event_hashes", []),
    )
    event_name = (
        "[RUN] startup_gate_auto_recovery_continue"
        if state.halt_reason_code == "STARTUP_SAFETY_GATE"
        else "[RUN] stale_reconcile_failure_halt_cleared"
    )
    _log_loop_event(
        logging.INFO,
        event_name,
        halt_reason_code=state.halt_reason_code or "-",
        reconcile_reason_code=state.last_reconcile_reason_code or "-",
        recovery_clearance_hash=clearance.as_dict()["decision_hash"],
    )
    return True


def maybe_clear_stale_live_execution_broker_halt(*, startup_gate_reason: str | None = None) -> bool:
    runtime_state.refresh_open_order_health()
    state = runtime_state.snapshot()
    clearance = evaluate_live_execution_broker_halt_clearance(
        startup_gate_reason=startup_gate_reason,
    )
    if not clearance.allowed:
        return False
    transition = _recovery_controller().apply_clearance(clearance)
    _record_runtime_cycle_artifact(
        "recovery:live_execution_broker_clear",
        startup_state="RECOVERY",
        recovery_decision_hash=clearance.as_dict()["decision_hash"],
        state_transition_hash=transition.as_dict()["decision_hash"],
        notification_event_hashes=clearance.as_dict().get("operator_event_hashes", []),
    )
    _log_loop_event(
        logging.INFO,
        "[RUN] stale_live_execution_broker_halt_cleared",
        halt_reason_code=state.halt_reason_code or "-",
        reconcile_reason_code=state.last_reconcile_reason_code or "-",
        recovery_clearance_hash=clearance.as_dict()["decision_hash"],
    )
    return True


def _can_clear_stale_risk_state_mismatch_halt(*, state, startup_gate_reason: str | None) -> bool:
    diagnostics = _evaluate_stale_risk_state_mismatch_halt(
        state=state,
        startup_gate_reason=startup_gate_reason,
    )
    return bool(diagnostics["stale_halt_clear_allowed"])


def maybe_clear_stale_risk_state_mismatch_halt(*, startup_gate_reason: str | None = None) -> bool:
    runtime_state.refresh_open_order_health()
    state = runtime_state.snapshot()
    clearance = evaluate_risk_state_mismatch_halt_clearance(
        startup_gate_reason=startup_gate_reason,
    )
    if not clearance.allowed:
        return False
    transition = _recovery_controller().apply_clearance(clearance)
    _record_runtime_cycle_artifact(
        "recovery:risk_state_mismatch_clear",
        startup_state="RECOVERY",
        recovery_decision_hash=clearance.as_dict()["decision_hash"],
        state_transition_hash=transition.as_dict()["decision_hash"],
        notification_event_hashes=clearance.as_dict().get("operator_event_hashes", []),
    )
    _log_loop_event(
        logging.INFO,
        "[RUN] stale_risk_state_mismatch_halt_cleared",
        halt_reason_code=state.halt_reason_code or "-",
        reconcile_reason_code=state.last_reconcile_reason_code or "-",
        recovery_clearance_hash=clearance.as_dict()["decision_hash"],
    )
    return True

def get_health_status() -> dict[str, float | int | bool | str | None]:
    maybe_clear_stale_initial_reconcile_halt()
    startup_gate_reason = evaluate_startup_safety_gate()
    maybe_clear_stale_live_execution_broker_halt(startup_gate_reason=startup_gate_reason)
    maybe_clear_stale_risk_state_mismatch_halt(startup_gate_reason=startup_gate_reason)
    state = runtime_state.snapshot()
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
    }



def evaluate_startup_safety_gate() -> str | None:
    return _runtime_gate_api().startup_safety_gate()


def evaluate_resume_eligibility() -> tuple[bool, list[ResumeBlocker]]:
    """Returns whether operator resume may proceed and structured blockers."""
    maybe_clear_stale_initial_reconcile_halt()
    startup_gate_reason = evaluate_startup_safety_gate()
    maybe_clear_stale_live_execution_broker_halt(startup_gate_reason=startup_gate_reason)
    maybe_clear_stale_risk_state_mismatch_halt(startup_gate_reason=startup_gate_reason)
    return _runtime_gate_api().resume_eligibility()

def build_resume_guidance(
    *,
    resume_allowed: bool,
    blockers: list[ResumeBlocker],
    unresolved_count: int,
    recovery_required_count: int,
    submit_unknown_count: int,
) -> ResumeGuidance:
    return _resume_guidance_service().build_resume_guidance(
        resume_allowed=resume_allowed,
        blockers=blockers,
        unresolved_count=unresolved_count,
        recovery_required_count=recovery_required_count,
        submit_unknown_count=submit_unknown_count,
    )

def evaluate_restart_readiness() -> list[tuple[str, bool, str]]:
    return _runtime_gate_api().restart_readiness()

def _safety_controller() -> SafetyController:
    from ..recovery import cancel_open_orders_with_broker
    state_store = RuntimeStateStore(runtime_state.snapshot)
    legacy_cancel = _LEGACY_ENGINE_ATTEMPT_OPEN_ORDER_CANCELLATION

    return SafetyController(
        symbol=settings.PAIR,
        state_snapshot=runtime_state.snapshot,
        enter_halt=state_store.enter_halt,
        resume_evaluator=evaluate_resume_eligibility,
        latest_order_identifiers=_latest_order_identifiers,
        count_open_orders=_count_open_orders,
        position_summary=_position_summary,
        cancel_open_orders_with_broker=cancel_open_orders_with_broker,
        record_cancel_open_orders_result=runtime_state.record_cancel_open_orders_result,
        flatten_position=_flatten_position_compat,
        record_flatten_position_result=runtime_state.record_flatten_position_result,
        exposure_snapshot=_get_exposure_snapshot,
        cleanup_revalidator=build_default_cleanup_revalidation_service().evaluate,
        now_ms=lambda: int(time.time() * 1000),
        live_dry_run=lambda: bool(settings.LIVE_DRY_RUN),
        legacy_cancel_open_orders=legacy_cancel,
    )


def _startup_controller() -> StartupController:
    recovery_controller = _recovery_controller()
    return StartupController(
        symbol=settings.PAIR,
        startup_gate_evaluator=evaluate_startup_safety_gate,
        state_snapshot=runtime_state.snapshot,
        latest_order_identifiers=_latest_order_identifiers,
        count_open_orders=_count_open_orders,
        position_summary=_position_summary,
        recommended_commands=_recommended_operator_commands,
        auto_recovery_allowed=_startup_gate_allows_process_auto_recovery,
        broker_factory=lambda: build_broker_with_auth_diagnostics(
            caller="run_loop",
            env_summary=get_last_explicit_env_load_summary().as_dict(),
            broker_factory=BithumbBroker,
        ),
        initial_reconcile=lambda broker: importlib.import_module("bithumb_bot.recovery").reconcile_with_broker(broker),
        halt_on_startup_failure=lambda *, reason_code, reason, unresolved: _safety_controller().apply(
            _safety_controller().evaluate_halt(
                _halt_reason(reason_code, reason),
                unresolved=bool(unresolved),
            )
        ),
        enable_trading=runtime_state.enable_trading,
        set_resume_gate=runtime_state.set_resume_gate,
        recovery_clearance_evaluators=(
            _evaluate_initial_reconcile_halt_clearance_after_observation_refresh,
            _evaluate_live_execution_broker_halt_clearance_after_observation_refresh,
            _evaluate_risk_state_mismatch_halt_clearance_after_observation_refresh,
        ),
        recovery_clearance_applier=recovery_controller.apply_clearance,
    )


def _halt_trading(reason: HaltReason, *, unresolved: bool = False, attempt_flatten: bool = False) -> None:
    safety_controller = _safety_controller()
    decision = safety_controller.evaluate_halt(
        reason,
        unresolved=unresolved,
        attempt_flatten=attempt_flatten,
    )
    transition = safety_controller.apply(decision)
    decision = type(decision)(
        action=decision.action,
        reason_code=decision.reason_code,
        reason=decision.reason,
        unresolved=decision.unresolved,
        attempt_flatten=decision.attempt_flatten,
        state_transition=transition,
        operator_event=decision.operator_event,
        input_hash=decision.input_hash,
        evidence_hash=decision.evidence_hash,
        decision_hash=decision.decision_hash,
        evidence=decision.evidence,
    )
    _dispatch_safety_decision(NotificationAdapter(_operator_notification_service()), decision)


def _dispatch_safety_decision(notification_adapter: NotificationAdapter, decision) -> None:
    if decision.operator_event:
        notification_adapter.send_event(decision.operator_event)


def _dispatch_runtime_safety_notifications(
    notification_adapter: NotificationAdapter,
    result,
) -> None:
    if result.safety_decision is not None:
        _dispatch_safety_decision(notification_adapter, result.safety_decision)
    safety_event_hashes = set()
    if result.safety_decision is not None:
        safety_event_hashes.update(result.safety_decision.as_dict().get("operator_event_hashes", []))
    for event in result.notification_events:
        if str(event.get("event_hash")) in safety_event_hashes:
            continue
        notification_adapter.send_event(event)
    for message in result.notification_messages:
        notification_adapter.send_message(message)


def _apply_runtime_safety_decision(safety_controller: SafetyController, result):
    if result.safety_decision is None:
        return result
    transition = safety_controller.apply(result.safety_decision)
    decision = result.safety_decision
    applied_decision = type(decision)(
        action=decision.action,
        reason_code=decision.reason_code,
        reason=decision.reason,
        unresolved=decision.unresolved,
        attempt_flatten=decision.attempt_flatten,
        state_transition=transition,
        operator_event=decision.operator_event,
        input_hash=decision.input_hash,
        evidence_hash=decision.evidence_hash,
        decision_hash=decision.decision_hash,
        evidence=decision.evidence,
    )
    transition_hash = transition.as_dict().get("decision_hash")
    return replace(
        result,
        safety_decision=applied_decision,
        state_transition_hash=str(transition_hash) if transition_hash else None,
    )


def _recommended_operator_commands(
    *,
    reason_code: str,
    startup_gate: bool,
    recovery_required: bool,
    unresolved_count: int,
) -> list[str]:
    return recommended_operator_commands(
        reason_code=reason_code,
        startup_gate=startup_gate,
        recovery_required=recovery_required,
        unresolved_count=unresolved_count,
    )


def _attempt_open_order_cancellation(broker: BithumbBroker, trigger: str) -> bool:
    events: list[dict[str, object]] = []
    messages: list[str] = []
    ok = _safety_controller().attempt_open_order_cancellation(
        broker,
        trigger=trigger,
        notification_events=events,
        notification_messages=messages,
    )
    adapter = NotificationAdapter(_operator_notification_service())
    for event in events:
        adapter.send_event(event)
    for message in messages:
        adapter.send_message(message)
    return ok


_ORIGINAL_ATTEMPT_OPEN_ORDER_CANCELLATION = _attempt_open_order_cancellation


def _attempt_cleanup_with_optional_flatten(
    broker: BithumbBroker,
    *,
    reason_code: str,
    reason_detail: str,
    cancel_trigger: str,
    flatten_trigger: str,
    attempt_flatten: bool,
) -> tuple[HaltReason, bool, bool]:
    result = _safety_controller().attempt_cleanup_with_optional_flatten(
        broker,
        reason_code=reason_code,
        reason_detail=reason_detail,
        cancel_trigger=cancel_trigger,
        flatten_trigger=flatten_trigger,
        attempt_flatten=attempt_flatten,
    )
    return result.halt_reason, result.canceled_ok, result.unresolved


def _attempt_risk_breach_flatten(
    broker: BithumbBroker,
    *,
    reason_code: str,
    reason_detail: str,
    cancel_trigger: str,
    flatten_trigger: str,
) -> tuple[HaltReason, bool, bool]:
    return _attempt_cleanup_with_optional_flatten(
        broker,
        reason_code=reason_code,
        reason_detail=reason_detail,
        cancel_trigger=cancel_trigger,
        flatten_trigger=flatten_trigger,
        attempt_flatten=True,
    )


def perform_panic_stop_cleanup(
    broker: BithumbBroker,
    *,
    reason_code: str,
    reason_detail: str,
    cancel_trigger: str,
    flatten_trigger: str,
    attempt_flatten: bool,
) -> tuple[HaltReason, bool, bool]:
    return _attempt_cleanup_with_optional_flatten(
        broker,
        reason_code=reason_code,
        reason_detail=reason_detail,
        cancel_trigger=cancel_trigger,
        flatten_trigger=flatten_trigger,
        attempt_flatten=attempt_flatten,
    )


def run_loop() -> None:
    global _LEGACY_ENGINE_ATTEMPT_OPEN_ORDER_CANCELLATION
    global operator_notification_service
    configure_runtime_logging()
    if settings.MODE != "live":
        try:
            validate_market_preflight(settings)
        except MarketPreflightValidationError as exc:
            _log_loop_event(
                logging.ERROR,
                "[RUN] startup_blocked",
                symbol=settings.PAIR,
                interval=settings.INTERVAL,
                reason=f"market preflight failed: {exc}",
            )
            raise
    validate_runtime_strategy_set_selection(settings)
    validate_live_mode_preflight(settings)
    runtime_strategy_set = active_runtime_strategy_set()
    runtime_strategy_set_manifest = normalized_runtime_strategy_set_manifest(
        strategy_set=runtime_strategy_set,
        settings_obj=settings,
    )
    run_start_manifest_refs = _persist_run_start_runtime_strategy_set_manifest(
        runtime_strategy_set=runtime_strategy_set,
        manifest_payload=runtime_strategy_set_manifest,
        created_ts=int(time.time() * 1000),
    )
    runtime_strategy_set_hash = str(run_start_manifest_refs["runtime_strategy_set_manifest_hash"])

    startup_result = _startup_controller().prepare_runtime_start(
        live_mode=settings.MODE == "live"
    )
    startup_notification_adapter = NotificationAdapter(_operator_notification_service())
    startup_notification_hashes = list(startup_result.as_dict().get("operator_event_hashes", []))
    if startup_result.operator_event:
        startup_notification_adapter.send_event(startup_result.operator_event)
        if startup_result.status == "BLOCKED":
            startup_snapshot = runtime_state.snapshot()
            open_order_count = _count_open_orders()
            unresolved_count = int(getattr(startup_snapshot, "unresolved_open_order_count", 0) or 0)
            recovery_required_count = int(getattr(startup_snapshot, "recovery_required_count", 0) or 0)
            operator_visible_order_count = max(
                int(open_order_count),
                int(unresolved_count),
                int(recovery_required_count),
            )
            startup_notification_adapter.send_event(
                RuntimeOperatorEventComposer(settings.PAIR).composer.trading_halted_event(
                    reason_code=str(startup_result.reason_code or "STARTUP_SAFETY_GATE"),
                    reason=str(startup_result.startup_gate_reason or "startup blocked"),
                    unresolved=True,
                    operator_action_required=True,
                    open_orders_present=operator_visible_order_count > 0,
                    open_order_count=operator_visible_order_count,
                    unresolved_order_count=max(unresolved_count, recovery_required_count),
                    position_summary=_position_summary(),
                    recommended_commands=recommended_operator_commands(
                        reason_code=str(startup_result.reason_code or "STARTUP_SAFETY_GATE"),
                        startup_gate=True,
                        recovery_required=True,
                        unresolved_count=max(unresolved_count, recovery_required_count),
                    ),
                )
            )
    else:
        halt_transition = startup_result.as_dict().get("halt_transition", {})
        halt_evidence = halt_transition.get("evidence", {}) if isinstance(halt_transition, dict) else {}
        operator_event = halt_evidence.get("operator_event", {}) if isinstance(halt_evidence, dict) else {}
        if isinstance(operator_event, dict) and operator_event:
            startup_notification_adapter.send_event(operator_event)
            if operator_event.get("event_hash"):
                startup_notification_hashes.append(str(operator_event.get("event_hash")))
    _record_runtime_cycle_artifact(
        "startup",
        startup_state=startup_result.status,
        readiness_hash=startup_result.as_dict()["decision_hash"],
        notification_event_hashes=startup_notification_hashes,
        state_transition_hash=(
            startup_result.as_dict().get("halt_transition", {}).get("decision_hash")
            if isinstance(startup_result.as_dict().get("halt_transition"), dict)
            else None
        ),
    )
    startup_clearances = startup_result.evidence.get("clearance_artifacts")
    startup_transitions = startup_result.evidence.get("transition_artifacts")
    if isinstance(startup_clearances, list):
        transition_items = startup_transitions if isinstance(startup_transitions, list) else []
        for idx, clearance_payload in enumerate(startup_clearances):
            if not isinstance(clearance_payload, dict) or not bool(clearance_payload.get("allowed")):
                continue
            transition_payload = transition_items[idx] if idx < len(transition_items) else {}
            _record_runtime_cycle_artifact(
                "startup:recovery_clear",
                startup_state=startup_result.status,
                readiness_hash=startup_result.as_dict()["decision_hash"],
                recovery_decision_hash=str(clearance_payload.get("decision_hash") or ""),
                state_transition_hash=(
                    str(transition_payload.get("decision_hash"))
                    if isinstance(transition_payload, dict) and transition_payload.get("decision_hash")
                    else None
                ),
                notification_event_hashes=clearance_payload.get("operator_event_hashes", []),
            )
    if startup_result.status == "BLOCKED":
        _log_loop_event(
            logging.WARNING,
            "[RUN] startup_blocked",
            symbol=settings.PAIR,
            interval=settings.INTERVAL,
            reason=startup_result.startup_gate_reason or startup_result.reason_code or "startup blocked",
            startup_result_hash=startup_result.as_dict()["decision_hash"],
        )
        return
    if startup_result.status == "DEGRADED_RECOVERY_CONTINUE":
        _log_loop_event(
            logging.WARNING,
            "[RUN] startup_gate_degraded_continue",
            reason=startup_result.startup_gate_reason or "-",
            recovery_stage="FEE_AUTO_RECOVERY_DEGRADED",
            startup_result_hash=startup_result.as_dict()["decision_hash"],
        )
    broker = startup_result.broker
    from ..recovery import reconcile_with_broker
    notification_adapter = NotificationAdapter(_operator_notification_service())
    runtime_events = RuntimeOperatorEventComposer(settings.PAIR)
    runtime_checkpoint = RuntimeCheckpoint(symbol=settings.PAIR, interval=settings.INTERVAL)
    decision_coordinator = DecisionCoordinator(
        run_start_manifest_payload=runtime_strategy_set_manifest,
        run_start_manifest_id=int(run_start_manifest_refs["runtime_strategy_set_manifest_id"]),
        run_start_manifest_hash=runtime_strategy_set_hash,
    )
    execution_coordinator = ExecutionCoordinator(
        str(getattr(settings, "EXECUTION_ENGINE", "lot_native") or "lot_native")
    )
    safety_controller = _safety_controller()

    sec = parse_interval_sec(settings.INTERVAL)
    _log_loop_event(
        logging.INFO,
        "[RUN] loop_start",
        symbol=settings.PAIR,
        interval=settings.INTERVAL,
        every_sec=sec,
        strategy=settings.STRATEGY_NAME,
        strategy_source=(
            "env:STRATEGY_NAME"
            if os.getenv("STRATEGY_NAME") not in (None, "")
            else "explicit_legacy_default_compat"
            if os.getenv("LEGACY_DEFAULT_STRATEGY_COMPAT") not in (None, "")
            else "missing_strategy_name"
        ),
        runtime_strategy_set_source=runtime_strategy_set.source,
        runtime_strategy_set_hash=runtime_strategy_set_hash,
        active_strategy_count=len(runtime_strategy_set.active_strategies),
        runtime_strategy_set_manifest=json.dumps(runtime_strategy_set_manifest, sort_keys=True),
    )
    _log_loop_event(logging.INFO, "[RUN] operator_hint", action="Ctrl+C to stop")
    fail_count = 0
    MAX_FAILS = 5
    last_open_order_reconcile_at: float | None = None
    last_market_runtime_check_at: float | None = None

    from .app_container import create_default_runtime_app

    container = create_default_runtime_app(settings)
    execution_service = container.execution_service_factory(
        mode=settings.MODE,
        broker=broker,
        paper_executor=container.paper_executor,
        live_executor=container.live_executor,
        harmless_dust_recorder=container.harmless_dust_recorder,
    )

    try:
        while True:
            tick_now = time.time()
            sleep_s = sec - (tick_now % sec) + 2
            time.sleep(sleep_s)
            now = time.time()

            state = runtime_state.snapshot()
            if (not state.trading_enabled) and state.retry_at_epoch_sec:
                if math.isinf(state.retry_at_epoch_sec):
                    _log_loop_event(
                        logging.WARNING,
                        "[RUN] halted_exit",
                        symbol=settings.PAIR,
                        interval=settings.INTERVAL,
                        reason="trading halted indefinitely",
                    )
                    return
                if now < state.retry_at_epoch_sec:
                    wait_sec = max(0, int(state.retry_at_epoch_sec - now))
                    _log_loop_event(
                        logging.WARNING,
                        "[RUN] failsafe_pause",
                        symbol=settings.PAIR,
                        interval=settings.INTERVAL,
                        wait_sec=wait_sec,
                        reason="retry window not reached",
                    )
                    continue
                runtime_state.enable_trading()
                notification_adapter.send_event(runtime_events.event("failsafe_retry_window_reached"))

            try:
                cmd_sync(quiet=True)
                sync_observed_epoch_sec = time.time()
                conn = ensure_db()
                try:
                    row = select_latest_candle(
                        conn,
                        pair=settings.PAIR,
                        interval=settings.INTERVAL,
                    )
                    closed_row, incomplete_ts = _select_latest_closed_candle(
                        conn,
                        pair=settings.PAIR,
                        interval=settings.INTERVAL,
                        interval_sec=sec,
                        now_ms=int(sync_observed_epoch_sec * 1000),
                    )
                finally:
                    conn.close()

                if row is None:
                    runtime_state.set_last_candle_observation(
                        status="missing_after_sync",
                        age_sec=None,
                        sync_epoch_sec=sync_observed_epoch_sec,
                        candle_ts_ms=None,
                        detail="sync completed but latest candle row was not found",
                    )
                    event = runtime_events.event("no_candles_after_sync")
                    notification_adapter.send_event(event)
                    _record_runtime_cycle_artifact(
                        "skip:no_candles",
                        startup_state="READY",
                        notification_event_hashes=[event.get("event_hash")],
                    )
                    continue

                last_ts = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
                last_close = float(row["close"] if hasattr(row, "keys") else row[1])
                candle_age_sec = max(0.0, (time.time() * 1000 - last_ts) / 1000)
                runtime_state.set_last_candle_observation(
                    status="ok",
                    age_sec=candle_age_sec,
                    sync_epoch_sec=sync_observed_epoch_sec,
                    candle_ts_ms=last_ts,
                    detail=(
                        None
                        if incomplete_ts is None
                        else f"latest candle ts={incomplete_ts} still open; using latest fully closed candle"
                    ),
                )

                fail_count = 0
                runtime_state.set_error_count(fail_count)
            except Exception as e:
                fail_count += 1
                runtime_state.set_error_count(fail_count)
                sync_event = runtime_events.event(
                    "sync_failed",
                    fail_count=fail_count,
                    max_fails=MAX_FAILS,
                    error=f"{type(e).__name__}: {e}",
                )
                notification_adapter.send_event(sync_event)
                if fail_count >= MAX_FAILS:
                    retry_at = time.time() + FAILSAFE_RETRY_DELAY_SEC
                    pause_trading_until(retry_at)
                    pause_event = runtime_events.event(
                        "failsafe_pause_enabled",
                        retry_at_epoch_sec=retry_at,
                    )
                    notification_adapter.send_event(pause_event)
                    event_hashes = [sync_event.get("event_hash"), pause_event.get("event_hash")]
                else:
                    event_hashes = [sync_event.get("event_hash")]
                _record_runtime_cycle_artifact(
                    "skip:sync_failed",
                    startup_state="READY",
                    notification_event_hashes=event_hashes,
                )
                continue

            stale_cutoff_sec = sec * 2
            if candle_age_sec > stale_cutoff_sec:
                event = runtime_events.event(
                    "stale_candle_detected",
                    age_sec=candle_age_sec,
                    stale_cutoff_sec=stale_cutoff_sec,
                )
                notification_adapter.send_event(event)
                _record_runtime_cycle_artifact(
                    "skip:stale_candle",
                    candle_ts=last_ts,
                    startup_state="READY",
                    notification_event_hashes=[event.get("event_hash")],
                )
                continue

            market_safety_result = safety_controller.evaluate_market_runtime(
                settings_obj=settings,
                now_epoch_sec=now,
                last_market_runtime_check_at=last_market_runtime_check_at,
                validate_market_runtime=validate_market_runtime,
                validation_error_type=MarketPreflightValidationError,
            )
            if market_safety_result.last_market_runtime_check_at is not None:
                last_market_runtime_check_at = market_safety_result.last_market_runtime_check_at
            if market_safety_result.blocked:
                market_safety_result = _apply_runtime_safety_decision(safety_controller, market_safety_result)
                _dispatch_runtime_safety_notifications(notification_adapter, market_safety_result)
                safety_hash = (
                    market_safety_result.safety_decision.as_dict()["decision_hash"]
                    if market_safety_result.safety_decision is not None
                    else None
                )
                _record_runtime_cycle_artifact(
                    market_safety_result.cycle_id or "halt:market_runtime",
                    candle_ts=last_ts,
                    startup_state="READY",
                    safety_decision_hash=safety_hash,
                    state_transition_hash=market_safety_result.state_transition_hash,
                    notification_event_hashes=market_safety_result.notification_event_hashes,
                )
                continue

            safety_result = safety_controller.evaluate_runtime_safety(
                settings_obj=settings,
                broker=broker,
                now_epoch_sec=now,
                last_close=float(last_close),
                last_open_order_reconcile_at=last_open_order_reconcile_at,
                portfolio_cash_qty_with_position_state=portfolio_cash_qty_with_position_state,
                db_factory=ensure_db,
                open_order_snapshot=_get_open_order_snapshot,
                mark_open_orders_recovery_required=_mark_open_orders_recovery_required,
                reconcile_with_broker=reconcile_with_broker,
            )
            last_open_order_reconcile_at = safety_result.last_open_order_reconcile_at
            if safety_result.blocked:
                safety_result = _apply_runtime_safety_decision(safety_controller, safety_result)
                _dispatch_runtime_safety_notifications(notification_adapter, safety_result)
                safety_hash = (
                    safety_result.safety_decision.as_dict()["decision_hash"]
                    if safety_result.safety_decision is not None
                    else None
                )
                _record_runtime_cycle_artifact(
                    safety_result.cycle_id or "safety:block",
                    candle_ts=last_ts,
                    startup_state="READY",
                    safety_decision_hash=safety_hash,
                    state_transition_hash=safety_result.state_transition_hash,
                    notification_event_hashes=safety_result.notification_event_hashes,
                )
                continue

            state = runtime_state.snapshot()
            last_processed_candle_ts_ms = state.last_processed_candle_ts_ms

            checkpoint_decision = runtime_checkpoint.evaluate_closed_candle(
                closed_row=closed_row,
                incomplete_ts=incomplete_ts,
                last_processed_candle_ts_ms=last_processed_candle_ts_ms,
                close_guard_ms=_close_guard_ms(sec),
            )
            if not checkpoint_decision.allowed:
                _record_runtime_cycle_artifact(
                    checkpoint_decision.cycle_id,
                    candle_ts=checkpoint_decision.candle_ts,
                    startup_state="READY",
                )
                continue

            closed_candle_ts_ms = int(checkpoint_decision.candle_ts or 0)
            decision_result = decision_coordinator.decide_cycle(
                runtime_strategy_set=runtime_strategy_set,
                candle_ts=closed_candle_ts_ms,
                updated_ts=int(now * 1000),
            )

            if decision_result.persistence_status == "insufficient_signal_history":
                _log_loop_event(
                    logging.INFO,
                    "[RUN] signal_skipped",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=closed_candle_ts_ms,
                    last_processed_candle_ts=last_processed_candle_ts_ms,
                    reason=decision_result.reason,
                )
                _record_runtime_cycle_artifact("skip:insufficient_signal_history", candle_ts=closed_candle_ts_ms, startup_state="READY")
                continue

            r = {
                "ts": decision_result.candle_ts,
                "last_close": decision_result.market_price,
                "signal": decision_result.signal,
                "reason": decision_result.reason,
                "strategy": decision_result.strategy_name,
            }

            _log_loop_event(
                logging.INFO,
                "[RUN] closed candle decision ready",
                symbol=settings.PAIR,
                interval=settings.INTERVAL,
                candle_ts=r["ts"],
                last_processed_candle_ts=last_processed_candle_ts_ms,
                close=f"{r['last_close']:,.0f}",
                signal=r["signal"],
                strategy=r["strategy"],
                reason=r["reason"],
            )

            decision_context_for_trade = decision_result.decision_context
            execution_decision_summary_for_trade = decision_result.execution_decision_summary
            execution_plan_bundle_for_trade = decision_result.execution_plan_bundle
            if decision_context_for_trade is not None:
                context = decision_context_for_trade
                _log_loop_event(
                    logging.INFO,
                    "[RUN] strategy decision",
                    strategy=decision_result.strategy_name,
                    decision_type=str(context.get("decision_type") or "-"),
                    raw_signal=str(context.get("raw_signal") or context.get("base_signal") or decision_result.signal),
                    final_signal=decision_result.signal,
                    entry_blocked=1 if bool(context.get("entry_blocked")) else 0,
                    entry_block_reason=str(context.get("entry_block_reason") or "-"),
                    dust_classification=str(context.get("dust_classification") or context.get("position_gate", {}).get("dust_state") or "-"),
                    effective_flat=1 if bool(context.get("effective_flat")) else 0,
                    raw_qty_open=f"{float(context.get('raw_qty_open', 0.0) or 0.0):.8f}",
                    normalized_exposure_active=1 if bool(context.get("normalized_exposure_active")) else 0,
                    normalized_exposure_qty=f"{float(context.get('normalized_exposure_qty', 0.0) or 0.0):.8f}",
                    final_action=str(context.get("final_action") or "-"),
                    submit_expected=1 if bool(context.get("submit_expected")) else 0,
                    pre_submit_proof_status=str(context.get("pre_submit_proof_status") or "-"),
                    execution_block_reason=str(context.get("execution_block_reason") or "-"),
                    residual_inventory_state=str(context.get("residual_inventory_state") or "-"),
                    decision_authority_source=str(context.get("decision_authority_source") or "legacy_context"),
                    decision_envelope_present=1 if bool(context.get("decision_envelope_present")) else 0,
                    execution_plan_bundle_present=1 if bool(context.get("execution_plan_bundle_present")) else 0,
                    submit_plan_source=str(context.get("submit_plan_source") or "-"),
                    submit_plan_authority=str(context.get("submit_plan_authority") or "-"),
                    portfolio_target_present=1 if bool(context.get("portfolio_target_present")) else 0,
                    portfolio_target_authoritative=1 if bool(context.get("portfolio_target_authoritative")) else 0,
                    portfolio_target_hash=str(context.get("portfolio_target_hash") or "-"),
                    allocation_decision_hash=str(context.get("allocation_decision_hash") or "-"),
                    allocator_config_hash=str(context.get("allocator_config_hash") or "-"),
                    strategy_contribution_hash=str(context.get("strategy_contribution_hash") or "-"),
                    allocator_policy=str(context.get("allocator_policy") or "-"),
                    allocator_reason=str(context.get("allocator_reason") or "-"),
                    allocation_conflict_count=int(context.get("allocation_conflict_count") or 0),
                    allocation_primary_block_reason=str(context.get("allocation_primary_block_reason") or "-"),
                    persistence_context_authoritative=int(context.get("persistence_context_authoritative") or 0),
                    policy_contract_hash=str(context.get("policy_contract_hash") or "-"),
                    policy_input_hash=str(context.get("policy_input_hash") or "-"),
                    policy_decision_hash=str(context.get("policy_decision_hash") or "-"),
                    strategy_parameters_hash=str(context.get("strategy_parameters_hash") or "-"),
                    approved_profile_hash=str(context.get("approved_profile_hash") or "-"),
                    runtime_contract_hash=str(context.get("runtime_contract_hash") or "-"),
                    plugin_contract_hash=str(context.get("plugin_contract_hash") or "-"),
                    runtime_decision_request_hash=str(context.get("runtime_decision_request_hash") or "-"),
                    replay_fingerprint_hash=str(context.get("replay_fingerprint_hash") or "-"),
                    execution_engine=str(context.get("execution_decision", {}).get("execution_engine") if isinstance(context.get("execution_decision"), dict) else "-"),
                    target_engine_mode=str(context.get("target_engine_mode") or "-"),
                    target_previous_exposure_krw=str(context.get("target_previous_exposure_krw") or "-"),
                    target_new_exposure_krw=str(context.get("target_new_exposure_krw") or "-"),
                    target_current_qty=str(context.get("target_current_qty") or "-"),
                    target_qty=str(context.get("target_qty") or "-"),
                    target_delta_qty=str(context.get("target_delta_qty") or "-"),
                    target_delta_side=str(context.get("target_delta_side") or "-"),
                    target_delta_notional_krw=str(context.get("target_delta_notional_krw") or "-"),
                    target_would_submit=1 if bool(context.get("target_would_submit")) else 0,
                    target_block_reason=str(context.get("target_block_reason") or "-"),
                    target_position_truth_state=str(context.get("target_position_truth_state") or "-"),
                    target_dust_classification=str(context.get("target_dust_classification") or "-"),
                    target_policy_action=str(context.get("target_policy_action") or "-"),
                    target_origin=str(context.get("target_origin") or "-"),
                    target_adoption_reason=str(context.get("target_adoption_reason") or "-"),
                    target_adopted_broker_qty=str(context.get("target_adopted_broker_qty") or "-"),
                    target_adopted_exposure_krw=str(context.get("target_adopted_exposure_krw") or "-"),
                    target_startup_policy_state=str(context.get("target_startup_policy_state") or "-"),
                    target_existing_state_present=1 if bool(context.get("target_existing_state_present")) else 0,
                    target_missing_state_resolution=str(context.get("target_missing_state_resolution") or "-"),
                    target_closeout_requested=1 if bool(context.get("target_closeout_requested")) else 0,
                    target_strategy_signal_source=str(context.get("target_strategy_signal_source") or "-"),
                    reason=decision_result.reason,
                )

            if decision_result.decision_id is None:
                _log_loop_event(
                    logging.WARNING,
                    "[ORDER_SKIP] strategy decision not durable",
                    signal=authoritative_execution_signal_for_trade(
                        decision_context_for_trade,
                        fallback_signal=r["signal"],
                    ),
                    candle_ts=r["ts"],
                    reason="decision_persistence_failed_retryable",
                )
                _record_runtime_cycle_artifact(
                    "skip:decision_persistence_failed",
                    candle_ts=int(r["ts"]),
                    startup_state="READY",
                    strategy_decision_hash=decision_result.strategy_decision_hash,
                    runtime_strategy_decision_bundle_id=decision_result.runtime_strategy_decision_bundle_id,
                    runtime_strategy_decision_bundle_hash=decision_result.runtime_strategy_decision_bundle_hash,
                    portfolio_allocation_decision_id=decision_result.portfolio_allocation_decision_id,
                    portfolio_allocation_decision_hash=decision_result.portfolio_allocation_decision_hash,
                    portfolio_target_id=decision_result.portfolio_target_id,
                    portfolio_target_hash=decision_result.portfolio_target_hash,
                    strategy_contribution_hash=decision_result.strategy_contribution_hash,
                    execution_plan_id=decision_result.execution_plan_id,
                    execution_plan_bundle_hash=decision_result.execution_plan_bundle_hash,
                    execution_submit_plan_hash=decision_result.execution_submit_plan_hash,
                )
                continue

            if execution_decision_summary_for_trade is None:
                _log_loop_event(
                    logging.WARNING,
                    "[ORDER_SKIP] typed execution decision summary missing",
                    signal=authoritative_execution_signal_for_trade(
                        decision_context_for_trade,
                        fallback_signal=r["signal"],
                    ),
                    candle_ts=r["ts"],
                    reason="execution_planning_failed_closed",
                )
                _record_runtime_cycle_artifact(
                    "skip:execution_summary_missing",
                    candle_ts=int(r["ts"]),
                    startup_state="READY",
                    strategy_decision_hash=decision_result.strategy_decision_hash,
                    runtime_strategy_decision_bundle_id=decision_result.runtime_strategy_decision_bundle_id,
                    runtime_strategy_decision_bundle_hash=decision_result.runtime_strategy_decision_bundle_hash,
                    portfolio_allocation_decision_id=decision_result.portfolio_allocation_decision_id,
                    portfolio_allocation_decision_hash=decision_result.portfolio_allocation_decision_hash,
                    portfolio_target_id=decision_result.portfolio_target_id,
                    portfolio_target_hash=decision_result.portfolio_target_hash,
                    strategy_contribution_hash=decision_result.strategy_contribution_hash,
                    execution_plan_id=decision_result.execution_plan_id,
                    execution_plan_bundle_hash=decision_result.execution_plan_bundle_hash,
                    execution_submit_plan_hash=decision_result.execution_submit_plan_hash,
                )
                continue

            execution_result = execution_coordinator.execute_cycle(
                candle_ts=int(r["ts"]),
                decision_id=decision_result.decision_id,
                signal=str(r["signal"] or "HOLD"),
                market_price=float(r["last_close"] or 0.0),
                strategy_name=decision_result.strategy_name,
                decision_reason=decision_result.reason,
                exit_rule_name=decision_result.exit_rule_name,
                decision_context=decision_context_for_trade,
                execution_plan_bundle=execution_plan_bundle_for_trade,
                execution_decision_summary=execution_decision_summary_for_trade,
                execution_service=execution_service,
                post_trade_reconcile=(
                    (lambda: reconcile_with_broker(broker))
                    if settings.MODE == "live" and broker is not None
                    else None
                ),
                input_hash=decision_result.as_dict()["decision_hash"],
                execution_plan_bundle_hash=decision_result.execution_plan_bundle_hash,
            )
            if execution_result.mark_processed_allowed:
                runtime_checkpoint.apply(candle_ts_ms=int(r["ts"]), now_epoch_sec=now)
            _record_runtime_cycle_artifact(
                "checkpoint:processed",
                candle_ts=int(r["ts"]),
                startup_state="READY",
                strategy_decision_hash=decision_result.strategy_decision_hash,
                runtime_strategy_decision_bundle_id=decision_result.runtime_strategy_decision_bundle_id,
                runtime_strategy_decision_bundle_hash=decision_result.runtime_strategy_decision_bundle_hash,
                portfolio_allocation_decision_id=decision_result.portfolio_allocation_decision_id,
                portfolio_allocation_decision_hash=decision_result.portfolio_allocation_decision_hash,
                portfolio_target_id=decision_result.portfolio_target_id,
                portfolio_target_hash=decision_result.portfolio_target_hash,
                strategy_contribution_hash=decision_result.strategy_contribution_hash,
                execution_plan_id=decision_result.execution_plan_id,
                execution_plan_bundle_hash=decision_result.execution_plan_bundle_hash,
                execution_submit_plan_hash=decision_result.execution_submit_plan_hash,
                execution_result_hash=execution_result.as_dict()["decision_hash"],
            )
            _log_loop_event(
                logging.INFO,
                "[RUN] processed closed candle",
                symbol=settings.PAIR,
                interval=settings.INTERVAL,
                candle_ts=r["ts"],
                last_processed_candle_ts=last_processed_candle_ts_ms,
                reason="decision_persisted_execution_planned",
            )

            if execution_result.halt_transition:
                halt_reason_code = str(execution_result.halt_transition.get("reason_code") or execution_result.planning_status)
                halt_reason = str(execution_result.halt_transition.get("evidence", {}).get("error") or halt_reason_code)
                decision = safety_controller.evaluate_halt(
                    _halt_reason(halt_reason_code, halt_reason),
                    unresolved=True,
                )
                transition = safety_controller.apply(decision)
                decision = type(decision)(
                    action=decision.action,
                    reason_code=decision.reason_code,
                    reason=decision.reason,
                    unresolved=decision.unresolved,
                    attempt_flatten=decision.attempt_flatten,
                    state_transition=transition,
                    operator_event=decision.operator_event,
                    input_hash=decision.input_hash,
                    evidence_hash=decision.evidence_hash,
                    decision_hash=decision.decision_hash,
                    evidence=decision.evidence,
                )
                _dispatch_safety_decision(notification_adapter, decision)
                event = runtime_events.execution_failure_from_transition(execution_result.halt_transition)
                notification_adapter.send_event(event)
                _record_runtime_cycle_artifact(
                    f"halt:{execution_result.planning_status}",
                    candle_ts=int(r["ts"]),
                    startup_state="READY",
                    strategy_decision_hash=decision_result.strategy_decision_hash,
                    runtime_strategy_decision_bundle_id=decision_result.runtime_strategy_decision_bundle_id,
                    runtime_strategy_decision_bundle_hash=decision_result.runtime_strategy_decision_bundle_hash,
                    portfolio_allocation_decision_id=decision_result.portfolio_allocation_decision_id,
                    portfolio_allocation_decision_hash=decision_result.portfolio_allocation_decision_hash,
                    portfolio_target_id=decision_result.portfolio_target_id,
                    portfolio_target_hash=decision_result.portfolio_target_hash,
                    strategy_contribution_hash=decision_result.strategy_contribution_hash,
                    execution_plan_id=decision_result.execution_plan_id,
                    execution_plan_bundle_hash=decision_result.execution_plan_bundle_hash,
                    execution_submit_plan_hash=decision_result.execution_submit_plan_hash,
                    execution_result_hash=execution_result.as_dict()["decision_hash"],
                    safety_decision_hash=decision.as_dict()["decision_hash"],
                    state_transition_hash=decision.as_dict()["state_transition"].get("decision_hash"),
                    notification_event_hashes=[
                        *decision.as_dict().get("operator_event_hashes", []),
                        event.get("event_hash"),
                    ],
                )
                continue

            trade = execution_result.trade

            if trade:
                _log_loop_event(
                    logging.INFO,
                    "[RUN] trade_applied",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=trade.get("candle_ts", r["ts"]),
                    signal_ts=trade.get("signal_ts", r["ts"]),
                    client_order_id=trade.get("client_order_id", "-"),
                    exchange_order_id=trade.get("exchange_order_id", "-"),
                    side=trade["side"],
                    qty=f"{trade['qty']:.8f}",
                    submit_qty=f"{float(trade.get('submit_qty', trade['qty'])):.8f}",
                    filled_qty=f"{float(trade.get('filled_qty', trade['qty'])):.8f}",
                    price=f"{trade['price']:,.0f}",
                    fee=f"{trade['fee']:,.0f}",
                    cash=f"{trade['cash']:,.0f}",
                    asset=f"{trade['asset']:.8f}",
                    post_trade_cash=f"{float(trade.get('post_trade_cash', trade['cash'])):,.0f}",
                    post_trade_asset=f"{float(trade.get('post_trade_asset', trade['asset'])):.8f}",
                )

    except KeyboardInterrupt:
        _log_loop_event(
            logging.INFO,
            "[RUN] stopped",
            symbol=settings.PAIR,
            interval=settings.INTERVAL,
            reason="stopped by user (Ctrl+C)",
        )
    finally:
        _LEGACY_ENGINE_ATTEMPT_OPEN_ORDER_CANCELLATION = None
        operator_notification_service = _ORIGINAL_OPERATOR_NOTIFICATION_SERVICE_FACTORY


def run_loop() -> None:
    from .app_container import create_default_runtime_app

    create_default_runtime_app(settings).runner.run_forever()
