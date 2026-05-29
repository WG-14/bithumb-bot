from __future__ import annotations

import logging
import math
import time
import json
import os
import importlib
from functools import partial

from ..config import (
    DEFAULT_RUNTIME_STRATEGY,
    MarketPreflightValidationError,
    settings,
    validate_live_mode_preflight,
    validate_market_preflight,
    validate_market_runtime,
    validate_runtime_strategy_set_selection,
)
from ..marketdata import cmd_sync
from ..runtime_decision_service import (
    RuntimeStrategyDecisionResult,
    RuntimeDecisionGateway,
    compute_strategy_decision_snapshot,
    get_runtime_decision_adapter,
    legacy_db_strategy_fallback_allowed,
)
from ..runtime_strategy_set import (
    RuntimeStrategyDecisionResultBundle,
    active_runtime_strategy_set,
    normalized_runtime_strategy_set_manifest,
)
from ..broker.bithumb import BithumbBroker, build_broker_with_auth_diagnostics
from ..broker.base import BrokerError
from ..db_core import (
    ensure_db,
    upsert_target_position_state,
)
from ..db_core import record_strategy_decision
from ..execution_service import build_execution_decision_summary
from ..runtime_data_access import (
    count_open_orders as _count_open_orders,
    latest_order_identifiers as _latest_order_identifiers,
    mark_open_orders_recovery_required as _mark_open_orders_recovery_required,
    open_order_identifiers_for_broker_revalidation,
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
from ..dust import build_dust_display_context
from ..utils_time import kst_str, parse_interval_sec
from ..observability import configure_runtime_logging, format_log_kv
from ..bootstrap import get_last_explicit_env_load_summary
from ..reason_codes import POSITION_LOSS_LIMIT, RISKY_ORDER_BLOCK
from .. import runtime_state
from ..risk import (
    RISK_STATE_MISMATCH,
    daily_loss_reason_code_from_reason,
    evaluate_daily_loss_breach,
    evaluate_position_loss_breach,
)
from ..execution_service import (
    ExecutionObservabilityPayload,
    SignalExecutionRequest,
    TypedExecutionRequest,
    build_signal_execution_service,
    live_execute_signal,
    paper_execute,
    record_harmless_dust_exit_suppression,
)
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
from .lifecycle_artifacts import RuntimeCycleArtifact
from .runtime_checkpoint import apply_processed_candle_checkpoint
from .state_store import pause_trading_until
from .notification_adapter import NotificationAdapter
from .operator_event_composer import (
    OperatorEventComposer,
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

FAILSAFE_RETRY_DELAY_SEC = 180
CLEANUP_REVALIDATION_MAX_ATTEMPTS = 2
CLEANUP_REVALIDATION_POSITION_EPS = 1e-12
RUN_LOG = logging.getLogger("bithumb_bot.run")


def _record_runtime_cycle_artifact(
    cycle_id: str,
    *,
    candle_ts: int | None = None,
    startup_state: str | None = None,
    readiness_hash: str | None = None,
    strategy_decision_hash: str | None = None,
    execution_plan_bundle_hash: str | None = None,
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
    artifact = RuntimeCycleArtifact(
        cycle_id=cycle_id,
        candle_ts=candle_ts,
        startup_state=startup_state,
        readiness_hash=readiness_hash,
        strategy_decision_hash=strategy_decision_hash,
        execution_plan_bundle_hash=execution_plan_bundle_hash,
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
            _notify_operator(service.event_formatter(event_name, **fields))

        def send_message(self, message: str) -> None:
            _notify_operator(message)

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
    if not _run_loop_uses_target_delta():
        return False
    target_decision = (
        execution_decision.get("target_shadow_decision")
        if isinstance(execution_decision, dict)
        and isinstance(execution_decision.get("target_shadow_decision"), dict)
        else None
    )
    if not isinstance(target_decision, dict):
        return False
    if (
        target_decision.get("target_new_exposure_krw") is None
        or target_decision.get("target_qty") is None
        or target_decision.get("target_reference_price") is None
    ):
        return False
    upsert_target_position_state(
        conn,
        pair=settings.PAIR,
        target_exposure_krw=float(target_decision["target_new_exposure_krw"] or 0.0),
        target_qty=float(target_decision["target_qty"] or 0.0),
        last_signal=signal,
        last_decision_id=decision_id,
        last_reference_price=float(target_decision["target_reference_price"] or 0.0),
        updated_ts=int(updated_ts),
        target_origin=str(target_decision.get("target_origin") or ""),
        adoption_reason=str(target_decision.get("target_adoption_reason") or ""),
        adopted_broker_qty=(
            None
            if target_decision.get("target_adopted_broker_qty") is None
            else float(target_decision.get("target_adopted_broker_qty") or 0.0)
        ),
        adopted_broker_exposure_krw=(
            None
            if target_decision.get("target_adopted_exposure_krw") is None
            else float(target_decision.get("target_adopted_exposure_krw") or 0.0)
        ),
        created_from_signal=str(target_decision.get("target_strategy_signal_source") or signal),
    )
    return True


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


def build_signal_execution_request(
    *,
    signal: str,
    ts: int,
    market_price: float,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    execution_decision_summary: object | None,
    decision_context: dict[str, object] | None,
    execution_plan_bundle: object | None = None,
) -> SignalExecutionRequest:
    typed_request = TypedExecutionRequest(
        signal=signal,
        ts=ts,
        market_price=market_price,
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        execution_decision_summary=execution_decision_summary,
        execution_plan_bundle=execution_plan_bundle,
    )
    return SignalExecutionRequest.from_typed(
        typed_request,
        observability_payload=ExecutionObservabilityPayload(decision_context or {}),
    )


def authoritative_execution_signal_for_trade(
    decision_context: dict[str, object] | None,
    *,
    fallback_signal: object,
) -> str:
    if isinstance(decision_context, dict):
        planned = str(decision_context.get("authoritative_execution_signal") or "").strip().upper()
        if planned in {"BUY", "SELL", "HOLD"}:
            return planned
        execution_decision = decision_context.get("execution_decision")
        if isinstance(execution_decision, dict):
            planned = str(execution_decision.get("final_signal") or "").strip().upper()
            if planned in {"BUY", "SELL", "HOLD"}:
                return planned
    fallback = str(fallback_signal or "HOLD").strip().upper()
    return fallback if fallback in {"BUY", "SELL", "HOLD"} else "HOLD"


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
    conn = ensure_db()
    try:
        readiness_snapshot = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()
    return bool(
        state.last_reconcile_status == "ok"
        and readiness_snapshot.recovery_stage in {"UNAPPLIED_PRINCIPAL_PENDING", "FEE_FINALIZATION_PENDING"}
        and readiness_snapshot.run_loop_allowed
        and int(readiness_snapshot.auto_recovery_count or 0) > 0
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
    runtime_state.refresh_open_order_health()
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


def maybe_clear_stale_initial_reconcile_halt() -> bool:
    state = runtime_state.snapshot()
    clearance = evaluate_initial_reconcile_halt_clearance()
    if not clearance.allowed:
        return False
    _recovery_controller().apply_clearance(clearance)
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
    state = runtime_state.snapshot()
    clearance = evaluate_live_execution_broker_halt_clearance(
        startup_gate_reason=startup_gate_reason,
    )
    if not clearance.allowed:
        return False
    _recovery_controller().apply_clearance(clearance)
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
    state = runtime_state.snapshot()
    clearance = evaluate_risk_state_mismatch_halt_clearance(
        startup_gate_reason=startup_gate_reason,
    )
    if not clearance.allowed:
        return False
    _recovery_controller().apply_clearance(clearance)
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

    return SafetyController(
        symbol=settings.PAIR,
        state_snapshot=runtime_state.snapshot,
        enter_halt=runtime_state.enter_halt,
        resume_evaluator=evaluate_resume_eligibility,
        latest_order_identifiers=_latest_order_identifiers,
        count_open_orders=_count_open_orders,
        position_summary=_position_summary,
        notification_sender=NotificationAdapter(_operator_notification_service()),
        cancel_open_orders_with_broker=cancel_open_orders_with_broker,
        record_cancel_open_orders_result=runtime_state.record_cancel_open_orders_result,
        flatten_position=_flatten_position_compat,
        record_flatten_position_result=runtime_state.record_flatten_position_result,
        exposure_snapshot=_get_exposure_snapshot,
        revalidate_cleanup_state_after_failure=_revalidate_cleanup_state_after_failure,
        now_ms=lambda: int(time.time() * 1000),
        live_dry_run=lambda: bool(settings.LIVE_DRY_RUN),
    )


def _startup_controller() -> StartupController:
    return StartupController(
        symbol=settings.PAIR,
        startup_gate_evaluator=evaluate_startup_safety_gate,
        stale_initial_reconcile_clearer=maybe_clear_stale_initial_reconcile_halt,
        stale_live_execution_broker_clearer=maybe_clear_stale_live_execution_broker_halt,
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
    )


def _halt_trading(reason: HaltReason, *, unresolved: bool = False, attempt_flatten: bool = False) -> None:
    _safety_controller().halt_trading(
        reason,
        unresolved=unresolved,
        attempt_flatten=attempt_flatten,
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


def _revalidate_cleanup_state_after_failure(
    broker: BithumbBroker,
    *,
    trigger: str,
    max_attempts: int = CLEANUP_REVALIDATION_MAX_ATTEMPTS,
) -> tuple[bool, str]:
    """Performs bounded broker-side revalidation after uncertain cleanup results."""
    from ..recovery import reconcile_with_broker

    attempts = max(1, int(max_attempts))
    last_open_orders_present: bool | None = None
    last_position_present: bool | None = None
    last_errors: list[str] = []

    for attempt in range(1, attempts + 1):
        try:
            reconcile_with_broker(broker)
        except Exception as exc:
            last_errors.append(f"attempt={attempt} reconcile={type(exc).__name__}: {exc}")

        open_orders_present: bool | None = None
        position_present: bool | None = None

        try:
            client_order_ids, exchange_order_ids = open_order_identifiers_for_broker_revalidation()
            if exchange_order_ids or client_order_ids:
                open_orders_present = (
                    len(
                        broker.get_open_orders(
                            exchange_order_ids=exchange_order_ids,
                            client_order_ids=client_order_ids,
                        )
                    )
                    > 0
            )
            else:
                open_orders_present = False
        except Exception as exc:
            last_errors.append(f"attempt={attempt} open_orders={type(exc).__name__}: {exc}")

        try:
            balance = broker.get_balance()
            position_present = (
                float(balance.asset_available) + float(balance.asset_locked)
            ) > CLEANUP_REVALIDATION_POSITION_EPS
        except Exception as exc:
            last_errors.append(f"attempt={attempt} balance={type(exc).__name__}: {exc}")

        if open_orders_present is not None:
            last_open_orders_present = open_orders_present
        if position_present is not None:
            last_position_present = position_present

        if open_orders_present is False and position_present is False:
            return True, (
                f"cleanup_revalidation(trigger={trigger}) attempts={attempt}/{attempts} "
                "broker_confirms_no_open_orders_and_no_position"
            )

    status_parts = [
        f"cleanup_revalidation(trigger={trigger}) attempts={attempts}/{attempts}",
        (
            f"open_orders_present={1 if last_open_orders_present else 0}"
            if last_open_orders_present is not None
            else "open_orders_present=unknown"
        ),
        (
            f"position_present={1 if last_position_present else 0}"
            if last_position_present is not None
            else "position_present=unknown"
        ),
    ]
    if last_errors:
        status_parts.append("errors=" + " | ".join(last_errors))
    return False, "; ".join(status_parts)


def _attempt_open_order_cancellation(broker: BithumbBroker, trigger: str) -> bool:
    return _safety_controller().attempt_open_order_cancellation(broker, trigger=trigger)


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

    startup_result = _startup_controller().prepare_runtime_start(
        live_mode=settings.MODE == "live"
    )
    if startup_result.operator_event:
        NotificationAdapter(_operator_notification_service()).send_event(startup_result.operator_event)
    _record_runtime_cycle_artifact(
        "startup",
        startup_state=startup_result.status,
        notification_event_hashes=startup_result.as_dict().get("operator_event_hashes", []),
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

    sec = parse_interval_sec(settings.INTERVAL)
    runtime_strategy_set = active_runtime_strategy_set()
    runtime_strategy_set_manifest = normalized_runtime_strategy_set_manifest(
        strategy_set=runtime_strategy_set,
        settings_obj=settings,
    )
    runtime_strategy_set_hash = str(runtime_strategy_set_manifest["runtime_strategy_set_manifest_hash"])
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
            else f"default:{DEFAULT_RUNTIME_STRATEGY}"
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

    execution_service = build_signal_execution_service(
        mode=settings.MODE,
        broker=broker,
        paper_executor=paper_execute,
        live_executor=live_execute_signal,
        harmless_dust_recorder=record_harmless_dust_exit_suppression,
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
                _operator_notification_service().send_message("failsafe retry window reached, attempting auto-resume")

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
                    _operator_notification_service().send_message("no candles after sync")
                    _record_runtime_cycle_artifact("skip:no_candles", startup_state="READY")
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
                _operator_notification_service().send_message(f"sync failed ({fail_count}/{MAX_FAILS}): {e}")
                if fail_count >= MAX_FAILS:
                    retry_at = time.time() + FAILSAFE_RETRY_DELAY_SEC
                    pause_trading_until(retry_at)
                    _operator_notification_service().send_message(
                        "failsafe enabled after consecutive sync failures. "
                        f"trading paused until epoch={int(retry_at)}"
                    )
                _record_runtime_cycle_artifact("skip:sync_failed", startup_state="READY")
                continue

            stale_cutoff_sec = sec * 2
            if candle_age_sec > stale_cutoff_sec:
                _operator_notification_service().send_message(
                    f"stale candle detected: age={candle_age_sec:.1f}s > "
                    f"{stale_cutoff_sec}s; order blocked"
                )
                _record_runtime_cycle_artifact("skip:stale_candle", candle_ts=last_ts, startup_state="READY")
                continue

            runtime_market_check_interval = float(settings.MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC)
            if runtime_market_check_interval < 0:
                _halt_trading(
                    _halt_reason(
                        "MARKET_RUNTIME_POLICY_INVALID",
                        "MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC must be >= 0",
                    ),
                    unresolved=False,
                )
                continue
            should_check_runtime_market = settings.MODE == "live" and runtime_market_check_interval > 0 and (
                last_market_runtime_check_at is None
                or (now - last_market_runtime_check_at) >= runtime_market_check_interval
            )
            if should_check_runtime_market:
                try:
                    validate_market_runtime(settings)
                    last_market_runtime_check_at = now
                except MarketPreflightValidationError as exc:
                    _halt_trading(
                        _halt_reason(
                            "MARKET_RUNTIME_CONTRACT_FAILED",
                            f"market runtime contract failed: {exc}",
                        ),
                        unresolved=False,
                    )
                    continue

            if settings.MODE == "live" and broker is not None:
                if settings.KILL_SWITCH:
                    halt_reason, _canceled_ok, unresolved = perform_panic_stop_cleanup(
                        broker,
                        reason_code="KILL_SWITCH",
                        reason_detail="KILL_SWITCH=ON",
                        cancel_trigger="kill-switch",
                        flatten_trigger="kill-switch",
                        attempt_flatten=bool(settings.KILL_SWITCH_LIQUIDATE),
                    )
                    _halt_trading(
                        halt_reason,
                        unresolved=unresolved,
                        attempt_flatten=bool(settings.KILL_SWITCH_LIQUIDATE),
                    )
                    continue

                portfolio_cash, portfolio_qty, position_state, lot_definition = (
                    portfolio_cash_qty_with_position_state(pair=settings.PAIR)
                )
                conn = ensure_db()
                try:
                    if position_state is not None:
                        dust_context = build_dust_display_context(
                            runtime_state.snapshot().last_reconcile_metadata
                        )
                        # Use latest candle close as the mark price for daily-loss evaluation.
                        blocked, reason = evaluate_daily_loss_breach(
                            conn,
                            ts_ms=int(now * 1000),
                            cash=portfolio_cash,
                            qty=portfolio_qty,
                            price=float(last_close),
                            broker=broker,
                            mark_price_source="closed_candle",
                            evaluation_origin="run_loop_daily_halt",
                        )
                        if blocked:
                            reason_code = daily_loss_reason_code_from_reason(reason)
                            if reason_code == RISK_STATE_MISMATCH:
                                _halt_trading(
                                    _halt_reason(RISK_STATE_MISMATCH, reason),
                                    unresolved=True,
                                )
                            else:
                                halt_reason, canceled_ok, cleanup_unresolved = _attempt_risk_breach_flatten(
                                    broker,
                                    reason_code="DAILY_LOSS_LIMIT",
                                    reason_detail=reason,
                                    cancel_trigger="daily-loss-halt",
                                    flatten_trigger="daily-loss-halt",
                                )
                                _halt_trading(
                                    halt_reason,
                                    unresolved=cleanup_unresolved,
                                )
                            continue

                        position_loss_qty = float(position_state.normalized_exposure.open_exposure_qty)
                        dust_view = position_state.normalized_exposure.dust_operator_view
                        min_position_loss_qty = max(
                            0.0,
                            float(0.0 if lot_definition is None else lot_definition.min_qty),
                            float(getattr(settings, "LIVE_MIN_ORDER_QTY", 0.0) or 0.0),
                        )
                        if (
                            bool(dust_context.classification.present)
                            and bool(dust_view.resume_allowed)
                            and min_position_loss_qty > 0.0
                            and 0.0 < float(portfolio_qty) < min_position_loss_qty
                        ):
                            position_loss_qty = 0.0

                        blocked, reason = evaluate_position_loss_breach(
                            conn,
                            qty=position_loss_qty,
                            price=float(last_close),
                        )
                        if blocked:
                            halt_reason, canceled_ok, cleanup_unresolved = _attempt_risk_breach_flatten(
                                broker,
                                reason_code=POSITION_LOSS_LIMIT,
                                reason_detail=reason,
                                cancel_trigger="position-loss-halt",
                                flatten_trigger="position-loss-halt",
                            )
                            _halt_trading(
                                halt_reason,
                                unresolved=cleanup_unresolved,
                            )
                            continue
                finally:
                    conn.close()

                open_count, oldest_open_age_sec = _get_open_order_snapshot(int(now * 1000))
                if open_count > 0:
                    min_reconcile_sec = max(
                        1, int(settings.OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC)
                    )
                    if (
                        last_open_order_reconcile_at is None
                        or (now - last_open_order_reconcile_at) >= min_reconcile_sec
                    ):
                        try:
                            reconcile_with_broker(broker)
                            last_open_order_reconcile_at = now
                        except Exception as e:
                            _halt_trading(
                                _halt_reason(
                                    "PERIODIC_RECONCILE_FAILED",
                                    f"periodic reconcile failed ({type(e).__name__}): {e}",
                                ),
                                unresolved=True,
                            )
                            continue

                    open_count, oldest_open_age_sec = _get_open_order_snapshot(
                        int(now * 1000)
                    )
                    if open_count > 0 and oldest_open_age_sec is not None:
                        max_age_sec = max(1, int(settings.MAX_OPEN_ORDER_AGE_SEC))
                        if oldest_open_age_sec > max_age_sec:
                            reason = (
                                "stale unresolved open order detected: "
                                f"age={oldest_open_age_sec:.1f}s > {max_age_sec}s"
                            )
                            marked = _mark_open_orders_recovery_required(
                                reason, int(now * 1000)
                            )
                            latest_client_order_id, latest_exchange_order_id = _latest_order_identifiers()
                            NotificationAdapter(_operator_notification_service()).send_event(
                                OperatorEventComposer(settings.PAIR).stale_open_order_recovery_required_event(
                                    reason=reason,
                                    marked_count=marked,
                                    latest_client_order_id=latest_client_order_id,
                                    latest_exchange_order_id=latest_exchange_order_id,
                                    open_order_count=_count_open_orders(),
                                    unresolved_order_count=runtime_state.snapshot().unresolved_open_order_count,
                                    position_summary=_position_summary(),
                                )
                            )
                            canceled_ok = _attempt_open_order_cancellation(
                                broker, trigger="stale-open-order-halt"
                            )
                            halt_detail = (
                                f"{reason}; marked={marked} recovery_required; "
                                + (
                                    "emergency cancellation attempted"
                                    if canceled_ok
                                    else "emergency cancellation failed"
                                )
                            )
                            if not canceled_ok:
                                revalidated_safe, revalidation_detail = _revalidate_cleanup_state_after_failure(
                                    broker,
                                    trigger="stale-open-order-halt",
                                )
                                halt_detail += f"; {revalidation_detail}"
                                _halt_trading(
                                    _halt_reason("STALE_OPEN_ORDER", halt_detail),
                                    unresolved=not revalidated_safe,
                                )
                            else:
                                _halt_trading(
                                    _halt_reason("STALE_OPEN_ORDER", halt_detail),
                                    unresolved=True,
                                )
                            continue

                    if open_count > 0:
                        NotificationAdapter(_operator_notification_service()).send_event(
                            OperatorEventComposer(settings.PAIR).recovery_required_event(
                                reason_code=RISKY_ORDER_BLOCK,
                                reason="unresolved open order exists; skip new order placement",
                                event_name="order_submit_blocked",
                            )
                        )
                        _record_runtime_cycle_artifact("skip:open_order_blocked", startup_state="READY")
                        continue

            state = runtime_state.snapshot()
            last_processed_candle_ts_ms = state.last_processed_candle_ts_ms

            if incomplete_ts is not None:
                _log_loop_event(
                    logging.INFO,
                    "[SKIP] incomplete/open candle",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=incomplete_ts,
                    last_processed_candle_ts=last_processed_candle_ts_ms,
                    reason=f"latest candle has not cleared close guard ({_close_guard_ms(sec)}ms)",
                )

            if closed_row is None:
                _log_loop_event(
                    logging.INFO,
                    "[SKIP] incomplete/open candle",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=incomplete_ts,
                    last_processed_candle_ts=last_processed_candle_ts_ms,
                    reason="no fully closed candle available yet",
                )
                _record_runtime_cycle_artifact("skip:no_closed_candle", candle_ts=incomplete_ts, startup_state="READY")
                continue

            closed_candle_ts_ms = int(closed_row["ts"]) if hasattr(closed_row, "keys") else int(closed_row[0])
            if last_processed_candle_ts_ms is not None:
                if closed_candle_ts_ms == last_processed_candle_ts_ms:
                    _log_loop_event(
                        logging.INFO,
                        "[SKIP] duplicate candle",
                        symbol=settings.PAIR,
                        interval=settings.INTERVAL,
                        candle_ts=closed_candle_ts_ms,
                        last_processed_candle_ts=last_processed_candle_ts_ms,
                        reason="closed candle already processed before restart/previous tick",
                    )
                    _record_runtime_cycle_artifact("skip:duplicate_candle", candle_ts=closed_candle_ts_ms, startup_state="READY")
                    continue
                if closed_candle_ts_ms < last_processed_candle_ts_ms:
                    _log_loop_event(
                        logging.INFO,
                        "[SKIP] stale candle",
                        symbol=settings.PAIR,
                        interval=settings.INTERVAL,
                        candle_ts=closed_candle_ts_ms,
                        last_processed_candle_ts=last_processed_candle_ts_ms,
                        reason="closed candle is older than persisted last processed candle",
                    )
                    _record_runtime_cycle_artifact("skip:stale_processed_candle", candle_ts=closed_candle_ts_ms, startup_state="READY")
                    continue

            conn = ensure_db()
            typed_runtime_decision: RuntimeStrategyDecisionResult | None = None
            typed_runtime_decision_bundle: RuntimeStrategyDecisionResultBundle | None = None
            try:
                typed_runtime_decision_bundle = RuntimeDecisionGateway().decide_bundle(
                    conn,
                    strategy_set=runtime_strategy_set,
                    through_ts_ms=closed_candle_ts_ms,
                )
            finally:
                conn.close()

            if typed_runtime_decision_bundle is None:
                _log_loop_event(
                    logging.INFO,
                    "[RUN] signal_skipped",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=closed_candle_ts_ms,
                    last_processed_candle_ts=last_processed_candle_ts_ms,
                    reason="insufficient candle history; signal will be recalculated after more syncs",
                )
                _record_runtime_cycle_artifact("skip:insufficient_signal_history", candle_ts=closed_candle_ts_ms, startup_state="READY")
                continue
            typed_runtime_decision = typed_runtime_decision_bundle.results[0]
            r = {
                "ts": typed_runtime_decision.candle_ts,
                "last_close": typed_runtime_decision.market_price,
                "signal": typed_runtime_decision.decision.final_signal,
                "reason": typed_runtime_decision.decision.final_reason,
                "strategy": (
                    "multi_strategy"
                    if typed_runtime_decision_bundle.strategy_set.multi_strategy_enabled
                    else typed_runtime_decision.decision.strategy_name
                ),
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

            conn = ensure_db()
            decision_id: int | None = None
            decision_reason_for_trade: str | None = None
            decision_exit_rule_name: str | None = None
            decision_strategy_name_for_trade: str | None = None
            decision_context_for_trade: dict[str, object] | None = None
            execution_decision_summary_for_trade = None
            execution_plan_bundle_for_trade = None
            try:
                assert typed_runtime_decision_bundle is not None
                strategy_name = (
                    "multi_strategy"
                    if typed_runtime_decision_bundle.strategy_set.multi_strategy_enabled
                    else typed_runtime_decision.decision.strategy_name
                )
                signal = typed_runtime_decision.decision.final_signal
                reason = typed_runtime_decision.decision.final_reason
                planner = run_loop_execution_planner(
                    target_state_resolver=_resolve_target_position_state_for_run_loop,
                    persistence_context_builder=prepare_strategy_decision_persistence_context,
                )
                planning_bundle = planner.plan_runtime_strategy_results(
                    conn,
                    typed_runtime_decision_bundle,
                    updated_ts=int(now * 1000),
                )
                context = planning_bundle.persistence_context
                if typed_runtime_decision_bundle.strategy_set.multi_strategy_enabled:
                    target_payload = context.get("portfolio_target")
                    if isinstance(target_payload, dict):
                        target_conflict = target_payload.get("conflict_resolution")
                        if isinstance(target_conflict, dict):
                            signal = str(target_conflict.get("selected_signal") or signal)
                    reason = str(context.get("allocator_reason") or reason)
                decision_context_for_trade = context
                execution_decision_summary_for_trade = planning_bundle.summary
                execution_plan_bundle_for_trade = planning_bundle
                execution_decision = dict(context["execution_decision"])  # type: ignore[arg-type]
                exit_ctx = context.get("exit")
                if isinstance(exit_ctx, dict):
                    raw_rule = exit_ctx.get("rule")
                    if raw_rule is not None:
                        decision_exit_rule_name = str(raw_rule)
                decision_reason_for_trade = reason
                decision_strategy_name_for_trade = strategy_name
                candle_ts_raw = context.get("ts")
                market_price_raw = context.get("last_close")
                confidence_raw = context.get("confidence")
                _log_loop_event(
                    logging.INFO,
                    "[RUN] strategy decision",
                    strategy=strategy_name,
                    decision_type=str(context.get("decision_type") or "-"),
                    raw_signal=str(context.get("raw_signal") or context.get("base_signal") or signal),
                    final_signal=signal,
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
                    reason=reason,
                )
                try:
                    decision_id = record_strategy_decision(
                        conn,
                        decision_ts=int(now * 1000),
                        strategy_name=strategy_name,
                        signal=signal,
                        reason=reason,
                        candle_ts=int(candle_ts_raw) if candle_ts_raw is not None else None,
                        market_price=float(market_price_raw) if market_price_raw is not None else None,
                        confidence=float(confidence_raw) if confidence_raw is not None else None,
                        context=context,
                    )
                    _persist_target_position_state_for_run_loop(
                        conn,
                        execution_decision=execution_decision,
                        signal=signal,
                        decision_id=decision_id,
                        updated_ts=int(now * 1000),
                    )
                    conn.commit()
                except Exception as exc:
                    _log_loop_event(
                        logging.WARNING,
                        "[WARN] strategy decision persistence failed",
                        error=f"{type(exc).__name__}: {exc}",
                        strategy=strategy_name,
                        signal=signal,
                    )
            finally:
                conn.close()

            if decision_id is None:
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
                    strategy_decision_hash=_artifact_hash(decision_context_for_trade or {}),
                    execution_plan_bundle_hash=_artifact_hash(execution_plan_bundle_for_trade),
                )
                continue

            execution_coordinator = ExecutionCoordinator(
                str(getattr(settings, "EXECUTION_ENGINE", "lot_native") or "lot_native")
            )
            submit_expectation = execution_coordinator.resolve_submit_expectation(
                execution_decision_summary_for_trade
            )
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
                    strategy_decision_hash=_artifact_hash(decision_context_for_trade or {}),
                    execution_plan_bundle_hash=_artifact_hash(execution_plan_bundle_for_trade),
                )
                continue
            apply_processed_candle_checkpoint(candle_ts_ms=int(r["ts"]), now_epoch_sec=now)
            _record_runtime_cycle_artifact(
                "checkpoint:processed",
                candle_ts=int(r["ts"]),
                startup_state="READY",
                strategy_decision_hash=_artifact_hash(decision_context_for_trade or {}),
                execution_plan_bundle_hash=_artifact_hash(execution_plan_bundle_for_trade),
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
            target_delta_submit = execution_coordinator.target_delta_submit_expected(submit_expected=submit_expectation.submit_expected)
            authoritative_execution_signal = authoritative_execution_signal_for_trade(
                decision_context_for_trade,
                fallback_signal=r["signal"],
            )
            if authoritative_execution_signal not in ("BUY", "SELL") and not target_delta_submit:
                continue

            trade = None
            if execution_service is not None:
                try:
                    trade = execution_service.execute(
                        build_signal_execution_request(
                            signal=authoritative_execution_signal,
                            ts=r["ts"],
                            market_price=float(r["last_close"]),
                            strategy_name=decision_strategy_name_for_trade,
                            decision_id=decision_id,
                            decision_reason=decision_reason_for_trade,
                            exit_rule_name=decision_exit_rule_name,
                            execution_decision_summary=execution_decision_summary_for_trade,
                            decision_context=decision_context_for_trade,
                            execution_plan_bundle=execution_plan_bundle_for_trade,
                        )
                    )
                except BrokerError as e:
                    _halt_trading(
                        _halt_reason(
                            "LIVE_EXECUTION_BROKER_ERROR",
                            f"live execution broker error ({type(e).__name__}): {e}",
                        ),
                        unresolved=True,
                    )
                    continue
                except Exception as e:
                    _halt_trading(
                        _halt_reason(
                            "LIVE_EXECUTION_FAILED",
                            f"live execution failed ({type(e).__name__}): {e}",
                        ),
                        unresolved=True,
                    )
                    continue
                if settings.MODE == "live" and broker is not None:
                    try:
                        reconcile_with_broker(broker)
                    except Exception as e:
                        _halt_trading(
                            _halt_reason(
                                "POST_TRADE_RECONCILE_FAILED",
                                f"reconcile failed ({type(e).__name__}): {e}",
                            ),
                            unresolved=True,
                        )
                        continue

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
