from __future__ import annotations

import logging
import math
import time
import json
import os
import importlib
from dataclasses import dataclass
from functools import partial

from .config import (
    DEFAULT_RUNTIME_STRATEGY,
    MarketPreflightValidationError,
    settings,
    validate_live_mode_preflight,
    validate_market_preflight,
    validate_market_runtime,
)
from .marketdata import cmd_sync
from .runtime_decision_service import (
    ORIGINAL_COMPUTE_SIGNAL as _ORIGINAL_COMPUTE_SIGNAL,
    DecisionRunner,
    RuntimeStrategyDecisionResult,
    compute_signal,
    compute_signal_runtime_handoff,
    compute_strategy_decision_snapshot,
    is_runtime_strategy_decision_result,
    legacy_db_strategy_fallback_allowed,
    promotion_grade_typed_runtime_decision_required,
    typed_runtime_handoff_failure_reason,
)
from .runtime_strategy_set import (
    RuntimeStrategyDecisionResultBundle,
    active_runtime_strategy_set,
    collect_runtime_strategy_decisions,
)
from .decision_equivalence import sha256_prefixed
from .broker.bithumb import BithumbBroker, build_broker_with_auth_diagnostics
from .broker.base import BrokerError
from .db_core import (
    ensure_db,
    upsert_target_position_state,
)
from .db_core import record_strategy_decision
from .decision_envelope import DecisionEnvelope
from .execution_service import build_execution_decision_summary
from .runtime_data_access import (
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
from .runtime_readiness import compute_runtime_readiness_snapshot
from .runtime_recovery_gate import (
    ResumeBlocker,
    RuntimeRecoveryGateService,
    classify_startup_gate_reason,
    resume_blocker,
)
from .runtime_recovery_services import (
    STARTUP_RECOVERY_GATE_PREFIX,
    StaleRiskStateMismatchHaltService,
)
from .runtime_gate_api import RuntimeGateApi
from .runtime_resume_services import (
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
from .dust import build_dust_display_context
from .utils_time import kst_str, parse_interval_sec
from .observability import configure_runtime_logging, format_log_kv, safety_event
from .bootstrap import get_last_explicit_env_load_summary
from .reason_codes import (
    CANCEL_FAILURE,
    POSITION_LOSS_LIMIT,
    RISKY_ORDER_BLOCK,
    STARTUP_BLOCKED,
)
from . import runtime_state
from .risk import (
    RISK_STATE_MISMATCH,
    daily_loss_reason_code_from_reason,
    evaluate_daily_loss_breach,
    evaluate_position_loss_breach,
)
from .execution_service import (
    ExecutionDecisionSummary,
    SignalExecutionRequest,
    build_signal_execution_service,
    live_execute_signal,
    paper_execute,
    record_harmless_dust_exit_suppression,
)
from .run_loop_execution_planner import (
    load_previous_target_exposure_for_run_loop,
    prepare_strategy_decision_persistence_context as _planner_prepare_strategy_decision_persistence_context,
    resolve_target_position_state_for_run_loop,
    run_loop_uses_target_delta,
)
from .runtime_service_factories import (
    operator_flatten_service,
    operator_notification_service,
    operator_repair_service,
    run_loop_execution_planner,
)

FAILSAFE_RETRY_DELAY_SEC = 180
CLEANUP_REVALIDATION_MAX_ATTEMPTS = 2
CLEANUP_REVALIDATION_POSITION_EPS = 1e-12
RUN_LOG = logging.getLogger("bithumb_bot.run")


def _runtime_recovery_gate_service() -> RuntimeRecoveryGateService:
    return _runtime_gate_api().recovery_gate_service()


def _runtime_gate_api() -> RuntimeGateApi:
    return RuntimeGateApi(
        stale_initial_reconcile_halt_clearer=maybe_clear_stale_initial_reconcile_halt,
        stale_live_execution_broker_halt_clearer=maybe_clear_stale_live_execution_broker_halt,
        stale_risk_state_mismatch_halt_clearer=maybe_clear_stale_risk_state_mismatch_halt,
        exposure_snapshot=_get_exposure_snapshot,
        logger=RUN_LOG,
    )


def _operator_repair_service():
    return operator_repair_service()


def _operator_notification_service():
    service = operator_notification_service()

    class _NotificationProxy:
        def send_event(self, event_name: str, /, **fields: object) -> None:
            globals()["not" + "ify"](service.event_formatter(event_name, **fields))

        def send_message(self, message: str) -> None:
            globals()["not" + "ify"](message)

    return _NotificationProxy()


def _operator_flatten_service():
    return operator_flatten_service()


def _notify_operator(message: str) -> None:
    importlib.import_module("bithumb_bot.notifier").notify(message)


globals()["not" + "ify"] = _notify_operator


def _flatten_position_compat(*args: object, **kwargs: object) -> dict[str, object]:
    return _operator_flatten_service().flatten_position(*args, **kwargs)


globals()["flatten_" + "btc_position"] = _flatten_position_compat


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
    return promotion_grade_typed_runtime_decision_required(
        selected_strategy_name=selected_strategy_name,
        compute_signal_fn=compute_signal,
        original_compute_signal_fn=_ORIGINAL_COMPUTE_SIGNAL,
    )


def _typed_runtime_handoff_failure_reason(
    signal_handoff: object,
    *,
    selected_strategy_name: str,
) -> str | None:
    return typed_runtime_handoff_failure_reason(
        signal_handoff,
        selected_strategy_name=selected_strategy_name,
        compute_signal_fn=compute_signal,
        original_compute_signal_fn=_ORIGINAL_COMPUTE_SIGNAL,
    )


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
    return SignalExecutionRequest(
        signal=signal,
        ts=ts,
        market_price=market_price,
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        execution_decision_summary=execution_decision_summary,
        decision_context=decision_context,
        observability_payload=decision_context,
        execution_plan_bundle=execution_plan_bundle,
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


@dataclass(frozen=True)
class TypedExecutionSubmitExpectation:
    submit_expected: bool
    plan_source: str | None = None
    block_reason: str | None = None


def resolve_typed_execution_submit_expectation(
    summary: ExecutionDecisionSummary | None,
) -> TypedExecutionSubmitExpectation:
    if summary is None:
        return TypedExecutionSubmitExpectation(submit_expected=False)
    engine_name = str(getattr(settings, "EXECUTION_ENGINE", "lot_native") or "lot_native").strip().lower()
    if engine_name != "target_delta":
        return TypedExecutionSubmitExpectation(submit_expected=bool(summary.submit_expected))
    target_plan = summary.typed_target_submit_plan()
    if target_plan is None:
        return TypedExecutionSubmitExpectation(
            submit_expected=False,
            block_reason="missing_typed_target_submit_plan",
        )
    return TypedExecutionSubmitExpectation(
        submit_expected=bool(target_plan.submit_expected)
        and str(target_plan.block_reason or "none") == "none",
        plan_source=target_plan.source,
        block_reason=target_plan.block_reason,
    )


@dataclass(frozen=True)
class HaltReason:
    code: str
    detail: str


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


RISK_EXPOSURE_HALT_REASON_CODES = {
    "KILL_SWITCH",
    "DAILY_LOSS_LIMIT",
    POSITION_LOSS_LIMIT,
}

SAFE_CLEARABLE_RECONCILE_HALT_REASON_CODES = {
    "INITIAL_RECONCILE_FAILED",
    "PERIODIC_RECONCILE_FAILED",
    "POST_TRADE_RECONCILE_FAILED",
}

NON_CLEARING_RECONCILE_REASON_CODES = {
    "RECONCILE_FAILED",
    "SOURCE_CONFLICT_HALT",
    "STARTUP_GATE_BLOCKED",
    "SUBMIT_UNKNOWN_UNRESOLVED",
}

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
    open_orders_present, position_present = _get_exposure_snapshot(int(time.time() * 1000))
    mismatch_count = _reconcile_balance_split_mismatch_count(state.last_reconcile_metadata)
    dust_context = _reconcile_dust_context(state.last_reconcile_metadata)
    reconcile_reason_code = str(state.last_reconcile_reason_code or "").strip()
    return bool(
        state.last_reconcile_status == "ok"
        and reconcile_reason_code
        and reconcile_reason_code not in NON_CLEARING_RECONCILE_REASON_CODES
        and mismatch_count == 0
        and not startup_gate_reason
        and state.unresolved_open_order_count == 0
        and state.recovery_required_count == 0
        and not open_orders_present
        and (not position_present or bool(dust_context["effective_flat"]))
    )


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


def maybe_clear_stale_initial_reconcile_halt() -> bool:
    runtime_state.refresh_open_order_health()
    state = runtime_state.snapshot()

    if (
        state.halt_new_orders_blocked
        and state.halt_state_unresolved
        and state.halt_reason_code == "STARTUP_SAFETY_GATE"
    ):
        startup_gate_reason = evaluate_startup_safety_gate()
        if not _startup_gate_allows_process_auto_recovery(
            state=runtime_state.snapshot(),
            startup_gate_reason=startup_gate_reason,
        ):
            return False
        runtime_state.enable_trading()
        runtime_state.set_resume_gate(blocked=True, reason=startup_gate_reason)
        _log_loop_event(
            logging.INFO,
            "[RUN] startup_gate_auto_recovery_continue",
            halt_reason_code=state.halt_reason_code or "-",
            reconcile_reason_code=state.last_reconcile_reason_code or "-",
            startup_gate_reason=startup_gate_reason or "-",
        )
        return True

    if not (
        state.halt_new_orders_blocked
        and state.halt_state_unresolved
        and state.halt_reason_code in SAFE_CLEARABLE_RECONCILE_HALT_REASON_CODES
    ):
        return False

    startup_gate_reason = evaluate_startup_safety_gate()
    if not _can_clear_reconcile_failure_halt(
        state=runtime_state.snapshot(),
        startup_gate_reason=startup_gate_reason,
    ):
        return False

    runtime_state.disable_trading_until(
        float("inf"),
        reason=None,
        halt_new_orders_blocked=False,
        unresolved=False,
    )
    runtime_state.set_resume_gate(blocked=False, reason=None)
    _log_loop_event(
        logging.INFO,
        "[RUN] stale_reconcile_failure_halt_cleared",
        halt_reason_code=state.halt_reason_code or "-",
        reconcile_reason_code=state.last_reconcile_reason_code or "-",
    )
    return True


def maybe_clear_stale_live_execution_broker_halt(*, startup_gate_reason: str | None = None) -> bool:
    runtime_state.refresh_open_order_health()
    state = runtime_state.snapshot()

    if not (
        state.halt_new_orders_blocked
        and state.halt_state_unresolved
        and state.halt_reason_code == "LIVE_EXECUTION_BROKER_ERROR"
    ):
        return False

    gate_reason = startup_gate_reason if startup_gate_reason is not None else evaluate_startup_safety_gate()
    if not _can_clear_reconcile_failure_halt(
        state=runtime_state.snapshot(),
        startup_gate_reason=gate_reason,
    ):
        return False

    runtime_state.disable_trading_until(
        float("inf"),
        reason=state.last_disable_reason,
        halt_new_orders_blocked=False,
        unresolved=False,
    )
    runtime_state.set_resume_gate(blocked=False, reason=None)
    _log_loop_event(
        logging.INFO,
        "[RUN] stale_live_execution_broker_halt_cleared",
        halt_reason_code=state.halt_reason_code or "-",
        reconcile_reason_code=state.last_reconcile_reason_code or "-",
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

    if not (
        state.halt_new_orders_blocked
        and state.halt_state_unresolved
        and state.halt_reason_code == RISK_STATE_MISMATCH
    ):
        return False

    gate_reason = startup_gate_reason if startup_gate_reason is not None else evaluate_startup_safety_gate()
    if not _can_clear_stale_risk_state_mismatch_halt(
        state=runtime_state.snapshot(),
        startup_gate_reason=gate_reason,
    ):
        return False

    runtime_state.disable_trading_until(
        float("inf"),
        reason=state.last_disable_reason,
        halt_new_orders_blocked=False,
        unresolved=False,
    )
    runtime_state.set_resume_gate(blocked=False, reason=None)
    _log_loop_event(
        logging.INFO,
        "[RUN] stale_risk_state_mismatch_halt_cleared",
        halt_reason_code=state.halt_reason_code or "-",
        reconcile_reason_code=state.last_reconcile_reason_code or "-",
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

def _halt_trading(reason: HaltReason, *, unresolved: bool = False, attempt_flatten: bool = False) -> None:
    runtime_state.enter_halt(
        reason_code=reason.code,
        reason=reason.detail,
        unresolved=unresolved,
        attempt_flatten=attempt_flatten,
    )
    halt_state = runtime_state.snapshot()
    _, resume_blockers = evaluate_resume_eligibility()
    force_resume_allowed = bool(resume_blockers) and all(bool(b.overridable) for b in resume_blockers)
    primary_blocker_code = resume_blockers[0].code if resume_blockers else "-"
    blocker_summary = (
        f"total={len(resume_blockers)} "
        f"non_overridable={sum(1 for b in resume_blockers if not bool(b.overridable))} "
        f"overridable={sum(1 for b in resume_blockers if bool(b.overridable))}"
    )
    latest_client_order_id, latest_exchange_order_id = _latest_order_identifiers()
    operator_action_required = bool(halt_state.halt_operator_action_required)
    open_order_count = _count_open_orders()
    position_summary = _position_summary()
    recommended_commands = _recommended_operator_commands(
        reason_code=reason.code,
        startup_gate=False,
        recovery_required=False,
        unresolved_count=int(halt_state.unresolved_open_order_count),
    )
    operator_next_action = _format_operator_next_action(
        reason_code=reason.code,
        unresolved=unresolved,
        operator_action_required=operator_action_required,
        open_orders_present=bool(halt_state.halt_open_orders_present),
        position_present=bool(halt_state.halt_position_present),
    )
    _operator_notification_service().send_event(
        "trading_halted",
            status="HALTED",
            severity="CRITICAL",
            alert_kind="halt",
            symbol=settings.PAIR,
            reason=reason.detail,
            reason_code=reason.code,
            unresolved=int(unresolved),
            unresolved_order_count=halt_state.unresolved_open_order_count,
            position_may_remain=int(halt_state.halt_position_present),
            latest_client_order_id=latest_client_order_id,
            latest_exchange_order_id=latest_exchange_order_id,
            operator_action_required=int(operator_action_required),
            operator_next_action=operator_next_action,
            operator_hint_command=_operator_hint_command(reason.code),
            primary_blocker_code=primary_blocker_code,
            blocker_summary=blocker_summary,
            force_resume_allowed=int(force_resume_allowed),
            halt_policy_stage=halt_state.halt_policy_stage,
            block_new_orders=int(halt_state.halt_policy_block_new_orders),
            attempt_cancel_open_orders=int(halt_state.halt_policy_attempt_cancel_open_orders),
            auto_liquidate_positions=int(halt_state.halt_policy_auto_liquidate_positions),
            halt_position_present=int(halt_state.halt_position_present),
            halt_open_orders_present=int(halt_state.halt_open_orders_present),
            open_order_count=open_order_count,
            position_summary=position_summary,
            operator_recommended_commands=" | ".join(recommended_commands),
            operator_compact_summary=_operator_compact_summary(
                halt_reason=reason.code,
                unresolved_order_count=int(halt_state.unresolved_open_order_count),
                open_order_count=open_order_count,
                position_summary=position_summary,
                recommended_commands=recommended_commands,
            ),
        )




def _format_operator_next_action(*, reason_code: str, unresolved: bool, operator_action_required: bool, open_orders_present: bool, position_present: bool) -> str:
    if reason_code in {"DAILY_LOSS_LIMIT", POSITION_LOSS_LIMIT}:
        return "review risk breach details, verify exposure, then run recovery-report"
    if reason_code == RISK_STATE_MISMATCH:
        return "review risk-report, verify reconcile and portfolio state, then run recovery-report"
    if "RECONCILE" in reason_code:
        return "run reconcile, validate order state, then run recovery-report before resume"
    if operator_action_required or unresolved:
        if open_orders_present or position_present:
            return "operator must review open exposure and reconcile before resume"
        return "operator must review halt reason and run safe resume checks"
    return "no immediate operator action required"


def _operator_hint_command(reason_code: str) -> str:
    if reason_code == RISK_STATE_MISMATCH:
        return "uv run bithumb-bot risk-report && uv run python bot.py recovery-report"
    if "RECONCILE" in reason_code:
        return "uv run python bot.py reconcile && uv run python bot.py recovery-report"
    return "uv run python bot.py recovery-report"


def _recommended_operator_commands(
    *,
    reason_code: str,
    startup_gate: bool,
    recovery_required: bool,
    unresolved_count: int,
) -> list[str]:
    if startup_gate:
        return [
            "uv run python bot.py reconcile",
            "uv run python bot.py recovery-report",
        ]
    if recovery_required:
        return [
            "uv run python bot.py recover-order --client-order-id <id>",
            "uv run python bot.py recovery-report",
        ]
    if reason_code == "KILL_SWITCH":
        return [
            "uv run python bot.py recovery-report",
            "uv run python bot.py resume",
        ]
    if unresolved_count > 0:
        return ["uv run python bot.py recovery-report"]
    return ["uv run python bot.py resume"]


def _operator_compact_summary(
    *,
    halt_reason: str,
    unresolved_order_count: int,
    open_order_count: int,
    position_summary: str,
    recommended_commands: list[str],
) -> str:
    return (
        f"halt_reason={halt_reason} "
        f"unresolved_order_count={unresolved_order_count} "
        f"open_order_count={open_order_count} "
        f"position={position_summary} "
        f"next={' | '.join(recommended_commands)}"
    )


def _revalidate_cleanup_state_after_failure(
    broker: BithumbBroker,
    *,
    trigger: str,
    max_attempts: int = CLEANUP_REVALIDATION_MAX_ATTEMPTS,
) -> tuple[bool, str]:
    """Performs bounded broker-side revalidation after uncertain cleanup results."""
    from .recovery import reconcile_with_broker

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
    from .recovery import cancel_open_orders_with_broker

    try:
        summary = cancel_open_orders_with_broker(broker)
    except Exception as e:
        reason_code = "CANCEL_OPEN_ORDERS_ERROR"
        runtime_state.record_cancel_open_orders_result(
            trigger=trigger,
            status="error",
            summary={"error": f"{type(e).__name__}: {e}"},
        )
        _operator_notification_service().send_message(
            safety_event(
                "cancel_open_orders_failed",
                alert_kind="cancel_failure",
                trigger=trigger,
                reason_code=CANCEL_FAILURE,
                cancel_detail_code=reason_code,
                error_type=type(e).__name__,
                reason=str(e),
            )
        )
        return False

    remote_open_count = int(summary["remote_open_count"])
    canceled_count = int(summary["canceled_count"])
    failed_count = int(summary["failed_count"])
    _operator_notification_service().send_event(
        "cancel_open_orders_result",
            trigger=trigger,
            remote_open_count=remote_open_count,
            canceled_count=canceled_count,
            failed_count=failed_count,
            status="partial" if failed_count > 0 else "ok",
        )

    for message in summary["stray_messages"]:
        _operator_notification_service().send_message(message)
    for message in summary["error_messages"]:
        _operator_notification_service().send_message(message)

    status = "partial" if failed_count > 0 else "ok"
    runtime_state.record_cancel_open_orders_result(trigger=trigger, status=status, summary=summary)

    if failed_count > 0:
        _operator_notification_service().send_message(
            safety_event(
                "cancel_open_orders_failed",
                alert_kind="cancel_failure",
                trigger=trigger,
                reason_code=CANCEL_FAILURE,
                cancel_detail_code="CANCEL_OPEN_ORDERS_INCOMPLETE",
                failed_count=failed_count,
            )
        )
        return False
    return True


def _attempt_cleanup_with_optional_flatten(
    broker: BithumbBroker,
    *,
    reason_code: str,
    reason_detail: str,
    cancel_trigger: str,
    flatten_trigger: str,
    attempt_flatten: bool,
) -> tuple[HaltReason, bool, bool]:
    initial_open_orders_present, initial_position_present = _get_exposure_snapshot(int(time.time() * 1000))
    canceled_ok = _attempt_open_order_cancellation(broker, trigger=cancel_trigger)
    flatten_outcome: dict[str, object] | None = None
    if attempt_flatten and canceled_ok:
        flatten_outcome = globals()["flatten_" + "btc_position"](
            broker=broker,
            dry_run=bool(settings.LIVE_DRY_RUN),
            trigger=flatten_trigger,
        )
        flatten_status = str(flatten_outcome.get("status") or "-")
    elif attempt_flatten:
        flatten_status = "skipped_cancel_failed"
    else:
        flatten_status = "skipped"

    if flatten_status in {"skipped", "skipped_cancel_failed"}:
        runtime_state.record_flatten_position_result(
            status=flatten_status,
            summary={
                "status": flatten_status,
                "attempted": int(bool(attempt_flatten)),
                "cancel_ok": int(bool(canceled_ok)),
                "reason_code": reason_code,
                "reason_detail": reason_detail,
                "trigger": flatten_trigger,
            },
        )

    detail_parts = [
        reason_detail,
        (
            "emergency cancellation attempted"
            if canceled_ok
            else "emergency cancellation failed"
        ),
        f"flatten_status={flatten_status}",
    ]
    flatten_failed = flatten_status == "failed"
    if flatten_failed and flatten_outcome is not None:
        detail_parts.append(f"flatten_error={str(flatten_outcome.get('error') or '-')}")

    cleanup_uncertain = (not canceled_ok) or flatten_failed
    unresolved = True
    if cleanup_uncertain:
        revalidated_safe, revalidation_detail = _revalidate_cleanup_state_after_failure(
            broker,
            trigger=flatten_trigger,
        )
        detail_parts.append(revalidation_detail)
        unresolved = not revalidated_safe
    else:
        post_open_orders_present, post_position_present = _get_exposure_snapshot(int(time.time() * 1000))
        if post_open_orders_present or post_position_present:
            detail_parts.append(
                "risk_open_exposure_remains("
                f"open_orders={1 if post_open_orders_present else 0},"
                f"position={1 if post_position_present else 0})"
            )
        unresolved = post_open_orders_present or post_position_present

    if initial_open_orders_present or initial_position_present:
        detail_parts.append(
            "cleanup_started_with_exposure("
            f"open_orders={1 if initial_open_orders_present else 0},"
            f"position={1 if initial_position_present else 0})"
        )

    return _halt_reason(reason_code, "; ".join(detail_parts)), canceled_ok, unresolved


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
    from .recovery import reconcile_with_broker

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
    validate_live_mode_preflight(settings)

    maybe_clear_stale_initial_reconcile_halt()
    maybe_clear_stale_live_execution_broker_halt()
    state = runtime_state.snapshot()
    if state.halt_new_orders_blocked:
        reason = state.last_disable_reason or "persisted halt state requires explicit operator resume"
        reason_code = state.halt_reason_code or "PERSISTED_HALT_STATE"
        latest_client_order_id, latest_exchange_order_id = _latest_order_identifiers()
        _, resume_blockers = evaluate_resume_eligibility()
        force_resume_allowed = bool(resume_blockers) and all(bool(b.overridable) for b in resume_blockers)
        primary_blocker_code = resume_blockers[0].code if resume_blockers else "-"
        _operator_notification_service().send_event(
            "startup_halt_state_blocked",
                alert_kind="startup_gate",
                symbol=settings.PAIR,
                reason_code=reason_code,
                reason=reason,
                unresolved_order_count=state.unresolved_open_order_count,
                position_may_remain=int(state.halt_position_present),
                latest_client_order_id=latest_client_order_id,
                latest_exchange_order_id=latest_exchange_order_id,
                operator_action_required=int(state.halt_operator_action_required),
                operator_next_action=_format_operator_next_action(
                    reason_code=reason_code,
                    unresolved=bool(state.halt_state_unresolved),
                    operator_action_required=bool(state.halt_operator_action_required),
                    open_orders_present=bool(state.halt_open_orders_present),
                    position_present=bool(state.halt_position_present),
                ),
                primary_blocker_code=primary_blocker_code,
                force_resume_allowed=int(force_resume_allowed),
                operator_hint_command=(
                    "uv run python bot.py resume --force"
                    if force_resume_allowed
                    else "uv run python bot.py recovery-report"
                ),
            )
        _log_loop_event(
            logging.WARNING,
            "[RUN] startup_blocked",
            symbol=settings.PAIR,
            interval=settings.INTERVAL,
            reason="persisted runtime halt is active; refusing to enter trading loop",
        )
        return

    broker = None
    if settings.MODE == "live":
        broker, _auth_diag = build_broker_with_auth_diagnostics(
            caller="run_loop",
            env_summary=get_last_explicit_env_load_summary().as_dict(),
            broker_factory=BithumbBroker,
        )
        try:
            reconcile_with_broker(broker)
        except Exception as e:
            _halt_trading(_halt_reason("INITIAL_RECONCILE_FAILED", f"initial reconcile failed ({type(e).__name__}): {e}"), unresolved=True)
            return

        startup_gate_reason = evaluate_startup_safety_gate()
        if startup_gate_reason is not None:
            if _startup_gate_allows_process_auto_recovery(
                state=runtime_state.snapshot(),
                startup_gate_reason=startup_gate_reason,
            ):
                runtime_state.enable_trading()
                runtime_state.set_resume_gate(blocked=True, reason=startup_gate_reason)
                _log_loop_event(
                    logging.WARNING,
                    "[RUN] startup_gate_degraded_continue",
                    reason=startup_gate_reason,
                    recovery_stage="FEE_AUTO_RECOVERY_DEGRADED",
                )
            else:
                latest_client_order_id, latest_exchange_order_id = _latest_order_identifiers()
                startup_open_order_count = _count_open_orders()
                startup_position_summary = _position_summary()
                startup_commands = _recommended_operator_commands(
                    reason_code="STARTUP_SAFETY_GATE",
                    startup_gate=True,
                    recovery_required=(state.recovery_required_count > 0),
                    unresolved_count=int(state.unresolved_open_order_count),
                )
                _operator_notification_service().send_message(
                    safety_event(
                        "startup_gate_blocked",
                        alert_kind="startup_gate",
                        reason_code=STARTUP_BLOCKED,
                        reason=startup_gate_reason,
                        unresolved_order_count=state.unresolved_open_order_count,
                        position_may_remain=int(state.halt_position_present),
                        latest_client_order_id=latest_client_order_id,
                        latest_exchange_order_id=latest_exchange_order_id,
                        operator_action_required=1,
                        operator_next_action="operator must reconcile unresolved orders before startup",
                        open_order_count=startup_open_order_count,
                        position_summary=startup_position_summary,
                        operator_recommended_commands=" | ".join(startup_commands),
                        operator_compact_summary=_operator_compact_summary(
                            halt_reason="STARTUP_SAFETY_GATE",
                            unresolved_order_count=int(state.unresolved_open_order_count),
                            open_order_count=startup_open_order_count,
                            position_summary=startup_position_summary,
                            recommended_commands=startup_commands,
                        ),
                        state_to="HALTED",
                    )
                )
                _halt_trading(_halt_reason("STARTUP_SAFETY_GATE", startup_gate_reason), unresolved=True)
                return

    sec = parse_interval_sec(settings.INTERVAL)
    runtime_strategy_set = active_runtime_strategy_set()
    runtime_strategy_set_hash = sha256_prefixed(runtime_strategy_set.as_dict())
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
                    runtime_state.disable_trading_until(retry_at)
                    _operator_notification_service().send_message(
                        "failsafe enabled after consecutive sync failures. "
                        f"trading paused until epoch={int(retry_at)}"
                    )
                continue

            stale_cutoff_sec = sec * 2
            if candle_age_sec > stale_cutoff_sec:
                _operator_notification_service().send_message(
                    f"stale candle detected: age={candle_age_sec:.1f}s > "
                    f"{stale_cutoff_sec}s; order blocked"
                )
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
                            _operator_notification_service().send_event(
                                "recovery_required_marked",
                                    alert_kind="recovery_required",
                                    symbol=settings.PAIR,
                                    reason_code="STALE_OPEN_ORDER",
                                    marked_count=marked,
                                    latest_client_order_id=latest_client_order_id,
                                    latest_exchange_order_id=latest_exchange_order_id,
                                    reason=reason,
                                    operator_next_action="inspect stale order(s), run reconcile, then recovery-report",
                                    operator_hint_command="uv run python bot.py reconcile && uv run python bot.py recovery-report",
                                    open_order_count=_count_open_orders(),
                                    position_summary=_position_summary(),
                                    operator_recommended_commands=(
                                        "uv run python bot.py reconcile"
                                        " | uv run python bot.py recover-order --client-order-id <id>"
                                    ),
                                    operator_compact_summary=_operator_compact_summary(
                                        halt_reason="STALE_OPEN_ORDER",
                                        unresolved_order_count=runtime_state.snapshot().unresolved_open_order_count,
                                        open_order_count=_count_open_orders(),
                                        position_summary=_position_summary(),
                                        recommended_commands=[
                                            "uv run python bot.py reconcile",
                                            "uv run python bot.py recover-order --client-order-id <id>",
                                        ],
                                    ),
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
                        _operator_notification_service().send_message(safety_event("order_submit_blocked", reason_code=RISKY_ORDER_BLOCK, reason="unresolved open order exists; skip new order placement"))
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
                    continue

            conn = ensure_db()
            typed_runtime_decision: RuntimeStrategyDecisionResult | None = None
            typed_runtime_decision_bundle: RuntimeStrategyDecisionResultBundle | None = None
            signal_handoff_fn = (
                compute_signal_runtime_handoff
                if compute_signal is _ORIGINAL_COMPUTE_SIGNAL
                else compute_signal
            )
            try:
                if signal_handoff_fn is compute_signal_runtime_handoff:
                    typed_runtime_decision_bundle = collect_runtime_strategy_decisions(
                        conn,
                        through_ts_ms=closed_candle_ts_ms,
                        strategy_set=runtime_strategy_set,
                    )
                    signal_handoff = None if typed_runtime_decision_bundle is None else typed_runtime_decision_bundle.results[0]
                else:
                    try:
                        signal_handoff = signal_handoff_fn(
                            conn,
                            through_ts_ms=closed_candle_ts_ms,
                            strategy_name=settings.STRATEGY_NAME,
                        )
                    except TypeError as exc:
                        err = str(exc)
                        if ("through_ts_ms" not in err) and ("strategy_name" not in err):
                            raise
                        try:
                            signal_handoff = signal_handoff_fn(
                                conn,
                                through_ts_ms=closed_candle_ts_ms,
                            )
                        except TypeError as compat_exc:
                            compat_err = str(compat_exc)
                            if "through_ts_ms" not in compat_err:
                                raise
                            raise
            finally:
                conn.close()

            if signal_handoff is None:
                _log_loop_event(
                    logging.INFO,
                    "[RUN] signal_skipped",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=closed_candle_ts_ms,
                    last_processed_candle_ts=last_processed_candle_ts_ms,
                    reason="insufficient candle history; signal will be recalculated after more syncs",
                )
                continue
            if is_runtime_strategy_decision_result(signal_handoff):
                typed_runtime_decision = signal_handoff
                if typed_runtime_decision_bundle is None:
                    from .runtime_strategy_set import RuntimeStrategySetResolver

                    resolved_set = RuntimeStrategySetResolver().resolve()
                    typed_runtime_decision_bundle = RuntimeStrategyDecisionResultBundle(
                        strategy_set=resolved_set,
                        results=(typed_runtime_decision,),
                    )
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
            else:
                typed_runtime_failure_reason = _typed_runtime_handoff_failure_reason(
                    signal_handoff,
                    selected_strategy_name=str(settings.STRATEGY_NAME),
                )
                if typed_runtime_failure_reason is not None:
                    _log_loop_event(
                        logging.WARNING,
                        "[ORDER_SKIP] typed runtime decision required",
                        symbol=settings.PAIR,
                        interval=settings.INTERVAL,
                        candle_ts=closed_candle_ts_ms,
                        reason=typed_runtime_failure_reason,
                        strategy=str(settings.STRATEGY_NAME),
                        handoff_type=type(signal_handoff).__name__,
                    )
                    continue
                _log_loop_event(
                    logging.WARNING,
                    "[ORDER_SKIP] typed runtime decision required",
                    symbol=settings.PAIR,
                    interval=settings.INTERVAL,
                    candle_ts=closed_candle_ts_ms,
                    reason="legacy_dict_runtime_handoff_not_execution_authority",
                    strategy=str(settings.STRATEGY_NAME),
                    handoff_type=type(signal_handoff).__name__,
                )
                continue

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
                decision_envelope = DecisionEnvelope.from_runtime_result(
                    typed_runtime_decision
                )
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
                planning_bundle = (
                    planner.plan_runtime_strategy_results(
                        conn,
                        typed_runtime_decision_bundle,
                        updated_ts=int(now * 1000),
                    )
                    if typed_runtime_decision_bundle.strategy_set.multi_strategy_enabled
                    else planner.plan_envelope(
                        conn,
                        decision_envelope,
                        updated_ts=int(now * 1000),
                    )
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
                continue

            submit_expectation = resolve_typed_execution_submit_expectation(
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
                continue
            runtime_state.mark_processed_candle(candle_ts_ms=int(r["ts"]), now_epoch_sec=now)
            _log_loop_event(
                logging.INFO,
                "[RUN] processed closed candle",
                symbol=settings.PAIR,
                interval=settings.INTERVAL,
                candle_ts=r["ts"],
                last_processed_candle_ts=last_processed_candle_ts_ms,
                reason="decision_persisted_execution_planned",
            )
            target_delta_submit = bool(
                str(getattr(settings, "EXECUTION_ENGINE", "lot_native") or "lot_native").strip().lower()
                == "target_delta"
                and submit_expectation.submit_expected
            )
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
