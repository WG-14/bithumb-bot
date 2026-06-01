from __future__ import annotations

from dataclasses import replace

from .config import settings
from .execution_service import build_execution_decision_summary
from .run_loop_execution_planner import prepare_strategy_decision_persistence_context
from .runtime.execution_coordinator import (
    resolve_typed_execution_submit_expectation as _resolve_typed_execution_submit_expectation,
)
from .runtime.cleanup_revalidation import (
    revalidate_cleanup_state_after_failure_compat as _revalidate_cleanup_state_after_failure,
)
from .runtime.runner import (
    ResumeBlocker,
    _attempt_open_order_cancellation,
    _classify_balance_split_blocker,
    _close_guard_ms,
    _is_closed_candle,
    _legacy_db_strategy_fallback_allowed,
    _load_previous_target_exposure_for_run_loop,
    _persist_target_position_state_for_run_loop,
    _promotion_grade_typed_runtime_decision_required,
    _resolve_target_position_state_for_run_loop,
    _select_latest_closed_candle,
    _typed_runtime_handoff_failure_reason,
    authoritative_execution_signal_for_trade,
    build_resume_guidance,
    build_signal_execution_request,
    compute_strategy_decision_snapshot,
    evaluate_restart_readiness,
    get_stale_risk_state_mismatch_halt_diagnostics,
    maybe_clear_stale_initial_reconcile_halt,
)
from .runtime.public_api import (
    evaluate_resume_eligibility,
    evaluate_startup_safety_gate,
    get_health_status,
    perform_panic_stop_cleanup,
)


def resolve_typed_execution_submit_expectation(summary):
    return _resolve_typed_execution_submit_expectation(
        summary,
        execution_engine_name=str(getattr(settings, "EXECUTION_ENGINE", "lot_native") or "lot_native"),
    )


def run_loop() -> None:
    from .runtime.app_container import create_default_runtime_app
    from .compat import engine_legacy as legacy
    from .decision_equivalence import sha256_prefixed

    class _LegacyScheduler:
        def sleep(self, seconds: float) -> None:
            legacy.time.sleep(seconds)

    def _legacy_allocation_payload(result_bundle, context: dict[str, object]) -> dict[str, object]:
        result = result_bundle.results[0]
        decision = result.decision
        pair = str(result_bundle.strategy_set.market_scope.pair)
        signal = str(context.get("authoritative_execution_signal") or decision.final_signal or "HOLD").upper()
        reason = str(context.get("final_reason") or decision.final_reason or "legacy_single_strategy_compat")
        contribution = {
            "strategy_instance_id": str(context.get("strategy_instance_id") or decision.strategy_name),
            "strategy_name": str(decision.strategy_name),
            "pair": pair,
            "signal_direction": signal,
            "priority": 0,
            "weight": 1.0,
            "desired_exposure_krw": context.get("target_exposure_krw"),
            "risk_budget_krw": context.get("target_exposure_krw"),
            "preference_hash": sha256_prefixed(
                {
                    "strategy_name": str(decision.strategy_name),
                    "signal_direction": signal,
                    "reason": reason,
                }
            ),
            "reason": reason,
        }
        conflict = {
            "selected_signal": signal,
            "selected_priority": 0,
            "selected_strategy_instance_ids": [contribution["strategy_instance_id"]],
        }
        target = {
            "pair": pair,
            "target_exposure_krw": context.get("target_exposure_krw"),
            "target_qty": None,
            "authoritative": False,
            "fail_closed_reason": "legacy_single_strategy_compatibility_projection",
            "conflict_resolution": conflict,
        }
        target["final_portfolio_target_hash"] = sha256_prefixed(target)
        payload = {
            "schema_version": 1,
            "authority_label": "PortfolioAllocationDecision",
            "source": "legacy_single_strategy_compatibility_projection",
            "authoritative": False,
            "primary_block_reason": "",
            "reason": reason,
            "conflict_resolution": conflict,
            "targets": [target],
            "contributions": [contribution],
            "allocation_input_hash": sha256_prefixed(contribution),
            "allocator_config_hash": sha256_prefixed({"source": "legacy_single_strategy_compatibility_projection"}),
            "strategy_contribution_hash": sha256_prefixed([contribution]),
        }
        payload["allocation_decision_hash"] = sha256_prefixed(payload)
        return payload

    def _legacy_planner_factory(**kwargs):
        planner = legacy.run_loop_execution_planner(**kwargs)

        class _LegacyPlanner:
            def plan_runtime_strategy_results(self, conn, result_bundle, *, updated_ts: int):
                bundle = planner.plan_runtime_strategy_results(conn, result_bundle, updated_ts=updated_ts)
                context = dict(getattr(bundle, "persistence_context", {}) or {})
                if "portfolio_allocation_decision" not in context:
                    context["portfolio_allocation_decision"] = _legacy_allocation_payload(result_bundle, context)
                return replace(bundle, persistence_context=context)

            def plan_envelope(self, *args, **planner_kwargs):
                return planner.plan_envelope(*args, **planner_kwargs)

        return _LegacyPlanner()

    container = create_default_runtime_app(settings)
    decision_coordinator = replace(
        container.decision_coordinator,
        decision_gateway_factory=legacy.RuntimeDecisionGateway,
        planner_factory=_legacy_planner_factory,
        record_strategy_decision_fn=legacy.record_strategy_decision,
    )
    safety_controller = replace(
        container.safety_controller,
        exposure_snapshot=legacy._get_exposure_snapshot,
        legacy_cancel_open_orders=legacy._attempt_open_order_cancellation,
    )
    compat_container = replace(
        container,
        clock=legacy.time.time,
        scheduler=_LegacyScheduler(),
        market_sync=legacy.cmd_sync,
        interval_parser=legacy.parse_interval_sec,
        decision_coordinator=decision_coordinator,
        broker_factory=legacy.BithumbBroker,
        validate_market_preflight=legacy.validate_market_preflight,
        validate_runtime_strategy_set_selection=legacy.validate_runtime_strategy_set_selection,
        validate_live_mode_preflight=legacy.validate_live_mode_preflight,
        validate_market_runtime=legacy.validate_market_runtime,
        safety_controller=safety_controller,
        startup_controller=replace(
            container.startup_controller,
            broker_factory=legacy.BithumbBroker,
            initial_reconcile=legacy.reconcile_with_broker,
        ),
        reconcile_with_broker=legacy.reconcile_with_broker,
        live_executor=legacy.live_execute_signal,
        paper_executor=legacy.paper_execute,
        harmless_dust_recorder=legacy.record_harmless_dust_exit_suppression,
        runtime_strategy_set_manifest_provider=legacy.normalized_runtime_strategy_set_manifest,
    )
    compat_container.runner.run_forever()


__all__ = [
    "ResumeBlocker",
    "_attempt_open_order_cancellation",
    "_classify_balance_split_blocker",
    "_close_guard_ms",
    "_is_closed_candle",
    "_legacy_db_strategy_fallback_allowed",
    "_load_previous_target_exposure_for_run_loop",
    "_persist_target_position_state_for_run_loop",
    "_promotion_grade_typed_runtime_decision_required",
    "_resolve_target_position_state_for_run_loop",
    "_revalidate_cleanup_state_after_failure",
    "_select_latest_closed_candle",
    "_typed_runtime_handoff_failure_reason",
    "authoritative_execution_signal_for_trade",
    "build_resume_guidance",
    "build_signal_execution_request",
    "build_execution_decision_summary",
    "compute_strategy_decision_snapshot",
    "evaluate_restart_readiness",
    "evaluate_resume_eligibility",
    "evaluate_startup_safety_gate",
    "get_health_status",
    "get_stale_risk_state_mismatch_halt_diagnostics",
    "maybe_clear_stale_initial_reconcile_halt",
    "perform_panic_stop_cleanup",
    "run_loop",
    "resolve_typed_execution_submit_expectation",
    "prepare_strategy_decision_persistence_context",
]
