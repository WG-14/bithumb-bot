from __future__ import annotations

import logging
import math
from dataclasses import dataclass

from .. import runtime_state
from ..observability import format_log_kv
from .cycle_artifact_assembler import RuntimeCycleArtifactAssembler
from .data_cycle_preflight import RuntimeDataCyclePreflight, RuntimeDataCyclePreflightProvider
from .live_order_settlement import LiveOrderSettlementWrapper
from .lifecycle_artifacts import RuntimeCycleArtifact
from .state_store import pause_trading_until

FAILSAFE_RETRY_DELAY_SEC = 180
RUN_LOG = logging.getLogger("bithumb_bot.run")


@dataclass(frozen=True)
class RuntimeCyclePipeline:
    runner: object

    def run_once(self) -> RuntimeCycleArtifact | None:
        from . import runner as runner_module

        r = self.runner
        c = r.container
        assert r.runtime_checkpoint is not None
        assert r.runtime_events is not None
        sec = c.interval_parser(c.settings_obj.INTERVAL)
        now = c.clock()
        state = runtime_state.snapshot()
        if (not state.trading_enabled) and state.retry_at_epoch_sec:
            if math.isinf(state.retry_at_epoch_sec):
                runner_module._log_loop_event(
                    logging.WARNING,
                    "[RUN] halted_exit",
                    symbol=c.settings_obj.PAIR,
                    interval=c.settings_obj.INTERVAL,
                    reason="trading halted indefinitely",
                )
                return None
            if now < state.retry_at_epoch_sec:
                runner_module._log_loop_event(
                    logging.WARNING,
                    "[RUN] failsafe_pause",
                    symbol=c.settings_obj.PAIR,
                    interval=c.settings_obj.INTERVAL,
                    wait_sec=max(0, int(state.retry_at_epoch_sec - now)),
                    reason="retry window not reached",
                )
                return None
            runtime_state.enable_trading()
            c.notification_adapter.send_event(r.runtime_events.event("failsafe_retry_window_reached"))

        try:
            preflight = RuntimeDataCyclePreflightProvider(
                container=c,
                runtime_checkpoint=r.runtime_checkpoint,
                runtime_events=r.runtime_events,
            ).evaluate(
                strategy_set=r.runtime_strategy_set,
                now_epoch_sec=now,
                interval_sec=sec,
            )
            r.fail_count = 0
            runtime_state.set_error_count(r.fail_count)
        except Exception as exc:
            r.fail_count += 1
            runtime_state.set_error_count(r.fail_count)
            sync_event = r.runtime_events.event(
                "sync_failed",
                fail_count=r.fail_count,
                max_fails=r.max_fails,
                error=f"{type(exc).__name__}: {exc}",
            )
            c.notification_adapter.send_event(sync_event)
            hashes = [sync_event.get("event_hash")]
            if r.fail_count >= r.max_fails:
                retry_at = c.clock() + FAILSAFE_RETRY_DELAY_SEC
                pause_trading_until(retry_at)
                pause_event = r.runtime_events.event("failsafe_pause_enabled", retry_at_epoch_sec=retry_at)
                c.notification_adapter.send_event(pause_event)
                hashes.append(pause_event.get("event_hash"))
            return r._record_artifact(
                "skip:sync_failed",
                candle_ts=None,
                startup_state="READY",
                notification_event_hashes=hashes,
            )

        if preflight.reason_code == "no_candles_after_sync":
            event = r.runtime_events.event("no_candles_after_sync")
            c.notification_adapter.send_event(event)
            return r._record_artifact(
                "skip:no_candles",
                candle_ts=None,
                startup_state="READY",
                notification_event_hashes=[event.get("event_hash")],
            )
        if preflight.reason_code == "stale_candle_detected":
            event = r.runtime_events.event(
                "stale_candle_detected",
                age_sec=preflight.candle_age_sec,
                stale_cutoff_sec=preflight.stale_cutoff_sec,
            )
            c.notification_adapter.send_event(event)
            return r._record_artifact(
                "skip:stale_candle",
                candle_ts=preflight.latest_candle_ts,
                startup_state="READY",
                notification_event_hashes=[event.get("event_hash")],
            )

        market_safety_result = c.safety_controller.evaluate_market_runtime(
            settings_obj=c.settings_obj,
            now_epoch_sec=now,
            last_market_runtime_check_at=r.last_market_runtime_check_at,
            validate_market_runtime=c.validate_market_runtime,
            validation_error_type=c.market_validation_error_type,
        )
        if market_safety_result.last_market_runtime_check_at is not None:
            r.last_market_runtime_check_at = market_safety_result.last_market_runtime_check_at
        if market_safety_result.blocked:
            market_safety_result = runner_module._apply_runtime_safety_decision(c.safety_controller, market_safety_result)
            runner_module._dispatch_runtime_safety_notifications(c.notification_adapter, market_safety_result)
            safety_hash = (
                market_safety_result.safety_decision.as_dict()["decision_hash"]
                if market_safety_result.safety_decision is not None
                else None
            )
            return r._record_artifact(
                market_safety_result.cycle_id or "halt:market_runtime",
                candle_ts=preflight.latest_candle_ts,
                startup_state="READY",
                safety_decision_hash=safety_hash,
                state_transition_hash=market_safety_result.state_transition_hash,
                notification_event_hashes=market_safety_result.notification_event_hashes,
            )

        safety_result = c.safety_controller.evaluate_runtime_safety(
            settings_obj=c.settings_obj,
            broker=r.broker,
            now_epoch_sec=now,
            last_close=float(preflight.latest_close or 0.0),
            last_open_order_reconcile_at=r.last_open_order_reconcile_at,
            portfolio_cash_qty_with_position_state=c.portfolio_cash_qty_with_position_state,
            db_factory=c.db_factory,
            open_order_snapshot=c.open_order_snapshot,
            mark_open_orders_recovery_required=c.mark_open_orders_recovery_required,
            reconcile_with_broker=c.reconcile_with_broker,
        )
        r.last_open_order_reconcile_at = safety_result.last_open_order_reconcile_at
        if safety_result.blocked:
            safety_result = runner_module._apply_runtime_safety_decision(c.safety_controller, safety_result)
            runner_module._dispatch_runtime_safety_notifications(c.notification_adapter, safety_result)
            safety_hash = (
                safety_result.safety_decision.as_dict()["decision_hash"]
                if safety_result.safety_decision is not None
                else None
            )
            return r._record_artifact(
                safety_result.cycle_id or "safety:block",
                candle_ts=preflight.latest_candle_ts,
                startup_state="READY",
                safety_decision_hash=safety_hash,
                state_transition_hash=safety_result.state_transition_hash,
                notification_event_hashes=safety_result.notification_event_hashes,
            )

        paper_runtime_data_preflight_warning = (
            preflight.reason_code == "runtime_data_preflight_failed"
            and c.settings_obj.MODE != "live"
            and preflight.closed_candle_allowed
        )
        if preflight.reason_code == "runtime_data_preflight_failed":
            runner_module._log_loop_event(
                logging.WARNING,
                "[RUN] runtime_data_preflight_failed",
                symbol=c.settings_obj.PAIR,
                interval=c.settings_obj.INTERVAL,
                candle_ts=preflight.closed_candle_ts,
                runtime_data_availability_report_hash=preflight.runtime_data_availability_report_hash or "-",
                reasons=",".join(str(item) for item in preflight.as_dict().get("runtime_data_preflight_reasons", []) or ()),
            )
            if c.settings_obj.MODE == "live":
                return r._record_artifact(
                    "skip:runtime_data_preflight_failed",
                    candle_ts=preflight.closed_candle_ts,
                    startup_state="READY",
                )
        if not preflight.closed_candle_allowed and not paper_runtime_data_preflight_warning:
            checkpoint_decision = preflight.checkpoint_decision
            return r._record_artifact(
                checkpoint_decision.cycle_id if checkpoint_decision is not None else "skip:no_closed_candle",
                candle_ts=None if checkpoint_decision is None else checkpoint_decision.candle_ts,
                startup_state="READY",
            )

        closed_candle_ts_ms = int(preflight.closed_candle_ts or 0)
        decision_result = c.decision_coordinator.decide_cycle(
            runtime_strategy_set=r.runtime_strategy_set,
            candle_ts=closed_candle_ts_ms,
            updated_ts=int(now * 1000),
            runtime_data_cycle_preflight_hash=preflight.as_dict()["decision_hash"],
            runtime_data_availability_report_hash=preflight.runtime_data_availability_report_hash,
            broker=r.broker,
        )
        if decision_result.persistence_status == "insufficient_signal_history":
            return r._record_artifact(
                "skip:insufficient_signal_history",
                candle_ts=closed_candle_ts_ms,
                startup_state="READY",
            )
        if decision_result.persistence_status == "failed":
            r.decision_persistence_failure_count = int(
                getattr(r, "decision_persistence_failure_count", 0) or 0
            ) + 1
            threshold = max(
                1,
                int(getattr(c.settings_obj, "DECISION_PERSISTENCE_FAILURE_HALT_THRESHOLD", 3) or 3),
            )
            runner_module._log_loop_event(
                logging.WARNING,
                "[RUN] decision_persistence_failed_retryable",
                symbol=c.settings_obj.PAIR,
                interval=c.settings_obj.INTERVAL,
                candle_ts=decision_result.candle_ts,
                reason=decision_result.failure_reason_code or "decision_persistence_failed_retryable",
                consecutive_failures=r.decision_persistence_failure_count,
                halt_threshold=threshold,
                db_subphase=decision_result.db_subphase or "-",
                sql_group=decision_result.sql_group or "-",
                retry_count=decision_result.persistence_retry_count
                if decision_result.persistence_retry_count is not None
                else "-",
            )
            if r.decision_persistence_failure_count >= threshold:
                runtime_state.disable_trading_until(
                    float("inf"),
                    reason="decision persistence failed repeatedly; operator review required",
                    reason_code="DECISION_PERSISTENCE_BLOCKED",
                    halt_new_orders_blocked=True,
                    unresolved=True,
                    attempt_flatten=False,
                    halt_projection={},
                )
                artifact = RuntimeCycleArtifactAssembler(
                    runtime_dependency_manifest_hash=c.runtime_dependency_manifest_hash,
                ).from_cycle_results(
                    cycle_id="halt:decision_persistence_blocked",
                    startup_state="READY",
                    decision_result=decision_result,
                )
                RUN_LOG.info(format_log_kv("[RUN] runtime_cycle_artifact", **artifact.as_dict()))
                return artifact
            artifact = RuntimeCycleArtifactAssembler(
                runtime_dependency_manifest_hash=c.runtime_dependency_manifest_hash,
            ).from_cycle_results(
                cycle_id="skip:decision_persistence_failed_retryable",
                startup_state="READY",
                decision_result=decision_result,
            )
            RUN_LOG.info(format_log_kv("[RUN] runtime_cycle_artifact", **artifact.as_dict()))
            return artifact
        r.decision_persistence_failure_count = 0

        execution_result = c.execution_coordinator.execute_cycle(
            candle_ts=decision_result.candle_ts,
            decision_id=decision_result.decision_id,
            signal=str(decision_result.signal or "HOLD"),
            market_price=float(decision_result.market_price or 0.0),
            strategy_name=decision_result.strategy_name,
            decision_reason=decision_result.reason,
            exit_rule_name=decision_result.exit_rule_name,
            decision_context=decision_result.decision_context,
            execution_plan_bundle=decision_result.execution_plan_bundle,
            execution_decision_summary=decision_result.execution_decision_summary,
            execution_service=r.execution_service,
            post_trade_reconcile=(
                (lambda: c.reconcile_with_broker(r.broker))
                if c.settings_obj.MODE == "live" and r.broker is not None
                else None
            ),
            settlement_coordinator=(
                LiveOrderSettlementWrapper(
                    broker=r.broker,
                    db_factory=c.db_factory,
                    reconcile_with_broker=c.reconcile_with_broker,
                )
                if c.settings_obj.MODE == "live" and r.broker is not None
                else None
            ),
            settlement_required=c.settings_obj.MODE == "live" and r.broker is not None,
            input_hash=decision_result.as_dict()["decision_hash"],
            execution_plan_bundle_hash=decision_result.execution_plan_bundle_hash,
        )
        if execution_result.mark_processed_allowed:
            r.runtime_checkpoint.apply(candle_ts_ms=decision_result.candle_ts, now_epoch_sec=now)
            runner_module._log_loop_event(
                logging.INFO,
                "[RUN] processed closed candle",
                symbol=c.settings_obj.PAIR,
                interval=c.settings_obj.INTERVAL,
                candle_ts=decision_result.candle_ts,
                signal=str(decision_result.signal or "HOLD"),
            )
        if execution_result.submitted and execution_result.trade:
            trade = dict(execution_result.trade)
            runner_module._log_loop_event(
                logging.INFO,
                "[RUN] trade_applied",
                symbol=c.settings_obj.PAIR,
                interval=c.settings_obj.INTERVAL,
                candle_ts=decision_result.candle_ts,
                signal_ts=trade.get("signal_ts", trade.get("candle_ts", decision_result.candle_ts)),
                client_order_id=trade.get("client_order_id"),
                exchange_order_id=trade.get("exchange_order_id"),
                side=trade.get("side"),
                submit_qty=f"{float(trade.get('submit_qty', trade.get('qty', 0.0)) or 0.0):.3f}",
                filled_qty=f"{float(trade.get('filled_qty', 0.0) or 0.0):.3f}",
                post_trade_cash=f"{float(trade.get('post_trade_cash', trade.get('cash', 0.0)) or 0.0):.0f}",
                post_trade_asset=f"{float(trade.get('post_trade_asset', trade.get('asset', 0.0)) or 0.0):.8f}",
            )
        artifact = RuntimeCycleArtifactAssembler(
            runtime_dependency_manifest_hash=c.runtime_dependency_manifest_hash,
        ).from_cycle_results(
            cycle_id="checkpoint:processed",
            startup_state="READY",
            decision_result=decision_result,
            execution_result=execution_result,
        )
        RUN_LOG.info(format_log_kv("[RUN] runtime_cycle_artifact", **artifact.as_dict()))
        if execution_result.halt_transition:
            halt_reason_code = str(execution_result.halt_transition.get("reason_code") or execution_result.planning_status)
            halt_reason = str(execution_result.halt_transition.get("evidence", {}).get("error") or halt_reason_code)
            decision = c.safety_controller.evaluate_halt(
                runner_module._halt_reason(halt_reason_code, halt_reason),
                unresolved=True,
            )
            transition = c.safety_controller.apply(decision)
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
            runner_module._dispatch_safety_decision(c.notification_adapter, decision)
            event = r.runtime_events.execution_failure_from_transition(execution_result.halt_transition)
            c.notification_adapter.send_event(event)
            return r._record_artifact(
                f"halt:{execution_result.planning_status}",
                candle_ts=decision_result.candle_ts,
                startup_state="READY",
                strategy_decision_hash=decision_result.strategy_decision_hash,
                runtime_strategy_decision_bundle_hash=decision_result.runtime_strategy_decision_bundle_hash,
                execution_result_hash=execution_result.as_dict()["decision_hash"],
                safety_decision_hash=decision.as_dict()["decision_hash"],
                state_transition_hash=decision.as_dict()["state_transition"].get("decision_hash"),
                notification_event_hashes=[
                    *decision.as_dict().get("operator_event_hashes", []),
                    event.get("event_hash"),
                ],
            )
        return artifact


__all__ = ["RuntimeCyclePipeline", "RuntimeDataCyclePreflight"]
