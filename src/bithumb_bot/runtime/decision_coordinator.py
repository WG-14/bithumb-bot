from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from ..config import settings
from ..db_core import (
    ensure_db,
    record_execution_plan,
    record_execution_plan_batch,
    record_portfolio_allocation_decision,
    record_runtime_strategy_decision_bundle,
    record_strategy_decision,
    upsert_target_position_state,
)
from ..decision_equivalence import sha256_prefixed
from ..observability import format_log_kv
from ..run_loop_execution_planner import (
    prepare_strategy_decision_persistence_context,
    resolve_target_position_state_for_run_loop,
    run_loop_uses_target_delta,
)
from ..target_position import (
    ACTUAL_PAIR_TARGET_SOURCE,
    ACTUAL_PAIR_TARGET_SOURCE_PROVENANCE_INCOMPLETE,
)
from ..runtime_decision_service import RuntimeDecisionGateway, RuntimeStrategyDecisionResult
from ..runtime_service_factories import run_loop_execution_planner
from ..runtime_strategy_set import RuntimeStrategyDecisionResultBundle
from .decision_failure_taxonomy import (
    DecisionCycleFailure,
    classify_decision_cycle_failure,
)
from .decision_persistence import DecisionPersistenceUnitOfWork


RUN_LOG = logging.getLogger("bithumb_bot.run")


def runtime_schema_ready_db_factory():
    return ensure_db(ensure_schema_ready=False)


def _no_broker_provider() -> object | None:
    return None


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
        return sha256_prefixed(value)
    return None


def _context_str(context: dict[str, object] | None, key: str) -> str | None:
    if not isinstance(context, dict):
        return None
    value = context.get(key)
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _context_int(context: dict[str, object] | None, key: str) -> int | None:
    if not isinstance(context, dict) or context.get(key) is None:
        return None
    return int(context[key])


_STRATEGY_RISK_KEYS = (
    "strategy_risk_decision_hash",
    "strategy_risk_policy_hash",
    "strategy_risk_input_hash",
    "strategy_risk_evidence_hash",
    "strategy_risk_state_source",
    "strategy_risk_status",
    "strategy_risk_reason_code",
)
_PORTFOLIO_RISK_KEYS = (
    "portfolio_risk_decision_hash",
    "portfolio_risk_policy_hash",
    "portfolio_risk_input_hash",
    "portfolio_risk_evidence_hash",
    "portfolio_risk_state_source",
    "portfolio_risk_status",
    "portfolio_risk_reason_code",
)
_PRE_SUBMIT_RISK_KEYS = (
    "pre_submit_risk_decision_hash",
    "pre_submit_risk_policy_hash",
    "pre_submit_risk_input_hash",
    "pre_submit_risk_evidence_hash",
    "pre_submit_risk_plan_hash",
    "pre_submit_risk_state_source",
    "pre_submit_risk_status",
    "pre_submit_risk_reason_code",
)


def _risk_layer_fields_from_context(context: Mapping[str, object] | None) -> dict[str, str | None]:
    fields: dict[str, str | None] = {
        key: None for key in (*_STRATEGY_RISK_KEYS, *_PORTFOLIO_RISK_KEYS, *_PRE_SUBMIT_RISK_KEYS)
    }
    if not isinstance(context, Mapping):
        return fields
    for key in (*_PORTFOLIO_RISK_KEYS, *_PRE_SUBMIT_RISK_KEYS, *_STRATEGY_RISK_KEYS):
        value = context.get(key)
        if value is not None and str(value).strip():
            fields[key] = str(value)
    if all(fields[key] is None for key in _STRATEGY_RISK_KEYS):
        for item in context.get("runtime_strategy_result_contexts") or []:
            if not isinstance(item, Mapping):
                continue
            if not str(item.get("strategy_risk_decision_hash") or "").strip():
                continue
            for key in _STRATEGY_RISK_KEYS:
                value = item.get(key)
                fields[key] = None if value is None or not str(value).strip() else str(value)
            break
    return fields


def _hard_gate_trace_entries_from_context(
    context: Mapping[str, object] | None,
) -> tuple[dict[str, object], ...]:
    if not isinstance(context, Mapping):
        return ()
    entries: list[dict[str, object]] = []
    raw_trace = context.get("gate_trace")
    if isinstance(raw_trace, list):
        for raw_entry in raw_trace:
            if isinstance(raw_entry, Mapping):
                entries.append(dict(raw_entry))
    raw_entry_authority = context.get("entry_authority")
    if not isinstance(raw_entry_authority, Mapping):
        target_decision = context.get("target_shadow_decision")
        if isinstance(target_decision, Mapping):
            raw_entry_authority = target_decision.get("entry_authority")
    if isinstance(raw_entry_authority, Mapping):
        entry = dict(raw_entry_authority)
        entry["gate"] = "entry_authority"
        existing_index = next(
            (
                index
                for index, item in enumerate(entries)
                if str(item.get("gate") or "").strip() == "entry_authority"
            ),
            None,
        )
        if existing_index is None:
            entries.insert(0, entry)
        else:
            entries[existing_index] = {**entries[existing_index], **entry}
    return tuple(entries)


def persist_target_position_state_for_run_loop(
    conn,
    *,
    execution_decision: dict[str, object],
    signal: str,
    decision_id: int | None,
    updated_ts: int,
    settings_obj: object = settings,
    runtime_pair: str | None = None,
    provenance_context: Mapping[str, object] | None = None,
) -> bool:
    if not run_loop_uses_target_delta(settings_obj):
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
    provenance = dict(provenance_context or {})
    required_provenance = {
        "runtime_strategy_set_manifest_hash": provenance.get("runtime_strategy_set_manifest_hash"),
        "runtime_strategy_decision_bundle_hash": provenance.get("runtime_strategy_decision_bundle_hash"),
        "portfolio_allocation_decision_hash": (
            provenance.get("portfolio_allocation_decision_hash")
            or provenance.get("allocation_decision_hash")
        ),
        "portfolio_target_hash": provenance.get("portfolio_target_hash"),
        "execution_plan_batch_hash": provenance.get("execution_plan_batch_hash"),
        "execution_submit_plan_hash": provenance.get("execution_submit_plan_hash"),
    }
    missing_provenance = [
        key for key, value in required_provenance.items() if not str(value or "").strip()
    ]
    if missing_provenance and provenance_context:
        raise RuntimeError(
            "actual_pair_target_allocator_provenance_incomplete:"
            + ",".join(sorted(missing_provenance))
        )
    actual_target_source = (
        ACTUAL_PAIR_TARGET_SOURCE_PROVENANCE_INCOMPLETE
        if missing_provenance
        else ACTUAL_PAIR_TARGET_SOURCE
    )
    upsert_target_position_state(
        conn,
        pair=str(runtime_pair or getattr(settings_obj, "PAIR")),
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
        runtime_strategy_set_manifest_hash=str(required_provenance["runtime_strategy_set_manifest_hash"] or ""),
        runtime_strategy_decision_bundle_hash=str(required_provenance["runtime_strategy_decision_bundle_hash"] or ""),
        portfolio_allocation_decision_hash=str(required_provenance["portfolio_allocation_decision_hash"] or ""),
        portfolio_target_hash=str(required_provenance["portfolio_target_hash"] or ""),
        execution_plan_batch_hash=str(required_provenance["execution_plan_batch_hash"] or ""),
        execution_submit_plan_hash=str(required_provenance["execution_submit_plan_hash"] or ""),
        actual_target_source=actual_target_source,
    )
    return True


@dataclass(frozen=True)
class DecisionCycleResult:
    candle_ts: int
    strategy_name: str | None
    signal: str | None
    reason: str | None
    decision_id: int | None
    decision_context: dict[str, object] | None
    execution_decision_summary: object | None
    execution_plan_bundle: object | None
    strategy_decision_hash: str | None
    execution_plan_bundle_hash: str | None
    persistence_status: str
    mark_processed_candidate: bool
    runtime_strategy_decision_bundle_id: int | None = None
    runtime_strategy_decision_bundle_hash: str | None = None
    portfolio_allocation_decision_id: int | None = None
    portfolio_allocation_decision_hash: str | None = None
    portfolio_target_id: int | None = None
    portfolio_target_hash: str | None = None
    strategy_contribution_hash: str | None = None
    execution_plan_id: int | None = None
    execution_plan_batch_hash: str | None = None
    execution_plan_batch_id: str | None = None
    execution_submit_plan_hash: str | None = None
    strategy_virtual_lifecycle_transition_hashes: tuple[str, ...] = ()
    strategy_risk_decision_hash: str | None = None
    strategy_risk_policy_hash: str | None = None
    strategy_risk_input_hash: str | None = None
    strategy_risk_evidence_hash: str | None = None
    strategy_risk_state_source: str | None = None
    strategy_risk_status: str | None = None
    strategy_risk_reason_code: str | None = None
    portfolio_risk_decision_hash: str | None = None
    portfolio_risk_policy_hash: str | None = None
    portfolio_risk_input_hash: str | None = None
    portfolio_risk_evidence_hash: str | None = None
    portfolio_risk_state_source: str | None = None
    portfolio_risk_status: str | None = None
    portfolio_risk_reason_code: str | None = None
    pre_submit_risk_decision_hash: str | None = None
    pre_submit_risk_policy_hash: str | None = None
    pre_submit_risk_input_hash: str | None = None
    pre_submit_risk_evidence_hash: str | None = None
    pre_submit_risk_plan_hash: str | None = None
    pre_submit_risk_state_source: str | None = None
    pre_submit_risk_status: str | None = None
    pre_submit_risk_reason_code: str | None = None
    hard_gate_trace_entries: tuple[dict[str, object], ...] = ()
    typed_runtime_decision: RuntimeStrategyDecisionResult | None = None
    representative_runtime_decision_for_observability: RuntimeStrategyDecisionResult | None = None
    typed_runtime_decision_bundle: RuntimeStrategyDecisionResultBundle | None = None
    market_price: float | None = None
    exit_rule_name: str | None = None
    failure_phase: str | None = None
    failure_subphase: str | None = None
    failure_reason_code: str | None = None
    failure_detail: str | None = None
    operator_next_action: str | None = None
    failure_evidence_hash: str | None = None
    persistence_failure_metadata: dict[str, object] | None = None
    persistence_retry_count: int | None = None
    persistence_max_retry_count: int | None = None
    db_subphase: str | None = None
    sql_group: str | None = None
    transaction_elapsed_ms: float | None = None
    lock_wait_elapsed_ms: float | None = None

    def as_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "artifact_type": "decision_cycle_result",
            "schema_version": 1,
            "candle_ts": self.candle_ts,
            "strategy_name": self.strategy_name,
            "signal": self.signal,
            "reason": self.reason,
            "decision_id": self.decision_id,
            "strategy_decision_hash": self.strategy_decision_hash,
            "runtime_strategy_decision_bundle_id": self.runtime_strategy_decision_bundle_id,
            "runtime_strategy_decision_bundle_hash": self.runtime_strategy_decision_bundle_hash,
            "portfolio_allocation_decision_id": self.portfolio_allocation_decision_id,
            "portfolio_allocation_decision_hash": self.portfolio_allocation_decision_hash,
            "portfolio_target_id": self.portfolio_target_id,
            "portfolio_target_hash": self.portfolio_target_hash,
            "strategy_contribution_hash": self.strategy_contribution_hash,
            "execution_plan_id": self.execution_plan_id,
            "execution_plan_batch_hash": self.execution_plan_batch_hash,
            "execution_plan_batch_id": self.execution_plan_batch_id,
            "execution_plan_bundle_hash": self.execution_plan_bundle_hash,
            "execution_submit_plan_hash": self.execution_submit_plan_hash,
            "strategy_virtual_lifecycle_transition_hashes": list(
                self.strategy_virtual_lifecycle_transition_hashes
            ),
            "strategy_risk_decision_hash": self.strategy_risk_decision_hash,
            "strategy_risk_policy_hash": self.strategy_risk_policy_hash,
            "strategy_risk_input_hash": self.strategy_risk_input_hash,
            "strategy_risk_evidence_hash": self.strategy_risk_evidence_hash,
            "strategy_risk_state_source": self.strategy_risk_state_source,
            "strategy_risk_status": self.strategy_risk_status,
            "strategy_risk_reason_code": self.strategy_risk_reason_code,
            "portfolio_risk_decision_hash": self.portfolio_risk_decision_hash,
            "portfolio_risk_policy_hash": self.portfolio_risk_policy_hash,
            "portfolio_risk_input_hash": self.portfolio_risk_input_hash,
            "portfolio_risk_evidence_hash": self.portfolio_risk_evidence_hash,
            "portfolio_risk_state_source": self.portfolio_risk_state_source,
            "portfolio_risk_status": self.portfolio_risk_status,
            "portfolio_risk_reason_code": self.portfolio_risk_reason_code,
            "pre_submit_risk_decision_hash": self.pre_submit_risk_decision_hash,
            "pre_submit_risk_policy_hash": self.pre_submit_risk_policy_hash,
            "pre_submit_risk_input_hash": self.pre_submit_risk_input_hash,
            "pre_submit_risk_evidence_hash": self.pre_submit_risk_evidence_hash,
            "pre_submit_risk_plan_hash": self.pre_submit_risk_plan_hash,
            "pre_submit_risk_state_source": self.pre_submit_risk_state_source,
            "pre_submit_risk_status": self.pre_submit_risk_status,
            "pre_submit_risk_reason_code": self.pre_submit_risk_reason_code,
            "hard_gate_trace_entries": [dict(item) for item in self.hard_gate_trace_entries],
            "persistence_status": self.persistence_status,
            "mark_processed_candidate": bool(self.mark_processed_candidate),
            "market_price": self.market_price,
            "exit_rule_name": self.exit_rule_name,
            "failure_phase": self.failure_phase,
            "failure_subphase": self.failure_subphase,
            "failure_reason_code": self.failure_reason_code,
            "failure_detail": self.failure_detail,
            "operator_next_action": self.operator_next_action,
            "failure_evidence_hash": self.failure_evidence_hash,
            "persistence_failure_metadata": dict(self.persistence_failure_metadata or {}),
            "persistence_retry_count": self.persistence_retry_count,
            "persistence_max_retry_count": self.persistence_max_retry_count,
            "db_subphase": self.db_subphase,
            "sql_group": self.sql_group,
            "transaction_elapsed_ms": self.transaction_elapsed_ms,
            "lock_wait_elapsed_ms": self.lock_wait_elapsed_ms,
        }
        payload["decision_hash"] = sha256_prefixed(payload)
        return payload


@dataclass(frozen=True)
class DecisionCoordinator:
    settings_obj: object = settings
    db_factory: Callable[[], object] = runtime_schema_ready_db_factory
    decision_gateway_factory: Callable[[], RuntimeDecisionGateway] = RuntimeDecisionGateway
    planner_factory: Callable[..., object] = run_loop_execution_planner
    broker_provider: Callable[[], object | None] = _no_broker_provider
    target_state_resolver: Callable[..., object] = resolve_target_position_state_for_run_loop
    persistence_context_builder: Callable[..., object] = prepare_strategy_decision_persistence_context
    decision_persistence_uow_factory: Callable[[], DecisionPersistenceUnitOfWork] = DecisionPersistenceUnitOfWork
    record_runtime_strategy_decision_bundle_fn: Callable[..., dict[str, object]] = record_runtime_strategy_decision_bundle
    record_portfolio_allocation_decision_fn: Callable[..., dict[str, object]] = record_portfolio_allocation_decision
    record_execution_plan_batch_fn: Callable[..., dict[str, object]] = record_execution_plan_batch
    record_execution_plan_fn: Callable[..., dict[str, object]] = record_execution_plan
    record_strategy_decision_fn: Callable[..., int] = record_strategy_decision
    target_position_state_persister: Callable[..., bool] = persist_target_position_state_for_run_loop
    run_start_manifest_payload: dict[str, object] | None = None
    run_start_manifest_id: int | None = None
    run_start_manifest_hash: str | None = None

    def decide_cycle(
        self,
        *,
        runtime_strategy_set: object,
        candle_ts: int,
        updated_ts: int,
        runtime_data_cycle_preflight_hash: str | None = None,
        runtime_data_availability_report_hash: str | None = None,
        broker: object | None = None,
    ) -> DecisionCycleResult:
        current_phase = "gateway"
        failure: DecisionCycleFailure | None = None
        conn = self.db_factory()
        try:
            typed_bundle = self.decision_gateway_factory().decide_bundle(
                conn,
                strategy_set=runtime_strategy_set,
                through_ts_ms=candle_ts,
                runtime_data_cycle_preflight_hash=runtime_data_cycle_preflight_hash,
            )
        except Exception as exc:
            failure = classify_decision_cycle_failure(exc, phase=current_phase)
            RUN_LOG.warning(
                format_log_kv(
                    "[WARN] strategy decision gateway failed",
                    error=failure.detail,
                    failure_phase=failure.phase,
                    failure_reason_code=failure.reason_code,
                )
            )
            return DecisionCycleResult(
                candle_ts=candle_ts,
                strategy_name=None,
                signal=None,
                reason=failure.reason_code,
                decision_id=None,
                decision_context=None,
                execution_decision_summary=None,
                execution_plan_bundle=None,
                strategy_decision_hash=None,
                execution_plan_bundle_hash=None,
                persistence_status=failure.persistence_status,
                mark_processed_candidate=False,
                failure_phase=failure.phase,
                failure_subphase=failure.subphase,
                failure_reason_code=failure.reason_code,
                failure_detail=failure.detail,
                operator_next_action=failure.operator_next_action,
                failure_evidence_hash=failure.evidence_hash,
                persistence_failure_metadata=failure.metadata,
            )
        finally:
            conn.close()

        if typed_bundle is None:
            return DecisionCycleResult(
                candle_ts=candle_ts,
                strategy_name=None,
                signal=None,
                reason="insufficient candle history; signal will be recalculated after more syncs",
                decision_id=None,
                decision_context=None,
                execution_decision_summary=None,
                execution_plan_bundle=None,
                strategy_decision_hash=None,
                execution_plan_bundle_hash=None,
                persistence_status="insufficient_signal_history",
                mark_processed_candidate=False,
            )

        representative_observability_decision = typed_bundle.results[0]
        single_runtime_decision = (
            None if typed_bundle.strategy_set.multi_strategy_enabled else representative_observability_decision
        )
        strategy_name = (
            "multi_strategy"
            if typed_bundle.strategy_set.multi_strategy_enabled
            else representative_observability_decision.decision.strategy_name
        )
        signal = "HOLD" if typed_bundle.strategy_set.multi_strategy_enabled else representative_observability_decision.decision.final_signal
        reason = (
            "multi_strategy_allocator_pending"
            if typed_bundle.strategy_set.multi_strategy_enabled
            else representative_observability_decision.decision.final_reason
        )

        conn = self.db_factory()
        decision_id: int | None = None
        context: dict[str, object] | None = None
        planning_bundle = None
        exit_rule_name: str | None = None
        persistence_status = "not_attempted"
        persistence_metadata: dict[str, object] | None = None
        try:
            current_phase = "planner"
            broker_provider = self.broker_provider if broker is None else (lambda: broker)
            try:
                planner = self.planner_factory(
                    settings_obj=self.settings_obj,
                    target_state_resolver=self.target_state_resolver,
                    persistence_context_builder=self.persistence_context_builder,
                    broker_provider=broker_provider,
                )
            except TypeError:
                try:
                    planner = self.planner_factory(
                        settings_obj=self.settings_obj,
                        target_state_resolver=self.target_state_resolver,
                        persistence_context_builder=self.persistence_context_builder,
                    )
                except TypeError:
                    planner = self.planner_factory(
                        target_state_resolver=self.target_state_resolver,
                        persistence_context_builder=self.persistence_context_builder,
                    )
            planning_bundle = planner.plan_runtime_strategy_results(
                conn,
                typed_bundle,
                updated_ts=updated_ts,
            )
            context = dict(planning_bundle.persistence_context)
            if runtime_data_cycle_preflight_hash:
                context["runtime_data_cycle_preflight_hash"] = runtime_data_cycle_preflight_hash
            if runtime_data_availability_report_hash:
                context["runtime_data_availability_report_hash"] = runtime_data_availability_report_hash
            if getattr(planning_bundle, "planning_error", None):
                planning_exc = RuntimeError(str(planning_bundle.planning_error))
                planning_exc.metadata = {
                    "failure_subphase": getattr(planning_bundle, "failure_subphase", None)
                    or context.get("failure_subphase")
                    or "execution_plan_batch_build",
                    "failure_reason_code": getattr(planning_bundle, "failure_reason_code", None)
                    or context.get("failure_reason_code")
                    or "execution_planning_failed",
                    "exception_type": getattr(planning_bundle, "exception_type", None)
                    or context.get("exception_type")
                    or "RuntimeError",
                    "exception_message": getattr(planning_bundle, "exception_message", None)
                    or context.get("exception_message")
                    or str(planning_bundle.planning_error),
                }
                raise planning_exc
            if typed_bundle.strategy_set.multi_strategy_enabled:
                context["strategy_decision_projection_type"] = (
                    "multi_strategy_compatibility_projection"
                )
                context["strategy_decisions_authority"] = (
                    "compatibility_projection_not_execution_authority"
                )
            if typed_bundle.strategy_set.multi_strategy_enabled:
                signal = str(context.get("authoritative_execution_signal") or "HOLD").upper()
                if signal not in {"BUY", "SELL", "HOLD"}:
                    signal = "HOLD"
                reason = str(
                    context.get("final_reason")
                    or context.get("allocation_primary_block_reason")
                    or context.get("allocator_reason")
                    or "portfolio_allocation_not_authoritative"
                )
            exit_ctx = context.get("exit")
            if isinstance(exit_ctx, dict) and exit_ctx.get("rule") is not None:
                exit_rule_name = str(exit_ctx.get("rule"))
            current_phase = "decision persistence"
            persistence_uow = self.decision_persistence_uow_factory()
            persistence_uow.record_runtime_strategy_decision_bundle_fn = self.record_runtime_strategy_decision_bundle_fn
            persistence_uow.record_portfolio_allocation_decision_fn = self.record_portfolio_allocation_decision_fn
            persistence_uow.record_execution_plan_batch_fn = self.record_execution_plan_batch_fn
            persistence_uow.record_execution_plan_fn = self.record_execution_plan_fn
            persistence_uow.record_strategy_decision_fn = self.record_strategy_decision_fn
            result = persistence_uow.persist(
                conn,
                typed_bundle=typed_bundle,
                planning_bundle=planning_bundle,
                context=context,
                strategy_name=strategy_name,
                signal=signal,
                reason=reason,
                updated_ts=updated_ts,
                settings_obj=self.settings_obj,
                run_start_manifest_payload=self.run_start_manifest_payload,
                run_start_manifest_id=self.run_start_manifest_id,
                run_start_manifest_hash=self.run_start_manifest_hash,
            )
            context = result.context
            decision_id = result.decision_id
            persistence_metadata = result.metadata()
            persistence_status = "persisted"
        except Exception as exc:
            failure = classify_decision_cycle_failure(exc, phase=current_phase)
            persistence_metadata = dict(failure.metadata or {})
            RUN_LOG.warning(
                format_log_kv(
                    "[WARN] strategy decision persistence failed",
                    error=failure.detail,
                    failure_phase=failure.phase,
                    failure_subphase=failure.subphase,
                    failure_reason_code=failure.reason_code,
                    db_subphase=persistence_metadata.get("db_subphase", ""),
                    sql_group=persistence_metadata.get("sql_group", ""),
                    retry_count=persistence_metadata.get("retry_count", ""),
                    strategy=strategy_name,
                    signal=signal,
                )
            )
            persistence_status = failure.persistence_status
        finally:
            conn.close()

        risk_layer_fields = _risk_layer_fields_from_context(context)
        hard_gate_trace_entries = _hard_gate_trace_entries_from_context(context)
        return DecisionCycleResult(
            candle_ts=typed_bundle.candle_ts,
            strategy_name=strategy_name,
            signal=signal,
            reason=reason,
            decision_id=decision_id,
            decision_context=context,
            execution_decision_summary=None if planning_bundle is None else planning_bundle.summary,
            execution_plan_bundle=planning_bundle,
            strategy_decision_hash=_artifact_hash(context or {}),
            execution_plan_bundle_hash=_artifact_hash(planning_bundle),
            runtime_strategy_decision_bundle_id=_context_int(context, "runtime_strategy_decision_bundle_id"),
            runtime_strategy_decision_bundle_hash=_context_str(context, "runtime_strategy_decision_bundle_hash"),
            portfolio_allocation_decision_id=_context_int(context, "portfolio_allocation_decision_id"),
            portfolio_allocation_decision_hash=_context_str(context, "portfolio_allocation_decision_hash"),
            portfolio_target_id=_context_int(context, "portfolio_target_id"),
            portfolio_target_hash=_context_str(context, "portfolio_target_hash"),
            strategy_contribution_hash=_context_str(context, "strategy_contribution_hash"),
            execution_plan_id=_context_int(context, "execution_plan_id"),
            execution_plan_batch_hash=_context_str(context, "execution_plan_batch_hash"),
            execution_plan_batch_id=_context_str(context, "execution_plan_batch_id"),
            execution_submit_plan_hash=_context_str(context, "execution_submit_plan_hash"),
            strategy_virtual_lifecycle_transition_hashes=tuple(
                str(item)
                for item in (
                    (context or {}).get("strategy_virtual_lifecycle_transition_hashes")
                    if isinstance((context or {}).get("strategy_virtual_lifecycle_transition_hashes"), list)
                    else []
                )
                if str(item).strip()
            ),
            strategy_risk_decision_hash=risk_layer_fields["strategy_risk_decision_hash"],
            strategy_risk_policy_hash=risk_layer_fields["strategy_risk_policy_hash"],
            strategy_risk_input_hash=risk_layer_fields["strategy_risk_input_hash"],
            strategy_risk_evidence_hash=risk_layer_fields["strategy_risk_evidence_hash"],
            strategy_risk_state_source=risk_layer_fields["strategy_risk_state_source"],
            strategy_risk_status=risk_layer_fields["strategy_risk_status"],
            strategy_risk_reason_code=risk_layer_fields["strategy_risk_reason_code"],
            portfolio_risk_decision_hash=risk_layer_fields["portfolio_risk_decision_hash"],
            portfolio_risk_policy_hash=risk_layer_fields["portfolio_risk_policy_hash"],
            portfolio_risk_input_hash=risk_layer_fields["portfolio_risk_input_hash"],
            portfolio_risk_evidence_hash=risk_layer_fields["portfolio_risk_evidence_hash"],
            portfolio_risk_state_source=risk_layer_fields["portfolio_risk_state_source"],
            portfolio_risk_status=risk_layer_fields["portfolio_risk_status"],
            portfolio_risk_reason_code=risk_layer_fields["portfolio_risk_reason_code"],
            pre_submit_risk_decision_hash=risk_layer_fields["pre_submit_risk_decision_hash"],
            pre_submit_risk_policy_hash=risk_layer_fields["pre_submit_risk_policy_hash"],
            pre_submit_risk_input_hash=risk_layer_fields["pre_submit_risk_input_hash"],
            pre_submit_risk_evidence_hash=risk_layer_fields["pre_submit_risk_evidence_hash"],
            pre_submit_risk_plan_hash=risk_layer_fields["pre_submit_risk_plan_hash"],
            pre_submit_risk_state_source=risk_layer_fields["pre_submit_risk_state_source"],
            pre_submit_risk_status=risk_layer_fields["pre_submit_risk_status"],
            pre_submit_risk_reason_code=risk_layer_fields["pre_submit_risk_reason_code"],
            hard_gate_trace_entries=hard_gate_trace_entries,
            persistence_status=persistence_status,
            mark_processed_candidate=decision_id is not None and planning_bundle is not None,
            typed_runtime_decision=single_runtime_decision,
            representative_runtime_decision_for_observability=representative_observability_decision,
            typed_runtime_decision_bundle=typed_bundle,
            market_price=typed_bundle.market_price,
            exit_rule_name=exit_rule_name,
            failure_phase=None if failure is None else failure.phase,
            failure_subphase=None if failure is None else failure.subphase,
            failure_reason_code=None if failure is None else failure.reason_code,
            failure_detail=None if failure is None else failure.detail,
            operator_next_action=None if failure is None else failure.operator_next_action,
            failure_evidence_hash=None if failure is None else failure.evidence_hash,
            persistence_failure_metadata=persistence_metadata if failure is not None else None,
            persistence_retry_count=(
                int(persistence_metadata["retry_count"])
                if isinstance(persistence_metadata, dict) and persistence_metadata.get("retry_count") is not None
                else None
            ),
            persistence_max_retry_count=(
                int(persistence_metadata["max_retry_count"])
                if isinstance(persistence_metadata, dict) and persistence_metadata.get("max_retry_count") is not None
                else None
            ),
            db_subphase=(
                str(persistence_metadata.get("db_subphase"))
                if isinstance(persistence_metadata, dict) and persistence_metadata.get("db_subphase") is not None
                else None
            ),
            sql_group=(
                str(persistence_metadata.get("sql_group"))
                if isinstance(persistence_metadata, dict) and persistence_metadata.get("sql_group") is not None
                else None
            ),
            transaction_elapsed_ms=(
                float(persistence_metadata["transaction_elapsed_ms"])
                if isinstance(persistence_metadata, dict) and persistence_metadata.get("transaction_elapsed_ms") is not None
                else None
            ),
            lock_wait_elapsed_ms=(
                float(persistence_metadata["lock_wait_elapsed_ms"])
                if isinstance(persistence_metadata, dict) and persistence_metadata.get("lock_wait_elapsed_ms") is not None
                else None
            ),
        )


__all__ = [
    "DecisionCoordinator",
    "DecisionCycleResult",
    "persist_target_position_state_for_run_loop",
]
