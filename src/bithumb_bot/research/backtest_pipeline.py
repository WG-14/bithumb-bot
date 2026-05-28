from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Protocol

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.market_regime import aggregate_regime_coverage, aggregate_regime_performance
from bithumb_bot.risk import PureRiskInput, evaluate_pure_risk

from . import backtest_support as support
from .backtest_stages import (
    ExperimentRecorder,
    MarketReplayClock,
    MetricsCollector,
    PortfolioLedgerStage,
    ReplayTick,
    RiskGate,
    RiskGateDecision,
    StrategyEvaluator,
    StrategyEvaluationEnvelope,
)
from .execution_simulator_stage import DefaultExecutionSimulator
from .experiment_manifest import ExecutionTimingPolicy, legacy_research_portfolio_policy
from .portfolio_ledger import PortfolioLedger
from .strategy_spec import exit_policy_from_parameters, exit_policy_hash, strategy_spec_for_name

if TYPE_CHECKING:
    from .backtest_support import BacktestRun, BacktestRunContext
    from .dataset_snapshot import DatasetSnapshot
    from .decision_event import ResearchDecisionEvent
    from .execution_model import ExecutionModel
    from .experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy


BacktestRun = support.BacktestRun
BacktestRunContext = support.BacktestRunContext
empty_execution_event_summary = support.empty_execution_event_summary
execution_event_summary = support.execution_event_summary


class ExecutionSimulator(Protocol):
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        ...


def _exit_rule_source(
    *,
    rule_name: str,
    common_exit_rule_names: set[str],
    strategy_exit_rule_names: set[str],
) -> str:
    in_common = rule_name in common_exit_rule_names
    in_strategy = rule_name in strategy_exit_rule_names
    if in_common and in_strategy:
        return "common_risk_and_plugin"
    if in_common:
        return "common_risk"
    if in_strategy:
        return "plugin"
    return "unknown"


def _float_or_none(value: object) -> float | None:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed


@dataclass
class BacktestPipelineState:
    dataset: DatasetSnapshot
    strategy_name: str
    parameter_values: dict[str, Any]
    fee_rate: float
    slippage_bps: float
    decision_events: tuple[ResearchDecisionEvent, ...]
    parameter_stability_score: float | None = None
    execution_model: ExecutionModel | None = None
    execution_timing_policy: ExecutionTimingPolicy | None = None
    portfolio_policy: PortfolioPolicy | None = None
    context: BacktestRunContext | None = None
    ticks: tuple[ReplayTick, ...] = ()
    ledger: PortfolioLedger | None = None
    result: BacktestRun | None = None


@dataclass(frozen=True)
class BacktestStageSet:
    market_clock: MarketReplayClock | None = None
    portfolio_ledger: PortfolioLedgerStage | None = None
    strategy_evaluator: StrategyEvaluator | None = None
    risk_gate: RiskGate | None = None
    execution_simulator: ExecutionSimulator | None = None
    metrics_collector: MetricsCollector | None = None
    experiment_recorder: ExperimentRecorder | None = None

    def ordered(self) -> tuple[object, ...]:
        return tuple(
            stage
            for stage in (
                self.market_clock,
                self.portfolio_ledger,
                self.strategy_evaluator,
                self.risk_gate,
                self.execution_simulator,
                self.metrics_collector,
                self.experiment_recorder,
            )
            if stage is not None
        )


def default_backtest_stage_set() -> BacktestStageSet:
    return BacktestStageSet(
        market_clock=DefaultMarketReplayClock(),
        portfolio_ledger=DefaultPortfolioLedgerStage(),
        strategy_evaluator=DefaultStrategyEvaluator(),
        risk_gate=DefaultRiskGate(),
        execution_simulator=DefaultExecutionSimulator(),
        metrics_collector=DefaultMetricsCollector(),
        experiment_recorder=DefaultExperimentRecorder(),
    )


@dataclass(frozen=True)
class DefaultMarketReplayClock:
    """Convert decision events into deterministic replay ticks."""

    def run(self, state: BacktestPipelineState) -> BacktestPipelineState:
        from .strategy_registry import resolve_research_strategy_plugin

        plugin = resolve_research_strategy_plugin(state.strategy_name)
        candles = state.dataset.candles
        candle_index_by_ts = {int(candle.ts): index for index, candle in enumerate(candles)}
        ticks: list[ReplayTick] = []
        for event in state.decision_events:
            if event.strategy_name != plugin.name:
                raise ValueError(f"decision_event_strategy_mismatch:{event.strategy_name}")
            index = candle_index_by_ts.get(int(event.candle_ts))
            if index is None:
                raise ValueError(f"decision_event_candle_missing:{event.candle_ts}")
            candle = candles[index]
            ticks.append(
                ReplayTick(
                    candle=candle,
                    candle_index=index,
                    candle_ts=int(candle.ts),
                    decision_ts=int(event.decision_ts),
                    event=event,
                )
            )
        return replace(state, ticks=tuple(ticks))


@dataclass(frozen=True)
class DefaultPortfolioLedgerStage:
    """Create the portfolio authority used by the default backtest path."""

    def run(self, state: BacktestPipelineState) -> BacktestPipelineState:
        policy = state.portfolio_policy or legacy_research_portfolio_policy()
        ledger = PortfolioLedger.create(
            starting_cash=float(policy.starting_cash_krw),
            initial_position_qty=float(policy.initial_position_qty),
        )
        return replace(state, portfolio_policy=policy, ledger=ledger)


@dataclass(frozen=True)
class DefaultStrategyEvaluator:
    """Policy-evaluation authority boundary for the default research path."""

    def run(self, state: BacktestPipelineState) -> BacktestPipelineState:
        return state

    def evaluate(
        self,
        tick: ReplayTick,
        position_snapshot: Any,
        strategy_context: dict[str, object],
    ) -> StrategyEvaluationEnvelope:
        from .strategy_registry import resolve_research_strategy_plugin

        dataset = strategy_context["dataset"]
        strategy_name = str(strategy_context["strategy_name"])
        parameter_values = dict(strategy_context["parameter_values"])  # type: ignore[arg-type]
        fee_rate = float(strategy_context["fee_rate"])
        slippage_bps = float(strategy_context["slippage_bps"])
        active_exit_policy = dict(strategy_context["active_exit_policy"])  # type: ignore[arg-type]
        buy_fraction = float(strategy_context["buy_fraction"])
        run_context = strategy_context["run_context"]
        event = tick.event
        plugin = resolve_research_strategy_plugin(strategy_name)
        event_extra = event.extra_payload if isinstance(event.extra_payload, dict) else {}
        raw_signal = str(event.raw_signal or "HOLD").upper()
        raw_reason = str(event_extra.get("raw_reason") or event.reason)
        raw_filter_would_block = bool(event_extra.get("raw_filter_would_block", bool(event.blocked_filters)))
        entry_filter_blocked = bool(event_extra.get("entry_filter_blocked", False))
        entry_signal = str(event.entry_signal or raw_signal).upper()
        policy_materialization_mode = str(
            getattr(run_context, "policy_materialization_mode", "research_exploratory")
        )
        promotion_grade_policy_required = policy_materialization_mode != "research_exploratory"
        policy_builder_kwargs: dict[str, object] = {
            "event": event,
            "dataset": dataset,
            "candle_index": int(tick.candle_index),
            "position": position_snapshot,
            "parameter_values": parameter_values,
            "fee_rate": fee_rate,
            "slippage_bps": slippage_bps,
            "active_exit_policy": active_exit_policy,
            "buy_fraction": buy_fraction,
        }
        if plugin.policy_assembly_factory is not None:
            policy_builder_kwargs.update(
                {
                    "materialization_mode": policy_materialization_mode,
                    "candidate_regime_policy": (
                        dict(getattr(run_context, "candidate_regime_policy"))
                        if isinstance(getattr(run_context, "candidate_regime_policy", None), dict)
                        else None
                    ),
                    "candidate_regime_policy_enforced": bool(
                        getattr(run_context, "candidate_regime_policy_drives_research_execution", True)
                    ),
                }
            )
        builder = plugin.research_policy_decision_builder
        policy_decision = builder(**policy_builder_kwargs) if builder is not None else None
        evaluates_exit_policy = bool(
            isinstance(event.exit_intent, dict)
            and str(event.exit_intent.get("mode") or "") == "evaluate_exit_policy"
        )
        allows_legacy_event_first_exit_policy = "research_runtime_contract.v2" not in str(
            event.strategy_version or ""
        )
        unsupported_reason = ""
        if (
            builder is not None
            and policy_decision is None
            and not (evaluates_exit_policy and allows_legacy_event_first_exit_policy)
        ):
            unsupported_reason = "research_policy_decision_missing_not_comparable"
        if promotion_grade_policy_required and policy_decision is None:
            raise ValueError(unsupported_reason or "research_policy_decision_missing_not_comparable")
        if policy_decision is not None:
            missing = [
                name
                for name in (
                    "policy_hash",
                    "policy_contract_hash",
                    "policy_input_hash",
                    "policy_decision_hash",
                )
                if not str(getattr(policy_decision, name, "") or "").strip()
            ]
            if promotion_grade_policy_required and missing:
                raise ValueError("research_strategy_decision_promotion_fields_missing:" + ",".join(missing))
            entry_decision = policy_decision.entry_decision
            raw_signal = str(policy_decision.raw_signal or "HOLD").upper()
            raw_reason = str(policy_decision.raw_reason or raw_reason)
            raw_filter_would_block = bool(policy_decision.trace.get("raw_filter_would_block"))
            entry_filter_blocked = bool(policy_decision.trace.get("entry_blocked"))
            entry_signal = str(policy_decision.entry_signal or raw_signal).upper()
            exit_signal = str(policy_decision.exit_signal or raw_signal).upper()
            blocked_filters = tuple(policy_decision.blocked_filters)
            replay_hash = str(policy_decision.trace.get("replay_fingerprint_hash") or "")
            if not replay_hash:
                replay_hash = canonical_payload_hash(
                    {
                        "policy_input_hash": policy_decision.policy_input_hash,
                        "policy_decision_hash": policy_decision.policy_decision_hash,
                        "policy_contract_hash": policy_decision.policy_contract_hash,
                        "candle_ts": int(tick.candle_ts),
                    }
                )
        else:
            entry_decision = event_extra.get("entry_decision")
            exit_signal = str(event.exit_signal or event.raw_signal or "HOLD").upper()
            blocked_filters = tuple(event.blocked_filters)
            replay_hash = canonical_payload_hash(
                {
                    "strategy": plugin.name,
                    "candle_ts": int(tick.candle_ts),
                    "decision_ts": int(tick.decision_ts),
                    "raw_signal": raw_signal,
                    "final_signal": str(event.final_signal or "HOLD").upper(),
                    "compatibility_fallback": True,
                }
            )
        provenance = {
            "stage_id": "strategy_evaluator",
            "strategy_name": plugin.name,
            "entry_decision": entry_decision,
            "raw_signal": raw_signal,
            "raw_reason": raw_reason,
            "raw_filter_would_block": raw_filter_would_block,
            "entry_filter_blocked": entry_filter_blocked,
            "entry_signal": entry_signal,
            "exit_signal": exit_signal,
            "blocked_filters": blocked_filters,
            "policy_materialization_mode": policy_materialization_mode,
            "promotion_grade_policy_required": promotion_grade_policy_required,
            "allows_legacy_event_first_exit_policy": allows_legacy_event_first_exit_policy,
            "evaluates_exit_policy": evaluates_exit_policy,
        }
        return StrategyEvaluationEnvelope(
            decision=policy_decision,
            provenance=provenance,
            replay_fingerprint_hash=replay_hash,
            unsupported_reason=unsupported_reason,
            compatibility_fallback=policy_decision is None,
            promotion_grade=bool(policy_decision is not None and not unsupported_reason),
            recommended_next_action=(
                "none"
                if policy_decision is not None
                else "regenerate_research_decisions_with_typed_strategy_decision"
            ),
        )


@dataclass(frozen=True)
class DefaultRiskGate:
    """Exit-policy and research risk admission boundary."""

    def run(self, state: BacktestPipelineState) -> BacktestPipelineState:
        return state

    def evaluate(
        self,
        strategy_decision: Any | None,
        position_snapshot: Any,
        market_snapshot: dict[str, object],
        portfolio_snapshot: dict[str, object],
        risk_context: dict[str, object],
    ) -> RiskGateDecision:
        from bithumb_bot.strategy.exit_rules import merge_exit_rules

        plugin = risk_context["strategy_plugin"]
        event = risk_context["event"]
        active_exit_policy = dict(risk_context["active_exit_policy"])  # type: ignore[arg-type]
        parameter_values = dict(risk_context["parameter_values"])  # type: ignore[arg-type]
        fee_rate = float(risk_context["fee_rate"])
        strategy_envelope = risk_context["strategy_envelope"]
        raw_signal = str(strategy_envelope.provenance.get("raw_signal") or "HOLD").upper()
        raw_reason = str(strategy_envelope.provenance.get("raw_reason") or event.reason)
        entry_signal = str(strategy_envelope.provenance.get("entry_signal") or raw_signal).upper()
        unsupported_reason = str(strategy_envelope.unsupported_reason or "")
        policy_drives_execution = True
        if strategy_decision is not None and policy_drives_execution:
            requested_action = str(strategy_decision.final_signal or "HOLD").upper()
        elif unsupported_reason:
            requested_action = "HOLD"
        else:
            requested_action = str(event.final_signal or "HOLD").upper()
        action = requested_action
        blocked = bool(unsupported_reason)
        block_reason = (
            str(strategy_decision.final_reason)
            if strategy_decision is not None and policy_drives_execution
            else unsupported_reason or str(event.reason)
        )
        sellable_qty = float(portfolio_snapshot.get("sellable_qty") or 0.0)
        pending_buy_qty = float(portfolio_snapshot.get("pending_buy_qty") or 0.0)
        current_qty = float(portfolio_snapshot.get("qty") or 0.0)
        pure_risk_input = risk_context.get("pure_risk_input")
        if pure_risk_input is None:
            pure_risk_input = PureRiskInput(
                evaluation_ts_ms=int(market_snapshot.get("candle_ts") or 0),
                current_equity=_float_or_none(risk_context.get("current_equity")),
                baseline_equity=_float_or_none(risk_context.get("baseline_equity")),
                loss_today=_float_or_none(risk_context.get("loss_today")),
                max_daily_loss_krw=float(risk_context.get("max_daily_loss_krw") or 0.0),
                mark_price=float(market_snapshot.get("close") or 0.0),
                current_cash_krw=_float_or_none(risk_context.get("current_cash")),
                current_asset_qty=current_qty,
                position_entry_price=_float_or_none(getattr(position_snapshot, "entry_price", None)),
                max_position_loss_pct=float(risk_context.get("max_position_loss_pct") or 0.0),
                broker_local_mismatch=bool(risk_context.get("broker_local_mismatch")),
                recovery_risk_mismatch_reason=(
                    str(risk_context.get("recovery_risk_mismatch_reason"))
                    if risk_context.get("recovery_risk_mismatch_reason") is not None
                    else None
                ),
            )
        pure_risk = evaluate_pure_risk(pure_risk_input)
        if pure_risk.blocked:
            action = "HOLD"
            blocked = True
            block_reason = pure_risk.reason_code
        exit_evaluations: list[dict[str, object]] = []
        exit_rule = str((event.exit_intent or {}).get("exit_rule") or "") if event.exit_intent else ""
        exit_reason = str((event.exit_intent or {}).get("exit_reason") or "") if event.exit_intent else ""
        evaluates_exit_policy = bool(strategy_envelope.provenance.get("evaluates_exit_policy"))
        if (
            evaluates_exit_policy
            and strategy_decision is None
            and not unsupported_reason
            and not pure_risk.blocked
        ):
            action = "BUY" if requested_action == "BUY" else "HOLD"
            if sellable_qty > 1e-12:
                position = support.ResearchPositionContext(
                    in_position=True,
                    entry_ts=getattr(position_snapshot, "entry_ts", None),
                    entry_price=getattr(position_snapshot, "entry_price", None),
                    qty_open=sellable_qty,
                    holding_time_sec=float(getattr(position_snapshot, "holding_time_sec", 0.0) or 0.0),
                    unrealized_pnl=float(getattr(position_snapshot, "unrealized_pnl", 0.0) or 0.0),
                    unrealized_pnl_ratio=float(
                        getattr(position_snapshot, "unrealized_pnl_ratio", 0.0) or 0.0
                    ),
                )
                common_exit_rules = support.create_exit_rules(
                    rule_names=list(active_exit_policy.get("common_rules") or ()),
                    stop_loss_ratio=float(active_exit_policy.get("stop_loss", {}).get("stop_loss_ratio", 0.0)),
                    max_holding_sec=float(
                        active_exit_policy.get("max_holding_time", {}).get("max_holding_min", 0.0)
                    )
                    * 60.0,
                )
                strategy_exit_rules = []
                if plugin.exit_rule_factory is not None:
                    strategy_exit_rules = plugin.exit_rule_factory(active_exit_policy, parameter_values, fee_rate)
                exit_rules = merge_exit_rules(common_exit_rules, strategy_exit_rules)
                common_exit_rule_names = {rule.name for rule in common_exit_rules}
                strategy_exit_rule_names = {rule.name for rule in strategy_exit_rules}
                for rule in exit_rules:
                    strategy_signal_context = (
                        plugin.exit_signal_context_builder(event)
                        if plugin.exit_signal_context_builder is not None
                        else {}
                    )
                    result = rule.evaluate(
                        position=position,
                        candle_ts=int(market_snapshot["candle_ts"]),
                        market_price=float(market_snapshot["close"]),
                        signal_context={
                            "base_signal": raw_signal,
                            "base_reason": raw_reason,
                            "entry_signal": entry_signal,
                            "exit_signal": event.exit_signal or raw_signal,
                            **strategy_signal_context,
                        },
                    )
                    exit_evaluations.append(
                        {
                            "rule": rule.name,
                            "rule_source": _exit_rule_source(
                                rule_name=rule.name,
                                common_exit_rule_names=common_exit_rule_names,
                                strategy_exit_rule_names=strategy_exit_rule_names,
                            ),
                            "triggered": bool(result.should_exit),
                            "reason": result.reason,
                            "context": result.context,
                        }
                    )
                    if result.should_exit:
                        action = "SELL"
                        exit_rule = rule.name
                        exit_reason = result.reason
                        break
        if action == "BUY" and (current_qty > 1e-12 or pending_buy_qty > 1e-12):
            action = "HOLD"
            blocked = True
            block_reason = "buy_blocked_existing_position_or_pending_buy"
        elif action == "SELL" and sellable_qty <= 1e-12:
            action = "HOLD"
            blocked = True
            block_reason = "sell_blocked_no_sellable_qty"
        elif action not in {"BUY", "SELL", "HOLD"}:
            raise ValueError(f"unsupported_decision_event_final_signal:{event.final_signal}")
        if strategy_decision is not None:
            exit_evaluations = [dict(item) for item in strategy_decision.exit_evaluations]
            exit_rule = str(strategy_decision.exit_rule or "")
            exit_reason = strategy_decision.exit_reason
        reason_code = block_reason if blocked or action == "HOLD" else "none"
        evidence_payload = {
            "stage": "risk_gate",
            "requested_action": requested_action,
            "final_signal": action,
            "reason_code": reason_code,
            "exit_rule": exit_rule,
            "exit_reason": exit_reason,
            "exit_evaluations": exit_evaluations,
        }
        return RiskGateDecision(
            allow=action in {"BUY", "SELL"} and not blocked,
            block=bool(blocked or action == "HOLD"),
            override_to_sell=bool(requested_action != "SELL" and action == "SELL"),
            final_signal=action,
            reason_code=reason_code,
            evidence_hash=canonical_payload_hash(evidence_payload),
            exit_rule=exit_rule,
            exit_reason=exit_reason,
            exit_evaluations=tuple(exit_evaluations),
            payload=evidence_payload,
        )


@dataclass
class DefaultMetricsCollector:
    """Retains decision, equity, metrics, and resource accounting state."""

    records: list[dict[str, object]] = field(default_factory=list)

    def run(self, state: BacktestPipelineState) -> BacktestPipelineState:
        return state

    def record(self, stage_id: str, payload: dict[str, object]) -> None:
        self.records.append({"stage_id": str(stage_id), **dict(payload)})


@dataclass
class DefaultExperimentRecorder:
    """Final observability stage for injected stage pipelines."""

    stage_records: list[dict[str, object]] = field(default_factory=list)

    def run(self, state: BacktestPipelineState) -> BacktestRun:
        if state.result is None:
            raise RuntimeError("experiment_recorder_missing_backtest_result")
        return state.result

    def record_stage(
        self,
        *,
        stage_id: str,
        input_hash: str,
        output_hash: str,
        reason_code: str,
        payload: dict[str, object] | None = None,
    ) -> None:
        record = {
            "stage_id": str(stage_id),
            "input_hash": str(input_hash),
            "output_hash": str(output_hash),
            "reason_code": str(reason_code),
        }
        if payload is not None:
            record["payload"] = dict(payload)
        self.stage_records.append(record)


@dataclass(frozen=True)
class DefaultBacktestPipeline:
    """Stage-composition boundary behind the public backtest kernel facade."""

    stages: BacktestStageSet = field(default_factory=default_backtest_stage_set)
    injected_stages: tuple[object, ...] = ()

    def run(
        self,
        *,
        dataset: DatasetSnapshot,
        strategy_name: str,
        parameter_values: dict[str, Any],
        fee_rate: float,
        slippage_bps: float,
        decision_events: tuple[ResearchDecisionEvent, ...],
        parameter_stability_score: float | None = None,
        execution_model: ExecutionModel | None = None,
        execution_timing_policy: ExecutionTimingPolicy | None = None,
        portfolio_policy: PortfolioPolicy | None = None,
        context: BacktestRunContext | None = None,
    ) -> BacktestRun:
        if self.injected_stages:
            return self._run_injected_stages(
                state=BacktestPipelineState(
                    dataset=dataset,
                    strategy_name=strategy_name,
                    parameter_values=parameter_values,
                    fee_rate=fee_rate,
                    slippage_bps=slippage_bps,
                    decision_events=decision_events,
                    parameter_stability_score=parameter_stability_score,
                    execution_model=execution_model,
                    execution_timing_policy=execution_timing_policy,
                    portfolio_policy=portfolio_policy,
                    context=context,
                ),
                stages=self.injected_stages,
            )
        return self._run_default_stages(
            BacktestPipelineState(
                dataset=dataset,
                strategy_name=strategy_name,
                parameter_values=parameter_values,
                fee_rate=fee_rate,
                slippage_bps=slippage_bps,
                decision_events=decision_events,
                parameter_stability_score=parameter_stability_score,
                execution_model=execution_model,
                execution_timing_policy=execution_timing_policy,
                portfolio_policy=portfolio_policy,
                context=context,
            )
        )

    def _run_injected_stages(self, **payload: object) -> BacktestRun:
        stages = tuple(payload.pop("stages"))
        state: object = payload.pop("state")
        for stage in stages:
            runner = getattr(stage, "run", None)
            if runner is None:
                if not callable(stage):
                    raise TypeError(f"backtest_stage_not_callable:{type(stage).__name__}")
                state = stage(state)  # type: ignore[misc]
            else:
                state = runner(state)
        return state  # type: ignore[return-value]

    def _run_default_stages(self, state: BacktestPipelineState) -> BacktestRun:
        if self.stages.market_clock is None:
            raise RuntimeError("default_backtest_pipeline_missing_market_clock")
        if self.stages.portfolio_ledger is None:
            raise RuntimeError("default_backtest_pipeline_missing_portfolio_ledger")
        if self.stages.strategy_evaluator is None:
            raise RuntimeError("default_backtest_pipeline_missing_strategy_evaluator")
        if self.stages.risk_gate is None:
            raise RuntimeError("default_backtest_pipeline_missing_risk_gate")
        if self.stages.execution_simulator is None:
            raise RuntimeError("default_backtest_pipeline_missing_execution_simulator")
        if self.stages.metrics_collector is None:
            raise RuntimeError("default_backtest_pipeline_missing_metrics_collector")
        if self.stages.experiment_recorder is None:
            raise RuntimeError("default_backtest_pipeline_missing_experiment_recorder")

        prepared = self.stages.market_clock.run(state)  # type: ignore[attr-defined]
        prepared = self.stages.portfolio_ledger.run(prepared)  # type: ignore[attr-defined]
        for stage in (
            self.stages.strategy_evaluator,
            self.stages.risk_gate,
            self.stages.execution_simulator,
            self.stages.metrics_collector,
        ):
            runner = getattr(stage, "run", None)
            if runner is not None:
                prepared = runner(prepared)
        result = _run_stage_composed_decision_event_backtest(
            dataset=prepared.dataset,
            strategy_name=prepared.strategy_name,
            parameter_values=prepared.parameter_values,
            fee_rate=prepared.fee_rate,
            slippage_bps=prepared.slippage_bps,
            decision_events=prepared.decision_events,
            parameter_stability_score=prepared.parameter_stability_score,
            execution_model=prepared.execution_model,
            execution_timing_policy=prepared.execution_timing_policy,
            portfolio_policy=prepared.portfolio_policy,
            context=prepared.context,
            prepared_ticks=prepared.ticks,
            prepared_ledger=prepared.ledger,
            strategy_evaluator=self.stages.strategy_evaluator,
            risk_gate=self.stages.risk_gate,
            execution_simulator=self.stages.execution_simulator,
            metrics_collector=self.stages.metrics_collector,
            experiment_recorder=self.stages.experiment_recorder,
        )
        return self.stages.experiment_recorder.run(replace(prepared, result=result))  # type: ignore[attr-defined]


def run_decision_event_backtest(
    *,
    dataset: DatasetSnapshot,
    strategy_name: str,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    decision_events: tuple[ResearchDecisionEvent, ...],
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    return DefaultBacktestPipeline().run(
        dataset=dataset,
        strategy_name=strategy_name,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        decision_events=decision_events,
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=execution_timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def _run_decision_event_backtest_impl(
    *,
    dataset: DatasetSnapshot,
    strategy_name: str,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    decision_events: tuple[ResearchDecisionEvent, ...],
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
    prepared_ticks: tuple[ReplayTick, ...] | None = None,
    prepared_ledger: PortfolioLedger | None = None,
) -> BacktestRun:
    """Compatibility shim; the default authority path is DefaultBacktestPipeline.run()."""
    if prepared_ticks is not None or prepared_ledger is not None:
        return _run_stage_composed_decision_event_backtest(
            dataset=dataset,
            strategy_name=strategy_name,
            parameter_values=parameter_values,
            fee_rate=fee_rate,
            slippage_bps=slippage_bps,
            decision_events=decision_events,
            parameter_stability_score=parameter_stability_score,
            execution_model=execution_model,
            execution_timing_policy=execution_timing_policy,
            portfolio_policy=portfolio_policy,
            context=context,
            prepared_ticks=prepared_ticks,
            prepared_ledger=prepared_ledger,
        )
    return DefaultBacktestPipeline().run(
        dataset=dataset,
        strategy_name=strategy_name,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        decision_events=decision_events,
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=execution_timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
    )


def _run_stage_composed_decision_event_backtest(
    *,
    dataset: DatasetSnapshot,
    strategy_name: str,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    decision_events: tuple[ResearchDecisionEvent, ...],
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
    prepared_ticks: tuple[ReplayTick, ...] | None = None,
    prepared_ledger: PortfolioLedger | None = None,
    strategy_evaluator: StrategyEvaluator | None = None,
    risk_gate: RiskGate | None = None,
    execution_simulator: ExecutionSimulator | None = None,
    metrics_collector: MetricsCollector | None = None,
    experiment_recorder: ExperimentRecorder | None = None,
) -> BacktestRun:
    """Execute strategy decision events through the shared research backtest kernel stages."""
    from .backtest_stage_runner import run_stage_owned_decision_event_backtest

    return run_stage_owned_decision_event_backtest(
        dataset=dataset,
        strategy_name=strategy_name,
        parameter_values=parameter_values,
        fee_rate=fee_rate,
        slippage_bps=slippage_bps,
        decision_events=decision_events,
        parameter_stability_score=parameter_stability_score,
        execution_model=execution_model,
        execution_timing_policy=execution_timing_policy,
        portfolio_policy=portfolio_policy,
        context=context,
        prepared_ticks=prepared_ticks,
        prepared_ledger=prepared_ledger,
        strategy_evaluator=strategy_evaluator or DefaultStrategyEvaluator(),
        risk_gate=risk_gate or DefaultRiskGate(),
        execution_simulator=execution_simulator or DefaultExecutionSimulator(),
        metrics_collector=metrics_collector or DefaultMetricsCollector(),
        experiment_recorder=experiment_recorder or DefaultExperimentRecorder(),
    )



# Compatibility re-exports for existing tests and downstream research tooling.
from .backtest_loop import (  # noqa: E402
    ResearchExecutionPlanBundle,
    _execution_plan_evidence,
    _research_execution_plan_bundle,
    _research_position_snapshot,
)
from .execution_simulator import (  # noqa: E402
    ResearchExecutionContext,
    ResearchVirtualExecutionService,
    execution_submit_plan_to_research_request,
)
