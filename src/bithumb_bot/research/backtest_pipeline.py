from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Protocol

from . import backtest_support as support
from .backtest_stages import (
    ExperimentRecorder,
    MarketReplayClock,
    MetricsCollector,
    PortfolioLedgerStage,
    ReplayTick,
    RiskGate,
    StrategyEvaluator,
)
from .execution_simulator_stage import DefaultExecutionSimulator
from .experiment_manifest import ExecutionTimingPolicy, legacy_research_portfolio_policy
from .portfolio_ledger import PortfolioLedger
from .risk_gate_stage import DefaultRiskGate
from .strategy_evaluator_stage import DefaultStrategyEvaluator

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
from .execution_planning import (  # noqa: E402
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
