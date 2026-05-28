from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from bithumb_bot.strategy_policy_contract import PositionSnapshot, StrategyDecisionV2


@dataclass(frozen=True)
class ReplayTick:
    candle: Any
    candle_index: int
    candle_ts: int
    decision_ts: int
    event: Any


@dataclass(frozen=True)
class StrategyEvaluationEnvelope:
    decision: StrategyDecisionV2 | None
    provenance: dict[str, object]
    replay_fingerprint_hash: str
    unsupported_reason: str = ""
    compatibility_fallback: bool = False
    promotion_grade: bool = True
    recommended_next_action: str = "none"


@dataclass(frozen=True)
class RiskGateDecision:
    allow: bool
    block: bool
    override_to_sell: bool
    final_signal: str
    reason_code: str
    evidence_hash: str
    exit_rule: str = ""
    exit_reason: str = ""
    exit_evaluations: tuple[dict[str, object], ...] = ()
    payload: dict[str, object] | None = None


@dataclass(frozen=True)
class StageTrace:
    stage_id: str
    input_hash: str
    output_hash: str
    reason_code: str
    payload: dict[str, object] | None = None

    def as_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "stage_id": self.stage_id,
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
            "reason_code": self.reason_code,
        }
        if self.payload is not None:
            result["payload"] = dict(self.payload)
        return result


class MarketReplayClock(Protocol):
    def ticks(self) -> tuple[ReplayTick, ...]:
        ...


class PortfolioLedgerStage(Protocol):
    def apply_pending_fills(self, boundary_ts: int) -> None:
        ...

    def snapshot_for_policy(self, candle_ts: int, market_price: float) -> PositionSnapshot:
        ...


class StrategyEvaluator(Protocol):
    def evaluate(
        self,
        tick: ReplayTick,
        position_snapshot: PositionSnapshot,
        strategy_context: dict[str, object],
    ) -> StrategyEvaluationEnvelope:
        ...


class RiskGate(Protocol):
    def evaluate(
        self,
        strategy_decision: StrategyDecisionV2 | None,
        position_snapshot: PositionSnapshot,
        market_snapshot: dict[str, object],
        portfolio_snapshot: dict[str, object],
        risk_context: dict[str, object],
    ) -> RiskGateDecision:
        ...


class ExecutionSimulatorStage(Protocol):
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        ...


class MetricsCollector(Protocol):
    def record(self, stage_id: str, payload: dict[str, object]) -> None:
        ...


class ExperimentRecorder(Protocol):
    def record_stage(
        self,
        *,
        stage_id: str,
        input_hash: str,
        output_hash: str,
        reason_code: str,
        payload: dict[str, object] | None = None,
    ) -> None:
        ...
