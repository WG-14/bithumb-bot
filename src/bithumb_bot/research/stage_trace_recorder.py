from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bithumb_bot.canonical_decision import canonical_payload_hash

from .backtest_stages import StageTrace


@dataclass
class StageTraceRecorder:
    """Records deterministic stage input/output hashes for observability only."""

    traces: list[StageTrace] = field(default_factory=list)

    def record_strategy(
        self,
        *,
        replay_tick_hash: str,
        position_snapshot_hash: str,
        strategy_decision_hash: str,
        compatibility_fallback: bool,
        unsupported_reason: str,
        recommended_next_action: str,
    ) -> StageTrace:
        return self.record(
            StageTrace(
                stage_id="strategy",
                input_hash=canonical_payload_hash(
                    {"replay_tick_hash": replay_tick_hash, "position_snapshot_hash": position_snapshot_hash}
                ),
                output_hash=strategy_decision_hash,
                reason_code=str(unsupported_reason or "OK"),
                payload={
                    "replay_tick_hash": replay_tick_hash,
                    "position_snapshot_hash": position_snapshot_hash,
                    "strategy_decision_hash": strategy_decision_hash,
                    "compatibility_fallback": bool(compatibility_fallback),
                    "recommended_next_action": recommended_next_action,
                },
            )
        )

    def record_risk(self, *, input_hash: str, risk_gate_hash: str, reason_code: str) -> StageTrace:
        return self.record(
            StageTrace(
                stage_id="risk",
                input_hash=input_hash,
                output_hash=risk_gate_hash,
                reason_code=reason_code,
                payload={"risk_gate_hash": risk_gate_hash},
            )
        )

    def record_execution(
        self,
        *,
        input_hash: str,
        execution_plan_hash: str,
        fill_hash: str,
        reason_code: str,
    ) -> StageTrace:
        return self.record(
            StageTrace(
                stage_id="execution",
                input_hash=input_hash,
                output_hash=execution_plan_hash,
                reason_code=reason_code,
                payload={"execution_plan_hash": execution_plan_hash, "fill_hash": fill_hash},
            )
        )

    def record_observability_error(
        self,
        *,
        stage_id: str,
        input_hash: str,
        reason_code: str,
        payload: dict[str, object],
    ) -> StageTrace:
        output_hash = canonical_payload_hash(
            {
                "stage_id": str(stage_id),
                "reason_code": str(reason_code),
                "payload": dict(payload),
            }
        )
        return self.record(
            StageTrace(
                stage_id=str(stage_id),
                input_hash=str(input_hash),
                output_hash=output_hash,
                reason_code=str(reason_code),
                payload=dict(payload),
            )
        )

    def record_ledger_and_equity(
        self,
        *,
        execution_plan_hash: str,
        ledger_snapshot: dict[str, object],
        mark_boundary_ts: int,
        mark_cash: float,
        mark_qty: float,
        mark_price: float,
    ) -> tuple[StageTrace, StageTrace]:
        ledger_hash = canonical_payload_hash(ledger_snapshot)
        equity_hash = canonical_payload_hash(
            {
                "ts": int(mark_boundary_ts),
                "cash": round(float(mark_cash), 12),
                "asset_qty": round(float(mark_qty), 12),
                "mark_price": round(float(mark_price), 12),
            }
        )
        ledger_trace = self.record(
            StageTrace(
                stage_id="ledger",
                input_hash=execution_plan_hash,
                output_hash=ledger_hash,
                reason_code="OK",
                payload={"ledger_hash": ledger_hash},
            )
        )
        equity_trace = self.record(
            StageTrace(
                stage_id="equity",
                input_hash=ledger_hash,
                output_hash=equity_hash,
                reason_code="OK",
                payload={"equity_hash": equity_hash},
            )
        )
        return ledger_trace, equity_trace

    def record(self, trace: StageTrace) -> StageTrace:
        self.traces.append(trace)
        return trace

    def latest_dicts(self, count: int) -> list[dict[str, object]]:
        return [trace.as_dict() for trace in self.traces[-count:]]

    def flush_latest(self, *, count: int, metrics_collector: Any | None, experiment_recorder: Any | None, event_number: int) -> None:
        latest = self.traces[-count:]
        if metrics_collector is not None:
            metrics_collector.record(
                "stage_trace",
                {"event_number": event_number, "stage_traces": [trace.as_dict() for trace in latest]},
            )
        if experiment_recorder is not None:
            for trace in latest:
                experiment_recorder.record_stage(**trace.as_dict())


__all__ = ["StageTraceRecorder"]
