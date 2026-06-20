from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Literal, Mapping
import time


FeeState = Literal["finalized", "pending", "blocked", "unknown"]


@dataclass(frozen=True)
class SettlementBarrierConfig:
    max_attempts: int = 5
    poll_intervals_ms: tuple[int, ...] = (100, 250, 500, 1000, 2000)
    deadline_ms: int = 5000


@dataclass(frozen=True)
class OrderSettlementResult:
    client_order_id: str
    exchange_order_id: str | None
    order_terminal: bool
    fill_set_complete: bool
    fee_state: FeeState
    principal_applied: bool
    accounting_finalized: bool
    projection_applied: bool
    broker_local_converged: bool
    settled: bool
    retryable: bool
    deadline_exceeded: bool
    operator_action_required: bool
    reason_code: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "order_terminal": bool(self.order_terminal),
            "fill_set_complete": bool(self.fill_set_complete),
            "fee_state": self.fee_state,
            "principal_applied": bool(self.principal_applied),
            "accounting_finalized": bool(self.accounting_finalized),
            "projection_applied": bool(self.projection_applied),
            "broker_local_converged": bool(self.broker_local_converged),
            "settled": bool(self.settled),
            "retryable": bool(self.retryable),
            "deadline_exceeded": bool(self.deadline_exceeded),
            "operator_action_required": bool(self.operator_action_required),
            "reason_code": self.reason_code,
            "evidence": dict(self.evidence),
        }


def _bool(value: object) -> bool:
    return bool(value)


def _fee_state(value: object) -> FeeState:
    state = str(value or "unknown").strip().lower()
    if state in {"finalized", "pending", "blocked", "unknown"}:
        return state  # type: ignore[return-value]
    if state in {"complete", "accounting_complete"}:
        return "finalized"
    if state in {"fee_pending", "missing", "order_level_candidate"}:
        return "pending"
    return "unknown"


def _snapshot_from_mapping(raw: Mapping[str, Any], *, attempt_index: int) -> dict[str, Any]:
    evidence = dict(raw)
    evidence["attempt_index"] = int(evidence.get("attempt_index", attempt_index))
    evidence.setdefault("order_state", "unknown")
    evidence.setdefault("fill_count", 0)
    evidence.setdefault("fill_set_complete", False)
    evidence.setdefault("paid_fee_present", False)
    evidence.setdefault("order_level_paid_fee_present", bool(evidence.get("paid_fee_present")))
    evidence.setdefault("complete_fill_set_available", bool(evidence.get("fill_set_complete")))
    evidence.setdefault("fee_state", "unknown")
    evidence.setdefault("principal_applied", False)
    evidence.setdefault("accounting_finalized", _fee_state(evidence.get("fee_state")) == "finalized")
    evidence.setdefault("projection_applied", False)
    evidence.setdefault("projected_total_qty", None)
    evidence.setdefault("portfolio_qty", None)
    evidence.setdefault("broker_qty", None)
    evidence.setdefault("broker_local_converged", False)
    evidence.setdefault("reason_code", "settlement_waiting")
    return evidence


def evaluate_settlement_snapshot(
    *,
    client_order_id: str,
    exchange_order_id: str | None,
    evidence: Mapping[str, Any],
    attempts: list[dict[str, Any]],
    deadline_exceeded: bool = False,
) -> OrderSettlementResult:
    fee_state = _fee_state(evidence.get("fee_state"))
    order_state = str(evidence.get("order_state") or "").strip().upper()
    order_terminal = _bool(evidence.get("order_terminal")) or order_state in {
        "FILLED",
        "CANCELED",
        "CANCELLED",
        "REJECTED",
        "DONE",
    }
    fill_set_complete = _bool(evidence.get("fill_set_complete"))
    principal_applied = _bool(evidence.get("principal_applied"))
    accounting_finalized = _bool(evidence.get("accounting_finalized")) and fee_state == "finalized"
    projection_applied = _bool(evidence.get("projection_applied"))
    broker_local_converged = _bool(evidence.get("broker_local_converged"))
    hard_blocked = _bool(evidence.get("hard_blocked")) or fee_state == "blocked"
    settled = bool(
        order_terminal
        and fill_set_complete
        and fee_state == "finalized"
        and principal_applied
        and accounting_finalized
        and projection_applied
        and broker_local_converged
        and not hard_blocked
    )
    reason = str(evidence.get("reason_code") or "").strip() or "settlement_waiting"
    if settled:
        reason = "settled"
    elif hard_blocked:
        reason = reason if reason != "settlement_waiting" else "hard_blocked"
    elif deadline_exceeded:
        reason = "timed_out"
    merged_evidence = dict(evidence)
    merged_evidence["attempts"] = [dict(item) for item in attempts]
    return OrderSettlementResult(
        client_order_id=str(client_order_id),
        exchange_order_id=exchange_order_id,
        order_terminal=order_terminal,
        fill_set_complete=fill_set_complete,
        fee_state=fee_state,
        principal_applied=principal_applied,
        accounting_finalized=accounting_finalized,
        projection_applied=projection_applied,
        broker_local_converged=broker_local_converged,
        settled=settled,
        retryable=not settled and not hard_blocked and not deadline_exceeded,
        deadline_exceeded=bool(deadline_exceeded),
        operator_action_required=bool(hard_blocked or evidence.get("operator_action_required")),
        reason_code=reason,
        evidence=merged_evidence,
    )


class OrderSettlementCoordinator:
    def __init__(
        self,
        config: SettlementBarrierConfig | None = None,
        *,
        monotonic: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self.config = config or SettlementBarrierConfig()
        self._monotonic = monotonic or time.monotonic
        self._sleeper = sleeper or time.sleep

    def settle(
        self,
        *,
        client_order_id: str,
        exchange_order_id: str | None = None,
        observe: Callable[[int], Mapping[str, Any]],
        reconcile: Callable[[], Any] | None = None,
    ) -> OrderSettlementResult:
        start = self._monotonic()
        deadline_at = start + (max(0, int(self.config.deadline_ms)) / 1000.0)
        attempts: list[dict[str, Any]] = []
        last: dict[str, Any] | None = None
        max_attempts = max(1, int(self.config.max_attempts))
        intervals = tuple(int(v) for v in self.config.poll_intervals_ms)

        for attempt_index in range(max_attempts):
            now = self._monotonic()
            if attempt_index > 0 and now > deadline_at:
                break
            if reconcile is not None:
                reconcile()
            last = _snapshot_from_mapping(observe(attempt_index), attempt_index=attempt_index)
            attempts.append(last)
            result = evaluate_settlement_snapshot(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                evidence=last,
                attempts=attempts,
                deadline_exceeded=False,
            )
            if result.settled or result.operator_action_required:
                return result
            if attempt_index >= max_attempts - 1:
                break
            delay_ms = intervals[min(attempt_index, len(intervals) - 1)] if intervals else 0
            if delay_ms > 0:
                sleep_for = min(delay_ms / 1000.0, max(0.0, deadline_at - self._monotonic()))
                if sleep_for > 0:
                    self._sleeper(sleep_for)

        deadline_exceeded = True
        if last is None:
            last = _snapshot_from_mapping({}, attempt_index=0)
        return evaluate_settlement_snapshot(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            evidence=last,
            attempts=attempts,
            deadline_exceeded=deadline_exceeded,
        )


__all__ = [
    "FeeState",
    "OrderSettlementCoordinator",
    "OrderSettlementResult",
    "SettlementBarrierConfig",
    "evaluate_settlement_snapshot",
]
