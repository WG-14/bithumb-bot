from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from .reason_codes import (
    INSUFFICIENT_STRATEGY_HISTORY,
    MAX_DRAWDOWN_PCT,
    RISK_METRIC_DENOMINATOR_MISSING,
    RISK_METRIC_RECONSTRUCTION_FAILED,
    RISK_METRIC_SCOPE_MISMATCH,
    RISK_METRIC_UNIT_MISMATCH,
)

from .canonical_decision import canonical_payload_hash

RiskEvaluationPoint = Literal["pre_decision", "pre_submit", "post_fill", "resume"]
RiskDecisionStatus = Literal["ALLOW", "BLOCK", "REQUIRE_RECONCILE", "REDUCE_ONLY", "FORCE_EXIT"]
RiskMetricUnit = Literal["ratio", "percent_point", "krw"]
RiskMetricScope = Literal["account", "strategy_instance", "risk_scope"]
RiskMetricDenominatorKind = Literal[
    "account_equity",
    "allocated_capital",
    "strategy_initial_capital",
    "peak_strategy_equity",
]
RiskMetricState = Literal["valid", "undefined", "insufficient_history", "reconstruction_failed"]


@dataclass(frozen=True)
class RiskPolicy:
    schema_version: int = 1
    max_daily_loss_krw: float = 0.0
    max_position_loss_pct: float = 0.0
    max_daily_order_count: int = 0
    max_trade_count_per_day: int = 0
    max_drawdown_pct: float = 0.0
    cooldown_after_loss_min: int = 0
    kill_switch: bool = False
    max_open_positions: int = 1
    unresolved_order_policy: str = "block"
    policy_status: str = "enabled"
    missing_policy: str = "fail_closed_for_promotion"
    source: str = "settings"

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "max_daily_loss_krw": float(self.max_daily_loss_krw),
            "max_position_loss_pct": float(self.max_position_loss_pct),
            "max_daily_order_count": int(self.max_daily_order_count),
            "max_trade_count_per_day": int(self.max_trade_count_per_day),
            "max_drawdown_pct": float(self.max_drawdown_pct),
            "cooldown_after_loss_min": int(self.cooldown_after_loss_min),
            "kill_switch": bool(self.kill_switch),
            "max_open_positions": int(self.max_open_positions),
            "unresolved_order_policy": str(self.unresolved_order_policy),
            "policy_status": str(self.policy_status),
            "missing_policy": str(self.missing_policy),
            "source": str(self.source),
        }

    def effective_limits(self) -> dict[str, object]:
        return {
            "max_daily_loss_krw": float(self.max_daily_loss_krw),
            "max_position_loss_pct": float(self.max_position_loss_pct),
            "max_daily_order_count": int(self.max_daily_order_count),
            "max_trade_count_per_day": int(self.max_trade_count_per_day),
            "max_drawdown_pct": float(self.max_drawdown_pct),
            "cooldown_after_loss_min": int(self.cooldown_after_loss_min),
            "max_open_positions": int(self.max_open_positions),
            "kill_switch": bool(self.kill_switch),
            "unresolved_order_policy": str(self.unresolved_order_policy),
            "risk_policy_status": str(self.policy_status),
        }

    def policy_hash(self) -> str:
        return canonical_payload_hash(self.as_dict())


@dataclass(frozen=True)
class RiskMetric:
    value: float | None
    unit: RiskMetricUnit
    scope: RiskMetricScope
    denominator_kind: RiskMetricDenominatorKind
    denominator_value: float | None
    sample_count: int
    state: RiskMetricState
    source_table: str
    formula_version: str
    reason_code: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "value": self.value,
            "unit": self.unit,
            "scope": self.scope,
            "denominator_kind": self.denominator_kind,
            "denominator_value": self.denominator_value,
            "sample_count": int(self.sample_count),
            "state": self.state,
            "source_table": self.source_table,
            "formula_version": self.formula_version,
            "reason_code": self.reason_code,
        }


DrawdownMetric = RiskMetric


@dataclass(frozen=True)
class RiskLimit:
    value: float
    unit: RiskMetricUnit
    scope: RiskMetricScope
    reason_code: str = MAX_DRAWDOWN_PCT

    def as_dict(self) -> dict[str, object]:
        return {
            "value": float(self.value),
            "unit": self.unit,
            "scope": self.scope,
            "reason_code": self.reason_code,
        }


@dataclass(frozen=True)
class RiskMetricComparison:
    exceeded: bool
    reason_code: str
    metric: RiskMetric
    limit: RiskLimit

    def as_dict(self) -> dict[str, object]:
        return {
            "exceeded": bool(self.exceeded),
            "reason_code": self.reason_code,
            "metric": self.metric.as_dict(),
            "limit": self.limit.as_dict(),
        }


def compare_risk_metric_to_limit(metric: RiskMetric, limit: RiskLimit) -> RiskMetricComparison:
    if metric.unit != limit.unit:
        return RiskMetricComparison(False, RISK_METRIC_UNIT_MISMATCH, metric, limit)
    if metric.scope != limit.scope:
        return RiskMetricComparison(False, RISK_METRIC_SCOPE_MISMATCH, metric, limit)
    if metric.state != "valid":
        if metric.state == "insufficient_history":
            reason = INSUFFICIENT_STRATEGY_HISTORY
        elif metric.state == "reconstruction_failed":
            reason = RISK_METRIC_RECONSTRUCTION_FAILED
        else:
            reason = metric.reason_code or "RISK_METRIC_UNDEFINED"
        return RiskMetricComparison(False, reason, metric, limit)
    try:
        denominator = float(metric.denominator_value or 0.0)
    except (TypeError, ValueError):
        denominator = 0.0
    if denominator <= 0.0:
        return RiskMetricComparison(False, RISK_METRIC_DENOMINATOR_MISSING, metric, limit)
    try:
        value = float(metric.value if metric.value is not None else 0.0)
    except (TypeError, ValueError):
        return RiskMetricComparison(False, RISK_METRIC_RECONSTRUCTION_FAILED, metric, limit)
    return RiskMetricComparison(value >= float(limit.value), limit.reason_code if value >= float(limit.value) else "OK", metric, limit)


@dataclass(frozen=True)
class RiskSnapshot:
    evaluation_ts_ms: int
    mark_price: float
    current_equity: float | None = None
    baseline_equity: float | None = None
    loss_today: float | None = None
    current_cash_krw: float | None = None
    current_asset_qty: float | None = None
    position_entry_price: float | None = None
    broker_local_mismatch: bool = False
    recovery_risk_mismatch_reason: str | None = None
    duplicate_entry: bool = False
    daily_order_count: int | None = None
    daily_trade_count: int | None = None
    current_drawdown_pct: float | None = None
    current_drawdown_metric: RiskMetric | None = None
    minutes_since_last_loss: float | None = None
    unresolved_order_blocked: bool = False
    unresolved_order_reason_code: str = "OK"
    unresolved_order_reason: str = "ok"
    state_source: str = "unknown"
    evidence: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        metric = self.current_drawdown_metric
        if isinstance(metric, dict):
            object.__setattr__(
                self,
                "current_drawdown_metric",
                RiskMetric(
                    value=metric.get("value"),  # type: ignore[arg-type]
                    unit=metric.get("unit", "percent_point"),  # type: ignore[arg-type]
                    scope=metric.get("scope", "risk_scope"),  # type: ignore[arg-type]
                    denominator_kind=metric.get("denominator_kind", "allocated_capital"),  # type: ignore[arg-type]
                    denominator_value=metric.get("denominator_value"),  # type: ignore[arg-type]
                    sample_count=int(metric.get("sample_count", 0) or 0),
                    state=metric.get("state", "undefined"),  # type: ignore[arg-type]
                    source_table=str(metric.get("source_table") or ""),
                    formula_version=str(metric.get("formula_version") or ""),
                    reason_code=str(metric.get("reason_code") or ""),
                ),
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "evaluation_ts_ms": int(self.evaluation_ts_ms),
            "mark_price": float(self.mark_price),
            "current_equity": self.current_equity,
            "baseline_equity": self.baseline_equity,
            "loss_today": self.loss_today,
            "current_cash_krw": self.current_cash_krw,
            "current_asset_qty": self.current_asset_qty,
            "position_entry_price": self.position_entry_price,
            "broker_local_mismatch": bool(self.broker_local_mismatch),
            "recovery_risk_mismatch_reason": self.recovery_risk_mismatch_reason,
            "duplicate_entry": bool(self.duplicate_entry),
            "daily_order_count": self.daily_order_count,
            "daily_trade_count": self.daily_trade_count,
            "current_drawdown_pct": self.current_drawdown_pct,
            "current_drawdown_metric": (
                None if self.current_drawdown_metric is None else self.current_drawdown_metric.as_dict()
            ),
            "minutes_since_last_loss": self.minutes_since_last_loss,
            "unresolved_order_blocked": bool(self.unresolved_order_blocked),
            "unresolved_order_reason_code": str(self.unresolved_order_reason_code),
            "unresolved_order_reason": str(self.unresolved_order_reason),
            "state_source": str(self.state_source),
            "evidence": dict(self.evidence),
        }

    def input_hash(self) -> str:
        return canonical_payload_hash(self.as_dict())


@dataclass(frozen=True)
class SubmitPlan:
    side: str
    qty: float
    notional_krw: float | None = None
    source: str = "unknown"
    evidence: dict[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "side": str(self.side).upper(),
            "qty": float(self.qty),
            "notional_krw": self.notional_krw,
            "source": str(self.source),
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True)
class FillEvent:
    side: str
    qty: float
    price: float
    ts_ms: int
    evidence: dict[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "side": str(self.side).upper(),
            "qty": float(self.qty),
            "price": float(self.price),
            "ts_ms": int(self.ts_ms),
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True)
class RiskDecision:
    evaluation_point: RiskEvaluationPoint
    status: RiskDecisionStatus
    reason_code: str
    reason: str
    allowed_actions: tuple[str, ...]
    recommended_action: str | None
    risk_input_hash: str
    risk_policy_hash: str
    risk_evidence_hash: str
    risk_decision_hash: str
    effective_limits: dict[str, object]
    state_source: str
    evidence: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "evaluation_point": self.evaluation_point,
            "status": self.status,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "allowed_actions": list(self.allowed_actions),
            "recommended_action": self.recommended_action,
            "risk_input_hash": self.risk_input_hash,
            "risk_policy_hash": self.risk_policy_hash,
            "risk_evidence_hash": self.risk_evidence_hash,
            "risk_decision_hash": self.risk_decision_hash,
            "effective_limits": dict(self.effective_limits),
            "state_source": self.state_source,
            "evidence": dict(self.evidence),
        }

    def identity_fields(self) -> dict[str, object]:
        return {
            "risk_input_hash": self.risk_input_hash,
            "risk_policy_hash": self.risk_policy_hash,
            "risk_evidence_hash": self.risk_evidence_hash,
            "risk_decision_hash": self.risk_decision_hash,
            "risk_reason_code": self.reason_code,
            "risk_status": self.status,
            "risk_evaluation_point": self.evaluation_point,
            "risk_state_source": self.state_source,
            "effective_risk_limits": dict(self.effective_limits),
        }


@dataclass(frozen=True)
class RiskEvent:
    evaluation_point: RiskEvaluationPoint
    reason_code: str
    risk_input_hash: str
    risk_policy_hash: str
    risk_event_hash: str
    state_source: str
    evidence: dict[str, object]


class RiskEngine(Protocol):
    def evaluate_pre_decision(self, snapshot: RiskSnapshot) -> RiskDecision:
        ...

    def evaluate_pre_submit(self, plan: SubmitPlan, snapshot: RiskSnapshot) -> RiskDecision:  # broker=not_applicable_protocol
        ...

    def evaluate_post_fill(self, fill: FillEvent, snapshot: RiskSnapshot) -> RiskEvent:
        ...


def build_risk_decision(
    *,
    evaluation_point: RiskEvaluationPoint,
    status: RiskDecisionStatus,
    reason_code: str,
    reason: str,
    allowed_actions: tuple[str, ...],
    recommended_action: str | None,
    snapshot: RiskSnapshot,
    policy: RiskPolicy,
    evidence: dict[str, object] | None = None,
) -> RiskDecision:
    input_hash = snapshot.input_hash()
    policy_hash = policy.policy_hash()
    effective_limits = policy.effective_limits()
    decision_evidence = dict(evidence or {})
    # Evidence is hashed as its own canonical payload before the decision hash
    # is computed. The decision hash then binds policy, input, evidence hash,
    # and decision outcome while preserving the full evidence payload for audit.
    evidence_hash = canonical_payload_hash(decision_evidence)
    payload_without_hash = {
        "evaluation_point": evaluation_point,
        "status": status,
        "reason_code": str(reason_code),
        "reason": str(reason),
        "allowed_actions": list(allowed_actions),
        "recommended_action": recommended_action,
        "risk_input_hash": input_hash,
        "risk_policy_hash": policy_hash,
        "risk_evidence_hash": evidence_hash,
        "effective_limits": effective_limits,
        "state_source": snapshot.state_source,
        "evidence": decision_evidence,
    }
    decision_hash = canonical_payload_hash(payload_without_hash)
    return RiskDecision(
        evaluation_point=evaluation_point,
        status=status,
        reason_code=str(reason_code),
        reason=str(reason),
        allowed_actions=tuple(str(action).upper() for action in allowed_actions),
        recommended_action=recommended_action,
        risk_input_hash=input_hash,
        risk_policy_hash=policy_hash,
        risk_evidence_hash=evidence_hash,
        risk_decision_hash=decision_hash,
        effective_limits=effective_limits,
        state_source=snapshot.state_source,
        evidence=decision_evidence,
    )


def risk_identity_fields(decision: RiskDecision) -> dict[str, object]:
    return decision.identity_fields()
