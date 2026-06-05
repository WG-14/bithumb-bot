from __future__ import annotations

from dataclasses import dataclass

from .reason_codes import POSITION_LOSS_LIMIT
from .risk import (
    DAILY_LOSS_LIMIT_REASON_CODE,
    POSITION_EPSILON,
    RISK_STATE_MISMATCH,
    PureRiskInput,
    evaluate_pure_risk,
)
from .risk_contract import (
    FillEvent,
    RiskDecision,
    RiskEvent,
    RiskPolicy,
    RiskSnapshot,
    SubmitPlan,
    build_risk_decision,
)


@dataclass(frozen=True)
class RiskPolicyEngine:
    policy: RiskPolicy

    def evaluate_pre_decision(self, snapshot: RiskSnapshot) -> RiskDecision:
        return self._evaluate(snapshot=snapshot, evaluation_point="pre_decision")

    def evaluate_pre_submit(self, plan: SubmitPlan, snapshot: RiskSnapshot) -> RiskDecision:
        evidence = {"submit_plan": plan.as_dict(), **dict(snapshot.evidence)}
        return self._evaluate(
            snapshot=RiskSnapshot(
                **{**snapshot.as_dict(), "evidence": evidence}  # type: ignore[arg-type]
            ),
            evaluation_point="pre_submit",
        )

    def evaluate_post_fill(self, fill: FillEvent, snapshot: RiskSnapshot) -> RiskEvent:
        del fill
        decision = self._allow(snapshot=snapshot, evaluation_point="post_fill")
        return RiskEvent(
            evaluation_point="post_fill",
            reason_code=decision.reason_code,
            risk_input_hash=decision.risk_input_hash,
            risk_policy_hash=decision.risk_policy_hash,
            risk_event_hash=decision.risk_decision_hash,
            state_source=decision.state_source,
            evidence=dict(decision.evidence),
        )

    def _evaluate(self, *, snapshot: RiskSnapshot, evaluation_point: str) -> RiskDecision:
        if self.policy.policy_status == "disabled_explicit":
            return build_risk_decision(
                evaluation_point=evaluation_point,  # type: ignore[arg-type]
                status="ALLOW",
                reason_code="RISK_POLICY_DISABLED_EXPLICIT",
                reason="risk policy disabled explicitly",
                allowed_actions=("BUY", "SELL", "HOLD"),
                recommended_action=None,
                snapshot=snapshot,
                policy=self.policy,
                evidence={"risk_policy_status": "disabled_explicit", **dict(snapshot.evidence)},
            )

        if self.policy.kill_switch:
            return build_risk_decision(
                evaluation_point=evaluation_point,  # type: ignore[arg-type]
                status="BLOCK",
                reason_code="KILL_SWITCH",
                reason="KILL_SWITCH=ON",
                allowed_actions=("HOLD",),
                recommended_action="halt",
                snapshot=snapshot,
                policy=self.policy,
                evidence=dict(snapshot.evidence),
            )

        pure = evaluate_pure_risk(
            PureRiskInput(
                evaluation_ts_ms=int(snapshot.evaluation_ts_ms),
                current_equity=snapshot.current_equity,
                baseline_equity=snapshot.baseline_equity,
                loss_today=snapshot.loss_today,
                max_daily_loss_krw=float(self.policy.max_daily_loss_krw),
                mark_price=float(snapshot.mark_price),
                current_cash_krw=snapshot.current_cash_krw,
                current_asset_qty=snapshot.current_asset_qty,
                position_entry_price=snapshot.position_entry_price,
                max_position_loss_pct=float(self.policy.max_position_loss_pct),
                broker_local_mismatch=bool(snapshot.broker_local_mismatch),
                recovery_risk_mismatch_reason=snapshot.recovery_risk_mismatch_reason,
            )
        )
        if pure.blocked:
            status = "REQUIRE_RECONCILE" if pure.reason_code == RISK_STATE_MISMATCH else "BLOCK"
            allowed_actions: tuple[str, ...] = ("HOLD",)
            recommended_action = "reconcile" if pure.reason_code == RISK_STATE_MISMATCH else "halt"
            if pure.reason_code == POSITION_LOSS_LIMIT:
                status = "REDUCE_ONLY"
                allowed_actions = ("SELL", "HOLD")
                recommended_action = "reduce_or_exit"
            evidence = {
                **dict(snapshot.evidence),
                "pure_risk": {
                    "reason_code": pure.reason_code,
                    "daily_loss": pure.daily_loss,
                    "position_loss_pct": pure.position_loss_pct,
                },
            }
            return build_risk_decision(
                evaluation_point=evaluation_point,  # type: ignore[arg-type]
                status=status,  # type: ignore[arg-type]
                reason_code=pure.reason_code,
                reason=pure.reason,
                allowed_actions=allowed_actions,
                recommended_action=recommended_action,
                snapshot=snapshot,
                policy=self.policy,
                evidence=evidence,
            )

        if (
            self.policy.max_open_positions <= 1
            and bool(snapshot.duplicate_entry)
            and float(snapshot.current_asset_qty or 0.0) > POSITION_EPSILON
        ):
            return build_risk_decision(
                evaluation_point=evaluation_point,  # type: ignore[arg-type]
                status="BLOCK",
                reason_code="DUPLICATE_ENTRY",
                reason="duplicate entry blocked",
                allowed_actions=("HOLD", "SELL"),
                recommended_action="hold",
                snapshot=snapshot,
                policy=self.policy,
                evidence=dict(snapshot.evidence),
            )

        if self.policy.max_daily_order_count > 0 and snapshot.daily_order_count is not None:
            today_orders = int(snapshot.daily_order_count)
            limit = int(self.policy.max_daily_order_count)
            if today_orders >= limit:
                return build_risk_decision(
                    evaluation_point=evaluation_point,  # type: ignore[arg-type]
                    status="BLOCK",
                    reason_code="MAX_DAILY_ORDER_COUNT",
                    reason=f"daily order count limit exceeded ({today_orders}/{limit})",
                    allowed_actions=("HOLD",),
                    recommended_action="halt",
                    snapshot=snapshot,
                    policy=self.policy,
                    evidence={**dict(snapshot.evidence), "today_orders": today_orders},
                )

        trade_limit = int(self.policy.max_trade_count_per_day)
        if trade_limit > 0 and snapshot.daily_trade_count is not None:
            today_trades = int(snapshot.daily_trade_count)
            if today_trades >= trade_limit:
                return build_risk_decision(
                    evaluation_point=evaluation_point,  # type: ignore[arg-type]
                    status="BLOCK",
                    reason_code="MAX_TRADE_COUNT_PER_DAY",
                    reason=f"daily trade count limit exceeded ({today_trades}/{trade_limit})",
                    allowed_actions=("HOLD",),
                    recommended_action="halt",
                    snapshot=snapshot,
                    policy=self.policy,
                    evidence={**dict(snapshot.evidence), "today_trades": today_trades},
                )

        drawdown_limit = float(self.policy.max_drawdown_pct)
        if drawdown_limit > 0.0 and snapshot.current_drawdown_pct is not None:
            current_drawdown = float(snapshot.current_drawdown_pct)
            if current_drawdown >= drawdown_limit:
                return build_risk_decision(
                    evaluation_point=evaluation_point,  # type: ignore[arg-type]
                    status="BLOCK",
                    reason_code="MAX_DRAWDOWN_PCT",
                    reason=f"drawdown limit exceeded ({current_drawdown}/{drawdown_limit})",
                    allowed_actions=("HOLD",),
                    recommended_action="halt",
                    snapshot=snapshot,
                    policy=self.policy,
                    evidence={**dict(snapshot.evidence), "current_drawdown_pct": current_drawdown},
                )

        cooldown_min = int(self.policy.cooldown_after_loss_min)
        if cooldown_min > 0 and snapshot.minutes_since_last_loss is not None:
            elapsed = float(snapshot.minutes_since_last_loss)
            if elapsed < float(cooldown_min):
                return build_risk_decision(
                    evaluation_point=evaluation_point,  # type: ignore[arg-type]
                    status="BLOCK",
                    reason_code="COOLDOWN_AFTER_LOSS",
                    reason=f"cooldown after loss active ({elapsed}/{cooldown_min} min)",
                    allowed_actions=("HOLD",),
                    recommended_action="hold",
                    snapshot=snapshot,
                    policy=self.policy,
                    evidence={**dict(snapshot.evidence), "minutes_since_last_loss": elapsed},
                )

        if (
            evaluation_point == "pre_submit"
            and self.policy.unresolved_order_policy == "block"
            and bool(snapshot.unresolved_order_blocked)
        ):
            return build_risk_decision(
                evaluation_point="pre_submit",
                status="REQUIRE_RECONCILE",
                reason_code=str(snapshot.unresolved_order_reason_code),
                reason=str(snapshot.unresolved_order_reason),
                allowed_actions=("HOLD",),
                recommended_action="reconcile",
                snapshot=snapshot,
                policy=self.policy,
                evidence={
                    **dict(snapshot.evidence),
                    "unresolved_order_gate": {
                        "blocked": True,
                        "reason_code": str(snapshot.unresolved_order_reason_code),
                        "reason": str(snapshot.unresolved_order_reason),
                    },
                },
            )

        return self._allow(
            snapshot=snapshot,
            evaluation_point=evaluation_point,
            evidence=dict(snapshot.evidence),
        )

    def _allow(
        self,
        *,
        snapshot: RiskSnapshot,
        evaluation_point: str,
        evidence: dict[str, object] | None = None,
    ) -> RiskDecision:
        return build_risk_decision(
            evaluation_point=evaluation_point,  # type: ignore[arg-type]
            status="ALLOW",
            reason_code="OK",
            reason="ok",
            allowed_actions=("BUY", "SELL", "HOLD"),
            recommended_action=None,
            snapshot=snapshot,
            policy=self.policy,
            evidence=evidence or {},
        )
