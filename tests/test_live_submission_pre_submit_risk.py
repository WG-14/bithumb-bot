from __future__ import annotations

from dataclasses import dataclass

from bithumb_bot.broker.live_submission_execution import _build_live_submission_pre_submit_risk_fields
from bithumb_bot.submit_authority_policy import is_pre_submit_risk_approved_for_plan


PLAN_HASH = "sha256:" + "a" * 64


@dataclass(frozen=True)
class _RiskDecision:
    status: str
    reason_code: str
    allowed_actions: tuple[str, ...]
    risk_decision_hash: str = "sha256:" + "1" * 64
    risk_policy_hash: str = "sha256:" + "2" * 64
    risk_input_hash: str = "sha256:" + "3" * 64
    risk_evidence_hash: str = "sha256:" + "4" * 64
    state_source: str = "unit"
    evidence: dict[str, object] | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "reason_code": self.reason_code,
            "allowed_actions": list(self.allowed_actions),
        }


def _decision_observability(
    *,
    side: str,
    target_delta_qty: float,
    plan_hash: str = PLAN_HASH,
) -> dict[str, object]:
    return {
        "execution_submit_plan_hash": plan_hash,
        "execution_submit_plan_source": "target_delta",
        "execution_submit_plan_authority": "canonical_target_delta_sizing",
        "submit_expected": True,
        "target_delta_qty": target_delta_qty,
        "effective_pre_submit_risk_policy_hash": "sha256:" + "2" * 64,
        "risk_policy_source": "strategy_risk_profiles",
        "pre_submit_risk_policy_composition_rule": "most_restrictive_selected_strategy_policy",
        "strategy_risk_profile_hashes": ["sha256:" + "8" * 64],
        "side": side,
    }


def _approval(*, side: str, target_delta_qty: float, plan_hash: str = PLAN_HASH, expected_hash: str = PLAN_HASH):
    fields = _build_live_submission_pre_submit_risk_fields(
        risk_decision=_RiskDecision(
            status="REDUCE_ONLY",
            reason_code="POSITION_LOSS_LIMIT",
            allowed_actions=("SELL", "HOLD"),
        ),
        side=side,
        decision_observability=_decision_observability(
            side=side,
            target_delta_qty=target_delta_qty,
            plan_hash=plan_hash,
        ),
    )
    return is_pre_submit_risk_approved_for_plan(fields, expected_submit_plan_hash=expected_hash)


def test_live_submission_reduce_only_target_delta_sell_passes_pre_submit_halt() -> None:
    approval = _approval(side="SELL", target_delta_qty=-0.00109271)

    assert approval.approved is True
    assert approval.status == "REDUCE_ONLY"


def test_live_submission_reduce_only_buy_is_blocked() -> None:
    approval = _approval(side="BUY", target_delta_qty=0.00109271)

    assert approval.approved is False
    assert approval.reason == "live_real_order_pre_submit_risk_reduce_only_not_authorized_for_plan"


def test_live_submission_reduce_only_sell_plan_hash_mismatch_is_blocked() -> None:
    approval = _approval(
        side="SELL",
        target_delta_qty=-0.00109271,
        plan_hash="sha256:" + "b" * 64,
        expected_hash=PLAN_HASH,
    )

    assert approval.approved is False
    assert approval.reason == "live_real_order_pre_submit_risk_plan_hash_mismatch"
