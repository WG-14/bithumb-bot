from __future__ import annotations

import pytest

from bithumb_bot.execution_service import ExecutionSubmitPlan


def _plan() -> ExecutionSubmitPlan:
    return ExecutionSubmitPlan(
        side="BUY",
        source="target_delta",
        authority="canonical_target_delta_sizing",
        final_action="REBALANCE_TO_TARGET",
        qty=0.001,
        notional_krw=100_000.0,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=100_000.0,
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        idempotency_key="unit-key",
        extra_payload={
            "portfolio_target_authoritative": True,
            "portfolio_target_hash": "sha256:portfolio",
            "allocation_decision_hash": "sha256:allocation",
            "strategy_contribution_hash": "sha256:strategy",
        },
    )


def test_as_final_payload_does_not_open_db(monkeypatch) -> None:
    def _fail(*_args, **_kwargs):
        raise AssertionError("ensure_db must not be called by as_final_payload")

    monkeypatch.setattr("bithumb_bot.execution_service.ensure_db", _fail)

    payload = _plan().as_final_payload()

    assert payload["schema_version"] == 1
    assert payload["content_hash"]


def test_as_final_payload_does_not_call_broker() -> None:
    class Broker:
        calls = 0

        def get_balance_snapshot(self):  # pragma: no cover - must not be reached
            self.calls += 1
            raise AssertionError("broker must not be called by as_final_payload")

    broker = Broker()
    payload = _plan().as_final_payload(extra={"broker_reference": "unit"})

    assert payload["broker_reference"] == "unit"
    assert broker.calls == 0


def test_content_hash_ignores_pre_submit_risk_fields() -> None:
    plan = _plan()
    base_hash = plan.content_hash()
    with_proof = plan.as_final_payload(
        extra={
            "pre_submit_risk_status": "ALLOW",
            "pre_submit_risk_decision_hash": "sha256:" + "1" * 64,
            "pre_submit_risk_evidence_hash": "sha256:" + "2" * 64,
        }
    )

    assert base_hash == plan.content_hash()
    assert with_proof["submit_plan_hash"] == base_hash
