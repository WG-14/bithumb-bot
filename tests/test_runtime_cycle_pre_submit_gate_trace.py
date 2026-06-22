from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from bithumb_bot.config import settings
from bithumb_bot.execution_service import (
    ExecutionDecisionSummary,
    ExecutionSubmitPlan,
    LiveSignalExecutionService,
)
from bithumb_bot.runtime.cycle_artifact_assembler import RuntimeCycleArtifactAssembler
from bithumb_bot.runtime.execution_coordinator import ExecutionCoordinator
from bithumb_bot.risk_contract import RiskDecision, RiskPolicy


@pytest.fixture(autouse=True)
def _restore_settings():
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "EXECUTION_ENGINE": settings.EXECUTION_ENGINE,
    }
    yield
    for key, value in original.items():
        object.__setattr__(settings, key, value)


def _arm_live_target_delta() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")


def _target_submit_plan() -> ExecutionSubmitPlan:
    policy = RiskPolicy(source="unit", max_daily_loss_krw=50_000.0)
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
            "pair": "KRW-BTC",
            "portfolio_target_authoritative": True,
            "portfolio_target_hash": "sha256:portfolio",
            "allocation_decision_hash": "sha256:allocation",
            "strategy_contribution_hash": "sha256:strategy",
            "pre_submit_risk_required": True,
            "portfolio_risk_policy_hash": "sha256:" + "9" * 64,
            "strategy_risk_profiles": [
                {
                    "strategy_instance_id": "h74-source-observation",
                    "strategy_name": "daily_participation_sma",
                    "strategy_risk_profile_hash": "sha256:" + "8" * 64,
                    "risk_policy": policy.as_dict(),
                    "risk_policy_hash": policy.policy_hash(),
                }
            ],
        },
    )


def _summary(plan: ExecutionSubmitPlan) -> ExecutionDecisionSummary:
    return ExecutionDecisionSummary(
        raw_signal="BUY",
        final_signal="BUY",
        final_action="REBALANCE_TO_TARGET",
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=100_000.0,
        current_effective_exposure_krw=0.0,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=100_000.0,
        residual_live_sell_mode="block",
        residual_buy_sizing_mode="block",
        residual_submit_plan=None,
        buy_submit_plan=None,
        target_shadow_decision=None,
        target_submit_plan=plan,
    )


def _decision(status: str = "BLOCK") -> RiskDecision:
    policy = RiskPolicy(source="unit", max_daily_loss_krw=50_000.0)
    return RiskDecision(
        evaluation_point="pre_submit",
        status=status,  # type: ignore[arg-type]
        reason_code="RISK_STATE_MISMATCH",
        reason="RISK_STATE_MISMATCH",
        allowed_actions=("HOLD",),
        recommended_action="halt",
        risk_input_hash="sha256:" + "1" * 64,
        risk_policy_hash=policy.policy_hash(),
        risk_evidence_hash="sha256:" + "2" * 64,
        risk_decision_hash="sha256:" + "3" * 64,
        effective_limits=policy.effective_limits(),
        state_source="runtime_db_broker",
        evidence={
            "current_asset_qty": 1.0,
            "submit_qty": 0.001,
            "current_asset_qty_source": "broker_current_position",
            "submit_plan_qty_source": "submit_plan.qty",
        },
    )


def _seed_execution_plan(db_path, submit_hash: str) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE execution_plan (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_submit_plan_hash TEXT,
                execution_submit_plan_json TEXT,
                submit_plan_side TEXT,
                submit_plan_qty REAL,
                submit_plan_notional_krw REAL,
                submit_plan_idempotency_key TEXT,
                submit_plan_source TEXT,
                submit_plan_authority TEXT,
                submit_expected INTEGER NOT NULL DEFAULT 0,
                final_action TEXT NOT NULL DEFAULT '',
                block_reason TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            "INSERT INTO execution_plan(execution_submit_plan_hash, execution_submit_plan_json) VALUES (?, ?)",
            (submit_hash, "{}"),
        )
        conn.commit()
    finally:
        conn.close()


class _Batch:
    pair_plans: tuple[object, ...]

    def __init__(self, pair_plan: object) -> None:
        self.pair_plans = (pair_plan,)

    def content_hash(self) -> str:
        return "sha256:batch"


def _execution_plan_bundle(plan: ExecutionSubmitPlan) -> object:
    pair_plan = SimpleNamespace(
        pair="KRW-BTC",
        execution_submit_plan_hash=plan.content_hash(),
        scope_key_hashes=("sha256:scope",),
        order_rule_snapshot_hash="sha256:rules",
        pre_submit_risk_required=True,
        pre_submit_risk_decision_hash="",
        pre_submit_risk_finalization_required=True,
        pre_submit_risk_not_required_reason="",
        lock_evidence_hash="sha256:lock",
        lock_status="active",
    )
    return SimpleNamespace(execution_plan_batch=_Batch(pair_plan))


def _decision_result(
    summary: ExecutionDecisionSummary,
    *,
    hard_gate_trace_entries: tuple[dict[str, object], ...] = (),
) -> object:
    return SimpleNamespace(
        candle_ts=1_800_000_000_000,
        decision_id=1,
        signal="BUY",
        market_price=100_000_000.0,
        strategy_name="daily_participation_sma",
        reason="unit",
        exit_rule_name=None,
        decision_context={"runtime_pair": "KRW-BTC", "execution_plan_batch_hash": "sha256:batch"},
        execution_plan_bundle=_execution_plan_bundle(summary.target_submit_plan),
        execution_decision_summary=summary,
        strategy_decision_hash="sha256:strategy",
        runtime_strategy_decision_bundle_id=None,
        runtime_strategy_decision_bundle_hash=None,
        portfolio_allocation_decision_id=None,
        portfolio_allocation_decision_hash=None,
        portfolio_target_id=None,
        portfolio_target_hash=None,
        strategy_contribution_hash=None,
        execution_plan_id=None,
        execution_plan_bundle_hash="sha256:bundle",
        execution_submit_plan_hash=summary.target_submit_plan.content_hash(),
        strategy_virtual_lifecycle_transition_hashes=(),
        strategy_risk_decision_hash=None,
        strategy_risk_policy_hash=None,
        strategy_risk_input_hash=None,
        strategy_risk_evidence_hash=None,
        strategy_risk_state_source=None,
        strategy_risk_status="ALLOW",
        strategy_risk_reason_code="OK",
        portfolio_risk_decision_hash=None,
        portfolio_risk_policy_hash=None,
        portfolio_risk_input_hash=None,
        portfolio_risk_evidence_hash=None,
        portfolio_risk_state_source=None,
        portfolio_risk_status="ALLOW",
        portfolio_risk_reason_code="OK",
        pre_submit_risk_decision_hash=None,
        pre_submit_risk_policy_hash=None,
        pre_submit_risk_input_hash=None,
        pre_submit_risk_evidence_hash=None,
        pre_submit_risk_plan_hash=None,
        pre_submit_risk_state_source=None,
        pre_submit_risk_status=None,
        pre_submit_risk_reason_code=None,
        hard_gate_trace_entries=hard_gate_trace_entries,
        failure_phase=None,
        failure_subphase=None,
        failure_reason_code=None,
        failure_detail=None,
        operator_next_action=None,
        failure_evidence_hash=None,
        persistence_failure_metadata={},
        db_subphase=None,
        sql_group=None,
        persistence_retry_count=None,
        persistence_max_retry_count=None,
        transaction_elapsed_ms=None,
        lock_wait_elapsed_ms=None,
    )


def _execute_pre_submit_block(
    tmp_path,
    monkeypatch,
    *,
    status: str = "BLOCK",
    hard_gate_trace_entries: tuple[dict[str, object], ...] = (),
) -> dict[str, object]:
    _arm_live_target_delta()
    plan = _target_submit_plan()
    summary = _summary(plan)
    db_path = tmp_path / f"runtime-pre-submit-{status.lower()}.sqlite"
    _seed_execution_plan(db_path, plan.as_final_payload()["submit_plan_hash"])
    submit_calls: list[object] = []

    def _runtime_db_factory():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        return conn

    monkeypatch.setattr(
        "bithumb_bot.runtime_risk_engine.RuntimeRiskEngineAdapter.evaluate_pre_submit",
        lambda *_args, **_kwargs: _decision(status),
    )
    service = LiveSignalExecutionService(
        broker=object(),
        executor=lambda *_args, **_kwargs: submit_calls.append(_kwargs) or {"status": "submitted"},
        harmless_dust_recorder=lambda **_kwargs: False,
        db_factory=_runtime_db_factory,
    )
    decision_result = _decision_result(summary, hard_gate_trace_entries=hard_gate_trace_entries)
    execution_result = ExecutionCoordinator("target_delta").execute_cycle(
        candle_ts=decision_result.candle_ts,
        decision_id=decision_result.decision_id,
        signal=decision_result.signal,
        market_price=decision_result.market_price,
        strategy_name=decision_result.strategy_name,
        decision_reason=decision_result.reason,
        decision_context=decision_result.decision_context,
        execution_plan_bundle=decision_result.execution_plan_bundle,
        execution_decision_summary=summary,
        execution_service=service,
    )
    artifact = RuntimeCycleArtifactAssembler().from_cycle_results(
        cycle_id="checkpoint:processed",
        startup_state="READY",
        decision_result=decision_result,
        execution_result=execution_result,
    ).as_dict()
    return {
        "artifact": artifact,
        "execution_result": execution_result.as_dict(),
        "submit_calls": submit_calls,
    }


def test_runtime_cycle_artifact_contains_execution_time_pre_submit_block(tmp_path, monkeypatch) -> None:
    result = _execute_pre_submit_block(tmp_path, monkeypatch)

    assert result["submit_calls"] == []
    artifact = result["artifact"]
    pre_submit = artifact["gate_trace"][-1]
    assert pre_submit["gate"] == "pre_submit_risk"
    assert pre_submit["status"] == "BLOCK"
    assert pre_submit["reason_code"] == "RISK_STATE_MISMATCH"
    assert pre_submit["blocking"] is True
    assert artifact["primary_block_gate"] == "pre_submit_risk"
    assert artifact["primary_block_reason"] == "RISK_STATE_MISMATCH"
    execution_result = result["execution_result"]
    assert execution_result["submitted"] is False
    assert execution_result["pre_submit_risk_decision_hash"] == "sha256:" + "3" * 64
    assert execution_result["pre_submit_risk_evidence_hash"] == "sha256:" + "2" * 64


def test_runtime_cycle_artifact_primary_block_gate_from_execution_result(tmp_path, monkeypatch) -> None:
    result = _execute_pre_submit_block(tmp_path, monkeypatch, status="REQUIRE_RECONCILE")

    artifact = result["artifact"]
    pre_submit = artifact["gate_trace"][-1]
    assert pre_submit["gate"] == "pre_submit_risk"
    assert pre_submit["status"] == "REQUIRE_RECONCILE"
    assert pre_submit["reason_code"] == "RISK_STATE_MISMATCH"
    assert pre_submit["blocking"] is True
    assert artifact["primary_block_gate"] == "pre_submit_risk"
    assert artifact["primary_block_reason"] == "RISK_STATE_MISMATCH"


def test_entry_authority_precedes_pre_submit_gate(tmp_path, monkeypatch) -> None:
    result = _execute_pre_submit_block(
        tmp_path,
        monkeypatch,
        hard_gate_trace_entries=(
            {
                "gate": "entry_authority",
                "status": "BLOCK",
                "reason_code": "target_delta_entry_without_strategy_buy_authority",
                "input_hash": "sha256:entry-input",
                "evidence_hash": "sha256:entry-evidence",
                "state_source": "entry_authority_policy",
                "blocking": True,
            },
        ),
    )

    artifact = result["artifact"]
    gates = [entry["gate"] for entry in artifact["gate_trace"]]
    assert gates.index("entry_authority") < gates.index("pre_submit_risk")
    assert artifact["primary_block_gate"] == "entry_authority"
    assert artifact["primary_block_reason"] == "target_delta_entry_without_strategy_buy_authority"
