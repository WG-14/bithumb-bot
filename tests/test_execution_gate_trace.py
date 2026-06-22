from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.runtime.cycle_artifact_assembler import RuntimeCycleArtifactAssembler
from bithumb_bot.runtime.execution_coordinator import ExecutionCycleResult
from bithumb_bot.runtime.lifecycle_artifacts import RuntimeCycleArtifact


def test_gate_trace_records_first_blocking_gate() -> None:
    artifact = RuntimeCycleArtifact(
        cycle_id="cycle-1",
        candle_ts=1_800_000_000_000,
        hard_gate_trace_entries=[
            {"gate": "time_window", "status": "ALLOW", "reason_code": "within_kst_window"}
        ],
        strategy_risk_status="ALLOW",
        strategy_risk_reason_code="OK",
        strategy_risk_input_hash="sha256:strategy-input",
        strategy_risk_evidence_hash="sha256:strategy-evidence",
        strategy_risk_state_source="runtime_db",
        portfolio_risk_status="BLOCK",
        portfolio_risk_reason_code="MAX_DAILY_ORDER_COUNT",
        portfolio_risk_input_hash="sha256:portfolio-input",
        portfolio_risk_evidence_hash="sha256:portfolio-evidence",
        portfolio_risk_state_source="runtime_db",
        pre_submit_risk_status="BLOCK",
        pre_submit_risk_reason_code="RISK_STATE_MISMATCH",
        pre_submit_risk_input_hash="sha256:pre-input",
        pre_submit_risk_evidence_hash="sha256:pre-evidence",
        pre_submit_risk_state_source="runtime_db_broker",
    ).as_dict()

    assert artifact["primary_block_gate"] == "portfolio_risk"
    assert artifact["primary_block_reason"] == "MAX_DAILY_ORDER_COUNT"
    assert [entry["gate"] for entry in artifact["gate_trace"]] == [
        "time_window",
        "strategy_risk",
        "portfolio_risk",
        "pre_submit_risk",
    ]


def test_pre_submit_risk_block_visible_in_runtime_artifact() -> None:
    artifact = RuntimeCycleArtifact(
        cycle_id="cycle-2",
        candle_ts=1_800_000_000_000,
        strategy_risk_status="ALLOW",
        strategy_risk_reason_code="OK",
        portfolio_risk_status="ALLOW",
        portfolio_risk_reason_code="OK",
        pre_submit_risk_status="BLOCK",
        pre_submit_risk_reason_code="RISK_STATE_MISMATCH",
        pre_submit_risk_input_hash="sha256:pre-input",
        pre_submit_risk_evidence_hash="sha256:pre-evidence",
        pre_submit_risk_state_source="runtime_db_broker",
    ).as_dict()

    pre_submit = artifact["gate_trace"][-1]
    assert pre_submit["gate"] == "pre_submit_risk"
    assert pre_submit["status"] == "BLOCK"
    assert pre_submit["reason_code"] == "RISK_STATE_MISMATCH"
    assert pre_submit["blocking"] is True
    assert artifact["primary_block_gate"] == "pre_submit_risk"
    assert artifact["primary_block_reason"] == "RISK_STATE_MISMATCH"


def test_non_risk_hard_gate_block_visible_in_runtime_artifact() -> None:
    artifact = RuntimeCycleArtifact(
        cycle_id="cycle-3",
        candle_ts=1_800_000_000_000,
        hard_gate_trace_entries=[
            {"gate": "time_window", "status": "ALLOW", "reason_code": "within_kst_window"},
            {
                "gate": "submit_authority",
                "status": "BLOCK",
                "reason_code": "target_delta_missing_target_submit_plan",
                "input_hash": "sha256:submit-authority-input",
                "evidence_hash": "sha256:submit-authority-evidence",
                "state_source": "submit_authority_policy",
            },
        ],
        strategy_risk_status="ALLOW",
        strategy_risk_reason_code="OK",
        portfolio_risk_status="ALLOW",
        portfolio_risk_reason_code="OK",
    ).as_dict()

    assert [entry["gate"] for entry in artifact["gate_trace"]] == [
        "time_window",
        "submit_authority",
        "strategy_risk",
        "portfolio_risk",
    ]
    submit_authority = artifact["gate_trace"][1]
    assert submit_authority["status"] == "BLOCK"
    assert submit_authority["blocking"] is True
    assert artifact["primary_block_gate"] == "submit_authority"
    assert artifact["primary_block_reason"] == "target_delta_missing_target_submit_plan"


def test_execution_result_pre_submit_trace_overrides_missing_decision_trace() -> None:
    decision_result = SimpleNamespace(
        candle_ts=1_800_000_000_000,
        strategy_decision_hash="sha256:strategy",
        runtime_strategy_decision_bundle_id=None,
        runtime_strategy_decision_bundle_hash=None,
        portfolio_allocation_decision_id=None,
        portfolio_allocation_decision_hash=None,
        portfolio_target_id=None,
        portfolio_target_hash=None,
        strategy_contribution_hash=None,
        execution_plan_id=None,
        execution_plan_bundle_hash=None,
        execution_submit_plan_hash=None,
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
    execution_result = ExecutionCycleResult(
        candle_ts=1_800_000_000_000,
        decision_id=1,
        planning_status="submit_blocked",
        submit_expected=True,
        submitted=False,
        post_trade_reconciled=False,
        mark_processed_allowed=True,
        pre_submit_risk_decision_hash="sha256:decision",
        pre_submit_risk_policy_hash="sha256:policy",
        pre_submit_risk_input_hash="sha256:input",
        pre_submit_risk_evidence_hash="sha256:evidence",
        pre_submit_risk_plan_hash="sha256:plan",
        pre_submit_risk_state_source="runtime_db_broker",
        pre_submit_risk_status="BLOCK",
        pre_submit_risk_reason_code="RISK_STATE_MISMATCH",
    )

    artifact = RuntimeCycleArtifactAssembler().from_cycle_results(
        cycle_id="checkpoint:processed",
        startup_state="READY",
        decision_result=decision_result,
        execution_result=execution_result,
    ).as_dict()

    pre_submit = artifact["gate_trace"][-1]
    assert pre_submit["gate"] == "pre_submit_risk"
    assert pre_submit["status"] == "BLOCK"
    assert pre_submit["reason_code"] == "RISK_STATE_MISMATCH"
    assert pre_submit["blocking"] is True
    assert artifact["primary_block_gate"] == "pre_submit_risk"


def test_entry_authority_block_is_primary_block_gate() -> None:
    artifact = RuntimeCycleArtifact(
        cycle_id="cycle-entry",
        candle_ts=1_800_000_000_000,
        hard_gate_trace_entries=[
            {
                "gate": "entry_authority",
                "status": "BLOCK",
                "reason_code": "target_delta_entry_without_strategy_buy_authority",
                "input_hash": "sha256:entry-input",
                "evidence_hash": "sha256:entry-evidence",
                "state_source": "entry_authority_policy",
                "blocking": True,
            }
        ],
        strategy_risk_status="ALLOW",
        strategy_risk_reason_code="OK",
        portfolio_risk_status="ALLOW",
        portfolio_risk_reason_code="OK",
    ).as_dict()

    assert artifact["primary_block_gate"] == "entry_authority"
    assert artifact["primary_block_reason"] == "target_delta_entry_without_strategy_buy_authority"


def test_entry_authority_allow_recorded_for_kst_10_buy() -> None:
    artifact = RuntimeCycleArtifact(
        cycle_id="cycle-entry-allow",
        candle_ts=1_800_000_000_000,
        hard_gate_trace_entries=[
            {
                "gate": "entry_authority",
                "status": "ALLOW",
                "reason_code": "daily_participation_entry",
                "input_hash": "sha256:entry-input",
                "evidence_hash": "sha256:entry-evidence",
                "state_source": "entry_authority_policy",
                "blocking": False,
            }
        ],
        strategy_risk_status="ALLOW",
        strategy_risk_reason_code="OK",
        portfolio_risk_status="ALLOW",
        portfolio_risk_reason_code="OK",
    ).as_dict()

    entry = [item for item in artifact["gate_trace"] if item["gate"] == "entry_authority"][0]
    assert entry["status"] == "ALLOW"
    assert artifact["primary_block_gate"] == "none"
