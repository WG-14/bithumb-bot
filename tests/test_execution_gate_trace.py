from __future__ import annotations

from bithumb_bot.runtime.lifecycle_artifacts import RuntimeCycleArtifact


def test_gate_trace_records_first_blocking_gate() -> None:
    artifact = RuntimeCycleArtifact(
        cycle_id="cycle-1",
        candle_ts=1_800_000_000_000,
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
