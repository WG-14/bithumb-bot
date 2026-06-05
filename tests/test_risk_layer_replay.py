from __future__ import annotations

import json
import sqlite3

from bithumb_bot.execution_service import ExecutionSubmitPlan
from bithumb_bot.portfolio_target import PortfolioTarget
from bithumb_bot.risk_contract import RiskPolicy, RiskSnapshot, SubmitPlan
from bithumb_bot.risk_layer_replay import verify_risk_layer_replay, verify_risk_layer_replay_db
from bithumb_bot.risk_policy_engine import RiskPolicyEngine


def _policy() -> RiskPolicy:
    return RiskPolicy(
        max_daily_loss_krw=10_000.0,
        max_daily_order_count=5,
        source="unit",
    )


def _snapshot() -> RiskSnapshot:
    return RiskSnapshot(
        evaluation_ts_ms=1_800_000_000_000,
        mark_price=100.0,
        current_equity=1_000_000.0,
        baseline_equity=1_000_000.0,
        loss_today=0.0,
        daily_order_count=0,
        state_source="runtime_db_ledger",
        evidence={"strategy_instance_id": "sma:unit", "pair": "KRW-BTC"},
    )


def _cycle_payload() -> tuple[dict[str, object], dict[str, object]]:
    policy = _policy()
    strategy_decision = RiskPolicyEngine(policy).evaluate_pre_decision(_snapshot()).as_dict()
    target = PortfolioTarget(
        pair="KRW-BTC",
        target_exposure_krw=100_000.0,
        target_qty=0.001,
        allocator_policy_name="unit_allocator",
        allocator_policy_version="1",
        allocator_config_hash="sha256:" + "a" * 64,
        strategy_contribution_hash="sha256:" + "b" * 64,
        allocation_input_hash="sha256:" + "c" * 64,
        reason="unit",
        conflict_resolution={"policy": "unit"},
    ).as_dict()
    plan = ExecutionSubmitPlan(
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
        extra_payload={"pre_submit_risk_required": True},
    )
    submit_payload = plan.as_final_payload()
    plan_hash = str(submit_payload["submit_plan_hash"])
    pre_submit_decision = RiskPolicyEngine(policy).evaluate_pre_submit(
        SubmitPlan(
            side="BUY",
            qty=0.001,
            notional_krw=100_000.0,
            source="target_delta",
            evidence={"execution_submit_plan_hash": plan_hash},
        ),
        _snapshot(),
    )
    submit_payload.update(
        {
            "pre_submit_risk_decision": pre_submit_decision.as_dict(),
            "pre_submit_risk_status": pre_submit_decision.status,
            "pre_submit_risk_decision_hash": pre_submit_decision.risk_decision_hash,
            "pre_submit_risk_policy_hash": pre_submit_decision.risk_policy_hash,
            "pre_submit_risk_input_hash": pre_submit_decision.risk_input_hash,
            "pre_submit_risk_evidence_hash": pre_submit_decision.risk_evidence_hash,
            "pre_submit_risk_plan_hash": plan_hash,
            "pre_submit_risk_reason_code": pre_submit_decision.reason_code,
            "pre_submit_risk_state_source": pre_submit_decision.state_source,
        }
    )
    context = {
        "strategy_risk_decision": strategy_decision,
        "portfolio_target": target,
        "target_submit_plan": submit_payload,
    }
    return context, submit_payload


def _write_cycle_db(path, context: dict[str, object]) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE strategy_decisions (
                id INTEGER PRIMARY KEY,
                decision_ts INTEGER NOT NULL,
                context_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO strategy_decisions(id, decision_ts, context_json) VALUES (?, ?, ?)",
            (1, 1_800_000_000_000, json.dumps(context, sort_keys=True)),
        )
        conn.commit()
    finally:
        conn.close()


def test_risk_layer_replay_reports_pass_for_matching_cycle(tmp_path) -> None:
    context, _submit_payload = _cycle_payload()
    db_path = tmp_path / "risk-replay.sqlite"
    _write_cycle_db(db_path, context)

    report = verify_risk_layer_replay_db(db_path, decision_id=1)

    assert report["read_only"] is True
    assert report["overall_status"] == "pass"
    assert report["strategy_risk_replay_status"] == "pass"
    assert report["portfolio_risk_replay_status"] == "pass"
    assert report["pre_submit_risk_replay_status"] == "pass"
    for layer in ("strategy", "portfolio", "pre_submit"):
        payload = report["layers"][layer]
        assert payload["stored_payload_integrity_status"] in {"pass", "not_applicable"}
        assert payload["source_reconstruction_status"] in {"pass", "not_applicable"}
        assert payload["final_layer_status"] in {"pass", "not_applicable"}
        assert payload["expected_decision_hash"].startswith("sha256:")
        assert payload["actual_decision_hash"].startswith("sha256:")
        assert payload["policy_hash"].startswith("sha256:")
        assert payload["input_hash"].startswith("sha256:")
        assert payload["evidence_hash"].startswith("sha256:")
        assert payload["state_source"]
        assert payload["risk_status"] == "ALLOW"
        assert payload["reason_code"] == "OK"
    assert report["layers"]["portfolio"]["source_reconstruction_status"] == "pass"
    assert report["layers"]["pre_submit"]["source_reconstruction_status"] == "pass"
    assert report["layers"]["strategy"]["source_reconstruction_status"] == "not_applicable"
    assert report["layers"]["strategy"]["missing_source_material"]


def test_risk_layer_replay_fails_on_tampered_strategy_hash(tmp_path) -> None:
    context, _submit_payload = _cycle_payload()
    context["strategy_risk_decision"] = {
        **context["strategy_risk_decision"],  # type: ignore[arg-type]
        "risk_decision_hash": "sha256:" + "f" * 64,
    }
    db_path = tmp_path / "risk-replay-tampered.sqlite"
    _write_cycle_db(db_path, context)

    report = verify_risk_layer_replay_db(db_path, decision_id=1)

    assert report["overall_status"] == "fail"
    assert report["strategy_risk_replay_status"] == "fail"
    assert report["layers"]["strategy"]["stored_payload_integrity_status"] == "fail"
    assert report["layers"]["strategy"]["mismatch_reason"] == "decision_hash_mismatch"


def test_risk_layer_replay_reports_not_applicable_layers() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        report = verify_risk_layer_replay(conn)
    finally:
        conn.close()

    assert report["overall_status"] == "not_applicable"
    assert report["strategy_risk_replay_status"] == "not_applicable"
    assert report["portfolio_risk_replay_status"] == "not_applicable"
    assert report["pre_submit_risk_replay_status"] == "not_applicable"
    assert report["layers"]["strategy"]["reason"] == "strategy_risk_decision_not_recorded"
