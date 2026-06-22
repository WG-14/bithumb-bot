from __future__ import annotations

import json

import pytest

from bithumb_bot import h74_live_rehearsal
from bithumb_bot.h74_live_rehearsal import (
    H74LiveRehearsalConfig,
    H74LiveRehearsalError,
    run_h74_live_rehearsal,
)


def _source_artifact(tmp_path, *, fee_rate: float = 0.0004) -> str:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "runtime_base_cost_assumption": {
                    "fee_rate": fee_rate,
                    "fee_source": "research_realistic_bithumb_app_fee",
                    "slippage_bps": 10,
                    "slippage_source": "research_assumption",
                },
                "candle_timing": "closed_candle_kst",
            }
        ),
        encoding="utf-8",
    )
    return str(source)


def test_h74_rehearsal_reaches_broker_submit_boundary_at_kst_10(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="10:00", no_submit=True, source_artifact_path=_source_artifact(tmp_path))
    )

    assert payload["strategy_name"] == "daily_participation_sma"
    assert payload["daily_participation_reason_code"] == "daily_participation_fallback_allowed"
    assert payload["pre_submit_risk_status"] == "ALLOW"
    assert payload["submit_authority_reason"] == "allowed_target_delta"
    assert payload["broker_submit_reached"] is True
    assert payload["actual_submit"] is False
    assert payload["LIVE_DRY_RUN"] is False


def test_h74_rehearsal_uses_runtime_cycle_pipeline(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["runtime_cycle_pipeline_called"] is True
    assert payload["execution_result_status"] == "submitted"
    assert payload["live_signal_execution_service_called"] is True


def test_h74_rehearsal_uses_production_runtime_strategy_set(tmp_path, monkeypatch) -> None:
    calls = {"count": 0}
    original = h74_live_rehearsal.runtime_strategy_set_manifest_hash

    def _wrapped(strategy_set):
        calls["count"] += 1
        return original(strategy_set)

    monkeypatch.setattr(h74_live_rehearsal, "runtime_strategy_set_manifest_hash", _wrapped)

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert calls["count"] >= 1
    assert payload["production_runtime_strategy_set_called"] is True
    assert str(payload["runtime_strategy_set_manifest_hash"]).startswith("sha256:")
    assert payload["broker_submit_reached"] is True


def test_h74_rehearsal_uses_production_allocator_portfolio_target(tmp_path, monkeypatch) -> None:
    from bithumb_bot import run_loop_execution_planner

    calls = {"count": 0}
    original = run_loop_execution_planner.PortfolioAllocator.allocate

    def _wrapped(self, allocation_input):
        calls["count"] += 1
        return original(self, allocation_input)

    monkeypatch.setattr(run_loop_execution_planner.PortfolioAllocator, "allocate", _wrapped)

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert calls["count"] == 1
    assert payload["production_allocator_portfolio_target_called"] is True
    assert str(payload["portfolio_target_hash"]).startswith("sha256:")
    assert payload["broker_submit_reached"] is True


def test_h74_rehearsal_invokes_live_signal_execution_service_before_mock_submit(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["live_signal_execution_service_called"] is True
    assert payload["target_delta_final_payload_created"] is True
    assert payload["pre_submit_proof_created"] is True
    assert payload["submit_authority_allowed"] is True
    assert payload["broker_submit_reached"] is True
    assert payload["would_submit_plan"]["pre_submit_risk_status"] == "ALLOW"
    assert payload["would_submit_plan"]["pre_submit_risk_decision_hash"].startswith("sha256:")
    assert payload["would_submit_plan"]["pre_submit_risk_evidence_hash"].startswith("sha256:")


def test_h74_rehearsal_uses_production_target_delta_planner(tmp_path, monkeypatch) -> None:
    from bithumb_bot import execution_service

    calls = {"count": 0}
    original = execution_service.build_target_delta_execution_sizing

    def _wrapped(*args, **kwargs):
        calls["count"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(execution_service, "build_target_delta_execution_sizing", _wrapped)

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert calls["count"] == 1
    assert payload["would_submit_plan"]["source"] == "target_delta"
    assert payload["would_submit_plan"]["authority"] == "canonical_target_delta_sizing"
    assert payload["broker_submit_reached"] is True


def test_h74_rehearsal_invokes_pre_submit_and_submit_authority_policies(tmp_path, monkeypatch) -> None:
    from bithumb_bot import execution_service
    from bithumb_bot.pre_submit_risk_coordinator import PreSubmitRiskCoordinator

    calls = {"pre_submit": 0, "submit_authority": 0}
    original_pre_submit = PreSubmitRiskCoordinator.evaluate_and_persist
    original_submit_authority = execution_service.evaluate_submit_authority_policy

    def _wrapped_pre_submit(self, *args, **kwargs):
        calls["pre_submit"] += 1
        return original_pre_submit(self, *args, **kwargs)

    def _wrapped_submit_authority(*args, **kwargs):
        calls["submit_authority"] += 1
        return original_submit_authority(*args, **kwargs)

    monkeypatch.setattr(PreSubmitRiskCoordinator, "evaluate_and_persist", _wrapped_pre_submit)
    monkeypatch.setattr(execution_service, "evaluate_submit_authority_policy", _wrapped_submit_authority)

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert calls["pre_submit"] >= 1
    assert calls["submit_authority"] >= 1
    assert payload["pre_submit_risk_status"] == "ALLOW"
    assert payload["submit_authority_allowed"] is True
    assert payload["broker_submit_reached"] is True


def test_h74_rehearsal_fails_if_target_plan_is_manual_fixture(tmp_path) -> None:
    assert not hasattr(h74_live_rehearsal, "_target_delta_submit_plan")

    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["would_submit_plan"]["source"] == "target_delta"
    assert payload["would_submit_plan"].get("authority_source") == "target_delta"
    assert payload["would_submit_plan"].get("portfolio_target_authoritative") is True


def test_h74_rehearsal_fails_if_daily_participation_plugin_not_called(tmp_path, monkeypatch) -> None:
    def _blocked(*_args, **_kwargs):
        raise AssertionError("daily_participation_sma plugin not called through rehearsal")

    monkeypatch.setattr(
        "bithumb_bot.strategy_plugins.daily_participation_sma.evaluate_daily_participation_policy",
        _blocked,
    )

    with pytest.raises(AssertionError, match="plugin not called"):
        run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))


def test_h74_rehearsal_does_not_use_operator_smoke_authority(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["operator_live_pipeline_smoke"] is False
    assert "operator_live_pipeline_smoke" not in payload["would_submit_plan"]
    with pytest.raises(H74LiveRehearsalError, match="rejects_operator_smoke_authority"):
        run_h74_live_rehearsal(H74LiveRehearsalConfig(smoke_authority_hash="sha256:smoke"))


def test_h74_rehearsal_does_not_accept_smoke_proof_as_pre_submit_proof() -> None:
    with pytest.raises(H74LiveRehearsalError, match="rejects_operator_smoke_authority"):
        run_h74_live_rehearsal(H74LiveRehearsalConfig(smoke_authority_hash="sha256:smoke"))


def test_h74_rehearsal_does_not_use_live_dry_run_or_paper_mode_for_success_path(tmp_path) -> None:
    payload = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path)))

    assert payload["MODE"] == "live"
    assert payload["LIVE_DRY_RUN"] is False
    assert payload["LIVE_REAL_ORDER_ARMED"] is True
    assert payload["decision_path_MODE"] == "live"
    assert payload["decision_path_LIVE_DRY_RUN"] is False
    assert payload["decision_path_LIVE_REAL_ORDER_ARMED"] is True
    assert payload["planning_path_MODE"] == "live"
    assert payload["planning_path_LIVE_DRY_RUN"] is False
    assert payload["planning_path_LIVE_REAL_ORDER_ARMED"] is True
    assert payload["broker_submit_reached"] is True
    assert payload["actual_submit"] is False
    assert payload["operator_live_pipeline_smoke"] is False
    assert "operator_live_pipeline_smoke" not in payload["would_submit_plan"]


def test_h74_rehearsal_fails_when_pre_submit_broker_snapshot_missing(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            broker_snapshot_available=False,
            source_artifact_path=_source_artifact(tmp_path),
        )
    )

    assert payload["pre_submit_risk_status"] == "REQUIRE_RECONCILE"
    assert payload["pre_submit_risk_reason_code"] == "RISK_STATE_MISMATCH"
    assert payload["broker_submit_reached"] is False


def test_rehearsal_reports_fee_equivalence_gate(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            source_artifact_path=_source_artifact(tmp_path),
            current_fee_rate=0.0025,
            fee_authority_source="chance_doc",
        )
    )

    assert payload["experiment_equivalence_status"] == "mismatch"
    assert payload["fee_authority_source"] == "chance_doc"
    gate = [entry for entry in payload["gate_trace"] if entry["gate"] == "fee_equivalence"][0]
    assert gate["status"] == "BLOCK"
    assert gate["reason_code"] == "mismatch"


def test_rehearsal_does_not_reach_submit_boundary_when_equivalence_blocks(tmp_path) -> None:
    payload = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            source_artifact_path=_source_artifact(tmp_path),
            current_fee_rate=0.0025,
        )
    )

    assert payload["experiment_equivalence_status"] == "mismatch"
    assert payload["broker_submit_reached"] is False
    assert payload["would_submit"] is False
    assert payload["primary_block_gate"] == "fee_equivalence"
