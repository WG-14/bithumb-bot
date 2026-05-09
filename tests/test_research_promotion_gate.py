from __future__ import annotations

import json
from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot import app as app_module
from bithumb_bot.research import cli as research_cli
from bithumb_bot.research.hashing import content_hash_payload, sha256_prefixed
from bithumb_bot.research.lineage import build_research_lineage, compute_lineage_hash, reproduce_promotion
from bithumb_bot.research.promotion_gate import PromotionGateError, build_candidate_profile, promote_candidate
from bithumb_bot.storage_io import write_json_atomic


def _manager(tmp_path: Path, monkeypatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def _candidate(**overrides):
    payload = {
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "dataset_quality_gate_status": "PASS",
        "dataset_quality_gate_reasons": [],
        "dataset_quality_report_hashes": {"train": "sha256:quality-train", "validation": "sha256:quality-validation"},
        "execution_timing_policy": {
            "signal_basis": "closed_candle",
            "decision_time": "candle_close",
            "decision_guard_ms": 0,
            "fill_reference_policy": "next_candle_open",
            "quote_selection": "first_after_or_equal",
            "max_quote_wait_ms": 3000,
            "missing_quote_policy": "warn",
            "allow_same_candle_close_fill": False,
            "source": "test",
        },
        "execution_reality_summary": {
            "signal_event_count": 4,
            "fillable_signal_event_count": 4,
            "missing_quote_on_signal_count": 0,
            "quote_after_decision_coverage_pct": None,
            "median_quote_age_ms_on_signal": None,
            "p95_quote_age_ms_on_signal": None,
            "execution_reference_policy": "next_candle_open",
            "execution_reality_level": "candle_next_open",
            "execution_attempt_count": 8,
            "execution_filled_count": 8,
            "filled_execution_count": 8,
            "portfolio_applied_trade_count": 8,
            "pending_execution_count": 0,
            "skipped_execution_count": 0,
            "failed_execution_count": 0,
            "closed_trade_count": 4,
            "pending_execution_at_end_count": 0,
            "pending_execution_after_dataset_end_count": 0,
            "execution_event_timeline_incomplete": False,
            "execution_reality_gate_status": "PASS",
            "execution_reality_gate_reasons": [],
        },
        "execution_event_summary": {
            "execution_attempt_count": 8,
            "execution_filled_count": 8,
            "filled_execution_count": 8,
            "portfolio_applied_trade_count": 8,
            "pending_execution_count": 0,
            "skipped_execution_count": 0,
            "failed_execution_count": 0,
            "closed_trade_count": 4,
            "pending_execution_at_end_count": 0,
            "pending_execution_after_dataset_end_count": 0,
            "execution_event_timeline_incomplete": False,
        },
        "strategy_name": "sma_with_filter",
        "parameter_candidate_id": "candidate_001",
        "parameter_values": {"SMA_SHORT": 2, "SMA_LONG": 4},
        "cost_model": {"fee_rate": 0.0, "slippage_bps": 0.0},
        "validation_metrics": {
            "trade_count": 4,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        "final_holdout_metrics": {
            "trade_count": 4,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        "final_holdout_present": True,
        "final_holdout_required_for_promotion": True,
        "acceptance_gate_result": "PASS",
        "scenario_policy": "single_scenario",
        "scenario_pass_count": 1,
        "scenario_fail_count": 0,
        "required_scenario_count": 1,
        "scenario_results": [
            {
                "scenario_id": "scenario_001_fixed_bps_unit",
                "scenario_index": 0,
                "scenario_type": "fixed_bps",
                "scenario_role": "base",
                "scenario_acceptance_gate_result": "PASS",
                "scenario_fail_reasons": [],
                "execution_model_hash": "sha256:model",
                "execution_model": {"type": "fixed_bps", "model_params_hash": "sha256:model"},
                "cost_model": {"fee_rate": 0.0, "slippage_bps": 0.0},
                "validation_metrics": {
                    "trade_count": 4,
                    "max_drawdown_pct": 1.0,
                    "profit_factor": 2.0,
                    "return_pct": 1.0,
                },
                "final_holdout_metrics": {
                    "trade_count": 4,
                    "max_drawdown_pct": 1.0,
                    "profit_factor": 2.0,
                    "return_pct": 1.0,
                },
            }
        ],
        "regime_classifier_version": "market_regime_v2",
        "allowed_live_regimes": ["uptrend_normal_vol_volume_increasing"],
        "blocked_live_regimes": ["sideways_low_vol_volume_decreasing"],
        "regime_evidence": {
            "uptrend_normal_vol_volume_increasing": {
                "trade_count": 12,
                "profit_factor": 1.4,
                "expectancy": 100.0,
            }
        },
        "regime_gate_result": {
            "result": "PASS",
            "passed": True,
            "reasons": [],
        },
        "walk_forward_required": False,
    }
    payload.update(overrides)
    explicit_hash = payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = explicit_hash or sha256_prefixed(build_candidate_profile(payload))
    return payload


def _production_candidate(**overrides):
    payload = _candidate(
        deployment_tier="paper_candidate",
        execution_model_source="execution_model",
        execution_model={
            "type": "fixed_bps",
            "fee_rate": 0.0,
            "slippage_bps": 5.0,
            "latency_ms": 0,
            "partial_fill_rate": 0.0,
            "order_failure_rate": 0.0,
            "market_order_extra_cost_bps": 0.0,
            "model_params_hash": "sha256:model",
        },
        execution_calibration_required=True,
        execution_calibration_strictness="fail",
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
            "artifact_hashes": ["sha256:calibration"],
            "scenario_gates": [
                {
                    "status": "PASS",
                    "reasons": [],
                    "artifact_hash": "sha256:calibration",
                    "content_hash_present": True,
                    "market": "KRW-BTC",
                    "interval": "1m",
                    "expected_market": "KRW-BTC",
                    "expected_interval": "1m",
                    "expected_fill_reference_policy": "next_candle_open",
                    "artifact_fill_reference_policy": "next_candle_open",
                    "artifact_execution_reality_level": "candle_next_open",
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "quality_gate_status": "PASS",
                }
            ],
        },
        execution_calibration_artifact_hash="sha256:calibration",
        execution_calibration_artifact_hashes=["sha256:calibration"],
        execution_calibration_policy_source="repo_production_calibration_policy_v1",
        production_calibration_policy_result={
            "target": "paper_candidate",
            "production_bound": True,
            "required": True,
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
            "artifact_hashes": ["sha256:calibration"],
            "policy_source": "repo_production_calibration_policy_v1",
            "operator_next_step": "none",
        },
        production_calibration_policy_reasons=[],
    )
    payload.update(overrides)
    explicit_hash = payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = explicit_hash or sha256_prefixed(build_candidate_profile(payload))
    return payload


def _lineage(*, execution_calibration_artifact_hash: str | None = None) -> dict[str, object]:
    return build_research_lineage(
        experiment_id="promo_exp",
        experiment_family_id="family_001",
        hypothesis_id="hypothesis_001",
        hypothesis_status="pre_registered",
        manifest_hash="sha256:manifest",
        dataset_snapshot_id="snap",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        repository_version="test",
        command_name="research-backtest",
        command_args={"manifest": "/external/manifest.json"},
        execution_calibration_artifact_hash=execution_calibration_artifact_hash,
        search_budget=4,
        parameter_grid_size=4,
        attempt_index=2,
        failed_candidate_count=1,
        holdout_reuse_count=3,
        dataset_reuse_policy="visible_reuse_not_hard_blocked",
        created_at="2026-05-04T00:00:00+00:00",
    )


def _write_report(manager: PathManager, candidate: dict[str, object]) -> None:
    _write_report_with_lineage(manager, candidate)


def _write_report_without_lineage(manager: PathManager, candidate: dict[str, object]) -> None:
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = {
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "dataset_quality_gate_status": "PASS",
        "dataset_quality_gate_reasons": [],
        "dataset_quality_reports": {
            "train": {
                "artifact_type": "dataset_quality_report",
                "content_hash": "sha256:quality-train",
                "quality_gate_status": "PASS",
                "quality_gate_reasons": [],
            },
            "validation": {
                "artifact_type": "dataset_quality_report",
                "content_hash": "sha256:quality-validation",
                "quality_gate_status": "PASS",
                "quality_gate_reasons": [],
            },
        },
        "repository_version": "test",
        "candidates": [candidate],
    }
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    write_json_atomic(path, payload)


def _write_report_with_lineage(
    manager: PathManager,
    candidate: dict[str, object],
    *,
    lineage_calibration_hash: str | None | object = ...,
) -> None:
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    if lineage_calibration_hash is ...:
        lineage_calibration_hash = (
            str(candidate.get("execution_calibration_artifact_hash"))
            if str(candidate.get("execution_calibration_artifact_hash") or "").startswith("sha256:")
            else None
        )
    lineage = _lineage(
        execution_calibration_artifact_hash=(
            str(lineage_calibration_hash)
            if str(lineage_calibration_hash or "").startswith("sha256:")
            else None
        )
    )
    payload = {
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "dataset_quality_gate_status": "PASS",
        "dataset_quality_gate_reasons": [],
        "dataset_quality_reports": {
            "train": {
                "artifact_type": "dataset_quality_report",
                "content_hash": "sha256:quality-train",
                "quality_gate_status": "PASS",
                "quality_gate_reasons": [],
            },
            "validation": {
                "artifact_type": "dataset_quality_report",
                "content_hash": "sha256:quality-validation",
                "quality_gate_status": "PASS",
                "quality_gate_reasons": [],
            },
        },
        "repository_version": "test",
        "experiment_family_id": "family_001",
        "hypothesis_id": "hypothesis_001",
        "hypothesis_status": "pre_registered",
        "search_budget": 4,
        "parameter_grid_size": 4,
        "attempt_index": 2,
        "failed_candidate_count": 1,
        "holdout_reuse_count": 3,
        "dataset_reuse_policy": "visible_reuse_not_hard_blocked",
        "lineage": lineage,
        "lineage_hash": lineage["lineage_hash"],
        "candidates": [candidate],
    }
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    write_json_atomic(path, payload)


def _walk_forward_candidate(backtest_candidate: dict[str, object], **overrides) -> dict[str, object]:
    payload = dict(backtest_candidate)
    payload.update(
        {
            "walk_forward_metrics": {
                "window_count": 3,
                "pass_window_count": 3,
                "fail_window_count": 0,
                "mean_test_return_pct": 1.0,
                "median_test_return_pct": 1.0,
                "worst_test_return_pct": 0.5,
                "return_consistency_pass": True,
            },
            "walk_forward_gate_result": "PASS",
        }
    )
    payload.pop("candidate_profile_hash", None)
    payload.update(overrides)
    explicit_hash = payload.pop("candidate_profile_hash", None)
    payload["candidate_profile_hash"] = explicit_hash or sha256_prefixed(build_candidate_profile(payload))
    return payload


def _write_walk_forward_report(manager: PathManager, candidate: dict[str, object]) -> None:
    path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    payload = {
        "experiment_id": "promo_exp",
        "manifest_hash": "sha256:manifest",
        "candidates": [candidate],
    }
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    write_json_atomic(path, payload)


def _canonical_report_hash(payload: dict[str, object]) -> str:
    return sha256_prefixed(content_hash_payload({key: value for key, value in payload.items() if key != "content_hash"}))


def test_promotion_refuses_candidate_without_validation_evidence(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(validation_metrics=None)
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="validation_oos_evidence_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


@pytest.mark.parametrize(
    "candidate",
    [
        _candidate(acceptance_gate_result="FAIL", gate_fail_reasons=["min_trade_count_failed"]),
        _candidate(acceptance_gate_result="FAIL", gate_fail_reasons=["max_drawdown_failed"]),
        _candidate(acceptance_gate_result="FAIL", gate_fail_reasons=["profit_factor_failed"]),
    ],
)
def test_promotion_refuses_failed_gate_candidates(tmp_path, monkeypatch, candidate) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="acceptance_gate_not_passed"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_does_not_mutate_env_file(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    env_file = tmp_path / "live.env"
    env_file.write_text("SMA_SHORT=99\n", encoding="utf-8")
    before = env_file.read_text(encoding="utf-8")
    _write_report(manager, _candidate())

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact_path.exists()
    assert env_file.read_text(encoding="utf-8") == before
    assert result.artifact["operator_next_step"].startswith("Review this artifact")


def test_promotion_refuses_candidate_profile_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(candidate_profile_hash="sha256:tampered")
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="candidate_profile_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert not (manager.data_dir() / "reports" / "research" / "promo_exp" / "promotion_candidate_001.json").exists()


def test_promotion_refuses_backtest_candidate_hash_mismatch_even_when_walk_forward_exists(
    tmp_path, monkeypatch
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    walk_forward_candidate = _walk_forward_candidate(backtest_candidate)
    backtest_candidate["candidate_profile_hash"] = "sha256:tampered"
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, walk_forward_candidate)

    with pytest.raises(PromotionGateError, match="backtest_candidate_profile_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert not (manager.data_dir() / "reports" / "research" / "promo_exp" / "promotion_candidate_001.json").exists()


def test_promotion_refuses_backtest_gate_failure_even_when_walk_forward_passes(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(
        acceptance_gate_result="FAIL",
        gate_fail_reasons=["min_trade_count_failed"],
        walk_forward_required=True,
    )
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(backtest_candidate))

    with pytest.raises(PromotionGateError, match="backtest_acceptance_gate_not_passed"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_walk_forward_candidate_profile_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(
        manager,
        _walk_forward_candidate(backtest_candidate, candidate_profile_hash="sha256:tampered"),
    )

    with pytest.raises(PromotionGateError, match="walk_forward_candidate_profile_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_uses_verified_candidate_profile_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    expected_hash = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["candidate_profile_hash"] == expected_hash
    assert result.artifact["verified_candidate_profile_hash"] == expected_hash
    assert result.artifact["strategy_profile_hash"] == expected_hash


def test_promotion_artifact_exposes_execution_event_summary_top_level(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["execution_event_summary"] == candidate["execution_event_summary"]
    assert result.artifact["train_execution_event_summary"] == candidate.get("train_execution_event_summary")
    assert result.artifact["validation_execution_event_summary"] == candidate.get("validation_execution_event_summary")
    assert result.artifact["final_holdout_execution_event_summary"] == candidate.get("final_holdout_execution_event_summary")
    assert result.artifact["pending_execution_after_dataset_end_count"] == 0
    assert result.artifact["execution_event_timeline_incomplete"] is False
    assert result.artifact["portfolio_applied_trade_count"] == 8
    assert result.artifact["execution_filled_count"] == 8
    assert result.artifact["closed_trade_count"] == 4


def test_promotion_artifact_execution_event_summary_matches_candidate_profile(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    profile = result.artifact["candidate_profile"]
    assert result.artifact["execution_event_summary"] == profile["execution_event_summary"]
    assert result.artifact["train_execution_event_summary"] == profile["train_execution_event_summary"]
    assert result.artifact["validation_execution_event_summary"] == profile["validation_execution_event_summary"]
    assert (
        result.artifact["final_holdout_execution_event_summary"]
        == profile["final_holdout_execution_event_summary"]
    )


def test_promotion_artifact_hash_changes_when_execution_event_summary_changes(tmp_path, monkeypatch) -> None:
    clean_manager = _manager(tmp_path / "clean", monkeypatch)
    clean_candidate = _candidate()
    _write_report(clean_manager, clean_candidate)
    clean = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=clean_manager,
        generated_at="2026-05-07T00:00:00+00:00",
    )

    changed_manager = _manager(tmp_path / "changed", monkeypatch)
    changed_summary = dict(clean_candidate["execution_event_summary"])
    changed_summary["execution_attempt_count"] = 9
    changed_summary["skipped_execution_count"] = 1
    changed_candidate = _candidate(execution_event_summary=changed_summary)
    _write_report(changed_manager, changed_candidate)
    changed = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=changed_manager,
        generated_at="2026-05-07T00:00:00+00:00",
    )

    assert clean.content_hash != changed.content_hash
    assert clean.artifact["execution_event_summary"] != changed.artifact["execution_event_summary"]


def test_promotion_artifact_records_backtest_and_walk_forward_evidence_hashes(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    walk_forward_candidate = _walk_forward_candidate(backtest_candidate)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, walk_forward_candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["validation_evidence_source"] == "backtest_report.json"
    assert result.artifact["backtest_candidate_profile_hash"] == backtest_candidate["candidate_profile_hash"]
    assert result.artifact["backtest_candidate_profile_verified"] is True
    assert result.artifact["walk_forward_required"] is True
    assert result.artifact["walk_forward_evidence_source"] == "walk_forward_report.json"
    assert result.artifact["walk_forward_candidate_profile_hash"] == walk_forward_candidate["candidate_profile_hash"]
    assert result.artifact["walk_forward_candidate_profile_verified"] is True


def test_promotion_artifact_uses_verified_report_hashes_in_artifact_and_lineage(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    walk_forward_candidate = _walk_forward_candidate(backtest_candidate)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, walk_forward_candidate)

    backtest_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    walk_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    expected_backtest_hash = _canonical_report_hash(json.loads(backtest_path.read_text(encoding="utf-8")))
    expected_walk_hash = _canonical_report_hash(json.loads(walk_path.read_text(encoding="utf-8")))

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["backtest_report_hash"] == expected_backtest_hash
    assert result.artifact["walk_forward_report_hash"] == expected_walk_hash
    assert result.artifact["lineage"]["backtest_report_hash"] == expected_backtest_hash
    assert result.artifact["lineage"]["walk_forward_report_hash"] == expected_walk_hash


def test_promotion_refuses_backtest_body_tamper_with_stale_embedded_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report(manager, _candidate())
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["dataset_content_hash"] = "sha256:tampered_dataset"
    write_json_atomic(report_path, payload)

    with pytest.raises(PromotionGateError, match="backtest_report_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_backtest_missing_content_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report(manager, _candidate())
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload.pop("content_hash", None)
    write_json_atomic(report_path, payload)

    with pytest.raises(PromotionGateError, match="backtest_report_content_hash_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_walk_forward_body_tamper_with_stale_embedded_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(backtest_candidate))
    walk_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    payload = json.loads(walk_path.read_text(encoding="utf-8"))
    payload["manifest_hash"] = "sha256:tampered_manifest"
    write_json_atomic(walk_path, payload)

    with pytest.raises(PromotionGateError, match="walk_forward_report_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_walk_forward_missing_content_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=True)
    _write_report(manager, backtest_candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(backtest_candidate))
    walk_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    payload = json.loads(walk_path.read_text(encoding="utf-8"))
    payload.pop("content_hash", None)
    write_json_atomic(walk_path, payload)

    with pytest.raises(PromotionGateError, match="walk_forward_report_content_hash_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_artifact_records_no_walk_forward_evidence_when_not_required(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    backtest_candidate = _candidate(walk_forward_required=False)
    _write_report(manager, backtest_candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["validation_evidence_source"] == "backtest_report.json"
    assert result.artifact["backtest_candidate_profile_hash"] == backtest_candidate["candidate_profile_hash"]
    assert result.artifact["backtest_candidate_profile_verified"] is True
    assert result.artifact["walk_forward_required"] is False
    assert result.artifact["walk_forward_evidence_source"] is None
    assert result.artifact["walk_forward_candidate_profile_hash"] is None
    assert result.artifact["walk_forward_candidate_profile_verified"] is False
    assert result.artifact["regime_classifier_version"] == "market_regime_v2"
    assert result.artifact["allowed_regimes"] == ["uptrend_normal_vol_volume_increasing"]
    assert result.artifact["blocked_regimes"] == ["sideways_low_vol_volume_decreasing"]
    assert result.artifact["live_regime_policy"]["missing_policy_behavior"] == "fail_closed"


def test_lineage_backed_promotion_records_reproducibility_fields(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    summary = reproduce_promotion(result.artifact_path).summary

    assert result.artifact["lineage_required"] is True
    assert result.artifact["legacy_compatibility_used"] is False
    assert result.artifact["lineage_hash"].startswith("sha256:")
    assert result.artifact["experiment_family_id"] == "family_001"
    assert result.artifact["hypothesis_id"] == "hypothesis_001"
    assert result.artifact["search_budget"] == 4
    assert result.artifact["parameter_grid_size"] == 4
    assert result.artifact["attempt_index"] == 2
    assert result.artifact["failed_candidate_count"] == 1
    assert result.artifact["holdout_reuse_count"] == 3
    assert result.artifact["dataset_reuse_policy"] == "visible_reuse_not_hard_blocked"
    assert summary["ok"] is True
    assert summary["lineage_hash"] == result.artifact["lineage_hash"]
    assert summary["candidate_profile_hash"] == result.artifact["candidate_profile_hash"]


def test_promotion_refuses_missing_lineage_by_default(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(manager, _candidate())

    with pytest.raises(PromotionGateError, match="promotion refused: lineage_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_invalid_lineage_hash(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    _write_report_with_lineage(manager, candidate)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["lineage"]["lineage_hash"] = "sha256:tampered"
    payload.pop("content_hash", None)
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    write_json_atomic(report_path, payload)

    with pytest.raises(PromotionGateError, match="promotion refused: lineage_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_allows_missing_lineage_only_with_explicit_compatibility(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(manager, _candidate())

    result = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=manager,
        allow_legacy_lineage=True,
    )

    assert result.artifact["lineage_required"] is False
    assert result.artifact["legacy_compatibility_used"] is True
    assert result.artifact["dataset_quality_legacy_bypass_used"] is True
    assert result.artifact["lineage_hash"] is None
    assert "legacy_lineage_compatibility_used" in result.artifact["promotion_warnings"]
    assert "legacy_dataset_quality_bypass_used" in result.artifact["promotion_warnings"]


def test_legacy_lineage_promotion_records_dataset_quality_bypass_when_quality_evidence_missing(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(manager, _candidate())
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    for key in ("dataset_quality_hash", "dataset_quality_gate_status", "dataset_quality_gate_reasons", "dataset_quality_reports"):
        payload.pop(key, None)
    payload.pop("content_hash", None)
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    write_json_atomic(report_path, payload)

    result = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=manager,
        allow_legacy_lineage=True,
    )

    assert result.artifact["dataset_quality_legacy_bypass_used"] is True
    assert "legacy_dataset_quality_bypass_used" in result.artifact["promotion_warnings"]
    assert result.artifact["dataset_quality_hash"] == "sha256:quality"


def test_legacy_lineage_promotion_does_not_bypass_failed_dataset_quality(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(
        manager,
        _candidate(dataset_quality_gate_status="FAIL", dataset_quality_gate_reasons=["missing_candles"]),
    )
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["dataset_quality_gate_status"] = "FAIL"
    payload["dataset_quality_gate_reasons"] = ["dataset_quality_train_missing_candles"]
    payload.pop("content_hash", None)
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    write_json_atomic(report_path, payload)

    with pytest.raises(PromotionGateError, match="dataset_quality_train_missing_candles"):
        promote_candidate(
            experiment_id="promo_exp",
            candidate_id="candidate_001",
            manager=manager,
            allow_legacy_lineage=True,
        )


def test_reproduce_fails_closed_when_lineage_missing_in_legacy_artifact(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    _write_report_without_lineage(manager, _candidate())
    result = promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        manager=manager,
        allow_legacy_lineage=True,
    )

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "lineage_missing"
    assert summary["legacy_compatibility_used"] is True


def test_reproduce_reports_backtest_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = {"experiment_id": "promo_exp", "content_hash": "sha256:drifted", "candidates": []}
    write_json_atomic(report_path, payload)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "backtest_report_hash_mismatch"
    assert summary["mismatches"][0]["field"] == "backtest_report_hash"


def test_reproduce_recomputes_backtest_hash_when_body_tampered_but_embedded_hash_unchanged(
    tmp_path, monkeypatch
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["dataset_content_hash"] = "sha256:tampered_dataset"
    write_json_atomic(report_path, payload)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "backtest_report_hash_mismatch"
    assert summary["mismatches"][0]["expected"] == result.artifact["backtest_report_hash"]
    assert summary["mismatches"][0]["actual"] != payload["content_hash"]


def test_reproduce_fails_when_backtest_embedded_content_hash_tampered_only(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "backtest_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["content_hash"] = "sha256:tampered_embedded_hash"
    write_json_atomic(report_path, payload)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "backtest_report_embedded_content_hash_mismatch"


def test_reproduce_recomputes_walk_forward_hash_when_body_tampered_but_embedded_hash_unchanged(
    tmp_path, monkeypatch
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=True)
    _write_report_with_lineage(manager, candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(candidate))
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    report_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["manifest_hash"] = "sha256:tampered_manifest"
    write_json_atomic(report_path, payload)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "walk_forward_report_hash_mismatch"


def test_reproduce_reports_lineage_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    artifact = dict(result.artifact)
    artifact["lineage"] = dict(artifact["lineage"])
    artifact["lineage"]["holdout_reuse_count"] = 99
    artifact.pop("content_hash")
    artifact["content_hash"] = sha256_prefixed(content_hash_payload(artifact))
    write_json_atomic(result.artifact_path, artifact)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "lineage_hash_mismatch"


def test_reproduce_reports_manifest_and_dataset_mismatches(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    artifact = dict(result.artifact)
    artifact["manifest_hash"] = "sha256:other_manifest"
    artifact["dataset_content_hash"] = "sha256:other_dataset"
    artifact.pop("content_hash")
    artifact["content_hash"] = sha256_prefixed(content_hash_payload(artifact))
    write_json_atomic(result.artifact_path, artifact)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert {item["reason"] for item in summary["mismatches"]} >= {
        "manifest_hash_mismatch",
        "dataset_content_hash_mismatch",
    }


def test_reproduce_reports_command_args_hash_mismatch(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=False)
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    artifact = dict(result.artifact)
    artifact["command_args_hash_expected"] = "sha256:other_args"
    artifact.pop("content_hash")
    artifact["content_hash"] = sha256_prefixed(content_hash_payload(artifact))
    write_json_atomic(result.artifact_path, artifact)

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "command_args_hash_mismatch"


def test_reproduce_reports_walk_forward_required_but_missing(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(walk_forward_required=True)
    _write_report_with_lineage(manager, candidate)
    _write_walk_forward_report(manager, _walk_forward_candidate(candidate))
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    walk_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "walk_forward_report.json"
    walk_path.unlink()

    summary = reproduce_promotion(result.artifact_path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "walk_forward_required_but_missing"


def test_promotion_refuses_old_candidate_without_regime_policy(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate()
    for key in (
        "regime_classifier_version",
        "allowed_live_regimes",
        "blocked_live_regimes",
        "regime_evidence",
        "regime_gate_result",
    ):
        candidate.pop(key, None)
    candidate["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate))
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="regime_policy_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_execution_calibration_breach(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_model={
            "type": "stress",
            "fee_rate": 0.0,
            "slippage_bps": 5.0,
            "latency_ms": 100,
            "model_params_hash": "sha256:model",
        },
        execution_calibration_required=True,
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_p95_slippage_exceeds_assumption"],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_calibration_p95_slippage_exceeds_assumption"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_candle_close_execution_for_live_ready_candidate(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_timing_policy={
            "signal_basis": "closed_candle",
            "decision_time": "candle_close",
            "decision_guard_ms": 0,
            "fill_reference_policy": "candle_close_legacy",
            "quote_selection": "first_after_or_equal",
            "max_quote_wait_ms": 3000,
            "missing_quote_policy": "warn",
            "allow_same_candle_close_fill": True,
            "source": "legacy_default",
        },
        execution_reality_summary={
            "signal_event_count": 4,
            "fillable_signal_event_count": 4,
            "missing_quote_on_signal_count": 0,
            "quote_after_decision_coverage_pct": None,
            "median_quote_age_ms_on_signal": None,
            "p95_quote_age_ms_on_signal": None,
            "execution_reference_policy": "candle_close_legacy",
            "execution_reality_level": "candle_close_optimistic",
            "execution_reality_gate_status": "PASS",
            "execution_reality_gate_reasons": [],
        },
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_reference_price_candle_close_not_promotable"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_allows_optional_warn_calibration_breach(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_content_hash_missing"],
            "artifact_hash": None,
        },
    )
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["gate_result"] == "PASS"
    assert result.artifact["has_execution_calibration_warning"] is True
    assert result.artifact["execution_calibration_warning_reasons"] == ["execution_calibration_content_hash_missing"]
    assert "execution_calibration_content_hash_missing" in result.artifact["promotion_warnings"]


def test_promotion_artifact_records_empty_calibration_warning_fields_when_no_breach(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert result.artifact["gate_result"] == "PASS"
    assert result.artifact["has_execution_calibration_warning"] is False
    assert result.artifact["execution_calibration_warning_reasons"] == []
    assert result.artifact["promotion_warnings"] == []


def test_promotion_cli_prints_optional_warn_calibration_breach(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    candidate = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_p95_slippage_exceeds_assumption"],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 0
    assert "  has_execution_calibration_warning=1" in output
    assert (
        "  execution_calibration_warning_reasons="
        "execution_calibration_p95_slippage_exceeds_assumption"
    ) in output
    assert "  promotion_warnings=execution_calibration_p95_slippage_exceeds_assumption" in output


def test_promotion_cli_prints_empty_warning_fields_when_no_breach(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    candidate = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 0
    assert "  has_execution_calibration_warning=0" in output
    assert "  execution_calibration_warning_reasons=none" in output
    assert "  promotion_warnings=none" in output


def test_promotion_cli_prints_execution_event_summary(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    _write_report(manager, _candidate())

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 0
    assert (
        "  execution_event_summary="
        "execution_attempt_count=8 "
        "execution_filled_count=8 "
        "portfolio_applied_trade_count=8 "
        "pending_execution_count=0 "
        "pending_execution_after_dataset_end_count=0 "
        "skipped_execution_count=0 "
        "failed_execution_count=0 "
        "closed_trade_count=4 "
        "execution_event_timeline_incomplete=False"
    ) in output


def test_promotion_cli_refuses_required_calibration_failure_without_success_block(
    tmp_path, monkeypatch, capsys
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    candidate = _candidate(
        execution_calibration_required=True,
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_p95_slippage_exceeds_assumption"],
            "artifact_hash": "sha256:calibration",
        },
    )
    _write_report(manager, candidate)

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 1
    assert "[RESEARCH-PROMOTE-CANDIDATE] error=promotion refused:" in output
    assert "execution_calibration_p95_slippage_exceeds_assumption" in output
    assert "  gate_result=PASS" not in output
    assert "  artifact_path=" not in output
    assert "  has_execution_calibration_warning=" not in output


def test_promotion_cli_refuses_missing_lineage_without_flag(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    _write_report_without_lineage(manager, _candidate())

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
    )

    output = capsys.readouterr().out
    assert status == 1
    assert "promotion refused: lineage_missing" in output


def test_promotion_cli_allows_legacy_lineage_with_explicit_flag(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    _write_report_without_lineage(manager, _candidate())

    status = research_cli.cmd_research_promote_candidate(
        experiment_id="promo_exp",
        candidate_id="candidate_001",
        allow_legacy_lineage=True,
    )

    output = capsys.readouterr().out
    assert status == 0
    assert "legacy_lineage_compatibility_used" in output
    assert "  legacy_compatibility_used=1" in output
    assert "  dataset_quality_legacy_bypass_used=1" in output


def test_allow_legacy_lineage_does_not_bypass_execution_reality_gate(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(
        execution_timing_policy={
            "signal_basis": "closed_candle",
            "decision_time": "candle_close",
            "decision_guard_ms": 0,
            "fill_reference_policy": "candle_close_legacy",
            "quote_selection": "first_after_or_equal",
            "max_quote_wait_ms": 3000,
            "missing_quote_policy": "warn",
            "allow_same_candle_close_fill": True,
            "source": "legacy_default",
        },
        execution_reality_summary={
            "signal_event_count": 4,
            "fillable_signal_event_count": 4,
            "missing_quote_on_signal_count": 0,
            "quote_after_decision_coverage_pct": None,
            "median_quote_age_ms_on_signal": None,
            "p95_quote_age_ms_on_signal": None,
            "execution_reference_policy": "candle_close_legacy",
            "execution_reality_level": "candle_close_optimistic",
            "execution_reality_gate_status": "FAIL",
            "execution_reality_gate_reasons": ["execution_reference_price_candle_close_not_promotable"],
        },
    )
    _write_report_without_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_reference_price_candle_close_not_promotable"):
        promote_candidate(
            experiment_id="promo_exp",
            candidate_id="candidate_001",
            manager=manager,
            allow_legacy_lineage=True,
        )


def test_allow_legacy_lineage_does_not_bypass_pending_execution_refusal(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    summary = {
        "execution_attempt_count": 1,
        "execution_filled_count": 1,
        "filled_execution_count": 1,
        "portfolio_applied_trade_count": 0,
        "pending_execution_count": 1,
        "skipped_execution_count": 0,
        "failed_execution_count": 0,
        "closed_trade_count": 0,
        "pending_execution_at_end_count": 1,
        "pending_execution_after_dataset_end_count": 1,
        "execution_event_timeline_incomplete": True,
    }
    candidate = _candidate(
        acceptance_gate_result="PASS",
        validation_metrics={
            "trade_count": 0,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        execution_event_summary=summary,
    )
    _write_report_without_lineage(manager, candidate)

    with pytest.raises(PromotionGateError, match="pending_execution_after_dataset_end"):
        promote_candidate(
            experiment_id="promo_exp",
            candidate_id="candidate_001",
            manager=manager,
            allow_legacy_lineage=True,
        )


def test_promotion_uses_portfolio_applied_trade_count_not_execution_filled_count(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    summary = {
        "execution_attempt_count": 2,
        "execution_filled_count": 2,
        "filled_execution_count": 2,
        "portfolio_applied_trade_count": 0,
        "pending_execution_count": 2,
        "skipped_execution_count": 0,
        "failed_execution_count": 0,
        "closed_trade_count": 0,
        "pending_execution_at_end_count": 0,
        "pending_execution_after_dataset_end_count": 0,
        "execution_event_timeline_incomplete": False,
    }
    candidate = _candidate(
        validation_metrics={
            "trade_count": 0,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        execution_event_summary=summary,
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="portfolio_applied_trade_count_insufficient"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_execution_event_timeline_incomplete(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    summary = {
        "execution_attempt_count": 1,
        "execution_filled_count": 1,
        "filled_execution_count": 1,
        "portfolio_applied_trade_count": 0,
        "pending_execution_count": 1,
        "skipped_execution_count": 0,
        "failed_execution_count": 0,
        "closed_trade_count": 0,
        "pending_execution_at_end_count": 1,
        "pending_execution_after_dataset_end_count": 1,
        "execution_event_timeline_incomplete": True,
    }
    candidate = _candidate(
        validation_metrics={
            "trade_count": 0,
            "max_drawdown_pct": 1.0,
            "profit_factor": 2.0,
            "return_pct": 1.0,
        },
        execution_event_summary=summary,
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="execution_event_timeline_incomplete"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_cli_argument_wires_allow_legacy_lineage(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_promote(**kwargs) -> int:
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(app_module, "cmd_research_promote_candidate", fake_promote)

    status = app_module.main(
        [
            "research-promote-candidate",
            "--experiment-id",
            "promo_exp",
            "--candidate-id",
            "candidate_001",
            "--allow-legacy-lineage",
        ]
    )

    assert status == 0
    assert captured["allow_legacy_lineage"] is True


def test_candidate_profile_hash_changes_when_calibration_warning_evidence_changes() -> None:
    clean = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={"status": "PASS", "reasons": [], "artifact_hash": "sha256:calibration"},
    )
    warned = _candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_p95_latency_exceeds_assumption"],
            "artifact_hash": "sha256:calibration",
        },
    )

    assert clean["candidate_profile_hash"] != warned["candidate_profile_hash"]


def test_production_bound_candidate_refuses_warn_mode_calibration(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(
        execution_calibration_required=False,
        execution_calibration_strictness="warn",
        execution_calibration_gate={
            "status": "FAIL",
            "reasons": ["execution_calibration_quality_gate_not_passed"],
            "artifact_hash": "sha256:calibration",
            "scenario_gates": [
                {
                    "status": "FAIL",
                    "reasons": ["execution_calibration_quality_gate_not_passed"],
                    "artifact_hash": "sha256:calibration",
                    "content_hash_present": True,
                    "market": "KRW-BTC",
                    "interval": "1m",
                    "expected_market": "KRW-BTC",
                    "expected_interval": "1m",
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "quality_gate_status": "FAIL",
                }
            ],
        },
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_execution_calibration_required"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_bound_candidate_refuses_hashless_calibration(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(
        execution_calibration_artifact_hash=None,
        execution_calibration_artifact_hashes=[],
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "scenario_gates": [
                {
                    "status": "PASS",
                    "reasons": [],
                    "content_hash_present": False,
                    "market": "KRW-BTC",
                    "interval": "1m",
                    "expected_market": "KRW-BTC",
                    "expected_interval": "1m",
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "quality_gate_status": "PASS",
                }
            ],
        },
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="production_execution_calibration_hash_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_production_bound_promotion_binds_calibration_hash_into_lineage_and_reproduces(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    summary = reproduce_promotion(result.artifact_path).summary

    assert result.artifact["execution_calibration_artifact_hash"] == "sha256:calibration"
    assert result.artifact["lineage"]["execution_calibration_artifact_hash"] == "sha256:calibration"
    assert result.artifact["production_calibration_policy_result"]["status"] == "PASS"
    assert summary["ok"] is True
    assert summary["execution_calibration_artifact_hash"] == "sha256:calibration"


def test_production_bound_promotion_binds_calibration_hash_when_base_lineage_lacks_it(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate, lineage_calibration_hash=None)

    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    summary = reproduce_promotion(result.artifact_path).summary

    assert result.artifact["execution_calibration_artifact_hash"] == "sha256:calibration"
    assert result.artifact["lineage"]["execution_calibration_artifact_hash"] == "sha256:calibration"
    assert result.artifact["lineage_hash"].startswith("sha256:")
    assert summary["ok"] is True


def test_production_bound_promotion_refuses_stale_base_lineage_calibration_hash_before_write(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate(
        execution_calibration_artifact_hash="sha256:calibration-correct",
        execution_calibration_artifact_hashes=["sha256:calibration-correct"],
        execution_calibration_gate={
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration-correct",
            "artifact_hashes": ["sha256:calibration-correct"],
            "scenario_gates": [
                {
                    "status": "PASS",
                    "reasons": [],
                    "artifact_hash": "sha256:calibration-correct",
                    "content_hash_present": True,
                    "market": "KRW-BTC",
                    "interval": "1m",
                    "expected_market": "KRW-BTC",
                    "expected_interval": "1m",
                    "expected_fill_reference_policy": "next_candle_open",
                    "artifact_fill_reference_policy": "next_candle_open",
                    "artifact_execution_reality_level": "candle_next_open",
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "quality_gate_status": "PASS",
                }
            ],
        },
        production_calibration_policy_result={
            "target": "paper_candidate",
            "production_bound": True,
            "required": True,
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration-correct",
            "artifact_hashes": ["sha256:calibration-correct"],
            "policy_source": "repo_production_calibration_policy_v1",
            "operator_next_step": "none",
        },
        candidate_profile_hash=None,
    )
    _write_report_with_lineage(
        manager,
        candidate,
        lineage_calibration_hash="sha256:calibration-stale",
    )
    artifact_path = manager.data_dir() / "reports" / "research" / "promo_exp" / "promotion_candidate_001.json"

    with pytest.raises(PromotionGateError, match="lineage_execution_calibration_artifact_hash_mismatch"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)

    assert not artifact_path.exists()


def test_reproduce_fails_when_required_calibration_hash_drifts(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    path = result.artifact_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["execution_calibration_artifact_hash"] = "sha256:tampered"
    payload["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in payload.items() if k != "content_hash"}))
    write_json_atomic(path, payload)

    summary = reproduce_promotion(path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "calibration_hash_mismatch"


def test_reproduce_fails_when_required_lineage_calibration_hash_missing(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _production_candidate()
    _write_report_with_lineage(manager, candidate)
    result = promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)
    path = result.artifact_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["lineage"]["execution_calibration_artifact_hash"] = None
    payload["lineage"].pop("lineage_hash", None)
    payload["lineage"]["lineage_hash"] = compute_lineage_hash(payload["lineage"])
    payload["lineage_hash"] = payload["lineage"]["lineage_hash"]
    payload["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in payload.items() if k != "content_hash"}))
    write_json_atomic(path, payload)

    summary = reproduce_promotion(path).summary

    assert summary["ok"] is False
    assert summary["reason"] == "calibration_hash_missing"


def test_candidate_profile_hash_changes_when_pending_execution_summary_changes() -> None:
    complete = _candidate()
    pending_summary = {
        "execution_attempt_count": 8,
        "execution_filled_count": 8,
        "filled_execution_count": 8,
        "portfolio_applied_trade_count": 7,
        "pending_execution_count": 1,
        "skipped_execution_count": 0,
        "failed_execution_count": 0,
        "closed_trade_count": 4,
        "pending_execution_at_end_count": 1,
        "pending_execution_after_dataset_end_count": 1,
        "execution_event_timeline_incomplete": True,
    }
    pending = _candidate(execution_event_summary=pending_summary)

    assert complete["candidate_profile_hash"] != pending["candidate_profile_hash"]


def test_promotion_refuses_missing_final_holdout_evidence(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    candidate = _candidate(final_holdout_present=False, final_holdout_metrics=None)
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="final_holdout_evidence_missing"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_promotion_refuses_failed_required_scenario_evidence(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    scenario_results = [
        {
            "scenario_id": "scenario_001_fixed_bps_base",
            "scenario_role": "base",
            "scenario_acceptance_gate_result": "PASS",
            "scenario_fail_reasons": [],
        },
        {
            "scenario_id": "scenario_002_fixed_bps_stress",
            "scenario_role": "stress",
            "scenario_acceptance_gate_result": "FAIL",
            "scenario_fail_reasons": ["profit_factor_failed"],
        },
    ]
    candidate = _candidate(
        acceptance_gate_result="FAIL",
        gate_fail_reasons=["scenario_policy_required_scenario_failed:scenario_002_fixed_bps_stress:profit_factor_failed"],
        scenario_policy="must_pass_base_and_survive_stress",
        scenario_pass_count=1,
        scenario_fail_count=1,
        required_scenario_count=2,
        scenario_results=scenario_results,
    )
    _write_report(manager, candidate)

    with pytest.raises(PromotionGateError, match="scenario_policy_required_scenario_failed"):
        promote_candidate(experiment_id="promo_exp", candidate_id="candidate_001", manager=manager)


def test_candidate_profile_hash_changes_when_final_holdout_evidence_changes() -> None:
    first = _candidate(final_holdout_metrics={"trade_count": 4, "return_pct": 1.0})
    second = _candidate(final_holdout_metrics={"trade_count": 4, "return_pct": 2.0})

    assert first["candidate_profile_hash"] != second["candidate_profile_hash"]
