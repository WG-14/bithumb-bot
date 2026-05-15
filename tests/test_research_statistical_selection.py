from __future__ import annotations

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.statistical_selection import (
    build_statistical_selection_evidence,
    candidate_metric_values_hash,
    recompute_candidate_metric_values_hash_from_report,
    selection_universe_hash,
    validate_statistical_evidence_for_candidate,
)
from bithumb_bot.research.return_panel import build_candidate_return_panel


def _manifest():
    return parse_manifest(
        {
            "experiment_id": "stat_exp",
            "hypothesis": "Synthetic edge should survive selection correction.",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "dataset": {
                "source": "sqlite_candles",
                "snapshot_id": "snap",
                "train": {"start": "2023-01-01", "end": "2023-01-01"},
                "validation": {"start": "2023-01-02", "end": "2023-01-02"},
                "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
            },
            "parameter_space": {"SMA_SHORT": [2, 3], "SMA_LONG": [4]},
            "cost_model": {"fee_rate": 0.0, "slippage_bps": [0]},
            "acceptance_gate": {
                "min_trade_count": 1,
                "max_mdd_pct": 50,
                "min_profit_factor": 1.0,
                "oos_return_must_be_positive": True,
                "parameter_stability_required": False,
            },
            "statistical_validation": {
                "required_for_promotion": True,
                "benchmark": "cash",
                "primary_metric": "net_excess_return",
                "selection_universe": "all_parameter_candidates_all_required_scenarios",
                "multiple_testing_scope": "experiment",
                "bootstrap": {
                    "method": "metric_centered_max_bootstrap",
                    "n_bootstrap": 100,
                    "block_length_policy": "not_applicable_summary_metric",
                    "seed_policy": "derived_from_selection_universe_hash",
                },
                "gates": {
                    "max_reality_check_p_value": 0.05,
                    "max_spa_p_value": None,
                    "min_deflated_sharpe_probability": None,
                    "max_holdout_reuse_count": 0,
                    "max_attempt_index_without_new_hypothesis": 1,
                },
            },
        }
    )


def _candidates() -> list[dict[str, object]]:
    return [
        {
            "parameter_candidate_id": "candidate_001",
            "parameter_values": {"SMA_SHORT": 2, "SMA_LONG": 4},
            "validation_metrics": {"return_pct": 1.0},
        },
        {
            "parameter_candidate_id": "candidate_002",
            "parameter_values": {"SMA_SHORT": 3, "SMA_LONG": 4},
            "validation_metrics": {"return_pct": 0.0},
        },
    ]


def test_selection_universe_hash_is_deterministic_and_binds_candidates() -> None:
    manifest = _manifest()
    contract = manifest.statistical_validation.as_dict()
    first = selection_universe_hash(
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        candidates=_candidates(),
        required_scenario_ids=["scenario_001"],
        primary_metric_source="validation_metrics",
        benchmark="cash",
        statistical_validation_contract=contract,
    )
    reordered = selection_universe_hash(
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        candidates=list(reversed(_candidates())),
        required_scenario_ids=["scenario_001"],
        primary_metric_source="validation_metrics",
        benchmark="cash",
        statistical_validation_contract=contract,
    )
    changed = _candidates()
    changed[0]["parameter_values"] = {"SMA_SHORT": 5, "SMA_LONG": 9}
    changed_hash = selection_universe_hash(
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        candidates=changed,
        required_scenario_ids=["scenario_001"],
        primary_metric_source="validation_metrics",
        benchmark="cash",
        statistical_validation_contract=contract,
    )

    assert first == reordered
    assert changed_hash != first


def test_statistical_evidence_content_hash_is_stable_and_fails_no_edge_large_universe() -> None:
    manifest = _manifest()
    candidates = _candidates()
    selection_hash = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

    evidence = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=candidates,
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash=selection_hash,
        search_budget=5000,
        parameter_grid_size=5000,
        attempt_index=3,
        holdout_reuse_count=2,
        dataset_reuse_policy="reuse_visible",
    )
    repeat = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=candidates,
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash=selection_hash,
        search_budget=5000,
        parameter_grid_size=5000,
        attempt_index=3,
        holdout_reuse_count=2,
        dataset_reuse_policy="reuse_visible",
    )

    assert evidence["content_hash"] == repeat["content_hash"]
    assert evidence["statistical_gate_result"] == "FAIL"
    assert "attempt_budget_exceeded" in evidence["gate_fail_reasons"]
    assert "holdout_reuse_budget_exceeded" in evidence["gate_fail_reasons"]


def test_candidate_metric_values_hash_is_deterministic_and_binds_metric_values() -> None:
    candidates = _candidates()

    first = candidate_metric_values_hash(
        candidates=candidates,
        required_scenario_ids=["scenario_001"],
        primary_metric="net_excess_return",
        primary_metric_source="validation_metrics",
        benchmark="cash",
    )
    reordered = candidate_metric_values_hash(
        candidates=list(reversed(candidates)),
        required_scenario_ids=["scenario_001"],
        primary_metric="net_excess_return",
        primary_metric_source="validation_metrics",
        benchmark="cash",
    )
    changed = _candidates()
    changed[0]["validation_metrics"] = {"return_pct": 2.0}
    changed_hash = candidate_metric_values_hash(
        candidates=changed,
        required_scenario_ids=["scenario_001"],
        primary_metric="net_excess_return",
        primary_metric_source="validation_metrics",
        benchmark="cash",
    )

    assert first == reordered
    assert changed_hash != first


def test_candidate_metric_values_hash_recompute_detects_changed_metric_value() -> None:
    manifest = _manifest()
    candidates = _candidates()
    evidence = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=candidates,
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        required_scenario_ids=["scenario_001"],
        search_budget=2,
        parameter_grid_size=2,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
    )
    report = {"candidates": _candidates()}
    assert recompute_candidate_metric_values_hash_from_report(report=report, evidence=evidence) == evidence[
        "candidate_metric_values_hash"
    ]

    report["candidates"][0]["validation_metrics"] = {"return_pct": 2.0}

    assert recompute_candidate_metric_values_hash_from_report(report=report, evidence=evidence) != evidence[
        "candidate_metric_values_hash"
    ]


def test_candidate_metric_values_hash_recompute_requires_required_scenario_ids() -> None:
    manifest = _manifest()
    evidence = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=_candidates(),
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        required_scenario_ids=["scenario_001"],
        search_budget=2,
        parameter_grid_size=2,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
    )
    stale = dict(evidence)
    stale.pop("required_scenario_ids")

    assert recompute_candidate_metric_values_hash_from_report(report={"candidates": _candidates()}, evidence=stale) is None


def test_candidate_metric_values_hash_recompute_requires_full_candidates_list() -> None:
    manifest = _manifest()
    evidence = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=_candidates(),
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        required_scenario_ids=["scenario_001"],
        search_budget=2,
        parameter_grid_size=2,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
    )

    assert recompute_candidate_metric_values_hash_from_report(report={}, evidence=evidence) is None
    assert recompute_candidate_metric_values_hash_from_report(report={"candidates": {"bad": "shape"}}, evidence=evidence) is None


def test_statistical_evidence_fails_closed_when_metric_universe_is_incomplete() -> None:
    manifest = _manifest()
    candidates = _candidates()
    candidates[1].pop("validation_metrics")

    evidence = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=candidates,
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        required_scenario_ids=["scenario_001"],
        search_budget=2,
        parameter_grid_size=2,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
    )

    assert evidence["metric_value_count"] == 1
    assert evidence["missing_metric_count"] == 1
    assert evidence["statistical_gate_result"] == "FAIL"
    assert "statistical_metric_values_missing" in evidence["gate_fail_reasons"]


def test_statistical_validation_detects_metadata_mismatch_and_underreported_trials() -> None:
    manifest = _manifest()
    candidates = _candidates()
    evidence = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=candidates,
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        required_scenario_ids=["scenario_001"],
        search_budget=5000,
        parameter_grid_size=5000,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
    )
    stale = dict(evidence)
    stale["search_budget"] = 1
    stale["effective_trial_count"] = 1
    stale["content_hash"] = evidence["content_hash"]
    report = {
        "deployment_tier": "paper_candidate",
        "manifest_hash": manifest.manifest_hash(),
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "candidate_count": 2,
        "search_budget": 5000,
        "parameter_grid_size": 5000,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single_final_holdout_for_experiment_family",
        "statistical_validation_required": True,
        "statistical_validation_contract": manifest.statistical_validation.as_dict(),
        "selection_universe_hash": evidence["selection_universe_hash"],
        "candidate_metric_values_hash": evidence["candidate_metric_values_hash"],
        "metric_value_count": 2,
        "statistical_evidence_hash": evidence["content_hash"],
        "candidates": candidates,
    }
    candidate = {
        **candidates[0],
        "deployment_tier": "paper_candidate",
        "statistical_validation_required": True,
        "statistical_validation_contract": manifest.statistical_validation.as_dict(),
        "selection_universe_hash": evidence["selection_universe_hash"],
        "candidate_metric_values_hash": evidence["candidate_metric_values_hash"],
        "metric_value_count": 2,
        "statistical_evidence_hash": evidence["content_hash"],
    }

    reasons = validate_statistical_evidence_for_candidate(
        candidate=candidate,
        report=report,
        evidence=stale,
    )

    assert "statistical_evidence_hash_mismatch" in reasons
    assert "statistical_search_budget_mismatch" in reasons
    assert "statistical_effective_trial_count_underreported" in reasons


def test_statistical_validation_refuses_missing_metric_value_hash() -> None:
    manifest = _manifest()
    candidates = _candidates()
    evidence = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=candidates,
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        required_scenario_ids=["scenario_001"],
        search_budget=2,
        parameter_grid_size=2,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
    )
    report = {
        "deployment_tier": "paper_candidate",
        "manifest_hash": manifest.manifest_hash(),
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "candidate_count": 2,
        "search_budget": 2,
        "parameter_grid_size": 2,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single_final_holdout_for_experiment_family",
        "statistical_validation_required": True,
        "statistical_validation_contract": manifest.statistical_validation.as_dict(),
        "selection_universe_hash": evidence["selection_universe_hash"],
        "metric_value_count": 2,
        "statistical_evidence_hash": evidence["content_hash"],
        "candidates": candidates,
    }

    reasons = validate_statistical_evidence_for_candidate(
        candidate={**candidates[0], "deployment_tier": "paper_candidate"},
        report=report,
        evidence=evidence,
    )

    assert "candidate_metric_values_hash_missing" in reasons


def test_summary_bootstrap_is_screening_grade_and_does_not_populate_wrc_field() -> None:
    manifest = _manifest()
    evidence = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=_candidates(),
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        required_scenario_ids=["scenario_001"],
        search_budget=2,
        parameter_grid_size=2,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
    )

    assert evidence["evidence_grade"] == "SCREENING_SUMMARY_BOOTSTRAP"
    assert evidence["statistical_method"] == "summary_metric_centered_max_bootstrap"
    assert evidence["summary_metric_max_bootstrap_p_value"] is not None
    assert evidence["white_reality_check_p_value"] is None
    assert evidence["white_reality_check_method"] is None


def test_screening_grade_evidence_cannot_satisfy_production_bound_promotion() -> None:
    manifest = _manifest()
    candidates = _candidates()
    evidence = build_statistical_selection_evidence(
        manifest=manifest,
        candidates=candidates,
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        selection_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        required_scenario_ids=["scenario_001"],
        search_budget=2,
        parameter_grid_size=2,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
    )
    report = {
        "deployment_tier": "paper_candidate",
        "manifest_hash": manifest.manifest_hash(),
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "candidate_count": 2,
        "search_budget": 2,
        "parameter_grid_size": 2,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single_final_holdout_for_experiment_family",
        "statistical_validation_required": True,
        "statistical_validation_contract": manifest.statistical_validation.as_dict(),
        "selection_universe_hash": evidence["selection_universe_hash"],
        "candidate_metric_values_hash": evidence["candidate_metric_values_hash"],
        "metric_value_count": 2,
        "missing_metric_count": 0,
        "statistical_evidence_hash": evidence["content_hash"],
        "candidates": candidates,
    }
    candidate = {
        **candidates[0],
        "deployment_tier": "paper_candidate",
        "statistical_validation_required": True,
        "statistical_validation_contract": manifest.statistical_validation.as_dict(),
        "selection_universe_hash": evidence["selection_universe_hash"],
        "candidate_metric_values_hash": evidence["candidate_metric_values_hash"],
        "metric_value_count": 2,
        "missing_metric_count": 0,
        "statistical_evidence_hash": evidence["content_hash"],
    }

    reasons = validate_statistical_evidence_for_candidate(candidate=candidate, report=report, evidence=evidence)

    assert "statistical_evidence_grade_insufficient" in reasons
    assert "return_panel_missing" in reasons


def test_candidate_return_panel_hash_binds_trade_return_series() -> None:
    candidates = _candidates()
    candidates[0]["scenario_results"] = [
        {
            "validation_closed_trades": [
                {"entry_ts": 1, "exit_ts": 2, "return_pct": 1.0},
                {"entry_ts": 3, "exit_ts": 4, "return_pct": -0.5},
            ]
        }
    ]
    first = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=candidates,
    )
    candidates[0]["scenario_results"][0]["validation_closed_trades"][1]["return_pct"] = -0.4
    changed = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=candidates,
    )

    assert first["content_hash"] != changed["content_hash"]
    assert first["candidate_return_series"][0]["observation_count"] == 2
