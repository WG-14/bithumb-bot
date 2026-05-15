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
from bithumb_bot.research.hashing import content_hash_payload, sha256_prefixed
from bithumb_bot.research.family_registry import EMPTY_REGISTRY_HASH
from bithumb_bot.research.family_registry import validate_family_registry_binding
from bithumb_bot.research.return_panel import validate_return_panel_binding


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


def test_candidate_return_panel_hash_binds_metadata_and_validation_recomputes_series() -> None:
    candidates = _candidates()
    candidates[0]["scenario_results"] = [
        {
            "scenario_id": "scenario_001",
            "validation_closed_trades": [{"entry_ts": 1, "exit_ts": 2, "return_pct": 1.0}],
        }
    ]
    panel = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=candidates,
    )
    changed_candidate = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=[{**candidates[0], "parameter_candidate_id": "candidate_changed"}, candidates[1]],
    )
    changed_benchmark = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="configured",
        candidates=candidates,
    )
    tampered = dict(panel)
    tampered["candidate_return_series"] = [dict(item) for item in panel["candidate_return_series"]]
    tampered["candidate_return_series"][0] = dict(tampered["candidate_return_series"][0])
    tampered["candidate_return_series"][0]["missing_observation_policy"] = "tampered"

    assert changed_candidate["content_hash"] != panel["content_hash"]
    assert changed_benchmark["content_hash"] != panel["content_hash"]
    assert "return_panel_series_malformed" not in validate_return_panel_binding(
        report={
            "manifest_hash": "sha256:manifest",
            "dataset_content_hash": "sha256:dataset",
            "dataset_quality_hash": "sha256:quality",
            "candidates": candidates,
        },
        evidence={"return_panel_hash": panel["content_hash"]},
        panel=panel,
    )
    tampered["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in tampered.items() if k != "content_hash"}))
    assert "return_panel_hash_mismatch" in validate_return_panel_binding(
        report={
            "manifest_hash": "sha256:manifest",
            "dataset_content_hash": "sha256:dataset",
            "dataset_quality_hash": "sha256:quality",
            "candidates": candidates,
        },
        evidence={"return_panel_hash": panel["content_hash"]},
        panel=tampered,
    )


def test_return_panel_binding_reports_aggregate_mismatches() -> None:
    candidates = _candidates()
    candidates[0]["scenario_results"] = [{"scenario_id": "scenario_001"}]
    candidates[0]["validation_closed_trades"] = [{"exit_ts": 2, "return_pct": 1.0}]
    panel = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=candidates,
    )
    tampered = {
        **panel,
        "candidate_count": 99,
        "observation_count": 99,
        "ordered_time_index": [2, 1],
        "panel_content_hash": "sha256:bad-panel-content",
    }
    rows = [dict(row) for row in panel["candidate_return_series"]]
    rows[0] = {**rows[0], "scenario_ids": ["wrong"], "time_index": [999]}
    tampered["candidate_return_series"] = rows

    reasons = validate_return_panel_binding(
        report={
            "manifest_hash": "sha256:manifest",
            "dataset_content_hash": "sha256:dataset",
            "dataset_quality_hash": "sha256:quality",
            "candidates": candidates,
        },
        evidence={"return_panel_hash": panel["content_hash"]},
        panel=tampered,
    )

    assert "return_panel_candidate_count_mismatch" in reasons
    assert "return_panel_observation_count_mismatch" in reasons
    assert "return_panel_scenario_id_mismatch" in reasons
    assert "return_panel_time_index_mismatch" in reasons
    assert "return_panel_panel_content_hash_mismatch" in reasons


def test_return_panel_binding_reports_series_alignment_mismatch() -> None:
    candidates = _candidates()
    candidates[0]["validation_closed_trades"] = [{"exit_ts": 2, "return_pct": 1.0}]
    panel = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=candidates,
    )
    tampered = {**panel, "candidate_return_series": [dict(row) for row in panel["candidate_return_series"]]}
    tampered["candidate_return_series"][0]["benchmark_return_series_values"] = [
        {"ts": 999, "sequence": 0, "return_pct": 0.0}
    ]

    reasons = validate_return_panel_binding(
        report={
            "manifest_hash": "sha256:manifest",
            "dataset_content_hash": "sha256:dataset",
            "dataset_quality_hash": "sha256:quality",
            "candidates": candidates,
        },
        evidence={"return_panel_hash": panel["content_hash"]},
        panel=tampered,
    )

    assert "return_panel_series_alignment_mismatch" in reasons


def test_promotion_grade_spoofed_wrc_is_refused_without_supported_provenance() -> None:
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
    spoofed = {
        **evidence,
        "evidence_grade": "PROMOTION_GRADE_WRC",
        "statistical_method": "white_reality_check_block_bootstrap",
        "white_reality_check_method": "white_reality_check_block_bootstrap",
        "white_reality_check_p_value": 0.01,
        "statistical_gate_result": "PASS",
        "gate_fail_reasons": [],
    }
    spoofed["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in spoofed.items() if k != "content_hash"}))
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
        "statistical_evidence_hash": spoofed["content_hash"],
        "candidates": _candidates(),
    }
    candidate = {
        **_candidates()[0],
        "deployment_tier": "paper_candidate",
        "statistical_validation_required": True,
        "statistical_validation_contract": manifest.statistical_validation.as_dict(),
        "selection_universe_hash": evidence["selection_universe_hash"],
        "candidate_metric_values_hash": evidence["candidate_metric_values_hash"],
        "metric_value_count": 2,
        "missing_metric_count": 0,
        "statistical_evidence_hash": spoofed["content_hash"],
    }

    reasons = validate_statistical_evidence_for_candidate(candidate=candidate, report=report, evidence=spoofed)

    assert "statistical_method_provenance_missing" in reasons
    assert "statistical_method_contract_mismatch" in reasons
    assert "bootstrap_sampling_contract_malformed" in reasons
    assert "return_panel_missing" in reasons


def test_promotion_grade_evidence_cannot_override_summary_bootstrap_manifest_contract() -> None:
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
    spoofed = {
        **evidence,
        "evidence_grade": "PROMOTION_GRADE_WRC",
        "statistical_method": "white_reality_check_block_bootstrap",
        "white_reality_check_method": "white_reality_check_block_bootstrap",
        "white_reality_check_p_value": 0.01,
        "statistical_gate_result": "PASS",
        "gate_fail_reasons": [],
        "method_provenance": {"implementation": "spoof", "version": 1},
    }
    spoofed["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in spoofed.items() if k != "content_hash"}))
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
        "statistical_evidence_hash": spoofed["content_hash"],
        "candidates": _candidates(),
    }
    candidate = {**_candidates()[0], **{key: report[key] for key in (
        "deployment_tier",
        "statistical_validation_required",
        "statistical_validation_contract",
        "selection_universe_hash",
        "candidate_metric_values_hash",
        "metric_value_count",
        "missing_metric_count",
        "statistical_evidence_hash",
    )}}

    reasons = validate_statistical_evidence_for_candidate(candidate=candidate, report=report, evidence=spoofed)

    assert "statistical_method_contract_mismatch" in reasons


def test_promotion_grade_bootstrap_sampling_method_name_only_fails_closed() -> None:
    contract = _manifest().statistical_validation.as_dict()
    contract["bootstrap"] = {
        "method": "white_reality_check_block_bootstrap",
        "n_bootstrap": 100,
        "block_length_policy": "fixed",
        "seed_policy": "derived_from_selection_universe_hash",
    }
    evidence = {
        "content_hash": "sha256:evidence",
        "candidate_metric_values_hash": "sha256:metric",
        "selection_universe_hash": "sha256:selection",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "candidate_count": 1,
        "metric_value_count": 1,
        "missing_metric_count": 0,
        "search_budget": 1,
        "parameter_grid_size": 1,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single",
        "benchmark": "cash",
        "primary_metric": "net_excess_return",
        "primary_metric_source": "validation_metrics",
        "evidence_grade": "PROMOTION_GRADE_WRC",
        "bootstrap_method": "white_reality_check_block_bootstrap",
        "statistical_method": "white_reality_check_block_bootstrap",
        "white_reality_check_method": "white_reality_check_block_bootstrap",
        "white_reality_check_p_value": 0.01,
        "statistical_gate_result": "PASS",
        "gate_fail_reasons": [],
        "effective_trial_count": 1,
        "return_unit": "bar_excess_return",
        "return_panel_observation_count": 2,
        "statistical_validation_contract": contract,
        "method_provenance": {"implementation": "test", "version": 1},
        "bootstrap_sampling_contract": {
            "method_name": "white_reality_check_block_bootstrap",
            "n_bootstrap": 100,
            "derived_seed": 1,
            "block_length": 2,
        },
    }
    evidence["bootstrap_sampling_contract"]["content_hash"] = sha256_prefixed(content_hash_payload(evidence["bootstrap_sampling_contract"]))
    evidence["bootstrap_sampling_contract_hash"] = evidence["bootstrap_sampling_contract"]["content_hash"]
    evidence["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in evidence.items() if k != "content_hash"}))
    report = {
        "deployment_tier": "paper_candidate",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "candidate_count": 1,
        "search_budget": 1,
        "parameter_grid_size": 1,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single",
        "statistical_validation_required": True,
        "statistical_validation_contract": contract,
        "selection_universe_hash": "sha256:selection",
        "candidate_metric_values_hash": "sha256:metric",
        "metric_value_count": 1,
        "missing_metric_count": 0,
        "statistical_evidence_hash": evidence["content_hash"],
        "candidates": [{"parameter_candidate_id": "candidate_001", "validation_metrics": {"return_pct": 1.0}}],
    }
    candidate = {**report["candidates"][0], **{key: report[key] for key in (
        "deployment_tier",
        "statistical_validation_required",
        "statistical_validation_contract",
        "selection_universe_hash",
        "candidate_metric_values_hash",
        "metric_value_count",
        "missing_metric_count",
        "statistical_evidence_hash",
    )}}

    reasons = validate_statistical_evidence_for_candidate(candidate=candidate, report=report, evidence=evidence)

    assert "bootstrap_sampling_contract_malformed" in reasons
    assert "bootstrap_sampling_contract_method_mismatch" in reasons


def test_trade_return_panel_cannot_satisfy_promotion_grade_wrc() -> None:
    contract = _manifest().statistical_validation.as_dict()
    contract["bootstrap"] = {
        "method": "white_reality_check_block_bootstrap",
        "n_bootstrap": 100,
        "block_length_policy": "fixed",
        "seed_policy": "derived_from_selection_universe_hash",
    }
    evidence = {
        "evidence_grade": "PROMOTION_GRADE_WRC",
        "statistical_method": "white_reality_check_block_bootstrap",
        "white_reality_check_method": "white_reality_check_block_bootstrap",
        "bootstrap_method": "white_reality_check_block_bootstrap",
        "white_reality_check_p_value": 0.01,
        "return_unit": "trade_return",
        "return_panel_observation_count": 2,
        "return_panel_hash": "sha256:return-panel",
        "statistical_validation_contract": contract,
        "method_provenance": {"implementation": "test", "version": 1},
        "bootstrap_sampling_contract": {
            "method": "white_reality_check_block_bootstrap",
            "n_bootstrap": 100,
            "derived_seed": 1,
            "block_length": 2,
        },
    }
    evidence["bootstrap_sampling_contract"]["content_hash"] = sha256_prefixed(content_hash_payload(evidence["bootstrap_sampling_contract"]))
    evidence["bootstrap_sampling_contract_hash"] = evidence["bootstrap_sampling_contract"]["content_hash"]

    reasons = validate_statistical_evidence_for_candidate(
        candidate={
            "deployment_tier": "paper_candidate",
            "statistical_validation_required": True,
            "statistical_validation_contract": contract,
        },
        report={"deployment_tier": "paper_candidate", "statistical_validation_contract": contract, "candidates": []},
        evidence=evidence,
    )

    assert "promotion_grade_requires_aligned_return_panel" in reasons


def test_family_registry_binding_detects_hash_tampering(tmp_path) -> None:
    manifest = _manifest()
    contract = {**manifest.statistical_validation.as_dict(), "multiple_testing_scope": "experiment_family"}
    path = tmp_path / "trial_registry.jsonl"
    evidence = {
        "experiment_id": "stat_exp",
        "experiment_family_id": "family",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "return_panel_hash": "sha256:return-panel",
        "family_trial_registry_path": str(path),
        "family_trial_registry_prior_hash": EMPTY_REGISTRY_HASH,
        "family_trial_registry_bound_evidence_hash": "sha256:pre-registry",
        "statistical_validation_contract": contract,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
    }
    row = {
        "schema_version": 1,
        "experiment_family_id": "family",
        "experiment_id": "stat_exp",
        "manifest_hash": "sha256:manifest",
        "hypothesis_id": "hypothesis",
        "hypothesis_status": "pre_registered",
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_content_hash": "sha256:dataset",
        "parameter_space_hash": "sha256:parameter-space",
        "candidate_count": 2,
        "return_panel_hash": "sha256:return-panel",
        "statistical_evidence_hash": "sha256:pre-registry",
        "statistical_evidence_hash_phase": "pre_registry_evidence_hash",
        "result_status": "PASS",
        "prior_registry_hash": EMPTY_REGISTRY_HASH,
    }
    row["row_hash"] = sha256_prefixed(content_hash_payload(row))
    evidence["family_trial_registry_row_hash"] = row["row_hash"]
    path.write_text(__import__("json").dumps(row, sort_keys=True) + "\n", encoding="utf-8")
    report = {
        "experiment_id": "stat_exp",
        "experiment_family_id": "family",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "parameter_space_hash": "sha256:parameter-space",
        "candidate_count": 2,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "statistical_validation_contract": contract,
    }

    assert validate_family_registry_binding(report=report, evidence=evidence) == []
    tampered = dict(row)
    tampered["return_panel_hash"] = "sha256:tampered"
    path.write_text(__import__("json").dumps(tampered, sort_keys=True) + "\n", encoding="utf-8")

    reasons = validate_family_registry_binding(report=report, evidence=evidence)

    assert "experiment_family_registry_row_hash_mismatch" in reasons
    assert "experiment_family_registry_return_panel_hash_mismatch" in reasons
