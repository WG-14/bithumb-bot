from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.statistical_selection import (
    build_statistical_selection_evidence,
    candidate_metric_universe_payload,
    candidate_metric_values_hash,
    recompute_candidate_metric_values_hash_from_report,
    recompute_white_reality_check_block_bootstrap,
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


def _aligned_return_candidates() -> list[dict[str, object]]:
    candidates = _candidates()
    candidates[0]["validation_equity_curve"] = [
        {"ts": 0, "equity": 1000.0, "cash": 1000.0, "asset_qty": 0.0},
        {"ts": 60_000, "equity": 1010.0, "cash": 1010.0, "asset_qty": 0.0},
        {"ts": 120_000, "equity": 1005.0, "cash": 1005.0, "asset_qty": 0.0},
        {"ts": 180_000, "equity": 1020.0, "cash": 1020.0, "asset_qty": 0.0},
    ]
    candidates[1]["validation_equity_curve"] = [
        {"ts": 0, "equity": 1000.0, "cash": 1000.0, "asset_qty": 0.0},
        {"ts": 60_000, "equity": 1001.0, "cash": 1001.0, "asset_qty": 0.0},
        {"ts": 120_000, "equity": 1002.0, "cash": 1002.0, "asset_qty": 0.0},
        {"ts": 180_000, "equity": 1003.0, "cash": 1003.0, "asset_qty": 0.0},
    ]
    return candidates


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


def test_statistical_buy_and_hold_benchmark_missing_marks_metric_missing() -> None:
    payload = candidate_metric_universe_payload(
        candidates=[
            {
                "parameter_candidate_id": "candidate_001",
                "parameter_values": {},
                "validation_metrics": {"return_pct": 12.0},
                "acceptance_gate_result": "PASS",
            }
        ],
        required_scenario_ids=["scenario_001"],
        primary_metric="net_excess_return",
        primary_metric_source="validation_metrics",
        benchmark="buy_and_hold",
    )

    assert payload["candidates"][0]["validation_metric_value"] is None
    assert payload["candidates"][0]["validation_metric_missing"] is True


def test_statistical_configured_benchmark_missing_marks_metric_missing() -> None:
    payload = candidate_metric_universe_payload(
        candidates=[
            {
                "parameter_candidate_id": "candidate_001",
                "parameter_values": {},
                "validation_metrics": {"return_pct": 12.0},
                "acceptance_gate_result": "PASS",
            }
        ],
        required_scenario_ids=["scenario_001"],
        primary_metric="net_excess_return",
        primary_metric_source="validation_metrics",
        benchmark="configured",
    )

    assert payload["candidates"][0]["validation_metric_value"] is None
    assert payload["candidates"][0]["validation_metric_missing"] is True


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


def test_fallback_hypothesis_identity_source_is_auditable_in_statistical_evidence() -> None:
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
        hypothesis_identity_source="manifest.hypothesis",
        experiment_family_identity_source="experiment_id",
        selection_hash="sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        search_budget=2,
        parameter_grid_size=2,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
    )

    assert evidence["hypothesis_identity_source"] == "manifest.hypothesis"
    assert evidence["experiment_family_identity_source"] == "experiment_id"


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


def test_configured_spa_and_deflated_sharpe_remain_unavailable_fail_closed() -> None:
    base = _manifest()
    assert base.statistical_validation is not None
    manifest = replace(
        base,
        statistical_validation=replace(
            base.statistical_validation,
            gates=replace(
                base.statistical_validation.gates,
                max_spa_p_value=0.05,
                min_deflated_sharpe_probability=0.8,
            ),
        ),
    )

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

    assert evidence["statistical_gate_result"] == "FAIL"
    assert "spa_method_unavailable" in evidence["gate_fail_reasons"]
    assert "deflated_sharpe_missing" in evidence["gate_fail_reasons"]
    assert "spa_not_implemented" in evidence["promotion_grade_limitations"]
    assert "deflated_sharpe_not_implemented" in evidence["promotion_grade_limitations"]


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


def test_metric_centered_manifest_still_generates_screening_summary_bootstrap() -> None:
    manifest = _manifest()
    assert manifest.statistical_validation is not None
    assert manifest.statistical_validation.bootstrap.method == "metric_centered_max_bootstrap"

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

    assert evidence["bootstrap_method"] == "metric_centered_max_bootstrap"
    assert evidence["evidence_grade"] == "SCREENING_SUMMARY_BOOTSTRAP"
    assert evidence["statistical_method"] == "summary_metric_centered_max_bootstrap"
    assert evidence["official_promotion_grade_wrc_generation_available"] is False
    assert "promotion_grade_statistical_generation_unavailable" in evidence["warnings"]
    assert evidence["summary_metric_max_bootstrap_p_value"] is not None
    assert evidence["selection_adjusted_summary_p_value"] == evidence["summary_metric_max_bootstrap_p_value"]
    assert evidence["white_reality_check_p_value"] is None
    assert evidence["white_reality_check_method"] is None
    assert "aligned_bar_portfolio_return_panel_not_generated" in evidence["promotion_grade_limitations"]
    assert "official_wrc_generation_requires_aligned_bar_return_panel" in evidence["promotion_grade_limitations"]


def test_sharpe_like_does_not_fall_back_to_return_pct() -> None:
    payload = candidate_metric_universe_payload(
        candidates=_candidates(),
        required_scenario_ids=["scenario_001"],
        primary_metric="sharpe_like",
        primary_metric_source="validation_metrics",
        benchmark="cash",
    )

    assert [row["validation_metric_missing"] for row in payload["candidates"]] == [True, True]
    assert [row["validation_metric_value"] for row in payload["candidates"]] == [None, None]


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


def test_trade_return_panel_machine_marks_promotion_grade_unavailable() -> None:
    panel = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=_candidates(),
    )

    assert panel["return_unit"] == "trade_return"
    assert panel["promotion_grade_available"] is False
    assert "trade_return_panel_cannot_satisfy_promotion_grade_wrc" in panel["limitations"]
    assert "promotion_grade_requires_aligned_return_panel" in panel["promotion_grade_fail_reasons"]


def test_research_validation_docs_match_machine_readable_wrc_limitations() -> None:
    panel = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=_candidates(),
    )
    docs = Path("docs/research-validation.md").read_text(encoding="utf-8")

    for reason in (
        "trade_return_panel_cannot_satisfy_promotion_grade_wrc",
        "official_wrc_generation_requires_aligned_bar_return_panel",
    ):
        assert reason in panel["limitations"]
        assert reason in docs


def test_return_panel_binding_detects_tampered_return_values_time_index_candidate_ids_and_metadata() -> None:
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
    report = {
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "candidates": candidates,
    }

    value_tampered = {**panel, "candidate_return_series": [dict(row) for row in panel["candidate_return_series"]]}
    value_tampered["candidate_return_series"][0]["candidate_return_series_values"] = [
        {"ts": 2, "sequence": 0, "return_pct": 9.0}
    ]
    assert "return_panel_series_malformed" in validate_return_panel_binding(
        report=report,
        evidence={"return_panel_hash": panel["content_hash"]},
        panel=value_tampered,
    )

    index_tampered = {**panel, "ordered_time_index": [999]}
    assert "return_panel_time_index_mismatch" in validate_return_panel_binding(
        report=report,
        evidence={"return_panel_hash": panel["content_hash"]},
        panel=index_tampered,
    )

    candidate_tampered = {**panel, "candidate_ids": ["candidate_999"]}
    assert "return_panel_candidate_mismatch" in validate_return_panel_binding(
        report=report,
        evidence={"return_panel_hash": panel["content_hash"]},
        panel=candidate_tampered,
    )

    metadata_tampered = {**panel, "manifest_hash": "sha256:other"}
    assert "return_panel_metadata_mismatch" in validate_return_panel_binding(
        report=report,
        evidence={"return_panel_hash": panel["content_hash"]},
        panel=metadata_tampered,
    )

    misclassified = {**panel, "promotion_grade_available": True}
    assert "return_panel_promotion_grade_misclassified" in validate_return_panel_binding(
        report=report,
        evidence={"return_panel_hash": panel["content_hash"]},
        panel=misclassified,
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


def test_spoofed_evidence_cannot_claim_bar_return_unit_for_trade_return_panel(tmp_path: Path) -> None:
    contract = _manifest().statistical_validation.as_dict()
    contract["bootstrap"] = {
        "method": "white_reality_check_block_bootstrap",
        "n_bootstrap": 100,
        "block_length_policy": "fixed",
        "seed_policy": "derived_from_selection_universe_hash",
    }
    candidates = _candidates()
    candidates[0]["scenario_results"] = [
        {
            "scenario_id": "scenario_001",
            "validation_closed_trades": [
                {"exit_ts": 1, "return_pct": 0.5},
                {"exit_ts": 2, "return_pct": -0.1},
            ],
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
    panel_path = tmp_path / "candidate_return_panel.json"
    panel_path.write_text(json.dumps(panel, sort_keys=True), encoding="utf-8")
    metric_hash = candidate_metric_values_hash(
        candidates=candidates,
        required_scenario_ids=["scenario_001"],
        primary_metric="net_excess_return",
        primary_metric_source="validation_metrics",
        benchmark="cash",
    )
    sampling = {
        "method": "white_reality_check_block_bootstrap",
        "method_name": "white_reality_check_block_bootstrap",
        "n_bootstrap": 100,
        "seed_policy": "derived_from_selection_universe_hash",
        "derived_seed": 1,
        "block_length": 2,
        "block_length_policy": "fixed",
        "stationary_bootstrap_probability": None,
        "observation_count": panel["observation_count"],
        "return_unit": "bar_excess_return",
        "benchmark": "cash",
        "missing_observation_policy": "skip_missing_candidate_trade_returns",
    }
    sampling["content_hash"] = sha256_prefixed(content_hash_payload(sampling))
    evidence = {
        "artifact_type": "statistical_selection_evidence",
        "schema_version": 1,
        "experiment_id": "stat_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "selection_universe_hash": "sha256:selection",
        "candidate_metric_values_hash": metric_hash,
        "required_scenario_ids": ["scenario_001"],
        "candidate_metric_values_summary": {
            "candidate_count": 2,
            "metric_value_count": 2,
            "missing_metric_count": 0,
            "primary_metric": "net_excess_return",
            "primary_metric_source": "validation_metrics",
            "benchmark": "cash",
        },
        "candidate_count": 2,
        "metric_value_count": 2,
        "missing_metric_count": 0,
        "search_budget": 2,
        "parameter_grid_size": 2,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single_final_holdout_for_experiment_family",
        "benchmark": "cash",
        "primary_metric": "net_excess_return",
        "primary_metric_source": "validation_metrics",
        "evidence_grade": "PROMOTION_GRADE_WRC",
        "bootstrap_method": "white_reality_check_block_bootstrap",
        "statistical_method": "white_reality_check_block_bootstrap",
        "white_reality_check_method": "white_reality_check_block_bootstrap",
        "white_reality_check_p_value": 0.01,
        "white_reality_check_available": True,
        "return_unit": "bar_excess_return",
        "return_panel_hash": panel["content_hash"],
        "return_panel_path": str(panel_path),
        "return_panel_observation_count": panel["observation_count"],
        "bootstrap_sampling_contract": sampling,
        "bootstrap_sampling_contract_hash": sampling["content_hash"],
        "statistical_validation_contract": contract,
        "method_provenance": {"implementation": "test", "version": 1},
        "effective_trial_count": 2,
        "statistical_gate_result": "PASS",
        "gate_fail_reasons": [],
    }
    evidence["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in evidence.items() if k != "content_hash"}))
    report = {
        "deployment_tier": "paper_candidate",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "candidate_count": 2,
        "search_budget": 2,
        "parameter_grid_size": 2,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single_final_holdout_for_experiment_family",
        "statistical_validation_required": True,
        "statistical_validation_contract": contract,
        "selection_universe_hash": "sha256:selection",
        "candidate_metric_values_hash": metric_hash,
        "metric_value_count": 2,
        "missing_metric_count": 0,
        "statistical_evidence_hash": evidence["content_hash"],
        "return_panel_hash": panel["content_hash"],
        "return_panel_path": str(panel_path),
        "candidates": candidates,
    }
    candidate = {
        **candidates[0],
        "deployment_tier": "paper_candidate",
        "statistical_validation_required": True,
        "statistical_validation_contract": contract,
        "selection_universe_hash": "sha256:selection",
        "candidate_metric_values_hash": metric_hash,
        "metric_value_count": 2,
        "missing_metric_count": 0,
        "statistical_evidence_hash": evidence["content_hash"],
    }

    reasons = validate_statistical_evidence_for_candidate(candidate=candidate, report=report, evidence=evidence)

    assert "return_panel_return_unit_mismatch" in reasons
    assert "return_panel_promotion_grade_unavailable" in reasons
    assert "promotion_grade_requires_aligned_return_panel" in reasons
    assert "return_panel_method_support_insufficient" in reasons


def test_recompute_white_reality_check_refuses_trade_return_panel_with_excess_values() -> None:
    candidates = _candidates()
    candidates[0]["validation_closed_trades"] = [
        {"exit_ts": 1, "return_pct": 0.5},
        {"exit_ts": 2, "return_pct": -0.1},
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
    assert panel["return_unit"] == "trade_return"
    assert panel["promotion_grade_available"] is False
    assert panel["candidate_return_series"][0]["excess_return_series_values"]

    assert recompute_white_reality_check_block_bootstrap(
        panel=panel,
        sampling_contract={
            "method": "white_reality_check_block_bootstrap",
            "n_bootstrap": 100,
            "derived_seed": 1,
            "block_length": 2,
        },
    ) is None


def test_aligned_portfolio_return_panel_hashes_bind_values_and_metadata() -> None:
    panel = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash="sha256:manifest",
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=_aligned_return_candidates(),
    )

    assert panel["return_unit"] == "portfolio_bar_return"
    assert panel["promotion_grade_available"] is True
    assert panel["ordered_time_index"] == [60_000, 120_000, 180_000]
    assert validate_return_panel_binding(
        report={
            "manifest_hash": "sha256:manifest",
            "dataset_content_hash": "sha256:dataset",
            "dataset_quality_hash": "sha256:quality",
            "return_unit": "portfolio_bar_return",
            "return_panel_hash": panel["content_hash"],
            "candidates": _aligned_return_candidates(),
        },
        evidence={"return_unit": "portfolio_bar_return", "return_panel_hash": panel["content_hash"]},
        panel=panel,
    ) == []

    tampered_values = json.loads(json.dumps(panel))
    tampered_values["candidate_return_series"][0]["candidate_return_series_values"][0]["return_pct"] += 1.0
    assert "return_panel_series_malformed" in validate_return_panel_binding(
        report={"return_panel_hash": panel["content_hash"], "candidates": _aligned_return_candidates()},
        evidence={"return_unit": "portfolio_bar_return", "return_panel_hash": panel["content_hash"]},
        panel=tampered_values,
    )

    tampered_index = json.loads(json.dumps(panel))
    tampered_index["ordered_time_index"][0] = 61_000
    assert "return_panel_time_index_mismatch" in validate_return_panel_binding(
        report={"return_panel_hash": panel["content_hash"], "candidates": _aligned_return_candidates()},
        evidence={"return_unit": "portfolio_bar_return", "return_panel_hash": panel["content_hash"]},
        panel=tampered_index,
    )

    tampered_benchmark = json.loads(json.dumps(panel))
    tampered_benchmark["candidate_return_series"][0]["benchmark_return_series_values"][0]["return_pct"] = 0.1
    assert "return_panel_series_malformed" in validate_return_panel_binding(
        report={"return_panel_hash": panel["content_hash"], "candidates": _aligned_return_candidates()},
        evidence={"return_unit": "portfolio_bar_return", "return_panel_hash": panel["content_hash"]},
        panel=tampered_benchmark,
    )


def test_wrc_generation_succeeds_only_from_aligned_promotion_grade_panel(tmp_path: Path) -> None:
    payload = _manifest().raw
    payload["statistical_validation"]["bootstrap"] = {
        "method": "white_reality_check_block_bootstrap",
        "n_bootstrap": 25,
        "block_length_policy": "fixed",
        "seed_policy": "derived_from_selection_universe_hash",
    }
    manifest = parse_manifest(payload)
    candidates = _aligned_return_candidates()
    panel = build_candidate_return_panel(
        experiment_id="stat_exp",
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        split="validation",
        benchmark="cash",
        candidates=candidates,
    )
    panel_path = tmp_path / "candidate_return_panel.json"
    panel_path.write_text(json.dumps(panel, sort_keys=True), encoding="utf-8")
    selection_hash = selection_universe_hash(
        manifest_hash=manifest.manifest_hash(),
        dataset_content_hash="sha256:dataset",
        dataset_quality_hash="sha256:quality",
        experiment_family_id="family",
        hypothesis_id="hypothesis",
        hypothesis_status="pre_registered",
        candidates=candidates,
        required_scenario_ids=[],
        primary_metric_source="validation_metrics",
        benchmark="cash",
        statistical_validation_contract=manifest.statistical_validation.as_dict(),
    )

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
        required_scenario_ids=[],
        search_budget=2,
        parameter_grid_size=2,
        attempt_index=1,
        holdout_reuse_count=0,
        dataset_reuse_policy="single_final_holdout_for_experiment_family",
        return_panel=panel,
        return_panel_path=panel_path,
    )

    assert evidence["evidence_grade"] == "PROMOTION_GRADE_WRC"
    assert evidence["official_promotion_grade_wrc_generation_available"] is True
    assert evidence["white_reality_check_p_value"] == recompute_white_reality_check_block_bootstrap(
        panel=panel,
        sampling_contract=evidence["bootstrap_sampling_contract"],
    )

    tampered = dict(evidence)
    tampered["white_reality_check_p_value"] = 0.999
    tampered["content_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in tampered.items() if k != "content_hash"}))
    report = {
        "deployment_tier": "paper_candidate",
        "manifest_hash": manifest.manifest_hash(),
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "statistical_validation_contract": manifest.statistical_validation.as_dict(),
        "selection_universe_hash": selection_hash,
        "candidate_metric_values_hash": evidence["candidate_metric_values_hash"],
        "candidate_count": 2,
        "metric_value_count": 2,
        "missing_metric_count": 0,
        "search_budget": 2,
        "parameter_grid_size": 2,
        "attempt_index": 1,
        "holdout_reuse_count": 0,
        "dataset_reuse_policy": "single_final_holdout_for_experiment_family",
        "statistical_evidence_hash": tampered["content_hash"],
        "return_panel_hash": panel["content_hash"],
        "return_panel_path": str(panel_path),
        "return_unit": "portfolio_bar_return",
        "candidates": candidates,
    }
    candidate = {**candidates[0], "deployment_tier": "paper_candidate", "statistical_validation_required": True}
    reasons = validate_statistical_evidence_for_candidate(candidate=candidate, report=report, evidence=tampered)
    assert "white_reality_check_p_value_recompute_mismatch" in reasons


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


def test_family_registry_binding_missing_file_returns_universe_missing(tmp_path) -> None:
    report, evidence, _row = _family_registry_binding_fixture(tmp_path)
    Path(evidence["family_trial_registry_path"]).unlink()

    reasons = validate_family_registry_binding(report=report, evidence=evidence)

    assert reasons == ["experiment_family_universe_missing"]


def test_family_registry_binding_missing_expected_row_hash_returns_mismatch(tmp_path) -> None:
    report, evidence, _row = _family_registry_binding_fixture(tmp_path)
    evidence.pop("family_trial_registry_row_hash")

    reasons = validate_family_registry_binding(report=report, evidence=evidence)

    assert reasons == ["experiment_family_registry_row_hash_mismatch"]


def test_family_registry_binding_unmatched_row_hash_returns_mismatch(tmp_path) -> None:
    report, evidence, _row = _family_registry_binding_fixture(tmp_path)
    evidence["family_trial_registry_row_hash"] = "sha256:" + "0" * 64

    reasons = validate_family_registry_binding(report=report, evidence=evidence)

    assert reasons == ["experiment_family_registry_row_hash_mismatch"]


def test_family_registry_binding_stale_metadata_stays_stale(tmp_path) -> None:
    report, evidence, _row = _family_registry_binding_fixture(tmp_path)
    report["experiment_id"] = "other_exp"
    evidence["experiment_id"] = "other_exp"

    reasons = validate_family_registry_binding(report=report, evidence=evidence)

    assert "experiment_family_registry_stale" in reasons
    assert "experiment_family_registry_row_hash_mismatch" not in reasons


def test_family_registry_binding_specific_hash_mismatches_remain_precise(tmp_path) -> None:
    report, evidence, _row = _family_registry_binding_fixture(tmp_path)
    cases = {
        "return_panel_hash": "experiment_family_registry_return_panel_hash_mismatch",
        "statistical_evidence_hash": "experiment_family_registry_statistical_evidence_hash_mismatch",
        "prior_registry_hash": "experiment_family_registry_prior_hash_mismatch",
    }
    for field, reason in cases.items():
        case_report, case_evidence, row = _family_registry_binding_fixture(tmp_path / field)
        row[field] = "sha256:wrong"
        row["row_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in row.items() if k != "row_hash"}))
        case_evidence["family_trial_registry_row_hash"] = row["row_hash"]
        Path(case_evidence["family_trial_registry_path"]).write_text(
            __import__("json").dumps(row, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        reasons = validate_family_registry_binding(report=case_report, evidence=case_evidence)

        assert reason in reasons


def _family_registry_binding_fixture(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
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
    row["row_hash"] = sha256_prefixed(content_hash_payload({k: v for k, v in row.items() if k != "row_hash"}))
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
    return report, evidence, row
