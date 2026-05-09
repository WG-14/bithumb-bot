from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

from bithumb_bot.execution_quality import ExecutionQualityThresholds
from bithumb_bot.paths import PathManager
from bithumb_bot.market_regime import MARKET_REGIME_VERSION, evaluate_regime_acceptance_gate

from .dataset_snapshot import (
    DatasetQualityReport,
    DatasetSnapshot,
    build_dataset_quality_report,
    combined_dataset_fingerprint,
    combined_dataset_quality_hash,
    load_dataset_range,
    load_dataset_split,
)
from .backtest_engine import BacktestRun, execution_event_summary
from .deployment_policy import validate_production_calibration_policy
from .execution_calibration import compare_calibration_to_scenario
from .execution_model import FixedBpsExecutionModel, StressExecutionModel, model_params_hash
from .execution_timing import execution_reality_gate, signal_quote_coverage_summary
from .experiment_manifest import DateRange, ExecutionScenario, ExperimentManifest
from .hashing import sha256_prefixed
from .lineage import build_research_lineage
from .metrics_contract import METRICS_SCHEMA_VERSION
from .parameter_space import candidate_id, iter_parameter_candidates
from .promotion_gate import build_candidate_profile
from .report_writer import ResearchReportPaths, write_research_report
from .strategy_registry import research_strategy_data_requirements, resolve_research_strategy


class ResearchValidationError(ValueError):
    pass


TOP_OF_BOOK_OPTIONAL_COVERAGE_WARNING = "top_of_book_optional_coverage_warning"
TOP_OF_BOOK_OPERATOR_NEXT_ACTION = (
    "collect orderbook top snapshots with sync-orderbook-top, rerun research-backtest, "
    "and verify top_of_book_coverage_pct"
)


def run_research_backtest(
    *,
    manifest: ExperimentManifest,
    db_path: str | Path,
    manager: PathManager,
    generated_at: str | None = None,
    execution_calibration: dict[str, Any] | None = None,
    manifest_path: str | None = None,
    command_args: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _validate_strategy_data_requirements(manifest)
    snapshots = {
        "train": load_dataset_split(db_path=db_path, manifest=manifest, split_name="train"),
        "validation": load_dataset_split(db_path=db_path, manifest=manifest, split_name="validation"),
    }
    if manifest.dataset.split.final_holdout is not None:
        snapshots["final_holdout"] = load_dataset_split(
            db_path=db_path,
            manifest=manifest,
            split_name="final_holdout",
        )
    quality_reports = _quality_reports(db_path=db_path, snapshots=snapshots)
    _require_enough_candles(snapshots.values())

    candidates = _evaluate_candidates(
        manifest=manifest,
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=False,
        execution_calibration=execution_calibration,
    )
    report = _report_payload(
        manifest=manifest,
        snapshots=tuple(snapshots.values()),
        quality_reports=tuple(quality_reports.values()),
        candidates=candidates,
        report_kind="backtest",
        generated_at=generated_at,
        manifest_path=manifest_path,
        command_name="research-backtest",
        command_args=command_args,
        execution_calibration=execution_calibration,
    )
    paths, content_hash = write_research_report(
        manager=manager,
        experiment_id=manifest.experiment_id,
        report_name="backtest",
        payload=report,
    )
    report["content_hash"] = content_hash
    report["artifact_paths"] = _path_payload(paths)
    return report


def run_research_walk_forward(
    *,
    manifest: ExperimentManifest,
    db_path: str | Path,
    manager: PathManager,
    generated_at: str | None = None,
    execution_calibration: dict[str, Any] | None = None,
    manifest_path: str | None = None,
    command_args: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if manifest.walk_forward is None:
        raise ResearchValidationError("walk_forward_missing")
    _validate_strategy_data_requirements(manifest)
    windows = _rolling_walk_forward_windows(manifest)
    if len(windows) < manifest.walk_forward.min_windows:
        raise ResearchValidationError(
            f"walk_forward_insufficient_windows: available={len(windows)} min_windows={manifest.walk_forward.min_windows}"
        )
    snapshots = _load_walk_forward_snapshots(db_path=db_path, manifest=manifest, windows=windows)
    quality_reports = _quality_reports(db_path=db_path, snapshots=snapshots)
    _require_enough_candles(snapshots.values())
    candidates = _evaluate_candidates(
        manifest=manifest,
        snapshots=snapshots,
        quality_reports=quality_reports,
        include_walk_forward=True,
        execution_calibration=execution_calibration,
    )
    report = _report_payload(
        manifest=manifest,
        snapshots=tuple(snapshots.values()),
        quality_reports=tuple(quality_reports.values()),
        candidates=candidates,
        report_kind="walk_forward",
        generated_at=generated_at,
        manifest_path=manifest_path,
        command_name="research-walk-forward",
        command_args=command_args,
        execution_calibration=execution_calibration,
    )
    paths, content_hash = write_research_report(
        manager=manager,
        experiment_id=manifest.experiment_id,
        report_name="walk_forward",
        payload=report,
    )
    report["content_hash"] = content_hash
    report["artifact_paths"] = _path_payload(paths)
    return report


def _evaluate_candidates(
    *,
    manifest: ExperimentManifest,
    snapshots: dict[str, DatasetSnapshot],
    quality_reports: dict[str, DatasetQualityReport],
    include_walk_forward: bool,
    execution_calibration: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    raw_candidates = iter_parameter_candidates(manifest.parameter_space)
    aggregates: dict[str, dict[str, Any]] = {}
    manifest_hash = manifest.manifest_hash()
    dataset_hash = combined_dataset_fingerprint(tuple(snapshots.values()))
    dataset_quality_hash = combined_dataset_quality_hash(tuple(quality_reports.values()))
    dataset_quality_status, dataset_quality_reasons = _combined_dataset_quality_gate(quality_reports)
    dataset_warning_codes = _dataset_quality_warning_codes(quality_reports)
    top_of_book_quality_summary = _top_of_book_quality_summary(quality_reports)
    runner = resolve_research_strategy(manifest.strategy_name)

    for scenario_index, scenario in enumerate(manifest.execution_model.scenarios):
        scenario_id = _scenario_id(scenario, scenario_index)
        calibration_gate = compare_calibration_to_scenario(
            calibration=execution_calibration,
            assumed_slippage_bps=scenario.slippage_bps + scenario.market_order_extra_cost_bps,
            assumed_latency_ms=scenario.latency_ms,
            assumed_partial_fill_rate=scenario.partial_fill_rate,
            assumed_order_failure_rate=scenario.order_failure_rate,
            expected_market=manifest.market,
            expected_interval=manifest.interval,
            expected_execution_timing_policy=manifest.execution_timing.as_dict(),
            require_content_hash=manifest.execution_model.calibration_required,
            min_sample_count=ExecutionQualityThresholds().min_sample,
            require_quality_gate_pass=(
                manifest.execution_model.calibration_required
                or manifest.execution_model.calibration_strictness == "fail"
            ),
        )
        base_results: list[dict[str, Any]] = []
        for index, params in enumerate(raw_candidates):
            param_candidate_id = candidate_id(params, index)
            train = runner(
                dataset=snapshots["train"],
                parameter_values=params,
                fee_rate=scenario.fee_rate,
                slippage_bps=float(scenario.slippage_bps),
                parameter_stability_score=None,
                execution_model=_execution_model_from_scenario(
                    scenario,
                    seed_context=_seed_context(
                        manifest_hash=manifest_hash,
                        scenario=scenario,
                        scenario_id=scenario_id,
                        parameter_candidate_id=param_candidate_id,
                        split_name="train",
                    ),
                ),
                execution_timing_policy=manifest.execution_timing,
            )
            validation = runner(
                dataset=snapshots["validation"],
                parameter_values=params,
                fee_rate=scenario.fee_rate,
                slippage_bps=float(scenario.slippage_bps),
                parameter_stability_score=None,
                execution_model=_execution_model_from_scenario(
                    scenario,
                    seed_context=_seed_context(
                        manifest_hash=manifest_hash,
                        scenario=scenario,
                        scenario_id=scenario_id,
                        parameter_candidate_id=param_candidate_id,
                        split_name="validation",
                    ),
                ),
                execution_timing_policy=manifest.execution_timing,
            )
            final_holdout = (
                runner(
                    dataset=snapshots["final_holdout"],
                    parameter_values=params,
                    fee_rate=scenario.fee_rate,
                    slippage_bps=float(scenario.slippage_bps),
                    parameter_stability_score=None,
                        execution_model=_execution_model_from_scenario(
                        scenario,
                        seed_context=_seed_context(
                            manifest_hash=manifest_hash,
                            scenario=scenario,
                            scenario_id=scenario_id,
                            parameter_candidate_id=param_candidate_id,
                            split_name="final_holdout",
                        ),
                        ),
                        execution_timing_policy=manifest.execution_timing,
                    )
                if "final_holdout" in snapshots
                else None
            )
            walk_forward = (
                _walk_forward_metrics(
                    manifest=manifest,
                    snapshots=snapshots,
                    parameter_values=params,
                    fee_rate=scenario.fee_rate,
                    scenario=scenario,
                    parameter_candidate_id=param_candidate_id,
                    parameter_stability_score=None,
                )
                if include_walk_forward
                else None
            )
            base_results.append(
                {
                    "index": index,
                    "candidate_id": param_candidate_id,
                    "parameter_values": params,
                    "train_metrics": train.metrics.as_dict(),
                    "validation_metrics": validation.metrics.as_dict(),
                    "final_holdout_metrics": final_holdout.metrics.as_dict() if final_holdout else None,
                    "train_metrics_v2": _metrics_v2_payload(train),
                    "validation_metrics_v2": _metrics_v2_payload(validation),
                    "final_holdout_metrics_v2": _metrics_v2_payload(final_holdout) if final_holdout else None,
                    "train_execution_metadata": _execution_metadata(train.trades),
                    "validation_execution_metadata": _execution_metadata(validation.trades),
                    "final_holdout_execution_metadata": _execution_metadata(final_holdout.trades) if final_holdout else None,
                    "train_execution_event_summary": train.execution_event_summary or execution_event_summary(train.trades),
                    "validation_execution_event_summary": validation.execution_event_summary or execution_event_summary(validation.trades),
                    "final_holdout_execution_event_summary": (
                        final_holdout.execution_event_summary or execution_event_summary(final_holdout.trades)
                        if final_holdout
                        else None
                    ),
                    "train_regime_performance": [row.as_dict() for row in train.regime_performance],
                    "train_regime_coverage": [row.as_dict() for row in train.regime_coverage],
                    "validation_regime_performance": [row.as_dict() for row in validation.regime_performance],
                    "validation_regime_coverage": [row.as_dict() for row in validation.regime_coverage],
                    "final_holdout_regime_performance": (
                        [row.as_dict() for row in final_holdout.regime_performance] if final_holdout else None
                    ),
                    "final_holdout_regime_coverage": (
                        [row.as_dict() for row in final_holdout.regime_coverage] if final_holdout else None
                    ),
                    "walk_forward_metrics": walk_forward,
                    "warnings": sorted(set(train.warnings + validation.warnings + ((final_holdout.warnings if final_holdout else ())))),
                }
            )
        stability = _parameter_stability_scores(
            manifest=manifest,
            candidates=raw_candidates,
            evaluated_candidates=base_results,
        )
        for base in base_results:
            index = int(base["index"])
            params = dict(base["parameter_values"])
            stability_payload = stability[index]
            stability_score = stability_payload["score"]
            train_metrics = dict(base["train_metrics"])
            validation_metrics = dict(base["validation_metrics"])
            train_metrics_v2 = dict(base["train_metrics_v2"])
            validation_metrics_v2 = dict(base["validation_metrics_v2"])
            final_holdout_metrics = (
                dict(base["final_holdout_metrics"]) if isinstance(base.get("final_holdout_metrics"), dict) else None
            )
            final_holdout_metrics_v2 = (
                dict(base["final_holdout_metrics_v2"]) if isinstance(base.get("final_holdout_metrics_v2"), dict) else None
            )
            train_metrics["parameter_stability_score"] = stability_score
            validation_metrics["parameter_stability_score"] = stability_score
            if final_holdout_metrics is not None:
                final_holdout_metrics["parameter_stability_score"] = stability_score
            walk_forward = base["walk_forward_metrics"]
            regime_gate = evaluate_regime_acceptance_gate(
                gate=manifest.acceptance_gate.regime_acceptance_gate,
                performance_rows=tuple(base.get("validation_regime_performance") or ()),
            )
            gate_result, fail_reasons = _gate_result(
                manifest=manifest,
                validation_metrics=validation_metrics,
                validation_metrics_v2=validation_metrics_v2,
                final_holdout_metrics=final_holdout_metrics,
                final_holdout_metrics_v2=final_holdout_metrics_v2,
                walk_forward_metrics=walk_forward,
                stability_score=stability_score,
                include_walk_forward=include_walk_forward,
                regime_gate_result=regime_gate.as_dict(),
                execution_calibration_gate=calibration_gate,
                dataset_quality_status=dataset_quality_status,
                dataset_quality_reasons=dataset_quality_reasons,
            )
            execution_metadata = list(base.get("validation_execution_metadata") or [])
            execution_reality_summary = _execution_reality_summary(
                policy=manifest.execution_timing,
                execution_metadata=execution_metadata,
                execution_event_summary=dict(base.get("validation_execution_event_summary") or {}),
            )
            execution_event_gate_reasons = _execution_event_gate_reasons(dict(base.get("validation_execution_event_summary") or {}))
            if execution_event_gate_reasons:
                gate_result = "FAIL"
                fail_reasons = sorted(set(fail_reasons) | set(execution_event_gate_reasons))
            if execution_reality_summary["execution_reality_gate_status"] == "FAIL":
                gate_result = "FAIL"
                fail_reasons = sorted(
                    set(fail_reasons)
                    | set(str(item) for item in execution_reality_summary["execution_reality_gate_reasons"])
                )
            cost_model = {
                "fee_rate": scenario.fee_rate,
                "slippage_bps": float(scenario.slippage_bps),
            }
            execution_model_payload = _scenario_payload(scenario)
            scenario_result = {
                "scenario_id": scenario_id,
                "scenario_index": scenario_index,
                "scenario_type": scenario.type,
                "scenario_role": scenario.scenario_role,
                "scenario_role_source": scenario.scenario_role_source,
                "execution_model": execution_model_payload,
                "execution_model_hash": execution_model_payload["model_params_hash"],
                "model_params_hash": execution_model_payload["model_params_hash"],
                "cost_model": cost_model,
                "execution_calibration_gate": calibration_gate,
                "execution_timing_policy": manifest.execution_timing.as_dict(),
                "execution_reality_summary": execution_reality_summary,
                "train_execution_event_summary": base.get("train_execution_event_summary") or {},
                "validation_execution_event_summary": base.get("validation_execution_event_summary") or {},
                "final_holdout_execution_event_summary": base.get("final_holdout_execution_event_summary"),
                "execution_event_summary": base.get("validation_execution_event_summary") or {},
                "train_metrics": train_metrics,
                "validation_metrics": validation_metrics,
                "final_holdout_metrics": final_holdout_metrics,
                "metrics_schema_version": METRICS_SCHEMA_VERSION,
                "train_metrics_v2": train_metrics_v2,
                "validation_metrics_v2": validation_metrics_v2,
                "final_holdout_metrics_v2": final_holdout_metrics_v2,
                "walk_forward_metrics": walk_forward,
                "regime_gate_result": regime_gate.as_dict(),
                "market_regime_bucket_performance": base["validation_regime_performance"],
                "market_regime_coverage": base["validation_regime_coverage"],
                "train_market_regime_bucket_performance": base["train_regime_performance"],
                "train_market_regime_coverage": base["train_regime_coverage"],
                "final_holdout_market_regime_bucket_performance": base["final_holdout_regime_performance"],
                "final_holdout_market_regime_coverage": base["final_holdout_regime_coverage"],
                "allowed_live_regimes": list(regime_gate.allowed_live_regimes),
                "blocked_live_regimes": list(regime_gate.blocked_live_regimes),
                "regime_evidence": regime_gate.evidence,
                "parameter_stability": stability_payload,
                "walk_forward_gate_result": "PASS" if walk_forward and walk_forward["return_consistency_pass"] else None,
                "scenario_acceptance_gate_result": gate_result,
                "scenario_fail_reasons": fail_reasons,
                "train_execution_metadata": base.get("train_execution_metadata") or [],
                "validation_execution_metadata": base.get("validation_execution_metadata") or [],
                "final_holdout_execution_metadata": base.get("final_holdout_execution_metadata"),
            }
            candidate_payload = aggregates.setdefault(
                base["candidate_id"],
                {
                    "experiment_id": manifest.experiment_id,
                    "manifest_hash": manifest_hash,
                    "dataset_snapshot_id": manifest.dataset.snapshot_id,
                    "dataset_content_hash": dataset_hash,
                    "dataset_quality_hash": dataset_quality_hash,
                    "dataset_quality_gate_status": dataset_quality_status,
                    "dataset_quality_gate_reasons": dataset_quality_reasons,
                    "dataset_quality_report_hashes": {
                        split_name: report.content_hash
                        for split_name, report in sorted(quality_reports.items())
                    },
                    "top_of_book_quality_summary": top_of_book_quality_summary,
                    "execution_timing_policy": manifest.execution_timing.as_dict(),
                    "strategy_name": manifest.strategy_name,
                    "parameter_candidate_id": base["candidate_id"],
                    "parameter_values": params,
                    "scenario_policy": manifest.execution_model.scenario_policy,
                    "scenario_results": [],
                    "execution_model_source": manifest.execution_model.source,
                    "deployment_tier": manifest.deployment_tier,
                    "execution_calibration_required": manifest.execution_model.calibration_required,
                    "execution_calibration_strictness": manifest.execution_model.calibration_strictness,
                    "final_holdout_required_for_promotion": manifest.acceptance_gate.final_holdout_required_for_promotion,
                    "final_holdout_present": "final_holdout" in snapshots,
                    "walk_forward_required": manifest.acceptance_gate.walk_forward_required,
                    "regime_classifier_version": MARKET_REGIME_VERSION,
                    "warnings": [],
                    "repository_version": _repository_version(),
                },
            )
            candidate_payload["scenario_results"].append(scenario_result)
            candidate_payload["warnings"] = sorted(
                set(candidate_payload.get("warnings") or ())
                | set(base.get("warnings") or ())
                | set(dataset_warning_codes)
            )
            if candidate_payload.get("_primary_scenario_result") is None:
                candidate_payload["_primary_scenario_result"] = scenario_result

    rows: list[dict[str, Any]] = []
    for candidate_payload in aggregates.values():
        _apply_scenario_policy(manifest=manifest, candidate=candidate_payload)
        primary = candidate_payload.pop("_primary_scenario_result", None) or (
            candidate_payload["scenario_results"][0] if candidate_payload.get("scenario_results") else {}
        )
        candidate_payload.update(
            {
                "cost_model": primary.get("cost_model"),
                "execution_model": primary.get("execution_model"),
                "execution_calibration_gate": _combined_calibration_gate(candidate_payload.get("scenario_results") or []),
                "train_metrics": primary.get("train_metrics"),
                "validation_metrics": primary.get("validation_metrics"),
                "final_holdout_metrics": primary.get("final_holdout_metrics"),
                "metrics_schema_version": primary.get("metrics_schema_version"),
                "train_metrics_v2": primary.get("train_metrics_v2"),
                "validation_metrics_v2": primary.get("validation_metrics_v2"),
                "final_holdout_metrics_v2": primary.get("final_holdout_metrics_v2"),
                "walk_forward_metrics": primary.get("walk_forward_metrics"),
                "market_regime_bucket_performance": primary.get("market_regime_bucket_performance"),
                "market_regime_coverage": primary.get("market_regime_coverage"),
                "train_market_regime_bucket_performance": primary.get("train_market_regime_bucket_performance"),
                "train_market_regime_coverage": primary.get("train_market_regime_coverage"),
                "final_holdout_market_regime_bucket_performance": primary.get("final_holdout_market_regime_bucket_performance"),
                "final_holdout_market_regime_coverage": primary.get("final_holdout_market_regime_coverage"),
                "regime_gate_result": primary.get("regime_gate_result"),
                "allowed_live_regimes": list(primary.get("allowed_live_regimes") or []),
                "blocked_live_regimes": list(primary.get("blocked_live_regimes") or []),
                "regime_evidence": dict(primary.get("regime_evidence") or {}),
                "walk_forward_gate_result": primary.get("walk_forward_gate_result"),
                "parameter_stability": primary.get("parameter_stability"),
                "execution_timing_policy": manifest.execution_timing.as_dict(),
                "execution_reality_summary": primary.get("execution_reality_summary"),
                "execution_event_summary": primary.get("execution_event_summary"),
                "train_execution_event_summary": primary.get("train_execution_event_summary"),
                "validation_execution_event_summary": primary.get("validation_execution_event_summary"),
                "final_holdout_execution_event_summary": primary.get("final_holdout_execution_event_summary"),
            }
        )
        warning_reasons = _execution_calibration_warning_reasons(candidate_payload)
        candidate_payload["has_execution_calibration_warning"] = bool(warning_reasons)
        candidate_payload["execution_calibration_warning_reasons"] = warning_reasons
        if warning_reasons:
            candidate_payload["warnings"] = sorted(
                set(candidate_payload.get("warnings") or ()) | set(warning_reasons)
            )
        policy_result = validate_production_calibration_policy(
            candidate_payload,
            target=manifest.deployment_tier,
        )
        candidate_payload["production_calibration_policy_result"] = policy_result.as_dict()
        candidate_payload["production_calibration_policy_reasons"] = list(policy_result.reasons)
        candidate_payload["execution_calibration_policy_source"] = policy_result.policy_source
        if policy_result.artifact_hash is not None:
            candidate_payload["execution_calibration_artifact_hash"] = policy_result.artifact_hash
        if policy_result.artifact_hashes:
            candidate_payload["execution_calibration_artifact_hashes"] = list(policy_result.artifact_hashes)
        if policy_result.status == "FAIL":
            candidate_payload["acceptance_gate_result"] = "FAIL"
            candidate_payload["gate_fail_reasons"] = sorted(
                set(candidate_payload.get("gate_fail_reasons") or ()) | set(policy_result.reasons)
            )
        candidate_payload["candidate_profile_hash"] = sha256_prefixed(build_candidate_profile(candidate_payload))
        rows.append(candidate_payload)
    return sorted(rows, key=_candidate_rank_key)


def _apply_scenario_policy(*, manifest: ExperimentManifest, candidate: dict[str, Any]) -> None:
    policy = manifest.execution_model.scenario_policy
    scenario_results = list(candidate.get("scenario_results") or [])
    pass_results = [item for item in scenario_results if item.get("scenario_acceptance_gate_result") == "PASS"]
    fail_results = [item for item in scenario_results if item.get("scenario_acceptance_gate_result") != "PASS"]
    candidate["scenario_pass_count"] = len(pass_results)
    candidate["scenario_fail_count"] = len(fail_results)
    candidate["required_scenario_count"] = len(scenario_results)
    reasons: list[str] = []
    primary = pass_results[0] if pass_results else (scenario_results[0] if scenario_results else None)
    candidate["required_scenario_ids"] = [str(item.get("scenario_id")) for item in scenario_results]

    if not scenario_results:
        reasons.append("scenario_result_missing")
    elif policy == "legacy_cost_model_single_pass":
        if not pass_results:
            for item in fail_results:
                for reason in item.get("scenario_fail_reasons") or []:
                    reasons.append(str(reason))
            reasons.append("scenario_policy_no_passing_base_scenario")
    elif policy == "single_scenario":
        if len(scenario_results) != 1:
            reasons.append("scenario_policy_unsupported")
        elif not pass_results:
            for reason in scenario_results[0].get("scenario_fail_reasons") or []:
                reasons.append(str(reason))
            reasons.append("scenario_policy_required_scenario_failed")
    elif policy == "must_pass_base_and_survive_stress":
        base_results = [item for item in scenario_results if item.get("scenario_role") == "base"]
        stress_results = [item for item in scenario_results if item.get("scenario_role") == "stress"]
        if not any(item.get("scenario_acceptance_gate_result") == "PASS" for item in base_results):
            reasons.append("scenario_policy_no_passing_base_scenario")
        if not any(item.get("scenario_acceptance_gate_result") == "PASS" for item in stress_results):
            reasons.append("scenario_policy_no_passing_stress_scenario")
        for item in fail_results:
            for reason in item.get("scenario_fail_reasons") or []:
                reasons.append(str(reason))
            reasons.append(
                "scenario_policy_required_scenario_failed:"
                f"{item.get('scenario_id')}:{','.join(str(reason) for reason in item.get('scenario_fail_reasons') or [])}"
            )
        primary = base_results[0] if base_results else primary
    else:
        reasons.append("scenario_policy_unsupported")

    candidate["_primary_scenario_result"] = primary
    candidate["acceptance_gate_result"] = "PASS" if not reasons else "FAIL"
    candidate["gate_fail_reasons"] = reasons


def _combined_calibration_gate(scenario_results: list[dict[str, Any]]) -> dict[str, Any]:
    gates = [item.get("execution_calibration_gate") for item in scenario_results if isinstance(item.get("execution_calibration_gate"), dict)]
    reasons = sorted({str(reason) for gate in gates for reason in gate.get("reasons") or []})
    statuses = {str(gate.get("status")) for gate in gates}
    hashes = sorted(
        {
            str(gate.get("artifact_hash"))
            for gate in gates
            if isinstance(gate.get("artifact_hash"), str) and str(gate.get("artifact_hash")).startswith("sha256:")
        }
    )
    status = "PASS"
    if "FAIL" in statuses:
        status = "FAIL"
    elif "MISSING" in statuses:
        status = "MISSING"
    payload: dict[str, Any] = {
        "status": status,
        "reasons": reasons,
        "scenario_gates": gates,
    }
    if len(hashes) == 1:
        payload["artifact_hash"] = hashes[0]
    if hashes:
        payload["artifact_hashes"] = hashes
    return payload


def _gate_result(
    *,
    manifest: ExperimentManifest,
    validation_metrics: dict[str, Any],
    final_holdout_metrics: dict[str, Any] | None,
    walk_forward_metrics: dict[str, Any] | None,
    stability_score: float | None,
    include_walk_forward: bool,
    validation_metrics_v2: dict[str, Any] | None = None,
    final_holdout_metrics_v2: dict[str, Any] | None = None,
    regime_gate_result: dict[str, Any] | None = None,
    execution_calibration_gate: dict[str, Any] | None = None,
    dataset_quality_status: str = "PASS",
    dataset_quality_reasons: list[str] | None = None,
) -> tuple[str, list[str]]:
    gate = manifest.acceptance_gate
    reasons: list[str] = []
    if dataset_quality_status != "PASS":
        reasons.extend(dataset_quality_reasons or ["dataset_quality_failed"])
    if int(validation_metrics.get("trade_count") or 0) < gate.min_trade_count:
        reasons.append("min_trade_count_failed")
    if float(validation_metrics.get("max_drawdown_pct") or 0.0) > gate.max_mdd_pct:
        reasons.append("max_drawdown_failed")
    profit_factor = validation_metrics.get("profit_factor")
    if profit_factor is None or float(profit_factor) < gate.min_profit_factor:
        reasons.append("profit_factor_failed")
    if gate.oos_return_must_be_positive and float(validation_metrics.get("return_pct") or 0.0) <= 0.0:
        reasons.append("validation_return_not_positive")
    if final_holdout_metrics and gate.oos_return_must_be_positive and float(final_holdout_metrics.get("return_pct") or 0.0) <= 0.0:
        reasons.append("final_holdout_return_not_positive")
    reasons.extend(_metrics_v2_gate_reasons(gate=gate, metrics_v2=validation_metrics_v2, prefix=""))
    if final_holdout_metrics_v2 is not None:
        reasons.extend(_metrics_v2_gate_reasons(gate=gate, metrics_v2=final_holdout_metrics_v2, prefix="final_holdout_"))
    if gate.parameter_stability_required and (stability_score is None or stability_score < 0.5):
        reasons.append("parameter_stability_failed")
    if gate.walk_forward_required:
        if not include_walk_forward or not walk_forward_metrics:
            reasons.append("walk_forward_missing")
        elif not bool(walk_forward_metrics.get("return_consistency_pass")):
            reasons.append("walk_forward_failed")
    if gate.regime_acceptance_gate.required:
        if not isinstance(regime_gate_result, dict):
            reasons.append("regime_gate_missing")
        elif regime_gate_result.get("result") != "PASS":
            reasons.extend(str(reason) for reason in regime_gate_result.get("reasons") or ["regime_gate_failed"])
    if manifest.execution_model.calibration_required:
        if not isinstance(execution_calibration_gate, dict):
            reasons.append("execution_calibration_missing")
        elif execution_calibration_gate.get("status") != "PASS":
            reasons.extend(str(reason) for reason in execution_calibration_gate.get("reasons") or ["execution_calibration_failed"])
    elif (
        manifest.execution_model.calibration_strictness == "fail"
        and isinstance(execution_calibration_gate, dict)
        and execution_calibration_gate.get("status") == "FAIL"
    ):
        reasons.extend(str(reason) for reason in execution_calibration_gate.get("reasons") or ["execution_calibration_failed"])
    return ("PASS" if not reasons else "FAIL", reasons)


def _metrics_v2_gate_reasons(*, gate, metrics_v2: dict[str, Any] | None, prefix: str) -> list[str]:
    has_v2_gate = any(
        value is not None
        for value in (
            gate.min_cagr_pct,
            gate.min_expectancy_per_trade_krw,
            gate.min_expectancy_per_trade_pct,
            gate.max_exposure_time_pct,
            gate.max_avg_holding_time_minutes,
            gate.max_fee_drag_ratio,
            gate.max_slippage_drag_ratio,
        )
    ) or gate.reject_open_position_at_end or gate.metrics_contract_required
    if not has_v2_gate:
        return []
    if not isinstance(metrics_v2, dict):
        return [f"{prefix}metrics_v2_missing" if prefix else "metrics_v2_missing"]
    if int(metrics_v2.get("metrics_schema_version") or 0) != METRICS_SCHEMA_VERSION:
        return [f"{prefix}metrics_contract_missing" if prefix else "metrics_contract_missing"]
    return_risk = metrics_v2.get("return_risk") if isinstance(metrics_v2.get("return_risk"), dict) else {}
    trade_quality = metrics_v2.get("trade_quality") if isinstance(metrics_v2.get("trade_quality"), dict) else {}
    time_exposure = metrics_v2.get("time_exposure") if isinstance(metrics_v2.get("time_exposure"), dict) else {}
    cost_execution = metrics_v2.get("cost_execution") if isinstance(metrics_v2.get("cost_execution"), dict) else {}
    reasons: list[str] = []
    _append_min_reason(
        reasons,
        value=return_risk.get("cagr_pct"),
        threshold=gate.min_cagr_pct,
        missing_code=f"{prefix}metrics_v2_required_field_missing",
        failed_code=f"{prefix}min_cagr_failed",
    )
    _append_min_reason(
        reasons,
        value=trade_quality.get("expectancy_per_trade_krw"),
        threshold=gate.min_expectancy_per_trade_krw,
        missing_code=f"{prefix}metrics_v2_required_field_missing",
        failed_code=f"{prefix}min_expectancy_per_trade_krw_failed",
    )
    _append_min_reason(
        reasons,
        value=trade_quality.get("expectancy_per_trade_pct"),
        threshold=gate.min_expectancy_per_trade_pct,
        missing_code=f"{prefix}metrics_v2_required_field_missing",
        failed_code=f"{prefix}min_expectancy_per_trade_pct_failed",
    )
    _append_max_reason(
        reasons,
        value=time_exposure.get("exposure_time_pct"),
        threshold=gate.max_exposure_time_pct,
        missing_code=f"{prefix}metrics_v2_required_field_missing",
        failed_code=f"{prefix}max_exposure_time_failed",
    )
    avg_holding_ms = time_exposure.get("avg_holding_time_ms")
    avg_holding_minutes = (float(avg_holding_ms) / 60_000.0) if avg_holding_ms is not None else None
    _append_max_reason(
        reasons,
        value=avg_holding_minutes,
        threshold=gate.max_avg_holding_time_minutes,
        missing_code=f"{prefix}metrics_v2_required_field_missing",
        failed_code=f"{prefix}max_avg_holding_time_failed",
    )
    _append_max_reason(
        reasons,
        value=cost_execution.get("fee_drag_ratio"),
        threshold=gate.max_fee_drag_ratio,
        missing_code=f"{prefix}metrics_v2_required_field_missing",
        failed_code=f"{prefix}max_fee_drag_ratio_failed",
    )
    _append_max_reason(
        reasons,
        value=cost_execution.get("slippage_drag_ratio"),
        threshold=gate.max_slippage_drag_ratio,
        missing_code=f"{prefix}metrics_v2_required_field_missing",
        failed_code=f"{prefix}max_slippage_drag_ratio_failed",
    )
    if gate.reject_open_position_at_end and bool(return_risk.get("open_position_at_end")):
        reasons.append(f"{prefix}open_position_at_end_failed")
    return reasons


def _append_min_reason(
    reasons: list[str],
    *,
    value: Any,
    threshold: float | None,
    missing_code: str,
    failed_code: str,
) -> None:
    if threshold is None:
        return
    if value is None:
        reasons.append(missing_code)
        return
    if float(value) < float(threshold):
        reasons.append(failed_code)


def _append_max_reason(
    reasons: list[str],
    *,
    value: Any,
    threshold: float | None,
    missing_code: str,
    failed_code: str,
) -> None:
    if threshold is None:
        return
    if value is None:
        reasons.append(missing_code)
        return
    if float(value) > float(threshold):
        reasons.append(failed_code)


def _parameter_stability_scores(
    *,
    manifest: ExperimentManifest,
    candidates: list[dict[str, Any]],
    evaluated_candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for index, params in enumerate(candidates):
        neighbors = _neighbor_indices(manifest.parameter_space, candidates, params)
        acceptable = [
            neighbor_index
            for neighbor_index in neighbors
            if _validation_metrics_gate_compatible(manifest, evaluated_candidates[neighbor_index]["validation_metrics"])
        ]
        score = (len(acceptable) / len(neighbors)) if neighbors else None
        out.append(
            {
                "score": score,
                "neighbor_count": len(neighbors),
                "acceptable_neighbor_count": len(acceptable),
                "neighbor_candidate_ids": [evaluated_candidates[item]["candidate_id"] for item in neighbors],
                "acceptable_neighbor_candidate_ids": [
                    evaluated_candidates[item]["candidate_id"] for item in acceptable
                ],
                "method": "one_parameter_grid_step_validation_gate_compatible_neighbors",
            }
        )
    return out


def _neighbor_indices(
    parameter_space: dict[str, tuple[object, ...]],
    candidates: list[dict[str, Any]],
    params: dict[str, Any],
) -> list[int]:
    value_positions = {
        key: {value: position for position, value in enumerate(values)}
        for key, values in parameter_space.items()
    }
    neighbors: list[int] = []
    for index, other in enumerate(candidates):
        differing_steps = 0
        comparable = True
        for key in sorted(parameter_space):
            if other.get(key) == params.get(key):
                continue
            left = value_positions[key].get(params.get(key))
            right = value_positions[key].get(other.get(key))
            if left is None or right is None or abs(left - right) != 1:
                comparable = False
                break
            differing_steps += 1
        if comparable and differing_steps == 1:
            neighbors.append(index)
    return neighbors


def _validation_metrics_gate_compatible(manifest: ExperimentManifest, metrics: dict[str, Any]) -> bool:
    gate = manifest.acceptance_gate
    if int(metrics.get("trade_count") or 0) < gate.min_trade_count:
        return False
    if float(metrics.get("max_drawdown_pct") or 0.0) > gate.max_mdd_pct:
        return False
    profit_factor = metrics.get("profit_factor")
    if profit_factor is None or float(profit_factor) < gate.min_profit_factor:
        return False
    if gate.oos_return_must_be_positive and float(metrics.get("return_pct") or 0.0) <= 0.0:
        return False
    return True


def _walk_forward_metrics(
    *,
    manifest: ExperimentManifest,
    snapshots: dict[str, DatasetSnapshot],
    parameter_values: dict[str, Any],
    fee_rate: float,
    scenario: ExecutionScenario | None = None,
    slippage_bps: float | None = None,
    parameter_candidate_id: str | None = None,
    parameter_stability_score: float | None = None,
) -> dict[str, Any]:
    config = manifest.walk_forward
    if config is None:
        return {
            "window_count": 0,
            "pass_window_count": 0,
            "fail_window_count": 0,
            "return_consistency_pass": False,
            "failure_reason": "walk_forward_missing",
            "windows": [],
        }
    runner = resolve_research_strategy(manifest.strategy_name)
    active_scenario = scenario or ExecutionScenario(
        type="fixed_bps",
        fee_rate=float(fee_rate),
        slippage_bps=float(slippage_bps or 0.0),
        source="legacy_test_call",
    )
    windows: list[dict[str, Any]] = []
    for window_id in sorted({key.rsplit("_", 1)[0] for key in snapshots if key.startswith("window_")}):
        train_snapshot = snapshots[f"{window_id}_train"]
        test_snapshot = snapshots[f"{window_id}_test"]
        train = runner(
            train_snapshot,
            parameter_values,
            active_scenario.fee_rate,
            active_scenario.slippage_bps,
            parameter_stability_score,
            _execution_model_from_scenario(
                active_scenario,
                seed_context=_seed_context(
                    manifest_hash=manifest.manifest_hash(),
                    scenario=active_scenario,
                    scenario_id=_scenario_id(active_scenario, 0),
                    parameter_candidate_id=parameter_candidate_id or "unknown_candidate",
                    split_name=f"{window_id}_train",
                ),
            ),
            manifest.execution_timing,
        )
        test = runner(
            test_snapshot,
            parameter_values,
            active_scenario.fee_rate,
            active_scenario.slippage_bps,
            parameter_stability_score,
            _execution_model_from_scenario(
                active_scenario,
                seed_context=_seed_context(
                    manifest_hash=manifest.manifest_hash(),
                    scenario=active_scenario,
                    scenario_id=_scenario_id(active_scenario, 0),
                    parameter_candidate_id=parameter_candidate_id or "unknown_candidate",
                    split_name=f"{window_id}_test",
                ),
            ),
            manifest.execution_timing,
        )
        test_metrics = test.metrics.as_dict()
        pass_reasons: list[str] = []
        if not _validation_metrics_gate_compatible(manifest, test_metrics):
            pass_reasons.append("test_metrics_gate_incompatible")
        if manifest.acceptance_gate.oos_return_must_be_positive and float(test_metrics.get("return_pct") or 0.0) <= 0.0:
            pass_reasons.append("test_return_not_positive")
        windows.append(
            {
                "window_id": window_id,
                "train_date_range": train_snapshot.date_range.as_dict(),
                "test_date_range": test_snapshot.date_range.as_dict(),
                "train_candle_count": len(train_snapshot.candles),
                "test_candle_count": len(test_snapshot.candles),
                "train_metrics": train.metrics.as_dict(),
                "test_metrics": test_metrics,
                "train_metrics_v2": _metrics_v2_payload(train),
                "test_metrics_v2": _metrics_v2_payload(test),
                "train_market_regime_coverage": [row.as_dict() for row in train.regime_coverage],
                "test_market_regime_coverage": [row.as_dict() for row in test.regime_coverage],
                "test_market_regime_bucket_performance": [row.as_dict() for row in test.regime_performance],
                "trade_count_by_regime": {
                    str(row.regime): int(row.trade_count)
                    for row in test.regime_coverage
                    if row.dimension == "composite_regime"
                },
                "candle_count_by_regime": {
                    str(row.regime): int(row.candle_count)
                    for row in test.regime_coverage
                    if row.dimension == "composite_regime"
                },
                "worst_regime_profit_factor": _worst_regime_metric(test.regime_performance, "profit_factor"),
                "worst_regime_net_pnl": _worst_regime_metric(test.regime_performance, "net_pnl"),
                "gate_result": "PASS" if not pass_reasons else "FAIL",
                "fail_reasons": pass_reasons,
            }
        )
    test_returns = [float(window["test_metrics"].get("return_pct") or 0.0) for window in windows]
    pass_count = sum(1 for window in windows if window["gate_result"] == "PASS")
    failure_reason = None
    if len(windows) < config.min_windows:
        failure_reason = "walk_forward_insufficient_windows"
    elif pass_count != len(windows):
        failure_reason = "walk_forward_failed"
    return {
        "window_count": len(windows),
        "pass_window_count": pass_count,
        "fail_window_count": len(windows) - pass_count,
        "mean_test_return_pct": (sum(test_returns) / len(test_returns)) if test_returns else None,
        "median_test_return_pct": median(test_returns) if test_returns else None,
        "worst_test_return_pct": min(test_returns) if test_returns else None,
        "return_consistency_pass": failure_reason is None,
        "failure_reason": failure_reason,
        "windows": windows,
    }


def _worst_regime_metric(rows: Any, key: str) -> float | None:
    values = [
        getattr(row, key)
        for row in rows
        if getattr(row, "dimension", "") == "composite_regime" and getattr(row, key) is not None
    ]
    return min(float(value) for value in values) if values else None


def _metrics_v2_payload(run: BacktestRun | None) -> dict[str, Any] | None:
    if run is None or run.metrics_v2 is None:
        return None
    return run.metrics_v2.as_dict()


def _report_payload(
    *,
    manifest: ExperimentManifest,
    snapshots: tuple[DatasetSnapshot, ...],
    quality_reports: tuple[DatasetQualityReport, ...],
    candidates: list[dict[str, Any]],
    report_kind: str,
    generated_at: str | None,
    manifest_path: str | None = None,
    command_name: str | None = None,
    command_args: dict[str, Any] | None = None,
    execution_calibration: dict[str, Any] | None = None,
) -> dict[str, Any]:
    best = next((candidate for candidate in candidates if candidate["acceptance_gate_result"] == "PASS"), None)
    warnings = sorted({warning for candidate in candidates for warning in candidate.get("warnings", [])})
    dataset_hash = combined_dataset_fingerprint(snapshots)
    dataset_quality_hash = combined_dataset_quality_hash(quality_reports)
    dataset_quality_status, dataset_quality_reasons = _combined_dataset_quality_gate(
        {report.payload["split_name"]: report for report in quality_reports}
    )
    top_of_book_quality_summary = _top_of_book_quality_summary(
        {str(report.payload["split_name"]): report for report in quality_reports}
    )
    top_of_book_requested = manifest.dataset.top_of_book is not None
    top_of_book_joined_count = sum(
        int(report.payload.get("top_of_book_joined_count") or 0)
        for report in quality_reports
    )
    repository_version = _repository_version()
    calibration_hash = (
        str(execution_calibration.get("content_hash"))
        if isinstance(execution_calibration, dict) and execution_calibration.get("content_hash")
        else None
    )
    parameter_grid_size = 1
    for values in manifest.parameter_space.values():
        parameter_grid_size *= len(values)
    failed_count = sum(1 for candidate in candidates if candidate.get("acceptance_gate_result") != "PASS")
    lineage = build_research_lineage(
        experiment_id=manifest.experiment_id,
        experiment_family_id=str(manifest.raw.get("experiment_family_id") or manifest.experiment_id),
        hypothesis_id=manifest.raw.get("hypothesis_id"),
        hypothesis_status=manifest.raw.get("hypothesis_status") or "pre_registered",
        pre_registered_at=manifest.raw.get("pre_registered_at"),
        manifest_path=manifest_path,
        manifest_hash=manifest.manifest_hash(),
        manifest_canonical_hash=manifest.manifest_hash(),
        dataset_snapshot_id=manifest.dataset.snapshot_id,
        dataset_content_hash=dataset_hash,
        dataset_quality_hash=dataset_quality_hash,
        dataset_split_hash=sha256_prefixed({
            snapshot.split_name: snapshot.date_range.as_dict()
            for snapshot in snapshots
        }),
        data_source_fingerprint=sha256_prefixed({
            "source": manifest.dataset.source,
            "market": manifest.market,
            "interval": manifest.interval,
            "snapshot_id": manifest.dataset.snapshot_id,
        }),
        repository_version=repository_version,
        command_name=command_name or f"research-{report_kind}",
        command_args=command_args or {},
        cost_execution_model_hash=sha256_prefixed(manifest.execution_model.as_dict()),
        execution_calibration_artifact_hash=calibration_hash,
        search_budget=parameter_grid_size,
        parameter_grid_size=parameter_grid_size,
        attempt_index=int(manifest.raw.get("attempt_index") or 1),
        failed_candidate_count=failed_count,
        holdout_reuse_count=int(manifest.raw.get("holdout_reuse_count") or 0),
        dataset_reuse_policy=str(manifest.raw.get("dataset_reuse_policy") or "single_final_holdout_for_experiment_family"),
        created_at=generated_at,
    )
    return {
        "report_kind": report_kind,
        "experiment_id": manifest.experiment_id,
        "hypothesis": manifest.hypothesis,
        "manifest_hash": manifest.manifest_hash(),
        "dataset_snapshot_id": manifest.dataset.snapshot_id,
        "dataset_content_hash": dataset_hash,
        "dataset_quality_hash": dataset_quality_hash,
        "dataset_quality_gate_status": dataset_quality_status,
        "dataset_quality_gate_reasons": dataset_quality_reasons,
        "dataset_quality_reports": {
            str(report.payload["split_name"]): report.payload
            for report in quality_reports
        },
        "market": manifest.market,
        "interval": manifest.interval,
        "dataset_splits": {
            snapshot.split_name: {
                "date_range": snapshot.date_range.as_dict(),
                "candle_count": len(snapshot.candles),
                "content_hash": snapshot.content_hash(),
                "quality_hash": next(
                    report.content_hash for report in quality_reports if report.payload["split_name"] == snapshot.split_name
                ),
            }
            for snapshot in snapshots
        },
        "data_limitations": {
            "candle_only": not top_of_book_requested,
            "top_of_book_requested": top_of_book_requested,
            "top_of_book_required": bool(manifest.dataset.top_of_book.required) if manifest.dataset.top_of_book else False,
            "top_of_book_available": top_of_book_joined_count > 0,
            "top_of_book_is_full_depth": False,
            "orderbook_depth_available": False,
            "intra_candle_path_available": False,
            "execution_reference_price": manifest.execution_timing.fill_reference_policy,
            "intra_candle_policy": _policy_intra_candle_limitation(manifest.execution_timing.fill_reference_policy),
            "portfolio_event_time_policy": "fills_apply_when_fill_reference_ts_reaches_mark_or_decision_boundary",
            "top_of_book_join_tolerance_ms": (
                manifest.dataset.top_of_book.join_tolerance_ms if manifest.dataset.top_of_book else None
            ),
        },
        "top_of_book_quality_summary": top_of_book_quality_summary,
        "execution_timing_policy": manifest.execution_timing.as_dict(),
        "execution_reality_level": _report_execution_reality_level(candidates),
        "execution_reality_gate_status": _report_execution_reality_gate_status(candidates),
        "execution_reality_gate_reasons": _report_execution_reality_gate_reasons(candidates),
        "signal_quote_coverage_summary": _report_signal_quote_coverage_summary(candidates),
        "execution_event_summary": _report_execution_event_summary(candidates),
        "strategy_name": manifest.strategy_name,
        "regime_classifier_version": MARKET_REGIME_VERSION,
        "regime_acceptance_gate": manifest.acceptance_gate.regime_acceptance_gate.as_dict(),
        "execution_model": manifest.execution_model.as_dict(),
        "execution_model_source": manifest.execution_model.source,
        "metrics_schema_version": METRICS_SCHEMA_VERSION,
        "deployment_tier": manifest.deployment_tier,
        "execution_calibration_required": manifest.execution_model.calibration_required,
        "market_regime_bucket_performance": (
            best.get("market_regime_bucket_performance") if best else None
        ),
        "market_regime_coverage": best.get("market_regime_coverage") if best else None,
        "walk_forward_regime_coverage": (
            best.get("walk_forward_metrics", {}).get("windows") if best and isinstance(best.get("walk_forward_metrics"), dict) else None
        ),
        "regime_gate_result": best.get("regime_gate_result") if best else None,
        "allowed_live_regimes": best.get("allowed_live_regimes") if best else None,
        "blocked_live_regimes": best.get("blocked_live_regimes") if best else None,
        "candidate_count": len(candidates),
        "experiment_family_id": lineage.get("experiment_family_id"),
        "hypothesis_id": lineage.get("hypothesis_id"),
        "hypothesis_status": lineage.get("hypothesis_status"),
        "pre_registered_gate": bool(lineage.get("pre_registered_at") or lineage.get("hypothesis_status")),
        "search_budget": lineage.get("search_budget"),
        "parameter_space_hash": sha256_prefixed(manifest.parameter_space),
        "parameter_grid_size": lineage.get("parameter_grid_size"),
        "attempt_index": lineage.get("attempt_index"),
        "failed_candidate_count": lineage.get("failed_candidate_count"),
        "holdout_reuse_count": lineage.get("holdout_reuse_count"),
        "dataset_reuse_policy": lineage.get("dataset_reuse_policy"),
        "best_candidate_id": best.get("parameter_candidate_id") if best else None,
        "best_validation_metrics_v2": best.get("validation_metrics_v2") if best else None,
        "best_final_holdout_metrics_v2": best.get("final_holdout_metrics_v2") if best else None,
        "gate_result": "PASS" if best else "FAIL",
        "warnings": warnings,
        "candidates": candidates,
        "repository_version": repository_version,
        "lineage": lineage,
        "lineage_hash": lineage["lineage_hash"],
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
    }


def _execution_model_from_scenario(scenario: ExecutionScenario, *, seed_context: dict[str, Any] | None = None):
    if scenario.type == "fixed_bps":
        return FixedBpsExecutionModel(fee_rate=scenario.fee_rate, slippage_bps=scenario.slippage_bps)
    if scenario.type == "stress":
        return StressExecutionModel(
            fee_rate=scenario.fee_rate,
            slippage_bps=scenario.slippage_bps,
            latency_ms=scenario.latency_ms,
            partial_fill_rate=scenario.partial_fill_rate,
            order_failure_rate=scenario.order_failure_rate,
            market_order_extra_cost_bps=scenario.market_order_extra_cost_bps,
            seed=scenario.seed,
            seed_derivation_inputs=seed_context,
        )
    raise ResearchValidationError(f"unsupported execution model scenario: {scenario.type}")


def _scenario_payload(scenario: ExecutionScenario) -> dict[str, Any]:
    payload = scenario.as_dict()
    payload["model_params_hash"] = model_params_hash(_execution_model_from_scenario(scenario).params_payload())
    return payload


def _scenario_id(scenario: ExecutionScenario, scenario_index: int) -> str:
    digest = model_params_hash(_execution_model_from_scenario(scenario).params_payload()).split(":", 1)[-1][:8]
    return f"scenario_{scenario_index + 1:03d}_{scenario.type}_{digest}"


def _seed_context(
    *,
    manifest_hash: str,
    scenario: ExecutionScenario,
    scenario_id: str,
    parameter_candidate_id: str,
    split_name: str,
) -> dict[str, Any]:
    scenario_hash = model_params_hash(_execution_model_from_scenario(scenario).params_payload())
    material = {
        "manifest_hash": manifest_hash,
        "scenario_id": scenario_id,
        "scenario_hash": scenario_hash,
        "parameter_candidate_id": parameter_candidate_id,
        "split_name": split_name,
        "base_seed": scenario.seed,
    }
    material["stress_seed_material"] = dict(material)
    material["stress_seed_hash"] = sha256_prefixed(material)
    return material


def _execution_metadata(trades: Any) -> list[dict[str, Any]]:
    metadata: list[dict[str, Any]] = []
    for trade in trades:
        if isinstance(trade, dict) and isinstance(trade.get("execution"), dict):
            item = dict(trade["execution"])
            for key in (
                "record_type",
                "is_execution_attempt",
                "is_filled_trade",
                "is_execution_filled",
                "is_portfolio_applied_trade",
                "is_effective_trade",
                "is_skipped_execution",
                "is_failed_execution",
                "portfolio_effective_ts",
                "portfolio_applied",
                "portfolio_application_status",
                "pending_execution_at_end",
                "pending_execution_after_dataset_end",
                "dataset_final_mark_ts",
            ):
                if key in trade:
                    item[key] = trade[key]
            metadata.append(item)
    return metadata


def _execution_reality_summary(
    *,
    policy,
    execution_metadata: list[dict[str, Any]],
    execution_event_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    coverage = signal_quote_coverage_summary(execution_metadata=execution_metadata, policy=policy)
    observed_levels = [
        str(item.get("execution_reality_level"))
        for item in execution_metadata
        if item.get("execution_reality_level")
    ]
    sources = [
        str(item.get("fill_reference_source"))
        for item in execution_metadata
        if item.get("fill_reference_source")
    ]
    gate = execution_reality_gate(
        policy=policy,
        observed_levels=observed_levels,
        fill_reference_sources=sources,
        quote_coverage_pct=coverage.get("quote_after_decision_coverage_pct"),
        latency_reference_warnings=[
            str(item.get("latency_reference_policy_warning"))
            for item in execution_metadata
            if item.get("latency_reference_policy_warning")
        ],
    )
    event_summary = execution_event_summary or _execution_event_summary_from_metadata(execution_metadata)
    return {
        **coverage,
        **event_summary,
        "execution_reality_gate_status": gate["status"],
        "execution_reality_gate_reasons": gate["reasons"],
        "execution_reality_gate": gate,
    }


def _execution_event_summary_from_metadata(execution_metadata: list[dict[str, Any]]) -> dict[str, object]:
    filled = [item for item in execution_metadata if bool(item.get("is_execution_filled"))]
    portfolio_applied = [item for item in execution_metadata if bool(item.get("is_portfolio_applied_trade"))]
    pending = [
        item
        for item in execution_metadata
        if bool(item.get("is_execution_filled")) and not bool(item.get("is_portfolio_applied_trade"))
    ]
    skipped = [item for item in execution_metadata if bool(item.get("is_skipped_execution"))]
    failed = [item for item in execution_metadata if bool(item.get("is_failed_execution"))]
    closed = [item for item in portfolio_applied if str(item.get("side") or "").upper() == "SELL"]
    pending_at_end = [item for item in pending if bool(item.get("pending_execution_at_end"))]
    pending_after_end = [item for item in pending if bool(item.get("pending_execution_after_dataset_end"))]
    return {
        "execution_attempt_count": len(execution_metadata),
        "execution_filled_count": len(filled),
        "filled_execution_count": len(filled),
        "portfolio_applied_trade_count": len(portfolio_applied),
        "pending_execution_count": len(pending),
        "skipped_execution_count": len(skipped),
        "failed_execution_count": len(failed),
        "closed_trade_count": len(closed),
        "pending_execution_at_end_count": len(pending_at_end),
        "pending_execution_after_dataset_end_count": len(pending_after_end),
        "execution_event_timeline_incomplete": bool(pending_after_end),
    }


def _execution_event_gate_reasons(summary: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if bool(summary.get("execution_event_timeline_incomplete")):
        reasons.append("execution_event_timeline_incomplete")
    if int(summary.get("pending_execution_after_dataset_end_count") or 0) > 0:
        reasons.append("pending_execution_after_dataset_end")
    return reasons


def _report_execution_reality_level(candidates: list[dict[str, Any]]) -> str | None:
    for candidate in candidates:
        summary = candidate.get("execution_reality_summary")
        if isinstance(summary, dict) and summary.get("execution_reality_level"):
            return str(summary["execution_reality_level"])
    return None


def _report_execution_reality_gate_status(candidates: list[dict[str, Any]]) -> str:
    statuses = {
        str(summary.get("execution_reality_gate_status"))
        for candidate in candidates
        if isinstance((summary := candidate.get("execution_reality_summary")), dict)
    }
    if "FAIL" in statuses:
        return "FAIL"
    if "PASS" in statuses:
        return "PASS"
    return "UNKNOWN"


def _report_execution_reality_gate_reasons(candidates: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            str(reason)
            for candidate in candidates
            if isinstance((summary := candidate.get("execution_reality_summary")), dict)
            for reason in summary.get("execution_reality_gate_reasons") or []
        }
    )


def _report_signal_quote_coverage_summary(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    for candidate in candidates:
        summary = candidate.get("execution_reality_summary")
        if isinstance(summary, dict):
            return {
                key: summary.get(key)
                for key in (
                    "signal_event_count",
                    "fillable_signal_event_count",
                    "missing_quote_on_signal_count",
                    "skipped_execution_signal_count",
                    "missing_quote_warning_count",
                    "quote_after_decision_coverage_pct",
                    "median_quote_age_ms_on_signal",
                    "p95_quote_age_ms_on_signal",
                    "execution_reference_policy",
                    "execution_reality_level",
                    "latency_applied_to_submit_ts_count",
                    "latency_applied_to_fill_reference_count",
                    "execution_attempt_count",
                    "execution_filled_count",
                    "filled_execution_count",
                    "portfolio_applied_trade_count",
                    "pending_execution_count",
                    "skipped_execution_count",
                    "failed_execution_count",
                    "closed_trade_count",
                    "pending_execution_at_end_count",
                    "pending_execution_after_dataset_end_count",
                    "execution_event_timeline_incomplete",
                )
            }
    return None


def _report_execution_event_summary(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    for candidate in candidates:
        summary = candidate.get("execution_event_summary")
        if isinstance(summary, dict):
            return dict(summary)
        reality_summary = candidate.get("execution_reality_summary")
        if isinstance(reality_summary, dict):
            return {
                key: reality_summary.get(key)
                for key in (
                    "execution_attempt_count",
                    "execution_filled_count",
                    "filled_execution_count",
                    "portfolio_applied_trade_count",
                    "pending_execution_count",
                    "skipped_execution_count",
                    "failed_execution_count",
                    "closed_trade_count",
                    "pending_execution_at_end_count",
                    "pending_execution_after_dataset_end_count",
                    "execution_event_timeline_incomplete",
                )
            }
    return None


def _policy_intra_candle_limitation(fill_reference_policy: str) -> str:
    if fill_reference_policy == "next_candle_open":
        return "next_candle_open_no_intracandle_path"
    if fill_reference_policy in {"first_orderbook_after_decision", "latency_adjusted_orderbook"}:
        return "top_of_book_snapshot_no_depth_no_queue"
    return "same_candle_close_legacy_no_intracandle_path"


def _execution_calibration_warning_reasons(candidate: dict[str, Any]) -> list[str]:
    if candidate.get("execution_calibration_required"):
        return []
    if candidate.get("execution_calibration_strictness") != "warn":
        return []
    gate = candidate.get("execution_calibration_gate")
    if not isinstance(gate, dict) or gate.get("status") == "PASS":
        return []
    return [str(reason) for reason in gate.get("reasons") or ["execution_calibration_failed"]]


def _candidate_rank_key(candidate: dict[str, Any]) -> tuple[int, int, float, float, int, float, float, float, float]:
    passed = 0 if candidate.get("acceptance_gate_result") == "PASS" else 1
    validation = candidate.get("validation_metrics") or {}
    metrics_v2 = candidate.get("validation_metrics_v2") if isinstance(candidate.get("validation_metrics_v2"), dict) else {}
    return_risk = metrics_v2.get("return_risk") if isinstance(metrics_v2.get("return_risk"), dict) else {}
    trade_quality = metrics_v2.get("trade_quality") if isinstance(metrics_v2.get("trade_quality"), dict) else {}
    cost_execution = metrics_v2.get("cost_execution") if isinstance(metrics_v2.get("cost_execution"), dict) else {}
    open_position_rank = 1 if bool(return_risk.get("open_position_at_end")) else 0
    expectancy = trade_quality.get("expectancy_per_trade_krw")
    fee_drag = cost_execution.get("fee_drag_ratio")
    slippage_drag = cost_execution.get("slippage_drag_ratio")
    cagr = return_risk.get("cagr_pct")
    dependency = trade_quality.get("single_trade_dependency_score")
    return (
        passed,
        open_position_rank,
        float(validation.get("max_drawdown_pct") or 0.0),
        -float(expectancy) if expectancy is not None else 0.0,
        -int(validation.get("trade_count") or 0),
        float(fee_drag) if fee_drag is not None else 0.0,
        float(slippage_drag) if slippage_drag is not None else 0.0,
        -float(cagr) if cagr is not None else -float(validation.get("return_pct") or 0.0),
        float(dependency) if dependency is not None else 0.0,
    )


def _path_payload(paths: ResearchReportPaths) -> dict[str, str]:
    return {
        "derived_path": str(paths.derived_path),
        "report_path": str(paths.report_path),
    }


def _require_enough_candles(snapshots: Any) -> None:
    for snapshot in snapshots:
        if len(snapshot.candles) == 0:
            raise ResearchValidationError(f"dataset split {snapshot.split_name} has no candles")


def _quality_reports(
    *,
    db_path: str | Path,
    snapshots: dict[str, DatasetSnapshot],
) -> dict[str, DatasetQualityReport]:
    return {
        split_name: build_dataset_quality_report(db_path=db_path, snapshot=snapshot)
        for split_name, snapshot in snapshots.items()
    }


def _validate_strategy_data_requirements(manifest: ExperimentManifest) -> None:
    requirements = research_strategy_data_requirements(manifest.strategy_name)
    if "top_of_book" in requirements.required_data and manifest.dataset.top_of_book is None:
        raise ResearchValidationError("research_data_requirement_top_of_book_missing")


def _combined_dataset_quality_gate(
    reports: dict[str, DatasetQualityReport],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    for split_name, report in sorted(reports.items()):
        if report.quality_gate_status != "PASS":
            for reason in report.quality_gate_reasons or ("dataset_quality_failed",):
                reasons.append(f"dataset_quality_{split_name}_{reason}")
    return ("PASS" if not reasons else "FAIL", reasons)


def _dataset_quality_warning_codes(reports: dict[str, DatasetQualityReport]) -> list[str]:
    summary = _top_of_book_quality_summary(reports)
    if summary.get("gate_status") == "WARN":
        return [TOP_OF_BOOK_OPTIONAL_COVERAGE_WARNING]
    return []


def _top_of_book_quality_summary(reports: dict[str, DatasetQualityReport]) -> dict[str, Any]:
    requested_reports = [
        (split_name, report.payload)
        for split_name, report in sorted(reports.items())
        if bool(report.payload.get("top_of_book_requested"))
    ]
    if not requested_reports:
        return {
            "requested": False,
            "required": False,
            "gate_status": "NOT_REQUESTED",
            "joined_quote_count": 0,
            "missing_quote_count": 0,
            "expected_signal_count": 0,
            "coverage_pct": None,
            "affected_splits": [],
            "next_action": None,
            "limitations": [
                "top_of_book_not_requested",
                "orderbook_depth_unavailable",
                "intra_candle_path_unavailable",
            ],
        }

    expected = sum(int(payload.get("top_of_book_expected_signal_count") or 0) for _, payload in requested_reports)
    joined = sum(int(payload.get("top_of_book_joined_count") or 0) for _, payload in requested_reports)
    missing = sum(int(payload.get("top_of_book_missing_count") or 0) for _, payload in requested_reports)
    statuses = [str(payload.get("top_of_book_gate_status") or "UNKNOWN") for _, payload in requested_reports]
    gate_status = "PASS"
    if "FAIL" in statuses:
        gate_status = "FAIL"
    elif "WARN" in statuses:
        gate_status = "WARN"
    elif any(status != "PASS" for status in statuses):
        gate_status = "UNKNOWN"
    affected_splits = [
        {
            "split_name": str(split_name),
            "top_of_book_gate_status": str(payload.get("top_of_book_gate_status") or "UNKNOWN"),
            "top_of_book_coverage_pct": payload.get("top_of_book_coverage_pct"),
            "top_of_book_missing_count": int(payload.get("top_of_book_missing_count") or 0),
            "top_of_book_joined_count": int(payload.get("top_of_book_joined_count") or 0),
            "top_of_book_required": bool(payload.get("top_of_book_required")),
            "top_of_book_gate_reasons": [str(item) for item in payload.get("top_of_book_gate_reasons") or []],
        }
        for split_name, payload in requested_reports
        if str(payload.get("top_of_book_gate_status") or "UNKNOWN") != "PASS"
        or int(payload.get("top_of_book_missing_count") or 0) > 0
    ]
    coverage_pct = round((joined / expected * 100.0), 8) if expected else 0.0
    required = any(bool(payload.get("top_of_book_required")) for _, payload in requested_reports)
    join_tolerances = sorted(
        {
            int(payload.get("top_of_book_join_tolerance_ms"))
            for _, payload in requested_reports
            if payload.get("top_of_book_join_tolerance_ms") is not None
        }
    )
    sources = sorted(
        {
            str(payload.get("top_of_book_source"))
            for _, payload in requested_reports
            if payload.get("top_of_book_source")
        }
    )
    return {
        "requested": True,
        "required": required,
        "fail_closed": gate_status == "FAIL",
        "gate_status": gate_status,
        "joined_quote_count": joined,
        "missing_quote_count": missing,
        "expected_signal_count": expected,
        "coverage_pct": coverage_pct,
        "join_tolerance_ms": join_tolerances[0] if len(join_tolerances) == 1 else join_tolerances,
        "sources": sources,
        "affected_splits": affected_splits,
        "warning_code": TOP_OF_BOOK_OPTIONAL_COVERAGE_WARNING if gate_status == "WARN" else None,
        "next_action": TOP_OF_BOOK_OPERATOR_NEXT_ACTION if gate_status in {"WARN", "FAIL"} else None,
        "limitations": [
            "top_of_book_is_best_bid_ask_only_not_full_depth",
            "queue_position_unavailable",
            "market_impact_unavailable",
            "trade_ticks_unavailable",
            "intra_candle_path_unavailable",
            "execution_reference_requires_execution_timing_policy",
        ],
    }


def _rolling_walk_forward_windows(manifest: ExperimentManifest) -> list[dict[str, DateRange]]:
    config = manifest.walk_forward
    if config is None:
        return []
    start = _parse_manifest_day(manifest.dataset.split.train.start)
    end = _parse_manifest_day(
        manifest.dataset.split.final_holdout.end
        if manifest.dataset.split.final_holdout is not None
        else manifest.dataset.split.validation.end
    )
    windows: list[dict[str, DateRange]] = []
    cursor = start
    while True:
        train_start = cursor
        train_end = train_start + timedelta(days=config.train_window_days - 1)
        test_start = train_end + timedelta(days=1)
        test_end = test_start + timedelta(days=config.test_window_days - 1)
        if test_end > end:
            break
        windows.append(
            {
                "train": DateRange(start=train_start.strftime("%Y-%m-%d"), end=train_end.strftime("%Y-%m-%d")),
                "test": DateRange(start=test_start.strftime("%Y-%m-%d"), end=test_end.strftime("%Y-%m-%d")),
            }
        )
        cursor = cursor + timedelta(days=config.step_days)
    return windows


def _load_walk_forward_snapshots(
    *,
    db_path: str | Path,
    manifest: ExperimentManifest,
    windows: list[dict[str, DateRange]],
) -> dict[str, DatasetSnapshot]:
    snapshots = {
        "train": load_dataset_split(db_path=db_path, manifest=manifest, split_name="train"),
        "validation": load_dataset_split(db_path=db_path, manifest=manifest, split_name="validation"),
    }
    if manifest.dataset.split.final_holdout is not None:
        snapshots["final_holdout"] = load_dataset_split(
            db_path=db_path,
            manifest=manifest,
            split_name="final_holdout",
        )
    for index, window in enumerate(windows, start=1):
        window_id = f"window_{index:03d}"
        snapshots[f"{window_id}_train"] = load_dataset_range(
            db_path=db_path,
            manifest=manifest,
            split_name=f"{window_id}_train",
            date_range=window["train"],
        )
        snapshots[f"{window_id}_test"] = load_dataset_range(
            db_path=db_path,
            manifest=manifest,
            split_name=f"{window_id}_test",
            date_range=window["test"],
        )
    return snapshots


def _parse_manifest_day(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _repository_version() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[3],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"
