from __future__ import annotations

import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

from bithumb_bot.paths import PathManager
from bithumb_bot.market_regime import MARKET_REGIME_VERSION, evaluate_regime_acceptance_gate

from .dataset_snapshot import DatasetSnapshot, combined_dataset_fingerprint, load_dataset_range, load_dataset_split
from .experiment_manifest import DateRange, ExperimentManifest
from .hashing import sha256_prefixed
from .parameter_space import candidate_id, iter_parameter_candidates
from .promotion_gate import build_candidate_profile
from .report_writer import ResearchReportPaths, write_research_report
from .strategy_registry import resolve_research_strategy


class ResearchValidationError(ValueError):
    pass


def run_research_backtest(
    *,
    manifest: ExperimentManifest,
    db_path: str | Path,
    manager: PathManager,
    generated_at: str | None = None,
) -> dict[str, Any]:
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
    _require_enough_candles(snapshots.values())

    candidates = _evaluate_candidates(manifest=manifest, snapshots=snapshots, include_walk_forward=False)
    report = _report_payload(
        manifest=manifest,
        snapshots=tuple(snapshots.values()),
        candidates=candidates,
        report_kind="backtest",
        generated_at=generated_at,
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
) -> dict[str, Any]:
    if manifest.walk_forward is None:
        raise ResearchValidationError("walk_forward_missing")
    windows = _rolling_walk_forward_windows(manifest)
    if len(windows) < manifest.walk_forward.min_windows:
        raise ResearchValidationError(
            f"walk_forward_insufficient_windows: available={len(windows)} min_windows={manifest.walk_forward.min_windows}"
        )
    snapshots = _load_walk_forward_snapshots(db_path=db_path, manifest=manifest, windows=windows)
    _require_enough_candles(snapshots.values())
    candidates = _evaluate_candidates(manifest=manifest, snapshots=snapshots, include_walk_forward=True)
    report = _report_payload(
        manifest=manifest,
        snapshots=tuple(snapshots.values()),
        candidates=candidates,
        report_kind="walk_forward",
        generated_at=generated_at,
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
    include_walk_forward: bool,
) -> list[dict[str, Any]]:
    raw_candidates = iter_parameter_candidates(manifest.parameter_space)
    rows: list[dict[str, Any]] = []
    manifest_hash = manifest.manifest_hash()
    dataset_hash = combined_dataset_fingerprint(tuple(snapshots.values()))
    runner = resolve_research_strategy(manifest.strategy_name)

    for slippage_bps in manifest.cost_model.slippage_bps:
        base_results: list[dict[str, Any]] = []
        for index, params in enumerate(raw_candidates):
            train = runner(
                dataset=snapshots["train"],
                parameter_values=params,
                fee_rate=manifest.cost_model.fee_rate,
                slippage_bps=float(slippage_bps),
                parameter_stability_score=None,
            )
            validation = runner(
                dataset=snapshots["validation"],
                parameter_values=params,
                fee_rate=manifest.cost_model.fee_rate,
                slippage_bps=float(slippage_bps),
                parameter_stability_score=None,
            )
            final_holdout = (
                runner(
                    dataset=snapshots["final_holdout"],
                    parameter_values=params,
                    fee_rate=manifest.cost_model.fee_rate,
                    slippage_bps=float(slippage_bps),
                    parameter_stability_score=None,
                )
                if "final_holdout" in snapshots
                else None
            )
            walk_forward = (
                _walk_forward_metrics(
                    manifest=manifest,
                    snapshots=snapshots,
                    parameter_values=params,
                    fee_rate=manifest.cost_model.fee_rate,
                    slippage_bps=float(slippage_bps),
                    parameter_stability_score=None,
                )
                if include_walk_forward
                else None
            )
            base_results.append(
                {
                    "index": index,
                    "candidate_id": candidate_id(params, index),
                    "parameter_values": params,
                    "train_metrics": train.metrics.as_dict(),
                    "validation_metrics": validation.metrics.as_dict(),
                    "final_holdout_metrics": final_holdout.metrics.as_dict() if final_holdout else None,
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
            final_holdout_metrics = (
                dict(base["final_holdout_metrics"]) if isinstance(base.get("final_holdout_metrics"), dict) else None
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
                final_holdout_metrics=final_holdout_metrics,
                walk_forward_metrics=walk_forward,
                stability_score=stability_score,
                include_walk_forward=include_walk_forward,
                regime_gate_result=regime_gate.as_dict(),
            )
            cost_model = {
                "fee_rate": manifest.cost_model.fee_rate,
                "slippage_bps": float(slippage_bps),
            }
            candidate_payload = {
                "experiment_id": manifest.experiment_id,
                "manifest_hash": manifest_hash,
                "dataset_snapshot_id": manifest.dataset.snapshot_id,
                "dataset_content_hash": dataset_hash,
                "strategy_name": manifest.strategy_name,
                "parameter_candidate_id": base["candidate_id"],
                "parameter_values": params,
                "cost_model": cost_model,
                "train_metrics": train_metrics,
                "validation_metrics": validation_metrics,
                "final_holdout_metrics": final_holdout_metrics,
                "walk_forward_metrics": walk_forward,
                "regime_classifier_version": MARKET_REGIME_VERSION,
                "market_regime_bucket_performance": base["validation_regime_performance"],
                "market_regime_coverage": base["validation_regime_coverage"],
                "train_market_regime_bucket_performance": base["train_regime_performance"],
                "train_market_regime_coverage": base["train_regime_coverage"],
                "final_holdout_market_regime_bucket_performance": base["final_holdout_regime_performance"],
                "final_holdout_market_regime_coverage": base["final_holdout_regime_coverage"],
                "regime_gate_result": regime_gate.as_dict(),
                "allowed_live_regimes": list(regime_gate.allowed_live_regimes),
                "blocked_live_regimes": list(regime_gate.blocked_live_regimes),
                "regime_evidence": regime_gate.evidence,
                "walk_forward_required": manifest.acceptance_gate.walk_forward_required,
                "walk_forward_gate_result": "PASS" if walk_forward and walk_forward["return_consistency_pass"] else None,
                "parameter_stability": stability_payload,
                "acceptance_gate_result": gate_result,
                "gate_fail_reasons": fail_reasons,
                "warnings": list(base.get("warnings") or ()),
                "repository_version": _repository_version(),
            }
            profile_hash = sha256_prefixed(build_candidate_profile(candidate_payload))
            candidate_payload["candidate_profile_hash"] = profile_hash
            rows.append(candidate_payload)
    return sorted(rows, key=_candidate_rank_key)


def _gate_result(
    *,
    manifest: ExperimentManifest,
    validation_metrics: dict[str, Any],
    final_holdout_metrics: dict[str, Any] | None,
    walk_forward_metrics: dict[str, Any] | None,
    stability_score: float | None,
    include_walk_forward: bool,
    regime_gate_result: dict[str, Any] | None = None,
) -> tuple[str, list[str]]:
    gate = manifest.acceptance_gate
    reasons: list[str] = []
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
    return ("PASS" if not reasons else "FAIL", reasons)


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
    slippage_bps: float,
    parameter_stability_score: float | None,
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
    windows: list[dict[str, Any]] = []
    for window_id in sorted({key.rsplit("_", 1)[0] for key in snapshots if key.startswith("window_")}):
        train_snapshot = snapshots[f"{window_id}_train"]
        test_snapshot = snapshots[f"{window_id}_test"]
        train = runner(
            train_snapshot,
            parameter_values,
            fee_rate,
            slippage_bps,
            parameter_stability_score,
        )
        test = runner(
            test_snapshot,
            parameter_values,
            fee_rate,
            slippage_bps,
            parameter_stability_score,
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


def _report_payload(
    *,
    manifest: ExperimentManifest,
    snapshots: tuple[DatasetSnapshot, ...],
    candidates: list[dict[str, Any]],
    report_kind: str,
    generated_at: str | None,
) -> dict[str, Any]:
    best = next((candidate for candidate in candidates if candidate["acceptance_gate_result"] == "PASS"), None)
    warnings = sorted({warning for candidate in candidates for warning in candidate.get("warnings", [])})
    return {
        "report_kind": report_kind,
        "experiment_id": manifest.experiment_id,
        "hypothesis": manifest.hypothesis,
        "manifest_hash": manifest.manifest_hash(),
        "dataset_snapshot_id": manifest.dataset.snapshot_id,
        "dataset_content_hash": combined_dataset_fingerprint(snapshots),
        "market": manifest.market,
        "interval": manifest.interval,
        "dataset_splits": {
            snapshot.split_name: {
                "date_range": snapshot.date_range.as_dict(),
                "candle_count": len(snapshot.candles),
                "content_hash": snapshot.content_hash(),
            }
            for snapshot in snapshots
        },
        "strategy_name": manifest.strategy_name,
        "regime_classifier_version": MARKET_REGIME_VERSION,
        "regime_acceptance_gate": manifest.acceptance_gate.regime_acceptance_gate.as_dict(),
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
        "best_candidate_id": best.get("parameter_candidate_id") if best else None,
        "gate_result": "PASS" if best else "FAIL",
        "warnings": warnings,
        "candidates": candidates,
        "repository_version": _repository_version(),
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
    }


def _candidate_rank_key(candidate: dict[str, Any]) -> tuple[int, float, float]:
    passed = 0 if candidate.get("acceptance_gate_result") == "PASS" else 1
    validation = candidate.get("validation_metrics") or {}
    return (passed, -float(validation.get("return_pct") or 0.0), float(validation.get("max_drawdown_pct") or 0.0))


def _path_payload(paths: ResearchReportPaths) -> dict[str, str]:
    return {
        "derived_path": str(paths.derived_path),
        "report_path": str(paths.report_path),
    }


def _require_enough_candles(snapshots: Any) -> None:
    for snapshot in snapshots:
        if len(snapshot.candles) == 0:
            raise ResearchValidationError(f"dataset split {snapshot.split_name} has no candles")


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
