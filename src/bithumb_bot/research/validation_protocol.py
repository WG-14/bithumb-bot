from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager

from .backtest_engine import run_sma_backtest
from .dataset_snapshot import DatasetSnapshot, combined_dataset_fingerprint, load_dataset_split
from .experiment_manifest import ExperimentManifest
from .hashing import sha256_prefixed
from .parameter_space import candidate_id, iter_parameter_candidates
from .promotion_gate import build_candidate_profile
from .report_writer import ResearchReportPaths, write_research_report


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
    snapshots = {
        "train": load_dataset_split(db_path=db_path, manifest=manifest, split_name="train"),
        "validation": load_dataset_split(db_path=db_path, manifest=manifest, split_name="validation"),
    }
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
    stability_scores = _parameter_stability_scores(raw_candidates)
    rows: list[dict[str, Any]] = []
    manifest_hash = manifest.manifest_hash()
    dataset_hash = combined_dataset_fingerprint(tuple(snapshots.values()))

    for index, params in enumerate(raw_candidates):
        stability_score = stability_scores[index]
        candidate = candidate_id(params, index)
        cost_results: list[dict[str, Any]] = []
        for slippage_bps in manifest.cost_model.slippage_bps:
            train = run_sma_backtest(
                dataset=snapshots["train"],
                parameter_values=params,
                fee_rate=manifest.cost_model.fee_rate,
                slippage_bps=float(slippage_bps),
                parameter_stability_score=stability_score,
            )
            validation = run_sma_backtest(
                dataset=snapshots["validation"],
                parameter_values=params,
                fee_rate=manifest.cost_model.fee_rate,
                slippage_bps=float(slippage_bps),
                parameter_stability_score=stability_score,
            )
            final_holdout = (
                run_sma_backtest(
                    dataset=snapshots["final_holdout"],
                    parameter_values=params,
                    fee_rate=manifest.cost_model.fee_rate,
                    slippage_bps=float(slippage_bps),
                    parameter_stability_score=stability_score,
                )
                if "final_holdout" in snapshots
                else None
            )
            walk_forward = _walk_forward_metrics(train.metrics.as_dict(), validation.metrics.as_dict()) if include_walk_forward else None
            gate_result, fail_reasons = _gate_result(
                manifest=manifest,
                validation_metrics=validation.metrics.as_dict(),
                final_holdout_metrics=final_holdout.metrics.as_dict() if final_holdout else None,
                walk_forward_metrics=walk_forward,
                stability_score=stability_score,
                include_walk_forward=include_walk_forward,
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
                "parameter_candidate_id": candidate,
                "parameter_values": params,
                "cost_model": cost_model,
                "train_metrics": train.metrics.as_dict(),
                "validation_metrics": validation.metrics.as_dict(),
                "final_holdout_metrics": final_holdout.metrics.as_dict() if final_holdout else None,
                "walk_forward_metrics": walk_forward,
                "walk_forward_required": manifest.acceptance_gate.walk_forward_required,
                "walk_forward_gate_result": "PASS" if walk_forward and walk_forward["return_consistency_pass"] else None,
                "acceptance_gate_result": gate_result,
                "gate_fail_reasons": fail_reasons,
                "repository_version": _repository_version(),
            }
            profile_hash = sha256_prefixed(build_candidate_profile(candidate_payload))
            candidate_payload["candidate_profile_hash"] = profile_hash
            cost_results.append(candidate_payload)
        rows.extend(cost_results)
    return sorted(rows, key=_candidate_rank_key)


def _gate_result(
    *,
    manifest: ExperimentManifest,
    validation_metrics: dict[str, Any],
    final_holdout_metrics: dict[str, Any] | None,
    walk_forward_metrics: dict[str, Any] | None,
    stability_score: float | None,
    include_walk_forward: bool,
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
    return ("PASS" if not reasons else "FAIL", reasons)


def _parameter_stability_scores(candidates: list[dict[str, Any]]) -> list[float | None]:
    if len(candidates) < 3:
        return [None for _ in candidates]
    return [1.0 for _ in candidates]


def _walk_forward_metrics(train_metrics: dict[str, Any], validation_metrics: dict[str, Any]) -> dict[str, Any]:
    train_return = float(train_metrics.get("return_pct") or 0.0)
    validation_return = float(validation_metrics.get("return_pct") or 0.0)
    return {
        "train_return_pct": train_return,
        "validation_return_pct": validation_return,
        "return_degradation_pct": train_return - validation_return,
        "return_consistency_pass": train_return > 0.0 and validation_return > 0.0,
    }


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
        "dataset_splits": {
            snapshot.split_name: {
                "date_range": snapshot.date_range.as_dict(),
                "candle_count": len(snapshot.candles),
                "content_hash": snapshot.content_hash(),
            }
            for snapshot in snapshots
        },
        "strategy_name": manifest.strategy_name,
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
