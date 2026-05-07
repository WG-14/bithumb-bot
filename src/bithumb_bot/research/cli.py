from __future__ import annotations

import json
from pathlib import Path

from bithumb_bot.config import PATH_MANAGER, settings

from .experiment_manifest import ManifestValidationError, load_manifest
from .execution_calibration import ExecutionCalibrationError, load_calibration_artifact
from .promotion_gate import PromotionGateError, promote_candidate
from .lineage import reproduce_promotion
from .run_summary import ResearchRunSummary, build_research_run_summary
from .validation_protocol import ResearchValidationError, run_research_backtest, run_research_walk_forward


def cmd_research_backtest(*, manifest_path: str, execution_calibration_path: str | None = None) -> int:
    try:
        manifest = load_manifest(manifest_path)
        calibration = load_calibration_artifact(execution_calibration_path) if execution_calibration_path else None
        report = run_research_backtest(
            manifest=manifest,
            db_path=settings.DB_PATH,
            manager=PATH_MANAGER,
            execution_calibration=calibration,
            manifest_path=manifest_path,
            command_args={
                "manifest": manifest_path,
                "execution_calibration": execution_calibration_path,
            },
        )
    except (ManifestValidationError, ExecutionCalibrationError, ResearchValidationError, OSError, ValueError) as exc:
        print(f"[RESEARCH-BACKTEST] error={exc}")
        return 1
    _print_report_summary("RESEARCH-BACKTEST", report)
    return 0


def cmd_research_walk_forward(*, manifest_path: str, execution_calibration_path: str | None = None) -> int:
    try:
        manifest = load_manifest(manifest_path)
        calibration = load_calibration_artifact(execution_calibration_path) if execution_calibration_path else None
        report = run_research_walk_forward(
            manifest=manifest,
            db_path=settings.DB_PATH,
            manager=PATH_MANAGER,
            execution_calibration=calibration,
            manifest_path=manifest_path,
            command_args={
                "manifest": manifest_path,
                "execution_calibration": execution_calibration_path,
            },
        )
    except (ManifestValidationError, ExecutionCalibrationError, ResearchValidationError, OSError, ValueError) as exc:
        print(f"[RESEARCH-WALK-FORWARD] error={exc}")
        return 1
    _print_report_summary("RESEARCH-WALK-FORWARD", report)
    return 0


def cmd_research_reproduce(*, promotion_path: str) -> int:
    result = reproduce_promotion(promotion_path)
    print(json.dumps(result.summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result.ok else 1


def cmd_research_promote_candidate(
    *,
    experiment_id: str,
    candidate_id: str,
    allow_legacy_lineage: bool = False,
) -> int:
    try:
        result = promote_candidate(
            experiment_id=experiment_id,
            candidate_id=candidate_id,
            manager=PATH_MANAGER,
            allow_legacy_lineage=allow_legacy_lineage,
        )
    except PromotionGateError as exc:
        print(f"[RESEARCH-PROMOTE-CANDIDATE] error={exc}")
        return 1
    print("[RESEARCH-PROMOTE-CANDIDATE]")
    print(f"  experiment_id={experiment_id}")
    print(f"  candidate_id={candidate_id}")
    print(f"  gate_result={result.artifact['gate_result']}")
    print(f"  artifact_path={result.artifact_path}")
    print(f"  content_hash={result.content_hash}")
    print(
        "  has_execution_calibration_warning="
        f"{1 if result.artifact.get('has_execution_calibration_warning') else 0}"
    )
    print(
        "  execution_calibration_warning_reasons="
        f"{_format_items(tuple(str(item) for item in result.artifact.get('execution_calibration_warning_reasons') or []))}"
    )
    print(
        "  promotion_warnings="
        f"{_format_items(tuple(str(item) for item in result.artifact.get('promotion_warnings') or []))}"
    )
    print(f"  legacy_compatibility_used={1 if result.artifact.get('legacy_compatibility_used') else 0}")
    print(
        "  dataset_quality_legacy_bypass_used="
        f"{1 if result.artifact.get('dataset_quality_legacy_bypass_used') else 0}"
    )
    print(f"  operator_next_step={result.artifact['operator_next_step']}")
    return 0


def _print_report_summary(label: str, report: dict[str, object]) -> None:
    artifact_paths = report.get("artifact_paths") if isinstance(report.get("artifact_paths"), dict) else {}
    summary = build_research_run_summary(report)
    print(f"[{label}]")
    print(f"  experiment_id={report.get('experiment_id')}")
    print(f"  manifest_hash={report.get('manifest_hash')}")
    print(f"  dataset_snapshot_id={report.get('dataset_snapshot_id')}")
    print(f"  dataset_content_hash={report.get('dataset_content_hash')}")
    print(f"  candidates_evaluated={report.get('candidate_count')}")
    print(f"  best_candidate_id={report.get('best_candidate_id') or 'none'}")
    print(f"  gate_result={report.get('gate_result')}")
    print(f"  candidate_gate_counts={_format_counts(summary.candidate_gate_counts)}")
    print(f"  top_fail_reasons={_format_counts(summary.top_fail_reasons)}")
    print(f"  promotion_allowed={1 if summary.promotion_allowed else 0}")
    print(f"  nearest_failed_candidate_id={summary.nearest_failed_candidate_id or 'none'}")
    print(
        "  nearest_failed_candidate_fail_reasons="
        f"{_format_items(summary.nearest_failed_candidate_fail_reasons)}"
    )
    print(f"  walk_forward_window_summary={_format_walk_forward_window_summary(summary)}")
    print(f"  top_window_fail_reasons={_format_counts(summary.top_window_fail_reasons)}")
    print(f"  next_action={summary.next_action}")
    print(f"  report_path={artifact_paths.get('report_path')}")
    print(f"  derived_path={artifact_paths.get('derived_path')}")
    print(f"  content_hash={report.get('content_hash')}")
    warnings = report.get("warnings") or []
    print(f"  warnings={','.join(str(item) for item in warnings) if warnings else 'none'}")
    quote_missing = _top_of_book_missing(report)
    if quote_missing:
        print(
            "  top_of_book_missing=collect orderbook top snapshots with sync-orderbook-top, "
            "rerun research-backtest, and verify top_of_book_coverage_pct"
        )


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ",".join(f"{key}:{value}" for key, value in counts.items())


def _format_items(items: tuple[str, ...]) -> str:
    if not items:
        return "none"
    return ",".join(items)


def _format_walk_forward_window_summary(summary: ResearchRunSummary) -> str:
    if summary.walk_forward_window_count is None:
        return "none"
    return (
        f"window_count:{summary.walk_forward_window_count},"
        f"pass:{summary.walk_forward_pass_window_count if summary.walk_forward_pass_window_count is not None else 'unknown'},"
        f"fail:{summary.walk_forward_fail_window_count if summary.walk_forward_fail_window_count is not None else 'unknown'}"
    )


def _top_of_book_missing(report: dict[str, object]) -> bool:
    quality = report.get("dataset_quality_reports")
    if not isinstance(quality, dict):
        return False
    for payload in quality.values():
        if isinstance(payload, dict) and int(payload.get("top_of_book_missing_count") or 0) > 0:
            return True
    return False
