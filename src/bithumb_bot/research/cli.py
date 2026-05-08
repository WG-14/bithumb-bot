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
    print(f"  execution_reference_policy={_nested(report, 'execution_timing_policy', 'fill_reference_policy') or 'unknown'}")
    print(f"  execution_reality_level={report.get('execution_reality_level') or 'unknown'}")
    print(f"  execution_reality_gate_status={report.get('execution_reality_gate_status') or 'unknown'}")
    print(
        "  execution_reality_gate_reasons="
        f"{_format_items(tuple(str(item) for item in report.get('execution_reality_gate_reasons') or []))}"
    )
    signal_coverage = report.get("signal_quote_coverage_summary")
    if isinstance(signal_coverage, dict):
        print(
            "  signal_quote_coverage="
            f"signal_event_count={signal_coverage.get('signal_event_count')} "
            f"fillable_signal_event_count={signal_coverage.get('fillable_signal_event_count')} "
            f"missing_quote_on_signal_count={signal_coverage.get('missing_quote_on_signal_count')} "
            f"quote_after_decision_coverage_pct={signal_coverage.get('quote_after_decision_coverage_pct')} "
            f"median_quote_age_ms={signal_coverage.get('median_quote_age_ms_on_signal')} "
            f"p95_quote_age_ms={signal_coverage.get('p95_quote_age_ms_on_signal')}"
        )
    print(f"  next_action={summary.next_action}")
    print(f"  report_path={artifact_paths.get('report_path')}")
    print(f"  derived_path={artifact_paths.get('derived_path')}")
    print(f"  content_hash={report.get('content_hash')}")
    warnings = report.get("warnings") or []
    print(f"  warnings={','.join(str(item) for item in warnings) if warnings else 'none'}")
    _print_top_of_book_summary(report)


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


def _print_top_of_book_summary(report: dict[str, object]) -> None:
    summary = report.get("top_of_book_quality_summary")
    if not isinstance(summary, dict) or not bool(summary.get("requested")):
        return
    affected = summary.get("affected_splits")
    affected_names = []
    if isinstance(affected, list):
        affected_names = [
            str(item.get("split_name"))
            for item in affected
            if isinstance(item, dict) and item.get("split_name")
        ]
    print(
        "  top_of_book_quote_coverage="
        f"requested=1 required={1 if summary.get('required') else 0} "
        f"gate_status={summary.get('gate_status')} "
        f"coverage_pct={summary.get('coverage_pct')} "
        f"joined_count={summary.get('joined_quote_count')} "
        f"missing_count={summary.get('missing_quote_count')} "
        f"join_tolerance_ms={summary.get('join_tolerance_ms')} "
        f"affected_splits={','.join(affected_names) if affected_names else 'none'}"
    )
    print(
        "  top_of_book_limitations="
        "best_bid_ask_only_not_full_depth,intra_candle_path_unavailable"
    )
    if summary.get("next_action"):
        print(f"  top_of_book_next_action={summary.get('next_action')}")


def _nested(payload: dict[str, object], *keys: str) -> object | None:
    current: object = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
