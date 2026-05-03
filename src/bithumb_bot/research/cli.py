from __future__ import annotations

import json
from pathlib import Path

from bithumb_bot.config import PATH_MANAGER, settings

from .experiment_manifest import ManifestValidationError, load_manifest
from .promotion_gate import PromotionGateError, promote_candidate
from .run_summary import ResearchRunSummary, build_research_run_summary
from .validation_protocol import ResearchValidationError, run_research_backtest, run_research_walk_forward


def cmd_research_backtest(*, manifest_path: str) -> int:
    try:
        manifest = load_manifest(manifest_path)
        report = run_research_backtest(
            manifest=manifest,
            db_path=settings.DB_PATH,
            manager=PATH_MANAGER,
        )
    except (ManifestValidationError, ResearchValidationError, OSError, ValueError) as exc:
        print(f"[RESEARCH-BACKTEST] error={exc}")
        return 1
    _print_report_summary("RESEARCH-BACKTEST", report)
    return 0


def cmd_research_walk_forward(*, manifest_path: str) -> int:
    try:
        manifest = load_manifest(manifest_path)
        report = run_research_walk_forward(
            manifest=manifest,
            db_path=settings.DB_PATH,
            manager=PATH_MANAGER,
        )
    except (ManifestValidationError, ResearchValidationError, OSError, ValueError) as exc:
        print(f"[RESEARCH-WALK-FORWARD] error={exc}")
        return 1
    _print_report_summary("RESEARCH-WALK-FORWARD", report)
    return 0


def cmd_research_promote_candidate(*, experiment_id: str, candidate_id: str) -> int:
    try:
        result = promote_candidate(
            experiment_id=experiment_id,
            candidate_id=candidate_id,
            manager=PATH_MANAGER,
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
