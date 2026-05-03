from __future__ import annotations

import json
from pathlib import Path

from bithumb_bot.config import PATH_MANAGER, settings

from .experiment_manifest import ManifestValidationError, load_manifest
from .promotion_gate import PromotionGateError, promote_candidate
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
    print(f"[{label}]")
    print(f"  experiment_id={report.get('experiment_id')}")
    print(f"  manifest_hash={report.get('manifest_hash')}")
    print(f"  dataset_snapshot_id={report.get('dataset_snapshot_id')}")
    print(f"  dataset_content_hash={report.get('dataset_content_hash')}")
    print(f"  candidates_evaluated={report.get('candidate_count')}")
    print(f"  best_candidate_id={report.get('best_candidate_id') or 'none'}")
    print(f"  gate_result={report.get('gate_result')}")
    print(f"  report_path={artifact_paths.get('report_path')}")
    print(f"  derived_path={artifact_paths.get('derived_path')}")
    print(f"  content_hash={report.get('content_hash')}")
    warnings = report.get("warnings") or []
    print(f"  warnings={','.join(str(item) for item in warnings) if warnings else 'none'}")
