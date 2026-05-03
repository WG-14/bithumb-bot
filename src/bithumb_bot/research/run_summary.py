from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ResearchRunSummary:
    candidate_gate_counts: dict[str, int]
    top_fail_reasons: dict[str, int]
    top_window_fail_reasons: dict[str, int]
    walk_forward_window_count: int | None
    walk_forward_pass_window_count: int | None
    walk_forward_fail_window_count: int | None
    promotion_allowed: bool
    nearest_failed_candidate_id: str | None
    nearest_failed_candidate_fail_reasons: tuple[str, ...]
    next_action: str


def build_research_run_summary(report: dict[str, object]) -> ResearchRunSummary:
    candidates = _candidate_rows(report)
    gate_counts: Counter[str] = Counter()
    fail_reasons: Counter[str] = Counter()
    window_fail_reasons: Counter[str] = Counter()
    first_walk_forward_metrics: dict[str, Any] | None = None

    for candidate in candidates:
        gate_counts[_safe_label(candidate.get("acceptance_gate_result"), default="UNKNOWN")] += 1
        for reason in _string_items(candidate.get("gate_fail_reasons")):
            fail_reasons[reason] += 1

        walk_forward_metrics = candidate.get("walk_forward_metrics")
        if isinstance(walk_forward_metrics, dict):
            if first_walk_forward_metrics is None:
                first_walk_forward_metrics = walk_forward_metrics
            windows = walk_forward_metrics.get("windows")
            if isinstance(windows, list):
                for window in windows:
                    if not isinstance(window, dict):
                        continue
                    for reason in _string_items(window.get("fail_reasons")):
                        window_fail_reasons[reason] += 1

    promotion_allowed = bool(report.get("best_candidate_id")) and report.get("gate_result") == "PASS"
    has_pass_candidate = any(candidate.get("acceptance_gate_result") == "PASS" for candidate in candidates)
    nearest_candidate = candidates[0] if candidates and not has_pass_candidate else None

    return ResearchRunSummary(
        candidate_gate_counts=_ordered_gate_counts(gate_counts) if candidates else {},
        top_fail_reasons=_ordered_counts(fail_reasons),
        top_window_fail_reasons=_ordered_counts(window_fail_reasons),
        walk_forward_window_count=_safe_int(first_walk_forward_metrics.get("window_count"))
        if first_walk_forward_metrics is not None
        else None,
        walk_forward_pass_window_count=_safe_int(first_walk_forward_metrics.get("pass_window_count"))
        if first_walk_forward_metrics is not None
        else None,
        walk_forward_fail_window_count=_safe_int(first_walk_forward_metrics.get("fail_window_count"))
        if first_walk_forward_metrics is not None
        else None,
        promotion_allowed=promotion_allowed,
        nearest_failed_candidate_id=_candidate_id(nearest_candidate),
        nearest_failed_candidate_fail_reasons=tuple(_string_items(nearest_candidate.get("gate_fail_reasons")))
        if nearest_candidate is not None
        else (),
        next_action=_next_action(
            promotion_allowed=promotion_allowed,
            has_candidates=bool(candidates),
            top_fail_reasons=fail_reasons,
            gate_result=report.get("gate_result"),
        ),
    )


def _candidate_rows(report: dict[str, object]) -> list[dict[str, Any]]:
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        return []
    return [candidate for candidate in candidates if isinstance(candidate, dict)]


def _string_items(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(str(item) for item in value if item is not None and str(item))


def _safe_label(value: object, *, default: str) -> str:
    if value is None:
        return default
    label = str(value)
    return label if label else default


def _safe_int(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return None


def _candidate_id(candidate: dict[str, Any] | None) -> str | None:
    if candidate is None:
        return None
    value = candidate.get("parameter_candidate_id") or candidate.get("candidate_id")
    if value is None:
        return None
    candidate_id = str(value)
    return candidate_id if candidate_id else None


def _ordered_counts(counts: Counter[str]) -> dict[str, int]:
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _ordered_gate_counts(counts: Counter[str]) -> dict[str, int]:
    ordered = {"PASS": counts.get("PASS", 0), "FAIL": counts.get("FAIL", 0)}
    for key, value in sorted(counts.items()):
        if key not in ordered:
            ordered[key] = value
    return ordered


def _next_action(
    *,
    promotion_allowed: bool,
    has_candidates: bool,
    top_fail_reasons: Counter[str],
    gate_result: object,
) -> str:
    if promotion_allowed:
        return "review_promotion_candidate"
    if not has_candidates:
        return "inspect_dataset_or_manifest"
    if "walk_forward_missing" in top_fail_reasons:
        return "run_walk_forward_before_promotion"
    if "walk_forward_failed" in top_fail_reasons:
        return "do_not_promote_review_walk_forward_windows"
    if "profit_factor_failed" in top_fail_reasons or "min_trade_count_failed" in top_fail_reasons:
        return "do_not_promote_revise_strategy_hypothesis"
    if gate_result == "FAIL":
        return "inspect_report_or_adjust_hypothesis_not_promote"
    return "inspect_report_or_adjust_hypothesis_not_promote"
