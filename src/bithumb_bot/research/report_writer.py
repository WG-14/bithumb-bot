from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager, PathPolicyError
from bithumb_bot.storage_io import write_json_atomic as write_json_atomic_untracked
from .artifact_store import ArtifactBudget, ArtifactStore, ResearchArtifactContext
from .hashing import observe_hashing, report_content_hash_payload, sha256_prefixed


@dataclass(frozen=True)
class ResearchReportPaths:
    derived_path: Path
    report_path: Path
    candidate_events_path: Path
    candidate_results_dir: Path
    candidate_failures_dir: Path
    trace_manifest_path: Path


@dataclass(frozen=True)
class ResearchReportWriteResult:
    paths: ResearchReportPaths
    content_hash: str
    artifact_write_summary: dict[str, Any]
    report_payload: dict[str, Any] | None = None
    substage_timings: list[dict[str, Any]] | None = None

    def __iter__(self):
        yield self.paths
        yield self.content_hash


@dataclass
class ReportFinalizationState:
    paths: ResearchReportPaths
    store: ArtifactStore | ResearchArtifactContext
    report_payload: dict[str, Any]
    derived_candidates_payload: dict[str, Any]
    artifact_write_summary: dict[str, Any]
    artifact_total_bytes_base: int = 0
    content_hash: str = ""
    substage_timings: list[dict[str, Any]] | None = None

    def timing(self, stage: str, started_at: float, **details: Any) -> dict[str, Any]:
        payload = {
            "stage": stage,
            "wall_seconds": round(time.perf_counter() - started_at, 6),
        }
        payload.update(details)
        if self.substage_timings is None:
            self.substage_timings = []
        self.substage_timings.append(payload)
        return payload


def research_paths(manager: PathManager, experiment_id: str, report_name: str) -> ResearchReportPaths:
    research_derived_root = manager.data_dir() / "derived" / "research" / experiment_id
    derived_path = research_derived_root / f"{report_name}_candidates.json"
    report_path = manager.data_dir() / "reports" / "research" / experiment_id / f"{report_name}_report.json"
    candidate_events_path = research_derived_root / "candidate_events.jsonl"
    candidate_results_dir = research_derived_root / "candidate_results"
    candidate_failures_dir = research_derived_root / "candidate_failures"
    trace_manifest_path = research_derived_root / "trace_manifest.json"
    _ensure_research_output_path_allowed(manager, derived_path)
    _ensure_research_output_path_allowed(manager, report_path)
    _ensure_research_output_path_allowed(manager, candidate_events_path)
    _ensure_research_output_path_allowed(manager, candidate_results_dir)
    _ensure_research_output_path_allowed(manager, candidate_failures_dir)
    _ensure_research_output_path_allowed(manager, trace_manifest_path)
    return ResearchReportPaths(
        derived_path=derived_path,
        report_path=report_path,
        candidate_events_path=candidate_events_path,
        candidate_results_dir=candidate_results_dir,
        candidate_failures_dir=candidate_failures_dir,
        trace_manifest_path=trace_manifest_path,
    )


def research_artifact_refs(paths: ResearchReportPaths, *, manager: PathManager) -> dict[str, str]:
    data_dir = manager.data_dir().resolve()
    return {
        "derived_candidates": _relative_artifact_ref(paths.derived_path, data_dir),
        "report": _relative_artifact_ref(paths.report_path, data_dir),
        "candidate_events": _relative_artifact_ref(paths.candidate_events_path, data_dir),
        "candidate_results_dir": _relative_artifact_ref(paths.candidate_results_dir, data_dir),
        "candidate_failures_dir": _relative_artifact_ref(paths.candidate_failures_dir, data_dir),
        "audit_trace_manifest": _relative_artifact_ref(paths.trace_manifest_path, data_dir),
    }


def research_artifact_paths(paths: ResearchReportPaths) -> dict[str, str]:
    return {
        "derived_path": str(paths.derived_path.resolve()),
        "report_path": str(paths.report_path.resolve()),
        "candidate_events_path": str(paths.candidate_events_path.resolve()),
        "candidate_results_dir": str(paths.candidate_results_dir.resolve()),
        "candidate_failures_dir": str(paths.candidate_failures_dir.resolve()),
        "audit_trace_manifest_path": str(paths.trace_manifest_path.resolve()),
    }


def finalize_research_report_payload(
    *,
    manager: PathManager,
    experiment_id: str,
    report_name: str,
    payload: dict[str, Any],
) -> tuple[ResearchReportPaths, dict[str, Any], str]:
    paths = research_paths(manager, experiment_id, report_name)
    report_payload, derived_candidates_payload, derived_candidates_hash = _reference_first_report_payload(
        payload,
        paths=paths,
        manager=manager,
    )
    report_payload["artifact_refs"] = research_artifact_refs(paths, manager=manager)
    report_payload["artifact_paths"] = research_artifact_paths(paths)
    report_payload.setdefault("artifact_hashes", {})["derived_candidates"] = derived_candidates_hash
    report_payload["derived_candidates_hash"] = derived_candidates_hash
    report_payload["candidate_count"] = len(derived_candidates_payload["candidates"])
    report_payload["candidate_summary_hash"] = sha256_prefixed(
        report_content_hash_payload({"candidates": report_payload.get("candidates", [])})
    )
    content_hash = sha256_prefixed(report_content_hash_payload(report_payload))
    report_payload["content_hash"] = content_hash
    return paths, report_payload, content_hash


def write_research_report(
    *,
    manager: PathManager,
    experiment_id: str,
    report_name: str,
    payload: dict[str, Any],
    artifact_budget: ArtifactBudget | None = None,
    artifact_context: ResearchArtifactContext | None = None,
) -> ResearchReportWriteResult:
    started = time.perf_counter()
    paths = research_paths(manager, experiment_id, report_name)
    store = artifact_context or ArtifactStore(root=manager.data_dir(), budget=artifact_budget)
    with observe_hashing() as hash_observer:
        state = build_report_artifacts(
            manager=manager,
            paths=paths,
            payload=payload,
            store=store,
        )
        compute_report_hashes(state)
        compute_artifact_write_summary(state)
        write_report_artifacts(state)
        state.artifact_write_summary["write_wall_seconds"] = time.perf_counter() - started
        state.artifact_write_summary["finalization_wall_seconds"] = state.artifact_write_summary["write_wall_seconds"]
        state.artifact_write_summary["file_write_wall_seconds"] = sum(
            float(item.get("wall_seconds") or 0.0)
            for item in state.substage_timings or []
            if item.get("stage") in {"write_derived", "write_report", "final_report_rewrite"}
        )
        state.artifact_write_summary.update(
            {
                "hash_call_count": hash_observer.hash_call_count,
                "observed_hash_call_count": hash_observer.hash_call_count,
                "observed_hash_payload_bytes": hash_observer.observed_hash_payload_bytes,
                "largest_hash_payload_bytes": hash_observer.largest_hash_payload_bytes,
                "observed_largest_hash_payload_bytes": hash_observer.largest_hash_payload_bytes,
                "largest_hash_label": hash_observer.largest_hash_label,
                "observed_report_finalization_seconds": state.artifact_write_summary["finalization_wall_seconds"],
            }
        )
        state.artifact_write_summary["substage_timings"] = list(state.substage_timings or [])
        state.report_payload.setdefault("artifact_observability", {})["report_write"] = dict(
            state.artifact_write_summary
        )
        sync_final_report_observability(state)
        state.artifact_write_summary.update(
            {
                "hash_call_count": hash_observer.hash_call_count,
                "observed_hash_call_count": hash_observer.hash_call_count,
                "observed_hash_payload_bytes": hash_observer.observed_hash_payload_bytes,
                "largest_hash_payload_bytes": hash_observer.largest_hash_payload_bytes,
                "observed_largest_hash_payload_bytes": hash_observer.largest_hash_payload_bytes,
                "largest_hash_label": hash_observer.largest_hash_label,
            }
        )
        state.artifact_write_summary["substage_timings"] = list(state.substage_timings or [])
        state.report_payload["artifact_write_summary"] = dict(state.artifact_write_summary)
        state.report_payload.setdefault("artifact_observability", {})["report_write"] = dict(
            state.artifact_write_summary
        )
        _sync_report_write_stage(state.report_payload, state.artifact_write_summary)
        _sync_report_write_substages(state.report_payload, state.artifact_write_summary)
        _sync_workload_estimate_comparison(state.report_payload, state.artifact_write_summary)
        _rewrite_final_report_payload(state)
    return ResearchReportWriteResult(
        paths=state.paths,
        content_hash=state.content_hash,
        artifact_write_summary=state.artifact_write_summary,
        report_payload=state.report_payload,
        substage_timings=list(state.substage_timings or []),
    )


def build_report_artifacts(
    *,
    manager: PathManager,
    paths: ResearchReportPaths,
    payload: dict[str, Any],
    store: ArtifactStore | ResearchArtifactContext,
) -> ReportFinalizationState:
    started = time.perf_counter()
    report_payload, derived_candidates_payload, derived_candidates_hash = _reference_first_report_payload(
        payload,
        paths=paths,
        manager=manager,
    )
    state = ReportFinalizationState(
        paths=paths,
        store=store,
        report_payload=report_payload,
        derived_candidates_payload=derived_candidates_payload,
        artifact_write_summary={
            "schema_version": 1,
            "derived_candidates_path": str(paths.derived_path.resolve()),
            "derived_candidates_ref": _relative_artifact_ref(paths.derived_path, manager.data_dir().resolve()),
            "derived_candidates_hash": derived_candidates_hash,
            "derived_candidates_bytes": 0,
            "report_path": str(paths.report_path.resolve()),
            "report_ref": _relative_artifact_ref(paths.report_path, manager.data_dir().resolve()),
            "report_bytes": 0,
            "artifact_file_count": _predicted_file_count(store, paths.derived_path, paths.report_path),
            "artifact_total_bytes": 0,
            "write_wall_seconds": 0.0,
            "finalization_wall_seconds": 0.0,
            "file_write_wall_seconds": 0.0,
        },
        substage_timings=[],
    )
    report_payload["artifact_refs"] = research_artifact_refs(paths, manager=manager)
    report_payload["artifact_paths"] = research_artifact_paths(paths)
    report_payload.setdefault("artifact_hashes", {})["derived_candidates"] = derived_candidates_hash
    report_payload["derived_candidates_hash"] = derived_candidates_hash
    report_payload["candidate_count"] = len(derived_candidates_payload["candidates"])
    report_payload["artifact_write_summary"] = dict(state.artifact_write_summary)
    report_payload.setdefault("artifact_observability", {})["report_write"] = dict(state.artifact_write_summary)
    state.timing("reference_first_payload", started, candidate_count=report_payload["candidate_count"])
    return state


def compute_report_hashes(state: ReportFinalizationState) -> None:
    started = time.perf_counter()
    state.report_payload["candidate_summary_hash"] = sha256_prefixed(
        report_content_hash_payload({"candidates": state.report_payload.get("candidates", [])}),
        label="report_candidate_summary",
    )
    state.timing("report_candidate_summary", started)
    started = time.perf_counter()
    state.artifact_write_summary["derived_candidates_hash"] = sha256_prefixed(
        report_content_hash_payload(state.derived_candidates_payload),
        label="derived_candidate_summary",
    )
    state.report_payload.setdefault("artifact_hashes", {})["derived_candidates"] = state.artifact_write_summary[
        "derived_candidates_hash"
    ]
    state.report_payload["derived_candidates_hash"] = state.artifact_write_summary["derived_candidates_hash"]
    state.timing("derived_candidate_summary", started)
    started = time.perf_counter()
    state.content_hash = sha256_prefixed(
        report_content_hash_payload(state.report_payload),
        label="report_content_hash",
    )
    state.report_payload["content_hash"] = state.content_hash
    state.timing("report_hashing", started)


def compute_artifact_write_summary(state: ReportFinalizationState) -> None:
    started = time.perf_counter()
    state.artifact_write_summary["derived_candidates_bytes"] = _json_byte_count(state.derived_candidates_payload)
    state.artifact_write_summary["report_bytes"] = _stable_report_byte_count(state.report_payload)
    state.artifact_write_summary["artifact_total_bytes"] = (
        _current_total_bytes(state.store)
        + int(state.artifact_write_summary["derived_candidates_bytes"])
        + int(state.artifact_write_summary["report_bytes"])
    )
    state.report_payload["artifact_write_summary"] = dict(state.artifact_write_summary)
    state.report_payload.setdefault("artifact_observability", {})["report_write"] = dict(state.artifact_write_summary)
    state.timing(
        "report_byte_count",
        started,
        derived_candidates_bytes=state.artifact_write_summary["derived_candidates_bytes"],
        report_bytes=state.artifact_write_summary["report_bytes"],
    )


def write_report_artifacts(state: ReportFinalizationState) -> None:
    started = time.perf_counter()
    derived_event = state.store.write_json_atomic(state.paths.derived_path, state.derived_candidates_payload)
    state.artifact_write_summary["derived_candidates_bytes"] = derived_event.bytes
    state.timing("write_derived", started, bytes=derived_event.bytes)
    started = time.perf_counter()
    final_report_event = state.store.write_json_atomic(state.paths.report_path, state.report_payload)
    state.artifact_total_bytes_base = int(state.store.total_bytes) - int(final_report_event.bytes)
    state.artifact_write_summary.update(
        {
            "artifact_file_count": int(state.store.file_count),
            "report_bytes": final_report_event.bytes,
            "artifact_total_bytes": int(state.store.total_bytes),
        }
    )
    state.timing("write_report", started, bytes=final_report_event.bytes)


def sync_final_report_observability(
    state: ReportFinalizationState,
    *,
    stage_timings: list[dict[str, Any]] | None = None,
) -> None:
    started = time.perf_counter()
    final_content_hash, final_summary = persist_final_research_report_observability(
        paths=state.paths,
        report_payload=state.report_payload,
        artifact_write_summary=state.artifact_write_summary,
        artifact_total_bytes_base=state.artifact_total_bytes_base,
        stage_timings=stage_timings,
        rewrite_stage_name="final_report_rewrite",
        rewrite_timing_sink=state.substage_timings,
    )
    state.content_hash = final_content_hash
    state.artifact_write_summary = final_summary
    state.timing("persist_final_observability", started)


def _rewrite_final_report_payload(state: ReportFinalizationState) -> None:
    state.artifact_write_summary["substage_timings"] = list(state.substage_timings or [])
    state.artifact_write_summary["file_write_wall_seconds"] = sum(
        float(item.get("wall_seconds") or 0.0)
        for item in state.substage_timings or []
        if item.get("stage") in {"write_derived", "write_report", "final_report_rewrite"}
    )
    state.artifact_write_summary["observed_report_finalization_seconds"] = state.artifact_write_summary.get(
        "finalization_wall_seconds",
        state.artifact_write_summary.get("write_wall_seconds", 0.0),
    )
    state.report_payload["artifact_write_summary"] = dict(state.artifact_write_summary)
    state.report_payload.setdefault("artifact_observability", {})["report_write"] = dict(
        state.artifact_write_summary
    )
    _sync_report_write_stage(state.report_payload, state.artifact_write_summary)
    _sync_report_write_substages(state.report_payload, state.artifact_write_summary)
    _sync_workload_estimate_comparison(state.report_payload, state.artifact_write_summary)
    state.content_hash = sha256_prefixed(
        report_content_hash_payload(state.report_payload),
        label="final_report_content_hash_after_observability",
    )
    state.report_payload["content_hash"] = state.content_hash
    state.artifact_write_summary["report_bytes"] = _stable_final_report_byte_count(
        state.report_payload,
        state.artifact_write_summary,
        artifact_total_bytes_base=int(state.artifact_total_bytes_base),
    )
    state.artifact_write_summary["artifact_total_bytes"] = (
        int(state.artifact_total_bytes_base) + int(state.artifact_write_summary["report_bytes"])
    )
    state.artifact_write_summary["observed_artifact_bytes"] = state.artifact_write_summary["artifact_total_bytes"]
    state.report_payload["artifact_write_summary"] = dict(state.artifact_write_summary)
    state.report_payload.setdefault("artifact_observability", {})["report_write"] = dict(
        state.artifact_write_summary
    )
    _sync_report_write_stage(state.report_payload, state.artifact_write_summary)
    _sync_report_write_substages(state.report_payload, state.artifact_write_summary)
    _sync_workload_estimate_comparison(state.report_payload, state.artifact_write_summary)
    state.content_hash = sha256_prefixed(
        report_content_hash_payload(state.report_payload),
        label="final_report_content_hash_after_byte_count",
    )
    state.report_payload["content_hash"] = state.content_hash
    _converge_final_report_size(state)
    rewrite_started = time.perf_counter()
    write_json_atomic_untracked(state.paths.report_path, state.report_payload)
    actual_report_bytes = state.paths.report_path.stat().st_size
    if actual_report_bytes != state.artifact_write_summary["report_bytes"]:
        state.artifact_write_summary["report_bytes"] = actual_report_bytes
        state.artifact_write_summary["artifact_total_bytes"] = int(state.artifact_total_bytes_base) + actual_report_bytes
        state.artifact_write_summary["observed_artifact_bytes"] = state.artifact_write_summary["artifact_total_bytes"]
        state.report_payload["artifact_write_summary"] = dict(state.artifact_write_summary)
        state.report_payload.setdefault("artifact_observability", {})["report_write"] = dict(
            state.artifact_write_summary
        )
        _sync_report_write_stage(state.report_payload, state.artifact_write_summary)
        _sync_report_write_substages(state.report_payload, state.artifact_write_summary)
        _sync_workload_estimate_comparison(state.report_payload, state.artifact_write_summary)
        _converge_final_report_size(state)
        write_json_atomic_untracked(state.paths.report_path, state.report_payload)
    if state.substage_timings is not None:
        state.substage_timings.append(
            {
                "stage": "final_report_rewrite",
                "wall_seconds": round(time.perf_counter() - rewrite_started, 6),
                "bytes": state.paths.report_path.stat().st_size,
                "reason": "post_observability_sync",
            }
        )
    persisted = json.loads(state.paths.report_path.read_text(encoding="utf-8"))
    if isinstance(persisted, dict):
        state.report_payload = persisted
        summary = persisted.get("artifact_write_summary")
        if isinstance(summary, dict):
            state.artifact_write_summary = dict(summary)
        state.content_hash = str(persisted.get("content_hash") or state.content_hash)


def _converge_final_report_size(state: ReportFinalizationState) -> None:
    for _ in range(50):
        expected_report_bytes = _json_byte_count(state.report_payload)
        if expected_report_bytes == state.artifact_write_summary.get("report_bytes"):
            return
        state.artifact_write_summary["report_bytes"] = expected_report_bytes
        state.artifact_write_summary["artifact_total_bytes"] = (
            int(state.artifact_total_bytes_base) + expected_report_bytes
        )
        state.artifact_write_summary["observed_artifact_bytes"] = state.artifact_write_summary["artifact_total_bytes"]
        state.report_payload["artifact_write_summary"] = dict(state.artifact_write_summary)
        state.report_payload.setdefault("artifact_observability", {})["report_write"] = dict(
            state.artifact_write_summary
        )
        _sync_report_write_stage(state.report_payload, state.artifact_write_summary)
        _sync_report_write_substages(state.report_payload, state.artifact_write_summary)
        _sync_workload_estimate_comparison(state.report_payload, state.artifact_write_summary)
        state.report_payload["artifact_write_summary"] = dict(state.artifact_write_summary)
        state.report_payload.setdefault("artifact_observability", {})["report_write"] = dict(
            state.artifact_write_summary
        )
        state.content_hash = sha256_prefixed(
            report_content_hash_payload(state.report_payload),
            label="final_report_content_hash_size_convergence",
        )
        state.report_payload["content_hash"] = state.content_hash
    raise RuntimeError(
        "final_report_byte_count_did_not_converge:"
        f" expected={_json_byte_count(state.report_payload)}"
        f" reported={state.artifact_write_summary.get('report_bytes')}"
    )


def persist_final_research_report_observability(
    *,
    paths: ResearchReportPaths,
    report_payload: dict[str, Any],
    artifact_write_summary: dict[str, Any],
    artifact_total_bytes_base: int | None = None,
    stage_timings: list[dict[str, Any]] | None = None,
    rewrite_stage_name: str = "final_report_rewrite",
    rewrite_timing_sink: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any]]:
    final_summary = dict(artifact_write_summary)
    if stage_timings is not None:
        report_payload.setdefault("execution_observability", {})["stage_timings"] = list(stage_timings)
    report_payload.setdefault("artifact_observability", {})
    if artifact_total_bytes_base is None:
        artifact_total_bytes_base = int(final_summary["artifact_total_bytes"]) - int(final_summary["report_bytes"])
    final_summary["report_bytes"] = _stable_final_report_byte_count(
        report_payload,
        final_summary,
        artifact_total_bytes_base=int(artifact_total_bytes_base),
    )
    final_summary["artifact_total_bytes"] = int(artifact_total_bytes_base) + int(final_summary["report_bytes"])
    report_payload["artifact_write_summary"] = dict(final_summary)
    report_payload["artifact_observability"]["report_write"] = dict(final_summary)
    _sync_report_write_stage(report_payload, final_summary)
    final_content_hash = sha256_prefixed(report_content_hash_payload(report_payload))
    report_payload["content_hash"] = final_content_hash
    final_summary["report_bytes"] = _stable_final_report_byte_count(
        report_payload,
        final_summary,
        artifact_total_bytes_base=int(artifact_total_bytes_base),
    )
    final_summary["artifact_total_bytes"] = int(artifact_total_bytes_base) + int(final_summary["report_bytes"])
    report_payload["artifact_write_summary"] = dict(final_summary)
    report_payload["artifact_observability"]["report_write"] = dict(final_summary)
    final_content_hash = sha256_prefixed(report_content_hash_payload(report_payload))
    report_payload["content_hash"] = final_content_hash
    rewrite_started = time.perf_counter()
    write_json_atomic_untracked(paths.report_path, report_payload)
    if rewrite_timing_sink is not None:
        rewrite_timing_sink.append(
            {
                "stage": rewrite_stage_name,
                "wall_seconds": round(time.perf_counter() - rewrite_started, 6),
                "bytes": paths.report_path.stat().st_size,
            }
        )
    actual_report_bytes = paths.report_path.stat().st_size
    if actual_report_bytes != final_summary["report_bytes"]:
        final_summary["report_bytes"] = actual_report_bytes
        final_summary["artifact_total_bytes"] = int(artifact_total_bytes_base) + actual_report_bytes
        report_payload["artifact_write_summary"] = dict(final_summary)
        report_payload["artifact_observability"]["report_write"] = dict(final_summary)
        _sync_report_write_stage(report_payload, final_summary)
        final_content_hash = sha256_prefixed(report_content_hash_payload(report_payload))
        report_payload["content_hash"] = final_content_hash
        rewrite_started = time.perf_counter()
        write_json_atomic_untracked(paths.report_path, report_payload)
        if rewrite_timing_sink is not None:
            rewrite_timing_sink.append(
                {
                    "stage": rewrite_stage_name,
                    "wall_seconds": round(time.perf_counter() - rewrite_started, 6),
                    "bytes": paths.report_path.stat().st_size,
                    "reason": "actual_report_bytes_mismatch",
                }
            )
    return final_content_hash, final_summary


def _reference_first_report_payload(
    payload: dict[str, Any],
    *,
    paths: ResearchReportPaths,
    manager: PathManager,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    report_payload = dict(payload)
    candidates = list(report_payload.get("candidates", []))
    report_detail = _report_detail(report_payload)
    derived_candidates_payload = {
        "detail_policy": f"{report_detail}_bounded" if report_detail in {"index", "summary", "standard"} else "full",
        "candidates": [summarize_derived_candidate(candidate, report_detail) for candidate in candidates],
    }
    derived_candidates_hash = sha256_prefixed(
        report_content_hash_payload(derived_candidates_payload),
        label="derived_candidates_payload_hash",
    )
    if report_detail in {"index", "summary", "standard"}:
        report_payload["candidates"] = [summarize_report_candidate(candidate) for candidate in candidates]
        report_payload["derived_candidates_ref"] = _relative_artifact_ref(paths.derived_path, manager.data_dir().resolve())
        report_payload["derived_candidates_path"] = str(paths.derived_path.resolve())
    return report_payload, derived_candidates_payload, derived_candidates_hash


def _report_detail(payload: dict[str, Any]) -> str:
    research_run = payload.get("research_run")
    if isinstance(research_run, dict):
        detail = str(research_run.get("report_detail") or "full").strip().lower()
        return detail if detail in {"index", "summary", "standard", "full"} else "full"
    return "full"


def summarize_report_candidate(candidate: Any) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        return {"candidate_repr_hash": sha256_prefixed({"repr": repr(candidate)}, label="candidate_repr_hash")}
    summary_keys = (
        "candidate_id",
        "acceptance_gate_result",
        "acceptance_gate_status",
        "status",
        "evaluation_status",
        "metrics_status",
        "metrics_v2_source",
        "validation_metrics_v2",
        "final_holdout_metrics_v2",
        "candidate_failed_before_complete_metrics",
        "gate_fail_reasons",
        "warnings",
        "failure_artifact_path",
        "failure_artifact_ref",
        "resource_guard",
        "behavior_hash",
        "strategy_behavior_hash",
        "profile_hash",
        "candidate_profile_hash",
        "metrics_hash",
        "content_hash",
        "cost_sensitivity",
        "strategy_runtime_capabilities",
        "promotion_interpretation",
        "exploratory_result",
    )
    summary = {key: candidate[key] for key in summary_keys if key in candidate}
    _copy_compact_diagnostics(summary, candidate)
    summary["candidate_payload_hash"] = sha256_prefixed(
        candidate_evidence_hash_inputs(candidate),
        label="candidate_evidence_hash",
    )
    return summary


def summarize_derived_candidate(candidate: Any, report_detail: str) -> Any:
    report_detail = _normalize_report_detail(report_detail)
    if report_detail == "full":
        return _strip_stage_trace_arrays(candidate)
    if not isinstance(candidate, dict):
        return {"candidate_repr_hash": sha256_prefixed({"repr": repr(candidate)}, label="candidate_repr_hash")}
    summary = _derived_candidate_index_summary(candidate, include_compact=report_detail != "index")
    summary["derived_detail_policy"] = f"{report_detail}_bounded"
    return summary


def _derived_candidate_index_summary(candidate: dict[str, Any], *, include_compact: bool = True) -> dict[str, Any]:
    summary_keys = (
        "experiment_id",
        "manifest_hash",
        "dataset_snapshot_id",
        "dataset_content_hash",
        "dataset_quality_hash",
        "dataset_quality_gate_status",
        "strategy_name",
        "parameter_candidate_id",
        "candidate_id",
        "parameter_values",
        "effective_strategy_parameters_hash",
        "candidate_behavior_profile_hash",
        "candidate_profile_hash",
        "acceptance_gate_result",
        "acceptance_gate_status",
        "gate_fail_reasons",
        "warnings",
        "failure_artifact_path",
        "failure_artifact_ref",
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "composite_behavior_hash_v2",
        "metrics_hash",
        "candidate_failed_before_complete_metrics",
        "evaluation_status",
        "metrics_status",
        "metrics_v2_source",
        "retained_detail_summary",
        "cost_sensitivity",
        "position_sizing_sensitivity",
        "strategy_runtime_capabilities",
        "promotion_interpretation",
        "exploratory_result",
    )
    summary = {key: candidate[key] for key in summary_keys if key in candidate}
    _compact_derived_candidate_large_blocks(summary)
    if include_compact:
        _copy_compact_diagnostics(summary, candidate)
    if "resource_guard" in candidate:
        summary["resource_guard"] = _compact_resource_guard(candidate["resource_guard"])
    summary["scenario_results"] = [
        _derived_scenario_index_summary(scenario, include_compact=include_compact)
        for scenario in candidate.get("scenario_results") or []
    ]
    summary["candidate_payload_hash"] = sha256_prefixed(
        candidate_evidence_hash_inputs(candidate),
        label="derived_candidate_evidence_hash",
    )
    summary["candidate_result_detail_policy"] = "summary_bounded"
    return summary


def _compact_derived_candidate_large_blocks(summary: dict[str, Any]) -> None:
    cost_sensitivity = summary.pop("cost_sensitivity", None)
    if isinstance(cost_sensitivity, dict):
        summary["cost_sensitivity_hash"] = sha256_prefixed(cost_sensitivity)
        summary["cost_sensitivity_status"] = {
            key: value.get("status") if isinstance(value, dict) else None
            for key, value in cost_sensitivity.items()
            if key in {"zero_cost", "base_cost", "stress_cost"}
        }
        if cost_sensitivity.get("promotion_authority"):
            summary["cost_sensitivity_promotion_authority"] = cost_sensitivity["promotion_authority"]
    elif cost_sensitivity is not None:
        summary["cost_sensitivity_hash"] = sha256_prefixed(cost_sensitivity)

    position_sizing = summary.pop("position_sizing_sensitivity", None)
    if isinstance(position_sizing, dict):
        summary["position_sizing_sensitivity_hash"] = sha256_prefixed(position_sizing)
        if position_sizing.get("status"):
            summary["position_sizing_sensitivity_status"] = position_sizing["status"]
    elif position_sizing is not None:
        summary["position_sizing_sensitivity_hash"] = sha256_prefixed(position_sizing)

    capabilities = summary.pop("strategy_runtime_capabilities", None)
    if isinstance(capabilities, dict):
        summary["strategy_runtime_capabilities_hash"] = sha256_prefixed(capabilities)
        for key in (
            "research_only",
            "promotion_runtime_decisions_supported",
            "live_dry_run_allowed",
            "live_real_order_allowed",
            "fail_closed_reason",
        ):
            if key in capabilities:
                summary[f"strategy_runtime_capability_{key}"] = capabilities[key]
    elif capabilities is not None:
        summary["strategy_runtime_capabilities_hash"] = sha256_prefixed(capabilities)


def _derived_scenario_index_summary(scenario: Any, *, include_compact: bool = True) -> dict[str, Any]:
    if not isinstance(scenario, dict):
        return {"scenario_repr_hash": sha256_prefixed({"repr": repr(scenario)}, label="scenario_repr_hash")}
    summary_keys = (
        "scenario_id",
        "scenario_index",
        "scenario_type",
        "scenario_role",
        "scenario_acceptance_gate_result",
        "scenario_fail_reasons",
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "composite_behavior_hash_v2",
        "train_behavior_hash",
        "validation_behavior_hash",
        "final_holdout_behavior_hash",
        "candidate_failed",
        "candidate_failed_before_complete_metrics",
        "evaluation_status",
        "metrics_status",
        "metrics_v2_source",
        "failure_reason",
        "failure_artifact_ref",
        "failure_artifact_path",
        "retained_detail_summary",
        "train_resource_usage",
        "validation_resource_usage",
        "final_holdout_resource_usage",
        "train_audit_trace_index",
        "validation_audit_trace_index",
        "final_holdout_audit_trace_index",
    )
    summary = {key: scenario[key] for key in summary_keys if key in scenario}
    if include_compact:
        _copy_compact_diagnostics(summary, scenario)
    if "resource_guard" in scenario:
        summary["resource_guard"] = _compact_resource_guard(scenario["resource_guard"])
    summary["train_equity_curve"] = []
    summary["validation_equity_curve"] = []
    summary["final_holdout_equity_curve"] = []
    for key in (
        "train_resource_usage",
        "validation_resource_usage",
        "final_holdout_resource_usage",
    ):
        if key in summary:
            summary[key] = summarize_resource_usage_for_candidate_artifact(summary[key])
    summary["detail_artifact_ref"] = scenario.get("detail_artifact_ref")
    summary["scenario_payload_hash"] = sha256_prefixed(
        scenario_evidence_hash_inputs(scenario),
        label="scenario_evidence_hash",
    )
    _ensure_scenario_retained_detail_evidence(summary)
    return summary


def summarize_candidate_result(candidate: Any, report_detail: str) -> Any:
    report_detail = _normalize_report_detail(report_detail)
    if report_detail == "full":
        return _strip_stage_trace_arrays(candidate)
    if not isinstance(candidate, dict):
        return {"candidate_repr_hash": sha256_prefixed({"repr": repr(candidate)}, label="candidate_repr_hash")}
    if report_detail == "index":
        summary = _candidate_result_index_summary(candidate)
        summary["candidate_result_detail_policy"] = "index_bounded"
        return summary
    summary_keys = (
        "experiment_id",
        "manifest_hash",
        "dataset_snapshot_id",
        "dataset_content_hash",
        "dataset_quality_hash",
        "dataset_quality_gate_status",
        "dataset_quality_gate_reasons",
        "dataset_quality_report_hashes",
        "strategy_name",
        "parameter_candidate_id",
        "candidate_id",
        "parameter_values",
        "effective_strategy_parameters_hash",
        "candidate_behavior_profile_hash",
        "candidate_profile_hash",
        "acceptance_gate_result",
        "acceptance_gate_status",
        "gate_fail_reasons",
        "warnings",
        "failure_artifact_path",
        "failure_artifact_ref",
        "resource_guard",
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "composite_behavior_hash_v2",
        "metrics_hash",
        "validation_metrics",
        "validation_metrics_v2",
        "final_holdout_metrics",
        "final_holdout_metrics_v2",
        "walk_forward_metrics",
        "production_calibration_policy_result",
        "production_calibration_policy_reasons",
        "execution_calibration_gate",
        "execution_calibration_policy_source",
        "execution_calibration_artifact_hash",
        "execution_calibration_artifact_hashes",
        "has_execution_calibration_warning",
        "execution_calibration_warning_reasons",
        "retained_detail_summary",
        "cost_sensitivity",
        "position_sizing_sensitivity",
        "strategy_runtime_capabilities",
        "promotion_interpretation",
        "exploratory_result",
    )
    summary = {key: candidate[key] for key in summary_keys if key in candidate}
    _copy_compact_diagnostics(summary, candidate)
    _compact_candidate_artifact_summary(summary)
    summary["scenario_results"] = [
        _scenario_result_summary(scenario, include_closed_trade_summary=report_detail == "standard")
        for scenario in candidate.get("scenario_results") or []
    ]
    summary["candidate_payload_hash"] = sha256_prefixed(
        candidate_evidence_hash_inputs(candidate),
        label="candidate_result_evidence_hash",
    )
    summary["candidate_result_detail_policy"] = f"{report_detail}_bounded"
    return summary


def _normalize_report_detail(report_detail: str) -> str:
    detail = str(report_detail or "full").strip().lower()
    return detail if detail in {"index", "summary", "standard", "full"} else "full"


def _candidate_result_index_summary(candidate: dict[str, Any]) -> dict[str, Any]:
    summary_keys = (
        "experiment_id",
        "manifest_hash",
        "strategy_name",
        "parameter_candidate_id",
        "candidate_id",
        "candidate_profile_hash",
        "candidate_behavior_profile_hash",
        "acceptance_gate_result",
        "acceptance_gate_status",
        "gate_fail_reasons",
        "evaluation_status",
        "metrics_status",
        "metrics_hash",
        "behavior_hash",
        "strategy_behavior_hash",
        "content_hash",
    )
    summary = {key: candidate[key] for key in summary_keys if key in candidate}
    summary["scenario_results"] = [
        _derived_scenario_index_summary(scenario, include_compact=False)
        for scenario in candidate.get("scenario_results") or []
    ]
    summary["candidate_payload_hash"] = sha256_prefixed(
        candidate_evidence_hash_inputs(candidate),
        label="candidate_result_index_evidence_hash",
    )
    return summary


def candidate_evidence_hash_inputs(candidate: dict[str, Any]) -> dict[str, Any]:
    scenario_hashes = [
        scenario_evidence_hash_inputs(scenario)
        for scenario in candidate.get("scenario_results") or []
        if isinstance(scenario, dict)
    ]
    evidence = {
        "candidate_id": candidate.get("candidate_id") or candidate.get("parameter_candidate_id"),
        "parameter_values_hash": _parameter_values_hash(candidate),
        "scenario_evidence_hashes": [
            sha256_prefixed(scenario, label="scenario_evidence_tree_hash") for scenario in scenario_hashes
        ],
    }
    for key in (
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "candidate_behavior_profile_hash",
        "candidate_profile_hash",
        "profile_hash",
        "metrics_hash",
        "content_hash",
    ):
        if candidate.get(key):
            evidence[key] = candidate[key]
    return evidence


def scenario_evidence_hash_inputs(scenario: dict[str, Any]) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "scenario_id": scenario.get("scenario_id"),
        "scenario_index": scenario.get("scenario_index"),
        "scenario_role": scenario.get("scenario_role"),
    }
    for key in (
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "composite_behavior_hash_v2",
        "train_behavior_hash",
        "validation_behavior_hash",
        "final_holdout_behavior_hash",
        "metrics_hash",
        "execution_contract_hash",
        "execution_capability_contract_hash",
        "model_params_hash",
    ):
        if scenario.get(key):
            evidence[key] = scenario[key]
    for key in (
        "train_resource_usage",
        "validation_resource_usage",
        "final_holdout_resource_usage",
    ):
        usage = scenario.get(key)
        if isinstance(usage, dict):
            evidence[f"{key}_hashes"] = _resource_usage_evidence_hashes(usage)
    return evidence


def _parameter_values_hash(candidate: dict[str, Any]) -> str | None:
    for key in ("effective_strategy_parameters_hash", "parameter_values_hash"):
        if candidate.get(key):
            return str(candidate[key])
    if "parameter_values" in candidate:
        return sha256_prefixed(candidate["parameter_values"], label="parameter_values_hash")
    if "parameter_values_raw" in candidate:
        return sha256_prefixed(candidate["parameter_values_raw"], label="parameter_values_hash")
    return None


def _resource_usage_evidence_hashes(resource_usage: dict[str, Any]) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    for key in (
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "stage_trace_hash",
    ):
        if resource_usage.get(key):
            evidence[key] = resource_usage[key]
    stage_trace = resource_usage.get("stage_trace")
    if isinstance(stage_trace, (list, tuple)):
        evidence["stage_trace_count"] = len(stage_trace)
    return evidence


def _bounded_collection_evidence(value: Any) -> dict[str, Any]:
    if not isinstance(value, (list, tuple)):
        return {"repr": repr(value)}
    return {
        "item_count": len(value),
        "first_item_repr": repr(value[0]) if value else None,
        "last_item_repr": repr(value[-1]) if value else None,
    }


def summarize_resource_usage_for_candidate_artifact(resource_usage: Any) -> Any:
    if not isinstance(resource_usage, dict):
        return resource_usage
    summary: dict[str, Any] = {}
    for key, value in resource_usage.items():
        if key in {
            "applied_resource_limits",
            "memory_sampling_policy",
            "resource_policy",
            "strategy_diagnostics",
            "strategy_specific_diagnostics",
        }:
            summary[f"{key}_hash"] = sha256_prefixed(value)
            if isinstance(value, (dict, list, tuple)):
                summary[f"{key}_count"] = len(value)
            continue
        if key == "stage_trace":
            if isinstance(value, (list, tuple)):
                summary["stage_trace_count"] = len(value)
            if "stage_trace_hash" not in resource_usage:
                summary["stage_trace_hash"] = sha256_prefixed(
                    _bounded_collection_evidence(value),
                    label="stage_trace_bounded_evidence_hash",
                )
            continue
        if isinstance(value, dict):
            summary[key] = summarize_resource_usage_for_candidate_artifact(value)
            continue
        if isinstance(value, (list, tuple)):
            summary[f"{key}_count"] = len(value)
            summary[f"{key}_hash"] = sha256_prefixed(list(value))
            continue
        summary[key] = value
    return summary


def _scenario_result_summary(
    scenario: Any,
    *,
    include_closed_trade_summary: bool = False,
) -> dict[str, Any]:
    if not isinstance(scenario, dict):
        return {"scenario_repr_hash": sha256_prefixed({"repr": repr(scenario)}, label="scenario_repr_hash")}
    summary_keys = (
        "scenario_id",
        "scenario_index",
        "scenario_type",
        "scenario_role",
        "scenario_acceptance_gate_result",
        "scenario_fail_reasons",
        "validation_metrics",
        "validation_metrics_v2",
        "final_holdout_metrics",
        "final_holdout_metrics_v2",
        "walk_forward_metrics",
        "regime_gate_result",
        "market_regime_bucket_performance",
        "market_regime_coverage",
        "execution_model_hash",
        "model_params_hash",
        "execution_contract_hash",
        "execution_capability_contract_hash",
        "execution_reality_summary",
        "train_execution_event_summary",
        "validation_execution_event_summary",
        "final_holdout_execution_event_summary",
        "behavior_hash",
        "decision_behavior_hash",
        "trade_ledger_hash",
        "equity_curve_hash",
        "composite_behavior_hash",
        "common_decision_behavior_hash",
        "strategy_behavior_hash",
        "composite_behavior_hash_v2",
        "train_behavior_hash",
        "validation_behavior_hash",
        "final_holdout_behavior_hash",
        "candidate_failed",
        "candidate_failed_before_complete_metrics",
        "evaluation_status",
        "metrics_status",
        "metrics_v2_source",
        "failure_reason",
        "resource_guard",
        "failure_artifact_ref",
        "failure_artifact_path",
        "retained_detail_summary",
        "train_resource_usage",
        "validation_resource_usage",
        "final_holdout_resource_usage",
        "train_audit_trace_index",
        "validation_audit_trace_index",
        "final_holdout_audit_trace_index",
    )
    summary = {key: scenario[key] for key in summary_keys if key in scenario}
    _copy_compact_diagnostics(summary, scenario)
    for key in ("train_strategy_diagnostics", "final_holdout_strategy_diagnostics"):
        summary.pop(key, None)
    if include_closed_trade_summary:
        summary["validation_closed_trade_summary"] = _closed_trade_summary(
            scenario.get("validation_closed_trades")
        )
    _compact_candidate_artifact_summary(summary)
    for key in (
        "train_resource_usage",
        "validation_resource_usage",
        "final_holdout_resource_usage",
    ):
        if key in summary:
            summary[key] = summarize_resource_usage_for_candidate_artifact(summary[key])
    summary["train_equity_curve"] = []
    summary["validation_equity_curve"] = []
    summary["final_holdout_equity_curve"] = []
    summary["detail_artifact_ref"] = scenario.get("detail_artifact_ref")
    summary["scenario_payload_hash"] = sha256_prefixed(
        scenario_evidence_hash_inputs(scenario),
        label="candidate_result_scenario_evidence_hash",
    )
    _ensure_scenario_retained_detail_evidence(summary)
    return summary


def _copy_compact_diagnostics(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key in (
        "train_strategy_diagnostics",
        "validation_strategy_diagnostics",
        "final_holdout_strategy_diagnostics",
        "strategy_diagnostics",
    ):
        if key in source:
            target[key] = _compact_strategy_diagnostics(source.get(key))


def _compact_strategy_diagnostics(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    compact: dict[str, Any] = {}
    for key in (
        "schema_version",
        "raw_signal_count",
        "final_signal_count",
        "entry_signal_count",
        "entry_count",
        "exit_count",
        "blocked_filter_distribution",
        "entry_reason_distribution",
        "exit_reason_distribution",
        "exit_rule_distribution",
        "return_by_exit_reason",
        "avg_holding_minutes_by_exit_reason",
        "mae_mfe_by_exit_reason",
        "p95_mae_pct",
        "p95_mfe_pct",
        "worst_trade_mae_pct",
        "strategy_diagnostics_namespace",
    ):
        if key in value:
            compact[key] = value[key]
    for key, raw in (
        ("blocked_filter_distribution", value.get("blocked_filter_distribution")),
        ("entry_reason_distribution", value.get("entry_reason_distribution")),
        ("exit_reason_distribution", value.get("exit_reason_distribution")),
    ):
        compact.setdefault(key, dict(raw) if isinstance(raw, dict) else {})
    compact.setdefault("strategy_diagnostics_namespace", value.get("strategy_diagnostics_namespace"))
    return compact


def _closed_trade_summary(value: Any) -> dict[str, Any]:
    trades = [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []
    return {
        "closed_trade_count": len(trades),
        "closed_trade_hash": sha256_prefixed(trades, label="closed_trade_summary_hash"),
    }


def _strip_stage_trace_arrays(value: Any) -> Any:
    if isinstance(value, dict):
        stripped: dict[str, Any] = {}
        for key, item in value.items():
            if key == "stage_trace":
                if isinstance(item, (list, tuple)):
                    stripped["stage_trace_count"] = len(item)
                    stripped["stage_trace_hash"] = sha256_prefixed(
                        _bounded_collection_evidence(item),
                        label="stage_trace_bounded_evidence_hash",
                    )
                continue
            stripped[key] = _strip_stage_trace_arrays(item)
        return stripped
    if isinstance(value, list):
        return [_strip_stage_trace_arrays(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_stage_trace_arrays(item) for item in value)
    return value


def _ensure_scenario_retained_detail_evidence(summary: dict[str, Any]) -> None:
    if summary.get("equity_curve_hash") or summary.get("retained_detail_summary"):
        return
    summary["retained_detail_summary"] = {
        "detail_unavailable_reason": (
            summary.get("failure_reason")
            or summary.get("evaluation_status")
            or "summary_detail_not_retained"
        ),
        "scenario_payload_hash": summary["scenario_payload_hash"],
    }


def _compact_candidate_artifact_summary(summary: dict[str, Any]) -> None:
    for key in (
        "validation_metrics",
        "validation_metrics_v2",
        "final_holdout_metrics",
        "final_holdout_metrics_v2",
        "train_metrics",
        "train_metrics_v2",
        "walk_forward_metrics",
    ):
        if key in summary:
            summary[key] = _compact_metrics_payload(summary[key])
    for key in ("market_regime_bucket_performance", "market_regime_coverage"):
        if key in summary:
            summary[key] = _hashed_collection_summary(summary[key])
    if "execution_reality_summary" in summary:
        summary["execution_reality_summary"] = _compact_execution_reality_summary(
            summary["execution_reality_summary"]
        )
    if "regime_gate_result" in summary:
        summary["regime_gate_result"] = _compact_regime_gate_result(summary["regime_gate_result"])
    if "resource_guard" in summary:
        summary["resource_guard"] = _compact_resource_guard(summary["resource_guard"])
    if "retained_detail_summary" in summary:
        summary["retained_detail_summary"] = _compact_retained_detail_summary(
            summary["retained_detail_summary"]
        )
    if "position_sizing_sensitivity" in summary:
        summary["position_sizing_sensitivity"] = _compact_large_diagnostic_block(
            summary["position_sizing_sensitivity"],
            hash_key="position_sizing_sensitivity_hash",
            status_key="position_sizing_sensitivity_status",
        )
    if "execution_calibration_gate" in summary:
        summary["execution_calibration_gate"] = _compact_status_payload(
            summary["execution_calibration_gate"],
            hash_key="execution_calibration_gate_hash",
        )
    if "production_calibration_policy_result" in summary:
        summary["production_calibration_policy_result"] = _compact_status_payload(
            summary["production_calibration_policy_result"],
            hash_key="production_calibration_policy_hash",
        )


def _compact_large_diagnostic_block(value: Any, *, hash_key: str, status_key: str) -> Any:
    if not isinstance(value, dict):
        return value
    if _json_byte_count(value) <= 1500:
        return value
    compact: dict[str, Any] = {hash_key: sha256_prefixed(value)}
    if value.get("status"):
        compact[status_key] = value["status"]
    return compact


def _compact_retained_detail_summary(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    if len(value) <= 8:
        return value
    return {
        "retained_detail_summary_hash": sha256_prefixed(value),
        "retained_detail_summary_key_count": len(value),
        "report_detail": value.get("report_detail"),
        "decision_count": value.get("decision_count"),
        "retained_decision_count": value.get("retained_decision_count"),
        "retained_equity_point_count": value.get("retained_equity_point_count"),
        "retained_regime_snapshot_count": value.get("retained_regime_snapshot_count"),
    }


def _compact_metrics_payload(metrics: Any) -> Any:
    if not isinstance(metrics, dict):
        return metrics
    compact: dict[str, Any] = {}
    for key in (
        "metrics_schema_version",
        "metrics_status",
        "metrics_v2_source",
        "evaluation_status",
        "candidate_failed_before_complete_metrics",
    ):
        if key in metrics:
            compact[key] = metrics[key]
    if "limitation_reasons" in metrics:
        compact["limitation_reasons"] = metrics["limitation_reasons"]
    for section, keys in {
        "return_risk": (
            "total_return_pct",
            "realized_return_pct",
            "max_drawdown_pct",
            "cagr_pct",
            "open_position_at_end",
        ),
        "trade_quality": (
            "closed_trade_count",
            "execution_count",
            "profit_factor",
            "win_rate",
            "single_trade_dependency_score",
        ),
        "time_exposure": (
            "active_bar_count",
            "exposure_time_pct",
            "period_start_ts",
            "period_end_ts",
        ),
        "cost_execution": (
            "fee_total",
            "slippage_total",
            "filled_execution_count",
            "failed_execution_count",
            "skipped_execution_count",
        ),
    }.items():
        value = metrics.get(section)
        if isinstance(value, dict):
            compact[section] = {key: value[key] for key in keys if key in value}
    for key in (
        "return_pct",
        "max_drawdown_pct",
        "profit_factor",
        "profit_factor_unbounded",
        "trade_count",
        "win_rate",
        "fee_total",
        "slippage_total",
    ):
        if key in metrics:
            compact[key] = metrics[key]
    compact["metrics_payload_hash"] = sha256_prefixed(metrics)
    return compact


def _hashed_collection_summary(value: Any) -> Any:
    if not isinstance(value, (list, tuple)):
        return value
    return {
        "item_count": len(value),
        "payload_hash": sha256_prefixed(list(value)),
    }


def _compact_execution_reality_summary(summary: Any) -> Any:
    if not isinstance(summary, dict):
        return summary
    compact = {
        key: summary[key]
        for key in (
            "execution_reality_level",
            "execution_reality_gate_status",
            "execution_reality_gate_reasons",
            "execution_reference_policy",
            "signal_event_count",
            "fillable_signal_event_count",
            "filled_execution_count",
            "failed_execution_count",
            "skipped_execution_count",
            "pending_execution_count",
            "pending_execution_at_end_count",
        )
        if key in summary
    }
    compact["execution_reality_payload_hash"] = sha256_prefixed(summary)
    return compact


def _compact_regime_gate_result(result: Any) -> Any:
    if not isinstance(result, dict):
        return result
    compact = {
        key: result[key]
        for key in (
            "result",
            "status",
            "passed",
            "reasons",
            "allowed_live_regimes",
            "blocked_live_regimes",
        )
        if key in result
    }
    compact["regime_gate_payload_hash"] = sha256_prefixed(result)
    return compact


def _compact_resource_guard(resource_guard: Any) -> Any:
    if not isinstance(resource_guard, dict):
        return resource_guard
    compact = {
        key: resource_guard[key]
        for key in (
            "status",
            "reasons",
            "stage",
            "split",
            "scenario",
            "candles_processed",
            "decision_count",
            "signal_count",
            "trade_count",
            "closed_trade_count",
            "retained_decision_count",
            "retained_equity_point_count",
            "rss_delta_mb",
        )
        if key in resource_guard
    }
    compact["resource_guard_payload_hash"] = sha256_prefixed(resource_guard)
    return compact


def _compact_status_payload(payload: Any, *, hash_key: str) -> Any:
    if not isinstance(payload, dict):
        return payload
    compact = {
        key: payload[key]
        for key in (
            "status",
            "result",
            "passed",
            "required",
            "target",
            "policy_source",
            "operator_next_step",
            "reasons",
        )
        if key in payload
    }
    for key in ("scenario_gates", "artifact_hashes"):
        value = payload.get(key)
        if isinstance(value, (list, tuple)):
            compact[f"{key}_count"] = len(value)
    compact[hash_key] = sha256_prefixed(payload)
    return compact


def _json_byte_count(payload: dict[str, Any]) -> int:
    return len(
        json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False).encode("utf-8")
    ) + 1


def _current_total_bytes(store: ArtifactStore | ResearchArtifactContext) -> int:
    return int(getattr(store, "total_bytes"))


def _predicted_file_count(store: ArtifactStore | ResearchArtifactContext, *paths: Path) -> int:
    known_files = _known_files(store)
    resolved_paths = {path.resolve() for path in paths}
    if known_files is None:
        return int(getattr(store, "file_count")) + len(resolved_paths)
    return len(set(known_files) | resolved_paths)


def _known_files(store: ArtifactStore | ResearchArtifactContext) -> set[Path] | None:
    inner_store = getattr(store, "store", store)
    known_files = getattr(inner_store, "_known_files", None)
    if isinstance(known_files, set):
        return {Path(path).resolve() for path in known_files}
    return None


def _stable_report_byte_count(report_payload: dict[str, Any]) -> int:
    last = -1
    current = _json_byte_count(report_payload)
    while current != last:
        last = current
        report_payload["artifact_write_summary"]["report_bytes"] = current
        report_payload["artifact_observability"]["report_write"]["report_bytes"] = current
        current = _json_byte_count(report_payload)
    return current


def _stable_final_report_byte_count(
    report_payload: dict[str, Any],
    artifact_write_summary: dict[str, Any],
    *,
    artifact_total_bytes_base: int,
) -> int:
    last = -1
    _sync_report_write_stage(report_payload, artifact_write_summary)
    current = _json_byte_count(report_payload)
    while current != last:
        last = current
        artifact_write_summary["report_bytes"] = current
        artifact_write_summary["artifact_total_bytes"] = int(artifact_total_bytes_base) + current
        report_payload["artifact_write_summary"] = dict(artifact_write_summary)
        report_payload.setdefault("artifact_observability", {})["report_write"] = dict(artifact_write_summary)
        _sync_report_write_stage(report_payload, artifact_write_summary)
        report_payload["content_hash"] = sha256_prefixed(
            report_content_hash_payload(report_payload),
            label="stable_final_report_content_hash",
        )
        current = _json_byte_count(report_payload)
    return current


def _sync_report_write_stage(report_payload: dict[str, Any], artifact_write_summary: dict[str, Any]) -> None:
    execution_observability = report_payload.setdefault("execution_observability", {})
    if not isinstance(execution_observability, dict):
        return
    stage_timings = execution_observability.setdefault("stage_timings", [])
    if not isinstance(stage_timings, list):
        return
    found = False
    for stage_timing in stage_timings:
        if isinstance(stage_timing, dict) and stage_timing.get("stage") == "report_write":
            found = True
            stage_timing["artifact_total_bytes"] = artifact_write_summary["artifact_total_bytes"]
            stage_timing["artifact_file_count"] = artifact_write_summary["artifact_file_count"]
            stage_timing["derived_candidates_bytes"] = artifact_write_summary["derived_candidates_bytes"]
            stage_timing["report_bytes"] = artifact_write_summary["report_bytes"]
            stage_timing["finalization_wall_seconds"] = artifact_write_summary.get("finalization_wall_seconds")
            stage_timing["file_write_wall_seconds"] = artifact_write_summary.get("file_write_wall_seconds")
            stage_timing["hash_call_count"] = artifact_write_summary.get("hash_call_count")
            stage_timing["observed_hash_payload_bytes"] = artifact_write_summary.get("observed_hash_payload_bytes")
    if not found:
        stage_timings.append(
            {
                "stage": "report_write",
                "wall_seconds": round(float(artifact_write_summary.get("write_wall_seconds") or 0.0), 6),
                "artifact_total_bytes": artifact_write_summary["artifact_total_bytes"],
                "artifact_file_count": artifact_write_summary["artifact_file_count"],
                "derived_candidates_bytes": artifact_write_summary["derived_candidates_bytes"],
                "report_bytes": artifact_write_summary["report_bytes"],
                "finalization_wall_seconds": artifact_write_summary.get("finalization_wall_seconds"),
                "file_write_wall_seconds": artifact_write_summary.get("file_write_wall_seconds"),
                "hash_call_count": artifact_write_summary.get("hash_call_count"),
                "observed_hash_payload_bytes": artifact_write_summary.get("observed_hash_payload_bytes"),
            }
        )


def _sync_report_write_substages(
    report_payload: dict[str, Any],
    artifact_write_summary: dict[str, Any],
) -> None:
    substages = artifact_write_summary.get("substage_timings")
    if not isinstance(substages, list):
        return
    execution_observability = report_payload.setdefault("execution_observability", {})
    stage_timings = execution_observability.setdefault("stage_timings", [])
    if not isinstance(stage_timings, list):
        return
    existing = {
        item.get("stage")
        for item in stage_timings
        if isinstance(item, dict) and str(item.get("stage") or "").startswith("report_write.")
    }
    for substage in substages:
        if not isinstance(substage, dict):
            continue
        name = str(substage.get("stage") or "").strip()
        if not name:
            continue
        stage_name = f"report_write.{name}"
        if stage_name in existing:
            continue
        stage_timings.append({"stage": stage_name, **{k: v for k, v in substage.items() if k != "stage"}})
        existing.add(stage_name)


def _sync_workload_estimate_comparison(
    report_payload: dict[str, Any],
    artifact_write_summary: dict[str, Any],
) -> None:
    estimate = report_payload.get("workload_estimate")
    if not isinstance(estimate, dict):
        execution_plan = report_payload.get("execution_plan")
        estimate = execution_plan.get("workload_estimate") if isinstance(execution_plan, dict) else None
    if not isinstance(estimate, dict):
        return
    estimated_hash = _optional_int(estimate.get("estimated_hash_payload_bytes"))
    estimated_artifact = _optional_int(estimate.get("estimated_artifact_bytes"))
    observed_hash = _optional_int(artifact_write_summary.get("observed_hash_payload_bytes"))
    observed_artifact = _optional_int(artifact_write_summary.get("artifact_total_bytes"))
    observed_seconds = artifact_write_summary.get("observed_report_finalization_seconds")
    comparison: dict[str, Any] = {
        "schema_version": 1,
        "estimated_hash_payload_bytes": estimated_hash,
        "observed_hash_payload_bytes": observed_hash,
        "observed_hash_call_count": artifact_write_summary.get("observed_hash_call_count"),
        "observed_largest_hash_payload_bytes": artifact_write_summary.get("observed_largest_hash_payload_bytes"),
        "estimated_artifact_bytes": estimated_artifact,
        "observed_artifact_bytes": observed_artifact,
        "observed_report_finalization_seconds": observed_seconds,
        "status": "UNKNOWN",
        "reasons": [],
    }
    reasons: list[str] = []
    if estimated_hash is not None and observed_hash is not None:
        comparison["hash_payload_ratio"] = observed_hash / estimated_hash if estimated_hash > 0 else None
        if estimated_hash <= 0:
            reasons.append("estimated_hash_payload_bytes_zero")
            comparison["status"] = "WARN"
        elif observed_hash <= estimated_hash * 2:
            comparison["status"] = "PASS"
        elif observed_hash <= estimated_hash * 5:
            comparison["status"] = "WARN"
            reasons.append("observed_hash_payload_bytes_exceeds_2x_estimate")
        else:
            comparison["status"] = "FAIL"
            reasons.append("observed_hash_payload_bytes_exceeds_5x_estimate")
    else:
        reasons.append("hash_payload_estimate_or_observation_missing")
    if estimated_artifact is None or observed_artifact is None:
        reasons.append("artifact_byte_estimate_or_observation_missing")
    comparison["reasons"] = reasons
    artifact_write_summary["observed_artifact_bytes"] = observed_artifact
    report_payload["workload_estimate_comparison"] = comparison


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _ensure_research_output_path_allowed(manager: PathManager, path: Path) -> None:
    project_root = manager.project_root.resolve()
    resolved = path.resolve()
    if PathManager._is_within(resolved, project_root):
        raise PathPolicyError(f"research output path must be outside repository: {resolved}")


def _relative_artifact_ref(path: Path, data_dir: Path) -> str:
    return path.resolve().relative_to(data_dir).as_posix()
