from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from bithumb_bot.paths import PathManager
from bithumb_bot.storage_io import append_jsonl

from .hashing import content_hash_payload, sha256_prefixed


EXPERIMENT_REGISTRY_SCHEMA_VERSION = 1
EMPTY_EXPERIMENT_REGISTRY_HASH = sha256_prefixed([])
PROMOTION_PERMITTED_STATUSES = {"COMPLETED"}
EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE = "pre_completion_evidence_hash"
PRE_CONTENT_COMPLETION_BOUND_FIELDS = {
    "dataset_content_hash",
    "dataset_quality_hash",
    "final_holdout_split_hash",
    "final_holdout_content_hash",
}


def experiment_registry_path(*, manager: PathManager) -> Path:
    path = manager.data_dir() / "reports" / "research" / "_registry" / "experiment_registry.jsonl"
    project_root = manager.project_root.resolve()
    if PathManager._is_within(path.resolve(), project_root):
        raise ValueError(f"experiment registry path must be outside repository: {path.resolve()}")
    return path


def registry_content_hash(path: Path) -> str:
    rows = load_experiment_registry_rows(path)
    return sha256_prefixed(rows) if rows else EMPTY_EXPERIMENT_REGISTRY_HASH


def row_hash_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key != "row_hash"}


def compute_row_hash(row: dict[str, Any]) -> str:
    return sha256_prefixed(content_hash_payload(row_hash_payload(row)))


def research_freedom_hash(payload: dict[str, Any]) -> str:
    return sha256_prefixed(
        {
            "experiment_family_id": payload.get("experiment_family_id"),
            "hypothesis_id": payload.get("hypothesis_id"),
            "hypothesis_status": payload.get("hypothesis_status"),
            "dataset_snapshot_id": payload.get("dataset_snapshot_id"),
            "train_split_hash": payload.get("train_split_hash"),
            "validation_split_hash": payload.get("validation_split_hash"),
            "final_holdout_split_hash": payload.get("final_holdout_split_hash"),
            "final_holdout_fingerprint": payload.get("final_holdout_fingerprint"),
            "final_holdout_identity_hash": payload.get("final_holdout_identity_hash"),
            "final_holdout_content_hash": payload.get("final_holdout_content_hash"),
            "final_holdout_reuse_key_hash": payload.get("final_holdout_reuse_key_hash"),
            "parameter_space_hash": payload.get("parameter_space_hash"),
            "computed_attempt_index": payload.get("computed_attempt_index"),
            "computed_holdout_reuse_count": payload.get("computed_holdout_reuse_count"),
            "experiment_registry_path": payload.get("experiment_registry_path") or payload.get("path"),
            "experiment_registry_prior_hash": payload.get("experiment_registry_prior_hash")
            or payload.get("prior_registry_hash"),
            "experiment_registry_row_hash": payload.get("experiment_registry_row_hash") or payload.get("row_hash"),
        }
    )


def research_identity_from_manifest(manifest: Any) -> dict[str, Any]:
    raw = getattr(manifest, "raw", {}) if isinstance(getattr(manifest, "raw", {}), dict) else {}
    experiment_id = str(getattr(manifest, "experiment_id", "") or raw.get("experiment_id") or "")
    explicit_family = raw.get("experiment_family_id")
    explicit_hypothesis = raw.get("hypothesis_id")
    manifest_hypothesis = getattr(manifest, "hypothesis", None)
    family_id = str(explicit_family or experiment_id)
    hypothesis_id = str(explicit_hypothesis or manifest_hypothesis or experiment_id)
    return {
        "experiment_family_id": family_id,
        "hypothesis_id": hypothesis_id,
        "hypothesis_status": str(raw.get("hypothesis_status") or "pre_registered"),
        "hypothesis_identity_source": (
            "manifest.hypothesis_id"
            if explicit_hypothesis
            else "manifest.hypothesis"
            if manifest_hypothesis
            else "experiment_id"
        ),
        "experiment_family_identity_source": "manifest.experiment_family_id" if explicit_family else "experiment_id",
        "experiment_id": experiment_id,
    }


def final_holdout_identity_hash_from_parts(
    *,
    dataset_source: str | None,
    market: str | None,
    interval: str | None,
    final_holdout: dict[str, Any] | None,
) -> str:
    return sha256_prefixed(
        {
            "dataset_source": dataset_source,
            "market": market,
            "interval": interval,
            "final_holdout_start": (final_holdout or {}).get("start"),
            "final_holdout_end": (final_holdout or {}).get("end"),
        }
    )


def final_holdout_content_hash_from_parts(
    *,
    dataset_snapshot_id: str | None,
    final_holdout_split_hash: str | None,
    dataset_quality_hash: str | None,
) -> str:
    return sha256_prefixed(
        {
            "dataset_snapshot_id": dataset_snapshot_id,
            "final_holdout_split_hash": final_holdout_split_hash,
            "dataset_quality_hash": dataset_quality_hash,
        }
    )


def final_holdout_hashes_from_manifest(
    *,
    manifest: Any,
    final_holdout_split_hash: str | None,
    dataset_quality_hash: str | None,
) -> dict[str, str | None]:
    dataset = getattr(manifest, "dataset", None)
    split = getattr(dataset, "split", None)
    final_holdout = getattr(split, "final_holdout", None)
    holdout_payload = final_holdout.as_dict() if final_holdout is not None else None
    identity_hash = final_holdout_identity_hash_from_parts(
        dataset_source=getattr(dataset, "source", None),
        market=getattr(manifest, "market", None),
        interval=getattr(manifest, "interval", None),
        final_holdout=holdout_payload,
    )
    content_hash = final_holdout_content_hash_from_parts(
        dataset_snapshot_id=getattr(dataset, "snapshot_id", None),
        final_holdout_split_hash=final_holdout_split_hash,
        dataset_quality_hash=dataset_quality_hash,
    )
    return {
        "final_holdout_identity_hash": identity_hash,
        "final_holdout_content_hash": content_hash,
        "final_holdout_reuse_key_hash": identity_hash,
        "final_holdout_fingerprint": identity_hash,
    }


def load_experiment_registry_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            payload = json.loads(text)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def compute_research_attempt_counters(
    *,
    manager: PathManager,
    base_payload: dict[str, Any],
) -> dict[str, int]:
    path = experiment_registry_path(manager=manager)
    rows = load_experiment_registry_rows(path)
    family_id = str(base_payload.get("experiment_family_id") or "")
    hypothesis_id = str(base_payload.get("hypothesis_id") or "")
    reuse_key = str(
        base_payload.get("final_holdout_reuse_key_hash")
        or base_payload.get("final_holdout_identity_hash")
        or base_payload.get("final_holdout_fingerprint")
        or ""
    )
    return {
        "computed_attempt_index": 1
        + sum(
            1
            for row in rows
            if row.get("event_type") == "research_attempt_reserved"
            and str(row.get("experiment_family_id") or "") == family_id
            and str(row.get("hypothesis_id") or "") == hypothesis_id
        ),
        "computed_holdout_reuse_count": sum(
            1
            for row in rows
            if row.get("event_type") == "research_attempt_reserved"
            and reuse_key
            and str(
                row.get("final_holdout_reuse_key_hash")
                or row.get("final_holdout_identity_hash")
                or row.get("final_holdout_fingerprint")
                or ""
            )
            == reuse_key
        ),
    }


def append_research_attempt_rejected(
    *,
    manager: PathManager,
    base_payload: dict[str, Any],
    reasons: list[str],
    computed_attempt_index: int,
    computed_holdout_reuse_count: int,
    created_at: str | None = None,
) -> dict[str, Any]:
    path = experiment_registry_path(manager=manager)
    with _locked_registry(path):
        rows = load_experiment_registry_rows(path)
        prior_hash = sha256_prefixed(rows) if rows else EMPTY_EXPERIMENT_REGISTRY_HASH
        row = {
            "schema_version": EXPERIMENT_REGISTRY_SCHEMA_VERSION,
            "event_type": "research_attempt_rejected",
            **base_payload,
            "computed_attempt_index": computed_attempt_index,
            "computed_holdout_reuse_count": computed_holdout_reuse_count,
            "result_status": "REJECTED",
            "rejection_reasons": list(reasons),
            "counted_attempt": False,
            "prior_registry_hash": prior_hash,
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        }
        row["row_hash"] = compute_row_hash(row)
        append_jsonl(path, row)
    return {"path": str(path.resolve()), "prior_hash": prior_hash, "row_hash": str(row["row_hash"]), "row": dict(row)}


def reserve_research_attempt(
    *,
    manager: PathManager,
    base_payload: dict[str, Any],
    created_at: str | None = None,
) -> dict[str, Any]:
    path = experiment_registry_path(manager=manager)
    with _locked_registry(path):
        rows = load_experiment_registry_rows(path)
        prior_hash = sha256_prefixed(rows) if rows else EMPTY_EXPERIMENT_REGISTRY_HASH
        counters = _compute_research_attempt_counters_from_rows(rows=rows, base_payload=base_payload)
        computed_attempt_index = counters["computed_attempt_index"]
        computed_holdout_reuse_count = counters["computed_holdout_reuse_count"]
        row = {
            "schema_version": EXPERIMENT_REGISTRY_SCHEMA_VERSION,
            "event_type": "research_attempt_reserved",
            **base_payload,
            "computed_attempt_index": computed_attempt_index,
            "computed_holdout_reuse_count": computed_holdout_reuse_count,
            "result_status": "IN_PROGRESS",
            "prior_registry_hash": prior_hash,
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        }
        row["row_hash"] = compute_row_hash(row)
        append_jsonl(path, row)
    result = {
        "path": str(path.resolve()),
        "prior_hash": prior_hash,
        "row_hash": str(row["row_hash"]),
        "row": dict(row),
        "computed_attempt_index": computed_attempt_index,
        "computed_holdout_reuse_count": computed_holdout_reuse_count,
    }
    result["research_freedom_hash"] = research_freedom_hash(
        {
            **row,
            "experiment_registry_path": result["path"],
            "experiment_registry_prior_hash": prior_hash,
            "experiment_registry_row_hash": row["row_hash"],
        }
    )
    return result


def reserve_research_attempt_checked(
    *,
    manager: PathManager,
    base_payload: dict[str, Any],
    statistical_validation_contract: dict[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    path = experiment_registry_path(manager=manager)
    with _locked_registry(path):
        rows = load_experiment_registry_rows(path)
        prior_hash = sha256_prefixed(rows) if rows else EMPTY_EXPERIMENT_REGISTRY_HASH
        counters = _compute_research_attempt_counters_from_rows(rows=rows, base_payload=base_payload)
        computed_attempt_index = counters["computed_attempt_index"]
        computed_holdout_reuse_count = counters["computed_holdout_reuse_count"]
        reasons = _checked_reservation_reasons(
            base_payload=base_payload,
            computed_attempt_index=computed_attempt_index,
            computed_holdout_reuse_count=computed_holdout_reuse_count,
            statistical_validation_contract=statistical_validation_contract,
        )
        if reasons:
            row = {
                "schema_version": EXPERIMENT_REGISTRY_SCHEMA_VERSION,
                "event_type": "research_attempt_rejected",
                **base_payload,
                "computed_attempt_index": computed_attempt_index,
                "computed_holdout_reuse_count": computed_holdout_reuse_count,
                "result_status": "REJECTED",
                "rejection_reasons": sorted(set(reasons)),
                "counted_attempt": False,
                "prior_registry_hash": prior_hash,
                "created_at": created_at or datetime.now(timezone.utc).isoformat(),
            }
            row["row_hash"] = compute_row_hash(row)
            append_jsonl(path, row)
            return {
                "accepted": False,
                "path": str(path.resolve()),
                "prior_hash": prior_hash,
                "row_hash": str(row["row_hash"]),
                "row": dict(row),
                "computed_attempt_index": computed_attempt_index,
                "computed_holdout_reuse_count": computed_holdout_reuse_count,
                "reasons": list(row["rejection_reasons"]),
            }
        row = {
            "schema_version": EXPERIMENT_REGISTRY_SCHEMA_VERSION,
            "event_type": "research_attempt_reserved",
            **base_payload,
            "computed_attempt_index": computed_attempt_index,
            "computed_holdout_reuse_count": computed_holdout_reuse_count,
            "result_status": "IN_PROGRESS",
            "prior_registry_hash": prior_hash,
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        }
        row["row_hash"] = compute_row_hash(row)
        append_jsonl(path, row)
    result = {
        "accepted": True,
        "path": str(path.resolve()),
        "prior_hash": prior_hash,
        "row_hash": str(row["row_hash"]),
        "row": dict(row),
        "computed_attempt_index": computed_attempt_index,
        "computed_holdout_reuse_count": computed_holdout_reuse_count,
    }
    result["research_freedom_hash"] = research_freedom_hash(
        {
            **row,
            "experiment_registry_path": result["path"],
            "experiment_registry_prior_hash": prior_hash,
            "experiment_registry_row_hash": row["row_hash"],
        }
    )
    return result


def append_attempt_aborted(
    *,
    manager: PathManager,
    reservation_row_hash: str,
    reason: str,
    created_at: str | None = None,
) -> dict[str, Any] | None:
    path = experiment_registry_path(manager=manager)
    with _locked_registry(path):
        rows = load_experiment_registry_rows(path)
        reservation = next((row for row in rows if row.get("row_hash") == reservation_row_hash), None)
        if not isinstance(reservation, dict):
            return None
        prior_hash = sha256_prefixed(rows) if rows else EMPTY_EXPERIMENT_REGISTRY_HASH
        row = {
            "schema_version": EXPERIMENT_REGISTRY_SCHEMA_VERSION,
            "event_type": "research_attempt_aborted",
            **{
                key: value
                for key, value in reservation.items()
                if key not in {"event_type", "result_status", "prior_registry_hash", "row_hash", "created_at"}
            },
            "reservation_row_hash": reservation_row_hash,
            "result_status": "ABORTED",
            "abort_reason": reason,
            "prior_registry_hash": prior_hash,
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        }
        row["row_hash"] = compute_row_hash(row)
        append_jsonl(path, row)
    return {"path": str(path.resolve()), "prior_hash": prior_hash, "row_hash": str(row["row_hash"]), "row": dict(row)}


def append_attempt_completion(
    *,
    manager: PathManager,
    reservation: dict[str, Any],
    updates: dict[str, Any],
    result_status: str = "COMPLETED",
    created_at: str | None = None,
) -> dict[str, Any]:
    path = experiment_registry_path(manager=manager)
    reservation_row = reservation.get("row") if isinstance(reservation.get("row"), dict) else {}
    with _locked_registry(path):
        rows = load_experiment_registry_rows(path)
        prior_hash = sha256_prefixed(rows) if rows else EMPTY_EXPERIMENT_REGISTRY_HASH
        row = {
            "schema_version": EXPERIMENT_REGISTRY_SCHEMA_VERSION,
            "event_type": "research_attempt_completed",
            **{key: value for key, value in reservation_row.items() if key not in {"event_type", "result_status", "prior_registry_hash", "row_hash", "created_at"}},
            **updates,
            "reservation_row_hash": reservation.get("row_hash") or reservation_row.get("row_hash"),
            "result_status": result_status,
            "prior_registry_hash": prior_hash,
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        }
        row["row_hash"] = compute_row_hash(row)
        append_jsonl(path, row)
    return {"path": str(path.resolve()), "prior_hash": prior_hash, "row_hash": str(row["row_hash"]), "row": dict(row)}


def append_promotion_registry_event(
    *,
    manager: PathManager,
    reservation_row_hash: str,
    promotion_artifact_hash: str,
    promoted_candidate_id: str,
    created_at: str | None = None,
) -> dict[str, Any] | None:
    path = experiment_registry_path(manager=manager)
    with _locked_registry(path):
        rows = load_experiment_registry_rows(path)
        reservation = next((row for row in rows if row.get("row_hash") == reservation_row_hash), None)
        if not isinstance(reservation, dict):
            return None
        prior_hash = sha256_prefixed(rows) if rows else EMPTY_EXPERIMENT_REGISTRY_HASH
        row = {
            "schema_version": EXPERIMENT_REGISTRY_SCHEMA_VERSION,
            "event_type": "research_attempt_promoted",
            "reservation_row_hash": reservation_row_hash,
            "experiment_id": reservation.get("experiment_id"),
            "experiment_family_id": reservation.get("experiment_family_id"),
            "hypothesis_id": reservation.get("hypothesis_id"),
            "promotion_artifact_hash": promotion_artifact_hash,
            "promoted_candidate_id": promoted_candidate_id,
            "result_status": "PROMOTED",
            "prior_registry_hash": prior_hash,
            "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        }
        row["row_hash"] = compute_row_hash(row)
        append_jsonl(path, row)
    return {"path": str(path.resolve()), "prior_hash": prior_hash, "row_hash": str(row["row_hash"]), "row": dict(row)}


def validate_experiment_registry_binding(
    *,
    report: dict[str, Any],
    evidence: dict[str, Any] | None = None,
    promotion: dict[str, Any] | None = None,
    require_complete: bool = False,
) -> list[str]:
    source = evidence if isinstance(evidence, dict) else report
    promotion = promotion if isinstance(promotion, dict) else {}
    reasons: list[str] = []
    path_value = str(
        source.get("experiment_registry_path")
        or report.get("experiment_registry_path")
        or promotion.get("experiment_registry_path")
        or ""
    ).strip()
    row_hash = str(
        source.get("experiment_registry_row_hash")
        or report.get("experiment_registry_row_hash")
        or promotion.get("experiment_registry_row_hash")
        or ""
    ).strip()
    prior_hash = str(
        source.get("experiment_registry_prior_hash")
        or report.get("experiment_registry_prior_hash")
        or promotion.get("experiment_registry_prior_hash")
        or ""
    ).strip()
    if not path_value:
        return ["experiment_registry_path_missing"]
    if not row_hash.startswith("sha256:"):
        return ["experiment_registry_row_hash_missing"]
    path = Path(path_value).expanduser()
    if not path.exists():
        return ["experiment_registry_missing"]
    try:
        rows = load_experiment_registry_rows(path)
    except (OSError, json.JSONDecodeError):
        return ["experiment_registry_missing"]
    row_index = next((index for index, row in enumerate(rows) if row.get("row_hash") == row_hash), None)
    if row_index is None:
        return ["experiment_registry_row_hash_mismatch"]
    row = rows[row_index]
    if compute_row_hash(row) != row_hash:
        reasons.append("experiment_registry_row_hash_mismatch")
    expected_prior = sha256_prefixed(rows[:row_index]) if row_index else EMPTY_EXPERIMENT_REGISTRY_HASH
    if str(row.get("prior_registry_hash") or "") != expected_prior or (prior_hash and prior_hash != expected_prior):
        reasons.append("experiment_registry_prior_hash_mismatch")
    completion_hash = str(
        source.get("experiment_registry_completion_row_hash")
        or report.get("experiment_registry_completion_row_hash")
        or promotion.get("experiment_registry_completion_row_hash")
        or ""
    ).strip()
    completion = _completion_for_reservation(rows, row_hash, completion_hash)
    _extend_registry_field_mismatch_reasons(
        reasons,
        row=row,
        completion=completion,
        report=report,
        evidence=evidence,
        promotion=promotion,
    )
    if require_complete:
        if not isinstance(completion, dict):
            reasons.append("experiment_registry_incomplete_attempt")
        elif compute_row_hash(completion) != completion.get("row_hash"):
            reasons.append("experiment_registry_row_hash_mismatch")
        elif str(completion.get("result_status") or "") not in PROMOTION_PERMITTED_STATUSES:
            reasons.append("experiment_registry_incomplete_attempt")
        elif str(completion.get("reservation_row_hash") or "") != row_hash:
            reasons.append("experiment_registry_stale")
    if completion_hash and not isinstance(completion, dict):
        reasons.append("experiment_registry_row_hash_mismatch")
    if isinstance(completion, dict):
        _extend_completion_mismatch_reasons(
            reasons,
            completion=completion,
            report=report,
            evidence=evidence,
            promotion=promotion,
        )
    _extend_declared_counter_reasons(reasons, report=report, evidence=evidence)
    _extend_budget_reasons(reasons, report=report, evidence=evidence)
    return sorted(set(reasons))


def _extend_registry_field_mismatch_reasons(
    reasons: list[str],
    *,
    row: dict[str, Any],
    completion: dict[str, Any] | None,
    report: dict[str, Any],
    evidence: dict[str, Any] | None,
    promotion: dict[str, Any],
) -> None:
    evidence = evidence if isinstance(evidence, dict) else {}
    completion = completion if isinstance(completion, dict) else {}
    content_pending = bool(row.get("final_holdout_content_pending_until_completion"))
    for field in (
        "experiment_id",
        "experiment_family_id",
        "hypothesis_id",
        "hypothesis_status",
        "hypothesis_identity_source",
        "experiment_family_identity_source",
        "manifest_hash",
        "dataset_snapshot_id",
        "dataset_content_hash",
        "dataset_quality_hash",
        "train_split_hash",
        "validation_split_hash",
        "final_holdout_split_hash",
        "final_holdout_identity_hash",
        "final_holdout_content_hash",
        "final_holdout_reuse_key_hash",
        "parameter_space_hash",
    ):
        expected = evidence.get(field)
        if expected is None:
            expected = report.get(field)
        if expected is None:
            expected = promotion.get(field)
        actual = row.get(field)
        if content_pending and field in PRE_CONTENT_COMPLETION_BOUND_FIELDS and actual is None:
            actual = completion.get(field)
        if expected is not None and str(row.get(field) or "") != str(expected or ""):
            if not (content_pending and field in PRE_CONTENT_COMPLETION_BOUND_FIELDS and str(actual or "") == str(expected or "")):
                reasons.append("experiment_registry_stale")
                break
        if expected is None and row.get(field) is not None and field.endswith("_identity_source"):
            reasons.append("experiment_registry_identity_source_missing")
            break
    fingerprint = evidence.get("final_holdout_fingerprint") or report.get("final_holdout_fingerprint") or promotion.get("final_holdout_fingerprint")
    if fingerprint is not None and str(row.get("final_holdout_fingerprint") or "") != str(fingerprint or ""):
        reasons.append("experiment_registry_final_holdout_fingerprint_mismatch")
    identity = evidence.get("final_holdout_identity_hash") or report.get("final_holdout_identity_hash") or promotion.get("final_holdout_identity_hash")
    if identity is not None and str(row.get("final_holdout_identity_hash") or "") != str(identity or ""):
        reasons.append("experiment_registry_final_holdout_identity_mismatch")
    content = evidence.get("final_holdout_content_hash") or report.get("final_holdout_content_hash") or promotion.get("final_holdout_content_hash")
    actual_content = row.get("final_holdout_content_hash")
    if content_pending and actual_content is None:
        actual_content = completion.get("final_holdout_content_hash")
    if content is not None and str(actual_content or "") != str(content or ""):
        reasons.append("experiment_registry_final_holdout_content_mismatch")
    reuse_key = evidence.get("final_holdout_reuse_key_hash") or report.get("final_holdout_reuse_key_hash") or promotion.get("final_holdout_reuse_key_hash")
    if reuse_key is not None and str(row.get("final_holdout_reuse_key_hash") or row.get("final_holdout_identity_hash") or "") != str(reuse_key or ""):
        reasons.append("experiment_registry_final_holdout_reuse_key_mismatch")
    for field, code in (
        ("computed_attempt_index", "experiment_registry_attempt_index_mismatch"),
        ("computed_holdout_reuse_count", "experiment_registry_holdout_reuse_count_mismatch"),
    ):
        expected = evidence.get(field)
        if expected is None:
            expected = report.get(field)
        if expected is None:
            expected = promotion.get(field)
        if expected is not None and str(row.get(field) or "") != str(expected or ""):
            reasons.append(code)
    if promotion:
        for field in ("return_panel_hash", "statistical_evidence_hash", "candidate_count"):
            expected = promotion.get(field)
            if expected is not None and row.get(field) is not None and str(row.get(field) or "") != str(expected or ""):
                reasons.append("experiment_registry_stale")


def _extend_declared_counter_reasons(
    reasons: list[str],
    *,
    report: dict[str, Any],
    evidence: dict[str, Any] | None,
) -> None:
    evidence = evidence if isinstance(evidence, dict) else {}
    for declared_field, computed_field, code in (
        ("declared_attempt_index", "computed_attempt_index", "declared_attempt_index_mismatch"),
        ("declared_holdout_reuse_count", "computed_holdout_reuse_count", "declared_holdout_reuse_count_mismatch"),
    ):
        declared = evidence.get(declared_field)
        if declared is None:
            declared = report.get(declared_field)
        computed = evidence.get(computed_field)
        if computed is None:
            computed = report.get(computed_field)
        if declared is not None and computed is not None and str(declared) != str(computed):
            reasons.append(code)


def _extend_completion_mismatch_reasons(
    reasons: list[str],
    *,
    completion: dict[str, Any],
    report: dict[str, Any],
    evidence: dict[str, Any] | None,
    promotion: dict[str, Any],
) -> None:
    evidence = evidence if isinstance(evidence, dict) else {}
    for field in ("return_panel_hash", "candidate_count"):
        expected = evidence.get(field)
        if expected is None:
            expected = report.get(field)
        if expected is None:
            expected = promotion.get(field)
        actual = completion.get(field)
        if expected is not None and actual is not None and str(actual or "") != str(expected or ""):
            reasons.append("experiment_registry_stale")
    phase = str(completion.get("statistical_evidence_hash_phase") or "").strip()
    if phase != EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE:
        reasons.append("experiment_registry_evidence_hash_phase_mismatch")
    if evidence:
        bound = str(evidence.get("experiment_registry_bound_evidence_hash") or "").strip()
        if not bound.startswith("sha256:"):
            reasons.append("experiment_registry_bound_evidence_hash_missing")
        elif str(completion.get("statistical_evidence_hash") or "") != bound:
            reasons.append("experiment_registry_statistical_evidence_hash_mismatch")
        evidence_phase = str(evidence.get("experiment_registry_evidence_hash_phase") or "").strip()
        if evidence_phase != EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE:
            reasons.append("experiment_registry_evidence_hash_phase_mismatch")
    promotion_bound = str(promotion.get("experiment_registry_bound_evidence_hash") or "").strip()
    if promotion_bound and str(completion.get("statistical_evidence_hash") or "") != promotion_bound:
        reasons.append("experiment_registry_statistical_evidence_hash_mismatch")


def _extend_budget_reasons(
    reasons: list[str],
    *,
    report: dict[str, Any],
    evidence: dict[str, Any] | None,
) -> None:
    contract = (evidence or {}).get("statistical_validation_contract") if isinstance(evidence, dict) else None
    if not isinstance(contract, dict):
        contract = report.get("statistical_validation_contract")
    gates = contract.get("gates") if isinstance(contract, dict) else None
    if not isinstance(gates, dict):
        return
    attempt = _as_int((evidence or {}).get("computed_attempt_index") if isinstance(evidence, dict) else None)
    if attempt is None:
        attempt = _as_int(report.get("computed_attempt_index"))
    reuse = _as_int((evidence or {}).get("computed_holdout_reuse_count") if isinstance(evidence, dict) else None)
    if reuse is None:
        reuse = _as_int(report.get("computed_holdout_reuse_count"))
    max_attempt = _as_int(gates.get("max_attempt_index_without_new_hypothesis"))
    max_reuse = _as_int(gates.get("max_holdout_reuse_count"))
    if attempt is not None and max_attempt is not None and attempt > max_attempt:
        reasons.append("experiment_registry_budget_exceeded")
        reasons.append("attempt_budget_exceeded")
    if reuse is not None and max_reuse is not None and reuse > max_reuse:
        reasons.append("experiment_registry_budget_exceeded")
        reasons.append("holdout_reuse_budget_exceeded")


def _completion_for_reservation(
    rows: list[dict[str, Any]],
    reservation_row_hash: str,
    completion_hash: str,
) -> dict[str, Any] | None:
    for row in reversed(rows):
        if row.get("event_type") != "research_attempt_completed":
            continue
        if str(row.get("reservation_row_hash") or "") != reservation_row_hash:
            continue
        if completion_hash and str(row.get("row_hash") or "") != completion_hash:
            continue
        return row
    return None


def _compute_research_attempt_counters_from_rows(
    *,
    rows: list[dict[str, Any]],
    base_payload: dict[str, Any],
) -> dict[str, int]:
    family_id = str(base_payload.get("experiment_family_id") or "")
    hypothesis_id = str(base_payload.get("hypothesis_id") or "")
    reuse_key = str(
        base_payload.get("final_holdout_reuse_key_hash")
        or base_payload.get("final_holdout_identity_hash")
        or base_payload.get("final_holdout_fingerprint")
        or ""
    )
    return {
        "computed_attempt_index": 1
        + sum(
            1
            for row in rows
            if row.get("event_type") == "research_attempt_reserved"
            and str(row.get("experiment_family_id") or "") == family_id
            and str(row.get("hypothesis_id") or "") == hypothesis_id
        ),
        "computed_holdout_reuse_count": sum(
            1
            for row in rows
            if row.get("event_type") == "research_attempt_reserved"
            and reuse_key
            and str(
                row.get("final_holdout_reuse_key_hash")
                or row.get("final_holdout_identity_hash")
                or row.get("final_holdout_fingerprint")
                or ""
            )
            == reuse_key
        ),
    }


def _checked_reservation_reasons(
    *,
    base_payload: dict[str, Any],
    computed_attempt_index: int,
    computed_holdout_reuse_count: int,
    statistical_validation_contract: dict[str, Any] | None,
) -> list[str]:
    reasons: list[str] = []
    declared_attempt = _as_int(base_payload.get("declared_attempt_index"))
    declared_reuse = _as_int(base_payload.get("declared_holdout_reuse_count"))
    if declared_attempt is not None and declared_attempt != computed_attempt_index:
        reasons.append("declared_attempt_index_mismatch")
    if declared_reuse is not None and declared_reuse != computed_holdout_reuse_count:
        reasons.append("declared_holdout_reuse_count_mismatch")
    gates = statistical_validation_contract.get("gates") if isinstance(statistical_validation_contract, dict) else None
    if isinstance(gates, dict):
        max_attempt = _as_int(gates.get("max_attempt_index_without_new_hypothesis"))
        max_reuse = _as_int(gates.get("max_holdout_reuse_count"))
        if max_attempt is not None and computed_attempt_index > max_attempt:
            reasons.extend(["experiment_registry_budget_exceeded", "attempt_budget_exceeded"])
        if max_reuse is not None and computed_holdout_reuse_count > max_reuse:
            reasons.extend(["experiment_registry_budget_exceeded", "holdout_reuse_budget_exceeded"])
    return sorted(set(reasons))


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


@contextmanager
def _locked_registry(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX)
        except ImportError:
            pass
        yield
    finally:
        try:
            try:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_UN)
            except ImportError:
                pass
        finally:
            os.close(fd)
