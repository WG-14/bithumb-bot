from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager
from bithumb_bot.storage_io import append_jsonl

from .hashing import content_hash_payload, sha256_prefixed


FAMILY_TRIAL_REGISTRY_SCHEMA_VERSION = 1
EMPTY_REGISTRY_HASH = sha256_prefixed([])


def family_trial_registry_path(*, manager: PathManager, experiment_family_id: str) -> Path:
    safe_family_id = _safe_path_segment(experiment_family_id)
    path = manager.data_dir() / "reports" / "research" / "families" / safe_family_id / "trial_registry.jsonl"
    project_root = manager.project_root.resolve()
    if PathManager._is_within(path.resolve(), project_root):
        raise ValueError(f"family trial registry path must be outside repository: {path.resolve()}")
    return path


def registry_content_hash(path: Path) -> str:
    if not path.exists():
        return EMPTY_REGISTRY_HASH
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            payload = json.loads(text)
            if isinstance(payload, dict):
                rows.append(payload)
    return sha256_prefixed(rows)


def append_family_trial_registry_row(
    *,
    manager: PathManager,
    experiment_family_id: str,
    experiment_id: str,
    manifest_hash: str,
    hypothesis_id: str | None,
    hypothesis_status: str | None,
    attempt_index: int,
    holdout_reuse_count: int,
    dataset_content_hash: str,
    parameter_space_hash: str,
    candidate_count: int,
    return_panel_hash: str | None,
    statistical_evidence_hash: str | None,
    result_status: str,
    created_at: str | None = None,
) -> dict[str, Any]:
    path = family_trial_registry_path(manager=manager, experiment_family_id=experiment_family_id)
    prior_hash = registry_content_hash(path)
    row: dict[str, Any] = {
        "schema_version": FAMILY_TRIAL_REGISTRY_SCHEMA_VERSION,
        "experiment_family_id": experiment_family_id,
        "experiment_id": experiment_id,
        "manifest_hash": manifest_hash,
        "hypothesis_id": hypothesis_id,
        "hypothesis_status": hypothesis_status,
        "attempt_index": int(attempt_index),
        "holdout_reuse_count": int(holdout_reuse_count),
        "dataset_content_hash": dataset_content_hash,
        "parameter_space_hash": parameter_space_hash,
        "candidate_count": int(candidate_count),
        "return_panel_hash": return_panel_hash,
        "statistical_evidence_hash": statistical_evidence_hash,
        "statistical_evidence_hash_phase": "pre_registry_evidence_hash",
        "result_status": result_status,
        "prior_registry_hash": prior_hash,
        "created_at": created_at or datetime.now(timezone.utc).isoformat(),
    }
    row["row_hash"] = sha256_prefixed(content_hash_payload(row))
    append_jsonl(path, row)
    return {
        "path": str(path.resolve()),
        "prior_hash": prior_hash,
        "current_hash": registry_content_hash(path),
        "row_hash": row["row_hash"],
    }


def validate_family_registry_binding(
    *,
    report: dict[str, Any],
    evidence: dict[str, Any],
) -> list[str]:
    contract = evidence.get("statistical_validation_contract") or report.get("statistical_validation_contract")
    path_value = str(evidence.get("family_trial_registry_path") or report.get("family_trial_registry_path") or "").strip()
    prior_hash = str(evidence.get("family_trial_registry_prior_hash") or report.get("family_trial_registry_prior_hash") or "").strip()
    row_hash = str(evidence.get("family_trial_registry_row_hash") or report.get("family_trial_registry_row_hash") or "").strip()
    registry_declared = bool(path_value or prior_hash or row_hash)
    if (
        not registry_declared
        and (not isinstance(contract, dict) or contract.get("multiple_testing_scope") != "experiment_family")
    ):
        return []
    reasons: list[str] = []
    if not path_value or not prior_hash.startswith("sha256:"):
        return ["experiment_family_universe_missing"]
    path = Path(path_value).expanduser()
    if not path.exists():
        return ["experiment_family_universe_missing"]
    try:
        rows = _load_registry_rows(path)
    except (OSError, json.JSONDecodeError):
        return ["experiment_family_universe_missing"]
    for row in rows:
        if row_hash.startswith("sha256:") and row.get("row_hash") != row_hash:
            continue
        computed_row_hash = sha256_prefixed(content_hash_payload({k: v for k, v in row.items() if k != "row_hash"}))
        if not row_hash.startswith("sha256:") or str(row.get("row_hash") or "") != row_hash:
            reasons.append("experiment_family_registry_row_hash_missing")
        elif computed_row_hash != row_hash:
            reasons.append("experiment_family_registry_row_hash_mismatch")
        expected_fields = {
            "experiment_family_id": evidence.get("experiment_family_id") or report.get("experiment_family_id"),
            "experiment_id": evidence.get("experiment_id") or report.get("experiment_id"),
            "manifest_hash": evidence.get("manifest_hash") or report.get("manifest_hash"),
            "dataset_content_hash": evidence.get("dataset_content_hash") or report.get("dataset_content_hash"),
            "attempt_index": evidence.get("attempt_index") or report.get("attempt_index"),
            "holdout_reuse_count": evidence.get("holdout_reuse_count") or report.get("holdout_reuse_count"),
        }
        for field, expected in expected_fields.items():
            if str(row.get(field) or "") != str(expected or ""):
                reasons.append("experiment_family_registry_stale")
                break
        if str(row.get("return_panel_hash") or "") != str(evidence.get("return_panel_hash") or ""):
            reasons.append("experiment_family_registry_return_panel_hash_mismatch")
        expected_evidence_hash = str(evidence.get("content_hash") or report.get("statistical_evidence_hash") or "")
        if row.get("statistical_evidence_hash_phase") == "pre_registry_evidence_hash":
            expected_evidence_hash = str(evidence.get("family_trial_registry_bound_evidence_hash") or "")
            if not expected_evidence_hash.startswith("sha256:"):
                reasons.append("experiment_family_registry_statistical_evidence_hash_missing")
        if str(row.get("statistical_evidence_hash") or "") != expected_evidence_hash:
            reasons.append("experiment_family_registry_statistical_evidence_hash_mismatch")
        if str(row.get("prior_registry_hash") or "") != prior_hash:
            reasons.append("experiment_family_registry_prior_hash_mismatch")
        if str(row.get("parameter_space_hash") or "") != str(report.get("parameter_space_hash") or ""):
            reasons.append("experiment_family_registry_stale")
        if int(row.get("candidate_count") or -1) != int(report.get("candidate_count") or -2):
            reasons.append("experiment_family_registry_stale")
        return sorted(set(reasons))
    return ["experiment_family_registry_stale"]


def _load_registry_rows(path: Path) -> list[dict[str, Any]]:
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


def _safe_path_segment(value: str) -> str:
    out = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value).strip())
    return out or "unknown_family"
