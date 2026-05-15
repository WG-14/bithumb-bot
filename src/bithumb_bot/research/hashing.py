from __future__ import annotations

import hashlib
import json
from typing import Any


REPORT_TOP_LEVEL_HASH_EXCLUDED_FIELDS = frozenset(
    {
        "content_hash",
        "generated_at",
        "created_at",
        "artifact_paths",
        "statistical_evidence_path",
        "return_panel_path",
        "family_trial_registry_path",
    }
)
REPORT_RUNTIME_ONLY_FIELDS = frozenset(
    {
        "failure_artifact_path",
        "statistical_evidence_path",
        "return_panel_path",
        "family_trial_registry_path",
    }
)


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def sha256_prefixed(payload: Any) -> str:
    return f"sha256:{sha256_hex(payload)}"


def content_hash_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in {"generated_at", "created_at"}}


def report_content_hash_payload(payload: dict[str, Any]) -> dict[str, Any]:
    logical_payload = {
        key: value
        for key, value in payload.items()
        if key not in REPORT_TOP_LEVEL_HASH_EXCLUDED_FIELDS
    }
    return _strip_report_runtime_only_fields(logical_payload)


def _strip_report_runtime_only_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_report_runtime_only_fields(item)
            for key, item in value.items()
            if key not in REPORT_RUNTIME_ONLY_FIELDS
        }
    if isinstance(value, list):
        return [_strip_report_runtime_only_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_report_runtime_only_fields(item) for item in value]
    return value
