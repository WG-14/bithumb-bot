#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_POLICY_PATH = Path("tests/policy/research_workload_budget_policy.json")
REQUIRED_LIMIT_FIELDS = (
    "max_estimated_tick_events",
    "max_estimated_audit_stream_rows",
    "max_estimated_artifact_write_count",
    "max_estimated_hash_payload_bytes",
    "max_estimated_artifact_bytes",
    "max_estimated_artifact_file_count",
    "max_estimated_plugin_runtime_us",
    "max_pre_parallel_work_unit_count",
    "max_pre_parallel_dataset_hash_payload_bytes",
    "max_pre_parallel_dataset_hash_call_count",
)
ESTIMATE_TO_LIMIT_FIELDS = (
    ("estimated_tick_events", "max_estimated_tick_events"),
    ("estimated_audit_stream_rows", "max_estimated_audit_stream_rows"),
    ("estimated_artifact_write_count", "max_estimated_artifact_write_count"),
    ("estimated_hash_payload_bytes", "max_estimated_hash_payload_bytes"),
    ("estimated_artifact_bytes", "max_estimated_artifact_bytes"),
    ("estimated_artifact_file_count", "max_estimated_artifact_file_count"),
    ("estimated_plugin_runtime_us", "max_estimated_plugin_runtime_us"),
    ("pre_parallel_work_unit_count", "max_pre_parallel_work_unit_count"),
    ("pre_parallel_dataset_hash_payload_bytes", "max_pre_parallel_dataset_hash_payload_bytes"),
    ("pre_parallel_dataset_hash_call_count", "max_pre_parallel_dataset_hash_call_count"),
)


@dataclass(frozen=True)
class WorkloadBudget:
    suite: str
    limits: dict[str, int]


def load_policy(path: Path) -> dict[str, WorkloadBudget]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SystemExit(f"workload budget policy file missing: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"workload budget policy file is invalid JSON: {path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise SystemExit("workload budget policy schema_version must be 1")
    suites = payload.get("suites")
    if not isinstance(suites, dict):
        raise SystemExit("workload budget policy must define suites")
    budgets: dict[str, WorkloadBudget] = {}
    for suite in ("fast", "research-nightly", "full"):
        raw = suites.get(suite)
        if not isinstance(raw, dict):
            raise SystemExit(f"workload budget policy missing suite={suite}")
        limits: dict[str, int] = {}
        for field in REQUIRED_LIMIT_FIELDS:
            limits[field] = _non_negative_int(raw, field, source=f"policy suite={suite}")
        budgets[suite] = WorkloadBudget(suite=suite, limits=limits)
    return budgets


def check_estimate(estimate: dict[str, Any], budget: WorkloadBudget) -> list[str]:
    violations: list[str] = []
    for estimate_field, limit_field in ESTIMATE_TO_LIMIT_FIELDS:
        observed = _non_negative_int(estimate, estimate_field, source="workload estimate")
        limit = budget.limits[limit_field]
        if observed > limit:
            violations.append(
                f"suite={budget.suite} field={estimate_field} observed={observed} limit={limit}"
            )
    return violations


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Fail fast when research workload estimates exceed suite budgets.")
    parser.add_argument("--suite", default="research-nightly")
    parser.add_argument("--estimate-json", type=Path, help="Optional synthetic workload estimate JSON.")
    parser.add_argument(
        "--policy-json",
        type=Path,
        default=repo_root / DEFAULT_POLICY_PATH,
        help="Suite budget policy JSON.",
    )
    args = parser.parse_args()

    sys.path.insert(0, str(repo_root))
    budgets = load_policy(args.policy_json)
    if args.suite not in budgets:
        raise SystemExit(f"unknown suite={args.suite}; policy suites={','.join(sorted(budgets))}")
    if args.estimate_json:
        estimate = json.loads(args.estimate_json.read_text(encoding="utf-8"))
        if not isinstance(estimate, dict):
            raise SystemExit("workload estimate JSON must be an object")
    else:
        from tests.policy.research_runner_policy import research_workload_summary

        summary = research_workload_summary(test_root=repo_root / "tests")
        estimate = {
            "estimated_tick_events": summary["total_estimated_tick_events"],
            "estimated_audit_stream_rows": summary["total_estimated_audit_stream_rows"],
            "estimated_artifact_write_count": summary["total_estimated_artifact_write_count"],
            "estimated_hash_payload_bytes": summary["total_estimated_hash_payload_bytes"],
            "estimated_artifact_bytes": summary["total_estimated_artifact_bytes"],
            "estimated_artifact_file_count": summary["total_estimated_artifact_file_count"],
            "estimated_plugin_runtime_us": summary.get("total_estimated_plugin_runtime_us", 0),
            "pre_parallel_work_unit_count": summary.get("total_pre_parallel_work_unit_count", 0),
            "pre_parallel_dataset_hash_payload_bytes": summary.get(
                "total_pre_parallel_dataset_hash_payload_bytes",
                0,
            ),
            "pre_parallel_dataset_hash_call_count": summary.get(
                "total_pre_parallel_dataset_hash_call_count",
                0,
            ),
        }

    violations = check_estimate(estimate, budgets[args.suite])
    if violations:
        print(f"research workload budget exceeded for suite={args.suite}", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1
    print(f"research workload budget: ok suite={args.suite}")
    return 0


def _non_negative_int(payload: dict[str, Any], field: str, *, source: str) -> int:
    if field in {
        "estimated_plugin_runtime_us",
        "pre_parallel_work_unit_count",
        "pre_parallel_dataset_hash_payload_bytes",
        "pre_parallel_dataset_hash_call_count",
    } and field not in payload:
        return 0
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SystemExit(f"{source} field {field} must be a non-negative integer")
    return int(value)


if __name__ == "__main__":
    raise SystemExit(main())
