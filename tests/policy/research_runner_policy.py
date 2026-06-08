from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_FAST_EXCLUDED_RESEARCH_MARKERS = frozenset(
    {
        "research_kernel",
        "research_e2e",
        "audit_e2e",
        "walk_forward_e2e",
        "parallel_e2e",
        "slow_research",
        "nightly",
        "memory_sensitive",
    }
)

EXPENSIVE_RESEARCH_MARKERS = DEFAULT_FAST_EXCLUDED_RESEARCH_MARKERS

ALLOWED_MUST_BE_E2E_REASONS = frozenset(
    {
        "artifact_persistence_boundary",
        "process_boundary",
        "fail_closed_safety",
        "promotion_gate",
        "audit_trace_persistence",
        "walk_forward_boundary",
        "operator_surface_convergence",
        "research_kernel_behavior",
        "memory_sensitive_behavior",
    }
)

PRODUCTION_RESEARCH_ENTRYPOINTS = {
    "run_research_backtest",
    "run_research_walk_forward",
}

REAL_KERNEL_ENTRYPOINTS = {
    "run_sma_backtest",
    "run_sma_with_filter_backtest",
    "run_plugin_backtest",
    "run_decision_event_backtest",
    "run_stage_owned_decision_event_backtest",
}

RESEARCH_EXECUTION_ENTRYPOINTS = PRODUCTION_RESEARCH_ENTRYPOINTS | REAL_KERNEL_ENTRYPOINTS

APPROVED_CONTRACT_HELPERS = {
    "_run_contract_research_backtest",
    "_run_contract_research_walk_forward",
}

# These helpers are production-path wrappers for fast-tier runtime guard
# regression tests only. They must be called from tests that set
# BITHUMB_TEST_TIER=fast and assert the guard failure before runner IO starts.
APPROVED_FAST_TIER_GUARD_HELPERS = {
    "_call_production_research_backtest",
    "_call_production_research_walk_forward",
}

INVENTORY_PATH = Path("tests/policy/research_e2e_inventory.json")

EXPECTED_WORKLOAD_FIELDS = (
    "strategy_count",
    "manifest_count",
    "strategy_canary_count",
    "estimated_strategy_runs",
    "estimated_tick_events",
    "estimated_audit_stream_rows",
    "estimated_artifact_write_count",
    "estimated_hash_payload_bytes",
    "estimated_artifact_bytes",
    "estimated_artifact_file_count",
)

DEFAULT_REQUIRED_MARKER_BY_ENTRYPOINT = {
    "run_research_backtest": "research_e2e",
    "run_research_walk_forward": "walk_forward_e2e",
}

SMALL_IN_MEMORY_DATASET_HELPERS = {
    "_dataset_from_closes",
    "_max_holding_dataset",
    "_raw_buy_protective_exit_dataset",
    "_research_dataset_from_closes",
    "_sell_filter_block_dataset",
    "_small_dataset_snapshot",
    "_snapshot_from_closes",
    "_stop_loss_dataset",
}

PATH_SCOPED_SMALL_IN_MEMORY_DATASET_HELPERS = {
    Path("tests/test_orderbook_top_research.py"): {"_signal_dataset"},
    Path("tests/test_orderbook_top_contracts.py"): {"_signal_dataset"},
    Path("tests/test_research_strategy_canary.py"): {"_dataset"},
}

MAX_STATIC_MICRO_KERNEL_CANDLE_TICK_COUNT = 128


@dataclass(frozen=True)
class RunnerCall:
    path: Path
    test_name: str
    nodeid: str
    line: int
    entrypoint: str
    markers: frozenset[str]


@dataclass(frozen=True)
class FastBudgetBypass:
    path: Path
    test_name: str
    nodeid: str
    line: int
    markers: frozenset[str]


@dataclass(frozen=True)
class ExpensiveResearchTest:
    path: Path
    test_name: str
    nodeid: str
    line: int
    markers: frozenset[str]


def load_inventory(path: Path = INVENTORY_PATH) -> dict[str, dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    tests = payload.get("tests")
    if not isinstance(tests, list):
        raise AssertionError(f"{path} must contain a tests list")
    inventory: dict[str, dict[str, object]] = {}
    for item in tests:
        if not isinstance(item, dict):
            raise AssertionError(f"{path} inventory entries must be objects")
        nodeid = item.get("nodeid")
        reason = item.get("reason")
        markers = item.get("markers")
        expected_workload = item.get("expected_workload")
        tier = item.get("tier")
        duration_budget_seconds = item.get("duration_budget_seconds")
        owner = item.get("owner")
        domain = item.get("domain")
        last_measured_seconds = item.get("last_measured_seconds")
        must_be_e2e_reason = item.get("must_be_e2e_reason")
        lower_level_contract_available = item.get("lower_level_contract_available")
        replacement_contract_test = item.get("replacement_contract_test")
        e2e_canary_group = item.get("e2e_canary_group")
        if not isinstance(nodeid, str) or not nodeid:
            raise AssertionError(f"{path} inventory entry missing nodeid")
        if _is_unfilled_placeholder(nodeid):
            raise AssertionError(f"{path} inventory entry has unfilled placeholder nodeid")
        if _is_unfilled_placeholder(reason):
            raise AssertionError(f"{path} inventory entry {nodeid} has unfilled placeholder reason")
        if not isinstance(reason, str) or not reason.strip():
            raise AssertionError(f"{path} inventory entry {nodeid} missing reason")
        if not isinstance(markers, list) or not markers:
            raise AssertionError(f"{path} inventory entry {nodeid} missing markers")
        marker_set = {marker for marker in markers if isinstance(marker, str)}
        if len(marker_set) != len(markers):
            raise AssertionError(f"{path} inventory entry {nodeid} has malformed markers")
        if marker_set.isdisjoint(EXPENSIVE_RESEARCH_MARKERS):
            raise AssertionError(f"{path} inventory entry {nodeid} lacks an expensive marker")
        if not isinstance(must_be_e2e_reason, str) or not must_be_e2e_reason.strip():
            raise AssertionError(f"{path} inventory entry {nodeid} missing must_be_e2e_reason")
        if must_be_e2e_reason not in ALLOWED_MUST_BE_E2E_REASONS:
            raise AssertionError(
                f"{path} inventory entry {nodeid} has unknown must_be_e2e_reason: {must_be_e2e_reason}"
            )
        if lower_level_contract_available is not None and not isinstance(lower_level_contract_available, bool):
            raise AssertionError(f"{path} inventory entry {nodeid} has malformed lower_level_contract_available")
        if lower_level_contract_available is True and (
            not isinstance(replacement_contract_test, str) or not replacement_contract_test.strip()
        ):
            raise AssertionError(
                f"{path} inventory entry {nodeid} missing replacement_contract_test "
                "when lower_level_contract_available=true"
            )
        if not isinstance(e2e_canary_group, str) or not e2e_canary_group.strip():
            raise AssertionError(f"{path} inventory entry {nodeid} missing e2e_canary_group")
        if not isinstance(tier, str) or not tier.strip() or _is_unfilled_placeholder(tier):
            raise AssertionError(f"{path} inventory entry {nodeid} missing tier")
        if not isinstance(expected_workload, dict) or not expected_workload:
            raise AssertionError(f"{path} inventory entry {nodeid} missing expected_workload")
        expected_workload = _normalized_expected_workload(expected_workload)
        item["expected_workload"] = expected_workload
        for workload_field in EXPECTED_WORKLOAD_FIELDS:
            if _is_unfilled_placeholder(expected_workload.get(workload_field)):
                raise AssertionError(
                    f"{path} inventory entry {nodeid} has unfilled placeholder expected_workload.{workload_field}"
                )
            if not _non_negative_number(expected_workload.get(workload_field)):
                raise AssertionError(
                    f"{path} inventory entry {nodeid} missing expected_workload.{workload_field}"
                )
        if _is_unfilled_placeholder(duration_budget_seconds):
            raise AssertionError(f"{path} inventory entry {nodeid} has unfilled placeholder duration_budget_seconds")
        if not _positive_number(duration_budget_seconds):
            raise AssertionError(f"{path} inventory entry {nodeid} missing duration_budget_seconds")
        if _is_unfilled_placeholder(last_measured_seconds):
            raise AssertionError(f"{path} inventory entry {nodeid} has unfilled placeholder last_measured_seconds")
        if not _positive_number(last_measured_seconds, allow_zero=True):
            raise AssertionError(f"{path} inventory entry {nodeid} missing last_measured_seconds")
        if not (
            isinstance(owner, str)
            and owner.strip()
            and not _is_unfilled_placeholder(owner)
            or isinstance(domain, str)
            and domain.strip()
            and not _is_unfilled_placeholder(domain)
        ):
            raise AssertionError(f"{path} inventory entry {nodeid} missing owner or domain")
        inventory[nodeid] = item
    return inventory


def discover_policy_violations(
    test_root: Path = Path("tests"),
    *,
    inventory_path: Path = INVENTORY_PATH,
) -> list[str]:
    violations: list[str] = []
    expensive_tests = list(discover_expensive_research_tests(test_root))
    direct_calls = list(discover_direct_production_runner_calls(test_root))
    kernel_calls = list(discover_real_kernel_calls(test_root))
    fast_budget_bypasses = list(discover_fast_budget_bypasses(test_root))
    inventory = load_inventory(inventory_path)
    inventory_nodeids = set(inventory)
    direct_nodeids = {call.nodeid for call in direct_calls}
    expensive_nodeids = {test.nodeid for test in expensive_tests}

    stale = sorted(inventory_nodeids - expensive_nodeids)
    if stale:
        violations.extend(f"stale workload inventory entry without expensive research marker: {nodeid}" for nodeid in stale)

    missing_expensive = sorted(expensive_nodeids - inventory_nodeids)
    if missing_expensive:
        violations.extend(
            f"expensive research test missing workload inventory entry: {nodeid}"
            for nodeid in missing_expensive
        )

    missing = sorted(direct_nodeids - inventory_nodeids)
    if missing:
        direct_call_by_nodeid = {call.nodeid: call for call in direct_calls}
        violations.extend(
            _format_missing_inventory_violation(direct_call_by_nodeid[nodeid], inventory_path)
            for nodeid in missing
        )

    for call in direct_calls:
        if call.markers.isdisjoint(EXPENSIVE_RESEARCH_MARKERS):
            violations.append(
                f"{call.path}:{call.line}:{call.test_name} calls {call.entrypoint} without an expensive marker"
            )
            continue
        entry = inventory.get(call.nodeid)
        if entry is None:
            continue
        declared_markers = set(entry.get("markers") or ())
        missing_markers = declared_markers - set(call.markers)
        if missing_markers:
            violations.append(
                f"{call.nodeid} inventory markers not present on test: {sorted(missing_markers)}"
            )

    for call in kernel_calls:
        if call.markers & DEFAULT_FAST_EXCLUDED_RESEARCH_MARKERS:
            continue
        violations.append(
            f"{call.path}:{call.line}:{call.test_name} calls {call.entrypoint} without research_kernel, "
            "an expensive marker, or a bounded in-memory micro-kernel dataset"
        )

    for bypass in fast_budget_bypasses:
        if bypass.markers.isdisjoint(EXPENSIVE_RESEARCH_MARKERS):
            violations.append(
                f"{bypass.path}:{bypass.line}:{bypass.test_name} disables the fast research workload budget "
                "without a default-fast-excluded marker"
            )

    violations.extend(validate_contract_helpers(test_root))
    violations.extend(validate_fast_tier_guard_helper_usage(test_root))
    return sorted(set(violations))


def inventory_entry_skeleton_for_call(call: RunnerCall) -> dict[str, object]:
    marker = _suggested_required_marker(call)
    return {
        "nodeid": call.nodeid,
        "markers": [marker],
        "reason": "__FILL_REASON_WHY_THIS_PRODUCTION_RESEARCH_ENTRYPOINT_MUST_RUN__",
        "expected_workload": {
            field: f"__FILL_{field.upper()}__"
            for field in EXPECTED_WORKLOAD_FIELDS
        },
        "duration_budget_seconds": "__FILL_DURATION_BUDGET_SECONDS__",
        "domain": "__FILL_OWNER_OR_DOMAIN__",
        "last_measured_seconds": "__FILL_LAST_MEASURED_SECONDS__",
        "must_be_e2e_reason": "__FILL_MUST_BE_E2E_REASON__",
        "lower_level_contract_available": False,
        "replacement_contract_test": "",
        "e2e_canary_group": "__FILL_E2E_CANARY_GROUP__",
        "tier": "research_nightly",
    }


def _format_missing_inventory_violation(call: RunnerCall, inventory_path: Path) -> str:
    marker = _suggested_required_marker(call)
    skeleton = json.dumps(inventory_entry_skeleton_for_call(call), indent=2, sort_keys=False)
    return (
        f"direct production runner test missing E2E inventory entry: {call.nodeid}\n"
        f"  file: {call.path}\n"
        f"  line: {call.line}\n"
        f"  production_entrypoint: {call.entrypoint}\n"
        f"  required_expensive_marker: {marker} "
        f"(or another marker from {sorted(EXPENSIVE_RESEARCH_MARKERS)} that is present on the test)\n"
        f"  inventory_file: {inventory_path.as_posix()}\n"
        "  inventory_json_skeleton:\n"
        f"{_indent_block(skeleton, '    ')}\n"
        "  replace every __FILL_*__ placeholder with measured or reviewed conservative values; "
        "unchanged placeholders fail policy validation"
    )


def _suggested_required_marker(call: RunnerCall) -> str:
    present_expensive = sorted(call.markers & EXPENSIVE_RESEARCH_MARKERS)
    if present_expensive:
        return present_expensive[0]
    return DEFAULT_REQUIRED_MARKER_BY_ENTRYPOINT.get(call.entrypoint, "research_e2e")


def _indent_block(text: str, prefix: str) -> str:
    return "\n".join(f"{prefix}{line}" for line in text.splitlines())


def discover_expensive_research_tests(test_root: Path) -> Iterable[ExpensiveResearchTest]:
    for path in _iter_test_files(test_root):
        display_path = _display_path(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parent_by_id = _parent_map(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
                continue
            markers = frozenset(_decorator_marker_names(node) | _class_marker_names(node, parent_by_id))
            if markers.isdisjoint(EXPENSIVE_RESEARCH_MARKERS):
                continue
            yield ExpensiveResearchTest(
                path=display_path,
                test_name=node.name,
                nodeid=f"{display_path.as_posix()}::{node.name}",
                line=node.lineno,
                markers=markers,
            )


def research_workload_summary(
    *,
    test_root: Path = Path("tests"),
    inventory_path: Path = INVENTORY_PATH,
) -> dict[str, object]:
    inventory = load_inventory(inventory_path)
    expensive_tests = list(discover_expensive_research_tests(test_root))
    marker_counts = {marker: 0 for marker in sorted(EXPENSIVE_RESEARCH_MARKERS)}
    totals = {
        "strategy_count": 0,
        "manifest_count": 0,
        "strategy_canary_count": 0,
        "total_estimated_strategy_runs": 0,
        "total_estimated_tick_events": 0,
        "total_estimated_audit_stream_rows": 0,
        "total_estimated_artifact_write_count": 0,
        "total_estimated_hash_payload_bytes": 0,
        "total_estimated_artifact_bytes": 0,
        "total_estimated_artifact_file_count": 0,
    }
    workload_key_by_total = {
        "strategy_count": "strategy_count",
        "manifest_count": "manifest_count",
        "strategy_canary_count": "strategy_canary_count",
        "total_estimated_strategy_runs": "estimated_strategy_runs",
        "total_estimated_tick_events": "estimated_tick_events",
        "total_estimated_audit_stream_rows": "estimated_audit_stream_rows",
        "total_estimated_artifact_write_count": "estimated_artifact_write_count",
        "total_estimated_hash_payload_bytes": "estimated_hash_payload_bytes",
        "total_estimated_artifact_bytes": "estimated_artifact_bytes",
        "total_estimated_artifact_file_count": "estimated_artifact_file_count",
    }
    for test in expensive_tests:
        for marker in test.markers & EXPENSIVE_RESEARCH_MARKERS:
            marker_counts[marker] += 1
        entry = inventory.get(test.nodeid)
        if entry is None:
            continue
        workload = entry["expected_workload"]
        if not isinstance(workload, dict):
            continue
        for total_key, workload_key in workload_key_by_total.items():
            totals[total_key] += int(workload.get(workload_key) or 0)
    return {
        "schema_version": 1,
        "inventory_path": inventory_path.as_posix(),
        "expensive_test_count": len(expensive_tests),
        "marker_counts": marker_counts,
        **totals,
    }


def _normalized_expected_workload(expected_workload: dict[str, object]) -> dict[str, object]:
    normalized = dict(expected_workload)
    strategy_runs = int(normalized.get("estimated_strategy_runs") or 0)
    tick_events = int(normalized.get("estimated_tick_events") or 0)
    audit_rows = int(normalized.get("estimated_audit_stream_rows") or 0)
    if "estimated_artifact_write_count" not in normalized:
        normalized["estimated_artifact_write_count"] = max(1, strategy_runs) * 4 + (1 if audit_rows else 0)
    if "estimated_hash_payload_bytes" not in normalized:
        normalized["estimated_hash_payload_bytes"] = tick_events * 128 + max(1, strategy_runs) * 512 + 4096
    if "estimated_artifact_bytes" not in normalized:
        normalized["estimated_artifact_bytes"] = (
            int(normalized["estimated_hash_payload_bytes"])
            + int(normalized["estimated_artifact_write_count"]) * 4096
            + audit_rows * 512
        )
    if "estimated_artifact_file_count" not in normalized:
        normalized["estimated_artifact_file_count"] = int(normalized["estimated_artifact_write_count"])
    return normalized


def discover_direct_production_runner_calls(test_root: Path) -> Iterable[RunnerCall]:
    for path in _iter_test_files(test_root):
        display_path = _display_path(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parent_by_id = _parent_map(tree)
        aliases = _entrypoint_aliases(tree, PRODUCTION_RESEARCH_ENTRYPOINTS)
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
                continue
            markers = frozenset(_decorator_marker_names(node) | _class_marker_names(node, parent_by_id))
            for call in ast.walk(node):
                if not isinstance(call, ast.Call):
                    continue
                entrypoint = _entrypoint_call_name(call, aliases)
                if entrypoint is None:
                    continue
                yield RunnerCall(
                    path=display_path,
                    test_name=node.name,
                    nodeid=f"{display_path.as_posix()}::{node.name}",
                    line=call.lineno,
                    entrypoint=entrypoint,
                    markers=markers,
                )


def discover_real_kernel_calls(test_root: Path) -> Iterable[RunnerCall]:
    for path in _iter_test_files(test_root):
        display_path = _display_path(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parent_by_id = _parent_map(tree)
        aliases = _entrypoint_aliases(tree, REAL_KERNEL_ENTRYPOINTS)
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
                continue
            markers = frozenset(_decorator_marker_names(node) | _class_marker_names(node, parent_by_id))
            bounded_candle_names = _bounded_candle_names(node)
            small_fixture_helpers = _small_fixture_helpers_for_path(display_path)
            bounded_names = _bounded_dataset_names(node, bounded_candle_names, small_fixture_helpers)
            bounded_mappings = _bounded_dataset_mapping_names(node, bounded_names, small_fixture_helpers)
            for call in ast.walk(node):
                if not isinstance(call, ast.Call):
                    continue
                entrypoint = _entrypoint_call_name(call, aliases)
                if entrypoint is None:
                    continue
                if _is_bounded_micro_kernel_call(call, bounded_names, bounded_mappings, bounded_candle_names, small_fixture_helpers):
                    continue
                yield RunnerCall(
                    path=display_path,
                    test_name=node.name,
                    nodeid=f"{display_path.as_posix()}::{node.name}",
                    line=call.lineno,
                    entrypoint=entrypoint,
                    markers=markers,
                )


def discover_fast_budget_bypasses(test_root: Path) -> Iterable[FastBudgetBypass]:
    for path in _iter_test_files(test_root):
        display_path = _display_path(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parent_by_id = _parent_map(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
                continue
            markers = frozenset(_decorator_marker_names(node) | _class_marker_names(node, parent_by_id))
            for call in ast.walk(node):
                if not isinstance(call, ast.Call):
                    continue
                if _call_name(call) not in APPROVED_CONTRACT_HELPERS:
                    continue
                if not any(keyword.arg == "enforce_fast_budget" and _is_false(keyword.value) for keyword in call.keywords):
                    continue
                yield FastBudgetBypass(
                    path=display_path,
                    test_name=node.name,
                    nodeid=f"{display_path.as_posix()}::{node.name}",
                    line=call.lineno,
                    markers=markers,
                )


def validate_contract_helpers(test_root: Path) -> list[str]:
    violations: list[str] = []
    for path in _iter_test_files(test_root):
        display_path = _display_path(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        aliases = _entrypoint_aliases(tree, PRODUCTION_RESEARCH_ENTRYPOINTS)
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            direct_runner_calls = [
                call
                for call in ast.walk(node)
                if isinstance(call, ast.Call) and _entrypoint_call_name(call, aliases) is not None
            ]
            if not direct_runner_calls:
                continue
            if (
                node.name not in APPROVED_CONTRACT_HELPERS
                and node.name not in APPROVED_FAST_TIER_GUARD_HELPERS
                and not node.name.startswith("test_")
            ):
                violations.append(
                    f"{display_path}:{node.lineno}:{node.name} wraps a production research runner but is not approved"
                )
                continue
            if node.name in APPROVED_CONTRACT_HELPERS:
                for call in direct_runner_calls:
                    if not any(keyword.arg == "candidate_evaluator" for keyword in call.keywords):
                        violations.append(
                            f"{display_path}:{call.lineno}:{node.name} must inject a deterministic candidate_evaluator"
                        )
                if not _calls_name(node, "assert_fast_research_workload"):
                    violations.append(
                        f"{display_path}:{node.lineno}:{node.name} must validate workload immediately after the report"
                    )
    return violations


def validate_fast_tier_guard_helper_usage(test_root: Path) -> list[str]:
    violations: list[str] = []
    for path in _iter_test_files(test_root):
        display_path = _display_path(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parent_by_id = _parent_map(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
                continue
            guard_calls = [
                call
                for call in ast.walk(node)
                if isinstance(call, ast.Call) and _call_name(call) in APPROVED_FAST_TIER_GUARD_HELPERS
            ]
            if not guard_calls:
                continue
            markers = frozenset(_decorator_marker_names(node) | _class_marker_names(node, parent_by_id))
            if markers & EXPENSIVE_RESEARCH_MARKERS:
                violations.append(
                    f"{display_path}:{node.lineno}:{node.name} uses a fast-tier guard helper with an "
                    "expensive marker; guard helpers are default-fast early-failure tests only"
                )
            if not _sets_fast_test_tier(node):
                violations.append(
                    f"{display_path}:{node.lineno}:{node.name} uses a fast-tier guard helper without "
                    "setting BITHUMB_TEST_TIER=fast"
                )
            if not _asserts_guard_failure(node):
                violations.append(
                    f"{display_path}:{node.lineno}:{node.name} uses a fast-tier guard helper without "
                    "asserting the production evaluator fast-tier guard failure"
                )
    return violations


def _iter_test_files(test_root: Path) -> Iterable[Path]:
    ignored_parts = {".git", ".pytest_cache", ".ruff_cache", ".mypy_cache", "__pycache__", ".venv", "venv"}
    for path in sorted(test_root.rglob("test_*.py")):
        if ignored_parts & set(path.parts):
            continue
        yield path


def _display_path(path: Path) -> Path:
    try:
        return path.resolve().relative_to(Path.cwd().resolve())
    except ValueError:
        return path


def _parent_map(tree: ast.AST) -> dict[int, ast.AST]:
    parents: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[id(child)] = parent
    return parents


def _entrypoint_aliases(tree: ast.AST, entrypoints: set[str]) -> dict[str, str]:
    aliases = {name: name for name in entrypoints}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        for alias in node.names:
            if alias.name in entrypoints:
                aliases[alias.asname or alias.name] = alias.name
    return aliases


def _decorator_marker_names(node: ast.FunctionDef | ast.ClassDef) -> set[str]:
    markers: set[str] = set()
    for decorator in node.decorator_list:
        current = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(current, ast.Attribute):
            markers.add(current.attr)
        elif isinstance(current, ast.Name):
            markers.add(current.id)
    return markers


def _class_marker_names(node: ast.FunctionDef, parent_by_id: dict[int, ast.AST]) -> set[str]:
    parent = parent_by_id.get(id(node))
    if isinstance(parent, ast.ClassDef):
        return _decorator_marker_names(parent)
    return set()


def _entrypoint_call_name(node: ast.Call, aliases: dict[str, str]) -> str | None:
    if isinstance(node.func, ast.Name):
        return aliases.get(node.func.id)
    if isinstance(node.func, ast.Attribute) and node.func.attr in aliases:
        return node.func.attr
    return None


def _calls_name(node: ast.FunctionDef, name: str) -> bool:
    for call in ast.walk(node):
        if not isinstance(call, ast.Call):
            continue
        func = call.func
        if isinstance(func, ast.Name) and func.id == name:
            return True
        if isinstance(func, ast.Attribute) and func.attr == name:
            return True
    return False


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _bounded_candle_names(node: ast.FunctionDef) -> set[str]:
    names: set[str] = set()
    changed = True
    while changed:
        changed = False
        for statement in ast.walk(node):
            value: ast.AST | None = None
            targets: list[ast.AST] = []
            if isinstance(statement, ast.Assign):
                value = statement.value
                targets = list(statement.targets)
            elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
                value = statement.value
                targets = [statement.target]
            if value is None or not _is_bounded_candles_expr(value, names):
                continue
            for target in targets:
                if isinstance(target, ast.Name) and target.id not in names:
                    names.add(target.id)
                    changed = True
    return names


def _small_fixture_helpers_for_path(path: Path) -> set[str]:
    return SMALL_IN_MEMORY_DATASET_HELPERS | PATH_SCOPED_SMALL_IN_MEMORY_DATASET_HELPERS.get(path, set())


def _bounded_dataset_names(
    node: ast.FunctionDef,
    bounded_candle_names: set[str],
    small_fixture_helpers: set[str],
) -> set[str]:
    names: set[str] = set()
    changed = True
    while changed:
        changed = False
        for statement in ast.walk(node):
            value: ast.AST | None = None
            targets: list[ast.AST] = []
            if isinstance(statement, ast.Assign):
                value = statement.value
                targets = list(statement.targets)
            elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
                value = statement.value
                targets = [statement.target]
            if value is None or not _is_bounded_dataset_expr(value, names, bounded_candle_names, small_fixture_helpers):
                continue
            for target in targets:
                if isinstance(target, ast.Name) and target.id not in names:
                    names.add(target.id)
                    changed = True
    return names


def _bounded_dataset_mapping_names(
    node: ast.FunctionDef,
    bounded_names: set[str],
    small_fixture_helpers: set[str],
) -> set[str]:
    names: set[str] = set()
    for statement in ast.walk(node):
        if isinstance(statement, ast.Assign) and _dict_has_bounded_dataset(statement.value, bounded_names, small_fixture_helpers):
            for target in statement.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
            if _dict_has_bounded_dataset(statement.value, bounded_names, small_fixture_helpers) and isinstance(statement.target, ast.Name):
                names.add(statement.target.id)
    return names


def _is_bounded_micro_kernel_call(
    call: ast.Call,
    bounded_names: set[str],
    bounded_mappings: set[str],
    bounded_candle_names: set[str],
    small_fixture_helpers: set[str],
) -> bool:
    dataset_keywords = [keyword for keyword in call.keywords if keyword.arg == "dataset"]
    if len(dataset_keywords) == 1 and _is_bounded_dataset_expr(
        dataset_keywords[0].value,
        bounded_names,
        bounded_candle_names,
        small_fixture_helpers,
    ):
        return True
    for keyword in call.keywords:
        if keyword.arg is None and _is_bounded_dataset_unpack(keyword.value, bounded_mappings):
            return True
    return False


def _is_bounded_dataset_expr(
    node: ast.AST,
    bounded_names: set[str] | None = None,
    bounded_candle_names: set[str] | None = None,
    small_fixture_helpers: set[str] | None = None,
) -> bool:
    bounded_names = bounded_names or set()
    bounded_candle_names = bounded_candle_names or set()
    small_fixture_helpers = small_fixture_helpers or SMALL_IN_MEMORY_DATASET_HELPERS
    if isinstance(node, ast.Name):
        return node.id in bounded_names
    if isinstance(node, ast.Call):
        name = _call_name(node)
        if name == "DatasetSnapshot":
            return _call_has_bounded_candles(node, bounded_candle_names)
        if name in small_fixture_helpers:
            return True
    return False


def _call_has_bounded_candles(node: ast.Call, bounded_candle_names: set[str]) -> bool:
    for keyword in node.keywords:
        if keyword.arg != "candles":
            continue
        return _is_bounded_candles_expr(keyword.value, bounded_candle_names)
    return False


def _is_bounded_candles_expr(node: ast.AST, bounded_candle_names: set[str]) -> bool:
    if isinstance(node, ast.Name):
        return node.id in bounded_candle_names
    if isinstance(node, (ast.Tuple, ast.List)):
        return True
    if isinstance(node, ast.ListComp):
        return _listcomp_iterates_bounded_literal(node, bounded_candle_names)
    if isinstance(node, ast.Call):
        name = _call_name(node)
        if name == "range":
            return _static_range_length(node) is not None
        if name == "tuple" and len(node.args) == 1:
            return _is_bounded_candles_expr(node.args[0], bounded_candle_names)
        if name == "list" and len(node.args) == 1:
            return _is_bounded_candles_expr(node.args[0], bounded_candle_names)
    if isinstance(node, ast.GeneratorExp):
        return _generator_iterates_bounded_literal(node, bounded_candle_names)
    return False


def _listcomp_iterates_bounded_literal(node: ast.ListComp, bounded_candle_names: set[str]) -> bool:
    return _generator_iterates_bounded_literal(node, bounded_candle_names)


def _generator_iterates_bounded_literal(
    node: ast.ListComp | ast.GeneratorExp,
    bounded_candle_names: set[str],
) -> bool:
    if len(node.generators) != 1:
        return False
    generator = node.generators[0]
    if _is_bounded_candles_expr(generator.iter, bounded_candle_names):
        return True
    if not isinstance(generator.iter, ast.Call):
        return False
    name = _call_name(generator.iter)
    if name == "range":
        return _static_range_length(generator.iter) is not None
    if name == "enumerate" and len(generator.iter.args) == 1:
        return _is_bounded_candles_expr(generator.iter.args[0], bounded_candle_names)
    return False


def _static_range_length(node: ast.Call) -> int | None:
    if _call_name(node) != "range" or node.keywords:
        return None
    if len(node.args) == 1:
        start = 0
        stop = _static_int_literal(node.args[0])
        step = 1
    elif len(node.args) == 2:
        start = _static_int_literal(node.args[0])
        stop = _static_int_literal(node.args[1])
        step = 1
    elif len(node.args) == 3:
        start = _static_int_literal(node.args[0])
        stop = _static_int_literal(node.args[1])
        step = _static_int_literal(node.args[2])
    else:
        return None
    if start is None or stop is None or step is None or step <= 0:
        return None
    length = max(0, (stop - start + step - 1) // step)
    if length > MAX_STATIC_MICRO_KERNEL_CANDLE_TICK_COUNT:
        return None
    return length


def _static_int_literal(node: ast.AST) -> int | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, int) and not isinstance(node.value, bool):
        return node.value
    return None


def _dict_has_bounded_dataset(node: ast.AST, bounded_names: set[str], small_fixture_helpers: set[str]) -> bool:
    if not isinstance(node, ast.Dict):
        return False
    for key, value in zip(node.keys, node.values):
        if isinstance(key, ast.Constant) and key.value == "dataset":
            return _is_bounded_dataset_expr(value, bounded_names, small_fixture_helpers=small_fixture_helpers)
    return False


def _is_bounded_dataset_unpack(node: ast.AST, bounded_mappings: set[str]) -> bool:
    if isinstance(node, ast.Name):
        return node.id in bounded_mappings
    if isinstance(node, ast.Dict):
        for key, value in zip(node.keys, node.values):
            if key is None and _is_bounded_dataset_unpack(value, bounded_mappings):
                return True
            if isinstance(key, ast.Constant) and key.value == "dataset":
                return _is_bounded_dataset_expr(value)
    return False


def _is_false(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is False


def _sets_fast_test_tier(node: ast.FunctionDef) -> bool:
    for call in ast.walk(node):
        if not isinstance(call, ast.Call):
            continue
        if _call_name(call) != "setenv" or len(call.args) < 2:
            continue
        name = _string_constant(call.args[0])
        value = _string_constant(call.args[1])
        if name == "BITHUMB_TEST_TIER" and value == "fast":
            return True
    return False


def _asserts_guard_failure(node: ast.FunctionDef) -> bool:
    for item in ast.walk(node):
        if not isinstance(item, ast.With):
            continue
        for context in item.items:
            expr = context.context_expr
            if not isinstance(expr, ast.Call) or _call_name(expr) != "raises":
                continue
            has_research_error = any(_name_or_attr(arg) == "ResearchValidationError" for arg in expr.args)
            has_guard_match = any(
                keyword.arg == "match"
                and isinstance(_string_constant(keyword.value), str)
                and "production_evaluator_blocked" in (_string_constant(keyword.value) or "")
                for keyword in expr.keywords
            )
            if has_research_error and has_guard_match:
                return True
    return False


def _string_constant(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _name_or_attr(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _is_unfilled_placeholder(value: object) -> bool:
    return isinstance(value, str) and value.startswith("__FILL_") and value.endswith("__")


def _positive_number(value: object, *, allow_zero: bool = False) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    return value >= 0 if allow_zero else value > 0


def _non_negative_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0
