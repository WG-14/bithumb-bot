from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


EXPENSIVE_RESEARCH_MARKERS = {
    "research_e2e",
    "audit_e2e",
    "walk_forward_e2e",
    "parallel_e2e",
    "slow_research",
    "nightly",
    "memory_sensitive",
}

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

INVENTORY_PATH = Path("tests/policy/research_e2e_inventory.json")

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
    Path("tests/test_research_strategy_canary.py"): {"_dataset"},
}


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
        duration_budget_seconds = item.get("duration_budget_seconds")
        owner = item.get("owner")
        domain = item.get("domain")
        last_measured_seconds = item.get("last_measured_seconds")
        if not isinstance(nodeid, str) or not nodeid:
            raise AssertionError(f"{path} inventory entry missing nodeid")
        if not isinstance(reason, str) or not reason.strip():
            raise AssertionError(f"{path} inventory entry {nodeid} missing reason")
        if not isinstance(markers, list) or not markers:
            raise AssertionError(f"{path} inventory entry {nodeid} missing markers")
        marker_set = {marker for marker in markers if isinstance(marker, str)}
        if len(marker_set) != len(markers):
            raise AssertionError(f"{path} inventory entry {nodeid} has malformed markers")
        if marker_set.isdisjoint(EXPENSIVE_RESEARCH_MARKERS):
            raise AssertionError(f"{path} inventory entry {nodeid} lacks an expensive marker")
        if not isinstance(expected_workload, dict) or not expected_workload:
            raise AssertionError(f"{path} inventory entry {nodeid} missing expected_workload")
        if not _positive_number(duration_budget_seconds):
            raise AssertionError(f"{path} inventory entry {nodeid} missing duration_budget_seconds")
        if not _positive_number(last_measured_seconds, allow_zero=True):
            raise AssertionError(f"{path} inventory entry {nodeid} missing last_measured_seconds")
        if not (
            isinstance(owner, str)
            and owner.strip()
            or isinstance(domain, str)
            and domain.strip()
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
    direct_calls = list(discover_direct_production_runner_calls(test_root))
    kernel_calls = list(discover_real_kernel_calls(test_root))
    fast_budget_bypasses = list(discover_fast_budget_bypasses(test_root))
    inventory = load_inventory(inventory_path)
    inventory_nodeids = set(inventory)
    direct_nodeids = {call.nodeid for call in direct_calls}

    stale = sorted(inventory_nodeids - direct_nodeids)
    if stale:
        violations.extend(f"stale inventory entry without direct production runner call: {nodeid}" for nodeid in stale)

    missing = sorted(direct_nodeids - inventory_nodeids)
    if missing:
        violations.extend(f"direct production runner test missing E2E inventory entry: {nodeid}" for nodeid in missing)

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
        if call.markers & (EXPENSIVE_RESEARCH_MARKERS | {"research_kernel"}):
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
    return sorted(set(violations))


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
                if _call_name(call) != "_run_contract_research_backtest":
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
            if node.name not in APPROVED_CONTRACT_HELPERS and not node.name.startswith("test_"):
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
        return True
    if name == "enumerate" and len(generator.iter.args) == 1:
        return _is_bounded_candles_expr(generator.iter.args[0], bounded_candle_names)
    return False


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


def _positive_number(value: object, *, allow_zero: bool = False) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    return value >= 0 if allow_zero else value > 0
