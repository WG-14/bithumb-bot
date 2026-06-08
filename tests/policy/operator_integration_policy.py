from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


INVENTORY_PATH = Path("tests/policy/operator_integration_inventory.json")
SLOW_MARKER = "slow_integration"
ALLOWED_MUST_BE_INTEGRATION_REASONS = frozenset(
    {
        "operator_surface_convergence",
        "resume_gate_safety",
        "accounting_repair_convergence",
        "submit_unknown_recovery",
        "live_submit_recovery",
    }
)


@dataclass(frozen=True)
class SlowIntegrationTest:
    path: Path
    test_name: str
    nodeid: str
    line: int


def load_operator_inventory(path: Path = INVENTORY_PATH) -> dict[str, dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    tests = payload.get("tests")
    if not isinstance(tests, list):
        raise AssertionError(f"{path} must contain a tests list")
    inventory: dict[str, dict[str, object]] = {}
    for item in tests:
        if not isinstance(item, dict):
            raise AssertionError(f"{path} inventory entries must be objects")
        nodeid = item.get("nodeid")
        if not isinstance(nodeid, str) or not nodeid.strip():
            raise AssertionError(f"{path} inventory entry missing nodeid")
        for field in ("domain", "reason", "db_seed"):
            if not isinstance(item.get(field), str) or not str(item.get(field)).strip():
                raise AssertionError(f"{path} inventory entry {nodeid} missing {field}")
        for field in ("duration_budget_seconds", "last_measured_seconds"):
            value = item.get(field)
            if not isinstance(value, (int, float)) or value <= 0:
                raise AssertionError(f"{path} inventory entry {nodeid} missing {field}")
        must_reason = item.get("must_be_integration_reason")
        if not isinstance(must_reason, str) or not must_reason.strip():
            raise AssertionError(f"{path} inventory entry {nodeid} missing must_be_integration_reason")
        if must_reason not in ALLOWED_MUST_BE_INTEGRATION_REASONS:
            raise AssertionError(
                f"{path} inventory entry {nodeid} has unknown must_be_integration_reason: {must_reason}"
            )
        surface_count = item.get("surface_count")
        if must_reason == "operator_surface_convergence" and (
            not isinstance(surface_count, int) or surface_count <= 0
        ):
            raise AssertionError(f"{path} inventory entry {nodeid} missing surface_count")
        inventory[nodeid] = item
    return inventory


def discover_slow_integration_tests(test_root: Path = Path("tests")) -> Iterable[SlowIntegrationTest]:
    for path in sorted(test_root.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        module_markers = _module_pytestmark_markers(tree)
        parent_by_id = _parent_map(tree)
        display_path = _display_path(path)
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
                continue
            markers = module_markers | _decorator_marker_names(node) | _class_marker_names(node, parent_by_id)
            if SLOW_MARKER not in markers:
                continue
            yield SlowIntegrationTest(
                path=display_path,
                test_name=node.name,
                nodeid=f"{display_path.as_posix()}::{node.name}",
                line=node.lineno,
            )


def discover_operator_policy_violations(
    test_root: Path = Path("tests"),
    *,
    inventory_path: Path = INVENTORY_PATH,
) -> list[str]:
    inventory = load_operator_inventory(inventory_path)
    discovered = {test.nodeid for test in discover_slow_integration_tests(test_root)}
    missing = sorted(discovered - set(inventory))
    return [f"slow integration test missing operator inventory entry: {nodeid}" for nodeid in missing]


def _display_path(path: Path) -> Path:
    cwd = Path.cwd()
    try:
        return path.resolve().relative_to(cwd.resolve())
    except ValueError:
        return path


def _parent_map(tree: ast.AST) -> dict[int, ast.AST]:
    parents: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[id(child)] = parent
    return parents


def _class_marker_names(node: ast.FunctionDef, parent_by_id: dict[int, ast.AST]) -> set[str]:
    parent = parent_by_id.get(id(node))
    if not isinstance(parent, ast.ClassDef):
        return set()
    return _decorator_marker_names(parent)


def _decorator_marker_names(node: ast.FunctionDef | ast.ClassDef) -> set[str]:
    markers: set[str] = set()
    for decorator in node.decorator_list:
        marker = _marker_name(decorator)
        if marker:
            markers.add(marker)
    return markers


def _module_pytestmark_markers(tree: ast.Module) -> set[str]:
    markers: set[str] = set()
    for stmt in tree.body:
        if not isinstance(stmt, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in stmt.targets):
            continue
        marker = _marker_name(stmt.value)
        if marker:
            markers.add(marker)
        if isinstance(stmt.value, (ast.List, ast.Tuple)):
            for item in stmt.value.elts:
                marker = _marker_name(item)
                if marker:
                    markers.add(marker)
    return markers


def _marker_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Call):
        return _marker_name(node.func)
    if isinstance(node, ast.Attribute):
        parts: list[str] = []
        current: ast.AST = node
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name) and current.id == "pytest":
            parts.append(current.id)
            dotted = ".".join(reversed(parts))
            prefix = "pytest.mark."
            if dotted.startswith(prefix):
                return dotted[len(prefix):]
    return None
