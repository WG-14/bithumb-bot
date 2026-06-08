from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.policy.operator_integration_policy import (
    discover_operator_policy_violations,
    load_operator_inventory,
)


def _write_inventory(path: Path, entries: list[dict[str, object]]) -> Path:
    path.write_text(json.dumps({"schema_version": 1, "tests": entries}, indent=2), encoding="utf-8")
    return path


def _entry(nodeid: str) -> dict[str, object]:
    return {
        "nodeid": nodeid,
        "markers": ["slow_integration"],
        "domain": "submit_unknown_recovery",
        "reason": "verifies submit_unknown recovery state transition",
        "duration_budget_seconds": 70,
        "last_measured_seconds": 65,
        "db_seed": "submit_unknown_candidate",
        "surface_count": 1,
        "must_be_integration_reason": "submit_unknown_recovery",
    }


def test_slow_integration_tests_require_inventory_entry(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_new_recovery.py"
    test_file.write_text(
        """
import pytest

pytestmark = pytest.mark.slow_integration

def test_new_recovery_surface():
    assert True
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_operator_policy_violations(test_root, inventory_path=inventory)

    assert violations == [
        f"slow integration test missing operator inventory entry: {test_file.as_posix()}::test_new_recovery_surface"
    ]


def test_operator_inventory_requires_duration_budget(tmp_path: Path) -> None:
    entry = _entry("tests/operator/test_x.py::test_y")
    entry["duration_budget_seconds"] = 0
    inventory = _write_inventory(tmp_path / "inventory.json", [entry])

    with pytest.raises(AssertionError, match="missing duration_budget_seconds"):
        load_operator_inventory(inventory)


def test_operator_inventory_requires_domain_and_reason(tmp_path: Path) -> None:
    entry = _entry("tests/operator/test_x.py::test_y")
    entry.pop("domain")
    entry["reason"] = ""
    inventory = _write_inventory(tmp_path / "inventory.json", [entry])

    with pytest.raises(AssertionError, match="missing domain"):
        load_operator_inventory(inventory)


def test_operator_inventory_rejects_unknown_integration_reason(tmp_path: Path) -> None:
    entry = _entry("tests/operator/test_x.py::test_y")
    entry["must_be_integration_reason"] = "slow_test"
    inventory = _write_inventory(tmp_path / "inventory.json", [entry])

    with pytest.raises(AssertionError, match="unknown must_be_integration_reason"):
        load_operator_inventory(inventory)
