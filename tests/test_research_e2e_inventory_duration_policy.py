from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.check_fast_test_durations import parse_pytest_durations
from scripts.check_research_e2e_inventory_durations import inventory_duration_violations, main


def _write_inventory(path: Path, *, budget: float = 5.0) -> Path:
    path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "tests": [
                    {
                        "nodeid": "tests/test_research.py::test_real_runner",
                        "markers": ["research_e2e"],
                        "reason": "synthetic duration ratchet fixture",
                        "expected_workload": {"strategy_runs": "fixture"},
                        "duration_budget_seconds": budget,
                        "domain": "duration_policy",
                        "last_measured_seconds": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_inventory_duration_policy_passes_when_reported_duration_is_within_budget(tmp_path: Path) -> None:
    inventory = _write_inventory(tmp_path / "inventory.json", budget=5.0)
    durations = parse_pytest_durations(
        """
4.99s call tests/test_research.py::test_real_runner
20.00s call tests/test_other.py::test_not_in_inventory
"""
    )

    assert inventory_duration_violations(durations, inventory_path=inventory) == []


def test_inventory_duration_policy_fails_when_reported_duration_exceeds_budget(tmp_path: Path) -> None:
    inventory = _write_inventory(tmp_path / "inventory.json", budget=5.0)
    durations = parse_pytest_durations(
        """
6.25s call tests/test_research.py::test_real_runner
"""
    )

    violations = inventory_duration_violations(durations, inventory_path=inventory)

    assert [(item.nodeid, item.phase, item.seconds, item.budget_seconds) for item in violations] == [
        ("tests/test_research.py::test_real_runner", "call", 6.25, 5.0)
    ]


def test_inventory_duration_policy_rejects_malformed_inventory_entries(tmp_path: Path) -> None:
    inventory = tmp_path / "inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "tests": [
                    {
                        "nodeid": "tests/test_research.py::test_real_runner",
                        "markers": ["research_e2e"],
                        "reason": "missing duration budget",
                        "expected_workload": {"strategy_runs": "fixture"},
                        "domain": "duration_policy",
                        "last_measured_seconds": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="missing duration_budget_seconds"):
        inventory_duration_violations([], inventory_path=inventory)


def test_inventory_duration_policy_main_reports_budget_failures(tmp_path: Path, capsys) -> None:
    inventory = _write_inventory(tmp_path / "inventory.json", budget=5.0)
    duration_log = tmp_path / "durations.log"
    duration_log.write_text(
        "6.25s call tests/test_research.py::test_real_runner\n",
        encoding="utf-8",
    )

    assert main([str(duration_log), "--inventory", str(inventory)]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.splitlines() == [
        "research E2E inventory duration budget exceeded:",
        "- 6.25s call tests/test_research.py::test_real_runner > budget 5s",
    ]
