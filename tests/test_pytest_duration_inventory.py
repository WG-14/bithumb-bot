from __future__ import annotations

import json
from pathlib import Path

from scripts.check_pytest_duration_inventory import (
    STATUS_MISSING_INVENTORY,
    STATUS_OVER_BUDGET,
    STATUS_OVER_LAST_MEASURED_2X,
    STATUS_UNPARSED_DURATION_LINE,
    compare_duration_inventory,
    main,
    parse_pytest_duration_lines,
)


def _inventory(path: Path, *, nodeid: str, budget: int = 45, measured: int = 3) -> Path:
    path.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "tests": [
                    {
                        "nodeid": nodeid,
                        "markers": ["research_e2e"],
                        "reason": "duration inventory test",
                        "expected_workload": {
                            "strategy_count": 1,
                            "manifest_count": 1,
                            "strategy_canary_count": 0,
                            "estimated_strategy_runs": 1,
                            "estimated_tick_events": 4320,
                            "estimated_audit_stream_rows": 0,
                            "pre_parallel_work_unit_count": 1,
                            "pre_parallel_dataset_hash_payload_bytes": 1024,
                            "pre_parallel_dataset_hash_call_count": 1,
                        },
                        "duration_budget_seconds": budget,
                        "domain": "duration_policy",
                        "last_measured_seconds": measured,
                        "must_be_e2e_reason": "artifact_persistence_boundary",
                        "lower_level_contract_available": False,
                        "replacement_contract_test": "",
                        "e2e_canary_group": "duration_policy",
                        "tier": "research_nightly",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_duration_inventory_reports_over_budget_nodeid(tmp_path: Path) -> None:
    nodeid = "tests/test_example.py::test_slow"
    durations = tmp_path / "durations.txt"
    durations.write_text(f"52.49s call     {nodeid}\n", encoding="utf-8")

    results, _ = compare_duration_inventory(
        durations_file=durations,
        inventory_path=_inventory(tmp_path / "inventory.json", nodeid=nodeid, budget=45, measured=30),
    )

    assert STATUS_OVER_BUDGET in results[0].status


def test_duration_inventory_reports_last_measured_drift(tmp_path: Path) -> None:
    nodeid = "tests/test_research_backtest_reproducibility.py::test_report_write_stage_timing_is_recorded_after_artifact_write"
    durations = tmp_path / "durations.txt"
    durations.write_text(f"52.49s call     {nodeid}\n", encoding="utf-8")

    results, _ = compare_duration_inventory(
        durations_file=durations,
        inventory_path=_inventory(tmp_path / "inventory.json", nodeid=nodeid, budget=45, measured=3),
    )

    assert STATUS_OVER_BUDGET in results[0].status
    assert STATUS_OVER_LAST_MEASURED_2X in results[0].status


def test_duration_inventory_rejects_unparseable_duration_file(tmp_path: Path) -> None:
    nodeid = "tests/test_example.py::test_slow"
    durations = tmp_path / "durations.txt"
    durations.write_text("52.49 seconds call tests/test_example.py::test_slow\n", encoding="utf-8")

    assert (
        main(
            [
                "--durations-file",
                str(durations),
                "--inventory",
                str(_inventory(tmp_path / "inventory.json", nodeid=nodeid)),
            ]
        )
        == 1
    )


def test_duration_inventory_maps_pytest_duration_line_to_nodeid() -> None:
    rows, unparsed = parse_pytest_duration_lines(
        ["52.49s call     tests/test_example.py::test_slow"]
    )

    assert unparsed == []
    assert rows[0].actual_seconds == 52.49
    assert rows[0].phase == "call"
    assert rows[0].nodeid == "tests/test_example.py::test_slow"


def test_duration_inventory_strict_new_fails_missing_inventory(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_new_research.py"
    test_file.write_text(
        """
import pytest

@pytest.mark.research_e2e
def test_new_expensive():
    assert True
""",
        encoding="utf-8",
    )
    nodeid = f"{test_file.as_posix()}::test_new_expensive"
    durations = tmp_path / "durations.txt"
    durations.write_text(f"1.00s call     {nodeid}\n", encoding="utf-8")
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps({"schema_version": 3, "tests": []}), encoding="utf-8")

    assert (
        main(
            [
                "--durations-file",
                str(durations),
                "--inventory",
                str(inventory),
                "--strict-new",
                "--test-root",
                str(test_root),
            ]
        )
        == 1
    )

    results, _ = compare_duration_inventory(durations_file=durations, inventory_path=inventory)
    assert STATUS_MISSING_INVENTORY in results[0].status
