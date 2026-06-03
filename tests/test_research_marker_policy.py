from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.factories.research_reports import assert_fast_research_workload, minimal_research_report
from tests.policy.research_runner_policy import discover_policy_violations, load_inventory


def test_direct_production_research_entrypoints_have_expensive_markers() -> None:
    assert discover_policy_violations() == []


def _write_inventory(path: Path, entries: list[dict[str, object]]) -> Path:
    path.write_text(
        json.dumps({"schema_version": 2, "tests": entries}, indent=2),
        encoding="utf-8",
    )
    return path


def _inventory_entry(nodeid: str, markers: list[str] | None = None) -> dict[str, object]:
    return {
        "nodeid": nodeid,
        "markers": markers or ["research_e2e"],
        "reason": "temporary policy fixture",
        "expected_workload": {"strategy_runs": "fixture"},
        "duration_budget_seconds": 30,
        "domain": "policy_test",
        "last_measured_seconds": 1,
    }


def test_policy_recursively_scans_nested_production_research_tests(tmp_path: Path) -> None:
    nested = tmp_path / "tests" / "nested"
    nested.mkdir(parents=True)
    test_file = nested / "test_nested_research.py"
    test_file.write_text(
        """
import pytest
from bithumb_bot.research.validation_protocol import run_research_backtest

@pytest.mark.research_e2e
def test_nested_real_runner():
    run_research_backtest(manifest=None, db_path=None, manager=None)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(
        tmp_path / "inventory.json",
        [_inventory_entry(f"{test_file.as_posix()}::test_nested_real_runner")],
    )

    assert discover_policy_violations(tmp_path / "tests", inventory_path=inventory) == []

    inventory = _write_inventory(tmp_path / "empty_inventory.json", [])
    assert any(
        "missing E2E inventory entry" in violation
        for violation in discover_policy_violations(tmp_path / "tests", inventory_path=inventory)
    )


def test_policy_rejects_unmarked_production_research_entrypoint(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_unmarked_runner.py"
    test_file.write_text(
        """
from bithumb_bot.research.validation_protocol import run_research_walk_forward

def test_unmarked_real_runner():
    run_research_walk_forward(manifest=None, db_path=None, manager=None)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(
        tmp_path / "inventory.json",
        [_inventory_entry(f"{test_file.as_posix()}::test_unmarked_real_runner", markers=["walk_forward_e2e"])],
    )

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any("without an expensive marker" in violation for violation in violations)


def test_policy_rejects_marked_production_research_entrypoint_missing_inventory(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_marked_runner.py"
    test_file.write_text(
        """
import pytest
from bithumb_bot.research.validation_protocol import run_research_backtest

@pytest.mark.research_e2e
def test_marked_real_runner():
    run_research_backtest(manifest=None, db_path=None, manager=None)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any("direct production runner test missing E2E inventory entry" in violation for violation in violations)


def test_policy_rejects_stale_inventory_entries(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_no_runner.py"
    test_file.write_text(
        """
def test_no_real_runner():
    assert True
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(
        tmp_path / "inventory.json",
        [_inventory_entry(f"{test_file.as_posix()}::test_no_real_runner")],
    )

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any("stale inventory entry without direct production runner call" in violation for violation in violations)


def test_policy_classifies_real_kernel_entrypoints(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_kernel_boundary.py"
    test_file.write_text(
        """
from bithumb_bot.research.backtest_engine import run_sma_backtest

def test_unbounded_kernel():
    run_sma_backtest(parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_bounded_micro_kernel():
    dataset = _dataset_from_closes([1.0, 2.0, 3.0])
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any("test_unbounded_kernel calls run_sma_backtest" in violation for violation in violations)
    assert not any("test_bounded_micro_kernel" in violation for violation in violations)


def test_policy_rejects_named_dataset_variables_without_bounded_origin(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_named_dataset.py"
    test_file.write_text(
        """
from bithumb_bot.research.backtest_engine import run_sma_backtest

def test_dataset_name_is_not_bounded():
    dataset = load_dataset_split(manifest=None, split_name="train")
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_snapshot_suffix_is_not_bounded():
    tiny_snapshot = load_dataset_range(db_path="prod.sqlite", start=None, end=None)
    run_sma_backtest(dataset=tiny_snapshot, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any("test_dataset_name_is_not_bounded calls run_sma_backtest" in violation for violation in violations)
    assert any("test_snapshot_suffix_is_not_bounded calls run_sma_backtest" in violation for violation in violations)


def test_policy_rejects_non_allowlisted_dataset_helper_names(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_helper_names.py"
    test_file.write_text(
        """
from bithumb_bot.research.backtest_engine import run_sma_backtest

def test_make_dataset_name_is_not_bounded():
    run_sma_backtest(dataset=make_dataset(), parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_load_big_dataset_name_is_not_bounded():
    run_sma_backtest(dataset=load_big_dataset(), parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_make_snapshot_name_is_not_bounded():
    run_sma_backtest(dataset=make_snapshot(), parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_closes_helper_name_is_not_bounded():
    run_sma_backtest(dataset=build_from_closes(), parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any("test_make_dataset_name_is_not_bounded calls run_sma_backtest" in violation for violation in violations)
    assert any("test_load_big_dataset_name_is_not_bounded calls run_sma_backtest" in violation for violation in violations)
    assert any("test_make_snapshot_name_is_not_bounded calls run_sma_backtest" in violation for violation in violations)
    assert any("test_closes_helper_name_is_not_bounded calls run_sma_backtest" in violation for violation in violations)


def test_policy_allows_direct_small_dataset_snapshot_and_derived_variable(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_direct_snapshot.py"
    test_file.write_text(
        """
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot

def test_direct_small_snapshot_is_bounded():
    run_sma_backtest(
        dataset=DatasetSnapshot(candles=()),
        parameter_values={},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

def test_same_function_derived_snapshot_is_bounded():
    candles = []
    snapshot = DatasetSnapshot(candles=candles)
    kwargs = {"dataset": snapshot}
    run_sma_backtest(**kwargs, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert violations == []


def test_policy_allows_small_literal_range_generated_candles(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_small_range_snapshot.py"
    test_file.write_text(
        """
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot

def test_range_stop_literal_is_bounded():
    run_sma_backtest(
        dataset=DatasetSnapshot(candles=tuple(object() for i in range(3))),
        parameter_values={},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

def test_range_start_stop_step_literals_are_bounded():
    candles = [object() for i in range(1, 9, 2)]
    dataset = DatasetSnapshot(candles=candles)
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_enumerated_small_literal_range_is_bounded():
    dataset = DatasetSnapshot(candles=tuple(object() for i, _ in enumerate(range(3))))
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert violations == []


def test_policy_rejects_large_range_generated_candle_sources(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_large_range_snapshot.py"
    test_file.write_text(
        """
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot

def test_large_literal_range_is_not_bounded():
    run_sma_backtest(
        dataset=DatasetSnapshot(candles=tuple(object() for i in range(1000000))),
        parameter_values={},
        fee_rate=0.0,
        slippage_bps=0.0,
    )

def test_large_start_stop_step_range_is_not_bounded():
    dataset = DatasetSnapshot(candles=[object() for i in range(0, 1000, 2)])
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any("test_large_literal_range_is_not_bounded calls run_sma_backtest" in violation for violation in violations)
    assert any("test_large_start_stop_step_range_is_not_bounded calls run_sma_backtest" in violation for violation in violations)


def test_policy_rejects_unknown_range_generated_candle_sources(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_unknown_range_snapshot.py"
    test_file.write_text(
        """
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot

def test_range_name_is_not_bounded():
    n = 3
    dataset = DatasetSnapshot(candles=tuple(object() for i in range(n)))
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_range_expression_is_not_bounded():
    days = 30
    dataset = DatasetSnapshot(candles=tuple(object() for i in range(days * 1440)))
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_range_len_call_is_not_bounded():
    big_source = load_big_source()
    dataset = DatasetSnapshot(candles=tuple(object() for i in range(len(big_source))))
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_zero_step_range_is_not_bounded():
    dataset = DatasetSnapshot(candles=tuple(object() for i in range(0, 10, 0)))
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any("test_range_name_is_not_bounded calls run_sma_backtest" in violation for violation in violations)
    assert any("test_range_expression_is_not_bounded calls run_sma_backtest" in violation for violation in violations)
    assert any("test_range_len_call_is_not_bounded calls run_sma_backtest" in violation for violation in violations)
    assert any("test_zero_step_range_is_not_bounded calls run_sma_backtest" in violation for violation in violations)


def test_policy_rejects_intermediate_variables_from_large_or_unknown_ranges(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_intermediate_range_snapshot.py"
    test_file.write_text(
        """
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot

def test_intermediate_large_range_candles_are_not_bounded():
    candles = tuple(object() for i in range(1000000))
    dataset = DatasetSnapshot(candles=candles)
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

def test_intermediate_unknown_range_candles_are_not_bounded():
    n = 3
    candles = [object() for i in range(n)]
    dataset = DatasetSnapshot(candles=candles)
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any(
        "test_intermediate_large_range_candles_are_not_bounded calls run_sma_backtest" in violation
        for violation in violations
    )
    assert any(
        "test_intermediate_unknown_range_candles_are_not_bounded calls run_sma_backtest" in violation
        for violation in violations
    )


def test_policy_allows_explicit_small_fixture_helper(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_allowlisted_helper.py"
    test_file.write_text(
        """
from bithumb_bot.research.backtest_engine import run_sma_backtest

def test_allowlisted_small_fixture_helper_is_bounded():
    dataset = _small_dataset_snapshot()
    run_sma_backtest(dataset=dataset, parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert violations == []


def test_policy_allows_marked_unbounded_real_kernel_calls(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_marked_kernel.py"
    test_file.write_text(
        """
import pytest
from bithumb_bot.research.backtest_engine import run_sma_backtest

@pytest.mark.research_kernel
def test_research_kernel_marker_allows_unbounded_call():
    run_sma_backtest(dataset=load_dataset_split(manifest=None, split_name="train"), parameter_values={}, fee_rate=0.0, slippage_bps=0.0)

@pytest.mark.slow_research
def test_expensive_marker_allows_unbounded_call():
    run_sma_backtest(dataset=load_big_dataset(), parameter_values={}, fee_rate=0.0, slippage_bps=0.0)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert violations == []


def test_policy_requires_excluded_marker_for_disabled_fast_budget(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_fast_budget_bypass.py"
    test_file.write_text(
        """
import pytest

def test_unmarked_budget_bypass():
    _run_contract_research_backtest(enforce_fast_budget=False)

@pytest.mark.slow_research
def test_marked_budget_bypass():
    _run_contract_research_backtest(enforce_fast_budget=False)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any("test_unmarked_budget_bypass disables the fast research workload budget" in violation for violation in violations)
    assert not any("test_marked_budget_bypass" in violation for violation in violations)


def test_policy_requires_excluded_marker_for_disabled_walk_forward_fast_budget(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    test_file = test_root / "test_walk_forward_fast_budget_bypass.py"
    test_file.write_text(
        """
import pytest

def test_unmarked_walk_forward_budget_bypass():
    _run_contract_research_walk_forward(enforce_fast_budget=False)

@pytest.mark.nightly
def test_marked_walk_forward_budget_bypass():
    _run_contract_research_walk_forward(enforce_fast_budget=False)
""",
        encoding="utf-8",
    )
    inventory = _write_inventory(tmp_path / "inventory.json", [])

    violations = discover_policy_violations(test_root, inventory_path=inventory)

    assert any(
        "test_unmarked_walk_forward_budget_bypass disables the fast research workload budget" in violation
        for violation in violations
    )
    assert not any("test_marked_walk_forward_budget_bypass" in violation for violation in violations)


def test_inventory_validation_rejects_missing_cost_metadata(tmp_path: Path) -> None:
    inventory = _write_inventory(
        tmp_path / "inventory.json",
        [
            {
                "nodeid": "tests/test_example.py::test_real_runner",
                "markers": ["research_e2e"],
                "reason": "missing cost metadata",
            }
        ],
    )

    with pytest.raises(AssertionError, match="missing expected_workload"):
        load_inventory(inventory)


def test_fast_research_workload_budget_rejects_large_strategy_run_count() -> None:
    report = minimal_research_report()
    report["workload_estimate"]["estimated_strategy_runs"] = 4

    with pytest.raises(AssertionError):
        assert_fast_research_workload(report)


def test_fast_research_workload_budget_rejects_tick_and_matrix_growth() -> None:
    report = minimal_research_report()
    report["workload_estimate"].update(
        {
            "candidate_count": 2,
            "scenario_count": 2,
            "split_count": 2,
            "estimated_strategy_runs": 2,
            "estimated_tick_events": 10_001,
        }
    )

    with pytest.raises(AssertionError):
        assert_fast_research_workload(report)


def test_fast_research_workload_budget_rejects_walk_forward_and_complete_external_audit() -> None:
    walk_forward_report = minimal_research_report()
    walk_forward_report["workload_estimate"]["walk_forward_window_count"] = 1
    with pytest.raises(AssertionError):
        assert_fast_research_workload(walk_forward_report)

    audit_report = minimal_research_report()
    audit_report["workload_estimate"]["audit_mode"] = "complete_external"
    with pytest.raises(AssertionError):
        assert_fast_research_workload(audit_report)


def test_fast_research_workload_budget_rejects_full_report_detail_and_full_decision_jsonl() -> None:
    detail_report = minimal_research_report()
    detail_report["workload_estimate"]["report_detail"] = "full"
    with pytest.raises(AssertionError):
        assert_fast_research_workload(detail_report)

    jsonl_report = minimal_research_report()
    jsonl_report["workload_estimate"]["full_decisions_external_jsonl"] = True
    with pytest.raises(AssertionError):
        assert_fast_research_workload(jsonl_report)
