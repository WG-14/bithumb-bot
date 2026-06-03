from __future__ import annotations

from pathlib import Path

from scripts.check_fast_test_durations import main, parse_pytest_durations, violations_over_budget


def test_parse_pytest_durations_extracts_reported_test_phases() -> None:
    text = """
============================= slowest 3 durations =============================
12.34s call tests/test_slow.py::test_default_fast_regression
0.42s setup tests/test_setup.py::test_fixture_cost
not a duration line
0.25s teardown tests/test_teardown.py::test_cleanup
"""

    durations = parse_pytest_durations(text)

    assert [duration.nodeid for duration in durations] == [
        "tests/test_slow.py::test_default_fast_regression",
        "tests/test_setup.py::test_fixture_cost",
        "tests/test_teardown.py::test_cleanup",
    ]
    assert [duration.phase for duration in durations] == ["call", "setup", "teardown"]
    assert [duration.seconds for duration in durations] == [12.34, 0.42, 0.25]


def test_parse_pytest_durations_ignores_non_duration_lines() -> None:
    text = """
tests/test_fast.py::test_ok PASSED
============================= slowest durations =============================
1.00 seconds call tests/test_bad_format.py::test_ignored
2.00s tests/test_missing_phase.py::test_ignored
3.00s call tests/test_valid.py::test_included
0.50s call not-a-nodeid
"""

    durations = parse_pytest_durations(text)

    assert [(duration.seconds, duration.phase, duration.nodeid) for duration in durations] == [
        (3.0, "call", "tests/test_valid.py::test_included")
    ]


def test_duration_guard_flags_only_tests_above_budget() -> None:
    durations = parse_pytest_durations(
        """
9.99s call tests/test_ok.py::test_under_budget
10.01s call tests/test_slow.py::test_over_budget
15.00s setup tests/test_slowest.py::test_fixture_over_budget
"""
    )

    violations = violations_over_budget(durations, max_seconds=10.0)

    assert [violation.nodeid for violation in violations] == [
        "tests/test_slowest.py::test_fixture_over_budget",
        "tests/test_slow.py::test_over_budget",
    ]


def test_duration_guard_orders_violations_deterministically() -> None:
    durations = parse_pytest_durations(
        """
12.00s setup tests/test_b.py::test_case
12.00s call tests/test_a.py::test_case
12.00s teardown tests/test_a.py::test_case
15.00s call tests/test_c.py::test_case
"""
    )

    violations = violations_over_budget(durations, max_seconds=10.0)

    assert [(violation.seconds, violation.nodeid, violation.phase) for violation in violations] == [
        (15.0, "tests/test_c.py::test_case", "call"),
        (12.0, "tests/test_a.py::test_case", "call"),
        (12.0, "tests/test_a.py::test_case", "teardown"),
        (12.0, "tests/test_b.py::test_case", "setup"),
    ]


def test_duration_guard_main_returns_zero_when_within_budget(tmp_path: Path, capsys) -> None:
    duration_log = tmp_path / "durations.log"
    duration_log.write_text(
        """
9.00s call tests/test_ok.py::test_under_budget
not a duration line
""",
        encoding="utf-8",
    )

    assert main([str(duration_log), "--max-seconds", "10"]) == 0

    captured = capsys.readouterr()
    assert "default-fast duration guard: ok" in captured.out
    assert captured.err == ""


def test_duration_guard_main_returns_one_and_reports_ordered_violations(tmp_path: Path, capsys) -> None:
    duration_log = tmp_path / "durations.log"
    duration_log.write_text(
        """
11.00s call tests/test_slow.py::test_b
14.00s setup tests/test_slowest.py::test_a
11.00s call tests/test_slow.py::test_a
""",
        encoding="utf-8",
    )

    assert main([str(duration_log), "--max-seconds", "10"]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.splitlines() == [
        "default-fast duration budget exceeded: max_seconds=10",
        "- 14.00s setup tests/test_slowest.py::test_a",
        "- 11.00s call tests/test_slow.py::test_a",
        "- 11.00s call tests/test_slow.py::test_b",
    ]
