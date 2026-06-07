from __future__ import annotations

from scripts.check_strategy_pr_workload_guard import validate_strategy_pr_evidence


_EVIDENCE = """
strategy level: level_3_promotion_grade
contract helper: assert_live_eligible_contract
registration path: builtin_manifest
built-in reason: canary
strategy-plugin-inventory --json
"""


def test_strategy_only_diff_allows_plugin_manifest_tests_docs() -> None:
    violations = validate_strategy_pr_evidence(
        changed_files=(
            "src/bithumb_bot/strategy_plugins/example.py",
            "src/bithumb_bot/strategy_plugins/builtin_manifest.py",
            "tests/test_example_contract.py",
            "docs/strategies/example.md",
        ),
        evidence_text=_EVIDENCE,
    )

    assert violations == []


def test_strategy_diff_rejects_runtime_runner_change() -> None:
    violations = validate_strategy_pr_evidence(
        changed_files=(
            "src/bithumb_bot/strategy_plugins/example.py",
            "src/bithumb_bot/runtime/runner.py",
        ),
        evidence_text=_EVIDENCE,
    )

    assert any("strategy_core_diff_forbidden:src/bithumb_bot/runtime/runner.py" in item for item in violations)


def test_strategy_diff_rejects_execution_service_change() -> None:
    violations = validate_strategy_pr_evidence(
        changed_files=(
            "src/bithumb_bot/strategy_plugins/example.py",
            "src/bithumb_bot/execution_service.py",
        ),
        evidence_text=_EVIDENCE,
    )

    assert any("strategy_core_diff_forbidden:src/bithumb_bot/execution_service.py" in item for item in violations)


def test_strategy_diff_rejects_backtest_kernel_change() -> None:
    violations = validate_strategy_pr_evidence(
        changed_files=(
            "src/bithumb_bot/strategy_plugins/example.py",
            "src/bithumb_bot/research/backtest_kernel.py",
        ),
        evidence_text=_EVIDENCE,
    )

    assert any("strategy_core_diff_forbidden:src/bithumb_bot/research/backtest_kernel.py" in item for item in violations)


def test_external_entry_point_strategy_does_not_require_builtin_manifest_change() -> None:
    violations = validate_strategy_pr_evidence(
        changed_files=("src/bithumb_bot/strategy_plugins/example.py",),
        evidence_text="""
strategy level: level_3_promotion_grade
contract helper: assert_live_eligible_contract
registration path: external_entry_point
bithumb_bot.strategy_plugins
entry point group
inventory evidence: strategy-plugin-inventory --json
""",
    )

    assert violations == []
