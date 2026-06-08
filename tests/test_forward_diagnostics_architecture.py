from __future__ import annotations

from pathlib import Path

from bithumb_bot.cli.registry import command_registry
from bithumb_bot.research.strategy_registry import list_research_strategy_plugins


ROOT = Path(__file__).resolve().parents[1]


def _source(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_forward_diagnostics_not_registered_as_strategy_plugin() -> None:
    plugin_names = {plugin.name for plugin in list_research_strategy_plugins()}

    assert "forward_return_diagnostics" not in plugin_names
    assert not (ROOT / "src/bithumb_bot/strategy_plugins/forward_return_diagnostics.py").exists()
    assert not (ROOT / "src/bithumb_bot/strategy_plugins/forward_diagnostics.py").exists()


def test_forward_diagnostics_not_added_to_backtest_pipeline() -> None:
    source = _source("src/bithumb_bot/research/backtest_pipeline.py")

    assert "forward_diagnostics" not in source
    assert "forward_return_diagnostic" not in source


def test_forward_diagnostics_not_registered_under_strategy_cli() -> None:
    source = _source("src/bithumb_bot/cli/commands/strategy.py")

    assert "research-forward-diagnostics" not in source


def test_forward_diagnostics_not_registered_under_runtime_cli() -> None:
    source = _source("src/bithumb_bot/cli/commands/runtime.py")

    assert "research-forward-diagnostics" not in source


def test_forward_diagnostics_modules_live_under_research_namespace() -> None:
    registry = command_registry()

    assert registry["research-forward-diagnostics"].domain == "research"
    for relative in (
        "src/bithumb_bot/research/forward_diagnostics.py",
        "src/bithumb_bot/research/forward_targets.py",
        "src/bithumb_bot/research/feature_diagnostic_features.py",
        "src/bithumb_bot/research/feature_bucket_metrics.py",
        "src/bithumb_bot/research/forward_diagnostics_report.py",
    ):
        assert (ROOT / relative).exists()
