from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.cli.parser import build_parser
from bithumb_bot.cli.registry import command_registry


ROOT = Path(__file__).resolve().parents[1]


def _source(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_forward_diagnostics_not_implemented_inside_strategy_sweep() -> None:
    source = _source("src/bithumb_bot/strategy_sweep.py")

    for forbidden in ("forward_return", "ForwardTarget", "mfe", "mae", "feature_bucket", "bucket_metrics"):
        assert forbidden not in source


def test_strategy_sweep_help_does_not_expose_forward_diagnostics_options(capsys: pytest.CaptureFixture[str]) -> None:
    parser = build_parser(command_registry())

    with pytest.raises(SystemExit):
        parser.parse_args(["strategy-sweep", "--help"])

    output = capsys.readouterr().out
    assert "--horizons" not in output
    assert "--bucket" not in output
    assert "--entry-price" not in output


def test_strategy_sweep_source_does_not_import_forward_targets() -> None:
    source = _source("src/bithumb_bot/strategy_sweep.py")

    assert "forward_targets" not in source
    assert "ForwardTarget" not in source
