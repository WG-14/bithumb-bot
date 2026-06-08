from __future__ import annotations

import pytest

from bithumb_bot.cli.parser import build_parser
from bithumb_bot.cli.registry import command_registry


def _parser():
    return build_parser(command_registry())


def test_research_forward_diagnostics_help_exposes_required_options(capsys: pytest.CaptureFixture[str]) -> None:
    parser = _parser()

    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["research-forward-diagnostics", "--help"])

    assert exc.value.code == 0
    output = capsys.readouterr().out
    for option in (
        "--manifest",
        "--split",
        "--features",
        "--horizons",
        "--bucket",
        "--entry-price",
        "--min-bucket-count",
        "--out",
        "--json",
    ):
        assert option in output


def test_research_forward_diagnostics_defaults_to_train_split() -> None:
    args = _parser().parse_args(
        [
            "research-forward-diagnostics",
            "--manifest",
            "manifest.json",
            "--features",
            "sma_gap",
            "--horizons",
            "1",
            "--bucket",
            "quantile:10",
        ]
    )

    assert args.split == "train"


def test_research_forward_diagnostics_defaults_to_next_open_entry_price() -> None:
    args = _parser().parse_args(
        [
            "research-forward-diagnostics",
            "--manifest",
            "manifest.json",
            "--features",
            "sma_gap",
            "--horizons",
            "1",
            "--bucket",
            "quantile:10",
        ]
    )

    assert args.entry_price == "next_open"


def test_research_forward_diagnostics_rejects_unknown_split() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(
            [
                "research-forward-diagnostics",
                "--manifest",
                "manifest.json",
                "--split",
                "unknown",
                "--features",
                "sma_gap",
                "--horizons",
                "1",
                "--bucket",
                "quantile:10",
            ]
        )


def test_research_forward_diagnostics_rejects_empty_feature_list() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(
            [
                "research-forward-diagnostics",
                "--manifest",
                "manifest.json",
                "--features",
                "",
                "--horizons",
                "1",
                "--bucket",
                "quantile:10",
            ]
        )


def test_research_forward_diagnostics_rejects_empty_horizon_list() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(
            [
                "research-forward-diagnostics",
                "--manifest",
                "manifest.json",
                "--features",
                "sma_gap",
                "--horizons",
                "",
                "--bucket",
                "quantile:10",
            ]
        )


def test_research_forward_diagnostics_requires_manifest() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(
            [
                "research-forward-diagnostics",
                "--features",
                "sma_gap",
                "--horizons",
                "1",
                "--bucket",
                "quantile:10",
            ]
        )


def test_research_forward_diagnostics_is_registered() -> None:
    assert "research-forward-diagnostics" in command_registry()
