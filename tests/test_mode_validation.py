from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from bithumb_bot.cli.main import main as cli_main
from bithumb_bot import config
from bithumb_bot.config import settings


def test_validate_mode_or_raise_rejects_typo() -> None:
    with pytest.raises(config.ModeValidationError) as exc:
        config.validate_mode_or_raise("papre")

    assert "invalid MODE='papre'" in str(exc.value)
    assert "allowed values: paper, live" in str(exc.value)


def test_live_preflight_rejects_legacy_smoke_strategy_by_capability() -> None:
    cfg = replace(settings, MODE="live", STRATEGY_NAME="sma_cross")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(cfg)

    message = str(exc.value)
    assert "live_strategy_capability_validation_failed" in message
    assert "STRATEGY_NAME='sma_cross'" in message
    assert "strategy_plugin_not_registered:sma_cross" in message


def test_live_real_order_preflight_rejects_legacy_smoke_strategy_before_run() -> None:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_REAL_ORDER_ARMED=True,
        LIVE_DRY_RUN=False,
        STRATEGY_NAME="sma_cross",
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(cfg)

    assert "strategy_plugin_not_registered:sma_cross" in str(exc.value)


def test_plain_sma_cross_remains_allowed_outside_live_preflight() -> None:
    cfg = replace(settings, MODE="paper", STRATEGY_NAME="sma_cross")

    config.validate_live_strategy_selection(cfg)


def test_operator_docs_describe_live_sma_regime_policy_boundary() -> None:
    env_example = Path(".env.example").read_text(encoding="utf-8")
    readme = Path("README.md").read_text(encoding="utf-8")
    research_doc = Path("docs/research-validation.md").read_text(encoding="utf-8")

    combined = "\n".join((env_example, readme, research_doc))
    assert "STRATEGY_CANDIDATE_PROFILE_PATH" in combined
    assert "live_strategy_capability_validation_failed" in combined
    assert "sma_cross" in combined
    assert "sma_with_filter" in combined
    assert "fail closed" in combined


def test_main_health_fails_fast_on_invalid_mode(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(
        config,
        "validate_mode_or_raise",
        lambda _mode: (_ for _ in ()).throw(config.ModeValidationError("invalid MODE='papre'; allowed values: paper, live")),
    )
    monkeypatch.setattr(
        "bithumb_bot.operator_commands.cmd_health",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main(["health"])

    assert exc.value.code == 1

    out = capsys.readouterr().out
    assert "[MODE] invalid MODE='papre'; allowed values: paper, live" in out


def test_main_run_fails_fast_on_invalid_mode(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(
        config,
        "validate_mode_or_raise",
        lambda _mode: (_ for _ in ()).throw(config.ModeValidationError("invalid MODE='papre'; allowed values: paper, live")),
    )
    monkeypatch.setattr(
        "bithumb_bot.operator_commands.cmd_run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main(["run"])

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "[MODE] invalid MODE='papre'; allowed values: paper, live" in out


def test_main_health_keeps_existing_valid_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    original_mode = settings.MODE
    calls: list[str] = []

    object.__setattr__(settings, "MODE", "paper")
    monkeypatch.setattr(
        "bithumb_bot.operator_commands.cmd_health",
        lambda *_args, **_kwargs: calls.append("cmd_health"),
    )

    try:
        rc = cli_main(["health"])
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert rc == 0
    assert calls == ["cmd_health"]
