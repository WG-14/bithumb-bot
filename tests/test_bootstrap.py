from __future__ import annotations

import importlib
import os
import sys

import pytest

from bithumb_bot.bootstrap import bootstrap_argv, run_cli


def _clear_explicit_env_selectors(monkeypatch) -> None:
    monkeypatch.delenv("BITHUMB_ENV_FILE", raising=False)
    monkeypatch.delenv("BITHUMB_ENV_FILE_LIVE", raising=False)
    monkeypatch.delenv("BITHUMB_ENV_FILE_PAPER", raising=False)


def test_bootstrap_preserves_subcommand_interval_flag(monkeypatch) -> None:
    _clear_explicit_env_selectors(monkeypatch)
    monkeypatch.delenv("INTERVAL", raising=False)

    argv = bootstrap_argv(
        [
            "bithumb-bot",
            "backfill-candles",
            "--market",
            "KRW-BTC",
            "--interval",
            "1m",
            "--start",
            "2023-01-01",
            "--end",
            "2026-05-01",
        ]
    )

    assert argv == [
        "bithumb-bot",
        "backfill-candles",
        "--market",
        "KRW-BTC",
        "--interval",
        "1m",
        "--start",
        "2023-01-01",
        "--end",
        "2026-05-01",
    ]
    assert "INTERVAL" not in __import__("os").environ


def test_bootstrap_consumes_legacy_global_interval_before_subcommand(monkeypatch) -> None:
    _clear_explicit_env_selectors(monkeypatch)
    monkeypatch.delenv("INTERVAL", raising=False)

    argv = bootstrap_argv(["bithumb-bot", "--interval", "1m", "run"])

    assert argv == ["bithumb-bot", "run"]
    assert __import__("os").environ["INTERVAL"] == "1m"


def test_bootstrap_preserves_subcommand_interval_equals_flag(monkeypatch) -> None:
    _clear_explicit_env_selectors(monkeypatch)
    monkeypatch.delenv("INTERVAL", raising=False)

    argv = bootstrap_argv(
        [
            "bithumb-bot",
            "backfill-candles",
            "--market",
            "KRW-BTC",
            "--interval=1m",
            "--start",
            "2023-01-01",
            "--end",
            "2026-05-01",
        ]
    )

    assert "--interval=1m" in argv
    assert argv[1] == "backfill-candles"
    assert "INTERVAL" not in __import__("os").environ


def test_bootstrap_consumes_legacy_mode_and_entry_before_subcommand(monkeypatch) -> None:
    _clear_explicit_env_selectors(monkeypatch)
    monkeypatch.delenv("MODE", raising=False)
    monkeypatch.delenv("ENTRY_MODE", raising=False)

    argv = bootstrap_argv(["bithumb-bot", "--mode", "paper", "--entry", "breakout", "run"])

    assert argv == ["bithumb-bot", "run"]
    assert __import__("os").environ["MODE"] == "paper"
    assert __import__("os").environ["ENTRY_MODE"] == "breakout"


def test_bootstrap_preserves_subcommand_mode_flag(monkeypatch) -> None:
    _clear_explicit_env_selectors(monkeypatch)
    monkeypatch.delenv("MODE", raising=False)

    argv = bootstrap_argv(["bithumb-bot", "profile-generate", "--mode", "paper"])

    assert argv == ["bithumb-bot", "profile-generate", "--mode", "paper"]
    assert "MODE" not in __import__("os").environ


def test_run_cli_dispatches_with_normalized_argv(monkeypatch) -> None:
    _clear_explicit_env_selectors(monkeypatch)
    monkeypatch.delenv("MODE", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        ["bithumb-bot", "--mode", "paper", "research-backtest", "--manifest", "m.json"],
    )
    monkeypatch.setattr("bithumb_bot.observability.configure_runtime_logging", lambda: None)

    calls: list[list[str]] = []

    def fake_main(argv: list[str] | None = None) -> int:
        calls.append(list(argv or []))
        return 0

    cli_main_module = importlib.import_module("bithumb_bot.cli.main")
    monkeypatch.setattr(cli_main_module, "main", fake_main)

    with pytest.raises(SystemExit) as exc:
        run_cli()

    assert exc.value.code == 0
    assert calls == [["research-backtest", "--manifest", "m.json"]]
    assert os.environ["MODE"] == "paper"
    assert sys.argv == ["bithumb-bot", "research-backtest", "--manifest", "m.json"]


def test_run_cli_propagates_cli_return_code(monkeypatch) -> None:
    _clear_explicit_env_selectors(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["bithumb-bot", "research-readiness", "--manifest", "missing.json"])
    monkeypatch.setattr("bithumb_bot.bootstrap.bootstrap_argv", lambda argv: argv)
    monkeypatch.setattr("bithumb_bot.observability.configure_runtime_logging", lambda: None)
    cli_main_module = importlib.import_module("bithumb_bot.cli.main")
    monkeypatch.setattr(cli_main_module, "main", lambda argv=None: 7)

    with pytest.raises(SystemExit) as exc:
        run_cli()

    assert exc.value.code == 7
