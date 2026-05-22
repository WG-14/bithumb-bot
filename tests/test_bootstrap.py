from __future__ import annotations

from bithumb_bot.bootstrap import bootstrap_argv


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
