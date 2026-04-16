from __future__ import annotations

import os
import runpy
import importlib
from pathlib import Path

from bithumb_bot import bootstrap


def test_does_not_auto_load_dotenv_without_explicit_env_file(monkeypatch):
    calls: list[dict[str, str | None]] = []

    def fake_load_dotenv(dotenv_path):
        calls.append({"dotenv_path": dotenv_path})
        return True

    monkeypatch.setattr(bootstrap, "_load_dotenv", fake_load_dotenv)
    monkeypatch.delenv("BITHUMB_ENV_FILE", raising=False)
    monkeypatch.delenv("BITHUMB_ENV_FILE_LIVE", raising=False)
    monkeypatch.delenv("BITHUMB_ENV_FILE_PAPER", raising=False)

    bootstrap.load_explicit_env_file(mode=None)

    assert calls == []


def test_loads_explicit_env_file(monkeypatch):
    calls: list[dict[str, str | None]] = []

    def fake_load_dotenv(dotenv_path):
        calls.append({"dotenv_path": dotenv_path})
        return True

    monkeypatch.setattr(bootstrap, "_load_dotenv", fake_load_dotenv)
    monkeypatch.setenv("BITHUMB_ENV_FILE", "/tmp/runtime.env")

    bootstrap.load_explicit_env_file(mode=None)

    assert calls == [{"dotenv_path": "/tmp/runtime.env"}]


def test_live_and_paper_env_files_are_mode_scoped(monkeypatch):
    calls: list[dict[str, str | None]] = []

    def fake_load_dotenv(dotenv_path):
        calls.append({"dotenv_path": dotenv_path})
        return True

    monkeypatch.setattr(bootstrap, "_load_dotenv", fake_load_dotenv)
    monkeypatch.delenv("BITHUMB_ENV_FILE", raising=False)
    monkeypatch.setenv("BITHUMB_ENV_FILE_LIVE", "/tmp/live.env")
    monkeypatch.setenv("BITHUMB_ENV_FILE_PAPER", "/tmp/paper.env")

    bootstrap.load_explicit_env_file(mode="live")
    bootstrap.load_explicit_env_file(mode="paper")

    assert calls == [
        {"dotenv_path": "/tmp/live.env"},
        {"dotenv_path": "/tmp/paper.env"},
    ]


def test_bootstrap_is_consistent_across_all_entrypoints(tmp_path, monkeypatch):
    env_file = tmp_path / "runtime.env"
    env_file.write_text("BOOTSTRAP_SHARED=ok\n")

    repo_root = Path(__file__).resolve().parents[1]
    bot_entry = repo_root / "bot.py"
    main_entry = repo_root / "main.py"

    def fake_load_dotenv(dotenv_path):
        for line in Path(dotenv_path).read_text().splitlines():
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ[k] = v

    def _run_and_capture(command: str):
        monkeypatch.setenv("BITHUMB_ENV_FILE", str(env_file))
        monkeypatch.delenv("BOOTSTRAP_SHARED", raising=False)
        monkeypatch.setattr("bithumb_bot.cli.main", lambda: None)
        monkeypatch.setattr("bithumb_bot.bootstrap._load_dotenv", fake_load_dotenv)

        if command == "bot.py":
            runpy.run_path(str(bot_entry), run_name="__main__")
        elif command == "main.py":
            runpy.run_path(str(main_entry), run_name="__main__")
        else:
            runpy.run_module("bithumb_bot", run_name="__main__")

        return os.environ.get("BOOTSTRAP_SHARED")

    assert _run_and_capture("bot.py") == "ok"
    assert _run_and_capture("main.py") == "ok"
    assert _run_and_capture("-m") == "ok"


def test_live_fill_fee_strict_env_is_loaded(monkeypatch):
    monkeypatch.setenv("LIVE_FILL_FEE_STRICT_MODE", "true")
    monkeypatch.setenv("LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", "250000")

    import bithumb_bot.config as config_module

    reloaded = importlib.reload(config_module)
    try:
        assert reloaded.settings.LIVE_FILL_FEE_STRICT_MODE is True
        assert reloaded.settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW == 250000.0
    finally:
        importlib.reload(config_module)


def test_describe_explicit_env_file_reports_source_key(monkeypatch):
    monkeypatch.delenv("BITHUMB_ENV_FILE", raising=False)
    monkeypatch.setenv("BITHUMB_ENV_FILE_LIVE", "/tmp/live.env")

    summary = bootstrap.describe_explicit_env_file("live")

    assert summary.env_file == "/tmp/live.env"
    assert summary.source_key == "BITHUMB_ENV_FILE_LIVE"
    assert summary.loaded is False
