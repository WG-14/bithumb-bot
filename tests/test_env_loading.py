from __future__ import annotations

import bot


def test_does_not_auto_load_dotenv_without_explicit_env_file(monkeypatch):
    calls: list[dict[str, str | None]] = []

    def fake_load_dotenv(*, dotenv_path=None):
        calls.append({"dotenv_path": dotenv_path})
        return True

    monkeypatch.setattr(bot, "load_dotenv", fake_load_dotenv)
    monkeypatch.delenv("BITHUMB_ENV_FILE", raising=False)
    monkeypatch.delenv("BITHUMB_ENV_FILE_LIVE", raising=False)
    monkeypatch.delenv("BITHUMB_ENV_FILE_PAPER", raising=False)

    bot._load_explicit_env_file(mode=None)

    assert calls == []


def test_loads_explicit_env_file(monkeypatch):
    calls: list[dict[str, str | None]] = []

    def fake_load_dotenv(*, dotenv_path=None):
        calls.append({"dotenv_path": dotenv_path})
        return True

    monkeypatch.setattr(bot, "load_dotenv", fake_load_dotenv)
    monkeypatch.setenv("BITHUMB_ENV_FILE", "/tmp/runtime.env")

    bot._load_explicit_env_file(mode=None)

    assert calls == [{"dotenv_path": "/tmp/runtime.env"}]


def test_live_and_paper_env_files_are_mode_scoped(monkeypatch):
    calls: list[dict[str, str | None]] = []

    def fake_load_dotenv(*, dotenv_path=None):
        calls.append({"dotenv_path": dotenv_path})
        return True

    monkeypatch.setattr(bot, "load_dotenv", fake_load_dotenv)
    monkeypatch.delenv("BITHUMB_ENV_FILE", raising=False)
    monkeypatch.setenv("BITHUMB_ENV_FILE_LIVE", "/tmp/live.env")
    monkeypatch.setenv("BITHUMB_ENV_FILE_PAPER", "/tmp/paper.env")

    bot._load_explicit_env_file(mode="live")
    bot._load_explicit_env_file(mode="paper")

    assert calls == [
        {"dotenv_path": "/tmp/live.env"},
        {"dotenv_path": "/tmp/paper.env"},
    ]
