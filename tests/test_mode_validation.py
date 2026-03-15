from __future__ import annotations

import pytest

from bithumb_bot import app
from bithumb_bot.config import ModeValidationError, validate_mode_or_raise, settings


def test_validate_mode_or_raise_rejects_typo() -> None:
    with pytest.raises(ModeValidationError) as exc:
        validate_mode_or_raise("papre")

    assert "invalid MODE='papre'" in str(exc.value)
    assert "allowed values: paper, live" in str(exc.value)


def test_main_health_fails_fast_on_invalid_mode(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "papre")
    monkeypatch.setattr(app, "cmd_health", lambda: (_ for _ in ()).throw(AssertionError("must not run")))

    try:
        with pytest.raises(SystemExit) as exc:
            app.main(["health"])
        assert exc.value.code == 1
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "[MODE] invalid MODE='papre'; allowed values: paper, live" in out


def test_main_run_fails_fast_on_invalid_mode(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "papre")
    monkeypatch.setattr(app, "cmd_run", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")))

    try:
        with pytest.raises(SystemExit) as exc:
            app.main(["run"])
        assert exc.value.code == 1
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "[MODE] invalid MODE='papre'; allowed values: paper, live" in out


def test_main_health_keeps_existing_valid_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    original_mode = settings.MODE
    calls: list[str] = []

    object.__setattr__(settings, "MODE", "paper")
    monkeypatch.setattr(app, "cmd_health", lambda: calls.append("health"))

    try:
        rc = app.main(["health"])
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert rc == 0
    assert calls == ["health"]
