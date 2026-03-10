from __future__ import annotations

import os

import pytest

from bithumb_bot import notifier


@pytest.fixture(autouse=True)
def clear_env(monkeypatch: pytest.MonkeyPatch):
    for key in [
        "NOTIFIER_ENABLED",
        "NOTIFIER_WEBHOOK_URL",
        "SLACK_WEBHOOK_URL",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
    ]:
        monkeypatch.delenv(key, raising=False)
    notifier._RECENT_MESSAGES.clear()


def test_notify_uses_generic_webhook(monkeypatch: pytest.MonkeyPatch):
    calls = []

    def fake_post(url: str, json: dict, timeout: float):
        calls.append((url, json, timeout))

    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/webhook")
    monkeypatch.setattr(notifier, "_post_json", lambda url, payload: fake_post(url, payload, 5.0))

    notifier.notify("hello")

    assert len(calls) == 1
    assert calls[0][0] == "https://example.com/webhook"
    assert calls[0][1] == {"text": "hello"}


def test_notify_uses_telegram_without_logging_secret(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    calls = []

    def fake_post(url: str, json: dict, timeout: float):
        calls.append((url, json, timeout))

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token-secret")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1234")
    monkeypatch.setattr(notifier, "_post_json", lambda url, payload: fake_post(url, payload, 5.0))

    notifier.notify("ping")

    assert len(calls) == 1
    assert calls[0][0].endswith("/sendMessage")
    assert calls[0][1] == {"chat_id": "1234", "text": "ping"}
    assert "token-secret" not in capsys.readouterr().out


def test_notify_falls_back_to_stdout(capsys: pytest.CaptureFixture[str]):
    notifier.notify("fallback")
    out = capsys.readouterr().out
    assert "[NOTIFY] fallback" in out


def test_format_event_skips_empty_fields():
    message = notifier.format_event(
        "reconcile_status_change",
        client_order_id="live_1000_buy",
        exchange_order_id="ex1",
        side="BUY",
        status="FILLED",
        reason="",
        ignored=None,
    )

    assert message == (
        "event=reconcile_status_change client_order_id=live_1000_buy "
        "exchange_order_id=ex1 side=BUY status=FILLED"
    )


def test_format_event_includes_operator_hint_fields():
    message = notifier.format_event(
        "trading_halted",
        reason_code="DAILY_LOSS_LIMIT",
        primary_blocker_code="HALT_STATE_UNRESOLVED",
        operator_next_action="review halt reason and run recovery-report",
        force_resume_allowed=0,
        operator_hint_command="uv run python bot.py recovery-report",
    )

    assert message == (
        "event=trading_halted reason_code=DAILY_LOSS_LIMIT "
        "primary_blocker_code=HALT_STATE_UNRESOLVED "
        "operator_next_action=review halt reason and run recovery-report "
        "force_resume_allowed=0 operator_hint_command=uv run python bot.py recovery-report"
    )


def test_notify_suppresses_identical_duplicates_within_window(monkeypatch: pytest.MonkeyPatch):
    calls = []

    def fake_post(url: str, json: dict, timeout: float):
        calls.append((url, json, timeout))

    ticks = iter([10.0, 15.0, 40.0])
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/webhook")
    monkeypatch.setenv("NOTIFIER_DEDUPE_WINDOW_SEC", "20")
    monkeypatch.setattr(notifier, "_post_json", lambda url, payload: fake_post(url, payload, 5.0))
    monkeypatch.setattr(notifier.time, "monotonic", lambda: next(ticks))

    notifier.notify("dupe")
    notifier.notify("dupe")
    notifier.notify("dupe")

    assert len(calls) == 2
