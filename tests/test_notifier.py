from __future__ import annotations

import os

import pytest

from bithumb_bot import notifier
from bithumb_bot.notifier import AlertSeverity
from bithumb_bot.observability import safety_event


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




def test_format_event_includes_operator_compact_summary_fields():
    message = notifier.format_event(
        "startup_gate_blocked",
        reason_code="STARTUP_BLOCKED",
        unresolved_order_count=2,
        open_order_count=3,
        position_summary="long_qty=0.01000000",
        operator_compact_summary=(
            "halt_reason=STARTUP_SAFETY_GATE unresolved_order_count=2 "
            "open_order_count=3 position=long_qty=0.01000000 "
            "next=uv run python bot.py reconcile | uv run python bot.py recovery-report"
        ),
        operator_recommended_commands="uv run python bot.py reconcile | uv run python bot.py recovery-report",
    )

    assert "event=startup_gate_blocked" in message
    assert "open_order_count=3" in message
    assert "position_summary=long_qty=0.01000000" in message
    assert "operator_compact_summary=halt_reason=STARTUP_SAFETY_GATE" in message
    assert "operator_recommended_commands=uv run python bot.py reconcile | uv run python bot.py recovery-report" in message

def test_notify_suppresses_identical_critical_duplicates_within_window(monkeypatch: pytest.MonkeyPatch):
    calls = []

    def fake_post(url: str, json: dict, timeout: float):
        calls.append((url, json, timeout))

    ticks = iter([10.0, 15.0, 40.0])
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/webhook")
    monkeypatch.setenv("NOTIFIER_DEDUPE_WINDOW_SEC", "20")
    monkeypatch.setattr(notifier, "_post_json", lambda url, payload: fake_post(url, payload, 5.0))
    monkeypatch.setattr(notifier.time, "monotonic", lambda: next(ticks))

    notifier.notify("dupe", severity=AlertSeverity.CRITICAL)
    notifier.notify("dupe", severity=AlertSeverity.CRITICAL)
    notifier.notify("dupe", severity=AlertSeverity.CRITICAL)

    assert len(calls) == 2


def test_notify_does_not_suppress_identical_info_duplicates(monkeypatch: pytest.MonkeyPatch):
    calls = []

    def fake_post(url: str, json: dict, timeout: float):
        calls.append((url, json, timeout))

    ticks = iter([10.0, 11.0])
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/webhook")
    monkeypatch.setenv("NOTIFIER_DEDUPE_WINDOW_SEC", "20")
    monkeypatch.setattr(notifier, "_post_json", lambda url, payload: fake_post(url, payload, 5.0))
    monkeypatch.setattr(notifier.time, "monotonic", lambda: next(ticks))

    notifier.notify("dupe", severity=AlertSeverity.INFO)
    notifier.notify("dupe", severity=AlertSeverity.INFO)

    assert len(calls) == 2


def test_safety_event_keeps_common_operator_fields_in_payload():
    message = safety_event(
        "order_submit_unknown",
        symbol="BTC_KRW",
        client_order_id="live_1000_buy_attempt1",
        submit_attempt_id="attempt1",
        exchange_order_id="-",
        reason_code="SUBMIT_TIMEOUT",
        state_from="PENDING_SUBMIT",
        state_to="SUBMIT_UNKNOWN",
    )

    assert "event=order_submit_unknown" in message
    assert "market=KRW-BTC" in message
    assert "symbol=KRW-BTC" in message
    assert "raw_symbol=BTC_KRW" in message
    assert "client_order_id=live_1000_buy_attempt1" in message
    assert "submit_attempt_id=attempt1" in message
    assert "exchange_order_id=-" in message
    assert "reason_code=SUBMIT_TIMEOUT" in message
    assert "state_from=PENDING_SUBMIT" in message
    assert "state_to=SUBMIT_UNKNOWN" in message


def test_safety_event_infers_critical_severity_for_major_safety_states():
    halt_msg = safety_event("trading_halted", reason_code="KILL_SWITCH", state_to="HALTED")
    submit_unknown_msg = safety_event("order_submit_unknown", state_to="SUBMIT_UNKNOWN")
    recovery_msg = safety_event("recovery_required_transition", state_to="RECOVERY_REQUIRED")

    assert "severity=CRITICAL" in halt_msg
    assert "severity=CRITICAL" in submit_unknown_msg
    assert "severity=CRITICAL" in recovery_msg


def test_safety_event_defaults_to_info_severity_for_noncritical_events():
    msg = safety_event("order_submit_started", state_to="PENDING_SUBMIT", reason_code="-")

    assert "severity=INFO" in msg
