from __future__ import annotations

import json

from bithumb_bot import notification_diagnostics
from bithumb_bot.notifier import AlertSeverity, NotificationResult


def test_notification_diagnose_json_masks_secrets(monkeypatch, capsys) -> None:
    monkeypatch.setenv("NTFY_TOPIC", "raw-topic-secret")
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://hooks.example/raw-secret")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-secret")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat-secret")

    rc = notification_diagnostics.cmd_notification_diagnose(as_json=True)

    assert rc == 0
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["ntfy_topic_present"] is True
    assert str(payload["ntfy_topic_hash"]).startswith("sha256:")
    assert "raw-topic-secret" not in output
    assert "hooks.example/raw-secret" not in output
    assert "telegram-secret" not in output


def test_notification_diagnose_json_does_not_send_probe(monkeypatch, capsys) -> None:
    def unexpected_probe(*args, **kwargs):
        raise AssertionError("--json must not send probe")

    monkeypatch.setattr(notification_diagnostics, "notify", unexpected_probe)

    rc = notification_diagnostics.cmd_notification_diagnose(as_json=True, probe=False)

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert "probe_result" not in payload


def test_notification_diagnose_probe_returns_notification_result(monkeypatch, capsys) -> None:
    expected = NotificationResult(
        message="probe",
        severity=AlertSeverity.INFO,
        enabled=True,
        configured=True,
        attempted_transports=("ntfy",),
        delivered_transports=("ntfy",),
        final_status="delivered",
    )
    monkeypatch.setattr(notification_diagnostics, "notify", lambda *args, **kwargs: expected)

    rc = notification_diagnostics.cmd_notification_diagnose(as_json=True, probe=True)

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["probe_result"]["final_status"] == "delivered"
    assert payload["probe_result"]["delivered_transports"] == ["ntfy"]
