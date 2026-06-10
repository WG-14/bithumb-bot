from __future__ import annotations

import json

from bithumb_bot.notification_outbox import append_notification_result
from bithumb_bot.notifier import AlertSeverity, NotificationAttempt, NotificationResult


def _result(*, status: str = "delivered") -> NotificationResult:
    delivered = status == "delivered"
    return NotificationResult(
        message="secret topic message https://hooks.example/secret token-secret",
        severity=AlertSeverity.WARN,
        enabled=True,
        configured=True,
        attempted_transports=("ntfy",),
        delivered_transports=("ntfy",) if delivered else (),
        attempts=(
            NotificationAttempt(
                transport="ntfy",
                attempted=True,
                delivered=delivered,
                http_status=200 if delivered else 429,
                failure_class=None if delivered else "HTTPStatusError",
                failure_message=None if delivered else "HTTP 429",
            ),
        ),
        final_status=status,
    )


def _rows(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_notification_result_is_appended_to_jsonl(managed_runtime_env) -> None:
    from bithumb_bot.config import PATH_MANAGER

    path = append_notification_result(
        _result(),
        manager=PATH_MANAGER,
        event_name="research_command_finished",
        policy="best_effort",
        source_command="research-backtest",
    )

    rows = _rows(path)
    assert rows[-1]["schema_version"] == 1
    assert rows[-1]["event_name"] == "research_command_finished"
    assert rows[-1]["source_command"] == "research-backtest"
    assert rows[-1]["message_hash"].startswith("sha256:")
    assert rows[-1]["final_status"] == "delivered"


def test_notification_outbox_masks_topic_and_urls(managed_runtime_env) -> None:
    from bithumb_bot.config import PATH_MANAGER

    path = append_notification_result(
        _result(status="failed"),
        manager=PATH_MANAGER,
        event_name="research_command_finished",
        policy="best_effort",
        source_command="research-backtest",
    )

    text = path.read_text(encoding="utf-8")
    assert "hooks.example" not in text
    assert "token-secret" not in text
    assert "secret topic message" not in text


def test_failed_delivery_records_http_status_and_failure_class(managed_runtime_env) -> None:
    from bithumb_bot.config import PATH_MANAGER

    path = append_notification_result(
        _result(status="failed"),
        manager=PATH_MANAGER,
        event_name="research_command_finished",
        policy="require_delivery",
        source_command="research-backtest",
    )

    row = _rows(path)[-1]
    assert row["final_status"] == "failed"
    assert row["http_statuses"] == [429]
    assert row["failure_classes"] == ["HTTPStatusError"]
