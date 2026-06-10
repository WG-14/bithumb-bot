from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .notifier import NotificationResult
from .paths import PathManager
from .storage_io import append_jsonl

SCHEMA_VERSION = 1


def message_hash(message: str) -> str:
    return "sha256:" + hashlib.sha256(str(message).encode("utf-8")).hexdigest()


def notification_outbox_path(manager: PathManager) -> Path:
    return manager.notification_events_path()


def notification_result_record(
    result: NotificationResult,
    *,
    event_name: str | None = None,
    policy: str | None = None,
    source_command: str | None = None,
) -> dict[str, Any]:
    failure_classes = sorted(
        {
            str(attempt.failure_class)
            for attempt in result.attempts
            if attempt.failure_class
        }
    )
    http_statuses = [
        int(attempt.http_status)
        for attempt in result.attempts
        if attempt.http_status is not None
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "created_ts": datetime.now(timezone.utc).isoformat(),
        "event_name": event_name or result.event_name or "",
        "message_hash": message_hash(result.message),
        "severity": str(result.severity.value),
        "policy": policy or result.policy or "",
        "final_status": result.final_status,
        "attempted_transports": list(result.attempted_transports),
        "delivered_transports": list(result.delivered_transports),
        "failure_classes": failure_classes,
        "http_statuses": http_statuses,
        "source_command": source_command or result.source_command or "",
    }


def append_notification_result(
    result: NotificationResult,
    *,
    manager: PathManager,
    event_name: str | None = None,
    policy: str | None = None,
    source_command: str | None = None,
) -> Path:
    path = notification_outbox_path(manager)
    append_jsonl(
        path,
        notification_result_record(
            result,
            event_name=event_name,
            policy=policy,
            source_command=source_command,
        ),
    )
    return path
