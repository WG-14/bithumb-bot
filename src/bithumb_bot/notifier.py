from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal


_RECENT_MESSAGES: dict[str, float] = {}


class AlertSeverity(StrEnum):
    INFO = "INFO"
    WARN = "WARN"
    CRITICAL = "CRITICAL"


class PytestNotificationSafetyViolation(RuntimeError):
    """Raised when pytest safety policy blocks a real notification transport."""


NotificationFinalStatus = Literal["delivered", "skipped_disabled", "skipped_unconfigured", "failed"]


@dataclass(frozen=True)
class NotificationAttempt:
    transport: str
    attempted: bool
    delivered: bool
    http_status: int | None = None
    failure_class: str | None = None
    failure_message: str | None = None


@dataclass(frozen=True)
class NotificationResult:
    message: str
    severity: AlertSeverity
    enabled: bool
    configured: bool
    attempted_transports: tuple[str, ...] = ()
    delivered_transports: tuple[str, ...] = ()
    attempts: tuple[NotificationAttempt, ...] = ()
    fallback_printed: bool = False
    final_status: NotificationFinalStatus = "skipped_unconfigured"
    event_name: str | None = None
    policy: str | None = None
    source_command: str | None = None
    outbox_errors: tuple[str, ...] = field(default_factory=tuple)


_SEVERITY_RE = re.compile(r"(?:^|\s)severity=([A-Za-z_]+)(?:\s|$)")


def _normalize_severity(value: str | AlertSeverity | None) -> AlertSeverity:
    if value is None:
        return AlertSeverity.INFO
    text = str(value).strip().upper()
    if text == AlertSeverity.WARN:
        return AlertSeverity.WARN
    if text == AlertSeverity.CRITICAL:
        return AlertSeverity.CRITICAL
    return AlertSeverity.INFO


def _is_enabled() -> bool:
    value = os.getenv("NOTIFIER_ENABLED", "true").strip().lower()
    return value in {"1", "true", "yes", "on", "y"}


def resolve_ntfy_server() -> str:
    server = os.getenv("NTFY_SERVER", "").strip()
    if server:
        return server.rstrip("/")
    alias = os.getenv("NTFY_URL", "").strip()
    if alias:
        return alias.rstrip("/")
    return "https://ntfy.sh"


def is_configured() -> bool:
    if not _is_enabled():
        return False
    return any(
        [
            os.getenv("NTFY_TOPIC", "").strip(),
            os.getenv("NOTIFIER_WEBHOOK_URL", "").strip(),
            os.getenv("SLACK_WEBHOOK_URL", "").strip(),
            os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
            and os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        ]
    )


def _timeout_sec() -> float:
    raw = os.getenv("NOTIFIER_TIMEOUT_SEC", "5")
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 5.0


def _raise_for_status(response: Any) -> None:
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()
        return
    status_code = getattr(response, "status_code", None)
    if status_code is None:
        return
    if not (200 <= int(status_code) < 300):
        raise RuntimeError(f"HTTP {status_code}")


def _status_code(response: Any) -> int | None:
    status_code = getattr(response, "status_code", None)
    if status_code is None:
        return None
    try:
        return int(status_code)
    except (TypeError, ValueError):
        return None


def _failure_status(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    return _status_code(response)


def _attempt_failure(transport: str, exc: BaseException, *, http_status: int | None = None) -> NotificationAttempt:
    return NotificationAttempt(
        transport=transport,
        attempted=True,
        delivered=False,
        http_status=http_status if http_status is not None else _failure_status(exc),
        failure_class=exc.__class__.__name__,
        failure_message=str(exc) or None,
    )


def _post_json(url: str, payload: dict[str, Any]) -> NotificationAttempt:
    import httpx

    timeout = _timeout_sec()
    response = httpx.post(url, json=payload, timeout=timeout)
    _raise_for_status(response)
    return NotificationAttempt(
        transport="webhook",
        attempted=True,
        delivered=True,
        http_status=_status_code(response),
    )


def _post_ntfy(msg: str, *, severity: AlertSeverity) -> NotificationAttempt:
    topic = os.getenv("NTFY_TOPIC", "").strip()
    if not topic:
        return NotificationAttempt(transport="ntfy", attempted=False, delivered=False)

    import httpx

    server = resolve_ntfy_server()
    priority_key = (
        "NTFY_PRIORITY_FAILURE"
        if severity in {AlertSeverity.WARN, AlertSeverity.CRITICAL}
        else "NTFY_PRIORITY_SUCCESS"
    )
    default_priority = "5" if priority_key == "NTFY_PRIORITY_FAILURE" else "3"
    priority = os.getenv(priority_key, default_priority).strip() or default_priority
    headers = {
        "Title": os.getenv("NTFY_TITLE_PREFIX", "bithumb-bot").strip() or "bithumb-bot",
        "Priority": priority,
        "Tags": "warning" if severity in {AlertSeverity.WARN, AlertSeverity.CRITICAL} else "bar_chart",
    }
    response = httpx.post(
        f"{server}/{topic.lstrip('/')}",
        content=msg.encode("utf-8"),
        headers=headers,
        timeout=_timeout_sec(),
    )
    _raise_for_status(response)
    return NotificationAttempt(
        transport="ntfy",
        attempted=True,
        delivered=True,
        http_status=_status_code(response),
    )


def _dedupe_window_sec() -> float:
    raw = os.getenv("NOTIFIER_DEDUPE_WINDOW_SEC", "20")
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 20.0


def _should_suppress_duplicate(msg: str, *, now: float) -> bool:
    window = _dedupe_window_sec()
    if window <= 0:
        return False
    previous = _RECENT_MESSAGES.get(msg)
    _RECENT_MESSAGES[msg] = now
    if previous is None:
        return False
    return (now - previous) < window


def _severity_from_message(msg: str) -> AlertSeverity:
    match = _SEVERITY_RE.search(msg)
    if not match:
        return AlertSeverity.INFO
    return _normalize_severity(match.group(1))


def _configured_transports() -> tuple[str, ...]:
    transports: list[str] = []
    if os.getenv("NTFY_TOPIC", "").strip():
        transports.append("ntfy")
    if os.getenv("NOTIFIER_WEBHOOK_URL", "").strip():
        transports.append("webhook")
    if os.getenv("SLACK_WEBHOOK_URL", "").strip():
        transports.append("slack")
    if os.getenv("TELEGRAM_BOT_TOKEN", "").strip() and os.getenv("TELEGRAM_CHAT_ID", "").strip():
        transports.append("telegram")
    return tuple(transports)


def _coerce_attempt(value: object, *, transport: str) -> NotificationAttempt:
    if isinstance(value, NotificationAttempt):
        if value.transport == transport:
            return value
        return NotificationAttempt(
            transport=transport,
            attempted=value.attempted,
            delivered=value.delivered,
            http_status=value.http_status,
            failure_class=value.failure_class,
            failure_message=value.failure_message,
        )
    if isinstance(value, bool):
        return NotificationAttempt(transport=transport, attempted=value, delivered=value)
    return NotificationAttempt(transport=transport, attempted=True, delivered=True)


def _final_status(
    *,
    enabled: bool,
    configured: bool,
    delivered_transports: tuple[str, ...],
    attempts: tuple[NotificationAttempt, ...],
) -> NotificationFinalStatus:
    if not enabled:
        return "skipped_disabled"
    if delivered_transports:
        return "delivered"
    if not configured:
        return "skipped_unconfigured"
    if any(attempt.attempted and attempt.failure_class for attempt in attempts):
        return "failed"
    return "failed"

def format_event(event: str, **fields: Any) -> str:
    parts = [f"event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    return " ".join(parts)

def notify(
    msg: str,
    *,
    severity: str | AlertSeverity | None = None,
    event_name: str | None = None,
    policy: str | None = None,
    source_command: str | None = None,
) -> NotificationResult:
    enabled = _is_enabled()

    effective_severity = _normalize_severity(severity)
    if severity is None:
        effective_severity = _severity_from_message(msg)

    configured = bool(_configured_transports()) if enabled else False

    if not enabled:
        return NotificationResult(
            message=msg,
            severity=effective_severity,
            enabled=False,
            configured=False,
            final_status="skipped_disabled",
            event_name=event_name,
            policy=policy,
            source_command=source_command,
        )

    if effective_severity == AlertSeverity.CRITICAL and _should_suppress_duplicate(msg, now=time.monotonic()):
        return NotificationResult(
            message=msg,
            severity=effective_severity,
            enabled=True,
            configured=configured,
            final_status="skipped_unconfigured" if not configured else "failed",
            event_name=event_name,
            policy=policy,
            source_command=source_command,
        )

    attempts: list[NotificationAttempt] = []

    try:
        attempt = _coerce_attempt(_post_ntfy(msg, severity=effective_severity), transport="ntfy")
        if attempt.attempted:
            attempts.append(attempt)
    except PytestNotificationSafetyViolation:
        raise
    except Exception as exc:
        attempts.append(_attempt_failure("ntfy", exc))
        print(f"[NOTIFY] ntfy delivery failed: {exc.__class__.__name__}")

    generic_webhook = os.getenv("NOTIFIER_WEBHOOK_URL", "").strip()
    if generic_webhook:
        try:
            attempts.append(_coerce_attempt(_post_json(generic_webhook, {"text": msg}), transport="webhook"))
        except PytestNotificationSafetyViolation:
            raise
        except Exception as exc:
            attempts.append(_attempt_failure("webhook", exc))
            print(f"[NOTIFY] generic webhook delivery failed: {exc.__class__.__name__}")

    slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if slack_webhook:
        try:
            attempts.append(_coerce_attempt(_post_json(slack_webhook, {"text": msg}), transport="slack"))
        except PytestNotificationSafetyViolation:
            raise
        except Exception as exc:
            attempts.append(_attempt_failure("slack", exc))
            print(f"[NOTIFY] slack delivery failed: {exc.__class__.__name__}")

    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if telegram_token and telegram_chat_id:
        telegram_url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        try:
            attempts.append(_coerce_attempt(_post_json(telegram_url, {"chat_id": telegram_chat_id, "text": msg}), transport="telegram"))
        except PytestNotificationSafetyViolation:
            raise
        except Exception as exc:
            attempts.append(_attempt_failure("telegram", exc))
            print(f"[NOTIFY] telegram delivery failed: {exc.__class__.__name__}")

    attempted_transports = tuple(attempt.transport for attempt in attempts if attempt.attempted)
    delivered_transports = tuple(attempt.transport for attempt in attempts if attempt.delivered)
    fallback_printed = False
    if not delivered_transports:
        print(f"[NOTIFY] {msg}")
        fallback_printed = True

    return NotificationResult(
        message=msg,
        severity=effective_severity,
        enabled=enabled,
        configured=configured,
        attempted_transports=attempted_transports,
        delivered_transports=delivered_transports,
        attempts=tuple(attempts),
        fallback_printed=fallback_printed,
        final_status=_final_status(
            enabled=enabled,
            configured=configured,
            delivered_transports=delivered_transports,
            attempts=tuple(attempts),
        ),
        event_name=event_name,
        policy=policy,
        source_command=source_command,
    )
