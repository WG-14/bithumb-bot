from __future__ import annotations

import os
import time
from typing import Any


_RECENT_MESSAGES: dict[str, float] = {}


def _is_enabled() -> bool:
    value = os.getenv("NOTIFIER_ENABLED", "true").strip().lower()
    return value in {"1", "true", "yes", "on", "y"}


def _timeout_sec() -> float:
    raw = os.getenv("NOTIFIER_TIMEOUT_SEC", "5")
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 5.0


def _post_json(url: str, payload: dict[str, Any]) -> None:
    import httpx

    timeout = _timeout_sec()
    httpx.post(url, json=payload, timeout=timeout)


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

def notify(msg: str) -> None:
    if not _is_enabled():
        return

    if _should_suppress_duplicate(msg, now=time.monotonic()):
        return

    delivered = False

    generic_webhook = os.getenv("NOTIFIER_WEBHOOK_URL", "").strip()
    if generic_webhook:
        try:
            _post_json(generic_webhook, {"text": msg})
            delivered = True
        except Exception as exc:
            print(f"[NOTIFY] generic webhook delivery failed: {exc.__class__.__name__}")

    slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if slack_webhook:
        try:
            _post_json(slack_webhook, {"text": msg})
            delivered = True
        except Exception as exc:
            print(f"[NOTIFY] slack delivery failed: {exc.__class__.__name__}")

    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if telegram_token and telegram_chat_id:
        telegram_url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        try:
            _post_json(telegram_url, {"chat_id": telegram_chat_id, "text": msg})
            delivered = True
        except Exception as exc:
            print(f"[NOTIFY] telegram delivery failed: {exc.__class__.__name__}")

    if not delivered:
        print(f"[NOTIFY] {msg}")
