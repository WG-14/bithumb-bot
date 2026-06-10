from __future__ import annotations

from dataclasses import dataclass
import importlib
from typing import Callable

from .notifier import AlertSeverity, NotificationResult, format_event, notify


@dataclass(frozen=True)
class OperatorNotificationService:
    """Stable alert boundary for runtime/operator notifications."""

    event_formatter: Callable[..., str] = format_event
    message_sender: Callable[[str], NotificationResult | None] | None = None

    def _message_sender(self) -> Callable[[str], NotificationResult | None]:
        if self.message_sender is not None:
            return self.message_sender
        return importlib.import_module("bithumb_bot.notifier").notify

    def send_event(self, event_name: str, /, **fields: object) -> NotificationResult | None:
        result = self._message_sender()(self.event_formatter(event_name, **fields))
        return result

    def send_message(self, message: str) -> NotificationResult | None:
        result = self._message_sender()(message)
        return result


__all__ = ["AlertSeverity", "NotificationResult", "OperatorNotificationService", "format_event", "notify"]
