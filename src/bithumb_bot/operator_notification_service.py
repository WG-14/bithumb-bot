from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .notifier import format_event, notify


@dataclass(frozen=True)
class OperatorNotificationService:
    """Stable alert boundary for runtime/operator notifications."""

    event_formatter: Callable[..., str] = format_event
    message_sender: Callable[[str], None] = notify

    def send_event(self, event_name: str, /, **fields: object) -> None:
        self.message_sender(self.event_formatter(event_name, **fields))

    def send_message(self, message: str) -> None:
        self.message_sender(message)


__all__ = ["OperatorNotificationService", "format_event", "notify"]
