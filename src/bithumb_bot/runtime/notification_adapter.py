from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ..operator_notification_service import OperatorNotificationService
from ..notifier import NotificationResult


@dataclass(frozen=True)
class NotificationAdapter:
    service: OperatorNotificationService

    def send_event(self, event: Mapping[str, object]) -> NotificationResult | None:
        event_name = str(event.get("event_type") or event.get("event_name") or "")
        fields = {key: value for key, value in event.items() if key not in {"event_type", "event_name", "event_hash"}}
        return self.service.send_event(event_name, **fields)

    def send_message(self, message: str) -> NotificationResult | None:
        return self.service.send_message(message)


__all__ = ["NotificationAdapter"]
