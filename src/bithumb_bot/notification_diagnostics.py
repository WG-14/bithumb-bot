from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict

from .notifier import NotificationResult, format_event, is_configured, notify, resolve_ntfy_server
from .research.cli import resolve_research_notification_policy


def _present_hash(value: str) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def notification_configuration_payload(*, policy: str | None = None) -> dict[str, object]:
    topic = os.getenv("NTFY_TOPIC", "").strip()
    webhook_url = os.getenv("NOTIFIER_WEBHOOK_URL", "").strip()
    slack_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    configured_transports: list[str] = []
    if topic:
        configured_transports.append("ntfy")
    if webhook_url:
        configured_transports.append("webhook")
    if slack_url:
        configured_transports.append("slack")
    if telegram_token and telegram_chat_id:
        configured_transports.append("telegram")
    notifier_enabled = os.getenv("NOTIFIER_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on", "y"}
    return {
        "notifier_enabled": notifier_enabled,
        "configured": is_configured(),
        "configured_transports": configured_transports,
        "ntfy_server": resolve_ntfy_server(),
        "ntfy_topic_present": bool(topic),
        "ntfy_topic_hash": _present_hash(topic),
        "webhook_configured": bool(webhook_url),
        "slack_configured": bool(slack_url),
        "telegram_configured": bool(telegram_token and telegram_chat_id),
        "policy": resolve_research_notification_policy(policy),
    }


def notification_probe(*, policy: str | None = None) -> NotificationResult:
    resolved_policy = resolve_research_notification_policy(policy)
    return notify(
        format_event("notification_diagnose_probe", source_command="notification-diagnose"),
        event_name="notification_diagnose_probe",
        policy=resolved_policy,
        source_command="notification-diagnose",
    )


def notification_diagnostics_payload(*, probe: bool = False, policy: str | None = None) -> dict[str, object]:
    payload = notification_configuration_payload(policy=policy)
    if probe:
        payload["probe_result"] = asdict(notification_probe(policy=policy))
    return payload


def cmd_notification_diagnose(*, as_json: bool = False, probe: bool = False, policy: str | None = None) -> int:
    payload = notification_diagnostics_payload(probe=probe, policy=policy)
    if as_json or probe:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        print(
            " ".join(
                [
                    f"notifier_enabled={str(payload['notifier_enabled']).lower()}",
                    f"configured={str(payload['configured']).lower()}",
                    f"configured_transports={','.join(payload['configured_transports'])}",
                    f"ntfy_server={payload['ntfy_server']}",
                    f"ntfy_topic_present={str(payload['ntfy_topic_present']).lower()}",
                    f"policy={payload['policy']}",
                ]
            )
        )
    return 0
