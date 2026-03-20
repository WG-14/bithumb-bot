from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

from .config import settings
from .notifier import AlertSeverity, format_event


_STDOUT_HANDLER_NAME = "bithumb_bot_stdout"
_LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"



_CRITICAL_EVENT_NAMES = {
    "trading_halted",
    "recovery_required_transition",
    "order_submit_unknown",
}


def _infer_severity(*, event: str, reason_code: str | None, state_to: str | None, alert_kind: str | None) -> AlertSeverity:
    reason = str(reason_code or "").strip().upper()
    next_state = str(state_to or "").strip().upper()
    kind = str(alert_kind or "").strip().lower()

    if event in _CRITICAL_EVENT_NAMES:
        return AlertSeverity.CRITICAL
    if next_state in {"HALTED", "RECOVERY_REQUIRED", "SUBMIT_UNKNOWN"}:
        return AlertSeverity.CRITICAL
    if reason in {"KILL_SWITCH", "DAILY_LOSS_LIMIT", "SUBMIT_UNKNOWN", "RECOVERY_REQUIRED"}:
        return AlertSeverity.CRITICAL
    if kind in {"halt", "kill_switch", "risk_breach", "recovery_required"}:
        return AlertSeverity.CRITICAL
    return AlertSeverity.INFO


def safety_event(
    event: str,
    *,
    symbol: str | None = None,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    submit_attempt_id: str | None = None,
    state_from: str | None = None,
    state_to: str | None = None,
    reason_code: str | None = None,
    severity: str | AlertSeverity | None = None,
    **fields: Any,
) -> str:
    alert_kind = fields.get("alert_kind")
    effective_severity = (
        severity
        if severity is not None
        else _infer_severity(
            event=event,
            reason_code=reason_code,
            state_to=state_to,
            alert_kind=(str(alert_kind) if alert_kind is not None else None),
        )
    )

    payload: dict[str, Any] = {
        "timestamp": int(time.time() * 1000),
        "symbol": symbol if symbol is not None else settings.PAIR,
        "client_order_id": client_order_id,
        "exchange_order_id": exchange_order_id,
        "submit_attempt_id": submit_attempt_id,
        "state_from": state_from,
        "state_to": state_to,
        "reason_code": reason_code,
        "severity": str(effective_severity),
    }
    payload.update(fields)
    return format_event(event, **payload)


def _reconfigure_text_stream(stream: Any) -> None:
    reconfigure = getattr(stream, "reconfigure", None)
    if callable(reconfigure):
        try:
            reconfigure(line_buffering=True, write_through=True)
        except Exception:
            return


def configure_runtime_logging(level: int = logging.INFO) -> None:
    _reconfigure_text_stream(sys.stdout)
    _reconfigure_text_stream(sys.stderr)

    root_logger = logging.getLogger()
    handler = next(
        (
            existing
            for existing in root_logger.handlers
            if getattr(existing, "name", "") == _STDOUT_HANDLER_NAME
        ),
        None,
    )
    if handler is None:
        handler = logging.StreamHandler(sys.stdout)
        handler.set_name(_STDOUT_HANDLER_NAME)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, _LOG_DATE_FORMAT))
        root_logger.addHandler(handler)
    else:
        try:
            handler.setStream(sys.stdout)
        except ValueError:
            root_logger.removeHandler(handler)
            handler = logging.StreamHandler(sys.stdout)
            handler.set_name(_STDOUT_HANDLER_NAME)
            handler.setFormatter(logging.Formatter(_LOG_FORMAT, _LOG_DATE_FORMAT))
            root_logger.addHandler(handler)
    root_logger.setLevel(level)


def format_log_kv(prefix: str, /, **fields: Any) -> str:
    parts = [prefix]
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, bool):
            rendered = "1" if value else "0"
        elif isinstance(value, float):
            rendered = f"{value:.3f}"
        elif isinstance(value, (dict, list, tuple)):
            rendered = json.dumps(value, ensure_ascii=False, sort_keys=True)
        else:
            rendered = str(value)
        if any(ch.isspace() for ch in rendered):
            rendered = json.dumps(rendered, ensure_ascii=False)
        parts.append(f"{key}={rendered}")
    return " ".join(parts)
