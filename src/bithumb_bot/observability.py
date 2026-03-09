from __future__ import annotations

import time
from typing import Any

from .config import settings
from .notifier import format_event


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
    **fields: Any,
) -> str:
    payload: dict[str, Any] = {
        "timestamp": int(time.time() * 1000),
        "symbol": symbol if symbol is not None else settings.PAIR,
        "client_order_id": client_order_id,
        "exchange_order_id": exchange_order_id,
        "submit_attempt_id": submit_attempt_id,
        "state_from": state_from,
        "state_to": state_to,
        "reason_code": reason_code,
    }
    payload.update(fields)
    return format_event(event, **payload)
