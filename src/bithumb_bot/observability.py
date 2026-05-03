from __future__ import annotations

import json
import logging
import math
import sys
import time
from typing import Any

from .config import settings
from .markets import canonical_market_with_raw
from .notifier import AlertSeverity, format_event, notify


_LOG_HANDLER_NAME = "bithumb_bot_stderr"
_LEGACY_STDOUT_HANDLER_NAME = "bithumb_bot_stdout"
_LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"



_CRITICAL_EVENT_NAMES = {
    "trading_halted",
    "recovery_required_transition",
    "order_submit_unknown",
}
_FILL_FEE_ANOMALY_COUNTS: dict[str, int] = {}


def _infer_severity(*, event: str, reason_code: str | None, state_to: str | None, alert_kind: str | None) -> AlertSeverity:
    reason = str(reason_code or "").strip().upper()
    next_state = str(state_to or "").strip().upper()
    kind = str(alert_kind or "").strip().lower()

    if event in _CRITICAL_EVENT_NAMES:
        return AlertSeverity.CRITICAL
    if next_state in {"HALTED", "RECOVERY_REQUIRED", "SUBMIT_UNKNOWN"}:
        return AlertSeverity.CRITICAL
    if reason in {"KILL_SWITCH", "DAILY_LOSS_LIMIT", "RISK_STATE_MISMATCH", "SUBMIT_UNKNOWN", "RECOVERY_REQUIRED"}:
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

    canonical_market, inferred_raw_symbol = canonical_market_with_raw(symbol or settings.PAIR)

    payload: dict[str, Any] = {
        "timestamp": int(time.time() * 1000),
        "market": canonical_market,
        "symbol": canonical_market,
        "raw_symbol": inferred_raw_symbol,
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
    for existing in list(root_logger.handlers):
        if getattr(existing, "name", "") == _LEGACY_STDOUT_HANDLER_NAME:
            root_logger.removeHandler(existing)
    handler = next(
        (
            existing
            for existing in root_logger.handlers
            if getattr(existing, "name", "") == _LOG_HANDLER_NAME
        ),
        None,
    )
    if handler is None:
        handler = logging.StreamHandler(sys.stderr)
        handler.set_name(_LOG_HANDLER_NAME)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, _LOG_DATE_FORMAT))
        root_logger.addHandler(handler)
    else:
        try:
            handler.setStream(sys.stderr)
        except ValueError:
            root_logger.removeHandler(handler)
            handler = logging.StreamHandler(sys.stderr)
            handler.set_name(_LOG_HANDLER_NAME)
            handler.setFormatter(logging.Formatter(_LOG_FORMAT, _LOG_DATE_FORMAT))
            root_logger.addHandler(handler)
    root_logger.setLevel(level)


def format_log_kv(prefix: str, /, **fields: Any) -> str:
    raw_market_input = fields.get("market")
    if raw_market_input is None:
        raw_market_input = fields.get("symbol")
    if raw_market_input is None:
        raw_market_input = fields.get("pair")
    if raw_market_input is not None:
        canonical_market, raw_symbol = canonical_market_with_raw(str(raw_market_input))
        fields["market"] = canonical_market
        fields["symbol"] = canonical_market
        if raw_symbol and fields.get("raw_symbol") is None:
            fields["raw_symbol"] = raw_symbol
        fields.pop("pair", None)

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


def record_fill_fee_anomaly(
    *,
    anomaly_type: str,
    mode: str,
    client_order_id: str,
    fill_id: str | None,
    side: str,
    price: float,
    qty: float,
    notional: float,
    fee: float,
    fee_ratio: float | None,
    min_notional: float,
    min_fee_ratio: float,
    max_fee_ratio: float,
) -> None:
    canonical_market, raw_symbol = canonical_market_with_raw(settings.PAIR)
    key = str(anomaly_type or "unknown")
    _FILL_FEE_ANOMALY_COUNTS[key] = int(_FILL_FEE_ANOMALY_COUNTS.get(key, 0)) + 1
    anomaly_count = _FILL_FEE_ANOMALY_COUNTS[key]
    ratio_text = f"{fee_ratio:.12g}" if fee_ratio is not None and math.isfinite(fee_ratio) else "na"
    fill_id_value = fill_id or "-"
    _message = format_event(
        "live_fill_fee_anomaly",
        severity=AlertSeverity.WARN,
        mode=mode,
        anomaly_type=key,
        anomaly_count=anomaly_count,
        client_order_id=client_order_id,
        fill_id=fill_id_value,
        side=side,
        price=f"{price:.12g}",
        qty=f"{qty:.12g}",
        notional=f"{notional:.12g}",
        fee=f"{fee:.12g}",
        fee_ratio=ratio_text,
        min_notional=f"{float(min_notional):.12g}",
        min_fee_ratio=f"{float(min_fee_ratio):.12g}",
        max_fee_ratio=f"{float(max_fee_ratio):.12g}",
        market=canonical_market,
        raw_symbol=raw_symbol,
    )
    logging.getLogger(__name__).warning(
        format_log_kv(
            "[FILL_FEE_ANOMALY]",
            event="live_fill_fee_anomaly",
            mode=mode,
            anomaly_type=key,
            anomaly_count=anomaly_count,
            client_order_id=client_order_id,
            fill_id=fill_id_value,
            side=side,
            price=price,
            qty=qty,
            notional=notional,
            fee=fee,
            fee_ratio=ratio_text,
            min_notional=min_notional,
            min_fee_ratio=min_fee_ratio,
            max_fee_ratio=max_fee_ratio,
        )
    )
    notify(_message, severity=AlertSeverity.WARN)
