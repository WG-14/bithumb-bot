from __future__ import annotations

import json
import logging
import math
import time

from ..config import settings
from ..db_core import ensure_db, get_portfolio, init_portfolio
from ..execution import apply_fill_and_trade, record_order_if_missing
from ..marketdata import fetch_orderbook_top
from ..notifier import format_event, notify
from ..observability import format_log_kv, safety_event
from ..public_api_orderbook import BestQuote
from ..reason_codes import AMBIGUOUS_SUBMIT, RISKY_ORDER_BLOCK, SUBMIT_FAILED, SUBMIT_TIMEOUT
from .order_rules import get_effective_order_rules, side_min_total_krw
from .balance_source import fetch_balance_snapshot
from ..risk import evaluate_buy_guardrails, evaluate_order_submission_halt
from .. import runtime_state
from ..oms import (
    MAX_CLIENT_ORDER_ID_LENGTH,
    build_client_order_id,
    TERMINAL_ORDER_STATUSES,
    build_order_intent_key,
    claim_order_intent_dedup,
    evaluate_unresolved_order_gate,
    new_client_order_id,
    payload_fingerprint,
    record_status_transition,
    record_submit_attempt,
    record_submit_blocked,
    record_submit_started,
    set_exchange_order_id,
    set_status,
    update_order_intent_dedup,
)
from .base import Broker, BrokerFill, BrokerSubmissionUnknownError, BrokerTemporaryError

POSITION_EPSILON = 1e-12
VALID_ORDER_SIDES = {"BUY", "SELL"}
UNSET_EVENT_FIELD = "-"

SUBMISSION_REASON_FAILED_BEFORE_SEND = "failed_before_send"
SUBMISSION_REASON_SENT_BUT_RESPONSE_TIMEOUT = "sent_but_response_timeout"
SUBMISSION_REASON_SENT_BUT_TRANSPORT_ERROR = "sent_but_transport_error"
SUBMISSION_REASON_AMBIGUOUS_RESPONSE = "ambiguous_response"
SUBMISSION_REASON_CONFIRMED_SUCCESS = "confirmed_success"
RUN_LOG = logging.getLogger("bithumb_bot.run")


class FillFeeStrictModeError(RuntimeError):
    """Raised when strict fee validation blocks fill aggregation."""


def _parse_fill_fee(*, fill_fee_raw: object) -> tuple[bool, float]:
    if fill_fee_raw is None:
        return False, 0.0
    try:
        fill_fee = float(fill_fee_raw)
    except (TypeError, ValueError):
        return False, 0.0
    if not math.isfinite(fill_fee) or fill_fee < 0:
        return False, 0.0
    return True, fill_fee


def _aggregate_fills_for_apply(
    *,
    fills: list[BrokerFill],
    client_order_id: str,
    exchange_order_id: str | None,
    side: str,
    context: str,
) -> list[BrokerFill]:
    if len(fills) <= 1:
        return fills

    weighted_notional = 0.0
    total_qty = 0.0
    total_fee = 0.0
    aggregate_fill_ts = 0
    invalid_fee_count = 0
    invalid_fee_notional = 0.0
    aggregate_notional = 0.0
    max_invalid_fill_notional = 0.0
    for fill in fills:
        fill_qty = float(fill.qty)
        fill_price = float(fill.price)
        fill_fee_raw = getattr(fill, "fee", None)

        if not math.isfinite(fill_qty) or fill_qty <= 0:
            RUN_LOG.warning(
                format_log_kv(
                    "[FILL_AGG] invalid fill qty skipped",
                    context=context,
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id or UNSET_EVENT_FIELD,
                    side=side,
                    fill_id=fill.fill_id,
                    qty=fill.qty,
                )
            )
            continue
        if not math.isfinite(fill_price) or fill_price <= 0:
            RUN_LOG.warning(
                format_log_kv(
                    "[FILL_AGG] invalid fill price skipped",
                    context=context,
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id or UNSET_EVENT_FIELD,
                    side=side,
                    fill_id=fill.fill_id,
                    price=fill.price,
                )
            )
            continue

        fill_notional = fill_price * fill_qty
        fee_valid, fill_fee = _parse_fill_fee(fill_fee_raw=fill_fee_raw)
        if not fee_valid:
            invalid_fee_count += 1
            invalid_fee_notional += fill_notional
            max_invalid_fill_notional = max(max_invalid_fill_notional, fill_notional)
            RUN_LOG.warning(
                format_log_kv(
                    "[FILL_AGG] missing_or_invalid fill fee; defaulting to 0",
                    context=context,
                    symbol=settings.PAIR,
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id or UNSET_EVENT_FIELD,
                    side=side,
                    fill_id=fill.fill_id,
                    fee=fill_fee_raw,
                    fill_notional=fill_notional,
                )
            )

        weighted_notional += fill_notional
        aggregate_notional += fill_notional
        total_qty += fill_qty
        total_fee += fill_fee
        aggregate_fill_ts = max(aggregate_fill_ts, int(fill.fill_ts))

    if not math.isfinite(total_qty) or total_qty <= 0:
        RUN_LOG.warning(
            format_log_kv(
                "[FILL_AGG] aggregate failed: no valid fills",
                context=context,
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id or UNSET_EVENT_FIELD,
                side=side,
                input_fill_count=len(fills),
            )
        )
        return []

    hard_alert_min_notional = max(0.0, float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW))
    strict_mode_enabled = bool(settings.LIVE_FILL_FEE_STRICT_MODE)
    strict_min_notional = max(0.0, float(settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW))
    if invalid_fee_count > 0 and math.isfinite(invalid_fee_notional):
        if invalid_fee_notional >= hard_alert_min_notional:
            alert_message = safety_event(
                "live_fill_fee_aggregate_invalid",
                client_order_id=client_order_id,
                exchange_order_id=(exchange_order_id or UNSET_EVENT_FIELD),
                side=side,
                status="FILL_AGGREGATE_FEE_ANOMALY",
                reason_code="FILL_FEE_INVALID",
                alert_kind="risk_breach",
                context=context,
                invalid_fee_count=invalid_fee_count,
                invalid_fee_notional=f"{invalid_fee_notional:.12g}",
                threshold_notional=f"{hard_alert_min_notional:.12g}",
                strict_mode_enabled=strict_mode_enabled,
                strict_min_notional=f"{strict_min_notional:.12g}",
                aggregate_notional=f"{aggregate_notional:.12g}",
                max_invalid_fill_notional=f"{max_invalid_fill_notional:.12g}",
            )
            RUN_LOG.error(
                format_log_kv(
                    "[FILL_AGG_HARD_ALERT] invalid fee encountered in high-notional aggregate",
                    context=context,
                    symbol=settings.PAIR,
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id or UNSET_EVENT_FIELD,
                    side=side,
                    invalid_fee_count=invalid_fee_count,
                    invalid_fee_notional=invalid_fee_notional,
                    aggregate_notional=aggregate_notional,
                    max_invalid_fill_notional=max_invalid_fill_notional,
                    threshold_notional=hard_alert_min_notional,
                    strict_mode_enabled=strict_mode_enabled,
                )
            )
            notify(alert_message)

        strict_violation = (
            strict_mode_enabled
            and (
                invalid_fee_notional >= strict_min_notional
                or aggregate_notional >= strict_min_notional
                or max_invalid_fill_notional >= strict_min_notional
            )
        )
        if strict_violation:
            raise FillFeeStrictModeError(
                "strict fee validation blocked fill aggregation: "
                f"context={context} invalid_fee_count={invalid_fee_count} "
                f"invalid_fee_notional={invalid_fee_notional:.12g} "
                f"aggregate_notional={aggregate_notional:.12g} "
                f"max_invalid_fill_notional={max_invalid_fill_notional:.12g} "
                f"strict_min_notional={strict_min_notional:.12g}"
            )

    aggregate_price = weighted_notional / total_qty
    aggregate_root = str(exchange_order_id or client_order_id)
    aggregate_fill_id = f"{aggregate_root}:aggregate:{aggregate_fill_ts}"
    return [
        BrokerFill(
            client_order_id=client_order_id,
            fill_id=aggregate_fill_id,
            fill_ts=aggregate_fill_ts,
            price=aggregate_price,
            qty=total_qty,
            fee=total_fee,
            exchange_order_id=exchange_order_id,
        )
    ]


def _classify_temporary_submit_error(exc: Exception) -> tuple[str, bool]:
    detail = str(exc).lower()
    if "timeout" in detail or "timed out" in detail:
        return SUBMISSION_REASON_SENT_BUT_RESPONSE_TIMEOUT, True
    return SUBMISSION_REASON_SENT_BUT_TRANSPORT_ERROR, False


def _submit_attempt_id() -> str:
    return new_client_order_id("attempt")


def _client_order_id(*, ts: int, side: str, submit_attempt_id: str) -> str:
    client_order_id = build_client_order_id(
        mode="live",
        side=side,
        intent_ts=int(ts),
        submit_attempt_id=submit_attempt_id,
    )
    if len(client_order_id) > MAX_CLIENT_ORDER_ID_LENGTH:
        raise ValueError(
            "client_order_id length overflow before broker submit: "
            f"len={len(client_order_id)} limit={MAX_CLIENT_ORDER_ID_LENGTH} "
            f"client_order_id={client_order_id}"
        )
    return client_order_id


def _as_bps(value: float, base: float) -> float:
    if not math.isfinite(base) or base <= 0:
        return float("inf")
    return (value / base) * 10_000.0


def _format_epoch_ts(epoch_sec: float | None) -> str:
    if epoch_sec is None or not math.isfinite(float(epoch_sec)):
        return "unknown"
    ts = float(epoch_sec)
    whole = int(ts)
    millis = int(round((ts - whole) * 1000.0))
    if millis >= 1000:
        whole += 1
        millis = 0
    return f"{time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(whole))}.{millis:03d}Z"


def _validated_best_quote(*, quote: BestQuote, market: str, side: str | None = None) -> tuple[float, float]:
    bid = float(quote.bid_price)
    ask = float(quote.ask_price)
    if not math.isfinite(bid) or not math.isfinite(ask) or bid <= 0 or ask <= 0:
        raise ValueError(
            "invalid best quote price: "
            f"market={market} side={side or 'UNKNOWN'} bid={bid} ask={ask}"
        )
    if bid > ask:
        raise ValueError(
            "crossed best quote: "
            f"market={market} side={side or 'UNKNOWN'} bid={bid} ask={ask}"
        )
    return bid, ask


def _load_live_reference_quote(*, pair: str, side: str | None = None) -> dict[str, float | str]:
    market = str(pair)
    try:
        quote = fetch_orderbook_top(pair)
        bid, ask = _validated_best_quote(quote=quote, market=market, side=side)
    except Exception as exc:
        raise ValueError(
            "reference price unavailable: "
            f"market={market} side={side or 'UNKNOWN'} {type(exc).__name__}: {exc}"
        ) from exc

    observed_epoch_sec = quote.observed_at_epoch_sec if quote.observed_at_epoch_sec is not None else time.time()
    reference_price = (float(bid) + float(ask)) / 2.0
    return {
        "bid": float(bid),
        "ask": float(ask),
        "reference_price": float(reference_price),
        "reference_ts_epoch_sec": float(observed_epoch_sec),
        "reference_source": quote.source or "bithumb_public_v1_orderbook",
    }


def validate_order(*, signal: str, side: str, qty: float, market_price: float) -> None:
    if signal not in ("BUY", "SELL"):
        raise ValueError(f"unsupported signal: {signal}")
    if side not in VALID_ORDER_SIDES:
        raise ValueError(f"unsupported side: {side}")
    if not math.isfinite(float(market_price)) or float(market_price) <= 0:
        raise ValueError(f"invalid market_price: {market_price}")
    if not math.isfinite(float(qty)) or float(qty) <= 0:
        raise ValueError(f"invalid order qty: {qty}")


def normalize_order_qty(*, qty: float, market_price: float) -> float:
    normalized = float(qty)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"invalid order qty: {qty}")

    rules = get_effective_order_rules(settings.PAIR).rules

    step = float(rules.qty_step)
    if math.isfinite(step) and step > 0:
        normalized = math.floor((normalized / step) + POSITION_EPSILON) * step

    max_decimals = int(rules.max_qty_decimals)
    if max_decimals > 0:
        scale = 10 ** max_decimals
        normalized = math.floor((normalized * scale) + POSITION_EPSILON) / scale

    if normalized <= 0:
        raise ValueError(f"normalized order qty is non-positive: {normalized}")

    min_qty = float(rules.min_qty)
    if min_qty > 0 and normalized < min_qty:
        raise ValueError(f"order qty below minimum: {normalized:.12f} < {min_qty:.12f}")

    return normalized


def _validate_live_price_protection(
    *,
    side: str,
    bid: float,
    ask: float,
    reference_price: float,
    reference_ts_epoch_sec: float,
    reference_source: str,
) -> None:
    max_slippage_bps = max(0.0, float(settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS))
    if max_slippage_bps <= 0:
        return

    if not math.isfinite(float(bid)) or not math.isfinite(float(ask)) or bid <= 0 or ask <= 0:
        raise ValueError(f"invalid orderbook top: side={side} bid={bid} ask={ask}")
    if bid > ask:
        raise ValueError(f"crossed orderbook top: side={side} bid={bid} ask={ask}")
    if not math.isfinite(float(reference_price)) or float(reference_price) <= 0:
        raise ValueError(f"invalid reference price: {reference_price}")
    if not math.isfinite(float(reference_ts_epoch_sec)):
        raise ValueError(f"invalid reference timestamp: {reference_ts_epoch_sec}")

    max_ref_age_sec = int(settings.LIVE_PRICE_REFERENCE_MAX_AGE_SEC)
    ref_age_sec = max(0.0, time.time() - float(reference_ts_epoch_sec))
    RUN_LOG.info(
        format_log_kv(
            "[ORDER_REF] live reference",
            side=side,
            reference_price=f"{float(reference_price):.8f}",
            reference_ts=_format_epoch_ts(reference_ts_epoch_sec),
            age_sec=f"{ref_age_sec:.3f}",
            stale_limit_sec=max_ref_age_sec,
            reference_source=reference_source,
            bid=f"{float(bid):.8f}",
            ask=f"{float(ask):.8f}",
        )
    )
    if max_ref_age_sec > 0:
        if float(ref_age_sec) > max_ref_age_sec:
            raise ValueError(
                "reference price stale: "
                f"side={side} "
                f"reference_price={float(reference_price):.8f} "
                f"reference_ts={_format_epoch_ts(reference_ts_epoch_sec)} "
                f"age_sec={float(ref_age_sec):.3f} > limit={max_ref_age_sec} "
                f"source={reference_source}"
            )

    expected_exec_price = float(ask) if side == "BUY" else float(bid)
    allowed_slippage_abs = reference_price * (max_slippage_bps / 10_000.0)

    if side == "BUY" and expected_exec_price - reference_price > allowed_slippage_abs:
        raise ValueError(
            "price protection blocked BUY: "
            f"side={side} "
            f"expected={expected_exec_price:.8f} reference={reference_price:.8f} "
            f"slippage_bps={_as_bps(expected_exec_price - reference_price, reference_price):.2f} "
            f"limit_bps={max_slippage_bps:.2f}"
        )

    if side == "SELL" and reference_price - expected_exec_price > allowed_slippage_abs:
        raise ValueError(
            "price protection blocked SELL: "
            f"side={side} "
            f"expected={expected_exec_price:.8f} reference={reference_price:.8f} "
            f"slippage_bps={_as_bps(reference_price - expected_exec_price, reference_price):.2f} "
            f"limit_bps={max_slippage_bps:.2f}"
        )


def validate_pretrade(
    *,
    broker: Broker,
    side: str,
    qty: float,
    market_price: float,
    reference_bid: float | None = None,
    reference_ask: float | None = None,
    reference_ts_epoch_sec: float | None = None,
    reference_source: str | None = None,
) -> dict[str, float | str] | None:
    if not math.isfinite(float(qty)) or float(qty) <= 0:
        raise ValueError(f"invalid order qty: {qty}")
    if not math.isfinite(float(market_price)) or float(market_price) <= 0:
        raise ValueError(f"invalid market/reference price: {market_price}")

    rules = get_effective_order_rules(settings.PAIR).rules

    notional = float(qty) * float(market_price)
    min_notional = side_min_total_krw(rules=rules, side=side)
    if min_notional > 0 and notional < min_notional:
        raise ValueError(f"order notional below minimum ({side}): {notional:.2f} < {min_notional:.2f}")

    balance_snapshot = fetch_balance_snapshot(broker)
    source_id = str(balance_snapshot.source_id or "unknown")
    observed_ts_ms = int(balance_snapshot.observed_ts_ms)
    balance = balance_snapshot.balance
    if not math.isfinite(float(balance.cash_available)) or not math.isfinite(float(balance.asset_available)):
        raise ValueError("invalid broker balance payload")
    if (
        settings.MODE == "live"
        and not bool(settings.LIVE_DRY_RUN)
        and source_id == "dry_run_static"
    ):
        raise ValueError("invalid live balance source: dry_run_static")
    if observed_ts_ms <= 0 and source_id not in {"dry_run_static", "legacy_balance_api"}:
        raise ValueError(f"invalid balance snapshot observed_ts_ms: source={source_id} observed_ts_ms={observed_ts_ms}")

    buffer_mult = 1.0 + max(0.0, float(settings.PRETRADE_BALANCE_BUFFER_BPS)) / 10_000.0
    if side == "BUY":
        # NOTE:
        # - LIVE_FEE_RATE_ESTIMATE: live pretrade 현금/잔고 보호용 보수적 추정치
        # - PAPER_FEE_RATE/FEE_RATE: paper 체결 시뮬레이션 및 기존 하위호환 fee rate
        # 두 값의 역할을 분리해 live pretrade 계산이 낙관적으로 과소추정되지 않게 한다.
        fee_mult = 1.0 + max(0.0, float(settings.LIVE_FEE_RATE_ESTIMATE))
        required_cash = notional * fee_mult * buffer_mult
        if float(balance.cash_available) + POSITION_EPSILON < required_cash:
            raise ValueError(
                f"insufficient available cash: need={required_cash:.2f} avail={float(balance.cash_available):.2f}"
            )
    elif side == "SELL":
        required_asset = float(qty) * buffer_mult
        if float(balance.asset_available) + POSITION_EPSILON < required_asset:
            raise ValueError(
                f"insufficient available asset: need={required_asset:.12f} avail={float(balance.asset_available):.12f}"
            )

    spread_limit_bps = float(settings.MAX_ORDERBOOK_SPREAD_BPS)
    slip_limit_bps = float(settings.MAX_MARKET_SLIPPAGE_BPS)
    protection_limit_bps = max(0.0, float(settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS))
    if spread_limit_bps <= 0 and slip_limit_bps <= 0 and protection_limit_bps <= 0:
        return

    if reference_bid is None or reference_ask is None:
        reference_quote = _load_live_reference_quote(pair=settings.PAIR, side=side)
    else:
        bid = float(reference_bid)
        ask = float(reference_ask)
        if not math.isfinite(bid) or not math.isfinite(ask) or bid <= 0 or ask <= 0 or bid > ask:
            raise ValueError(f"invalid orderbook top: market={settings.PAIR} side={side} bid={bid} ask={ask}")
        if reference_ts_epoch_sec is None:
            reference_ts_epoch_sec = time.time()
        reference_quote = {
            "bid": bid,
            "ask": ask,
            "reference_price": (bid + ask) / 2.0,
            "reference_ts_epoch_sec": float(reference_ts_epoch_sec),
            "reference_source": reference_source or "orderbook_top_mid",
        }

    bid = float(reference_quote["bid"])
    ask = float(reference_quote["ask"])
    reference_price = float(reference_quote["reference_price"])
    ref_ts_epoch_sec = float(reference_quote["reference_ts_epoch_sec"])
    ref_source = str(reference_quote["reference_source"])

    _validate_live_price_protection(
        side=side,
        bid=bid,
        ask=ask,
        reference_price=reference_price,
        reference_ts_epoch_sec=ref_ts_epoch_sec,
        reference_source=ref_source,
    )

    mid = (bid + ask) / 2.0
    spread_bps = _as_bps(ask - bid, mid)
    if spread_limit_bps > 0 and spread_bps > spread_limit_bps:
        raise ValueError(
            "spread guard blocked: "
            f"market={settings.PAIR} side={side} spread_bps={spread_bps:.2f} > limit={spread_limit_bps:.2f}"
        )

    exec_price = ask if side == "BUY" else bid
    reference_mid = (bid + ask) / 2.0
    slippage_bps = _as_bps(abs(exec_price - reference_mid), reference_mid)
    if slip_limit_bps > 0 and slippage_bps > slip_limit_bps:
        raise ValueError(
            "slippage guard blocked: "
            f"market={settings.PAIR} side={side} requested_price={float(market_price):.8f} "
            f"exec_price={exec_price:.8f} reference_mid={reference_mid:.8f} "
            f"bps={slippage_bps:.2f} > limit={slip_limit_bps:.2f}"
        )
    return reference_quote


def _mark_submit_unknown(*, conn, client_order_id: str, submit_attempt_id: str, side: str, reason: str) -> None:
    record_status_transition(
        client_order_id,
        from_status="PENDING_SUBMIT",
        to_status="SUBMIT_UNKNOWN",
        reason=reason,
        conn=conn,
    )
    set_status(client_order_id, "SUBMIT_UNKNOWN", last_error=reason, conn=conn)
    notify(
        safety_event(
            "order_submit_unknown",
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_from="PENDING_SUBMIT",
            state_to="SUBMIT_UNKNOWN",
            reason_code=SUBMIT_TIMEOUT,
            side=side,
            status="SUBMIT_UNKNOWN",
            reason=reason,
        )
    )


def _mark_submit_failed(*, conn, client_order_id: str, submit_attempt_id: str, side: str, reason: str) -> None:
    record_status_transition(
        client_order_id,
        from_status="PENDING_SUBMIT",
        to_status="FAILED",
        reason=reason,
        conn=conn,
    )
    set_status(client_order_id, "FAILED", last_error=reason, conn=conn)
    notify(
        safety_event(
            "order_submit_failed",
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_from="PENDING_SUBMIT",
            state_to="FAILED",
            reason_code=SUBMIT_FAILED,
            side=side,
            status="FAILED",
            reason=reason,
        )
    )


def _mark_recovery_required(*, conn, client_order_id: str, side: str, from_status: str, reason: str) -> None:
    record_status_transition(
        client_order_id,
        from_status=from_status,
        to_status="RECOVERY_REQUIRED",
        reason=reason,
        conn=conn,
    )
    set_status(
        client_order_id,
        "RECOVERY_REQUIRED",
        last_error=reason,
        conn=conn,
    )
    notify(
        safety_event(
            "recovery_required_transition",
            client_order_id=client_order_id,
            submit_attempt_id=UNSET_EVENT_FIELD,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_from=from_status,
            state_to="RECOVERY_REQUIRED",
            reason_code=AMBIGUOUS_SUBMIT,
            side=side,
            status="RECOVERY_REQUIRED",
            reason=reason,
        )
    )


def _block_new_submission_for_unresolved_risk(
    *,
    conn,
    client_order_id: str,
    side: str,
    qty: float,
    ts: int,
    reason_code: str,
    reason: str,
) -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        submit_attempt_id=None,
        side=side,
        qty_req=qty,
        price=None,
        ts_ms=ts,
        status="FAILED",
    )
    persisted_reason = f"code={reason_code};reason={reason}"
    record_submit_blocked(client_order_id, status="FAILED", reason=persisted_reason, conn=conn)
    notify(
        safety_event(
            "order_submit_blocked",
            client_order_id=client_order_id,
            submit_attempt_id=client_order_id.split("_")[-1],
            reason_code=RISKY_ORDER_BLOCK,
            side=side,
            status="FAILED",
            reason_detail_code=reason_code,
            reason=persisted_reason,
        )
    )


def _record_submit_attempt_result(
    *,
    conn,
    client_order_id: str,
    submit_attempt_id: str,
    symbol: str,
    side: str,
    qty: float,
    ts: int,
    payload_hash: str,
    reference_price: float | None,
    order_status: str,
    broker_response_summary: str,
    submission_reason_code: str,
    exception_class: str | None,
    timeout_flag: bool,
    submit_evidence: str | None,
    exchange_order_id_obtained: bool,
) -> None:
    record_submit_attempt(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        price=reference_price,
        submit_ts=ts,
        payload_fingerprint=payload_hash,
        broker_response_summary=broker_response_summary,
        submission_reason_code=submission_reason_code,
        exception_class=exception_class,
        timeout_flag=timeout_flag,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=exchange_order_id_obtained,
        order_status=order_status,
    )


def _record_submit_attempt_preflight(
    *,
    conn,
    client_order_id: str,
    submit_attempt_id: str,
    symbol: str,
    side: str,
    qty: float,
    ts: int,
    payload_hash: str,
    reference_price: float | None,
    submit_evidence: str | None,
) -> None:
    record_submit_attempt(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        price=reference_price,
        submit_ts=ts,
        payload_fingerprint=payload_hash,
        broker_response_summary="submit_dispatched",
        submission_reason_code="submit_dispatched_preflight",
        exception_class=None,
        timeout_flag=False,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=False,
        order_status="PENDING_SUBMIT",
        event_type="submit_attempt_preflight",
    )


def _order_intent_strategy_context() -> str:
    return f"{settings.MODE}:{settings.STRATEGY_NAME}:{settings.INTERVAL}"


def _order_intent_type(*, side: str) -> str:
    return "market_entry" if side == "BUY" else "market_exit"

def _encode_submit_evidence(*, payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _submit_via_standard_path(
    *,
    conn,
    broker: Broker,
    signal: str,
    client_order_id: str,
    submit_attempt_id: str,
    side: str,
    order_qty: float,
    qty: float,
    ts: int,
    intent_key: str,
    market_price: float,
    reference_price: float | None,
    top_of_book_summary: dict[str, float | str] | None,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
):
    symbol = settings.PAIR
    payload = {
        "client_order_id": client_order_id,
        "submit_attempt_id": submit_attempt_id,
        "symbol": symbol,
        "side": side,
        "qty": float(qty),
        "price": reference_price,
        "submit_ts": int(ts),
    }
    payload_hash = payload_fingerprint(payload)
    submit_path = "live_standard_market"
    preflight_evidence = _encode_submit_evidence(
        payload={
            "symbol": symbol,
            "side": side,
            "intended_qty": float(qty),
            "reference_price": reference_price,
            "top_of_book": top_of_book_summary,
            "request_ts": None,
            "response_ts": None,
            "submit_path": submit_path,
            "submit_mode": settings.MODE,
            "error_class": None,
            "error_summary": None,
        }
    )

    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        side=side,
        qty_req=qty,
        price=None,
        strategy_name=strategy_name,
        entry_decision_id=(decision_id if side == "BUY" else None),
        exit_decision_id=(decision_id if side == "SELL" else None),
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        ts_ms=ts,
        status="PENDING_SUBMIT",
    )
    record_submit_started(client_order_id, conn=conn)
    _record_submit_attempt_preflight(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        ts=ts,
        payload_hash=payload_hash,
        reference_price=reference_price,
        submit_evidence=preflight_evidence,
    )
    notify(
        safety_event(
            "order_submit_started",
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_to="PENDING_SUBMIT",
            reason_code=UNSET_EVENT_FIELD,
            side=side,
            status="PENDING_SUBMIT",
        )
    )
    conn.commit()

    try:
        RUN_LOG.info(
            format_log_kv(
                "[ORDER_DECISION] broker.place_order dispatch",
                signal=signal,
                side=side,
                market_price=market_price,
                order_qty=order_qty,
                normalized_qty=qty,
                reference_price=reference_price,
                client_order_id=client_order_id,
            )
        )
        request_ts = int(time.time() * 1000)
        order = broker.place_order(client_order_id=client_order_id, side=side, qty=qty, price=None)
        response_ts = int(time.time() * 1000)
    except BrokerTemporaryError as e:
        response_ts = int(time.time() * 1000)
        err = BrokerSubmissionUnknownError(f"submit unknown: {type(e).__name__}: {e}")
        submission_reason_code, timeout_flag = _classify_temporary_submit_error(e)
        submit_evidence = _encode_submit_evidence(
            payload={
                "symbol": symbol,
                "side": side,
                "intended_qty": float(qty),
                "reference_price": reference_price,
                "top_of_book": top_of_book_summary,
                "request_ts": request_ts,
                "response_ts": response_ts,
                "submit_path": submit_path,
                "submit_mode": settings.MODE,
                "error_class": type(e).__name__,
                "error_summary": str(e),
            }
        )
        _mark_submit_unknown(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=side,
            reason=str(err),
        )
        _record_submit_attempt_result(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            symbol=symbol,
            side=side,
            qty=qty,
            ts=ts,
            payload_hash=payload_hash,
            reference_price=reference_price,
            order_status="SUBMIT_UNKNOWN",
            broker_response_summary=f"submit_exception={type(e).__name__};error={e}",
            submission_reason_code=submission_reason_code,
            exception_class=type(e).__name__,
            timeout_flag=timeout_flag,
            submit_evidence=submit_evidence,
            exchange_order_id_obtained=False,
        )
        update_order_intent_dedup(
            conn,
            intent_key=intent_key,
            client_order_id=client_order_id,
            order_status="SUBMIT_UNKNOWN",
            last_error=str(err),
        )
        conn.commit()
        return None
    except Exception as e:
        response_ts = int(time.time() * 1000)
        reason = f"submit failed: {type(e).__name__}: {e}"
        submit_evidence = _encode_submit_evidence(
            payload={
                "symbol": symbol,
                "side": side,
                "intended_qty": float(qty),
                "reference_price": reference_price,
                "top_of_book": top_of_book_summary,
                "request_ts": request_ts,
                "response_ts": response_ts,
                "submit_path": submit_path,
                "submit_mode": settings.MODE,
                "error_class": type(e).__name__,
                "error_summary": str(e),
            }
        )
        _mark_submit_failed(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=side,
            reason=reason,
        )
        _record_submit_attempt_result(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            symbol=symbol,
            side=side,
            qty=qty,
            ts=ts,
            payload_hash=payload_hash,
            reference_price=reference_price,
            order_status="FAILED",
            broker_response_summary=f"submit_exception={type(e).__name__};error={e}",
            submission_reason_code=SUBMISSION_REASON_FAILED_BEFORE_SEND,
            exception_class=type(e).__name__,
            timeout_flag=False,
            submit_evidence=submit_evidence,
            exchange_order_id_obtained=False,
        )
        update_order_intent_dedup(
            conn,
            intent_key=intent_key,
            client_order_id=client_order_id,
            order_status="FAILED",
            last_error=reason,
        )
        conn.commit()
        return None

    if order.exchange_order_id:
        set_exchange_order_id(client_order_id, order.exchange_order_id, conn=conn)
        notify(
            safety_event(
                "exchange_order_id_attached",
                client_order_id=client_order_id,
                submit_attempt_id=submit_attempt_id,
                exchange_order_id=order.exchange_order_id,
                reason_code=UNSET_EVENT_FIELD,
                side=side,
                status=order.status,
            )
        )
    if not order.exchange_order_id:
        reason = "submit acknowledged without exchange_order_id; classification=SUBMIT_UNKNOWN"
        submit_evidence = _encode_submit_evidence(
            payload={
                "symbol": symbol,
                "side": side,
                "intended_qty": float(qty),
                "reference_price": reference_price,
                "top_of_book": top_of_book_summary,
                "request_ts": request_ts,
                "response_ts": response_ts,
                "submit_path": submit_path,
                "submit_mode": settings.MODE,
                "error_class": None,
                "error_summary": "missing exchange_order_id",
            }
        )
        _mark_submit_unknown(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=side,
            reason=reason,
        )
        _record_submit_attempt_result(
            conn=conn,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            symbol=symbol,
            side=side,
            qty=qty,
            ts=ts,
            payload_hash=payload_hash,
            reference_price=reference_price,
            order_status="SUBMIT_UNKNOWN",
            broker_response_summary=f"broker_status={order.status};exchange_order_id=-",
            submission_reason_code=SUBMISSION_REASON_AMBIGUOUS_RESPONSE,
            exception_class=None,
            timeout_flag=False,
            submit_evidence=submit_evidence,
            exchange_order_id_obtained=False,
        )
        update_order_intent_dedup(
            conn,
            intent_key=intent_key,
            client_order_id=client_order_id,
            order_status="SUBMIT_UNKNOWN",
            last_error=reason,
        )
        conn.commit()
        return None

    set_status(client_order_id, order.status, conn=conn)

    submit_evidence = _encode_submit_evidence(
        payload={
            "symbol": symbol,
            "side": side,
            "intended_qty": float(qty),
            "reference_price": reference_price,
            "top_of_book": top_of_book_summary,
            "request_ts": request_ts,
            "response_ts": response_ts,
            "submit_path": submit_path,
            "submit_mode": settings.MODE,
            "error_class": None,
            "error_summary": None,
        }
    )

    _record_submit_attempt_result(
        conn=conn,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        ts=ts,
        payload_hash=payload_hash,
        reference_price=reference_price,
        order_status=order.status,
        broker_response_summary=f"broker_status={order.status};exchange_order_id={order.exchange_order_id}",
        submission_reason_code=SUBMISSION_REASON_CONFIRMED_SUCCESS,
        exception_class=None,
        timeout_flag=False,
        submit_evidence=submit_evidence,
        exchange_order_id_obtained=True,
    )
    update_order_intent_dedup(
        conn,
        intent_key=intent_key,
        client_order_id=client_order_id,
        order_status=order.status,
    )
    conn.commit()
    return order


def live_execute_signal(
    broker: Broker,
    signal: str,
    ts: int,
    market_price: float,
    *,
    strategy_name: str | None = None,
    decision_id: int | None = None,
    decision_reason: str | None = None,
    exit_rule_name: str | None = None,
) -> dict | None:
    conn = ensure_db()
    try:
        init_portfolio(conn)
        state = runtime_state.snapshot()

        if state.halt_new_orders_blocked:
            halt_reason = f"runtime halted: code={state.halt_reason_code or '-'} reason={state.last_disable_reason or '-'}"
            RUN_LOG.info(format_log_kv("[ORDER_SKIP] runtime halt", side=signal, reason=halt_reason, signal=signal))
            notify(
                safety_event(  # CHANGED
                    "order_submit_blocked",
                    client_order_id=UNSET_EVENT_FIELD,
                    submit_attempt_id=UNSET_EVENT_FIELD,
                    exchange_order_id=UNSET_EVENT_FIELD,
                    status="HALTED",
                    state_to="HALTED",
                    reason_code=RISKY_ORDER_BLOCK,
                    halt_detail_code=state.halt_reason_code or "-",
                    reason=halt_reason,
                )
            )
            return None

        cash, qty = get_portfolio(conn)

        if signal == "BUY" and qty <= POSITION_EPSILON:
            if not math.isfinite(float(market_price)) or float(market_price) <= 0:
                reason = f"invalid market/reference price: {market_price}"
                RUN_LOG.info(format_log_kv("[ORDER_SKIP] invalid market price", side="BUY", reason=reason, signal=signal))
                notify(f"live pretrade validation blocked (BUY): {reason}")
                return None

            blocked, guardrail_reason = evaluate_buy_guardrails(conn=conn, ts_ms=ts, cash=cash, qty=qty, price=market_price)
            if blocked:
                RUN_LOG.info(format_log_kv("[ORDER_SKIP] buy guardrails", signal=signal, side="BUY", reason=guardrail_reason or "blocked"))
                return None

            spend = cash * float(settings.BUY_FRACTION)
            if settings.MAX_ORDER_KRW > 0:
                spend = min(spend, float(settings.MAX_ORDER_KRW))
            if spend <= 0:
                RUN_LOG.info(format_log_kv("[ORDER_SKIP] non-positive spend", side="BUY", reason=f"spend={float(spend):.8f}", signal=signal))
                return None

            order_qty = max(0.0, spend / market_price)
            side = "BUY"

        elif signal == "SELL" and qty > POSITION_EPSILON:
            order_qty = qty
            side = "SELL"

        else:
            skip_reason = "no actionable position state for signal"
            RUN_LOG.info(format_log_kv("[ORDER_SKIP] no-op signal", side=signal, reason=skip_reason, signal=signal, position_qty=f"{float(qty):.12f}"))
            return None

        reference_quote: dict[str, float | str] | None = None
        pretrade_needs_live_reference = any(
            limit > 0
            for limit in (
                float(settings.MAX_ORDERBOOK_SPREAD_BPS),
                float(settings.MAX_MARKET_SLIPPAGE_BPS),
                max(0.0, float(settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS)),
            )
        )
        try:
            if pretrade_needs_live_reference:
                reference_quote = _load_live_reference_quote(pair=settings.PAIR)
            normalized_qty = normalize_order_qty(qty=order_qty, market_price=market_price)
            validate_order(signal=signal, side=side, qty=normalized_qty, market_price=market_price)
            validate_pretrade(
                broker=broker,
                side=side,
                qty=normalized_qty,
                market_price=market_price,
                reference_bid=(float(reference_quote["bid"]) if reference_quote is not None else None),
                reference_ask=(float(reference_quote["ask"]) if reference_quote is not None else None),
                reference_ts_epoch_sec=(
                    float(reference_quote["reference_ts_epoch_sec"]) if reference_quote is not None else None
                ),
                reference_source=(str(reference_quote["reference_source"]) if reference_quote is not None else None),
            )
        except ValueError as e:
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] pretrade blocked",
                    signal=signal,
                    side=side,
                    reason=str(e),
                    market_price=market_price,
                    order_qty=order_qty,
                )
            )
            notify(f"live pretrade validation blocked ({side}): {e}")
            return None

        submit_attempt_id = _submit_attempt_id()
        client_order_id = _client_order_id(ts=ts, side=side, submit_attempt_id=submit_attempt_id)
        strategy_context = _order_intent_strategy_context()
        intent_type = _order_intent_type(side=side)
        intent_key = build_order_intent_key(
            symbol=settings.PAIR,
            side=side,
            strategy_context=strategy_context,
            intent_ts=int(ts),
            intent_type=intent_type,
            qty=normalized_qty,
        )

        reference_price: float | None = None
        top_of_book_summary: dict[str, float | str] | None = None
        if reference_quote is not None:
            reference_price = float(reference_quote["reference_price"])
            top_of_book_summary = {
                "bid": float(reference_quote["bid"]),
                "ask": float(reference_quote["ask"]),
                "spread": float(reference_quote["ask"]) - float(reference_quote["bid"]),
                "reference_ts": _format_epoch_ts(float(reference_quote["reference_ts_epoch_sec"])),
                "reference_source": str(reference_quote["reference_source"]),
            }
        else:
            try:
                reference_quote = _load_live_reference_quote(pair=settings.PAIR)
                reference_price = float(reference_quote["reference_price"])
                top_of_book_summary = {
                    "bid": float(reference_quote["bid"]),
                    "ask": float(reference_quote["ask"]),
                    "spread": float(reference_quote["ask"]) - float(reference_quote["bid"]),
                    "reference_ts": _format_epoch_ts(float(reference_quote["reference_ts_epoch_sec"])),
                    "reference_source": str(reference_quote["reference_source"]),
                }
            except ValueError as exc:
                reference_price = None
                top_of_book_summary = {"error": str(exc).removeprefix("reference price unavailable: ")}

        blocked, reason = evaluate_order_submission_halt(
            conn,
            ts_ms=int(ts),
            now_ms=int(time.time() * 1000),
            cash=float(cash),
            qty=float(qty),
            price=float(market_price),
        )
        if blocked:
            gate_blocked, reason_code, gate_reason = evaluate_unresolved_order_gate(
                conn,
                now_ms=int(time.time() * 1000),
                max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
            )
            if gate_blocked:
                RUN_LOG.info(
                    format_log_kv(
                        "[ORDER_SKIP] unresolved risk gate",
                        signal=signal,
                        side=side,
                        reason=gate_reason,
                    )
                )
                _block_new_submission_for_unresolved_risk(
                    conn=conn,
                    client_order_id=client_order_id,
                    side=side,
                    qty=normalized_qty,
                    ts=ts,
                    reason_code=reason_code,
                    reason=gate_reason,
                )
                conn.commit()
                return None

            RUN_LOG.info(format_log_kv("[ORDER_SKIP] submission halt", signal=signal, side=side, reason=reason))
            notify(f"live order placement blocked ({side}): {reason}")
            return None

        existing = conn.execute(
            "SELECT status FROM orders WHERE client_order_id=?",
            (client_order_id,),
        ).fetchone()
        if existing is not None:
            existing_status = str(existing["status"])
            if existing_status in TERMINAL_ORDER_STATUSES:
                reason = f"duplicate submit blocked: terminal status {existing_status}"
                RUN_LOG.info(format_log_kv("[ORDER_SKIP] duplicate client order id", signal=signal, side=side, reason=reason, client_order_id=client_order_id))
                record_submit_blocked(client_order_id, status=existing_status, reason=reason, conn=conn)
                notify(
                    safety_event(  # CHANGED
                        "order_submit_blocked",
                        client_order_id=client_order_id,
                        submit_attempt_id=submit_attempt_id,
                        side=side,
                        status=existing_status,
                        reason_code=RISKY_ORDER_BLOCK,
                        reason=reason,
                    )
                )
                conn.commit()
                return None

        claimed, existing_intent = claim_order_intent_dedup(
            conn,
            intent_key=intent_key,
            client_order_id=client_order_id,
            symbol=settings.PAIR,
            side=side,
            strategy_context=strategy_context,
            intent_type=intent_type,
            intent_ts=int(ts),
            qty=normalized_qty,
            order_status="PENDING_SUBMIT",
        )
        if not claimed:
            existing_client_order_id = (
                str(existing_intent["client_order_id"])
                if existing_intent is not None and existing_intent["client_order_id"] is not None
                else "-"
            )
            existing_status = (
                str(existing_intent["order_status"])
                if existing_intent is not None and existing_intent["order_status"] is not None
                else "UNKNOWN"
            )
            skip_reason = (
                f"duplicate intent already recorded "
                f"existing_client_order_id={existing_client_order_id} existing_status={existing_status}"
            )
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] duplicate order intent",
                    mode=settings.MODE,
                    symbol=settings.PAIR,
                    side=side,
                    qty=f"{float(normalized_qty):.12f}",
                    intent_ts=int(ts),
                    intent_key=intent_key,
                    reason=skip_reason,
                )
            )
            notify(
                format_event(
                    "order_intent_dedup_skip",
                    symbol=settings.PAIR,
                    side=side,
                    qty=float(normalized_qty),
                    intent_ts=int(ts),
                    client_order_id=client_order_id,
                    dedup_key=intent_key,
                    skip_reason=skip_reason,
                    existing_client_order_id=existing_client_order_id,
                    existing_status=existing_status,
                )
            )
            conn.commit()
            return None

        RUN_LOG.info(
            format_log_kv(
                "[ORDER_DECISION] submit order intent",
                mode=settings.MODE,
                symbol=settings.PAIR,
                signal=signal,
                side=side,
                market_price=market_price,
                order_qty=order_qty,
                normalized_qty=normalized_qty,
                reference_price=reference_price,
                client_order_id=client_order_id,
                intent_ts=int(ts),
                intent_key=intent_key,
                top_of_book=top_of_book_summary,
            )
        )

        order = _submit_via_standard_path(
            conn=conn,
            broker=broker,
            signal=signal,
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            side=side,
            order_qty=order_qty,
            qty=normalized_qty,
            ts=ts,
            intent_key=intent_key,
            market_price=market_price,
            reference_price=reference_price,
            top_of_book_summary=top_of_book_summary,
            strategy_name=(strategy_name or settings.STRATEGY_NAME),
            decision_id=decision_id,
            decision_reason=decision_reason,
            exit_rule_name=exit_rule_name,
        )
        if order is None:
            return None

        fills = broker.get_fills(client_order_id=client_order_id, exchange_order_id=order.exchange_order_id)
        try:
            fills_to_apply = _aggregate_fills_for_apply(
                fills=fills,
                client_order_id=client_order_id,
                exchange_order_id=order.exchange_order_id,
                side=side,
                context="_submit_via_standard_path",
            )
        except FillFeeStrictModeError as exc:
            from_status = str(order.status or "NEW")
            _mark_recovery_required(
                conn=conn,
                client_order_id=client_order_id,
                side=side,
                from_status=from_status,
                reason=str(exc),
            )
            update_order_intent_dedup(
                conn,
                intent_key=intent_key,
                client_order_id=client_order_id,
                order_status="RECOVERY_REQUIRED",
            )
            conn.commit()
            RUN_LOG.error(
                format_log_kv(
                    "[FILL_AGG] strict mode blocked aggregate; transitioned to recovery required",
                    client_order_id=client_order_id,
                    exchange_order_id=order.exchange_order_id or UNSET_EVENT_FIELD,
                    side=side,
                    from_status=from_status,
                    reason=str(exc),
                )
            )
            return None
        trade = None
        for fill in fills_to_apply:
            trade = apply_fill_and_trade(
                conn,
                client_order_id=client_order_id,
                side=side,
                fill_id=fill.fill_id,
                fill_ts=fill.fill_ts,
                price=fill.price,
                qty=fill.qty,
                fee=fill.fee,
                strategy_name=(strategy_name or settings.STRATEGY_NAME),
                entry_decision_id=(decision_id if side == "BUY" else None),
                exit_decision_id=(decision_id if side == "SELL" else None),
                exit_reason=(decision_reason if side == "SELL" else None),
                exit_rule_name=(exit_rule_name if side == "SELL" else None),
                note=f"live exchange_order_id={order.exchange_order_id}",
            ) or trade

        refreshed = broker.get_order(client_order_id=client_order_id, exchange_order_id=order.exchange_order_id)
        set_status(client_order_id, refreshed.status, conn=conn)
        update_order_intent_dedup(
            conn,
            intent_key=intent_key,
            client_order_id=client_order_id,
            order_status=refreshed.status,
        )
        conn.commit()
        return trade

    finally:
        conn.close()
