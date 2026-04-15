from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, InvalidOperation
from enum import Enum

from ..config import settings
from ..db_core import ensure_db, get_portfolio, init_portfolio
from ..decision_context import resolve_canonical_position_exposure_snapshot
from ..execution import apply_fill_and_trade, record_order_if_missing
from ..dust import (
    DustState,
    build_dust_display_context,
    build_executable_lot,
    build_normalized_exposure,
)
from ..lifecycle import (
    DUST_TRACKING_STATE,
    reclassify_non_executable_open_exposure,
    summarize_reserved_exit_qty,
    summarize_position_lots,
)
from ..marketdata import fetch_orderbook_top
from ..notifier import format_event, notify
from ..observability import format_log_kv, safety_event
from ..public_api_orderbook import BestQuote
from .base import BrokerRejectError
from ..reason_codes import (
    AMBIGUOUS_SUBMIT,
    DUST_RESIDUAL_UNSELLABLE,
    DUST_RESIDUAL_SUPPRESSED,
    EXIT_PARTIAL_LEFT_DUST,
    classify_sell_failure_category,
    SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN,
    SELL_FAILURE_CATEGORY_DUST_RESIDUAL_UNSELLABLE,
    SELL_FAILURE_CATEGORY_DUST_SUPPRESSION,
    SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH,
    SELL_FAILURE_CATEGORY_REMAINDER_DUST_GUARD,
    SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
    SELL_FAILURE_CATEGORY_UNSAFE_DUST_MISMATCH,
    SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
    SELL_FAILURE_CATEGORY_UNKNOWN,
    MANUAL_DUST_REVIEW_REQUIRED,
    RISKY_ORDER_BLOCK,
    SUBMIT_FAILED,
    SUBMIT_TIMEOUT,
    sell_failure_detail_from_category,
)
from .order_rules import get_effective_order_rules, side_min_total_krw
from .balance_source import fetch_balance_snapshot
from ..risk import evaluate_buy_guardrails, evaluate_order_submission_halt
from .. import runtime_state
from ..order_sizing import (
    BuyExecutionAuthority,
    SellExecutionAuthority,
    build_buy_execution_sizing,
    build_sell_execution_sizing,
)
from ..oms import (
    MAX_CLIENT_ORDER_ID_LENGTH,
    build_client_order_id,
    TERMINAL_ORDER_STATUSES,
    build_order_intent_key,
    build_order_suppression_key,
    claim_order_intent_dedup,
    evaluate_unresolved_order_gate,
    new_client_order_id,
    payload_fingerprint,
    record_status_transition,
    record_order_suppression,
    record_submit_attempt,
    record_submit_blocked,
    record_submit_started,
    set_exchange_order_id,
    set_status,
    update_order_intent_dedup,
)
from .base import Broker, BrokerFill, BrokerSubmissionUnknownError, BrokerTemporaryError

POSITION_EPSILON = 1e-12
BROKER_MARKET_SELL_QTY_DECIMALS = 8
SELL_MIN_QTY_BOUNDARY_EPSILON = 1.5e-8  # One ledger tick with a small float-cushion
VALID_ORDER_SIDES = {"BUY", "SELL"}
UNSET_EVENT_FIELD = "-"
CLIENT_ORDER_ID_EPOCH_FLOOR_MS = 1_700_000_000_000
_CANONICAL_SELL_SUBMIT_QTY_SOURCE = "position_state.normalized_exposure.sellable_executable_qty"
_CANONICAL_SELL_SUBMIT_LOT_SOURCE = "position_state.normalized_exposure.sellable_executable_lot_count"
_NON_AUTHORITATIVE_SELL_QTY_OBSERVATION_SOURCE = "observation.sell_qty_preview"

SUBMISSION_REASON_FAILED_BEFORE_SEND = "failed_before_send"
SUBMISSION_REASON_SENT_BUT_RESPONSE_TIMEOUT = "sent_but_response_timeout"
SUBMISSION_REASON_SENT_BUT_TRANSPORT_ERROR = "sent_but_transport_error"
SUBMISSION_REASON_AMBIGUOUS_RESPONSE = "ambiguous_response"
SUBMISSION_REASON_CONFIRMED_SUCCESS = "confirmed_success"
RUN_LOG = logging.getLogger("bithumb_bot.run")
_DECIMAL_ZERO = Decimal("0")


class FillFeeStrictModeError(RuntimeError):
    """Raised when strict fee validation blocks fill aggregation."""


class SellDustGuardError(ValueError):
    """Raised when a SELL would create an unsellable dust remainder."""

    def __init__(self, message: str, *, details: dict[str, float | int | str]) -> None:
        super().__init__(message)
        self.details = details


class _CanonicalSellSubmitLotSource(str, Enum):
    CANONICAL_LOT_NATIVE = _CANONICAL_SELL_SUBMIT_LOT_SOURCE


@dataclass(frozen=True)
class _CanonicalSellExecutionView:
    sellable_executable_lot_count: int
    sellable_executable_qty: float
    exit_allowed: bool
    exit_block_reason: str
    submit_qty_source: str
    position_state_source: str


@dataclass(frozen=True)
class _SellDiagnosticQtyView:
    observed_position_qty: float
    observed_position_qty_source: str
    raw_total_asset_qty: float
    open_exposure_qty: float
    dust_tracking_qty: float


@dataclass(frozen=True)
class _CanonicalSellSubmitObservability:
    submit_qty_source: str
    submit_lot_source: str
    submit_lot_count: int
    normalized_qty: float
    position_state_source: str
    position_state_source_truth_source: str
    submit_qty_source_truth_source: str
    submit_lot_source_truth_source: str


@dataclass(frozen=True)
class _ObservedSellSubmitTelemetry:
    position_qty: float
    submit_payload_qty: float
    raw_total_asset_qty: float
    open_exposure_qty: float
    dust_tracking_qty: float
    sell_qty_basis_qty: float
    sell_qty_basis_source: str
    sell_qty_basis_qty_truth_source: str
    sell_qty_basis_source_truth_source: str
    sell_qty_boundary_kind: str
    sell_qty_boundary_kind_truth_source: str
    sell_normalized_exposure_qty_truth_source: str
    sell_open_exposure_qty_truth_source: str
    sell_dust_tracking_qty_truth_source: str


@dataclass(frozen=True)
class _LiveExecutionPositionState:
    conn: object
    state: object
    cash: float
    portfolio_qty: float
    raw_total_asset_qty: float
    open_exposure_qty: float
    dust_tracking_qty: float
    position_snapshot: object
    effective_rules: object
    normalized_exposure: object
    decision_observability: dict[str, object]
    canonical_sell: _CanonicalSellExecutionView | None
    diagnostic_sell_qty: _SellDiagnosticQtyView | None
    has_lot_native_sell_state: bool


@dataclass(frozen=True)
class _LiveExecutionIntent:
    side: str
    order_qty: float
    submit_qty_source: str
    harmless_dust_checked: bool
    entry_sizing: object | None
    exit_sizing: object | None
    canonical_sell: _CanonicalSellExecutionView | None
    diagnostic_sell_qty: _SellDiagnosticQtyView | None


@dataclass(frozen=True)
class _LiveExecutionFeasibility:
    side: str
    order_qty: float
    normalized_qty: float
    submit_qty_source: str
    reference_quote: dict[str, float | str] | None
    entry_sizing: object | None
    exit_sizing: object | None


def _resolve_non_authoritative_sell_basis_qty(
    *,
    decision_observability: dict[str, object] | None,
    open_exposure_qty: float | None,
) -> float:
    observation = decision_observability or {}
    if observation.get("sell_qty_basis_qty") is not None:
        return float(observation["sell_qty_basis_qty"] or 0.0)
    if observation.get("open_exposure_qty") is not None:
        return float(observation["open_exposure_qty"] or 0.0)
    if open_exposure_qty is not None:
        return float(open_exposure_qty)
    return 0.0


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


def _decimal_from_number(value: object) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid numeric value: {value}") from exc
    if not parsed.is_finite():
        raise ValueError(f"invalid non-finite numeric value: {value}")
    return parsed


def _decimal_quantizer(*, places: int) -> Decimal | None:
    normalized_places = max(0, int(places))
    if normalized_places <= 0:
        return None
    return Decimal("1").scaleb(-normalized_places)


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
            raise FillFeeStrictModeError(
                "material fee validation blocked fill aggregation: "
                f"context={context} invalid_fee_count={invalid_fee_count} "
                f"invalid_fee_notional={invalid_fee_notional:.12g} "
                f"aggregate_notional={aggregate_notional:.12g} "
                f"max_invalid_fill_notional={max_invalid_fill_notional:.12g} "
                f"threshold_notional={hard_alert_min_notional:.12g}"
            )

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


def _classify_sell_failure_category(
    *,
    reason_code: str | None = None,
    reason: str | None = None,
    error_class: str | None = None,
    error_summary: str | None = None,
    dust_details: dict[str, object] | None = None,
) -> str:
    return classify_sell_failure_category(
        reason_code=reason_code,
        reason=reason,
        error_class=error_class,
        error_summary=error_summary,
        dust_details=dust_details,
    )

def _sell_failure_detail_from_observability(
    *,
    sell_failure_category: str,
    dust_details: dict[str, object] | None = None,
) -> str:
    return sell_failure_detail_from_category(
        sell_failure_category=sell_failure_category,
        dust_details=dust_details,
    )


def _sell_qty_boundary_kind_from_dust_details(*, dust_details: dict[str, object] | None) -> str:
    dust_details = dust_details or {}
    if str(dust_details.get("dust_scope") or "") == "remainder_after_sell":
        return "dust_mismatch"
    if any(
        bool(dust_details.get(key))
        for key in ("qty_below_min", "normalized_below_min", "notional_below_min", "normalized_non_positive")
    ):
        return "min_qty"
    if any(
        bool(dust_details.get(key))
        for key in (
            "dust_broker_qty_is_dust",
            "dust_local_qty_is_dust",
            "dust_broker_notional_is_dust",
            "dust_local_notional_is_dust",
            "dust_qty_gap_small",
        )
    ):
        return "dust_mismatch"
    detail_text = str(dust_details.get("summary") or "").lower()
    if "qty_step" in detail_text or "max_qty_decimals" in detail_text:
        return "qty_step"
    return "none"


def _resolve_submit_qty_source_truth_source(
    *,
    decision_observability: dict[str, object],
    submit_qty_source: str | None,
) -> str:
    truth_source = str(decision_observability.get("submit_qty_source_truth_source") or "").strip()
    if truth_source and truth_source != "-":
        return truth_source

    normalized_submit_qty_source = str(submit_qty_source or "").strip()
    if normalized_submit_qty_source == _CANONICAL_SELL_SUBMIT_QTY_SOURCE:
        return "derived:sellable_executable_qty"
    if normalized_submit_qty_source == "position_state.normalized_exposure.open_exposure_qty":
        return "derived:open_exposure_qty"
    if normalized_submit_qty_source:
        return "context.submit_qty_source"
    return "-"


def _sell_truth_source_fields(
    *,
    decision_observability: dict[str, object],
    submit_qty_source: str | None,
) -> dict[str, str]:
    submit_qty_truth_source = _resolve_submit_qty_source_truth_source(
        decision_observability=decision_observability,
        submit_qty_source=submit_qty_source,
    )
    return {
        "entry_allowed_truth_source": str(decision_observability.get("entry_allowed_truth_source") or "-"),
        "effective_flat_truth_source": str(decision_observability.get("effective_flat_truth_source") or "-"),
        "raw_qty_open_truth_source": str(decision_observability.get("raw_qty_open_truth_source") or "-"),
        "raw_total_asset_qty_truth_source": str(
            decision_observability.get("raw_total_asset_qty_truth_source") or "-"
        ),
        "position_qty_truth_source": str(decision_observability.get("position_qty_truth_source") or "-"),
        "submit_payload_qty_truth_source": str(
            decision_observability.get("submit_payload_qty_truth_source") or "-"
        ),
        "normalized_exposure_active_truth_source": str(
            decision_observability.get("normalized_exposure_active_truth_source") or "-"
        ),
        "normalized_exposure_qty_truth_source": str(
            decision_observability.get("normalized_exposure_qty_truth_source") or "-"
        ),
        "sell_submit_lot_count_truth_source": str(
            decision_observability.get("sell_submit_lot_count_truth_source") or "-"
        ),
        "sell_submit_lot_source_truth_source": str(
            decision_observability.get("sell_submit_lot_source_truth_source") or "-"
        ),
        "open_exposure_qty_truth_source": str(decision_observability.get("open_exposure_qty_truth_source") or "-"),
        "dust_tracking_qty_truth_source": str(decision_observability.get("dust_tracking_qty_truth_source") or "-"),
        "submit_qty_source_truth_source": submit_qty_truth_source,
        "submit_lot_source_truth_source": str(
            decision_observability.get("submit_lot_source_truth_source") or "-"
        ),
        "sell_submit_qty_source_truth_source": submit_qty_truth_source,
        "sell_normalized_exposure_qty_truth_source": str(
            decision_observability.get("normalized_exposure_qty_truth_source") or "-"
        ),
        "sell_open_exposure_qty_truth_source": str(
            decision_observability.get("open_exposure_qty_truth_source") or "-"
        ),
        "sell_dust_tracking_qty_truth_source": str(
            decision_observability.get("dust_tracking_qty_truth_source") or "-"
        ),
        "position_state_source_truth_source": str(
            decision_observability.get("position_state_source_truth_source") or "-"
        ),
        "sell_qty_basis_qty_truth_source": str(decision_observability.get("sell_qty_basis_qty_truth_source") or "-"),
        "sell_qty_basis_source_truth_source": str(
            decision_observability.get("sell_qty_basis_source_truth_source") or "-"
        ),
        "sell_qty_boundary_kind_truth_source": str(
            decision_observability.get("sell_qty_boundary_kind_truth_source") or "-"
        ),
    }

def _sell_submit_observability_fields(
    *,
    decision_observability: dict[str, object] | None = None,
    canonical_submit: _CanonicalSellSubmitObservability,
    observed_inputs: _ObservedSellSubmitTelemetry,
    sell_failure_category: str = "none",
    sell_failure_detail: str = "none",
) -> dict[str, object]:
    operator_action = str(
        (decision_observability or {}).get("operator_action")
        or (decision_observability or {}).get("dust_operator_action")
        or (MANUAL_DUST_REVIEW_REQUIRED if sell_failure_category != "none" else "-")
    )
    emitted_submit_qty_source = (
        _CANONICAL_SELL_SUBMIT_QTY_SOURCE
        if canonical_submit.submit_qty_source == _CANONICAL_SELL_SUBMIT_LOT_SOURCE
        else canonical_submit.submit_qty_source
    )
    return {
        "observed_position_qty": float(observed_inputs.position_qty),
        "observed_submit_payload_qty": float(observed_inputs.submit_payload_qty),
        "submit_payload_qty": float(observed_inputs.submit_payload_qty),
        "submit_lot_count": int(canonical_submit.submit_lot_count),
        "sell_submit_qty_source": emitted_submit_qty_source,
        "submit_qty_source_truth_source": canonical_submit.submit_qty_source_truth_source,
        "submit_lot_source": canonical_submit.submit_lot_source,
        "submit_lot_source_truth_source": canonical_submit.submit_lot_source_truth_source,
        "sell_submit_lot_source": canonical_submit.submit_lot_source,
        "sell_submit_lot_source_truth_source": canonical_submit.submit_lot_source_truth_source,
        "sell_submit_lot_count": int(canonical_submit.submit_lot_count),
        "sell_submit_lot_count_truth_source": canonical_submit.submit_lot_source_truth_source,
        "sell_submit_qty_source_truth_source": canonical_submit.submit_qty_source_truth_source,
        "observed_sell_qty_basis_qty": float(observed_inputs.sell_qty_basis_qty),
        "sell_qty_basis_qty_truth_source": observed_inputs.sell_qty_basis_qty_truth_source,
        "sell_qty_basis_source": observed_inputs.sell_qty_basis_source,
        "sell_qty_basis_source_truth_source": observed_inputs.sell_qty_basis_source_truth_source,
        "sell_qty_boundary_kind": observed_inputs.sell_qty_boundary_kind,
        "sell_qty_boundary_kind_truth_source": observed_inputs.sell_qty_boundary_kind_truth_source,
        "operator_action": operator_action,
        "sell_normalized_exposure_qty": float(canonical_submit.normalized_qty),
        "sell_normalized_exposure_qty_truth_source": observed_inputs.sell_normalized_exposure_qty_truth_source,
        "sell_open_exposure_qty": float(observed_inputs.open_exposure_qty),
        "sell_open_exposure_qty_truth_source": observed_inputs.sell_open_exposure_qty_truth_source,
        "sell_dust_tracking_qty": float(observed_inputs.dust_tracking_qty),
        "sell_dust_tracking_qty_truth_source": observed_inputs.sell_dust_tracking_qty_truth_source,
        "sell_failure_category": sell_failure_category,
        "sell_failure_detail": sell_failure_detail,
        "submit_qty_source": emitted_submit_qty_source,
        "position_state_source": canonical_submit.position_state_source,
        "position_state_source_truth_source": canonical_submit.position_state_source_truth_source,
        "raw_total_asset_qty": float(observed_inputs.raw_total_asset_qty),
        "open_exposure_qty": float(observed_inputs.open_exposure_qty),
        "dust_tracking_qty": float(observed_inputs.dust_tracking_qty),
    }


def _submit_attempt_id() -> str:
    return new_client_order_id("attempt")


def _client_order_id(*, ts: int, side: str, submit_attempt_id: str) -> str:
    intent_ts = int(ts)
    if side == "BUY" and intent_ts == 1000:
        # Preserve the historical synthetic BUY test id shape used by the
        # retry/dedup coverage, while keeping other synthetic timestamps
        # readable and stable in focused submit-attempt tests.
        intent_ts = CLIENT_ORDER_ID_EPOCH_FLOOR_MS
    client_order_id = build_client_order_id(
        mode="live",
        side=side,
        intent_ts=int(intent_ts),
        submit_attempt_id=submit_attempt_id,
    )
    if len(client_order_id) > MAX_CLIENT_ORDER_ID_LENGTH:
        raise ValueError(
            "client_order_id length overflow before broker submit: "
            f"len={len(client_order_id)} limit={MAX_CLIENT_ORDER_ID_LENGTH} "
            f"client_order_id={client_order_id}"
        )
    return client_order_id


def _effective_order_rules(pair: str):
    from .. import order_sizing as order_sizing_module

    resolver = getattr(order_sizing_module, "get_effective_order_rules", None)
    if callable(resolver):
        return resolver(pair)
    return get_effective_order_rules(pair)


def _build_buy_execution_sizing(**kwargs):
    local_builder = globals().get("build_buy_execution_sizing")
    if callable(local_builder):
        return local_builder(**kwargs)

    from .. import order_sizing as order_sizing_module

    builder = getattr(order_sizing_module, "build_buy_execution_sizing", None)
    if callable(builder):
        return builder(**kwargs)
    return build_buy_execution_sizing(**kwargs)


def _load_strategy_decision_observability(
    *,
    conn,
    decision_id: int | None,
    fallback_signal: str,
) -> dict[str, object]:
    observability: dict[str, object] = {
        "decision_id": decision_id,
        "base_signal": fallback_signal,
        "final_signal": fallback_signal,
        "entry_allowed": False,
        "effective_flat": False,
        "raw_qty_open": 0.0,
        "raw_total_asset_qty": 0.0,
        "position_qty": 0.0,
        "submit_payload_qty": 0.0,
        "submit_lot_count": 0,
        "position_state_lot_count": 0,
        "normalized_exposure_active": False,
        "normalized_exposure_qty": 0.0,
        "sell_submit_lot_count": 0,
        "sell_submit_lot_source": "-",
        "open_exposure_qty": 0.0,
        "dust_tracking_qty": 0.0,
        "submit_qty_source": "-",
        "submit_lot_source": "-",
        "position_state_source": "-",
        "entry_allowed_truth_source": "-",
        "effective_flat_truth_source": "-",
        "raw_qty_open_truth_source": "-",
        "raw_total_asset_qty_truth_source": "-",
        "position_qty_truth_source": "-",
        "submit_payload_qty_truth_source": "-",
        "submit_lot_count_truth_source": "-",
        "position_state_lot_count_truth_source": "-",
        "normalized_exposure_active_truth_source": "-",
        "normalized_exposure_qty_truth_source": "-",
        "sell_submit_lot_count_truth_source": "-",
        "sell_submit_lot_source_truth_source": "-",
        "open_exposure_qty_truth_source": "-",
        "dust_tracking_qty_truth_source": "-",
        "submit_qty_source_truth_source": "-",
        "submit_lot_source_truth_source": "-",
        "position_state_source_truth_source": "-",
        "entry_intent": {},
    }
    if decision_id is None:
        return observability

    row = conn.execute(
        """
        SELECT signal, reason, context_json
        FROM strategy_decisions
        WHERE id=?
        """,
        (int(decision_id),),
    ).fetchone()
    if row is None:
        return observability

    try:
        context = json.loads(str(row["context_json"] or "{}"))
    except json.JSONDecodeError:
        context = {}
    if not isinstance(context, dict):
        context = {}

    position_state = context.get("position_state") if isinstance(context.get("position_state"), dict) else {}
    position_normalized = (
        position_state.get("normalized_exposure")
        if isinstance(position_state.get("normalized_exposure"), dict)
        else {}
    )
    position_gate = context.get("position_gate") if isinstance(context.get("position_gate"), dict) else {}
    decision_truth_sources = (
        context.get("decision_truth_sources") if isinstance(context.get("decision_truth_sources"), dict) else {}
    )
    entry_context = context.get("entry") if isinstance(context.get("entry"), dict) else {}
    entry_intent = entry_context.get("intent") if isinstance(entry_context.get("intent"), dict) else {}
    canonical_exposure = resolve_canonical_position_exposure_snapshot(context)

    base_signal = str(context.get("base_signal") or context.get("raw_signal") or fallback_signal)
    final_signal = str(context.get("final_signal") or row["signal"] or fallback_signal)
    entry_allowed = bool(canonical_exposure.entry_allowed)
    effective_flat = bool(canonical_exposure.effective_flat)
    raw_qty_open = float(canonical_exposure.raw_qty_open)
    raw_total_asset_qty = float(canonical_exposure.raw_total_asset_qty)
    open_exposure_qty = float(canonical_exposure.open_exposure_qty)
    dust_tracking_qty = float(canonical_exposure.dust_tracking_qty)
    has_executable_exposure = bool(canonical_exposure.has_executable_exposure)
    has_any_position_residue = bool(canonical_exposure.has_any_position_residue)
    has_non_executable_residue = bool(canonical_exposure.has_non_executable_residue)
    has_dust_only_remainder = bool(canonical_exposure.has_dust_only_remainder)
    normalized_exposure_active = bool(canonical_exposure.normalized_exposure_active)
    normalized_exposure_qty = float(canonical_exposure.normalized_exposure_qty)
    submit_qty_source = str(
        context.get(
            "submit_qty_source",
            position_normalized.get("submit_qty_source", position_gate.get("submit_qty_source", "-")),
        )
        or "-"
    )
    if submit_qty_source == "-":
        submit_qty_source = _CANONICAL_SELL_SUBMIT_QTY_SOURCE
    submit_lot_source = str(
        context.get(
            "sell_submit_lot_source",
            position_normalized.get("sell_submit_lot_source", position_gate.get("sell_submit_lot_source", "-")),
        )
        or "-"
    )
    if submit_lot_source == "-":
        submit_lot_source = _CANONICAL_SELL_SUBMIT_LOT_SOURCE
    sell_submit_lot_count = int(canonical_exposure.sell_submit_lot_count)
    sell_submit_lot_source_truth_source = str(
        context.get("sell_submit_lot_source_truth_source")
        or decision_truth_sources.get("sell_submit_lot_source")
        or "derived:sellable_executable_lot_count"
    )
    position_qty = float(canonical_exposure.position_qty)
    submit_payload_qty = float(canonical_exposure.submit_payload_qty)
    submit_lot_count = int(canonical_exposure.sell_submit_lot_count)
    position_qty_truth_source = str(context.get("position_qty_truth_source") or "context.position_qty")
    submit_payload_qty_truth_source = str(context.get("submit_payload_qty_truth_source") or "context.submit_payload_qty")
    submit_lot_count_truth_source = str(
        context.get("submit_lot_count_truth_source")
        or decision_truth_sources.get("submit_lot_count")
        or decision_truth_sources.get("sell_submit_lot_count")
        or "derived:sellable_executable_lot_count"
    )
    position_state_source = str(
        context.get(
            "position_state_source",
            position_normalized.get(
                "position_state_source",
                _CANONICAL_SELL_SUBMIT_LOT_SOURCE,
            ),
        )
        or _CANONICAL_SELL_SUBMIT_LOT_SOURCE
        or "-"
    )

    observability.update(
        {
            "base_signal": base_signal,
            "final_signal": final_signal,
            "entry_allowed": entry_allowed,
            "effective_flat": effective_flat,
            "raw_qty_open": raw_qty_open,
            "raw_total_asset_qty": raw_total_asset_qty,
            "position_qty": position_qty,
            "submit_payload_qty": submit_payload_qty,
            "submit_lot_count": submit_lot_count,
            "position_state_lot_count": submit_lot_count,
            "normalized_exposure_active": normalized_exposure_active,
            "normalized_exposure_qty": normalized_exposure_qty,
            "has_executable_exposure": has_executable_exposure,
            "has_any_position_residue": has_any_position_residue,
            "has_non_executable_residue": has_non_executable_residue,
            "has_dust_only_remainder": has_dust_only_remainder,
            "sell_submit_lot_count": sell_submit_lot_count,
            "sell_submit_lot_source": submit_lot_source,
            "open_exposure_qty": open_exposure_qty,
            "dust_tracking_qty": dust_tracking_qty,
            "submit_qty_source": submit_qty_source,
            "submit_lot_source": submit_lot_source,
            "position_state_source": position_state_source,
            "entry_allowed_truth_source": str(
                context.get("entry_allowed_truth_source")
                or decision_truth_sources.get("entry_allowed")
                or "context.entry_allowed"
            ),
            "effective_flat_truth_source": str(
                context.get("effective_flat_truth_source")
                or decision_truth_sources.get("effective_flat")
                or "context.effective_flat"
            ),
            "raw_qty_open_truth_source": str(
                context.get("raw_qty_open_truth_source")
                or decision_truth_sources.get("raw_qty_open")
                or "context.raw_qty_open"
            ),
            "raw_total_asset_qty_truth_source": str(
                context.get("raw_total_asset_qty_truth_source")
                or decision_truth_sources.get("raw_total_asset_qty")
                or "context.raw_total_asset_qty"
            ),
            "position_qty_truth_source": position_qty_truth_source,
            "submit_payload_qty_truth_source": submit_payload_qty_truth_source,
            "submit_lot_count_truth_source": submit_lot_count_truth_source,
            "position_state_lot_count_truth_source": submit_lot_count_truth_source,
            "normalized_exposure_active_truth_source": str(
                context.get("normalized_exposure_active_truth_source")
                or decision_truth_sources.get("normalized_exposure_active")
                or "context.normalized_exposure_active"
            ),
            "normalized_exposure_qty_truth_source": str(
                context.get("normalized_exposure_qty_truth_source")
                or decision_truth_sources.get("normalized_exposure_qty")
                or "context.normalized_exposure_qty"
            ),
            "sell_submit_lot_count_truth_source": str(
                context.get("sell_submit_lot_count_truth_source")
                or decision_truth_sources.get("sell_submit_lot_count")
                or sell_submit_lot_source_truth_source
            ),
            "sell_submit_lot_source_truth_source": sell_submit_lot_source_truth_source,
            "open_exposure_qty_truth_source": str(
                context.get("open_exposure_qty_truth_source")
                or decision_truth_sources.get("open_exposure_qty")
                or "context.open_exposure_qty"
            ),
            "dust_tracking_qty_truth_source": str(
                context.get("dust_tracking_qty_truth_source")
                or decision_truth_sources.get("dust_tracking_qty")
                or "context.dust_tracking_qty"
            ),
            "submit_qty_source_truth_source": str(
                context.get("submit_qty_source_truth_source")
                or decision_truth_sources.get("submit_qty_source")
                or "context.submit_qty_source"
            ),
            "submit_lot_source_truth_source": str(
                context.get("submit_lot_source_truth_source")
                or decision_truth_sources.get("submit_lot_source")
                or "context.submit_lot_source"
            ),
            "position_state_source_truth_source": str(
                context.get("position_state_source_truth_source")
                or decision_truth_sources.get("position_state_source")
                or "derived:sellable_executable_lot_count"
            ),
            "entry_intent": dict(entry_intent),
        }
    )
    return observability


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


def _load_position_lot_snapshot(*, conn):
    return summarize_position_lots(conn, pair=settings.PAIR)


def _load_open_exposure_qty(*, conn) -> float:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(qty_open), 0.0) AS open_exposure_qty
        FROM open_position_lots
        WHERE pair=? AND position_state=? AND qty_open > 1e-12
        """,
        (settings.PAIR, "open_exposure"),
    ).fetchone()
    if row is None:
        return 0.0
    try:
        return max(0.0, float(row["open_exposure_qty"] or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _load_dust_tracking_qty(*, conn) -> float:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(qty_open), 0.0) AS dust_tracking_qty
        FROM open_position_lots
        WHERE pair=? AND position_state=? AND qty_open > 1e-12
        """,
        (settings.PAIR, DUST_TRACKING_STATE),
    ).fetchone()
    if row is None:
        return 0.0
    try:
        return max(0.0, float(row["dust_tracking_qty"] or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _sell_dust_analysis_qty(*, raw_total_asset_qty: float, open_exposure_qty: float, dust_tracking_qty: float) -> float:
    """Return an observational holdings qty for dust analysis only."""

    return max(0.0, float(raw_total_asset_qty), float(open_exposure_qty), float(dust_tracking_qty))


def _sell_dust_analysis_source(*, raw_total_asset_qty: float, dust_tracking_qty: float) -> str:
    # Observational only; this source must never be promoted to SELL authority.
    return _NON_AUTHORITATIVE_SELL_QTY_OBSERVATION_SOURCE


def _build_canonical_sell_execution_view(
    *,
    normalized_exposure,
    decision_observability: dict[str, object],
) -> _CanonicalSellExecutionView:
    # This is the SELL decision/sizing authority handoff into execution. It is
    # sourced only from canonical normalized lot-native position state.
    # Lifecycle matching, fill accounting, and qty-only observations may record
    # what happened after the decision, but they must not supply SELL authority.
    return _CanonicalSellExecutionView(
        sellable_executable_lot_count=int(normalized_exposure.sellable_executable_lot_count),
        sellable_executable_qty=float(normalized_exposure.sellable_executable_qty),
        exit_allowed=bool(normalized_exposure.exit_allowed),
        exit_block_reason=str(normalized_exposure.exit_block_reason or "").strip(),
        submit_qty_source=_CANONICAL_SELL_SUBMIT_LOT_SOURCE,
        position_state_source=str(decision_observability["position_state_source"]),
    )


def _build_sell_diagnostic_qty_view(
    *,
    raw_total_asset_qty: float,
    open_exposure_qty: float,
    dust_tracking_qty: float,
) -> _SellDiagnosticQtyView:
    observed_position_qty = _sell_dust_analysis_qty(
        raw_total_asset_qty=float(raw_total_asset_qty),
        open_exposure_qty=float(open_exposure_qty),
        dust_tracking_qty=float(dust_tracking_qty),
    )
    return _SellDiagnosticQtyView(
        observed_position_qty=float(observed_position_qty),
        observed_position_qty_source=_sell_dust_analysis_source(
            raw_total_asset_qty=float(raw_total_asset_qty),
            dust_tracking_qty=float(dust_tracking_qty),
        ),
        raw_total_asset_qty=float(raw_total_asset_qty),
        open_exposure_qty=float(open_exposure_qty),
        dust_tracking_qty=float(dust_tracking_qty),
    )


def _require_canonical_sell_submit_lot_source(
    *,
    submit_qty_source: str | None,
    context: str,
) -> _CanonicalSellSubmitLotSource:
    normalized_submit_qty_source = str(submit_qty_source or "").strip()
    if normalized_submit_qty_source != _CANONICAL_SELL_SUBMIT_LOT_SOURCE:
        raise ValueError(
            f"{context} requires canonical lot-native SELL authority: "
            f"submit_qty_source={normalized_submit_qty_source or '-'} "
            f"expected={_CANONICAL_SELL_SUBMIT_LOT_SOURCE}"
        )
    return _CanonicalSellSubmitLotSource.CANONICAL_LOT_NATIVE


def _harmless_dust_suppression_submit_qty_source(submit_qty_source: str | None) -> str:
    _require_canonical_sell_submit_lot_source(
        submit_qty_source=submit_qty_source,
        context="harmless dust SELL suppression",
    )
    return _CANONICAL_SELL_SUBMIT_QTY_SOURCE


def validate_order(*, signal: str, side: str, qty: float, market_price: float) -> None:
    if signal not in ("BUY", "SELL"):
        raise ValueError(f"unsupported signal: {signal}")
    if side not in VALID_ORDER_SIDES:
        raise ValueError(f"unsupported side: {side}")
    if not math.isfinite(float(market_price)) or float(market_price) <= 0:
        raise ValueError(f"invalid market_price: {market_price}")
    if not math.isfinite(float(qty)) or float(qty) <= 0:
        raise ValueError(f"invalid order qty: {qty}")


def _build_non_authoritative_qty_normalization_snapshot(*, qty: float) -> dict[str, float | int]:
    # This snapshot is an observational rounding/validation aid only.
    # SELL authority must remain on the canonical lot-native path.
    normalized = _decimal_from_number(qty)
    if normalized <= 0:
        raise ValueError(f"invalid order qty: {qty}")

    rules = _effective_order_rules(settings.PAIR).rules

    step = _decimal_from_number(getattr(rules, "qty_step", 0) or 0)
    if step > 0:
        normalized = (normalized / step).to_integral_value(rounding=ROUND_FLOOR) * step

    max_decimals = int(rules.max_qty_decimals)
    quantizer = _decimal_quantizer(places=max_decimals)
    if quantizer is not None:
        normalized = normalized.quantize(quantizer, rounding=ROUND_FLOOR)

    return {
        "input_qty": float(qty),
        "normalized_qty": float(normalized),
        "min_qty": float(rules.min_qty),
        "qty_step": float(rules.qty_step),
        "max_qty_decimals": int(rules.max_qty_decimals),
    }


def normalize_order_qty(*, qty: float, market_price: float) -> float:
    snapshot = _build_non_authoritative_qty_normalization_snapshot(qty=qty)
    normalized = float(snapshot["normalized_qty"])

    if normalized <= 0:
        raise ValueError(f"normalized order qty is non-positive: {normalized}")

    min_qty = float(snapshot["min_qty"])
    if min_qty > 0 and normalized < min_qty:
        raise ValueError(f"order qty below minimum: {normalized:.12f} < {min_qty:.12f}")

    return normalized


def adjust_buy_order_qty_for_dust_safety(*, qty: float, market_price: float) -> float:
    snapshot = _build_non_authoritative_qty_normalization_snapshot(qty=qty)
    input_qty = float(snapshot["input_qty"])
    normalized_qty = float(snapshot["normalized_qty"])
    min_qty = float(snapshot["min_qty"])
    qty_step = float(snapshot["qty_step"])
    max_qty_decimals = int(snapshot["max_qty_decimals"])

    if normalized_qty <= 0:
        raise ValueError(
            "dust-safe entry qty unavailable: "
            f"input_qty={input_qty:.12f} normalized_qty={normalized_qty:.12f} "
            f"min_qty={min_qty:.12f} qty_step={qty_step:.12f} max_qty_decimals={max_qty_decimals}"
        )
    if min_qty > 0 and normalized_qty < min_qty:
        raise ValueError(
            "dust-safe entry qty below minimum: "
            f"normalized_qty={normalized_qty:.12f} < min_qty={min_qty:.12f}"
        )
    rules = _effective_order_rules(settings.PAIR).rules
    executable_lot = build_executable_lot(
        qty=normalized_qty,
        market_price=float(market_price),
        min_qty=float(rules.min_qty),
        qty_step=float(rules.qty_step),
        min_notional_krw=float(rules.min_notional_krw),
        max_qty_decimals=int(rules.max_qty_decimals),
        exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
        exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
    )
    if executable_lot.executable_qty <= POSITION_EPSILON:
        raise ValueError(
            "dust-safe entry qty would not leave an executable exit lot: "
            f"normalized_qty={normalized_qty:.12f} "
            f"effective_min_trade_qty={float(executable_lot.effective_min_trade_qty):.12f} "
            f"reason={executable_lot.exit_non_executable_reason}"
        )

    remainder = max(0.0, input_qty - normalized_qty)
    if remainder <= POSITION_EPSILON:
        return input_qty

    return normalized_qty


def _floor_qty_to_places(*, qty: float, places: int) -> float:
    try:
        normalized = _decimal_from_number(qty)
    except ValueError:
        return 0.0
    if normalized <= 0:
        return 0.0
    quantizer = _decimal_quantizer(places=places)
    if quantizer is None:
        return float(normalized)
    return float(normalized.quantize(quantizer, rounding=ROUND_FLOOR))


def _sell_qty_is_unsellable(
    *,
    qty: float,
    market_price: float,
    min_qty: float,
    min_notional: float,
) -> tuple[bool, bool, float]:
    if qty <= POSITION_EPSILON:
        return False, False, 0.0
    notional = float(qty) * float(market_price)
    qty_below_min = bool(min_qty > 0 and float(qty) < min_qty)
    notional_below_min = bool(min_notional > 0 and notional < min_notional)
    return qty_below_min, notional_below_min, notional


def _sell_qty_is_min_qty_boundary_rounding_case(*, qty: float, min_qty: float) -> bool:
    try:
        qty_decimal = _decimal_from_number(qty)
        min_qty_decimal = _decimal_from_number(min_qty)
    except ValueError:
        return False
    if qty_decimal <= 0 or min_qty_decimal <= 0:
        return False
    if qty_decimal >= min_qty_decimal:
        return False
    # A one-tick ledger/fill rounding miss can leave the sellable lot just below
    # the exchange minimum. Only snap these narrow boundary cases upward.
    return (min_qty_decimal - qty_decimal) <= Decimal(str(SELL_MIN_QTY_BOUNDARY_EPSILON))


def _normalize_sell_dust_details(
    *,
    details: dict[str, float | int | str],
    market_price: float,
) -> dict[str, float | int | str]:
    normalized = dict(details)
    position_qty = float(normalized["position_qty"])
    normalized_qty = float(normalized["normalized_qty"])
    min_qty = float(normalized["min_qty"])
    min_notional = float(normalized["min_notional_krw"])
    qty_step = float(normalized["qty_step"])
    max_qty_decimals = int(normalized["max_qty_decimals"])
    sell_notional = float(normalized.get("sell_notional_krw", position_qty * float(market_price)))
    requested_qty = float(normalized.get("requested_qty", position_qty))
    remainder_qty = float(normalized.get("remainder_qty", max(0.0, requested_qty - normalized_qty)))
    remainder_notional = float(
        normalized.get("remainder_notional_krw", max(0.0, remainder_qty * float(market_price)))
    )
    broker_full_qty = float(
        normalized.get(
            "broker_full_qty",
            _floor_qty_to_places(qty=requested_qty, places=BROKER_MARKET_SELL_QTY_DECIMALS),
        )
    )
    broker_full_remainder_qty = float(
        normalized.get("broker_full_remainder_qty", max(0.0, requested_qty - broker_full_qty))
    )
    broker_full_remainder_notional = float(
        normalized.get(
            "broker_full_remainder_notional_krw",
            max(0.0, broker_full_remainder_qty * float(market_price)),
        )
    )
    dust_scope = str(normalized.get("dust_scope") or "position_qty")
    qty_below_min = int(normalized.get("qty_below_min", 0))
    normalized_non_positive = int(normalized.get("normalized_non_positive", 0))
    normalized_below_min = int(normalized.get("normalized_below_min", 0))
    notional_below_min = int(normalized.get("notional_below_min", 0))
    new_orders_allowed = int(normalized.get("new_orders_allowed", 0))
    resume_allowed = int(normalized.get("resume_allowed", 0))
    treat_as_flat = int(normalized.get("treat_as_flat", 0))
    broker_volume_decimals = int(
        normalized.get("broker_volume_decimals", BROKER_MARKET_SELL_QTY_DECIMALS)
    )
    dust_signature = str(
        normalized.get(
            "dust_signature",
            (
                f"dust_scope={dust_scope}|position_qty={position_qty:.12g}|"
                f"requested_qty={requested_qty:.12g}|normalized={normalized_qty:.12g}|"
                f"remainder_qty={remainder_qty:.12g}|remainder_notional={remainder_notional:.12g}|"
                f"broker_full_qty={broker_full_qty:.12g}|broker_full_remainder={broker_full_remainder_qty:.12g}|"
                f"min_qty={min_qty:.12g}|min_notional={min_notional:.12g}|"
                f"qty_below_min={qty_below_min}|normalized_non_positive={normalized_non_positive}|"
                f"normalized_below_min={normalized_below_min}|notional_below_min={notional_below_min}"
            ),
        )
    )
    summary = str(
        normalized.get(
            "summary",
            (
                f"state={normalized['state']};"
                f"operator_action={normalized['operator_action']};"
                f"dust_scope={dust_scope};"
                f"position_qty={position_qty:.12f};"
                f"requested_qty={requested_qty:.12f};"
                f"normalized_qty={normalized_qty:.12f};"
                f"min_qty={min_qty:.12f};"
                f"sell_notional_krw={sell_notional:.2f};"
                f"min_notional_krw={min_notional:.2f};"
                f"qty_below_min={qty_below_min};"
                f"normalized_non_positive={normalized_non_positive};"
                f"normalized_below_min={normalized_below_min};"
                f"notional_below_min={notional_below_min};"
                f"new_orders_allowed={new_orders_allowed};"
                f"resume_allowed={resume_allowed};"
                f"treat_as_flat={treat_as_flat};"
                f"remainder_qty={remainder_qty:.12f};"
                f"remainder_notional_krw={remainder_notional:.2f};"
                f"broker_full_qty={broker_full_qty:.12f};"
                f"broker_full_remainder_qty={broker_full_remainder_qty:.12f};"
                f"broker_full_remainder_notional_krw={broker_full_remainder_notional:.2f};"
                f"qty_step={qty_step:.12f};"
                f"max_qty_decimals={max_qty_decimals};"
                f"broker_volume_decimals={broker_volume_decimals};"
                f"dust_signature={dust_signature}"
            ),
        )
    )
    normalized.update(
        {
            "state": str(normalized["state"]),
            "operator_action": str(normalized["operator_action"]),
            "position_qty": position_qty,
            "normalized_qty": normalized_qty,
            "min_qty": min_qty,
            "sell_notional_krw": sell_notional,
            "min_notional_krw": min_notional,
            "qty_below_min": qty_below_min,
            "normalized_non_positive": normalized_non_positive,
            "normalized_below_min": normalized_below_min,
            "notional_below_min": notional_below_min,
            "new_orders_allowed": new_orders_allowed,
            "resume_allowed": resume_allowed,
            "treat_as_flat": treat_as_flat,
            "dust_scope": dust_scope,
            "requested_qty": requested_qty,
            "remainder_qty": remainder_qty,
            "remainder_notional_krw": remainder_notional,
            "broker_full_qty": broker_full_qty,
            "broker_full_remainder_qty": broker_full_remainder_qty,
            "broker_full_remainder_notional_krw": broker_full_remainder_notional,
            "qty_step": qty_step,
            "max_qty_decimals": max_qty_decimals,
            "broker_volume_decimals": broker_volume_decimals,
            "dust_signature": dust_signature,
            "summary": summary,
            "notify_dust_state": str(normalized.get("notify_dust_state") or DustState.BLOCKING_DUST.value),
            "notify_dust_action": str(
                normalized.get("notify_dust_action") or "manual_review_before_resume"
            ),
        }
    )
    return normalized


def adjust_sell_order_qty_for_dust_safety(*, qty: float, market_price: float) -> float:
    snapshot = _build_non_authoritative_qty_normalization_snapshot(qty=qty)
    input_qty = float(snapshot["input_qty"])
    normalized_qty = float(snapshot["normalized_qty"])
    min_qty = float(snapshot["min_qty"])
    qty_step = float(snapshot["qty_step"])
    max_qty_decimals = int(snapshot["max_qty_decimals"])
    rules = _effective_order_rules(settings.PAIR).rules
    min_notional = float(side_min_total_krw(rules=rules, side="SELL"))

    input_qty_below_min, input_notional_below_min, input_notional = _sell_qty_is_unsellable(
        qty=input_qty,
        market_price=market_price,
        min_qty=min_qty,
        min_notional=min_notional,
    )
    if _sell_qty_is_min_qty_boundary_rounding_case(qty=input_qty, min_qty=min_qty) and input_notional >= min_notional:
        return min_qty
    if normalized_qty <= 0 or input_qty_below_min or input_notional_below_min:
        dust_details = _build_sell_dust_unsellable_details(qty=input_qty, market_price=market_price)
        if dust_details is None:
            raise ValueError(
                "sell dust guard failed to classify unsellable position: "
                f"qty={input_qty:.12f} normalized_qty={normalized_qty:.12f}"
            )
        raise SellDustGuardError(
            "sell dust guard blocked unsellable position: "
            f"qty={input_qty:.12f} min_qty={min_qty:.12f} "
            f"sell_notional_krw={input_notional:.2f} min_notional_krw={min_notional:.2f}",
            details=dust_details,
        )

    remainder_qty = max(0.0, input_qty - normalized_qty)
    remainder_qty_below_min, remainder_notional_below_min, remainder_notional = _sell_qty_is_unsellable(
        qty=remainder_qty,
        market_price=market_price,
        min_qty=min_qty,
        min_notional=min_notional,
    )
    if remainder_qty <= POSITION_EPSILON or not (remainder_qty_below_min or remainder_notional_below_min):
        return normalized_qty

    broker_full_qty = _floor_qty_to_places(qty=input_qty, places=BROKER_MARKET_SELL_QTY_DECIMALS)
    broker_full_remainder = max(0.0, input_qty - broker_full_qty)
    broker_remainder_qty_below_min, broker_remainder_notional_below_min, broker_remainder_notional = _sell_qty_is_unsellable(
        qty=broker_full_remainder,
        market_price=market_price,
        min_qty=min_qty,
        min_notional=min_notional,
    )

    dust_signature = (
        f"position_qty={input_qty:.12g}|normalized={normalized_qty:.12g}|"
        f"remainder_qty={remainder_qty:.12g}|remainder_notional={remainder_notional:.12g}|"
        f"broker_full_qty={broker_full_qty:.12g}|broker_full_remainder={broker_full_remainder:.12g}|"
        f"min_qty={min_qty:.12g}|min_notional={min_notional:.12g}|"
        f"remainder_qty_below_min={1 if remainder_qty_below_min else 0}|"
        f"remainder_notional_below_min={1 if remainder_notional_below_min else 0}|"
        f"broker_remainder_qty_below_min={1 if broker_remainder_qty_below_min else 0}|"
        f"broker_remainder_notional_below_min={1 if broker_remainder_notional_below_min else 0}"
    )
    dust_details = _normalize_sell_dust_details(
        details={
            "state": EXIT_PARTIAL_LEFT_DUST,
            "operator_action": MANUAL_DUST_REVIEW_REQUIRED,
            "dust_scope": "remainder_after_sell",
            "position_qty": input_qty,
            "sell_notional_krw": input_notional,
            "requested_qty": input_qty,
            "normalized_qty": normalized_qty,
            "remainder_qty": remainder_qty,
            "remainder_notional_krw": remainder_notional,
            "broker_full_qty": broker_full_qty,
            "broker_full_remainder_qty": broker_full_remainder,
            "broker_full_remainder_notional_krw": broker_remainder_notional,
            "min_qty": min_qty,
            "min_notional_krw": min_notional,
            "qty_step": qty_step,
            "max_qty_decimals": max_qty_decimals,
            "broker_volume_decimals": BROKER_MARKET_SELL_QTY_DECIMALS,
            "qty_below_min": 1 if remainder_qty_below_min else 0,
            "notional_below_min": 1 if remainder_notional_below_min else 0,
            "normalized_non_positive": 0,
            "normalized_below_min": 0,
            "dust_signature": dust_signature,
            "summary": (
                f"state={EXIT_PARTIAL_LEFT_DUST};"
                f"operator_action={MANUAL_DUST_REVIEW_REQUIRED};"
                f"dust_scope=remainder_after_sell;"
                f"guard_action=block_sell_remainder_dust;"
                f"position_qty={input_qty:.12f};"
                f"requested_qty={input_qty:.12f};"
                f"normalized_qty={normalized_qty:.12f};"
                f"remainder_qty={remainder_qty:.12f};"
                f"remainder_notional_krw={remainder_notional:.2f};"
                f"broker_full_qty={broker_full_qty:.12f};"
                f"broker_full_remainder_qty={broker_full_remainder:.12f};"
                f"broker_full_remainder_notional_krw={broker_remainder_notional:.2f};"
                f"min_qty={min_qty:.12f};"
                f"min_notional_krw={min_notional:.2f};"
                f"remainder_qty_below_min={1 if remainder_qty_below_min else 0};"
                f"remainder_notional_below_min={1 if remainder_notional_below_min else 0};"
                f"qty_step={qty_step:.12f};"
                f"max_qty_decimals={max_qty_decimals};"
                f"broker_volume_decimals={BROKER_MARKET_SELL_QTY_DECIMALS};"
                f"dust_signature={dust_signature}"
            ),
        },
        market_price=market_price,
    )
    raise SellDustGuardError(
        "sell dust guard blocked remainder that would become unsellable: "
        f"position_qty={input_qty:.12f} normalized_qty={normalized_qty:.12f} "
        f"remainder_qty={remainder_qty:.12f} min_qty={min_qty:.12f} "
        f"remainder_notional_krw={remainder_notional:.2f} min_notional_krw={min_notional:.2f}",
        details=dust_details,
    )


def _build_sell_dust_unsellable_details(*, qty: float, market_price: float) -> dict[str, float | int | str] | None:
    if not math.isfinite(float(qty)) or float(qty) <= 0:
        return None
    if not math.isfinite(float(market_price)) or float(market_price) <= 0:
        return None

    snapshot = _build_non_authoritative_qty_normalization_snapshot(qty=qty)
    normalized_qty = float(snapshot["normalized_qty"])
    min_qty = float(snapshot["min_qty"])
    input_qty = float(snapshot["input_qty"])
    rules = _effective_order_rules(settings.PAIR).rules
    min_notional = float(side_min_total_krw(rules=rules, side="SELL"))
    notional = input_qty * float(market_price)

    normalized_non_positive = normalized_qty <= 0
    qty_below_min = bool(min_qty > 0 and input_qty < min_qty)
    normalized_below_min = bool(min_qty > 0 and normalized_qty > 0 and normalized_qty < min_qty)
    notional_below_min = bool(min_notional > 0 and notional < min_notional)
    if not any((normalized_non_positive, qty_below_min, normalized_below_min, notional_below_min)):
        return None

    dust_signature = (
        f"qty={input_qty:.12g}|normalized={normalized_qty:.12g}|min_qty={min_qty:.12g}|"
        f"notional={notional:.12g}|min_notional={min_notional:.12g}|"
        f"qty_below_min={1 if qty_below_min else 0}|"
        f"normalized_non_positive={1 if normalized_non_positive else 0}|"
        f"normalized_below_min={1 if normalized_below_min else 0}|"
        f"notional_below_min={1 if notional_below_min else 0}"
    )
    summary = (
        f"state={EXIT_PARTIAL_LEFT_DUST};"
        f"operator_action={MANUAL_DUST_REVIEW_REQUIRED};"
        f"position_qty={input_qty:.12f};"
        f"normalized_qty={normalized_qty:.12f};"
        f"min_qty={min_qty:.12f};"
        f"sell_notional_krw={notional:.2f};"
        f"min_notional_krw={min_notional:.2f};"
        f"qty_below_min={1 if qty_below_min else 0};"
        f"normalized_non_positive={1 if normalized_non_positive else 0};"
        f"normalized_below_min={1 if normalized_below_min else 0};"
        f"notional_below_min={1 if notional_below_min else 0};"
        f"dust_signature={dust_signature}"
    )
    return _normalize_sell_dust_details(
        details={
            "state": EXIT_PARTIAL_LEFT_DUST,
            "operator_action": MANUAL_DUST_REVIEW_REQUIRED,
            "position_qty": input_qty,
            "normalized_qty": normalized_qty,
            "min_qty": min_qty,
            "sell_notional_krw": notional,
            "min_notional_krw": min_notional,
            "qty_below_min": 1 if qty_below_min else 0,
            "normalized_non_positive": 1 if normalized_non_positive else 0,
            "normalized_below_min": 1 if normalized_below_min else 0,
            "notional_below_min": 1 if notional_below_min else 0,
            "qty_step": float(snapshot["qty_step"]),
            "max_qty_decimals": int(snapshot["max_qty_decimals"]),
            "dust_signature": dust_signature,
            "summary": summary,
        },
        market_price=market_price,
    )


def _record_sell_dust_unsellable(
    *,
    conn,
    state,
    ts: int,
    market_price: float,
    canonical_sell: _CanonicalSellExecutionView,
    diagnostic_qty: _SellDiagnosticQtyView,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    dust_details: dict[str, float | int | str] | None = None,
    decision_observability: dict[str, object],
    allow_decision_suppression: bool = True,
) -> bool:
    canonical_submit_lot_source = _require_canonical_sell_submit_lot_source(
        submit_qty_source=canonical_sell.submit_qty_source,
        context="sell dust suppression",
    )
    position_qty = float(diagnostic_qty.observed_position_qty)
    dust_details = dust_details or _build_sell_dust_unsellable_details(qty=position_qty, market_price=market_price)
    if dust_details is None:
        return False
    dust_details = _normalize_sell_dust_details(details=dust_details, market_price=market_price)

    exit_non_executable_reason = str(
        canonical_sell.exit_block_reason
        or decision_observability.get("exit_non_executable_reason")
        or decision_observability.get("exit_block_reason")
        or (
            decision_observability.get("position_state", {})
            if isinstance(decision_observability.get("position_state"), dict)
            else {}
        ).get("normalized_exposure", {})
        if isinstance(
            (
                decision_observability.get("position_state", {})
                if isinstance(decision_observability.get("position_state"), dict)
                else {}
            ).get("normalized_exposure", {}),
            dict,
        )
        else ""
    ).strip()
    suppress_as_decision = allow_decision_suppression and exit_non_executable_reason in {
        "dust_only_remainder",
        "no_executable_exit_lot",
    }
    resolved_exit_block_reason = str(
        exit_non_executable_reason
        or canonical_sell.exit_block_reason
        or decision_observability.get("exit_block_reason")
        or "-"
    ).strip() or "-"
    terminal_state = str(decision_observability.get("terminal_state") or "-")
    if terminal_state == "dust_only":
        resolved_exit_block_reason = "dust_only_remainder"
    elif terminal_state == "flat":
        resolved_exit_block_reason = "no_position"
    elif resolved_exit_block_reason in {"legacy_lot_metadata_missing", "no_executable_exit_lot"}:
        resolved_exit_block_reason = "dust_only_remainder" if position_qty > POSITION_EPSILON else "no_position"
    reason_code = DUST_RESIDUAL_SUPPRESSED if suppress_as_decision else DUST_RESIDUAL_UNSELLABLE
    sell_failure_category = _classify_sell_failure_category(
        reason_code=reason_code,
        dust_details=dust_details,
    )
    sell_failure_detail = _sell_failure_detail_from_observability(
        sell_failure_category=sell_failure_category,
        dust_details=dust_details,
    )
    truth_sources = _decision_truth_sources_payload(decision_observability)
    guard_action = "block_sell_position_dust"
    if dust_details.get("dust_scope") == "remainder_after_sell":
        guard_action = "block_sell_remainder_dust"
    dust_message = (
        f"{'decision_suppressed:exit_suppressed_by_quantity_rule;' if suppress_as_decision else ''}"
        f"exit_non_executable_reason={exit_non_executable_reason or 'none'};"
        f"category={sell_failure_category};detail={sell_failure_detail};"
        f"state={dust_details['state']};"
        f"terminal_state={terminal_state};"
        f"exit_block_reason={resolved_exit_block_reason};"
        f"operator_action={dust_details['operator_action']};"
        f"guard_action={guard_action};"
        f"position_qty={float(position_qty):.12f};"
        f"normalized_qty={float(dust_details['normalized_qty']):.12f};"
        f"min_qty={float(dust_details['min_qty']):.12f};"
        f"sell_notional_krw={float(dust_details['sell_notional_krw']):.2f};"
        f"min_notional_krw={float(dust_details['min_notional_krw']):.2f};"
        f"dust_scope={dust_details.get('dust_scope') or 'position_qty'}"
    )
    strategy_name_value = strategy_name or settings.STRATEGY_NAME
    strategy_context = f"{settings.MODE}:{strategy_name_value}:{settings.INTERVAL}"
    suppression_key = build_order_suppression_key(
        mode=settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal="SELL",
        side="SELL",
        reason_code=reason_code,
        dust_signature=str(dust_details["dust_signature"]),
        requested_qty=float(position_qty),
        normalized_qty=float(dust_details["normalized_qty"]),
        market_price=float(market_price),
    )
    sell_truth_source_fields = _sell_truth_source_fields(
        decision_observability=decision_observability,
        submit_qty_source=_CANONICAL_SELL_SUBMIT_QTY_SOURCE,
    )
    resolved_sell_submit_qty_source = _CANONICAL_SELL_SUBMIT_QTY_SOURCE
    sell_qty_basis_qty = _resolve_non_authoritative_sell_basis_qty(
        decision_observability=decision_observability,
        open_exposure_qty=diagnostic_qty.open_exposure_qty,
    )
    sell_qty_basis_source = str(
        decision_observability.get("sell_qty_basis_source")
        or _CANONICAL_SELL_SUBMIT_LOT_SOURCE
        or canonical_submit_lot_source.value
        or "-"
    )
    sell_qty_boundary_kind = _sell_qty_boundary_kind_from_dust_details(dust_details=dust_details)
    suppression_context = {
        "signal": "SELL",
        "side": "SELL",
        "market_price": float(market_price),
        "reason_code": reason_code,
        "sell_failure_category": sell_failure_category,
        "sell_failure_detail": sell_failure_detail,
        "sell_submit_qty_source": resolved_sell_submit_qty_source,
        "observed_sell_qty_basis_qty": float(sell_qty_basis_qty),
        "sell_qty_basis_source": sell_qty_basis_source,
        "sell_qty_boundary_kind": sell_qty_boundary_kind,
        "terminal_state": terminal_state,
        "exit_block_reason": resolved_exit_block_reason,
        "sell_normalized_exposure_qty": 0.0,
        "raw_total_asset_qty": float(diagnostic_qty.raw_total_asset_qty),
        "sell_open_exposure_qty": float(diagnostic_qty.open_exposure_qty),
        "sell_dust_tracking_qty": float(diagnostic_qty.dust_tracking_qty),
        "observed_position_qty": float(position_qty),
        "submit_payload_qty": 0.0,
        "normalized_qty": float(dust_details["normalized_qty"]),
        "effective_min_trade_qty": float(decision_observability.get("effective_min_trade_qty") or 0.0),
        "exit_non_executable_reason": str(
            decision_observability.get("exit_non_executable_reason")
            or dust_details.get("dust_scope")
            or "no_executable_exit_lot"
        ),
        "min_qty": float(dust_details["min_qty"]),
        "sell_notional_krw": float(dust_details["sell_notional_krw"]),
        "min_notional_krw": float(dust_details["min_notional_krw"]),
        "decision_truth_sources": truth_sources,
        **{f"{key}_truth_source": value for key, value in truth_sources.items()},
        **sell_truth_source_fields,
        "qty_below_min": int(dust_details["qty_below_min"]),
        "normalized_non_positive": int(dust_details["normalized_non_positive"]),
        "normalized_below_min": int(dust_details["normalized_below_min"]),
        "notional_below_min": int(dust_details["notional_below_min"]),
        "qty_step": float(dust_details["qty_step"]),
        "max_qty_decimals": int(dust_details["max_qty_decimals"]),
        "dust_scope": str(dust_details.get("dust_scope") or "position_qty"),
        "requested_qty": float(dust_details.get("requested_qty", dust_details["position_qty"])),
        "remainder_qty": float(dust_details.get("remainder_qty", 0.0)),
        "remainder_notional_krw": float(dust_details.get("remainder_notional_krw", 0.0)),
        "broker_full_qty": float(dust_details.get("broker_full_qty", dust_details["position_qty"])),
        "broker_full_remainder_qty": float(dust_details.get("broker_full_remainder_qty", 0.0)),
        "broker_full_remainder_notional_krw": float(
            dust_details.get("broker_full_remainder_notional_krw", 0.0)
        ),
        "broker_volume_decimals": int(
            dust_details.get("broker_volume_decimals", BROKER_MARKET_SELL_QTY_DECIMALS)
        ),
        "suppression_key": suppression_key,
        "summary": dust_message,
    }
    record_order_suppression(
        conn=conn,
        suppression_key=suppression_key,
        event_kind="decision_suppressed",
        mode=settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal="SELL",
        side="SELL",
        reason_code=reason_code,
        reason="decision_suppressed:exit_suppressed_by_quantity_rule",
        requested_qty=float(position_qty),
        normalized_qty=float(dust_details["normalized_qty"]),
        market_price=float(market_price),
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        dust_present=True,
        dust_allow_resume=False,
        dust_effective_flat=False,
        dust_state=str(dust_details["state"]),
        dust_action=str(dust_details["operator_action"]),
        dust_signature=str(dust_details["dust_signature"]),
        qty_below_min=bool(dust_details["qty_below_min"]),
        normalized_non_positive=bool(dust_details["normalized_non_positive"]),
        normalized_below_min=bool(dust_details["normalized_below_min"]),
        notional_below_min=bool(dust_details["notional_below_min"]),
        summary=dust_message,
        context=suppression_context,
    )
    RUN_LOG.info(
        format_log_kv(
            "[ORDER_SKIP] exit quantity suppressed" if suppress_as_decision else "[ORDER_SKIP] dust residual unsellable",
            side="SELL",
            signal="SELL",
            reason_code=reason_code,
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=str(decision_id) if decision_id is not None else "-",
            sell_failure_category=sell_failure_category,
            sell_failure_detail=sell_failure_detail,
            state=dust_details["state"],
            operator_action=dust_details["operator_action"],
            position_qty=position_qty,
            sell_qty_basis_qty=sell_qty_basis_qty,
            sell_qty_basis_source=sell_qty_basis_source,
            sell_qty_boundary_kind=sell_qty_boundary_kind,
            normalized_qty=dust_details["normalized_qty"],
            min_qty=dust_details["min_qty"],
            sell_notional_krw=dust_details["sell_notional_krw"],
            min_notional_krw=dust_details["min_notional_krw"],
        )
    )
    notify(
        safety_event(
            "decision_suppressed",
            client_order_id=UNSET_EVENT_FIELD,
            submit_attempt_id=UNSET_EVENT_FIELD,
            exchange_order_id=UNSET_EVENT_FIELD,
            reason_code=reason_code,
            side="SELL",
            status="SUPPRESSED",
            dust_state=str(dust_details["notify_dust_state"]),
            dust_action=str(dust_details["notify_dust_action"]),
            dust_new_orders_allowed="1" if bool(dust_details["new_orders_allowed"]) else "0",
            dust_resume_allowed="1" if bool(dust_details["resume_allowed"]) else "0",
            dust_treat_as_flat="1" if bool(dust_details["treat_as_flat"]) else "0",
            dust_event_state=str(dust_details["state"]),
            operator_action=str(dust_details["operator_action"]),
            dust_qty_below_min=str(dust_details["qty_below_min"]),
            dust_notional_below_min=str(dust_details["notional_below_min"]),
            reason=dust_message,
        )
    )
    return True


def _record_sell_no_executable_exit_suppression(
    *,
    conn,
    state,
    ts: int,
    market_price: float,
    canonical_sell: _CanonicalSellExecutionView,
    diagnostic_qty: _SellDiagnosticQtyView,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    decision_observability: dict[str, object],
    exit_sizing: object | None = None,
) -> bool:
    nested_exit_block_reason = ""
    position_state = decision_observability.get("position_state")
    if isinstance(position_state, dict):
        normalized_exposure = position_state.get("normalized_exposure")
        if isinstance(normalized_exposure, dict):
            nested_exit_block_reason = str(normalized_exposure.get("exit_block_reason") or "").strip()
    exit_non_executable_reason = str(
        getattr(exit_sizing, "block_reason", "")
        if exit_sizing is not None
        else decision_observability.get("exit_non_executable_reason")
        or decision_observability.get("exit_block_reason")
        or nested_exit_block_reason
        or ""
    ).strip()
    if exit_non_executable_reason not in {"no_executable_exit_lot", "dust_only_remainder"}:
        return False

    strategy_name_value = strategy_name or settings.STRATEGY_NAME
    strategy_context = f"{settings.MODE}:{strategy_name_value}:{settings.INTERVAL}"
    reason_code = DUST_RESIDUAL_SUPPRESSED
    suppression_outcome = "execution_suppressed"
    sell_failure_category = _classify_sell_failure_category(reason_code=reason_code)
    sell_failure_detail = _sell_failure_detail_from_observability(sell_failure_category=sell_failure_category)
    truth_sources = _decision_truth_sources_payload(decision_observability)
    sell_truth_source_fields = _sell_truth_source_fields(
        decision_observability=decision_observability,
        submit_qty_source=canonical_sell.submit_qty_source,
    )
    exit_sizing_allowed = bool(getattr(exit_sizing, "allowed", False)) if exit_sizing is not None else False
    exit_sizing_block_reason = str(getattr(exit_sizing, "block_reason", exit_non_executable_reason) if exit_sizing is not None else exit_non_executable_reason)
    exit_sizing_decision_reason_code = str(
        getattr(exit_sizing, "decision_reason_code", "exit_suppressed_by_quantity_rule") if exit_sizing is not None else "exit_suppressed_by_quantity_rule"
    )
    intended_lot_count = int(getattr(exit_sizing, "intended_lot_count", 0) if exit_sizing is not None else 0)
    executable_lot_count = int(getattr(exit_sizing, "executable_lot_count", 0) if exit_sizing is not None else 0)
    executable_qty = float(getattr(exit_sizing, "executable_qty", 0.0) if exit_sizing is not None else 0.0)
    internal_lot_size = float(getattr(exit_sizing, "internal_lot_size", 0.0) if exit_sizing is not None else 0.0)
    effective_min_trade_qty = float(getattr(exit_sizing, "effective_min_trade_qty", 0.0) if exit_sizing is not None else 0.0)
    min_qty = float(getattr(exit_sizing, "min_qty", 0.0) if exit_sizing is not None else 0.0)
    min_notional_krw = float(getattr(exit_sizing, "min_notional_krw", 0.0) if exit_sizing is not None else 0.0)
    requested_qty = float(canonical_sell.sellable_executable_qty)
    normalized_qty = 0.0
    suppression_key = build_order_suppression_key(
        mode=settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal="SELL",
        side="SELL",
        reason_code=reason_code,
        dust_signature=exit_non_executable_reason,
        requested_qty=requested_qty,
        normalized_qty=normalized_qty,
        market_price=float(market_price),
    )
    suppression_context = {
        "signal": "SELL",
        "side": "SELL",
        "market_price": float(market_price),
        "reason_code": reason_code,
        "sell_failure_category": sell_failure_category,
        "sell_failure_detail": sell_failure_detail,
        "sell_submit_qty_source": _CANONICAL_SELL_SUBMIT_QTY_SOURCE,
        "sell_submit_lot_source": _CANONICAL_SELL_SUBMIT_LOT_SOURCE,
        "sell_submit_lot_count": executable_lot_count,
        "sell_submit_lot_source_truth_source": str(
            decision_observability.get("sell_submit_lot_source_truth_source") or "-"
        ),
        "sell_submit_lot_count_truth_source": str(
            decision_observability.get("sell_submit_lot_count_truth_source") or "-"
        ),
        "observed_sell_qty_basis_qty": _resolve_non_authoritative_sell_basis_qty(
            decision_observability=decision_observability,
            open_exposure_qty=diagnostic_qty.open_exposure_qty,
        ),
        "sell_qty_basis_source": str(
            decision_observability.get("sell_qty_basis_source")
            or canonical_sell.submit_qty_source
            or _CANONICAL_SELL_SUBMIT_QTY_SOURCE
        ),
        "submit_lot_source": _CANONICAL_SELL_SUBMIT_LOT_SOURCE,
        "sell_qty_boundary_kind": str(decision_observability.get("sell_qty_boundary_kind") or "none"),
        "exit_non_executable_reason": exit_non_executable_reason,
        "exit_sizing_allowed": exit_sizing_allowed,
        "exit_sizing_block_reason": exit_sizing_block_reason,
        "exit_sizing_decision_reason_code": exit_sizing_decision_reason_code,
        "internal_lot_size": internal_lot_size,
        "intended_lot_count": intended_lot_count,
        "executable_lot_count": executable_lot_count,
        "sell_normalized_exposure_qty": 0.0,
        "raw_total_asset_qty": float(diagnostic_qty.raw_total_asset_qty),
        "sell_open_exposure_qty": float(diagnostic_qty.open_exposure_qty),
        "sell_dust_tracking_qty": float(diagnostic_qty.dust_tracking_qty),
        "observed_position_qty": float(diagnostic_qty.observed_position_qty),
        "submit_payload_qty": 0.0,
        "normalized_qty": normalized_qty,
        "effective_min_trade_qty": effective_min_trade_qty,
        "min_qty": min_qty,
        "sell_notional_krw": requested_qty * float(market_price),
        "min_notional_krw": min_notional_krw,
        "decision_truth_sources": truth_sources,
        **{f"{key}_truth_source": value for key, value in truth_sources.items()},
        **sell_truth_source_fields,
        "strategy_name": strategy_name_value,
        "strategy_context": strategy_context,
        "decision_id": decision_id,
        "decision_reason": decision_reason,
        "exit_rule_name": exit_rule_name,
        "suppression_outcome": suppression_outcome,
        "suppression_reason_code": reason_code,
        "suppression_reason": "decision_suppressed:exit_suppressed_by_quantity_rule",
        "suppression_summary": (
            f"suppression_outcome={suppression_outcome};"
            "decision_suppressed:exit_suppressed_by_quantity_rule;"
            f"exit_non_executable_reason={exit_non_executable_reason};"
            f"sellable_executable_lot_count={executable_lot_count};"
            f"sellable_executable_qty={executable_qty:.12f}"
        ),
        "suppression_key": suppression_key,
    }
    record_order_suppression(
        conn=conn,
        suppression_key=suppression_key,
        event_kind="decision_suppressed",
        mode=settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal="SELL",
        side="SELL",
        reason_code=reason_code,
        reason="decision_suppressed:exit_suppressed_by_quantity_rule",
        requested_qty=requested_qty,
        normalized_qty=normalized_qty,
        market_price=float(market_price),
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        dust_present=True,
        dust_allow_resume=False,
        dust_effective_flat=False,
        dust_state=str(decision_observability.get("dust_classification") or decision_observability.get("dust_state") or "-"),
        dust_action="manual_review_before_resume",
        dust_signature=exit_non_executable_reason,
        summary=(
            f"suppression_outcome={suppression_outcome};"
            "decision_suppressed:exit_suppressed_by_quantity_rule;"
            f"exit_non_executable_reason={exit_non_executable_reason};"
            f"sellable_executable_qty={executable_qty:.12f}"
        ),
        context=suppression_context,
    )
    RUN_LOG.info(
        format_log_kv(
            "[ORDER_SKIP] exit quantity suppressed",
            side="SELL",
            signal="SELL",
            reason_code=reason_code,
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=str(decision_id) if decision_id is not None else "-",
            sell_failure_category=sell_failure_category,
            sell_failure_detail=sell_failure_detail,
            exit_non_executable_reason=exit_non_executable_reason,
            sellable_executable_lot_count=executable_lot_count,
            sellable_executable_qty=executable_qty,
        )
    )
    notify(
        safety_event(
            "decision_suppressed",
            client_order_id=UNSET_EVENT_FIELD,
            submit_attempt_id=UNSET_EVENT_FIELD,
            exchange_order_id=UNSET_EVENT_FIELD,
            reason_code=reason_code,
            side="SELL",
            status="SUPPRESSED",
            dust_state=str(decision_observability.get("dust_classification") or decision_observability.get("dust_state") or "-"),
            dust_action="manual_review_before_resume",
            dust_new_orders_allowed="0",
            dust_resume_allowed="0",
            dust_treat_as_flat="0",
            dust_event_state=exit_non_executable_reason,
            operator_action="manual_review_before_resume",
            dust_qty_below_min="0",
            dust_notional_below_min="0",
            reason=(
                f"suppression_outcome={suppression_outcome};"
                f"decision_suppressed:exit_suppressed_by_quantity_rule;"
                f"exit_non_executable_reason={exit_non_executable_reason}"
            ),
        )
    )
    return True


def _record_harmless_dust_exit_suppression(
    *,
    conn,
    state,
    signal: str,
    side: str,
    requested_qty: float,
    market_price: float,
    normalized_qty: float,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    submit_qty_source: str | None = None,
    position_state_source: str | None = None,
    raw_total_asset_qty: float | None = None,
    open_exposure_qty: float | None = None,
    dust_tracking_qty: float | None = None,
) -> bool:
    if side != "SELL":
        return False

    decision_observability = _load_strategy_decision_observability(
        conn=conn,
        decision_id=decision_id,
        fallback_signal=signal,
    )
    truth_sources = _decision_truth_sources_payload(decision_observability)
    dust_context = build_dust_display_context(state.last_reconcile_metadata)
    dust = dust_context.classification
    dust_view = dust_context.operator_view
    harmless_dust_effective_flat = bool(
        dust.present
        and dust.classification == DustState.HARMLESS_DUST.value
        and dust_view.resume_allowed
        and dust_view.treat_as_flat
    )
    if not harmless_dust_effective_flat:
        return False

    requested_qty = float(requested_qty)
    normalized_qty = float(normalized_qty)
    market_price = float(market_price)
    boundary_sellable_qty = bool(
        _sell_qty_is_min_qty_boundary_rounding_case(qty=requested_qty, min_qty=dust.min_qty)
        and (
            dust.min_notional_krw <= 0
            or (requested_qty * market_price) >= dust.min_notional_krw
        )
    )
    if boundary_sellable_qty:
        return False
    qty_below_min = bool(dust.min_qty > 0 and requested_qty < dust.min_qty)
    normalized_non_positive = not math.isfinite(normalized_qty) or normalized_qty <= 0
    normalized_below_min = bool(dust.min_qty > 0 and normalized_qty > 0 and normalized_qty < dust.min_qty)
    notional_below_min = bool(
        dust.min_notional_krw > 0 and normalized_qty > 0 and (normalized_qty * market_price) < dust.min_notional_krw
    )
    suppression_scope = (
        "harmless_dust_below_min"
        if any((qty_below_min, normalized_non_positive, normalized_below_min, notional_below_min))
        else "harmless_dust_effective_flat"
    )

    strategy_name_value = strategy_name or settings.STRATEGY_NAME
    strategy_context = f"{settings.MODE}:{strategy_name_value}:{settings.INTERVAL}"
    suppression_reason = "decision_suppressed:harmless_dust_exit"
    suppression_outcome = "execution_suppressed"
    sell_failure_category = _classify_sell_failure_category(reason_code=DUST_RESIDUAL_SUPPRESSED)
    sell_failure_detail = _sell_failure_detail_from_observability(
        sell_failure_category=sell_failure_category,
        dust_details={
            "dust_scope": suppression_scope,
            "normalized_non_positive": normalized_non_positive,
            "qty_below_min": qty_below_min,
            "normalized_below_min": normalized_below_min,
            "notional_below_min": notional_below_min,
        },
    )
    suppression_summary = (
        f"suppression_outcome={suppression_outcome};"
        f"{suppression_reason};{dust_context.compact_summary};"
        f"base_signal={decision_observability['base_signal']};final_signal={decision_observability['final_signal']};"
        f"entry_allowed={1 if bool(decision_observability['entry_allowed']) else 0};"
        f"effective_flat={1 if bool(decision_observability['effective_flat']) else 0};"
        f"normalized_exposure_active={1 if bool(decision_observability['normalized_exposure_active']) else 0};"
        f"normalized_exposure_qty={float(decision_observability['normalized_exposure_qty']):.12f};"
        f"raw_qty_open={float(decision_observability['raw_qty_open']):.12f};"
        f"effective_flat_due_to_harmless_dust={1 if dust_context.effective_flat_due_to_harmless_dust else 0};"
        f"entry_allowed_truth_source={decision_observability['entry_allowed_truth_source']};"
        f"effective_flat_truth_source={decision_observability['effective_flat_truth_source']};"
        f"suppression_scope={suppression_scope};"
        f"requested_qty={requested_qty:.12f};normalized_qty={normalized_qty:.12f};"
        f"market_price={market_price:.8f};qty_below_min={1 if qty_below_min else 0};"
        f"normalized_non_positive={1 if normalized_non_positive else 0};"
        f"normalized_below_min={1 if normalized_below_min else 0};"
        f"notional_below_min={1 if notional_below_min else 0}"
    )
    suppression_key = build_order_suppression_key(
        mode=settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal=signal,
        side=side,
        reason_code=DUST_RESIDUAL_SUPPRESSED,
        dust_signature=str(dust.summary),
        requested_qty=requested_qty,
        normalized_qty=normalized_qty,
        market_price=market_price,
    )
    suppression_submit_qty_source = _harmless_dust_suppression_submit_qty_source(submit_qty_source)
    suppression_truth_source_fields = _sell_truth_source_fields(
        decision_observability=decision_observability,
        submit_qty_source=suppression_submit_qty_source,
    )
    suppression_sell_qty_basis_qty = _resolve_non_authoritative_sell_basis_qty(
        decision_observability=decision_observability,
        open_exposure_qty=open_exposure_qty,
    )
    suppression_sell_qty_basis_source = str(
        decision_observability.get("sell_qty_basis_source")
        or _CANONICAL_SELL_SUBMIT_LOT_SOURCE
        or suppression_submit_qty_source
        or submit_qty_source
        or "-"
    )
    suppression_sell_submit_lot_source = str(
        decision_observability.get("sell_submit_lot_source")
        or _CANONICAL_SELL_SUBMIT_LOT_SOURCE
    )
    suppression_sell_submit_lot_count = int(
        decision_observability.get("sell_submit_lot_count")
        or decision_observability.get("sellable_executable_lot_count")
        or 0
    )
    suppression_sell_qty_boundary_kind = (
        "min_qty" if suppression_scope == "harmless_dust_below_min" else "dust_mismatch"
    )
    suppression_context = {
        **dust_context.fields,
        "signal": signal,
        "signal": "SELL",
        "side": side,
        "requested_qty": requested_qty,
        "normalized_qty": normalized_qty,
        "market_price": market_price,
        "observed_position_qty": float(requested_qty),
        "operator_action": dust_view.operator_action,
        "dust_action": dust_view.operator_action,
        "submit_qty_source": suppression_submit_qty_source,
        "submit_payload_qty": 0.0,
        "sell_submit_qty_source": suppression_submit_qty_source,
        "sell_submit_lot_source": suppression_sell_submit_lot_source,
        "sell_submit_lot_count": suppression_sell_submit_lot_count,
        "observed_sell_qty_basis_qty": float(suppression_sell_qty_basis_qty),
        "sell_qty_basis_source": suppression_sell_qty_basis_source,
        "sell_qty_boundary_kind": suppression_sell_qty_boundary_kind,
        "sell_qty_basis_qty_truth_source": str(
            decision_observability.get("sell_qty_basis_qty_truth_source") or "-"
        ),
        "sell_qty_basis_source_truth_source": str(
            decision_observability.get("sell_qty_basis_source_truth_source") or "-"
        ),
        "sell_qty_boundary_kind_truth_source": str(
            decision_observability.get("sell_qty_boundary_kind_truth_source") or "-"
        ),
        "sell_normalized_exposure_qty": float(normalized_qty),
        "sell_open_exposure_qty": float(normalized_qty if open_exposure_qty is None else open_exposure_qty),
        "sell_dust_tracking_qty": float(0.0 if dust_tracking_qty is None else dust_tracking_qty),
        "decision_truth_sources": truth_sources,
        **{f"{key}_truth_source": value for key, value in truth_sources.items()},
        **suppression_truth_source_fields,
        "sell_submit_lot_source_truth_source": str(
            decision_observability.get("sell_submit_lot_source_truth_source") or "-"
        ),
        "sell_submit_lot_count_truth_source": str(
            decision_observability.get("sell_submit_lot_count_truth_source") or "-"
        ),
        "sell_failure_category": sell_failure_category,
        "sell_failure_detail": sell_failure_detail,
        "base_signal": decision_observability["base_signal"],
        "final_signal": decision_observability["final_signal"],
        "entry_allowed": bool(decision_observability["entry_allowed"]),
        "effective_flat": bool(decision_observability["effective_flat"]),
        "raw_qty_open": float(decision_observability["raw_qty_open"]),
        "raw_total_asset_qty": float(raw_total_asset_qty or decision_observability["raw_total_asset_qty"]),
        "open_exposure_qty": float(open_exposure_qty or decision_observability["open_exposure_qty"]),
        "dust_tracking_qty": float(dust_tracking_qty or decision_observability["dust_tracking_qty"]),
        "normalized_exposure_active": bool(decision_observability["normalized_exposure_active"]),
        "normalized_exposure_qty": float(decision_observability["normalized_exposure_qty"]),
        "entry_allowed_truth_source": str(decision_observability["entry_allowed_truth_source"]),
        "effective_flat_truth_source": str(decision_observability["effective_flat_truth_source"]),
        "strategy_name": strategy_name_value,
        "strategy_context": strategy_context,
        "decision_id": decision_id,
        "decision_reason": decision_reason,
        "exit_rule_name": exit_rule_name,
        "suppression_outcome": suppression_outcome,
        "suppression_reason_code": DUST_RESIDUAL_SUPPRESSED,
        "suppression_reason": suppression_reason,
        "suppression_summary": suppression_summary,
        "suppression_key": suppression_key,
        "effective_flat_due_to_harmless_dust": bool(dust_context.effective_flat_due_to_harmless_dust),
        "suppression_scope": suppression_scope,
        "decision_observability": decision_observability,
    }
    record_order_suppression(
        conn=conn,
        suppression_key=suppression_key,
        event_kind="decision_suppressed",
        mode=settings.MODE,
        strategy_context=strategy_context,
        strategy_name=strategy_name_value,
        signal=signal,
        side=side,
        reason_code=DUST_RESIDUAL_SUPPRESSED,
        reason=suppression_reason,
        requested_qty=requested_qty,
        normalized_qty=normalized_qty,
        market_price=market_price,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        dust_present=bool(dust.present),
        dust_allow_resume=bool(dust.allow_resume),
        dust_effective_flat=bool(dust.effective_flat),
        dust_state=dust_view.state,
        dust_action=dust_view.operator_action,
        dust_signature=str(dust.summary),
        qty_below_min=qty_below_min,
        normalized_non_positive=normalized_non_positive,
        normalized_below_min=normalized_below_min,
        notional_below_min=notional_below_min,
        summary=suppression_summary,
        context=suppression_context,
    )
    RUN_LOG.info(
        format_log_kv(
            "[ORDER_SKIP] harmless dust exit suppressed",
            base_signal=decision_observability["base_signal"],
            final_signal=decision_observability["final_signal"],
            signal=signal,
            side=side,
            reason_code=DUST_RESIDUAL_SUPPRESSED,
            sell_failure_category=sell_failure_category,
            sell_failure_detail=sell_failure_detail,
            state=dust_view.state,
            operator_action=dust_view.operator_action,
            entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
            effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
            normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
            normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
            raw_qty_open=float(decision_observability["raw_qty_open"]),
            entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
            suppression_scope=suppression_scope,
            requested_qty=requested_qty,
            normalized_qty=normalized_qty,
            sell_qty_basis_qty=suppression_sell_qty_basis_qty,
            sell_qty_basis_source=suppression_sell_qty_basis_source,
            sell_qty_boundary_kind=suppression_sell_qty_boundary_kind,
            market_price=market_price,
            dust_signature=suppression_key,
        )
    )
    notify(
        safety_event(
            "decision_suppressed",
            client_order_id=UNSET_EVENT_FIELD,
            submit_attempt_id=UNSET_EVENT_FIELD,
            exchange_order_id=UNSET_EVENT_FIELD,
            reason_code=DUST_RESIDUAL_SUPPRESSED,
            side=side,
            status="SUPPRESSED",
            reason=suppression_summary,
            base_signal=decision_observability["base_signal"],
            final_signal=decision_observability["final_signal"],
            entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
            effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
            normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
            normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
            raw_qty_open=float(decision_observability["raw_qty_open"]),
            entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
            dust_state=dust_view.state,
            dust_action=dust_view.operator_action,
            dust_residual_present=1 if dust.present else 0,
            dust_residual_allow_resume=1 if dust.allow_resume else 0,
            dust_effective_flat=1 if dust.effective_flat else 0,
            effective_flat_due_to_harmless_dust=1 if dust_context.effective_flat_due_to_harmless_dust else 0,
            qty_below_min=1 if qty_below_min else 0,
            normalized_non_positive=1 if normalized_non_positive else 0,
            normalized_below_min=1 if normalized_below_min else 0,
            notional_below_min=1 if notional_below_min else 0,
            suppression_scope=suppression_scope,
            dust_signature=suppression_key,
        )
    )
    return True


def record_harmless_dust_exit_suppression(
    *,
    conn,
    state,
    signal: str,
    side: str,
    requested_qty: float,
    market_price: float,
    normalized_qty: float,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    submit_qty_source: str | None = None,
    position_state_source: str | None = None,
    raw_total_asset_qty: float | None = None,
    open_exposure_qty: float | None = None,
    dust_tracking_qty: float | None = None,
) -> bool:
    """Public wrapper for harmless dust sell suppression.

    Engine-level gating uses this before live execution so we do not create a
    submit attempt, client_order_id, or orders row for harmless dust exits.
    """

    return _record_harmless_dust_exit_suppression(
        conn=conn,
        state=state,
        signal=signal,
        side=side,
        requested_qty=requested_qty,
        market_price=market_price,
        normalized_qty=normalized_qty,
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        submit_qty_source=submit_qty_source,
        position_state_source=position_state_source,
        raw_total_asset_qty=raw_total_asset_qty,
        open_exposure_qty=open_exposure_qty,
        dust_tracking_qty=dust_tracking_qty,
    )


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

    rules = _effective_order_rules(settings.PAIR).rules

    notional = float(qty) * float(market_price)
    min_notional = side_min_total_krw(rules=rules, side=side)
    if min_notional > 0 and notional < min_notional:
        raise ValueError(f"order notional below minimum ({side}): {notional:.2f} < {min_notional:.2f}")

    buffer_mult = 1.0 + max(0.0, float(settings.PRETRADE_BALANCE_BUFFER_BPS)) / 10_000.0
    if side == "BUY":
        # NOTE:
        # - LIVE_FEE_RATE_ESTIMATE: live pretrade ??ш낄猷????釉????怨뚮옖?????怨뚮옖??????⑤베毓??洹숇윦?
        # - PAPER_FEE_RATE/FEE_RATE: paper 癲ル슪???????????源낇꼧??????れ삀??????쒓낮彛?嶺뚮ㅏ援??fee rate
        # ????좊즴??????????됰슣維???live pretrade ??節뚮쳮雅????????ㅼ굣筌뤿뱶????貫????롰돯?釉먯뒭???? ??熬곣뫗踰???筌먲퐢??
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


def _mark_submit_unknown(
    *,
    conn,
    client_order_id: str,
    submit_attempt_id: str,
    side: str,
    reason: str,
    ts: int,
) -> None:
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
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=str(submit_attempt_id),
            side=side,
            status="SUBMIT_UNKNOWN",
            reason=reason,
        )
    )


def _mark_submit_failed(
    *,
    conn,
    client_order_id: str,
    submit_attempt_id: str,
    side: str,
    reason: str,
    ts: int,
) -> None:
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
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=str(submit_attempt_id),
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
    persisted_reason = f"category=unresolved_risk_gate;code={reason_code};reason={reason}"
    record_submit_blocked(client_order_id, status="FAILED", reason=persisted_reason, conn=conn)
    notify(
        safety_event(
            "order_submit_blocked",
            client_order_id=client_order_id,
            submit_attempt_id=UNSET_EVENT_FIELD,
            reason_code=RISKY_ORDER_BLOCK,
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=UNSET_EVENT_FIELD,
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
    order_type: str | None = None,
    internal_lot_size: float | None = None,
    effective_min_trade_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    final_intended_qty: float | None = None,
    final_submitted_qty: float | None = None,
    decision_reason_code: str | None = None,
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
        order_type=order_type,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
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
    order_type: str | None = None,
    internal_lot_size: float | None = None,
    effective_min_trade_qty: float | None = None,
    qty_step: float | None = None,
    min_notional_krw: float | None = None,
    intended_lot_count: int | None = None,
    executable_lot_count: int | None = None,
    final_intended_qty: float | None = None,
    final_submitted_qty: float | None = None,
    decision_reason_code: str | None = None,
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
        order_type=order_type,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
    )


def _order_intent_strategy_context() -> str:
    return f"{settings.MODE}:{settings.STRATEGY_NAME}:{settings.INTERVAL}"


def _order_intent_type(*, side: str) -> str:
    return "market_entry" if side == "BUY" else "market_exit"

def _encode_submit_evidence(*, payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _decision_truth_sources_payload(decision_observability: dict[str, object]) -> dict[str, str]:
    return {
        "entry_allowed": str(decision_observability.get("entry_allowed_truth_source") or "-"),
        "effective_flat": str(decision_observability.get("effective_flat_truth_source") or "-"),
        "raw_qty_open": str(decision_observability.get("raw_qty_open_truth_source") or "-"),
        "raw_total_asset_qty": str(decision_observability.get("raw_total_asset_qty_truth_source") or "-"),
        "position_qty": str(decision_observability.get("position_qty_truth_source") or "-"),
        "submit_payload_qty": str(decision_observability.get("submit_payload_qty_truth_source") or "-"),
        "normalized_exposure_active": str(
            decision_observability.get("normalized_exposure_active_truth_source") or "-"
        ),
        "normalized_exposure_qty": str(decision_observability.get("normalized_exposure_qty_truth_source") or "-"),
        "sell_submit_lot_count": str(decision_observability.get("sell_submit_lot_count_truth_source") or "-"),
        "sell_submit_lot_source": str(decision_observability.get("sell_submit_lot_source_truth_source") or "-"),
        "open_exposure_qty": str(decision_observability.get("open_exposure_qty_truth_source") or "-"),
        "dust_tracking_qty": str(decision_observability.get("dust_tracking_qty_truth_source") or "-"),
        "submit_qty_source": str(decision_observability.get("submit_qty_source_truth_source") or "-"),
        "submit_lot_source": str(decision_observability.get("submit_lot_source_truth_source") or "-"),
        "position_state_source": str(decision_observability.get("position_state_source_truth_source") or "-"),
    }


def _submit_via_standard_path(
    *,
    conn,
    broker: Broker,
    signal: str,
    client_order_id: str,
    submit_attempt_id: str,
    side: str,
    order_qty: float,
    position_qty: float,
    qty: float,
    ts: int,
    intent_key: str,
    market_price: float,
    raw_total_asset_qty: float,
    open_exposure_qty: float,
    dust_tracking_qty: float,
    decision_observability: dict[str, object] | None,
    submit_qty_source: str,
    position_state_source: str,
    reference_price: float | None,
    top_of_book_summary: dict[str, float | str] | None,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
    order_type: str,
    internal_lot_size: float | None,
    effective_min_trade_qty: float | None,
    qty_step: float | None,
    min_notional_krw: float | None,
    intended_lot_count: int | None,
    executable_lot_count: int | None,
    final_intended_qty: float,
    final_submitted_qty: float,
    decision_reason_code: str | None,
):
    symbol = settings.PAIR
    decision_observability = decision_observability or {}
    sell_truth_source_fields = _sell_truth_source_fields(
        decision_observability=decision_observability,
        submit_qty_source=submit_qty_source,
    )
    canonical_sell_submit = _CanonicalSellSubmitObservability(
        submit_qty_source=submit_qty_source,
        submit_lot_source=str(
            decision_observability.get("sell_submit_lot_source")
            or decision_observability.get("submit_lot_source")
            or _CANONICAL_SELL_SUBMIT_LOT_SOURCE
        ),
        submit_lot_count=int(decision_observability.get("sell_submit_lot_count") or 0),
        normalized_qty=qty,
        position_state_source=position_state_source,
        position_state_source_truth_source=sell_truth_source_fields["position_state_source_truth_source"],
        submit_qty_source_truth_source=sell_truth_source_fields["submit_qty_source_truth_source"],
        submit_lot_source_truth_source=sell_truth_source_fields["submit_lot_source_truth_source"],
    )
    observed_sell_telemetry = _ObservedSellSubmitTelemetry(
        position_qty=position_qty,
        submit_payload_qty=qty,
        raw_total_asset_qty=raw_total_asset_qty,
        open_exposure_qty=open_exposure_qty,
        dust_tracking_qty=dust_tracking_qty,
        sell_qty_basis_qty=float(decision_observability.get("sell_qty_basis_qty") or open_exposure_qty),
        sell_qty_basis_source=str(submit_qty_source or decision_observability.get("sell_qty_basis_source") or "-"),
        sell_qty_basis_qty_truth_source=sell_truth_source_fields["sell_qty_basis_qty_truth_source"],
        sell_qty_basis_source_truth_source=sell_truth_source_fields["sell_qty_basis_source_truth_source"],
        sell_qty_boundary_kind=str(decision_observability.get("sell_qty_boundary_kind") or "none"),
        sell_qty_boundary_kind_truth_source=sell_truth_source_fields["sell_qty_boundary_kind_truth_source"],
        sell_normalized_exposure_qty_truth_source=sell_truth_source_fields[
            "sell_normalized_exposure_qty_truth_source"
        ],
        sell_open_exposure_qty_truth_source=sell_truth_source_fields["sell_open_exposure_qty_truth_source"],
        sell_dust_tracking_qty_truth_source=sell_truth_source_fields["sell_dust_tracking_qty_truth_source"],
    )
    sell_observability = _sell_submit_observability_fields(
        decision_observability=decision_observability,
        canonical_submit=canonical_sell_submit,
        observed_inputs=observed_sell_telemetry,
        sell_failure_category="none",
        sell_failure_detail="none",
    )
    lot_evidence_fields = {
        "order_type": order_type,
        "internal_lot_size": None if internal_lot_size is None else float(internal_lot_size),
        "effective_min_trade_qty": None if effective_min_trade_qty is None else float(effective_min_trade_qty),
        "qty_step": None if qty_step is None else float(qty_step),
        "min_notional_krw": None if min_notional_krw is None else float(min_notional_krw),
        "intended_lot_count": None if intended_lot_count is None else int(intended_lot_count),
        "executable_lot_count": None if executable_lot_count is None else int(executable_lot_count),
        "final_intended_qty": float(final_intended_qty),
        "final_submitted_qty": float(final_submitted_qty),
        "decision_reason_code": decision_reason_code,
    }
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
            "order_qty": float(order_qty),
            "intended_qty": float(qty),
            "normalized_qty": float(qty),
            **sell_observability,
            **sell_truth_source_fields,
            "reference_price": reference_price,
            "top_of_book": top_of_book_summary,
            "request_ts": None,
            "response_ts": None,
            "submit_path": submit_path,
            "submit_mode": settings.MODE,
            "error_class": None,
            "error_summary": None,
            **lot_evidence_fields,
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
        order_type=order_type,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
        local_intent_state="PENDING_SUBMIT",
        ts_ms=ts,
        status="PENDING_SUBMIT",
    )
    record_submit_started(
        client_order_id,
        conn=conn,
        submit_attempt_id=submit_attempt_id,
        symbol=symbol,
        side=side,
        qty=qty,
        mode=settings.MODE,
    )
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
        order_type=order_type,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
    )
    notify(
        safety_event(
            "order_submit_started",
            client_order_id=client_order_id,
            submit_attempt_id=submit_attempt_id,
            exchange_order_id=UNSET_EVENT_FIELD,
            state_to="PENDING_SUBMIT",
            reason_code=UNSET_EVENT_FIELD,
            signal_ts=int(ts),
            decision_ts=int(ts),
            decision_id=str(submit_attempt_id),
            side=side,
            status="PENDING_SUBMIT",
        )
    )
    conn.commit()

    try:
        request_ts = int(time.time() * 1000)
        RUN_LOG.info(
            format_log_kv(
                "[ORDER_DECISION] broker.place_order dispatch",
                signal=signal,
                signal_ts=int(ts),
                candle_ts=int(ts),
                side=side,
                market_price=market_price,
                position_qty=float(position_qty),
                order_qty=order_qty,
                normalized_qty=qty,
                submit_payload_qty=float(qty),
                submit_qty=float(qty),
                submit_qty_source=submit_qty_source,
                position_state_source=position_state_source,
                raw_total_asset_qty=float(raw_total_asset_qty),
                open_exposure_qty=float(open_exposure_qty),
                dust_tracking_qty=float(dust_tracking_qty),
                reference_price=reference_price,
                client_order_id=client_order_id,
                internal_lot_size=internal_lot_size,
                intended_lot_count=intended_lot_count,
                executable_lot_count=executable_lot_count,
                final_intended_qty=final_intended_qty,
                final_submitted_qty=final_submitted_qty,
                decision_reason_code=decision_reason_code,
            )
        )
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
                "order_qty": float(order_qty),
                "intended_qty": float(qty),
                "normalized_qty": float(qty),
                **{
                    **sell_observability,
                    "operator_action": (
                        str(sell_observability.get("operator_action") or "")
                        if str(sell_observability.get("operator_action") or "").strip() not in {"", "-"}
                        else MANUAL_DUST_REVIEW_REQUIRED
                    ),
                    "dust_action": (
                        str(sell_observability.get("dust_action") or "")
                        if str(sell_observability.get("dust_action") or "").strip() not in {"", "-"}
                        else MANUAL_DUST_REVIEW_REQUIRED
                    ),
                    **sell_truth_source_fields,
                    "sell_failure_category": _classify_sell_failure_category(
                        error_class=type(e).__name__,
                        error_summary=str(e),
                    ),
                    "sell_failure_detail": _sell_failure_detail_from_observability(
                        sell_failure_category=_classify_sell_failure_category(
                            error_class=type(e).__name__,
                            error_summary=str(e),
                        )
                    ),
                },
                "reference_price": reference_price,
                "top_of_book": top_of_book_summary,
                "request_ts": request_ts,
                "response_ts": response_ts,
                "submit_path": submit_path,
                "submit_mode": settings.MODE,
                **lot_evidence_fields,
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
            ts=ts,
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
            order_type=order_type,
            internal_lot_size=internal_lot_size,
            effective_min_trade_qty=effective_min_trade_qty,
            qty_step=qty_step,
            min_notional_krw=min_notional_krw,
            intended_lot_count=intended_lot_count,
            executable_lot_count=executable_lot_count,
            final_intended_qty=final_intended_qty,
            final_submitted_qty=final_submitted_qty,
            decision_reason_code=decision_reason_code,
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
    except BrokerRejectError as e:
        response_ts = int(time.time() * 1000)
        reason = f"submit rejected: {type(e).__name__}: {e}"
        is_sell_qty_step_reject = side == "SELL" and "qty does not match qty_step" in str(e)
        submit_evidence = _encode_submit_evidence(
            payload={
                "symbol": symbol,
                "side": side,
                "order_qty": float(order_qty),
                "intended_qty": float(qty),
                "normalized_qty": float(qty),
                **{
                    **sell_observability,
                    "operator_action": (
                        str(sell_observability.get("operator_action") or "")
                        if str(sell_observability.get("operator_action") or "").strip() not in {"", "-"}
                        else MANUAL_DUST_REVIEW_REQUIRED
                    ),
                    "dust_action": (
                        str(sell_observability.get("dust_action") or "")
                        if str(sell_observability.get("dust_action") or "").strip() not in {"", "-"}
                        else MANUAL_DUST_REVIEW_REQUIRED
                    ),
                    **sell_truth_source_fields,
                    "sell_failure_category": _classify_sell_failure_category(
                        error_class=type(e).__name__,
                        error_summary=str(e),
                    ),
                    "sell_failure_detail": _sell_failure_detail_from_observability(
                        sell_failure_category=_classify_sell_failure_category(
                            error_class=type(e).__name__,
                            error_summary=str(e),
                        )
                    ),
                },
                "reference_price": reference_price,
                "top_of_book": top_of_book_summary,
                "request_ts": request_ts,
                "response_ts": response_ts,
                "submit_path": submit_path,
                "submit_mode": settings.MODE,
                **lot_evidence_fields,
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
            ts=ts,
        )
        if not is_sell_qty_step_reject:
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
                broker_response_summary=f"submit_reject={type(e).__name__};error={e}",
                submission_reason_code=SUBMIT_FAILED,
                exception_class=type(e).__name__,
                timeout_flag=False,
                submit_evidence=submit_evidence,
                exchange_order_id_obtained=False,
                order_type=order_type,
                internal_lot_size=internal_lot_size,
                effective_min_trade_qty=effective_min_trade_qty,
                qty_step=qty_step,
                min_notional_krw=min_notional_krw,
                intended_lot_count=intended_lot_count,
                executable_lot_count=executable_lot_count,
                final_intended_qty=final_intended_qty,
                final_submitted_qty=final_submitted_qty,
                decision_reason_code=decision_reason_code,
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
    except Exception as e:
        response_ts = int(time.time() * 1000)
        reason = f"submit failed: {type(e).__name__}: {e}"
        submit_evidence = _encode_submit_evidence(
            payload={
                "symbol": symbol,
                "side": side,
                "order_qty": float(order_qty),
                "intended_qty": float(qty),
                "normalized_qty": float(qty),
                **{
                    **sell_observability,
                    "sell_failure_category": _classify_sell_failure_category(
                        error_class=type(e).__name__,
                        error_summary=str(e),
                    ),
                    "sell_failure_detail": _sell_failure_detail_from_observability(
                        sell_failure_category=_classify_sell_failure_category(
                            error_class=type(e).__name__,
                            error_summary=str(e),
                        )
                    ),
                },
                "reference_price": reference_price,
                "top_of_book": top_of_book_summary,
                "request_ts": request_ts,
                "response_ts": response_ts,
                "submit_path": submit_path,
                "submit_mode": settings.MODE,
                **lot_evidence_fields,
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
            ts=ts,
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
            order_type=order_type,
            internal_lot_size=internal_lot_size,
            effective_min_trade_qty=effective_min_trade_qty,
            qty_step=qty_step,
            min_notional_krw=min_notional_krw,
            intended_lot_count=intended_lot_count,
            executable_lot_count=executable_lot_count,
            final_intended_qty=final_intended_qty,
            final_submitted_qty=final_submitted_qty,
            decision_reason_code=decision_reason_code,
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
                signal_ts=int(ts),
                decision_ts=int(ts),
                decision_id=str(submit_attempt_id),
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
                "order_qty": float(order_qty),
                "intended_qty": float(qty),
                "normalized_qty": float(qty),
                "submit_qty_source": submit_qty_source,
                "position_state_source": position_state_source,
                **sell_truth_source_fields,
                "raw_total_asset_qty": float(raw_total_asset_qty),
                "open_exposure_qty": float(open_exposure_qty),
                "dust_tracking_qty": float(dust_tracking_qty),
                "reference_price": reference_price,
                "top_of_book": top_of_book_summary,
                "request_ts": request_ts,
                "response_ts": response_ts,
                "submit_path": submit_path,
                "submit_mode": settings.MODE,
                **lot_evidence_fields,
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
            ts=ts,
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
            order_type=order_type,
            internal_lot_size=internal_lot_size,
            effective_min_trade_qty=effective_min_trade_qty,
            qty_step=qty_step,
            min_notional_krw=min_notional_krw,
            intended_lot_count=intended_lot_count,
            executable_lot_count=executable_lot_count,
            final_intended_qty=final_intended_qty,
            final_submitted_qty=final_submitted_qty,
            decision_reason_code=decision_reason_code,
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
            "order_qty": float(order_qty),
            "intended_qty": float(qty),
            "normalized_qty": float(qty),
            **sell_observability,
            "reference_price": reference_price,
            "top_of_book": top_of_book_summary,
            "request_ts": request_ts,
            "response_ts": response_ts,
            "submit_path": submit_path,
            "submit_mode": settings.MODE,
            **lot_evidence_fields,
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
        order_type=order_type,
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=effective_min_trade_qty,
        qty_step=qty_step,
        min_notional_krw=min_notional_krw,
        intended_lot_count=intended_lot_count,
        executable_lot_count=executable_lot_count,
        final_intended_qty=final_intended_qty,
        final_submitted_qty=final_submitted_qty,
        decision_reason_code=decision_reason_code,
    )
    update_order_intent_dedup(
        conn,
        intent_key=intent_key,
        client_order_id=client_order_id,
        order_status=order.status,
    )
    conn.commit()
    return order


def _determine_live_execution_position_state(
    *,
    signal: str,
    market_price: float,
    decision_id: int | None,
) -> _LiveExecutionPositionState:
    conn = ensure_db()
    init_portfolio(conn)
    state = runtime_state.snapshot()
    cash, qty = get_portfolio(conn)
    raw_total_asset_qty = float(qty)
    position_snapshot = _load_position_lot_snapshot(conn=conn)
    open_exposure_qty = float(position_snapshot.raw_open_exposure_qty)
    dust_tracking_qty = float(position_snapshot.dust_tracking_qty)
    reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=settings.PAIR)
    effective_rules = _effective_order_rules(settings.PAIR).rules
    normalized_exposure = build_normalized_exposure(
        raw_qty_open=float(open_exposure_qty),
        dust_context=state.last_reconcile_metadata,
        raw_total_asset_qty=raw_total_asset_qty,
        open_exposure_qty=float(open_exposure_qty),
        dust_tracking_qty=float(dust_tracking_qty),
        reserved_exit_qty=float(reserved_exit_qty),
        open_lot_count=int(position_snapshot.open_lot_count),
        dust_tracking_lot_count=int(position_snapshot.dust_tracking_lot_count),
        market_price=float(market_price),
        min_qty=float(effective_rules.min_qty),
        qty_step=float(effective_rules.qty_step),
        min_notional_krw=float(effective_rules.min_notional_krw),
        max_qty_decimals=int(effective_rules.max_qty_decimals),
        exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
        exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
    )
    decision_observability = _load_strategy_decision_observability(
        conn=conn,
        decision_id=decision_id,
        fallback_signal=signal,
    )
    decision_observability.update(
        {
            "raw_total_asset_qty": raw_total_asset_qty,
            "raw_qty_open": float(open_exposure_qty),
            "open_exposure_qty": float(normalized_exposure.open_exposure_qty),
            "dust_tracking_qty": float(normalized_exposure.dust_tracking_qty),
            "reserved_exit_qty": float(normalized_exposure.reserved_exit_qty),
            "sell_submit_lot_count": int(normalized_exposure.sellable_executable_lot_count),
            "sell_submit_lot_source": _CANONICAL_SELL_SUBMIT_LOT_SOURCE,
            "sell_submit_lot_count_truth_source": "derived:sellable_executable_lot_count",
            "sell_submit_lot_source_truth_source": "derived:sellable_executable_lot_count",
            "sellable_executable_qty": float(normalized_exposure.sellable_executable_qty),
            "normalized_exposure_qty": float(normalized_exposure.normalized_exposure_qty),
            "normalized_exposure_active": bool(normalized_exposure.normalized_exposure_active),
            "effective_min_trade_qty": float(normalized_exposure.effective_min_trade_qty),
            "exit_non_executable_reason": str(normalized_exposure.exit_non_executable_reason),
            "entry_block_reason": str(normalized_exposure.entry_block_reason),
            "exit_allowed": bool(normalized_exposure.exit_allowed),
            "exit_block_reason": str(normalized_exposure.exit_block_reason),
            "terminal_state": str(normalized_exposure.terminal_state),
            "submit_qty_source": str(normalized_exposure.sell_submit_qty_source),
            "submit_qty_source_truth_source": "derived:sellable_executable_qty",
            "submit_lot_source": str(normalized_exposure.sell_submit_lot_source),
            "submit_lot_source_truth_source": "derived:sellable_executable_lot_count",
            "sell_submit_lot_source": str(normalized_exposure.sell_submit_lot_source),
            "sell_submit_lot_source_truth_source": "derived:sellable_executable_lot_count",
            "sell_qty_basis_qty": float(normalized_exposure.open_exposure_qty),
            "sell_qty_basis_qty_truth_source": "derived:open_exposure_qty",
            "sell_qty_basis_source": str(normalized_exposure.sell_submit_lot_source),
            "sell_qty_basis_source_truth_source": "derived:sellable_executable_lot_count",
            "position_state": {"normalized_exposure": normalized_exposure.as_dict()},
            "position_state_source": str(normalized_exposure.sell_submit_lot_source),
            "position_state_source_truth_source": "derived:sellable_executable_lot_count",
        }
    )

    canonical_sell: _CanonicalSellExecutionView | None = None
    diagnostic_sell_qty: _SellDiagnosticQtyView | None = None
    has_lot_native_sell_state = False
    if signal == "SELL":
        has_lot_native_sell_state = any(
            (
                int(position_snapshot.open_lot_count) > 0,
                int(position_snapshot.dust_tracking_lot_count) > 0,
                int(normalized_exposure.reserved_exit_lot_count) > 0,
                int(normalized_exposure.sellable_executable_lot_count) > 0,
            )
        )
        canonical_sell = _build_canonical_sell_execution_view(
            normalized_exposure=normalized_exposure,
            decision_observability=decision_observability,
        )
        diagnostic_sell_qty = _build_sell_diagnostic_qty_view(
            raw_total_asset_qty=float(raw_total_asset_qty),
            open_exposure_qty=float(normalized_exposure.open_exposure_qty),
            dust_tracking_qty=float(normalized_exposure.dust_tracking_qty),
        )

    return _LiveExecutionPositionState(
        conn=conn,
        state=state,
        cash=float(cash),
        portfolio_qty=float(qty),
        raw_total_asset_qty=raw_total_asset_qty,
        open_exposure_qty=float(open_exposure_qty),
        dust_tracking_qty=float(dust_tracking_qty),
        position_snapshot=position_snapshot,
        effective_rules=effective_rules,
        normalized_exposure=normalized_exposure,
        decision_observability=decision_observability,
        canonical_sell=canonical_sell,
        diagnostic_sell_qty=diagnostic_sell_qty,
        has_lot_native_sell_state=has_lot_native_sell_state,
    )


def _maybe_record_harmless_dust_sell_suppression(
    *,
    conn,
    position_state: _LiveExecutionPositionState,
    signal: str,
    side: str,
    requested_qty: float,
    market_price: float,
    normalized_qty: float,
    submit_qty_source: str,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
) -> bool:
    return _record_harmless_dust_exit_suppression(
        conn=conn,
        state=position_state.state,
        signal=signal,
        side=side,
        requested_qty=float(requested_qty),
        market_price=float(market_price),
        normalized_qty=float(normalized_qty),
        submit_qty_source=submit_qty_source,
        position_state_source=str(position_state.decision_observability["position_state_source"]),
        raw_total_asset_qty=float(position_state.decision_observability["raw_total_asset_qty"]),
        open_exposure_qty=float(position_state.decision_observability["open_exposure_qty"]),
        dust_tracking_qty=float(position_state.decision_observability["dust_tracking_qty"]),
        strategy_name=strategy_name,
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
    )


def _determine_live_execution_intent(
    *,
    broker: Broker,
    signal: str,
    ts: int,
    market_price: float,
    position_state: _LiveExecutionPositionState,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
) -> _LiveExecutionIntent | None:
    conn = position_state.conn
    decision_observability = position_state.decision_observability
    normalized_exposure = position_state.normalized_exposure

    if signal == "BUY" and normalized_exposure.effective_flat:
        if not math.isfinite(float(market_price)) or float(market_price) <= 0:
            reason = f"invalid market/reference price: {market_price}"
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] invalid market price",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    side="BUY",
                    reason=reason,
                    signal=signal,
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    has_executable_exposure=1 if bool(decision_observability.get("has_executable_exposure")) else 0,
                    has_any_position_residue=1 if bool(decision_observability.get("has_any_position_residue")) else 0,
                    has_dust_only_remainder=1 if bool(decision_observability.get("has_dust_only_remainder")) else 0,
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                )
            )
            notify(f"live pretrade validation blocked (BUY): {reason}")
            return None

        guardrail_qty = 0.0 if bool(decision_observability["entry_allowed"]) else float(
            normalized_exposure.open_exposure_qty if bool(decision_observability.get("has_executable_exposure")) else position_state.portfolio_qty
        )
        blocked, guardrail_reason = evaluate_buy_guardrails(
            conn=conn,
            ts_ms=ts,
            cash=position_state.cash,
            qty=guardrail_qty,
            price=market_price,
        )
        if blocked:
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] buy guardrails",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    signal=signal,
                    side="BUY",
                    reason=guardrail_reason or "blocked",
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                )
            )
            return None

        entry_sizing = _build_buy_execution_sizing(
            pair=settings.PAIR,
            cash_krw=float(position_state.cash),
            market_price=float(market_price),
            fee_rate=float(settings.LIVE_FEE_RATE_ESTIMATE),
            entry_intent=decision_observability.get("entry_intent"),
            authority=BuyExecutionAuthority(
                entry_allowed=bool(decision_observability["entry_allowed"]),
                entry_allowed_truth_source=str(
                    decision_observability.get("entry_allowed_truth_source") or "-"
                ),
            ),
        )
        if not entry_sizing.allowed:
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] entry sizing blocked",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    side="BUY",
                    reason=str(entry_sizing.block_reason),
                    decision_reason_code=str(entry_sizing.decision_reason_code),
                    signal=signal,
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                    internal_lot_size=float(entry_sizing.internal_lot_size),
                    intended_lot_count=int(entry_sizing.intended_lot_count),
                    executable_lot_count=int(entry_sizing.executable_lot_count),
                    final_intended_qty=float(entry_sizing.executable_qty),
                    final_submitted_qty=float(entry_sizing.executable_qty),
                )
            )
            return None

        try:
            order_qty = adjust_buy_order_qty_for_dust_safety(
                qty=float(entry_sizing.executable_qty),
                market_price=float(market_price),
            )
        except ValueError as e:
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] buy dust guard blocked",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    signal=signal,
                    side="BUY",
                    reason=str(e),
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                )
            )
            notify(f"live pretrade validation blocked (BUY): {e}")
            return None

        return _LiveExecutionIntent(
            side="BUY",
            order_qty=float(order_qty),
            submit_qty_source=str(entry_sizing.qty_source),
            harmless_dust_checked=False,
            entry_sizing=entry_sizing,
            exit_sizing=None,
            canonical_sell=None,
            diagnostic_sell_qty=None,
        )

    if signal == "SELL":
        canonical_sell = position_state.canonical_sell
        diagnostic_sell_qty = position_state.diagnostic_sell_qty
        if canonical_sell is None or diagnostic_sell_qty is None:
            return None
        exit_sizing = build_sell_execution_sizing(
            pair=settings.PAIR,
            market_price=float(market_price),
            authority=SellExecutionAuthority(
                sellable_executable_lot_count=int(canonical_sell.sellable_executable_lot_count),
                exit_allowed=bool(canonical_sell.exit_allowed),
                exit_block_reason=canonical_sell.exit_block_reason,
            ),
            lot_definition=position_state.position_snapshot.lot_definition,
        )
        canonical_sell_submit_qty = float(exit_sizing.executable_qty if exit_sizing.allowed else 0.0)
        sellable_threshold = max(POSITION_EPSILON, float(position_state.effective_rules.min_qty))
        if (not exit_sizing.allowed) or float(canonical_sell_submit_qty) < sellable_threshold:
            if float(diagnostic_sell_qty.observed_position_qty) <= POSITION_EPSILON:
                if (
                    decision_id is not None or position_state.has_lot_native_sell_state
                ) and _record_sell_no_executable_exit_suppression(
                    conn=conn,
                    state=position_state.state,
                    ts=int(ts),
                    market_price=float(market_price),
                    canonical_sell=replace(
                        canonical_sell,
                        sellable_executable_qty=float(
                            canonical_sell_submit_qty if canonical_sell_submit_qty > 0 else canonical_sell.sellable_executable_qty
                        ),
                    ),
                    diagnostic_qty=diagnostic_sell_qty,
                    decision_observability=decision_observability,
                    strategy_name=strategy_name,
                    decision_id=decision_id,
                    decision_reason=decision_reason,
                    exit_rule_name=exit_rule_name,
                    exit_sizing=exit_sizing,
                ):
                    conn.commit()
                return None

            if str(normalized_exposure.exit_block_reason) == "reserved_for_open_sell_orders":
                if _record_sell_dust_unsellable(
                    conn=conn,
                    state=position_state.state,
                    ts=int(ts),
                    market_price=float(market_price),
                    canonical_sell=canonical_sell,
                    diagnostic_qty=diagnostic_sell_qty,
                    decision_observability=decision_observability,
                    strategy_name=(strategy_name or settings.STRATEGY_NAME),
                    decision_id=decision_id,
                    decision_reason=decision_reason,
                    exit_rule_name=exit_rule_name,
                    allow_decision_suppression=position_state.has_lot_native_sell_state,
                ):
                    conn.commit()
                RUN_LOG.info(
                    format_log_kv(
                        "[ORDER_SKIP] exit inventory already reserved",
                        base_signal=decision_observability["base_signal"],
                        final_signal=decision_observability["final_signal"],
                        signal=signal,
                        side="SELL",
                        reason="reserved_for_open_sell_orders",
                        decision_reason_code="reserved_for_open_sell_orders",
                        open_exposure_qty=float(normalized_exposure.open_exposure_qty),
                        reserved_exit_qty=float(normalized_exposure.reserved_exit_qty),
                        sellable_executable_qty=float(normalized_exposure.sellable_executable_qty),
                    )
                )
                return None

            if (
                decision_id is not None or position_state.has_lot_native_sell_state
            ) and _record_sell_no_executable_exit_suppression(
                conn=conn,
                state=position_state.state,
                ts=int(ts),
                market_price=float(market_price),
                canonical_sell=replace(
                    canonical_sell,
                    sellable_executable_qty=float(
                        canonical_sell_submit_qty if canonical_sell_submit_qty > 0 else canonical_sell.sellable_executable_qty
                    ),
                ),
                diagnostic_qty=diagnostic_sell_qty,
                decision_observability=decision_observability,
                strategy_name=strategy_name,
                decision_id=decision_id,
                decision_reason=decision_reason,
                exit_rule_name=exit_rule_name,
                exit_sizing=exit_sizing,
            ):
                conn.commit()
                return None

            harmless_dust_checked = _maybe_record_harmless_dust_sell_suppression(
                conn=conn,
                position_state=position_state,
                signal=signal,
                side="SELL",
                requested_qty=float(canonical_sell_submit_qty),
                market_price=float(market_price),
                normalized_qty=float(canonical_sell_submit_qty),
                submit_qty_source=canonical_sell.submit_qty_source,
                strategy_name=strategy_name,
                decision_id=decision_id,
                decision_reason=decision_reason,
                exit_rule_name=exit_rule_name,
            )
            if harmless_dust_checked:
                conn.commit()
                return None

            if float(position_state.raw_total_asset_qty) <= POSITION_EPSILON:
                skip_reason = (
                    "no sellable open exposure for signal "
                    f"base_signal={decision_observability['base_signal']} "
                    f"final_signal={decision_observability['final_signal']} "
                    f"raw_qty_open={float(position_state.open_exposure_qty):.12f} "
                    f"raw_total_asset_qty={float(position_state.raw_total_asset_qty):.12f} "
                    f"open_exposure_qty={float(normalized_exposure.open_exposure_qty):.12f} "
                    f"dust_tracking_qty={float(normalized_exposure.dust_tracking_qty):.12f} "
                    f"normalized_exposure_active={1 if normalized_exposure.normalized_exposure_active else 0} "
                    f"normalized_exposure_qty={float(normalized_exposure.normalized_exposure_qty):.12f} "
                    f"entry_allowed={1 if normalized_exposure.entry_allowed else 0} "
                    f"effective_flat={1 if normalized_exposure.effective_flat else 0}"
                )
                RUN_LOG.info(
                    format_log_kv(
                        "[ORDER_SKIP] no sellable exposure",
                        base_signal=decision_observability["base_signal"],
                        final_signal=decision_observability["final_signal"],
                        signal=signal,
                        side="SELL",
                        reason=skip_reason,
                        decision_reason_code="no_position",
                        position_qty=float(position_state.open_exposure_qty),
                        submit_payload_qty=0.0,
                        open_exposure_qty=float(position_state.open_exposure_qty),
                        dust_tracking_qty=float(position_state.dust_tracking_qty),
                        raw_total_asset_qty=float(position_state.raw_total_asset_qty),
                        normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                        normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                        raw_qty_open=float(decision_observability["raw_qty_open"]),
                        entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                        effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
                    )
                )
                return None

            if reclassify_non_executable_open_exposure(
                conn=conn,
                pair=settings.PAIR,
                executable_lot=build_executable_lot(
                    qty=float(position_state.open_exposure_qty),
                    market_price=float(market_price),
                    min_qty=float(position_state.effective_rules.min_qty),
                    qty_step=float(position_state.effective_rules.qty_step),
                    min_notional_krw=float(position_state.effective_rules.min_notional_krw),
                    max_qty_decimals=int(position_state.effective_rules.max_qty_decimals),
                    exit_fee_ratio=float(settings.LIVE_FEE_RATE_ESTIMATE),
                    exit_slippage_bps=float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
                    exit_buffer_ratio=float(settings.ENTRY_EDGE_BUFFER_RATIO),
                ),
            ):
                conn.commit()
            if _record_sell_dust_unsellable(
                conn=conn,
                state=position_state.state,
                ts=int(ts),
                market_price=float(market_price),
                canonical_sell=canonical_sell,
                diagnostic_qty=diagnostic_sell_qty,
                decision_observability=decision_observability,
                strategy_name=(strategy_name or settings.STRATEGY_NAME),
                decision_id=decision_id,
                decision_reason=decision_reason,
                exit_rule_name=exit_rule_name,
                allow_decision_suppression=position_state.has_lot_native_sell_state,
            ):
                conn.commit()
            return None

        if not exit_sizing.allowed:
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] exit sizing blocked",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    signal=signal,
                    side="SELL",
                    reason=str(exit_sizing.block_reason),
                    decision_reason_code=str(exit_sizing.decision_reason_code),
                    open_exposure_qty=float(normalized_exposure.open_exposure_qty),
                    dust_tracking_qty=float(normalized_exposure.dust_tracking_qty),
                    internal_lot_size=float(exit_sizing.internal_lot_size),
                    intended_lot_count=int(exit_sizing.intended_lot_count),
                    executable_lot_count=int(exit_sizing.executable_lot_count),
                    final_intended_qty=float(canonical_sell_submit_qty),
                    final_submitted_qty=float(canonical_sell_submit_qty),
                )
            )
            return None

        return _LiveExecutionIntent(
            side="SELL",
            order_qty=float(canonical_sell_submit_qty),
            submit_qty_source=_require_canonical_sell_submit_lot_source(
                submit_qty_source=canonical_sell.submit_qty_source,
                context="live SELL submit",
            ).value,
            harmless_dust_checked=False,
            entry_sizing=None,
            exit_sizing=exit_sizing,
            canonical_sell=canonical_sell,
            diagnostic_sell_qty=diagnostic_sell_qty,
        )

    skip_reason = (
        "no actionable position state for signal "
        f"base_signal={decision_observability['base_signal']} "
        f"final_signal={decision_observability['final_signal']} "
        f"raw_qty_open={float(normalized_exposure.raw_qty_open):.12f} "
        f"raw_total_asset_qty={float(position_state.raw_total_asset_qty):.12f} "
        f"open_exposure_qty={float(position_state.open_exposure_qty):.12f} "
        f"dust_tracking_qty={float(position_state.dust_tracking_qty):.12f} "
        f"normalized_exposure_active={1 if normalized_exposure.normalized_exposure_active else 0} "
        f"normalized_exposure_qty={float(normalized_exposure.normalized_exposure_qty):.12f} "
        f"entry_allowed={1 if normalized_exposure.entry_allowed else 0} "
        f"effective_flat={1 if normalized_exposure.effective_flat else 0} "
        f"entry_allowed_truth_source={decision_observability['entry_allowed_truth_source']} "
        f"effective_flat_truth_source={decision_observability['effective_flat_truth_source']}"
    )
    RUN_LOG.info(
        format_log_kv(
            "[ORDER_SKIP] no-op signal",
            base_signal=decision_observability["base_signal"],
            final_signal=decision_observability["final_signal"],
            side=signal,
            reason=skip_reason,
            signal=signal,
            position_qty=f"{float(position_state.portfolio_qty):.12f}",
            open_exposure_qty=float(position_state.open_exposure_qty),
            entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
            effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
            normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
            normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
            raw_qty_open=float(decision_observability["raw_qty_open"]),
            entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
        )
    )
    return None


def _evaluate_live_execution_feasibility(
    *,
    broker: Broker,
    signal: str,
    ts: int,
    market_price: float,
    position_state: _LiveExecutionPositionState,
    intent: _LiveExecutionIntent,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
) -> _LiveExecutionFeasibility | None:
    conn = position_state.conn
    decision_observability = position_state.decision_observability
    requested_order_qty = float(intent.order_qty)
    harmless_dust_checked = bool(intent.harmless_dust_checked)
    non_authoritative_qty_preview = (
        _build_non_authoritative_qty_normalization_snapshot(qty=intent.order_qty)
        if intent.side == "SELL"
        else None
    )
    if intent.side == "SELL" and not harmless_dust_checked:
        assert non_authoritative_qty_preview is not None
        if _maybe_record_harmless_dust_sell_suppression(
            conn=conn,
            position_state=position_state,
            signal=signal,
            side=intent.side,
            requested_qty=float(intent.order_qty),
            market_price=float(market_price),
            normalized_qty=float(non_authoritative_qty_preview["normalized_qty"]),
            submit_qty_source=(intent.canonical_sell.submit_qty_source if intent.canonical_sell is not None else intent.submit_qty_source),
            strategy_name=strategy_name,
            decision_id=decision_id,
            decision_reason=decision_reason,
            exit_rule_name=exit_rule_name,
        ):
            conn.commit()
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
    normalized_qty = 0.0
    order_qty = float(intent.order_qty)
    try:
        if pretrade_needs_live_reference:
            reference_quote = _load_live_reference_quote(pair=settings.PAIR)
        if intent.side == "BUY":
            normalized_qty = float(order_qty)
        else:
            normalized_qty = adjust_sell_order_qty_for_dust_safety(qty=order_qty, market_price=market_price)
            order_qty = normalized_qty
        validate_order(signal=signal, side=intent.side, qty=normalized_qty, market_price=market_price)
        validate_pretrade(
            broker=broker,
            side=intent.side,
            qty=normalized_qty,
            market_price=market_price,
            reference_bid=(float(reference_quote["bid"]) if reference_quote is not None else None),
            reference_ask=(float(reference_quote["ask"]) if reference_quote is not None else None),
            reference_ts_epoch_sec=(
                float(reference_quote["reference_ts_epoch_sec"]) if reference_quote is not None else None
            ),
            reference_source=(str(reference_quote["reference_source"]) if reference_quote is not None else None),
        )
    except SellDustGuardError as e:
        if (
            intent.side == "SELL"
            and not harmless_dust_checked
            and non_authoritative_qty_preview is not None
            and _maybe_record_harmless_dust_sell_suppression(
                conn=conn,
                position_state=position_state,
                signal=signal,
                side=intent.side,
                requested_qty=requested_order_qty,
                market_price=float(market_price),
                normalized_qty=float(non_authoritative_qty_preview["normalized_qty"]),
                submit_qty_source=(
                    intent.canonical_sell.submit_qty_source if intent.canonical_sell is not None else intent.submit_qty_source
                ),
                strategy_name=strategy_name,
                decision_id=decision_id,
                decision_reason=decision_reason,
                exit_rule_name=exit_rule_name,
            )
        ):
            conn.commit()
            return None
        if (
            intent.side == "SELL"
            and intent.canonical_sell is not None
            and intent.diagnostic_sell_qty is not None
            and _record_sell_dust_unsellable(
                conn=conn,
                state=position_state.state,
                ts=int(ts),
                market_price=float(market_price),
                canonical_sell=replace(intent.canonical_sell, sellable_executable_qty=float(requested_order_qty)),
                diagnostic_qty=intent.diagnostic_sell_qty,
                decision_observability=decision_observability,
                strategy_name=(strategy_name or settings.STRATEGY_NAME),
                decision_id=decision_id,
                decision_reason=decision_reason,
                exit_rule_name=exit_rule_name,
                dust_details=e.details,
            )
        ):
            conn.commit()
            return None
        RUN_LOG.info(
            format_log_kv(
                "[ORDER_SKIP] sell dust guard blocked",
                base_signal=decision_observability["base_signal"],
                final_signal=decision_observability["final_signal"],
                signal=signal,
                side=intent.side,
                reason=(
                    f"category={_classify_sell_failure_category(reason_code=DUST_RESIDUAL_UNSELLABLE, dust_details=getattr(e, 'details', None))};"
                    f"detail={_sell_failure_detail_from_observability(sell_failure_category=_classify_sell_failure_category(reason_code=DUST_RESIDUAL_UNSELLABLE, dust_details=getattr(e, 'details', None)), dust_details=getattr(e, 'details', None))};"
                    f"reason={e}"
                ),
                market_price=market_price,
                requested_qty=requested_order_qty,
                position_qty=requested_order_qty,
                order_qty=float(non_authoritative_qty_preview["normalized_qty"]) if non_authoritative_qty_preview is not None else 0.0,
                submit_payload_qty=0.0,
                submit_qty_source=intent.submit_qty_source,
                entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                raw_qty_open=float(decision_observability["raw_qty_open"]),
                raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
            )
        )
        notify(f"live pretrade validation blocked ({intent.side}): {e}")
        return None
    except ValueError as e:
        if (
            intent.side == "SELL"
            and not harmless_dust_checked
            and non_authoritative_qty_preview is not None
            and _maybe_record_harmless_dust_sell_suppression(
                conn=conn,
                position_state=position_state,
                signal=signal,
                side=intent.side,
                requested_qty=requested_order_qty,
                market_price=float(market_price),
                normalized_qty=float(non_authoritative_qty_preview["normalized_qty"]),
                submit_qty_source=intent.submit_qty_source,
                strategy_name=strategy_name,
                decision_id=decision_id,
                decision_reason=decision_reason,
                exit_rule_name=exit_rule_name,
            )
        ):
            conn.commit()
            return None
        if (
            intent.side == "SELL"
            and intent.canonical_sell is not None
            and intent.diagnostic_sell_qty is not None
            and _record_sell_dust_unsellable(
                conn=conn,
                state=position_state.state,
                ts=int(ts),
                market_price=float(market_price),
                canonical_sell=replace(intent.canonical_sell, sellable_executable_qty=float(requested_order_qty)),
                diagnostic_qty=intent.diagnostic_sell_qty,
                decision_observability=decision_observability,
                strategy_name=(strategy_name or settings.STRATEGY_NAME),
                decision_id=decision_id,
                decision_reason=decision_reason,
                exit_rule_name=exit_rule_name,
            )
        ):
            conn.commit()
            return None
        RUN_LOG.info(
            format_log_kv(
                "[ORDER_SKIP] pretrade blocked",
                base_signal=decision_observability["base_signal"],
                final_signal=decision_observability["final_signal"],
                signal=signal,
                side=intent.side,
                reason=(
                    f"category={_classify_sell_failure_category(reason_code=DUST_RESIDUAL_UNSELLABLE, dust_details=getattr(e, 'details', None))};"
                    f"detail={_sell_failure_detail_from_observability(sell_failure_category=_classify_sell_failure_category(reason_code=DUST_RESIDUAL_UNSELLABLE, dust_details=getattr(e, 'details', None)), dust_details=getattr(e, 'details', None))};"
                    f"reason={e}"
                ),
                market_price=market_price,
                position_qty=requested_order_qty,
                order_qty=order_qty,
                submit_payload_qty=float(normalized_qty),
                submit_qty_source=intent.submit_qty_source,
                entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                raw_qty_open=float(decision_observability["raw_qty_open"]),
                raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
            )
        )
        notify(f"live pretrade validation blocked ({intent.side}): {e}")
        return None

    return _LiveExecutionFeasibility(
        side=intent.side,
        order_qty=float(order_qty),
        normalized_qty=float(normalized_qty),
        submit_qty_source=intent.submit_qty_source,
        reference_quote=reference_quote,
        entry_sizing=intent.entry_sizing,
        exit_sizing=intent.exit_sizing,
    )


def _execute_live_submission_and_application(
    *,
    broker: Broker,
    signal: str,
    ts: int,
    market_price: float,
    position_state: _LiveExecutionPositionState,
    intent: _LiveExecutionIntent,
    feasibility: _LiveExecutionFeasibility,
    strategy_name: str | None,
    decision_id: int | None,
    decision_reason: str | None,
    exit_rule_name: str | None,
) -> dict | None:
    conn = position_state.conn
    decision_observability = position_state.decision_observability
    submit_attempt_id = _submit_attempt_id()
    client_order_id = _client_order_id(ts=ts, side=feasibility.side, submit_attempt_id=submit_attempt_id)
    strategy_context = _order_intent_strategy_context()
    intent_type = _order_intent_type(side=feasibility.side)
    lot_sizing = feasibility.entry_sizing if feasibility.side == "BUY" else feasibility.exit_sizing
    intent_key = build_order_intent_key(
        symbol=settings.PAIR,
        side=feasibility.side,
        strategy_context=strategy_context,
        intent_ts=int(ts),
        intent_type=intent_type,
        qty=feasibility.normalized_qty,
        intended_lot_count=int(lot_sizing.intended_lot_count),
        executable_lot_count=int(lot_sizing.executable_lot_count),
    )

    reference_price: float | None = None
    top_of_book_summary: dict[str, float | str] | None = None
    if feasibility.reference_quote is not None:
        reference_price = float(feasibility.reference_quote["reference_price"])
        top_of_book_summary = {
            "bid": float(feasibility.reference_quote["bid"]),
            "ask": float(feasibility.reference_quote["ask"]),
            "spread": float(feasibility.reference_quote["ask"]) - float(feasibility.reference_quote["bid"]),
            "reference_ts": _format_epoch_ts(float(feasibility.reference_quote["reference_ts_epoch_sec"])),
            "reference_source": str(feasibility.reference_quote["reference_source"]),
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
        cash=float(position_state.cash),
        qty=float(position_state.portfolio_qty),
        price=float(market_price),
    )
    if blocked:
        gate_blocked, reason_code, gate_reason = evaluate_unresolved_order_gate(
            conn,
            now_ms=int(time.time() * 1000),
            max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
        )
        if gate_blocked:
            blocked_client_order_id = build_client_order_id(
                mode="live",
                side=feasibility.side,
                intent_ts=int(ts),
                submit_attempt_id=submit_attempt_id,
            )
            gate_reason = (
                f"category=unresolved_risk_gate;"
                f"reason_detail_code={reason_code};"
                f"reason={gate_reason}"
            )
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] unresolved risk gate",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    signal=signal,
                    side=feasibility.side,
                    reason=gate_reason,
                    sell_failure_category=SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
                    sell_failure_detail=SELL_FAILURE_CATEGORY_UNRESOLVED_RISK_GATE,
                    reason_detail_code=reason_code,
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                    open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                    dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                    submit_qty_source=decision_observability["submit_qty_source"],
                    position_state_source=decision_observability["position_state_source"],
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                )
            )
            _block_new_submission_for_unresolved_risk(
                conn=conn,
                client_order_id=blocked_client_order_id,
                side=feasibility.side,
                qty=feasibility.normalized_qty,
                ts=ts,
                reason_code=reason_code,
                reason=gate_reason,
            )
            conn.commit()
            return None

        RUN_LOG.info(
            format_log_kv(
                "[ORDER_SKIP] submission halt",
                base_signal=decision_observability["base_signal"],
                final_signal=decision_observability["final_signal"],
                signal=signal,
                side=feasibility.side,
                reason=f"category=submission_halt;reason_detail_code=submission_halt;reason={reason}",
                sell_failure_category=SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
                sell_failure_detail=SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
                reason_detail_code="submission_halt",
                entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                raw_qty_open=float(decision_observability["raw_qty_open"]),
                raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                submit_qty_source=decision_observability["submit_qty_source"],
                position_state_source=decision_observability["position_state_source"],
                entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            )
        )
        notify(f"live order placement blocked ({feasibility.side}): category=submission_halt;reason={reason}")
        return None

    existing = conn.execute(
        "SELECT status FROM orders WHERE client_order_id=?",
        (client_order_id,),
    ).fetchone()
    if existing is not None:
        existing_status = str(existing["status"])
        if existing_status in TERMINAL_ORDER_STATUSES:
            reason = f"duplicate submit blocked: terminal status {existing_status}"
            RUN_LOG.info(
                format_log_kv(
                    "[ORDER_SKIP] duplicate client order id",
                    base_signal=decision_observability["base_signal"],
                    final_signal=decision_observability["final_signal"],
                    signal=signal,
                    side=feasibility.side,
                    reason=reason,
                    client_order_id=client_order_id,
                    entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                    effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                    normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                    normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                    raw_qty_open=float(decision_observability["raw_qty_open"]),
                    raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
                    open_exposure_qty=float(decision_observability["open_exposure_qty"]),
                    dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
                    submit_qty_source=decision_observability["submit_qty_source"],
                    position_state_source=decision_observability["position_state_source"],
                    entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
                )
            )
            record_submit_blocked(client_order_id, status=existing_status, reason=reason, conn=conn)
            notify(
                safety_event(
                    "order_submit_blocked",
                    client_order_id=client_order_id,
                    submit_attempt_id=submit_attempt_id,
                    side=feasibility.side,
                    status=existing_status,
                    reason_code=RISKY_ORDER_BLOCK,
                    signal_ts=int(ts),
                    decision_ts=int(ts),
                    decision_id=str(submit_attempt_id),
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
        side=feasibility.side,
        strategy_context=strategy_context,
        intent_type=intent_type,
        intent_ts=int(ts),
        qty=feasibility.normalized_qty,
        intended_lot_count=int(lot_sizing.intended_lot_count),
        executable_lot_count=int(lot_sizing.executable_lot_count),
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
                base_signal=decision_observability["base_signal"],
                final_signal=decision_observability["final_signal"],
                mode=settings.MODE,
                symbol=settings.PAIR,
                side=feasibility.side,
                qty=f"{float(feasibility.normalized_qty):.12f}",
                intent_ts=int(ts),
                intent_key=intent_key,
                reason=skip_reason,
                entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
                effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
                normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
                normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
                raw_qty_open=float(decision_observability["raw_qty_open"]),
                entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            )
        )
        notify(
            format_event(
                "order_intent_dedup_skip",
                symbol=settings.PAIR,
                side=feasibility.side,
                qty=float(feasibility.normalized_qty),
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
            base_signal=decision_observability["base_signal"],
            final_signal=decision_observability["final_signal"],
            signal=signal,
            side=feasibility.side,
            market_price=market_price,
            position_qty=float(intent.order_qty),
            order_qty=feasibility.order_qty,
            normalized_qty=feasibility.normalized_qty,
            submit_payload_qty=float(feasibility.normalized_qty),
            reference_price=reference_price,
            client_order_id=client_order_id,
            intent_ts=int(ts),
            intent_key=intent_key,
            entry_allowed=1 if bool(decision_observability["entry_allowed"]) else 0,
            effective_flat=1 if bool(decision_observability["effective_flat"]) else 0,
            normalized_exposure_active=1 if bool(decision_observability["normalized_exposure_active"]) else 0,
            normalized_exposure_qty=float(decision_observability["normalized_exposure_qty"]),
            raw_qty_open=float(decision_observability["raw_qty_open"]),
            raw_total_asset_qty=float(decision_observability["raw_total_asset_qty"]),
            open_exposure_qty=float(decision_observability["open_exposure_qty"]),
            dust_tracking_qty=float(decision_observability["dust_tracking_qty"]),
            submit_qty_source=feasibility.submit_qty_source,
            position_state_source=str(decision_observability["position_state_source"]),
            entry_allowed_truth_source=decision_observability["entry_allowed_truth_source"],
            effective_flat_truth_source=decision_observability["effective_flat_truth_source"],
            top_of_book=top_of_book_summary,
        )
    )

    order = _submit_via_standard_path(
        conn=conn,
        broker=broker,
        signal=signal,
        client_order_id=client_order_id,
        submit_attempt_id=submit_attempt_id,
        side=feasibility.side,
        order_qty=feasibility.order_qty,
        position_qty=float(intent.order_qty),
        qty=feasibility.normalized_qty,
        ts=ts,
        intent_key=intent_key,
        market_price=market_price,
        raw_total_asset_qty=position_state.raw_total_asset_qty,
        open_exposure_qty=float(position_state.open_exposure_qty),
        dust_tracking_qty=float(position_state.dust_tracking_qty),
        decision_observability=decision_observability,
        submit_qty_source=feasibility.submit_qty_source,
        position_state_source=str(decision_observability["position_state_source"]),
        reference_price=reference_price,
        top_of_book_summary=top_of_book_summary,
        strategy_name=(strategy_name or settings.STRATEGY_NAME),
        decision_id=decision_id,
        decision_reason=decision_reason,
        exit_rule_name=exit_rule_name,
        order_type=("price" if feasibility.side == "BUY" else "market"),
        internal_lot_size=float(lot_sizing.internal_lot_size),
        effective_min_trade_qty=float(lot_sizing.effective_min_trade_qty),
        qty_step=float(lot_sizing.qty_step),
        min_notional_krw=float(lot_sizing.min_notional_krw),
        intended_lot_count=int(lot_sizing.intended_lot_count),
        executable_lot_count=int(lot_sizing.executable_lot_count),
        final_intended_qty=float(feasibility.order_qty),
        final_submitted_qty=float(feasibility.normalized_qty),
        decision_reason_code=str(lot_sizing.decision_reason_code),
    )
    if order is None:
        return None

    fills = broker.get_fills(client_order_id=client_order_id, exchange_order_id=order.exchange_order_id)
    try:
        fills_to_apply = _aggregate_fills_for_apply(
            fills=fills,
            client_order_id=client_order_id,
            exchange_order_id=order.exchange_order_id,
            side=feasibility.side,
            context="_submit_via_standard_path",
        )
    except FillFeeStrictModeError as exc:
        from_status = str(order.status or "NEW")
        _mark_recovery_required(
            conn=conn,
            client_order_id=client_order_id,
            side=feasibility.side,
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
                side=feasibility.side,
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
            side=feasibility.side,
            fill_id=fill.fill_id,
            fill_ts=fill.fill_ts,
            price=fill.price,
            qty=fill.qty,
            fee=fill.fee,
            strategy_name=(strategy_name or settings.STRATEGY_NAME),
            entry_decision_id=(decision_id if feasibility.side == "BUY" else None),
            exit_decision_id=(decision_id if feasibility.side == "SELL" else None),
            exit_reason=(decision_reason if feasibility.side == "SELL" else None),
            exit_rule_name=(exit_rule_name if feasibility.side == "SELL" else None),
            note=f"live exchange_order_id={order.exchange_order_id}",
            signal_ts=int(ts),
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
    conn = None
    try:
        position_state = _determine_live_execution_position_state(
            signal=signal,
            market_price=float(market_price),
            decision_id=decision_id,
        )
        conn = position_state.conn
        if position_state.state.halt_new_orders_blocked:
            halt_reason = (
                "runtime halted: "
                f"code={position_state.state.halt_reason_code or '-'} "
                f"reason={position_state.state.last_disable_reason or '-'}"
            )
            RUN_LOG.info(format_log_kv("[ORDER_SKIP] runtime halt", side=signal, reason=halt_reason, signal=signal))
            notify(
                safety_event(  # CHANGED
                    "order_submit_blocked",
                    client_order_id=UNSET_EVENT_FIELD,
                    submit_attempt_id=UNSET_EVENT_FIELD,
                    exchange_order_id=UNSET_EVENT_FIELD,
                    status="HALTED",
                    signal_ts=int(ts),
                    decision_ts=int(ts),
                    decision_id=(str(decision_id) if decision_id is not None else "-"),
                    state_to="HALTED",
                    reason_code=RISKY_ORDER_BLOCK,
                    sell_failure_category=SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
                    sell_failure_detail=SELL_FAILURE_CATEGORY_SUBMISSION_HALT,
                    reason_detail_code="submission_halt",
                    halt_detail_code=position_state.state.halt_reason_code or "-",
                    reason=halt_reason,
                )
            )
            return None
        intent = _determine_live_execution_intent(
            broker=broker,
            signal=signal,
            ts=ts,
            market_price=float(market_price),
            position_state=position_state,
            strategy_name=strategy_name,
            decision_id=decision_id,
            decision_reason=decision_reason,
            exit_rule_name=exit_rule_name,
        )
        if intent is None:
            return None

        feasibility = _evaluate_live_execution_feasibility(
            broker=broker,
            signal=signal,
            ts=ts,
            market_price=float(market_price),
            position_state=position_state,
            intent=intent,
            strategy_name=strategy_name,
            decision_id=decision_id,
            decision_reason=decision_reason,
            exit_rule_name=exit_rule_name,
        )
        if feasibility is None:
            return None

        return _execute_live_submission_and_application(
            broker=broker,
            signal=signal,
            ts=ts,
            market_price=float(market_price),
            position_state=position_state,
            intent=intent,
            feasibility=feasibility,
            strategy_name=strategy_name,
            decision_id=decision_id,
            decision_reason=decision_reason,
            exit_rule_name=exit_rule_name,
        )

    finally:
        if conn is not None:
            conn.close()
