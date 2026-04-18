from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SubmitPriceTickPolicy:
    applies: bool
    price_unit: float
    reason: str


@dataclass(frozen=True)
class OrderIntent:
    client_order_id: str
    market: str
    side: str
    normalized_side: str
    qty: float
    price: float | None
    created_ts: int
    submit_contract: Any | None = None
    market_price_hint: float | None = None
    trace_id: str | None = None

    @property
    def order_side(self) -> str:
        return "BUY" if self.normalized_side == "bid" else "SELL"


@dataclass(frozen=True)
class SubmitPlan:
    intent: OrderIntent
    rules: Any
    chance_validation_order_type: str
    chance_supported_order_types: tuple[str, ...]
    exchange_submit_field: str
    exchange_order_type: str
    exchange_submit_price: float | None
    exchange_submit_volume: float | None
    exchange_submit_notional_krw: float | None
    submit_contract_context: dict[str, object]
    submit_price_tick_policy: SubmitPriceTickPolicy
    effective_market_price: float | None
    lot_rules: Any
    qty_split: Any
    internal_lot_qty: float
    exchange_submit_qty: float
    buy_price_none_submit_contract: Any | None = None
    trace_id: str | None = None
    plan_id: str | None = None
    phase_identity: str = "planning"
    phase_result: str = "planned"


@dataclass(frozen=True)
class SignedOrderRequest:
    intent: OrderIntent
    plan: SubmitPlan
    payload: dict[str, object]
    submit_contract_context: dict[str, object]
    exchange_submit_field: str
    exchange_submit_notional_krw: float | None
    exchange_submit_qty: float
    internal_lot_qty: float
    canonical_payload: str
    trace_id: str | None = None
    plan_id: str | None = None
    request_id: str | None = None
    phase_identity: str = "signed_request"
    phase_result: str = "signed"


@dataclass(frozen=True)
class SubmissionRecord:
    intent: OrderIntent
    plan: SubmitPlan
    signed_request: SignedOrderRequest
    request_ts: int
    retry_safe: bool = False
    trace_id: str | None = None
    plan_id: str | None = None
    request_id: str | None = None
    submission_id: str | None = None
    phase_identity: str = "submission"
    phase_result: str = "submitted"


@dataclass(frozen=True)
class OrderConfirmation:
    submission: SubmissionRecord
    client_order_id: str
    exchange_order_id: str
    side: str
    status: str
    price: float | None
    qty: float
    filled_qty: float
    created_ts: int
    updated_ts: int
    raw: dict[str, object]
    submit_contract_context: dict[str, object]
    trace_id: str | None = None
    plan_id: str | None = None
    request_id: str | None = None
    submission_id: str | None = None
    confirmation_id: str | None = None
    phase_identity: str = "confirmation"
    phase_result: str = "confirmed"
