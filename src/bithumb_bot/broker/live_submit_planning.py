from __future__ import annotations

from ..config import settings
from ..execution_models import OrderIntent, SubmitPlan
from .base import Broker
from .order_rules import build_buy_price_none_submit_contract
from .order_submit import plan_place_order


def build_live_submit_plan(
    *,
    broker: Broker,
    client_order_id: str,
    side: str,
    qty: float,
    ts: int,
    effective_rules,
    reference_price: float | None,
) -> SubmitPlan:
    explicit_submit_contract = (
        build_buy_price_none_submit_contract(rules=effective_rules)
        if side == "BUY"
        else None
    )
    return plan_place_order(
        broker,
        intent=OrderIntent(
            client_order_id=client_order_id,
            market=settings.PAIR,
            side=side,
            normalized_side=("bid" if side == "BUY" else "ask"),
            qty=float(qty),
            price=None,
            created_ts=int(ts),
            submit_contract=explicit_submit_contract,
            market_price_hint=reference_price,
            trace_id=client_order_id,
        ),
        rules=effective_rules,
        skip_qty_revalidation=(side == "SELL"),
    )
