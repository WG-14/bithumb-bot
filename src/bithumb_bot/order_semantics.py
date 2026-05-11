from __future__ import annotations

from dataclasses import dataclass


CANONICAL_MARKET_BUY_QUOTE_NOTIONAL = "market_buy_quote_notional"
CANONICAL_MARKET_SELL_BASE_QTY = "market_sell_base_qty"
CANONICAL_MARKET_BASE_QTY = "market_base_qty"
CANONICAL_LIMIT_QTY_PRICE = "limit_qty_price"
CANONICAL_LEGACY_UNKNOWN = "legacy_unknown"
CANONICAL_UNSUPPORTED_UNKNOWN = "unsupported_unknown"


@dataclass(frozen=True)
class OrderSemantics:
    raw_order_type: str | None
    side: str | None
    exchange: str | None
    submit_contract_kind: str | None
    canonical_execution_kind: str
    market_equivalent: bool
    limit_equivalent: bool
    legacy_unknown: bool
    unsupported_unknown: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "raw_order_type": self.raw_order_type,
            "side": self.side,
            "exchange": self.exchange,
            "submit_contract_kind": self.submit_contract_kind,
            "canonical_execution_kind": self.canonical_execution_kind,
            "market_equivalent": self.market_equivalent,
            "limit_equivalent": self.limit_equivalent,
            "legacy_unknown": self.legacy_unknown,
            "unsupported_unknown": self.unsupported_unknown,
        }


def classify_order_semantics(
    *,
    raw_order_type: object,
    side: object,
    exchange: object = None,
    submit_contract_kind: object = None,
) -> OrderSemantics:
    raw_text = None if raw_order_type is None else str(raw_order_type).strip()
    order_type = (raw_text or "").lower()
    normalized_side = str(side or "").strip().upper() or None
    exchange_text = str(exchange or "").strip().lower() or None
    contract_text = str(submit_contract_kind or "").strip().lower() or None

    if not order_type:
        return OrderSemantics(
            raw_order_type=None,
            side=normalized_side,
            exchange=exchange_text,
            submit_contract_kind=contract_text,
            canonical_execution_kind=CANONICAL_LEGACY_UNKNOWN,
            market_equivalent=False,
            limit_equivalent=False,
            legacy_unknown=True,
            unsupported_unknown=False,
        )

    if order_type == "price" and normalized_side == "BUY":
        return OrderSemantics(
            raw_order_type=raw_text,
            side=normalized_side,
            exchange=exchange_text,
            submit_contract_kind=contract_text,
            canonical_execution_kind=CANONICAL_MARKET_BUY_QUOTE_NOTIONAL,
            market_equivalent=True,
            limit_equivalent=False,
            legacy_unknown=False,
            unsupported_unknown=False,
        )

    if order_type == "market" and normalized_side == "SELL":
        return OrderSemantics(
            raw_order_type=raw_text,
            side=normalized_side,
            exchange=exchange_text,
            submit_contract_kind=contract_text,
            canonical_execution_kind=CANONICAL_MARKET_SELL_BASE_QTY,
            market_equivalent=True,
            limit_equivalent=False,
            legacy_unknown=False,
            unsupported_unknown=False,
        )

    if order_type == "market" and normalized_side == "BUY" and exchange_text == "bithumb":
        return OrderSemantics(
            raw_order_type=raw_text,
            side=normalized_side,
            exchange=exchange_text,
            submit_contract_kind=contract_text,
            canonical_execution_kind=CANONICAL_UNSUPPORTED_UNKNOWN,
            market_equivalent=False,
            limit_equivalent=False,
            legacy_unknown=False,
            unsupported_unknown=True,
        )

    if order_type == "market":
        return OrderSemantics(
            raw_order_type=raw_text,
            side=normalized_side,
            exchange=exchange_text,
            submit_contract_kind=contract_text,
            canonical_execution_kind=CANONICAL_MARKET_BASE_QTY,
            market_equivalent=True,
            limit_equivalent=False,
            legacy_unknown=False,
            unsupported_unknown=False,
        )

    if order_type == "limit":
        return OrderSemantics(
            raw_order_type=raw_text,
            side=normalized_side,
            exchange=exchange_text,
            submit_contract_kind=contract_text,
            canonical_execution_kind=CANONICAL_LIMIT_QTY_PRICE,
            market_equivalent=False,
            limit_equivalent=True,
            legacy_unknown=False,
            unsupported_unknown=False,
        )

    return OrderSemantics(
        raw_order_type=raw_text,
        side=normalized_side,
        exchange=exchange_text,
        submit_contract_kind=contract_text,
        canonical_execution_kind=CANONICAL_UNSUPPORTED_UNKNOWN,
        market_equivalent=False,
        limit_equivalent=False,
        legacy_unknown=False,
        unsupported_unknown=True,
    )
