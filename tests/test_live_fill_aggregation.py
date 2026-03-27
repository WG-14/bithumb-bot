from __future__ import annotations

import logging

import pytest

from bithumb_bot.broker.base import BrokerFill
from bithumb_bot.broker.live import FillFeeStrictModeError, _aggregate_fills_for_apply
from bithumb_bot.config import settings


def test_aggregate_fills_sums_qty_and_fee_and_keeps_weighted_price() -> None:
    fills = [
        BrokerFill(
            client_order_id="cid-1",
            fill_id="f1",
            fill_ts=1000,
            price=100.0,
            qty=2.0,
            fee=1.2,
            exchange_order_id="ex-1",
        ),
        BrokerFill(
            client_order_id="cid-1",
            fill_id="f2",
            fill_ts=1010,
            price=110.0,
            qty=3.0,
            fee=1.8,
            exchange_order_id="ex-1",
        ),
    ]

    aggregated = _aggregate_fills_for_apply(
        fills=fills,
        client_order_id="cid-1",
        exchange_order_id="ex-1",
        side="BUY",
        context="test",
    )

    assert len(aggregated) == 1
    agg = aggregated[0]
    assert agg.fill_id == "ex-1:aggregate:1010"
    assert agg.fill_ts == 1010
    assert agg.qty == pytest.approx(5.0)
    assert agg.fee == pytest.approx(3.0)
    assert agg.price == pytest.approx((100.0 * 2.0 + 110.0 * 3.0) / 5.0)


def test_aggregate_fills_warns_when_fee_missing_or_invalid(caplog: pytest.LogCaptureFixture) -> None:
    original_alert_min_notional = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    original_strict_mode = settings.LIVE_FILL_FEE_STRICT_MODE
    original_strict_min_notional = settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 5_000_000.0)

    fills = [
        BrokerFill(
            client_order_id="cid-2",
            fill_id="f1",
            fill_ts=1000,
            price=100.0,
            qty=1.0,
            fee=0.5,
            exchange_order_id="ex-2",
        ),
        BrokerFill(
            client_order_id="cid-2",
            fill_id="f2",
            fill_ts=1010,
            price=110.0,
            qty=1.0,
            fee=float("nan"),
            exchange_order_id="ex-2",
        ),
        BrokerFill(
            client_order_id="cid-2",
            fill_id="f3",
            fill_ts=1020,
            price=120.0,
            qty=1.0,
            fee=-1.0,
            exchange_order_id="ex-2",
        ),
    ]

    try:
        with caplog.at_level(logging.WARNING, logger="bithumb_bot.run"):
            aggregated = _aggregate_fills_for_apply(
                fills=fills,
                client_order_id="cid-2",
                exchange_order_id="ex-2",
                side="SELL",
                context="test",
            )
    finally:
        object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", original_alert_min_notional)
        object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", original_strict_mode)
        object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", original_strict_min_notional)

    assert len(aggregated) == 1
    assert aggregated[0].fee == pytest.approx(0.5)
    warning_messages = [record.getMessage() for record in caplog.records]
    assert any("missing_or_invalid fill fee" in msg for msg in warning_messages)
    assert not any("[FILL_AGG_HARD_ALERT]" in msg for msg in warning_messages)


def test_aggregate_fills_high_notional_invalid_fee_emits_hard_alert_and_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    original_alert_min_notional = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    original_strict_mode = settings.LIVE_FILL_FEE_STRICT_MODE
    original_strict_min_notional = settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 5_000_000.0)

    fills = [
        BrokerFill(
            client_order_id="cid-4",
            fill_id="f1",
            fill_ts=1000,
            price=100000000.0,
            qty=0.01,
            fee=5.0,
            exchange_order_id="ex-4",
        ),
        BrokerFill(
            client_order_id="cid-4",
            fill_id="f2",
            fill_ts=1010,
            price=100000000.0,
            qty=0.01,
            fee=float("nan"),
            exchange_order_id="ex-4",
        ),
    ]

    try:
        with caplog.at_level(logging.WARNING, logger="bithumb_bot.run"):
            aggregated = _aggregate_fills_for_apply(
                fills=fills,
                client_order_id="cid-4",
                exchange_order_id="ex-4",
                side="BUY",
                context="test",
            )
    finally:
        object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", original_alert_min_notional)
        object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", original_strict_mode)
        object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", original_strict_min_notional)

    assert len(aggregated) == 1
    assert aggregated[0].qty == pytest.approx(0.02)
    messages = [record.getMessage() for record in caplog.records]
    assert any("[FILL_AGG_HARD_ALERT]" in msg for msg in messages)


def test_aggregate_fills_strict_mode_blocks_high_notional_invalid_fee() -> None:
    original_alert_min_notional = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    original_strict_mode = settings.LIVE_FILL_FEE_STRICT_MODE
    original_strict_min_notional = settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 10_000.0)

    fills = [
        BrokerFill(
            client_order_id="cid-5",
            fill_id="f1",
            fill_ts=1000,
            price=100000000.0,
            qty=0.01,
            fee=10.0,
            exchange_order_id="ex-5",
        ),
        BrokerFill(
            client_order_id="cid-5",
            fill_id="f2",
            fill_ts=1010,
            price=100000000.0,
            qty=0.01,
            fee=-1.0,
            exchange_order_id="ex-5",
        ),
    ]

    try:
        with pytest.raises(FillFeeStrictModeError, match="strict fee validation blocked fill aggregation"):
            _aggregate_fills_for_apply(
                fills=fills,
                client_order_id="cid-5",
                exchange_order_id="ex-5",
                side="BUY",
                context="test",
            )
    finally:
        object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", original_alert_min_notional)
        object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", original_strict_mode)
        object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", original_strict_min_notional)


def test_aggregate_fills_returns_empty_when_no_valid_fills(caplog: pytest.LogCaptureFixture) -> None:
    fills = [
        BrokerFill(
            client_order_id="cid-3",
            fill_id="f-bad-qty",
            fill_ts=1000,
            price=100.0,
            qty=0.0,
            fee=0.1,
            exchange_order_id="ex-3",
        ),
        BrokerFill(
            client_order_id="cid-3",
            fill_id="f-bad-price",
            fill_ts=1010,
            price=0.0,
            qty=1.0,
            fee=0.1,
            exchange_order_id="ex-3",
        ),
    ]

    with caplog.at_level(logging.WARNING, logger="bithumb_bot.run"):
        aggregated = _aggregate_fills_for_apply(
            fills=fills,
            client_order_id="cid-3",
            exchange_order_id="ex-3",
            side="BUY",
            context="test",
        )

    assert aggregated == []
    warning_messages = [record.getMessage() for record in caplog.records]
    assert any("aggregate failed: no valid fills" in msg for msg in warning_messages)
