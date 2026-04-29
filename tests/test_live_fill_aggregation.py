from __future__ import annotations

import logging

import pytest

from bithumb_bot.broker.base import BrokerFill
from bithumb_bot.broker.live import FillFeeStrictModeError, _aggregate_fills_for_apply
from bithumb_bot.broker.live_submission_execution import _fill_accounting_status
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


def test_aggregate_fills_does_not_finalize_when_any_component_fee_is_pending() -> None:
    fills = [
        BrokerFill(
            client_order_id="cid-pending",
            fill_id="f-complete",
            fill_ts=1000,
            price=114304000.0,
            qty=0.00055623,
            fee=25.43,
            exchange_order_id="ex-pending",
            fee_status="validated_order_level_paid_fee_allocated",
            fee_source="order_level_paid_fee",
            fee_confidence="validated",
            fee_provenance="order_level_paid_fee_validated_allocated",
            fee_validation_reason="order_level_paid_fee_validated_allocated",
        ),
        BrokerFill(
            client_order_id="cid-pending",
            fill_id="f-pending",
            fill_ts=1010,
            price=114325000.0,
            qty=0.00004374,
            fee=0.0,
            exchange_order_id="ex-pending",
            fee_status="assumed_zero_non_material",
            fee_source="missing",
            fee_confidence="ambiguous",
            fee_provenance="missing_fee_field",
            fee_validation_reason="assumed_zero_non_material",
        ),
    ]

    aggregated = _aggregate_fills_for_apply(
        fills=fills,
        client_order_id="cid-pending",
        exchange_order_id="ex-pending",
        side="SELL",
        context="test",
    )

    assert len(aggregated) == 1
    agg = aggregated[0]
    assert agg.fee == pytest.approx(25.43)
    assert agg.fee_status == "assumed_zero_non_material"
    assert agg.fee_source == "mixed_component_fee_evidence"
    assert agg.fee_confidence == "ambiguous"
    assert agg.fee_provenance == "aggregate_contains_pending_component_fee"
    assert agg.fee_validation_reason == "component_fee_pending"
    assert agg.fee_validation_checks["component_fee_pending_count"] == 1
    assert agg.parse_warnings == ("component_fee_pending:f-pending",)


def test_live_application_preserves_fee_pending_status_across_aggregation() -> None:
    aggregated = _aggregate_fills_for_apply(
        fills=[
            BrokerFill(
                client_order_id="cid-live-pending",
                fill_id="f-complete",
                fill_ts=1000,
                price=114304000.0,
                qty=0.00055623,
                fee=25.43,
                exchange_order_id="ex-live-pending",
                fee_status="validated_order_level_paid_fee_allocated",
                fee_source="order_level_paid_fee",
                fee_confidence="validated",
            ),
            BrokerFill(
                client_order_id="cid-live-pending",
                fill_id="f-pending",
                fill_ts=1010,
                price=114325000.0,
                qty=0.00004374,
                fee=0.0,
                exchange_order_id="ex-live-pending",
                fee_status="assumed_zero_non_material",
                fee_source="missing",
                fee_confidence="ambiguous",
                fee_provenance="missing_fee_field",
                fee_validation_reason="assumed_zero_non_material",
            ),
        ],
        client_order_id="cid-live-pending",
        exchange_order_id="ex-live-pending",
        side="SELL",
        context="test",
    )

    assert _fill_accounting_status(aggregated[0]) == "fee_pending"


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


def test_aggregate_fills_high_notional_invalid_fee_blocks_after_hard_alert(
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
            with pytest.raises(FillFeeStrictModeError, match="material fee validation blocked fill aggregation"):
                _aggregate_fills_for_apply(
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
        with pytest.raises(FillFeeStrictModeError, match="blocked fill aggregation"):
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


@pytest.mark.parametrize(
    ("invalid_fee_raw", "strict_mode", "strict_min_notional", "expect_block"),
    [
        (None, False, 10_000.0, False),
        ("", False, 10_000.0, False),
        ("bad-fee", True, 1_000_000.0, False),
        (-1.0, True, 10_000.0, True),
        (float("nan"), True, 10_000.0, True),
    ],
)
def test_aggregate_fills_strict_mode_enforcement_by_invalid_fee_types_and_threshold(
    invalid_fee_raw: object,
    strict_mode: bool,
    strict_min_notional: float,
    expect_block: bool,
) -> None:
    original_alert_min_notional = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    original_strict_mode = settings.LIVE_FILL_FEE_STRICT_MODE
    original_strict_min_notional = settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", strict_mode)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", strict_min_notional)

    fills = [
        BrokerFill(
            client_order_id="cid-6",
            fill_id="f1",
            fill_ts=1000,
            price=5_000.0,
            qty=1.0,
            fee=1.0,
            exchange_order_id="ex-6",
        ),
        BrokerFill(
            client_order_id="cid-6",
            fill_id="f2",
            fill_ts=1010,
            price=6_000.0,
            qty=1.0,
            fee=invalid_fee_raw,
            exchange_order_id="ex-6",
        ),
    ]

    try:
        if expect_block:
            with pytest.raises(FillFeeStrictModeError):
                _aggregate_fills_for_apply(
                    fills=fills,
                    client_order_id="cid-6",
                    exchange_order_id="ex-6",
                    side="BUY",
                    context="test",
                )
        else:
            aggregated = _aggregate_fills_for_apply(
                fills=fills,
                client_order_id="cid-6",
                exchange_order_id="ex-6",
                side="BUY",
                context="test",
            )
            assert len(aggregated) == 1
    finally:
        object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", original_alert_min_notional)
        object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", original_strict_mode)
        object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", original_strict_min_notional)


def test_aggregate_fills_strict_mode_blocks_on_aggregate_notional_boundary_even_when_invalid_fill_notional_is_lower() -> None:
    original_alert_min_notional = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    original_strict_mode = settings.LIVE_FILL_FEE_STRICT_MODE
    original_strict_min_notional = settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 1.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 10_000.0)

    fills = [
        BrokerFill(
            client_order_id="cid-7",
            fill_id="f1",
            fill_ts=1000,
            price=9_000.0,
            qty=1.0,
            fee=1.0,
            exchange_order_id="ex-7",
        ),
        BrokerFill(
            client_order_id="cid-7",
            fill_id="f2",
            fill_ts=1010,
            price=1_000.0,
            qty=1.0,
            fee="",
            exchange_order_id="ex-7",
        ),
    ]

    try:
        with pytest.raises(FillFeeStrictModeError, match="aggregate_notional=10000"):
            _aggregate_fills_for_apply(
                fills=fills,
                client_order_id="cid-7",
                exchange_order_id="ex-7",
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
