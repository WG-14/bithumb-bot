from __future__ import annotations

import pytest

from bithumb_bot.execution_order_rules import ExecutionOrderRules
from bithumb_bot.live_pipeline_smoke import _validate_smoke_roundtrip_notional_buffer
from bithumb_bot.live_pipeline_smoke_preflight import LivePipelineSmokePreflightError
from bithumb_bot.live_pipeline_smoke_preflight import LivePipelineSmokeReadiness
from bithumb_bot.runtime.live_pipeline_smoke_decision import (
    LivePipelineSmokeDecisionError,
    LivePipelineSmokeDecisionProvider,
)


def _readiness(qty: float) -> LivePipelineSmokeReadiness:
    return LivePipelineSmokeReadiness(
        broker_qty=qty,
        portfolio_qty=qty,
        projected_total_qty=qty,
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
        fee_pending_count=0,
        active_fee_accounting_blocker=False,
        broker_qty_known=True,
        balance_source_stale=False,
        projection_converged=True,
    )


def test_provider_emits_buy_sell_x5_then_stop() -> None:
    provider = LivePipelineSmokeDecisionProvider(run_id="lps_test")
    emitted = []
    for qty in [0.0, 0.1] * 5:
        side = provider.next_side(_readiness(qty))
        emitted.append(side)
        provider.mark_step_complete()

    assert emitted == ["BUY", "SELL"] * 5
    assert provider.next_side(_readiness(0.0)) == "STOP"


def test_provider_rejects_buy_before_flat_and_sell_before_position() -> None:
    provider = LivePipelineSmokeDecisionProvider(run_id="lps_test")
    with pytest.raises(LivePipelineSmokeDecisionError, match="buy_requires_flat"):
        provider.next_side(_readiness(0.1))
    provider.mark_step_complete()
    with pytest.raises(LivePipelineSmokeDecisionError, match="sell_requires_position"):
        provider.next_side(_readiness(0.0))


def test_provider_rejects_invalid_order_bounds() -> None:
    with pytest.raises(LivePipelineSmokeDecisionError, match="cycles_x2"):
        LivePipelineSmokeDecisionProvider(run_id="lps_test", cycles=5, max_orders=9)
    with pytest.raises(LivePipelineSmokeDecisionError, match="above_10"):
        LivePipelineSmokeDecisionProvider(run_id="lps_test", cycles=6, max_orders=12)


def _rules() -> ExecutionOrderRules:
    return ExecutionOrderRules(
        market="KRW-BTC",
        min_qty=0.0001,
        qty_step=0.00000001,
        min_notional_krw=5_000.0,
        bid_min_total_krw=5_000.0,
        ask_min_total_krw=5_000.0,
        stale=False,
        source="unit",
    )


def test_live_pipeline_smoke_rejects_notional_below_roundtrip_min_qty_buffer() -> None:
    with pytest.raises(
        LivePipelineSmokePreflightError,
        match="live_pipeline_smoke_max_notional_below_sellable_roundtrip_minimum",
    ):
        _validate_smoke_roundtrip_notional_buffer(
            rules=_rules(),
            reference_price=96_933_000,
            max_notional_krw=10_000,
        )


def test_live_pipeline_smoke_allows_notional_above_roundtrip_min_qty_buffer() -> None:
    _validate_smoke_roundtrip_notional_buffer(
        rules=_rules(),
        reference_price=96_933_000,
        max_notional_krw=11_000,
    )


def test_live_pipeline_smoke_required_notional_uses_market_reference_not_hardcoded_price() -> None:
    with pytest.raises(LivePipelineSmokePreflightError):
        _validate_smoke_roundtrip_notional_buffer(
            rules=_rules(),
            reference_price=120_000_000,
            max_notional_krw=12_000,
        )
