from __future__ import annotations

import pytest

from bithumb_bot.strategy.daily_participation_events import ParticipationEvent
from bithumb_bot.strategy.daily_participation_policy import (
    DailyParticipationPolicyConfig,
    build_research_daily_count_snapshot,
)


def _config() -> DailyParticipationPolicyConfig:
    return DailyParticipationPolicyConfig(
        enabled=True,
        timezone="Asia/Seoul",
        count_basis="filled",
        window_start_hour=0,
        window_end_hour=24,
        buy_fraction=0.05,
        max_order_krw=10000.0,
    )


def test_research_buy_fill_maps_to_filled_event() -> None:
    snapshot = build_research_daily_count_snapshot(
        config=_config(),
        decision_ts=1_704_046_800_000,
        pair="KRW-BTC",
        strategy_instance_id="daily:research",
        trade_records=(
            {
                "side": "BUY",
                "client_order_id": "buy-1",
                "fill_id": "fill-1",
                "fill_ts": 1_704_043_200_000,
                "is_execution_filled": True,
            },
        ),
    )

    assert snapshot.count_for_kst_day == 1
    assert snapshot.rows[0]["lifecycle_stage"] == "filled"
    assert snapshot.rows[0]["side"] == "BUY"


def test_sell_fill_does_not_satisfy_filled_buy_participation() -> None:
    snapshot = build_research_daily_count_snapshot(
        config=_config(),
        decision_ts=1_704_046_800_000,
        pair="KRW-BTC",
        strategy_instance_id="daily:research",
        trade_records=(
            {
                "side": "SELL",
                "client_order_id": "sell-1",
                "fill_id": "fill-1",
                "fill_ts": 1_704_043_200_000,
                "is_execution_filled": True,
            },
        ),
    )

    assert snapshot.count_for_kst_day == 0


def test_other_strategy_fill_does_not_satisfy_strategy_scoped_participation() -> None:
    left = build_research_daily_count_snapshot(
        config=_config(),
        decision_ts=1_704_046_800_000,
        pair="KRW-BTC",
        strategy_instance_id="daily:current",
        trade_records=(
            {
                "side": "BUY",
                "client_order_id": "buy-1",
                "fill_id": "fill-1",
                "fill_ts": 1_704_043_200_000,
                "is_execution_filled": True,
            },
        ),
    )
    right = build_research_daily_count_snapshot(
        config=_config(),
        decision_ts=1_704_046_800_000,
        pair="KRW-BTC",
        strategy_instance_id="daily:other",
        trade_records=(
            {
                "side": "BUY",
                "client_order_id": "buy-1",
                "fill_id": "fill-1",
                "fill_ts": 1_704_043_200_000,
                "is_execution_filled": True,
            },
        ),
    )

    assert left.snapshot_hash != right.snapshot_hash


def test_event_requires_strategy_instance_and_pair_for_live_scope() -> None:
    event = ParticipationEvent(
        event_id="event-1",
        strategy_instance_id="",
        strategy_name="daily_participation_sma",
        pair="",
        side="BUY",
        lifecycle_stage="filled",
        event_ts=1_704_043_200_000,
        count_basis="filled",
        client_order_id="buy-1",
        fill_id="fill-1",
        source="unit",
    )

    with pytest.raises(ValueError, match="daily_participation_event_scope_missing"):
        event.validate_live_scope()
