from __future__ import annotations

import pytest

from bithumb_bot.strategy.daily_participation_policy import (
    DailyParticipationCountSnapshot,
    DailyParticipationPolicyConfig,
    build_research_daily_count_snapshot,
    require_runtime_comparable_daily_count_snapshot,
)


DECISION_TS = 1_704_046_800_000
FILL_TS = 1_704_043_200_000


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


def _snapshot(**record_overrides):
    record = {
        "side": "BUY",
        "client_order_id": "buy-1",
        "fill_id": "fill-1",
        "fill_ts": FILL_TS,
        "is_execution_filled": True,
    }
    record.update(record_overrides)
    return build_research_daily_count_snapshot(
        config=_config(),
        decision_ts=DECISION_TS,
        pair=str(record.pop("pair", "KRW-BTC")),
        strategy_instance_id=str(record.pop("strategy_instance_id", "daily:unit")),
        trade_records=(record,),
    )


def test_snapshot_hash_changes_when_event_id_changes() -> None:
    assert _snapshot(fill_id="fill-1").snapshot_hash != _snapshot(fill_id="fill-2").snapshot_hash


def test_snapshot_hash_changes_when_side_changes() -> None:
    buy = _snapshot(side="BUY")
    sell = _snapshot(side="SELL")

    assert buy.count_for_kst_day == 1
    assert sell.count_for_kst_day == 0
    assert buy.snapshot_hash != sell.snapshot_hash


def test_snapshot_hash_changes_when_strategy_instance_changes() -> None:
    assert _snapshot(strategy_instance_id="daily:a").snapshot_hash != _snapshot(
        strategy_instance_id="daily:b"
    ).snapshot_hash


def test_same_count_different_events_do_not_share_hash() -> None:
    left = _snapshot(client_order_id="buy-1", fill_id="fill-1")
    right = _snapshot(client_order_id="buy-2", fill_id="fill-2")

    assert left.count_for_kst_day == right.count_for_kst_day == 1
    assert left.snapshot_hash != right.snapshot_hash


def test_live_snapshot_requires_event_set_hash() -> None:
    snapshot = DailyParticipationCountSnapshot(
        count_basis="filled",
        timezone="Asia/Seoul",
        kst_day="2024-01-01",
        count_for_kst_day=1,
        timestamp_field="fill_ts",
        source="unit",
        rows=(),
        pair="KRW-BTC",
        strategy_instance_id="daily:unit",
    )

    assert snapshot.snapshot_hash == "sha256:missing"
    with pytest.raises(ValueError, match="daily_count_snapshot_hash_missing"):
        require_runtime_comparable_daily_count_snapshot(snapshot)
