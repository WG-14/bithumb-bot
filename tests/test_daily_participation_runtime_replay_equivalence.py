from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.canonical_decision import export_runtime_replay_decisions
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange
from bithumb_bot.research.strategy_registry import resolve_research_strategy_plugin
from bithumb_bot.strategy_contract_testing import _seed_replay_db
from bithumb_bot.strategy.daily_participation_policy import (
    DailyParticipationPolicyConfig,
    DailyParticipationStateSnapshot,
    build_research_daily_count_snapshot,
    evaluate_daily_participation_policy,
    kst_day,
    require_runtime_comparable_daily_count_snapshot,
)
from bithumb_bot.runtime.daily_participation_count_provider import build_runtime_daily_count_snapshot_from_sqlite


def _config(**overrides):
    values = {
        "enabled": True,
        "timezone": "Asia/Seoul",
        "count_basis": "filled",
        "window_start_hour": 0,
        "window_end_hour": 24,
        "buy_fraction": 0.05,
        "max_order_krw": 10000.0,
    }
    values.update(overrides)
    return DailyParticipationPolicyConfig(**values)


def _state(**overrides):
    values = {
        "decision_ts": 1_704_046_800_000,
        "count_for_kst_day": 0,
        "position_open": False,
        "daily_count_snapshot_hash": "sha256:" + "4" * 64,
    }
    values.update(overrides)
    return DailyParticipationStateSnapshot(**values)


def test_research_and_runtime_daily_participation_policy_hash_match() -> None:
    research = evaluate_daily_participation_policy(config=_config(), state=_state())
    runtime = evaluate_daily_participation_policy(config=_config(), state=_state())

    assert research.participation_input_hash == runtime.participation_input_hash
    assert research.participation_policy_hash == runtime.participation_policy_hash


def test_research_and_runtime_daily_participation_policy_hash_match_with_real_adapters() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE orders (
            client_order_id TEXT NOT NULL,
            side TEXT NOT NULL,
            pair TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            strategy_instance_id TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fills (
            client_order_id TEXT NOT NULL,
            fill_id TEXT,
            fill_ts INTEGER NOT NULL,
            qty REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(client_order_id, side, pair, strategy_name, strategy_instance_id)
        VALUES ('unit-buy-1', 'BUY', 'KRW-BTC', 'daily_participation_sma', 'daily_participation_sma:unit')
        """
    )
    conn.execute(
        "INSERT INTO fills(client_order_id, fill_id, fill_ts, qty) VALUES (?, ?, ?, ?)",
        ("unit-buy-1", "fill-1", 1_704_043_200_000, 1.0),
    )
    config = _config(count_basis="filled")
    decision_ts = 1_704_046_800_000
    research_snapshot = build_research_daily_count_snapshot(
        config=config,
        decision_ts=decision_ts,
        trade_records=(
            {
                "side": "BUY",
                "client_order_id": "unit-buy-1",
                "fill_id": "fill-1",
                "fill_ts": 1_704_043_200_000,
                "is_execution_filled": True,
            },
        ),
        pair="KRW-BTC",
        strategy_instance_id="daily_participation_sma:unit",
    )
    runtime_snapshot = build_runtime_daily_count_snapshot_from_sqlite(
        conn=conn,
        config=config,
        decision_ts=decision_ts,
        pair="KRW-BTC",
        strategy_instance_id="daily_participation_sma:unit",
    )
    research = evaluate_daily_participation_policy(
        config=config,
        state=research_snapshot.state_snapshot(decision_ts=decision_ts, position_open=False, entry_allowed=True),
    )
    runtime = evaluate_daily_participation_policy(
        config=config,
        state=runtime_snapshot.state_snapshot(decision_ts=decision_ts, position_open=False, entry_allowed=True),
    )

    assert research.count_basis == runtime.count_basis
    assert research.kst_day == runtime.kst_day
    assert research.daily_count_snapshot_hash != "sha256:missing"
    assert runtime.daily_count_snapshot_hash != "sha256:missing"
    assert research.participation_policy_hash == runtime.participation_policy_hash
    assert research.participation_input_hash != runtime.participation_input_hash
    assert research_snapshot.event_set_hash.startswith("sha256:")
    assert runtime_snapshot.event_set_hash.startswith("sha256:")
    assert research_snapshot.count_for_kst_day == runtime_snapshot.count_for_kst_day == 1


def test_daily_count_snapshot_hash_missing_fails_runtime_comparable_mode() -> None:
    with pytest.raises(ValueError, match="daily_count_snapshot_hash_missing"):
        require_runtime_comparable_daily_count_snapshot(_state(daily_count_snapshot_hash="sha256:missing"))


def test_count_basis_mismatch_changes_policy_input_hash() -> None:
    filled = evaluate_daily_participation_policy(config=_config(count_basis="filled"), state=_state())
    intent = evaluate_daily_participation_policy(config=_config(count_basis="intent"), state=_state())

    assert filled.participation_input_hash != intent.participation_input_hash


def test_kst_day_boundary_mismatch_changes_policy_input_hash() -> None:
    first = evaluate_daily_participation_policy(config=_config(), state=_state(decision_ts=1_704_034_799_000))
    second = evaluate_daily_participation_policy(config=_config(), state=_state(decision_ts=1_704_034_800_000))

    assert first.kst_day != second.kst_day
    assert first.participation_input_hash != second.participation_input_hash


def _runtime_dataset() -> DatasetSnapshot:
    start = 1_704_043_200_000
    closes = tuple(100.0 + index for index in range(20))
    return DatasetSnapshot(
        snapshot_id="daily_participation_runtime_replay_fixture",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2024-01-01", "2024-01-01"),
        candles=tuple(
            Candle(ts=start + index * 60_000, open=close, high=close, low=close, close=close, volume=1.0)
            for index, close in enumerate(closes)
        ),
    )


def _runtime_params() -> dict[str, object]:
    return {
        "SMA_SHORT": 2,
        "SMA_LONG": 4,
        "DAILY_PARTICIPATION_ENABLED": True,
        "DAILY_PARTICIPATION_TIMEZONE": "Asia/Seoul",
        "DAILY_PARTICIPATION_COUNT_BASIS": "filled",
        "DAILY_PARTICIPATION_WINDOW_START_HOUR_KST": 0,
        "DAILY_PARTICIPATION_WINDOW_END_HOUR_KST": 24,
        "DAILY_PARTICIPATION_BUY_FRACTION": 0.05,
        "DAILY_PARTICIPATION_MAX_ORDER_KRW": 10000.0,
    }


def test_runtime_replay_bundle_contains_daily_participation_evidence(tmp_path) -> None:
    dataset = _runtime_dataset()
    db_path = tmp_path / "daily_participation_runtime.sqlite"
    through_ts = _seed_replay_db(db_path, dataset)
    plugin = resolve_research_strategy_plugin("daily_participation_sma")
    strategy = plugin.runtime_replay_builder(
        {
            "strategy_name": "daily_participation_sma",
            "market": dataset.market,
            "interval": dataset.interval,
            "strategy_parameters": _runtime_params(),
        },
        None,
    )
    conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        decisions = export_runtime_replay_decisions(
            conn=conn,
            strategy=strategy,
            through_ts_list=[through_ts],
            market=dataset.market,
            interval=dataset.interval,
            profile_content_hash="sha256:" + "1" * 64,
            dataset_content_hash=dataset.content_hash(),
            db_data_fingerprint="sha256:daily-runtime-db",
            strategy_version=plugin.version,
            strategy_decision_contract_version=plugin.decision_contract_version,
        )
    finally:
        conn.close()

    assert len(decisions) == 1
    decision = decisions[0]
    assert decision["strategy_name"] == "daily_participation_sma"
    assert decision["count_basis"] == "filled"
    assert decision["kst_day"] == kst_day(through_ts, "Asia/Seoul")
    assert decision["daily_count_snapshot_hash"].startswith("sha256:")
    assert decision["daily_count_snapshot_hash"] != "sha256:missing"
    assert decision["participation_policy_hash"].startswith("sha256:")
    assert decision["participation_decision_hash"].startswith("sha256:")
