from __future__ import annotations

import sqlite3

from bithumb_bot.db_core import ensure_schema
from bithumb_bot.runtime_data_provider import RuntimeFeatureSnapshot
from bithumb_bot.runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec
from bithumb_bot.strategy_plugins.daily_participation_sma import (
    DAILY_PARTICIPATION_SMA_SPEC,
    runtime_feature_snapshot_builder,
)
from bithumb_bot.strategy.daily_participation_policy import DailyParticipationCountSnapshot


def _params() -> dict[str, object]:
    values = dict(DAILY_PARTICIPATION_SMA_SPEC.default_parameters)
    values.update(
        {
            "SMA_SHORT": 2,
            "SMA_LONG": 4,
            "DAILY_PARTICIPATION_ENABLED": True,
            "DAILY_PARTICIPATION_COUNT_BASIS": "filled",
        }
    )
    return values


def _request():
    return RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec(
            strategy_name="daily_participation_sma",
            pair="KRW-BTC",
            interval="1m",
            parameters=_params(),
            approved_profile_hash="sha256:" + "a" * 64,
            runtime_contract_hash="sha256:" + "b" * 64,
            strategy_instance_id="daily:feature",
        ),
        through_ts_ms=1_704_031_200_000 + 9 * 60_000,
    )


def _conn(*, with_fill: bool = False) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    base_ts = 1_704_031_200_000
    for index in range(10):
        close = 100.0 + index
        conn.execute(
            """
            INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, 1.0)
            """,
            (base_ts + index * 60_000, close, close, close, close),
        )
    if with_fill:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, status, side, pair, qty_req, qty_filled, strategy_name,
                strategy_instance_id, created_ts, updated_ts
            )
            VALUES ('buy-1', 'FILLED', 'BUY', 'KRW-BTC', 1.0, 1.0, 'daily_participation_sma',
                    'daily:feature', ?, ?)
            """,
            (base_ts, base_ts),
        )
        conn.execute(
            """
            INSERT INTO fills(client_order_id, fill_id, fill_ts, price, qty, fee)
            VALUES ('buy-1', 'fill-1', ?, 100.0, 1.0, 0.0)
            """,
            (base_ts + 60_000,),
        )
    conn.commit()
    return conn


def _generic_snapshot() -> RuntimeFeatureSnapshot:
    base_ts = 1_704_031_200_000
    rows = [
        {"ts": base_ts + index * 60_000, "open": 100.0 + index, "high": 100.0 + index,
         "low": 100.0 + index, "close": 100.0 + index, "volume": 1.0}
        for index in range(10)
    ]
    return RuntimeFeatureSnapshot(
        {
            "pair": "KRW-BTC",
            "interval": "1m",
            "through_ts_ms": rows[-1]["ts"],
            "feature_payload": {"capabilities": {"candles": {"rows": rows}}},
            "feature_snapshot_hash": "sha256:" + "0" * 64,
        }
    )


def test_daily_feature_snapshot_contains_base_sma_projection() -> None:
    conn = _conn()
    snapshot = runtime_feature_snapshot_builder(conn=conn, request=_request(), feature_snapshot=_generic_snapshot())

    assert snapshot is not None
    payload = snapshot.as_dict()["feature_payload"]
    assert "sma_with_filter" in payload


def test_daily_feature_snapshot_contains_count_snapshot() -> None:
    conn = _conn()
    snapshot = runtime_feature_snapshot_builder(conn=conn, request=_request(), feature_snapshot=_generic_snapshot())

    payload = snapshot.as_dict()["feature_payload"]
    assert "daily_participation_count_snapshot" in payload
    assert payload["daily_count_snapshot_hash"] != "sha256:missing"
    assert payload["count_basis"] == "filled"


def test_daily_feature_snapshot_hash_changes_when_count_snapshot_changes() -> None:
    empty = runtime_feature_snapshot_builder(conn=_conn(), request=_request(), feature_snapshot=_generic_snapshot())
    counted = runtime_feature_snapshot_builder(conn=_conn(with_fill=True), request=_request(), feature_snapshot=_generic_snapshot())

    assert empty is not None
    assert counted is not None
    assert empty.feature_snapshot_hash != counted.feature_snapshot_hash


def test_daily_feature_snapshot_missing_count_fails_closed(monkeypatch) -> None:
    from bithumb_bot.strategy_plugins import daily_participation_sma

    def _missing_count(**_kwargs):
        return DailyParticipationCountSnapshot(
            count_basis="filled",
            timezone="Asia/Seoul",
            kst_day="2024-01-01",
            count_for_kst_day=0,
            timestamp_field="fill_ts",
            source="unit",
            rows=(),
            fail_closed_reason="unit_missing_count",
        )

    monkeypatch.setattr(daily_participation_sma, "build_runtime_daily_count_snapshot_from_sqlite", _missing_count)

    snapshot = runtime_feature_snapshot_builder(conn=_conn(), request=_request(), feature_snapshot=_generic_snapshot())

    assert snapshot is None
