from __future__ import annotations

import sqlite3

from bithumb_bot.runtime.daily_participation_count_provider import build_runtime_daily_count_snapshot_from_sqlite
from bithumb_bot.strategy.daily_participation_policy import DailyParticipationPolicyConfig


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


def _conn_with_scope() -> sqlite3.Connection:
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
    return conn


def _insert_fill(
    conn: sqlite3.Connection,
    *,
    client_order_id: str = "order-1",
    side: str = "BUY",
    pair: str = "KRW-BTC",
    strategy_instance_id: str = "daily:unit",
) -> None:
    conn.execute(
        """
        INSERT INTO orders(client_order_id, side, pair, strategy_name, strategy_instance_id)
        VALUES (?, ?, ?, 'daily_participation_sma', ?)
        """,
        (client_order_id, side, pair, strategy_instance_id),
    )
    conn.execute(
        "INSERT INTO fills(client_order_id, fill_id, fill_ts, qty) VALUES (?, 'fill-1', ?, 1.0)",
        (client_order_id, FILL_TS),
    )


def _snapshot(conn: sqlite3.Connection, *, pair: str = "KRW-BTC", strategy_instance_id: str = "daily:unit"):
    return build_runtime_daily_count_snapshot_from_sqlite(
        conn=conn,
        config=_config(),
        decision_ts=DECISION_TS,
        pair=pair,
        strategy_instance_id=strategy_instance_id,
        strategy_name="daily_participation_sma",
    )


def test_runtime_filled_count_requires_buy_order_join() -> None:
    conn = _conn_with_scope()
    _insert_fill(conn)

    snapshot = _snapshot(conn)

    assert snapshot.count_for_kst_day == 1
    assert snapshot.snapshot_hash != "sha256:missing"
    assert snapshot.rows[0]["side"] == "BUY"


def test_runtime_filled_count_excludes_sell_fill() -> None:
    conn = _conn_with_scope()
    _insert_fill(conn, side="SELL")

    snapshot = _snapshot(conn)

    assert snapshot.count_for_kst_day == 0
    assert snapshot.snapshot_hash != "sha256:missing"


def test_runtime_filled_count_excludes_other_strategy_instance() -> None:
    conn = _conn_with_scope()
    _insert_fill(conn, strategy_instance_id="other:instance")

    snapshot = _snapshot(conn)

    assert snapshot.count_for_kst_day == 0


def test_runtime_filled_count_excludes_other_pair() -> None:
    conn = _conn_with_scope()
    _insert_fill(conn, pair="KRW-ETH")

    snapshot = _snapshot(conn)

    assert snapshot.count_for_kst_day == 0


def test_runtime_count_provider_fails_closed_when_scope_columns_missing() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE fills (fill_ts INTEGER NOT NULL, qty REAL NOT NULL)")

    snapshot = build_runtime_daily_count_snapshot_from_sqlite(
        conn=conn,
        config=_config(),
        decision_ts=DECISION_TS,
        pair="KRW-BTC",
        strategy_instance_id="daily:unit",
    )

    assert snapshot.snapshot_hash == "sha256:missing"
    assert snapshot.fail_closed_reason.startswith("daily_participation_runtime_count_source_unavailable")
