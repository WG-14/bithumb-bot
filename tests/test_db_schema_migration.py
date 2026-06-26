from __future__ import annotations

import json
import sqlite3

from bithumb_bot.db_core import ensure_schema


def _insert_backfill_fixture(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            1,
            "daily_participation_sma",
            "BUY",
            "fixture",
            json.dumps(
                {
                    "strategy_instance_id": "h74-source-observation",
                    "runtime_strategy_set_manifest_hash": "sha256:manifest",
                }
            ),
        ),
    )
    decision_id = int(conn.execute("SELECT id FROM strategy_decisions").fetchone()[0])
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
            entry_ts, exit_ts, matched_qty, entry_price, exit_price, gross_pnl, fee_total,
            net_pnl, holding_time_sec, strategy_name, entry_decision_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "KRW-BTC",
            1,
            2,
            "buy-1",
            "sell-1",
            1,
            2,
            0.0001,
            100.0,
            101.0,
            1.0,
            0.1,
            0.9,
            60.0,
            "daily_participation_sma",
            decision_id,
        ),
    )


def test_ensure_schema_does_not_require_row_factory() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        ensure_schema(conn)
    finally:
        conn.close()


def test_trade_lifecycle_strategy_scope_backfill_accepts_tuple_rows() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        ensure_schema(conn)
        _insert_backfill_fixture(conn)
        ensure_schema(conn)
        row = conn.execute(
            """
            SELECT strategy_instance_id, runtime_strategy_set_manifest_hash
            FROM trade_lifecycles
            """
        ).fetchone()
    finally:
        conn.close()

    assert row[0] == "h74-source-observation"
    assert row[1] == "sha256:manifest"


def test_ensure_schema_accepts_sqlite_row_factory() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        ensure_schema(conn)
        ensure_schema(conn)
    finally:
        conn.close()
