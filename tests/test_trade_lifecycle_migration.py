from __future__ import annotations

from bithumb_bot.db_core import ensure_db


def _insert_legacy(conn) -> None:
    decision_id = conn.execute(
        """
        INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, candle_ts, market_price, context_json)
        VALUES (1, 'daily_participation_sma', 'BUY', 'unit', 1, 100, ?)
        """,
        ('{"strategy_name":"daily_participation_sma","strategy_instance_id":"H74","risk_scope_id":"H74"}',),
    ).lastrowid
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
            entry_ts, exit_ts, matched_qty, entry_price, exit_price, gross_pnl, fee_total,
            net_pnl, holding_time_sec, strategy_name, strategy_instance_id, entry_decision_id
        ) VALUES ('KRW-BTC', 1, 2, 'entry', 'operator_flatten-1', 1, 2, 1, 100, 90, -10, 0, -10, 1,
            'operator_flatten', 'H74', ?)
        """,
        (decision_id,),
    )


def test_trade_lifecycle_owner_actor_migration_is_idempotent(tmp_path) -> None:
    db_path = tmp_path / "migration.sqlite"
    conn = ensure_db(str(db_path))
    _insert_legacy(conn)
    conn.commit()

    ensure_db(str(db_path)).close()
    first = conn.execute("SELECT COUNT(*) AS c, owner_strategy_instance_id, exit_actor FROM trade_lifecycles").fetchone()
    ensure_db(str(db_path)).close()
    second = conn.execute("SELECT COUNT(*) AS c, owner_strategy_instance_id, exit_actor FROM trade_lifecycles").fetchone()

    assert first["c"] == second["c"] == 1
    assert second["owner_strategy_instance_id"] == "H74"
    assert second["exit_actor"] == "operator"


def test_operator_flatten_backfills_exit_actor_without_losing_owner(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "operator.sqlite"))
    _insert_legacy(conn)
    conn.commit()
    ensure_db(str(tmp_path / "operator.sqlite")).close()

    row = conn.execute("SELECT owner_strategy_instance_id, exit_actor, exit_authority FROM trade_lifecycles").fetchone()

    assert row["owner_strategy_instance_id"] == "H74"
    assert row["exit_actor"] == "operator"
    assert row["exit_authority"] == "operator_flatten"


def test_existing_strategy_name_compatibility_is_preserved(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "compat.sqlite"))
    _insert_legacy(conn)
    conn.commit()
    ensure_db(str(tmp_path / "compat.sqlite")).close()

    row = conn.execute("SELECT strategy_name FROM trade_lifecycles").fetchone()

    assert row["strategy_name"] == "operator_flatten"
