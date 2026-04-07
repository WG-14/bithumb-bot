from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.db_core import ensure_db
from bithumb_bot.dust import DUST_TRACKING_LOT_STATE, OPEN_EXPOSURE_LOT_STATE, build_dust_display_context, classify_dust_residual, dust_qty_gap_tolerance
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.lifecycle import mark_harmless_dust_positions


def _record_order(conn, *, client_order_id: str, side: str, qty_req: float, ts_ms: int) -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side=side,
        qty_req=qty_req,
        submit_attempt_id=f"attempt_{client_order_id}",
        price=None,
        ts_ms=ts_ms,
        status="NEW",
    )


def test_trade_lifecycle_tracks_realized_pnl_fee_and_holding_time(tmp_path):
    conn = ensure_db(str(tmp_path / "lifecycle.sqlite"))
    base_ts = 1_700_000_000_000

    _record_order(conn, client_order_id="entry_1", side="BUY", qty_req=2.0, ts_ms=base_ts)
    buy_trade = apply_fill_and_trade(
        conn,
        client_order_id="entry_1",
        side="BUY",
        fill_id="fill_entry_1",
        fill_ts=base_ts,
        price=100.0,
        qty=2.0,
        fee=2.0,
        strategy_name="sma_with_filter",
        entry_decision_id=101,
        note="entry",
    )
    assert buy_trade is not None

    _record_order(conn, client_order_id="exit_1", side="SELL", qty_req=1.0, ts_ms=base_ts + 60_000)
    sell_trade_1 = apply_fill_and_trade(
        conn,
        client_order_id="exit_1",
        side="SELL",
        fill_id="fill_exit_1",
        fill_ts=base_ts + 60_000,
        price=110.0,
        qty=1.0,
        fee=1.0,
        strategy_name="sma_with_filter",
        entry_decision_id=101,
        exit_decision_id=202,
        exit_reason="opposite signal",
        exit_rule_name="opposite_cross",
        note="exit_partial",
    )
    assert sell_trade_1 is not None

    _record_order(conn, client_order_id="exit_2", side="SELL", qty_req=1.0, ts_ms=base_ts + 120_000)
    sell_trade_2 = apply_fill_and_trade(
        conn,
        client_order_id="exit_2",
        side="SELL",
        fill_id="fill_exit_2",
        fill_ts=base_ts + 120_000,
        price=120.0,
        qty=1.0,
        fee=1.0,
        strategy_name="sma_with_filter",
        entry_decision_id=101,
        exit_decision_id=203,
        exit_reason="max holding reached",
        exit_rule_name="max_holding_time",
        note="exit_final",
    )
    assert sell_trade_2 is not None

    rows = conn.execute(
        """
        SELECT
            entry_client_order_id,
            exit_client_order_id,
            matched_qty,
            gross_pnl,
            fee_total,
            net_pnl,
            holding_time_sec,
            entry_fill_id,
            exit_fill_id,
            strategy_name,
            entry_decision_id,
            entry_decision_linkage,
            exit_decision_id,
            exit_reason,
            exit_rule_name
        FROM trade_lifecycles
        ORDER BY id ASC
        """
    ).fetchall()
    open_lots = conn.execute("SELECT COUNT(*) FROM open_position_lots").fetchone()[0]
    conn.close()

    assert len(rows) == 2

    assert rows[0]["entry_client_order_id"] == "entry_1"
    assert rows[0]["exit_client_order_id"] == "exit_1"
    assert float(rows[0]["matched_qty"]) == pytest.approx(1.0)
    assert float(rows[0]["gross_pnl"]) == pytest.approx(10.0)
    assert float(rows[0]["fee_total"]) == pytest.approx(2.0)
    assert float(rows[0]["net_pnl"]) == pytest.approx(8.0)
    assert float(rows[0]["holding_time_sec"]) == pytest.approx(60.0)
    assert rows[0]["entry_fill_id"] == "fill_entry_1"
    assert rows[0]["exit_fill_id"] == "fill_exit_1"
    assert rows[0]["strategy_name"] == "sma_with_filter"
    assert int(rows[0]["entry_decision_id"]) == 101
    assert rows[0]["entry_decision_linkage"] == "direct"
    assert int(rows[0]["exit_decision_id"]) == 202
    assert rows[0]["exit_reason"] == "opposite signal"
    assert rows[0]["exit_rule_name"] == "opposite_cross"

    assert rows[1]["entry_client_order_id"] == "entry_1"
    assert rows[1]["exit_client_order_id"] == "exit_2"
    assert float(rows[1]["matched_qty"]) == pytest.approx(1.0)
    assert float(rows[1]["gross_pnl"]) == pytest.approx(20.0)
    assert float(rows[1]["fee_total"]) == pytest.approx(2.0)
    assert float(rows[1]["net_pnl"]) == pytest.approx(18.0)
    assert float(rows[1]["holding_time_sec"]) == pytest.approx(120.0)
    assert rows[1]["entry_fill_id"] == "fill_entry_1"
    assert rows[1]["exit_fill_id"] == "fill_exit_2"
    assert rows[1]["strategy_name"] == "sma_with_filter"
    assert int(rows[1]["entry_decision_id"]) == 101
    assert rows[1]["entry_decision_linkage"] == "direct"
    assert int(rows[1]["exit_decision_id"]) == 203
    assert rows[1]["exit_reason"] == "max holding reached"
    assert rows[1]["exit_rule_name"] == "max_holding_time"

    assert open_lots == 0


def test_schema_bootstrap_creates_lifecycle_tables(tmp_path):
    conn = ensure_db(str(tmp_path / "schema.sqlite"))
    trade_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
    lot_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(open_position_lots)").fetchall()}
    lifecycle_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(trade_lifecycles)").fetchall()}
    lot_schema_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='open_position_lots'"
    ).fetchone()[0]
    conn.close()

    assert "entry_client_order_id" in lot_cols
    assert "entry_fee_total" in lot_cols
    assert "gross_pnl" in lifecycle_cols
    assert "fee_total" in lifecycle_cols
    assert "net_pnl" in lifecycle_cols
    assert "holding_time_sec" in lifecycle_cols
    assert "entry_client_order_id" in lifecycle_cols
    assert "exit_client_order_id" in lifecycle_cols
    assert "client_order_id" in trade_cols
    assert "strategy_name" in trade_cols
    assert "entry_decision_id" in trade_cols
    assert "exit_decision_id" in trade_cols
    assert "exit_reason" in trade_cols
    assert "exit_rule_name" in trade_cols
    assert "entry_decision_linkage" in lot_cols
    assert "exit_decision_id" in lifecycle_cols
    assert "entry_decision_linkage" in lifecycle_cols
    assert "exit_reason" in lifecycle_cols
    assert "exit_rule_name" in lifecycle_cols
    assert "position_state" in lot_cols
    assert "CHECK (position_state IN ('open_exposure', 'dust_tracking'))" in lot_schema_sql


def test_mark_harmless_dust_positions_only_reclassifies_strict_sub_min_open_exposure_rows(tmp_path):
    conn = ensure_db(str(tmp_path / "harmless_dust_boundary.sqlite"))
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            position_state
        ) VALUES
            (?, ?, ?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "BTC_KRW",
            1,
            "below_min",
            1_700_000_000_000,
            40_000_000.0,
            0.00009999,
            OPEN_EXPOSURE_LOT_STATE,
            "BTC_KRW",
            2,
            "exact_min",
            1_700_000_000_500,
            40_000_000.0,
            0.0001,
            OPEN_EXPOSURE_LOT_STATE,
            "BTC_KRW",
            3,
            "above_min",
            1_700_000_001_000,
            40_000_000.0,
            0.00010001,
            OPEN_EXPOSURE_LOT_STATE,
        ),
    )
    conn.commit()

    dust = classify_dust_residual(
        broker_qty=0.00009999,
        local_qty=0.00009999,
        min_qty=0.0001,
        min_notional_krw=5000.0,
        latest_price=40_000_000.0,
        partial_flatten_recent=False,
        partial_flatten_reason="not_recent",
        qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
        matched_harmless_resume_allowed=True,
    )
    updated = mark_harmless_dust_positions(
        conn,
        pair="BTC_KRW",
        dust_metadata=build_dust_display_context(dust),
    )
    conn.commit()

    rows = conn.execute(
        """
        SELECT entry_client_order_id, position_state, qty_open
        FROM open_position_lots
        ORDER BY entry_client_order_id ASC
        """
    ).fetchall()
    conn.close()

    assert updated == 1
    assert rows[0]["entry_client_order_id"] == "above_min"
    assert rows[0]["position_state"] == OPEN_EXPOSURE_LOT_STATE
    assert float(rows[0]["qty_open"]) == pytest.approx(0.00010001)
    assert rows[1]["entry_client_order_id"] == "below_min"
    assert rows[1]["position_state"] == DUST_TRACKING_LOT_STATE
    assert float(rows[1]["qty_open"]) == pytest.approx(0.00009999)
    assert rows[2]["entry_client_order_id"] == "exact_min"
    assert rows[2]["position_state"] == OPEN_EXPOSURE_LOT_STATE
    assert float(rows[2]["qty_open"]) == pytest.approx(0.0001)


def test_schema_bootstrap_backfills_open_position_lot_state_for_legacy_rows(tmp_path):
    db_path = tmp_path / "legacy_lot_state.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE open_position_lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            entry_trade_id INTEGER NOT NULL,
            entry_client_order_id TEXT NOT NULL,
            entry_fill_id TEXT,
            entry_ts INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            qty_open REAL NOT NULL,
            entry_fee_total REAL NOT NULL DEFAULT 0,
            strategy_name TEXT,
            entry_decision_id INTEGER,
            entry_decision_linkage TEXT,
            created_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
        """
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("BTC_KRW", 1, "legacy_entry", 1_700_000_000_000, 40_000_000.0, 0.00009629),
    )
    conn.commit()
    conn.close()

    conn = ensure_db(str(db_path))
    lot_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(open_position_lots)").fetchall()}
    state_row = conn.execute(
        "SELECT position_state FROM open_position_lots WHERE entry_client_order_id='legacy_entry'"
    ).fetchone()
    conn.close()

    assert "position_state" in lot_cols
    assert state_row["position_state"] == "open_exposure"


def test_sell_without_known_entry_writes_unknown_lifecycle_row(tmp_path):
    conn = ensure_db(str(tmp_path / "unknown_entry.sqlite"))
    conn.execute(
        """
        INSERT OR REPLACE INTO portfolio(
            id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
        ) VALUES (1, 1000.0, 1.0, 1000.0, 0.0, 1.0, 0.0)
        """
    )
    _record_order(conn, client_order_id="orphan_sell", side="SELL", qty_req=0.5, ts_ms=1700000000000)
    apply_fill_and_trade(
        conn,
        client_order_id="orphan_sell",
        side="SELL",
        fill_id="fill_orphan_sell",
        fill_ts=1700000001000,
        price=100.0,
        qty=0.5,
        fee=0.1,
        note="reconcile legacy",
    )
    row = conn.execute(
        """
        SELECT entry_trade_id, entry_client_order_id, matched_qty, gross_pnl, fee_total, net_pnl
        FROM trade_lifecycles
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert int(row["entry_trade_id"]) == 0
    assert row["entry_client_order_id"] == "__unknown_entry__"
    assert float(row["matched_qty"]) == pytest.approx(0.5)
    assert float(row["gross_pnl"]) == pytest.approx(0.0)
    assert float(row["fee_total"]) == pytest.approx(0.1)
    assert float(row["net_pnl"]) == pytest.approx(-0.1)


def test_buy_without_direct_decision_uses_strict_single_fallback_match(tmp_path):
    conn = ensure_db(str(tmp_path / "strict_match.sqlite"))
    fill_ts = 1_700_000_100_000
    conn.execute(
        """
        INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json)
        VALUES (?, ?, 'BUY', 'entry', ?)
        """,
        (fill_ts - 5_000, "sma_with_filter", '{"pair":"KRW-BTC"}'),
    )
    _record_order(conn, client_order_id="entry_strict", side="BUY", qty_req=1.0, ts_ms=fill_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="entry_strict",
        side="BUY",
        fill_id="fill_entry_strict",
        fill_ts=fill_ts,
        price=100.0,
        qty=1.0,
        fee=0.1,
        strategy_name="sma_with_filter",
    )
    row = conn.execute(
        "SELECT entry_decision_id, entry_decision_linkage FROM open_position_lots WHERE entry_client_order_id='entry_strict'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert int(row["entry_decision_id"]) == 1
    assert row["entry_decision_linkage"] == "fallback_strict_match"


def test_buy_fallback_does_not_misattributed_other_strategy_or_pair(tmp_path):
    conn = ensure_db(str(tmp_path / "no_misattribution.sqlite"))
    fill_ts = 1_700_000_200_000
    conn.execute(
        """
        INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json)
        VALUES (?, ?, 'BUY', 'entry', ?)
        """,
        (fill_ts - 1_000, "other_strategy", '{"pair":"KRW-BTC"}'),
    )
    conn.execute(
        """
        INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json)
        VALUES (?, ?, 'BUY', 'entry', ?)
        """,
        (fill_ts - 2_000, "sma_with_filter", '{"pair":"KRW-ETH"}'),
    )
    _record_order(conn, client_order_id="entry_unattributed", side="BUY", qty_req=1.0, ts_ms=fill_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="entry_unattributed",
        side="BUY",
        fill_id="fill_entry_unattributed",
        fill_ts=fill_ts,
        price=100.0,
        qty=1.0,
        fee=0.1,
        strategy_name="sma_with_filter",
    )
    row = conn.execute(
        "SELECT entry_decision_id, entry_decision_linkage FROM open_position_lots WHERE entry_client_order_id='entry_unattributed'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["entry_decision_id"] is None
    assert row["entry_decision_linkage"] == "unattributed_no_strict_match"


def test_buy_fallback_marks_ambiguous_when_multiple_strict_candidates(tmp_path):
    conn = ensure_db(str(tmp_path / "ambiguous_fallback.sqlite"))
    fill_ts = 1_700_000_300_000
    conn.execute(
        """
        INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json)
        VALUES (?, ?, 'BUY', 'entry', ?)
        """,
        (fill_ts - 2_000, "sma_with_filter", '{"pair":"KRW-BTC"}'),
    )
    conn.execute(
        """
        INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json)
        VALUES (?, ?, 'BUY', 'entry', ?)
        """,
        (fill_ts - 1_000, "sma_with_filter", '{"pair":"KRW-BTC"}'),
    )
    _record_order(conn, client_order_id="entry_ambiguous", side="BUY", qty_req=1.0, ts_ms=fill_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="entry_ambiguous",
        side="BUY",
        fill_id="fill_entry_ambiguous",
        fill_ts=fill_ts,
        price=100.0,
        qty=1.0,
        fee=0.1,
        strategy_name="sma_with_filter",
    )
    row = conn.execute(
        "SELECT entry_decision_id, entry_decision_linkage FROM open_position_lots WHERE entry_client_order_id='entry_ambiguous'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["entry_decision_id"] is None
    assert row["entry_decision_linkage"] == "ambiguous_multi_candidate"


def test_buy_recovery_mode_keeps_degraded_unattributed_without_fallback(tmp_path):
    conn = ensure_db(str(tmp_path / "recovery_degraded.sqlite"))
    fill_ts = 1_700_000_400_000
    conn.execute(
        """
        INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json)
        VALUES (?, ?, 'BUY', 'entry', ?)
        """,
        (fill_ts - 1_000, "sma_with_filter", '{"pair":"KRW-BTC"}'),
    )
    _record_order(conn, client_order_id="entry_recovery_mode", side="BUY", qty_req=1.0, ts_ms=fill_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="entry_recovery_mode",
        side="BUY",
        fill_id="fill_entry_recovery_mode",
        fill_ts=fill_ts,
        price=100.0,
        qty=1.0,
        fee=0.1,
        strategy_name="sma_with_filter",
        allow_entry_decision_fallback=False,
    )
    row = conn.execute(
        "SELECT entry_decision_id, entry_decision_linkage FROM open_position_lots WHERE entry_client_order_id='entry_recovery_mode'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["entry_decision_id"] is None
    assert row["entry_decision_linkage"] == "degraded_recovery_unattributed"


def test_sell_lifecycle_uses_open_exposure_lots_and_keeps_dust_tracking_operator_only(tmp_path):
    conn = ensure_db(str(tmp_path / "state_routing.sqlite"))
    base_ts = 1_700_001_000_000

    _record_order(conn, client_order_id="entry_open", side="BUY", qty_req=1.0, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="entry_open",
        side="BUY",
        fill_id="fill_entry_open",
        fill_ts=base_ts,
        price=100.0,
        qty=1.0,
        fee=0.1,
        strategy_name="sma_with_filter",
        entry_decision_id=301,
        note="entry_open",
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC_KRW", 999, "entry_dust", base_ts + 1_000, 100.0, 0.00009193, DUST_TRACKING_LOT_STATE),
    )
    conn.commit()

    _record_order(conn, client_order_id="exit_sell", side="SELL", qty_req=0.5, ts_ms=base_ts + 2_000)
    apply_fill_and_trade(
        conn,
        client_order_id="exit_sell",
        side="SELL",
        fill_id="fill_exit_sell",
        fill_ts=base_ts + 2_000,
        price=110.0,
        qty=0.5,
        fee=0.05,
        strategy_name="sma_with_filter",
        entry_decision_id=301,
        exit_decision_id=401,
        exit_reason="take_profit",
        exit_rule_name="signal_exit",
        note="exit_sell",
    )

    rows = conn.execute(
        """
        SELECT entry_client_order_id, position_state, qty_open
        FROM open_position_lots
        ORDER BY entry_client_order_id ASC
        """
    ).fetchall()
    lifecycle_row = conn.execute(
        """
        SELECT entry_client_order_id, exit_client_order_id, matched_qty
        FROM trade_lifecycles
        ORDER BY id ASC
        """
    ).fetchall()
    conn.close()

    assert rows[0]["entry_client_order_id"] == "entry_dust"
    assert rows[0]["position_state"] == DUST_TRACKING_LOT_STATE
    assert float(rows[0]["qty_open"]) == pytest.approx(0.00009193)
    assert rows[1]["entry_client_order_id"] == "entry_open"
    assert rows[1]["position_state"] == OPEN_EXPOSURE_LOT_STATE
    assert float(rows[1]["qty_open"]) == pytest.approx(0.5)
    assert len(lifecycle_row) == 1
    assert lifecycle_row[0]["entry_client_order_id"] == "entry_open"
    assert lifecycle_row[0]["exit_client_order_id"] == "exit_sell"
    assert float(lifecycle_row[0]["matched_qty"]) == pytest.approx(0.5)
