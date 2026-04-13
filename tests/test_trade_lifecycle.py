from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.dust import (
    DUST_TRACKING_LOT_STATE,
    OPEN_EXPOSURE_LOT_STATE,
    build_dust_display_context,
    build_position_state_model,
    classify_dust_residual,
    dust_qty_gap_tolerance,
)
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.oms import build_order_intent_key, claim_order_intent_dedup
from bithumb_bot.lot_model import build_market_lot_rules, lot_count_to_qty
from bithumb_bot.order_sizing import SellExecutionAuthority, build_sell_execution_sizing
from bithumb_bot.lifecycle import (
    ENTRY_DECISION_LINKAGE_AMBIGUOUS_MULTI_CANDIDATE,
    ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED,
    ENTRY_DECISION_LINKAGE_DIRECT,
    ENTRY_DECISION_LINKAGE_STRICT_SINGLE_FALLBACK,
    ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH,
    LOT_SEMANTIC_VERSION_V1,
    apply_fill_lifecycle,
    mark_harmless_dust_positions,
    summarize_position_lots,
)


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


def _test_lot_rules(*, market_price: float = 40_000_000.0):
    rules = type(
        "_TestOrderRules",
        (object,),
        {
            "min_qty": float(settings.LIVE_MIN_ORDER_QTY),
            "qty_step": float(settings.LIVE_ORDER_QTY_STEP),
            "min_notional_krw": float(settings.MIN_ORDER_NOTIONAL_KRW),
            "max_qty_decimals": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        },
    )()
    return build_market_lot_rules(
        market_id="BTC_KRW",
        market_price=float(market_price),
        rules=rules,
        source_mode="derived",
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
    assert rows[0]["entry_decision_linkage"] == ENTRY_DECISION_LINKAGE_DIRECT
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
    assert rows[1]["entry_decision_linkage"] == ENTRY_DECISION_LINKAGE_DIRECT
    assert int(rows[1]["exit_decision_id"]) == 203
    assert rows[1]["exit_reason"] == "max holding reached"
    assert rows[1]["exit_rule_name"] == "max_holding_time"

    assert open_lots == 0


def test_schema_bootstrap_creates_lifecycle_tables(tmp_path):
    conn = ensure_db(str(tmp_path / "schema.sqlite"))
    trade_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
    lot_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(open_position_lots)").fetchall()}
    fill_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(fills)").fetchall()}
    intent_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(order_intent_dedup)").fetchall()}
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
    assert "executable_lot_count" in lot_cols
    assert "dust_tracking_lot_count" in lot_cols
    assert "lot_semantic_version" in lot_cols
    assert "internal_lot_size" in lot_cols
    assert "lot_min_qty" in lot_cols
    assert "lot_qty_step" in lot_cols
    assert "lot_min_notional_krw" in lot_cols
    assert "lot_max_qty_decimals" in lot_cols
    assert "lot_rule_source_mode" in lot_cols
    assert "position_semantic_basis" in lot_cols
    assert "intended_lot_count" in fill_cols
    assert "executable_lot_count" in fill_cols
    assert "internal_lot_size" in fill_cols
    assert "intended_lot_count" in intent_cols
    assert "executable_lot_count" in intent_cols
    assert "exit_decision_id" in lifecycle_cols
    assert "entry_decision_linkage" in lifecycle_cols
    assert "exit_reason" in lifecycle_cols
    assert "exit_rule_name" in lifecycle_cols
    assert "position_state" in lot_cols
    assert "CHECK (position_state IN ('open_exposure', 'dust_tracking'))" in lot_schema_sql


def test_buy_fill_persists_immutable_lot_definition_snapshot(tmp_path):
    conn = ensure_db(str(tmp_path / "lot_definition_snapshot.sqlite"))

    apply_fill_lifecycle(
        conn,
        side="BUY",
        pair="BTC_KRW",
        trade_id=1,
        client_order_id="entry_1",
        fill_id="fill_entry_1",
        fill_ts=1_700_000_000_000,
        price=40_000_000.0,
        qty=0.0004,
        fee=0.0,
        strategy_name="sma_with_filter",
        entry_decision_id=11,
    )

    row = conn.execute(
        """
        SELECT
            lot_semantic_version,
            internal_lot_size,
            lot_min_qty,
            lot_qty_step,
            lot_min_notional_krw,
            lot_max_qty_decimals,
            lot_rule_source_mode
        FROM open_position_lots
        WHERE pair='BTC_KRW'
        ORDER BY id ASC
        LIMIT 1
        """
    ).fetchone()

    assert row is not None
    assert int(row["lot_semantic_version"]) == LOT_SEMANTIC_VERSION_V1
    assert float(row["internal_lot_size"]) > 0.0
    assert float(row["lot_min_qty"]) == pytest.approx(float(settings.LIVE_MIN_ORDER_QTY))
    assert float(row["lot_qty_step"]) == pytest.approx(float(settings.LIVE_ORDER_QTY_STEP))
    assert float(row["lot_min_notional_krw"]) == pytest.approx(float(settings.MIN_ORDER_NOTIONAL_KRW))
    assert int(row["lot_max_qty_decimals"]) == int(settings.LIVE_ORDER_MAX_QTY_DECIMALS)
    assert row["lot_rule_source_mode"] == "ledger"


def test_position_lot_summary_uses_persisted_lot_definition_snapshot_under_rule_drift(tmp_path):
    conn = ensure_db(str(tmp_path / "lot_definition_drift.sqlite"))
    original = {
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
    }
    try:
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
        apply_fill_lifecycle(
            conn,
            side="BUY",
            pair="BTC_KRW",
            trade_id=1,
            client_order_id="entry_1",
            fill_id="fill_entry_1",
            fill_ts=1_700_000_000_000,
            price=40_000_000.0,
            qty=0.0008,
            fee=0.0,
            strategy_name="sma_with_filter",
            entry_decision_id=11,
        )

        baseline = summarize_position_lots(conn, pair="BTC_KRW")
        assert baseline.lot_definition is not None
        assert baseline.lot_definition.is_authoritative is True
        persisted_lot_size = float(baseline.lot_definition.internal_lot_size or 0.0)

        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.001)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.001)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 6)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 100000.0)

        drifted = summarize_position_lots(conn, pair="BTC_KRW")

        assert drifted.lot_definition is not None
        assert drifted.lot_definition.is_authoritative is True
        assert drifted.lot_definition.internal_lot_size == pytest.approx(persisted_lot_size)
        assert drifted.lot_definition.min_qty == pytest.approx(0.0001)
        assert drifted.lot_definition.qty_step == pytest.approx(0.0001)
        assert drifted.lot_definition.min_notional_krw == pytest.approx(5000.0)
        assert drifted.lot_definition.max_qty_decimals == 8
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


def test_position_lot_summary_derives_legacy_lot_size_from_consistent_row_qty_ratios(tmp_path):
    conn = ensure_db(str(tmp_path / "lot_definition_legacy_ratio.sqlite"))
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC_KRW", 1, "entry_open", 1_700_000_000_000, 40_000_000.0, 0.0002, 2, 0, "lot-native", "open_exposure"),
    )

    snapshot = summarize_position_lots(conn, pair="BTC_KRW")

    assert snapshot.lot_definition is not None
    assert snapshot.lot_definition.semantic_version == 0
    assert snapshot.lot_definition.internal_lot_size == pytest.approx(0.0001)
    assert snapshot.lot_definition.source_mode == "derived_from_row_qty"


def test_open_position_lots_schema_rejects_lot_native_state_count_mismatch(tmp_path):
    conn = ensure_db(str(tmp_path / "schema_invariant.sqlite"))

    with pytest.raises(sqlite3.IntegrityError, match="lot-native state/count mismatch"):
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "BTC_KRW",
                1,
                "mixed_state",
                1_700_000_000_000,
                40_000_000.0,
                0.00009997,
                0,
                1,
                "lot-native",
                OPEN_EXPOSURE_LOT_STATE,
            ),
        )

    conn.close()


def test_open_position_lots_schema_rejects_negative_lot_counts(tmp_path):
    conn = ensure_db(str(tmp_path / "schema_negative_counts.sqlite"))

    with pytest.raises(sqlite3.IntegrityError, match="negative lot counts"):
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "BTC_KRW",
                1,
                "negative_state",
                1_700_000_000_000,
                40_000_000.0,
                0.0001,
                -1,
                0,
                "lot-native",
                OPEN_EXPOSURE_LOT_STATE,
            ),
        )

    conn.close()


def test_open_position_lots_schema_rejects_executable_qty_lot_size_mismatch(tmp_path):
    conn = ensure_db(str(tmp_path / "schema_executable_qty_mismatch.sqlite"))

    with pytest.raises(sqlite3.IntegrityError, match="executable qty must match lot authority"):
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                internal_lot_size,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "BTC_KRW",
                1,
                "qty_mismatch",
                1_700_000_000_000,
                40_000_000.0,
                0.00025,
                2,
                0,
                0.0001,
                "lot-native",
                OPEN_EXPOSURE_LOT_STATE,
            ),
        )

    conn.close()


def test_open_position_lots_schema_rejects_dust_qty_lot_size_mismatch(tmp_path):
    conn = ensure_db(str(tmp_path / "schema_dust_qty_mismatch.sqlite"))

    with pytest.raises(sqlite3.IntegrityError, match="dust qty must match lot authority"):
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                executable_lot_count,
                dust_tracking_lot_count,
                internal_lot_size,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "BTC_KRW",
                1,
                "dust_qty_mismatch",
                1_700_000_000_000,
                40_000_000.0,
                0.00015,
                0,
                2,
                0.0001,
                "lot-native",
                DUST_TRACKING_LOT_STATE,
            ),
        )

    conn.close()


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
            position_semantic_basis,
            position_state
        ) VALUES
            (?, ?, ?, ?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "BTC_KRW",
            1,
            "below_min",
            1_700_000_000_000,
            40_000_000.0,
            0.00009999,
            "legacy",
            OPEN_EXPOSURE_LOT_STATE,
            "BTC_KRW",
            2,
            "exact_min",
            1_700_000_000_500,
            40_000_000.0,
            0.0001,
            "legacy",
            OPEN_EXPOSURE_LOT_STATE,
            "BTC_KRW",
            3,
            "above_min",
            1_700_000_001_000,
            40_000_000.0,
            0.00010001,
            "legacy",
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
        SELECT entry_client_order_id, position_state, qty_open, position_semantic_basis
        FROM open_position_lots
        ORDER BY entry_client_order_id ASC
        """
    ).fetchall()
    conn.close()

    assert updated == 1
    assert rows[0]["entry_client_order_id"] == "above_min"
    assert rows[0]["position_state"] == OPEN_EXPOSURE_LOT_STATE
    assert float(rows[0]["qty_open"]) == pytest.approx(0.00010001)
    assert rows[0]["position_semantic_basis"] == "legacy"
    assert rows[1]["entry_client_order_id"] == "below_min"
    assert rows[1]["position_state"] == DUST_TRACKING_LOT_STATE
    assert float(rows[1]["qty_open"]) == pytest.approx(0.00009999)
    assert rows[1]["position_semantic_basis"] == "lot-native"
    assert rows[2]["entry_client_order_id"] == "exact_min"
    assert rows[2]["position_state"] == OPEN_EXPOSURE_LOT_STATE
    assert float(rows[2]["qty_open"]) == pytest.approx(0.0001)
    assert rows[2]["position_semantic_basis"] == "legacy"


def test_lifecycle_helpers_work_with_raw_tuple_rows(tmp_path):
    db_path = tmp_path / "tuple_rows.sqlite"
    ensure_db(str(db_path)).close()
    conn = sqlite3.connect(db_path)
    try:
        fill_ts = 1_700_002_000_000
        conn.execute(
            """
            INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json)
            VALUES (?, ?, 'BUY', 'entry', ?)
            """,
            (fill_ts - 1_000, "sma_with_filter", '{"pair":"BTC_KRW"}'),
        )

        apply_fill_lifecycle(
            conn,
            side="BUY",
            pair="BTC_KRW",
            trade_id=1,
            client_order_id="tuple_buy",
            fill_id="fill_tuple_buy",
            fill_ts=fill_ts,
            price=100.0,
            qty=0.5,
            fee=0.1,
            strategy_name=None,
            entry_decision_id=1,
        )

        buy_row = conn.execute(
            """
            SELECT strategy_name, entry_decision_id, entry_decision_linkage
            FROM open_position_lots
            WHERE entry_client_order_id='tuple_buy'
            """
        ).fetchone()

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
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair,
                entry_trade_id,
                entry_client_order_id,
                entry_ts,
                entry_price,
                qty_open,
                position_semantic_basis,
                position_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("BTC_KRW", 2, "raw_dust", fill_ts + 1_000, 40_000_000.0, 0.00009999, "legacy", OPEN_EXPOSURE_LOT_STATE),
        )
        conn.commit()

        updated = mark_harmless_dust_positions(
            conn,
            pair="BTC_KRW",
            dust_metadata=build_dust_display_context(dust),
        )
        state_row = conn.execute(
            "SELECT position_state FROM open_position_lots WHERE entry_client_order_id='raw_dust'"
        ).fetchone()
    finally:
        conn.close()

    assert buy_row is not None
    assert buy_row[0] == "sma_with_filter"
    assert int(buy_row[1]) == 1
    assert buy_row[2] == "direct"
    assert updated == 1
    assert state_row is not None
    assert state_row[0] == DUST_TRACKING_LOT_STATE


def test_open_position_lots_rejects_zero_qty_rows_with_lot_authority(tmp_path):
    conn = ensure_db(str(tmp_path / "lot_count_authority.sqlite"))
    conn.close()

    with pytest.raises(sqlite3.IntegrityError, match="zero qty rows must not keep lot authority"):
        conn = ensure_db(str(tmp_path / "lot_count_authority.sqlite"))
        try:
            conn.execute(
                """
                INSERT INTO open_position_lots(
                    pair,
                    entry_trade_id,
                    entry_client_order_id,
                    entry_ts,
                    entry_price,
                    qty_open,
                    executable_lot_count,
                    dust_tracking_lot_count,
                    position_semantic_basis,
                    position_state
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "BTC_KRW",
                    1,
                    "count_only_exposure",
                    1_700_000_123_000,
                    40_000_000.0,
                    0.0,
                    2,
                    0,
                    "lot-native",
                    OPEN_EXPOSURE_LOT_STATE,
                ),
            )
        finally:
            conn.close()


def test_buy_lifecycle_writes_executable_lot_counts_and_separates_sub_lot_dust(tmp_path):
    conn = ensure_db(str(tmp_path / "buy_lot_counts.sqlite"))
    base_ts = 1_700_000_600_000
    lot_rules = _test_lot_rules()
    lot_qty = lot_count_to_qty(lot_count=3, lot_size=lot_rules.lot_size)
    dust_qty = lot_rules.lot_size / 2.0
    fill_qty = lot_qty + dust_qty

    _record_order(conn, client_order_id="entry_buy", side="BUY", qty_req=fill_qty, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="entry_buy",
        side="BUY",
        fill_id="fill_entry_buy",
        fill_ts=base_ts,
        price=100.0,
        qty=fill_qty,
        fee=0.1,
        strategy_name="sma_with_filter",
        entry_decision_id=501,
        note="entry_buy",
    )

    pair = str(settings.PAIR)
    rows = conn.execute(
        """
        SELECT pair, entry_client_order_id, position_state, qty_open, executable_lot_count, dust_tracking_lot_count
        FROM open_position_lots
        ORDER BY id ASC
        """
    ).fetchall()
    summary = summarize_position_lots(conn, pair=str(rows[0]["pair"]))
    conn.close()

    assert len(rows) == 2
    assert rows[0]["position_state"] == OPEN_EXPOSURE_LOT_STATE
    assert float(rows[0]["qty_open"]) == pytest.approx(lot_qty)
    assert int(rows[0]["executable_lot_count"]) == 3
    assert int(rows[0]["dust_tracking_lot_count"]) == 0
    assert rows[1]["position_state"] == DUST_TRACKING_LOT_STATE
    assert float(rows[1]["qty_open"]) == pytest.approx(dust_qty)
    assert int(rows[1]["executable_lot_count"]) == 0
    assert int(rows[1]["dust_tracking_lot_count"]) == 1
    assert summary.open_lot_count == 3
    assert summary.dust_tracking_lot_count == 1
    assert summary.raw_open_exposure_qty == pytest.approx(lot_qty)
    assert summary.dust_tracking_qty == pytest.approx(dust_qty)


def test_buy_lifecycle_maps_exact_lot_fills_to_canonical_lot_counts(tmp_path):
    conn = ensure_db(str(tmp_path / "buy_exact_lots.sqlite"))
    base_ts = 1_700_000_650_000
    lot_rules = _test_lot_rules()
    fill_qty = lot_count_to_qty(lot_count=4, lot_size=lot_rules.lot_size)

    _record_order(conn, client_order_id="entry_exact", side="BUY", qty_req=fill_qty, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="entry_exact",
        side="BUY",
        fill_id="fill_entry_exact",
        fill_ts=base_ts,
        price=100.0,
        qty=fill_qty,
        fee=0.1,
        strategy_name="sma_with_filter",
        entry_decision_id=502,
        note="entry_exact",
    )

    row = conn.execute(
        """
        SELECT pair, position_state, qty_open, executable_lot_count, dust_tracking_lot_count
        FROM open_position_lots
        WHERE entry_client_order_id='entry_exact'
        """
    ).fetchone()
    summary = summarize_position_lots(conn, pair=str(row["pair"]))
    conn.close()

    assert row is not None
    assert row["position_state"] == OPEN_EXPOSURE_LOT_STATE
    assert float(row["qty_open"]) == pytest.approx(fill_qty)
    assert int(row["executable_lot_count"]) == 4
    assert int(row["dust_tracking_lot_count"]) == 0
    assert summary.open_lot_count == 4
    assert summary.dust_tracking_lot_count == 0
    assert summary.raw_open_exposure_qty == pytest.approx(fill_qty)


def test_buy_lifecycle_tracks_dust_only_when_fill_is_smaller_than_one_lot(tmp_path):
    conn = ensure_db(str(tmp_path / "buy_dust_only.sqlite"))
    base_ts = 1_700_000_700_000
    lot_rules = _test_lot_rules()
    fill_qty = lot_rules.lot_size / 2.0

    _record_order(conn, client_order_id="entry_dust_only", side="BUY", qty_req=fill_qty, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="entry_dust_only",
        side="BUY",
        fill_id="fill_entry_dust_only",
        fill_ts=base_ts,
        price=100.0,
        qty=fill_qty,
        fee=0.1,
        strategy_name="sma_with_filter",
        entry_decision_id=503,
        note="entry_dust_only",
    )

    row = conn.execute(
        """
        SELECT pair, position_state, qty_open, executable_lot_count, dust_tracking_lot_count
        FROM open_position_lots
        WHERE entry_client_order_id='entry_dust_only'
        """
    ).fetchone()
    summary = summarize_position_lots(conn, pair=str(row["pair"]))
    conn.close()

    assert row is not None
    assert row["position_state"] == DUST_TRACKING_LOT_STATE
    assert float(row["qty_open"]) == pytest.approx(fill_qty)
    assert int(row["executable_lot_count"]) == 0
    assert int(row["dust_tracking_lot_count"]) == 1
    assert summary.open_lot_count == 0
    assert summary.dust_tracking_lot_count == 1
    assert summary.raw_open_exposure_qty == pytest.approx(0.0)
    assert summary.dust_tracking_qty == pytest.approx(fill_qty)


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
    assert "position_semantic_basis" in lot_cols
    assert state_row["position_state"] == "open_exposure"


def test_schema_bootstrap_adds_lot_count_columns_for_open_position_lots(tmp_path):
    conn = ensure_db(str(tmp_path / "schema_lot_counts.sqlite"))
    lot_cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(open_position_lots)").fetchall()}
    conn.close()

    assert "executable_lot_count" in lot_cols
    assert "dust_tracking_lot_count" in lot_cols
    assert "position_semantic_basis" in lot_cols


def test_order_intent_dedup_prefers_lot_counts_over_qty_for_keying_and_storage(tmp_path):
    conn = ensure_db(str(tmp_path / "intent_dedup.sqlite"))
    lot_key_1 = build_order_intent_key(
        symbol="BTC_KRW",
        side="SELL",
        strategy_context="paper:sma_with_filter:1h",
        intent_ts=1_700_000_000_000,
        intent_type="market_exit",
        qty=0.123456789,
        intended_lot_count=3,
        executable_lot_count=2,
    )
    lot_key_2 = build_order_intent_key(
        symbol="BTC_KRW",
        side="SELL",
        strategy_context="paper:sma_with_filter:1h",
        intent_ts=1_700_000_000_000,
        intent_type="market_exit",
        qty=0.999999999,
        intended_lot_count=3,
        executable_lot_count=2,
    )
    qty_key_1 = build_order_intent_key(
        symbol="BTC_KRW",
        side="SELL",
        strategy_context="paper:sma_with_filter:1h",
        intent_ts=1_700_000_000_000,
        intent_type="market_exit",
        qty=0.123456789,
    )
    qty_key_2 = build_order_intent_key(
        symbol="BTC_KRW",
        side="SELL",
        strategy_context="paper:sma_with_filter:1h",
        intent_ts=1_700_000_000_000,
        intent_type="market_exit",
        qty=0.999999999,
    )

    assert lot_key_1 == lot_key_2
    assert qty_key_1 != qty_key_2

    claimed, _ = claim_order_intent_dedup(
        conn,
        intent_key=lot_key_1,
        client_order_id="order_1",
        symbol="BTC_KRW",
        side="SELL",
        strategy_context="paper:sma_with_filter:1h",
        intent_type="market_exit",
        intent_ts=1_700_000_000_000,
        qty=0.123456789,
        intended_lot_count=3,
        executable_lot_count=2,
        order_status="PENDING_SUBMIT",
    )
    assert claimed is True

    row = conn.execute(
        """
        SELECT qty, intended_lot_count, executable_lot_count
        FROM order_intent_dedup
        WHERE intent_key=?
        """,
        (lot_key_1,),
    ).fetchone()
    conn.close()

    assert row is not None
    assert float(row["qty"]) == pytest.approx(0.123456789)
    assert int(row["intended_lot_count"]) == 3
    assert int(row["executable_lot_count"]) == 2


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


def test_buy_strict_single_fallback_links_unique_strict_match(tmp_path):
    conn = ensure_db(str(tmp_path / "strict_match.sqlite"))
    fill_ts = 1_700_000_100_000
    original_pair = settings.PAIR
    row = None
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    try:
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
    finally:
        conn.close()
        object.__setattr__(settings, "PAIR", original_pair)

    assert row is not None
    assert int(row["entry_decision_id"]) == 1
    assert row["entry_decision_linkage"] == ENTRY_DECISION_LINKAGE_STRICT_SINGLE_FALLBACK


def test_buy_unattributed_no_strict_match_does_not_misattribute_other_strategy_or_pair(tmp_path):
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
    assert row["entry_decision_linkage"] == ENTRY_DECISION_LINKAGE_UNATTRIBUTED_NO_STRICT_MATCH


def test_buy_ambiguous_multi_candidate_when_multiple_strict_pair_matches_exist(tmp_path):
    conn = ensure_db(str(tmp_path / "ambiguous_fallback.sqlite"))
    fill_ts = 1_700_000_300_000
    original_pair = settings.PAIR
    row = None
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    try:
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
    finally:
        conn.close()
        object.__setattr__(settings, "PAIR", original_pair)

    assert row is not None
    assert row["entry_decision_id"] is None
    assert row["entry_decision_linkage"] == ENTRY_DECISION_LINKAGE_AMBIGUOUS_MULTI_CANDIDATE


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
    assert row["entry_decision_linkage"] == ENTRY_DECISION_LINKAGE_DEGRADED_RECOVERY_UNATTRIBUTED


def test_buy_direct_linked_decision_preempts_strict_fallback_candidates(tmp_path):
    conn = ensure_db(str(tmp_path / "direct_preempts_fallback.sqlite"))
    fill_ts = 1_700_000_450_000
    original_pair = settings.PAIR
    row = None
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    try:
        # Fallback would have been valid here, but an explicit decision id must win.
        conn.execute(
            """
            INSERT INTO strategy_decisions(decision_ts, strategy_name, signal, reason, context_json)
            VALUES (?, ?, 'BUY', 'entry', ?)
            """,
            (fill_ts - 1_000, "sma_with_filter", '{"pair":"KRW-BTC"}'),
        )
        _record_order(conn, client_order_id="entry_direct", side="BUY", qty_req=1.0, ts_ms=fill_ts)
        apply_fill_and_trade(
            conn,
            client_order_id="entry_direct",
            side="BUY",
            fill_id="fill_entry_direct",
            fill_ts=fill_ts,
            price=100.0,
            qty=1.0,
            fee=0.1,
            strategy_name="sma_with_filter",
            entry_decision_id=77,
        )
        row = conn.execute(
            """
            SELECT entry_decision_id, entry_decision_linkage
            FROM open_position_lots
            WHERE entry_client_order_id='entry_direct'
            """
        ).fetchone()
    finally:
        conn.close()
        object.__setattr__(settings, "PAIR", original_pair)

    assert row is not None
    assert int(row["entry_decision_id"]) == 77
    assert row["entry_decision_linkage"] == ENTRY_DECISION_LINKAGE_DIRECT


# Authority boundary regression suite.


def test_authority_boundary_sell_lifecycle_uses_open_exposure_lots_and_keeps_dust_tracking_operator_only(tmp_path):
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
            executable_lot_count,
            dust_tracking_lot_count,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC_KRW", 999, "entry_dust", base_ts + 1_000, 100.0, 0.00009193, 0, 1, DUST_TRACKING_LOT_STATE),
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
        SELECT entry_client_order_id, position_state, qty_open, executable_lot_count, dust_tracking_lot_count
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
    assert int(rows[0]["executable_lot_count"]) == 0
    assert int(rows[0]["dust_tracking_lot_count"]) == 1
    assert rows[1]["entry_client_order_id"] == "entry_open"
    assert rows[1]["position_state"] == OPEN_EXPOSURE_LOT_STATE
    assert float(rows[1]["qty_open"]) == pytest.approx(0.5)
    assert int(rows[1]["executable_lot_count"]) > 0
    assert int(rows[1]["dust_tracking_lot_count"]) == 0
    assert len(lifecycle_row) == 1
    assert lifecycle_row[0]["entry_client_order_id"] == "entry_open"
    assert lifecycle_row[0]["exit_client_order_id"] == "exit_sell"
    assert float(lifecycle_row[0]["matched_qty"]) == pytest.approx(0.5)


def test_authority_boundary_sell_lifecycle_ignores_dust_tracking_even_if_it_is_above_min_qty(tmp_path):
    conn = ensure_db(str(tmp_path / "malformed_dust_tracking.sqlite"))
    base_ts = 1_700_001_100_000

    _record_order(conn, client_order_id="entry_open", side="BUY", qty_req=0.5, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="entry_open",
        side="BUY",
        fill_id="fill_entry_open",
        fill_ts=base_ts,
        price=100.0,
        qty=0.5,
        fee=0.05,
        strategy_name="sma_with_filter",
        entry_decision_id=302,
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
            executable_lot_count,
            dust_tracking_lot_count,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC_KRW", 998, "malformed_dust", base_ts + 1_000, 100.0, 0.5, 0, 1, DUST_TRACKING_LOT_STATE),
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
        entry_decision_id=302,
        exit_decision_id=402,
        exit_reason="take_profit",
        exit_rule_name="signal_exit",
        note="exit_sell",
    )

    rows = conn.execute(
        """
        SELECT entry_client_order_id, position_state, qty_open, executable_lot_count, dust_tracking_lot_count
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

    rows_by_id = {row["entry_client_order_id"]: row for row in rows}
    assert "entry_open" not in rows_by_id
    assert rows_by_id["malformed_dust"]["position_state"] == DUST_TRACKING_LOT_STATE
    assert float(rows_by_id["malformed_dust"]["qty_open"]) == pytest.approx(0.5)
    assert int(rows_by_id["malformed_dust"]["executable_lot_count"]) == 0
    assert int(rows_by_id["malformed_dust"]["dust_tracking_lot_count"]) == 1
    assert len(lifecycle_row) == 1
    assert lifecycle_row[0]["entry_client_order_id"] == "entry_open"
    assert lifecycle_row[0]["exit_client_order_id"] == "exit_sell"
    assert float(lifecycle_row[0]["matched_qty"]) == pytest.approx(0.5)


@pytest.mark.lot_native_regression_gate
def test_recovery_reconstructs_lot_native_exposure_and_dust_after_restart(tmp_path):
    db_path = tmp_path / "restart_lot_native.sqlite"
    lot_rules = _test_lot_rules()
    executable_qty = lot_count_to_qty(lot_count=2, lot_size=lot_rules.lot_size)
    dust_qty = lot_rules.lot_size / 2.0
    fill_qty = executable_qty + dust_qty

    conn = ensure_db(str(db_path))
    base_ts = 1_700_001_200_000
    _record_order(conn, client_order_id="restart_entry", side="BUY", qty_req=fill_qty, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="restart_entry",
        side="BUY",
        fill_id="fill_restart_entry",
        fill_ts=base_ts,
        price=100.0,
        qty=fill_qty,
        fee=0.1,
        strategy_name="sma_with_filter",
        entry_decision_id=601,
    )
    conn.commit()
    conn.close()

    conn = ensure_db(str(db_path))
    pair = str(conn.execute("SELECT pair FROM open_position_lots LIMIT 1").fetchone()[0])
    summary = summarize_position_lots(conn, pair=pair)
    normalized = build_position_state_model(
        raw_qty_open=float(summary.raw_open_exposure_qty),
        metadata_raw={},
        raw_total_asset_qty=float(summary.raw_total_asset_qty),
        open_exposure_qty=float(summary.raw_open_exposure_qty),
        dust_tracking_qty=float(summary.dust_tracking_qty),
        open_lot_count=int(summary.open_lot_count),
        dust_tracking_lot_count=int(summary.dust_tracking_lot_count),
        market_price=40_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    )
    sell_plan = build_sell_execution_sizing(
        pair=pair,
        market_price=40_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=int(
                normalized.normalized_exposure.sellable_executable_lot_count
            ),
            exit_allowed=bool(normalized.normalized_exposure.exit_allowed),
            exit_block_reason=str(normalized.normalized_exposure.exit_block_reason),
        ),
        lot_definition=summary.lot_definition,
    )
    conn.close()

    assert summary.open_lot_count == 2
    assert summary.dust_tracking_lot_count == 1
    assert summary.raw_open_exposure_qty == pytest.approx(executable_qty)
    assert summary.dust_tracking_qty == pytest.approx(dust_qty)
    assert summary.raw_total_asset_qty == pytest.approx(fill_qty)
    assert normalized.normalized_exposure.sellable_executable_lot_count == 2
    assert normalized.normalized_exposure.exit_allowed is True
    assert sell_plan.allowed is True
    assert sell_plan.executable_lot_count == 2
    assert sell_plan.requested_qty == pytest.approx(executable_qty)


@pytest.mark.lot_native_regression_gate
def test_partial_exit_keeps_remaining_sell_authority_lot_native(tmp_path):
    conn = ensure_db(str(tmp_path / "partial_exit_remaining_lot_authority.sqlite"))
    lot_rules = _test_lot_rules()
    base_ts = 1_700_001_250_000
    buy_qty = lot_count_to_qty(lot_count=3, lot_size=lot_rules.lot_size)
    partial_exit_qty = lot_count_to_qty(lot_count=1, lot_size=lot_rules.lot_size)

    _record_order(conn, client_order_id="partial_entry", side="BUY", qty_req=buy_qty, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="partial_entry",
        side="BUY",
        fill_id="fill_partial_entry",
        fill_ts=base_ts,
        price=40_000_000.0,
        qty=buy_qty,
        fee=0.0,
        strategy_name="sma_with_filter",
        entry_decision_id=701,
    )
    _record_order(
        conn,
        client_order_id="partial_exit",
        side="SELL",
        qty_req=partial_exit_qty,
        ts_ms=base_ts + 60_000,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="partial_exit",
        side="SELL",
        fill_id="fill_partial_exit",
        fill_ts=base_ts + 60_000,
        price=41_000_000.0,
        qty=partial_exit_qty,
        fee=0.0,
        strategy_name="sma_with_filter",
        entry_decision_id=701,
        exit_decision_id=702,
        exit_reason="trim",
        exit_rule_name="partial_trim",
    )

    pair = str(settings.PAIR)
    summary = summarize_position_lots(conn, pair=pair)
    normalized = build_position_state_model(
        raw_qty_open=float(summary.raw_open_exposure_qty),
        metadata_raw={},
        raw_total_asset_qty=float(summary.raw_total_asset_qty),
        open_exposure_qty=float(summary.raw_open_exposure_qty),
        dust_tracking_qty=float(summary.dust_tracking_qty),
        open_lot_count=int(summary.open_lot_count),
        dust_tracking_lot_count=int(summary.dust_tracking_lot_count),
        market_price=40_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    )
    sell_plan = build_sell_execution_sizing(
        pair=pair,
        market_price=40_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=int(
                normalized.normalized_exposure.sellable_executable_lot_count
            ),
            exit_allowed=bool(normalized.normalized_exposure.exit_allowed),
            exit_block_reason=str(normalized.normalized_exposure.exit_block_reason),
        ),
        lot_definition=summary.lot_definition,
    )
    conn.close()

    assert summary.open_lot_count == 2
    assert summary.raw_open_exposure_qty == pytest.approx(buy_qty - partial_exit_qty)
    assert normalized.normalized_exposure.sellable_executable_lot_count == 2
    assert normalized.normalized_exposure.sellable_executable_qty == pytest.approx(buy_qty - partial_exit_qty)
    assert normalized.normalized_exposure.exit_allowed is True
    assert sell_plan.allowed is True
    assert sell_plan.executable_lot_count == 2
    assert sell_plan.requested_qty == pytest.approx(buy_qty - partial_exit_qty)


@pytest.mark.lot_native_regression_gate
def test_partial_exit_residue_crosses_into_dust_only_state(tmp_path):
    conn = ensure_db(str(tmp_path / "partial_exit_dust_only_residue.sqlite"))
    lot_rules = _test_lot_rules()
    base_ts = 1_700_001_275_000
    executable_entry_qty = lot_count_to_qty(lot_count=2, lot_size=lot_rules.lot_size)
    dust_qty = lot_rules.lot_size / 2.0
    buy_qty = float(executable_entry_qty + dust_qty)
    partial_exit_qty = float(executable_entry_qty)

    _record_order(conn, client_order_id="partial_dust_entry", side="BUY", qty_req=buy_qty, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="partial_dust_entry",
        side="BUY",
        fill_id="fill_partial_dust_entry",
        fill_ts=base_ts,
        price=40_000_000.0,
        qty=buy_qty,
        fee=0.0,
        strategy_name="sma_with_filter",
        entry_decision_id=711,
    )
    _record_order(
        conn,
        client_order_id="partial_dust_exit",
        side="SELL",
        qty_req=partial_exit_qty,
        ts_ms=base_ts + 60_000,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="partial_dust_exit",
        side="SELL",
        fill_id="fill_partial_dust_exit",
        fill_ts=base_ts + 60_000,
        price=41_000_000.0,
        qty=partial_exit_qty,
        fee=0.0,
        strategy_name="sma_with_filter",
        entry_decision_id=711,
        exit_decision_id=712,
        exit_reason="trim_to_dust",
        exit_rule_name="partial_trim",
    )

    pair = str(settings.PAIR)
    summary = summarize_position_lots(conn, pair=pair)
    normalized = build_position_state_model(
        raw_qty_open=float(summary.raw_open_exposure_qty),
        metadata_raw={},
        raw_total_asset_qty=float(summary.raw_total_asset_qty),
        open_exposure_qty=float(summary.raw_open_exposure_qty),
        dust_tracking_qty=float(summary.dust_tracking_qty),
        open_lot_count=int(summary.open_lot_count),
        dust_tracking_lot_count=int(summary.dust_tracking_lot_count),
        market_price=40_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    )
    sell_plan = build_sell_execution_sizing(
        pair=pair,
        market_price=40_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=int(
                normalized.normalized_exposure.sellable_executable_lot_count
            ),
            exit_allowed=bool(normalized.normalized_exposure.exit_allowed),
            exit_block_reason=str(normalized.normalized_exposure.exit_block_reason),
        ),
        lot_definition=summary.lot_definition,
    )
    conn.close()

    assert summary.open_lot_count == 0
    assert summary.raw_open_exposure_qty == pytest.approx(0.0)
    assert summary.dust_tracking_lot_count == 1
    assert summary.dust_tracking_qty == pytest.approx(dust_qty)
    assert normalized.normalized_exposure.sellable_executable_lot_count == 0
    assert normalized.normalized_exposure.sellable_executable_qty == pytest.approx(0.0)
    assert normalized.normalized_exposure.exit_allowed is False
    assert normalized.normalized_exposure.exit_block_reason == "dust_only_remainder"
    assert normalized.normalized_exposure.terminal_state == "dust_only"
    assert sell_plan.allowed is False
    assert sell_plan.requested_qty == pytest.approx(0.0)
    assert sell_plan.executable_qty == pytest.approx(0.0)


@pytest.mark.lot_native_regression_gate
def test_rounding_induced_residue_ends_in_dust_only_state(tmp_path):
    conn = ensure_db(str(tmp_path / "rounding_residue_dust_only.sqlite"))
    lot_rules = _test_lot_rules()
    base_ts = 1_700_001_290_000
    executable_entry_qty = lot_count_to_qty(lot_count=2, lot_size=lot_rules.lot_size)
    rounded_residue_qty = float(lot_rules.lot_size / 4.0)
    buy_qty = float(executable_entry_qty + rounded_residue_qty)

    _record_order(conn, client_order_id="rounded_entry", side="BUY", qty_req=buy_qty, ts_ms=base_ts)
    apply_fill_and_trade(
        conn,
        client_order_id="rounded_entry",
        side="BUY",
        fill_id="fill_rounded_entry",
        fill_ts=base_ts,
        price=40_000_000.0,
        qty=buy_qty,
        fee=0.0,
        strategy_name="sma_with_filter",
        entry_decision_id=721,
    )
    _record_order(
        conn,
        client_order_id="rounded_exit",
        side="SELL",
        qty_req=executable_entry_qty,
        ts_ms=base_ts + 60_000,
    )
    apply_fill_and_trade(
        conn,
        client_order_id="rounded_exit",
        side="SELL",
        fill_id="fill_rounded_exit",
        fill_ts=base_ts + 60_000,
        price=41_000_000.0,
        qty=executable_entry_qty,
        fee=0.0,
        strategy_name="sma_with_filter",
        entry_decision_id=721,
        exit_decision_id=722,
        exit_reason="rounded_residue_cleanup",
        exit_rule_name="full_exit",
    )

    pair = str(settings.PAIR)
    summary = summarize_position_lots(conn, pair=pair)
    normalized = build_position_state_model(
        raw_qty_open=float(summary.raw_open_exposure_qty),
        metadata_raw={},
        raw_total_asset_qty=float(summary.raw_total_asset_qty),
        open_exposure_qty=float(summary.raw_open_exposure_qty),
        dust_tracking_qty=float(summary.dust_tracking_qty),
        open_lot_count=int(summary.open_lot_count),
        dust_tracking_lot_count=int(summary.dust_tracking_lot_count),
        market_price=40_000_000.0,
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    )
    sell_plan = build_sell_execution_sizing(
        pair=pair,
        market_price=40_000_000.0,
        authority=SellExecutionAuthority(
            sellable_executable_lot_count=int(
                normalized.normalized_exposure.sellable_executable_lot_count
            ),
            exit_allowed=bool(normalized.normalized_exposure.exit_allowed),
            exit_block_reason=str(normalized.normalized_exposure.exit_block_reason),
        ),
        lot_definition=summary.lot_definition,
    )
    conn.close()

    assert summary.open_lot_count == 0
    assert summary.raw_open_exposure_qty == pytest.approx(0.0)
    assert summary.dust_tracking_lot_count == 1
    assert summary.dust_tracking_qty == pytest.approx(rounded_residue_qty)
    assert normalized.normalized_exposure.sellable_executable_lot_count == 0
    assert normalized.normalized_exposure.sellable_executable_qty == pytest.approx(0.0)
    assert normalized.normalized_exposure.has_executable_exposure is False
    assert normalized.normalized_exposure.terminal_state == "dust_only"
    assert normalized.normalized_exposure.exit_block_reason == "dust_only_remainder"
    assert sell_plan.allowed is False
    assert sell_plan.requested_qty == pytest.approx(0.0)
    assert sell_plan.executable_qty == pytest.approx(0.0)


@pytest.mark.lot_native_regression_gate
def test_recovery_does_not_infer_executable_semantics_from_qty_without_lot_counts(tmp_path):
    conn = ensure_db(str(tmp_path / "legacy_qty_only.sqlite"))
    base_ts = 1_700_001_300_000

    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_state,
            position_semantic_basis
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC_KRW", 1001, "legacy_qty_only", base_ts, 100.0, 0.5, 0, 0, OPEN_EXPOSURE_LOT_STATE, "legacy-compat"),
    )
    conn.commit()

    summary = summarize_position_lots(conn, pair="BTC_KRW")
    conn.close()

    assert summary.open_lot_count == 0
    assert summary.dust_tracking_lot_count == 0
    assert summary.raw_open_exposure_qty == pytest.approx(0.0)
    assert summary.executable_open_exposure_qty == pytest.approx(0.0)
    assert summary.dust_tracking_qty == pytest.approx(0.0)
    assert summary.raw_total_asset_qty == pytest.approx(0.0)
    assert summary.exit_non_executable_reason == "no_executable_open_lots"
    assert summary.semantic_basis == "lot-native"
