from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot.db_core import ensure_schema
from bithumb_bot.h74_observation_report import build_h74_observation_report
from bithumb_bot.live_trade_classification import require_buy_authority_source
from bithumb_bot.oms import add_fill, create_order
from bithumb_bot.runtime.daily_participation_claims import claim_rows, reconstruct_daily_participation_claims_from_orders


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    return conn


def test_daily_participation_buy_creates_claim() -> None:
    conn = _conn()
    create_order(
        client_order_id="daily-buy",
        symbol="KRW-BTC",
        side="BUY",
        qty_req=1.0,
        price=100.0,
        strategy_name="daily_participation_sma",
        strategy_instance_id="daily:a",
        daily_participation_policy_hash="sha256:policy",
        daily_count_snapshot_hash="sha256:count",
        participation_decision_hash="sha256:decision",
        daily_participation_kst_day="2026-06-22",
        daily_participation_fallback_mode="unconditional_participation",
        status="FILLED",
        ts_ms=1,
        conn=conn,
    )

    reconstruct_daily_participation_claims_from_orders(conn, now_ms=2)

    rows = claim_rows(conn)
    assert len(rows) == 1
    assert rows[0]["status"] == "fulfilled"


def test_out_of_window_target_delta_block_creates_no_claim() -> None:
    conn = _conn()
    reconstruct_daily_participation_claims_from_orders(conn, now_ms=2)

    assert claim_rows(conn) == ()


def test_buy_order_requires_authority_source() -> None:
    with pytest.raises(ValueError, match="buy_order_authority_source_missing"):
        require_buy_authority_source({"side": "BUY", "decision_reason_code": "target_delta_rebalance"})


def test_buy_report_requires_authority_source() -> None:
    conn = _conn()
    create_order(
        client_order_id="missing-authority-buy",
        symbol="KRW-BTC",
        side="BUY",
        qty_req=1.0,
        price=100.0,
        strategy_name="daily_participation_sma",
        status="FILLED",
        ts_ms=1_782_120_000_000,
        conn=conn,
    )
    add_fill(
        client_order_id="missing-authority-buy",
        fill_id="missing-authority-buy:fill",
        fill_ts=1_782_120_000_000,
        price=100.0,
        qty=1.0,
        fee=0.1,
        conn=conn,
    )

    report = build_h74_observation_report(conn=conn, days=7)

    row = report["h74_live_trade_classifications"][0]
    assert row["h74_backtest_validation_sample"] is False
    assert row["incident_type"] == "classification_error"
    assert row["classification_error"] == "buy_order_authority_source_missing"
