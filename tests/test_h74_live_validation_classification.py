from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from bithumb_bot.db_core import ensure_schema
from bithumb_bot.h74_observation_report import build_h74_observation_report
from bithumb_bot.live_trade_classification import classify_h74_live_trade
from bithumb_bot.oms import add_fill, create_order


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    return conn


def _ts(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _seed_buy(
    conn: sqlite3.Connection,
    client_order_id: str,
    *,
    created_ts: str,
    decision_reason_code: str,
    authority_source: str,
    entry_authority_source: str,
    entry_authority_status: str = "ALLOW",
    decision_kst_hour: int,
) -> None:
    create_order(
        client_order_id=client_order_id,
        symbol="KRW-BTC",
        side="BUY",
        qty_req=1.0,
        price=100.0,
        strategy_name="daily_participation_sma",
        strategy_instance_id="h74:one",
        decision_reason_code=decision_reason_code,
        intent_type="target_delta_rebalance",
        authority_source=authority_source,
        entry_authority_source=entry_authority_source,
        entry_authority_status=entry_authority_status,
        decision_kst_hour=decision_kst_hour,
        status="FILLED",
        ts_ms=_ts(created_ts),
        conn=conn,
    )
    add_fill(
        client_order_id=client_order_id,
        fill_id=f"{client_order_id}:fill",
        fill_ts=_ts(created_ts),
        price=100.0,
        qty=1.0,
        fee=0.1,
        conn=conn,
    )


def test_daily_participation_fill_is_validation_sample() -> None:
    classified = classify_h74_live_trade(
        {
            "side": "BUY",
            "filled": True,
            "decision_reason_code": "daily_participation_fallback_allowed",
            "authority_source": "daily_participation_entry",
            "decision_kst_hour": 10,
        }
    )

    assert classified["h74_backtest_validation_sample"] is True
    assert classified["incident_type"] == "none"


def test_out_of_window_target_delta_fill_is_incident_not_sample() -> None:
    classified = classify_h74_live_trade(
        {
            "side": "BUY",
            "filled": True,
            "decision_reason_code": "target_delta_rebalance",
            "authority_source": "target_delta",
            "entry_authority_status": "ALLOW",
            "decision_kst_hour": 18,
        }
    )

    assert classified["h74_backtest_validation_sample"] is False
    assert classified["incident_type"] == "out_of_window_target_delta_entry"


def test_performance_report_excludes_incident_samples() -> None:
    conn = _conn()
    _seed_buy(
        conn,
        "daily-buy",
        created_ts="2026-06-22T01:00:00Z",
        decision_reason_code="daily_participation_fallback_allowed",
        authority_source="daily_participation_entry",
        entry_authority_source="daily_participation_entry",
        decision_kst_hour=10,
    )
    _seed_buy(
        conn,
        "live_1782120240000_buy_ae16bfbd",
        created_ts="2026-06-22T09:00:00Z",
        decision_reason_code="target_delta_rebalance",
        authority_source="target_delta",
        entry_authority_source="target_delta",
        decision_kst_hour=18,
    )

    report = build_h74_observation_report(
        conn=conn,
        days=7,
        now=datetime(2026, 6, 23, tzinfo=timezone.utc),
    )
    rows = {
        str(row["client_order_id"]): row
        for row in report["h74_live_trade_classifications"]
    }

    assert report["h74_backtest_validation_sample_count"] == 1
    assert rows["daily-buy"]["h74_backtest_validation_sample"] is True
    assert rows["live_1782120240000_buy_ae16bfbd"]["h74_backtest_validation_sample"] is False
    assert rows["live_1782120240000_buy_ae16bfbd"]["incident_type"] == "out_of_window_target_delta_entry"


def test_report_row_contains_incident_type() -> None:
    conn = _conn()
    _seed_buy(
        conn,
        "incident-buy",
        created_ts="2026-06-22T09:00:00Z",
        decision_reason_code="target_delta_rebalance",
        authority_source="target_delta",
        entry_authority_source="target_delta",
        decision_kst_hour=18,
    )

    report = build_h74_observation_report(
        conn=conn,
        days=7,
        now=datetime(2026, 6, 23, tzinfo=timezone.utc),
    )

    assert report["h74_live_trade_classifications"][0]["incident_type"] == "out_of_window_target_delta_entry"
