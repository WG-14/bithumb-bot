from __future__ import annotations

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.execution import apply_fill_and_trade, order_fill_tolerance, record_order_if_missing
import bithumb_bot.execution as execution_module


def test_apply_fill_dedupes_by_fill_id_and_notifies_once(tmp_path, monkeypatch):
    db_path = tmp_path / "fill_dedupe.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 3_000_000.0)

    notifications: list[str] = []
    monkeypatch.setattr(execution_module, "notify", lambda msg: notifications.append(msg))

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="o1",
            side="BUY",
            qty_req=0.02,
            price=100000000.0,
            ts_ms=1000,
        )

        r1 = apply_fill_and_trade(
            conn,
            client_order_id="o1",
            side="BUY",
            fill_id="fill-1",
            fill_ts=1000,
            price=100000000.0,
            qty=0.02,
            fee=20.0,
        )
        r2 = apply_fill_and_trade(
            conn,
            client_order_id="o1",
            side="BUY",
            fill_id="fill-1",
            fill_ts=1001,
            price=100000001.0,
            qty=0.02,
            fee=20.0,
        )

        conn.commit()

        fills = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE client_order_id='o1'"
        ).fetchone()[0]
        trades = conn.execute(
            "SELECT COUNT(*) FROM trades"
        ).fetchone()[0]
    finally:
        conn.close()

    assert r1 is not None
    assert r2 is None
    assert fills == 1
    assert trades == 1
    assert len(notifications) == 1
    assert "event=fill_applied" in notifications[0]
    assert "client_order_id=o1" in notifications[0]
    assert "fill_id=fill-1" in notifications[0]


def test_apply_fill_dedupes_aggregate_snapshot_after_detailed_fill(tmp_path, monkeypatch):
    db_path = tmp_path / "fill_aggregate_dedupe.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 3_000_000.0)

    notifications: list[str] = []
    monkeypatch.setattr(execution_module, "notify", lambda msg: notifications.append(msg))

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="o-agg",
            side="BUY",
            qty_req=0.00009778,
            price=100000000.0,
            ts_ms=1000,
        )

        r1 = apply_fill_and_trade(
            conn,
            client_order_id="o-agg",
            side="BUY",
            fill_id="trade-fill-1",
            fill_ts=1000,
            price=100000000.0,
            qty=0.00009777,
            fee=0.0,
        )
        r2 = apply_fill_and_trade(
            conn,
            client_order_id="o-agg",
            side="BUY",
            fill_id="ex-order:aggregate:1001",
            fill_ts=1001,
            price=100000000.0,
            qty=0.00009777,
            fee=0.0,
        )
        conn.commit()

        fills = conn.execute(
            "SELECT COUNT(*) AS c FROM fills WHERE client_order_id='o-agg'"
        ).fetchone()["c"]
        qty_filled = conn.execute(
            "SELECT qty_filled FROM orders WHERE client_order_id='o-agg'"
        ).fetchone()["qty_filled"]
    finally:
        conn.close()

    assert r1 is not None
    assert r2 is None
    assert fills == 1
    assert float(qty_filled) == 0.00009777
    assert len(notifications) == 1


def test_apply_fill_allows_small_requested_qty_precision_gap(tmp_path):
    db_path = tmp_path / "fill_precision_gap.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 3_000_000.0)

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="o-precision",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=1000,
        )

        qty = 1.0 + (order_fill_tolerance(1.0) * 0.25)
        result = apply_fill_and_trade(
            conn,
            client_order_id="o-precision",
            side="BUY",
            fill_id="fill-precision",
            fill_ts=1001,
            price=100.0,
            qty=qty,
            fee=0.0,
        )
        conn.commit()

        qty_filled = conn.execute(
            "SELECT qty_filled FROM orders WHERE client_order_id='o-precision'"
        ).fetchone()["qty_filled"]
    finally:
        conn.close()

    assert result is not None
    assert float(qty_filled) == qty


def test_apply_fill_rejects_non_positive_price_without_partial_commit(tmp_path):
    db_path = tmp_path / "fill_invalid_price.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 3_000_000.0)

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="o-invalid-price",
            side="SELL",
            qty_req=0.1,
            price=100.0,
            ts_ms=1000,
        )

        with pytest.raises(RuntimeError, match="invalid fill price"):
            apply_fill_and_trade(
                conn,
                client_order_id="o-invalid-price",
                side="SELL",
                fill_id="fill-invalid-price",
                fill_ts=1001,
                price=0.0,
                qty=0.1,
                fee=0.0,
            )

        conn.commit()

        fill_count = conn.execute("SELECT COUNT(*) FROM fills WHERE client_order_id='o-invalid-price'").fetchone()[0]
        trade_count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        qty_filled = conn.execute(
            "SELECT qty_filled FROM orders WHERE client_order_id='o-invalid-price'"
        ).fetchone()["qty_filled"]
    finally:
        conn.close()

    assert fill_count == 0
    assert trade_count == 0
    assert float(qty_filled) == 0.0
