from __future__ import annotations

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing


def test_apply_fill_dedupes_by_fill_id(tmp_path):
    db_path = tmp_path / "fill_dedupe.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 3_000_000.0)

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