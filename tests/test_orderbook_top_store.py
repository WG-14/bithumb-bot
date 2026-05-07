from __future__ import annotations

import pytest

from bithumb_bot.db_core import ensure_db
from bithumb_bot.orderbook_top_store import (
    build_orderbook_top_snapshot,
    compute_spread_bps,
    upsert_orderbook_top_snapshot,
)


def test_ensure_schema_creates_orderbook_top_snapshots(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "quotes.sqlite"))
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='orderbook_top_snapshots'"
        ).fetchone()
        index = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_orderbook_top_pair_ts'"
        ).fetchone()
    finally:
        conn.close()

    assert table is not None
    assert index is not None


def test_valid_orderbook_top_snapshot_insert_and_spread(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "quotes.sqlite"))
    try:
        snapshot = build_orderbook_top_snapshot(
            ts=1_700_000_000_000,
            pair="BTC_KRW",
            bid_price=99.0,
            ask_price=101.0,
            source="bithumb_public_v1_orderbook",
            observed_at_epoch_sec=1_700_000_000.0,
        )
        assert snapshot.pair == "KRW-BTC"
        assert snapshot.spread_bps == pytest.approx(200.0)
        assert upsert_orderbook_top_snapshot(conn, snapshot) == 1
        conn.commit()
        row = conn.execute(
            "SELECT bid_price, ask_price, spread_bps, source FROM orderbook_top_snapshots"
        ).fetchone()
    finally:
        conn.close()

    assert tuple(row) == (99.0, 101.0, 200.0, "bithumb_public_v1_orderbook")


def test_invalid_crossed_quote_fails_before_insert(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "quotes.sqlite"))
    try:
        with pytest.raises(ValueError, match="crossed orderbook top quote"):
            build_orderbook_top_snapshot(
                ts=1,
                pair="KRW-BTC",
                bid_price=101.0,
                ask_price=100.0,
                source="bithumb_public_v1_orderbook",
            )
        count = conn.execute("SELECT COUNT(*) FROM orderbook_top_snapshots").fetchone()[0]
    finally:
        conn.close()

    assert count == 0


def test_invalid_non_positive_quote_fails_before_insert(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "quotes.sqlite"))
    try:
        with pytest.raises(ValueError, match="invalid orderbook top quote"):
            build_orderbook_top_snapshot(
                ts=1,
                pair="KRW-BTC",
                bid_price=0.0,
                ask_price=100.0,
                source="bithumb_public_v1_orderbook",
            )
        count = conn.execute("SELECT COUNT(*) FROM orderbook_top_snapshots").fetchone()[0]
    finally:
        conn.close()

    assert count == 0


def test_spread_bps_is_computed_from_mid_price() -> None:
    assert compute_spread_bps(bid_price=100.0, ask_price=101.0) == pytest.approx((1.0 / 100.5) * 10_000.0)


def test_orderbook_top_upsert_is_deterministic(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "quotes.sqlite"))
    try:
        first = build_orderbook_top_snapshot(
            ts=1,
            pair="KRW-BTC",
            bid_price=99.0,
            ask_price=101.0,
            source="bithumb_public_v1_orderbook",
        )
        second = build_orderbook_top_snapshot(
            ts=1,
            pair="KRW-BTC",
            bid_price=98.0,
            ask_price=102.0,
            source="bithumb_public_v1_orderbook",
        )
        upsert_orderbook_top_snapshot(conn, first)
        upsert_orderbook_top_snapshot(conn, second)
        conn.commit()
        rows = conn.execute(
            "SELECT bid_price, ask_price, spread_bps FROM orderbook_top_snapshots"
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 1
    assert tuple(rows[0]) == (98.0, 102.0, 400.0)
