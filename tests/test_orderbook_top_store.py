from __future__ import annotations

import pytest

from bithumb_bot.db_core import ensure_db
from bithumb_bot.orderbook_top_store import (
    build_orderbook_top_snapshot,
    compute_spread_bps,
    upsert_orderbook_top_snapshot,
)
from bithumb_bot.orderbook_depth_store import (
    build_orderbook_depth_snapshot,
    depth_snapshot_from_orderbook_snapshot,
    has_orderbook_depth_evidence,
    load_orderbook_depth_snapshot_after_or_equal,
    upsert_orderbook_depth_snapshot,
)
from bithumb_bot.public_api_orderbook import OrderbookSnapshot, OrderbookUnit


def test_ensure_schema_creates_orderbook_top_snapshots(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "quotes.sqlite"))
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='orderbook_top_snapshots'"
        ).fetchone()
        index = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_orderbook_top_pair_ts'"
        ).fetchone()
        depth_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='orderbook_depth_levels'"
        ).fetchone()
        depth_index = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_orderbook_depth_pair_ts'"
        ).fetchone()
    finally:
        conn.close()

    assert table is not None
    assert index is not None
    assert depth_table is not None
    assert depth_index is not None


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


def test_depth_snapshot_from_orderbook_snapshot_requires_size_fields() -> None:
    snapshot = OrderbookSnapshot(
        market="KRW-BTC",
        orderbook_units=(OrderbookUnit(bid_price=100.0, ask_price=101.0),),
    )

    assert depth_snapshot_from_orderbook_snapshot(ts=1, snapshot=snapshot) is None


def test_valid_depth_snapshot_insert_and_load(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "quotes.sqlite"))
    try:
        snapshot = build_orderbook_depth_snapshot(
            ts=1_700_000_000_000,
            pair="BTC_KRW",
            bid_levels=[(100.0, 2.0), (99.0, 3.0)],
            ask_levels=[(101.0, 1.5), (102.0, 4.0)],
            source="bithumb_public_v1_orderbook",
            observed_at_epoch_sec=1_700_000_000.0,
        )
        assert snapshot.pair == "KRW-BTC"
        assert snapshot.bids[1].cumulative_size == pytest.approx(5.0)
        assert snapshot.asks[1].cumulative_notional == pytest.approx((101.0 * 1.5) + (102.0 * 4.0))
        assert upsert_orderbook_depth_snapshot(conn, snapshot) == 4
        conn.commit()

        assert has_orderbook_depth_evidence(conn, pair="KRW-BTC", start_ts=1, end_ts=2_000_000_000_000) is True
        loaded = load_orderbook_depth_snapshot_after_or_equal(
            conn,
            pair="KRW-BTC",
            target_ts=1_700_000_000_000,
            max_wait_ms=1000,
        )
    finally:
        conn.close()

    assert loaded is not None
    assert [level.price for level in loaded.bids] == [100.0, 99.0]
    assert [level.size for level in loaded.asks] == [1.5, 4.0]


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"bid_levels": [(101.0, 1.0)], "ask_levels": [(100.0, 1.0)]}, "crossed orderbook depth"),
        ({"bid_levels": [(100.0, -1.0)], "ask_levels": [(101.0, 1.0)]}, "invalid orderbook depth size"),
        ({"bid_levels": [(float("nan"), 1.0)], "ask_levels": [(101.0, 1.0)]}, "invalid orderbook depth price"),
        ({"bid_levels": [(99.0, 1.0), (100.0, 1.0)], "ask_levels": [(101.0, 1.0)]}, "bid depth levels"),
        ({"bid_levels": [(100.0, 1.0)], "ask_levels": [(102.0, 1.0), (101.0, 1.0)]}, "ask depth levels"),
    ],
)
def test_invalid_depth_snapshot_fails_before_insert(tmp_path, kwargs, match) -> None:
    conn = ensure_db(str(tmp_path / "quotes.sqlite"))
    try:
        with pytest.raises(ValueError, match=match):
            build_orderbook_depth_snapshot(
                ts=1,
                pair="KRW-BTC",
                source="bithumb_public_v1_orderbook",
                **kwargs,
            )
        count = conn.execute("SELECT COUNT(*) FROM orderbook_depth_levels").fetchone()[0]
    finally:
        conn.close()

    assert count == 0
