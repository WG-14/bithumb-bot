from __future__ import annotations

import pytest

from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder
from bithumb_bot.config import settings
from bithumb_bot.db_core import (
    ensure_db,
    get_external_cash_adjustment_summary,
    get_portfolio_breakdown,
    replay_fill_portfolio_snapshot,
    record_external_cash_adjustment,
    set_portfolio_breakdown,
)
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.recovery import reconcile_with_broker


class _AvailableOnlyBalanceBroker:
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex1", "BUY", "NEW", 100.0, 1.0, 0.0, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []

    def get_balance(self) -> BrokerBalance:
        # Simulates a response that only reflects available balances.
        return BrokerBalance(cash_available=1000.0, cash_locked=0.0, asset_available=0.5, asset_locked=0.0)


class _SplitBalanceBroker(_AvailableOnlyBalanceBroker):
    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(cash_available=900.0, cash_locked=100.0, asset_available=0.4, asset_locked=0.1)


def test_partial_fill_sequence_preserves_locked_aware_accounting(tmp_path):
    db_path = tmp_path / "partial_fill.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1000.0)

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="buy1",
            side="BUY",
            qty_req=1.0,
            price=1000.0,
            ts_ms=1,
        )
        set_portfolio_breakdown(
            conn,
            cash_available=100.0,
            cash_locked=900.0,
            asset_available=0.0,
            asset_locked=0.0,
        )

        apply_fill_and_trade(
            conn,
            client_order_id="buy1",
            side="BUY",
            fill_id="f1",
            fill_ts=10,
            price=1000.0,
            qty=0.4,
            fee=0.0,
        )
        apply_fill_and_trade(
            conn,
            client_order_id="buy1",
            side="BUY",
            fill_id="f2",
            fill_ts=20,
            price=1000.0,
            qty=0.6,
            fee=0.0,
        )
        conn.commit()

        cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
        qty_filled = float(
            conn.execute("SELECT qty_filled FROM orders WHERE client_order_id='buy1'").fetchone()["qty_filled"]
        )
    finally:
        conn.close()

    assert cash_available == pytest.approx(0.0)
    assert cash_locked == pytest.approx(0.0)
    assert asset_available == pytest.approx(1.0)
    assert asset_locked == pytest.approx(0.0)
    assert qty_filled == pytest.approx(1.0)


def test_apply_fill_and_trade_tolerates_tiny_negative_cash_available_dust(tmp_path):
    db_path = tmp_path / "tiny_negative_cash_dust.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="buy1",
            side="BUY",
            qty_req=1.0,
            price=1000.0,
            ts_ms=1,
        )
        set_portfolio_breakdown(
            conn,
            cash_available=-1.1641532182693481e-10,
            cash_locked=1_000_000.0,
            asset_available=0.0,
            asset_locked=0.0,
        )

        apply_fill_and_trade(
            conn,
            client_order_id="buy1",
            side="BUY",
            fill_id="f1",
            fill_ts=10,
            price=1_000_000.0,
            qty=1.0,
            fee=0.0,
        )
        conn.commit()

        cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
        qty_filled = float(
            conn.execute("SELECT qty_filled FROM orders WHERE client_order_id='buy1'").fetchone()["qty_filled"]
        )
    finally:
        conn.close()

    assert cash_available == pytest.approx(0.0)
    assert cash_locked == pytest.approx(0.0)
    assert asset_available == pytest.approx(1.0)
    assert asset_locked == pytest.approx(0.0)
    assert qty_filled == pytest.approx(1.0)


def test_apply_fill_and_trade_tolerates_tiny_negative_asset_available_dust(tmp_path):
    db_path = tmp_path / "tiny_negative_asset_dust.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="sell1",
            side="SELL",
            qty_req=1.0,
            price=1_000_000.0,
            ts_ms=1,
        )
        set_portfolio_breakdown(
            conn,
            cash_available=0.0,
            cash_locked=1_000_000.0,
            asset_available=-1.1641532182693481e-10,
            asset_locked=1_000_000.0,
        )

        # Matching owns qty rounding; ledger application only clamps the
        # representational dust that remains after the subtraction.
        apply_fill_and_trade(
            conn,
            client_order_id="sell1",
            side="SELL",
            fill_id="f1",
            fill_ts=10,
            price=1_000_000.0,
            qty=1.0,
            fee=0.0,
        )
        conn.commit()

        cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
        qty_filled = float(
            conn.execute("SELECT qty_filled FROM orders WHERE client_order_id='sell1'").fetchone()["qty_filled"]
        )
    finally:
        conn.close()

    assert cash_available == pytest.approx(1_000_000.0)
    assert cash_locked == pytest.approx(1_000_000.0)
    assert asset_available == pytest.approx(0.0)
    assert asset_locked == pytest.approx(999_999.0)
    assert qty_filled == pytest.approx(1.0)


def test_overfill_raises_and_leaves_ledger_consistent(tmp_path):
    db_path = tmp_path / "overfill.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1000.0)

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="buy1",
            side="BUY",
            qty_req=1.0,
            price=1000.0,
            ts_ms=1,
        )
        set_portfolio_breakdown(
            conn,
            cash_available=0.0,
            cash_locked=1000.0,
            asset_available=0.0,
            asset_locked=0.0,
        )

        apply_fill_and_trade(
            conn,
            client_order_id="buy1",
            side="BUY",
            fill_id="f1",
            fill_ts=10,
            price=1000.0,
            qty=0.8,
            fee=0.0,
        )

        with pytest.raises(RuntimeError, match="overfill detected"):
            apply_fill_and_trade(
                conn,
                client_order_id="buy1",
                side="BUY",
                fill_id="f2",
                fill_ts=20,
                price=1000.0,
                qty=0.3,
                fee=0.0,
            )

        conn.commit()
        fills = conn.execute("SELECT COUNT(*) FROM fills WHERE client_order_id='buy1'").fetchone()[0]
        trades = conn.execute("SELECT COUNT(*) FROM trades WHERE side='BUY'").fetchone()[0]
        qty_filled = float(
            conn.execute("SELECT qty_filled FROM orders WHERE client_order_id='buy1'").fetchone()["qty_filled"]
        )
    finally:
        conn.close()

    assert fills == 1
    assert trades == 1
    assert qty_filled == pytest.approx(0.8)


def test_impossible_sell_fill_raises_on_negative_asset(tmp_path):
    db_path = tmp_path / "negative_asset.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1000.0)

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="sell1",
            side="SELL",
            qty_req=0.5,
            price=1000.0,
            ts_ms=1,
        )
        set_portfolio_breakdown(
            conn,
            cash_available=1000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )

        with pytest.raises(RuntimeError, match="negative asset"):
            apply_fill_and_trade(
                conn,
                client_order_id="sell1",
                side="SELL",
                fill_id="s1",
                fill_ts=10,
                price=1000.0,
                qty=0.2,
                fee=0.0,
            )

        fills = conn.execute("SELECT COUNT(*) FROM fills WHERE client_order_id='sell1'").fetchone()[0]
        trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    finally:
        conn.close()

    assert fills == 0
    assert trades == 0


def test_reconcile_with_open_orders_preserves_local_locked_when_balance_is_available_only(tmp_path):
    db_path = tmp_path / "reconcile_locked.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))

    conn = ensure_db(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
            VALUES ('live_1_buy','ex1','NEW', 'BUY', 100.0, 1.0, 0.0, 1, 1, NULL)
            """
        )
        set_portfolio_breakdown(
            conn,
            cash_available=1000.0,
            cash_locked=250.0,
            asset_available=0.5,
            asset_locked=0.2,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_AvailableOnlyBalanceBroker())

    conn = ensure_db(str(db_path))
    try:
        cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
    finally:
        conn.close()

    assert cash_available == pytest.approx(1000.0)
    assert cash_locked == pytest.approx(250.0)
    assert asset_available == pytest.approx(0.5)
    assert asset_locked == pytest.approx(0.2)


def test_reconcile_with_open_orders_uses_broker_locked_split_when_provided(tmp_path):
    db_path = tmp_path / "reconcile_locked_from_broker.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))

    conn = ensure_db(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
            VALUES ('live_1_buy','ex1','NEW', 'BUY', 100.0, 1.0, 0.0, 1, 1, NULL)
            """
        )
        set_portfolio_breakdown(
            conn,
            cash_available=1000.0,
            cash_locked=250.0,
            asset_available=0.5,
            asset_locked=0.2,
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_SplitBalanceBroker())

    conn = ensure_db(str(db_path))
    try:
        cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
    finally:
        conn.close()

    assert cash_available == pytest.approx(900.0)
    assert cash_locked == pytest.approx(100.0)
    assert asset_available == pytest.approx(0.4)
    assert asset_locked == pytest.approx(0.1)


@pytest.mark.parametrize("fee", [0.0, None])
def test_buy_fill_cash_after_matches_portfolio_available_for_canonical_snapshot(tmp_path, fee):
    db_path = tmp_path / f"buy_snapshot_{'none' if fee is None else 'zero'}.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="buy_snapshot",
            side="BUY",
            qty_req=0.01,
            price=100_000_000.0,
            ts_ms=1,
        )
        trade = apply_fill_and_trade(
            conn,
            client_order_id="buy_snapshot",
            side="BUY",
            fill_id="buy_snapshot_fill",
            fill_ts=10,
            price=100_000_000.0,
            qty=0.01,
            fee=fee,
        )
        portfolio = conn.execute(
            "SELECT cash_krw, cash_available, cash_locked FROM portfolio WHERE id=1"
        ).fetchone()
    finally:
        conn.close()

    assert trade is not None
    assert float(trade["cash"]) == pytest.approx(float(portfolio["cash_available"]))
    assert float(trade["cash"]) == pytest.approx(float(portfolio["cash_krw"]))
    assert float(portfolio["cash_locked"]) == pytest.approx(0.0)


@pytest.mark.parametrize("fee", [0.0, None])
def test_sell_fill_cash_after_matches_portfolio_available_for_canonical_snapshot(tmp_path, fee):
    db_path = tmp_path / "sell_snapshot.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    conn = ensure_db(str(db_path))
    try:
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.01,
            asset_locked=0.0,
        )
        record_order_if_missing(
            conn,
            client_order_id="sell_snapshot",
            side="SELL",
            qty_req=0.01,
            price=100_000_000.0,
            ts_ms=1,
        )
        trade = apply_fill_and_trade(
            conn,
            client_order_id="sell_snapshot",
            side="SELL",
            fill_id="sell_snapshot_fill",
            fill_ts=10,
            price=100_000_000.0,
            qty=0.01,
            fee=fee,
        )
        portfolio = conn.execute(
            "SELECT cash_krw, cash_available, cash_locked FROM portfolio WHERE id=1"
        ).fetchone()
    finally:
        conn.close()

    assert trade is not None
    assert float(trade["cash"]) == pytest.approx(float(portfolio["cash_available"]))
    assert float(trade["cash"]) == pytest.approx(float(portfolio["cash_krw"]))
    assert float(portfolio["cash_locked"]) == pytest.approx(0.0)


def test_replay_fill_portfolio_snapshot_is_fee_and_quantization_stable(tmp_path):
    db_path = tmp_path / "replay_snapshot.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    snapshot = replay_fill_portfolio_snapshot(
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
        rows=(
            ("BUY", 100_000_000.0, 0.0000000000015, None),
            ("SELL", 100_000_000.0, 0.0000000000015, 0.0),
        ),
    )

    cash_available_after, cash_locked_after, asset_available_after, asset_locked_after, cash_after, asset_after = snapshot
    assert cash_available_after == pytest.approx(1_000_000.0)
    assert cash_locked_after == pytest.approx(0.0)
    assert asset_available_after == pytest.approx(0.0)
    assert asset_locked_after == pytest.approx(0.0)
    assert cash_after == pytest.approx(1_000_000.0)
    assert asset_after == pytest.approx(0.0)


def test_fractional_qty_roundtrip_does_not_accumulate_cash_drift(tmp_path):
    db_path = tmp_path / "roundtrip_drift.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    conn = ensure_db(str(db_path))
    try:
        start_cash = 1_000_000.0
        qty = 0.00009777
        price = 102_247_000.0
        for idx in range(5):
            buy_id = f"roundtrip_buy_{idx}"
            sell_id = f"roundtrip_sell_{idx}"
            record_order_if_missing(
                conn,
                client_order_id=buy_id,
                side="BUY",
                qty_req=qty,
                price=price,
                ts_ms=1_000 + idx * 10,
            )
            buy_trade = apply_fill_and_trade(
                conn,
                client_order_id=buy_id,
                side="BUY",
                fill_id=f"{buy_id}_fill",
                fill_ts=1_000 + idx * 10,
                price=price,
                qty=qty,
                fee=0.0,
            )
            record_order_if_missing(
                conn,
                client_order_id=sell_id,
                side="SELL",
                qty_req=qty,
                price=price,
                ts_ms=1_500 + idx * 10,
            )
            sell_trade = apply_fill_and_trade(
                conn,
                client_order_id=sell_id,
                side="SELL",
                fill_id=f"{sell_id}_fill",
                fill_ts=1_500 + idx * 10,
                price=price,
                qty=qty,
                fee=0.0,
            )
            portfolio = conn.execute(
                "SELECT cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
            ).fetchone()
            assert buy_trade is not None
            assert sell_trade is not None
            assert float(buy_trade["cash"]) == pytest.approx(float(conn.execute("SELECT cash_after FROM trades WHERE client_order_id=?", (buy_id,)).fetchone()["cash_after"]))
            assert float(sell_trade["cash"]) == pytest.approx(float(portfolio["cash_available"]))
            assert float(portfolio["cash_available"]) == pytest.approx(start_cash)
            assert float(portfolio["cash_locked"]) == pytest.approx(0.0)
            assert float(portfolio["asset_available"]) == pytest.approx(0.0)
            assert float(portfolio["asset_locked"]) == pytest.approx(0.0)
    finally:
        conn.close()


def test_external_cash_adjustment_preserves_prior_trade_cash_after_snapshot(tmp_path):
    db_path = tmp_path / "external_cash_after_consistency.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    conn = ensure_db(str(db_path))
    try:
        record_order_if_missing(
            conn,
            client_order_id="cash_after_buy",
            side="BUY",
            qty_req=0.001,
            price=100_000_000.0,
            ts_ms=1,
        )
        trade = apply_fill_and_trade(
            conn,
            client_order_id="cash_after_buy",
            side="BUY",
            fill_id="cash_after_buy_fill",
            fill_ts=10,
            price=100_000_000.0,
            qty=0.001,
            fee=50.0,
        )
        before_adjustment_cash_after = float(
            conn.execute("SELECT cash_after FROM trades WHERE client_order_id='cash_after_buy'").fetchone()["cash_after"]
        )

        record_external_cash_adjustment(
            conn,
            event_ts=20,
            currency="KRW",
            delta_amount=-30.0,
            source="bank_transfer_fee",
            reason="deposit_fee",
            broker_snapshot_basis={
                "balance_source": "manual",
                "broker_cash_total": before_adjustment_cash_after - 30.0,
                "local_cash_total": before_adjustment_cash_after,
            },
            note="deposit fee bookkeeping",
            adjustment_key="bank_transfer_fee:deposit_fee:1",
        )
        portfolio = conn.execute(
            "SELECT cash_krw, cash_available, cash_locked FROM portfolio WHERE id=1"
        ).fetchone()
        summary = get_external_cash_adjustment_summary(conn)
    finally:
        conn.close()

    assert trade is not None
    assert float(trade["cash"]) == pytest.approx(before_adjustment_cash_after)
    assert float(portfolio["cash_krw"]) == pytest.approx(before_adjustment_cash_after - 30.0)
    assert float(portfolio["cash_available"]) == pytest.approx(before_adjustment_cash_after - 30.0)
    assert float(portfolio["cash_locked"]) == pytest.approx(0.0)
    assert summary["adjustment_count"] == 1
    assert summary["adjustment_total"] == pytest.approx(-30.0)
