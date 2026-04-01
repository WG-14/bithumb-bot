from __future__ import annotations

import pytest

from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, get_portfolio_breakdown, set_portfolio_breakdown
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
