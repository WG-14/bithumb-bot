from __future__ import annotations

import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder, BrokerTemporaryError
from bithumb_bot.broker.live import live_execute_signal, validate_order
from bithumb_bot.db_core import ensure_db
from bithumb_bot.recovery import cancel_open_orders_with_broker, reconcile_with_broker
from bithumb_bot.config import settings


class _FakeBroker:
    def __init__(self) -> None:
        self.place_order_calls = 0
        self._last_qty = 0.01
        self._last_side = "BUY"
        self._last_price = None
        self._last_client_order_id = "live_1000_buy"

    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        self.place_order_calls += 1
        self._last_client_order_id = client_order_id
        self._last_side = side
        self._last_qty = qty
        self._last_price = price
        return BrokerOrder(client_order_id, "ex1", side, "NEW", price, qty, 0.0, 1, 1)

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        raise NotImplementedError

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id or "ex1",
            self._last_side,
            "FILLED",
            self._last_price,
            self._last_qty,
            self._last_qty,
            1,
            1,
        )

    def get_open_orders(self) -> list[BrokerOrder]:
        return []

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if client_order_id:
            return [
                BrokerFill(
                    client_order_id=client_order_id,
                    fill_id="f1",
                    fill_ts=1000,
                    price=100000000.0,
                    qty=self._last_qty,
                    fee=10.0,
                )
            ]
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=float(settings.START_CASH_KRW),
            cash_locked=0.0,
            asset_available=0.01,
            asset_locked=0.0,
        )

    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []


class _CommitCheckingBroker(_FakeBroker):
    def __init__(self, *, db_path: str) -> None:
        super().__init__()
        self._db_path = db_path

    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        conn = ensure_db(self._db_path)
        try:
            row = conn.execute(
                "SELECT status FROM orders WHERE client_order_id=?",
                (client_order_id,),
            ).fetchone()
            assert row is not None
            assert row["status"] == "PENDING_SUBMIT"

            event = conn.execute(
                "SELECT 1 FROM order_events WHERE client_order_id=? AND event_type='submit_started'",
                (client_order_id,),
            ).fetchone()
            assert event is not None
        finally:
            conn.close()

        return super().place_order(client_order_id=client_order_id, side=side, qty=qty, price=price)

class _TimeoutBroker(_FakeBroker):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        raise BrokerTemporaryError("timeout")


class _StrayBroker(_FakeBroker):
    def get_open_orders(self) -> list[BrokerOrder]:
        return [BrokerOrder("", "stray1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1)]


class _NoExchangeIdBroker(_FakeBroker):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        self.place_order_calls += 1
        self._last_client_order_id = client_order_id
        self._last_side = side
        self._last_qty = qty
        self._last_price = price
        return BrokerOrder(client_order_id, None, side, "NEW", price, qty, 0.0, 1, 1)




class _CancelOpenOrdersBroker(_FakeBroker):
    def __init__(self) -> None:
        super().__init__()
        self.canceled: list[tuple[str, str | None]] = []

    def get_open_orders(self) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1),
            BrokerOrder("", "stray1", "SELL", "PARTIAL", 110.0, 0.2, 0.05, 1, 1),
        ]

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        self.canceled.append((client_order_id, exchange_order_id))
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "CANCELED", None, 0.0, 0.0, 1, 1)


class _CancelFailureBroker(_CancelOpenOrdersBroker):
    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        if exchange_order_id == "stray1":
            raise RuntimeError("cancel failed")
        return super().cancel_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)


class _StrictRecoveryBroker(_FakeBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        if exchange_order_id is None:
            raise AssertionError("unsafe get_order called without exchange_order_id")
        return super().get_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)


class _RecentActivityBroker(_FakeBroker):
    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="",
                exchange_order_id="ex_recent_known",
                side="BUY",
                status="FILLED",
                price=100.0,
                qty_req=0.01,
                qty_filled=0.01,
                created_ts=1001,
                updated_ts=1002,
            )
        ]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="recent_fill_1",
                fill_ts=1002,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id="ex_recent_known",
            )
        ]


class _UnmatchedRecentFillBroker(_FakeBroker):
    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="recent_fill_unmatched",
                fill_ts=7777,
                price=100000000.0,
                qty=0.02,
                fee=20.0,
                exchange_order_id="stray_recent_ex",
            )
        ]


@pytest.fixture(autouse=True)
def _reset_pretrade_guards():
    old_values = {
        "DB_PATH": settings.DB_PATH,
        "START_CASH_KRW": settings.START_CASH_KRW,
        "BUY_FRACTION": settings.BUY_FRACTION,
        "FEE_RATE": settings.FEE_RATE,
        "MAX_ORDERBOOK_SPREAD_BPS": settings.MAX_ORDERBOOK_SPREAD_BPS,
        "MAX_MARKET_SLIPPAGE_BPS": settings.MAX_MARKET_SLIPPAGE_BPS,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
        "PRETRADE_BALANCE_BUFFER_BPS": settings.PRETRADE_BALANCE_BUFFER_BPS,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "BITHUMB_API_KEY": settings.BITHUMB_API_KEY,
        "BITHUMB_API_SECRET": settings.BITHUMB_API_SECRET,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "KILL_SWITCH": settings.KILL_SWITCH,
        "MAX_OPEN_ORDER_AGE_SEC": settings.MAX_OPEN_ORDER_AGE_SEC,
    }

    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "PRETRADE_BALANCE_BUFFER_BPS", 0.0)
    object.__setattr__(settings, "FEE_RATE", 0.0004)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "KILL_SWITCH", False)

    yield

    for key, value in old_values.items():
        object.__setattr__(settings, key, value)


def test_bithumb_broker_dry_run(monkeypatch):
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "k")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "s")

    broker = BithumbBroker()
    order = broker.place_order(client_order_id="a", side="BUY", qty=0.1, price=None)

    assert order.exchange_order_id.startswith("dry_")


def test_live_execute_idempotent(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "idempotent.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    broker = _FakeBroker()
    t1 = live_execute_signal(broker, "BUY", 1000, 100000000.0)
    t2 = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert t1 is not None
    assert t2 is None


def test_live_open_order_guard_blocks_new_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "open_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    conn = ensure_db(str(tmp_path / "open_guard.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('existing_open','ex_open','NEW','BUY',NULL,0.01,0,999,999,NULL)
        """
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 2000, 100000000.0)
    assert trade is None


def test_live_kill_switch_blocks_new_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "kill_switch.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "KILL_SWITCH", True)

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_live_daily_loss_limit_blocks_new_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "daily_loss.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 1000.0)

    conn = ensure_db(str(tmp_path / "daily_loss.sqlite"))
    conn.execute("INSERT INTO daily_risk(day_kst, start_equity) VALUES ('1970-01-01', 1010000.0)")
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_live_recovery_required_order_blocks_new_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recovery_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    conn = ensure_db(str(tmp_path / "recovery_guard.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('needs_recovery',NULL,'RECOVERY_REQUIRED','BUY',NULL,0.01,0,999,999,'manual recovery required')
        """
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 2000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_live_stale_unresolved_open_order_blocks_new_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "stale_open_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 5)

    conn = ensure_db(str(tmp_path / "stale_open_guard.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('stale_open','ex_open','NEW','BUY',NULL,0.01,0,1,1,NULL)
        """
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 10_000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0




def test_live_submit_intent_is_committed_before_remote_submit(tmp_path):
    db_path = str(tmp_path / "pre_submit_commit.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    trade = live_execute_signal(_CommitCheckingBroker(db_path=db_path), "BUY", 1000, 100000000.0)

    assert trade is not None

def test_live_timeout_marks_submit_unknown(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_TimeoutBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "submit_unknown.sqlite"))
    row = conn.execute("SELECT status, last_error FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "SUBMIT_UNKNOWN"
    assert "submit unknown" in str(row["last_error"])
    assert any("event=order_submit_started" in msg for msg in notifications)
    assert any("event=order_submit_unknown" in msg for msg in notifications)


def test_live_submit_without_exchange_id_marks_recovery_required(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "missing_exchange_id.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_NoExchangeIdBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "missing_exchange_id.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='live_1000_buy'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert any("event=recovery_required_transition" in msg for msg in notifications)


def test_reconcile_updates_portfolio(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "reconcile.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "reconcile.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_FakeBroker())

    conn = ensure_db(str(tmp_path / "reconcile.sqlite"))
    row = conn.execute("SELECT status FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    p = conn.execute("SELECT cash_krw, asset_qty FROM portfolio WHERE id=1").fetchone()
    conn.close()

    assert row["status"] == "FILLED"
    assert float(p["asset_qty"]) == 0.01


def test_reconcile_records_stray_remote_open_order(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "stray.sqlite"))
    reconcile_with_broker(_StrayBroker())

    conn = ensure_db(str(tmp_path / "stray.sqlite"))
    row = conn.execute("SELECT status, exchange_order_id, side FROM orders WHERE client_order_id='remote_stray1'").fetchone()
    conn.close()

    assert row is not None
    assert row["exchange_order_id"] == "stray1"
    assert row["status"] == "NEW"
    assert row["side"] == "BUY"




def test_cancel_open_orders_cancels_remote_and_updates_local(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_open_orders.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_open_orders.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    broker = _CancelOpenOrdersBroker()
    summary = cancel_open_orders_with_broker(broker)

    conn = ensure_db(str(tmp_path / "cancel_open_orders.sqlite"))
    row = conn.execute("SELECT status FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "CANCELED"
    assert summary["remote_open_count"] == 2
    assert summary["canceled_count"] == 2
    assert summary["matched_local_count"] == 1
    assert summary["stray_canceled_count"] == 1
    assert summary["failed_count"] == 0
    assert len(summary["stray_messages"]) == 1


def test_cancel_open_orders_reports_cancel_failures(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_failure.sqlite"))

    summary = cancel_open_orders_with_broker(_CancelFailureBroker())

    assert summary["remote_open_count"] == 2
    assert summary["canceled_count"] == 1
    assert summary["failed_count"] == 1
    assert len(summary["error_messages"]) == 1


def test_reconcile_submit_unknown_without_exchange_id_marks_recovery_required_and_continues(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recovery_required.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "recovery_required.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_2000_buy','ex2','NEW','BUY',NULL,0.01,0,2000,2000,NULL)
        """
    )
    conn.commit()
    conn.close()

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.recovery.notify", lambda msg: notifications.append(msg))

    reconcile_with_broker(_StrictRecoveryBroker())

    conn = ensure_db(str(tmp_path / "recovery_required.sqlite"))
    ambiguous = conn.execute(
        "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    reconciled = conn.execute("SELECT status FROM orders WHERE client_order_id='live_2000_buy'").fetchone()
    conn.close()

    assert ambiguous is not None
    assert ambiguous["status"] == "RECOVERY_REQUIRED"
    assert ambiguous["exchange_order_id"] is None
    assert "manual recovery required" in str(ambiguous["last_error"])
    assert reconciled is not None
    assert reconciled["status"] == "FILLED"
    assert any("event=recovery_required_transition" in msg for msg in notifications)
    assert any(
        "event=reconcile_status_change" in msg and "client_order_id=live_2000_buy" in msg
        for msg in notifications
    )


def test_reconcile_recovers_known_local_order_from_recent_activity(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recent_known.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "recent_known.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_3000_buy','ex_recent_known','NEW','BUY',NULL,0.01,0,3000,3000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_RecentActivityBroker())

    conn = ensure_db(str(tmp_path / "recent_known.sqlite"))
    row = conn.execute(
        "SELECT status, qty_filled FROM orders WHERE client_order_id='live_3000_buy'"
    ).fetchone()
    fill = conn.execute(
        "SELECT fill_id FROM fills WHERE client_order_id='live_3000_buy'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == 0.01
    assert fill is not None


def test_reconcile_unmatched_recent_activity_creates_recovery_required_record(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recent_unmatched.sqlite"))

    reconcile_with_broker(_UnmatchedRecentFillBroker())

    conn = ensure_db(str(tmp_path / "recent_unmatched.sqlite"))
    row = conn.execute(
        """
        SELECT client_order_id, status, exchange_order_id, last_error
        FROM orders
        WHERE exchange_order_id='stray_recent_ex'
        """
    ).fetchone()
    conn.close()

    assert row is not None
    assert str(row["client_order_id"]).startswith("recovery_")
    assert row["status"] == "RECOVERY_REQUIRED"
    assert "manual recovery required" in str(row["last_error"])


def test_validate_order_rejects_invalid_qty():
    try:
        validate_order(signal="BUY", side="BUY", qty=0.0, market_price=100.0)
    except ValueError as e:
        assert "invalid order qty" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_live_invalid_market_price_rejected_before_submit(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "invalid_market.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    broker = _FakeBroker()

    trade = live_execute_signal(broker, "BUY", 1000, 0.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_live_insufficient_available_balance_rejected(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "insufficient_balance.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "BUY_FRACTION", 0.99)
    object.__setattr__(settings, "FEE_RATE", 0.001)
    object.__setattr__(settings, "PRETRADE_BALANCE_BUFFER_BPS", 10.0)

    broker = _FakeBroker()
    monkeypatch.setattr(
        broker,
        "get_balance",
        lambda: BrokerBalance(cash_available=1000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0),
    )

    trade = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0


def test_live_excessive_spread_rejected_before_submit(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "spread_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 5.0)

    from bithumb_bot.broker import live as live_module

    monkeypatch.setattr(live_module, "fetch_orderbook_top", lambda _pair: (100.0, 101.0))

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100.5)

    assert trade is None
    assert broker.place_order_calls == 0