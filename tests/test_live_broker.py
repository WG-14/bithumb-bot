from __future__ import annotations

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder, BrokerTemporaryError
from bithumb_bot.broker.live import live_execute_signal, validate_order
from bithumb_bot.db_core import ensure_db
from bithumb_bot.recovery import reconcile_with_broker
from bithumb_bot.config import settings


class _FakeBroker:
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, "ex1", side, "NEW", price, qty, 0.0, 1, 1)

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        raise NotImplementedError

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex1", "BUY", "FILLED", None, 0.01, 0.01, 1, 1)

    def get_open_orders(self) -> list[BrokerOrder]:
        return []

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if client_order_id:
            return [BrokerFill(client_order_id=client_order_id, fill_id="f1", fill_ts=1000, price=100000000.0, qty=0.01, fee=10.0)]
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(cash_available=500000.0, cash_locked=0.0, asset_available=0.01, asset_locked=0.0)


class _TimeoutBroker(_FakeBroker):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        raise BrokerTemporaryError("timeout")


class _StrayBroker(_FakeBroker):
    def get_open_orders(self) -> list[BrokerOrder]:
        return [BrokerOrder("", "stray1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1)]


class _NoExchangeIdBroker(_FakeBroker):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, None, side, "NEW", price, qty, 0.0, 1, 1)


class _StrictRecoveryBroker(_FakeBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        if exchange_order_id is None:
            raise AssertionError("unsafe get_order called without exchange_order_id")
        return super().get_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)


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


def test_live_timeout_marks_submit_unknown(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    trade = live_execute_signal(_TimeoutBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "submit_unknown.sqlite"))
    row = conn.execute("SELECT status, last_error FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "SUBMIT_UNKNOWN"
    assert "submit unknown" in str(row["last_error"])


def test_live_submit_without_exchange_id_marks_recovery_required(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "missing_exchange_id.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

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


def test_reconcile_updates_portfolio(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "reconcile.sqlite"))
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


def test_reconcile_submit_unknown_without_exchange_id_marks_recovery_required_and_continues(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recovery_required.sqlite"))
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


def test_validate_order_rejects_invalid_qty():
    try:
        validate_order(signal="BUY", side="BUY", qty=0.0, market_price=100.0)
    except ValueError as e:
        assert "invalid order qty" in str(e)
    else:
        raise AssertionError("expected ValueError")
