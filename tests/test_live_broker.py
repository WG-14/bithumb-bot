from __future__ import annotations

import json
import time

import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder, BrokerTemporaryError
from bithumb_bot.broker.live import live_execute_signal, normalize_order_qty, validate_order, validate_pretrade
from bithumb_bot.oms import payload_fingerprint
from bithumb_bot.db_core import ensure_db, set_portfolio_breakdown
from bithumb_bot.recovery import cancel_open_orders_with_broker, reconcile_with_broker, recover_order_with_exchange_id
from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from tests.fakes import FakeMarketData


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
            preflight = conn.execute(
                "SELECT 1 FROM order_events WHERE client_order_id=? AND event_type='submit_attempt_preflight'",
                (client_order_id,),
            ).fetchone()
            assert preflight is not None
        finally:
            conn.close()

        return super().place_order(client_order_id=client_order_id, side=side, qty=qty, price=price)


class _TimeoutBroker(_FakeBroker):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        raise BrokerTemporaryError("timeout")


class _FailingSubmitBroker(_FakeBroker):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        raise RuntimeError("exchange rejected")


class _TransportErrorBroker(_FakeBroker):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        raise BrokerTemporaryError("connection reset by peer")


class _CanceledBroker(_FakeBroker):
    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id or "ex1",
            self._last_side,
            "CANCELED",
            self._last_price,
            self._last_qty,
            0.0,
            1,
            1,
        )

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


class _ConflictingRecoverySourcesBroker(_FakeBroker):
    def get_open_orders(self) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_conflict", "BUY", "NEW", 100.0, 0.01, 0.0, 1001, 1002)
        ]

    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_conflict", "BUY", "FILLED", 100.0, 0.01, 0.01, 1001, 1003)
        ]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="recent_fill_conflict",
                fill_ts=1003,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id="ex_conflict",
            )
        ]


class _OpenOrderPreferredBroker(_FakeBroker):
    def get_open_orders(self) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_precedence", "BUY", "NEW", 100.0, 0.02, 0.0, 2001, 2002)
        ]

    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_precedence", "BUY", "PARTIAL", 100.0, 0.02, 0.01, 2001, 2003)
        ]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []


class _BalanceMismatchBroker(_FakeBroker):
    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=900000.0,
            cash_locked=20000.0,
            asset_available=0.2,
            asset_locked=0.1,
        )


class _SubmitUnknownRecentFillBroker(_StrictRecoveryBroker):
    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="ambiguous_missing_exid",
                fill_id="recent_submit_unknown_fill",
                fill_ts=1003,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id="ex_submit_unknown_fill",
            )
        ]


class _SubmitUnknownRecentOrderBroker(_StrictRecoveryBroker):
    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="ambiguous_missing_exid",
                exchange_order_id="ex_submit_unknown_order",
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=0.01,
                qty_filled=0.0,
                created_ts=1001,
                updated_ts=1002,
            )
        ]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []




@pytest.fixture(autouse=True)
def _fake_market_data(monkeypatch):
    fake = FakeMarketData()
    from bithumb_bot.broker import live as live_module

    monkeypatch.setattr(live_module, "fetch_orderbook_top", fake.fetch_orderbook_top)
    return fake

@pytest.fixture(autouse=True)
def _reset_pretrade_guards():
    old_values = {
        "DB_PATH": settings.DB_PATH,
        "START_CASH_KRW": settings.START_CASH_KRW,
        "BUY_FRACTION": settings.BUY_FRACTION,
        "FEE_RATE": settings.FEE_RATE,
        "LIVE_FEE_RATE_ESTIMATE": settings.LIVE_FEE_RATE_ESTIMATE,
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
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS": settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS,
        "LIVE_PRICE_REFERENCE_MAX_AGE_SEC": settings.LIVE_PRICE_REFERENCE_MAX_AGE_SEC,
    }

    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "PRETRADE_BALANCE_BUFFER_BPS", 0.0)
    object.__setattr__(settings, "FEE_RATE", 0.0004)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0025)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_REFERENCE_MAX_AGE_SEC", 0)

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


def test_validate_pretrade_price_protection_buy_within_threshold() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)

    validate_pretrade(
        broker=_FakeBroker(),
        side="BUY",
        qty=0.001,
        market_price=100.5,
    )


def test_validate_pretrade_price_protection_buy_beyond_threshold() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 40.0)

    with pytest.raises(ValueError, match="price protection blocked BUY"):
        validate_pretrade(
            broker=_FakeBroker(),
            side="BUY",
            qty=0.001,
            market_price=100.5,
        )


def test_validate_pretrade_price_protection_sell_within_threshold() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)

    validate_pretrade(
        broker=_FakeBroker(),
        side="SELL",
        qty=0.001,
        market_price=100.5,
    )


def test_validate_pretrade_price_protection_sell_beyond_threshold() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 40.0)

    with pytest.raises(ValueError, match="price protection blocked SELL"):
        validate_pretrade(
            broker=_FakeBroker(),
            side="SELL",
            qty=0.001,
            market_price=100.5,
        )


def test_validate_pretrade_price_protection_blocks_missing_reference_price(monkeypatch) -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)
    monkeypatch.setattr("bithumb_bot.broker.live.fetch_orderbook_top", lambda _pair: (_ for _ in ()).throw(RuntimeError("no data")))

    with pytest.raises(ValueError, match="reference price unavailable"):
        validate_pretrade(
            broker=_FakeBroker(),
            side="BUY",
            qty=0.001,
            market_price=100.5,
        )


def test_validate_pretrade_price_protection_uses_fresh_quote_age_not_last_candle_age() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)
    object.__setattr__(settings, "LIVE_PRICE_REFERENCE_MAX_AGE_SEC", 5)
    runtime_state.set_last_candle_age_sec(30.0)

    validate_pretrade(
        broker=_FakeBroker(),
        side="BUY",
        qty=0.001,
        market_price=100.5,
    )

    runtime_state.set_last_candle_age_sec(None)


def test_validate_pretrade_price_protection_blocks_stale_quote_timestamp() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 60.0)
    object.__setattr__(settings, "LIVE_PRICE_REFERENCE_MAX_AGE_SEC", 5)

    with pytest.raises(ValueError, match="reference price stale"):
        validate_pretrade(
            broker=_FakeBroker(),
            side="BUY",
            qty=0.001,
            market_price=100.5,
            reference_bid=100.0,
            reference_ask=101.0,
            reference_ts_epoch_sec=time.time() - 30.0,
            reference_source="test_quote",
        )


def test_live_duplicate_intent_after_cancel_is_skipped_by_submit_dedup(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "retry_after_cancel.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    from bithumb_bot.broker import live as live_module

    attempt_ids = iter(["attempt_a", "attempt_b"])
    monkeypatch.setattr(live_module, "_submit_attempt_id", lambda: next(attempt_ids))
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    broker = _CanceledBroker()
    first = live_execute_signal(broker, "BUY", 1000, 100000000.0)
    second = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert first is None
    assert second is None
    assert broker.place_order_calls == 1

    conn = ensure_db(str(tmp_path / "retry_after_cancel.sqlite"))
    try:
        rows = conn.execute(
            """
            SELECT client_order_id, submit_attempt_id, status
            FROM orders
            WHERE client_order_id LIKE 'live_1000_buy_%'
            ORDER BY id
            """
        ).fetchall()
        dedup_row = conn.execute(
            """
            SELECT client_order_id, order_status
            FROM order_intent_dedup
            WHERE symbol='BTC_KRW' AND side='BUY' AND intent_ts=1000
            """
        ).fetchone()
    finally:
        conn.close()

    assert len(rows) == 1
    assert rows[0]["status"] == "CANCELED"
    assert rows[0]["submit_attempt_id"] == "attempt_a"
    assert dedup_row is not None
    assert dedup_row["client_order_id"] == rows[0]["client_order_id"]
    assert dedup_row["order_status"] == "CANCELED"
    assert any("event=order_intent_dedup_skip" in msg for msg in notifications)


def test_live_failed_before_send_releases_dedup_for_same_intent_retry(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "terminal_resubmit_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    from bithumb_bot.broker import live as live_module

    attempt_ids = iter(["attempt_fail", "attempt_retry"])
    monkeypatch.setattr(live_module, "_submit_attempt_id", lambda: next(attempt_ids))

    failed_trade = live_execute_signal(_FailingSubmitBroker(), "BUY", 1000, 100000000.0)
    retried_trade = live_execute_signal(_FakeBroker(), "BUY", 1000, 100000000.0)

    assert failed_trade is None
    assert retried_trade is not None

    conn = ensure_db(str(tmp_path / "terminal_resubmit_guard.sqlite"))
    try:
        rows = conn.execute(
            """
            SELECT client_order_id, submit_attempt_id, status
            FROM orders
            WHERE client_order_id LIKE 'live_1000_buy_%'
            ORDER BY id
            """
        ).fetchall()
        dedup_row = conn.execute(
            """
            SELECT client_order_id, order_status
            FROM order_intent_dedup
            WHERE symbol='BTC_KRW' AND side='BUY' AND intent_ts=1000
            """
        ).fetchone()
    finally:
        conn.close()

    assert len(rows) == 2
    assert rows[0]["status"] == "FAILED"
    assert rows[0]["submit_attempt_id"] == "attempt_fail"
    assert rows[1]["status"] == "FILLED"
    assert rows[1]["submit_attempt_id"] == "attempt_retry"
    assert dedup_row is not None
    assert dedup_row["client_order_id"] == rows[1]["client_order_id"]
    assert dedup_row["order_status"] == "FILLED"

def test_live_submit_unknown_unresolved_blocks_and_persists_reason(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_gate.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    conn = ensure_db(str(tmp_path / "submit_unknown_gate.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('unknown_open',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,999,999,'submit timeout')
        """
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 2000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "submit_unknown_gate.sqlite"))
    blocked = conn.execute(
        """
        SELECT message
        FROM order_events
        WHERE client_order_id LIKE 'live_2000_buy_%' AND event_type='submit_blocked'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert blocked is not None
    assert "code=SUBMIT_UNKNOWN_PRESENT" in str(blocked["message"])
    assert any("event=order_submit_blocked" in msg and "reason_code=RISKY_ORDER_BLOCK" in msg and "submit_attempt_id=" in msg for msg in notifications)


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




def test_live_runtime_halt_blocks_new_order_submission(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "runtime_halt.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    runtime_state.enter_halt(
        reason_code="MANUAL_PAUSE",
        reason="manual operator pause",
        unresolved=False,
    )

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0
    assert any("event=order_submit_blocked" in msg and "status=HALTED" in msg and "timestamp=" in msg for msg in notifications)

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


def test_live_recovery_required_order_blocks_new_order(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "recovery_guard.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

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

    conn = ensure_db(str(tmp_path / "recovery_guard.sqlite"))
    blocked = conn.execute(
        """
        SELECT message
        FROM order_events
        WHERE client_order_id LIKE 'live_2000_buy_%' AND event_type='submit_blocked'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert blocked is not None
    assert "code=RECOVERY_REQUIRED_PRESENT" in str(blocked["message"])
    assert any("event=order_submit_blocked" in msg for msg in notifications)


def test_live_unresolved_open_order_blocks_and_persists_reason(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "open_guard_persisted.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    now_ms = int(time.time() * 1000)
    conn = ensure_db(str(tmp_path / "open_guard_persisted.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('existing_open','ex_open','NEW','BUY',NULL,0.01,0,?,?,NULL)
        """,
        (now_ms, now_ms),
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 2000, 100000000.0)

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "open_guard_persisted.sqlite"))
    blocked = conn.execute(
        """
        SELECT message
        FROM order_events
        WHERE client_order_id LIKE 'live_2000_buy_%' AND event_type='submit_blocked'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert blocked is not None
    assert "code=UNRESOLVED_OPEN_ORDER_PRESENT" in str(blocked["message"])
    assert any("event=order_submit_blocked" in msg for msg in notifications)


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


def test_live_success_persists_submit_attempt_record(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_success.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    trade = live_execute_signal(_FakeBroker(), "BUY", 1000, 100000000.0)
    assert trade is not None

    conn = ensure_db(str(tmp_path / "submit_success.sqlite"))
    row = conn.execute(
        "SELECT client_order_id FROM orders WHERE client_order_id LIKE 'live_1000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, price, submit_ts, payload_fingerprint, broker_response_summary, submission_reason_code, exception_class, timeout_flag, submit_evidence, exchange_order_id_obtained, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    preflight = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, price, submit_ts, payload_fingerprint, submit_evidence, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_preflight'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert submit_attempt is not None
    assert preflight is not None
    assert submit_attempt["submit_attempt_id"]
    assert submit_attempt["symbol"] == settings.PAIR
    assert submit_attempt["side"] == "BUY"
    assert float(submit_attempt["qty"]) > 0
    assert submit_attempt["price"] is None or float(submit_attempt["price"]) > 0
    assert int(submit_attempt["submit_ts"]) == 1000
    assert submit_attempt["payload_fingerprint"]
    assert "exchange_order_id=ex1" in str(submit_attempt["broker_response_summary"])
    assert submit_attempt["exception_class"] is None
    assert submit_attempt["timeout_flag"] == 0
    assert submit_attempt["exchange_order_id_obtained"] == 1
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["symbol"] == settings.PAIR
    assert submit_evidence["side"] == "BUY"
    assert float(submit_evidence["intended_qty"]) > 0
    assert submit_evidence["submit_path"] == "live_standard_market"
    assert submit_evidence["submit_mode"] == settings.MODE
    assert submit_evidence["request_ts"] is not None
    assert submit_evidence["response_ts"] is not None
    assert int(submit_evidence["response_ts"]) >= int(submit_evidence["request_ts"])
    preflight_evidence = json.loads(str(preflight["submit_evidence"]))
    assert preflight_evidence["request_ts"] is None
    assert preflight_evidence["response_ts"] is None
    assert preflight["submit_attempt_id"] == submit_attempt["submit_attempt_id"]
    assert preflight["symbol"] == settings.PAIR
    assert preflight["side"] == "BUY"
    assert float(preflight["qty"]) > 0
    assert preflight["price"] is None or float(preflight["price"]) > 0
    assert int(preflight["submit_ts"]) == 1000
    assert preflight["order_status"] == "PENDING_SUBMIT"
    expected_fp = payload_fingerprint(
        {
            "client_order_id": row["client_order_id"],
            "submit_attempt_id": submit_attempt["submit_attempt_id"],
            "symbol": settings.PAIR,
            "side": "BUY",
            "qty": float(submit_attempt["qty"]),
            "price": (float(preflight["price"]) if preflight["price"] is not None else None),
            "submit_ts": 1000,
        }
    )
    assert preflight["payload_fingerprint"] == expected_fp
    assert submit_attempt["payload_fingerprint"] == expected_fp


def test_live_timeout_marks_submit_unknown(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_TimeoutBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "submit_unknown.sqlite"))
    row = conn.execute("SELECT client_order_id, status, last_error FROM orders WHERE client_order_id LIKE 'live_1000_buy_%' ORDER BY id DESC LIMIT 1").fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, price, submit_ts, payload_fingerprint, broker_response_summary, submission_reason_code, exception_class, timeout_flag, submit_evidence, exchange_order_id_obtained, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    transition = conn.execute(
        """
        SELECT message, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='status_transition'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    preflight = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, submit_ts, payload_fingerprint, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_preflight'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "SUBMIT_UNKNOWN"
    assert "submit unknown" in str(row["last_error"])
    assert submit_attempt is not None
    assert submit_attempt["submit_attempt_id"]
    assert submit_attempt["symbol"] == settings.PAIR
    assert submit_attempt["side"] == "BUY"
    assert float(submit_attempt["qty"]) > 0
    assert submit_attempt["price"] is None or float(submit_attempt["price"]) > 0
    assert int(submit_attempt["submit_ts"]) == 1000
    assert submit_attempt["payload_fingerprint"]
    assert "submit_exception=BrokerTemporaryError" in str(submit_attempt["broker_response_summary"])
    assert submit_attempt["exception_class"] == "BrokerTemporaryError"
    assert submit_attempt["submission_reason_code"] == "sent_but_response_timeout"
    assert submit_attempt["timeout_flag"] == 1
    assert submit_attempt["exchange_order_id_obtained"] == 0
    assert submit_attempt["order_status"] == "SUBMIT_UNKNOWN"
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["submit_path"] == "live_standard_market"
    assert submit_evidence["error_class"] == "BrokerTemporaryError"
    assert "timeout" in str(submit_evidence["error_summary"])
    assert int(submit_evidence["response_ts"]) >= int(submit_evidence["request_ts"])
    assert preflight is not None
    assert preflight["submit_attempt_id"] == submit_attempt["submit_attempt_id"]
    assert preflight["order_status"] == "PENDING_SUBMIT"
    assert preflight["payload_fingerprint"] == submit_attempt["payload_fingerprint"]
    assert transition is not None
    assert transition["order_status"] == "SUBMIT_UNKNOWN"
    assert "from=PENDING_SUBMIT" in str(transition["message"])
    assert "to=SUBMIT_UNKNOWN" in str(transition["message"])
    assert any("event=order_submit_started" in msg for msg in notifications)
    assert any("event=order_submit_unknown" in msg and "reason_code=SUBMIT_TIMEOUT" in msg and "state_to=SUBMIT_UNKNOWN" in msg for msg in notifications)


def test_live_submit_error_marks_failed_and_records_submit_started(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_failed.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_FailingSubmitBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "submit_failed.sqlite"))
    row = conn.execute(
        "SELECT client_order_id, status, last_error FROM orders WHERE client_order_id LIKE 'live_1000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    started_event = conn.execute(
        "SELECT 1 FROM order_events WHERE client_order_id=? AND event_type='submit_started'",
        (row["client_order_id"],),
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_attempt_id, symbol, side, qty, submit_ts, payload_fingerprint, broker_response_summary, submission_reason_code, exception_class, timeout_flag, exchange_order_id_obtained, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    transition = conn.execute(
        """
        SELECT message, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='status_transition'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "FAILED"
    assert "submit failed" in str(row["last_error"])
    assert started_event is not None
    assert submit_attempt is not None
    assert submit_attempt["submit_attempt_id"]
    assert submit_attempt["symbol"] == settings.PAIR
    assert submit_attempt["side"] == "BUY"
    assert float(submit_attempt["qty"]) > 0
    assert int(submit_attempt["submit_ts"]) == 1000
    assert submit_attempt["payload_fingerprint"]
    assert "submit_exception=RuntimeError" in str(submit_attempt["broker_response_summary"])
    assert submit_attempt["exception_class"] == "RuntimeError"
    assert submit_attempt["submission_reason_code"] == "failed_before_send"
    assert submit_attempt["timeout_flag"] == 0
    assert submit_attempt["exchange_order_id_obtained"] == 0
    assert submit_attempt["order_status"] == "FAILED"
    assert transition is not None
    assert transition["order_status"] == "FAILED"
    assert "from=PENDING_SUBMIT" in str(transition["message"])
    assert "to=FAILED" in str(transition["message"])
    assert any("event=order_submit_started" in msg for msg in notifications)
    assert any("event=order_submit_failed" in msg for msg in notifications)


def test_each_submit_attempt_records_exactly_one_classification(tmp_path):
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    db_success = str(tmp_path / "classify_success.sqlite")
    object.__setattr__(settings, "DB_PATH", db_success)
    assert live_execute_signal(_FakeBroker(), "BUY", 1000, 100000000.0) is not None

    db_failed = str(tmp_path / "classify_failed.sqlite")
    object.__setattr__(settings, "DB_PATH", db_failed)
    assert live_execute_signal(_FailingSubmitBroker(), "BUY", 1001, 100000000.0) is None

    db_unknown = str(tmp_path / "classify_unknown.sqlite")
    object.__setattr__(settings, "DB_PATH", db_unknown)
    assert live_execute_signal(_TimeoutBroker(), "BUY", 1002, 100000000.0) is None

    for db_path in (db_success, db_failed, db_unknown):
        conn = ensure_db(db_path)
        row = conn.execute(
            "SELECT client_order_id, status FROM orders WHERE client_order_id LIKE 'live_%_buy_%' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        attempts = conn.execute(
            """
            SELECT order_status
            FROM order_events
            WHERE client_order_id=? AND event_type='submit_attempt_recorded'
            """,
            (row["client_order_id"],),
        ).fetchall()
        conn.close()

        assert len(attempts) == 1
        assert attempts[0]["order_status"] in {"NEW", "FAILED", "SUBMIT_UNKNOWN"}
        assert row["status"] in {"NEW", "FILLED", "FAILED", "SUBMIT_UNKNOWN"}


def test_live_submit_without_exchange_id_marks_submit_unknown(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "missing_exchange_id.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_NoExchangeIdBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "missing_exchange_id.sqlite"))
    row = conn.execute(
        "SELECT client_order_id, status, exchange_order_id, last_error FROM orders WHERE client_order_id LIKE 'live_1000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_attempt_id, order_status, broker_response_summary, submission_reason_code, timeout_flag, submit_evidence, exchange_order_id_obtained
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "SUBMIT_UNKNOWN"
    assert row["exchange_order_id"] is None
    assert "classification=SUBMIT_UNKNOWN" in str(row["last_error"])
    assert submit_attempt is not None
    assert submit_attempt["submit_attempt_id"]
    assert submit_attempt["order_status"] == "SUBMIT_UNKNOWN"
    assert "exchange_order_id=-" in str(submit_attempt["broker_response_summary"])
    assert submit_attempt["submission_reason_code"] == "ambiguous_response"
    assert submit_attempt["timeout_flag"] == 0
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["error_summary"] == "missing exchange_order_id"
    assert submit_evidence["submit_path"] == "live_standard_market"
    assert submit_attempt["exchange_order_id_obtained"] == 0
    assert any("event=order_submit_unknown" in msg and "reason_code=SUBMIT_TIMEOUT" in msg for msg in notifications)


def test_submit_evidence_handles_unavailable_optional_fields(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_evidence_optional.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    def _raise_orderbook(_symbol: str) -> tuple[float, float]:
        raise RuntimeError("orderbook offline")

    monkeypatch.setattr("bithumb_bot.broker.live.fetch_orderbook_top", _raise_orderbook)

    trade = live_execute_signal(_FakeBroker(), "BUY", 1000, 100000000.0)
    assert trade is not None

    conn = ensure_db(str(tmp_path / "submit_evidence_optional.sqlite"))
    row = conn.execute(
        "SELECT client_order_id FROM orders WHERE client_order_id LIKE 'live_1000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT submit_evidence
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (row["client_order_id"],),
    ).fetchone()
    conn.close()

    evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert evidence["reference_price"] is None
    assert evidence["top_of_book"]["error"].startswith("RuntimeError:")


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

    state = runtime_state.snapshot()
    assert state.last_reconcile_reason_code == "REMOTE_OPEN_ORDER_FOUND"


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
    transition = conn.execute(
        """
        SELECT message, order_status
        FROM order_events
        WHERE client_order_id='ambiguous_missing_exid' AND event_type='status_transition'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    reconciled = conn.execute("SELECT status FROM orders WHERE client_order_id='live_2000_buy'").fetchone()
    conn.close()

    assert ambiguous is not None
    assert ambiguous["status"] == "RECOVERY_REQUIRED"
    assert ambiguous["exchange_order_id"] is None
    assert "manual recovery required" in str(ambiguous["last_error"])
    assert transition is not None
    assert transition["order_status"] == "RECOVERY_REQUIRED"
    assert "from=SUBMIT_UNKNOWN" in str(transition["message"])
    assert "to=RECOVERY_REQUIRED" in str(transition["message"])
    assert reconciled is not None
    assert reconciled["status"] == "FILLED"
    recovery_alerts = [
        msg for msg in notifications
        if "event=recovery_required_transition" in msg and "reason_code=WEAK_ORDER_CORRELATION" in msg
    ]
    assert recovery_alerts
    assert any("symbol=" in msg for msg in recovery_alerts)
    assert any("exchange_order_id=-" in msg for msg in recovery_alerts)
    assert any("operator_next_action=review submit ambiguity and recover order with exchange_order_id" in msg for msg in recovery_alerts)
    assert any("operator_hint_command=uv run python bot.py recover-order --client-order-id ambiguous_missing_exid --exchange-order-id <exchange_order_id>" in msg for msg in recovery_alerts)
    assert any(
        "event=reconcile_status_change" in msg and "client_order_id=live_2000_buy" in msg
        for msg in notifications
    )


def test_reconcile_submit_unknown_without_exchange_id_ambiguous_remote_fill_escalates(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_recent_fill.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "submit_unknown_recent_fill.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_SubmitUnknownRecentFillBroker())

    conn = ensure_db(str(tmp_path / "submit_unknown_recent_fill.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id, qty_filled FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    fill = conn.execute(
        "SELECT fill_id FROM fills WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert float(row["qty_filled"]) == pytest.approx(0.0)
    assert fill is None


def test_reconcile_submit_unknown_recent_fill_path_keeps_manual_recovery_required(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_recent_fill_transition.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "submit_unknown_recent_fill_transition.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_SubmitUnknownRecentFillBroker())

    conn = ensure_db(str(tmp_path / "submit_unknown_recent_fill_transition.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    fill = conn.execute(
        "SELECT fill_id FROM fills WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert fill is None


def test_reconcile_submit_unknown_without_exchange_id_weak_order_correlation_escalates(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_recent_order.sqlite"))
    conn = ensure_db(str(tmp_path / "submit_unknown_recent_order.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_SubmitUnknownRecentOrderBroker())

    conn = ensure_db(str(tmp_path / "submit_unknown_recent_order.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None


def test_reconcile_submit_unknown_recent_order_path_escalates_to_recovery_required(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "submit_unknown_recent_order_no_recovery.sqlite"))
    conn = ensure_db(str(tmp_path / "submit_unknown_recent_order_no_recovery.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('ambiguous_missing_exid',NULL,'SUBMIT_UNKNOWN','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_SubmitUnknownRecentOrderBroker())

    conn = ensure_db(str(tmp_path / "submit_unknown_recent_order_no_recovery.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='ambiguous_missing_exid'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])


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

    state = runtime_state.snapshot()
    assert state.last_reconcile_reason_code == "RECENT_FILL_APPLIED"


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


def test_manual_recover_order_attaches_exchange_order_id_and_applies_fills(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "manual_recover.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    conn = ensure_db(str(tmp_path / "manual_recover.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('manual_1',NULL,'RECOVERY_REQUIRED','BUY',NULL,0.01,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    recover_order_with_exchange_id(_FakeBroker(), client_order_id="manual_1", exchange_order_id="ex_manual_fill")

    conn = ensure_db(str(tmp_path / "manual_recover.sqlite"))
    row = conn.execute(
        "SELECT status, exchange_order_id, qty_filled FROM orders WHERE client_order_id='manual_1'"
    ).fetchone()
    fill = conn.execute("SELECT fill_id FROM fills WHERE client_order_id='manual_1'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex_manual_fill"
    assert float(row["qty_filled"]) == 0.01
    assert fill is not None


def test_reconcile_conflicting_sources_halts_conservatively(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "source_conflict.sqlite"))
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.recovery.notify", lambda msg: notifications.append(msg))

    reconcile_with_broker(_ConflictingRecoverySourcesBroker())

    state = runtime_state.snapshot()
    assert state.halt_new_orders_blocked is True
    assert state.halt_state_unresolved is True
    assert state.halt_reason_code == "RECOVERY_SOURCE_CONFLICT"
    assert state.last_reconcile_reason_code == "SOURCE_CONFLICT_HALT"
    assert state.last_disable_reason is not None
    assert "source conflict" in state.last_disable_reason
    assert any("event=reconcile_source_conflict" in msg and "reason_code=RECONCILE_MISMATCH" in msg for msg in notifications)


def test_reconcile_precedence_prefers_open_orders_over_recent_orders(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "source_precedence.sqlite"))

    reconcile_with_broker(_OpenOrderPreferredBroker())

    conn = ensure_db(str(tmp_path / "source_precedence.sqlite"))
    row = conn.execute(
        "SELECT status FROM orders WHERE exchange_order_id='ex_precedence'"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "PARTIAL"

    state = runtime_state.snapshot()
    assert state.last_reconcile_reason_code == "REMOTE_OPEN_ORDER_FOUND"


def test_reconcile_records_balance_split_mismatch_metadata(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "balance_mismatch.sqlite"))

    conn = ensure_db(str(tmp_path / "balance_mismatch.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1000000.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    reconcile_with_broker(_BalanceMismatchBroker())

    state = runtime_state.snapshot()
    assert state.last_reconcile_metadata is not None
    payload = json.loads(state.last_reconcile_metadata)
    assert int(payload.get("balance_split_mismatch_count", 0)) >= 1
    assert "cash_available" in str(payload.get("balance_split_mismatch_summary", ""))


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
    object.__setattr__(settings, "FEE_RATE", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.001)
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


def test_validate_pretrade_buy_uses_live_fee_rate_estimate_not_fee_rate() -> None:
    object.__setattr__(settings, "FEE_RATE", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.10)
    broker = _FakeBroker()
    broker.get_balance = lambda: BrokerBalance(  # type: ignore[method-assign]
        cash_available=109.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )

    with pytest.raises(ValueError, match="insufficient available cash"):
        validate_pretrade(
            broker=broker,
            side="BUY",
            qty=1.0,
            market_price=100.0,
            reference_bid=99.9,
            reference_ask=100.1,
        )


def test_validate_pretrade_buy_becomes_more_conservative_when_live_fee_increases() -> None:
    object.__setattr__(settings, "PRETRADE_BALANCE_BUFFER_BPS", 0.0)
    object.__setattr__(settings, "FEE_RATE", 0.0)
    broker = _FakeBroker()
    broker.get_balance = lambda: BrokerBalance(  # type: ignore[method-assign]
        cash_available=105.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )

    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.01)
    validate_pretrade(
        broker=broker,
        side="BUY",
        qty=1.0,
        market_price=100.0,
        reference_bid=99.9,
        reference_ask=100.1,
    )

    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.06)
    with pytest.raises(ValueError, match="insufficient available cash"):
        validate_pretrade(
            broker=broker,
            side="BUY",
            qty=1.0,
            market_price=100.0,
            reference_bid=99.9,
            reference_ask=100.1,
        )


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


def test_normalize_order_qty_floors_to_step_and_decimals():
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.01)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 3)

    normalized = normalize_order_qty(qty=1.2399, market_price=100.0)

    assert normalized == pytest.approx(1.23)


def test_normalize_order_qty_rejects_when_collapses_to_zero():
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.01)

    with pytest.raises(ValueError, match="normalized order qty is non-positive"):
        normalize_order_qty(qty=0.009, market_price=100.0)


def test_normalize_order_qty_rejects_when_normalized_notional_below_minimum():
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.1)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 100.0)

    with pytest.raises(ValueError, match="normalized order notional below minimum"):
        normalize_order_qty(qty=1.09, market_price=99.0)

def test_live_submit_attempt_reason_codes_cover_ambiguous_paths(tmp_path):
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)

    scenarios = [
        ("success", _FakeBroker(), 1100, "confirmed_success"),
        ("failed_before_send", _FailingSubmitBroker(), 1101, "failed_before_send"),
        ("timeout", _TimeoutBroker(), 1102, "sent_but_response_timeout"),
        ("transport", _TransportErrorBroker(), 1103, "sent_but_transport_error"),
        ("ambiguous", _NoExchangeIdBroker(), 1104, "ambiguous_response"),
    ]

    for name, broker, ts, expected_reason_code in scenarios:
        db_path = str(tmp_path / f"submit_reason_{name}.sqlite")
        object.__setattr__(settings, "DB_PATH", db_path)

        live_execute_signal(broker, "BUY", ts, 100000000.0)

        conn = ensure_db(db_path)
        row = conn.execute(
            "SELECT client_order_id FROM orders WHERE client_order_id LIKE ? ORDER BY id DESC LIMIT 1",
            (f"live_{ts}_buy_%",),
        ).fetchone()
        attempt = conn.execute(
            """
            SELECT submission_reason_code, submit_attempt_id, symbol, side, qty, price, submit_ts, payload_fingerprint
            FROM order_events
            WHERE client_order_id=? AND event_type='submit_attempt_recorded'
            ORDER BY id DESC
            LIMIT 1
            """,
            (row["client_order_id"],),
        ).fetchone()
        conn.close()

        assert attempt is not None
        assert attempt["submission_reason_code"] == expected_reason_code
        assert attempt["submit_attempt_id"]
        assert attempt["symbol"] == settings.PAIR
        assert attempt["side"] == "BUY"
        assert float(attempt["qty"]) > 0
        assert attempt["price"] is None or float(attempt["price"]) > 0
        assert int(attempt["submit_ts"]) == ts
        assert attempt["payload_fingerprint"]


class _JournaledReconcileBroker:
    def get_open_orders(self) -> list[BrokerOrder]:
        return []

    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "NEW", None, 0.0, 0.0, 0, 0)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(cash_available=1_000_000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)

    def get_read_journal_summary(self) -> dict[str, str]:
        return {
            "/v1/accounts": "{'path': '/v1/accounts', 'status': '0000', 'row_count': 1}",
            "/v1/orders(open_orders)": "{'path': '/v1/orders(open_orders)', 'status': '0000', 'row_count': 0}",
        }


def test_reconcile_persists_broker_read_journal_metadata(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "reconcile_journal.sqlite"))

    reconcile_with_broker(_JournaledReconcileBroker())

    state = runtime_state.snapshot()
    assert state.last_reconcile_metadata is not None
    payload = json.loads(state.last_reconcile_metadata)
    assert "broker_read_journal" in payload
    assert "/v1/accounts" in str(payload["broker_read_journal"])
