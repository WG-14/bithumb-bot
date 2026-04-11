from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

from bithumb_bot.broker.bithumb import BithumbBroker, build_broker_with_auth_diagnostics
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder, BrokerRejectError, BrokerTemporaryError
from bithumb_bot.broker import live as live_module
from bithumb_bot.broker.balance_source import BalanceSnapshot
from bithumb_bot.broker.live import (
    adjust_buy_order_qty_for_dust_safety,
    adjust_sell_order_qty_for_dust_safety,
    live_execute_signal,
    SellDustGuardError,
    normalize_order_qty,
    validate_order,
    validate_pretrade,
)
from bithumb_bot.oms import payload_fingerprint
from bithumb_bot.db_core import ensure_db, init_portfolio, record_strategy_decision, set_portfolio_breakdown
from bithumb_bot.reason_codes import (
    DUST_RESIDUAL_SUPPRESSED,
    DUST_RESIDUAL_UNSELLABLE,
    EXIT_PARTIAL_LEFT_DUST,
    MANUAL_DUST_REVIEW_REQUIRED,
    SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN,
    SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH,
)
from bithumb_bot.recovery import cancel_open_orders_with_broker, reconcile_with_broker, recover_order_with_exchange_id
from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.public_api_orderbook import BestQuote
from tests.fakes import FakeMarketData


pytestmark = pytest.mark.slow_integration


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

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
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
            intent = conn.execute(
                "SELECT 1 FROM order_events WHERE client_order_id=? AND event_type='intent_created'",
                (client_order_id,),
            ).fetchone()
            assert intent is not None

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
    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [BrokerOrder("", "stray1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1)]


class _NoExchangeIdBroker(_FakeBroker):
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None) -> BrokerOrder:
        self.place_order_calls += 1
        self._last_client_order_id = client_order_id
        self._last_side = side
        self._last_qty = qty
        self._last_price = price
        return BrokerOrder(client_order_id, None, side, "NEW", price, qty, 0.0, 1, 1)


class _InvalidFeeAggregateBroker(_FakeBroker):
    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if not client_order_id:
            return []
        return [
            BrokerFill(
                client_order_id=client_order_id,
                fill_id="agg_bad_fee_1",
                fill_ts=1000,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id=exchange_order_id or "ex1",
            ),
            BrokerFill(
                client_order_id=client_order_id,
                fill_id="agg_bad_fee_2",
                fill_ts=1001,
                price=100000000.0,
                qty=0.01,
                fee=float("nan"),
                exchange_order_id=exchange_order_id or "ex1",
            ),
        ]


class _CancelOpenOrdersBroker(_FakeBroker):
    def __init__(self) -> None:
        super().__init__()
        self.canceled: list[tuple[str, str | None]] = []

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
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


class _CancelAcceptedBroker(_CancelOpenOrdersBroker):
    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        self.canceled.append((client_order_id, exchange_order_id))
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "CANCEL_REQUESTED", None, 0.0, 0.0, 1, 1)

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        if exchange_order_id == "ex1":
            return BrokerOrder(client_order_id, exchange_order_id, "BUY", "CANCELED", 100.0, 0.1, 0.0, 1, 2)
        return BrokerOrder(client_order_id, exchange_order_id or "stray1", "SELL", "NEW", 110.0, 0.2, 0.05, 1, 2)


class _CancelIdentifierMismatchBroker(_CancelOpenOrdersBroker):
    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [BrokerOrder("live_1000_buy", "remote_mismatch_exid", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1)]


class _CancelAcceptedUnknownStatusBroker(_CancelOpenOrdersBroker):
    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        self.canceled.append((client_order_id, exchange_order_id))
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "CANCEL_REQUESTED", None, 0.0, 0.0, 1, 1)

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 2)


class _StrictRecoveryBroker(_FakeBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        if exchange_order_id is None:
            raise AssertionError("unsafe get_order called without exchange_order_id")
        return super().get_order(client_order_id=client_order_id, exchange_order_id=exchange_order_id)


class _RecentActivityBroker(_FakeBroker):
    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        if exchange_order_id != "ex_recent_known":
            return []
        return [
            BrokerFill(
                client_order_id=client_order_id or "",
                fill_id="recent_fill_1",
                fill_ts=1002,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id="ex_recent_known",
            )
        ]

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
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

class _UnmatchedRecentFillBroker(_FakeBroker):
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []


class _ConflictingRecoverySourcesBroker(_FakeBroker):
    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_conflict", "BUY", "NEW", 100.0, 0.01, 0.0, 1001, 1002)
        ]

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
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
    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return [
            BrokerOrder("", "ex_precedence", "BUY", "NEW", 100.0, 0.02, 0.0, 2001, 2002)
        ]

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
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
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
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
    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
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
    from bithumb_bot.broker import order_rules as _order_rules

    old_values = {
        "MODE": settings.MODE,
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
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
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
        "LIVE_FILL_FEE_STRICT_MODE": settings.LIVE_FILL_FEE_STRICT_MODE,
        "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW": settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW,
        "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW": settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW,
        "PAIR": settings.PAIR,
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
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 100_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    object.__setattr__(settings, "PAIR", old_values["PAIR"])
    object.__setattr__(settings, "MODE", old_values["MODE"])
    _order_rules._cached_rules.clear()

    yield

    for key, value in old_values.items():
        object.__setattr__(settings, key, value)
    object.__setattr__(settings, "MODE", "paper")
    _order_rules._cached_rules.clear()


def test_bithumb_broker_dry_run(monkeypatch):
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "k")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "s")

    broker = BithumbBroker()
    order = broker.place_order(client_order_id="a", side="BUY", qty=0.1, price=None)

    assert order.exchange_order_id.startswith("dry_")


def test_broker_auth_runtime_diagnostics_redact_secret_values(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "live-key-visible-length-only")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "super-secret-value-should-not-appear")
    object.__setattr__(settings, "BITHUMB_WS_MYASSET_ENABLED", False)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")

    broker = BithumbBroker()
    diag = broker.get_auth_runtime_diagnostics(
        caller="test",
        env_summary={"source_key": "BITHUMB_ENV_FILE", "env_file": "/runtime/live.env", "loaded": True},
    )

    assert diag["api_key_present"] is True
    assert diag["api_key_length"] == len("live-key-visible-length-only")
    assert diag["api_secret_present"] is True
    assert diag["api_secret_length"] == len("super-secret-value-should-not-appear")
    assert "super-secret-value-should-not-appear" not in json.dumps(diag, ensure_ascii=False)
    assert diag["chance_auth"]["endpoint"] == "/v1/orders/chance"
    assert diag["chance_auth"]["query_hash_included"] is True
    assert diag["accounts_auth"]["endpoint"] == "/v1/accounts"
    assert diag["accounts_auth"]["query_hash_included"] is False


def test_build_broker_with_auth_diagnostics_reuses_same_private_auth_preview_for_health_and_run(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "live-key-visible-length-only")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "super-secret-value-should-not-appear")
    monkeypatch.setattr("bithumb_bot.broker.bithumb.canonical_market_id", lambda _market: "KRW-BTC")

    health_broker, health_diag = build_broker_with_auth_diagnostics(
        caller="cmd_health",
        env_summary={"source_key": "BITHUMB_ENV_FILE_LIVE", "loaded": True},
        broker_factory=BithumbBroker,
    )
    run_broker, run_diag = build_broker_with_auth_diagnostics(
        caller="run_loop",
        env_summary={"source_key": "BITHUMB_ENV_FILE_LIVE", "loaded": True},
        broker_factory=BithumbBroker,
    )

    assert isinstance(health_broker, BithumbBroker)
    assert isinstance(run_broker, BithumbBroker)
    assert health_diag["chance_auth"]["endpoint"] == "/v1/orders/chance"
    assert run_diag["chance_auth"]["endpoint"] == "/v1/orders/chance"
    assert health_diag["chance_auth"]["query_hash_included"] is True
    assert run_diag["chance_auth"]["query_hash_included"] is True
    assert health_diag["accounts_auth"]["endpoint"] == "/v1/accounts"
    assert run_diag["accounts_auth"]["endpoint"] == "/v1/accounts"


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
    object.__setattr__(settings, "MODE", "paper")
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


def test_validate_pretrade_rejects_crossed_quote() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 1.0)
    with pytest.raises(ValueError, match="invalid orderbook top: market="):
        validate_pretrade(
            broker=_FakeBroker(),
            side="BUY",
            qty=0.001,
            market_price=100.5,
            reference_bid=101.0,
            reference_ask=100.0,
            reference_source="test_quote",
        )


def test_validate_pretrade_rejects_non_positive_quote() -> None:
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 1.0)
    with pytest.raises(ValueError, match="invalid orderbook top: market="):
        validate_pretrade(
            broker=_FakeBroker(),
            side="SELL",
            qty=0.001,
            market_price=100.5,
            reference_bid=0.0,
            reference_ask=101.0,
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
            WHERE client_order_id LIKE 'live_1700000000000_buy_%'
            ORDER BY id
            """
        ).fetchall()
        dedup_row = conn.execute(
            """
            SELECT client_order_id, order_status
            FROM order_intent_dedup
            WHERE symbol=? AND side='BUY' AND intent_ts=1000
            """,
            (settings.PAIR,),
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
            WHERE client_order_id LIKE 'live_1700000000000_buy_%'
            ORDER BY id
            """
        ).fetchall()
        dedup_row = conn.execute(
            """
            SELECT client_order_id, order_status
            FROM order_intent_dedup
            WHERE symbol=? AND side='BUY' AND intent_ts=1000
            """,
            (settings.PAIR,),
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

    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=(),
                order_sides=(),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=(),
                order_sides=(),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
            )
        ),
    )
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
    assert any("event=order_submit_blocked" in msg and "reason_code=RISKY_ORDER_BLOCK" in msg and "submit_attempt_id=-" in msg for msg in notifications)
    assert any("event=order_submit_blocked" in msg and "decision_id=-" in msg for msg in notifications)
    assert any("reason_detail_code=SUBMIT_UNKNOWN_PRESENT" in msg for msg in notifications)


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
    assert any("reason_detail_code=submission_halt" in msg for msg in notifications)

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


def test_extension_invariant_live_submit_requires_persisted_local_intent_and_preflight(tmp_path):
    db_path = str(tmp_path / "submit_invariant.sqlite")
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
        "SELECT client_order_id FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
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
    row = conn.execute("SELECT client_order_id, status, last_error FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1").fetchone()
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
        "SELECT client_order_id, status, last_error FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
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
        "SELECT client_order_id, status, exchange_order_id, last_error FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
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


def test_live_execute_signal_marks_recovery_required_when_strict_fill_fee_blocks_aggregate(tmp_path, monkeypatch):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "strict_fee_block.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 10_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))

    trade = live_execute_signal(_InvalidFeeAggregateBroker(), "BUY", 1000, 100000000.0)
    assert trade is None

    conn = ensure_db(str(tmp_path / "strict_fee_block.sqlite"))
    row = conn.execute(
        "SELECT client_order_id, status, last_error FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert "strict fee validation blocked fill aggregation" in str(row["last_error"])
    assert any("event=recovery_required_transition" in msg for msg in notifications)


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
        "SELECT client_order_id FROM orders WHERE client_order_id LIKE 'live_1700000000000_buy_%' ORDER BY id DESC LIMIT 1"
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
    assert evidence["top_of_book"]["error"].startswith("market=KRW-BTC side=UNKNOWN RuntimeError:")


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

    assert row is None

    state = runtime_state.snapshot()
    assert state.last_reconcile_reason_code == "RECONCILE_OK"


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
    assert summary["canceled_count"] == 1
    assert summary["matched_local_count"] == 1
    assert summary["stray_canceled_count"] == 0
    assert summary["failed_count"] == 0
    assert len(summary["stray_messages"]) == 1


def test_cancel_open_orders_reports_cancel_failures(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_failure.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_failure.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    summary = cancel_open_orders_with_broker(_CancelFailureBroker())

    assert summary["remote_open_count"] == 2
    assert summary["canceled_count"] == 1
    assert summary["failed_count"] == 0
    assert len(summary["error_messages"]) == 0
    assert len(summary["stray_messages"]) == 1


def test_cancel_open_orders_tracks_cancel_acceptance_and_confirmation(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_acceptance.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_acceptance.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    summary = cancel_open_orders_with_broker(_CancelAcceptedBroker())
    conn = ensure_db(str(tmp_path / "cancel_acceptance.sqlite"))
    row = conn.execute("SELECT status FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "CANCELED"
    assert summary["cancel_accepted_count"] == 1
    assert summary["canceled_count"] == 1
    assert summary["cancel_confirm_pending_count"] == 0
    assert summary["stray_canceled_count"] == 0
    assert len(summary["stray_messages"]) == 1


def test_cancel_open_orders_does_not_bind_by_client_order_id_when_exchange_id_mismatch(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_identifier_mismatch.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_identifier_mismatch.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    summary = cancel_open_orders_with_broker(_CancelIdentifierMismatchBroker())

    conn = ensure_db(str(tmp_path / "cancel_identifier_mismatch.sqlite"))
    row = conn.execute("SELECT status FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "NEW"
    assert summary["failed_count"] == 1
    assert any("identifier mismatch" in str(message) for message in summary["error_messages"])


def test_cancel_open_orders_escalates_when_post_cancel_status_is_unresolved(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "cancel_unresolved_status.sqlite"))
    conn = ensure_db(str(tmp_path / "cancel_unresolved_status.sqlite"))
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('live_1000_buy','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    summary = cancel_open_orders_with_broker(_CancelAcceptedUnknownStatusBroker())

    conn = ensure_db(str(tmp_path / "cancel_unresolved_status.sqlite"))
    row = conn.execute("SELECT status, last_error FROM orders WHERE client_order_id='live_1000_buy'").fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert "manual recovery required" in str(row["last_error"])
    assert summary["failed_count"] == 1
    assert any("final status unresolved" in str(message) for message in summary["error_messages"])


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
    assert "to=RECOVERY_REQUIRED" in str(transition["message"])
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

    assert row is None


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
    assert state.halt_new_orders_blocked is False
    assert state.last_reconcile_reason_code == "RECONCILE_OK"
    assert not any("event=reconcile_source_conflict" in msg for msg in notifications)


def test_reconcile_precedence_prefers_open_orders_over_recent_orders(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "source_precedence.sqlite"))

    reconcile_with_broker(_OpenOrderPreferredBroker())

    conn = ensure_db(str(tmp_path / "source_precedence.sqlite"))
    row = conn.execute(
        "SELECT status FROM orders WHERE exchange_order_id='ex_precedence'"
    ).fetchone()
    conn.close()

    assert row is None

    state = runtime_state.snapshot()
    assert state.last_reconcile_reason_code == "RECONCILE_OK"


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
    assert payload.get("balance_source") in {"legacy_balance_api", "accounts_v1_rest_snapshot", "dry_run_static"}
    assert int(payload.get("balance_observed_ts_ms", 0)) >= 0


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


def test_validate_pretrade_rejects_dry_run_balance_source_in_live_real_order() -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    broker = _FakeBroker()
    broker.get_balance_snapshot = lambda: BalanceSnapshot(  # type: ignore[attr-defined]
        source_id="dry_run_static",
        observed_ts_ms=0,
        asset_ts_ms=0,
        balance=BrokerBalance(cash_available=1_000_000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0),
    )

    with pytest.raises(ValueError, match="invalid live balance source: dry_run_static"):
        validate_pretrade(
            broker=broker,
            side="BUY",
            qty=1.0,
            market_price=100.0,
            reference_bid=99.9,
            reference_ask=100.1,
        )


def test_validate_pretrade_accepts_accounts_snapshot_balance_source(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "bid_min_total_krw": 0.0,
                        "ask_min_total_krw": 0.0,
                        "min_notional_krw": 0.0,
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "max_qty_decimals": 8,
                    },
                )(),
            },
        )(),
    )
    broker = _FakeBroker()
    broker.get_balance_snapshot = lambda: BalanceSnapshot(  # type: ignore[attr-defined]
        source_id="accounts_v1_rest_snapshot",
        observed_ts_ms=int(time.time() * 1000),
        asset_ts_ms=int(time.time() * 1000),
        balance=BrokerBalance(cash_available=1_000_000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0),
    )

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

    monkeypatch.setattr(
        live_module,
        "fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100.0, ask_price=101.0),
    )

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


def test_normalize_order_qty_does_not_apply_notional_guard():
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.1)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 100.0)

    normalized = normalize_order_qty(qty=1.09, market_price=99.0)
    assert normalized == pytest.approx(1.0)


def test_normalize_order_qty_avoids_float_boundary_drift():
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    normalized = normalize_order_qty(qty=0.0003, market_price=100_000_000.0)

    assert normalized == pytest.approx(0.0003)


@pytest.mark.fast_regression
def test_adjust_buy_order_qty_for_dust_safety_floors_to_sellable_qty():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    with pytest.raises(ValueError, match="would not leave an executable exit lot"):
        adjust_buy_order_qty_for_dust_safety(qty=0.00019193, market_price=100_000_000.0)


@pytest.mark.fast_regression
def test_adjust_buy_order_qty_for_dust_safety_rejects_when_no_sellable_qty_remains():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    with pytest.raises(ValueError, match="dust-safe entry qty unavailable"):
        adjust_buy_order_qty_for_dust_safety(qty=0.00009193, market_price=100_000_000.0)


@pytest.mark.fast_regression
@pytest.mark.parametrize(
    ("qty", "expected_qty"),
    [
        (0.0001, 0.0001),
        (0.00019999, 0.0001),
    ],
    ids=["already_sellable_qty", "floors_to_sellable_qty_without_leaving_entry_dust"],
)
def test_adjust_buy_order_qty_for_dust_safety_preserves_only_sellable_entry_qty(qty, expected_qty):
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    with pytest.raises(ValueError, match="would not leave an executable exit lot"):
        adjust_buy_order_qty_for_dust_safety(qty=qty, market_price=100_000_000.0)


@pytest.mark.fast_regression
def test_live_execute_signal_buy_blocks_dust_unsafe_entry_qty_before_submit(tmp_path):
    original = {
        "DB_PATH": settings.DB_PATH,
        "START_CASH_KRW": float(settings.START_CASH_KRW),
        "BUY_FRACTION": float(settings.BUY_FRACTION),
        "MAX_ORDER_KRW": float(settings.MAX_ORDER_KRW),
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
        "LIVE_FEE_RATE_ESTIMATE": float(settings.LIVE_FEE_RATE_ESTIMATE),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        "ENTRY_EDGE_BUFFER_RATIO": float(settings.ENTRY_EDGE_BUFFER_RATIO),
        "MAX_ORDERBOOK_SPREAD_BPS": float(settings.MAX_ORDERBOOK_SPREAD_BPS),
        "MAX_MARKET_SLIPPAGE_BPS": float(settings.MAX_MARKET_SLIPPAGE_BPS),
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS": float(settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS),
    }
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "buy_dust_unsafe.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 19_193.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "buy_dust_unsafe.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=19_193.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    try:
        broker = _FakeBroker()
        trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

        assert trade is None
        assert broker.place_order_calls == 0
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_blocks_when_step_floor_would_leave_unsellable_dust():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    with pytest.raises(SellDustGuardError) as exc:
        adjust_sell_order_qty_for_dust_safety(qty=0.00019192, market_price=100_000_000.0)

    details = exc.value.details
    assert details["normalized_qty"] == pytest.approx(0.0001)
    assert details["remainder_qty"] == pytest.approx(0.00009192)
    assert details["dust_scope"] == "remainder_after_sell"
    assert "guard_action=block_sell_remainder_dust" in str(details["summary"])


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_blocks_when_broker_precision_still_leaves_dust():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    with pytest.raises(SellDustGuardError) as exc:
        adjust_sell_order_qty_for_dust_safety(qty=0.000191939, market_price=100_000_000.0)

    details = exc.value.details
    assert details["normalized_qty"] == pytest.approx(0.0001)
    assert details["remainder_qty"] == pytest.approx(0.000091939)
    assert details["broker_full_qty"] == pytest.approx(0.00019193)
    assert details["dust_scope"] == "remainder_after_sell"


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_exposes_remainder_dust_guard_details():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    with pytest.raises(SellDustGuardError) as exc:
        adjust_sell_order_qty_for_dust_safety(qty=0.000191939, market_price=100_000_000.0)

    details = exc.value.details
    assert details["state"] == EXIT_PARTIAL_LEFT_DUST
    assert details["operator_action"] == MANUAL_DUST_REVIEW_REQUIRED
    assert details["dust_scope"] == "remainder_after_sell"
    assert details["qty_below_min"] == 1
    assert details["notional_below_min"] == 0
    assert details["remainder_qty"] == pytest.approx(0.000091939)


@pytest.mark.fast_regression
def test_live_execute_signal_sell_treats_sub_min_residual_as_dust_before_submit(tmp_path):
    original = {
        "DB_PATH": settings.DB_PATH,
        "START_CASH_KRW": float(settings.START_CASH_KRW),
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
        "LIVE_FEE_RATE_ESTIMATE": float(settings.LIVE_FEE_RATE_ESTIMATE),
        "STRATEGY_ENTRY_SLIPPAGE_BPS": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        "ENTRY_EDGE_BUFFER_RATIO": float(settings.ENTRY_EDGE_BUFFER_RATIO),
        "MAX_ORDERBOOK_SPREAD_BPS": float(settings.MAX_ORDERBOOK_SPREAD_BPS),
        "MAX_MARKET_SLIPPAGE_BPS": float(settings.MAX_MARKET_SLIPPAGE_BPS),
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS": float(settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS),
    }
    db_path = str(tmp_path / "sell_sub_min_residual.sqlite")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.00000001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(db_path)
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009997,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.00009997, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.00009997,
            "normalized_exposure_active": True,
            "normalized_exposure_qty": 0.00009997,
            "open_exposure_qty": 0.00009997,
            "dust_tracking_qty": 0.0,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.00009997,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.00009997,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_qty": 0.0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.0,
                    "sellable_executable_lot_count": 0,
                    "exit_allowed": False,
                    "exit_block_reason": "dust_only_remainder",
                    "terminal_state": "dust_only",
                    "normalized_exposure_qty": 0.0,
                    "normalized_exposure_active": False,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    try:
        broker = _FakeBroker()
        trade = live_execute_signal(
            broker,
            "SELL",
            1000,
            100_000_000.0,
            strategy_name="dust_exit_test",
            decision_id=decision_id,
            decision_reason="partial_take_profit",
            exit_rule_name="exit_signal",
        )

        assert trade is None
        assert broker.place_order_calls == 0

        conn = ensure_db(db_path)
        order_row = conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM orders
            WHERE side='SELL'
            """
        ).fetchone()
        suppression_row = conn.execute(
            """
            SELECT reason_code, summary, context_json
            FROM order_suppressions
            WHERE reason_code=?
            ORDER BY updated_ts DESC
            LIMIT 1
            """,
            (DUST_RESIDUAL_SUPPRESSED,),
        ).fetchone()
        conn.close()

        assert order_row is not None
        assert order_row["n"] == 0
        assert suppression_row is not None
        assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
        assert "decision_suppressed:exit_suppressed_by_quantity_rule" in str(suppression_row["summary"])
        assert "exit_non_executable_reason=" in str(suppression_row["summary"])
        assert any(
            token in str(suppression_row["summary"])
            for token in ("exit_non_executable_reason=dust_only_remainder", "exit_non_executable_reason=no_executable_exit_lot")
        )
        suppression_context = json.loads(str(suppression_row["context_json"]))
        assert suppression_context["reason_code"] == DUST_RESIDUAL_SUPPRESSED
        assert suppression_context["exit_non_executable_reason"] in {"dust_only_remainder", "no_executable_exit_lot"}
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_snaps_tiny_min_qty_boundary_upward():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    adjusted = adjust_sell_order_qty_for_dust_safety(qty=0.00009999, market_price=100_000_000.0)

    assert adjusted == pytest.approx(0.0001)


@pytest.mark.fast_regression
def test_adjust_sell_order_qty_for_dust_safety_keeps_exact_min_qty_executable():
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    adjusted = adjust_sell_order_qty_for_dust_safety(qty=0.0001, market_price=100_000_000.0)

    assert adjusted == pytest.approx(0.0001)


def test_live_execute_signal_buy_does_not_floor_market_buy_spend_via_qty_step(tmp_path, monkeypatch):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_qty_step.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr(
        live_module,
        "get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=(),
                order_sides=(),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(
        live_module,
        "build_buy_execution_sizing",
        lambda **_kwargs: SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            budget_krw=20_000.0,
            requested_qty=0.0002,
            executable_qty=0.0002,
            internal_lot_size=0.0001,
            intended_lot_count=2,
            executable_lot_count=2,
            qty_source="test",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        ),
    )

    conn = ensure_db(str(tmp_path / "market_buy_qty_step.sqlite"))
    try:
        init_portfolio(conn)
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        decision_id = record_strategy_decision(
            conn,
            decision_ts=1000,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1000,
            market_price=100_000_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "entry_allowed": True,
                "effective_flat": True,
                "normalized_exposure_active": False,
                "normalized_exposure_qty": 0.0,
                "raw_qty_open": 0.0,
                "raw_total_asset_qty": 0.0,
                "open_exposure_qty": 0.0,
                "dust_tracking_qty": 0.0,
                "has_executable_exposure": False,
                "has_any_position_residue": False,
                "has_non_executable_residue": False,
                "has_dust_only_remainder": False,
            },
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0, decision_id=decision_id, decision_reason="sma golden cross")

    assert trade is not None
    assert broker.place_order_calls == 1
    # 20,000 / 100,000,000 = 0.0002. If qty-step flooring leaked into market BUY,
    # this would collapse to 0.0001 and halve the KRW notional (regression).
    assert broker._last_qty == pytest.approx(0.0002)


@pytest.mark.fast_regression
def test_live_execute_signal_buy_adjusts_dust_creating_entry_qty(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_dust_safe.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 19_193.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "market_buy_dust_safe.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    conn.close()

    assert order_count == 0
    assert event_count == 0


@pytest.mark.fast_regression
def test_live_execute_signal_buy_blocks_when_dust_safe_adjustment_collapses_to_zero(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_dust_blocked.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 9_193.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

    assert trade is None
    assert broker.place_order_calls == 0


    conn = ensure_db(str(tmp_path / "market_buy_dust_blocked.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    conn.close()

    assert order_count == 0
    assert event_count == 0


@pytest.mark.fast_regression
def test_live_execute_signal_buy_records_intent_when_harmless_dust_is_effective_flat(tmp_path, monkeypatch):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_harmless_dust.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr(
        live_module,
        "get_effective_order_rules",
        lambda _pair: SimpleNamespace(
            rules=SimpleNamespace(
                min_qty=0.0001,
                qty_step=0.0001,
                min_notional_krw=0.0,
                max_qty_decimals=8,
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=10.0,
                ask_price_unit=1.0,
                order_types=(),
                order_sides=(),
                bid_fee=0.0,
                ask_fee=0.0,
                maker_bid_fee=0.0,
                maker_ask_fee=0.0,
            )
        ),
    )
    monkeypatch.setattr(
        live_module,
        "build_buy_execution_sizing",
        lambda **_kwargs: SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            budget_krw=20_000.0,
            requested_qty=0.0002,
            executable_qty=0.0002,
            internal_lot_size=0.0001,
            intended_lot_count=2,
            executable_lot_count=2,
            qty_source="test",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        ),
    )

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009193,
            "dust_local_qty": 0.00009193,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9_193.0,
            "dust_local_notional_krw": 9_193.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    conn = ensure_db(str(tmp_path / "market_buy_harmless_dust.sqlite"))
    try:
        init_portfolio(conn)
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.00009193,
            asset_locked=0.0,
        )
        decision_id = record_strategy_decision(
            conn,
            decision_ts=1_700_000_000_000,
            strategy_name="sma_with_filter",
            signal="BUY",
            reason="sma golden cross",
            candle_ts=1_699_999_940_000,
            market_price=100_000_000.0,
            context={
                "base_signal": "BUY",
                "base_reason": "sma golden cross",
                "entry_reason": "sma golden cross",
                "entry_allowed": True,
                "effective_flat": True,
                "normalized_exposure_active": True,
                "has_executable_exposure": False,
                "has_any_position_residue": True,
                "has_non_executable_residue": True,
                "has_dust_only_remainder": True,
                "normalized_exposure_qty": 0.00009629,
                "raw_qty_open": 0.00009629,
                "open_exposure_qty": 0.00009629,
                "dust_tracking_qty": 0.00009629,
                "position_gate": {
                    "entry_allowed": True,
                    "effective_flat_due_to_harmless_dust": True,
                    "normalized_exposure_active": True,
                    "has_executable_exposure": False,
                    "has_any_position_residue": True,
                    "has_non_executable_residue": True,
                    "has_dust_only_remainder": True,
                    "raw_qty_open": 0.00009629,
                },
                "position_state": {
                    "normalized_exposure": {
                        "entry_allowed": True,
                        "effective_flat": True,
                        "normalized_exposure_active": True,
                        "has_executable_exposure": False,
                        "has_any_position_residue": True,
                        "has_non_executable_residue": True,
                        "has_dust_only_remainder": True,
                        "normalized_exposure_qty": 0.00009629,
                        "raw_qty_open": 0.00009629,
                        "open_exposure_qty": 0.00009629,
                        "dust_tracking_qty": 0.00009629,
                    }
                },
            },
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "BUY",
        1_700_000_000_000,
        100_000_000.0,
        decision_id=decision_id,
        decision_reason="sma golden cross",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_side == "BUY"
    assert broker._last_qty > 0

    conn = ensure_db(str(tmp_path / "market_buy_harmless_dust.sqlite"))
    order_row = conn.execute(
        """
        SELECT client_order_id, side, status, qty_req
        FROM orders
        WHERE client_order_id LIKE 'live_1700000000000_buy_%'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    assert order_row is not None
    intent_row = conn.execute(
        """
        SELECT event_type, side, qty, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='intent_created'
        ORDER BY id DESC
        LIMIT 1
        """,
        (order_row["client_order_id"],),
    ).fetchone()
    submit_attempt_row = conn.execute(
        """
        SELECT event_type, side, qty, order_status
        FROM order_events
        WHERE client_order_id=? AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """,
        (order_row["client_order_id"],),
    ).fetchone()
    conn.close()

    assert order_row["side"] == "BUY"
    assert order_row["status"] in {"NEW", "FILLED"}
    assert float(order_row["qty_req"]) > 0
    assert intent_row is not None
    assert intent_row["side"] == "BUY"
    assert float(intent_row["qty"]) > 0
    assert submit_attempt_row is not None
    assert submit_attempt_row["side"] == "BUY"
    assert float(submit_attempt_row["qty"]) > 0


@pytest.mark.fast_regression
def test_live_execute_signal_buy_blocks_defensively_for_blocking_dust_mismatch(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "market_buy_blocking_dust.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "BUY_FRACTION", 1.0)
    object.__setattr__(settings, "MAX_ORDER_KRW", 20_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "blocking_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 0,
            "dust_broker_qty": 0.000099,
            "dust_local_qty": 0.000010,
            "dust_delta_qty": 0.000089,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 40_000_000.0,
            "dust_broker_notional_krw": 3_960.0,
            "dust_local_notional_krw": 400.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 1,
            "dust_local_notional_is_dust": 1,
            "dust_broker_local_match": 0,
            "dust_residual_summary": (
                "classification=blocking_dust harmless_dust=0 broker_local_match=0 "
                "allow_resume=0 effective_flat=0 policy_reason=dangerous_dust_operator_review_required"
            ),
        },
    )

    conn = ensure_db(str(tmp_path / "market_buy_blocking_dust.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.000099,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(broker, "BUY", 1000, 100_000_000.0)

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "market_buy_blocking_dust.sqlite"))
    order_count = conn.execute(
        "SELECT COUNT(*) AS n FROM orders WHERE side='BUY'"
    ).fetchone()["n"]
    intent_count = conn.execute(
        "SELECT COUNT(*) AS n FROM order_events WHERE event_type='intent_created'"
    ).fetchone()["n"]
    submit_attempt_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type IN ('submit_attempt_preflight', 'submit_attempt_recorded', 'submit_blocked')
        """
    ).fetchone()["n"]
    conn.close()

    assert order_count == 0
    assert intent_count == 0
    assert submit_attempt_count == 0


def test_live_execute_signal_sell_blocks_when_qty_step_floor_would_leave_unsellable_dust(tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_safe_full_qty.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db(str(tmp_path / "sell_dust_safe_full_qty.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00019192,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_safe_full_qty.sqlite"))
    dust_rows = conn.execute(
        """
        SELECT reason_code
        FROM order_suppressions
        WHERE reason_code=?
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchall()
    latest_order = conn.execute(
        """
        SELECT qty_req, qty_filled, status
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    event_row = conn.execute(
        """
        SELECT reason_code, requested_qty, normalized_qty, context_json
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts DESC
        LIMIT 1
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()
    conn.close()

    assert len(dust_rows) == 0
    assert latest_order is None
    assert event_row is None


# Authority boundary regression suite.


@pytest.mark.fast_regression
def test_authority_boundary_live_execute_signal_sell_uses_normalized_exposure_qty_and_excludes_dust_tracking(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_normalized_exposure_only.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_normalized_exposure_only.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00029193,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0002, 2, 0, "lot-native", "open_exposure"),
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
            context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "entry_allowed": False,
                "effective_flat": False,
                "raw_qty_open": 0.0002,
                "raw_total_asset_qty": 0.00029193,
                "normalized_exposure_active": True,
                "normalized_exposure_qty": 0.0002,
                "open_exposure_qty": 0.0002,
                "dust_tracking_qty": 0.00009193,
                "open_lot_count": 2,
                "dust_tracking_lot_count": 1,
                "sellable_executable_lot_count": 2,
                "sellable_executable_qty": 0.0002,
                "has_executable_exposure": True,
                "has_any_position_residue": True,
                "has_non_executable_residue": False,
                "has_dust_only_remainder": False,
                "submit_lot_count": 2,
                "submit_lot_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                "sell_qty_basis_qty": 0.0002,
                "sell_qty_basis_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                "exit_allowed": True,
                "exit_block_reason": "none",
                "terminal_state": "open_exposure",
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.0002,
                        "raw_total_asset_qty": 0.00029193,
                        "effective_flat": False,
                        "entry_allowed": False,
                        "normalized_exposure_active": True,
                        "normalized_exposure_qty": 0.0002,
                        "open_exposure_qty": 0.0002,
                        "dust_tracking_qty": 0.00009193,
                        "open_lot_count": 2,
                        "dust_tracking_lot_count": 1,
                        "sellable_executable_lot_count": 2,
                        "sellable_executable_qty": 0.0002,
                        "has_executable_exposure": True,
                        "has_any_position_residue": True,
                        "has_non_executable_residue": False,
                        "has_dust_only_remainder": False,
                        "submit_lot_count": 2,
                        "position_state_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                        "sell_qty_basis_qty": 0.0002,
                        "sell_qty_basis_source": "position_state.normalized_exposure.sellable_executable_lot_count",
                        "exit_allowed": True,
                        "exit_block_reason": "none",
                        "terminal_state": "open_exposure",
                    }
                },
            },
        )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0002)

    conn = ensure_db(str(tmp_path / "sell_normalized_exposure_only.sqlite"))
    order_row = conn.execute(
        """
        SELECT qty_req, qty_filled, status
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_row is not None
    assert float(order_row["qty_req"]) == pytest.approx(0.0002)
    assert float(order_row["qty_filled"]) == pytest.approx(0.0002)
    assert order_row["status"] == "FILLED"
    assert submit_attempt is not None
    assert float(submit_attempt["qty"]) == pytest.approx(0.0002)
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["order_qty"] == pytest.approx(0.0002)
    assert submit_evidence["normalized_qty"] == pytest.approx(0.0002)
    assert submit_evidence["submit_lot_count"] == 2
    assert submit_evidence["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert submit_evidence["submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert submit_evidence["sell_submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert submit_evidence["position_state_source"] == "derived:sellable_executable_lot_count"
    assert submit_evidence["raw_total_asset_qty"] == pytest.approx(0.00029193)
    assert submit_evidence["open_exposure_qty"] == pytest.approx(0.0002)
    assert submit_evidence["dust_tracking_qty"] == pytest.approx(0.00009193)


@pytest.mark.fast_regression
def test_authority_boundary_live_execute_signal_sell_does_not_sum_open_exposure_and_dust_tracking_for_submission(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_sum_regression.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_sum_regression.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00049193,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0004, 1, 0, "lot-native", "open_exposure"),
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "entry_allowed": False,
                "effective_flat": False,
                "raw_qty_open": 0.0004,
                "raw_total_asset_qty": 0.00049193,
                "open_exposure_qty": 0.0004,
                "dust_tracking_qty": 0.00009193,
                "open_lot_count": 1,
                "dust_tracking_lot_count": 1,
                "reserved_exit_lot_count": 0,
                "sellable_executable_lot_count": 1,
                "normalized_exposure_active": True,
                "normalized_exposure_qty": 0.0004,
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.0004,
                        "raw_total_asset_qty": 0.00049193,
                        "open_exposure_qty": 0.0004,
                        "dust_tracking_qty": 0.00009193,
                        "open_lot_count": 1,
                        "dust_tracking_lot_count": 1,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 1,
                        "effective_flat": False,
                        "entry_allowed": False,
                        "normalized_exposure_active": True,
                        "normalized_exposure_qty": 0.0004,
                    }
                },
            },
        )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0004)

    conn = ensure_db(str(tmp_path / "sell_sum_regression.sqlite"))
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert submit_attempt is not None
    assert float(submit_attempt["qty"]) == pytest.approx(0.0004)
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["order_qty"] == pytest.approx(0.0004)
    assert submit_evidence["position_qty"] == pytest.approx(0.0004)
    assert submit_evidence["submit_payload_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_open_exposure_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_dust_tracking_qty"] == pytest.approx(0.00009193)
    assert submit_evidence["raw_total_asset_qty"] == pytest.approx(0.00049193)
    assert submit_evidence["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert submit_evidence["sell_qty_basis_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_qty_basis_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert submit_evidence["sell_qty_boundary_kind"] == "none"
    assert submit_evidence["order_qty"] != pytest.approx(0.00049193)


@pytest.mark.fast_regression
def test_authority_boundary_live_execute_signal_sell_uses_exit_sizing_executable_qty_for_final_submit_payload(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_exit_sizing_authority.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr(
        live_module,
        "build_sell_execution_sizing",
        lambda **_kwargs: SimpleNamespace(
            allowed=True,
            block_reason="none",
            decision_reason_code="none",
            requested_qty=0.0004,
            executable_qty=0.0004,
            internal_lot_size=0.0004,
            intended_lot_count=1,
            executable_lot_count=1,
            qty_source="position_state.normalized_exposure.sellable_executable_lot_count",
            effective_min_trade_qty=0.0001,
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=0.0,
            non_executable_reason="executable",
        ),
    )

    conn = ensure_db(str(tmp_path / "sell_exit_sizing_authority.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00049193,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0004, 1, 0, "lot-native", "open_exposure"),
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.0004,
            "raw_total_asset_qty": 0.00049193,
            "open_exposure_qty": 0.0004,
            "dust_tracking_qty": 0.00009193,
            "open_lot_count": 1,
            "dust_tracking_lot_count": 1,
            "reserved_exit_lot_count": 0,
            "sellable_executable_qty": 0.00049193,
            "sellable_executable_lot_count": 1,
            "normalized_exposure_active": True,
            "normalized_exposure_qty": 0.0004,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0004,
                    "raw_total_asset_qty": 0.00049193,
                    "open_exposure_qty": 0.0004,
                    "dust_tracking_qty": 0.00009193,
                    "open_lot_count": 1,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.00049193,
                    "sellable_executable_lot_count": 1,
                    "effective_flat": False,
                    "entry_allowed": False,
                    "normalized_exposure_active": True,
                    "normalized_exposure_qty": 0.0004,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.0004)
    assert broker._last_qty != pytest.approx(0.00049193)

    conn = ensure_db(str(tmp_path / "sell_exit_sizing_authority.sqlite"))
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert submit_attempt is not None
    assert float(submit_attempt["qty"]) == pytest.approx(0.0004)
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["order_qty"] == pytest.approx(0.0004)
    assert submit_evidence["submit_payload_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_qty_basis_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_qty_basis_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert submit_evidence["sell_open_exposure_qty"] == pytest.approx(0.0004)
    assert submit_evidence["sell_dust_tracking_qty"] == pytest.approx(0.00009193)
    assert submit_evidence["order_qty"] != pytest.approx(0.00049193)

@pytest.mark.fast_regression
def test_live_execute_signal_sell_uses_open_exposure_only_when_dust_tracking_coexists(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_open_exposure_and_dust_tracking.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_open_exposure_and_dust_tracking.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00021999,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.00012, 3, 0, "lot-native", "open_exposure"),
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009999, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
                "base_signal": "SELL",
                "final_signal": "SELL",
                "entry_allowed": False,
                "effective_flat": False,
                "raw_qty_open": 0.0,
                "raw_total_asset_qty": 0.00021999,
                "open_exposure_qty": 0.00012,
                "dust_tracking_qty": 0.00009999,
                "open_lot_count": 3,
                "dust_tracking_lot_count": 1,
                "reserved_exit_lot_count": 0,
                "sellable_executable_lot_count": 3,
                "normalized_exposure_active": False,
                "normalized_exposure_qty": 0.0,
                "position_state": {
                    "normalized_exposure": {
                        "raw_qty_open": 0.0,
                        "raw_total_asset_qty": 0.00021999,
                        "open_exposure_qty": 0.00012,
                        "dust_tracking_qty": 0.00009999,
                        "open_lot_count": 3,
                        "dust_tracking_lot_count": 1,
                        "reserved_exit_lot_count": 0,
                        "sellable_executable_lot_count": 3,
                        "effective_flat": False,
                        "entry_allowed": False,
                        "normalized_exposure_active": False,
                        "normalized_exposure_qty": 0.0,
                    }
                },
            },
        )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 0,
            "dust_effective_flat": 0,
            "dust_policy_reason": "none",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is not None
    assert broker.place_order_calls == 1
    assert broker._last_qty == pytest.approx(0.00012)

    conn = ensure_db(str(tmp_path / "sell_open_exposure_and_dust_tracking.sqlite"))
    order_row = conn.execute(
        """
        SELECT qty_req, qty_filled, status
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    intent_row = conn.execute(
        """
        SELECT event_type, qty, submit_attempt_id
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='intent_created'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_row is not None
    assert float(order_row["qty_req"]) == pytest.approx(0.00012)
    assert float(order_row["qty_filled"]) == pytest.approx(0.00012)
    assert order_row["status"] == "FILLED"
    assert intent_row is not None
    assert float(intent_row["qty"]) == pytest.approx(0.00012)
    assert intent_row["submit_attempt_id"]
    assert submit_attempt is not None
    assert float(submit_attempt["qty"]) == pytest.approx(0.00012)
    submit_evidence = json.loads(str(submit_attempt["submit_evidence"]))
    assert submit_evidence["order_qty"] == pytest.approx(0.00012)
    assert submit_evidence["position_qty"] == pytest.approx(0.00012)
    assert submit_evidence["submit_payload_qty"] == pytest.approx(0.00012)
    assert submit_evidence["normalized_qty"] == pytest.approx(0.00012)
    assert submit_evidence["raw_total_asset_qty"] == pytest.approx(0.00021999)
    assert submit_evidence["open_exposure_qty"] == pytest.approx(0.00012)
    assert submit_evidence["dust_tracking_qty"] == pytest.approx(0.00009999)


@pytest.mark.fast_regression
def test_live_execute_signal_sell_snaps_tiny_open_exposure_boundary_upward_with_harmless_dust(
    monkeypatch,
    tmp_path,
):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_boundary_harmless_dust.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 5)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_boundary_harmless_dust.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00019192,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.00009999, 0, 1, "lot-native", "dust_tracking"),
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 2, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.00009999,
            "normalized_exposure_active": False,
            "normalized_exposure_qty": 0.0,
            "open_exposure_qty": 0.00009999,
            "dust_tracking_qty": 0.00009193,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.00009999,
                    "effective_flat": False,
                    "entry_allowed": False,
                    "normalized_exposure_active": False,
                    "normalized_exposure_qty": 0.0,
                    "open_exposure_qty": 0.00009999,
                    "dust_tracking_qty": 0.00009193,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009999,
            "dust_local_qty": 0.00009999,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9_999.0,
            "dust_local_notional_krw": 9_999.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_boundary_harmless_dust.sqlite"))
    order_row = conn.execute(
        """
        SELECT qty_req, qty_filled, status
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    suppression_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        """
    ).fetchone()["n"]
    submit_attempt = conn.execute(
        """
        SELECT event_type, qty, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert suppression_count == 1
    assert order_row is None
    assert submit_attempt is None


@pytest.mark.fast_regression
def test_live_execute_signal_sell_classifies_qty_step_mismatch_broker_reject(monkeypatch, tmp_path):
    class _RejectingBroker:
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=1_000_000.0, cash_locked=0.0, asset_available=0.0002, asset_locked=0.0)

        def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None):
            raise BrokerRejectError(f"{side} qty does not match qty_step: qty={qty} qty_step=0.0001")

    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_qty_step_reject.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_qty_step_reject.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0002,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_open", 1_700_000_000_000, 100_000_000.0, 0.0002, 2, 0, "lot-native", "open_exposure"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.0002,
            "raw_total_asset_qty": 0.0002,
            "open_exposure_qty": 0.0002,
            "dust_tracking_qty": 0.0,
            "open_lot_count": 1,
            "dust_tracking_lot_count": 0,
            "reserved_exit_lot_count": 0,
            "sellable_executable_lot_count": 1,
            "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
            "position_state_source": "context.raw_qty_open",
            "normalized_exposure_active": False,
            "normalized_exposure_qty": 0.0,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0002,
                    "raw_total_asset_qty": 0.0002,
                    "open_exposure_qty": 0.0002,
                    "dust_tracking_qty": 0.0,
                    "open_lot_count": 1,
                    "dust_tracking_lot_count": 0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_lot_count": 1,
                    "submit_qty_source": "position_state.normalized_exposure.sellable_executable_qty",
                    "position_state_source": "context.raw_qty_open",
                    "entry_allowed": False,
                    "effective_flat": False,
                    "normalized_exposure_active": False,
                    "normalized_exposure_qty": 0.0,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    trade = live_execute_signal(
        _RejectingBroker(),
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None

    conn = ensure_db(str(tmp_path / "sell_qty_step_reject.sqlite"))
    submit_row = conn.execute(
        """
        SELECT submission_reason_code, submit_evidence
        FROM order_events
        WHERE client_order_id LIKE 'live_1000_sell_%' AND event_type='submit_attempt_recorded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    suppression_row = conn.execute(
        """
        SELECT reason_code, summary
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts DESC
        LIMIT 1
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()
    conn.close()

    assert submit_row is None


def test_sell_failure_category_prefers_boundary_kind_over_unsafe_mismatch():
    category = live_module._classify_sell_failure_category(
        reason_code=DUST_RESIDUAL_UNSELLABLE,
        dust_details={
            "sell_qty_boundary_kind": "min_qty",
            "qty_below_min": 0,
            "normalized_below_min": 0,
            "notional_below_min": 0,
            "dust_qty_gap_small": 1,
            "summary": "sell_qty_boundary_kind=min_qty dust_qty_gap_small=1",
        },
    )

    assert category == SELL_FAILURE_CATEGORY_BOUNDARY_BELOW_MIN


def test_sell_failure_category_prefers_qty_step_boundary_kind():
    category = live_module._classify_sell_failure_category(
        reason_code=DUST_RESIDUAL_UNSELLABLE,
        dust_details={
            "sell_qty_boundary_kind": "qty_step",
            "summary": "sell_qty_boundary_kind=qty_step qty_step=0.0001",
        },
    )

    assert category == SELL_FAILURE_CATEGORY_QTY_STEP_MISMATCH


@pytest.mark.fast_regression
def test_live_execute_signal_sell_blocks_when_only_dust_tracking_remains(monkeypatch, tmp_path):
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_tracking_only.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.00001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    conn = ensure_db(str(tmp_path / "sell_dust_tracking_only.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009193,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_semantic_basis,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (settings.PAIR, 1, "entry_dust", 1_700_000_000_100, 100_000_000.0, 0.00009193, 0, 1, "lot-native", "dust_tracking"),
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_000_000_200,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_000_000_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.0,
            "raw_total_asset_qty": 0.00009193,
            "open_exposure_qty": 0.0,
            "dust_tracking_qty": 0.00009193,
            "normalized_exposure_active": False,
            "normalized_exposure_qty": 0.0,
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.00009193,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.00009193,
                    "effective_flat": False,
                    "entry_allowed": False,
                    "normalized_exposure_active": False,
                    "normalized_exposure_qty": 0.0,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_residual_present": 1,
            "dust_classification": "blocking_dust",
            "dust_effective_flat": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_residual_summary": "classification=blocking_dust harmless_dust=0 effective_flat=0",
        },
    )

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_tracking_only.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    order_row = conn.execute(
        """
        SELECT client_order_id, status, side, qty_req
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    submit_row = conn.execute(
        """
        SELECT reason_code, context_json, summary
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts DESC
        LIMIT 1
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()
    dust_event_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_suppressions
        WHERE reason_code=?
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()["n"]
    conn.close()

    assert order_count == 0
    assert order_row is None
    assert submit_row is None
    assert dust_event_count == 0


@pytest.mark.fast_regression
def test_live_execute_signal_sell_blocks_when_broker_precision_still_leaves_unsellable_dust(tmp_path):
    notifications: list[str] = []
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_precision_block.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db(str(tmp_path / "sell_dust_precision_block.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.000191939,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_precision_block.sqlite"))
    row = conn.execute(
        """
        SELECT reason_code, summary, context_json
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts DESC
        LIMIT 1
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchone()
    conn.close()

    assert row is None
    assert len(notifications) == 0
    monkeypatch.undo()


@pytest.mark.fast_regression
def test_live_execute_signal_sell_dust_unsellable_records_operational_event_and_dedups(tmp_path):
    notifications: list[str] = []
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_unsellable.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    conn = ensure_db(str(tmp_path / "sell_dust_unsellable.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()

    trade_first = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )
    trade_second = live_execute_signal(
        broker,
        "SELL",
        1001,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade_first is None
    assert trade_second is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_unsellable.sqlite"))
    order_row = conn.execute(
        """
        SELECT client_order_id, status, side, qty_req, decision_reason, exit_rule_name
        FROM orders
        WHERE side='SELL'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    event_rows = conn.execute(
        """
        SELECT reason_code, seen_count, dust_state, dust_action, summary, context_json
        FROM order_suppressions
        WHERE reason_code=?
        ORDER BY updated_ts
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchall()
    submit_attempt_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM order_events
        WHERE event_type='submit_attempt_recorded'
        """
    ).fetchone()[0]
    conn.close()

    assert order_row is None
    assert len(event_rows) == 1
    assert int(submit_attempt_count) == 0
    assert event_rows[0]["reason_code"] == DUST_RESIDUAL_UNSELLABLE
    assert int(event_rows[0]["seen_count"]) == 2
    assert event_rows[0]["dust_state"] == EXIT_PARTIAL_LEFT_DUST
    assert event_rows[0]["dust_action"] == MANUAL_DUST_REVIEW_REQUIRED
    assert "position_qty=0.000090000000" in str(event_rows[0]["summary"])
    assert "normalized_qty=0.000000000000" in str(event_rows[0]["summary"])
    assert "detail=boundary_below_min" in str(event_rows[0]["summary"])
    suppression_context = json.loads(str(event_rows[0]["context_json"]))
    assert suppression_context["raw_total_asset_qty"] == pytest.approx(0.00009)
    assert suppression_context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert suppression_context["terminal_state"] in {"dust_only", "non_executable_position"}
    assert suppression_context["exit_block_reason"] in {"dust_only_remainder", "no_position"}
    assert "decision_truth_sources" in suppression_context
    assert suppression_context["entry_allowed_truth_source"] == "-"
    assert suppression_context["effective_flat_truth_source"] == "-"
    assert suppression_context["position_qty"] == pytest.approx(0.00009)
    assert suppression_context["submit_payload_qty"] == pytest.approx(0.0)
    assert suppression_context["normalized_qty"] == pytest.approx(0.0)
    assert EXIT_PARTIAL_LEFT_DUST in str(event_rows[0]["summary"])
    assert MANUAL_DUST_REVIEW_REQUIRED in str(event_rows[0]["summary"])
    assert len(notifications) == 2
    assert all("event=decision_suppressed" in msg for msg in notifications)
    assert all("dust_state=blocking_dust" in msg for msg in notifications)
    assert all("dust_action=manual_review_before_resume" in msg for msg in notifications)
    assert all("dust_new_orders_allowed=0" in msg for msg in notifications)
    assert all("dust_resume_allowed=0" in msg for msg in notifications)
    monkeypatch.undo()


@pytest.mark.fast_regression
def test_live_execute_signal_sell_suppresses_harmless_dust_exit_without_order_row(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    monkeypatch.setattr(
        "bithumb_bot.broker.live._submit_attempt_id",
        lambda: (_ for _ in ()).throw(AssertionError("client_order_id must not be generated")),
    )
    monkeypatch.setattr(
        "bithumb_bot.broker.live._client_order_id",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("client_order_id must not be generated")),
    )
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_harmless_dust_suppression.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009193,
            "dust_local_qty": 0.00009193,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9_193.0,
            "dust_local_notional_krw": 9_193.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_suppression.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009193,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade_first = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )
    trade_second = live_execute_signal(
        broker,
        "SELL",
        1001,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade_first is None
    assert trade_second is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_suppression.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, seen_count, dust_state, dust_action, context_json
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_count == 0
    assert event_count == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    assert int(suppression_row["seen_count"]) == 2
    assert suppression_row["dust_state"] == "harmless_dust"
    assert suppression_row["dust_action"] == "harmless_dust_tracked_resume_allowed"
    context = json.loads(str(suppression_row["context_json"]))
    assert context["operator_action"] == "harmless_dust_tracked_resume_allowed"
    assert context["dust_action"] == "harmless_dust_tracked_resume_allowed"
    assert context["sell_failure_category"] == "dust_suppression"
    assert context["sell_failure_detail"] == "dust_suppression"
    assert context["submit_qty_source"] == "observation.sell_qty_preview"
    assert context["sell_submit_qty_source"] == "observation.sell_qty_preview"
    assert context["position_state_source"] == "observation.sell_qty_preview"
    assert context["entry_allowed_truth_source"] == "-"
    assert context["effective_flat_truth_source"] == "-"
    assert context["submit_qty_source_truth_source"] == "context.submit_qty_source"
    assert context["sell_submit_qty_source_truth_source"] == "context.submit_qty_source"
    assert any("event=decision_suppressed" in msg for msg in notifications)
    assert any("reason_code=DUST_RESIDUAL_SUPPRESSED" in msg for msg in notifications)


@pytest.mark.fast_regression
def test_live_execute_signal_sell_falls_back_to_harmless_dust_suppression_when_sell_guard_raises(
    monkeypatch,
    tmp_path,
):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_harmless_dust_fallback.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009193,
            "dust_local_qty": 0.00009193,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5_000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9_193.0,
            "dust_local_notional_krw": 9_193.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_fallback.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009193,
        asset_locked=0.0,
    )
    conn.commit()
    conn.close()

    original_suppression = live_module._record_harmless_dust_exit_suppression
    suppression_calls = {"count": 0}

    def _suppression_with_one_miss(*args, **kwargs):
        suppression_calls["count"] += 1
        if suppression_calls["count"] == 1:
            return False
        return original_suppression(*args, **kwargs)

    monkeypatch.setattr(
        live_module,
        "_record_harmless_dust_exit_suppression",
        _suppression_with_one_miss,
    )

    broker = _FakeBroker()
    trade_first = live_execute_signal(
        broker,
        "SELL",
        1000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )
    trade_second = live_execute_signal(
        broker,
        "SELL",
        1001,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade_first is None
    assert trade_second is None
    assert broker.place_order_calls == 0
    assert suppression_calls["count"] >= 2

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_fallback.sqlite"))
    order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
    event_count = conn.execute("SELECT COUNT(*) AS n FROM order_events").fetchone()["n"]
    submit_attempt_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type IN ('submit_attempt_preflight', 'submit_attempt_recorded', 'submit_blocked')
        """
    ).fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, seen_count, dust_state, dust_action, context_json
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_count == 0
    assert event_count == 0
    assert submit_attempt_count == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    assert int(suppression_row["seen_count"]) == 1
    assert suppression_row["dust_state"] == "harmless_dust"
    assert suppression_row["dust_action"] == "harmless_dust_tracked_resume_allowed"
    context = json.loads(str(suppression_row["context_json"]))
    assert context["operator_action"] == "harmless_dust_tracked_resume_allowed"
    assert context["entry_allowed_truth_source"] == "-"
    assert context["effective_flat_truth_source"] == "-"
    assert context["submit_qty_source_truth_source"] == "context.submit_qty_source"
    assert context["sell_submit_qty_source_truth_source"] == "context.submit_qty_source"
    assert any("event=decision_suppressed" in msg for msg in notifications)
    assert any("reason_code=DUST_RESIDUAL_SUPPRESSED" in msg for msg in notifications)


@pytest.mark.fast_regression
def test_live_execute_signal_sell_no_executable_exit_suppresses_before_broker_submit(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_no_executable_exit.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    metadata = {
        "dust_classification": "harmless_dust",
        "dust_residual_present": 1,
        "dust_residual_allow_resume": 1,
        "dust_effective_flat": 1,
        "dust_policy_reason": "matched_harmless_dust_resume_allowed",
        "dust_partial_flatten_recent": 0,
        "dust_partial_flatten_reason": "flatten_not_recent",
        "dust_qty_gap_tolerance": 0.00005,
        "dust_qty_gap_small": 1,
        "dust_broker_qty": 0.00009629,
        "dust_local_qty": 0.00009629,
        "dust_delta_qty": 0.0,
        "dust_min_qty": 0.0001,
        "dust_min_notional_krw": 5_000.0,
        "dust_latest_price": 100_000_000.0,
        "dust_broker_notional_krw": 9_629.0,
        "dust_local_notional_krw": 9_629.0,
        "dust_broker_qty_is_dust": 1,
        "dust_local_qty_is_dust": 1,
        "dust_broker_notional_is_dust": 0,
        "dust_local_notional_is_dust": 0,
        "dust_residual_summary": (
            "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
            "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
        ),
    }
    runtime_state.record_reconcile_result(success=True, metadata=metadata)

    conn = ensure_db(str(tmp_path / "sell_no_executable_exit.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009629,
        asset_locked=0.0,
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_001_200_000,
        strategy_name="dust_exit_test",
        signal="SELL",
        reason="partial_take_profit",
        candle_ts=1_700_001_140_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "entry_allowed": False,
            "effective_flat": False,
            "raw_qty_open": 0.00009629,
            "normalized_exposure_active": True,
            "normalized_exposure_qty": 0.0,
            "open_exposure_qty": 0.0,
            "dust_tracking_qty": 0.00009629,
            "sell_submit_lot_count": 0,
            "sell_submit_lot_source": "position_state.normalized_exposure.sellable_executable_lot_count",
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0,
                    "raw_total_asset_qty": 0.00009629,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.00009629,
                    "open_lot_count": 0,
                    "dust_tracking_lot_count": 1,
                    "reserved_exit_qty": 0.0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.0,
                    "sellable_executable_lot_count": 0,
                    "exit_allowed": False,
                    "exit_block_reason": "dust_only_remainder",
                    "terminal_state": "dust_only",
                    "normalized_exposure_qty": 0.0,
                    "normalized_exposure_active": False,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1200,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_id=decision_id,
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_no_executable_exit.sqlite"))
    order_events = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM order_events
        WHERE event_type IN ('submit_attempt_preflight', 'submit_attempt_recorded', 'submit_blocked')
        """
    ).fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, summary, context_json
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_events == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    assert "decision_suppressed:exit_suppressed_by_quantity_rule" in str(suppression_row["summary"])
    suppression_context = json.loads(str(suppression_row["context_json"]))
    assert suppression_context["sell_submit_lot_count"] == 0
    assert suppression_context["sell_submit_lot_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"
    assert suppression_context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert any("event=decision_suppressed" in msg for msg in notifications)


@pytest.mark.fast_regression
@pytest.mark.lot_native_regression_gate
def test_live_execute_signal_sell_ignores_stale_recorded_sellable_qty_without_current_lot_state(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_stale_recorded_qty.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    runtime_state.record_reconcile_result(success=True, metadata={"dust_residual_present": 0})

    conn = ensure_db(str(tmp_path / "sell_stale_recorded_qty.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_700_001_300_000,
        strategy_name="stale_sell_context",
        signal="SELL",
        reason="stale sellable qty",
        candle_ts=1_700_001_240_000,
        market_price=100_000_000.0,
        context={
            "base_signal": "SELL",
            "final_signal": "SELL",
            "raw_qty_open": 0.0002,
            "raw_total_asset_qty": 0.0002,
            "open_exposure_qty": 0.0002,
            "dust_tracking_qty": 0.0,
            "sellable_executable_qty": 0.0002,
            "sell_submit_lot_count": 2,
            "exit_allowed": True,
            "exit_block_reason": "none",
            "position_state": {
                "normalized_exposure": {
                    "raw_qty_open": 0.0002,
                    "raw_total_asset_qty": 0.0002,
                    "open_exposure_qty": 0.0002,
                    "dust_tracking_qty": 0.0,
                    "open_lot_count": 2,
                    "dust_tracking_lot_count": 0,
                    "reserved_exit_qty": 0.0,
                    "reserved_exit_lot_count": 0,
                    "sellable_executable_qty": 0.0002,
                    "sellable_executable_lot_count": 2,
                    "exit_allowed": True,
                    "exit_block_reason": "none",
                    "terminal_state": "open",
                    "normalized_exposure_qty": 0.0002,
                    "normalized_exposure_active": True,
                    "entry_allowed": False,
                    "effective_flat": False,
                }
            },
        },
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        1300,
        100_000_000.0,
        strategy_name="stale_sell_context",
        decision_id=decision_id,
        decision_reason="stale sellable qty",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_stale_recorded_qty.sqlite"))
    order_count = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM orders
        WHERE side='SELL'
        """
    ).fetchone()["n"]
    suppression_row = conn.execute(
        """
        SELECT reason_code, context_json
        FROM order_suppressions
        WHERE strategy_name='stale_sell_context' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert order_count == 0
    assert suppression_row is None


def test_bithumb_broker_defensively_rejects_sell_qty_below_executable_threshold(monkeypatch):
    original = {
        "LIVE_DRY_RUN": bool(settings.LIVE_DRY_RUN),
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
    }
    try:
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)

        broker = BithumbBroker()
        monkeypatch.setattr(broker, "_market", lambda: "KRW-BTC")
        monkeypatch.setattr(
            live_module,
            "get_effective_order_rules",
            lambda _pair: SimpleNamespace(
                rules=SimpleNamespace(
                    market_id="KRW-BTC",
                    bid_min_total_krw=5000.0,
                    ask_min_total_krw=5000.0,
                    bid_price_unit=1.0,
                    ask_price_unit=1.0,
                    order_types=("limit", "price", "market"),
                    order_sides=("ask", "bid"),
                    bid_fee=0.0025,
                    ask_fee=0.0025,
                    maker_bid_fee=0.0020,
                    maker_ask_fee=0.0020,
                    min_qty=0.0001,
                    qty_step=0.0001,
                    min_notional_krw=5000.0,
                    max_qty_decimals=8,
                )
            ),
        )
        monkeypatch.setattr(
            broker,
            "_post_private",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("broker should reject before HTTP")),
        )

        with pytest.raises(BrokerRejectError, match="qty suppressed by quantity rule"):
            broker.place_order(client_order_id="cid-defensive", side="SELL", qty=0.00005, price=None)
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


@pytest.mark.fast_regression
def test_harmless_dust_exit_suppression_blocks_sell_path_even_without_sub_min_qty_preview(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_harmless_dust_state_only.sqlite"))
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    metadata = {
        "dust_classification": "harmless_dust",
        "dust_residual_present": 1,
        "dust_residual_allow_resume": 1,
        "dust_effective_flat": 1,
        "dust_policy_reason": "matched_harmless_dust_resume_allowed",
        "dust_partial_flatten_recent": 0,
        "dust_partial_flatten_reason": "flatten_not_recent",
        "dust_qty_gap_tolerance": 0.00005,
        "dust_qty_gap_small": 1,
        "dust_broker_qty": 0.00009193,
        "dust_local_qty": 0.00009193,
        "dust_delta_qty": 0.0,
        "dust_min_qty": 0.0001,
        "dust_min_notional_krw": 5_000.0,
        "dust_latest_price": 100_000_000.0,
        "dust_broker_notional_krw": 9_193.0,
        "dust_local_notional_krw": 9_193.0,
        "dust_broker_qty_is_dust": 1,
        "dust_local_qty_is_dust": 1,
        "dust_broker_notional_is_dust": 0,
        "dust_local_notional_is_dust": 0,
        "dust_residual_summary": (
            "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
            "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
        ),
    }
    runtime_state.record_reconcile_result(success=True, metadata=metadata)

    conn = ensure_db(str(tmp_path / "sell_harmless_dust_state_only.sqlite"))
    try:
        state = type("_State", (), {"last_reconcile_metadata": json.dumps(metadata)})()
        suppressed = live_module._record_harmless_dust_exit_suppression(
            conn=conn,
            state=state,
            signal="SELL",
            side="SELL",
            requested_qty=0.0002,
            market_price=100_000_000.0,
            normalized_qty=0.0002,
            strategy_name="dust_exit_test",
            decision_id=77,
            decision_reason="partial_take_profit",
            exit_rule_name="exit_signal",
        )
        conn.commit()

        order_count = conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()["n"]
        suppression_row = conn.execute(
            """
            SELECT reason_code, dust_state, dust_action, summary, context_json
            FROM order_suppressions
            WHERE strategy_name='dust_exit_test' AND signal='SELL'
            """
        ).fetchone()
    finally:
        conn.close()

    assert suppressed is True
    assert order_count == 0
    assert suppression_row is not None
    assert suppression_row["reason_code"] == DUST_RESIDUAL_SUPPRESSED
    assert suppression_row["dust_state"] == "harmless_dust"
    assert suppression_row["dust_action"] == "harmless_dust_tracked_resume_allowed"
    assert "suppression_scope=harmless_dust_effective_flat" in suppression_row["summary"]
    assert "effective_flat_due_to_harmless_dust=1" in suppression_row["summary"]
    context = json.loads(str(suppression_row["context_json"]))
    assert context["operator_action"] == "harmless_dust_tracked_resume_allowed"
    assert context["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert context["sell_submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_qty"
    assert context["sell_submit_qty_source_truth_source"] == "derived:sellable_executable_qty"
    assert len(notifications) == 1
    assert "reason_code=DUST_RESIDUAL_SUPPRESSED" in notifications[0]


@pytest.mark.fast_regression
def test_live_execute_signal_sell_with_dust_and_unresolved_open_order_still_records_dust_evidence(monkeypatch, tmp_path):
    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda msg: notifications.append(msg))
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "sell_dust_with_open.sqlite"))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)

    now_ms = int(time.time() * 1000)
    conn = ensure_db(str(tmp_path / "sell_dust_with_open.sqlite"))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.00009,
        asset_locked=0.0,
    )
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('existing_open_sell','ex_open_sell','NEW','SELL',100000000.0,0.00009,0,?,?,NULL)
        """,
        (now_ms, now_ms),
    )
    conn.commit()
    conn.close()

    broker = _FakeBroker()
    trade = live_execute_signal(
        broker,
        "SELL",
        3000,
        100_000_000.0,
        strategy_name="dust_exit_test",
        decision_reason="partial_take_profit",
        exit_rule_name="exit_signal",
    )

    assert trade is None
    assert broker.place_order_calls == 0

    conn = ensure_db(str(tmp_path / "sell_dust_with_open.sqlite"))
    dust_blocked = conn.execute(
        """
        SELECT reason_code, summary
        FROM order_suppressions
        WHERE strategy_name='dust_exit_test' AND signal='SELL'
        ORDER BY updated_ts DESC
        LIMIT 1
        """
    ).fetchone()
    dust_rows = conn.execute(
        """
        SELECT reason_code
        FROM order_suppressions
        WHERE reason_code=?
        """,
        (DUST_RESIDUAL_UNSELLABLE,),
    ).fetchall()
    conn.close()

    assert dust_blocked is not None
    assert dust_blocked["reason_code"] == DUST_RESIDUAL_UNSELLABLE
    assert EXIT_PARTIAL_LEFT_DUST in str(dust_blocked["summary"])
    assert MANUAL_DUST_REVIEW_REQUIRED in str(dust_blocked["summary"])
    assert len(dust_rows) == 1
    assert any("event=decision_suppressed" in msg for msg in notifications)


def test_validate_pretrade_applies_side_specific_min_total():
    from bithumb_bot.broker import live as live_module
    from bithumb_bot.broker import order_rules

    original_pair = settings.PAIR
    object.__setattr__(settings, "PAIR", "KRW-BTC")

    class _BalanceOnlyBroker:
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=1_000_000.0, asset_available=100.0, cash_locked=0.0, asset_locked=0.0)

    broker = _BalanceOnlyBroker()
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                bid_min_total_krw=5100.0,
                ask_min_total_krw=5000.0,
                min_notional_krw=7000.0,
                min_qty=0.0001,
                qty_step=0.0001,
                max_qty_decimals=8,
            ),
            source={},
        ),
    )
    monkeypatch.setattr(
        live_module,
        "_load_live_reference_quote",
        lambda **_kwargs: {
            "bid": 100.0,
            "ask": 100.1,
            "reference_price": 100.05,
            "reference_ts_epoch_sec": 1_700_000_000.0,
            "reference_source": "test",
        },
    )
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    try:
        with pytest.raises(ValueError, match=r"order notional below minimum \(BUY\): 5050.00 < 5100.00"):
            validate_pretrade(broker=broker, side="BUY", qty=50.5, market_price=100.0)

        validate_pretrade(broker=broker, side="SELL", qty=50.5, market_price=100.0)
    finally:
        object.__setattr__(settings, "PAIR", original_pair)
        monkeypatch.undo()

def test_live_submit_attempt_reason_codes_cover_ambiguous_paths(tmp_path, monkeypatch):
    from bithumb_bot.broker import order_rules
    import bithumb_bot.order_sizing as order_sizing

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "START_CASH_KRW", 1000000.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "PRETRADE_BALANCE_BUFFER_BPS", 0.0)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 0.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)
    object.__setattr__(settings, "BITHUMB_API_KEY", "test-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "test-secret")
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "bootstrap_live_state.sqlite"))
    runtime_state.enable_trading()
    monkeypatch.setattr(
        "bithumb_bot.order_sizing.get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                min_notional_krw=0.0,
                min_qty=0.0001,
                qty_step=0.0001,
                max_qty_decimals=8,
            ),
            source={"source": "test"},
        ),
    )
    monkeypatch.setattr(
        order_sizing,
        "get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                min_notional_krw=0.0,
                min_qty=0.0001,
                qty_step=0.0001,
                max_qty_decimals=8,
            ),
            source={"source": "test"},
        ),
    )
    monkeypatch.setattr(
        live_module,
        "_load_live_reference_quote",
        lambda **_kwargs: {
            "bid": 100.0,
            "ask": 100.1,
            "reference_price": 100.05,
            "reference_ts_epoch_sec": 1_700_000_000.0,
            "reference_source": "test",
        },
    )

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
            (f"%_{ts}_buy_%",),
        ).fetchone()
        assert row is not None
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
