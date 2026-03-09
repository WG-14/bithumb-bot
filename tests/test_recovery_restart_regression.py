from __future__ import annotations

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.engine import evaluate_startup_safety_gate
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.oms import set_exchange_order_id, set_status
from bithumb_bot.recovery import reconcile_with_broker
import bithumb_bot.recovery as recovery_module


@pytest.fixture
def isolated_db(tmp_path):
    old_db_path = settings.DB_PATH
    db_path = tmp_path / "restart_regression.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    ensure_db().close()
    runtime_state.enable_trading()
    runtime_state.set_startup_gate_reason(None)
    runtime_state.record_reconcile_result(success=True, reason_code=None, metadata=None, now_epoch_sec=0.0)
    yield db_path
    object.__setattr__(settings, "DB_PATH", old_db_path)


class _NoopBroker:
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-noop", "BUY", "NEW", 100.0, 1.0, 0.0, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

    def get_open_orders(self) -> list[BrokerOrder]:
        return []

    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(cash_available=1000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)


class _RecentFillBroker(_NoopBroker):
    def __init__(self, *, status: str = "FILLED") -> None:
        self.status = status

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-partial", "BUY", self.status, 100.0, 1.0, 1.0, 1, 1)

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="fill-rest",
                fill_ts=220,
                price=100.0,
                qty=0.6,
                fee=0.0,
                exchange_order_id="ex-partial",
            )
        ]


class _CancelRaceBroker(_NoopBroker):
    def __init__(self) -> None:
        self.remote_status = "NEW"

    def get_open_orders(self) -> list[BrokerOrder]:
        if self.remote_status == "CANCELED":
            return []
        return [BrokerOrder("", "ex-cancel-race", "BUY", "NEW", 100.0, 1.0, 0.0, 1, 1)]

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        self.remote_status = "CANCELED"
        return BrokerOrder(client_order_id, exchange_order_id or "ex-cancel-race", "BUY", "CANCELED", 100.0, 1.0, 0.0, 1, 1)

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-cancel-race", "BUY", self.remote_status, 100.0, 1.0, 0.0, 1, 1)



def test_restart_after_submit_immediate_exit_keeps_gate_blocked(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_crash",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="PENDING_SUBMIT",
        )
        conn.commit()
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()

    assert reason is not None
    assert "unresolved_open_orders=1" in reason
    assert state.unresolved_open_order_count == 1
    assert state.startup_gate_reason == reason



def test_restart_after_partial_fill_applies_recent_fill_and_clears_gate(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="partial_crash",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("partial_crash", "ex-partial", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="partial_crash",
            side="BUY",
            fill_id="fill-part",
            fill_ts=120,
            price=100.0,
            qty=0.4,
            fee=0.0,
        )
        set_status("partial_crash", "PARTIAL", conn=conn)
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_RecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='partial_crash'"
        ).fetchone()
        fills = conn.execute("SELECT COUNT(*) AS c FROM fills WHERE client_order_id='partial_crash'").fetchone()[0]
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert fills == 2
    assert reason is None
    assert state.unresolved_open_order_count == 0



def test_restart_during_cancel_request_reconciles_to_canceled(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="cancel_race",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("cancel_race", "ex-cancel-race", conn=conn)
        conn.commit()
    finally:
        conn.close()

    broker = _CancelRaceBroker()
    broker.cancel_order(client_order_id="cancel_race", exchange_order_id="ex-cancel-race")

    reconcile_with_broker(broker)

    conn = ensure_db(str(isolated_db))
    try:
        status = conn.execute("SELECT status FROM orders WHERE client_order_id='cancel_race'").fetchone()[0]
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()
    assert status == "CANCELED"
    assert reason is None



def test_restart_mid_reconcile_rolls_back_then_retries_cleanly(isolated_db, monkeypatch):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="reconcile_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="PARTIAL",
        )
        set_exchange_order_id("reconcile_restart", "ex-partial", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="reconcile_restart",
            side="BUY",
            fill_id="fill-existing",
            fill_ts=110,
            price=100.0,
            qty=0.4,
            fee=0.0,
        )
        conn.commit()
    finally:
        conn.close()

    original_set_portfolio_breakdown = recovery_module.set_portfolio_breakdown

    def _crash_once(*args, **kwargs):
        raise RuntimeError("crash during reconcile")

    monkeypatch.setattr("bithumb_bot.recovery.set_portfolio_breakdown", _crash_once)
    monkeypatch.setattr("bithumb_bot.recovery.runtime_state.record_reconcile_result", lambda **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.recovery.runtime_state.refresh_open_order_health", lambda **_kwargs: None)
    with pytest.raises(RuntimeError, match="crash during reconcile"):
        reconcile_with_broker(_RecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        fills_after_crash = conn.execute("SELECT COUNT(*) FROM fills WHERE client_order_id='reconcile_restart'").fetchone()[0]
    finally:
        conn.close()

    assert fills_after_crash == 1

    monkeypatch.setattr("bithumb_bot.recovery.set_portfolio_breakdown", original_set_portfolio_breakdown)
    reconcile_with_broker(_RecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='reconcile_restart'"
        ).fetchone()
        fills_after_retry = conn.execute("SELECT COUNT(*) FROM fills WHERE client_order_id='reconcile_restart'").fetchone()[0]
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert fills_after_retry == 2
    assert evaluate_startup_safety_gate() is None
