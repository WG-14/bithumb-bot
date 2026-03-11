from __future__ import annotations

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.engine import evaluate_startup_safety_gate, run_loop
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.oms import set_exchange_order_id, set_status
from bithumb_bot.recovery import reconcile_with_broker
from tests.test_failsafe import _prepare_run_loop


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    old_db_path = settings.DB_PATH
    old_mode = settings.MODE
    db_path = tmp_path / "failure_scenarios.sqlite"

    monkeypatch.setenv("DB_PATH", str(db_path))
    object.__setattr__(settings, "DB_PATH", str(db_path))

    ensure_db().close()
    runtime_state.enable_trading()
    runtime_state.set_startup_gate_reason(None)
    runtime_state.record_reconcile_result(success=True, reason_code=None, metadata=None, now_epoch_sec=0.0)

    yield db_path

    object.__setattr__(settings, "DB_PATH", old_db_path)
    object.__setattr__(settings, "MODE", old_mode)


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


class _FillRecoveryBroker(_NoopBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-partial", "BUY", "FILLED", 100.0, 1.0, 1.0, 1, 1)

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


class _CancelThenLateFillBroker(_NoopBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-cancel", "BUY", "CANCELED", 100.0, 1.0, 0.0, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="late-fill-1",
                fill_ts=250,
                price=100.0,
                qty=0.25,
                fee=0.0,
                exchange_order_id="ex-cancel",
            )
        ]

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return []


def _patch_single_tick_live_loop(monkeypatch) -> None:
    monkeypatch.setattr("bithumb_bot.config.notifier_is_configured", lambda: True)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "INTERVAL", "1m")
    object.__setattr__(settings, "OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC", 30)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 1)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)

    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 1)
    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr(
        "bithumb_bot.engine.compute_signal",
        lambda conn, s, l: {
            "ts": 1000,
            "last_close": 100.0,
            "curr_s": 1.0,
            "curr_l": 0.5,
            "signal": "BUY",
        },
    )
    monkeypatch.setattr("bithumb_bot.engine.BithumbBroker", lambda: object())
    monkeypatch.setattr("bithumb_bot.engine.evaluate_daily_loss_breach", lambda *_args, **_kwargs: (False, "ok"))

    ticks = iter([10.0, 11.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(ticks, 11.0))

    sleeps = {"n": 0}

    def _sleep(_sec: float):
        sleeps["n"] += 1
        if sleeps["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)


def test_failure_scenario_submit_timeout_restart_reconcile_blocks_until_manual_recovery(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_timeout_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_NoopBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    gate_reason = evaluate_startup_safety_gate()
    assert gate_reason is not None
    assert "recovery_required_orders=1" in gate_reason


def test_failure_scenario_partial_fill_restart_recovers_ledger_and_clears_gate(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(conn, client_order_id="partial_restart", side="BUY", qty_req=1.0, price=100.0, ts_ms=100, status="NEW")
        set_exchange_order_id("partial_restart", "ex-partial", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="partial_restart",
            side="BUY",
            fill_id="fill-part",
            fill_ts=120,
            price=100.0,
            qty=0.4,
            fee=0.0,
        )
        set_status("partial_restart", "PARTIAL", conn=conn)
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_FillRecoveryBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute("SELECT status, qty_filled FROM orders WHERE client_order_id='partial_restart'").fetchone()
        fill_count = conn.execute("SELECT COUNT(*) FROM fills WHERE client_order_id='partial_restart'").fetchone()[0]
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert fill_count == 2
    assert evaluate_startup_safety_gate() is None


def test_failure_scenario_cancel_then_late_fill_is_applied_deterministically(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(conn, client_order_id="cancel_late_fill", side="BUY", qty_req=1.0, price=100.0, ts_ms=100, status="NEW")
        set_exchange_order_id("cancel_late_fill", "ex-cancel", conn=conn)
        conn.commit()
    finally:
        conn.close()

    reconcile_with_broker(_CancelThenLateFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute("SELECT status, qty_filled FROM orders WHERE client_order_id='cancel_late_fill'").fetchone()
        fills = conn.execute("SELECT COUNT(*) FROM fills WHERE client_order_id='cancel_late_fill'").fetchone()[0]
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "CANCELED"
    assert float(row["qty_filled"]) == pytest.approx(0.25)
    assert fills == 1


def test_failure_scenario_stale_open_order_detection_triggers_safe_halt(isolated_db, monkeypatch):
    loop_conn = _prepare_run_loop(monkeypatch, open_order_created_ts=1)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 5)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine._attempt_open_order_cancellation", lambda *_args, **_kwargs: True)

    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert loop_conn.marked_recovery_required == 1
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "STALE_OPEN_ORDER"


def test_failure_scenario_recovery_required_ambiguity_blocks_new_trading_loop_progress(isolated_db, monkeypatch):
    _patch_single_tick_live_loop(monkeypatch)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_unknown_restart",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="RECOVERY_REQUIRED",
        )
        conn.commit()
    finally:
        conn.close()

    calls = {"n": 0}
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: calls.__setitem__("n", calls["n"] + 1))

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert calls["n"] == 0
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.startup_gate_reason is not None
    assert "recovery_required_orders=1" in state.startup_gate_reason


def test_failure_scenario_position_hard_loss_breach_triggers_halt(isolated_db, monkeypatch):
    _prepare_run_loop(monkeypatch)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )

    live_calls = {"n": 0}
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: live_calls.__setitem__("n", live_calls["n"] + 1))

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert live_calls["n"] == 0
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "POSITION_LOSS_LIMIT"
