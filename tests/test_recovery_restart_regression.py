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
import bithumb_bot.recovery as recovery_module


@pytest.fixture
def isolated_db(tmp_path):
    old_db_path = settings.DB_PATH
    old_mode = settings.MODE
    db_path = tmp_path / "restart_regression.sqlite"
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


class _SubmitUnknownRecentFillBroker(_NoopBroker):
    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="submit_timeout_restart",
                fill_id="submit_unknown_fill",
                fill_ts=300,
                price=100.0,
                qty=1.0,
                fee=0.0,
                exchange_order_id="ex-submit-unknown-fill",
            )
        ]


class _SubmitUnknownRecentOrderBroker(_NoopBroker):
    def get_recent_orders(self, *, limit: int = 100) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-order",
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=250,
                updated_ts=260,
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


class _ApiErrorBroker(_NoopBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        raise RuntimeError("broker api unavailable")



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


def test_startup_gate_explicitly_blocks_pending_submit_order(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="pending_submit_blocker",
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

    assert reason is not None
    assert "pending_submit_orders=1" in reason


def test_startup_gate_explicitly_blocks_submit_unknown_order(isolated_db):
    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_unknown_blocker",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="SUBMIT_UNKNOWN",
        )
        conn.commit()
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()

    assert reason is not None
    assert "submit_unknown_orders=1" in reason


def test_submit_timeout_then_restart_moves_to_recovery_required_and_stays_blocked(isolated_db):
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
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None
    assert "manual recovery required" in str(row["last_error"])
    assert reason is not None
    assert "recovery_required_orders=1" in reason
    assert state.unresolved_open_order_count == 1


def test_submit_unknown_without_exchange_id_resolves_from_recent_fill_on_restart(isolated_db):
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

    reconcile_with_broker(_SubmitUnknownRecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, qty_filled FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex-submit-unknown-fill"
    assert float(row["qty_filled"]) == pytest.approx(1.0)
    assert evaluate_startup_safety_gate() is None


def test_submit_unknown_recent_fill_restart_path_applies_fill_and_clears_unresolved_state(isolated_db):
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

    reconcile_with_broker(_SubmitUnknownRecentFillBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()[0]
    finally:
        conn.close()

    state = runtime_state.snapshot()
    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex-submit-unknown-fill"
    assert row["last_error"] is None
    assert fill_count == 1
    assert evaluate_startup_safety_gate() is None
    assert state.unresolved_open_order_count == 0


def test_submit_unknown_without_exchange_id_resolves_from_recent_order_on_restart(isolated_db):
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

    reconcile_with_broker(_SubmitUnknownRecentOrderBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "CANCELED"
    assert row["exchange_order_id"] == "ex-submit-unknown-order"
    assert evaluate_startup_safety_gate() is None


def test_submit_unknown_recent_order_restart_path_avoids_manual_recovery_and_clears_gate(isolated_db):
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

    reconcile_with_broker(_SubmitUnknownRecentOrderBroker())

    conn = ensure_db(str(isolated_db))
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='submit_timeout_restart'"
        ).fetchone()
    finally:
        conn.close()

    state = runtime_state.snapshot()
    assert row is not None
    assert row["status"] == "CANCELED"
    assert row["exchange_order_id"] == "ex-submit-unknown-order"
    assert row["last_error"] is None
    assert evaluate_startup_safety_gate() is None
    assert state.unresolved_open_order_count == 0



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


def test_submit_success_then_crash_restart_blocks_new_submit_attempt(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="submit_success_crash",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("submit_success_crash", "ex-submit-success", conn=conn)
        conn.commit()
    finally:
        conn.close()

    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    run_loop(5, 20)

    reason = evaluate_startup_safety_gate()
    state = runtime_state.snapshot()
    assert reason is not None
    assert "unresolved_open_orders=1" in reason
    assert state.trading_enabled is False
    assert live_execute_calls["n"] == 0



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


def test_restart_reconcile_api_exception_halts_and_prevents_resume(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="reconcile_api_exception",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("reconcile_api_exception", "ex-api-down", conn=conn)
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr("bithumb_bot.engine.BithumbBroker", lambda: _ApiErrorBroker())
    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "INITIAL_RECONCILE_FAILED"
    assert live_execute_calls["n"] == 0


def _patch_single_tick_run_loop(monkeypatch) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "INTERVAL", "1m")
    object.__setattr__(settings, "OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC", 30)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 900)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)

    object.__setattr__(settings, "MAX_ORDER_KRW", 100000)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)

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
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (False, "ok"),
    )

    ticks = iter([10.0, 11.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(ticks, 11.0))

    sleeps = {"n": 0}

    def _sleep(_sec: float):
        sleeps["n"] += 1
        if sleeps["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)


def test_restart_with_risky_state_does_not_resume_trading_loop(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="restart_block",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="RECOVERY_REQUIRED",
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.startup_gate_reason is not None
    assert "recovery_required_orders=1" in state.startup_gate_reason
    assert live_execute_calls["n"] == 0


def test_restart_while_persisted_halted_does_not_resume_trading_loop(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    runtime_state.enter_halt(
        reason_code="MANUAL_HALT",
        reason="operator requested stop",
        unresolved=True,
    )

    reconcile_calls = {"n": 0}
    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.recovery.reconcile_with_broker",
        lambda _broker: reconcile_calls.__setitem__("n", reconcile_calls["n"] + 1),
        raising=False,
    )
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "MANUAL_HALT"
    assert reconcile_calls["n"] == 0
    assert live_execute_calls["n"] == 0


def test_restart_startup_proceeds_when_reconcile_clears_risky_state(isolated_db, monkeypatch):
    _patch_single_tick_run_loop(monkeypatch)

    conn = ensure_db(str(isolated_db))
    try:
        record_order_if_missing(
            conn,
            client_order_id="restart_clear",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        set_exchange_order_id("restart_clear", "ex-restart-clear", conn=conn)
        conn.commit()
    finally:
        conn.close()

    class _ResolveToCanceledBroker(_NoopBroker):
        def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
            return BrokerOrder(client_order_id, exchange_order_id or "ex-restart-clear", "BUY", "CANCELED", 100.0, 1.0, 0.0, 1, 1)

    monkeypatch.setattr(
        "bithumb_bot.recovery.reconcile_with_broker",
        lambda _broker: reconcile_with_broker(_ResolveToCanceledBroker()),
        raising=False,
    )
    live_execute_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_execute_calls.__setitem__("n", live_execute_calls["n"] + 1),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.startup_gate_reason is None
    assert state.trading_enabled is True
