from __future__ import annotations

import time

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.app import _load_recovery_report, cmd_pause, cmd_reconcile, cmd_recover_order, cmd_recovery_report, cmd_resume
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db


def _set_tmp_db(tmp_path):
    db_path = tmp_path / "operator.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    return db_path


def _insert_order(*, status: str, client_order_id: str, created_ts: int) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price,
                qty_req, qty_filled, created_ts, updated_ts, last_error
            )
            VALUES (?, NULL, ?, 'BUY', NULL, 0.01, 0.0, ?, ?, NULL)
            """,
            (client_order_id, status, created_ts, created_ts),
        )
        conn.commit()
    finally:
        conn.close()


def _set_last_error(*, client_order_id: str, last_error: str) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            "UPDATE orders SET last_error=? WHERE client_order_id=?",
            (last_error, client_order_id),
        )
        conn.commit()
    finally:
        conn.close()


class _RecoverSuccessBroker:
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "FILLED", None, 0.01, 0.01, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id=str(client_order_id or ""),
                fill_id="recover_fill_1",
                fill_ts=1000,
                price=100000000.0,
                qty=0.01,
                fee=10.0,
                exchange_order_id=exchange_order_id,
            )
        ]

    def get_open_orders(self):
        return []

    def get_recent_orders(self, *, limit: int = 100):
        return []

    def get_recent_fills(self, *, limit: int = 100):
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)


class _RecoverAmbiguousBroker(_RecoverSuccessBroker):
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id, "BUY", "NEW", None, 0.01, 0.0, 1, 1)


def test_pause_disables_trading_via_persistent_runtime_state(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    cmd_pause()

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason == "manual operator pause"
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "MANUAL_PAUSE"
    assert state.halt_state_unresolved is False


def test_resume_refuses_when_unresolved_state_exists_without_force(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is False
    assert state.halt_reason_code is None




def test_resume_runs_preflight_reconcile_and_refuses_when_recovery_required(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    calls = {"n": 0}

    def _reconcile(_broker):
        calls["n"] += 1

    monkeypatch.setattr("bithumb_bot.app.reconcile_with_broker", _reconcile)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1
    assert calls["n"] == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is False


def test_resume_refuses_when_halt_state_unresolved_even_without_open_orders(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed (RuntimeError): boom",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_state_unresolved is True

def test_resume_force_enables_even_when_unresolved_state_exists(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    cmd_resume(force=True)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.retry_at_epoch_sec is None
    assert state.halt_new_orders_blocked is False
    assert state.halt_reason_code is None
    assert state.halt_state_unresolved is False



def test_cancel_open_orders_persists_runtime_state(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())
    monkeypatch.setattr(
        "bithumb_bot.app.cancel_open_orders_with_broker",
        lambda _broker: {
            "remote_open_count": 2,
            "canceled_count": 1,
            "matched_local_count": 1,
            "stray_canceled_count": 0,
            "failed_count": 1,
            "stray_messages": [],
            "error_messages": ["cancel failed: order_2"],
        },
    )

    try:
        from bithumb_bot.app import cmd_cancel_open_orders

        cmd_cancel_open_orders()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    state = runtime_state.snapshot()
    assert state.last_cancel_open_orders_trigger == "operator-command"
    assert state.last_cancel_open_orders_status == "partial"
    assert state.last_cancel_open_orders_epoch_sec is not None
    assert state.last_cancel_open_orders_summary is not None
    assert '"failed_count": 1' in state.last_cancel_open_orders_summary

def test_recovery_report_summarizes_unresolved_and_recovery_required(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="open_1", created_ts=now_ms - 30_000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="open_2", created_ts=now_ms - 20_000)

    report = _load_recovery_report()

    assert int(report["unresolved_count"]) == 2
    assert int(report["recovery_required_count"]) == 1
    assert report["oldest_unresolved_age_sec"] is not None
    assert float(report["oldest_unresolved_age_sec"]) >= 20.0
    oldest_orders = report["oldest_orders"]
    assert isinstance(oldest_orders, list)
    assert len(oldest_orders) == 2
    assert oldest_orders[0]["client_order_id"] == "open_1"
    assert oldest_orders[0]["status"] == "NEW"
    assert oldest_orders[1]["client_order_id"] == "open_2"


def test_recovery_report_shows_concise_oldest_order_list(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    for i in range(6):
        _insert_order(
            status="RECOVERY_REQUIRED" if i % 2 == 0 else "NEW",
            client_order_id=f"open_{i}",
            created_ts=now_ms - (60_000 - i * 1_000),
        )
    _set_last_error(
        client_order_id="open_0",
        last_error="timeout while polling exchange status endpoint due to transient error and retry budget exceeded",
    )

    cmd_recovery_report()
    out = capsys.readouterr().out

    assert "[RECOVERY-REPORT]" in out
    assert "unresolved_open_orders=6" in out
    assert "recovery_required_orders=3" in out
    assert "oldest_unresolved_orders(top 5):" in out
    assert "client_order_id=open_0" in out
    assert "client_order_id=open_4" in out
    assert "client_order_id=open_5" not in out
    assert "last_error=timeout while polling exchange status endpoint due to transi..." in out


def test_reconcile_skips_in_non_live_mode(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "paper")
    try:
        cmd_reconcile()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "[RECONCILE] skipped" in out


def test_recover_order_success_for_known_exchange_order_id(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _RecoverSuccessBroker())

    try:
        cmd_recover_order(client_order_id="needs_recovery", exchange_order_id="ex_manual_1")
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, qty_filled FROM orders WHERE client_order_id='needs_recovery'"
        ).fetchone()
        fills = conn.execute(
            "SELECT fill_id, qty, fee FROM fills WHERE client_order_id='needs_recovery'"
        ).fetchall()
        trades = conn.execute(
            "SELECT side, qty, fee FROM trades WHERE note LIKE 'manual recovery%'"
        ).fetchall()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex_manual_1"
    assert row["qty_filled"] == pytest.approx(0.01)
    assert len(fills) == 1
    assert fills[0]["fill_id"] == "recover_fill_1"
    assert fills[0]["qty"] == pytest.approx(0.01)
    assert len(trades) == 1
    assert trades[0]["side"] == "BUY"


def test_recover_order_failure_keeps_recovery_required_and_exits_non_zero(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _RecoverAmbiguousBroker())

    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(client_order_id="needs_recovery", exchange_order_id="ex_manual_2")
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    assert exc.value.code == 1

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id, last_error FROM orders WHERE client_order_id='needs_recovery'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] == "ex_manual_2"
    assert "manual recovery failed" in str(row["last_error"])


def test_recover_order_does_not_auto_resume_trading(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _RecoverSuccessBroker())

    runtime_state.enable_trading()
    try:
        cmd_recover_order(client_order_id="needs_recovery", exchange_order_id="ex_manual_3")
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")


def test_resume_succeeds_after_manual_recovery_clears_recovery_required(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _RecoverSuccessBroker())

    try:
        runtime_state.disable_trading_until(float("inf"), reason="startup recovery gate")
        cmd_recover_order(client_order_id="needs_recovery", exchange_order_id="ex_manual_4")
        cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    report = _load_recovery_report()
    state = runtime_state.snapshot()
    assert int(report["recovery_required_count"]) == 0
    assert int(report["unresolved_count"]) == 0
    assert state.trading_enabled is True
