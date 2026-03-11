from __future__ import annotations

import os
from pathlib import Path

import httpx
import pytest

from bithumb_bot import runtime_state
from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.engine import get_health_status, run_loop
from bithumb_bot.marketdata import _get_with_retry


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path):
    old_settings = {
        "DB_PATH": settings.DB_PATH,
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "MAX_ORDER_KRW": settings.MAX_ORDER_KRW,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "MAX_DAILY_ORDER_COUNT": settings.MAX_DAILY_ORDER_COUNT,
        "MAX_OPEN_ORDER_AGE_SEC": settings.MAX_OPEN_ORDER_AGE_SEC,
        "KILL_SWITCH": settings.KILL_SWITCH,
        "KILL_SWITCH_LIQUIDATE": settings.KILL_SWITCH_LIQUIDATE,
        "BITHUMB_API_KEY": settings.BITHUMB_API_KEY,
        "BITHUMB_API_SECRET": settings.BITHUMB_API_SECRET,
    }
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "failsafe.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", False)

    ensure_db().close()
    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
    runtime_state.set_startup_gate_reason(None)

    yield

    for key, value in old_settings.items():
        object.__setattr__(settings, key, value)

    if old_env_db_path is None:
        os.environ.pop("DB_PATH", None)
    else:
        os.environ["DB_PATH"] = old_env_db_path

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
    runtime_state.set_startup_gate_reason(None)


def _set_tmp_db(tmp_path, monkeypatch: pytest.MonkeyPatch | None = None):
    db_path = str(tmp_path / "live_loop.sqlite")
    if monkeypatch is not None:
        monkeypatch.setenv("DB_PATH", db_path)
    else:
        os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    ensure_db().close()
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


class _LoopConn:
    def __init__(self, *, open_order_created_ts: int | None = None):
        self.open_order_created_ts = open_order_created_ts
        self.marked_recovery_required = 0

    def execute(self, query, params=None):
        q = " ".join(str(query).split())

        if "FROM candles" in q:
            return _Rows({"ts": int(10_000_000_000_000), "close": 100.0})

        if "COUNT(*) AS open_count" in q:
            if self.open_order_created_ts is None:
                return _Rows({"open_count": 0, "oldest_created_ts": None})
            return _Rows({"open_count": 1, "oldest_created_ts": self.open_order_created_ts})

        if "COUNT(*) AS open_order_count" in q:
            return _Rows({"open_order_count": 0 if self.open_order_created_ts is None else 1})

        if "FROM portfolio" in q:
            return _Rows({"cash_krw": 100000.0, "asset_qty": 0.0})

        if "SELECT COUNT(*) AS cnt FROM orders WHERE status='SUBMIT_UNKNOWN'" in q:
            return _Rows({"cnt": 0})

        if "SELECT COUNT(*) AS cnt FROM orders WHERE status='RECOVERY_REQUIRED'" in q:
            return _Rows({"cnt": 0})

        if "status='SUBMIT_UNKNOWN'" in q and "exchange_order_id" in q:
            return _Rows({"cnt": 0})

        if "client_order_id LIKE 'remote_%'" in q:
            return _Rows({"cnt": 0})

        if (
            "AS pending_submit_count" in q
            and "AS submit_unknown_count" in q
            and "AS recovery_required_count" in q
            and "AS stale_new_partial_count" in q
            and "FROM orders" in q
        ):
            if self.open_order_created_ts is not None:
                return _Rows(
                    {
                        "pending_submit_count": 0,
                        "submit_unknown_count": 0,
                        "recovery_required_count": 0,
                        "stale_new_partial_count": 0,
                    }
                )

            real_conn = ensure_db()
            try:
                row = real_conn.execute(query, params or ()).fetchone()
            finally:
                real_conn.close()

            if row is None:
                return _Rows(
                    {
                        "pending_submit_count": 0,
                        "submit_unknown_count": 0,
                        "recovery_required_count": 0,
                        "stale_new_partial_count": 0,
                    }
                )

            return _Rows(
                {
                    "pending_submit_count": row["pending_submit_count"] or 0,
                    "submit_unknown_count": row["submit_unknown_count"] or 0,
                    "recovery_required_count": row["recovery_required_count"] or 0,
                    "stale_new_partial_count": row["stale_new_partial_count"] or 0,
                }
            )

        if "SET status='RECOVERY_REQUIRED'" in q:
            if self.open_order_created_ts is None:
                self.marked_recovery_required = 0
                return _Rows(None, rowcount=0)
            self.marked_recovery_required = 1
            return _Rows(None, rowcount=1)

        if "SELECT client_order_id, exchange_order_id" in q and "WHERE status IN" in q:
            if self.open_order_created_ts is None:
                return _Rows(None)
            return _Rows({"client_order_id": "open_1", "exchange_order_id": "ex-open-1"})

        raise AssertionError(f"unexpected query: {query}")

    def commit(self):
        return None

    def close(self):
        return None


class _Rows:
    def __init__(self, row, rowcount: int = 0):
        self._row = row
        self.rowcount = rowcount

    def fetchone(self):
        return self._row


class _DummyBroker:
    def get_open_orders(self):
        return []

    def cancel_order(self, *args, **kwargs):
        return None


def _prepare_run_loop(monkeypatch, open_order_created_ts=None):
    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
    runtime_state.set_startup_gate_reason(None)

    monkeypatch.setenv("DB_PATH", settings.DB_PATH)

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

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

    loop_conn = _LoopConn(open_order_created_ts=open_order_created_ts)
    monkeypatch.setattr("bithumb_bot.engine.ensure_db", lambda: loop_conn)
    monkeypatch.setattr("bithumb_bot.engine.BithumbBroker", lambda: _DummyBroker())
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (False, "ok"),
    )
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_position_loss_breach",
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
    return loop_conn


def test_run_loop_live_broker_error_halts_instead_of_crash(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda broker, signal, ts, px: (_ for _ in ()).throw(BrokerRejectError("reject")),
    )

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.engine.notify", lambda msg: notifications.append(msg))

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason is not None
    assert "BrokerRejectError" in state.last_disable_reason
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "LIVE_EXECUTION_BROKER_ERROR"
    assert state.halt_state_unresolved is True
    halted = [n for n in notifications if "event=trading_halted" in n]
    assert halted
    assert any("halt_policy_stage=SAFE_HALT_REVIEW_ONLY" in n for n in halted)
    assert any("auto_liquidate_positions=0" in n for n in halted)
    assert any("operator_action_required=1" in n for n in halted)
    assert any("unresolved_order_count=" in n for n in halted)
    assert any("position_may_remain=" in n for n in halted)
    assert any("operator_next_action=" in n for n in halted)


def test_run_loop_reconcile_error_halts_instead_of_crash(monkeypatch):
    _prepare_run_loop(monkeypatch)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.engine.notify", lambda msg: notifications.append(msg))

    calls = {"n": 0}

    def _reconcile(_broker):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise RuntimeError("reconcile boom")

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", _reconcile, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda broker, signal, ts, px: None)

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason is not None
    assert "reconcile failed" in state.last_disable_reason
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "POST_TRADE_RECONCILE_FAILED"
    assert state.halt_state_unresolved is True
    halted = [n for n in notifications if "event=trading_halted" in n and "reason_code=POST_TRADE_RECONCILE_FAILED" in n]
    assert halted
    assert any("symbol=" in n for n in halted)
    assert any(
        "operator_next_action=run reconcile, validate order state, then run recovery-report before resume" in n
        for n in halted
    )
    assert any(
        "operator_hint_command=uv run python bot.py reconcile && uv run python bot.py recovery-report" in n
        for n in halted
    )


def test_run_loop_periodically_reconciles_when_open_order_exists(monkeypatch):
    _prepare_run_loop(monkeypatch, open_order_created_ts=10_500)

    calls = {"n": 0}

    def _reconcile(_broker):
        calls["n"] += 1

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", _reconcile, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop(5, 20)

    assert calls["n"] == 2


def test_run_loop_stale_open_order_halts_and_marks_recovery_required(monkeypatch):
    loop_conn = _prepare_run_loop(monkeypatch, open_order_created_ts=0)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 5)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason is not None
    assert "stale unresolved open order" in state.last_disable_reason
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "STALE_OPEN_ORDER"
    assert state.halt_state_unresolved is True
    assert loop_conn.marked_recovery_required == 1


def test_run_loop_unresolved_open_order_blocks_new_trading(monkeypatch):
    _prepare_run_loop(monkeypatch, open_order_created_ts=10_500)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    called = {"n": 0}

    def _live_execute(*_args, **_kwargs):
        called["n"] += 1

    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", _live_execute)

    run_loop(5, 20)

    assert called["n"] == 0
    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.halt_new_orders_blocked is False


def test_run_loop_startup_recovery_gate_halts_when_unresolved_state_exists(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="startup_block", created_ts=1)
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    called = {"n": 0}

    def _live_execute(*_args, **_kwargs):
        called["n"] += 1

    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", _live_execute)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.engine.notify", lambda msg: notifications.append(msg))

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason is not None
    assert state.last_disable_reason.startswith("startup safety gate:")
    assert state.halt_reason_code == "STARTUP_SAFETY_GATE"
    assert state.halt_new_orders_blocked is True
    assert state.halt_state_unresolved is True
    assert called["n"] == 0
    assert any(
        "event=startup_gate_blocked" in n and "reason_code=STARTUP_BLOCKED" in n and "timestamp=" in n
        for n in notifications
    )
    assert any("operator_action_required=1" in n for n in notifications if "event=startup_gate_blocked" in n)
    startup = [n for n in notifications if "event=startup_gate_blocked" in n]
    assert any("operator_next_action=operator must reconcile unresolved orders before startup" in n for n in startup)
    assert any("operator_compact_summary=halt_reason=STARTUP_SAFETY_GATE" in n for n in startup)
    assert any("open_order_count=" in n for n in startup)
    assert any("position_summary=" in n for n in startup)
    assert any("reason_code=STARTUP_SAFETY_GATE" in n for n in notifications)
    halted = [n for n in notifications if "event=trading_halted" in n and "alert_kind=halt" in n]
    assert halted
    assert any("halt_open_orders_present=1" in n for n in halted)
    assert any("operator_action_required=1" in n for n in halted)
    assert any("unresolved_order_count=" in n for n in halted)
    assert any("position_may_remain=" in n for n in halted)
    assert any("operator_next_action=" in n for n in halted)


def test_run_loop_startup_safety_gate_halts_when_unresolved_open_order_exists(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(status="NEW", client_order_id="startup_unresolved", created_ts=1)
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop(5, 20)

    state = runtime_state.snapshot()
    health = get_health_status()
    assert state.trading_enabled is False
    assert health["startup_gate_reason"] is not None
    assert "unresolved_open_orders=1" in str(health["startup_gate_reason"])


def test_run_loop_startup_recovery_gate_allows_clean_startup(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    called = {"n": 0}

    def _live_execute(*_args, **_kwargs):
        called["n"] += 1

    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", _live_execute)

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.retry_at_epoch_sec is None
    assert called["n"] == 1


def test_run_loop_kill_switch_halts_with_risk_open_reason_and_cancel_attempt(monkeypatch):
    _prepare_run_loop(monkeypatch)
    object.__setattr__(settings, "KILL_SWITCH", True)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    cancel_calls = {"n": 0}

    def _cancel(_broker, trigger: str):
        cancel_calls["n"] += 1
        assert trigger == "kill-switch"
        return True

    monkeypatch.setattr("bithumb_bot.engine._attempt_open_order_cancellation", _cancel)
    monkeypatch.setattr("bithumb_bot.engine._get_exposure_snapshot", lambda _now_ms: (False, True))

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.engine.notify", lambda msg: notifications.append(msg))

    run_loop(5, 20)

    assert cancel_calls["n"] == 1
    halted = [n for n in notifications if "event=trading_halted" in n and "reason_code=KILL_SWITCH" in n]
    assert halted
    assert any("operator_compact_summary=halt_reason=KILL_SWITCH" in n for n in halted)
    assert any("open_order_count=" in n for n in halted)
    assert any("position_summary=" in n for n in halted)
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_operator_action_required is True
    assert state.halt_state_unresolved is True
    assert state.last_disable_reason is not None
    assert "risk_open_exposure_remains" in state.last_disable_reason


def test_run_loop_daily_loss_breach_halts_persistently(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (True, "daily loss limit exceeded (60,000/50,000 KRW)"),
    )

    called = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: called.__setitem__("n", called["n"] + 1),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason is not None
    assert "daily loss limit exceeded" in state.last_disable_reason
    assert state.halt_reason_code == "DAILY_LOSS_LIMIT"
    assert state.halt_new_orders_blocked is True
    assert called["n"] == 0


def test_run_loop_daily_loss_breach_attempts_open_order_cancel(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (True, "daily loss limit exceeded (60,000/50,000 KRW)"),
    )

    cancel_calls = {"n": 0}

    def _cancel(_broker, trigger: str):
        cancel_calls["n"] += 1
        assert trigger == "daily-loss-halt"
        return True

    monkeypatch.setattr("bithumb_bot.engine._attempt_open_order_cancellation", _cancel)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop(5, 20)

    assert cancel_calls["n"] == 1


def test_run_loop_daily_loss_breach_has_no_auto_resume(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (True, "daily loss limit exceeded (60,000/50,000 KRW)"),
    )

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.engine.notify", lambda msg: notifications.append(msg))
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert all("attempting auto-resume" not in n for n in notifications)
    halted = [n for n in notifications if "event=trading_halted" in n and "reason_code=DAILY_LOSS_LIMIT" in n]
    assert halted
    assert any("symbol=" in n for n in halted)
    assert any(
        "operator_next_action=review risk breach details, verify exposure, then run recovery-report" in n
        for n in halted
    )


def test_run_loop_stale_open_order_emits_recovery_and_cancel_failure_alerts(monkeypatch):
    _prepare_run_loop(monkeypatch, open_order_created_ts=0)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 5)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.engine._attempt_open_order_cancellation", lambda *_args, **_kwargs: False)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.engine.notify", lambda msg: notifications.append(msg))

    run_loop(5, 20)

    marked = [n for n in notifications if "event=recovery_required_marked" in n and "reason_code=STALE_OPEN_ORDER" in n]
    assert marked
    assert any("symbol=" in n for n in marked)
    assert any("latest_client_order_id=" in n for n in marked)
    assert any(
        "operator_hint_command=uv run python bot.py reconcile && uv run python bot.py recovery-report" in n
        for n in marked
    )
    assert any("operator_compact_summary=halt_reason=STALE_OPEN_ORDER" in n for n in marked)
    assert any(
        "operator_recommended_commands=uv run python bot.py reconcile | uv run python bot.py recover-order --client-order-id <id>"
        in n
        for n in marked
    )
    assert any("event=trading_halted" in n and "reason_code=STALE_OPEN_ORDER" in n for n in notifications)


def test_attempt_open_order_cancellation_failure_emits_reason_code(monkeypatch):
    err = RuntimeError("boom")
    monkeypatch.setattr(
        "bithumb_bot.recovery.cancel_open_orders_with_broker",
        lambda _broker: (_ for _ in ()).throw(err),
        raising=False,
    )

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.engine.notify", lambda msg: notifications.append(msg))

    from bithumb_bot.engine import _attempt_open_order_cancellation

    ok = _attempt_open_order_cancellation(object(), trigger="kill-switch")

    assert ok is False
    assert any("event=cancel_open_orders_failed" in n for n in notifications)
    assert any(
        "reason_code=CANCEL_FAILURE" in n and "cancel_detail_code=CANCEL_OPEN_ORDERS_ERROR" in n
        for n in notifications
    )


class _DummyClient:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, path, params=None):
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _response(status_code: int) -> httpx.Response:
    req = httpx.Request("GET", "https://api.bithumb.com/public/test")
    return httpx.Response(status_code=status_code, request=req, json={"ok": True})


def test_get_with_retry_retries_on_429(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("bithumb_bot.marketdata.time.sleep", lambda sec: sleeps.append(sec))
    monkeypatch.setattr("bithumb_bot.marketdata.random.uniform", lambda a, b: 0.0)

    client = _DummyClient([_response(429), _response(503), _response(200)])
    result = _get_with_retry(client, "/public/test")

    assert result.status_code == 200
    assert len(sleeps) == 2


def test_health_status_contains_runtime_flags():
    runtime_state.set_error_count(3)
    runtime_state.set_last_candle_age_sec(12.5)
    runtime_state.disable_trading_until(999.0)

    health = get_health_status()

    assert health["error_count"] == 3
    assert health["last_candle_age_sec"] == 12.5
    assert health["trading_enabled"] is False
    assert health["retry_at_epoch_sec"] == 999.0
    assert health["last_disable_reason"] is None
    assert health["halt_new_orders_blocked"] is False
    assert health["halt_reason_code"] is None
    assert health["halt_state_unresolved"] is False
    assert int(health["unresolved_open_order_count"]) >= 0
    assert int(health["recovery_required_count"]) >= 0
    if int(health["unresolved_open_order_count"]) == 0:
        assert health["oldest_unresolved_order_age_sec"] is None
    assert health["last_reconcile_status"] in (None, "ok", "error")
    if health["last_reconcile_status"] != "error":
        assert health["last_reconcile_error"] is None

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
    runtime_state.set_startup_gate_reason(None)


def test_run_loop_position_loss_breach_triggers_halt(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "POSITION_LOSS_LIMIT"
    assert state.last_disable_reason is not None
    assert "position loss threshold breached" in state.last_disable_reason


def test_run_loop_position_loss_breach_blocks_new_orders(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )

    called = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: called.__setitem__("n", called["n"] + 1),
    )

    run_loop(5, 20)

    assert called["n"] == 0


def test_run_loop_position_loss_breach_sends_halt_notification(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.engine.notify", lambda msg: notifications.append(msg))

    run_loop(5, 20)

    halted = [n for n in notifications if "event=trading_halted" in n and "reason_code=POSITION_LOSS_LIMIT" in n]
    assert halted
    assert any("symbol=" in n for n in halted)
    assert any(
        "operator_next_action=review risk breach details, verify exposure, then run recovery-report" in n
        for n in halted
    )