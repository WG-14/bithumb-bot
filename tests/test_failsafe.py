from __future__ import annotations

import httpx

from bithumb_bot import runtime_state
from bithumb_bot.engine import get_health_status
from bithumb_bot.marketdata import _get_with_retry


from bithumb_bot.config import settings
from bithumb_bot.broker.base import BrokerRejectError, BrokerTemporaryError
from bithumb_bot.engine import run_loop


class _LoopConn:
    def __init__(self, *, has_recovery_required: bool = False):
        self._has_recovery_required = has_recovery_required

    def execute(self, query, params=None):
        q = " ".join(str(query).split())
        if "FROM candles" in q:
            return _Rows({"ts": int(10_000_000_000_000)})
        if "status='RECOVERY_REQUIRED'" in q:
            return _Rows((1,) if self._has_recovery_required else None)
        raise AssertionError(f"unexpected query: {query}")

    def close(self):
        return None


class _Rows:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


def _prepare_run_loop(monkeypatch, *, has_recovery_required: bool = False):
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "INTERVAL", "1m")

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)

    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 1)
    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr("bithumb_bot.engine.compute_signal", lambda conn, s, l: {
        "ts": 1000,
        "last_close": 100.0,
        "curr_s": 1.0,
        "curr_l": 0.5,
        "signal": "BUY",
    })
    monkeypatch.setattr("bithumb_bot.engine.ensure_db", lambda: _LoopConn(has_recovery_required=has_recovery_required))
    monkeypatch.setattr("bithumb_bot.engine.BithumbBroker", lambda: object())

    ticks = iter([10.0, 11.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(ticks, 11.0))

    sleeps = {"n": 0}

    def _sleep(_sec: float):
        sleeps["n"] += 1
        if sleeps["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)


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
    assert any("trading halted" in n for n in notifications)


def test_run_loop_reconcile_error_halts_instead_of_crash(monkeypatch):
    _prepare_run_loop(monkeypatch)

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


def test_run_loop_skips_new_order_when_recovery_required_exists(monkeypatch):
    _prepare_run_loop(monkeypatch, has_recovery_required=True)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    called = {"n": 0}

    def _live_execute(*_args, **_kwargs):
        called["n"] += 1
        raise BrokerTemporaryError("should not be called")

    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", _live_execute)

    run_loop(5, 20)

    assert called["n"] == 0
    assert runtime_state.snapshot().trading_enabled is True


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

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
