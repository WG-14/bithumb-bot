from __future__ import annotations

import os

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.engine import _select_latest_closed_candle, run_loop
from bithumb_bot.strategy.sma import compute_signal


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path):
    old_db_path = settings.DB_PATH
    old_mode = settings.MODE
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "candle_stability.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "paper")
    ensure_db().close()
    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_observation(
        status="waiting_first_sync",
        age_sec=None,
        sync_epoch_sec=None,
        candle_ts_ms=None,
        detail=None,
    )
    runtime_state.set_startup_gate_reason(None)

    yield

    object.__setattr__(settings, "DB_PATH", old_db_path)
    object.__setattr__(settings, "MODE", old_mode)
    if old_env_db_path is None:
        os.environ.pop("DB_PATH", None)
    else:
        os.environ["DB_PATH"] = old_env_db_path
    runtime_state.enable_trading()


def _insert_candle(ts_ms: int, close: float) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts_ms, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
        )
        conn.commit()
    finally:
        conn.close()


def test_last_processed_candle_ts_persists_to_bot_health() -> None:
    runtime_state.mark_processed_candle(candle_ts_ms=1_700_000_000_000, now_epoch_sec=1_700_000_100.0)

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT last_processed_candle_ts_ms, last_candle_status FROM bot_health WHERE id=1"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert int(row["last_processed_candle_ts_ms"]) == 1_700_000_000_000
    assert str(row["last_candle_status"]) == "processed_closed"
    assert runtime_state.snapshot().last_processed_candle_ts_ms == 1_700_000_000_000


def test_compute_signal_through_ts_excludes_newer_candles() -> None:
    base_ts = 1_700_000_000_000
    for idx, close in enumerate([10.0, 11.0, 12.0, 13.0, 14.0, 50.0]):
        _insert_candle(base_ts + idx * 60_000, close)

    conn = ensure_db()
    try:
        latest = compute_signal(conn, 2, 3)
        bounded = compute_signal(conn, 2, 3, through_ts_ms=base_ts + 4 * 60_000)
    finally:
        conn.close()

    assert latest is not None
    assert bounded is not None
    assert latest["ts"] == base_ts + 5 * 60_000
    assert bounded["ts"] == base_ts + 4 * 60_000
    assert latest["last_close"] == 50.0
    assert bounded["last_close"] == 14.0


def test_select_latest_closed_candle_skips_open_tail() -> None:
    _insert_candle(0, 100.0)
    _insert_candle(60_000, 101.0)

    conn = ensure_db()
    try:
        closed_row, incomplete_ts = _select_latest_closed_candle(
            conn,
            pair=settings.PAIR,
            interval=settings.INTERVAL,
            interval_sec=60,
            now_ms=65_000,
        )
    finally:
        conn.close()

    assert closed_row is not None
    assert int(closed_row["ts"]) == 0
    assert incomplete_ts == 60_000


def test_run_loop_logs_duplicate_and_incomplete_candle_and_skips_reprocessing(monkeypatch, capsys):
    closed_ts = 0
    open_ts = 60_000
    _insert_candle(closed_ts, 100.0)
    _insert_candle(open_ts, 101.0)
    runtime_state.mark_processed_candle(candle_ts_ms=closed_ts, now_epoch_sec=1.0)

    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 60)
    monkeypatch.setattr(
        "bithumb_bot.engine.compute_signal",
        lambda *_args, **_kwargs: pytest.fail("duplicate candle should not reach compute_signal"),
    )

    times = iter([64.0, 65.0, 65.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(times, 65.0))
    sleep_calls = {"n": 0}

    def _sleep(_sec: float) -> None:
        sleep_calls["n"] += 1
        if sleep_calls["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)

    run_loop(5, 20)

    output = capsys.readouterr().out
    assert "[SKIP] incomplete/open candle" in output
    assert f"candle_ts={open_ts}" in output
    assert "[SKIP] duplicate candle" in output
    assert f"last_processed_candle_ts={closed_ts}" in output
    assert runtime_state.snapshot().last_processed_candle_ts_ms == closed_ts


def test_run_loop_processes_latest_closed_candle_and_persists_it(monkeypatch, capsys):
    closed_ts = 0
    open_ts = 60_000
    _insert_candle(closed_ts, 100.0)
    _insert_candle(open_ts, 101.0)

    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 60)
    monkeypatch.setattr(
        "bithumb_bot.engine.compute_signal",
        lambda _conn, _short, _long, *, through_ts_ms=None: {
            "ts": through_ts_ms,
            "last_close": 100.0,
            "curr_s": 1.0,
            "curr_l": 1.0,
            "signal": "HOLD",
        },
    )

    times = iter([64.0, 65.0, 65.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(times, 65.0))
    sleep_calls = {"n": 0}

    def _sleep(_sec: float) -> None:
        sleep_calls["n"] += 1
        if sleep_calls["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)
    monkeypatch.setattr("bithumb_bot.engine.paper_execute", lambda *_args, **_kwargs: pytest.fail("HOLD should not execute"))

    run_loop(5, 20)

    output = capsys.readouterr().out
    assert "[RUN] processed closed candle" in output
    assert f"candle_ts={closed_ts}" in output
    assert runtime_state.snapshot().last_processed_candle_ts_ms == closed_ts
