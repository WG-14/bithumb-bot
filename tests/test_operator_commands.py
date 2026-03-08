from __future__ import annotations

import time

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.app import _load_recovery_report, cmd_pause, cmd_reconcile, cmd_resume
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


def test_pause_disables_trading_via_persistent_runtime_state(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    cmd_pause()

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason == "manual operator pause"


def test_resume_refuses_when_unresolved_state_exists_without_force(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    assert exc.value.code == 1
    assert runtime_state.snapshot().trading_enabled is False


def test_resume_force_enables_even_when_unresolved_state_exists(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    cmd_resume(force=True)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.retry_at_epoch_sec is None


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
