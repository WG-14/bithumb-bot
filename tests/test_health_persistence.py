from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from bithumb_bot import runtime_state
from bithumb_bot.broker.base import BrokerBalance
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.engine import evaluate_startup_safety_gate
from bithumb_bot.recovery import reconcile_with_broker


def _set_tmp_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "health.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    return db_path


def test_health_state_written_by_one_component_read_by_another(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    runtime_state.set_error_count(4)
    runtime_state.set_last_candle_age_sec(8.5)
    runtime_state.disable_trading_until(456.0, reason="manual stop")

    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT
                trading_enabled,
                halt_new_orders_blocked,
                halt_reason_code,
                halt_state_unresolved,
                error_count,
                last_candle_age_sec,
                retry_at_epoch_sec,
                last_disable_reason,
                unresolved_open_order_count,
                oldest_unresolved_order_age_sec,
                recovery_required_count,
                last_reconcile_epoch_sec,
                last_reconcile_status,
                last_reconcile_error,
                last_reconcile_reason_code,
                last_reconcile_metadata,
                last_cancel_open_orders_epoch_sec,
                last_cancel_open_orders_trigger,
                last_cancel_open_orders_status,
                last_cancel_open_orders_summary,
                startup_gate_reason
            FROM bot_health
            WHERE id = 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert int(row["trading_enabled"]) == 0
    assert int(row["halt_new_orders_blocked"]) == 0
    assert row["halt_reason_code"] is None
    assert int(row["halt_state_unresolved"]) == 0
    assert int(row["error_count"]) == 4
    assert float(row["last_candle_age_sec"]) == 8.5
    assert float(row["retry_at_epoch_sec"]) == 456.0
    assert str(row["last_disable_reason"]) == "manual stop"
    assert int(row["unresolved_open_order_count"]) == 0
    assert row["oldest_unresolved_order_age_sec"] is None
    assert int(row["recovery_required_count"]) == 0
    assert row["last_reconcile_epoch_sec"] is None
    assert row["last_reconcile_status"] is None
    assert row["last_reconcile_error"] is None
    assert row["last_reconcile_reason_code"] is None
    assert row["last_reconcile_metadata"] is None
    assert row["last_cancel_open_orders_epoch_sec"] is None
    assert row["last_cancel_open_orders_trigger"] is None
    assert row["last_cancel_open_orders_status"] is None
    assert row["last_cancel_open_orders_summary"] is None
    assert row["startup_gate_reason"] is None




def test_startup_gate_reason_is_persisted_to_health_state(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = 1_730_000_000_000
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price,
                qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, NULL, 'NEW', 'BUY', NULL, 0.01, 0.0, ?, ?, NULL)
            """,
            ("startup_unresolved", now_ms - 30_000, now_ms - 30_000),
        )
        conn.commit()
    finally:
        conn.close()

    reason = evaluate_startup_safety_gate()
    assert reason is not None

    state = runtime_state.snapshot()
    assert state.startup_gate_reason == reason

    conn = ensure_db()
    try:
        row = conn.execute("SELECT startup_gate_reason FROM bot_health WHERE id=1").fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["startup_gate_reason"] == reason

def test_open_order_health_fields_are_persisted(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = 1_730_000_000_000
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price,
                qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, NULL, 'RECOVERY_REQUIRED', 'BUY', NULL, 0.01, 0.0, ?, ?, NULL)
            """,
            ("needs_recovery", now_ms - 30_000, now_ms - 30_000),
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.refresh_open_order_health(now_epoch_sec=now_ms / 1000)
    state = runtime_state.snapshot()

    assert state.unresolved_open_order_count == 1
    assert state.recovery_required_count == 1
    assert state.oldest_unresolved_order_age_sec is not None
    assert state.oldest_unresolved_order_age_sec >= 30.0


class _ReconcileOkBroker:
    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None):
        raise NotImplementedError

    def cancel_order(self, *, client_order_id: str, exchange_order_id: str | None = None):
        raise NotImplementedError

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None):
        raise NotImplementedError

    def get_open_orders(self):
        return []

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None):
        return []

    def get_recent_orders(self, *, limit: int = 100):
        return []

    def get_recent_fills(self, *, limit: int = 100):
        return []

    def get_balance(self):
        return BrokerBalance(cash_available=1_000_000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)


class _ReconcileFailBroker(_ReconcileOkBroker):
    def get_open_orders(self):
        raise RuntimeError("boom")


def test_reconcile_result_persisted_on_success_and_failure(tmp_path):
    _set_tmp_db(tmp_path)

    reconcile_with_broker(_ReconcileOkBroker())
    state = runtime_state.snapshot()
    assert state.last_reconcile_status == "ok"
    assert state.last_reconcile_error is None
    assert state.last_reconcile_reason_code == "RECONCILE_OK"
    assert state.last_reconcile_metadata is not None
    assert state.last_reconcile_epoch_sec is not None

    try:
        reconcile_with_broker(_ReconcileFailBroker())
    except RuntimeError:
        pass

    failed = runtime_state.snapshot()
    assert failed.last_reconcile_status == "error"
    assert failed.last_reconcile_error is not None
    assert "RuntimeError" in failed.last_reconcile_error
    assert failed.last_reconcile_reason_code == "RECONCILE_FAILED"
    assert failed.last_reconcile_metadata is not None


def test_healthcheck_reports_disabled_state_from_persistent_store(tmp_path):
    db_path = _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(1.0)
    runtime_state.disable_trading_until(999.0, reason="recovery required")

    env = dict(os.environ)
    env["DB_PATH"] = str(db_path)
    env["PYTHONPATH"] = str(Path.cwd() / "src")
    env["NOTIFIER_ENABLED"] = "false"

    proc = subprocess.run(
        [sys.executable, "scripts/healthcheck.py"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 1
    assert "[HEALTHCHECK] FAIL" in proc.stdout
    assert "trading disabled" in proc.stdout
    assert "reason=recovery required" in proc.stdout


def test_healthcheck_reports_reconcile_failure(tmp_path):
    db_path = _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(success=False, error="reconcile blew up")

    env = dict(os.environ)
    env["DB_PATH"] = str(db_path)
    env["PYTHONPATH"] = str(Path.cwd() / "src")
    env["NOTIFIER_ENABLED"] = "false"

    proc = subprocess.run(
        [sys.executable, "scripts/healthcheck.py"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 1
    assert "last reconcile failed" in proc.stdout


def test_healthcheck_healthy_default_path(tmp_path):
    db_path = _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
    runtime_state.record_reconcile_result(success=True)

    env = dict(os.environ)
    env["DB_PATH"] = str(db_path)
    env["PYTHONPATH"] = str(Path.cwd() / "src")
    env["NOTIFIER_ENABLED"] = "false"

    proc = subprocess.run(
        [sys.executable, "scripts/healthcheck.py"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert "[HEALTHCHECK] OK" in proc.stdout


def test_cancel_open_orders_result_is_persisted(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.record_cancel_open_orders_result(
        trigger="test",
        status="ok",
        summary={"remote_open_count": 0, "failed_count": 0},
        now_epoch_sec=123.0,
    )

    state = runtime_state.snapshot()
    assert state.last_cancel_open_orders_epoch_sec == 123.0
    assert state.last_cancel_open_orders_trigger == "test"
    assert state.last_cancel_open_orders_status == "ok"
    assert state.last_cancel_open_orders_summary is not None
    assert '"failed_count": 0' in state.last_cancel_open_orders_summary


def test_startup_gate_sets_structured_reason_code(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.set_startup_gate_reason("unresolved_open_orders=1")
    state = runtime_state.snapshot()

    assert state.last_reconcile_reason_code == "STARTUP_GATE_BLOCKED"
    assert state.last_reconcile_metadata is not None
    assert "startup_gate_reason" in state.last_reconcile_metadata
