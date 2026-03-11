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
                halt_policy_stage,
                halt_policy_block_new_orders,
                halt_policy_attempt_cancel_open_orders,
                halt_policy_auto_liquidate_positions,
                halt_position_present,
                halt_open_orders_present,
                halt_operator_action_required,
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
                startup_gate_reason,
                resume_gate_blocked,
                resume_gate_reason
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
    assert str(row["halt_policy_stage"]) == "SAFE_HALT_REVIEW_ONLY"
    assert int(row["halt_policy_block_new_orders"]) == 1
    assert int(row["halt_policy_attempt_cancel_open_orders"]) == 1
    assert int(row["halt_policy_auto_liquidate_positions"]) == 0
    assert int(row["halt_position_present"]) == 0
    assert int(row["halt_open_orders_present"]) == 0
    assert int(row["halt_operator_action_required"]) == 0
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
    assert int(row["resume_gate_blocked"]) == 0
    assert row["resume_gate_reason"] is None






def test_enter_halt_sets_consistent_halted_state_for_all_reason_codes(tmp_path):
    _set_tmp_db(tmp_path)

    reason_codes = [
        "MANUAL_PAUSE",
        "KILL_SWITCH",
        "DAILY_LOSS_LIMIT",
        "RECOVERY_REQUIRED_PRESENT",
        "STALE_OPEN_ORDER",
    ]

    for code in reason_codes:
        runtime_state.enable_trading()
        runtime_state.enter_halt(
            reason_code=code,
            reason=f"halt triggered by {code}",
            unresolved=(code != "MANUAL_PAUSE"),
        )
        state = runtime_state.snapshot()
        assert state.trading_enabled is False
        assert state.retry_at_epoch_sec == float("inf")
        assert state.halt_new_orders_blocked is True
        assert state.halt_reason_code == code
        assert state.last_disable_reason == f"halt triggered by {code}"
        assert state.halt_policy_stage == "SAFE_HALT_REVIEW_ONLY"
        assert state.halt_policy_block_new_orders is True
        assert state.halt_policy_attempt_cancel_open_orders is True
        assert state.halt_policy_auto_liquidate_positions is False



def test_enter_halt_tracks_position_and_open_order_visibility(tmp_path):
    _set_tmp_db(tmp_path)

    conn = ensure_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked) VALUES (1, 1000000.0, 0.25, 900000.0, 100000.0, 0.2, 0.05)"
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price,
                qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, NULL, 'NEW', 'SELL', NULL, 0.1, 0.0, ?, ?, NULL)
            """,
            ("halt_visibility_open_order", 10, 10),
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.enter_halt(
        reason_code="MANUAL_HALT",
        reason="manual halt for review",
        unresolved=False,
    )

    state = runtime_state.snapshot()
    assert state.halt_policy_auto_liquidate_positions is False
    assert state.halt_position_present is True
    assert state.halt_open_orders_present is True
    assert state.halt_operator_action_required is True


def test_halt_state_persists_across_restart_boundary(tmp_path):
    db_path = _set_tmp_db(tmp_path)

    runtime_state.enable_trading()
    runtime_state.enter_halt(
        reason_code="PERIODIC_RECONCILE_FAILED",
        reason="periodic reconcile failed",
        unresolved=True,
    )

    env = dict(os.environ)
    env["DB_PATH"] = str(db_path)
    env["PYTHONPATH"] = str(Path.cwd() / "src")
    probe = (
        "from bithumb_bot import runtime_state; "
        "s = runtime_state.snapshot(); "
        "print(int(s.halt_new_orders_blocked), s.halt_reason_code, int(s.halt_state_unresolved))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    assert proc.stdout.strip() == "1 PERIODIC_RECONCILE_FAILED 1"



def test_kill_switch_risk_open_reason_persists_in_health_state(tmp_path):
    _set_tmp_db(tmp_path)
    reason = "KILL_SWITCH=ON; emergency cancellation attempted; risk_open_exposure_remains(open_orders=0,position=1)"
    runtime_state.disable_trading_until(
        float("inf"),
        reason=reason,
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.last_disable_reason == reason

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT halt_reason_code, last_disable_reason, halt_new_orders_blocked FROM bot_health WHERE id=1"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["halt_reason_code"] == "KILL_SWITCH"
    assert row["last_disable_reason"] == reason
    assert int(row["halt_new_orders_blocked"]) == 1


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


def test_startup_gate_exposes_stale_unresolved_open_order_blocker(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = 1_730_000_000_000
    old_max_open_order_age_sec = settings.MAX_OPEN_ORDER_AGE_SEC
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 60)
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price,
                qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, NULL, 'NEW', 'BUY', NULL, 0.01, 0.0, ?, ?, NULL)
            """,
            ("stale_open_order", now_ms - 120_000, now_ms - 120_000),
        )
        conn.commit()
    finally:
        conn.close()

    try:
        reason = evaluate_startup_safety_gate()
    finally:
        object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", old_max_open_order_age_sec)

    assert reason is not None
    assert "stale_new_partial_orders=1" in reason

    health = runtime_state.snapshot()
    assert health.startup_gate_reason is not None
    assert "stale_new_partial_orders=1" in str(health.startup_gate_reason)

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


def test_resume_gate_fields_roundtrip_in_health_persistence(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.set_resume_gate(
        blocked=True,
        reason="LAST_RECONCILE_FAILED:last reconcile failed: reason_code=RECONCILE_FAILED",
    )
    state = runtime_state.snapshot()

    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "LAST_RECONCILE_FAILED" in state.resume_gate_reason

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT resume_gate_blocked, resume_gate_reason FROM bot_health WHERE id=1"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert int(row["resume_gate_blocked"]) == 1
    assert "LAST_RECONCILE_FAILED" in str(row["resume_gate_reason"])

    runtime_state.set_resume_gate(blocked=False, reason=None)
    cleared = runtime_state.snapshot()
    assert cleared.resume_gate_blocked is False
    assert cleared.resume_gate_reason is None


def test_emergency_flatten_failure_persists_across_restart(tmp_path):
    db_path = _set_tmp_db(tmp_path)

    runtime_state.record_flatten_position_result(
        status="failed",
        summary={"status": "failed", "error": "submit boom", "trigger": "daily-loss-halt"},
        now_epoch_sec=123.0,
    )

    env = dict(os.environ)
    env["DB_PATH"] = str(db_path)
    env["PYTHONPATH"] = str(Path.cwd() / "src")
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from bithumb_bot import runtime_state; "
                "s = runtime_state.snapshot(); "
                "print(int(s.emergency_flatten_blocked)); "
                "print(s.emergency_flatten_block_reason or '-')"
            ),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    assert lines[0] == "1"
    assert "emergency flatten unresolved" in lines[1]
    assert "submit boom" in lines[1]
