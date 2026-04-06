from __future__ import annotations

import json
import os
import time

import pytest

import bithumb_bot.app as app_module
from bithumb_bot import runtime_state
from bithumb_bot.app import (
    _load_recovery_report,
    cmd_broker_diagnose,
    cmd_health,
    cmd_pause,
    cmd_flatten_position,
    cmd_reconcile,
    cmd_recover_order,
    cmd_recovery_report,
    cmd_restart_checklist,
    cmd_resume,
)
from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, get_portfolio_breakdown
from bithumb_bot.engine import evaluate_resume_eligibility
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.oms import set_exchange_order_id, set_status
from bithumb_bot.public_api_orderbook import BestQuote
from bithumb_bot.reason_codes import DUST_RESIDUAL_UNSELLABLE
from bithumb_bot.recovery import reconcile_with_broker


@pytest.fixture(autouse=True)
def _default_operator_mode(monkeypatch: pytest.MonkeyPatch):
    original_mode = settings.MODE
    monkeypatch.setenv("MODE", "paper")
    object.__setattr__(settings, "MODE", "paper")
    try:
        yield
    finally:
        object.__setattr__(settings, "MODE", original_mode)


def _set_tmp_db(tmp_path, monkeypatch: pytest.MonkeyPatch | None = None):
    db_path = str((tmp_path / "operator.sqlite").resolve())
    if monkeypatch is not None:
        monkeypatch.setenv("DB_PATH", db_path)
    else:
        os.environ["DB_PATH"] = db_path
    roots = {
        "ENV_ROOT": (tmp_path / "env").resolve(),
        "RUN_ROOT": (tmp_path / "run").resolve(),
        "DATA_ROOT": (tmp_path / "data").resolve(),
        "LOG_ROOT": (tmp_path / "logs").resolve(),
        "BACKUP_ROOT": (tmp_path / "backup").resolve(),
    }
    for key, value in roots.items():
        if monkeypatch is not None:
            monkeypatch.setenv(key, str(value))
        else:
            os.environ[key] = str(value)
    run_lock_path = str((roots["RUN_ROOT"] / "live" / "bithumb-bot.lock").resolve())
    if monkeypatch is not None:
        monkeypatch.setenv("RUN_LOCK_PATH", run_lock_path)
    else:
        os.environ["RUN_LOCK_PATH"] = run_lock_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(app_module.settings, "DB_PATH", db_path)


def _insert_order(
    *,
    status: str,
    client_order_id: str,
    created_ts: int,
    side: str = "BUY",
    qty_req: float = 0.01,
    price: float | None = None,
) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price,
                qty_req, qty_filled, created_ts, updated_ts, last_error
            )
            VALUES (?, NULL, ?, ?, ?, ?, 0.0, ?, ?, NULL)
            """,
            (client_order_id, status, side, price, qty_req, created_ts, created_ts),
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


class _ResumeFilledReplayBroker:
    def __init__(self, *, balance: BrokerBalance) -> None:
        self._balance = balance

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-resume-filled", "BUY", "FILLED", 100.0, 1.0, 1.0, 1, 1)

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
        return []

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return []

    def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
        return [
            BrokerFill(
                client_order_id="",
                fill_id="ex-resume-filled:aggregate:201",
                fill_ts=201,
                price=100.0,
                qty=0.4,
                fee=0.0,
                exchange_order_id="ex-resume-filled",
            )
        ]

    def get_balance(self) -> BrokerBalance:
        return self._balance


def _insert_order_event(
    *,
    client_order_id: str,
    event_type: str,
    event_ts: int,
    submit_attempt_id: str | None = None,
    intent_ts: int | None = None,
    submit_ts: int | None = None,
    timeout_flag: int | None = None,
    exchange_order_id_obtained: int | None = None,
    order_status: str | None = None,
) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO order_events(
                client_order_id, event_type, event_ts, order_status, submit_attempt_id,
                intent_ts, submit_ts, timeout_flag, exchange_order_id_obtained, side, qty
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'BUY', 0.01)
            """,
            (
                client_order_id,
                event_type,
                event_ts,
                order_status,
                submit_attempt_id,
                intent_ts,
                submit_ts,
                timeout_flag,
                exchange_order_id_obtained,
            ),
        )
        conn.commit()
    finally:
        conn.close()


class _RecoverSuccessBroker:
    def get_order(
        self, *, client_order_id: str, exchange_order_id: str | None = None
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id,
            "BUY",
            "FILLED",
            None,
            0.01,
            0.01,
            1,
            1,
        )

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> list[BrokerFill]:
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

    def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_fills(self, *, limit: int = 100):
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=0.0,
            cash_locked=0.0,
            asset_available=0.01,
            asset_locked=0.0,
        )


class _RecoverAmbiguousBroker(_RecoverSuccessBroker):
    def get_order(
        self, *, client_order_id: str, exchange_order_id: str | None = None
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id,
            "BUY",
            "NEW",
            None,
            0.01,
            0.0,
            1,
            1,
        )


class _RecoverUnresolvedHighConfidenceTerminalBroker(_RecoverSuccessBroker):
    def __init__(self, *, recent_orders: list[BrokerOrder] | None = None, remote_status: str = "FILLED") -> None:
        self._recent_orders = list(recent_orders or [])
        self._remote_status = remote_status

    def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
        return list(self._recent_orders)

    def get_order(
        self, *, client_order_id: str, exchange_order_id: str | None = None
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id or "ex-unresolved-1",
            "BUY",
            self._remote_status,
            100.0,
            0.01,
            0.01 if self._remote_status == "FILLED" else 0.0,
            1,
            1,
        )

class _SubmitUnknownRecoveredByRecentFillBroker:
    def get_order(
        self, *, client_order_id: str, exchange_order_id: str | None = None
    ) -> BrokerOrder:
        return BrokerOrder(
            client_order_id,
            exchange_order_id or "ex-submit-unknown-1",
            "BUY",
            "FILLED",
            100.0,
            0.01,
            0.01,
            1,
            1,
        )

    def get_fills(
        self,
        *,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> list[BrokerFill]:
        return []

    def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_fills(self, *, limit: int = 100):
        return [
            BrokerFill(
                client_order_id="ambiguous_resume_case",
                fill_id="ambiguous_submit_fill_1",
                fill_ts=1000,
                price=100.0,
                qty=0.01,
                fee=0.0,
                exchange_order_id="ex-submit-unknown-1",
            )
        ]

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(
            cash_available=0.0,
            cash_locked=0.0,
            asset_available=0.01,
            asset_locked=0.0,
        )


class _RecoveryReportCandidateBroker:
    def __init__(self, recent_orders: list[BrokerOrder]):
        self._recent_orders = recent_orders

    def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return self._recent_orders[:limit]

    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        raise NotImplementedError

    def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None):
        return []

    def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
        return []

    def get_recent_fills(self, *, limit: int = 100):
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(0.0, 0.0, 0.0, 0.0)


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


def test_manual_pause_then_resume_success_path(tmp_path):
    _set_tmp_db(tmp_path)

    cmd_pause()
    cmd_resume(force=False)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.halt_new_orders_blocked is False
    assert state.halt_reason_code is None
    assert state.resume_gate_blocked is False
    assert state.resume_gate_reason is None


def test_resume_live_recent_fill_replay_does_not_fail_with_filled_to_partial(tmp_path):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    conn = ensure_db()
    try:
        record_order_if_missing(
            conn,
            client_order_id="resume_filled_replay",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="PENDING_SUBMIT",
        )
        set_exchange_order_id("resume_filled_replay", "ex-resume-filled", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="resume_filled_replay",
            side="BUY",
            fill_id="resume-fill-existing",
            fill_ts=120,
            price=100.0,
            qty=0.4,
            fee=0.0,
        )
        set_status("resume_filled_replay", "FILLED", conn=conn)

        record_order_if_missing(
            conn,
            client_order_id="resume_flatten",
            side="SELL",
            qty_req=0.4,
            price=110.0,
            ts_ms=130,
            status="PENDING_SUBMIT",
        )
        set_exchange_order_id("resume_flatten", "ex-resume-flat", conn=conn)
        apply_fill_and_trade(
            conn,
            client_order_id="resume_flatten",
            side="SELL",
            fill_id="resume-flat-fill",
            fill_ts=140,
            price=110.0,
            qty=0.4,
            fee=0.0,
        )
        set_status("resume_flatten", "FILLED", conn=conn)

        cash_available, cash_locked, asset_available, asset_locked = get_portfolio_breakdown(conn)
        conn.commit()
    finally:
        conn.close()

    broker = _ResumeFilledReplayBroker(
        balance=BrokerBalance(
            cash_available=cash_available,
            cash_locked=cash_locked,
            asset_available=asset_available,
            asset_locked=asset_locked,
        )
    )

    try:
        cmd_pause()
        cmd_resume(
            force=False,
            broker_factory=lambda: broker,
            reconcile_fn=reconcile_with_broker,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.last_reconcile_status in {"ok", "success"}

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='resume_filled_replay'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == pytest.approx(0.4)


def test_resume_live_accepts_injected_reconcile_dependencies(tmp_path):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    broker = object()
    calls: list[object] = []

    def _broker_factory():
        calls.append("factory")
        return broker

    def _reconcile(candidate):
        calls.append(("reconcile", candidate))

    try:
        cmd_pause()
        cmd_resume(
            force=False,
            broker_factory=_broker_factory,
            reconcile_fn=_reconcile,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert calls == ["factory", ("reconcile", broker)]
    state = runtime_state.snapshot()
    assert state.trading_enabled is True


def test_resume_refuses_when_unresolved_state_exists_without_force(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is False
    assert state.halt_reason_code is None
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "STARTUP_SAFETY_GATE_BLOCKED" in state.resume_gate_reason


def test_resume_refused_when_ambiguous_submit_only_weakly_matches_recent_fill(
    monkeypatch, tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="SUBMIT_UNKNOWN",
        client_order_id="ambiguous_resume_case",
        created_ts=now_ms,
    )

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit):
        cmd_resume(force=False)

    state_blocked = runtime_state.snapshot()
    assert state_blocked.resume_gate_blocked is True
    assert "STARTUP_SAFETY_GATE_BLOCKED" in str(state_blocked.resume_gate_reason)
    assert "submit_unknown_orders=1" in str(state_blocked.resume_gate_reason)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _SubmitUnknownRecoveredByRecentFillBroker(),
    )
    try:
        cmd_reconcile()

        with pytest.raises(SystemExit) as exc:
            cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert exc.value.code == 1
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "recovery_required_orders=1" in out
    assert "LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS" in out

    state_after = runtime_state.snapshot()
    assert state_after.trading_enabled is False
    assert state_after.resume_gate_blocked is True
    assert state_after.resume_gate_reason is not None
    assert "STARTUP_SAFETY_GATE_BLOCKED" in str(state_after.resume_gate_reason)
    assert "recovery_required_orders=1" in str(state_after.resume_gate_reason)
    assert "LAST_RECONCILE_DID_NOT_CLEAR_BLOCKERS" in str(
        state_after.resume_gate_reason
    )

    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT status, last_error
            FROM orders
            WHERE client_order_id='ambiguous_resume_case'
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert "manual recovery required" in str(row["last_error"])


def test_resume_runs_preflight_reconcile_and_refuses_when_recovery_required(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )

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
    assert state.startup_gate_reason is not None
    assert "recovery_required_orders=1" in str(state.startup_gate_reason)


def test_resume_refuses_when_reconcile_has_balance_split_mismatch(
    monkeypatch, tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 2,
            "balance_split_mismatch_summary": (
                "cash_available(local=1000000,broker=900000,delta=-100000)"
            ),
        },
    )

    monkeypatch.setattr("bithumb_bot.app.reconcile_with_broker", lambda _broker: None)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())

    try:
        with pytest.raises(SystemExit) as exc:
            cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "code=BALANCE_SPLIT_MISMATCH" in out
    assert "balance split mismatch detected after reconcile" in out
    assert exc.value.code == 1




def test_resume_refuses_when_kill_switch_halt_has_open_position(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked) VALUES (1, 1000000.0, 0.5, 900000.0, 100000.0, 0.5, 0.0)"
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="KILL_SWITCH=ON; emergency cancellation attempted; risk_open_exposure_remains(open_orders=0,position=1)",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=HALT_RISK_OPEN_POSITION" in out
    assert "open exposure" in out
    assert "position_present=1" in out
    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "HALT_RISK_OPEN_POSITION" in state.resume_gate_reason


def test_resume_blocks_risk_halt_when_only_matched_dust_policy_review_remains(tmp_path, monkeypatch, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked) VALUES (1, 1000000.0, 0.00009629, 1000000.0, 0.0, 0.00009629, 0.0)"
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="KILL_SWITCH=ON; flatten submitted; dust residual only",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_classification": "matched_harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_operator_review_required",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 classification=matched_harmless_dust matched_harmless=1 broker_local_match=1 min_qty=0.00010000 allow_resume=0 effective_flat=1 policy_reason=matched_harmless_dust_operator_review_required",
        },
    )
    monkeypatch.setattr("bithumb_bot.app.reconcile_with_broker", lambda _broker: None)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    state = runtime_state.snapshot()
    assert exc.value.code == 1
    assert "code=MATCHED_DUST_POLICY_REVIEW_REQUIRED" in out
    assert state.trading_enabled is False
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "MATCHED_DUST_POLICY_REVIEW_REQUIRED" in state.resume_gate_reason


def test_resume_refuses_when_dust_residual_policy_requires_review(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked) VALUES (1, 1000000.0, 0.00009629, 1000000.0, 0.0, 0.00009629, 0.0)"
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="KILL_SWITCH=ON; flatten submitted but attribution inconsistent",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00020000 delta=-0.00010371 min_qty=0.00010000 min_notional_krw=5000.0",
        },
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=HALT_RISK_OPEN_POSITION" in out
    assert "dust_policy=dangerous_dust_operator_review_required" in out
    assert exc.value.code == 1




def test_resume_allows_risk_halt_when_exposure_is_flat(tmp_path):
    _set_tmp_db(tmp_path)

    runtime_state.disable_trading_until(
        float("inf"),
        reason="KILL_SWITCH=ON",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=False,
    )

    cmd_resume(force=False)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.resume_gate_blocked is False
    assert state.resume_gate_reason is None


def test_resume_live_clears_post_trade_reconcile_halt_after_flatten(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path, monkeypatch)
    old_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.0, 1000000.0, 0.0, 0.0, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="post trade reconcile failed (RuntimeError): duplicate fill replay",
        reason_code="POST_TRADE_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    class _SafeFlatBroker:
        def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
            return BrokerOrder(client_order_id, exchange_order_id or "ex-safe-flat", "BUY", "FILLED", 100.0, 1.0, 1.0, 1, 1)

        def get_fills(self, *, client_order_id: str | None = None, exchange_order_id: str | None = None) -> list[BrokerFill]:
            return []

        def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
            return []

        def get_recent_orders(
        self,
        *,
        limit: int = 100,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ) -> list[BrokerOrder]:
            return []

        def get_recent_fills(self, *, limit: int = 100) -> list[BrokerFill]:
            return []

        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=1000000.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _SafeFlatBroker())

    try:
        cmd_resume(force=False)

        state = runtime_state.snapshot()
        assert state.trading_enabled is True
        assert state.halt_new_orders_blocked is False
        assert state.halt_state_unresolved is False
        assert state.halt_reason_code is None
    finally:
        object.__setattr__(settings, "MODE", old_mode)


def test_resume_non_risk_halt_with_open_exposure_message_is_unchanged(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked) VALUES (1, 1000000.0, 0.25, 900000.0, 100000.0, 0.25, 0.0)"
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="manual operator pause",
        reason_code="MANUAL_PAUSE",
        halt_new_orders_blocked=True,
        unresolved=False,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=HALT_RISK_OPEN_POSITION" in out
    assert "risk halt resume rejected until exposure is flattened/resolved first" not in out
    assert exc.value.code == 1


def _set_stale_initial_reconcile_halt_with_clean_reconcile() -> None:
    runtime_state.disable_trading_until(
        float("inf"),
        reason=(
            "initial reconcile failed (BrokerRejectError): "
            "bithumb private /info/orders rejected with http status=400"
        ),
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 0,
            "balance_split_mismatch_summary": "none",
            "remote_open_order_found": 0,
        },
    )
    runtime_state.refresh_open_order_health()


def test_resume_refuses_when_halt_state_unresolved_even_without_open_orders(
    tmp_path, capsys
):
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

    out = capsys.readouterr().out
    assert "code=HALT_STATE_UNRESOLVED" in out
    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_state_unresolved is True


def test_resume_refuses_when_last_reconcile_failed(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    runtime_state.record_reconcile_result(
        success=False,
        error="boom",
        reason_code="PERIODIC_RECONCILE_FAILED",
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "code=LAST_RECONCILE_FAILED" in out
    assert "PERIODIC_RECONCILE_FAILED" in out
    assert exc.value.code == 1

    state = runtime_state.snapshot()
    assert state.resume_gate_blocked is True
    assert state.resume_gate_reason is not None
    assert "LAST_RECONCILE_FAILED" in state.resume_gate_reason


def test_resume_force_refuses_when_last_reconcile_failed(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")
    runtime_state.record_reconcile_result(
        success=False,
        error="boom",
        reason_code="PERIODIC_RECONCILE_FAILED",
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=LAST_RECONCILE_FAILED" in out
    assert "overridable=0" in out
    assert "reason_code=PERIODIC_RECONCILE_FAILED" in out
    assert exc.value.code == 1


def test_resume_force_refuses_when_startup_safety_gate_blocked(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "overridable=0" in out
    assert "recovery_required_orders=1" in out
    assert exc.value.code == 1


def test_resume_force_rejects_startup_blocker_before_clearing_manual_pause(
    tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="NEW",
        client_order_id="still_open_on_startup",
        created_ts=now_ms,
    )
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "unresolved_open_orders=1" in out
    assert "manual operator pause" not in out
    assert exc.value.code == 1
    state = runtime_state.snapshot()
    assert state.trading_enabled is False


def test_resume_force_refuses_when_halt_state_unresolved(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed (RuntimeError): boom",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=HALT_STATE_UNRESOLVED" in out
    assert "overridable=0" in out
    assert exc.value.code == 1


def test_resume_force_rejects_initial_reconcile_failure_with_operator_readable_reason(
    tmp_path, capsys
):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed (RuntimeError): broker timeout",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=True)

    out = capsys.readouterr().out
    assert "[RESUME] refused: force override denied" in out
    assert "code=HALT_STATE_UNRESOLVED" in out
    assert "INITIAL_RECONCILE_FAILED" in out
    assert "broker timeout" in out
    assert exc.value.code == 1


def test_resume_auto_clears_stale_initial_reconcile_halt_after_clean_reconcile(tmp_path):
    _set_tmp_db(tmp_path)
    _set_stale_initial_reconcile_halt_with_clean_reconcile()

    eligible, blockers = evaluate_resume_eligibility()

    assert eligible is True
    assert blockers == []
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is False
    assert state.halt_state_unresolved is False
    assert state.halt_reason_code is None
    assert state.last_disable_reason is None


def test_resume_force_enables_for_safe_manual_pause(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="manual operator pause",
        reason_code="MANUAL_PAUSE",
        unresolved=False,
    )
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
            "cancel_accepted_count": 1,
            "canceled_count": 1,
            "cancel_confirm_pending_count": 0,
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


def test_broker_diagnose_success_output(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('diag_live_1','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()
    original_mode = settings.MODE
    original_max_order = settings.MAX_ORDER_KRW
    original_max_daily_loss = settings.MAX_DAILY_LOSS_KRW
    original_max_daily_count = settings.MAX_DAILY_ORDER_COUNT
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_max_market_slippage_bps = settings.MAX_MARKET_SLIPPAGE_BPS
    original_live_price_protection_max_slippage_bps = settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS
    original_live_min_order_qty = settings.LIVE_MIN_ORDER_QTY
    original_live_order_qty_step = settings.LIVE_ORDER_QTY_STEP
    original_min_order_notional_krw = settings.MIN_ORDER_NOTIONAL_KRW
    original_live_order_max_qty_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MAX_ORDER_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1200000.0, 10000.0, 0.12, 0.01)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return [
                BrokerOrder("a", "ex1", "BUY", "NEW", 100.0, 0.1, 0.0, 1, 1),
                BrokerOrder("b", "ex2", "SELL", "PARTIAL", 110.0, 0.1, 0.05, 1, 1),
            ]

        def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return [
                BrokerOrder("", "ex3", "BUY", "FILLED", 120.0, 0.2, 0.2, 1, 2),
                BrokerOrder("", "ex4", "SELL", "CANCELED", 121.0, 0.2, 0.0, 1, 2),
            ][:limit]

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "min_notional_krw": 5000.0,
                        "max_qty_decimals": 8,
                        "bid_min_total_krw": 0.0,
                        "ask_min_total_krw": 0.0,
                        "bid_price_unit": 0.0,
                        "ask_price_unit": 0.0,
                    },
                )(),
                "source": {
                    "min_qty": "local_fallback",
                    "qty_step": "local_fallback",
                    "min_notional_krw": "local_fallback",
                    "max_qty_decimals": "local_fallback",
                    "bid_min_total_krw": "chance_doc",
                    "ask_min_total_krw": "chance_doc",
                    "bid_price_unit": "chance_doc",
                    "ask_price_unit": "chance_doc",
                },
            },
        )(),
    )

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(app_module.settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", original_max_market_slippage_bps)
        object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", original_live_price_protection_max_slippage_bps)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", original_live_min_order_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_live_order_qty_step)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", original_min_order_notional_krw)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", original_live_order_max_qty_decimals)

    out = capsys.readouterr().out
    assert "[BROKER-READINESS]" in out
    assert "overall=PASS" in out
    assert "[PASS] config/env loaded" in out
    assert "[PASS] broker authentication" in out
    assert "[PASS] balance query" in out
    assert "[PASS] live execution mode: MODE=live LIVE_DRY_RUN=True armed=False" in out
    assert "[PASS] order submit routing: price=None => /v2/orders market/price order, price set => /v2/orders limit order" in out
    assert "[PASS] order lookup path: get_order reads /v1/order directly; open/recent snapshots use /v1/orders" in out
    assert "[PASS] open order query: known_unresolved_count=2" in out
    assert "[PASS] symbol/order rule query" in out
    assert "[PASS] accounts snapshot(/v1/accounts) validation diagnostic: reason=ok" in out
    assert "execution_mode=- quote_currency=- base_currency=-" in out
    assert "base_currency_missing_policy=- preflight_outcome=-" in out
    assert "bid_min_total_krw=0.0 (source=chance_doc)" in out
    assert "ask_price_unit=0.0 (source=chance_doc)" in out
    assert "min_qty=0.0001 (source=local_fallback)" in out
    assert "[PASS] DB writable" in out


def test_broker_diagnose_partial_failure(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('diag_live_2','ex1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()
    original_mode = settings.MODE
    original_max_order = settings.MAX_ORDER_KRW
    original_max_daily_loss = settings.MAX_DAILY_LOSS_KRW
    original_max_daily_count = settings.MAX_DAILY_ORDER_COUNT
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_max_market_slippage_bps = settings.MAX_MARKET_SLIPPAGE_BPS
    original_live_price_protection_max_slippage_bps = settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS
    original_live_min_order_qty = settings.LIVE_MIN_ORDER_QTY
    original_live_order_qty_step = settings.LIVE_ORDER_QTY_STEP
    original_min_order_notional_krw = settings.MIN_ORDER_NOTIONAL_KRW
    original_live_order_max_qty_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MAX_ORDER_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagPartialBroker:
        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            raise RuntimeError("open orders timeout")

        def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return []

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagPartialBroker())
    monkeypatch.setattr("bithumb_bot.app.get_effective_order_rules", lambda _pair: (_ for _ in ()).throw(RuntimeError("rules api down")))

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(app_module.settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", original_max_market_slippage_bps)
        object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", original_live_price_protection_max_slippage_bps)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", original_live_min_order_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_live_order_qty_step)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", original_min_order_notional_krw)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", original_live_order_max_qty_decimals)

    out = capsys.readouterr().out
    assert "overall=WARN" in out
    assert "[PASS] live execution mode: MODE=live LIVE_DRY_RUN=True armed=False" in out
    assert "[WARN] symbol/order rule query" in out
    assert "[WARN] open order query" in out


def test_broker_diagnose_accounts_policy_context_is_operator_readable(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_live_real_order_armed = settings.LIVE_REAL_ORDER_ARMED
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(self, **_kwargs):
            return []

        def get_accounts_validation_diagnostics(self):
            return {
                "reason": "ok",
                "failure_category": "none",
                "row_count": 1,
                "currencies": ["KRW"],
                "missing_required_currencies": [],
                "duplicate_currencies": [],
                "execution_mode": "live_dry_run_unarmed",
                "quote_currency": "KRW",
                "base_currency": "BTC",
                "base_currency_missing_policy": "allow_zero_position_start_in_dry_run",
                "preflight_outcome": "pass_no_position_allowed",
                "last_success_reason": "ok",
                "last_failure_reason": "required currency missing",
            }

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("skip rule detail")),
    )
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", original_live_real_order_armed)

    out = capsys.readouterr().out
    assert "execution_mode=live_dry_run_unarmed quote_currency=KRW base_currency=BTC" in out
    assert "base_currency_missing_policy=allow_zero_position_start_in_dry_run" in out
    assert "preflight_outcome=pass_no_position_allowed" in out


def test_broker_diagnose_accounts_policy_context_shows_real_order_block(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_live_real_order_armed = settings.LIVE_REAL_ORDER_ARMED
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(self, **_kwargs):
            return []

        def get_accounts_validation_diagnostics(self):
            return {
                "reason": "required currency missing",
                "failure_category": "schema_mismatch",
                "row_count": 1,
                "currencies": ["KRW"],
                "missing_required_currencies": ["BTC"],
                "duplicate_currencies": [],
                "execution_mode": "live_real_order_path",
                "quote_currency": "KRW",
                "base_currency": "BTC",
                "base_currency_missing_policy": "block_when_base_currency_row_missing",
                "preflight_outcome": "fail_real_order_blocked",
                "last_success_reason": "ok",
                "last_failure_reason": "required currency missing",
            }

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("skip rule detail")),
    )
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", original_live_real_order_armed)

    out = capsys.readouterr().out
    assert "execution_mode=live_real_order_path quote_currency=KRW base_currency=BTC" in out
    assert "base_currency_missing_policy=block_when_base_currency_row_missing" in out
    assert "preflight_outcome=fail_real_order_blocked" in out


def test_broker_diagnose_config_failure_is_critical(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_max_order = settings.MAX_ORDER_KRW
    original_max_daily_loss = settings.MAX_DAILY_LOSS_KRW
    original_max_daily_count = settings.MAX_DAILY_ORDER_COUNT
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_api_key = settings.BITHUMB_API_KEY
    original_api_secret = settings.BITHUMB_API_SECRET
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(app_module.settings, "MODE", "live")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(app_module.settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)
    object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", 0)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(app_module.settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(app_module.settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")
    object.__setattr__(app_module.settings, "BITHUMB_API_SECRET", "")

    class _DiagBroker:
        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return []

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _DiagBroker())
    monkeypatch.setattr(
        app_module,
        "validate_live_mode_preflight",
        lambda _cfg: (_ for _ in ()).throw(
            app_module.LiveModeValidationError(
                "live mode preflight validation failed: MAX_ORDER_KRW must be > 0"
            )
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "min_notional_krw": 5000.0,
                        "max_qty_decimals": 8,
                        "bid_min_total_krw": 0.0,
                        "ask_min_total_krw": 0.0,
                        "bid_price_unit": 0.0,
                        "ask_price_unit": 0.0,
                    },
                )(),
                "source": {"min_qty": "local_fallback"},
            },
        )(),
    )

    try:
        with pytest.raises(SystemExit):
            app_module.cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(app_module.settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(app_module.settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "BITHUMB_API_KEY", original_api_key)
        object.__setattr__(app_module.settings, "BITHUMB_API_KEY", original_api_key)
        object.__setattr__(settings, "BITHUMB_API_SECRET", original_api_secret)
        object.__setattr__(app_module.settings, "BITHUMB_API_SECRET", original_api_secret)

    out = capsys.readouterr().out
    assert "overall=FAIL" in out
    assert "[FAIL] config/env loaded" in out


def test_broker_diagnose_never_calls_place_order(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    original_max_order = settings.MAX_ORDER_KRW
    original_max_daily_loss = settings.MAX_DAILY_LOSS_KRW
    original_max_daily_count = settings.MAX_DAILY_ORDER_COUNT
    original_live_dry_run = settings.LIVE_DRY_RUN
    original_max_market_slippage_bps = settings.MAX_MARKET_SLIPPAGE_BPS
    original_live_price_protection_max_slippage_bps = settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS
    original_live_min_order_qty = settings.LIVE_MIN_ORDER_QTY
    original_live_order_qty_step = settings.LIVE_ORDER_QTY_STEP
    original_min_order_notional_krw = settings.MIN_ORDER_NOTIONAL_KRW
    original_live_order_max_qty_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MAX_ORDER_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 10000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    
    monkeypatch.setenv("NOTIFIER_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)
    place_calls = {"n": 0}

    class _NoTradeBroker:
        def place_order(self, **_kwargs):
            place_calls["n"] += 1
            raise AssertionError("place_order must not be called")

        def get_balance(self):
            return BrokerBalance(1000000.0, 0.0, 0.0, 0.0)

        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            return []

        def get_recent_orders(
            self,
            *,
            limit: int = 100,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            raise NotImplementedError

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _NoTradeBroker())
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "min_notional_krw": 5000.0,
                        "max_qty_decimals": 8,
                        "bid_min_total_krw": 0.0,
                        "ask_min_total_krw": 0.0,
                        "bid_price_unit": 0.0,
                        "ask_price_unit": 0.0,
                    },
                )(),
                "source": {"min_qty": "local_fallback"},
            },
        )(),
    )

    try:
        cmd_broker_diagnose()
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(app_module.settings, "MODE", original_mode)
        object.__setattr__(settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(app_module.settings, "MAX_ORDER_KRW", original_max_order)
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(app_module.settings, "MAX_DAILY_LOSS_KRW", original_max_daily_loss)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(app_module.settings, "MAX_DAILY_ORDER_COUNT", original_max_daily_count)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)
        object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", original_max_market_slippage_bps)
        object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", original_live_price_protection_max_slippage_bps)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", original_live_min_order_qty)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", original_live_order_qty_step)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", original_min_order_notional_krw)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", original_live_order_max_qty_decimals)

    assert place_calls["n"] == 0


def test_recovery_report_summarizes_unresolved_and_recovery_required(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="open_1", created_ts=now_ms - 30_000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="open_2",
        created_ts=now_ms - 20_000,
    )

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={"remote_open_order_found": 2},
        now_epoch_sec=time.time() - 3,
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="periodic reconcile failed",
        reason_code="PERIODIC_RECONCILE_FAILED",
        unresolved=True,
    )

    report = _load_recovery_report()

    assert int(report["unresolved_count"]) == 2
    assert int(report["recovery_required_count"]) == 1
    assert report["oldest_unresolved_age_sec"] is not None
    assert float(report["oldest_unresolved_age_sec"]) >= 20.0
    assert "status=ok" in str(report["last_reconcile_summary"])
    assert "reason_code=RECONCILE_OK" in str(report["last_reconcile_summary"])
    assert "code=PERIODIC_RECONCILE_FAILED" in str(report["recent_halt_reason"])
    assert int(report["unprocessed_remote_open_orders"]) == 2
    oldest_orders = report["oldest_orders"]
    assert isinstance(oldest_orders, list)
    assert len(oldest_orders) == 2
    assert oldest_orders[0]["client_order_id"] == "open_1"
    assert oldest_orders[0]["status"] == "NEW"
    assert oldest_orders[1]["client_order_id"] == "open_2"


def test_recovery_report_shows_defaults_when_empty(tmp_path):
    _set_tmp_db(tmp_path)

    report = _load_recovery_report()

    assert int(report["unresolved_count"]) == 0
    assert int(report["recovery_required_count"]) == 0
    assert report["oldest_unresolved_age_sec"] is None
    assert report["oldest_orders"] == []
    assert report["last_reconcile_summary"] == "none"
    assert report["recent_halt_reason"] == "none"
    assert int(report["unprocessed_remote_open_orders"]) == 0


def test_recovery_report_candidate_no_match(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="rr_none", created_ts=now_ms - 20_000)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoveryReportCandidateBroker(
            [
                BrokerOrder("remote_x", "ex_x", "SELL", "NEW", None, 5.0, 0.0, now_ms - 1_000_000, now_ms - 900_000),
            ]
        ),
    )
    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    entries = [e for e in report["recovery_candidates"] if e["client_order_id"] == "rr_none"]
    assert len(entries) == 1
    assert entries[0]["attempted_locally"] is False
    assert entries[0]["request_likely_sent"] == "unknown"
    assert entries[0]["candidate_outcome"] == "no_candidate"
    assert entries[0]["candidates"] == []


def test_recovery_report_candidate_single_plausible(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="rr_one", created_ts=now_ms - 30_000, price=100.0)
    _insert_order_event(
        client_order_id="rr_one",
        event_type="submit_attempt_recorded",
        event_ts=now_ms - 29_000,
        submit_attempt_id="attempt_one",
        submit_ts=now_ms - 29_000,
        timeout_flag=1,
        exchange_order_id_obtained=0,
        order_status="SUBMIT_UNKNOWN",
    )

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoveryReportCandidateBroker(
            [
                BrokerOrder("remote_1", "ex_match", "BUY", "PARTIAL", 100.05, 0.01, 0.003, now_ms - 32_000, now_ms - 10_000),
                BrokerOrder("remote_2", "ex_weak", "BUY", "NEW", None, 0.02, 0.0, now_ms - 3_600_000, now_ms - 3_500_000),
            ]
        ),
    )
    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    entries = [e for e in report["recovery_candidates"] if e["client_order_id"] == "rr_one"]
    assert len(entries) == 1
    assert entries[0]["candidate_outcome"] == "single_plausible_candidate"
    assert entries[0]["likely_broker_match"] is True
    assert entries[0]["likely_broker_exchange_order_id"] == "ex_match"
    assert entries[0]["attempted_locally"] is True
    assert entries[0]["request_likely_sent"] == "yes"
    assert int(entries[0]["plausible_candidate_count"]) == 1
    assert entries[0]["candidates"][0]["exchange_order_id"] == "ex_match"
    assert float(entries[0]["candidates"][0]["time_gap_sec"]) < 90.0
    assert float(entries[0]["candidates"][0]["qty_gap_pct"]) < 1.0
    assert float(entries[0]["candidates"][0]["price_gap_pct"]) < 0.2


def test_recovery_report_candidate_multiple_plausible(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="rr_many", created_ts=now_ms - 40_000, price=100.0)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoveryReportCandidateBroker(
            [
                BrokerOrder("rr_many", "ex_m1", "BUY", "NEW", 100.05, 0.0101, 0.0, now_ms - 42_000, now_ms - 20_000),
                BrokerOrder("rr_many", "ex_m2", "BUY", "PARTIAL", 99.95, 0.0099, 0.005, now_ms - 41_000, now_ms - 19_000),
            ]
        ),
    )
    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    entries = [e for e in report["recovery_candidates"] if e["client_order_id"] == "rr_many"]
    assert len(entries) == 1
    assert entries[0]["candidate_outcome"] == "multiple_plausible_candidates"
    assert entries[0]["likely_broker_match"] is False
    assert int(entries[0]["plausible_candidate_count"]) == 2
    assert all("same client_order_id" in c["match_reason"] for c in entries[0]["candidates"][:2])


def test_recovery_report_candidate_weak_only(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="rr_weak", created_ts=now_ms - 30_000, side="BUY", qty_req=0.01)

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoveryReportCandidateBroker(
            [
                BrokerOrder("remote_weak", "ex_weak", "BUY", "NEW", None, 0.01025, 0.0, now_ms - 580_000, now_ms - 20_000),
            ]
        ),
    )
    try:
        report = _load_recovery_report()
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    entries = [e for e in report["recovery_candidates"] if e["client_order_id"] == "rr_weak"]
    assert len(entries) == 1
    assert entries[0]["candidate_outcome"] == "weak_candidates_only"
    assert entries[0]["likely_broker_match"] is False
    assert int(entries[0]["plausible_candidate_count"]) == 0
    assert entries[0]["candidates"][0]["exchange_order_id"] == "ex_weak"



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
        last_error=(
            "timeout while polling exchange status endpoint due to transient "
            "error and retry budget exceeded"
        ),
    )

    cmd_recovery_report()
    out = capsys.readouterr().out

    assert "[RECOVERY-REPORT]" in out
    assert "[P0] blocker_summary_view" in out
    assert "blocker=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "recommended_next_action=uv run python bot.py recover-order --client-order-id <id>" in out
    assert "[P1] order_recovery_status" in out
    assert "unresolved_count=6" in out
    assert "recovery_required_count=3" in out
    assert "[P2] resume_eligibility" in out
    assert "active_blocker_summary=" in out
    assert "risk_level=high" in out
    assert "[P3] balance_mismatch" in out
    assert "summary=none" in out
    assert "[P4] last_reconcile_summary" in out
    assert "[P5] recent_halt_reason" in out
    assert "[P6] operator_next_action" in out
    assert "action=manual_recovery_required" in out
    assert (
        "recommended_next_action="
        "Recover RECOVERY_REQUIRED orders before attempting resume."
    ) in out
    assert (
        "resume_blocked_reason=resume blocked by RECOVERY_REQUIRED orders" in out
    )
    assert (
        "command=uv run python bot.py recover-order --client-order-id <id>" in out
    )
    assert "[P7] unprocessed_remote_open_orders" in out
    assert "resume_allowed=0" in out
    assert "can_resume=false" in out
    assert "blockers=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "force_resume_allowed=0" in out
    assert "blocker_summary=total=" in out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "overridable=0" in out
    assert "oldest_unresolved_orders(top 5):" in out
    assert "recovery_required_orders(top 3):" in out
    assert "client_order_id=open_0" in out
    assert "client_order_id=open_4" in out
    assert "reason=timeout while polling exchange status endpoint due to transi..." in out
    assert (
        "last_error=timeout while polling exchange status endpoint due to transi..."
        in out
    )


def test_recovery_report_includes_recent_order_lifecycle_block(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="o_submit_unknown", created_ts=now_ms - 20_000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="o_recovery", created_ts=now_ms - 10_000)

    conn = ensure_db()
    try:
        conn.execute(
            "UPDATE orders SET submit_attempt_id=?, exchange_order_id=NULL WHERE client_order_id=?",
            ("attempt_a", "o_submit_unknown"),
        )
        conn.execute(
            "UPDATE orders SET submit_attempt_id=?, exchange_order_id=? WHERE client_order_id=?",
            ("attempt_b", "ex-123", "o_recovery"),
        )
        conn.commit()
    finally:
        conn.close()

    _insert_order_event(
        client_order_id="o_submit_unknown",
        event_type="intent_created",
        event_ts=now_ms - 20_000,
        submit_attempt_id="attempt_a",
        intent_ts=now_ms - 20_000,
    )
    _insert_order_event(
        client_order_id="o_submit_unknown",
        event_type="submit_attempt_preflight",
        event_ts=now_ms - 19_500,
        submit_attempt_id="attempt_a",
    )
    _insert_order_event(
        client_order_id="o_submit_unknown",
        event_type="submit_attempt_recorded",
        event_ts=now_ms - 19_000,
        submit_attempt_id="attempt_a",
        submit_ts=now_ms - 19_000,
        timeout_flag=1,
        exchange_order_id_obtained=0,
        order_status="SUBMIT_UNKNOWN",
    )

    cmd_recovery_report()
    out = capsys.readouterr().out

    assert "[P8] recent_order_lifecycle(top 2):" in out
    assert "client_order_id=o_submit_unknown" in out
    assert "submit_ts=" in out
    assert "correlation=attempt=attempt_a meta=1 timeout=1" in out
    assert "mapping=submit_no_mapping" in out
    assert "state=SUBMIT_UNKNOWN unresolved=1" in out
    assert "client_order_id=o_recovery" in out
    assert "mapping=mapped" in out
    assert "state=RECOVERY_REQUIRED unresolved=1" in out


def test_health_prints_risk_snapshot_for_operator_visibility(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_effective_order_rules",
        lambda _pair: type(
            "_ResolvedRules",
            (),
            {
                "rules": type(
                    "_Rules",
                    (),
                    {
                        "min_qty": 0.0001,
                        "qty_step": 0.0001,
                        "min_notional_krw": 5000.0,
                        "max_qty_decimals": 8,
                        "bid_min_total_krw": 5500.0,
                        "ask_min_total_krw": 5000.0,
                        "bid_price_unit": 10.0,
                        "ask_price_unit": 1.0,
                    },
                )(),
                "source": {
                    "min_qty": "local_fallback",
                    "qty_step": "local_fallback",
                    "min_notional_krw": "local_fallback",
                    "max_qty_decimals": "local_fallback",
                    "bid_min_total_krw": "chance_doc",
                    "ask_min_total_krw": "chance_doc",
                    "bid_price_unit": "chance_doc",
                    "ask_price_unit": "chance_doc",
                },
            },
        )(),
    )
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": 2.0,
            "error_count": 0,
            "trading_enabled": False,
            "retry_at_epoch_sec": 1200.0,
            "unresolved_open_order_count": 4,
            "oldest_unresolved_order_age_sec": 95.0,
            "recovery_required_count": 2,
            "last_reconcile_epoch_sec": 1000.0,
            "last_reconcile_status": "error",
            "last_reconcile_error": "timeout",
            "last_reconcile_reason_code": "RECONCILE_TIMEOUT",
            "last_reconcile_metadata": None,
            "last_disable_reason": "periodic reconcile failed",
            "halt_new_orders_blocked": True,
            "halt_reason_code": "PERIODIC_RECONCILE_FAILED",
            "halt_state_unresolved": True,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (False, []))

    cmd_health()
    out = capsys.readouterr().out

    assert "[RISK-SNAPSHOT]" in out
    assert "[HALT-RECOVERY-STATUS]" in out
    assert "state=halted trading_enabled=0 halt_new_orders_blocked=1" in out
    assert "can_resume=false" in out
    assert "blockers=none" in out
    assert "resume_safety=unsafe" in out
    assert "unresolved_open_order_count=4 recovery_required_count=2 submit_unknown_count=0" in out
    assert "current_halt_reason=code=PERIODIC_RECONCILE_FAILED reason=periodic reconcile failed" in out
    assert "reconcile_latest=epoch_sec=1000.0 status=error reason_code=RECONCILE_TIMEOUT" in out
    assert (
        "unresolved_attribution_count=0 recent_recovery_derived_trade_count=0 "
        "ambiguous_linkage_after_recent_reconcile=False"
    ) in out
    assert "[CRITICAL-OPERATOR-SUMMARY]" in out
    assert "halt_reason=PERIODIC_RECONCILE_FAILED unresolved_order_count=4" in out
    assert "open_order_count=0" in out
    assert "position=flat" in out
    assert "next_commands=uv run python bot.py recover-order --client-order-id <id> | uv run python bot.py recovery-report" in out
    assert "[ORDER-RULE-SNAPSHOT]" in out
    assert "BUY(min_total_krw=5500.0 (source=chance_doc), price_unit=10.0 (source=chance_doc))" in out


def test_health_summary_shows_paused_state(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": None,
            "last_candle_status": "waiting_first_sync",
            "last_candle_sync_epoch_sec": None,
            "last_candle_ts_ms": None,
            "last_candle_status_detail": "startup warming up",
            "error_count": 0,
            "trading_enabled": False,
            "retry_at_epoch_sec": None,
            "unresolved_open_order_count": 0,
            "oldest_unresolved_order_age_sec": None,
            "recovery_required_count": 0,
            "last_reconcile_epoch_sec": None,
            "last_reconcile_status": None,
            "last_reconcile_error": None,
            "last_reconcile_reason_code": None,
            "last_reconcile_metadata": None,
            "last_disable_reason": "manual operator pause",
            "halt_new_orders_blocked": False,
            "halt_reason_code": None,
            "halt_state_unresolved": False,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (True, []))

    cmd_health()
    out = capsys.readouterr().out

    assert "[HALT-RECOVERY-STATUS]" in out
    assert "state=paused trading_enabled=0 halt_new_orders_blocked=0" in out
    assert "reason=code=- reason=manual operator pause" in out
    assert "can_resume=true" in out
    assert "blockers=none" in out
    assert "resume_safety=safe" in out
    assert "last_candle_age_sec=None (status=waiting_first_sync" in out
    assert "last_candle_status_detail=startup warming up" in out


def test_health_includes_balance_source_diagnostics(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": 1.0,
            "last_candle_status": "ok",
            "last_candle_sync_epoch_sec": 1.0,
            "last_candle_ts_ms": 1000,
            "last_candle_status_detail": "ok",
            "error_count": 0,
            "trading_enabled": True,
            "retry_at_epoch_sec": None,
            "unresolved_open_order_count": 0,
            "oldest_unresolved_order_age_sec": None,
            "recovery_required_count": 0,
            "last_reconcile_epoch_sec": None,
            "last_reconcile_status": None,
            "last_reconcile_error": None,
            "last_reconcile_reason_code": None,
            "last_reconcile_metadata": None,
            "last_disable_reason": None,
            "halt_new_orders_blocked": False,
            "halt_reason_code": None,
            "halt_state_unresolved": False,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (True, []))
    monkeypatch.setattr(
        "bithumb_bot.app.DEFAULT_BITHUMB_BROKER_CLASS",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "myasset_ws_private_stream",
                    "reason": "myAsset stream stale",
                    "failure_category": "stale_source",
                    "stale": True,
                    "last_success_ts_ms": 1710000000000,
                    "last_observed_ts_ms": 1710000005000,
                    "last_asset_ts_ms": 1710000000000,
                }
            },
        )(),
    )

    cmd_health()
    out = capsys.readouterr().out

    assert "balance_source=myasset_ws_private_stream" in out
    assert "diag_category=stale_source stale=True" in out
    assert "diag_execution_mode=- quote_currency=- base_currency=- base_missing_policy=- preflight_outcome=-" in out
    assert "balance_source_last_asset_ts_ms=1710000000000" in out


def test_health_prints_accounts_preflight_outcome_context(monkeypatch, capsys, tmp_path):
    _set_tmp_db(tmp_path)
    monkeypatch.setattr("bithumb_bot.app.refresh_open_order_health", lambda: None)
    monkeypatch.setattr(
        "bithumb_bot.app.get_health_status",
        lambda: {
            "last_candle_age_sec": 1.0,
            "last_candle_status": "ok",
            "last_candle_sync_epoch_sec": 1.0,
            "last_candle_ts_ms": 1000,
            "last_candle_status_detail": "ok",
            "error_count": 0,
            "trading_enabled": False,
            "retry_at_epoch_sec": None,
            "unresolved_open_order_count": 0,
            "oldest_unresolved_order_age_sec": None,
            "recovery_required_count": 0,
            "last_reconcile_epoch_sec": None,
            "last_reconcile_status": None,
            "last_reconcile_error": None,
            "last_reconcile_reason_code": None,
            "last_reconcile_metadata": None,
            "last_disable_reason": "accounts preflight blocked",
            "halt_new_orders_blocked": True,
            "halt_reason_code": "PRECHECK_FAILED",
            "halt_state_unresolved": False,
            "last_cancel_open_orders_epoch_sec": None,
            "last_cancel_open_orders_trigger": None,
            "last_cancel_open_orders_status": None,
            "last_cancel_open_orders_summary": None,
            "startup_gate_reason": None,
        },
    )
    monkeypatch.setattr("bithumb_bot.app.evaluate_resume_eligibility", lambda: (False, []))
    monkeypatch.setattr(
        "bithumb_bot.app.DEFAULT_BITHUMB_BROKER_CLASS",
        lambda: type(
            "_DiagBroker",
            (),
            {
                "get_accounts_validation_diagnostics": lambda self: {
                    "source": "accounts_v1_rest_snapshot",
                    "reason": "required currency missing",
                    "failure_category": "schema_mismatch",
                    "stale": False,
                    "execution_mode": "live_real_order_path",
                    "quote_currency": "KRW",
                    "base_currency": "BTC",
                    "base_currency_missing_policy": "block_when_base_currency_row_missing",
                    "preflight_outcome": "fail_real_order_blocked",
                }
            },
        )(),
    )

    cmd_health()
    out = capsys.readouterr().out

    assert "diag_execution_mode=live_real_order_path quote_currency=KRW base_currency=BTC" in out
    assert "base_missing_policy=block_when_base_currency_row_missing preflight_outcome=fail_real_order_blocked" in out
    assert "balance_source_preflight_outcome=fail_real_order_blocked" in out


def test_health_summary_flags_unresolved_orders_as_resume_unsafe(capsys, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="unsafe_resume_1", created_ts=now_ms - 15_000)
    runtime_state.refresh_open_order_health(now_epoch_sec=now_ms / 1000)
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    cmd_health()
    out = capsys.readouterr().out

    assert "state=paused" in out
    assert "unresolved_open_order_count=1" in out
    assert "can_resume=false" in out
    assert "blockers=STARTUP_SAFETY_GATE_BLOCKED" in out
    assert "resume_safety=unsafe (STARTUP_SAFETY_GATE_BLOCKED)" in out



def test_health_auto_clears_stale_initial_reconcile_halt(capsys, tmp_path):
    _set_tmp_db(tmp_path)
    _set_stale_initial_reconcile_halt_with_clean_reconcile()

    cmd_health()
    out = capsys.readouterr().out

    assert "halt_new_orders_blocked=0" in out
    assert "can_resume=true" in out
    assert "blockers=none" in out
    assert "resume_safety=safe" in out
    assert "halt_state_unresolved=False" in out
    assert "halt_reason_code=None" in out




def test_recovery_report_includes_submit_unknown_count(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="su_1", created_ts=now_ms - 10_000)

    report = _load_recovery_report()

    assert int(report["submit_unknown_count"]) == 1


def test_resume_refusal_prints_explicit_blocking_reasons_header(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)
    runtime_state.disable_trading_until(float("inf"), reason="manual operator pause")

    with pytest.raises(SystemExit):
        cmd_resume(force=False)

    out = capsys.readouterr().out
    assert "[RESUME] refused:" in out
    assert "blocking_reasons:" in out
    assert "code=STARTUP_SAFETY_GATE_BLOCKED" in out

def test_recovery_report_json_snapshot_schema_is_stable(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="open_1", created_ts=now_ms - 40_000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="recovery_1",
        created_ts=now_ms - 30_000,
    )

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={"remote_open_order_found": 1},
        now_epoch_sec=time.time() - 2,
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="periodic reconcile failed",
        reason_code="PERIODIC_RECONCILE_FAILED",
        unresolved=True,
    )

    cmd_recovery_report(as_json=True)
    out = capsys.readouterr().out
    payload = json.loads(out)

    assert set(payload.keys()) == {
        "broker_recent_orders_snapshot_error",
        "balance_split_mismatch_summary",
        "dust_classification",
        "dust_residual_present",
        "dust_residual_allow_resume",
        "dust_policy_reason",
        "dust_residual_summary",
        "dust_state",
        "dust_state_label",
        "dust_operator_action",
        "dust_operator_message",
        "dust_broker_local_match",
        "dust_new_orders_allowed",
        "dust_resume_allowed_by_policy",
        "dust_treat_as_flat",
        "dust_broker_qty",
        "dust_local_qty",
        "dust_delta_qty",
        "dust_min_qty",
        "dust_min_notional_krw",
        "dust_broker_qty_below_min",
        "dust_local_qty_below_min",
        "dust_broker_notional_below_min",
        "dust_local_notional_below_min",
        "recent_dust_unsellable_event",
        "active_blocker_summary",
        "blocker_summary",
        "blocker_summary_view",
        "blockers",
        "force_resume_allowed",
        "can_resume",
        "resume_blockers",
        "last_reconcile_summary",
        "oldest_orders",
        "oldest_unresolved_age_sec",
        "operator_next_action",
        "recommended_next_action",
        "non_overridable_blockers",
        "primary_blocker_code",
        "recent_halt_reason",
        "recommended_command",
        "recent_order_lifecycle",
        "recovery_required_count",
        "recovery_required_summary",
        "submit_unknown_count",
        "resume_blocked_reason",
        "resume_allowed",
        "risk_level",
        "trading_enabled",
        "emergency_flatten_blocked",
        "emergency_flatten_block_reason",
        "recovery_candidates",
        "remote_known_unresolved_verification_summary",
        "unprocessed_remote_open_orders",
        "unresolved_count",
        "unresolved_summary",
    }


def test_recovery_report_json_snapshot_has_required_fields(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="NEW", client_order_id="open_1", created_ts=now_ms - 50_000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="recovery_1",
        created_ts=now_ms - 20_000,
    )

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={"remote_open_order_found": 3},
        now_epoch_sec=time.time() - 1,
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="manual operator pause",
        reason_code="MANUAL_PAUSE",
        unresolved=True,
    )

    cmd_recovery_report(as_json=True)
    payload = json.loads(capsys.readouterr().out)

    assert payload["trading_enabled"] is False
    assert "code=" in payload["recent_halt_reason"]
    assert payload["recent_halt_reason"] != "none"
    assert payload["unresolved_count"] >= 1
    assert isinstance(payload["unresolved_summary"], list)
    assert payload["unresolved_summary"]
    assert payload["unresolved_summary"][0]["client_order_id"]
    assert payload["recovery_required_count"] >= 1
    assert isinstance(payload["recovery_required_summary"], list)
    assert payload["recovery_required_summary"]
    assert payload["primary_blocker_code"] != "-"
    assert payload["recovery_required_summary"][0]["client_order_id"]
    assert payload["last_reconcile_summary"] != "none"
    assert "status=" in payload["last_reconcile_summary"]
    assert payload["resume_allowed"] is False
    assert payload["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in payload["resume_blockers"]
    assert payload["force_resume_allowed"] is False
    assert isinstance(payload["blockers"], list)
    assert payload["blockers"]
    assert payload["blockers"][0]["code"]
    assert isinstance(payload["blockers"][0]["overridable"], bool)
    assert "total=" in payload["blocker_summary"]
    assert "non_overridable=" in payload["blocker_summary"]
    assert payload["active_blocker_summary"]
    assert payload["risk_level"] in {"low", "medium", "high"}
    assert isinstance(payload["non_overridable_blockers"], list)
    assert payload["operator_next_action"] in {
        "resume_now",
        "review_and_force_resume",
        "manual_recovery_required",
        "investigate_blockers",
    }
    assert payload["recommended_next_action"]
    assert payload["resume_blocked_reason"]
    assert payload["recommended_command"]




def test_recovery_report_blocker_summary_view_for_submit_unknown(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="SUBMIT_UNKNOWN", client_order_id="summary_submit_unknown", created_ts=now_ms - 5_000)

    report = _load_recovery_report()

    view = report["blocker_summary_view"]
    assert view
    assert view[0]["blocker"] == "STARTUP_SAFETY_GATE_BLOCKED"
    assert "submit_unknown=1" in view[0]["evidence"]
    assert view[0]["recommended_next_action"] == "uv run python bot.py reconcile"


def test_recovery_report_blocker_summary_view_for_recovery_required(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="summary_recovery_required", created_ts=now_ms - 5_000)

    report = _load_recovery_report()

    view = report["blocker_summary_view"]
    assert view
    assert view[0]["blocker"] == "STARTUP_SAFETY_GATE_BLOCKED"
    assert "recovery_required=1" in view[0]["evidence"]
    assert (
        view[0]["recommended_next_action"]
        == "uv run python bot.py recover-order --client-order-id <id>"
    )


def test_recovery_report_exposes_invalid_fill_price_recovery_reason(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="invalid_price_recovery",
        created_ts=now_ms - 5_000,
    )
    _set_last_error(
        client_order_id="invalid_price_recovery",
        last_error="recent fill has missing/invalid execution price; exchange_order_id=ex-sell-1; manual recovery required",
    )

    report = _load_recovery_report()
    rows = report["recovery_required_summary"]

    assert rows
    assert rows[0]["client_order_id"] == "invalid_price_recovery"
    assert "missing/invalid execution price" in rows[0]["last_error"]


def test_recovery_report_blocker_summary_view_for_persistent_halt(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="manual operator pause",
        reason_code="MANUAL_PAUSE",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    report = _load_recovery_report()

    view = report["blocker_summary_view"]
    assert view
    assert view[0]["blocker"] == "HALT_STATE_UNRESOLVED"
    assert "halt unresolved:" in view[0]["evidence"]
    assert view[0]["recommended_next_action"] == "uv run python bot.py restart-checklist"
def test_recovery_report_auto_clears_stale_initial_reconcile_halt(tmp_path):
    _set_tmp_db(tmp_path)
    _set_stale_initial_reconcile_halt_with_clean_reconcile()

    report = _load_recovery_report()

    assert report["can_resume"] is True
    assert report["resume_blockers"] == []
    assert report["recent_halt_reason"] == "none"
    state = runtime_state.snapshot()
    assert state.halt_state_unresolved is False
    assert state.halt_reason_code is None


def test_recovery_report_auto_clears_stale_locked_post_trade_reconcile_halt_when_safe(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason=(
            "reconcile failed (OperationalError): database is locked"
        ),
        reason_code="POST_TRADE_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "recent_fill_applied": 1,
            "balance_split_mismatch_count": 0,
            "balance_split_mismatch_summary": "none",
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
            "startup_gate_blocked": 0,
            "source_conflict_halt": 0,
        },
    )
    runtime_state.refresh_open_order_health()

    report = _load_recovery_report()

    assert report["can_resume"] is True
    assert report["resume_blockers"] == []
    assert report["recent_halt_reason"] == "none"
    assert report["last_reconcile_summary"] != "none"
    assert "status=ok" in str(report["last_reconcile_summary"])
    assert "reason_code=RECENT_FILL_APPLIED" in str(report["last_reconcile_summary"])
    state = runtime_state.snapshot()
    assert state.halt_new_orders_blocked is False
    assert state.halt_state_unresolved is False
    assert state.halt_reason_code is None


def test_recovery_report_can_resume_clean_state(tmp_path):
    _set_tmp_db(tmp_path)

    report = _load_recovery_report()

    assert report["can_resume"] is True
    assert report["resume_blockers"] == []


def test_recovery_report_can_resume_false_for_unresolved_recovery_state(tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="resume_blocked_rr", created_ts=now_ms)

    report = _load_recovery_report()

    assert report["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in report["resume_blockers"]


def test_recovery_report_can_resume_false_for_risk_halt_with_non_flat_position(tmp_path):
    _set_tmp_db(tmp_path)
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.25, 1000000.0, 0.0, 0.25, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.disable_trading_until(
        float("inf"),
        reason="kill switch engaged",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=False,
    )

    report = _load_recovery_report()

    assert report["can_resume"] is False
    assert "HALT_RISK_OPEN_POSITION" in report["resume_blockers"]


def test_recovery_report_can_resume_true_again_after_risk_halt_is_flat(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="kill switch engaged",
        reason_code="KILL_SWITCH",
        halt_new_orders_blocked=True,
        unresolved=False,
    )
    runtime_state.enable_trading()

    report = _load_recovery_report()

    assert report["can_resume"] is True
    assert report["resume_blockers"] == []


def test_resume_eligibility_blocks_when_dust_requires_operator_review_even_without_halt(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 min_qty=0.00010000 min_notional_krw=5000.0",
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    eligible, blockers = evaluate_resume_eligibility()

    assert eligible is False
    assert [b.code for b in blockers] == ["DANGEROUS_DUST_REVIEW_REQUIRED"]
    assert all(b.overridable is False for b in blockers)


def test_resume_eligibility_keeps_unresolved_open_order_block_even_when_dust_is_resume_safe(tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(
        status="NEW",
        client_order_id="open_dust_guard",
        created_ts=int(time.time() * 1000),
        side="SELL",
        qty_req=0.00009,
        price=100000000.0,
    )
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 min_qty=0.00010000 min_notional_krw=5000.0",
            "remote_open_order_found": 1,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    eligible, blockers = evaluate_resume_eligibility()

    assert eligible is False
    blocker_codes = [b.code for b in blockers]
    assert "STARTUP_SAFETY_GATE_BLOCKED" in blocker_codes
    assert "DANGEROUS_DUST_REVIEW_REQUIRED" not in blocker_codes


def test_recovery_report_blocks_resume_now_when_dust_requires_operator_review(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 min_qty=0.00010000",
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    report = _load_recovery_report()

    assert report["resume_allowed"] is False
    assert report["can_resume"] is False
    assert "DANGEROUS_DUST_REVIEW_REQUIRED" in report["resume_blockers"]
    assert report["operator_next_action"] != "resume_now"
    assert report["operator_next_action"] == "manual_dust_review_required"
    assert report["dust_state"] == "dangerous_dust"
    assert report["dust_new_orders_allowed"] is False
    assert report["dust_resume_allowed_by_policy"] is False


def test_recovery_report_prioritizes_dangerous_dust_when_unresolved_order_also_blocks_resume(tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(
        status="NEW",
        client_order_id="open_dangerous_dust_report_guard",
        created_ts=int(time.time() * 1000),
        side="SELL",
        qty_req=0.00009,
        price=100000000.0,
    )
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_residual_summary": (
                "broker_qty=0.00009900 local_qty=0.00001000 delta=0.00008900 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=0 "
                "classification=dangerous_dust matched_harmless=0 broker_local_match=0 "
                "allow_resume=0 effective_flat=0 policy_reason=dangerous_dust_operator_review_required"
            ),
            "remote_open_order_found": 1,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    report = _load_recovery_report()

    assert report["dust_state"] == "dangerous_dust"
    assert report["dust_broker_local_match"] is False
    assert report["dust_resume_allowed_by_policy"] is False
    assert report["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in report["resume_blockers"]
    assert "DANGEROUS_DUST_REVIEW_REQUIRED" in report["resume_blockers"]
    assert report["operator_next_action"] == "manual_dust_review_required"


def test_recovery_report_keeps_effective_flat_dust_visible_when_unresolved_order_also_blocks_resume(tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(
        status="NEW",
        client_order_id="open_dust_report_guard",
        created_ts=int(time.time() * 1000),
        side="SELL",
        qty_req=0.00009,
        price=100000000.0,
    )
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": "broker_qty=0.00009629 local_qty=0.00009629 min_qty=0.00010000",
            "remote_open_order_found": 1,
            "submit_unknown_unresolved": 0,
            "balance_split_mismatch_count": 0,
        },
    )

    report = _load_recovery_report()

    assert report["dust_state"] == "matched_harmless_dust"
    assert report["dust_resume_allowed_by_policy"] is True
    assert report["can_resume"] is False
    assert "STARTUP_SAFETY_GATE_BLOCKED" in report["resume_blockers"]
    assert "DANGEROUS_DUST_REVIEW_REQUIRED" not in report["resume_blockers"]
    assert report["operator_next_action"] == "investigate_blockers"


def test_resume_eligibility_clears_stale_lock_halt_after_successful_reconcile_evidence(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.disable_trading_until(
        float("inf"),
        reason="reconcile failed (OperationalError): database is locked",
        reason_code="POST_TRADE_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "recent_fill_applied": 1,
            "balance_split_mismatch_count": 0,
        },
    )
    runtime_state.refresh_open_order_health()

    eligible, blockers = evaluate_resume_eligibility()

    assert eligible is True
    assert blockers == []
    state = runtime_state.snapshot()
    assert state.resume_gate_blocked is False
    assert state.resume_gate_reason is None
    assert state.halt_new_orders_blocked is False
    assert state.halt_state_unresolved is False


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


def test_reconcile_live_accepts_injected_dependencies(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    broker = object()
    calls: list[object] = []

    def _broker_factory():
        calls.append("factory")
        return broker

    def _reconcile(candidate):
        calls.append(("reconcile", candidate))

    try:
        cmd_reconcile(broker_factory=_broker_factory, reconcile_fn=_reconcile)
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "[RECONCILE] completed one live reconciliation pass" in out
    assert calls == ["factory", ("reconcile", broker)]


def test_recover_order_success_for_known_exchange_order_id(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverSuccessBroker(),
    )

    try:
        cmd_recover_order(
            client_order_id="needs_recovery",
            exchange_order_id="ex_manual_1",
            confirm=True,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT status, exchange_order_id, qty_filled
            FROM orders
            WHERE client_order_id='needs_recovery'
            """
        ).fetchone()
        fills = conn.execute(
            """
            SELECT fill_id, qty, fee
            FROM fills
            WHERE client_order_id='needs_recovery'
            """
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


def test_recover_order_failure_keeps_recovery_required_and_exits_non_zero(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverAmbiguousBroker(),
    )

    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="needs_recovery",
                exchange_order_id="ex_manual_2",
                confirm=True,
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    assert exc.value.code == 1

    conn = ensure_db()
    try:
        row = conn.execute(
            """
            SELECT status, exchange_order_id, last_error
            FROM orders
            WHERE client_order_id='needs_recovery'
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] == "ex_manual_2"
    assert "manual recovery failed" in str(row["last_error"])


def test_recover_order_dry_run_prints_preview_and_makes_no_changes(capsys, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    try:
        cmd_recover_order(
            client_order_id="needs_recovery",
            exchange_order_id="ex_preview_1",
            dry_run=True,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    out = capsys.readouterr().out
    assert "[RECOVER-ORDER] preview" in out
    assert "target_order_id=needs_recovery exchange_order_id=ex_preview_1" in out
    assert "current_known_state=status=RECOVERY_REQUIRED" in out
    assert "proposed_recovery_action=manual_recover_with_exchange_id" in out
    assert "[RECOVER-ORDER] dry-run: no changes applied" in out

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE client_order_id='needs_recovery'"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["status"] == "RECOVERY_REQUIRED"
    assert row["exchange_order_id"] is None


def test_recover_order_requires_explicit_confirmation(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    broker_calls = {"n": 0}

    def _unexpected_broker():
        broker_calls["n"] += 1
        return _RecoverSuccessBroker()

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _unexpected_broker)
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="needs_recovery",
                exchange_order_id="ex_confirm_needed",
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1
    assert broker_calls["n"] == 0


def test_recover_order_refuses_when_order_not_recovery_required(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="FILLED",
        client_order_id="already_filled",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    broker_calls = {"n": 0}

    def _unexpected_broker():
        broker_calls["n"] += 1
        return _RecoverSuccessBroker()

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _unexpected_broker)
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="already_filled",
                exchange_order_id="ex_should_refuse",
                confirm=True,
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1
    assert broker_calls["n"] == 0


def test_recover_order_allows_new_unresolved_when_single_high_confidence_terminal_match(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="NEW",
        client_order_id="new_unresolved_recoverable",
        created_ts=now_ms,
        price=100.0,
    )

    conn = ensure_db()
    try:
        set_exchange_order_id("new_unresolved_recoverable", "ex-unresolved-1", conn=conn)
        conn.commit()
    finally:
        conn.close()

    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    broker = _RecoverUnresolvedHighConfidenceTerminalBroker(
        recent_orders=[
            BrokerOrder(
                "new_unresolved_recoverable",
                "ex-unresolved-1",
                "BUY",
                "FILLED",
                100.0,
                0.01,
                0.01,
                now_ms,
                now_ms,
            )
        ],
        remote_status="FILLED",
    )

    try:
        cmd_recover_order(
            client_order_id="new_unresolved_recoverable",
            exchange_order_id="ex-unresolved-1",
            confirm=True,
            broker_factory=lambda: broker,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE client_order_id='new_unresolved_recoverable'"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "ex-unresolved-1"


def test_recover_order_refuses_when_unresolved_has_multiple_high_confidence_candidates(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="NEW",
        client_order_id="new_unresolved_ambiguous",
        created_ts=now_ms,
        price=100.0,
    )

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    broker = _RecoverUnresolvedHighConfidenceTerminalBroker(
        recent_orders=[
            BrokerOrder("new_unresolved_ambiguous", "ex-a", "BUY", "FILLED", 100.0, 0.01, 0.01, now_ms, now_ms),
            BrokerOrder("new_unresolved_ambiguous", "ex-b", "BUY", "FILLED", 100.0, 0.01, 0.01, now_ms, now_ms),
        ],
        remote_status="FILLED",
    )
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="new_unresolved_ambiguous",
                exchange_order_id="ex-a",
                confirm=True,
                broker_factory=lambda: broker,
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1


def test_recover_order_refuses_when_unresolved_candidate_is_not_terminal(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="NEW",
        client_order_id="new_unresolved_non_terminal",
        created_ts=now_ms,
        price=100.0,
    )
    conn = ensure_db()
    try:
        set_exchange_order_id("new_unresolved_non_terminal", "ex-non-terminal", conn=conn)
        conn.commit()
    finally:
        conn.close()

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")
    broker = _RecoverUnresolvedHighConfidenceTerminalBroker(
        recent_orders=[
            BrokerOrder(
                "new_unresolved_non_terminal",
                "ex-non-terminal",
                "BUY",
                "NEW",
                100.0,
                0.01,
                0.0,
                now_ms,
                now_ms,
            )
        ],
        remote_status="NEW",
    )
    try:
        with pytest.raises(SystemExit) as exc:
            cmd_recover_order(
                client_order_id="new_unresolved_non_terminal",
                exchange_order_id="ex-non-terminal",
                confirm=True,
                broker_factory=lambda: broker,
            )
    finally:
        object.__setattr__(settings, "MODE", original_mode)

    assert exc.value.code == 1


def test_recover_order_does_not_auto_resume_trading(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverSuccessBroker(),
    )

    runtime_state.enable_trading()
    try:
        cmd_recover_order(
            client_order_id="needs_recovery",
            exchange_order_id="ex_manual_3",
            confirm=True,
        )
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")


def test_resume_succeeds_after_manual_recovery_clears_recovery_required(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="needs_recovery",
        created_ts=now_ms,
    )
    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverSuccessBroker(),
    )

    try:
        runtime_state.disable_trading_until(float("inf"), reason="startup recovery gate")
        cmd_recover_order(
            client_order_id="needs_recovery",
            exchange_order_id="ex_manual_4",
            confirm=True,
        )
        cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    report = _load_recovery_report()
    state = runtime_state.snapshot()
    assert int(report["recovery_required_count"]) == 0
    assert int(report["unresolved_count"]) == 0
    assert state.trading_enabled is True


def test_halt_resume_flow_requires_manual_recover_order_before_resume(
    monkeypatch, tmp_path
):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="RECOVERY_REQUIRED",
        client_order_id="halt_resume_recovery",
        created_ts=now_ms,
    )

    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed (RuntimeError): broker timeout",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    with pytest.raises(SystemExit):
        cmd_resume(force=False)

    state_blocked = runtime_state.snapshot()
    assert state_blocked.halt_state_unresolved is True

    original_mode = settings.MODE
    original_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1000010.0)
    monkeypatch.setattr(
        "bithumb_bot.broker.bithumb.BithumbBroker",
        lambda: _RecoverSuccessBroker(),
    )
    try:
        cmd_recover_order(
            client_order_id="halt_resume_recovery",
            exchange_order_id="ex_halt_resume_1",
            confirm=True,
        )
        cmd_resume(force=False)
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_cash)

    state_after = runtime_state.snapshot()
    assert state_after.halt_state_unresolved is False
    assert state_after.trading_enabled is True


def test_cmd_run_notifies_run_lock_conflict(monkeypatch):
    from bithumb_bot.app import cmd_run
    from bithumb_bot.run_lock import RunLockError

    notifications: list[str] = []
    run_loop_calls = {"n": 0}
    monkeypatch.setattr("bithumb_bot.app.notify", lambda msg: notifications.append(msg))
    monkeypatch.setattr(
        "bithumb_bot.engine.run_loop",
        lambda *_args, **_kwargs: run_loop_calls.__setitem__(
            "n", run_loop_calls["n"] + 1
        ),
    )

    class _RaiseOnEnter:
        def __enter__(self):
            raise RunLockError("another bot run loop is already running")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("bithumb_bot.run_lock.acquire_run_lock", lambda *_args, **_kwargs: _RaiseOnEnter())

    with pytest.raises(SystemExit) as exc:
        cmd_run(5, 20)

    assert exc.value.code == 1
    assert run_loop_calls["n"] == 0
    assert any("event=run_lock_conflict" in n for n in notifications)
    assert any("reason_code=RUN_LOCK_CONFLICT" in n for n in notifications)
    assert any("client_order_id=-" in n for n in notifications)
    assert any("submit_attempt_id=-" in n for n in notifications)
    assert any("exchange_order_id=-" in n for n in notifications)


def test_restart_checklist_blocks_when_restart_risks_exist(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="needs_recovery", created_ts=now_ms)
    _insert_order(status="NEW", client_order_id="open_order", created_ts=now_ms)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.25, 1000000.0, 0.0, 0.25, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=False,
        error="timeout",
        reason_code="RECONCILE_TIMEOUT",
    )
    runtime_state.disable_trading_until(
        float("inf"),
        reason="initial reconcile failed",
        reason_code="INITIAL_RECONCILE_FAILED",
        halt_new_orders_blocked=True,
        unresolved=True,
    )

    cmd_restart_checklist()
    out = capsys.readouterr().out

    assert "[RESTART-SAFETY-CHECKLIST]" in out
    assert "BLOCKED unresolved/recovery-required orders" in out
    assert "BLOCKED open orders" in out
    assert "BLOCKED open position" in out
    assert "BLOCKED halt state" in out
    assert "BLOCKED last reconcile" in out
    assert "safe_to_resume=0" in out


def test_restart_checklist_auto_clears_stale_initial_reconcile_halt(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    _set_stale_initial_reconcile_halt_with_clean_reconcile()

    cmd_restart_checklist()
    out = capsys.readouterr().out

    assert "PASS    halt state" in out
    assert "safe_to_resume=1" in out


def test_restart_checklist_passes_when_safe_to_resume(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()

    cmd_restart_checklist()
    out = capsys.readouterr().out

    assert "PASS    unresolved/recovery-required orders" in out
    assert "PASS    open orders" in out
    assert "PASS    open position" in out
    assert "PASS    halt state" in out
    assert "PASS    last reconcile" in out
    assert "safe_to_resume=1" in out


class _FlattenBrokerSuccess:
    def __init__(self):
        self.calls: list[dict[str, str | float | None]] = []
        self.balance = BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=10.0, asset_locked=0.0)

    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None):
        self.calls.append({"client_order_id": client_order_id, "side": side, "qty": qty, "price": price})

        class _Order:
            exchange_order_id = "ex-flat-1"
            status = "NEW"

        return _Order()

    def get_balance(self) -> BrokerBalance:
        return self.balance


def test_flatten_position_no_position_safe_noop(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    cmd_flatten_position(dry_run=False)
    out = capsys.readouterr().out

    assert "no position to flatten" in out
    state = runtime_state.snapshot()
    assert state.last_flatten_position_status == "no_position"
    assert state.last_flatten_position_summary is not None
    assert '"status": "no_position"' in state.last_flatten_position_summary


def test_flatten_position_submits_sell_when_position_exists(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    prev_step = settings.LIVE_ORDER_QTY_STEP
    prev_max_decimals = settings.LIVE_ORDER_MAX_QTY_DECIMALS
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.000001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 6)
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.12345678, 1000000.0, 0.0, 0.12345678, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    broker = _FlattenBrokerSuccess()

    class _BrokerFactory:
        def __call__(self):
            return broker

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _BrokerFactory())
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    try:
        cmd_flatten_position(dry_run=False)
        out = capsys.readouterr().out

        assert "submitted" in out
        assert len(broker.calls) == 1
        assert broker.calls[0]["side"] == "SELL"
        assert abs(float(broker.calls[0]["qty"]) - 0.123456) < 1e-12
        state = runtime_state.snapshot()
        assert state.last_flatten_position_status == "submitted"
    finally:
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", prev_step)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", prev_max_decimals)


def test_flatten_position_submit_failure_persisted(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.01, 1000000.0, 0.0, 0.01, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    class _FailBroker:
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=10.0, asset_locked=0.0)

        def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None):
            raise RuntimeError("submit boom")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _FailBroker())
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "failed" in out
    state = runtime_state.snapshot()
    assert state.last_flatten_position_status == "failed"
    assert state.last_flatten_position_summary is not None
    assert "submit boom" in state.last_flatten_position_summary


def test_flatten_position_validation_failure_blocks_submission(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.015, 1000000.0, 0.0, 0.015, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    class _LowAssetBroker:
        def __init__(self):
            self.place_order_calls = 0

        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)

        def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None):
            self.place_order_calls += 1
            raise AssertionError("place_order must not be called when pretrade fails")

    broker = _LowAssetBroker()
    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: broker)
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "failed: ValueError: insufficient available asset" in out
    assert broker.place_order_calls == 0


def test_flatten_position_blocks_on_invalid_best_quote(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", lambda _cfg: None)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.015, 1000000.0, 0.0, 0.015, 0.0)
            """
        )
        conn.commit()
    finally:
        conn.close()

    class _NoSubmitBroker:
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(cash_available=0.0, cash_locked=0.0, asset_available=10.0, asset_locked=0.0)

        def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None):
            raise AssertionError("place_order must not be called when best quote is invalid")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: _NoSubmitBroker())
    monkeypatch.setattr(
        "bithumb_bot.flatten.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_010_000.0, ask_price=100_000_000.0),
    )

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "failed: RuntimeError: orderbook top invalid quote" in out


def test_flatten_position_blocks_on_live_preflight_failure(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")

    def _raise_preflight(_cfg):
        raise app_module.LiveModeValidationError("preflight boom")

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", _raise_preflight)

    class _BrokerFactory:
        def __call__(self):
            raise AssertionError("broker should not be constructed when preflight fails")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _BrokerFactory())

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "failed: live mode preflight" not in out
    assert "failed: preflight boom" in out


def test_flatten_position_blocks_when_live_unarmed(monkeypatch, tmp_path, capsys):
    _set_tmp_db(tmp_path, monkeypatch)
    monkeypatch.setenv("MODE", "live")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)

    def _armed_gate(_cfg):
        raise app_module.LiveModeValidationError("LIVE_REAL_ORDER_ARMED=true is required")

    monkeypatch.setattr("bithumb_bot.app.validate_live_mode_preflight", _armed_gate)

    class _BrokerFactory:
        def __call__(self):
            raise AssertionError("broker should not be constructed when live mode is unarmed")

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", _BrokerFactory())

    with pytest.raises(SystemExit) as exc:
        cmd_flatten_position(dry_run=False)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "LIVE_REAL_ORDER_ARMED=true is required" in out


def test_resume_blocked_when_emergency_flatten_unresolved(tmp_path, monkeypatch):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    runtime_state.record_flatten_position_result(
        status="failed",
        summary={"status": "failed", "error": "submit boom", "trigger": "position-loss-halt"},
    )

    monkeypatch.setattr("bithumb_bot.broker.bithumb.BithumbBroker", lambda: object())
    monkeypatch.setattr("bithumb_bot.app.reconcile_with_broker", lambda broker: None)

    with pytest.raises(SystemExit) as exc:
        cmd_resume(force=False)

    assert exc.value.code == 1
    report = _load_recovery_report()
    assert report["can_resume"] is False
    assert "EMERGENCY_FLATTEN_UNRESOLVED" in report["resume_blockers"]


def test_health_and_recovery_report_expose_emergency_flatten_blocker(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.enable_trading()
    runtime_state.record_flatten_position_result(
        status="failed",
        summary={"status": "failed", "error": "submit boom", "trigger": "kill-switch"},
    )

    cmd_health()
    health_out = capsys.readouterr().out
    assert "emergency_flatten_blocked=True" in health_out
    assert "emergency_flatten_block_reason=emergency flatten unresolved" in health_out
    assert "blockers=STARTUP_SAFETY_GATE_BLOCKED, EMERGENCY_FLATTEN_UNRESOLVED" in health_out

    cmd_recovery_report(as_json=False)
    report_out = capsys.readouterr().out
    assert "emergency_flatten_blocked=1" in report_out
    assert "emergency_flatten_block_reason=emergency flatten unresolved" in report_out
    assert "EMERGENCY_FLATTEN_UNRESOLVED" in report_out


def test_health_and_recovery_report_include_dust_residual_metadata(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "balance_split_mismatch_count": 0,
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": (
                "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
                "classification=matched_harmless_dust matched_harmless=1 broker_local_match=1 allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
            "oversized_debug_blob": "x" * 5000,
        },
    )

    cmd_health()
    health_out = capsys.readouterr().out
    assert "dust_residual_present=1" in health_out
    assert "dust_residual_allow_resume=1" in health_out
    assert "dust_policy_reason=matched_harmless_dust_resume_allowed" in health_out
    assert "dust_state=matched_harmless_dust" in health_out
    assert "dust_operator_action=monitor_only" in health_out
    assert "dust_resume_allowed_by_policy=1" in health_out
    assert "dust_treat_as_flat=1" in health_out
    assert "dust_broker_qty=0.00009629" in health_out
    assert "dust_local_qty=0.00009629" in health_out
    assert "dust_broker_local_match=1" in health_out
    assert "dust_qty_below_min=broker=1 local=1" in health_out
    assert "dust_notional_below_min=broker=0 local=0" in health_out

    cmd_recovery_report(as_json=False)
    report_out = capsys.readouterr().out
    assert "[P3.0] dust_residual" in report_out
    assert "present=1" in report_out
    assert "allow_resume=1" in report_out
    assert "policy_reason=matched_harmless_dust_resume_allowed" in report_out
    assert "state=matched_harmless_dust" in report_out
    assert "operator_action=monitor_only" in report_out
    assert "resume_allowed_by_policy=1" in report_out
    assert "treat_as_flat=1" in report_out
    assert (
        "broker_qty=0.00009629 local_qty=0.00009629 delta_qty=0.00000000 "
        "min_qty=0.00010000 min_notional_krw=5000.0"
    ) in report_out
    assert "qty_below_min=broker=1 local=1 notional_below_min=broker=0 local=0" in report_out
    assert "broker_local_match=1" in report_out


def test_recovery_report_includes_recent_dust_unsellable_sell_event(tmp_path, capsys):
    _set_tmp_db(tmp_path)
    now_ms = int(time.time() * 1000)
    _insert_order(
        status="FAILED",
        client_order_id="dust_exit_1",
        created_ts=now_ms,
        side="SELL",
        qty_req=0.00009,
        price=100000000.0,
    )

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO order_events(
                client_order_id, event_type, event_ts, order_status, side, qty, price, submission_reason_code, message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "dust_exit_1",
                "submit_attempt_recorded",
                now_ms,
                "FAILED",
                "SELL",
                0.00009,
                100000000.0,
                DUST_RESIDUAL_UNSELLABLE,
                "state=EXIT_PARTIAL_LEFT_DUST;operator_action=MANUAL_DUST_REVIEW_REQUIRED;position_qty=0.000090000000",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_recovery_report(as_json=False)
    out = capsys.readouterr().out

    assert "[P3.0a] recent_dust_unsellable_event" in out
    assert f"reason_code={DUST_RESIDUAL_UNSELLABLE}" in out
    assert "EXIT_PARTIAL_LEFT_DUST" in out
    assert "MANUAL_DUST_REVIEW_REQUIRED" in out

