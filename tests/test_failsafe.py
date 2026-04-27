from __future__ import annotations

import os
from pathlib import Path

import httpx
import pytest

from bithumb_bot import runtime_state
from bithumb_bot.broker.base import BrokerBalance, BrokerOrder, BrokerRejectError
from bithumb_bot.broker.balance_source import _default_flat_start_safety_check
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.engine import get_health_status, run_loop
from bithumb_bot.execution_service import (
    LiveSignalExecutionService,
    SignalExecutionRequest,
    build_execution_decision_summary,
    build_residual_sell_candidate,
    build_residual_sell_presubmit_proof,
)
from bithumb_bot.marketdata import _get_with_retry
from bithumb_bot.public_api_orderbook import BestQuote


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
        "RESIDUAL_LIVE_SELL_MODE": settings.RESIDUAL_LIVE_SELL_MODE,
        "RESIDUAL_BUY_SIZING_MODE": settings.RESIDUAL_BUY_SIZING_MODE,
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


def _set_live_runtime_paths(monkeypatch: pytest.MonkeyPatch, *, base_dir: Path) -> None:
    roots = {
        "ENV_ROOT": (base_dir / "env").resolve(),
        "RUN_ROOT": (base_dir / "run").resolve(),
        "DATA_ROOT": (base_dir / "data").resolve(),
        "LOG_ROOT": (base_dir / "logs").resolve(),
        "BACKUP_ROOT": (base_dir / "backup").resolve(),
    }
    for key, value in roots.items():
        monkeypatch.setenv(key, str(value))
    monkeypatch.setenv("RUN_LOCK_PATH", str((roots["RUN_ROOT"] / "live" / "bithumb-bot.lock").resolve()))


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
    def __init__(self, *, open_order_created_ts: int | None = None, asset_qty: float = 0.0):
        self.open_order_created_ts = open_order_created_ts
        self.asset_qty = float(asset_qty)
        self.marked_recovery_required = 0

    def execute(self, query, params=None):
        q = " ".join(str(query).split())

        if "FROM candles" in q:
            return _Rows({"ts": 10_000, "close": 100.0})

        if "COUNT(*) AS open_count" in q:
            if self.open_order_created_ts is None:
                return _Rows({"open_count": 0, "oldest_created_ts": None})
            return _Rows({"open_count": 1, "oldest_created_ts": self.open_order_created_ts})

        if "COUNT(*) AS open_order_count" in q:
            return _Rows(
                {
                    "open_order_count": 0 if self.open_order_created_ts is None else 1,
                    "recovery_required_count": 0,
                }
            )

        if "FROM portfolio" in q:
            return _Rows({"cash_krw": 100000.0, "asset_qty": self.asset_qty})

        if "SELECT COUNT(*) AS cnt FROM orders WHERE status='SUBMIT_UNKNOWN'" in q:
            return _Rows({"cnt": 0})

        if "SELECT COUNT(*) AS cnt FROM orders WHERE status='RECOVERY_REQUIRED'" in q:
            return _Rows({"cnt": 0})

        if "SELECT COUNT(*) AS cnt FROM orders WHERE status='ACCOUNTING_PENDING'" in q:
            return _Rows({"cnt": 0})

        if "status='SUBMIT_UNKNOWN'" in q and "exchange_order_id" in q:
            return _Rows({"cnt": 0})

        if "client_order_id LIKE 'remote_%'" in q:
            return _Rows({"cnt": 0})

        if (
            "COALESCE(SUM(MAX(qty_req - qty_filled, 0.0)), 0.0) AS reserved_exit_qty" in q
            and "FROM orders" in q
            and "side='SELL'" in q
        ):
            return _Rows({"reserved_exit_qty": 0.0})

        if (
            "SELECT DISTINCT" in q
            and "FROM open_position_lots" in q
            and "lot_semantic_version" in q
        ):
            if self.asset_qty <= 1e-12:
                return _Rows([])
            return _Rows(
                [
                    {
                        "lot_semantic_version": 1,
                        "internal_lot_size": 0.0001,
                        "lot_min_qty": 0.0001,
                        "lot_qty_step": 0.0001,
                        "lot_min_notional_krw": 5000.0,
                        "lot_max_qty_decimals": 8,
                        "lot_rule_source_mode": "ledger",
                    }
                ]
            )

        if "FROM open_position_lots" in q and "SUM(" in q:
            if "executable_lot_count" in q and "dust_tracking_lot_count, 0) = 0" in q:
                return _Rows((self.asset_qty, 1 if self.asset_qty > 1e-12 else 0))
            if "dust_tracking_lot_count" in q and "executable_lot_count, 0) = 0" in q:
                return _Rows((0.0, 0))

        if (
            "AS pending_submit_count" in q
            and "AS accounting_pending_count" in q
            and "AS submit_unknown_count" in q
            and "AS recovery_required_count" in q
            and "AS stale_new_partial_count" in q
            and "FROM orders" in q
        ):
            if self.open_order_created_ts is not None:
                return _Rows(
                    {
                        "pending_submit_count": 0,
                        "accounting_pending_count": 0,
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
                        "accounting_pending_count": 0,
                        "submit_unknown_count": 0,
                        "recovery_required_count": 0,
                        "stale_new_partial_count": 0,
                    }
                )

            return _Rows(
                {
                    "pending_submit_count": row["pending_submit_count"] or 0,
                    "accounting_pending_count": row["accounting_pending_count"] or 0,
                    "submit_unknown_count": row["submit_unknown_count"] or 0,
                    "recovery_required_count": row["recovery_required_count"] or 0,
                    "stale_new_partial_count": row["stale_new_partial_count"] or 0,
                }
            )

        if "COUNT(*) AS repair_count" in q and "FROM fee_gap_accounting_repairs" in q:
            return _Rows({"repair_count": 0})

        if "COUNT(*) AS repair_count" in q and "FROM position_authority_repairs" in q:
            return _Rows({"repair_count": 0})

        if "FROM fee_gap_accounting_repairs" in q and "ORDER BY event_ts DESC" in q:
            return _Rows(None)

        if "FROM external_position_adjustments" in q and "COUNT(*) AS adjustment_count" in q:
            return _Rows({"adjustment_count": 0, "asset_qty_total": 0.0, "cash_total": 0.0})

        if "FROM external_position_adjustments" in q and "ORDER BY event_ts DESC" in q:
            return _Rows(None)

        if "FROM broker_fill_observations" in q:
            return _Rows([])

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

    def fetchall(self):
        if self._row is None:
            return []
        if isinstance(self._row, list):
            return self._row
        return [self._row]


class _DummyBroker:
    def get_open_orders(self):
        return []

    def cancel_order(self, *args, **kwargs):
        return None

class _FlattenFailBroker(_DummyBroker):
    def get_balance(self):
        return BrokerBalance(
            cash_available=100_000.0,
            cash_locked=0.0,
            asset_available=1.0,
            asset_locked=0.0,
        )

    def place_order(self, *args, **kwargs):
        raise RuntimeError("place_order boom")

def _prepare_run_loop(monkeypatch, open_order_created_ts=None, asset_qty: float = 0.0):
    monkeypatch.setattr("bithumb_bot.config.notifier_is_configured", lambda: True)
    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
    runtime_state.set_startup_gate_reason(None)
    runtime_state._STATE.last_processed_candle_ts_ms = None  # type: ignore[attr-defined]
    runtime_state._STATE.last_candle_ts_ms = None  # type: ignore[attr-defined]
    runtime_state._STATE.last_candle_status = None  # type: ignore[attr-defined]
    runtime_state._STATE.last_candle_status_detail = None  # type: ignore[attr-defined]

    resolved_db_path = str(Path(settings.DB_PATH).resolve())
    monkeypatch.setenv("DB_PATH", resolved_db_path)
    object.__setattr__(settings, "DB_PATH", resolved_db_path)
    _set_live_runtime_paths(monkeypatch, base_dir=Path(resolved_db_path).parent)

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    monkeypatch.setattr("bithumb_bot.engine.validate_live_mode_preflight", lambda _cfg: None)
    monkeypatch.setattr("bithumb_bot.engine.validate_market_runtime", lambda _cfg: None)
    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 1)
    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr(
        "bithumb_bot.engine._select_latest_closed_candle",
        lambda _conn, **_kwargs: ({"ts": 9000, "close": 100.0}, None),
    )
    monkeypatch.setattr(
        "bithumb_bot.engine.compute_signal",
        lambda conn, s, l: {
            "ts": 9000,
            "last_close": 100.0,
            "curr_s": 1.0,
            "curr_l": 0.5,
            "signal": "BUY",
        },
    )

    loop_conn = _LoopConn(open_order_created_ts=open_order_created_ts, asset_qty=asset_qty)
    monkeypatch.setattr("bithumb_bot.engine.ensure_db", lambda: loop_conn)
    monkeypatch.setattr("bithumb_bot.flatten.ensure_db", lambda: loop_conn)
    monkeypatch.setattr("bithumb_bot.flatten.init_portfolio", lambda _conn: None)
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


def test_flat_start_safety_check_avoids_self_lock_when_writer_transaction_open():
    writer_conn = ensure_db()
    try:
        writer_conn.execute("BEGIN IMMEDIATE")
        allowed, reason = _default_flat_start_safety_check()
    finally:
        writer_conn.rollback()
        writer_conn.close()

    assert isinstance(allowed, bool)
    assert isinstance(reason, str)


def test_flat_start_safety_check_blocks_local_dust_position_without_broker_confirmation(monkeypatch):
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.00009629, 1000000.0, 0.0, 0.00009629, 0.0)
            """
        )
        conn.execute(
            "INSERT INTO candles(pair, interval, ts, open, high, low, close, volume) VALUES ('KRW-BTC', '1m', 1, 100000000, 100000000, 100000000, 100000000, 1.0)"
        )
        conn.commit()
    finally:
        conn.close()

    class _Resolved:
        class rules:
            min_qty = 0.0001
            min_notional_krw = 5000.0

    monkeypatch.setattr("bithumb_bot.broker.order_rules.get_effective_order_rules", lambda _pair: _Resolved())

    allowed, reason = _default_flat_start_safety_check()

    assert allowed is False
    assert "flat_start_requires_operator_review" in reason
    assert "state=blocking_dust" in reason
    assert "broker_qty=0.00000000" in reason
    assert "local_qty=0.00009629" in reason
    assert "min_qty=0.00010000" in reason
    assert "qty_below_min(broker=0 local=1)" in reason


def test_run_loop_surfaces_market_preflight_error_during_live_startup(monkeypatch):
    _prepare_run_loop(monkeypatch)
    called = {"n": 0}

    def _market_runtime(_cfg):
        called["n"] += 1
        raise ValueError("market gate")

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.validate_market_runtime", _market_runtime)

    with pytest.raises(Exception) as exc:
        run_loop(5, 20)

    assert "market gate" in str(exc.value)
    assert called["n"] == 1


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


def test_run_loop_live_harmless_dust_sell_suppresses_before_live_execution(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.00009193)

    monkeypatch.setattr(
        "bithumb_bot.engine.compute_signal",
        lambda conn, s, l: {
            "ts": 1000,
            "last_close": 100_000_000.0,
            "curr_s": 1.0,
            "curr_l": 0.5,
            "signal": "SELL",
            "position_state": {
                "normalized_exposure": {
                    "raw_total_asset_qty": 0.00009193,
                    "open_exposure_qty": 0.0,
                    "dust_tracking_qty": 0.00009193,
                    "sellable_executable_qty": 0.0,
                    "sellable_executable_lot_count": 0,
                    "exit_allowed": False,
                    "exit_block_reason": "dust_only_remainder",
                }
            },
        },
    )

    suppression_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        "bithumb_bot.engine.record_harmless_dust_exit_suppression",
        lambda **kwargs: suppression_calls.append(kwargs) or True,
    )
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not reach live execution")),
    )
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009193,
            "dust_local_qty": 0.00009193,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9193.0,
            "dust_local_notional_krw": 9193.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert suppression_calls
    assert suppression_calls[0]["signal"] == "SELL"
    assert suppression_calls[0]["side"] == "SELL"
    assert suppression_calls[0]["requested_qty"] == pytest.approx(0.00009193)
    assert suppression_calls[0]["market_price"] == pytest.approx(100_000_000.0)
    assert suppression_calls[0]["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"


def test_run_loop_live_sell_does_not_presuppress_when_canonical_sell_authority_is_executable(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.00049193)

    monkeypatch.setattr(
        "bithumb_bot.engine.compute_signal",
        lambda conn, s, l: {
            "ts": 1000,
            "last_close": 100_000_000.0,
            "curr_s": 1.0,
            "curr_l": 0.5,
            "signal": "SELL",
            "position_state": {
                "normalized_exposure": {
                    "raw_total_asset_qty": 0.00049193,
                    "open_exposure_qty": 0.0004,
                    "dust_tracking_qty": 0.00009193,
                    "sellable_executable_qty": 0.0004,
                    "sellable_executable_lot_count": 1,
                    "exit_allowed": True,
                    "exit_block_reason": "none",
                }
            },
        },
    )

    suppression_calls: list[dict[str, object]] = []
    live_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.record_harmless_dust_exit_suppression",
        lambda **kwargs: suppression_calls.append(kwargs) or True,
    )
    monkeypatch.setattr(
        "bithumb_bot.engine.live_execute_signal",
        lambda *_args, **_kwargs: live_calls.__setitem__("n", live_calls["n"] + 1) or None,
    )
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
        },
    )

    run_loop(5, 20)


def test_residual_sell_candidate_is_modeled_separately_from_strategy_sell_authority() -> None:
    context = {
        "signal": "SELL",
        "sellable_executable_lot_count": 0,
        "residual_inventory_mode": "track",
        "residual_inventory_state": "RESIDUAL_INVENTORY_TRACKED",
        "residual_inventory_policy_allows_sell": True,
        "residual_inventory": {
            "residual_qty": 0.0004998,
            "residual_notional_krw": 6497.4,
            "residual_classes": [
                "DEGRADED_RECOVERY_RESIDUAL",
                "LEDGER_SPLIT_RESIDUAL",
                "NEAR_LOT_RESIDUAL",
                "PORTFOLIO_ANCHOR_RESIDUAL",
                "TRUE_DUST",
            ],
            "exchange_sellable": True,
        },
        "projection_converged": True,
        "accounting_projection_ok": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "locked_qty": 0.0,
        "active_fee_accounting_blocker": False,
        "min_qty": 0.0001,
        "min_notional_krw": 5000.0,
        "idempotency_scope": "live_client_order_id_generator",
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.0004998,
            "balance_source_stale": False,
            "asset_locked": 0.0,
        },
    }

    candidate = build_residual_sell_candidate(context)
    proof = build_residual_sell_presubmit_proof(context)

    assert candidate is not None
    assert candidate.source == "residual_inventory"
    assert candidate.qty == pytest.approx(0.0004998)
    assert candidate.exchange_sellable is True
    assert candidate.allowed_by_policy is True
    assert context["sellable_executable_lot_count"] == 0
    assert proof.passed is True

    decision = build_execution_decision_summary(
        decision_context={
            **context,
            "raw_signal": "SELL",
            "final_signal": "HOLD",
            "has_dust_only_remainder": True,
            "exit_block_reason": "dust_only_remainder",
        },
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )

    assert decision.final_action == "CLOSE_RESIDUAL_CANDIDATE"
    assert decision.submit_expected is False
    assert decision.pre_submit_proof_status == "passed"
    assert decision.block_reason == "residual_live_sell_mode_telemetry"
    assert decision.strategy_sell_candidate is None
    assert decision.residual_sell_candidate is not None
    assert decision.residual_sell_candidate["qty"] == pytest.approx(0.0004998)
    assert decision.residual_submit_plan is not None
    assert decision.residual_submit_plan["side"] == "SELL"
    assert decision.residual_submit_plan["source"] == "residual_inventory"


def test_residual_sell_candidate_is_absent_for_unsellable_tracked_tiny_dust() -> None:
    context = {
        "signal": "SELL",
        "sellable_executable_lot_count": 0,
        "residual_inventory_mode": "track",
        "residual_inventory_state": "RESIDUAL_INVENTORY_TRACKED",
        "residual_inventory_policy_allows_sell": False,
        "residual_inventory": {
            "residual_qty": 0.00009998,
            "residual_notional_krw": 1299.74,
            "residual_classes": ["TRUE_DUST"],
            "exchange_sellable": False,
        },
        "projection_converged": True,
        "accounting_projection_ok": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "locked_qty": 0.0,
        "active_fee_accounting_blocker": False,
        "min_qty": 0.0001,
        "min_notional_krw": 5000.0,
        "idempotency_scope": "live_client_order_id_generator",
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.00009998,
            "balance_source_stale": False,
            "asset_locked": 0.0,
        },
    }

    candidate = build_residual_sell_candidate(context)
    proof = build_residual_sell_presubmit_proof(context)

    assert candidate is None
    assert proof.passed is False
    assert "missing_residual_sell_candidate" in proof.reasons

    decision = build_execution_decision_summary(
        decision_context={
            **context,
            "raw_signal": "SELL",
            "final_signal": "HOLD",
            "has_dust_only_remainder": True,
            "exit_block_reason": "dust_only_remainder",
        },
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )

    assert decision.final_action == "HOLD_TRACKED_DUST"
    assert decision.submit_expected is False
    assert decision.residual_sell_candidate is None
    assert decision.block_reason == "below_min_qty_or_min_notional"


def test_residual_sell_proof_fails_closed_for_submit_unknown() -> None:
    context = {
        "signal": "SELL",
        "residual_inventory_mode": "track",
        "residual_inventory_state": "RESIDUAL_INVENTORY_TRACKED",
        "residual_inventory_policy_allows_sell": True,
        "residual_inventory": {
            "residual_qty": 0.0004998,
            "residual_notional_krw": 57_816.0,
            "residual_classes": ["SELLABLE_RESIDUAL"],
            "exchange_sellable": True,
        },
        "projection_converged": True,
        "accounting_projection_ok": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 1,
        "locked_qty": 0.0,
        "active_fee_accounting_blocker": False,
        "min_qty": 0.0001,
        "min_notional_krw": 5000.0,
        "idempotency_scope": "live_client_order_id_generator",
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.0004998,
            "balance_source_stale": False,
            "asset_locked": 0.0,
        },
    }

    proof = build_residual_sell_presubmit_proof(context)
    decision = build_execution_decision_summary(
        decision_context={**context, "raw_signal": "SELL", "final_signal": "HOLD"},
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )

    assert proof.passed is False
    assert "submit_unknown_count_nonzero" in proof.reasons
    assert decision.final_action == "BLOCK_UNRESOLVED_RESIDUAL"
    assert decision.submit_expected is False
    assert decision.pre_submit_proof_status == "failed"
    assert decision.block_reason == "submit_unknown_count_nonzero"


class _ResidualFakeBroker:
    def __init__(self) -> None:
        self.orders: list[dict[str, object]] = []

    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None, **_kwargs):
        self.orders.append({"client_order_id": client_order_id, "side": side, "qty": qty, "price": price})
        return BrokerOrder(
            client_order_id=client_order_id,
            exchange_order_id="ex-residual-1",
            side=side,
            status="open",
            price=price,
            qty_req=qty,
            qty_filled=0.0,
            created_ts=123,
            updated_ts=123,
            raw={},
        )


def _ec2_residual_context() -> dict[str, object]:
    return {
        "raw_signal": "SELL",
        "final_signal": "HOLD",
        "sellable_executable_lot_count": 0,
        "sellable_executable_qty": 0.0,
        "has_dust_only_remainder": True,
        "exit_block_reason": "dust_only_remainder",
        "residual_inventory_mode": "track",
        "residual_inventory_state": "RESIDUAL_INVENTORY_TRACKED",
        "residual_inventory_policy_allows_run": True,
        "residual_inventory_policy_allows_buy": True,
        "residual_inventory_policy_allows_sell": True,
        "residual_inventory": {
            "residual_qty": 0.0004998,
            "residual_notional_krw": 57_816.0,
            "residual_classes": ["SELLABLE_RESIDUAL"],
            "exchange_sellable": True,
        },
        "projection_converged": True,
        "projection_convergence": {"converged": True, "portfolio_qty": 0.0004998, "projected_total_qty": 0.0004998},
        "accounting_projection_ok": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "locked_qty": 0.0,
        "active_fee_accounting_blocker": False,
        "min_qty": 0.0001,
        "min_notional_krw": 5000.0,
        "idempotency_scope": "live_client_order_id_generator",
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.0004998,
            "balance_source_stale": False,
            "asset_locked": 0.0,
        },
        "total_effective_exposure_notional_krw": 57_816.0,
        "residual_inventory_notional_krw": 57_816.0,
    }


def test_residual_sell_policy_dry_run_builds_plan_without_broker_submit() -> None:
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "dry_run")
    decision = build_execution_decision_summary(
        decision_context=_ec2_residual_context(),
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert decision.pre_submit_proof_status == "passed"
    assert decision.submit_expected is False
    assert decision.block_reason == "residual_live_sell_mode_dry_run"
    assert decision.residual_submit_plan is not None
    assert decision.residual_submit_plan["qty"] == pytest.approx(0.0004998)

    broker = _ResidualFakeBroker()
    service = LiveSignalExecutionService(broker=broker, executor=lambda *_a, **_k: None, harmless_dust_recorder=lambda **_k: False)
    service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_679_000.0,
            decision_context={"execution_decision": decision.as_dict()},
        )
    )
    assert broker.orders == []


def test_residual_sell_policy_enabled_submits_residual_qty_without_strategy_lot_authority() -> None:
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "enabled")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    decision = build_execution_decision_summary(
        decision_context=_ec2_residual_context(),
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert decision.submit_expected is True
    assert decision.strategy_sell_candidate is None
    assert decision.residual_submit_plan is not None
    assert decision.residual_submit_plan["authority"] == "residual_inventory_policy"

    broker = _ResidualFakeBroker()
    service = LiveSignalExecutionService(broker=broker, executor=lambda *_a, **_k: None, harmless_dust_recorder=lambda **_k: False)
    trade = service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_679_000.0,
            decision_context={"execution_decision": decision.as_dict()},
        )
    )
    assert trade is not None
    assert broker.orders[0]["side"] == "SELL"
    assert broker.orders[0]["qty"] == pytest.approx(0.0004998)
    assert trade["source"] == "residual_inventory"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ({"broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0004998, "balance_source_stale": True, "asset_locked": 0.0}}, "broker_evidence_stale"),
        ({"broker_position_evidence": {"broker_qty_known": False, "broker_qty": 0.0004998, "balance_source_stale": False, "asset_locked": 0.0}}, "broker_qty_unknown"),
        ({"broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0001, "balance_source_stale": False, "asset_locked": 0.0}}, "broker_qty_below_candidate_qty"),
        ({"locked_qty": 0.0001}, "locked_qty_nonzero"),
        ({"open_order_count": 1}, "open_order_count_nonzero"),
        ({"unresolved_open_order_count": 1}, "unresolved_open_order_count_nonzero"),
        ({"recovery_required_count": 1}, "recovery_required_count_nonzero"),
        ({"submit_unknown_count": 1}, "submit_unknown_count_nonzero"),
        ({"accounting_projection_ok": False}, "accounting_projection_not_ok"),
        ({"min_qty": 0.001}, "qty_below_min_qty"),
        ({"min_notional_krw": 100_000.0}, "notional_below_min_notional"),
        ({"active_fee_accounting_blocker": True}, "active_fee_accounting_blocker"),
        ({"residual_sell_candidate": {"qty": 0.0004998, "notional": 57_816.0, "source": "residual_inventory", "classes": ["SELLABLE_RESIDUAL"], "exchange_sellable": True, "allowed_by_policy": False, "requires_final_pre_submit_proof": True}}, "candidate_policy_blocked"),
    ],
)
def test_residual_sell_proof_failure_reasons_are_explicit(mutation: dict[str, object], reason: str) -> None:
    context = _ec2_residual_context()
    context.update(mutation)
    proof = build_residual_sell_presubmit_proof(context)
    decision = build_execution_decision_summary(
        decision_context=context,
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert proof.passed is False
    assert reason in proof.reasons
    assert decision.final_action == "BLOCK_UNRESOLVED_RESIDUAL"
    assert decision.submit_expected is False
    assert decision.pre_submit_proof_status == "failed"
    assert decision.block_reason == reason


def test_residual_buy_sizing_modes_telemetry_and_delta() -> None:
    context = _ec2_residual_context() | {
        "raw_signal": "BUY",
        "final_signal": "BUY",
        "market_price": 115_679_000.0,
        "total_effective_exposure_notional_krw": 57_816.0,
        "residual_inventory_notional_krw": 57_816.0,
    }
    object.__setattr__(settings, "MAX_ORDER_KRW", 100_000.0)
    object.__setattr__(settings, "RESIDUAL_BUY_SIZING_MODE", "telemetry")
    telemetry = build_execution_decision_summary(decision_context=context, raw_signal="BUY", final_signal="BUY")
    assert telemetry.buy_delta_krw == pytest.approx(42_184.0)
    assert telemetry.submit_expected is True
    assert telemetry.buy_submit_plan is not None
    assert telemetry.buy_submit_plan["notional_krw"] == pytest.approx(100_000.0)
    assert telemetry.block_reason == "residual_buy_sizing_mode_telemetry"

    object.__setattr__(settings, "RESIDUAL_BUY_SIZING_MODE", "delta")
    delta = build_execution_decision_summary(decision_context=context, raw_signal="BUY", final_signal="BUY")
    assert delta.buy_submit_plan is not None
    assert delta.buy_submit_plan["notional_krw"] == pytest.approx(42_184.0)
    assert delta.submit_expected is True
    assert delta.block_reason == "none"

    covered = build_execution_decision_summary(
        decision_context=context | {"total_effective_exposure_notional_krw": 120_000.0},
        raw_signal="BUY",
        final_signal="BUY",
    )
    assert covered.final_action == "HOLD_TARGET_ALREADY_COVERED"
    assert covered.submit_expected is False
    assert covered.block_reason == "tracked_residual_exposure_covers_target"

    below_min = build_execution_decision_summary(
        decision_context=context | {"total_effective_exposure_notional_krw": 96_000.0},
        raw_signal="BUY",
        final_signal="BUY",
    )
    assert below_min.final_action == "BLOCK_ORDER_RULE"
    assert below_min.submit_expected is False
    assert below_min.block_reason == "buy_delta_below_min_notional"


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


def test_run_loop_kill_switch_liquidate_with_open_position_triggers_flatten(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.05)
    object.__setattr__(settings, "KILL_SWITCH", True)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.engine._get_exposure_snapshot", lambda _now_ms: (False, True))

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_policy_auto_liquidate_positions is True
    assert state.last_flatten_position_status == "dry_run"
    assert state.last_flatten_position_summary is not None
    assert '"trigger": "kill-switch"' in state.last_flatten_position_summary
    assert "flatten_status=dry_run" in str(state.last_disable_reason)


def test_run_loop_kill_switch_liquidate_with_no_position_enters_safe_halt(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.0)
    object.__setattr__(settings, "KILL_SWITCH", True)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_new_orders_blocked is True
    assert state.halt_state_unresolved is False
    assert state.last_flatten_position_status == "no_position"
    assert "flatten_status=no_position" in str(state.last_disable_reason)


def test_run_loop_kill_switch_liquidate_flatten_failure_is_persisted(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.02)
    object.__setattr__(settings, "KILL_SWITCH", True)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.engine._get_exposure_snapshot", lambda _now_ms: (False, True))
    monkeypatch.setattr("bithumb_bot.engine.BithumbBroker", lambda: _FlattenFailBroker())
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_policy_auto_liquidate_positions is True
    assert state.halt_state_unresolved is True
    assert state.last_flatten_position_status == "failed"
    assert state.last_flatten_position_summary is not None
    assert "place_order boom" in state.last_flatten_position_summary
    assert "flatten_status=failed" in str(state.last_disable_reason)

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
    _prepare_run_loop(monkeypatch, asset_qty=0.02)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (True, "daily loss limit exceeded (60,000/50,000 KRW)"),
    )

    cancel_calls = {"n": 0}
    flatten_calls = {"n": 0}

    def _cancel(_broker, trigger: str):
        cancel_calls["n"] += 1
        assert trigger == "daily-loss-halt"
        return True

    monkeypatch.setattr("bithumb_bot.engine._attempt_open_order_cancellation", _cancel)
    monkeypatch.setattr(
        "bithumb_bot.engine.flatten_btc_position",
        lambda *_args, **_kwargs: (
            flatten_calls.__setitem__("n", flatten_calls["n"] + 1)
            or {"status": "dry_run", "qty": 0.02}
        ),
    )
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop(5, 20)

    assert cancel_calls["n"] == 1
    assert flatten_calls["n"] == 1


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


class _CleanupRevalidateBroker:
    def __init__(self, *, open_orders_seq, position_seq):
        self._open_orders_seq = list(open_orders_seq)
        self._position_seq = list(position_seq)
        self.open_order_calls = 0
        self.balance_calls = 0

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ):
        self.open_order_calls += 1
        idx = min(self.open_order_calls - 1, len(self._open_orders_seq) - 1)
        open_present = bool(self._open_orders_seq[idx])
        return [object()] if open_present else []

    def get_balance(self):
        self.balance_calls += 1
        idx = min(self.balance_calls - 1, len(self._position_seq) - 1)
        position_present = bool(self._position_seq[idx])
        return BrokerBalance(
            cash_available=100_000.0,
            cash_locked=0.0,
            asset_available=0.01 if position_present else 0.0,
            asset_locked=0.0,
        )


def test_cleanup_revalidation_recovers_safe_state_after_initial_uncertainty(monkeypatch):
    from bithumb_bot.engine import _revalidate_cleanup_state_after_failure

    broker = _CleanupRevalidateBroker(open_orders_seq=[True, False], position_seq=[True, False])

    reconcile_calls = {"n": 0}

    def _reconcile(_broker):
        reconcile_calls["n"] += 1

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", _reconcile, raising=False)

    safe, detail = _revalidate_cleanup_state_after_failure(
        broker,
        trigger="unit-test",
        max_attempts=2,
    )

    assert safe is True
    assert "attempts=2/2" in detail
    assert reconcile_calls["n"] == 2


def test_cleanup_revalidation_ambiguous_state_remains_halted(monkeypatch):
    from bithumb_bot.engine import _revalidate_cleanup_state_after_failure

    class _AmbiguousBroker:
        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            raise RuntimeError("open orders unavailable")

        def get_balance(self):
            raise RuntimeError("balance unavailable")

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    conn = ensure_db()
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('reval_ambiguous_1','ex-reval-1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    safe, detail = _revalidate_cleanup_state_after_failure(
        _AmbiguousBroker(),
        trigger="unit-test",
        max_attempts=2,
    )

    assert safe is False
    assert "open_orders_present=unknown" in detail
    assert "position_present=unknown" in detail
    assert "errors=" in detail


def test_cleanup_revalidation_is_bounded_by_max_attempts(monkeypatch):
    from bithumb_bot.engine import _revalidate_cleanup_state_after_failure

    broker = _CleanupRevalidateBroker(open_orders_seq=[True], position_seq=[True])
    conn = ensure_db()
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('reval_bounded_1','ex-reval-2','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_calls = {"n": 0}

    def _reconcile(_broker):
        reconcile_calls["n"] += 1

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", _reconcile, raising=False)

    safe, detail = _revalidate_cleanup_state_after_failure(
        broker,
        trigger="unit-test",
        max_attempts=2,
    )

    assert safe is False
    assert "attempts=2/2" in detail
    assert reconcile_calls["n"] == 2
    assert broker.open_order_calls == 2
    assert broker.balance_calls == 2


class _DummyClient:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, path, params=None):
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _response(status_code: int) -> httpx.Response:
    req = httpx.Request("GET", "https://api.bithumb.com/v1/test")
    return httpx.Response(status_code=status_code, request=req, json={"ok": True})


def test_get_with_retry_retries_on_429(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("bithumb_bot.marketdata.time.sleep", lambda sec: sleeps.append(sec))
    monkeypatch.setattr("bithumb_bot.marketdata.random.uniform", lambda a, b: 0.0)

    client = _DummyClient([_response(429), _response(503), _response(200)])
    result = _get_with_retry(client, "/v1/test")

    assert result.status_code == 200
    assert len(sleeps) == 2


def test_health_status_contains_runtime_flags():
    runtime_state.set_error_count(3)
    runtime_state.set_last_candle_observation(
        status="ok",
        age_sec=12.5,
        sync_epoch_sec=1700000000.0,
        candle_ts_ms=1700000000000,
    )
    runtime_state.disable_trading_until(999.0)

    health = get_health_status()

    assert health["error_count"] == 3
    assert health["last_candle_age_sec"] == 12.5
    assert health["last_candle_status"] == "ok"
    assert health["last_candle_sync_epoch_sec"] == 1700000000.0
    assert health["last_candle_ts_ms"] == 1700000000000
    assert health["last_candle_status_detail"] is None
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
    runtime_state.set_last_candle_observation(
        status="waiting_first_sync",
        age_sec=None,
        sync_epoch_sec=None,
        candle_ts_ms=None,
        detail="test cleanup",
    )
    runtime_state.set_startup_gate_reason(None)


def test_run_loop_position_loss_breach_triggers_halt(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.03)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )

    flatten_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.engine.flatten_btc_position",
        lambda *_args, **_kwargs: (
            flatten_calls.__setitem__("n", flatten_calls["n"] + 1)
            or {"status": "dry_run", "qty": 0.03}
        ),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "POSITION_LOSS_LIMIT"
    assert state.last_disable_reason is not None
    assert "position loss threshold breached" in state.last_disable_reason
    assert "flatten_status=dry_run" in state.last_disable_reason
    assert flatten_calls["n"] == 1


def test_run_loop_position_loss_breach_uses_executable_exposure_qty(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.00009629)
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_classification": "harmless_dust",
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": (
                "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
            "dust_broker_qty": 0.00009629,
            "dust_local_qty": 0.00009629,
            "dust_effective_flat": 1,
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
        },
    )

    captured_qty: list[float] = []

    def _capture_position_loss_breach(_conn, *, qty: float, price: float):
        captured_qty.append(qty)
        return False, "ok"

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.engine.evaluate_position_loss_breach", _capture_position_loss_breach)
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop(5, 20)

    assert captured_qty
    assert captured_qty[0] == 0.0


def test_run_loop_daily_loss_breach_with_no_position_records_no_position_flatten(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.0)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (True, "daily loss limit exceeded (60,000/50,000 KRW)"),
    )
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "DAILY_LOSS_LIMIT"
    assert state.halt_state_unresolved is False
    assert "flatten_status=no_position" in str(state.last_disable_reason)


def test_run_loop_position_loss_breach_flatten_failure_marks_unresolved(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.03)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.engine.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )
    monkeypatch.setattr("bithumb_bot.engine.live_execute_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.engine.BithumbBroker", lambda: _FlattenFailBroker())
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    run_loop(5, 20)

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "POSITION_LOSS_LIMIT"
    assert state.halt_state_unresolved is True
    assert "flatten_status=failed" in str(state.last_disable_reason)


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
