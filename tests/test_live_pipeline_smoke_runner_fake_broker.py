from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from bithumb_bot.broker import live as live_broker
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.execution_service import LiveSignalExecutionService
from bithumb_bot.live_pipeline_smoke import (
    LivePipelineSmokeError,
    LivePipelineSmokeExecutionService,
    _validate_smoke_roundtrip_notional_buffer,
    _readiness_from_broker,
    run_live_pipeline_smoke,
    validate_live_pipeline_smoke_request,
)
from bithumb_bot.live_pipeline_smoke_preflight import LivePipelineSmokePreflightError
from bithumb_bot.live_pipeline_smoke_preflight import LivePipelineSmokeReadiness, validate_live_pipeline_smoke_step_readiness
from bithumb_bot.execution_order_rules import ExecutionOrderRules
from bithumb_bot.live_pipeline_smoke_authority import (
    LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
    build_live_pipeline_smoke_authority_payload,
)
from bithumb_bot.order_settlement import evaluate_settlement_snapshot
from bithumb_bot.storage_io import write_json_atomic


class _Broker:
    qty = 0.0

    def apply_fill(self, *, side: str, qty: float) -> None:
        if side == "BUY":
            self.qty += qty
        else:
            self.qty = max(0.0, self.qty - qty)

    def get_open_orders(self):
        return []


def _record_reconcile_attempt(attempts: list[str]) -> None:
    attempts.append("reconcile")


def _settlement_from_readiness(readiness_provider, reconcile):
    def _settle(trade):
        reconcile()
        readiness = readiness_provider()
        filled_qty = float(trade.get("filled_qty") or trade.get("submit_qty") or 0.0)
        evidence = {
            "order_state": "FILLED",
            "order_terminal": True,
            "fill_count": 1 if filled_qty > 0.0 else 0,
            "fill_set_complete": filled_qty > 0.0,
            "paid_fee_present": True,
            "order_level_paid_fee_present": True,
            "complete_fill_set_available": filled_qty > 0.0,
            "fee_state": "finalized",
            "principal_applied": filled_qty > 0.0,
            "accounting_finalized": True,
            "projection_applied": bool(readiness.projection_converged),
            "projected_total_qty": float(readiness.projected_total_qty),
            "portfolio_qty": float(readiness.portfolio_qty),
            "broker_qty": float(readiness.broker_qty),
            "broker_local_converged": bool(readiness.converged),
            "reason_code": "settlement_evidence_complete",
        }
        return evaluate_settlement_snapshot(
            client_order_id=str(trade.get("client_order_id") or ""),
            exchange_order_id=str(trade.get("exchange_order_id") or "") or None,
            evidence=evidence,
            attempts=[evidence],
        )

    return _settle


def _patch_settings(monkeypatch, db_path):
    old = {}
    for name, value in {
        "MODE": "live",
        "LIVE_DRY_RUN": False,
        "LIVE_REAL_ORDER_ARMED": True,
        "KILL_SWITCH": False,
        "EXECUTION_ENGINE": "target_delta",
        "PAIR": "KRW-BTC",
        "INTERVAL": "1m",
        "DB_PATH": str(db_path),
        "BITHUMB_API_KEY": "account",
        "LIVE_MIN_ORDER_QTY": 0.00000001,
        "LIVE_ORDER_QTY_STEP": 0.00000001,
        "LIVE_ORDER_MAX_QTY_DECIMALS": 8,
        "MIN_ORDER_NOTIONAL_KRW": 5_000.0,
    }.items():
        old[name] = getattr(settings, name)
        object.__setattr__(settings, name, value)
    return old


def _restore_settings(old):
    for name, value in old.items():
        object.__setattr__(settings, name, value)


def _authority(tmp_path, db_path, *, max_notional_krw: float = 20_000.0):
    path = tmp_path / "authority.json"
    payload = build_live_pipeline_smoke_authority_payload(
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        market="KRW-BTC",
        db_path=str(db_path),
        account_key="account",
        code_commit_sha="unavailable",
        max_notional_krw=max_notional_krw,
    )
    write_json_atomic(path, payload)
    return path


def _insert_top_of_book(conn) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO orderbook_top_snapshots(
            ts, pair, bid_price, ask_price, spread_bps, source, observed_at_epoch_sec
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1_800_000_000_000, "KRW-BTC", 99_900_000.0, 100_100_000.0, 20.0, "unit", 1_800_000_000.0),
    )
    conn.commit()


def test_fake_broker_executes_five_round_trips(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    try:
        conn = ensure_db(str(db_path))
        _insert_top_of_book(conn)
        broker = _Broker()
        authority = _authority(tmp_path, db_path)
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)

        service = LivePipelineSmokeExecutionService(broker=broker)
        reconcile_attempts: list[str] = []
        readiness_provider = lambda: _readiness_from_broker(broker)
        payload = run_live_pipeline_smoke(
            conn=conn,
            broker=broker,
            cycles=5,
            max_orders=10,
            max_notional_krw=20_000.0,
            yes=True,
            authority_path=str(authority),
            confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
            execution_service=service,
            readiness_provider=readiness_provider,
            post_trade_reconcile=lambda: _record_reconcile_attempt(reconcile_attempts),
            settlement_coordinator=_settlement_from_readiness(
                readiness_provider,
                lambda: _record_reconcile_attempt(reconcile_attempts),
            ),
            run_id="lps_test",
        )

        assert payload["status"] == "passed"
        assert payload["orders_submitted"] == 10
        assert payload["buy_submitted"] == 5
        assert payload["sell_submitted"] == 5
        assert len(payload["rounds"]) == 5
        assert payload["final"]["broker_qty"] == 0.0
        assert conn.execute("SELECT COUNT(*) FROM strategy_decisions WHERE strategy_name='operator_live_pipeline_smoke'").fetchone()[0] == 10
        assert len(service.submissions) == 10
        assert reconcile_attempts == ["reconcile"] * 10
    finally:
        _restore_settings(old)


def test_real_live_service_executes_five_round_trips_with_fake_executor(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    try:
        conn = ensure_db(str(db_path))
        _insert_top_of_book(conn)
        broker = _Broker()
        authority = _authority(tmp_path, db_path)
        calls: list[dict[str, object]] = []

        def _executor(_broker, signal, ts, market_price, **kwargs):
            plan = dict(kwargs["execution_submit_plan"])
            side = str(signal).upper()
            qty = float(plan["qty"])
            broker.apply_fill(side=side, qty=qty)
            calls.append(
                {
                    "signal": side,
                    "ts": ts,
                    "market_price": market_price,
                    "plan": plan,
                }
            )
            return {
                "status": "submitted",
                "client_order_id": f"lps_live_service_{len(calls)}",
                "exchange_order_id": f"ex_lps_live_service_{len(calls)}",
                "side": side,
                "filled_qty": qty,
            }

        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)
        service = LiveSignalExecutionService(
            broker=broker,
            executor=_executor,
            harmless_dust_recorder=lambda **_kwargs: False,
        )
        reconcile_attempts: list[str] = []
        readiness_provider = lambda: _readiness_from_broker(broker)

        payload = run_live_pipeline_smoke(
            conn=conn,
            broker=broker,
            cycles=5,
            max_orders=10,
            max_notional_krw=20_000.0,
            yes=True,
            authority_path=str(authority),
            confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
            execution_service=service,
            readiness_provider=readiness_provider,
            post_trade_reconcile=lambda: _record_reconcile_attempt(reconcile_attempts),
            settlement_coordinator=_settlement_from_readiness(
                readiness_provider,
                lambda: _record_reconcile_attempt(reconcile_attempts),
            ),
            run_id="lps_real_service_test",
        )

        assert payload["status"] == "passed"
        assert payload["orders_submitted"] == 10
        assert payload["buy_submitted"] == 5
        assert payload["sell_submitted"] == 5
        assert payload["final"]["broker_qty"] == 0.0
        assert payload["final"]["portfolio_qty"] == 0.0
        assert payload["final"]["projected_total_qty"] == 0.0
        assert len(calls) == 10
        assert reconcile_attempts == ["reconcile"] * 10
        assert [call["signal"] for call in calls].count("BUY") == 5
        assert [call["signal"] for call in calls].count("SELL") == 5
        assert payload["execution_mode_metadata"]["market_reference_source"] == "orderbook_top_mid"
        buy_plans = [call["plan"] for call in calls if call["signal"] == "BUY"]
        assert all(plan["strategy_performance_gate_blocked"] is True for plan in buy_plans)
        assert all(plan["operator_live_pipeline_smoke"] is True for plan in buy_plans)
        assert all(plan["market_reference_source"] == "orderbook_top_mid" for plan in buy_plans)
        assert all(
            live_broker._is_operator_live_pipeline_smoke_submit(
                plan,
                strategy_name="operator_live_pipeline_smoke",
            )
            for plan in buy_plans
        )
    finally:
        _restore_settings(old)


def test_failure_after_step_prevents_next_step(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    try:
        conn = ensure_db(str(db_path))
        _insert_top_of_book(conn)
        broker = _Broker()
        authority = _authority(tmp_path, db_path)
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)

        service = LivePipelineSmokeExecutionService(broker=broker, fail_at_step=1)
        reconcile_attempts: list[str] = []
        readiness_provider = lambda: _readiness_from_broker(broker)
        payload = run_live_pipeline_smoke(
            conn=conn,
            broker=broker,
            cycles=5,
            max_orders=10,
            max_notional_krw=20_000.0,
            yes=True,
            authority_path=str(authority),
            confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
            execution_service=service,
            readiness_provider=readiness_provider,
            post_trade_reconcile=lambda: _record_reconcile_attempt(reconcile_attempts),
            settlement_coordinator=_settlement_from_readiness(
                readiness_provider,
                lambda: _record_reconcile_attempt(reconcile_attempts),
            ),
            run_id="lps_test",
        )

        assert payload["status"] == "failed"
        assert payload["orders_submitted"] == 1
        assert len(service.submissions) == 1
        assert reconcile_attempts == ["reconcile"]
    finally:
        _restore_settings(old)


def test_execute_none_does_not_increment_orders_submitted(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    try:
        conn = ensure_db(str(db_path))
        _insert_top_of_book(conn)
        broker = _Broker()
        authority = _authority(tmp_path, db_path)
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)

        class _NoneService:
            def execute(self, _request):
                return None

        reconcile_attempts: list[str] = []
        payload = run_live_pipeline_smoke(
            conn=conn,
            broker=broker,
            cycles=5,
            max_orders=10,
            max_notional_krw=20_000.0,
            yes=True,
            authority_path=str(authority),
            confirm=LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
            execution_service=_NoneService(),
            readiness_provider=lambda: _readiness_from_broker(broker),
            post_trade_reconcile=lambda: _record_reconcile_attempt(reconcile_attempts),
            run_id="lps_test",
        )

        assert payload["status"] == "failed"
        assert payload["orders_submitted"] == 0
        assert broker.qty == 0.0
        assert reconcile_attempts == []
    finally:
        _restore_settings(old)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"apply": True, "yes": False, "authority_path": "/tmp/a", "confirm": LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN},
        {"apply": True, "yes": True, "authority_path": None, "confirm": LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN},
        {"apply": True, "yes": True, "authority_path": "/tmp/a", "confirm": "wrong"},
        {"apply": True, "yes": True, "authority_path": "/tmp/a", "confirm": LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN, "cycles": 4},
        {"apply": True, "yes": True, "authority_path": "/tmp/a", "confirm": LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN, "max_orders": 9},
    ],
)
def test_apply_regression_bounds_rejected(kwargs) -> None:
    base = {
        "apply": True,
        "yes": True,
        "cycles": 5,
        "max_orders": 10,
        "max_notional_krw": 10_000.0,
        "authority_path": "/tmp/a",
        "confirm": LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN,
        "mode": "live",
    }
    base.update(kwargs)
    with pytest.raises(LivePipelineSmokeError):
        validate_live_pipeline_smoke_request(**base)


def test_fake_broker_roundtrip_min_qty_rejects_low_notional() -> None:
    rules = ExecutionOrderRules(
        market="KRW-BTC",
        min_qty=0.0001,
        qty_step=0.00000001,
        min_notional_krw=5_000.0,
        source="unit",
    )
    with pytest.raises(
        LivePipelineSmokePreflightError,
        match="live_pipeline_smoke_max_notional_below_sellable_roundtrip_minimum",
    ):
        _validate_smoke_roundtrip_notional_buffer(
            rules=rules,
            reference_price=96_933_000,
            max_notional_krw=10_000,
        )


def test_fake_broker_fee_pending_allows_authorized_sell_closeout_readiness() -> None:
    readiness = LivePipelineSmokeReadiness(
        broker_qty=0.0002,
        portfolio_qty=0.0002,
        projected_total_qty=0.0002,
        open_order_count=0,
        submit_unknown_count=0,
        recovery_required_count=0,
        fee_pending_count=1,
        active_fee_accounting_blocker=True,
        broker_qty_known=True,
        balance_source_stale=False,
        projection_converged=True,
    )

    validate_live_pipeline_smoke_step_readiness(
        readiness,
        expected_side="SELL",
        requested_qty=0.0002,
        terminal_flat_authority=True,
    )
    with pytest.raises(LivePipelineSmokePreflightError):
        validate_live_pipeline_smoke_step_readiness(readiness, expected_side="BUY")
