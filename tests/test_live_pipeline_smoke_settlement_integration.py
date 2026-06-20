from __future__ import annotations

from bithumb_bot.db_core import (
    ensure_db,
    record_fee_pending_accounting_repair,
    record_position_authority_repair,
)
from bithumb_bot.live_pipeline_smoke import LivePipelineSmokeExecutionService, run_live_pipeline_smoke
from bithumb_bot.live_pipeline_smoke_authority import LIVE_PIPELINE_SMOKE_CONFIRMATION_TOKEN
from tests.test_live_pipeline_smoke_runner_fake_broker import (
    _Broker,
    _authority,
    _insert_top_of_book,
    _patch_settings,
    _restore_settings,
    _readiness_from_broker,
)


def _noop_reconcile():
    return None


def _run_smoke(monkeypatch, tmp_path, *, readiness_provider=None, service=None):
    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    conn = ensure_db(str(db_path))
    _insert_top_of_book(conn)
    broker = _Broker()
    authority = _authority(tmp_path, db_path, max_notional_krw=20_000.0)
    monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
    monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)
    service = service or LivePipelineSmokeExecutionService(broker=broker)
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
        readiness_provider=readiness_provider or (lambda: _readiness_from_broker(broker)),
        post_trade_reconcile=_noop_reconcile,
        run_id="lps_settlement_test",
    )
    return payload, conn, broker, service, old


def test_live_pipeline_smoke_passes_only_when_all_10_orders_settle_without_repair(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        assert payload["status"] == "passed"
        assert payload["orders_submitted"] == 10
        assert payload["buy_submitted"] == 5
        assert payload["sell_submitted"] == 5
        assert payload["repair_events_created_during_run"] == 0
        assert payload["final"]["broker_qty"] == 0.0
        assert payload["final"]["portfolio_qty"] == 0.0
        assert payload["final"]["projected_total_qty"] == 0.0
        assert len(service.submissions) == 10
        assert all(
            step["settlement_result"]["settled"] is True
            for round_item in payload["rounds"]
            for step in (round_item["buy"], round_item["sell"])
        )
        assert conn.execute("SELECT COUNT(*) FROM manual_flat_accounting_repairs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM fee_pending_accounting_repairs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM position_authority_repairs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM external_position_adjustments").fetchone()[0] == 0
    finally:
        conn.close()
        _restore_settings(old)


def test_live_pipeline_smoke_fails_if_fee_pending_requires_manual_repair(monkeypatch, tmp_path) -> None:
    class _RepairingService(LivePipelineSmokeExecutionService):
        def execute(self, request):
            result = super().execute(request)
            if len(self.submissions) == 1:
                record_fee_pending_accounting_repair(
                    request.execution_decision_summary.target_submit_plan.extra_payload["conn"]
                    if False
                    else conn,
                    event_ts=1,
                    client_order_id="repair_fee",
                    exchange_order_id="ex_repair_fee",
                    fill_id="fill_repair_fee",
                    fill_ts=1,
                    price=1.0,
                    qty=1.0,
                    fee=1.0,
                    source="unit",
                    reason="fee_pending_accounting_repair",
                    repair_basis={"unit": True},
                )
            return result

    db_path = tmp_path / "live.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    conn = ensure_db(str(db_path))
    try:
        _insert_top_of_book(conn)
        broker = _Broker()
        service = _RepairingService(broker=broker)
        authority = _authority(tmp_path, db_path, max_notional_krw=20_000.0)
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)
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
            readiness_provider=lambda: _readiness_from_broker(broker),
            post_trade_reconcile=_noop_reconcile,
            run_id="lps_fee_repair_test",
        )
        assert payload["status"] == "failed"
        assert payload["reason"] == "live_pipeline_smoke_final_completion_criteria_failed"
    finally:
        conn.close()
        _restore_settings(old)


def test_live_pipeline_smoke_fails_if_projection_repair_event_created(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        assert payload["status"] == "passed"
    finally:
        conn.close()
        _restore_settings(old)

    db_path = tmp_path / "live_repair.sqlite"
    old = _patch_settings(monkeypatch, db_path)
    conn = ensure_db(str(db_path))
    try:
        _insert_top_of_book(conn)
        broker = _Broker()
        authority = _authority(tmp_path, db_path, max_notional_krw=20_000.0)
        service = LivePipelineSmokeExecutionService(broker=broker)
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.runtime_code_provenance", lambda: {"commit_sha": "unavailable"})
        monkeypatch.setattr("bithumb_bot.live_pipeline_smoke.validate_live_pipeline_smoke_start_preflight", lambda **_kwargs: None)

        def _reconcile():
            if len(service.submissions) == 1:
                record_position_authority_repair(
                    conn,
                    event_ts=1,
                    source="unit",
                    reason="historical_fragmentation_projection_drift_repair",
                    repair_basis={"unit": True},
                )

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
            readiness_provider=lambda: _readiness_from_broker(broker),
            post_trade_reconcile=_reconcile,
            run_id="lps_position_repair_test",
        )
        assert payload["status"] == "failed"
        assert payload["reason"] == "live_pipeline_smoke_final_completion_criteria_failed"
    finally:
        conn.close()
        _restore_settings(old)


def test_live_pipeline_smoke_records_settlement_evidence_for_each_step(monkeypatch, tmp_path) -> None:
    payload, conn, _broker, _service, old = _run_smoke(monkeypatch, tmp_path)
    try:
        assert payload["status"] == "passed"
        steps = [step for round_item in payload["rounds"] for step in (round_item["buy"], round_item["sell"])]
        assert len(steps) == 10
        assert all("settlement_result" in step for step in steps)
        assert all("attempts" in step["settlement_result"]["evidence"] for step in steps)
    finally:
        conn.close()
        _restore_settings(old)


def test_smoke_result_failed_when_fee_pending_repair_event_created(monkeypatch, tmp_path) -> None:
    test_live_pipeline_smoke_fails_if_fee_pending_requires_manual_repair(monkeypatch, tmp_path)


def test_smoke_result_failed_when_position_authority_repair_event_created(monkeypatch, tmp_path) -> None:
    test_live_pipeline_smoke_fails_if_projection_repair_event_created(monkeypatch, tmp_path)


def test_smoke_result_passed_when_no_repair_events_and_all_10_orders_settle(monkeypatch, tmp_path) -> None:
    test_live_pipeline_smoke_passes_only_when_all_10_orders_settle_without_repair(monkeypatch, tmp_path)
