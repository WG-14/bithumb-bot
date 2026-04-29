from __future__ import annotations

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.app import _ledger_replay, _load_recovery_report, main as app_main
from bithumb_bot.config import settings
from bithumb_bot.db_core import (
    ACCOUNTING_PROJECTION_MODEL,
    AUTHORITATIVE_ACCOUNTING_EVENT_FAMILIES,
    DIAGNOSTIC_ACCOUNTING_EVENT_FAMILIES,
    compute_accounting_replay,
    ensure_db,
    init_portfolio,
    record_broker_fill_observation,
    record_external_cash_adjustment,
    summarize_fill_accounting_incident_projection,
)
from bithumb_bot.execution import (
    apply_fill_and_trade,
    apply_fill_principal_with_pending_fee,
    record_order_if_missing,
)
from bithumb_bot.fee_pending_repair import (
    apply_fee_pending_accounting_repair,
    build_fee_pending_accounting_repair_preview,
)
from bithumb_bot.repair_plan import build_recovery_policy_from_report, build_repair_plan_preview_from_report
from bithumb_bot.reporting import fetch_cash_drift_report
from bithumb_bot.runtime_readiness import compute_runtime_readiness_snapshot


@pytest.fixture
def projection_db(tmp_path, monkeypatch):
    original_db_path = settings.DB_PATH
    original_mode = settings.MODE
    original_start_cash = settings.START_CASH_KRW
    original_fee_threshold = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    db_path = tmp_path / "accounting_projection.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("MODE", "paper")
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    yield db_path

    object.__setattr__(settings, "DB_PATH", original_db_path)
    object.__setattr__(settings, "MODE", original_mode)
    object.__setattr__(settings, "START_CASH_KRW", original_start_cash)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", original_fee_threshold)


def _seed_filled_roundtrip_with_later_cash_adjustment(db_path):
    conn = ensure_db(str(db_path))
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="projection_buy",
            side="BUY",
            qty_req=0.001,
            price=100_000_000.0,
            ts_ms=1_700_000_000_000,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="projection_buy",
            side="BUY",
            fill_id="projection_buy_fill",
            fill_ts=1_700_000_000_100,
            price=100_000_000.0,
            qty=0.001,
            fee=50.0,
            note="projection fixture buy",
        )
        record_order_if_missing(
            conn,
            client_order_id="projection_sell",
            side="SELL",
            qty_req=0.001,
            price=110_000_000.0,
            ts_ms=1_700_000_000_200,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="projection_sell",
            side="SELL",
            fill_id="projection_sell_fill",
            fill_ts=1_700_000_000_300,
            price=110_000_000.0,
            qty=0.001,
            fee=60.0,
            note="projection fixture sell",
        )
        latest_trade = conn.execute(
            "SELECT id, cash_after, asset_after FROM trades ORDER BY id DESC LIMIT 1"
        ).fetchone()
        record_external_cash_adjustment(
            conn,
            event_ts=1_700_000_000_400,
            currency="KRW",
            delta_amount=77.0,
            source="operator_adjustment",
            reason="reconcile_cash_drift",
            broker_snapshot_basis={
                "balance_source": "test",
                "broker_cash_total": 1_009_967.0,
                "local_cash_total": 1_009_890.0,
            },
            adjustment_key="projection:cash:77",
        )
        conn.commit()
        return {
            "latest_trade_id": int(latest_trade["id"]),
            "latest_trade_cash_after": float(latest_trade["cash_after"]),
            "latest_trade_asset_after": float(latest_trade["asset_after"]),
        }
    finally:
        conn.close()


def test_audit_downgrades_stale_latest_trade_after_state_when_projection_matches(
    projection_db, capsys
):
    seeded = _seed_filled_roundtrip_with_later_cash_adjustment(projection_db)

    conn = ensure_db(str(projection_db))
    try:
        portfolio = conn.execute(
            "SELECT cash_krw, asset_qty FROM portfolio WHERE id=1"
        ).fetchone()
        replay = compute_accounting_replay(conn)
    finally:
        conn.close()

    assert seeded["latest_trade_cash_after"] != pytest.approx(float(portfolio["cash_krw"]))
    assert seeded["latest_trade_asset_after"] == pytest.approx(float(portfolio["asset_qty"]))
    assert float(replay["replay_cash"]) == pytest.approx(float(portfolio["cash_krw"]))
    assert float(replay["replay_qty"]) == pytest.approx(float(portfolio["asset_qty"]))

    app_main(["audit"])
    out = capsys.readouterr().out

    assert "[AUDIT] WARN stale execution snapshot:" in out
    assert f"trade_id={seeded['latest_trade_id']}" in out
    assert "post_trade_accounting_event_count=1" in out
    assert f"model={ACCOUNTING_PROJECTION_MODEL}" in out
    assert "[AUDIT] OK" in out


def test_audit_ledger_and_cash_drift_report_share_authoritative_projection_metadata(
    projection_db, monkeypatch, capsys
):
    _seed_filled_roundtrip_with_later_cash_adjustment(projection_db)
    monkeypatch.setattr(
        "bithumb_bot.reporting._broker_cash_snapshot",
        lambda: {"cash_krw": 1_009_967.0, "source": "test"},
    )

    app_main(["audit-ledger"])
    audit_ledger_out = capsys.readouterr().out

    conn = ensure_db(str(projection_db))
    try:
        report = fetch_cash_drift_report(conn, recent_limit=5)
        replay_a = compute_accounting_replay(conn)
        replay_b = compute_accounting_replay(conn)
    finally:
        conn.close()

    expected_included = ",".join(AUTHORITATIVE_ACCOUNTING_EVENT_FAMILIES)
    expected_diagnostic = ",".join(DIAGNOSTIC_ACCOUNTING_EVENT_FAMILIES)
    assert f"projection_model={ACCOUNTING_PROJECTION_MODEL}" in audit_ledger_out
    assert f"included_event_families={expected_included}" in audit_ledger_out
    assert f"diagnostic_event_families={expected_diagnostic}" in audit_ledger_out
    assert report["authoritative_projection"]["projection_model"] == ACCOUNTING_PROJECTION_MODEL
    assert tuple(report["authoritative_projection"]["included_event_families"]) == AUTHORITATIVE_ACCOUNTING_EVENT_FAMILIES
    assert tuple(report["authoritative_projection"]["diagnostic_event_families"]) == DIAGNOSTIC_ACCOUNTING_EVENT_FAMILIES
    assert report["local"]["consistent"] is True
    assert replay_a == replay_b


def test_external_adjustment_idempotency_keeps_projection_deterministic(projection_db):
    _seed_filled_roundtrip_with_later_cash_adjustment(projection_db)
    conn = ensure_db(str(projection_db))
    try:
        before = compute_accounting_replay(conn)
        second = record_external_cash_adjustment(
            conn,
            event_ts=1_700_000_000_999,
            currency="KRW",
            delta_amount=77.0,
            source="operator_adjustment",
            reason="reconcile_cash_drift",
            broker_snapshot_basis={"balance_source": "test"},
            adjustment_key="projection:cash:77",
        )
        after = compute_accounting_replay(conn)
    finally:
        conn.close()

    assert second is not None and second["created"] is False
    assert after == before
    assert after["external_cash_adjustment_count"] == 1
    assert after["external_cash_adjustment_total"] == pytest.approx(77.0)


def test_audit_allows_sell_snapshot_with_residual_holdings_when_projection_converges(
    projection_db,
    capsys,
):
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="residual_audit_buy",
            side="BUY",
            qty_req=0.0008998,
            price=100_000_000.0,
            ts_ms=1_700_000_100_000,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="residual_audit_buy",
            side="BUY",
            fill_id="residual_audit_buy_fill",
            fill_ts=1_700_000_100_100,
            price=100_000_000.0,
            qty=0.0008998,
            fee=50.0,
        )
        record_order_if_missing(
            conn,
            client_order_id="residual_audit_sell",
            side="SELL",
            qty_req=0.0004,
            price=110_000_000.0,
            ts_ms=1_700_000_100_200,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="residual_audit_sell",
            side="SELL",
            fill_id="residual_audit_sell_fill",
            fill_ts=1_700_000_100_300,
            price=110_000_000.0,
            qty=0.0004,
            fee=60.0,
        )
        latest_sell = conn.execute(
            """
            SELECT qty, cash_after, asset_after
            FROM trades
            WHERE client_order_id='residual_audit_sell'
            """
        ).fetchone()
        replay = compute_accounting_replay(conn)
    finally:
        conn.close()

    assert latest_sell is not None
    assert float(latest_sell["asset_after"]) > float(latest_sell["qty"])
    assert float(replay["replay_qty"]) == pytest.approx(float(latest_sell["asset_after"]))

    app_main(["audit"])
    out = capsys.readouterr().out

    assert "[AUDIT] FAILED" not in out
    assert "[AUDIT] OK" in out


def test_audit_still_fails_negative_sell_snapshot_after_relaxing_residual_rule(
    projection_db,
    capsys,
):
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="negative_sell_buy",
            side="BUY",
            qty_req=0.001,
            price=100_000_000.0,
            ts_ms=1_700_000_200_000,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="negative_sell_buy",
            side="BUY",
            fill_id="negative_sell_buy_fill",
            fill_ts=1_700_000_200_100,
            price=100_000_000.0,
            qty=0.001,
            fee=50.0,
        )
        record_order_if_missing(
            conn,
            client_order_id="negative_sell_sell",
            side="SELL",
            qty_req=0.001,
            price=110_000_000.0,
            ts_ms=1_700_000_200_200,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="negative_sell_sell",
            side="SELL",
            fill_id="negative_sell_sell_fill",
            fill_ts=1_700_000_200_300,
            price=110_000_000.0,
            qty=0.001,
            fee=60.0,
        )
        conn.execute(
            "UPDATE trades SET asset_after=? WHERE client_order_id='negative_sell_sell'",
            (-0.0001,),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(SystemExit):
        app_main(["audit"])
    out = capsys.readouterr().out

    assert "[AUDIT] FAILED" in out
    assert "SELL snapshot impossible" in out


def test_fee_observation_lifecycle_is_visible_without_becoming_projection_authority(
    projection_db,
):
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        record_broker_fill_observation(
            conn,
            event_ts=1_700_000_000_100,
            client_order_id="fee_lifecycle",
            exchange_order_id="ex_fee_lifecycle",
            fill_id="fee_fill_1",
            fill_ts=1_700_000_000_090,
            side="BUY",
            price=100_000_000.0,
            qty=0.001,
            fee=None,
            fee_status="missing",
            accounting_status="fee_pending",
            source="test_missing_fee",
            parse_warnings="missing_fee",
            raw_payload={"uuid": "fee_fill_1"},
        )
        pending = compute_accounting_replay(conn)
        record_broker_fill_observation(
            conn,
            event_ts=1_700_000_000_200,
            client_order_id="fee_lifecycle",
            exchange_order_id="ex_fee_lifecycle",
            fill_id="fee_fill_1",
            fill_ts=1_700_000_000_090,
            side="BUY",
            price=100_000_000.0,
            qty=0.001,
            fee=50.0,
            fee_status="operator_confirmed",
            accounting_status="accounting_complete",
            source="fee_pending_accounting_repair",
            parse_warnings="operator_fee_provenance=test",
            raw_payload={"uuid": "fee_fill_1", "repair_key": "test"},
        )
        completed = compute_accounting_replay(conn)
    finally:
        conn.close()

    assert pending["projection_model"] == ACCOUNTING_PROJECTION_MODEL
    assert "broker_fill_observations" in pending["diagnostic_event_families"]
    assert pending["unresolved_fee_state"] is True
    assert pending["broker_fill_latest_unresolved_fee_pending_count"] == 1
    assert completed["unresolved_fee_state"] is False
    assert completed["broker_fill_latest_unresolved_fee_pending_count"] == 0
    assert completed["broker_fill_latest_accounting_complete_count"] == 1
    assert completed["replay_cash"] == pytest.approx(pending["replay_cash"])
    assert completed["replay_qty"] == pytest.approx(pending["replay_qty"])


def _record_fee_pending_order_and_observation(
    conn,
    *,
    client_order_id: str = "canonical_fee_pending",
    fill_id: str = "canonical-fill-1",
    event_ts: int = 1_700_000_010_100,
) -> None:
    record_order_if_missing(
        conn,
        client_order_id=client_order_id,
        side="BUY",
        qty_req=0.001,
        price=100_000_000.0,
        ts_ms=event_ts - 100,
        status="NEW",
    )
    record_broker_fill_observation(
        conn,
        event_ts=event_ts,
        client_order_id=client_order_id,
        exchange_order_id=f"ex-{client_order_id}",
        fill_id=fill_id,
        fill_ts=event_ts - 10,
        side="BUY",
        price=100_000_000.0,
        qty=0.001,
        fee=26.86,
        fee_status="order_level_candidate",
        accounting_status="fee_pending",
        source="test_fee_pending_incident",
        parse_warnings="order_level_fee_candidate",
        raw_payload={"fixture": "canonical_fee_pending"},
    )


def _incident_summary(conn) -> dict[str, object]:
    return summarize_fill_accounting_incident_projection(conn)


def test_fee_pending_observation_without_fill_remains_active_incident(projection_db):
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        _record_fee_pending_order_and_observation(conn)
        conn.commit()
        replay = compute_accounting_replay(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        preview = build_fee_pending_accounting_repair_preview(
            conn,
            client_order_id="canonical_fee_pending",
            fill_id="canonical-fill-1",
            fee=26.86,
            fee_provenance="operator_checked_bithumb_trade_history",
        )
        summary = _incident_summary(conn)
    finally:
        conn.close()

    verdict = summary["verdicts"][0]
    assert verdict["canonical_incident_state"] == "unapplied_principal_pending"
    assert verdict["incident_scope"] == "active_blocking"
    assert summary["active_issue_count"] == 1
    assert replay["unresolved_fee_state"] is True
    assert replay["broker_fill_latest_unresolved_fee_pending_count"] == 1
    assert readiness.fee_pending_count == 1
    assert readiness.recovery_stage == "UNAPPLIED_PRINCIPAL_PENDING"
    assert preview["needs_repair"] is True
    assert preview["safe_to_apply"] is True
    assert "fee_authority" in preview
    assert preview["fee_authority"]


def test_already_accounted_fill_reclassifies_stale_fee_pending_observation(projection_db, capsys):
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        _record_fee_pending_order_and_observation(
            conn,
            client_order_id="already_accounted",
            fill_id="already-accounted-fill",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="already_accounted",
            side="BUY",
            fill_id="already-accounted-fill",
            fill_ts=1_700_000_010_090,
            price=100_000_000.0,
            qty=0.001,
            fee=26.86,
            note="authoritative fill already contains final fee",
        )
        conn.commit()
        replay = compute_accounting_replay(conn)
        ledger = _ledger_replay(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        preview = build_fee_pending_accounting_repair_preview(
            conn,
            client_order_id="already_accounted",
            fill_id="already-accounted-fill",
            fee=26.86,
            fee_provenance="operator_checked_bithumb_trade_history",
        )
        recovery_report = _load_recovery_report()
        app_main(["audit-ledger"])
        audit_out = capsys.readouterr().out
        summary = _incident_summary(conn)
    finally:
        conn.close()

    verdict = summary["verdicts"][0]
    assert verdict["canonical_incident_state"] == "already_accounted_observation_stale"
    assert verdict["incident_scope"] == "historical_context"
    assert verdict["active_issue"] is False
    assert summary["active_issue_count"] == 0
    assert summary["already_accounted_observation_stale_count"] == 1
    assert replay["broker_fill_fee_pending_count"] == 1
    assert replay["broker_fill_latest_unresolved_fee_pending_count"] == 0
    assert replay["unresolved_fee_state"] is False
    assert ledger["broker_fill_latest_unresolved_fee_pending_count"] == 0
    assert ledger["fill_accounting_already_accounted_observation_stale_count"] == 1
    assert "broker_fill_latest_unresolved_fee_pending_count=0" in audit_out


def test_incident_shape_buy_order_level_candidate_applies_principal_immediately(projection_db):
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="live_1777104360000_buy_aee4c564",
            side="BUY",
            qty_req=0.00059998,
            price=115_465_000.0,
            ts_ms=1_777_104_360_000,
            status="NEW",
        )
        result = apply_fill_principal_with_pending_fee(
            conn,
            client_order_id="live_1777104360000_buy_aee4c564",
            side="BUY",
            fill_id="C0101000000983820316",
            fill_ts=1_777_104_360_321,
            price=115_465_000.0,
            qty=0.00059998,
            fee=27.71,
            fee_status="order_level_candidate",
            fee_source="order_level_paid_fee",
            fee_confidence="ambiguous",
            fee_provenance="incident_fixture_order_level_paid_fee",
            fee_validation_reason="order_level_paid_fee_validation_failed",
            fee_validation_checks={"single_fill": True, "expected_fee_rate_match": False},
            note="incident-shape buy fixture",
        )
        order = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='live_1777104360000_buy_aee4c564'"
        ).fetchone()
        portfolio = conn.execute(
            "SELECT cash_krw, asset_qty, cash_available, asset_available FROM portfolio WHERE id=1"
        ).fetchone()
        fill = conn.execute(
            """
            SELECT fee, fee_accounting_status, observed_fee_status, observed_fee_source
            FROM fills
            WHERE client_order_id='live_1777104360000_buy_aee4c564'
            """
        ).fetchone()
        record_broker_fill_observation(
            conn,
            event_ts=1_777_104_360_500,
            client_order_id="live_1777104360000_buy_aee4c564",
            exchange_order_id="C0101000002949768709",
            fill_id="C0101000000983820316",
            fill_ts=1_777_104_360_321,
            side="BUY",
            price=115_465_000.0,
            qty=0.00059998,
            fee=27.71,
            fee_status="order_level_candidate",
            accounting_status="fee_pending",
            source="incident_shape_buy_fee_pending",
            fee_source="order_level_paid_fee",
            fee_confidence="ambiguous",
            fee_provenance="incident_fixture_order_level_paid_fee",
            fee_validation_reason="order_level_paid_fee_validation_failed",
            fee_validation_checks={"single_fill": True, "expected_fee_rate_match": False},
            raw_payload={"fixture": "incident_shape_buy"},
        )
        conn.commit()
        incident_summary = summarize_fill_accounting_incident_projection(conn)
    finally:
        conn.close()

    conn = ensure_db(str(projection_db))
    try:
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert result is not None
    assert result["principal_applied"] is True
    assert result["fee_accounting_status"] == "principal_applied_fee_pending"
    assert float(order["qty_filled"]) == pytest.approx(0.00059998)
    assert float(portfolio["asset_qty"]) == pytest.approx(0.00059998)
    assert float(portfolio["asset_available"]) == pytest.approx(0.00059998)
    assert float(portfolio["cash_krw"]) == pytest.approx(1_000_000.0 - (115_465_000.0 * 0.00059998))
    assert float(fill["fee"]) == pytest.approx(0.0)
    assert fill["fee_accounting_status"] == "principal_applied_fee_pending"
    assert fill["observed_fee_status"] == "order_level_candidate"
    assert fill["observed_fee_source"] == "order_level_paid_fee"
    assert incident_summary["principal_applied_fee_pending_count"] == 1
    assert incident_summary["unapplied_principal_pending_count"] == 0
    assert readiness.recovery_stage == "FEE_FINALIZATION_PENDING"
    assert readiness.fill_accounting_incident_summary["principal_applied_fee_pending_count"] == 1
    assert readiness.fill_accounting_incident_summary["unapplied_principal_pending_count"] == 0


def test_incident_shape_sell_order_level_candidate_applies_principal_immediately(projection_db):
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="seed_buy",
            side="BUY",
            qty_req=0.00059998,
            price=115_465_000.0,
            ts_ms=1_777_104_300_000,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="seed_buy",
            side="BUY",
            fill_id="seed-buy-fill",
            fill_ts=1_777_104_300_100,
            price=115_465_000.0,
            qty=0.00059998,
            fee=27.71,
            note="seed executable position",
        )
        record_order_if_missing(
            conn,
            client_order_id="live_1777104365000_sell_b0f98b71",
            side="SELL",
            qty_req=0.00059998,
            price=115_465_000.0,
            ts_ms=1_777_104_365_000,
            status="NEW",
        )
        result = apply_fill_principal_with_pending_fee(
            conn,
            client_order_id="live_1777104365000_sell_b0f98b71",
            side="SELL",
            fill_id="C0101000000983820317",
            fill_ts=1_777_104_365_321,
            price=115_465_000.0,
            qty=0.00059998,
            fee=27.71,
            fee_status="order_level_candidate",
            fee_source="order_level_paid_fee",
            fee_confidence="ambiguous",
            fee_provenance="incident_fixture_order_level_paid_fee",
            fee_validation_reason="order_level_paid_fee_validation_failed",
            fee_validation_checks={"single_fill": True, "expected_fee_rate_match": False},
            note="incident-shape sell fixture",
        )
        order = conn.execute(
            "SELECT status, qty_filled FROM orders WHERE client_order_id='live_1777104365000_sell_b0f98b71'"
        ).fetchone()
        portfolio = conn.execute(
            "SELECT cash_krw, asset_qty, cash_available, asset_available FROM portfolio WHERE id=1"
        ).fetchone()
        fill = conn.execute(
            """
            SELECT fee, fee_accounting_status, observed_fee_status
            FROM fills
            WHERE client_order_id='live_1777104365000_sell_b0f98b71'
            """
        ).fetchone()
        record_broker_fill_observation(
            conn,
            event_ts=1_777_104_365_500,
            client_order_id="live_1777104365000_sell_b0f98b71",
            exchange_order_id="C0101000002949768710",
            fill_id="C0101000000983820317",
            fill_ts=1_777_104_365_321,
            side="SELL",
            price=115_465_000.0,
            qty=0.00059998,
            fee=27.71,
            fee_status="order_level_candidate",
            accounting_status="fee_pending",
            source="incident_shape_sell_fee_pending",
            fee_source="order_level_paid_fee",
            fee_confidence="ambiguous",
            fee_provenance="incident_fixture_order_level_paid_fee",
            fee_validation_reason="order_level_paid_fee_validation_failed",
            fee_validation_checks={"single_fill": True, "expected_fee_rate_match": False},
            raw_payload={"fixture": "incident_shape_sell"},
        )
        conn.commit()
        incident_summary = summarize_fill_accounting_incident_projection(conn)
    finally:
        conn.close()

    conn = ensure_db(str(projection_db))
    try:
        readiness = compute_runtime_readiness_snapshot(conn)
    finally:
        conn.close()

    assert result is not None
    assert result["principal_applied"] is True
    assert result["fee_accounting_status"] == "principal_applied_fee_pending"
    assert float(order["qty_filled"]) == pytest.approx(0.00059998)
    assert float(portfolio["asset_qty"]) == pytest.approx(0.0)
    assert float(portfolio["asset_available"]) == pytest.approx(0.0)
    assert float(fill["fee"]) == pytest.approx(0.0)
    assert fill["fee_accounting_status"] == "principal_applied_fee_pending"
    assert fill["observed_fee_status"] == "order_level_candidate"
    assert incident_summary["principal_applied_fee_pending_count"] == 1
    assert incident_summary["unapplied_principal_pending_count"] == 0
    assert readiness.recovery_stage == "FEE_FINALIZATION_PENDING"
    assert readiness.fill_accounting_incident_summary["principal_applied_fee_pending_count"] == 1
    assert readiness.fill_accounting_incident_summary["unapplied_principal_pending_count"] == 0


def test_later_accounting_complete_observation_resolves_fee_pending_without_fill(projection_db):
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        _record_fee_pending_order_and_observation(
            conn,
            client_order_id="later_complete",
            fill_id="later-complete-fill",
        )
        record_broker_fill_observation(
            conn,
            event_ts=1_700_000_010_200,
            client_order_id="later_complete",
            exchange_order_id="ex-later_complete",
            fill_id="later-complete-fill",
            fill_ts=1_700_000_010_090,
            side="BUY",
            price=100_000_000.0,
            qty=0.001,
            fee=26.86,
            fee_status="operator_confirmed",
            accounting_status="accounting_complete",
            source="test_later_complete",
            raw_payload={"fixture": "later_complete"},
        )
        conn.commit()
        replay = compute_accounting_replay(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        summary = _incident_summary(conn)
    finally:
        conn.close()

    verdict = summary["verdicts"][0]
    assert verdict["canonical_incident_state"] == "none"
    assert verdict["latest_observation_accounting_status"] == "accounting_complete"
    assert summary["active_issue_count"] == 0
    assert replay["broker_fill_fee_pending_count"] == 1
    assert replay["broker_fill_latest_unresolved_fee_pending_count"] == 0
    assert replay["unresolved_fee_state"] is False
    assert readiness.fee_pending_count == 0


def test_fee_pending_repair_complete_incident_is_historical_not_active(projection_db):
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        _record_fee_pending_order_and_observation(
            conn,
            client_order_id="repaired_pending",
            fill_id="repaired-fill",
        )
        apply_fee_pending_accounting_repair(
            conn,
            client_order_id="repaired_pending",
            fill_id="repaired-fill",
            fee=26.86,
            fee_provenance="operator_checked_bithumb_trade_history",
        )
        conn.commit()
        replay = compute_accounting_replay(conn)
        readiness = compute_runtime_readiness_snapshot(conn)
        summary = _incident_summary(conn)
    finally:
        conn.close()

    verdict = summary["verdicts"][0]
    assert verdict["canonical_incident_state"] == "repaired"
    assert verdict["incident_scope"] == "historical_context"
    assert verdict["repair_present"] is True
    assert verdict["active_issue"] is False
    assert summary["active_issue_count"] == 0
    assert summary["repaired_count"] == 1
    assert replay["fee_pending_accounting_repair_count"] == 1
    assert replay["broker_fill_latest_unresolved_fee_pending_count"] == 0
    assert replay["fill_accounting_repaired_incident_count"] == 1
    assert readiness.fee_pending_count == 0


def test_repair_plan_treats_open_position_lots_as_rebuildable_projection(projection_db):
    conn = ensure_db(str(projection_db))
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="projection_authority_buy",
            side="BUY",
            qty_req=0.001,
            price=100_000_000.0,
            ts_ms=1_700_000_000_000,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="projection_authority_buy",
            side="BUY",
            fill_id="projection_authority_buy_fill",
            fill_ts=1_700_000_000_100,
            price=100_000_000.0,
            qty=0.001,
            fee=50.0,
            note="projection authority fixture buy",
        )
        conn.execute("DELETE FROM open_position_lots")
        conn.commit()
    finally:
        conn.close()

    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_split_mismatch_count": 0,
            "balance_observed_ts_ms": 1_700_000_000_200,
            "dust_residual_present": 0,
            "dust_state": "no_dust",
            "dust_policy_reason": "no_dust_residual",
            "dust_broker_qty": 0.001,
            "dust_local_qty": 0.001,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5000.0,
            "dust_broker_qty_is_dust": 0,
            "dust_local_qty_is_dust": 0,
            "dust_qty_gap_small": 1,
        },
    )

    report = _load_recovery_report()
    plan = build_repair_plan_preview_from_report(report)

    assert plan["projection_kind"] == "open_position_lots"
    assert plan["rebuildable"] is True
    assert plan["source_of_truth"] == "fills+trades+fee_adjustments+external_adjustments+repair_events"
    assert plan["flatten_primary_recommendation"] is False
    assert any(
        candidate["name"] == "rebuild-position-authority" and candidate["needed"]
        for candidate in plan["candidate_repairs"]
    )


def test_recovery_policy_prefers_position_management_resume_for_reliable_open_exposure() -> None:
    report = {
        "runtime_readiness": {
            "normalized_exposure": {"has_executable_exposure": True},
            "run_loop_allowed": True,
            "position_management_allowed": True,
            "new_entry_allowed": False,
            "closeout_allowed": True,
        },
        "accounting_projection_ok": True,
        "broker_portfolio_converged": True,
        "lot_projection_converged": True,
        "fill_accounting_incident_projection": {"active_issue_count": 0},
        "fee_gap_accounting_repair_preview": {"needs_repair": False, "active_issue": False, "resume_blocking": False},
        "manual_flat_accounting_repair_preview": {"needs_repair": False},
        "external_position_accounting_repair_preview": {"needs_repair": False},
        "position_authority_rebuild_preview": {"needs_rebuild": False},
    }

    policy = build_recovery_policy_from_report(report)

    assert policy["primary_incident_class"] == "CANONICAL_OPEN_POSITION"
    assert policy["recommended_mode"] == "position_management"
    assert policy["accounting_root_cause_unresolved"] is False
    assert policy["accounting_evidence_reliable"] is True
    assert policy["actual_executable_exposure"] is True
    assert policy["position_management_allowed"] is True
    assert policy["additional_orders_allowed"] is False
    assert policy["flatten_primary_recommendation"] is False
    assert policy["flatten_not_primary"] is True
    assert policy["recommended_action"] == "resume_position_management"
    assert policy["recommended_command"] == "uv run python bot.py resume"


def test_repair_plan_emits_fee_pending_cash_effect_adjustment_when_repair_delta_remains() -> None:
    report = {
        "mode": "live",
        "cash_available": 398_600.51777,
        "cash_locked": 0.0,
        "portfolio_qty": 0.0,
        "broker_qty": 0.0,
        "broker_qty_known": True,
        "broker_portfolio_converged": False,
        "lot_projection_converged": True,
        "accounting_projection_ok": False,
        "runtime_readiness": {
            "normalized_exposure": {"has_executable_exposure": False},
            "resume_blockers": ["BALANCE_SPLIT_MISMATCH"],
            "projection_convergence": {"projected_total_qty": 0.0, "reason": "converged"},
        },
        "fill_accounting_incident_projection": {
            "active_issue_count": 0,
            "repaired_count": 1,
        },
        "fee_pending_accounting_repair_summary": {
            "repair_count": 1,
            "last_event_ts": 1_777_104_360_900,
            "last_repair_key": "fee-pending-repair-11",
            "last_fee": 2.0,
        },
        "fee_gap_accounting_repair_preview": {
            "needs_repair": False,
            "active_issue": False,
            "resume_blocking": False,
            "portfolio_cash": 398_602.51777,
            "cash_available": 398_602.51777,
            "cash_locked": 0.0,
        },
        "manual_flat_accounting_repair_preview": {"needs_repair": False},
        "external_position_accounting_repair_preview": {"needs_repair": False},
        "position_authority_rebuild_preview": {"needs_rebuild": False},
        "fee_rate_drift_diagnostics": {"recent_expected_fee_rate_mismatch_count": 0},
    }

    plan = build_repair_plan_preview_from_report(report)
    candidate = next(
        item
        for item in plan["candidate_repairs"]
        if item["name"] == "fee-pending-cash-effect-adjustment"
    )

    assert candidate["needed"] is True
    assert candidate["safe_to_apply"] is True
    assert candidate["cash_delta"] == pytest.approx(-2.0)
    assert candidate["repaired_fee"] == pytest.approx(2.0)
    assert "record-external-cash-adjustment" in candidate["recommended_command"]
    assert "--delta-amount -2.00000000" in candidate["recommended_command"]
    assert "--reason fee_pending_cash_effect_repair" in candidate["recommended_command"]
    assert plan["recommended_action"] == "apply_fee-pending-cash-effect-adjustment"


def test_repair_plan_blocks_fee_pending_cash_effect_adjustment_with_active_incident() -> None:
    report = {
        "mode": "live",
        "cash_available": 398_600.51777,
        "cash_locked": 0.0,
        "broker_portfolio_converged": False,
        "lot_projection_converged": True,
        "accounting_projection_ok": False,
        "runtime_readiness": {
            "normalized_exposure": {"has_executable_exposure": False},
            "projection_convergence": {"projected_total_qty": 0.0, "reason": "converged"},
        },
        "fill_accounting_incident_projection": {
            "active_issue_count": 1,
            "repaired_count": 1,
        },
        "fee_pending_accounting_repair_summary": {
            "repair_count": 1,
            "last_event_ts": 1_777_104_360_900,
            "last_repair_key": "fee-pending-repair-11",
            "last_fee": 2.0,
        },
        "fee_gap_accounting_repair_preview": {
            "needs_repair": False,
            "active_issue": False,
            "resume_blocking": False,
            "portfolio_cash": 398_602.51777,
            "cash_available": 398_602.51777,
            "cash_locked": 0.0,
        },
        "manual_flat_accounting_repair_preview": {"needs_repair": False},
        "external_position_accounting_repair_preview": {"needs_repair": False},
        "position_authority_rebuild_preview": {"needs_rebuild": False},
    }

    plan = build_repair_plan_preview_from_report(report)
    candidate = next(
        item
        for item in plan["candidate_repairs"]
        if item["name"] == "fee-pending-cash-effect-adjustment"
    )

    assert candidate["needed"] is False
    assert candidate["safe_to_apply"] is False
    assert candidate["recommended_command"] is None
    assert "active_fee_pending_issue_present" in candidate["why_not_safe"]


def test_projection_drift_routes_to_repair_plan_not_flatten() -> None:
    report = {
        "mode": "live",
        "portfolio_qty": 0.25,
        "broker_qty": 0.25,
        "broker_qty_known": True,
        "broker_qty_evidence_source": "accounts_v1_rest_snapshot",
        "broker_qty_evidence_observed_ts_ms": 1_700_000_000_200,
        "balance_source": "accounts_v1_rest_snapshot",
        "balance_source_stale": False,
        "balance_snapshot_available_for_health": True,
        "balance_snapshot_available_for_position_rebuild": True,
        "missing_evidence_fields": [],
        "broker_portfolio_converged": True,
        "lot_projection_converged": False,
        "accounting_projection_ok": True,
        "runtime_readiness": {
            "normalized_exposure": {"has_executable_exposure": True},
            "closeout_allowed": True,
            "projection_convergence": {
                "projected_total_qty": 0.1,
                "reason": "projection_non_converged",
            },
        },
        "fill_accounting_incident_projection": {"active_issue_count": 0},
        "fee_gap_accounting_repair_preview": {"needs_repair": False, "active_issue": False, "resume_blocking": False},
        "manual_flat_accounting_repair_preview": {"needs_repair": False},
        "external_position_accounting_repair_preview": {"needs_repair": False},
        "position_authority_rebuild_preview": {
            "needs_rebuild": True,
            "safe_to_apply": True,
            "pre_gate_passed": True,
            "final_safe_to_apply": True,
            "eligibility_reason": "projection drift requires rebuild",
            "repair_mode": "full_projection_rebuild",
            "recommended_command": "uv run python bot.py rebuild-position-authority --full-projection-rebuild --apply --yes",
        },
    }

    policy = build_recovery_policy_from_report(report)
    plan = build_repair_plan_preview_from_report(report)

    assert policy["accounting_root_cause_unresolved"] is True
    assert policy["accounting_evidence_reliable"] is False
    assert policy["flatten_primary_recommendation"] is False
    assert policy["flatten_not_primary"] is True
    assert policy["recommended_mode"] == "forensic_accounting"
    assert policy["incident_reasons"] == ["open_position_lots_projection_drift"]
    assert plan["broker_portfolio_converged"] is True
    assert plan["projection_converged"] is False
    assert plan["canonical_portfolio_qty"] == pytest.approx(0.25)
    assert plan["broker_qty"] == pytest.approx(0.25)
    assert plan["broker_qty_known"] is True
    assert plan["broker_qty_evidence_source"] == "accounts_v1_rest_snapshot"
    assert plan["balance_snapshot_available_for_position_rebuild"] is True
    assert plan["missing_evidence_fields"] == []
    assert plan["open_position_lots_projected_qty"] == pytest.approx(0.1)
    assert plan["actual_executable_exposure"] is True
    assert plan["flatten_not_primary"] is True
    assert plan["incident_reasons"] == ["open_position_lots_projection_drift"]
    assert plan["non_mutating_preview"] is True
    assert any(
        candidate["name"] == "rebuild-position-authority" and candidate["needed"] and candidate["safe_to_apply"]
        for candidate in plan["candidate_repairs"]
    )
    rebuild_candidate = next(
        candidate for candidate in plan["candidate_repairs"] if candidate["name"] == "rebuild-position-authority"
    )
    assert rebuild_candidate["pre_gate_passed"] is True
    assert rebuild_candidate["final_safe_to_apply"] is True
    assert rebuild_candidate["recommended_command"] == (
        "uv run python bot.py rebuild-position-authority --full-projection-rebuild --apply --yes"
    )


def test_repair_plan_exposes_missing_broker_evidence_fields() -> None:
    report = {
        "mode": "live",
        "portfolio_qty": 0.00079982,
        "broker_qty": 0.0,
        "broker_qty_known": False,
        "broker_qty_evidence_source": "accounts_v1_rest_snapshot",
        "broker_qty_evidence_observed_ts_ms": 0,
        "balance_source": "accounts_v1_rest_snapshot",
        "balance_source_stale": False,
        "balance_snapshot_available_for_health": True,
        "balance_snapshot_available_for_position_rebuild": False,
        "missing_evidence_fields": ["balance_observed_ts_ms", "base_currency", "broker_asset_qty"],
        "broker_portfolio_converged": False,
        "lot_projection_converged": False,
        "accounting_projection_ok": True,
        "runtime_readiness": {
            "normalized_exposure": {"has_executable_exposure": True},
            "closeout_allowed": False,
            "projection_convergence": {
                "projected_total_qty": 0.00298794,
                "reason": "projection_non_converged",
            },
        },
        "fill_accounting_incident_projection": {"active_issue_count": 0},
        "fee_gap_accounting_repair_preview": {"needs_repair": False, "active_issue": False, "resume_blocking": False},
        "manual_flat_accounting_repair_preview": {"needs_repair": False},
        "external_position_accounting_repair_preview": {"needs_repair": False},
        "position_authority_rebuild_preview": {
            "needs_rebuild": True,
            "safe_to_apply": False,
            "pre_gate_passed": False,
            "final_safe_to_apply": False,
            "eligibility_reason": "broker_position_qty_evidence_missing",
            "repair_mode": "full_projection_rebuild",
            "recommended_command": None,
        },
    }

    plan = build_repair_plan_preview_from_report(report)

    assert plan["balance_snapshot_available_for_health"] is True
    assert plan["balance_snapshot_available_for_position_rebuild"] is False
    assert plan["missing_evidence_fields"] == [
        "balance_observed_ts_ms",
        "base_currency",
        "broker_asset_qty",
    ]
    rebuild_candidate = next(
        candidate for candidate in plan["candidate_repairs"] if candidate["name"] == "rebuild-position-authority"
    )
    assert rebuild_candidate["pre_gate_passed"] is False
    assert rebuild_candidate["final_safe_to_apply"] is False
    assert rebuild_candidate["recommended_command"] == ""
