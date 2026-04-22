from __future__ import annotations

import pytest

from bithumb_bot.app import main as app_main
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
)
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.reporting import fetch_cash_drift_report


@pytest.fixture
def projection_db(tmp_path, monkeypatch):
    original_db_path = settings.DB_PATH
    original_mode = settings.MODE
    original_start_cash = settings.START_CASH_KRW
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
