from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

from bithumb_bot.broker.base import BrokerBalance, BrokerFill, BrokerOrder
from bithumb_bot.config import PATH_MANAGER, settings
from bithumb_bot.db_core import (
    ensure_db,
    get_external_cash_adjustment_summary,
    init_portfolio,
    record_external_cash_adjustment,
    set_portfolio_breakdown,
)
from bithumb_bot.execution import apply_fill_and_trade, record_order_if_missing
from bithumb_bot.engine import evaluate_resume_eligibility, evaluate_startup_safety_gate, _classify_balance_split_blocker
from bithumb_bot.oms import add_fill, set_status
from bithumb_bot.recovery import reconcile_with_broker
from bithumb_bot import runtime_state
from bithumb_bot.reporting import cmd_cash_drift_report, fetch_cash_drift_report


class _CashOnlyDriftBroker:
    def get_order(self, *, client_order_id: str, exchange_order_id: str | None = None) -> BrokerOrder:
        return BrokerOrder(client_order_id, exchange_order_id or "ex-cash-drift", "BUY", "NEW", 100.0, 1.0, 0.0, 1, 1)

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
        return BrokerBalance(cash_available=1_050.0, cash_locked=0.0, asset_available=0.0, asset_locked=0.0)


class _VariableCashDriftBroker(_CashOnlyDriftBroker):
    def __init__(self, *, cash_available: float) -> None:
        self._balance = BrokerBalance(
            cash_available=cash_available,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )

    def get_balance(self) -> BrokerBalance:
        return self._balance


@pytest.fixture(autouse=True)
def _restore_settings_state():
    original_mode = settings.MODE
    original_start_cash = settings.START_CASH_KRW
    original_db_path = settings.DB_PATH
    original_live_dry_run = settings.LIVE_DRY_RUN

    try:
        yield
    finally:
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "LIVE_DRY_RUN", original_live_dry_run)


@pytest.fixture
def mixed_trade_cash_ledger(tmp_path, monkeypatch):
    db_path = tmp_path / "mixed_trade_cash_ledger.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("MODE", "paper")
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)

    conn = ensure_db(str(db_path))
    try:
        init_portfolio(conn)
        record_order_if_missing(
            conn,
            client_order_id="mixed_buy_1",
            side="BUY",
            qty_req=0.001,
            price=100_000_000.0,
            ts_ms=1_700_000_000_000,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="mixed_buy_1",
            side="BUY",
            fill_id="mixed_buy_1_fill",
            fill_ts=1_700_000_000_100,
            price=100_000_000.0,
            qty=0.001,
            fee=50.0,
            note="fixture buy fill",
        )
        record_order_if_missing(
            conn,
            client_order_id="mixed_sell_1",
            side="SELL",
            qty_req=0.001,
            price=110_000_000.0,
            ts_ms=1_700_000_000_200,
            status="NEW",
        )
        apply_fill_and_trade(
            conn,
            client_order_id="mixed_sell_1",
            side="SELL",
            fill_id="mixed_sell_1_fill",
            fill_ts=1_700_000_000_300,
            price=110_000_000.0,
            qty=0.001,
            fee=60.0,
            note="fixture sell fill",
        )
        conn.commit()
    finally:
        conn.close()

    yield {
        "db_path": db_path,
        "trade_cash_after": 1_009_890.0,
        "trade_asset_after": 0.0,
    }

def _reconcile_cash_drift_at(*, cash_available: float) -> None:
    reconcile_with_broker(_VariableCashDriftBroker(cash_available=cash_available))


def test_external_cash_adjustment_is_idempotent_and_updates_portfolio(tmp_path):
    db_path = tmp_path / "cash_adjustment.sqlite"
    original_start_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    try:
        conn = ensure_db(str(db_path))
        try:
            first = record_external_cash_adjustment(
                conn,
                event_ts=1_700_000_000_000,
                currency="KRW",
                delta_amount=123.0,
                source="manual_deposit",
                reason="operator_correction",
                broker_snapshot_basis={
                    "balance_source": "manual",
                    "broker_cash_total": 1_000_123.0,
                    "local_cash_total": 1_000_000.0,
                },
                correlation_metadata={"ticket": "ops-42"},
                note="manual cash top-up",
                adjustment_key="manual_deposit:ops-42",
            )
            second = record_external_cash_adjustment(
                conn,
                event_ts=1_700_000_000_999,
                currency="KRW",
                delta_amount=123.0,
                source="manual_deposit",
                reason="operator_correction",
                broker_snapshot_basis={
                    "balance_source": "manual",
                    "broker_cash_total": 1_000_123.0,
                    "local_cash_total": 1_000_000.0,
                },
                correlation_metadata={"ticket": "ops-42"},
                note="manual cash top-up",
                adjustment_key="manual_deposit:ops-42",
            )
            row = conn.execute(
                "SELECT COUNT(*) AS adjustment_count, COALESCE(SUM(delta_amount), 0.0) AS total_delta FROM external_cash_adjustments"
            ).fetchone()
            portfolio = conn.execute(
                "SELECT cash_krw, cash_available, cash_locked FROM portfolio WHERE id=1"
            ).fetchone()
            summary = get_external_cash_adjustment_summary(conn)
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)

    assert first is not None and first["created"] is True
    assert second is not None and second["created"] is False
    assert int(row["adjustment_count"]) == 1
    assert float(row["total_delta"]) == pytest.approx(123.0)
    assert float(portfolio["cash_krw"]) == pytest.approx(1_000_123.0)
    assert float(portfolio["cash_available"]) == pytest.approx(1_000_123.0)
    assert float(portfolio["cash_locked"]) == pytest.approx(0.0)
    assert summary["adjustment_count"] == 1
    assert summary["adjustment_total"] == pytest.approx(123.0)
    assert summary["last_source"] == "manual_deposit"


def test_reconcile_records_external_cash_adjustment_for_cash_only_drift(tmp_path):
    db_path = tmp_path / "reconcile_cash_adjustment.sqlite"
    original_db_path = settings.DB_PATH
    original_start_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000.0)
    try:
        ensure_db(str(db_path)).close()
        runtime_state.record_reconcile_result(success=True, reason_code=None, metadata=None, now_epoch_sec=0.0)

        reconcile_with_broker(_CashOnlyDriftBroker())

        conn = ensure_db(str(db_path))
        try:
            row = conn.execute(
                "SELECT cash_krw, cash_available, cash_locked FROM portfolio WHERE id=1"
            ).fetchone()
            adjustment = conn.execute(
                """
                SELECT event_ts, currency, delta_amount, source, reason, broker_snapshot_basis
                FROM external_cash_adjustments
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            trade_count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            fill_count = conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0]
            metadata_raw = conn.execute(
                "SELECT last_reconcile_metadata FROM bot_health WHERE id=1"
            ).fetchone()[0]
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)

    metadata = json.loads(str(metadata_raw))
    assert float(row["cash_krw"]) == pytest.approx(1_050.0)
    assert float(row["cash_available"]) == pytest.approx(1_050.0)
    assert float(row["cash_locked"]) == pytest.approx(0.0)
    assert int(adjustment["event_ts"]) > 0
    assert adjustment["currency"] == "KRW"
    assert float(adjustment["delta_amount"]) == pytest.approx(50.0)
    assert adjustment["source"] == "legacy_balance_api"
    assert adjustment["reason"] == "reconcile_cash_drift"
    assert trade_count == 0
    assert fill_count == 0
    assert metadata["external_cash_adjustment_count"] == 1


def test_reconcile_marks_fee_related_cash_drift_and_blocks_recovery_state(tmp_path) -> None:
    db_path = tmp_path / "reconcile_fee_gap_cash_drift.sqlite"
    original_db_path = settings.DB_PATH
    original_start_cash = settings.START_CASH_KRW
    original_mode = settings.MODE
    original_min_notional = settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 1_000.0)
    class _FeeGapDriftBroker(_VariableCashDriftBroker):
        def get_balance(self) -> BrokerBalance:
            return BrokerBalance(
                cash_available=899_950.0,
                cash_locked=0.0,
                asset_available=0.001,
                asset_locked=0.0,
            )

    try:
        conn = ensure_db(str(db_path))
        try:
            init_portfolio(conn)
            record_order_if_missing(
                conn,
                client_order_id="fee_gap_buy_1",
                side="BUY",
                qty_req=0.001,
                price=100_000_000.0,
                ts_ms=1_700_000_000_000,
                status="NEW",
            )
            # Seed representative historical contamination directly. This test models
            # a pre-existing live ledger defect, not a currently accepted live write.
            add_fill(
                conn=conn,
                client_order_id="fee_gap_buy_1",
                fill_id="fee_gap_buy_1_fill",
                fill_ts=1_700_000_000_100,
                price=100_000_000.0,
                qty=0.001,
                fee=0.0,
            )
            set_status("fee_gap_buy_1", "FILLED", conn=conn)
            set_portfolio_breakdown(
                conn,
                cash_available=900_000.0,
                cash_locked=0.0,
                asset_available=0.001,
                asset_locked=0.0,
            )
            conn.commit()
        finally:
            conn.close()

        runtime_state.record_reconcile_result(success=True, reason_code=None, metadata=None, now_epoch_sec=0.0)
        reconcile_with_broker(_FeeGapDriftBroker(cash_available=899_950.0))
        eligible, blockers = evaluate_resume_eligibility()
        startup_reason = evaluate_startup_safety_gate()

        conn = ensure_db(str(db_path))
        try:
            adjustment = conn.execute(
                """
                SELECT reason, note
                FROM external_cash_adjustments
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            metadata_raw = conn.execute(
                "SELECT last_reconcile_metadata FROM bot_health WHERE id=1"
            ).fetchone()[0]
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)
        object.__setattr__(settings, "MODE", original_mode)
        object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", original_min_notional)

    metadata = json.loads(str(metadata_raw))
    assert adjustment["reason"] == "reconcile_fee_gap_cash_drift"
    assert "material zero-fee fill history present" in str(adjustment["note"])
    assert metadata["external_cash_adjustment_reason"] == "reconcile_fee_gap_cash_drift"
    assert metadata["material_zero_fee_fill_count"] == 1
    assert metadata["fee_gap_recovery_required"] == 1
    assert metadata["external_cash_adjustment_delta_krw"] == pytest.approx(-50.0)
    assert metadata["external_cash_adjustment_total_krw"] == pytest.approx(-50.0)
    assert eligible is False
    assert "FEE_GAP_RECOVERY_REQUIRED" in [b.code for b in blockers]
    assert metadata["external_cash_adjustment_residual_krw"] == pytest.approx(0.0)
    assert startup_reason is not None
    assert "fee_gap_recovery_required=1" in startup_reason


def test_reconcile_cash_adjustment_clears_prior_cash_mismatch_blocker(tmp_path):
    db_path = tmp_path / "reconcile_cash_adjustment_blocker.sqlite"
    original_db_path = settings.DB_PATH
    original_start_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000.0)
    try:
        ensure_db(str(db_path)).close()
        blocked_metadata = {
            "balance_split_mismatch_count": 1,
            "balance_split_mismatch_summary": "cash_available(local=1000,broker=1050,delta=50)",
            "external_cash_adjustment_count": 0,
            "external_cash_adjustment_total_krw": 0.0,
        }
        assert _classify_balance_split_blocker(blocked_metadata) is not None

        reconcile_with_broker(_CashOnlyDriftBroker())

        conn = ensure_db(str(db_path))
        try:
            row = conn.execute(
                "SELECT cash_krw, cash_available, cash_locked FROM portfolio WHERE id=1"
            ).fetchone()
            summary = get_external_cash_adjustment_summary(conn)
            metadata_raw = conn.execute(
                "SELECT last_reconcile_metadata FROM bot_health WHERE id=1"
            ).fetchone()[0]
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)

    metadata = json.loads(str(metadata_raw))
    assert float(row["cash_krw"]) == pytest.approx(1_050.0)
    assert float(row["cash_available"]) == pytest.approx(1_050.0)
    assert float(row["cash_locked"]) == pytest.approx(0.0)
    assert summary["adjustment_count"] == 1
    assert summary["adjustment_total"] == pytest.approx(50.0)
    assert metadata["external_cash_adjustment_count"] == 1
    assert metadata["external_cash_adjustment_total_krw"] == pytest.approx(50.0)
    assert metadata["external_cash_adjustment_residual_krw"] == pytest.approx(0.0)
    assert _classify_balance_split_blocker(metadata) is None


def test_reconcile_does_not_duplicate_external_cash_adjustment_for_same_snapshot(tmp_path):
    db_path = tmp_path / "reconcile_cash_adjustment_dedup.sqlite"
    original_db_path = settings.DB_PATH
    original_start_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "START_CASH_KRW", 1_000.0)
    try:
        ensure_db(str(db_path)).close()
        runtime_state.record_reconcile_result(success=True, reason_code=None, metadata=None, now_epoch_sec=0.0)

        reconcile_with_broker(_CashOnlyDriftBroker())
        reconcile_with_broker(_CashOnlyDriftBroker())

        conn = ensure_db(str(db_path))
        try:
            row = conn.execute(
                "SELECT cash_krw, cash_available, cash_locked FROM portfolio WHERE id=1"
            ).fetchone()
            adjustment_count = conn.execute(
                "SELECT COUNT(*) FROM external_cash_adjustments"
            ).fetchone()[0]
            total_delta = conn.execute(
                "SELECT COALESCE(SUM(delta_amount), 0.0) FROM external_cash_adjustments"
            ).fetchone()[0]
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)

    assert int(adjustment_count) == 1
    assert float(total_delta) == pytest.approx(50.0)
    assert float(row["cash_krw"]) == pytest.approx(1_050.0)
    assert float(row["cash_available"]) == pytest.approx(1_050.0)
    assert float(row["cash_locked"]) == pytest.approx(0.0)


def test_reconcile_records_cumulative_external_cash_adjustments_and_survives_restart(
    mixed_trade_cash_ledger,
    monkeypatch,
    capsys,
):
    db_path = Path(mixed_trade_cash_ledger["db_path"])
    first_cash = 1_009_880.0
    second_cash = 1_009_860.0

    monkeypatch.setattr(
        "bithumb_bot.reporting.BithumbBroker",
        lambda: _VariableCashDriftBroker(cash_available=second_cash),
    )

    runtime_state.record_reconcile_result(success=True, reason_code=None, metadata=None, now_epoch_sec=0.0)

    _reconcile_cash_drift_at(cash_available=first_cash)
    _reconcile_cash_drift_at(cash_available=second_cash)
    importlib.reload(runtime_state)
    _reconcile_cash_drift_at(cash_available=second_cash)

    conn = ensure_db(str(db_path))
    try:
        trade_row = conn.execute(
            "SELECT cash_after, asset_after FROM trades ORDER BY id DESC LIMIT 1"
        ).fetchone()
        portfolio_row = conn.execute(
            "SELECT cash_krw, cash_available, cash_locked, asset_qty, asset_available, asset_locked FROM portfolio WHERE id=1"
        ).fetchone()
        adjustment_rows = conn.execute(
            "SELECT event_ts, delta_amount, source, reason FROM external_cash_adjustments ORDER BY id ASC"
        ).fetchall()
        summary = get_external_cash_adjustment_summary(conn)
        report = fetch_cash_drift_report(conn, recent_limit=5)
    finally:
        conn.close()

    assert trade_row is not None
    assert float(trade_row["cash_after"]) == pytest.approx(mixed_trade_cash_ledger["trade_cash_after"])
    assert float(trade_row["asset_after"]) == pytest.approx(mixed_trade_cash_ledger["trade_asset_after"])
    assert float(portfolio_row["cash_krw"]) == pytest.approx(1_009_860.0)
    assert float(portfolio_row["cash_available"]) == pytest.approx(1_009_860.0)
    assert float(portfolio_row["cash_locked"]) == pytest.approx(0.0)
    assert float(portfolio_row["asset_qty"]) == pytest.approx(0.0)
    assert float(portfolio_row["asset_available"]) == pytest.approx(0.0)
    assert float(portfolio_row["asset_locked"]) == pytest.approx(0.0)

    assert len(adjustment_rows) == 2
    assert [float(row["delta_amount"]) for row in adjustment_rows] == pytest.approx([-10.0, -20.0])
    assert [str(row["reason"]) for row in adjustment_rows] == ["reconcile_cash_drift", "reconcile_cash_drift"]
    assert summary["adjustment_count"] == 2
    assert summary["adjustment_total"] == pytest.approx(-30.0)
    assert report["local"]["cash_without_external_adjustments_krw"] == pytest.approx(
        mixed_trade_cash_ledger["trade_cash_after"]
    )
    assert report["local"]["cash_krw"] == pytest.approx(1_009_860.0)
    assert report["local"]["consistent"] is True
    assert report["cash_drift"]["external_cash_adjustment_count"] == 2
    assert report["cash_drift"]["external_cash_adjustment_total_krw"] == pytest.approx(-30.0)
    assert report["cash_drift"]["explained_delta_krw"] == pytest.approx(-30.0)
    assert report["cash_drift"]["unexplained_residual_delta_krw"] == pytest.approx(0.0)

    cmd_cash_drift_report(recent_limit=5)
    out = capsys.readouterr().out
    assert "[CASH-DRIFT-REPORT]" in out
    assert "ledger_cash_without_adjustments=1,009,890.000" in out
    assert "external_cash_adjustment_total=-30.000" in out
    assert "recent_adjustment_count=2" in out
    assert "reason=reconcile_cash_drift" in out


def test_cash_drift_report_tracks_manual_fee_trade_mix_duplicate_reconcile_and_restart(
    mixed_trade_cash_ledger,
    monkeypatch,
    capsys,
):
    db_path = Path(mixed_trade_cash_ledger["db_path"])
    original_db_path = settings.DB_PATH
    original_start_cash = settings.START_CASH_KRW
    trade_cash_after = mixed_trade_cash_ledger["trade_cash_after"]
    first_broker_cash = trade_cash_after - 30.0
    second_broker_cash = trade_cash_after - 50.0

    monkeypatch.setattr(
        "bithumb_bot.reporting.BithumbBroker",
        lambda: _VariableCashDriftBroker(cash_available=second_broker_cash),
    )

    object.__setattr__(settings, "DB_PATH", str(db_path))
    try:
        conn = ensure_db(str(db_path))
        try:
            record_external_cash_adjustment(
                conn,
                event_ts=1_700_000_500_000,
                currency="KRW",
                delta_amount=-30.0,
                source="bank_fee",
                reason="deposit_fee",
                broker_snapshot_basis={
                    "balance_source": "manual_statement",
                    "trade_cash_after": trade_cash_after,
                    "cash_before_fee": trade_cash_after,
                    "cash_after_fee": first_broker_cash,
                },
                correlation_metadata={"ticket": "fee-1"},
                note="deposit fee on transfer",
                adjustment_key="bank_fee:deposit_fee:1",
            )
        finally:
            conn.close()

        importlib.reload(runtime_state)
        reconcile_with_broker(_VariableCashDriftBroker(cash_available=first_broker_cash))
        importlib.reload(runtime_state)
        reconcile_with_broker(_VariableCashDriftBroker(cash_available=first_broker_cash))

        importlib.reload(runtime_state)
        reconcile_with_broker(_VariableCashDriftBroker(cash_available=second_broker_cash))
        importlib.reload(runtime_state)
        reconcile_with_broker(_VariableCashDriftBroker(cash_available=second_broker_cash))

        conn = ensure_db(str(db_path))
        try:
            adjustment_rows = conn.execute(
                "SELECT event_ts, delta_amount, source, reason, adjustment_key FROM external_cash_adjustments ORDER BY id ASC"
            ).fetchall()
            report = fetch_cash_drift_report(conn, recent_limit=5)
        finally:
            conn.close()
    finally:
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)

    assert len(adjustment_rows) == 2
    assert [float(row["delta_amount"]) for row in adjustment_rows] == pytest.approx([-30.0, -20.0])
    assert [str(row["reason"]) for row in adjustment_rows] == ["deposit_fee", "reconcile_cash_drift"]
    assert report["local"]["cash_without_external_adjustments_krw"] == pytest.approx(trade_cash_after)
    assert report["local"]["cash_krw"] == pytest.approx(second_broker_cash)
    assert report["cash_drift"]["external_cash_adjustment_count"] == 2
    assert report["cash_drift"]["external_cash_adjustment_total_krw"] == pytest.approx(-50.0)
    assert report["cash_drift"]["explained_delta_krw"] == pytest.approx(-50.0)
    assert report["cash_drift"]["unexplained_residual_delta_krw"] == pytest.approx(0.0)
    assert len(report["recent_adjustments"]) == 2
    assert report["recent_adjustments"][0]["reason"] == "reconcile_cash_drift"
    assert report["recent_adjustments"][1]["reason"] == "deposit_fee"

    cmd_cash_drift_report(recent_limit=5)
    out = capsys.readouterr().out
    report_path = PATH_MANAGER.cash_drift_report_path()
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["cash_drift"]["external_cash_adjustment_count"] == 2
    assert payload["cash_drift"]["external_cash_adjustment_total_krw"] == pytest.approx(-50.0)
    assert payload["cash_drift"]["unexplained_residual_delta_krw"] == pytest.approx(0.0)
    assert payload["recent_adjustments"][0]["broker_snapshot_basis_summary"] != "-"
    assert "[CASH-DRIFT-REPORT]" in out
    assert "broker_cash=" in out
    assert "local_cash=" in out
    assert "recent_adjustments:" in out
    assert "reason=reconcile_cash_drift" in out
    assert "reason=deposit_fee" in out


def test_external_cash_adjustments_remain_db_scoped(tmp_path):
    paper_db = tmp_path / "paper.sqlite"
    live_db = tmp_path / "live.sqlite"
    expected_start_cash = 1_000_000.0
    original_start_cash = settings.START_CASH_KRW
    object.__setattr__(settings, "START_CASH_KRW", expected_start_cash)
    paper_conn = ensure_db(str(paper_db))
    live_conn = ensure_db(str(live_db))
    try:
        init_portfolio(live_conn)
        record_external_cash_adjustment(
            paper_conn,
            event_ts=1_700_000_100_000,
            currency="KRW",
            delta_amount=77.0,
            source="manual_deposit",
            reason="paper_only",
            broker_snapshot_basis={"balance_source": "paper"},
            note="paper runtime adjustment",
            adjustment_key="paper:manual_deposit:77",
        )
        paper_count = paper_conn.execute("SELECT COUNT(*) FROM external_cash_adjustments").fetchone()[0]
        live_count = live_conn.execute("SELECT COUNT(*) FROM external_cash_adjustments").fetchone()[0]
        paper_portfolio = paper_conn.execute("SELECT cash_krw FROM portfolio WHERE id=1").fetchone()[0]
        live_portfolio = live_conn.execute("SELECT cash_krw FROM portfolio WHERE id=1").fetchone()[0]
    finally:
        paper_conn.close()
        live_conn.close()
        object.__setattr__(settings, "START_CASH_KRW", original_start_cash)

    assert paper_count == 1
    assert live_count == 0
    assert float(paper_portfolio) == pytest.approx(1_000_077.0)
    assert float(live_portfolio) == pytest.approx(expected_start_cash)
