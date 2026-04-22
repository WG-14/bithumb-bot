from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.broker.balance_source import BalanceSnapshot
from bithumb_bot.broker.base import BrokerBalance
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, set_portfolio_breakdown
from bithumb_bot.risk import (
    DAILY_LOSS_LIMIT_REASON_CODE,
    RISK_STATE_MISMATCH,
    evaluate_daily_loss_state,
    fetch_daily_risk_baseline,
    fetch_recent_risk_evaluations,
)

KST = timezone(timedelta(hours=9))


class _RiskSnapshotBroker:
    def __init__(self, *, cash_available: float, cash_locked: float = 0.0, asset_available: float = 0.0, asset_locked: float = 0.0, observed_ts_ms: int = 0) -> None:
        self.cash_available = float(cash_available)
        self.cash_locked = float(cash_locked)
        self.asset_available = float(asset_available)
        self.asset_locked = float(asset_locked)
        self.observed_ts_ms = int(observed_ts_ms)

    def get_balance_snapshot(self) -> BalanceSnapshot:
        return BalanceSnapshot(
            source_id="accounts_v1_rest_snapshot",
            observed_ts_ms=int(self.observed_ts_ms),
            asset_ts_ms=int(self.observed_ts_ms),
            balance=BrokerBalance(
                cash_available=float(self.cash_available),
                cash_locked=float(self.cash_locked),
                asset_available=float(self.asset_available),
                asset_locked=float(self.asset_locked),
            ),
        )


@pytest.fixture(autouse=True)
def _restore_risk_settings() -> None:
    original_values = {
        "DB_PATH": settings.DB_PATH,
        "MODE": settings.MODE,
        "START_CASH_KRW": settings.START_CASH_KRW,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "PAIR": settings.PAIR,
    }
    try:
        yield
    finally:
        for key, value in original_values.items():
            object.__setattr__(settings, key, value)


def _record_verified_reconcile(*, observed_ts_ms: int) -> None:
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_source": "accounts_v1_rest_snapshot",
            "balance_observed_ts_ms": int(observed_ts_ms),
            "dust_residual_present": 0,
        },
    )


def test_daily_loss_evaluation_matches_verified_snapshot_math(tmp_path):
    db_path = tmp_path / "verified_daily_loss.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 30_000.0)

    conn = ensure_db(str(db_path))
    try:
        now_ms = int(time.time() * 1000)
        _record_verified_reconcile(observed_ts_ms=now_ms)
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        seeded = evaluate_daily_loss_state(
            conn,
            ts_ms=now_ms,
            price=100_000_000.0,
            broker=_RiskSnapshotBroker(cash_available=1_000_000.0, observed_ts_ms=now_ms),
            mark_price_source="test_seed",
            evaluation_origin="test_seed",
        )

        set_portfolio_breakdown(
            conn,
            cash_available=954_734.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        breached = evaluate_daily_loss_state(
            conn,
            ts_ms=now_ms + 1,
            price=100_000_000.0,
            broker=_RiskSnapshotBroker(cash_available=954_734.0, observed_ts_ms=now_ms + 1),
            mark_price_source="test_mark",
            evaluation_origin="test_breach",
        )
        baseline = fetch_daily_risk_baseline(conn)
        recent = fetch_recent_risk_evaluations(conn, limit=2)
    finally:
        conn.close()

    assert seeded.blocked is False
    assert baseline is not None
    assert baseline["baseline_origin"] == "seeded_on_first_verified_eval"
    assert baseline["baseline_balance_source"] == "accounts_v1_rest_snapshot"
    assert breached.blocked is True
    assert breached.reason_code == DAILY_LOSS_LIMIT_REASON_CODE
    assert breached.loss_today == pytest.approx(45_266.0)
    assert breached.start_equity == pytest.approx(1_000_000.0)
    assert breached.current_equity == pytest.approx(954_734.0)
    assert recent[0]["reason_code"] == DAILY_LOSS_LIMIT_REASON_CODE
    assert recent[0]["current_cash_krw"] == pytest.approx(954_734.0)
    assert recent[0]["loss_today"] == pytest.approx(45_266.0)
    assert recent[0]["current_balance_source"] == "accounts_v1_rest_snapshot"


def test_daily_loss_mismatch_does_not_masquerade_as_limit_breach(tmp_path):
    db_path = tmp_path / "risk_state_mismatch.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 30_000.0)

    conn = ensure_db(str(db_path))
    try:
        now_ms = int(time.time() * 1000)
        _record_verified_reconcile(observed_ts_ms=now_ms)
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        evaluate_daily_loss_state(
            conn,
            ts_ms=now_ms,
            price=100_000_000.0,
            broker=_RiskSnapshotBroker(cash_available=1_000_000.0, observed_ts_ms=now_ms),
            evaluation_origin="test_seed",
        )
        mismatch = evaluate_daily_loss_state(
            conn,
            ts_ms=now_ms + 1,
            price=100_000_000.0,
            broker=_RiskSnapshotBroker(cash_available=954_734.0, observed_ts_ms=now_ms + 1),
            evaluation_origin="test_mismatch",
        )
        recent = fetch_recent_risk_evaluations(conn, limit=1)
    finally:
        conn.close()

    assert mismatch.blocked is True
    assert mismatch.reason_code == RISK_STATE_MISMATCH
    assert mismatch.reason.startswith("risk state mismatch")
    assert "daily loss limit exceeded" not in mismatch.reason
    assert recent[0]["reason_code"] == RISK_STATE_MISMATCH
    assert recent[0]["decision"] == "unverified"
    assert "broker/local portfolio mismatch" in str(recent[0]["mismatch_summary"])


def test_daily_loss_rejects_live_baseline_without_provenance(tmp_path):
    db_path = tmp_path / "legacy_baseline.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 30_000.0)

    conn = ensure_db(str(db_path))
    try:
        now_ms = int(time.time() * 1000)
        _record_verified_reconcile(observed_ts_ms=now_ms)
        set_portfolio_breakdown(
            conn,
            cash_available=954_734.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        day_kst = datetime.fromtimestamp(now_ms / 1000, tz=KST).strftime("%Y-%m-%d")
        conn.execute(
            "INSERT INTO daily_risk(day_kst, start_equity) VALUES (?, ?)",
            (day_kst, 1_000_000.0),
        )
        conn.commit()
        evaluation = evaluate_daily_loss_state(
            conn,
            ts_ms=now_ms,
            price=100_000_000.0,
            broker=_RiskSnapshotBroker(cash_available=954_734.0, observed_ts_ms=now_ms),
            evaluation_origin="test_legacy_baseline",
        )
        recent = fetch_recent_risk_evaluations(conn, limit=1)
    finally:
        conn.close()

    assert evaluation.blocked is True
    assert evaluation.reason_code == RISK_STATE_MISMATCH
    assert "baseline" in evaluation.reason
    assert recent[0]["reason_code"] == RISK_STATE_MISMATCH
    assert "baseline provenance missing" in str(recent[0]["mismatch_summary"])


def test_daily_loss_requires_verified_reconcile_before_live_verdict(tmp_path):
    db_path = tmp_path / "reconcile_required.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 30_000.0)

    conn = ensure_db(str(db_path))
    try:
        now_ms = int(time.time() * 1000)
        runtime_state.record_reconcile_result(
            success=False,
            reason_code="RECONCILE_FAILED",
            error="broker timeout",
            metadata={"balance_source": "accounts_v1_rest_snapshot"},
        )
        set_portfolio_breakdown(
            conn,
            cash_available=954_734.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        evaluation = evaluate_daily_loss_state(
            conn,
            ts_ms=now_ms,
            price=100_000_000.0,
            broker=_RiskSnapshotBroker(cash_available=954_734.0, observed_ts_ms=now_ms),
            evaluation_origin="test_reconcile_required",
        )
        recent = fetch_recent_risk_evaluations(conn, limit=1)
    finally:
        conn.close()

    assert evaluation.blocked is True
    assert evaluation.reason_code == RISK_STATE_MISMATCH
    assert "latest reconcile" in evaluation.reason
    assert recent[0]["reason_code"] == RISK_STATE_MISMATCH
    assert "latest reconcile state is not ok" in str(recent[0]["mismatch_summary"])
