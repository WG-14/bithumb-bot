from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot.config import LiveModeValidationError, settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.operator_smoke_preflight import validate_operator_smoke_preflight
import bithumb_bot.operator_smoke_preflight as smoke_preflight


def _live_roots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    for key, dirname in {
        "ENV_ROOT": "envroot",
        "RUN_ROOT": "runroot",
        "DATA_ROOT": "dataroot",
        "LOG_ROOT": "logroot",
        "BACKUP_ROOT": "backuproot",
    }.items():
        monkeypatch.setenv(key, str(tmp_path / dirname))
    db_path = tmp_path / "dataroot" / "live" / "trades" / "live.sqlite"
    monkeypatch.setenv("DB_PATH", str(db_path))
    return db_path


def _live_settings(db_path: Path, **overrides):
    base = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        KILL_SWITCH=False,
        DB_PATH=str(db_path),
        PAIR="KRW-BTC",
        BITHUMB_API_KEY="key",
        BITHUMB_API_SECRET="x" * 64,
    )
    return replace(base, **overrides)


def _readiness_snapshot(
    *,
    broker_qty_known: bool = True,
    broker_qty: float = 0.0,
    portfolio_qty: float = 0.0,
    projected_qty: float = 0.0,
    projection_converged: bool = True,
    recovery_required_count: int = 0,
    fee_pending_count: int = 0,
    active_fee_accounting_blocker: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        broker_position_evidence={
            "broker_qty_known": broker_qty_known,
            "broker_qty": broker_qty,
            "balance_source_stale": False,
            "missing_evidence_fields": [] if broker_qty_known else ["broker_asset_qty"],
        },
        projection_convergence={
            "converged": projection_converged,
            "portfolio_qty": portfolio_qty,
            "projected_total_qty": projected_qty,
            "reason": "converged" if projection_converged else "projection_non_converged",
        },
        recovery_required_count=recovery_required_count,
        fee_pending_count=fee_pending_count,
        active_fee_accounting_blocker=active_fee_accounting_blocker,
    )


def test_operator_smoke_preflight_allows_live_armed_without_approved_profile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    monkeypatch.setattr(
        smoke_preflight,
        "compute_runtime_readiness_snapshot",
        lambda _conn: _readiness_snapshot(),
    )
    try:
        validate_operator_smoke_preflight(
            cfg=_live_settings(db_path, APPROVED_STRATEGY_PROFILE_PATH=""),
            conn=conn,
            market="KRW-BTC",
            market_preflight=lambda _cfg: None,
        )
    finally:
        conn.close()


def test_operator_smoke_preflight_rejects_broker_local_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    monkeypatch.setattr(
        smoke_preflight,
        "compute_runtime_readiness_snapshot",
        lambda _conn: _readiness_snapshot(broker_qty=0.01, portfolio_qty=0.0, projected_qty=0.0),
    )
    try:
        with pytest.raises(LiveModeValidationError, match="broker_local_mismatch"):
            validate_operator_smoke_preflight(
                cfg=_live_settings(db_path, APPROVED_STRATEGY_PROFILE_PATH=""),
                conn=conn,
                market="KRW-BTC",
                market_preflight=lambda _cfg: None,
            )
    finally:
        conn.close()


def test_operator_smoke_preflight_rejects_live_dry_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    try:
        with pytest.raises(LiveModeValidationError, match="LIVE_DRY_RUN=false"):
            validate_operator_smoke_preflight(
                cfg=_live_settings(db_path, LIVE_DRY_RUN=True, LIVE_REAL_ORDER_ARMED=False),
                conn=conn,
                market="KRW-BTC",
                market_preflight=lambda _cfg: None,
            )
    finally:
        conn.close()


def test_operator_smoke_preflight_rejects_unarmed_live(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    try:
        with pytest.raises(LiveModeValidationError, match="LIVE_REAL_ORDER_ARMED=true"):
            validate_operator_smoke_preflight(
                cfg=_live_settings(db_path, LIVE_REAL_ORDER_ARMED=False),
                conn=conn,
                market="KRW-BTC",
                market_preflight=lambda _cfg: None,
            )
    finally:
        conn.close()


def test_operator_smoke_preflight_rejects_kill_switch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = _live_roots(monkeypatch, tmp_path)
    conn = ensure_db(str(db_path))
    try:
        with pytest.raises(LiveModeValidationError, match="KILL_SWITCH=true"):
            validate_operator_smoke_preflight(
                cfg=_live_settings(db_path, KILL_SWITCH=True),
                conn=conn,
                market="KRW-BTC",
                market_preflight=lambda _cfg: None,
            )
    finally:
        conn.close()
