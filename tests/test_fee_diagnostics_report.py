from __future__ import annotations

import json
from pathlib import Path
import pytest

from bithumb_bot.app import main as app_main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.paths import PathManager
from bithumb_bot.reporting import FeeDiagnosticSummary, cmd_fee_diagnostics, fetch_fee_diagnostics
import bithumb_bot.reporting as reporting


def _set_managed_runtime_paths(monkeypatch, tmp_path: Path) -> PathManager:
    runtime_root = tmp_path / "runtime"
    monkeypatch.setenv("ENV_ROOT", str((runtime_root / "env").resolve()))
    monkeypatch.setenv("RUN_ROOT", str((runtime_root / "run").resolve()))
    monkeypatch.setenv("DATA_ROOT", str((runtime_root / "data").resolve()))
    monkeypatch.setenv("LOG_ROOT", str((runtime_root / "logs").resolve()))
    monkeypatch.setenv("BACKUP_ROOT", str((runtime_root / "backup").resolve()))
    manager = PathManager.from_env(Path.cwd())
    monkeypatch.setattr(reporting, "PATH_MANAGER", manager)
    return manager


def test_fee_diagnostics_metrics_are_computed_correctly(tmp_path, monkeypatch):
    _set_managed_runtime_paths(monkeypatch, tmp_path)
    db_path = str(tmp_path / "fee-diagnostics.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("coid-1", "ex-1", "FILLED", "BUY", 100_000_000.0, 0.001, 0.001, 1, 1),
        )
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            ("coid-2", "ex-2", "FILLED", "SELL", 110_000_000.0, 0.001, 0.001, 2, 2),
        )
        conn.executemany(
            """
            INSERT INTO fills(client_order_id, fill_id, fill_ts, price, qty, fee)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("coid-1", "fill-1", 10, 100_000_000.0, 0.001, 40.0),  # 4.0 bps
                ("coid-2", "fill-2", 20, 110_000_000.0, 0.001, 0.0),   # 0 bps
            ],
        )
        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                id, pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
                entry_fill_id, exit_fill_id, entry_ts, exit_ts, matched_qty, entry_price, exit_price,
                gross_pnl, fee_total, net_pnl, holding_time_sec, strategy_name, entry_decision_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "BTC_KRW",
                1,
                2,
                "coid-1",
                "coid-2",
                "fill-1",
                "fill-2",
                10,
                20,
                0.001,
                100_000_000.0,
                110_000_000.0,
                950.0,
                40.0,
                910.0,
                60.0,
                "strategy-test",
                None,
            ),
        )
        conn.commit()

        summary = fetch_fee_diagnostics(conn, fill_limit=10, roundtrip_limit=10, estimated_fee_rate=0.0005)
    finally:
        conn.close()

    assert summary.fill_count == 2
    assert summary.fee_zero_count == 1
    assert summary.fee_zero_ratio == 0.5
    assert summary.average_fee_rate == 40.0 / (100_000.0 + 110_000.0)
    assert summary.average_fee_bps == 2.0
    assert summary.median_fee_bps == 2.0
    assert summary.estimated_minus_actual_bps == (0.0005 - summary.average_fee_rate) * 10000.0
    assert summary.roundtrip_fee_total == 40.0
    assert summary.pnl_before_fee_total == 950.0
    assert summary.pnl_after_fee_total == 910.0
    assert summary.pnl_fee_drag_total == 40.0


def test_fee_diagnostics_handles_empty_data(tmp_path, monkeypatch, capsys):
    manager = _set_managed_runtime_paths(monkeypatch, tmp_path)
    db_path = str(tmp_path / "fee-diagnostics-empty.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    conn.close()

    cmd_fee_diagnostics(fill_limit=5, roundtrip_limit=5, estimated_fee_rate=0.0004, as_json=False)
    out = capsys.readouterr().out
    assert "[FEE-DIAGNOSTICS]" in out
    assert "avg_fee_rate=-" in out
    assert "no fills found in the selected window" in out
    assert manager.fee_diagnostics_report_path().exists()


def test_fee_diagnostics_cli_json_smoke(tmp_path, monkeypatch, capsys):
    manager = _set_managed_runtime_paths(monkeypatch, tmp_path)
    db_path = str(tmp_path / "fee-diagnostics-cli.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    conn.close()

    app_main(["fee-diagnostics", "--fill-limit", "3", "--roundtrip-limit", "2", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["fill_window"]["limit"] == 3
    assert payload["roundtrip_window"]["limit"] == 2
    assert "fills" in payload
    assert "roundtrip" in payload
    assert "fee_rate_drift" in payload
    assert manager.fee_diagnostics_report_path().exists()


def test_fee_diagnostics_exposes_fee_rate_drift_operational_fields(tmp_path, monkeypatch, capsys):
    _set_managed_runtime_paths(monkeypatch, tmp_path)
    db_path = str(tmp_path / "fee-diagnostics-drift.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0025)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 10_000.0)

    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO broker_fill_observations(
                event_ts, client_order_id, exchange_order_id, fill_id, fill_ts, side,
                price, qty, fee, fee_status, fee_source, fee_confidence, accounting_status, source,
                fee_provenance, fee_validation_reason, fee_validation_checks
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1_777_104_360_500,
                "live_1777104360000_buy_aee4c564",
                "C0101000002949768709",
                "C0101000000983820316",
                1_777_104_360_321,
                "BUY",
                115_465_000.0,
                0.00059998,
                27.71,
                "validated_order_level_paid_fee",
                "order_level_paid_fee",
                "validated",
                "accounting_complete",
                "live_application_fee_rate_warning",
                "order_level_paid_fee_validated_single_fill_fee_rate_warning",
                "order_level_paid_fee_validated_single_fill_expected_fee_rate_mismatch",
                json.dumps(
                    {
                        "single_fill": True,
                        "paid_fee_present": True,
                        "executed_volume_match": True,
                        "executed_funds_match": True,
                        "expected_fee_rate_match": False,
                        "expected_fee_rate_warning": True,
                        "identifiers_match": True,
                        "material_notional_suspicious": True,
                    },
                    sort_keys=True,
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    cmd_fee_diagnostics(as_json=False)
    out = capsys.readouterr().out
    assert "[FEE-RATE-DRIFT]" in out
    assert "configured_fee_rate=0.002500" in out
    assert "configured_fee_bps=25.000" in out
    assert "observed_fee_bps_median=4.000 bps" in out
    assert "observed_fee_sample_count=1" in out
    assert "fee_rate_deviation_pct=525.02%" in out
    assert "expected_fee_rate_warning_count=1" in out
    assert "fee_pending_count=0" in out
    assert "fee_pending_accounting_repair_count=0" in out
    assert "position_authority_repair_count=0" in out
    assert "diagnostic_only_vs_startup_blocking=diagnostic_only" in out
    assert "startup_impact=diagnostic_only_without_active_fee_pending" in out
    assert "operator_action=review_fee_diagnostics" in out
    assert "recommended_command=uv run python bot.py fee-diagnostics" in out

    cmd_fee_diagnostics(as_json=True)
    payload = json.loads(capsys.readouterr().out)
    assert payload["fee_rate_drift"]["configured_fee_rate"] == 0.0025
    assert payload["fee_rate_drift"]["configured_fee_bps"] == 25.0
    assert payload["fee_rate_drift"]["observed_fee_bps_median"] == pytest.approx(3.9999023798635354)
    assert payload["fee_rate_drift"]["observed_fee_sample_count"] == 1
    assert payload["fee_rate_drift"]["expected_fee_rate_warning_count"] == 1
    assert payload["fee_rate_drift"]["fee_pending_count"] == 0
    assert payload["fee_rate_drift"]["fee_pending_accounting_repair_count"] == 0
    assert payload["fee_rate_drift"]["position_authority_repair_count"] == 0
    assert payload["fee_rate_drift"]["diagnostic_only_vs_startup_blocking"] == "diagnostic_only"
    assert payload["fee_rate_drift"]["operator_action"] == "review_fee_diagnostics"
    assert payload["fee_rate_drift"]["recommended_command"] == "uv run python bot.py fee-diagnostics"


def test_fee_diagnostics_default_estimate_uses_live_fee_rate_in_live_mode(monkeypatch):
    captured: dict[str, float] = {}

    class _DummyConn:
        def close(self) -> None:
            return None

    def _fake_fetch_fee_diagnostics(conn, *, fill_limit, roundtrip_limit, estimated_fee_rate):
        captured["estimated_fee_rate"] = float(estimated_fee_rate)
        return FeeDiagnosticSummary(
            fill_count=0,
            fills_with_notional=0,
            fee_zero_count=0,
            fee_zero_ratio=0.0,
            average_fee_rate=None,
            average_fee_bps=None,
            median_fee_bps=None,
            estimated_fee_rate=float(estimated_fee_rate),
            estimated_minus_actual_bps=None,
            total_fee_recent_fills=0.0,
            total_notional_recent_fills=0.0,
            roundtrip_count=0,
            roundtrip_fee_total=0.0,
            pnl_before_fee_total=0.0,
            pnl_after_fee_total=0.0,
            pnl_fee_drag_total=0.0,
            notes=[],
        )

    monkeypatch.setattr("bithumb_bot.reporting.ensure_db", lambda: _DummyConn())
    monkeypatch.setattr("bithumb_bot.reporting.fetch_fee_diagnostics", _fake_fetch_fee_diagnostics)
    orig_mode = settings.MODE
    orig_live = settings.LIVE_FEE_RATE_ESTIMATE
    orig_paper = settings.PAPER_FEE_RATE
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0025)
        object.__setattr__(settings, "PAPER_FEE_RATE", 0.0004)
        cmd_fee_diagnostics(as_json=True)
    finally:
        object.__setattr__(settings, "MODE", orig_mode)
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", orig_live)
        object.__setattr__(settings, "PAPER_FEE_RATE", orig_paper)

    assert captured["estimated_fee_rate"] == 0.0025


def test_fee_diagnostics_default_estimate_uses_paper_fee_rate_in_non_live_mode(monkeypatch):
    captured: dict[str, float] = {}

    class _DummyConn:
        def close(self) -> None:
            return None

    def _fake_fetch_fee_diagnostics(conn, *, fill_limit, roundtrip_limit, estimated_fee_rate):
        captured["estimated_fee_rate"] = float(estimated_fee_rate)
        return FeeDiagnosticSummary(
            fill_count=0,
            fills_with_notional=0,
            fee_zero_count=0,
            fee_zero_ratio=0.0,
            average_fee_rate=None,
            average_fee_bps=None,
            median_fee_bps=None,
            estimated_fee_rate=float(estimated_fee_rate),
            estimated_minus_actual_bps=None,
            total_fee_recent_fills=0.0,
            total_notional_recent_fills=0.0,
            roundtrip_count=0,
            roundtrip_fee_total=0.0,
            pnl_before_fee_total=0.0,
            pnl_after_fee_total=0.0,
            pnl_fee_drag_total=0.0,
            notes=[],
        )

    monkeypatch.setattr("bithumb_bot.reporting.ensure_db", lambda: _DummyConn())
    monkeypatch.setattr("bithumb_bot.reporting.fetch_fee_diagnostics", _fake_fetch_fee_diagnostics)
    orig_mode = settings.MODE
    orig_live = settings.LIVE_FEE_RATE_ESTIMATE
    orig_paper = settings.PAPER_FEE_RATE
    try:
        object.__setattr__(settings, "MODE", "paper")
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0025)
        object.__setattr__(settings, "PAPER_FEE_RATE", 0.0004)
        cmd_fee_diagnostics(as_json=True)
    finally:
        object.__setattr__(settings, "MODE", orig_mode)
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", orig_live)
        object.__setattr__(settings, "PAPER_FEE_RATE", orig_paper)

    assert captured["estimated_fee_rate"] == 0.0004


def test_fee_diagnostics_explicit_estimate_overrides_mode_defaults(monkeypatch):
    captured: dict[str, float] = {}

    class _DummyConn:
        def close(self) -> None:
            return None

    def _fake_fetch_fee_diagnostics(conn, *, fill_limit, roundtrip_limit, estimated_fee_rate):
        captured["estimated_fee_rate"] = float(estimated_fee_rate)
        return FeeDiagnosticSummary(
            fill_count=0,
            fills_with_notional=0,
            fee_zero_count=0,
            fee_zero_ratio=0.0,
            average_fee_rate=None,
            average_fee_bps=None,
            median_fee_bps=None,
            estimated_fee_rate=float(estimated_fee_rate),
            estimated_minus_actual_bps=None,
            total_fee_recent_fills=0.0,
            total_notional_recent_fills=0.0,
            roundtrip_count=0,
            roundtrip_fee_total=0.0,
            pnl_before_fee_total=0.0,
            pnl_after_fee_total=0.0,
            pnl_fee_drag_total=0.0,
            notes=[],
        )

    monkeypatch.setattr("bithumb_bot.reporting.ensure_db", lambda: _DummyConn())
    monkeypatch.setattr("bithumb_bot.reporting.fetch_fee_diagnostics", _fake_fetch_fee_diagnostics)
    orig_mode = settings.MODE
    orig_live = settings.LIVE_FEE_RATE_ESTIMATE
    orig_paper = settings.PAPER_FEE_RATE
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0025)
        object.__setattr__(settings, "PAPER_FEE_RATE", 0.0004)
        cmd_fee_diagnostics(estimated_fee_rate=0.0011, as_json=True)
    finally:
        object.__setattr__(settings, "MODE", orig_mode)
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", orig_live)
        object.__setattr__(settings, "PAPER_FEE_RATE", orig_paper)

    assert captured["estimated_fee_rate"] == 0.0011
