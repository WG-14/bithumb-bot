from __future__ import annotations

import sqlite3

import pytest

from bithumb_bot import db_schema
from bithumb_bot.operator_commands import cmd_validate_db
from bithumb_bot.config import settings
from bithumb_bot.db_core import (
    ACCOUNTING_PROJECTION_MODEL,
    OPERATIONAL_SCHEMA_VERSION,
    SchemaValidationError,
    assert_current_schema,
    build_runtime_schema_diagnostics,
    ensure_db,
    get_portfolio,
    get_portfolio_breakdown,
    init_portfolio,
    set_portfolio,
    set_portfolio_breakdown,
)


def test_schema_bootstrap_creates_portfolio_split_columns(tmp_path):
    conn = ensure_db(str(tmp_path / "bootstrap.sqlite"))
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(portfolio)").fetchall()}
    meta = conn.execute(
        "SELECT schema_version, accounting_projection_model FROM schema_meta WHERE key='operational_schema'"
    ).fetchone()
    assert_current_schema(conn)
    conn.close()

    assert "cash_available" in cols
    assert "cash_locked" in cols
    assert "asset_available" in cols
    assert "asset_locked" in cols
    assert int(meta["schema_version"]) == OPERATIONAL_SCHEMA_VERSION
    assert str(meta["accounting_projection_model"]) == ACCOUNTING_PROJECTION_MODEL


def test_empty_db_opened_by_ensure_db_produces_validated_current_schema(tmp_path):
    db_path = tmp_path / "empty.sqlite"
    db_path.touch()

    conn = ensure_db(str(db_path))
    try:
        assert_current_schema(conn)
        diagnostics = build_runtime_schema_diagnostics(conn)
    finally:
        conn.close()

    assert diagnostics["status"] == "PASS"
    assert diagnostics["schema_version"] == OPERATIONAL_SCHEMA_VERSION
    assert diagnostics["expected_schema_version"] == OPERATIONAL_SCHEMA_VERSION
    assert diagnostics["observed_schema_version"] == OPERATIONAL_SCHEMA_VERSION
    assert diagnostics["accounting_projection_model"] == ACCOUNTING_PROJECTION_MODEL
    assert diagnostics["expected_accounting_projection_model"] == ACCOUNTING_PROJECTION_MODEL
    assert diagnostics["observed_accounting_projection_model"] == ACCOUNTING_PROJECTION_MODEL


def test_current_db_reopened_by_ensure_db_still_validates(tmp_path):
    db_path = tmp_path / "current.sqlite"
    conn = ensure_db(str(db_path))
    conn.close()

    reopened = ensure_db(str(db_path))
    try:
        assert_current_schema(reopened)
        diagnostics = build_runtime_schema_diagnostics(reopened)
    finally:
        reopened.close()

    assert diagnostics["status"] == "PASS"


def test_deprecated_db_schema_module_no_longer_creates_cash_qty_portfolio(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "deprecated-module.sqlite"))
    conn.row_factory = sqlite3.Row
    with pytest.warns(DeprecationWarning):
        db_schema.ensure_schema(conn)
    with pytest.warns(DeprecationWarning):
        db_schema.init_portfolio(conn, 123_456.0)
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(portfolio)").fetchall()}
    row = conn.execute(
        "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    conn.close()

    assert "cash" not in cols
    assert "qty" not in cols
    assert {"cash_krw", "asset_qty", "cash_available", "cash_locked", "asset_available", "asset_locked"} <= cols
    assert float(row["cash_krw"]) == 123_456.0
    assert float(row["cash_available"]) == 123_456.0


def test_schema_bootstrap_creates_candles_pair_interval_ts_index(tmp_path):
    conn = ensure_db(str(tmp_path / "candles-index.sqlite"))
    try:
        index_rows = conn.execute("PRAGMA index_list(candles)").fetchall()
        indexed_columns = []
        for index_row in index_rows:
            index_name = str(index_row[1])
            columns = [
                str(column_row[2])
                for column_row in conn.execute(f"PRAGMA index_info({index_name})").fetchall()
            ]
            indexed_columns.append(tuple(columns))
    finally:
        conn.close()

    assert ("pair", "interval", "ts") in indexed_columns


def test_portfolio_read_write_supports_breakdown_shape(tmp_path):
    conn = ensure_db(str(tmp_path / "shape.sqlite"))
    init_portfolio(conn)

    set_portfolio_breakdown(
        conn,
        cash_available=123_000.0,
        cash_locked=7_000.0,
        asset_available=0.25,
        asset_locked=0.75,
    )

    cash_a, cash_l, asset_a, asset_l = get_portfolio_breakdown(conn)
    cash_total, asset_total = get_portfolio(conn)
    raw = conn.execute(
        "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    conn.close()

    assert (cash_a, cash_l, asset_a, asset_l) == (123_000.0, 7_000.0, 0.25, 0.75)
    assert cash_total == 130_000.0
    assert asset_total == 1.0
    assert float(raw["cash_krw"]) == 130_000.0
    assert float(raw["asset_qty"]) == 1.0


def test_portfolio_schema_upgrade_backfills_from_legacy_aggregate_columns(tmp_path):
    db_path = tmp_path / "legacy.sqlite"

    legacy = sqlite3.connect(str(db_path))
    legacy.execute(
        """
        CREATE TABLE portfolio (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cash_krw REAL NOT NULL,
            asset_qty REAL NOT NULL
        )
        """
    )
    legacy.execute("INSERT INTO portfolio(id, cash_krw, asset_qty) VALUES (1, 456789.0, 0.42)")
    legacy.commit()
    legacy.close()

    conn = ensure_db(str(db_path))
    row = conn.execute(
        "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    conn.close()

    assert float(row["cash_krw"]) == 456789.0
    assert float(row["asset_qty"]) == 0.42
    assert float(row["cash_available"]) == 456789.0
    assert float(row["cash_locked"]) == 0.0
    assert float(row["asset_available"]) == 0.42
    assert float(row["asset_locked"]) == 0.0


def test_old_cash_qty_portfolio_schema_is_rejected_before_runtime_use(tmp_path):
    db_path = tmp_path / "old-cash-qty.sqlite"
    legacy = sqlite3.connect(str(db_path))
    legacy.execute(
        """
        CREATE TABLE portfolio (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cash REAL NOT NULL,
            qty REAL NOT NULL
        )
        """
    )
    legacy.execute("INSERT INTO portfolio(id, cash, qty) VALUES (1, 456789.0, 0.42)")
    legacy.commit()

    diagnostics = build_runtime_schema_diagnostics(legacy)
    legacy.close()

    assert diagnostics["legacy_schema_detected"] is True
    assert diagnostics["status"] == "FAIL"
    assert diagnostics["expected_schema_version"] == OPERATIONAL_SCHEMA_VERSION
    assert diagnostics["observed_schema_version"] is None
    assert diagnostics["expected_accounting_projection_model"] == ACCOUNTING_PROJECTION_MODEL
    assert diagnostics["observed_accounting_projection_model"] is None
    assert "restore_current_backup_or_run_reviewed_legacy_cash_qty_migration" in diagnostics["recommended_action"]
    with pytest.raises(SchemaValidationError, match=r"portfolio\(cash, qty\)"):
        ensure_db(str(db_path))


def test_malformed_partial_portfolio_schema_fails_with_clear_schema_error(tmp_path):
    db_path = tmp_path / "malformed-portfolio.sqlite"
    malformed = sqlite3.connect(str(db_path))
    malformed.execute(
        """
        CREATE TABLE portfolio (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cash_krw REAL NOT NULL
        )
        """
    )
    malformed.commit()
    malformed.close()

    with pytest.raises(SchemaValidationError, match="portfolio table is missing required aggregate column"):
        ensure_db(str(db_path))

    malformed = sqlite3.connect(str(db_path))
    malformed.row_factory = sqlite3.Row
    try:
        diagnostics = build_runtime_schema_diagnostics(malformed)
    finally:
        malformed.close()

    assert diagnostics["status"] == "FAIL"
    assert diagnostics["malformed_portfolio_detected"] is True
    assert "portfolio" in diagnostics["missing_columns"]
    assert "asset_qty" in diagnostics["missing_columns"]["portfolio"]
    assert diagnostics["observed_schema_version"] is None


def test_schema_diagnostics_report_observed_metadata_mismatch_without_repairing(tmp_path):
    db_path = tmp_path / "metadata-mismatch.sqlite"
    conn = ensure_db(str(db_path))
    conn.execute(
        """
        UPDATE schema_meta
        SET schema_version = ?, accounting_projection_model = ?
        WHERE key = ?
        """,
        (OPERATIONAL_SCHEMA_VERSION + 1, "unexpected_projection_model", "operational_schema"),
    )
    conn.commit()

    try:
        diagnostics = build_runtime_schema_diagnostics(conn)
    finally:
        conn.close()

    assert diagnostics["status"] == "FAIL"
    assert diagnostics["expected_schema_version"] == OPERATIONAL_SCHEMA_VERSION
    assert diagnostics["observed_schema_version"] == OPERATIONAL_SCHEMA_VERSION + 1
    assert diagnostics["expected_accounting_projection_model"] == ACCOUNTING_PROJECTION_MODEL
    assert diagnostics["observed_accounting_projection_model"] == "unexpected_projection_model"
    assert any("schema_meta version mismatch" in error for error in diagnostics["validation_errors"])
    assert any("schema_meta accounting projection model mismatch" in error for error in diagnostics["validation_errors"])


def test_validate_db_cli_plain_output_includes_expected_and_observed_metadata(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "validate-db.sqlite")
    ensure_db(db_path).close()
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    exit_code = cmd_validate_db(as_json=False)

    out = capsys.readouterr().out
    assert exit_code == 0
    assert f"expected_schema_version={OPERATIONAL_SCHEMA_VERSION}" in out
    assert f"observed_schema_version={OPERATIONAL_SCHEMA_VERSION}" in out
    assert f"expected_accounting_projection_model={ACCOUNTING_PROJECTION_MODEL}" in out
    assert f"observed_accounting_projection_model={ACCOUNTING_PROJECTION_MODEL}" in out
    assert "diagnostic_schema_status=PASS" in out


def test_execution_quality_diagnostic_table_missing_warns_without_blocking_startup(tmp_path):
    db_path = tmp_path / "missing-diagnostic-table.sqlite"
    conn = ensure_db(str(db_path))
    try:
        conn.execute("DROP TABLE execution_quality_events")
        conn.commit()
        assert_current_schema(conn)
        diagnostics = build_runtime_schema_diagnostics(conn)
    finally:
        conn.close()

    assert diagnostics["status"] == "PASS"
    assert diagnostics["diagnostic_schema_status"] == "WARN"
    assert diagnostics["diagnostic_missing_tables"] == ["execution_quality_events"]
    assert diagnostics["diagnostic_recommended_command"] == "execution-quality-report"


def test_execution_quality_old_diagnostic_schema_warns_with_refresh_command(tmp_path):
    db_path = tmp_path / "old-diagnostic-schema.sqlite"
    conn = ensure_db(str(db_path))
    try:
        conn.execute("DROP TABLE execution_quality_events")
        conn.execute(
            """
            CREATE TABLE execution_quality_events (
                client_order_id TEXT,
                canonical_execution_kind TEXT,
                market_equivalent INTEGER NOT NULL DEFAULT 0,
                quality_status TEXT NOT NULL DEFAULT 'insufficient_evidence'
            )
            """
        )
        conn.commit()
        assert_current_schema(conn)
        diagnostics = build_runtime_schema_diagnostics(conn)
    finally:
        conn.close()

    assert diagnostics["status"] == "PASS"
    assert diagnostics["diagnostic_schema_status"] == "WARN"
    assert diagnostics["diagnostic_recommended_command"] == "execution-quality-report"
    missing = diagnostics["diagnostic_missing_columns"]["execution_quality_events"]
    assert "semantic_evidence_quality" in missing
    assert "remaining_qty_materiality_reason" in missing


def test_validate_db_prints_execution_quality_diagnostic_schema_warning(tmp_path, monkeypatch, capsys):
    db_path = str(tmp_path / "validate-db-diagnostic-warning.sqlite")
    conn = ensure_db(db_path)
    try:
        conn.execute("DROP TABLE execution_quality_events")
        conn.commit()
    finally:
        conn.close()
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    exit_code = cmd_validate_db(as_json=False)

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "db_schema_status=PASS" in out
    assert "diagnostic_schema_status=WARN" in out
    assert "diagnostic_recommended_command=execution-quality-report" in out
    assert "diagnostic_schema_warning=missing table: execution_quality_events" in out


def test_portfolio_total_mismatch_fails_schema_validation_before_runtime_use(tmp_path):
    db_path = tmp_path / "portfolio-mismatch.sqlite"
    conn = ensure_db(str(db_path))
    conn.execute(
        """
        INSERT INTO portfolio(id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked)
        VALUES (1, 100.0, 1.0, 90.0, 0.0, 1.0, 0.0)
        """
    )
    conn.commit()
    conn.close()

    with pytest.raises(SchemaValidationError, match="portfolio cash total mismatch"):
        ensure_db(str(db_path))


def test_set_portfolio_legacy_api_still_works(tmp_path):
    conn = ensure_db(str(tmp_path / "legacy_api.sqlite"))
    init_portfolio(conn)

    set_portfolio(conn, cash_krw=10_000.0, asset_qty=0.5)

    row = conn.execute(
        "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
    ).fetchone()
    conn.close()

    assert float(row["cash_krw"]) == 10_000.0
    assert float(row["asset_qty"]) == 0.5
    assert float(row["cash_available"]) == 10_000.0
    assert float(row["cash_locked"]) == 0.0
    assert float(row["asset_available"]) == 0.5
    assert float(row["asset_locked"]) == 0.0
