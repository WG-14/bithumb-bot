from __future__ import annotations

from pathlib import Path

import pytest

from bithumb_bot.paths import PathManager, PathPolicyError


def _set_roots(monkeypatch: pytest.MonkeyPatch, root: Path, mode: str) -> None:
    monkeypatch.setenv("MODE", mode)
    monkeypatch.setenv("ENV_ROOT", str(root / "env"))
    monkeypatch.setenv("RUN_ROOT", str(root / "run"))
    monkeypatch.setenv("DATA_ROOT", str(root / "data"))
    monkeypatch.setenv("LOG_ROOT", str(root / "logs"))
    monkeypatch.setenv("BACKUP_ROOT", str(root / "backup"))


def test_path_manager_builds_mode_scoped_layout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_roots(monkeypatch, tmp_path, "paper")
    pm = PathManager.from_env(project_root=tmp_path / "repo")

    assert pm.run_dir() == tmp_path / "run" / "paper"
    assert pm.data_dir() == tmp_path / "data" / "paper"
    assert pm.log_dir() == tmp_path / "logs" / "paper"
    assert pm.primary_db_path() == tmp_path / "data" / "paper" / "trades" / "paper.sqlite"


def test_path_manager_separates_paper_and_live(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_roots(monkeypatch, tmp_path, "paper")
    paper = PathManager.from_env(project_root=tmp_path / "repo")

    _set_roots(monkeypatch, tmp_path, "live")
    live = PathManager.from_env(project_root=tmp_path / "repo")

    assert paper.run_lock_path() != live.run_lock_path()
    assert "/paper/" in str(paper.run_lock_path()).replace("\\", "/")
    assert "/live/" in str(live.run_lock_path()).replace("\\", "/")


def test_path_manager_uses_topic_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_roots(monkeypatch, tmp_path, "live")
    pm = PathManager.from_env(project_root=tmp_path / "repo")

    assert pm.raw_path("market", day="2026-03-30") == tmp_path / "data" / "live" / "raw" / "market" / "market_2026-03-30.jsonl"
    assert pm.derived_path("validation", day="2026-03-30", ext="parquet") == tmp_path / "data" / "live" / "derived" / "validation" / "validation_2026-03-30.parquet"
    assert pm.trade_data_path("fills", day="2026-03-30") == tmp_path / "data" / "live" / "trades" / "fills" / "fills_2026-03-30.jsonl"
    assert pm.report_path("ops", day="2026-03-30") == tmp_path / "data" / "live" / "reports" / "ops" / "ops_2026-03-30.json"
    assert pm.log_path("errors", day="2026-03-30") == tmp_path / "logs" / "live" / "errors" / "errors_2026-03-30.log"


def test_log_path_kinds_are_strictly_partitioned(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_roots(monkeypatch, tmp_path, "paper")
    pm = PathManager.from_env(project_root=tmp_path / "repo")

    assert pm.app_log_path(day="2026-03-30") == tmp_path / "logs" / "paper" / "app" / "app_2026-03-30.log"
    assert pm.strategy_log_path(day="2026-03-30") == tmp_path / "logs" / "paper" / "strategy" / "strategy_2026-03-30.log"
    assert pm.orders_log_path(day="2026-03-30") == tmp_path / "logs" / "paper" / "orders" / "orders_2026-03-30.log"
    assert pm.fills_log_path(day="2026-03-30") == tmp_path / "logs" / "paper" / "fills" / "fills_2026-03-30.log"
    assert pm.error_log_path(day="2026-03-30") == tmp_path / "logs" / "paper" / "errors" / "errors_2026-03-30.log"
    assert pm.audit_log_path(day="2026-03-30") == tmp_path / "logs" / "paper" / "audit" / "audit_2026-03-30.log"

    with pytest.raises(PathPolicyError):
        pm.log_path("unknown", day="2026-03-30")


def test_trade_report_derived_entrypoints_match_storage_contract(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _set_roots(monkeypatch, tmp_path, "live")
    pm = PathManager.from_env(project_root=tmp_path / "repo")

    assert pm.orders_artifact_path(day="2026-03-30") == tmp_path / "data" / "live" / "trades" / "orders" / "orders_2026-03-30.jsonl"
    assert pm.fills_artifact_path(day="2026-03-30") == tmp_path / "data" / "live" / "trades" / "fills" / "fills_2026-03-30.jsonl"
    assert pm.balance_snapshot_path(day="2026-03-30") == tmp_path / "data" / "live" / "trades" / "balance_snapshots" / "balance_snapshots_2026-03-30.jsonl"
    assert pm.portfolio_snapshot_path(day="2026-03-30") == tmp_path / "data" / "live" / "trades" / "portfolio_snapshots" / "portfolio_snapshots_2026-03-30.jsonl"
    assert pm.reconcile_event_path(day="2026-03-30") == tmp_path / "data" / "live" / "trades" / "reconcile_events" / "reconcile_events_2026-03-30.jsonl"
    assert pm.ops_report_path(day="2026-03-30") == tmp_path / "data" / "live" / "reports" / "ops_report" / "ops_report_2026-03-30.json"
    assert pm.strategy_validation_report_path(day="2026-03-30") == tmp_path / "data" / "live" / "reports" / "strategy_validation" / "strategy_validation_2026-03-30.json"
    assert pm.fee_diagnostics_report_path(day="2026-03-30") == tmp_path / "data" / "live" / "reports" / "fee_diagnostics" / "fee_diagnostics_2026-03-30.json"
    assert pm.recovery_report_path(day="2026-03-30") == tmp_path / "data" / "live" / "reports" / "recovery_report" / "recovery_report_2026-03-30.json"
    assert pm.feature_snapshot_path(day="2026-03-30") == tmp_path / "data" / "live" / "derived" / "feature_snapshot" / "feature_snapshot_2026-03-30.jsonl"
    assert pm.signal_trace_path(day="2026-03-30") == tmp_path / "data" / "live" / "derived" / "signal_trace" / "signal_trace_2026-03-30.jsonl"


def test_live_blocks_repo_relative_roots(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()

    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("ENV_ROOT", "env")
    monkeypatch.setenv("RUN_ROOT", "run")
    monkeypatch.setenv("DATA_ROOT", "data")
    monkeypatch.setenv("LOG_ROOT", "logs")
    monkeypatch.setenv("BACKUP_ROOT", "backup")

    with pytest.raises(PathPolicyError):
        PathManager.from_env(project_root=repo_root)


@pytest.mark.parametrize(
    ("env_key", "env_value"),
    [
        ("LOG_ROOT", "logs"),
        ("BACKUP_ROOT", "backup"),
    ],
)
def test_live_rejects_relative_log_and_backup_roots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_key: str,
    env_value: str,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _set_roots(monkeypatch, tmp_path, "live")
    monkeypatch.setenv(env_key, env_value)

    with pytest.raises(PathPolicyError) as exc:
        PathManager.from_env(project_root=repo_root)

    assert f"{env_key} must be an absolute path when MODE=live" in str(exc.value)


@pytest.mark.parametrize(("env_key", "child"), [("DATA_ROOT", "data"), ("LOG_ROOT", "logs"), ("BACKUP_ROOT", "backup")])
def test_live_rejects_repo_internal_roots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_key: str,
    child: str,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _set_roots(monkeypatch, tmp_path, "live")
    monkeypatch.setenv(env_key, str((repo_root / child).resolve()))

    with pytest.raises(PathPolicyError) as exc:
        PathManager.from_env(project_root=repo_root)

    assert f"{env_key} must be outside repository when MODE=live" in str(exc.value)


def test_live_rejects_paper_scoped_root_segment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _set_roots(monkeypatch, tmp_path, "live")
    monkeypatch.setenv("DATA_ROOT", str((tmp_path / "runtime" / "paper" / "data").resolve()))

    with pytest.raises(PathPolicyError) as exc:
        PathManager.from_env(project_root=repo_root)

    assert "DATA_ROOT must not contain a paper-scoped path segment when MODE=live" in str(exc.value)


@pytest.mark.parametrize("env_key", ["LOG_ROOT", "BACKUP_ROOT"])
def test_live_rejects_paper_scoped_log_and_backup_segments(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_key: str,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    _set_roots(monkeypatch, tmp_path, "live")
    monkeypatch.setenv(env_key, str((tmp_path / "runtime" / "paper" / env_key.lower()).resolve()))

    with pytest.raises(PathPolicyError) as exc:
        PathManager.from_env(project_root=repo_root)

    assert f"{env_key} must not contain a paper-scoped path segment when MODE=live" in str(exc.value)
