from __future__ import annotations

import json
from pathlib import Path

from bithumb_bot.paths import PathManager
from bithumb_bot.research.profiling import run_with_cprofile


def _manager(tmp_path: Path, monkeypatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def test_profiling_mode_writes_profile_artifact(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)

    result, observability = run_with_cprofile(
        func=lambda: {"return_pct": 1.0, "trade_count": 2},
        manager=manager,
        experiment_id="profile_unit",
        candidate_id="candidate_1",
        scenario_id="scenario_1",
        split_name="validation",
        candles_processed=12,
    )

    assert result == {"return_pct": 1.0, "trade_count": 2}
    path = Path(observability["profile_artifact_path"])
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["candidate_id"] == "candidate_1"
    assert payload["scenario_id"] == "scenario_1"
    assert payload["split_name"] == "validation"
    assert payload["candles_processed"] == 12
    assert "wall_seconds_total" in payload
    assert "wall_seconds_by_stage" in payload
    assert payload["top_hotspots"]


def test_profiling_mode_does_not_change_candidate_metrics(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)
    metrics = {"return_pct": 2.5, "trade_count": 3}

    profiled, _ = run_with_cprofile(
        func=lambda: dict(metrics),
        manager=manager,
        experiment_id="profile_metrics",
        candidate_id="candidate_1",
        scenario_id="scenario_1",
        split_name="train",
        candles_processed=4,
    )

    assert profiled == metrics
    assert dict(metrics) == {"return_pct": 2.5, "trade_count": 3}


def test_profiling_artifact_is_outside_repo(tmp_path, monkeypatch) -> None:
    manager = _manager(tmp_path, monkeypatch)

    _, observability = run_with_cprofile(
        func=lambda: {"ok": True},
        manager=manager,
        experiment_id="profile_paths",
        candidate_id="candidate_1",
        scenario_id="scenario_1",
        split_name="train",
        candles_processed=1,
    )

    path = Path(observability["profile_artifact_path"]).resolve()
    assert PathManager._is_within(path, manager.data_dir().resolve())
    assert not PathManager._is_within(path, manager.project_root.resolve())
