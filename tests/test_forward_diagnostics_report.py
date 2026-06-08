from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot.paths import PathConfig, PathManager
from bithumb_bot.research.feature_bucket_metrics import FeatureBucketMetric
from bithumb_bot.research.forward_diagnostics import ForwardDiagnosticsResult
from bithumb_bot.research.forward_diagnostics_report import write_forward_diagnostics_report


def _manager(tmp_path: Path) -> PathManager:
    return PathManager(
        project_root=Path(__file__).resolve().parents[1],
        config=PathConfig(
            mode="paper",
            env_root=tmp_path / "env",
            run_root=tmp_path / "run",
            data_root=tmp_path / "data",
            log_root=tmp_path / "logs",
            backup_root=tmp_path / "backup",
            archive_root=tmp_path / "archive",
        ),
    )


def _manifest():
    return SimpleNamespace(experiment_id="exp1", manifest_hash=lambda: "sha256:" + "1" * 64)


def _metric(value: float) -> FeatureBucketMetric:
    return FeatureBucketMetric(
        feature_name="sma_gap",
        bucket_id="q00",
        bucket_label="quantile 1/1",
        horizon_label="1c",
        count=1,
        mean_forward_return=value,
        median_forward_return=value,
        win_rate=1.0,
        p10_forward_return=value,
        p90_forward_return=value,
        mean_mfe=0.02,
        median_mfe=0.02,
        mean_mae=-0.01,
        median_mae=-0.01,
        mfe_mae_ratio=2.0,
        warnings=(),
    )


def _result(value: float = 0.01) -> ForwardDiagnosticsResult:
    return ForwardDiagnosticsResult(
        experiment_id="exp1",
        split_name="train",
        feature_names=("sma_gap",),
        horizon_steps=(1,),
        bucket_method="quantile:1",
        entry_price_mode="next_open",
        sample_count=1,
        target_count=1,
        feature_bucket_metrics=(_metric(value),),
        feature_horizon_metrics=(_metric(value),),
        warnings=(),
    )


def test_forward_diagnostics_report_writes_diagnostic_only_flags(tmp_path: Path) -> None:
    report = write_forward_diagnostics_report(manager=_manager(tmp_path), manifest=_manifest(), result=_result())

    assert report["artifact_type"] == "forward_return_diagnostic_report"
    assert report["diagnostic_only"] is True
    assert report["promotion_evidence"] is False
    assert report["approved_profile_evidence"] is False
    assert report["live_readiness_evidence"] is False
    assert report["capital_allocation_evidence"] is False


def test_forward_diagnostics_report_writes_under_research_report_and_derived_paths(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    write_forward_diagnostics_report(manager=manager, manifest=_manifest(), result=_result())
    base = manager.data_dir()

    assert (base / "reports/research/exp1/forward_diagnostics_report.json").exists()
    assert (base / "derived/research/exp1/forward_diagnostics/feature_bucket_metrics.csv").exists()
    assert (base / "derived/research/exp1/forward_diagnostics/feature_horizon_metrics.csv").exists()
    assert (base / "derived/research/exp1/forward_diagnostics/warnings.json").exists()


def test_forward_diagnostics_report_does_not_use_candidate_report_fields(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    write_forward_diagnostics_report(manager=manager, manifest=_manifest(), result=_result())
    payload = json.loads((manager.data_dir() / "reports/research/exp1/forward_diagnostics_report.json").read_text())

    assert "candidate_count" not in payload
    assert "derived_candidates_hash" not in payload


def test_forward_diagnostics_report_content_hash_changes_when_metrics_change(tmp_path: Path) -> None:
    first = write_forward_diagnostics_report(manager=_manager(tmp_path / "a"), manifest=_manifest(), result=_result(0.01))
    second = write_forward_diagnostics_report(manager=_manager(tmp_path / "b"), manifest=_manifest(), result=_result(0.02))

    assert first["content_hash"] != second["content_hash"]


def test_forward_diagnostics_report_rejects_promotion_evidence_true(tmp_path: Path) -> None:
    from bithumb_bot.research.forward_diagnostics_report import validate_forward_diagnostics_report_flags

    payload = write_forward_diagnostics_report(manager=_manager(tmp_path), manifest=_manifest(), result=_result())
    payload["promotion_evidence"] = True

    with pytest.raises(ValueError, match="diagnostic-only"):
        validate_forward_diagnostics_report_flags(payload)
