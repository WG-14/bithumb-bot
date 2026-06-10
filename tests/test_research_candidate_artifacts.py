from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from bithumb_bot.paths import PathManager
from bithumb_bot.research.experiment_manifest import parse_manifest
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.validation_protocol import run_research_backtest
from tests.factories.research_reports import DeterministicResearchEvaluator, assert_fast_research_workload


FORBIDDEN_DETAIL_KEYS = {
    "stage_trace",
    "decisions",
    "train_execution_metadata",
    "validation_execution_metadata",
    "final_holdout_execution_metadata",
}
FORBIDDEN_NON_EMPTY_ARRAY_KEYS = {
    "train_equity_curve",
    "validation_equity_curve",
    "final_holdout_equity_curve",
}
MAX_BACKTEST_CANDIDATES_BYTES = 128_000
MAX_CANDIDATE_RESULT_BYTES = 64_000


class StageTraceContractEvaluator(DeterministicResearchEvaluator):
    def evaluate(self, work_unit, context):  # type: ignore[no-untyped-def]
        result = super().evaluate(work_unit, context)
        for key in (
            "train_resource_usage",
            "validation_resource_usage",
            "final_holdout_resource_usage",
        ):
            usage = result.base_result.get(key)
            if isinstance(usage, dict):
                stage_trace = [
                    {
                        "candidate_id": work_unit.candidate_id,
                        "scenario_id": work_unit.scenario_id,
                        "split": key.removesuffix("_resource_usage"),
                        "bar_index": 1,
                    }
                ]
                usage["stage_trace"] = stage_trace
                usage["stage_trace_hash"] = sha256_prefixed(stage_trace)
        return result


def _ts(day: str, minute: int) -> int:
    base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(base.timestamp() * 1000) + minute * 60_000


def _create_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE candles(
                ts INTEGER PRIMARY KEY,
                pair TEXT,
                interval TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
            """
        )
        pattern = [100, 99, 98, 97, 99, 102, 105, 104, 103, 100, 98, 96]
        for day in ("2023-01-01", "2023-01-02", "2023-01-03"):
            for index in range(12):
                close = pattern[index % len(pattern)]
                conn.execute(
                    """
                    INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                    VALUES (?, 'KRW-BTC', '1m', ?, ?, ?, ?, 1.0)
                    """,
                    (_ts(day, index), close, close * 1.01, close * 0.99, close),
                )
        conn.commit()
    finally:
        conn.close()


def _manifest() -> dict[str, Any]:
    return {
        "experiment_id": "summary_candidate_artifacts",
        "hypothesis": "Summary candidate artifacts remain bounded.",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "unit_candles_v1",
            "source_uri": "managed-db:unit_candles_v1",
            "source_content_hash": "sha256:unit-candles-content",
            "source_schema_hash": "sha256:66a0dab69243f592c1dae02908aed5d1bf11194ec0ec692337a85a5636f711d3",
            "locator": {"snapshot_id": "unit_candles_v1", "immutable": True},
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": {
            "SMA_SHORT": [2, 3],
            "SMA_LONG": [4],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0],
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
        },
        "cost_model": {"fee_rate": 0.0, "slippage_bps": [0, 1]},
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 90,
            "min_profit_factor": 0.1,
            "oos_return_must_be_positive": False,
            "parameter_stability_required": False,
        },
        "research_run": {
            "report_detail": "summary",
            "resource_limits": {
                "max_decisions_retained": 0,
                "max_trades": 1,
                "max_equity_points_retained": 0,
                "max_rss_mb": None,
            },
        },
    }


def _manager(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def _run_contract_research_backtest(**kwargs: object) -> dict[str, Any]:
    report = run_research_backtest(
        candidate_evaluator=StageTraceContractEvaluator(),
        **kwargs,  # type: ignore[arg-type]
    )
    assert_fast_research_workload(
        report,
        max_strategy_runs=12,
        max_tick_events=144,
        max_matrix_size=12,
        max_artifact_write_count=7,
    )
    return report


@pytest.fixture()
def summary_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    db_path = tmp_path / "candles.sqlite"
    _create_db(db_path)
    manager = _manager(tmp_path, monkeypatch)
    report = _run_contract_research_backtest(
        manifest=parse_manifest(_manifest()),
        db_path=db_path,
        manager=manager,
        generated_at="2026-05-03T00:00:00+00:00",
    )
    root = manager.data_dir() / "derived" / "research" / "summary_candidate_artifacts"
    candidate_result_paths = sorted((root / "candidate_results").glob("candidate_*.json"))
    return {
        "report": report,
        "root": root,
        "backtest_candidates_path": root / "backtest_candidates.json",
        "candidate_result_paths": candidate_result_paths,
    }


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _assert_no_forbidden_detail(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            assert key not in FORBIDDEN_DETAIL_KEYS, f"forbidden detail key at {child_path}"
            if key in FORBIDDEN_NON_EMPTY_ARRAY_KEYS:
                assert child == [], f"forbidden non-empty equity curve at {child_path}"
            _assert_no_forbidden_detail(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_no_forbidden_detail(child, f"{path}[{index}]")


def test_summary_backtest_candidates_artifact_is_bounded(summary_artifacts: dict[str, Any]) -> None:
    path = summary_artifacts["backtest_candidates_path"]
    payload = _load(path)

    assert path.exists()
    assert path.stat().st_size < MAX_BACKTEST_CANDIDATES_BYTES
    assert payload["detail_policy"] == "summary_bounded"
    assert len(payload["candidates"]) == 2


def test_summary_candidate_result_artifacts_are_bounded(summary_artifacts: dict[str, Any]) -> None:
    paths = summary_artifacts["candidate_result_paths"]

    assert len(paths) == 2
    for path in paths:
        payload = _load(path)
        assert path.stat().st_size < MAX_CANDIDATE_RESULT_BYTES
        assert payload["candidate_result_detail_policy"] == "summary_bounded"
        assert len(payload["scenario_results"]) == 2
        for scenario in payload["scenario_results"]:
            usage = scenario["validation_resource_usage"]
            assert "stage_trace" not in usage
            assert usage["stage_trace_count"] == 1
            assert usage["stage_trace_hash"].startswith("sha256:")


def test_summary_candidate_artifacts_do_not_contain_forbidden_detail_keys(
    summary_artifacts: dict[str, Any],
) -> None:
    _assert_no_forbidden_detail(_load(summary_artifacts["backtest_candidates_path"]))
    for path in summary_artifacts["candidate_result_paths"]:
        _assert_no_forbidden_detail(_load(path))
