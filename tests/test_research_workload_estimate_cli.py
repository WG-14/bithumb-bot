from __future__ import annotations

import json

from bithumb_bot.cli.registry import command_registry
from bithumb_bot.research.cli import cmd_research_workload_estimate
from tests.test_research_backtest_reproducibility import _manifest


def test_research_workload_estimate_cli_is_read_only_json_surface() -> None:
    spec = command_registry()["research-workload-estimate"]

    assert spec.read_only is True
    assert spec.writes_db is False
    assert spec.uses_broker is False
    assert spec.produces_artifact is False
    assert spec.json_output_supported is True


def test_research_workload_estimate_reports_candidate_and_pre_parallel_counts(tmp_path, capsys) -> None:
    payload = _manifest()
    payload["parameter_space"]["SMA_SHORT"] = [2, 3, 4]
    payload["execution_model"] = {
        "type": "fixed_bps",
        "fee_rate": 0.0,
        "slippage_bps": [0.0, 1.0],
        "latency_ms": 0,
        "partial_fill_rate": 0.0,
        "order_failure_rate": 0.0,
        "market_order_extra_cost_bps": 0.0,
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    rc = cmd_research_workload_estimate(manifest_path=str(manifest_path), as_json=True)
    out = capsys.readouterr().out
    estimate = json.loads(out)

    assert rc == 0
    assert estimate["candidate_count"] == 3
    assert estimate["scenario_count"] == 2
    assert estimate["split_count"] == 3
    assert estimate["work_unit_count"] == 6
    assert estimate["pre_parallel_work_unit_count"] == 6
    assert estimate["pre_parallel_dataset_hash_call_count"] == 3
