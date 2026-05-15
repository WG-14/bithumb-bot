from __future__ import annotations

from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager
from bithumb_bot.storage_io import write_json_atomic

from .hashing import content_hash_payload, sha256_prefixed


CANDIDATE_RETURN_PANEL_SCHEMA_VERSION = 1
RETURN_PANEL_ARTIFACT_TYPE = "candidate_return_panel"
DEFAULT_RETURN_UNIT = "trade_return"
DEFAULT_MISSING_OBSERVATION_POLICY = "skip_missing_candidate_trade_returns"


def build_candidate_return_panel(
    *,
    experiment_id: str,
    manifest_hash: str,
    dataset_content_hash: str,
    dataset_quality_hash: str | None,
    split: str,
    benchmark: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    all_timestamps: list[int] = []
    for candidate in sorted(candidates, key=lambda item: str(item.get("parameter_candidate_id") or "")):
        series = _candidate_trade_return_series(candidate, split=split)
        timestamps = [int(row["ts"]) for row in series]
        all_timestamps.extend(timestamps)
        benchmark_series = [{"ts": row["ts"], "sequence": row["sequence"], "return_pct": 0.0} for row in series]
        excess_series = [
            {
                "ts": row["ts"],
                "sequence": row["sequence"],
                "excess_return_pct": row["return_pct"],
            }
            for row in series
        ]
        rows.append(
            {
                "candidate_id": str(candidate.get("parameter_candidate_id") or ""),
                "parameter_values": candidate.get("parameter_values") or {},
                "scenario_ids": _candidate_scenario_ids(candidate),
                "return_unit": DEFAULT_RETURN_UNIT,
                "benchmark": benchmark,
                "observation_count": len(series),
                "time_index": timestamps,
                "time_index_hash": sha256_prefixed(timestamps),
                "candidate_return_series_values": series,
                "candidate_return_series_hash": sha256_prefixed(series),
                "benchmark_return_series_values": benchmark_series,
                "benchmark_series_hash": sha256_prefixed(benchmark_series),
                "excess_return_series_values": excess_series,
                "benchmark_excess_return_series_hash": sha256_prefixed(excess_series),
                "missing_observation_policy": DEFAULT_MISSING_OBSERVATION_POLICY,
                "return_series_available": bool(series),
            }
        )
    ordered_index = sorted(set(all_timestamps))
    payload: dict[str, Any] = {
        "artifact_type": RETURN_PANEL_ARTIFACT_TYPE,
        "schema_version": CANDIDATE_RETURN_PANEL_SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "manifest_hash": manifest_hash,
        "dataset_content_hash": dataset_content_hash,
        "dataset_quality_hash": dataset_quality_hash,
        "split": split,
        "return_unit": DEFAULT_RETURN_UNIT,
        "benchmark": benchmark,
        "ordered_time_index": ordered_index,
        "ordered_time_index_hash": sha256_prefixed(ordered_index),
        "candidate_count": len(rows),
        "candidate_ids": [row["candidate_id"] for row in rows],
        "candidate_return_series": rows,
        "observation_count": sum(int(row["observation_count"]) for row in rows),
        "missing_observation_policy": DEFAULT_MISSING_OBSERVATION_POLICY,
        "limitations": [
            "trade_return_panel_from_closed_trade_records",
            "bar_level_portfolio_return_panel_not_available",
            "cash_benchmark_zero_return_series",
        ],
    }
    payload["panel_content_hash"] = sha256_prefixed(content_hash_payload(payload))
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    return payload


def write_candidate_return_panel(
    *,
    manager: PathManager,
    experiment_id: str,
    panel: dict[str, Any],
) -> Path:
    path = manager.data_dir() / "reports" / "research" / experiment_id / "candidate_return_panel.json"
    project_root = manager.project_root.resolve()
    if PathManager._is_within(path.resolve(), project_root):
        raise ValueError(f"candidate return panel path must be outside repository: {path.resolve()}")
    write_json_atomic(path, panel)
    return path


def validate_return_panel_binding(
    *,
    report: dict[str, Any],
    evidence: dict[str, Any],
    panel: dict[str, Any] | None,
) -> list[str]:
    reasons: list[str] = []
    expected_hash = str(evidence.get("return_panel_hash") or report.get("return_panel_hash") or "").strip()
    if not expected_hash.startswith("sha256:"):
        reasons.append("return_panel_hash_missing")
    if not isinstance(panel, dict):
        reasons.append("return_panel_missing")
        return reasons
    if panel.get("artifact_type") != RETURN_PANEL_ARTIFACT_TYPE:
        reasons.append("return_panel_artifact_type_mismatch")
    actual_hash = sha256_prefixed(content_hash_payload({k: v for k, v in panel.items() if k != "content_hash"}))
    embedded_hash = str(panel.get("content_hash") or "").strip()
    if expected_hash.startswith("sha256:") and actual_hash != expected_hash:
        reasons.append("return_panel_hash_mismatch")
    if embedded_hash != actual_hash:
        reasons.append("return_panel_hash_mismatch")
    if panel.get("schema_version") != CANDIDATE_RETURN_PANEL_SCHEMA_VERSION:
        reasons.append("return_panel_schema_version_mismatch")
    for field in ("manifest_hash", "dataset_content_hash", "dataset_quality_hash"):
        expected = report.get(field)
        actual = panel.get(field)
        if expected or actual:
            if str(expected or "") != str(actual or ""):
                reasons.append("return_panel_metadata_mismatch")
                break
    candidates = report.get("candidates")
    expected_candidate_ids = sorted(
        str(candidate.get("parameter_candidate_id") or "")
        for candidate in candidates
        if isinstance(candidate, dict)
    ) if isinstance(candidates, list) else []
    panel_candidate_ids = sorted(str(item) for item in panel.get("candidate_ids") or [])
    if expected_candidate_ids != panel_candidate_ids:
        reasons.append("return_panel_candidate_mismatch")
    if not _valid_return_panel_series(panel):
        reasons.append("return_panel_series_malformed")
    return sorted(set(reasons))


def _valid_return_panel_series(panel: dict[str, Any]) -> bool:
    ordered_index = panel.get("ordered_time_index")
    if not isinstance(ordered_index, list):
        return False
    parsed_ordered_index = [_as_int(item) for item in ordered_index]
    if any(item is None for item in parsed_ordered_index):
        return False
    if sha256_prefixed(parsed_ordered_index) != panel.get("ordered_time_index_hash"):
        return False
    rows = panel.get("candidate_return_series")
    if not isinstance(rows, list):
        return False
    for row in rows:
        if not isinstance(row, dict):
            return False
        candidate_series = row.get("candidate_return_series_values")
        benchmark_series = row.get("benchmark_return_series_values")
        excess_series = row.get("excess_return_series_values")
        time_index = row.get("time_index")
        if not isinstance(candidate_series, list) or not isinstance(benchmark_series, list) or not isinstance(excess_series, list):
            return False
        if not isinstance(time_index, list):
            return False
        observation_count = _as_int(row.get("observation_count"))
        if observation_count is None or observation_count != len(candidate_series):
            return False
        if sha256_prefixed(time_index) != row.get("time_index_hash"):
            return False
        if sha256_prefixed(candidate_series) != row.get("candidate_return_series_hash"):
            return False
        if sha256_prefixed(benchmark_series) != row.get("benchmark_series_hash"):
            return False
        if sha256_prefixed(excess_series) != row.get("benchmark_excess_return_series_hash"):
            return False
    return True


def _candidate_trade_return_series(candidate: dict[str, Any], *, split: str) -> list[dict[str, Any]]:
    key = f"{split}_closed_trades"
    trades = candidate.get(key)
    if not isinstance(trades, list):
        scenario_results = candidate.get("scenario_results")
        if isinstance(scenario_results, list):
            for scenario in scenario_results:
                if not isinstance(scenario, dict):
                    continue
                trades = scenario.get(key)
                if isinstance(trades, list):
                    break
    if not isinstance(trades, list):
        return []
    rows: list[dict[str, Any]] = []
    for index, trade in enumerate(trades):
        if hasattr(trade, "as_dict"):
            trade = trade.as_dict()
        if not isinstance(trade, dict):
            continue
        ts = _as_int(trade.get("exit_ts"))
        value = _as_float(trade.get("return_pct"))
        if ts is None or value is None:
            continue
        rows.append({"ts": ts, "sequence": index, "return_pct": value})
    return sorted(rows, key=lambda row: (int(row["ts"]), int(row["sequence"])))


def _candidate_scenario_ids(candidate: dict[str, Any]) -> list[str]:
    scenario_results = candidate.get("scenario_results")
    if not isinstance(scenario_results, list):
        return []
    ids = [
        str(scenario.get("scenario_id"))
        for scenario in scenario_results
        if isinstance(scenario, dict) and scenario.get("scenario_id") is not None
    ]
    return sorted(ids)


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _as_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        return None
    return parsed
