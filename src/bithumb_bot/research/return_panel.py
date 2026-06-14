from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bithumb_bot.paths import PathManager

from .artifact_store import ResearchArtifactContext
from .hashing import content_hash_payload, sha256_prefixed


CANDIDATE_RETURN_PANEL_SCHEMA_VERSION = 1
RETURN_PANEL_ARTIFACT_TYPE = "candidate_return_panel"
DEFAULT_RETURN_UNIT = "trade_return"
PROMOTION_GRADE_RETURN_UNIT = "portfolio_bar_return"
DEFAULT_MISSING_OBSERVATION_POLICY = "skip_missing_candidate_trade_returns"
PROMOTION_MISSING_OBSERVATION_POLICY = "fail_closed_complete_candidate_bar_alignment_required"


def build_candidate_return_panel(
    *,
    experiment_id: str,
    manifest_hash: str,
    dataset_content_hash: str,
    dataset_quality_hash: str | None,
    split: str,
    benchmark: str,
    candidates: list[dict[str, Any]],
    manager: PathManager | None = None,
) -> dict[str, Any]:
    aligned = _build_aligned_portfolio_return_panel(
        experiment_id=experiment_id,
        manifest_hash=manifest_hash,
        dataset_content_hash=dataset_content_hash,
        dataset_quality_hash=dataset_quality_hash,
        split=split,
        benchmark=benchmark,
        candidates=candidates,
        manager=manager,
    )
    if aligned is not None:
        return aligned
    return _build_trade_return_panel(
        experiment_id=experiment_id,
        manifest_hash=manifest_hash,
        dataset_content_hash=dataset_content_hash,
        dataset_quality_hash=dataset_quality_hash,
        split=split,
        benchmark=benchmark,
        candidates=candidates,
    )


def _build_trade_return_panel(
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
        "attempted_promotion_grade_return_unit": PROMOTION_GRADE_RETURN_UNIT,
        "limitations": [
            "trade_return_panel_from_closed_trade_records",
            "bar_level_portfolio_return_panel_not_available",
            "aligned_bar_portfolio_return_panel_not_generated",
            "official_candidate_equity_curve_missing_or_unaligned",
            "trade_return_panel_cannot_satisfy_promotion_grade_wrc",
            "official_wrc_generation_requires_aligned_bar_return_panel",
            "cash_benchmark_zero_return_series",
        ],
        "promotion_grade_available": False,
        "official_promotion_grade_wrc_generation_available": False,
        "promotion_grade_fail_reasons": [
            "promotion_grade_requires_aligned_return_panel",
            "trade_return_panel_cannot_satisfy_promotion_grade_wrc",
            "official_candidate_equity_curve_missing_or_unaligned",
        ],
        "operator_next_step": (
            "retain and emit aligned candidate bar-level portfolio return series, then rerun "
            "with an official promotion-grade WRC implementation bound to that panel"
        ),
    }
    payload["panel_content_hash"] = sha256_prefixed(content_hash_payload(payload))
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    return payload


def _build_aligned_portfolio_return_panel(
    *,
    experiment_id: str,
    manifest_hash: str,
    dataset_content_hash: str,
    dataset_quality_hash: str | None,
    split: str,
    benchmark: str,
    candidates: list[dict[str, Any]],
    manager: PathManager | None = None,
) -> dict[str, Any] | None:
    if benchmark != "cash":
        return None
    rows: list[dict[str, Any]] = []
    canonical_index: list[int] | None = None
    for candidate in sorted(candidates, key=lambda item: str(item.get("parameter_candidate_id") or "")):
        series = _candidate_portfolio_bar_return_series(candidate, split=split, manager=manager)
        if not series:
            return None
        timestamps = [int(row["ts"]) for row in series]
        if canonical_index is None:
            canonical_index = timestamps
        elif timestamps != canonical_index:
            return None
        benchmark_series = [{"ts": row["ts"], "sequence": row["sequence"], "return_pct": 0.0} for row in series]
        excess_series = [
            {
                "ts": row["ts"],
                "sequence": row["sequence"],
                "excess_return_pct": float(row["return_pct"]),
            }
            for row in series
        ]
        row_metadata = {
            "candidate_id": str(candidate.get("parameter_candidate_id") or ""),
            "parameter_values": candidate.get("parameter_values") or {},
            "scenario_ids": _candidate_scenario_ids(candidate),
            "return_unit": PROMOTION_GRADE_RETURN_UNIT,
            "benchmark": benchmark,
            "split": split,
            "missing_observation_policy": PROMOTION_MISSING_OBSERVATION_POLICY,
        }
        rows.append(
            {
                **row_metadata,
                "metadata_hash": sha256_prefixed(row_metadata),
                "observation_count": len(series),
                "time_index": timestamps,
                "time_index_hash": sha256_prefixed(timestamps),
                "candidate_return_series_values": series,
                "candidate_return_series_hash": sha256_prefixed(series),
                "benchmark_return_series_values": benchmark_series,
                "benchmark_series_hash": sha256_prefixed(benchmark_series),
                "excess_return_series_values": excess_series,
                "benchmark_excess_return_series_hash": sha256_prefixed(excess_series),
                "return_series_available": True,
            }
        )
    if canonical_index is None or not canonical_index:
        return None
    metadata = {
        "experiment_id": experiment_id,
        "manifest_hash": manifest_hash,
        "dataset_content_hash": dataset_content_hash,
        "dataset_quality_hash": dataset_quality_hash,
        "split": split,
        "return_unit": PROMOTION_GRADE_RETURN_UNIT,
        "benchmark": benchmark,
        "missing_observation_policy": PROMOTION_MISSING_OBSERVATION_POLICY,
        "candidate_ids": [row["candidate_id"] for row in rows],
        "scenario_ids_by_candidate": {row["candidate_id"]: row["scenario_ids"] for row in rows},
    }
    payload: dict[str, Any] = {
        "artifact_type": RETURN_PANEL_ARTIFACT_TYPE,
        "schema_version": CANDIDATE_RETURN_PANEL_SCHEMA_VERSION,
        **metadata,
        "metadata_hash": sha256_prefixed(metadata),
        "ordered_time_index": canonical_index,
        "ordered_time_index_hash": sha256_prefixed(canonical_index),
        "candidate_count": len(rows),
        "candidate_return_series": rows,
        "observation_count": sum(int(row["observation_count"]) for row in rows),
        "per_candidate_observation_count": len(canonical_index),
        "limitations": [
            "cash_benchmark_zero_return_series",
            "spa_not_implemented",
            "deflated_sharpe_not_implemented",
        ],
        "promotion_grade_available": True,
        "official_promotion_grade_wrc_generation_available": True,
        "promotion_grade_fail_reasons": [],
        "operator_next_step": "review recomputable WRC evidence and keep SPA/DSR gates fail-closed unless implemented",
    }
    payload["panel_content_hash"] = sha256_prefixed(content_hash_payload(payload))
    payload["content_hash"] = sha256_prefixed(content_hash_payload(payload))
    return payload


def write_candidate_return_panel(
    *,
    manager: PathManager,
    experiment_id: str,
    panel: dict[str, Any],
    artifact_context: ResearchArtifactContext | None = None,
) -> Path:
    path = manager.data_dir() / "reports" / "research" / experiment_id / "candidate_return_panel.json"
    project_root = manager.project_root.resolve()
    if PathManager._is_within(path.resolve(), project_root):
        raise ValueError(f"candidate return panel path must be outside repository: {path.resolve()}")
    store = artifact_context or ResearchArtifactContext(manager=manager, experiment_id=experiment_id)
    store.write_json_atomic(path, panel)
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
    panel_content_payload = {k: v for k, v in panel.items() if k not in {"content_hash", "panel_content_hash"}}
    if str(panel.get("panel_content_hash") or "").strip() != sha256_prefixed(content_hash_payload(panel_content_payload)):
        reasons.append("return_panel_panel_content_hash_mismatch")
    if panel.get("schema_version") != CANDIDATE_RETURN_PANEL_SCHEMA_VERSION:
        reasons.append("return_panel_schema_version_mismatch")
    evidence_return_unit = evidence.get("return_unit") or report.get("return_unit")
    if evidence_return_unit is not None and str(evidence_return_unit) != str(panel.get("return_unit") or ""):
        reasons.append("return_panel_return_unit_mismatch")
    if panel.get("return_unit") == DEFAULT_RETURN_UNIT and panel.get("promotion_grade_available") is True:
        reasons.append("return_panel_promotion_grade_misclassified")
    if panel.get("promotion_grade_available") is True and panel.get("return_unit") not in {PROMOTION_GRADE_RETURN_UNIT, "bar_excess_return"}:
        reasons.append("return_panel_promotion_grade_misclassified")
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
    rows = panel.get("candidate_return_series")
    if isinstance(rows, list):
        if _as_int(panel.get("candidate_count")) != len(panel_candidate_ids) or _as_int(panel.get("candidate_count")) != len(rows):
            reasons.append("return_panel_candidate_count_mismatch")
        row_candidate_ids = sorted(str(row.get("candidate_id") or "") for row in rows if isinstance(row, dict))
        if row_candidate_ids != panel_candidate_ids:
            reasons.append("return_panel_candidate_mismatch")
        row_observations = [_as_int(row.get("observation_count")) for row in rows if isinstance(row, dict)]
        if any(value is None for value in row_observations) or _as_int(panel.get("observation_count")) != sum(
            int(value or 0) for value in row_observations
        ):
            reasons.append("return_panel_observation_count_mismatch")
        scenario_reasons = _return_panel_scenario_reasons(report=report, rows=rows)
        reasons.extend(scenario_reasons)
    series_reasons = _return_panel_series_reasons(panel)
    reasons.extend(series_reasons)
    return sorted(set(reasons))


def _return_panel_series_reasons(panel: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    ordered_index = panel.get("ordered_time_index")
    if not isinstance(ordered_index, list):
        return ["return_panel_time_index_mismatch"]
    parsed_ordered_index = [_as_int(item) for item in ordered_index]
    if any(item is None for item in parsed_ordered_index):
        return ["return_panel_time_index_mismatch"]
    if parsed_ordered_index != sorted(parsed_ordered_index):
        reasons.append("return_panel_time_index_mismatch")
    if sha256_prefixed(parsed_ordered_index) != panel.get("ordered_time_index_hash"):
        reasons.append("return_panel_time_index_mismatch")
    rows = panel.get("candidate_return_series")
    if not isinstance(rows, list):
        return sorted(set(reasons + ["return_panel_series_malformed"]))
    union_index: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            reasons.append("return_panel_series_malformed")
            continue
        candidate_series = row.get("candidate_return_series_values")
        benchmark_series = row.get("benchmark_return_series_values")
        excess_series = row.get("excess_return_series_values")
        time_index = row.get("time_index")
        if not isinstance(candidate_series, list) or not isinstance(benchmark_series, list) or not isinstance(excess_series, list):
            reasons.append("return_panel_series_malformed")
            continue
        if not isinstance(time_index, list):
            reasons.append("return_panel_time_index_mismatch")
            continue
        observation_count = _as_int(row.get("observation_count"))
        if observation_count is None or observation_count != len(candidate_series):
            reasons.append("return_panel_observation_count_mismatch")
        if len(candidate_series) != len(benchmark_series) or len(candidate_series) != len(excess_series):
            reasons.append("return_panel_series_alignment_mismatch")
        candidate_keys = _series_keys(candidate_series)
        benchmark_keys = _series_keys(benchmark_series)
        excess_keys = _series_keys(excess_series)
        if (
            candidate_keys is None
            or benchmark_keys is None
            or excess_keys is None
            or candidate_keys != benchmark_keys
            or candidate_keys != excess_keys
        ):
            reasons.append("return_panel_series_alignment_mismatch")
        if candidate_keys is not None and [ts for ts, _seq in candidate_keys] != [_as_int(item) for item in time_index]:
            reasons.append("return_panel_time_index_mismatch")
        parsed_time_index = [_as_int(item) for item in time_index]
        if any(item is None for item in parsed_time_index):
            reasons.append("return_panel_time_index_mismatch")
        else:
            union_index.update(int(item) for item in parsed_time_index if item is not None)
        if sha256_prefixed(time_index) != row.get("time_index_hash"):
            reasons.append("return_panel_time_index_mismatch")
        if sha256_prefixed(candidate_series) != row.get("candidate_return_series_hash"):
            reasons.append("return_panel_series_malformed")
        if sha256_prefixed(benchmark_series) != row.get("benchmark_series_hash"):
            reasons.append("return_panel_series_malformed")
        if sha256_prefixed(excess_series) != row.get("benchmark_excess_return_series_hash"):
            reasons.append("return_panel_series_malformed")
    if parsed_ordered_index != sorted(union_index):
        reasons.append("return_panel_time_index_mismatch")
    if panel.get("promotion_grade_available") is True:
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("time_index") != parsed_ordered_index:
                reasons.append("return_panel_time_index_mismatch")
    return sorted(set(reasons))


def _return_panel_scenario_reasons(*, report: dict[str, Any], rows: list[Any]) -> list[str]:
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        return []
    expected_by_candidate: dict[str, list[str]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        scenario_ids = _candidate_scenario_ids(candidate)
        if scenario_ids:
            expected_by_candidate[str(candidate.get("parameter_candidate_id") or "")] = scenario_ids
    reasons: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        expected = expected_by_candidate.get(str(row.get("candidate_id") or ""))
        if expected is None:
            continue
        actual = sorted(str(item) for item in row.get("scenario_ids") or [])
        if actual != expected:
            reasons.append("return_panel_scenario_id_mismatch")
    return reasons


def _series_keys(series: list[Any]) -> list[tuple[int, int]] | None:
    keys: list[tuple[int, int]] = []
    for item in series:
        if not isinstance(item, dict):
            return None
        ts = _as_int(item.get("ts"))
        sequence = _as_int(item.get("sequence"))
        if ts is None or sequence is None:
            return None
        keys.append((ts, sequence))
    return keys


def _candidate_trade_return_series(candidate: dict[str, Any], *, split: str) -> list[dict[str, Any]]:
    key = f"{split}_closed_trades"
    trades = candidate.get(key)
    source = {
        "return_panel_scenario_role": candidate.get("primary_scenario_role", "candidate"),
        "return_panel_scenario_id": candidate.get("primary_scenario_id"),
        "return_panel_series_source": f"candidate.{key}",
    }
    if not isinstance(trades, list):
        scenario_results = candidate.get("scenario_results")
        if isinstance(scenario_results, list):
            base_scenarios = [
                scenario
                for scenario in scenario_results
                if isinstance(scenario, dict)
                and scenario.get("scenario_role") in {None, "base"}
                and isinstance(scenario.get(key), list)
            ]
            if len(base_scenarios) != 1:
                return []
            scenario = base_scenarios[0]
            trades = scenario.get(key)
            scenario_role = scenario.get("scenario_role") or "base"
            source = {
                "return_panel_scenario_role": scenario_role,
                "return_panel_scenario_id": scenario.get("scenario_id"),
                "return_panel_series_source": f"scenario_results.{scenario_role}.{key}",
            }
            if not isinstance(trades, list):
                return []
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
        rows.append({"ts": ts, "sequence": index, "return_pct": value, **source})
    return sorted(rows, key=lambda row: (int(row["ts"]), int(row["sequence"])))


def _candidate_portfolio_bar_return_series(
    candidate: dict[str, Any],
    *,
    split: str,
    manager: PathManager | None = None,
) -> list[dict[str, Any]]:
    curve = _candidate_equity_curve(candidate, split=split, manager=manager)
    if len(curve) < 2:
        return []
    rows: list[dict[str, Any]] = []
    for index, (previous, current) in enumerate(zip(curve, curve[1:])):
        previous_equity = _as_float(previous.get("equity"))
        current_equity = _as_float(current.get("equity"))
        ts = _as_int(current.get("ts"))
        if previous_equity is None or current_equity is None or ts is None or previous_equity <= 0.0:
            return []
        rows.append(
            {
                "ts": ts,
                "sequence": index,
                "return_pct": ((current_equity / previous_equity) - 1.0) * 100.0,
            }
        )
    return rows


def _candidate_equity_curve(
    candidate: dict[str, Any],
    *,
    split: str,
    manager: PathManager | None = None,
) -> list[dict[str, Any]]:
    key = f"{split}_equity_curve"
    curve = candidate.get(key)
    if not isinstance(curve, list):
        scenario_results = candidate.get("scenario_results")
        if isinstance(scenario_results, list):
            for scenario in scenario_results:
                if not isinstance(scenario, dict):
                    continue
                if scenario.get("scenario_role") not in {None, "base"}:
                    continue
                curve = scenario.get(key)
                if isinstance(curve, list):
                    break
    if not isinstance(curve, list):
        if manager is not None:
            return _candidate_equity_curve_from_audit_trace(candidate, split=split, manager=manager)
        return []
    if not curve and manager is not None:
        traced = _candidate_equity_curve_from_audit_trace(candidate, split=split, manager=manager)
        if traced:
            return traced
    rows: list[dict[str, Any]] = []
    for item in curve:
        if hasattr(item, "as_dict"):
            item = item.as_dict()
        if not isinstance(item, dict):
            return []
        ts = _as_int(item.get("ts"))
        equity = _as_float(item.get("equity"))
        cash = _as_float(item.get("cash"))
        asset_qty = _as_float(item.get("asset_qty"))
        if ts is None or equity is None:
            return []
        rows.append(
            {
                "ts": ts,
                "equity": equity,
                "cash": cash,
                "asset_qty": asset_qty,
            }
        )
    return sorted(rows, key=lambda row: int(row["ts"]))


def _candidate_equity_curve_from_audit_trace(
    candidate: dict[str, Any],
    *,
    split: str,
    manager: PathManager,
) -> list[dict[str, Any]]:
    scenario_results = candidate.get("scenario_results")
    if not isinstance(scenario_results, list):
        return []
    for scenario in scenario_results:
        if not isinstance(scenario, dict):
            continue
        if scenario.get("scenario_role") not in {None, "base"}:
            continue
        index = scenario.get(f"{split}_audit_trace_index")
        if not isinstance(index, dict):
            continue
        equity = index.get("equity")
        if not isinstance(equity, dict):
            continue
        ref = str(equity.get("path") or "")
        if not ref:
            continue
        path = manager.data_dir() / ref
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    return []
                payload = event.get("payload")
                if not isinstance(payload, dict):
                    return []
                ts = _as_int(payload.get("ts"))
                equity_value = _as_float(payload.get("equity"))
                cash = _as_float(payload.get("cash"))
                asset_qty = _as_float(payload.get("asset_qty"))
                if ts is None or equity_value is None:
                    return []
                rows.append({"ts": ts, "equity": equity_value, "cash": cash, "asset_qty": asset_qty})
        return sorted(rows, key=lambda row: int(row["ts"]))
    return []


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
