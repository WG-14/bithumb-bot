from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .h74_observation import H74_SOURCE_CANDIDATE_ID, H74_SOURCE_OBSERVATION_PARAMETERS
from .research.hashing import sha256_prefixed


H74_EQUIVALENCE_SCHEMA_VERSION = 1
H74_SOURCE_BASE_FEE_RATE = 0.0004
H74_SOURCE_BASE_SLIPPAGE_BPS = 10.0


def build_h74_equivalence_manifest(
    *,
    source_artifact_path: str | Path | None = None,
    order_rules: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    source = _load_source_artifact(source_artifact_path)
    source_missing = source is None
    source_cost = _source_cost_assumptions(source)
    source_identity = _source_artifact_identity(source)
    parameters = dict(H74_SOURCE_OBSERVATION_PARAMETERS)
    manifest: dict[str, Any] = {
        "schema_version": H74_EQUIVALENCE_SCHEMA_VERSION,
        "artifact_type": "h74_backtest_live_equivalence_manifest",
        "candidate_id": H74_SOURCE_CANDIDATE_ID,
        "source_candidate_id": source_identity["source_candidate_id"],
        "source_backtest_report_hash": source_identity["source_backtest_report_hash"],
        "source_artifact_schema": source_identity["source_artifact_schema"],
        "source_artifact_status": "missing" if source_missing else "loaded",
        "source_artifact_path": None if source_artifact_path is None else str(source_artifact_path),
        "source_artifact_hash": "" if source is None else sha256_prefixed(source),
        "source_assumption_status": source_cost["source_assumption_status"],
        "source_missing_assumption_fields": source_cost["source_missing_assumption_fields"],
        "fee_rate": source_cost["fee_rate"],
        "fee_source": source_cost["fee_source"],
        "slippage_bps": source_cost["slippage_bps"],
        "slippage_source": source_cost["slippage_source"],
        "candle_timing": source_cost["candle_timing"],
        "time_window": {
            "timezone": parameters["DAILY_PARTICIPATION_TIMEZONE"],
            "start_hour_kst": parameters["DAILY_PARTICIPATION_WINDOW_START_HOUR_KST"],
            "end_hour_kst": parameters["DAILY_PARTICIPATION_WINDOW_END_HOUR_KST"],
        },
        "exit_policy": {
            "rules": parameters["STRATEGY_EXIT_RULES"],
            "max_holding_min": parameters["STRATEGY_EXIT_MAX_HOLDING_MIN"],
            "min_take_profit_ratio": parameters["STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO"],
            "small_loss_tolerance_ratio": parameters["STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO"],
        },
        "risk_policy": {
            "max_daily_entry_count": parameters["max_daily_entry_count"],
            "max_daily_total_order_count": parameters["max_daily_total_order_count"],
            "daily_participation_count_scope": parameters["daily_participation_count_scope"],
            "daily_order_count_scope": parameters["daily_order_count_scope"],
        },
        "order_rules": dict(order_rules or {}),
    }
    missing_order_rules = [
        key
        for key in ("min_qty", "min_notional_krw")
        if manifest["order_rules"].get(key) in (None, "")
    ]
    manifest["order_rule_status"] = "missing" if missing_order_rules else "present"
    manifest["missing_order_rule_fields"] = missing_order_rules
    manifest["manifest_hash"] = sha256_prefixed(manifest)
    return manifest


def compare_h74_equivalence(
    manifest: Mapping[str, object],
    *,
    current_fee_rate: float,
    current_fee_authority_source: str,
    current_order_rules: Mapping[str, object],
) -> dict[str, Any]:
    expected_fee_raw = manifest.get("fee_rate")
    expected_fee = None if expected_fee_raw in (None, "") else float(expected_fee_raw)
    actual_fee = float(current_fee_rate)
    fee_match = expected_fee is not None and abs(expected_fee - actual_fee) <= 1e-12
    order_rules = manifest.get("order_rules") if isinstance(manifest.get("order_rules"), Mapping) else {}
    order_rule_matches = {
        key: order_rules.get(key) == current_order_rules.get(key)
        for key in ("min_qty", "min_notional_krw")
    }
    source_missing = str(manifest.get("source_artifact_status") or "") == "missing"
    source_assumptions_valid = str(manifest.get("source_assumption_status") or "") == "valid"
    missing_rules = list(manifest.get("missing_order_rule_fields") or [])
    if source_missing:
        status = "unknown_source_artifact_missing"
    elif not source_assumptions_valid:
        status = "unknown_source_assumption_missing"
    elif not fee_match or missing_rules or not all(order_rule_matches.values()):
        status = "mismatch"
    else:
        status = "pass"
    return {
        "experiment_equivalence_status": status,
        "fee_authority_source": str(current_fee_authority_source),
        "fee_comparison": {
            "expected_fee_rate": expected_fee,
            "current_fee_rate": actual_fee,
            "match": fee_match,
        },
        "order_rule_comparison": {
            "expected": dict(order_rules),
            "current": dict(current_order_rules),
            "matches": order_rule_matches,
            "missing_manifest_fields": missing_rules,
        },
    }


def _load_source_artifact(source_artifact_path: str | Path | None) -> Mapping[str, object] | None:
    if source_artifact_path is None:
        return None
    path = Path(source_artifact_path)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, Mapping) else None


def _source_artifact_identity(source: Mapping[str, object] | None) -> dict[str, object]:
    if source is None:
        return {
            "source_candidate_id": None,
            "source_backtest_report_hash": None,
            "source_artifact_schema": "missing",
        }
    cost_schema = (
        "runtime_base_cost_assumption"
        if isinstance(source.get("runtime_base_cost_assumption"), Mapping)
        else "cost_model"
        if isinstance(source.get("cost_model"), Mapping)
        else "unknown"
    )
    return {
        "source_candidate_id": source.get("candidate_id"),
        "source_backtest_report_hash": source.get("backtest_report_hash"),
        "source_artifact_schema": cost_schema,
    }


def _source_cost_assumptions(source: Mapping[str, object] | None) -> dict[str, object]:
    if source is None:
        return {
            "source_assumption_status": "missing_source",
            "source_missing_assumption_fields": ["source_artifact"],
            "fee_rate": None,
            "fee_source": "source_artifact_missing",
            "slippage_bps": None,
            "slippage_source": "source_artifact_missing",
            "candle_timing": "unknown_source_artifact_missing",
        }
    cost = source.get("runtime_base_cost_assumption")
    if not isinstance(cost, Mapping):
        cost = source.get("cost_model") if isinstance(source.get("cost_model"), Mapping) else {}
    missing: list[str] = []
    if "fee_rate" not in cost:
        missing.append("fee_rate")
    if "slippage_bps" not in cost:
        missing.append("slippage_bps")
    if "candle_timing" not in source:
        missing.append("candle_timing")
    return {
        "source_assumption_status": "valid" if not missing else "missing_required_fields",
        "source_missing_assumption_fields": missing,
        "fee_rate": None if "fee_rate" in missing else float(cost.get("fee_rate") or 0.0),
        "fee_source": str(cost.get("fee_source") or "source_artifact"),
        "slippage_bps": None if "slippage_bps" in missing else float(cost.get("slippage_bps") or 0.0),
        "slippage_source": str(cost.get("slippage_source") or "source_artifact"),
        "candle_timing": None if "candle_timing" in missing else str(source.get("candle_timing")),
    }


__all__ = [
    "build_h74_equivalence_manifest",
    "compare_h74_equivalence",
]
