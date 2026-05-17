from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bithumb_bot.bootstrap import get_last_explicit_env_load_summary
from bithumb_bot.config import settings
from bithumb_bot.execution_quality import ExecutionQualityThresholds
from bithumb_bot.execution_reality_contract import build_execution_capability_contract

from .data_plane import (
    PERSISTENT_MISSING_CLASSIFICATIONS,
    build_dataset_quality_report_sql,
    dataset_quality_policy_payload,
    persistent_missing_overall_next_action,
    readiness_mode_payload,
    split_names,
    walk_forward_payload,
)
from .execution_calibration import compare_calibration_to_scenario, load_calibration_artifact
from .experiment_manifest import ExperimentManifest, load_manifest


def build_research_readiness_report(
    *,
    manifest_path: str | Path,
    db_path: str | Path | None = None,
    execution_calibration_path: str | Path | None = None,
    missing_classification_path: str | Path | None = None,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    resolved_manifest_path = Path(manifest_path).expanduser().resolve()
    manifest = load_manifest(resolved_manifest_path)
    resolved_db_path = Path(db_path or settings.DB_PATH).expanduser().resolve()
    env_summary = get_last_explicit_env_load_summary().as_dict()

    split_reports: dict[str, dict[str, Any]] = {}
    failed = False
    for split_name in split_names(manifest):
        if progress_callback is not None:
            progress_callback(split_name)
        report = build_dataset_quality_report_sql(
            db_path=resolved_db_path,
            manifest=manifest,
            split_name=split_name,
        ).payload
        split_payload = _split_payload(report)
        split_reports[split_name] = split_payload
        failed = failed or split_payload["quality_status"] != "PASS"

    top_of_book = _top_of_book_payload(manifest=manifest, split_reports=split_reports)
    failed = failed or top_of_book["status"] == "FAIL"
    execution_capability = _execution_capability_payload(manifest=manifest, top_of_book=top_of_book)
    failed = failed or bool(execution_capability.get("unavailable_required_capabilities"))

    execution_calibration = _execution_calibration_payload(
        manifest=manifest,
        execution_calibration_path=execution_calibration_path,
    )
    failed = failed or execution_calibration["status"] == "FAIL"

    walk_forward = walk_forward_payload(manifest)
    failed = failed or walk_forward["status"] == "FAIL"
    persistent_missing_classification = _persistent_missing_classification_payload(
        path=missing_classification_path,
        manifest=manifest,
        db_path=resolved_db_path,
    )
    failed = failed or persistent_missing_classification["status"] == "FAIL"

    next_actions = _next_actions(
        split_reports=split_reports,
        top_of_book=top_of_book,
        execution_capability=execution_capability,
        execution_calibration=execution_calibration,
        walk_forward=walk_forward,
        persistent_missing_classification=persistent_missing_classification,
    )

    return {
        "status": "FAIL" if failed else "PASS",
        "manifest_path": str(resolved_manifest_path),
        "manifest_hash": manifest.manifest_hash(),
        "mode": settings.MODE,
        "db_path": str(resolved_db_path),
        "env_file": env_summary.get("env_file"),
        "env_loaded": bool(env_summary.get("loaded")),
        "env_exists": bool(env_summary.get("exists")),
        "market": manifest.market,
        "interval": manifest.interval,
        "readiness_mode": readiness_mode_payload(manifest),
        "dataset_quality_policy": dataset_quality_policy_payload(manifest),
        "split_ranges": {
            split_name: getattr(manifest.dataset.split, split_name).as_dict()
            for split_name in split_names(manifest)
        },
        "splits": split_reports,
        "top_of_book": top_of_book,
        "execution_capability": execution_capability,
        "execution_capability_contract": execution_capability["contract"],
        "execution_capability_contract_hash": execution_capability["contract_hash"],
        "evidence_tier": execution_capability["evidence_tier"],
        "unavailable_required_capabilities": execution_capability["unavailable_required_capabilities"],
        "execution_calibration": execution_calibration,
        "walk_forward": walk_forward,
        "persistent_missing_classification": persistent_missing_classification,
        "next_actions": next_actions,
    }


def cmd_research_readiness(
    *,
    manifest_path: str,
    execution_calibration_path: str | None = None,
    missing_classification_path: str | None = None,
    as_json: bool = False,
) -> int:
    try:
        report = build_research_readiness_report(
            manifest_path=manifest_path,
            execution_calibration_path=execution_calibration_path,
            missing_classification_path=missing_classification_path,
            progress_callback=(
                None
                if as_json
                else lambda split_name: print(f"[RESEARCH-READINESS] scanning split={split_name} method=sqlite_streaming")
            ),
        )
    except Exception as exc:
        print(f"[RESEARCH-READINESS] error={exc}")
        return 1
    if as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        _print_readiness(report)
    return 0 if report["status"] == "PASS" else 1


def _split_payload(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "scan_method": report.get("scan_method"),
        "expected_candle_buckets": report["expected_candle_count"],
        "present_candle_buckets": report["present_expected_bucket_count"],
        "missing_count": report["missing_bucket_count"],
        "coverage_pct": report["coverage_pct"],
        "first_ts": report["first_ts"],
        "last_ts": report["last_ts"],
        "duplicate_candle_key_count": report["duplicate_key_count"],
        "non_monotonic_ts_count": report["non_monotonic_ts_count"],
        "interval_mismatch_count": report["interval_mismatch_count"],
        "unexpected_bucket_count": report["unexpected_bucket_count"],
        "ohlc_violation_count": report["ohlc_violation_count"],
        "non_positive_price_count": report["non_positive_price_count"],
        "negative_volume_count": report["negative_volume_count"],
        "missing_bucket_ranges": list(report.get("missing_bucket_ranges") or []),
        "missing_bucket_sample": list(report.get("missing_bucket_sample") or []),
        "missing_ranges_truncated": bool(report.get("missing_ranges_truncated")),
        "db_schema_fingerprint": report["db_schema_fingerprint"],
        "quality_status": report["quality_gate_status"],
        "quality_reasons": list(report.get("quality_gate_reasons") or []),
        "top_of_book_required": bool(report.get("top_of_book_required")),
        "top_of_book_missing_policy": report.get("top_of_book_missing_policy"),
        "top_of_book_expected_signal_count": report.get("top_of_book_expected_signal_count"),
        "top_of_book_candle_quote_expected_count": report.get("top_of_book_expected_signal_count"),
        "top_of_book_joined_count": report.get("top_of_book_joined_count"),
        "top_of_book_candle_quote_joined_count": report.get("top_of_book_joined_count"),
        "top_of_book_missing_count": report.get("top_of_book_missing_count"),
        "top_of_book_coverage_pct": report.get("top_of_book_coverage_pct"),
        "top_of_book_candle_quote_coverage": report.get("top_of_book_coverage_pct"),
        "top_of_book_candle_quote_coverage_pct": report.get("top_of_book_coverage_pct"),
        "signal_execution_quote_coverage_pct": None,
        "signal_execution_quote_coverage_status": "not_computable_without_strategy_signal_run",
        "signal_level_depth_coverage_pct": None,
        "signal_level_depth_coverage_status": "not_computable_without_strategy_signal_run",
        "depth_available": bool(report.get("depth_available")),
        "depth_availability_source": report.get("depth_availability_source"),
        "depth_liquidity_sufficiency_status": "not_implemented_order_size_depth_walk_required",
        "top_of_book_gate_status": report.get("top_of_book_gate_status"),
        "top_of_book_gate_reasons": list(report.get("top_of_book_gate_reasons") or []),
    }


def _top_of_book_payload(*, manifest: ExperimentManifest, split_reports: dict[str, dict[str, Any]]) -> dict[str, Any]:
    spec = manifest.dataset.top_of_book
    if spec is None:
        return {
            "required": False,
            "missing_policy": None,
            "min_coverage_pct": None,
            "observed_coverage_pct": None,
            "top_of_book_candle_quote_coverage_pct": None,
            "top_of_book_candle_quote_expected_count": 0,
            "top_of_book_candle_quote_joined_count": 0,
            "signal_execution_quote_coverage_pct": None,
            "signal_execution_quote_coverage_status": "not_computable_without_strategy_signal_run",
            "signal_level_depth_coverage_pct": None,
            "signal_level_depth_coverage_status": "not_computable_without_strategy_signal_run",
            "signal_depth_coverage_limitation": "readiness_sql_scan_has_no_strategy_signal_events",
            "depth_available": False,
            "depth_evidence_available": False,
            "depth_availability_source": "not_requested_or_not_scanned",
            "depth_liquidity_sufficiency_status": "not_implemented_order_size_depth_walk_required",
            "status": "NOT_REQUESTED",
            "reasons": [],
            "next_action": "none",
        }
    expected = sum(int(item.get("top_of_book_expected_signal_count") or 0) for item in split_reports.values())
    joined = sum(int(item.get("top_of_book_joined_count") or 0) for item in split_reports.values())
    coverage = round((joined / expected * 100.0), 8) if expected else 0.0
    statuses = {str(item.get("top_of_book_gate_status") or "UNKNOWN") for item in split_reports.values()}
    status = "PASS"
    if "FAIL" in statuses:
        status = "FAIL"
    elif "WARN" in statuses:
        status = "WARN"
    reasons = sorted(
        {
            str(reason)
            for item in split_reports.values()
            for reason in item.get("top_of_book_gate_reasons") or []
        }
    )
    next_action = "none"
    if status in {"FAIL", "WARN"}:
        next_action = (
            "candle backfill does not satisfy production top-of-book requirements; "
            "collect or backfill real orderbook_top_snapshots, or use a separate non-production candle-only manifest"
        )
    depth_available = any(bool(item.get("depth_available")) for item in split_reports.values())
    return {
        "required": bool(spec.required),
        "missing_policy": spec.missing_policy,
        "min_coverage_pct": spec.min_coverage_pct,
        "observed_coverage_pct": coverage,
        "top_of_book_candle_quote_coverage": coverage,
        "top_of_book_candle_quote_coverage_pct": coverage,
        "top_of_book_candle_quote_expected_count": expected,
        "top_of_book_candle_quote_joined_count": joined,
        "signal_execution_quote_coverage": None,
        "signal_execution_quote_coverage_pct": None,
        "signal_execution_quote_coverage_status": "not_computable_without_strategy_signal_run",
        "signal_level_depth_coverage_pct": None,
        "signal_level_depth_coverage_status": "not_computable_without_strategy_signal_run",
        "signal_depth_coverage_limitation": "readiness_sql_scan_has_no_strategy_signal_events",
        "depth_available": depth_available,
        "depth_evidence_available": depth_available,
        "depth_availability_source": (
            "sqlite_orderbook_depth_levels" if depth_available else "orderbook_depth_levels_missing_or_empty"
        ),
        "depth_liquidity_sufficiency_status": "not_implemented_order_size_depth_walk_required",
        "expected_signal_count": expected,
        "joined_count": joined,
        "missing_count": expected - joined,
        "status": status,
        "reasons": reasons,
        "next_action": next_action,
    }


def _execution_capability_payload(*, manifest: ExperimentManifest, top_of_book: dict[str, Any]) -> dict[str, Any]:
    policy = manifest.execution_timing
    evidence_tier = {
        "candle_close_legacy": "candle_close_optimistic",
        "next_candle_open": "candle_next_open",
        "first_orderbook_after_decision": "top_of_book_after_decision",
        "latency_adjusted_orderbook": "latency_adjusted_top_of_book",
    }.get(policy.fill_reference_policy, "unknown")
    contract = build_execution_capability_contract(
        fill_reference_policy=policy.fill_reference_policy,
        top_of_book_required=bool(manifest.dataset.top_of_book.required) if manifest.dataset.top_of_book else False,
        top_of_book_available=top_of_book.get("status") == "PASS" and int(top_of_book.get("joined_count") or 0) > 0,
        top_of_book_is_full_depth=False,
        full_orderbook_depth_required=policy.depth_required,
        trade_ticks_required=policy.trade_tick_required,
        queue_position_required=policy.queue_position_required,
        market_impact_model_required=policy.market_impact_required,
        intra_candle_path_required=policy.intra_candle_path_required,
        full_orderbook_depth_available=bool(top_of_book.get("depth_available")),
        trade_ticks_available=False,
        queue_position_available=False,
        market_impact_model_available=False,
        intra_candle_path_available=False,
        evidence_tier=evidence_tier,
    )
    unavailable = list(contract.get("unavailable_required_capabilities") or [])
    next_action = "none"
    if unavailable:
        next_action = "remove unsupported execution capability requirements or add matching depth/tick/queue/impact evidence and models"
    return {
        "contract": contract,
        "contract_hash": contract["execution_capability_contract_hash"],
        "evidence_tier": contract["evidence_tier"],
        "unavailable_required_capabilities": unavailable,
        "market_impact_required": policy.market_impact_required,
        "depth_required": policy.depth_required,
        "depth_available": contract["available_capabilities"]["full_orderbook_depth"],
        "signal_level_depth_coverage_pct": top_of_book.get("signal_level_depth_coverage_pct"),
        "signal_level_depth_coverage_status": top_of_book.get("signal_level_depth_coverage_status"),
        "depth_liquidity_sufficiency_status": top_of_book.get("depth_liquidity_sufficiency_status"),
        "market_impact_model_available": contract["available_capabilities"]["market_impact_model"],
        "top_of_book_is_full_depth": contract["available_capabilities"]["top_of_book_is_full_depth"],
        "status": "PASS" if not unavailable else "FAIL",
        "next_action": next_action,
    }


def _execution_calibration_payload(
    *,
    manifest: ExperimentManifest,
    execution_calibration_path: str | Path | None,
) -> dict[str, Any]:
    required = bool(manifest.execution_model.calibration_required)
    if execution_calibration_path is None:
        status = "FAIL" if required else "WARN"
        return {
            "required": required,
            "artifact_path": None,
            "artifact_hash": None,
            "status": status,
            "reasons": ["execution_calibration_missing"],
            "next_action": "provide --execution-calibration with a repo-generated calibration artifact" if required else "optional",
        }
    try:
        artifact = load_calibration_artifact(execution_calibration_path)
    except Exception as exc:
        return {
            "required": required,
            "artifact_path": str(execution_calibration_path),
            "artifact_hash": None,
            "status": "FAIL",
            "reasons": [str(exc)],
            "next_action": "regenerate the execution calibration artifact",
        }
    gates = [
        compare_calibration_to_scenario(
            calibration=artifact,
            assumed_slippage_bps=scenario.slippage_bps + scenario.market_order_extra_cost_bps,
            assumed_latency_ms=scenario.latency_ms,
            assumed_partial_fill_rate=scenario.partial_fill_rate,
            assumed_order_failure_rate=scenario.order_failure_rate,
            expected_market=manifest.market,
            expected_interval=manifest.interval,
            expected_execution_timing_policy=manifest.execution_timing.as_dict(),
            require_content_hash=required,
            min_sample_count=ExecutionQualityThresholds().min_sample,
            require_quality_gate_pass=required or manifest.execution_model.calibration_strictness == "fail",
        )
        for scenario in manifest.execution_model.scenarios
    ]
    reasons = sorted({str(reason) for gate in gates for reason in gate.get("reasons") or []})
    status = "PASS" if not reasons else ("FAIL" if required or manifest.execution_model.calibration_strictness == "fail" else "WARN")
    return {
        "required": required,
        "artifact_path": str(Path(execution_calibration_path).expanduser()),
        "artifact_hash": artifact.get("content_hash"),
        "min_sample_count": ExecutionQualityThresholds().min_sample,
        "scenario_gates": gates,
        "status": status,
        "reasons": reasons,
        "next_action": "none" if status == "PASS" else "regenerate or collect sufficient live execution calibration evidence",
    }


def _persistent_missing_classification_payload(
    *,
    path: str | Path | None,
    manifest: ExperimentManifest,
    db_path: Path,
) -> dict[str, Any]:
    if path is None:
        return {
            "provided": False,
            "artifact_path": None,
            "artifact_hash": None,
            "status": "NOT_PROVIDED",
            "production_gate_effect": "none",
            "summary": {},
            "reasons": [],
            "next_action": "none",
        }
    resolved_path = Path(path).expanduser().resolve()
    try:
        artifact = json.loads(resolved_path.read_text(encoding="utf-8"))
        _validate_persistent_missing_classification_artifact(
            artifact=artifact,
            manifest=manifest,
            db_path=db_path,
        )
    except Exception as exc:
        return {
            "provided": True,
            "artifact_path": str(resolved_path),
            "artifact_hash": None,
            "status": "FAIL",
            "production_gate_effect": "none",
            "summary": {},
            "reasons": [str(exc)],
            "next_action": "regenerate classify-persistent-missing-candles from matching manifest, DB, missing ranges, and retry attempts",
        }
    summary = dict(artifact.get("summary") or {})
    return {
        "provided": True,
        "artifact_path": str(resolved_path),
        "artifact_hash": artifact.get("content_hash"),
        "status": "DIAGNOSTIC_ONLY",
        "production_gate_effect": "none",
        "summary": {
            "exchange_gap_candidate": int(summary.get("exchange_gap_candidate") or 0),
            "api_unavailable_candidate": int(summary.get("api_unavailable_candidate") or 0),
            "no_trade_missing_candidate": int(summary.get("no_trade_missing_candidate") or 0),
            "unclassified_missing": int(summary.get("unclassified_missing") or 0),
        },
        "reasons": [],
        "next_action": persistent_missing_overall_next_action(summary),
    }


def _validate_persistent_missing_classification_artifact(
    *,
    artifact: dict[str, Any],
    manifest: ExperimentManifest,
    db_path: Path,
) -> None:
    if artifact.get("artifact_type") != "persistent_missing_candle_classification":
        raise ValueError("missing classification artifact_type must be persistent_missing_candle_classification")
    if artifact.get("schema_version") != 1:
        raise ValueError("unsupported missing classification schema_version")
    embedded_hash = artifact.get("content_hash")
    if not isinstance(embedded_hash, str) or not embedded_hash.startswith("sha256:"):
        raise ValueError("missing classification content_hash is required")
    recomputed_payload = {key: value for key, value in artifact.items() if key != "content_hash"}
    from .hashing import sha256_prefixed

    if sha256_prefixed(recomputed_payload) != embedded_hash:
        raise ValueError("missing classification content_hash does not match artifact body")
    if artifact.get("manifest_hash") != manifest.manifest_hash():
        raise ValueError("missing classification manifest_hash does not match manifest")
    if artifact.get("market") != manifest.market or artifact.get("interval") != manifest.interval:
        raise ValueError("missing classification market/interval does not match manifest")
    artifact_db = Path(str(artifact.get("db_path") or "")).expanduser().resolve()
    if artifact_db != db_path:
        raise ValueError("missing classification db_path does not match configured DB_PATH")
    if artifact.get("policy_effect") != "diagnostic_only_no_gate_relaxation":
        raise ValueError("missing classification policy_effect must not relax gates")
    missing_ranges_hash = artifact.get("missing_ranges_hash")
    if not isinstance(missing_ranges_hash, str) or not missing_ranges_hash.startswith("sha256:"):
        raise ValueError("missing classification missing_ranges_hash is required")
    retry_attempts_hash = artifact.get("retry_attempts_hash")
    if not isinstance(retry_attempts_hash, str) or not retry_attempts_hash.startswith("sha256:"):
        raise ValueError("missing classification retry_attempts_hash is required")
    summary = artifact.get("summary") if isinstance(artifact.get("summary"), dict) else {}
    if summary.get("production_gate_effect") != "none":
        raise ValueError("missing classification summary production_gate_effect must be none")
    allowed_summary_keys = set(PERSISTENT_MISSING_CLASSIFICATIONS) | {
        "classified_range_count",
        "persistent_range_count",
        "production_gate_effect",
    }
    unexpected_summary_keys = sorted(str(key) for key in summary if key not in allowed_summary_keys)
    if unexpected_summary_keys:
        raise ValueError(f"missing classification summary has unsupported keys: {unexpected_summary_keys}")
    limitations = artifact.get("limitations") if isinstance(artifact.get("limitations"), dict) else {}
    if limitations.get("synthetic_ohlcv_authorized") is not False:
        raise ValueError("missing classification must not authorize synthetic OHLCV")
    if limitations.get("production_gate_relaxed") is not False:
        raise ValueError("missing classification must not relax production gates")
    if limitations.get("top_of_book_satisfied") is not False:
        raise ValueError("missing classification must not satisfy top-of-book")
    if limitations.get("execution_calibration_satisfied") is not False:
        raise ValueError("missing classification must not satisfy execution calibration")
    ranges = artifact.get("ranges")
    if not isinstance(ranges, list):
        raise ValueError("missing classification ranges must be a list")
    counts = {classification: 0 for classification in PERSISTENT_MISSING_CLASSIFICATIONS}
    for item in ranges:
        if not isinstance(item, dict):
            raise ValueError("missing classification range must be an object")
        classification = item.get("classification")
        if classification not in PERSISTENT_MISSING_CLASSIFICATIONS:
            raise ValueError("missing classification range has unsupported classification")
        if item.get("gate_effect") != "none":
            raise ValueError("missing classification range gate_effect must be none")
        if item.get("confidence") != "candidate":
            raise ValueError("missing classification range confidence must be candidate")
        counts[str(classification)] += 1
    for classification, count in counts.items():
        if int(summary.get(classification) or 0) != count:
            raise ValueError(f"missing classification summary count mismatch for {classification}")
    if int(summary.get("persistent_range_count") or 0) != len(ranges):
        raise ValueError("missing classification summary persistent_range_count does not match ranges")
    if int(summary.get("classified_range_count") or 0) != len(ranges):
        raise ValueError("missing classification summary classified_range_count does not match ranges")


def _next_actions(
    *,
    split_reports: dict[str, dict[str, Any]],
    top_of_book: dict[str, Any],
    execution_capability: dict[str, Any],
    execution_calibration: dict[str, Any],
    walk_forward: dict[str, Any],
    persistent_missing_classification: dict[str, Any],
) -> list[str]:
    actions: list[str] = []
    if any(split["quality_status"] != "PASS" for split in split_reports.values()):
        actions.append("backfill missing historical candles for the manifest split ranges")
    if persistent_missing_classification.get("provided") and persistent_missing_classification.get("status") != "NOT_PROVIDED":
        action = str(persistent_missing_classification.get("next_action") or "none")
        if action != "none":
            actions.append(action)
    if top_of_book["status"] == "FAIL":
        actions.append(str(top_of_book["next_action"]))
    if execution_capability["status"] == "FAIL":
        actions.append(str(execution_capability["next_action"]))
    if execution_calibration["status"] == "FAIL":
        actions.append(str(execution_calibration["next_action"]))
    if walk_forward["status"] == "FAIL":
        actions.append(str(walk_forward["next_action"]))
    return actions or ["none"]


def _print_readiness(report: dict[str, Any]) -> None:
    print("[RESEARCH-READINESS]")
    print(f"  status={report['status']}")
    print(f"  manifest_path={report['manifest_path']}")
    print(f"  manifest_hash={report['manifest_hash']}")
    print(f"  MODE={report['mode']}")
    print(f"  DB_PATH={report['db_path']}")
    print(f"  env_file={report['env_file']} env_loaded={1 if report['env_loaded'] else 0} env_exists={1 if report['env_exists'] else 0}")
    print(f"  market={report['market']} interval={report['interval']}")
    readiness_mode = report["readiness_mode"]
    print(
        "  readiness_mode="
        f"type={readiness_mode['readiness_type']} production_bound={1 if readiness_mode['production_bound'] else 0} "
        f"candle_only_diagnostic={1 if readiness_mode['candle_only_diagnostic'] else 0}"
    )
    for split_name, split in report["splits"].items():
        print(
            f"  split={split_name} expected_candles={split['expected_candle_buckets']} "
            f"present_candles={split['present_candle_buckets']} missing={split['missing_count']} "
            f"coverage_pct={split['coverage_pct']} first_ts={split['first_ts']} last_ts={split['last_ts']} "
            f"duplicates={split['duplicate_candle_key_count']} interval_mismatch={split['interval_mismatch_count']} "
            f"quality_status={split['quality_status']} reasons={','.join(split['quality_reasons']) if split['quality_reasons'] else 'none'}"
        )
    tob = report["top_of_book"]
    print(
        "  top_of_book="
        f"required={1 if tob['required'] else 0} missing_policy={tob['missing_policy']} "
        f"min_coverage_pct={tob['min_coverage_pct']} observed_coverage_pct={tob['observed_coverage_pct']} "
        f"status={tob['status']} reasons={','.join(tob['reasons']) if tob['reasons'] else 'none'}"
    )
    print(f"  top_of_book_next_action={tob['next_action']}")
    cap = report["execution_capability"]
    print(
        "  execution_capability="
        f"hash={cap['contract_hash']} evidence_tier={cap['evidence_tier']} "
        f"unavailable_required={','.join(cap['unavailable_required_capabilities']) if cap['unavailable_required_capabilities'] else 'none'} "
        f"market_impact_required={1 if cap['market_impact_required'] else 0} "
        f"market_impact_model_available={1 if cap['market_impact_model_available'] else 0} "
        f"top_of_book_is_full_depth={1 if cap['top_of_book_is_full_depth'] else 0} "
        f"status={cap['status']}"
    )
    print(f"  execution_capability_next_action={cap['next_action']}")
    cal = report["execution_calibration"]
    print(
        "  execution_calibration="
        f"required={1 if cal['required'] else 0} artifact_path={cal['artifact_path']} "
        f"artifact_hash={cal['artifact_hash']} status={cal['status']} "
        f"reasons={','.join(cal['reasons']) if cal['reasons'] else 'none'}"
    )
    wf = report["walk_forward"]
    print(
        "  walk_forward="
        f"required={1 if wf['required'] else 0} available_windows={wf['available_windows']} "
        f"expected_min_windows={wf['expected_min_windows']} status={wf['status']} "
        f"reasons={','.join(wf['reasons']) if wf['reasons'] else 'none'}"
    )
    pmc = report["persistent_missing_classification"]
    print(
        "  persistent_missing_classification="
        f"provided={1 if pmc['provided'] else 0} status={pmc['status']} "
        f"artifact_hash={pmc['artifact_hash']} production_gate_effect={pmc['production_gate_effect']} "
        f"reasons={','.join(pmc['reasons']) if pmc['reasons'] else 'none'}"
    )
    if pmc["provided"]:
        summary = pmc.get("summary") or {}
        print(
            "  persistent_missing_classification_summary="
            f"exchange_gap_candidate={summary.get('exchange_gap_candidate', 0)} "
            f"api_unavailable_candidate={summary.get('api_unavailable_candidate', 0)} "
            f"no_trade_missing_candidate={summary.get('no_trade_missing_candidate', 0)} "
            f"unclassified_missing={summary.get('unclassified_missing', 0)}"
        )
    for action in report["next_actions"]:
        print(f"  next_action={action}")
