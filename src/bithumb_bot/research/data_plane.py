from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from bithumb_bot.bootstrap import get_last_explicit_env_load_summary
from bithumb_bot.config import PROJECT_ROOT, settings
from bithumb_bot.marketdata import BASE_URL
from bithumb_bot.historical_backfill import backfill_candles
from bithumb_bot.orderbook_depth_store import summarize_orderbook_depth_evidence
from bithumb_bot.paths import PathManager
from bithumb_bot.public_api import decode_json_response, extract_api_error
from bithumb_bot.public_api_minute_candles import interval_to_minute_unit, parse_minute_candles
from bithumb_bot.storage_io import write_json_atomic

from .dataset_snapshot import (
    DatasetQualityReport,
    _db_schema_fingerprint,
    _expected_bucket_count,
    _interval_ms,
    _is_expected_bucket,
    _split_range,
)
from .datasets.registry import default_dataset_adapter_registry
from .experiment_manifest import ExperimentManifest, load_manifest
from .hashing import sha256_prefixed
from .validation_protocol import _rolling_walk_forward_windows

KST = ZoneInfo("Asia/Seoul")
PERSISTENT_MISSING_CLASSIFICATIONS = {
    "exchange_gap_candidate",
    "api_unavailable_candidate",
    "no_trade_missing_candidate",
    "unclassified_missing",
}
RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}

MISSING_CLASSIFICATIONS = {
    "untried_missing",
    "retried_recovered",
    "retry_persistent_missing",
    "exchange_gap_candidate",
    "api_unavailable_candidate",
    "no_trade_missing_candidate",
    "unclassified_missing",
}


DATA_PLANE_POLICY_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class DataPlanePolicy:
    snapshot_storage_mode: str
    worker_snapshot_load_policy: str
    dataset_cache_budget_mb: int
    memory_map_enabled: bool
    cache_key_material: dict[str, object]
    disabled_reasons: tuple[str, ...]
    effective_max_workers: int

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": DATA_PLANE_POLICY_SCHEMA_VERSION,
            "snapshot_storage_mode": self.snapshot_storage_mode,
            "worker_snapshot_load_policy": self.worker_snapshot_load_policy,
            "dataset_cache_budget_mb": self.dataset_cache_budget_mb,
            "memory_map_enabled": self.memory_map_enabled,
            "cache_key_material": dict(self.cache_key_material),
            "disabled_reasons": list(self.disabled_reasons),
            "effective_max_workers": self.effective_max_workers,
        }


def build_data_plane_policy(
    *,
    manifest_hash: str,
    dataset_hashes: dict[str, str],
    split_names: tuple[str, ...] | list[str],
    memory_budget_mb: int | None,
    estimated_total_memory_bytes: int | None,
    effective_max_workers: int,
) -> DataPlanePolicy:
    disabled_reasons: list[str] = []
    budget = int(memory_budget_mb) if memory_budget_mb is not None else None
    estimated_mb = (
        int(estimated_total_memory_bytes) // (1024 * 1024)
        if estimated_total_memory_bytes is not None
        else None
    )
    cache_budget_mb = 0
    load_policy = "db_reload"
    snapshot_mode = "in_memory_parent_snapshot"
    if budget is None:
        disabled_reasons.append("memory_budget_unknown")
    elif estimated_mb is None:
        disabled_reasons.append("estimated_total_memory_unknown")
    else:
        headroom = budget - estimated_mb
        if headroom > 0:
            cache_budget_mb = max(1, headroom // 2)
            load_policy = "worker_local_lazy_cache"
        else:
            disabled_reasons.append("memory_headroom_unavailable")
    split_tuple = tuple(str(item) for item in split_names)
    key_hashes = {name: str(dataset_hashes.get(name, "")) for name in split_tuple}
    return DataPlanePolicy(
        snapshot_storage_mode=snapshot_mode,
        worker_snapshot_load_policy=load_policy,
        dataset_cache_budget_mb=cache_budget_mb,
        memory_map_enabled=False,
        cache_key_material={
            "manifest_hash": manifest_hash,
            "split_names": list(split_tuple),
            "dataset_hashes": key_hashes,
            "dataset_hash": sha256_prefixed(key_hashes),
        },
        disabled_reasons=tuple(disabled_reasons),
        effective_max_workers=max(1, int(effective_max_workers)),
    )


@dataclass(frozen=True)
class RangeCoverage:
    expected_buckets: int
    present_buckets: int
    missing_buckets: int
    coverage_pct: float

    def as_dict(self) -> dict[str, object]:
        return {
            "expected_buckets": self.expected_buckets,
            "present_buckets": self.present_buckets,
            "missing_buckets": self.missing_buckets,
            "coverage_pct": self.coverage_pct,
        }


def split_names(manifest: ExperimentManifest) -> tuple[str, ...]:
    names = ["train", "validation"]
    if manifest.dataset.split.final_holdout is not None:
        names.append("final_holdout")
    return tuple(names)


def build_dataset_quality_report_sql(
    *,
    db_path: str | Path,
    manifest: ExperimentManifest,
    split_name: str,
    max_missing_ranges: int | None = 20,
    max_missing_sample: int = 20,
    include_top_of_book: bool = True,
) -> DatasetQualityReport:
    adapter = default_dataset_adapter_registry().resolve(manifest.dataset.source)
    if not getattr(adapter, "supports_sqlite_streaming_quality_scan", False):
        raise ValueError(f"dataset_adapter_sqlite_streaming_not_supported:{manifest.dataset.source}")
    if manifest.dataset.top_of_book is not None:
        default_dataset_adapter_registry().resolve_top_of_book(manifest.dataset.top_of_book.source)
    date_range = _split_range(manifest, split_name)
    interval_ms = _interval_ms(manifest.interval)
    start_ts = date_range.start_ts_ms()
    end_ts = date_range.end_ts_ms()
    expected_count = _expected_bucket_count(start_ts=start_ts, end_ts=end_ts, interval_ms=interval_ms)
    stats = _scan_candles_sql(
        db_path=db_path,
        market=manifest.market,
        interval=manifest.interval,
        start_ts=start_ts,
        end_ts=end_ts,
        interval_ms=interval_ms,
        max_missing_ranges=max_missing_ranges,
        max_missing_sample=max_missing_sample,
    )
    top_of_book = (
        _top_of_book_split_sql(
            db_path=db_path,
            manifest=manifest,
            start_ts=start_ts,
            end_ts=end_ts,
            expected_signal_count=int(stats["actual_candle_count"]),
        )
        if include_top_of_book
        else {}
    )

    reasons: list[str] = []
    if int(stats["missing_bucket_count"]):
        reasons.append("missing_candles")
    if int(stats["duplicate_key_count"]):
        reasons.append("duplicate_candle_keys")
    if int(stats["non_monotonic_ts_count"]):
        reasons.append("non_monotonic_timestamps")
    if int(stats["interval_mismatch_count"]):
        reasons.append("interval_mismatch")
    if int(stats["ohlc_violation_count"]):
        reasons.append("ohlc_invariant_violation")
    if int(stats["non_positive_price_count"]):
        reasons.append("non_positive_price")
    if int(stats["negative_volume_count"]):
        reasons.append("negative_volume")
    if int(stats["unexpected_bucket_count"]):
        reasons.append("unexpected_candle_bucket")

    depth_summary = _depth_summary_sql(
        db_path=db_path,
        market=manifest.market,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    depth_rows_available = bool(depth_summary["l2_depth_rows_available"])
    depth_complete_snapshots_available = bool(depth_summary["l2_depth_complete_snapshots_available"])
    payload: dict[str, Any] = {
        "schema_version": 2,
        "artifact_type": "dataset_quality_report",
        "scan_method": "sqlite_streaming",
        "dataset_source": manifest.dataset.source,
        "adapter_name": "sqlite_candle_adapter",
        "adapter_version": "1",
        "source": manifest.dataset.source,
        "market": manifest.market,
        "interval": manifest.interval,
        "snapshot_id": manifest.dataset.snapshot_id,
        "split_name": split_name,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "expected_candle_count": expected_count,
        "actual_candle_count": int(stats["actual_candle_count"]),
        "present_expected_bucket_count": int(stats["present_expected_bucket_count"]),
        "coverage_pct": stats["coverage_pct"],
        "missing_bucket_count": int(stats["missing_bucket_count"]),
        "missing_bucket_ranges": stats["missing_bucket_ranges"],
        "missing_bucket_sample": stats["missing_bucket_sample"],
        "missing_ranges_truncated": bool(stats["missing_ranges_truncated"]),
        "duplicate_key_count": int(stats["duplicate_key_count"]),
        "non_monotonic_ts_count": int(stats["non_monotonic_ts_count"]),
        "non_monotonic_detection": "ordered_sql_scan_with_duplicate_key_check",
        "interval_mismatch_count": int(stats["interval_mismatch_count"]),
        "unexpected_bucket_count": int(stats["unexpected_bucket_count"]),
        "ohlc_violation_count": int(stats["ohlc_violation_count"]),
        "non_positive_price_count": int(stats["non_positive_price_count"]),
        "negative_volume_count": int(stats["negative_volume_count"]),
        "first_ts": stats["first_ts"],
        "last_ts": stats["last_ts"],
        "db_schema_fingerprint": _safe_db_schema_fingerprint(db_path),
        "dataset_content_hash": "not_materialized:sqlite_streaming_readiness_scan",
        "canonical_snapshot_hash": "not_materialized:sqlite_streaming_readiness_scan",
        "source_content_hash": manifest.dataset.source_content_hash
        or "missing:sqlite_streaming_source_content_hash_not_declared",
        "source_schema_hash": manifest.dataset.source_schema_hash or _safe_db_schema_fingerprint(db_path),
        "source_hash_status": "present" if manifest.dataset.source_content_hash else "missing_compatibility_streaming_scan",
        "source_schema_hash_status": "present",
        "adapter_provenance": {
            "sqlite": {
                "source_locator_policy": "runtime_db_path_excluded_from_dataset_quality_hash",
                "db_schema_fingerprint": _safe_db_schema_fingerprint(db_path),
                "tables": _sqlite_present_tables(db_path),
                "scan_method": "sqlite_streaming",
            }
        },
        "quality_gate_status": "PASS" if not reasons else "FAIL",
        "quality_gate_reasons": reasons,
        "limitations": {
            "orderbook_depth_available": depth_complete_snapshots_available,
            "l2_depth_evidence_available": depth_complete_snapshots_available,
            "l2_depth_rows_available": depth_rows_available,
            "l2_depth_complete_snapshots_available": depth_complete_snapshots_available,
            "full_orderbook_depth_available": False,
            "trade_tick_evidence_available": False,
            "queue_evidence_available": False,
            "impact_model_evidence_available": False,
            "top_of_book_available": top_of_book.get("top_of_book_joined_count", 0) > 0,
            "intra_candle_path_available": False,
            "execution_reference_price": "configured_by_execution_timing_policy",
            "available_execution_reference_sources": [
                "candle_ohlcv",
                "top_of_book_if_requested",
            ],
            "intra_candle_policy": "configured_by_execution_timing_policy",
            "top_of_book_is_full_depth": False,
        },
        "depth_available": depth_complete_snapshots_available,
        "depth_available_semantics": "stored_l2_depth_complete_snapshots_exist_not_execution_model_used",
        "depth_evidence_available": depth_complete_snapshots_available,
        "l2_depth_evidence_available": depth_complete_snapshots_available,
        "depth_availability_source": (
            "sqlite_orderbook_depth_levels_complete_snapshots"
            if depth_complete_snapshots_available
            else ("sqlite_orderbook_depth_levels_rows_only" if depth_rows_available else "orderbook_depth_levels_missing_or_empty")
        ),
        **depth_summary,
        "signal_level_depth_coverage_pct": None,
        "signal_level_depth_coverage_status": "not_computed_depth_walk_not_wired_to_research_backtest",
        "depth_liquidity_sufficiency_status": "not_computed_depth_walk_not_wired_to_research_backtest",
    }
    payload["adapter_provenance_hash"] = sha256_prefixed(payload["adapter_provenance"])
    if top_of_book:
        payload.update(top_of_book)
        tob_reasons = list(top_of_book.get("top_of_book_gate_reasons") or [])
        if top_of_book.get("top_of_book_gate_status") == "FAIL":
            payload["quality_gate_status"] = "FAIL"
            payload["quality_gate_reasons"] = list(payload["quality_gate_reasons"]) + tob_reasons
    payload["content_hash"] = sha256_prefixed(payload)
    return DatasetQualityReport(payload=payload)


def build_missing_candle_ranges_artifact(
    *,
    manifest_path: str | Path,
    db_path: str | Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    resolved_manifest_path = Path(manifest_path).expanduser().resolve()
    manifest = load_manifest(resolved_manifest_path)
    resolved_db_path = Path(db_path or settings.DB_PATH).expanduser().resolve()
    now = generated_at or datetime.now(UTC).isoformat()
    splits: dict[str, Any] = {}
    for split_name in split_names(manifest):
        report = build_dataset_quality_report_sql(
            db_path=resolved_db_path,
            manifest=manifest,
            split_name=split_name,
            max_missing_ranges=None,
            include_top_of_book=False,
        ).payload
        ranges = [
            _artifact_range(
                split_name=split_name,
                start_ts=int(item["start_ts"]),
                end_ts=int(item["end_ts"]),
                bucket_count=int(item["bucket_count"]),
            )
            for item in report.get("missing_bucket_ranges") or []
        ]
        splits[split_name] = {
            "expected_buckets": report["expected_candle_count"],
            "present_buckets": report["present_expected_bucket_count"],
            "missing_buckets": report["missing_bucket_count"],
            "coverage_pct": report["coverage_pct"],
            "ranges": ranges,
        }
    payload: dict[str, Any] = {
        "schema_version": 1,
        "artifact_type": "missing_candle_ranges",
        "manifest_path": str(resolved_manifest_path),
        "manifest_hash": manifest.manifest_hash(),
        "db_path": str(resolved_db_path),
        "market": manifest.market,
        "interval": manifest.interval,
        "generated_at": now,
        "timezone_contract": {
            "canonical_ts": "utc_epoch_ms",
            "display_timezones": ["UTC", "Asia/Seoul"],
            "retry_plan_basis": "utc_days_derived_from_exact_missing_epoch_ms_ranges",
        },
        "splits": splits,
    }
    payload["content_hash"] = sha256_prefixed(payload)
    return payload


def write_missing_candle_ranges_artifact(
    *,
    manifest_path: str | Path,
    out_path: str | Path,
) -> dict[str, Any]:
    payload = build_missing_candle_ranges_artifact(manifest_path=manifest_path)
    resolved_out = _validate_report_artifact_out_path(out_path)
    write_json_atomic(resolved_out, payload)
    return payload


def retry_missing_candles_from_artifact(
    *,
    manifest_path: str | Path,
    missing_ranges_path: str | Path,
    out_path: str | Path,
    min_buckets: int = 1,
    max_attempts: int = 1,
    split: str | None = None,
    limit: int | None = None,
    request_interval_ms: int = 0,
    max_retries: int = 3,
    backfill_func: Callable[..., Any] = backfill_candles,
) -> dict[str, Any]:
    if max_attempts < 1:
        raise ValueError("--max-attempts must be >= 1")
    resolved_manifest_path = Path(manifest_path).expanduser().resolve()
    manifest = load_manifest(resolved_manifest_path)
    artifact = json.loads(Path(missing_ranges_path).expanduser().read_text(encoding="utf-8"))
    _validate_missing_artifact(artifact=artifact, manifest=manifest, db_path=Path(settings.DB_PATH).expanduser().resolve())

    attempts: list[dict[str, Any]] = []
    selected = _select_missing_ranges(artifact=artifact, min_buckets=min_buckets, split=split, limit=limit)
    for item in selected:
        before = _range_coverage(
            db_path=settings.DB_PATH,
            market=manifest.market,
            interval=manifest.interval,
            start_ts=int(item["start_ts"]),
            end_ts=int(item["end_ts"]),
        )
        backfill_results: list[dict[str, Any]] = []
        for attempt_index in range(max_attempts):
            for day in item["retry_utc_days"]:
                try:
                    result = backfill_func(
                        market=manifest.market,
                        interval=manifest.interval,
                        start=str(day),
                        end=str(day),
                        dry_run=False,
                        request_interval_ms=request_interval_ms,
                        max_retries=max_retries,
                    )
                except Exception as exc:
                    backfill_results.append(
                        _backfill_exception_attempt_payload(
                            attempt_index=attempt_index + 1,
                            retry_utc_day=str(day),
                            exc=exc,
                        )
                    )
                    continue
                backfill_results.append(
                    {
                        "attempt_index": attempt_index + 1,
                        "retry_utc_day": str(day),
                        "progress_status": getattr(getattr(result, "progress", None), "status", None),
                        "progress_reason": getattr(getattr(result, "progress", None), "reason", None),
                        "api_fetch_status": getattr(result, "api_fetch_status", None),
                        "cursor_status": getattr(result, "cursor_status", None),
                        "db_write_status": getattr(result, "db_write_status", None),
                        "coverage_status": getattr(result, "coverage_status", None),
                        "upserted_count": getattr(getattr(result, "progress", None), "upserted_count", None),
                        "recovered_missing_bucket_count": getattr(
                            getattr(result, "progress", None), "recovered_missing_bucket_count", None
                        ),
                        "coverage": getattr(result, "coverage", None),
                    }
                )
        after = _range_coverage(
            db_path=settings.DB_PATH,
            market=manifest.market,
            interval=manifest.interval,
            start_ts=int(item["start_ts"]),
            end_ts=int(item["end_ts"]),
        )
        recovered = after.missing_buckets == 0
        classification = "retried_recovered" if recovered else "retry_persistent_missing"
        attempts.append(
            {
                "split": item["split"],
                "start_ts": item["start_ts"],
                "end_ts": item["end_ts"],
                "start_utc": item["start_utc"],
                "end_utc": item["end_utc"],
                "start_kst": item["start_kst"],
                "end_kst": item["end_kst"],
                "bucket_count": item["bucket_count"],
                "retry_utc_days": item["retry_utc_days"],
                "before": before.as_dict(),
                "after": after.as_dict(),
                "recovered_buckets": max(0, before.missing_buckets - after.missing_buckets),
                "classification": classification,
                "backfill_attempts": backfill_results,
            }
        )

    payload: dict[str, Any] = {
        "schema_version": 1,
        "artifact_type": "missing_candle_retry_attempts",
        "manifest_path": str(resolved_manifest_path),
        "manifest_hash": manifest.manifest_hash(),
        "missing_ranges_path": str(Path(missing_ranges_path).expanduser().resolve()),
        "missing_ranges_hash": artifact.get("content_hash"),
        "db_path": str(Path(settings.DB_PATH).expanduser().resolve()),
        "market": manifest.market,
        "interval": manifest.interval,
        "generated_at": datetime.now(UTC).isoformat(),
        "filters": {
            "min_buckets": int(min_buckets),
            "max_attempts": int(max_attempts),
            "split": split,
            "limit": limit,
            "request_interval_ms": int(request_interval_ms),
            "max_retries": int(max_retries),
        },
        "attempt_count": len(attempts),
        "attempts": attempts,
        "summary": {
            "retried_recovered": sum(1 for item in attempts if item["classification"] == "retried_recovered"),
            "retry_persistent_missing": sum(1 for item in attempts if item["classification"] == "retry_persistent_missing"),
        },
    }
    payload["content_hash"] = sha256_prefixed(payload)
    write_json_atomic(_validate_report_artifact_out_path(out_path), payload)
    return payload


def build_missing_candle_source_probe_artifact(
    *,
    manifest_path: str | Path,
    missing_ranges_path: str | Path,
    generated_at: str | None = None,
    split: str | None = None,
    limit: int | None = None,
    count: int = 3,
    client_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    resolved_manifest_path = Path(manifest_path).expanduser().resolve()
    resolved_missing_path = Path(missing_ranges_path).expanduser().resolve()
    manifest = load_manifest(resolved_manifest_path)
    resolved_db_path = Path(settings.DB_PATH).expanduser().resolve()
    missing_artifact = json.loads(resolved_missing_path.read_text(encoding="utf-8"))
    _validate_missing_artifact(artifact=missing_artifact, manifest=manifest, db_path=resolved_db_path)
    minute_unit = interval_to_minute_unit(manifest.interval)
    interval_ms = minute_unit * 60_000
    selected = _select_missing_ranges(artifact=missing_artifact, min_buckets=1, split=split, limit=limit)
    probe_records: list[dict[str, Any]] = []
    factory = client_factory or (lambda: httpx.Client(base_url=BASE_URL, timeout=15.0))
    with factory() as client:
        for item in selected:
            for label, target_ts in _probe_targets_for_range(item, interval_ms=interval_ms):
                probe_records.append(
                    _probe_minute_candle_source(
                        client=client,
                        market=manifest.market,
                        minute_unit=minute_unit,
                        count=max(1, int(count)),
                        target_ts=target_ts,
                        target_label=label,
                    )
                )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "artifact_type": "missing_candle_source_probe",
        "manifest_path": str(resolved_manifest_path),
        "manifest_hash": manifest.manifest_hash(),
        "missing_ranges_path": str(resolved_missing_path),
        "missing_ranges_hash": missing_artifact["content_hash"],
        "db_path": str(resolved_db_path),
        "market": manifest.market,
        "interval": manifest.interval,
        "minute_unit": minute_unit,
        "generated_at": generated_at or datetime.now(UTC).isoformat(),
        "probe_policy": {
            "target_labels": [
                "before_range_last_bucket",
                "range_start",
                "range_middle",
                "range_end",
                "after_range_first_bucket",
            ],
            "selected_range_count": len(selected),
            "probe_count_per_target": max(1, int(count)),
            "coverage_scope": "selected_missing_ranges_targeted_probe_not_exhaustive_unless_all_ranges_selected",
        },
        "probe_records": probe_records,
        "summary": {
            "target_present_true": sum(1 for item in probe_records if item["target_present"] is True),
            "target_present_false": sum(1 for item in probe_records if item["target_present"] is False),
            "api_error_count": sum(1 for item in probe_records if item["http_status"] is None or int(item["http_status"]) >= 400),
        },
    }
    payload["content_hash"] = sha256_prefixed(payload)
    return payload


def write_missing_candle_source_probe_artifact(
    *,
    manifest_path: str | Path,
    missing_ranges_path: str | Path,
    out_path: str | Path,
    split: str | None = None,
    limit: int | None = None,
    count: int = 3,
) -> dict[str, Any]:
    payload = build_missing_candle_source_probe_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=missing_ranges_path,
        split=split,
        limit=limit,
        count=count,
    )
    write_json_atomic(_validate_report_artifact_out_path(out_path), payload)
    return payload


def build_clean_candle_segments_artifact(
    *,
    db_path: str | Path | None = None,
    market: str,
    interval: str,
    min_days: int,
    generated_at: str | None = None,
) -> dict[str, Any]:
    resolved_db_path = Path(db_path or settings.DB_PATH).expanduser().resolve()
    minute_unit = interval_to_minute_unit(interval)
    interval_ms = minute_unit * 60_000
    min_segment_minutes = max(1, int(min_days)) * 24 * 60
    rows: list[tuple[int]] = []
    if resolved_db_path.exists():
        conn = sqlite3.connect(f"file:{resolved_db_path}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                """
                SELECT DISTINCT ts
                FROM candles
                WHERE pair=? AND interval=?
                ORDER BY ts ASC
                """,
                (market, interval),
            ).fetchall()
        finally:
            conn.close()
    segments: list[dict[str, Any]] = []
    run_start: int | None = None
    run_prev: int | None = None
    run_count = 0

    def close_run() -> None:
        nonlocal run_start, run_prev, run_count
        if run_start is not None and run_prev is not None and run_count >= min_segment_minutes:
            segments.append(
                {
                    "start_utc": _format_utc(run_start),
                    "end_utc": _format_utc(run_prev),
                    "bucket_count": run_count,
                    "coverage_pct": 100.0,
                    "missing_buckets": 0,
                    "source": "sqlite_distinct_ts_contiguous_scan",
                }
            )
        run_start = None
        run_prev = None
        run_count = 0

    for row in rows:
        ts = int(row[0])
        if run_start is None:
            run_start = ts
            run_prev = ts
            run_count = 1
            continue
        if run_prev is not None and ts - run_prev == interval_ms:
            run_prev = ts
            run_count += 1
            continue
        close_run()
        run_start = ts
        run_prev = ts
        run_count = 1
    close_run()
    payload: dict[str, Any] = {
        "schema_version": 1,
        "artifact_type": "clean_candle_segments",
        "db_path": str(resolved_db_path),
        "market": market,
        "interval": interval,
        "min_segment_minutes": min_segment_minutes,
        "generated_at": generated_at or datetime.now(UTC).isoformat(),
        "segments": segments,
    }
    payload["content_hash"] = sha256_prefixed(payload)
    return payload


def write_clean_candle_segments_artifact(
    *,
    market: str,
    interval: str,
    min_days: int,
    out_path: str | Path,
) -> dict[str, Any]:
    payload = build_clean_candle_segments_artifact(
        market=market,
        interval=interval,
        min_days=min_days,
    )
    write_json_atomic(_validate_report_artifact_out_path(out_path), payload)
    return payload


def build_persistent_missing_candle_classification_artifact(
    *,
    manifest_path: str | Path,
    missing_ranges_path: str | Path,
    retry_attempts_path: str | Path,
    source_probe_path: str | Path | None = None,
    generated_at: str | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_manifest_path = Path(manifest_path).expanduser().resolve()
    resolved_missing_path = Path(missing_ranges_path).expanduser().resolve()
    resolved_retry_path = Path(retry_attempts_path).expanduser().resolve()
    manifest = load_manifest(resolved_manifest_path)
    resolved_db_path = Path(db_path or settings.DB_PATH).expanduser().resolve()
    missing_artifact = json.loads(resolved_missing_path.read_text(encoding="utf-8"))
    retry_artifact = json.loads(resolved_retry_path.read_text(encoding="utf-8"))
    source_probe_artifact: dict[str, Any] | None = None

    _validate_missing_artifact(artifact=missing_artifact, manifest=manifest, db_path=resolved_db_path)
    _validate_retry_attempts_artifact(
        artifact=retry_artifact,
        manifest=manifest,
        db_path=resolved_db_path,
        missing_ranges_path=resolved_missing_path,
        missing_ranges_hash=str(missing_artifact.get("content_hash") or ""),
    )
    if source_probe_path is not None:
        resolved_probe_path = Path(source_probe_path).expanduser().resolve()
        source_probe_artifact = json.loads(resolved_probe_path.read_text(encoding="utf-8"))
        _validate_source_probe_artifact(
            artifact=source_probe_artifact,
            manifest=manifest,
            db_path=resolved_db_path,
            missing_ranges_path=resolved_missing_path,
            missing_ranges_hash=str(missing_artifact.get("content_hash") or ""),
        )

    ranges = [
        _classify_persistent_missing_attempt(
            attempt=attempt,
            retry_artifact_hash=str(retry_artifact["content_hash"]),
            source_probe_artifact=source_probe_artifact,
            db_path=resolved_db_path,
            market=manifest.market,
            interval=manifest.interval,
        )
        for attempt in retry_artifact.get("attempts") or []
        if attempt.get("classification") == "retry_persistent_missing"
    ]
    summary = {
        classification: sum(1 for item in ranges if item["classification"] == classification)
        for classification in sorted(PERSISTENT_MISSING_CLASSIFICATIONS)
    }
    summary["classified_range_count"] = len(ranges)
    summary["persistent_range_count"] = len(ranges)
    summary["production_gate_effect"] = "none"

    payload: dict[str, Any] = {
        "schema_version": 1,
        "artifact_type": "persistent_missing_candle_classification",
        "manifest_path": str(resolved_manifest_path),
        "manifest_hash": manifest.manifest_hash(),
        "missing_ranges_path": str(resolved_missing_path),
        "missing_ranges_hash": missing_artifact["content_hash"],
        "retry_attempts_path": str(resolved_retry_path),
        "retry_attempts_hash": retry_artifact["content_hash"],
        "source_probe_path": str(Path(source_probe_path).expanduser().resolve()) if source_probe_path is not None else None,
        "source_probe_hash": source_probe_artifact.get("content_hash") if source_probe_artifact else None,
        "db_path": str(resolved_db_path),
        "db_schema_fingerprint": _safe_db_schema_fingerprint(resolved_db_path),
        "market": manifest.market,
        "interval": manifest.interval,
        "generated_at": generated_at or datetime.now(UTC).isoformat(),
        "classifier_version": "persistent_missing_classifier_v1",
        "policy_effect": "diagnostic_only_no_gate_relaxation",
        "source_gap_policy": "diagnostic_only",
        "synthetic_candle_authority": "not_allowed",
        "ranges": ranges,
        "summary": summary,
        "limitations": {
            "classification_is_candidate_evidence_only": True,
            "synthetic_ohlcv_authorized": False,
            "synthetic_candle_authority": "not_allowed",
            "production_gate_relaxed": False,
            "top_of_book_satisfied": False,
            "execution_calibration_satisfied": False,
        },
    }
    payload["content_hash"] = sha256_prefixed(payload)
    return payload


def write_persistent_missing_candle_classification_artifact(
    *,
    manifest_path: str | Path,
    missing_ranges_path: str | Path,
    retry_attempts_path: str | Path,
    out_path: str | Path,
    source_probe_path: str | Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    payload = build_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=missing_ranges_path,
        retry_attempts_path=retry_attempts_path,
        source_probe_path=source_probe_path,
        generated_at=generated_at,
    )
    write_json_atomic(_validate_report_artifact_out_path(out_path), payload)
    return payload


def dataset_quality_policy_payload(manifest: ExperimentManifest) -> dict[str, Any]:
    raw = manifest.raw.get("dataset_quality_policy")
    if not isinstance(raw, dict):
        return {
            "source": "default_strict",
            "dense_candles_required": True,
            "missing_candle_policy": "fail",
            "allow_classified_no_trade_missing": False,
            "require_retry_attempts_for_missing_ranges": True,
            "max_unclassified_missing_buckets": 0,
            "readiness_gate_effect": "strict_fail_closed",
            "production_readiness_effect": "missing candles fail production readiness",
            "synthetic_candle_authority": "not_allowed",
        }
    return {
        "source": "manifest",
        "dense_candles_required": bool(raw.get("dense_candles_required", True)),
        "missing_candle_policy": str(raw.get("missing_candle_policy") or "fail"),
        "allow_classified_no_trade_missing": bool(raw.get("allow_classified_no_trade_missing", False)),
        "require_retry_attempts_for_missing_ranges": bool(raw.get("require_retry_attempts_for_missing_ranges", True)),
        "max_unclassified_missing_buckets": int(raw.get("max_unclassified_missing_buckets", 0) or 0),
        "readiness_gate_effect": (
            "metadata_only_no_gate_relaxation"
            if str(raw.get("missing_candle_policy") or "fail").strip().lower() == "diagnostic_only"
            else "strict_fail_closed"
        ),
        "production_readiness_effect": (
            "diagnostic_only does not satisfy or weaken production readiness"
            if str(raw.get("missing_candle_policy") or "fail").strip().lower() == "diagnostic_only"
            else "missing candles fail production readiness"
        ),
        "synthetic_candle_authority": "not_allowed",
    }


def readiness_mode_payload(manifest: ExperimentManifest) -> dict[str, Any]:
    production_bound = manifest.deployment_tier != "research_only"
    return {
        "readiness_type": "production_readiness" if production_bound else "research_only_diagnostic",
        "production_bound": production_bound,
        "candle_only_diagnostic": not production_bound and manifest.dataset.top_of_book is None,
        "production_gate_statement": (
            "production-bound readiness requires candle coverage, top_of_book if requested, "
            "execution calibration when required, and walk-forward prerequisites"
        ),
    }


def walk_forward_payload(manifest: ExperimentManifest) -> dict[str, Any]:
    required = bool(manifest.acceptance_gate.walk_forward_required)
    if manifest.walk_forward is None:
        return {
            "required": required,
            "available_windows": 0,
            "expected_min_windows": None,
            "status": "FAIL" if required else "NOT_REQUIRED",
            "reasons": ["walk_forward_missing"] if required else [],
            "next_action": "add walk_forward config and run research-walk-forward" if required else "none",
        }
    windows = _rolling_walk_forward_windows(manifest)
    expected = manifest.walk_forward.min_windows
    status = "PASS" if len(windows) >= expected else "FAIL"
    return {
        "required": required,
        "available_windows": len(windows),
        "expected_min_windows": expected,
        "status": status if required else "NOT_REQUIRED",
        "reasons": [] if status == "PASS" else ["walk_forward_insufficient_windows"],
        "next_action": "none" if status == "PASS" else "adjust manifest walk_forward dates only with reviewed research intent",
    }


def _scan_candles_sql(
    *,
    db_path: str | Path,
    market: str,
    interval: str,
    start_ts: int,
    end_ts: int,
    interval_ms: int,
    max_missing_ranges: int | None,
    max_missing_sample: int,
) -> dict[str, Any]:
    expected_count = _expected_bucket_count(start_ts=start_ts, end_ts=end_ts, interval_ms=interval_ms)
    present_expected = 0
    actual_count = 0
    unexpected_count = 0
    ohlc_violations = 0
    non_positive_prices = 0
    negative_volume = 0
    interval_mismatch = 0
    non_monotonic = 0
    first_ts: int | None = None
    last_ts: int | None = None
    previous_row_ts: int | None = None
    previous_distinct_expected_ts: int | None = None
    expected_cursor = start_ts
    missing_count = 0
    missing_ranges: list[dict[str, int]] = []
    missing_sample: list[int] = []
    active_start: int | None = None
    active_prev: int | None = None
    active_count = 0
    ranges_truncated = False

    def add_missing(ts: int) -> None:
        nonlocal missing_count, active_start, active_prev, active_count
        missing_count += 1
        if len(missing_sample) < max_missing_sample:
            missing_sample.append(ts)
        if active_start is None:
            active_start = ts
            active_count = 1
        else:
            active_count += 1
        active_prev = ts

    def close_missing_range() -> None:
        nonlocal active_start, active_prev, active_count, ranges_truncated
        if active_start is None:
            return
        if max_missing_ranges is None or len(missing_ranges) < max_missing_ranges:
            missing_ranges.append({"start_ts": active_start, "end_ts": active_prev or active_start, "bucket_count": active_count})
        else:
            ranges_truncated = True
        active_start = None
        active_prev = None
        active_count = 0

    resolved_db = Path(db_path).expanduser().resolve()
    if not resolved_db.exists():
        while expected_cursor <= end_ts:
            add_missing(expected_cursor)
            expected_cursor += interval_ms
        close_missing_range()
        return {
            "actual_candle_count": 0,
            "present_expected_bucket_count": 0,
            "coverage_pct": 0.0,
            "missing_bucket_count": missing_count,
            "missing_bucket_ranges": missing_ranges,
            "missing_bucket_sample": missing_sample,
            "missing_ranges_truncated": ranges_truncated,
            "duplicate_key_count": 0,
            "non_monotonic_ts_count": 0,
            "interval_mismatch_count": 0,
            "unexpected_bucket_count": 0,
            "ohlc_violation_count": 0,
            "non_positive_price_count": 0,
            "negative_volume_count": 0,
            "first_ts": None,
            "last_ts": None,
        }

    conn = sqlite3.connect(f"file:{resolved_db}?mode=ro", uri=True)
    try:
        duplicate_row = conn.execute(
            """
            SELECT COUNT(*) - COUNT(DISTINCT ts)
            FROM candles
            WHERE pair=? AND interval=? AND ts >= ? AND ts <= ?
            """,
            (market, interval, start_ts, end_ts),
        ).fetchone()
        duplicate_count = int(duplicate_row[0] or 0) if duplicate_row else 0
        rows = conn.execute(
            """
            SELECT ts, open, high, low, close, volume
            FROM candles
            WHERE pair=? AND interval=? AND ts >= ? AND ts <= ?
            ORDER BY ts ASC
            """,
            (market, interval, start_ts, end_ts),
        )
        seen_expected_ts: int | None = None
        for row in rows:
            ts = int(row[0])
            actual_count += 1
            first_ts = ts if first_ts is None else first_ts
            last_ts = ts
            if previous_row_ts is not None and ts < previous_row_ts:
                non_monotonic += 1
            previous_row_ts = ts
            open_price = float(row[1])
            high = float(row[2])
            low = float(row[3])
            close = float(row[4])
            volume = float(row[5] or 0.0)
            if not (low <= open_price <= high and low <= close <= high and low <= high):
                ohlc_violations += 1
            if open_price <= 0.0 or high <= 0.0 or low <= 0.0 or close <= 0.0:
                non_positive_prices += 1
            if volume < 0.0:
                negative_volume += 1
            if not _is_expected_bucket(ts, start_ts=start_ts, end_ts=end_ts, interval_ms=interval_ms):
                unexpected_count += 1
                continue
            while expected_cursor < ts:
                add_missing(expected_cursor)
                expected_cursor += interval_ms
            if seen_expected_ts == ts:
                continue
            close_missing_range()
            present_expected += 1
            if previous_distinct_expected_ts is not None and ts - previous_distinct_expected_ts != interval_ms:
                interval_mismatch += 1
            previous_distinct_expected_ts = ts
            seen_expected_ts = ts
            expected_cursor = max(expected_cursor, ts + interval_ms)
        while expected_cursor <= end_ts:
            add_missing(expected_cursor)
            expected_cursor += interval_ms
        close_missing_range()
    finally:
        conn.close()

    coverage_pct = round((present_expected / expected_count * 100.0), 8) if expected_count else 0.0
    return {
        "actual_candle_count": actual_count,
        "present_expected_bucket_count": present_expected,
        "coverage_pct": coverage_pct,
        "missing_bucket_count": missing_count,
        "missing_bucket_ranges": missing_ranges,
        "missing_bucket_sample": missing_sample,
        "missing_ranges_truncated": ranges_truncated,
        "duplicate_key_count": duplicate_count,
        "non_monotonic_ts_count": non_monotonic,
        "interval_mismatch_count": interval_mismatch,
        "unexpected_bucket_count": unexpected_count,
        "ohlc_violation_count": ohlc_violations,
        "non_positive_price_count": non_positive_prices,
        "negative_volume_count": negative_volume,
        "first_ts": first_ts,
        "last_ts": last_ts,
    }


def _top_of_book_split_sql(
    *,
    db_path: str | Path,
    manifest: ExperimentManifest,
    start_ts: int,
    end_ts: int,
    expected_signal_count: int,
) -> dict[str, Any]:
    spec = manifest.dataset.top_of_book
    if spec is None:
        return {}
    if not Path(db_path).expanduser().resolve().exists():
        return _top_of_book_fail_payload(spec=spec, expected=expected_signal_count, reason="top_of_book_db_missing")
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='orderbook_top_snapshots'"
        ).fetchone()
        if table is None:
            return _top_of_book_fail_payload(spec=spec, expected=expected_signal_count, reason="top_of_book_table_missing")
        params: list[object] = [manifest.market, start_ts - int(spec.join_tolerance_ms), end_ts + int(spec.join_tolerance_ms)]
        source_predicate = ""
        if spec.quote_source is not None:
            source_predicate = "AND source=?"
            params.append(spec.quote_source)
        quote_count = int(
            (
                conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM orderbook_top_snapshots
                    WHERE pair=? AND ts >= ? AND ts <= ? {source_predicate}
                    """,
                    tuple(params),
                ).fetchone()
                or (0,)
            )[0]
            or 0
        )
        if quote_count == 0:
            return _top_of_book_fail_payload(spec=spec, expected=expected_signal_count, reason="top_of_book_rows_missing")
        join_params: list[object] = [manifest.market, manifest.interval, start_ts, end_ts, manifest.market]
        source_clause = ""
        if spec.quote_source is not None:
            source_clause = "AND q.source=?"
        join_params.extend([int(spec.join_tolerance_ms), int(spec.join_tolerance_ms)])
        if spec.quote_source is not None:
            join_params.append(spec.quote_source)
        joined = int(
            (
                conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM candles c
                    WHERE c.pair=? AND c.interval=? AND c.ts >= ? AND c.ts <= ?
                      AND EXISTS (
                        SELECT 1
                        FROM orderbook_top_snapshots q
                        WHERE q.pair=?
                          AND q.ts >= c.ts - ?
                          AND q.ts <= c.ts + ?
                          {source_clause}
                        LIMIT 1
                      )
                    """,
                    tuple(join_params),
                ).fetchone()
                or (0,)
            )[0]
            or 0
        )
        sample_params = list(join_params)
        sample_rows = conn.execute(
            f"""
            SELECT c.ts
            FROM candles c
            WHERE c.pair=? AND c.interval=? AND c.ts >= ? AND c.ts <= ?
              AND NOT EXISTS (
                SELECT 1
                FROM orderbook_top_snapshots q
                WHERE q.pair=?
                  AND q.ts >= c.ts - ?
                  AND q.ts <= c.ts + ?
                  {source_clause}
                LIMIT 1
              )
            ORDER BY c.ts ASC
            LIMIT 20
            """,
            tuple(sample_params),
        ).fetchall()
    finally:
        conn.close()

    coverage_pct = round((joined / expected_signal_count * 100.0), 8) if expected_signal_count else 0.0
    reasons: list[str] = []
    if joined < expected_signal_count:
        reasons.append("top_of_book_missing")
    if coverage_pct < float(spec.min_coverage_pct):
        reasons.append("top_of_book_coverage_below_threshold")
    gate_status = "PASS"
    if reasons:
        gate_status = "FAIL" if spec.required or spec.missing_policy == "fail" else "WARN"
    return {
        "top_of_book_requested": True,
        "top_of_book_scan_method": "sqlite_exists_join",
        "top_of_book_required": bool(spec.required),
        "top_of_book_missing_policy": spec.missing_policy,
        "top_of_book_source": spec.source,
        "top_of_book_join_tolerance_ms": spec.join_tolerance_ms,
        "top_of_book_expected_signal_count": expected_signal_count,
        "top_of_book_available_row_count": quote_count,
        "top_of_book_joined_count": joined,
        "top_of_book_missing_count": expected_signal_count - joined,
        "top_of_book_missing_sample": [int(row[0]) for row in sample_rows],
        "top_of_book_coverage_pct": coverage_pct,
        "top_of_book_gate_status": gate_status,
        "top_of_book_gate_reasons": reasons,
    }


def _depth_summary_sql(
    *,
    db_path: str | Path,
    market: str,
    start_ts: int,
    end_ts: int,
) -> dict[str, Any]:
    if not Path(db_path).expanduser().resolve().exists():
        return {
            "l2_depth_table_exists": False,
            "l2_depth_rows_available": False,
            "l2_depth_complete_snapshots_available": False,
            "l2_depth_snapshot_count": 0,
            "l2_depth_row_count": 0,
            "l2_depth_first_ts": None,
            "l2_depth_last_ts": None,
            "l2_depth_sources": [],
            "l2_depth_content_hash": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "depth_snapshot_selection_policy": "first_snapshot_after_or_equal_reference_ts_with_max_wait",
            "depth_walk_execution_model_available": True,
            "depth_walk_execution_model_used": False,
            "full_orderbook_depth_available": False,
            "queue_position_available": False,
            "trade_ticks_available": False,
            "market_impact_model_available": False,
            "intra_candle_path_available": False,
        }
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        return summarize_orderbook_depth_evidence(
            conn,
            pair=market,
            start_ts=start_ts,
            end_ts=end_ts,
        )
    finally:
        conn.close()


def _top_of_book_fail_payload(*, spec: Any, expected: int, reason: str) -> dict[str, Any]:
    reasons = ["top_of_book_missing", reason, "top_of_book_coverage_below_threshold"]
    gate_status = "FAIL" if spec.required or spec.missing_policy == "fail" else "WARN"
    return {
        "top_of_book_requested": True,
        "top_of_book_scan_method": "sqlite_fast_absence_check",
        "top_of_book_required": bool(spec.required),
        "top_of_book_missing_policy": spec.missing_policy,
        "top_of_book_source": spec.source,
        "top_of_book_join_tolerance_ms": spec.join_tolerance_ms,
        "top_of_book_expected_signal_count": expected,
        "top_of_book_available_row_count": 0,
        "top_of_book_joined_count": 0,
        "top_of_book_missing_count": expected,
        "top_of_book_missing_sample": [],
        "top_of_book_coverage_pct": 0.0,
        "top_of_book_gate_status": gate_status,
        "top_of_book_gate_reasons": reasons,
    }


def _safe_db_schema_fingerprint(db_path: str | Path) -> str:
    if not Path(db_path).expanduser().resolve().exists():
        return sha256_prefixed({"db_schema": "missing_db", "table": "candles"})
    return _db_schema_fingerprint(db_path)


def _sqlite_present_tables(db_path: str | Path) -> list[str]:
    resolved = Path(db_path).expanduser().resolve()
    if not resolved.exists():
        return []
    conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table'
              AND name IN ('candles', 'orderbook_top_snapshots', 'orderbook_depth_levels')
            ORDER BY name ASC
            """
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]) for row in rows]


def _artifact_range(*, split_name: str, start_ts: int, end_ts: int, bucket_count: int) -> dict[str, Any]:
    return {
        "split": split_name,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "start_utc": _format_utc(start_ts),
        "end_utc": _format_utc(end_ts),
        "start_kst": _format_kst(start_ts),
        "end_kst": _format_kst(end_ts),
        "bucket_count": bucket_count,
        "retry_utc_days": _retry_utc_days(start_ts=start_ts, end_ts=end_ts),
        "classification": "untried_missing",
    }


def _format_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat()


def _format_kst(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).astimezone(KST).isoformat()


def _retry_utc_days(*, start_ts: int, end_ts: int) -> list[str]:
    start_day = datetime.fromtimestamp(start_ts / 1000, tz=UTC).date()
    end_day = datetime.fromtimestamp(end_ts / 1000, tz=UTC).date()
    days = []
    day = start_day
    while day <= end_day:
        days.append(day.isoformat())
        day += timedelta(days=1)
    return days


def _validate_missing_artifact(*, artifact: dict[str, Any], manifest: ExperimentManifest, db_path: Path) -> None:
    if artifact.get("artifact_type") != "missing_candle_ranges":
        raise ValueError("missing ranges artifact_type must be missing_candle_ranges")
    if artifact.get("schema_version") != 1:
        raise ValueError("unsupported missing ranges schema_version")
    embedded_hash = artifact.get("content_hash")
    if not isinstance(embedded_hash, str) or not embedded_hash.startswith("sha256:"):
        raise ValueError("missing ranges content_hash is required")
    recomputed_payload = {key: value for key, value in artifact.items() if key != "content_hash"}
    if sha256_prefixed(recomputed_payload) != embedded_hash:
        raise ValueError("missing ranges content_hash does not match artifact body")
    if artifact.get("manifest_hash") != manifest.manifest_hash():
        raise ValueError("missing ranges manifest_hash does not match manifest")
    if artifact.get("market") != manifest.market or artifact.get("interval") != manifest.interval:
        raise ValueError("missing ranges market/interval does not match manifest")
    artifact_db = Path(str(artifact.get("db_path") or "")).expanduser().resolve()
    if artifact_db != db_path:
        raise ValueError("missing ranges db_path does not match configured DB_PATH")
    for split_payload in (artifact.get("splits") or {}).values():
        for item in split_payload.get("ranges") or []:
            if item.get("classification") not in MISSING_CLASSIFICATIONS:
                raise ValueError("missing ranges artifact has unsupported classification")


def _validate_retry_attempts_artifact(
    *,
    artifact: dict[str, Any],
    manifest: ExperimentManifest,
    db_path: Path,
    missing_ranges_path: Path,
    missing_ranges_hash: str,
) -> None:
    if artifact.get("artifact_type") != "missing_candle_retry_attempts":
        raise ValueError("retry attempts artifact_type must be missing_candle_retry_attempts")
    if artifact.get("schema_version") != 1:
        raise ValueError("unsupported retry attempts schema_version")
    embedded_hash = artifact.get("content_hash")
    if not isinstance(embedded_hash, str) or not embedded_hash.startswith("sha256:"):
        raise ValueError("retry attempts content_hash is required")
    recomputed_payload = {key: value for key, value in artifact.items() if key != "content_hash"}
    if sha256_prefixed(recomputed_payload) != embedded_hash:
        raise ValueError("retry attempts content_hash does not match artifact body")
    if artifact.get("manifest_hash") != manifest.manifest_hash():
        raise ValueError("retry attempts manifest_hash does not match manifest")
    if artifact.get("market") != manifest.market or artifact.get("interval") != manifest.interval:
        raise ValueError("retry attempts market/interval does not match manifest")
    artifact_db = Path(str(artifact.get("db_path") or "")).expanduser().resolve()
    if artifact_db != db_path:
        raise ValueError("retry attempts db_path does not match configured DB_PATH")
    artifact_missing_path = Path(str(artifact.get("missing_ranges_path") or "")).expanduser().resolve()
    if artifact_missing_path != missing_ranges_path:
        raise ValueError("retry attempts missing_ranges_path does not match input")
    if not missing_ranges_hash or artifact.get("missing_ranges_hash") != missing_ranges_hash:
        raise ValueError("retry attempts missing_ranges_hash does not match missing ranges artifact")
    for item in artifact.get("attempts") or []:
        if item.get("classification") not in MISSING_CLASSIFICATIONS:
            raise ValueError("retry attempts artifact has unsupported classification")


def _validate_source_probe_artifact(
    *,
    artifact: dict[str, Any],
    manifest: ExperimentManifest,
    db_path: Path,
    missing_ranges_path: Path,
    missing_ranges_hash: str,
) -> None:
    if artifact.get("artifact_type") != "missing_candle_source_probe":
        raise ValueError("source probe artifact_type must be missing_candle_source_probe")
    if artifact.get("schema_version") != 1:
        raise ValueError("unsupported source probe schema_version")
    embedded_hash = artifact.get("content_hash")
    if not isinstance(embedded_hash, str) or not embedded_hash.startswith("sha256:"):
        raise ValueError("source probe content_hash is required")
    recomputed_payload = {key: value for key, value in artifact.items() if key != "content_hash"}
    if sha256_prefixed(recomputed_payload) != embedded_hash:
        raise ValueError("source probe content_hash does not match artifact body")
    if artifact.get("manifest_hash") != manifest.manifest_hash():
        raise ValueError("source probe manifest_hash does not match manifest")
    if artifact.get("market") != manifest.market or artifact.get("interval") != manifest.interval:
        raise ValueError("source probe market/interval does not match manifest")
    artifact_db = Path(str(artifact.get("db_path") or "")).expanduser().resolve()
    if artifact_db != db_path:
        raise ValueError("source probe db_path does not match configured DB_PATH")
    artifact_missing_path = Path(str(artifact.get("missing_ranges_path") or "")).expanduser().resolve()
    if artifact_missing_path != missing_ranges_path:
        raise ValueError("source probe missing_ranges_path does not match input")
    if artifact.get("missing_ranges_hash") != missing_ranges_hash:
        raise ValueError("source probe missing_ranges_hash does not match missing ranges artifact")


def _probe_targets_for_range(item: dict[str, Any], *, interval_ms: int) -> list[tuple[str, int]]:
    start_ts = int(item["start_ts"])
    end_ts = int(item["end_ts"])
    middle = start_ts + ((end_ts - start_ts) // (2 * interval_ms)) * interval_ms
    return [
        ("before_range_last_bucket", start_ts - interval_ms),
        ("range_start", start_ts),
        ("range_middle", middle),
        ("range_end", end_ts),
        ("after_range_first_bucket", end_ts + interval_ms),
    ]


def _probe_minute_candle_source(
    *,
    client: httpx.Client,
    market: str,
    minute_unit: int,
    count: int,
    target_ts: int,
    target_label: str,
) -> dict[str, Any]:
    endpoint = f"/v1/candles/minutes/{minute_unit}"
    probe_to_kst = _format_kst(target_ts + minute_unit * 60_000).replace("+09:00", "")
    params = {"market": market, "count": count, "to": probe_to_kst}
    http_status: int | None = None
    api_error_name: str | None = None
    api_error_message: str | None = None
    response_payload: Any = None
    returned_sample: list[str] = []
    returned_ts: list[int] = []
    target_present = False
    try:
        response = client.get(endpoint, params=params)
        http_status = int(response.status_code)
        response_payload = decode_json_response(response=response, endpoint=endpoint, params=params)
        api_error = extract_api_error(response_payload)
        if api_error is not None:
            api_error_name, api_error_message = api_error
        if 200 <= http_status < 300:
            candles = parse_minute_candles(response_payload)
            for candle in candles:
                ts = _minute_candle_utc_ts(candle.candle_date_time_utc)
                returned_ts.append(ts)
                returned_sample.append(candle.candle_date_time_kst)
            target_present = target_ts in returned_ts
    except Exception as exc:
        api_error_name = exc.__class__.__name__
        api_error_message = str(exc)
        response_payload = {"error": api_error_message}
    return {
        "target_label": target_label,
        "target_ts": target_ts,
        "target_utc": _format_utc(target_ts),
        "target_kst": _format_kst(target_ts),
        "probe_to_kst": probe_to_kst,
        "market": market,
        "minute_unit": minute_unit,
        "count": count,
        "http_status": http_status,
        "api_error_name": api_error_name,
        "api_error_message": api_error_message,
        "response_hash": sha256_prefixed(response_payload),
        "target_present": target_present,
        "returned_newest_kst": max(returned_sample) if returned_sample else None,
        "returned_oldest_kst": min(returned_sample) if returned_sample else None,
        "returned_kst_sample": returned_sample[:10],
    }


def _minute_candle_utc_ts(value: str) -> int:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.astimezone(UTC).timestamp() * 1000)


def _backfill_exception_attempt_payload(
    *,
    attempt_index: int,
    retry_utc_day: str,
    exc: Exception,
) -> dict[str, Any]:
    return {
        "attempt_index": int(attempt_index),
        "retry_utc_day": retry_utc_day,
        "progress_status": "ERROR",
        "progress_reason": "backfill_exception",
        "error_class": exc.__class__.__name__,
        "error_message": str(exc),
        "api_unavailable_evidence": _is_api_unavailable_exception(exc),
        "coverage": None,
    }


def _is_api_unavailable_exception(exc: Exception) -> bool:
    return _has_api_unavailable_evidence(
        {
            "error_class": exc.__class__.__name__,
            "error_message": str(exc),
        }
    )


def _validate_report_artifact_out_path(path: str | Path) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        raise ValueError(f"research report artifact --out must be an absolute path: {path!r}")
    resolved = resolved.resolve()
    if PathManager._is_within(resolved, PROJECT_ROOT.resolve()):
        raise ValueError(f"research report artifact --out must be outside repository: {resolved}")
    return resolved


def _select_missing_ranges(
    *,
    artifact: dict[str, Any],
    min_buckets: int,
    split: str | None,
    limit: int | None,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for split_name, split_payload in sorted((artifact.get("splits") or {}).items()):
        if split is not None and split_name != split:
            continue
        for item in split_payload.get("ranges") or []:
            if int(item.get("bucket_count") or 0) < min_buckets:
                continue
            selected.append(dict(item))
            if limit is not None and len(selected) >= limit:
                return selected
    return selected


def _classify_persistent_missing_attempt(
    *,
    attempt: dict[str, Any],
    retry_artifact_hash: str,
    source_probe_artifact: dict[str, Any] | None,
    db_path: Path,
    market: str,
    interval: str,
) -> dict[str, Any]:
    api_unavailable = _has_api_unavailable_evidence(attempt)
    no_trade_supported = _has_no_trade_evidence(attempt)
    surrounding_present = _surrounding_candles_present(
        db_path=db_path,
        market=market,
        interval=interval,
        start_ts=int(attempt["start_ts"]),
        end_ts=int(attempt["end_ts"]),
    )
    normal_response = _has_normal_backfill_response(attempt)
    probe_evidence = _source_probe_evidence_for_attempt(attempt=attempt, source_probe_artifact=source_probe_artifact)
    probe_target_absent = bool(probe_evidence and probe_evidence.get("target_present") is False)
    probe_target_present = bool(probe_evidence and probe_evidence.get("target_present") is True)
    probe_api_unavailable = bool(probe_evidence and _has_api_unavailable_evidence(probe_evidence))

    if api_unavailable or probe_api_unavailable:
        classification = "api_unavailable_candidate"
    elif no_trade_supported:
        classification = "no_trade_missing_candidate"
    elif normal_response and surrounding_present and probe_target_absent:
        classification = "exchange_gap_candidate"
    elif probe_target_present:
        classification = "unclassified_missing"
    else:
        classification = "unclassified_missing"

    evidence = _classification_evidence(
        attempt=attempt,
        retry_artifact_hash=retry_artifact_hash,
        surrounding_present=surrounding_present,
        source_probe_evidence=probe_evidence,
    )
    return {
        "split": attempt["split"],
        "start_ts": int(attempt["start_ts"]),
        "end_ts": int(attempt["end_ts"]),
        "start_utc": attempt["start_utc"],
        "end_utc": attempt["end_utc"],
        "start_kst": attempt["start_kst"],
        "end_kst": attempt["end_kst"],
        "bucket_count": int(attempt["bucket_count"]),
        "classification": classification,
        "confidence": "candidate",
        "gate_effect": "none",
        "hypotheses": _classification_hypotheses(
            classification=classification,
            api_unavailable=api_unavailable,
            no_trade_supported=no_trade_supported,
            surrounding_present=surrounding_present,
            normal_response=normal_response,
        ),
        "evidence": evidence,
        "next_action": _persistent_missing_next_action(classification),
    }


def _classification_evidence(
    *,
    attempt: dict[str, Any],
    retry_artifact_hash: str,
    surrounding_present: bool,
    source_probe_evidence: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    before = attempt.get("before") if isinstance(attempt.get("before"), dict) else {}
    after = attempt.get("after") if isinstance(attempt.get("after"), dict) else {}
    backfill_attempts = [
        item for item in attempt.get("backfill_attempts") or [] if isinstance(item, dict)
    ]
    evidence: list[dict[str, Any]] = [
        {
            "type": "retry_attempt_summary",
            "artifact_hash": retry_artifact_hash,
            "before_missing_buckets": int(before.get("missing_buckets") or 0),
            "after_missing_buckets": int(after.get("missing_buckets") or 0),
            "recovered_buckets": int(attempt.get("recovered_buckets") or 0),
            "backfill_progress_statuses": sorted(
                {str(item.get("progress_status")) for item in backfill_attempts if item.get("progress_status") is not None}
            ),
            "backfill_progress_reasons": sorted(
                {str(item.get("progress_reason")) for item in backfill_attempts if item.get("progress_reason") is not None}
            ),
        },
        {
            "type": "db_surrounding_bucket_check",
            "surrounding_buckets_present": surrounding_present,
        },
    ]
    if source_probe_evidence is not None:
        evidence.append({"type": "source_probe", **source_probe_evidence})
    optional_evidence = attempt.get("probe_evidence")
    if isinstance(optional_evidence, dict):
        evidence.append({"type": "optional_probe_evidence", **optional_evidence})
    if _has_api_unavailable_evidence(attempt):
        evidence.append(
            {
                "type": "api_unavailable_signal",
                "evidence_refs": _api_unavailable_evidence_refs(attempt),
            }
        )
    if _has_no_trade_evidence(attempt):
        evidence.append(
            {
                "type": "no_trade_signal",
                "evidence_refs": _no_trade_evidence_refs(attempt),
            }
        )
    return evidence


def _source_probe_evidence_for_attempt(
    *,
    attempt: dict[str, Any],
    source_probe_artifact: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if source_probe_artifact is None:
        return None
    start_ts = int(attempt["start_ts"])
    end_ts = int(attempt["end_ts"])
    records = [
        item
        for item in source_probe_artifact.get("probe_records") or []
        if isinstance(item, dict)
        and str(item.get("target_label")) in {"range_start", "range_middle", "range_end"}
        and start_ts <= int(item.get("target_ts") or -1) <= end_ts
    ]
    if not records:
        return None
    present_records = [item for item in records if item.get("target_present") is True]
    absent_records = [item for item in records if item.get("target_present") is False]
    api_error_records = [item for item in records if item.get("http_status") is None or int(item.get("http_status") or 0) >= 400]
    if present_records:
        target_present: bool | None = True
        selected = present_records[0]
    elif api_error_records:
        target_present = None
        selected = api_error_records[0]
    elif absent_records:
        target_present = False
        selected = absent_records[0]
    else:
        target_present = None
        selected = records[0]
    return {
        "artifact_hash": source_probe_artifact.get("content_hash"),
        "target_present": target_present,
        "target_label": selected.get("target_label"),
        "target_ts": selected.get("target_ts"),
        "http_status": selected.get("http_status"),
        "api_error_name": selected.get("api_error_name"),
        "api_error_message": selected.get("api_error_message"),
        "response_hash": selected.get("response_hash"),
    }


def _classification_hypotheses(
    *,
    classification: str,
    api_unavailable: bool,
    no_trade_supported: bool,
    surrounding_present: bool,
    normal_response: bool,
) -> list[dict[str, Any]]:
    return [
        {
            "name": "cursor_timezone_or_pagination_stall",
            "status": "unknown",
            "evidence_refs": ["retry_attempt_summary"],
        },
        {
            "name": "exchange_gap_or_no_trade_interval",
            "status": "supported" if classification in {"exchange_gap_candidate", "no_trade_missing_candidate"} else "unknown",
            "evidence_refs": ["retry_attempt_summary", "db_surrounding_bucket_check"],
        },
        {
            "name": "api_unavailable_or_rate_limited",
            "status": "supported" if api_unavailable else ("weakened" if normal_response else "unknown"),
            "evidence_refs": ["api_unavailable_signal"] if api_unavailable else ["retry_attempt_summary"],
        },
        {
            "name": "no_trade_candle_omission",
            "status": "supported" if no_trade_supported else "unknown",
            "evidence_refs": ["no_trade_signal"] if no_trade_supported else [],
        },
        {
            "name": "db_env_or_writer_mismatch",
            "status": "weakened" if surrounding_present else "unknown",
            "evidence_refs": ["manifest_hash", "db_schema_fingerprint", "missing_ranges_hash", "retry_attempts_hash"],
        },
    ]


def _persistent_missing_next_action(classification: str) -> str:
    if classification == "api_unavailable_candidate":
        return "retry bounded probes/backfill after API stability is confirmed; production readiness remains fail-closed"
    if classification == "unclassified_missing":
        return "collect additional retry/probe/exchange evidence before production-bound research"
    return "review candidate evidence; this classification does not relax production readiness without a reviewed exception policy"


def persistent_missing_overall_next_action(summary: dict[str, Any]) -> str:
    if int(summary.get("api_unavailable_candidate") or 0):
        return "retry bounded probes/backfill after API stability is confirmed; resolve api_unavailable persistent missing ranges before production research"
    if int(summary.get("unclassified_missing") or 0):
        return "collect additional evidence and resolve unclassified persistent missing ranges before production research"
    if int(summary.get("persistent_range_count") or 0):
        return "review classified candidate evidence; production readiness remains fail-closed while missing candles remain unresolved"
    return "none"


def _has_normal_backfill_response(attempt: dict[str, Any]) -> bool:
    attempts = [item for item in attempt.get("backfill_attempts") or [] if isinstance(item, dict)]
    if not attempts:
        return False
    statuses = {str(item.get("progress_status") or "").upper() for item in attempts}
    return bool(statuses) and statuses <= {"COMPLETE"}


def _has_api_unavailable_evidence(payload: Any) -> bool:
    for key, value in _walk_key_values(payload):
        lowered_key = key.lower()
        lowered_value = str(value).lower()
        if lowered_key in {"http_status", "status_code"}:
            try:
                if int(value) in RETRYABLE_HTTP_STATUS_CODES:
                    return True
            except (TypeError, ValueError):
                pass
        if any(
            token in lowered_value
            for token in (
                "publicapitransienterror",
                "public api transient",
                "api transient",
                "retryable http status exhausted",
                "retryable status",
                "retry exhausted",
                "retry_exhausted",
                "rate_limited",
                "rate limit",
                "api_unavailable",
                "api unavailable",
                "request failed",
                "http 429",
                "status=429",
                "status=500",
                "status=502",
                "status=503",
                "status=504",
            )
        ):
            return True
    return False


def _api_unavailable_evidence_refs(payload: Any) -> list[str]:
    refs: list[str] = []
    for key, value in _walk_key_values(payload):
        text = f"{key}={value}"
        if _has_api_unavailable_evidence({key: value}):
            refs.append(text)
    return refs[:20]


def _has_no_trade_evidence(payload: Any) -> bool:
    for key, value in _walk_key_values(payload):
        lowered_key = key.lower()
        lowered_value = str(value).lower()
        if lowered_key in {"exchange_contract", "source_signal", "probe_interpretation", "reason"} and any(
            token in lowered_value
            for token in ("no_trade_candle_omission", "zero_volume_no_trade", "no_trade_interval")
        ):
            return True
    return False


def _no_trade_evidence_refs(payload: Any) -> list[str]:
    refs: list[str] = []
    for key, value in _walk_key_values(payload):
        if _has_no_trade_evidence({key: value}):
            refs.append(f"{key}={value}")
    return refs[:20]


def _walk_key_values(payload: Any) -> list[tuple[str, Any]]:
    values: list[tuple[str, Any]] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                values.extend(_walk_key_values(value))
            else:
                values.append((str(key), value))
    elif isinstance(payload, list):
        for item in payload:
            values.extend(_walk_key_values(item))
    return values


def _surrounding_candles_present(
    *,
    db_path: Path,
    market: str,
    interval: str,
    start_ts: int,
    end_ts: int,
) -> bool:
    resolved_db = db_path.expanduser().resolve()
    if not resolved_db.exists():
        return False
    interval_ms = _interval_ms(interval)
    before_ts = start_ts - interval_ms
    after_ts = end_ts + interval_ms
    conn = sqlite3.connect(f"file:{resolved_db}?mode=ro", uri=True)
    try:
        before = conn.execute(
            "SELECT 1 FROM candles WHERE pair=? AND interval=? AND ts=? LIMIT 1",
            (market, interval, before_ts),
        ).fetchone()
        after = conn.execute(
            "SELECT 1 FROM candles WHERE pair=? AND interval=? AND ts=? LIMIT 1",
            (market, interval, after_ts),
        ).fetchone()
    finally:
        conn.close()
    return before is not None and after is not None



def _range_coverage(
    *,
    db_path: str | Path,
    market: str,
    interval: str,
    start_ts: int,
    end_ts: int,
) -> RangeCoverage:
    interval_ms = _interval_ms(interval)
    stats = _scan_candles_sql(
        db_path=db_path,
        market=market,
        interval=interval,
        start_ts=start_ts,
        end_ts=end_ts,
        interval_ms=interval_ms,
        max_missing_ranges=0,
        max_missing_sample=0,
    )
    expected = _expected_bucket_count(start_ts=start_ts, end_ts=end_ts, interval_ms=interval_ms)
    return RangeCoverage(
        expected_buckets=expected,
        present_buckets=int(stats["present_expected_bucket_count"]),
        missing_buckets=int(stats["missing_bucket_count"]),
        coverage_pct=float(stats["coverage_pct"]),
    )


def env_payload() -> dict[str, object]:
    return get_last_explicit_env_load_summary().as_dict()
