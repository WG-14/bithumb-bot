from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from bisect import bisect_left, bisect_right
from pathlib import Path
from typing import Any

from bithumb_bot.public_api_minute_candles import interval_to_minute_unit
from bithumb_bot.orderbook_depth_store import summarize_orderbook_depth_evidence
from bithumb_bot.orderbook_depth_store import build_orderbook_depth_snapshot, OrderbookDepthSnapshot

from .datasets.contracts import DatasetLoadContext
from .datasets.registry import default_dataset_adapter_registry
from .experiment_manifest import DateRange, ExperimentManifest, ManifestValidationError
from .hashing import sha256_prefixed


@dataclass(frozen=True)
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    def as_tuple(self) -> tuple[int, float, float, float, float, float]:
        return (self.ts, self.open, self.high, self.low, self.close, self.volume)


@dataclass(frozen=True)
class TopOfBookQuote:
    ts: int
    pair: str
    bid_price: float
    ask_price: float
    spread_bps: float
    source: str
    observed_at_epoch_sec: float | None = None
    matched_candle_ts: int | None = None
    age_ms: int | None = None

    def as_tuple(self) -> tuple[int, str, float, float, float, str, float | None, int | None, int | None]:
        return (
            self.ts,
            self.pair,
            self.bid_price,
            self.ask_price,
            self.spread_bps,
            self.source,
            self.observed_at_epoch_sec,
            self.matched_candle_ts,
            self.age_ms,
        )

    def execution_payload(self) -> dict[str, object]:
        return {
            "best_bid": self.bid_price,
            "best_ask": self.ask_price,
            "spread_bps": self.spread_bps,
            "top_of_book_ts": self.ts,
            "top_of_book_source": self.source,
            "top_of_book_age_ms": self.age_ms,
        }


@dataclass(frozen=True)
class DatasetSnapshot:
    snapshot_id: str
    source: str
    market: str
    interval: str
    split_name: str
    date_range: DateRange
    candles: tuple[Candle, ...]
    source_uri: str | None = None
    source_content_hash: str | None = None
    source_schema_hash: str | None = None
    locator: dict[str, Any] | None = None
    options: dict[str, Any] | None = None
    adapter_provenance: dict[str, Any] | None = None
    top_of_book_quotes: tuple[TopOfBookQuote | None, ...] = ()
    top_of_book_event_quotes: tuple[TopOfBookQuote, ...] = ()
    top_of_book_requested: bool = False
    top_of_book_required: bool = False
    top_of_book_missing_policy: str | None = None
    top_of_book_source: str | None = None
    top_of_book_join_tolerance_ms: int | None = None
    top_of_book_min_coverage_pct: float = 100.0
    top_of_book_source_content_hash: str | None = None
    top_of_book_source_schema_hash: str | None = None
    top_of_book_adapter_provenance: dict[str, Any] | None = None
    orderbook_depth_snapshots: tuple[OrderbookDepthSnapshot, ...] = ()
    orderbook_depth_requested: bool = False
    orderbook_depth_required: bool = False
    orderbook_depth_source: str | None = None
    orderbook_depth_source_content_hash: str | None = None
    orderbook_depth_source_schema_hash: str | None = None
    orderbook_depth_adapter_provenance: dict[str, Any] | None = None

    def fingerprint_payload(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "market": self.market,
            "interval": self.interval,
            "split_name": self.split_name,
            "date_range": self.date_range.as_dict(),
            "candles": [candle.as_tuple() for candle in self.candles],
            "top_of_book": [
                quote.as_tuple() if quote is not None else None
                for quote in self.top_of_book_quotes
            ],
            "top_of_book_event_quotes": [quote.as_tuple() for quote in self.top_of_book_event_quotes],
            "top_of_book_config": {
                "requested": self.top_of_book_requested,
                "required": self.top_of_book_required,
                "missing_policy": self.top_of_book_missing_policy,
                "source": self.top_of_book_source,
                "join_tolerance_ms": self.top_of_book_join_tolerance_ms,
                "min_coverage_pct": self.top_of_book_min_coverage_pct,
            },
            "orderbook_depth_snapshots": [_depth_snapshot_payload(snapshot) for snapshot in self.orderbook_depth_snapshots],
        }

    def content_hash(self) -> str:
        cached = getattr(self, "_content_hash_cache", None)
        if cached is not None:
            return str(cached)
        value = sha256_prefixed(self.fingerprint_payload())
        object.__setattr__(self, "_content_hash_cache", value)
        return value

    def top_of_book_for_ts(self, ts: int) -> TopOfBookQuote | None:
        if not self.top_of_book_quotes:
            return None
        lookup = getattr(self, "_top_of_book_by_candle_ts", None)
        if lookup is None:
            lookup = {int(candle.ts): quote for candle, quote in zip(self.candles, self.top_of_book_quotes)}
            object.__setattr__(self, "_top_of_book_by_candle_ts", lookup)
        return lookup.get(int(ts))

    def execution_top_of_book_quotes(self) -> tuple[TopOfBookQuote, ...]:
        if self.top_of_book_event_quotes:
            return self.top_of_book_event_quotes
        return tuple(quote for quote in self.top_of_book_quotes if quote is not None)

    def sorted_execution_top_of_book_quotes(self) -> tuple[TopOfBookQuote, ...]:
        cached = getattr(self, "_sorted_execution_top_of_book_quotes", None)
        if cached is not None:
            return cached
        quotes = self.execution_top_of_book_quotes()
        if all(
            (int(prev.ts), str(prev.source)) <= (int(curr.ts), str(curr.source))
            for prev, curr in zip(quotes, quotes[1:])
        ):
            sorted_quotes = quotes
        else:
            sorted_quotes = tuple(sorted(quotes, key=lambda quote: (int(quote.ts), str(quote.source))))
        object.__setattr__(self, "_sorted_execution_top_of_book_quotes", sorted_quotes)
        object.__setattr__(self, "_sorted_execution_top_of_book_timestamps", tuple(int(quote.ts) for quote in sorted_quotes))
        return sorted_quotes

    def first_quote_after_or_equal(self, *, target_ts: int, max_wait_ms: int) -> TopOfBookQuote | None:
        quotes = self.sorted_execution_top_of_book_quotes()
        timestamps = getattr(self, "_sorted_execution_top_of_book_timestamps", None)
        if timestamps is None:
            timestamps = tuple(int(quote.ts) for quote in quotes)
            object.__setattr__(self, "_sorted_execution_top_of_book_timestamps", timestamps)
        max_ts = int(target_ts) + int(max_wait_ms)
        index = bisect_left(timestamps, int(target_ts))
        if index < len(quotes) and int(quotes[index].ts) <= max_ts:
            return quotes[index]
        return None

    def first_depth_snapshot_after_or_equal(
        self,
        *,
        target_ts: int,
        max_wait_ms: int,
    ) -> OrderbookDepthSnapshot | None:
        snapshots = self.sorted_orderbook_depth_snapshots()
        timestamps = getattr(self, "_sorted_orderbook_depth_timestamps", None)
        if timestamps is None:
            timestamps = tuple(int(snapshot.ts) for snapshot in snapshots)
            object.__setattr__(self, "_sorted_orderbook_depth_timestamps", timestamps)
        max_ts = int(target_ts) + int(max_wait_ms)
        index = bisect_left(timestamps, int(target_ts))
        if index < len(snapshots) and int(snapshots[index].ts) <= max_ts:
            return snapshots[index]
        return None

    def sorted_orderbook_depth_snapshots(self) -> tuple[OrderbookDepthSnapshot, ...]:
        cached = getattr(self, "_sorted_orderbook_depth_snapshots", None)
        if cached is not None:
            return cached
        snapshots = self.orderbook_depth_snapshots
        if all(
            (int(prev.ts), str(prev.source)) <= (int(curr.ts), str(curr.source))
            for prev, curr in zip(snapshots, snapshots[1:])
        ):
            sorted_snapshots = snapshots
        else:
            sorted_snapshots = tuple(sorted(snapshots, key=lambda snapshot: (int(snapshot.ts), str(snapshot.source))))
        object.__setattr__(self, "_sorted_orderbook_depth_snapshots", sorted_snapshots)
        object.__setattr__(self, "_sorted_orderbook_depth_timestamps", tuple(int(snapshot.ts) for snapshot in sorted_snapshots))
        return sorted_snapshots


@dataclass(frozen=True)
class DatasetQualityReport:
    payload: dict[str, Any]

    @property
    def content_hash(self) -> str:
        return str(self.payload["content_hash"])

    @property
    def quality_gate_status(self) -> str:
        return str(self.payload["quality_gate_status"])

    @property
    def quality_gate_reasons(self) -> tuple[str, ...]:
        return tuple(str(reason) for reason in self.payload.get("quality_gate_reasons", ()))


def load_dataset_split(
    *,
    db_path: str | Path,
    manifest: ExperimentManifest,
    split_name: str,
) -> DatasetSnapshot:
    date_range = _split_range(manifest, split_name)
    return load_dataset_range(db_path=db_path, manifest=manifest, split_name=split_name, date_range=date_range)


def load_dataset_range(
    *,
    db_path: str | Path,
    manifest: ExperimentManifest,
    split_name: str,
    date_range: DateRange,
) -> DatasetSnapshot:
    registry = default_dataset_adapter_registry()
    adapter = registry.resolve(manifest.dataset.source)
    top_of_book_spec = manifest.dataset.top_of_book
    top_of_book_adapter = None
    if top_of_book_spec is not None:
        top_of_book_adapter = registry.resolve_top_of_book(top_of_book_spec.source)
    depth_requested = _depth_requested(manifest)
    depth_spec = manifest.dataset.depth
    depth_source = depth_spec.source if depth_spec is not None else "orderbook_depth_levels"
    depth_adapter = registry.resolve_depth(depth_source) if depth_requested else None
    snapshot = adapter.load_range(
        manifest=manifest,
        split_name=split_name,
        date_range=date_range,
        context=DatasetLoadContext(db_path=db_path),
    )
    execution_lookahead_ms = (
        int(manifest.execution_timing.decision_guard_ms)
        + int(max((scenario.latency_ms for scenario in manifest.execution_model.scenarios), default=0))
        + int(manifest.execution_timing.max_quote_wait_ms)
    )
    top_of_book_quotes: tuple[TopOfBookQuote | None, ...] = ()
    top_of_book_event_quotes: tuple[TopOfBookQuote, ...] = ()
    if top_of_book_adapter is not None:
        top_of_book_quotes = tuple(
            top_of_book_adapter.load_candle_quotes(
                manifest=manifest,
                candles=snapshot.candles,
                context=DatasetLoadContext(db_path=db_path),
            )
        )
        top_of_book_event_quotes = tuple(
            top_of_book_adapter.load_event_quotes(
                manifest=manifest,
                candles=snapshot.candles,
                execution_quote_lookahead_ms=execution_lookahead_ms,
                context=DatasetLoadContext(db_path=db_path),
            )
        )
    orderbook_depth_snapshots: tuple[OrderbookDepthSnapshot, ...] = ()
    if depth_adapter is not None:
        orderbook_depth_snapshots = tuple(
            depth_adapter.load_event_snapshots(
                manifest=manifest,
                candles=snapshot.candles,
                execution_depth_lookahead_ms=execution_lookahead_ms,
                context=DatasetLoadContext(db_path=db_path),
            )
        )
    top_of_book_provenance = (
        top_of_book_adapter.provenance(manifest=manifest, context=DatasetLoadContext(db_path=db_path))
        if top_of_book_adapter is not None
        else None
    )
    depth_provenance = (
        depth_adapter.provenance(manifest=manifest, context=DatasetLoadContext(db_path=db_path))
        if depth_adapter is not None
        else None
    )
    return DatasetSnapshot(
        snapshot_id=snapshot.snapshot_id,
        source=snapshot.source,
        market=snapshot.market,
        interval=snapshot.interval,
        split_name=snapshot.split_name,
        date_range=snapshot.date_range,
        candles=snapshot.candles,
        source_uri=snapshot.source_uri,
        source_content_hash=snapshot.source_content_hash,
        source_schema_hash=snapshot.source_schema_hash,
        locator=snapshot.locator,
        options=snapshot.options,
        adapter_provenance=snapshot.adapter_provenance,
        top_of_book_quotes=top_of_book_quotes,
        top_of_book_event_quotes=top_of_book_event_quotes,
        top_of_book_requested=top_of_book_spec is not None,
        top_of_book_required=bool(top_of_book_spec.required) if top_of_book_spec is not None else False,
        top_of_book_missing_policy=top_of_book_spec.missing_policy if top_of_book_spec is not None else None,
        top_of_book_source=top_of_book_spec.source if top_of_book_spec is not None else None,
        top_of_book_join_tolerance_ms=top_of_book_spec.join_tolerance_ms if top_of_book_spec is not None else None,
        top_of_book_min_coverage_pct=top_of_book_spec.min_coverage_pct if top_of_book_spec is not None else 100.0,
        top_of_book_source_content_hash=top_of_book_spec.source_content_hash if top_of_book_spec is not None else None,
        top_of_book_source_schema_hash=top_of_book_spec.source_schema_hash if top_of_book_spec is not None else None,
        top_of_book_adapter_provenance=top_of_book_provenance,
        orderbook_depth_snapshots=orderbook_depth_snapshots,
        orderbook_depth_requested=depth_requested,
        orderbook_depth_required=bool(getattr(depth_spec, "required", False)) or bool(manifest.execution_timing.depth_required),
        orderbook_depth_source=depth_source if depth_requested else None,
        orderbook_depth_source_content_hash=depth_spec.source_content_hash if depth_spec is not None else None,
        orderbook_depth_source_schema_hash=depth_spec.source_schema_hash if depth_spec is not None else None,
        orderbook_depth_adapter_provenance=depth_provenance,
    )


def _depth_requested(manifest: ExperimentManifest) -> bool:
    return (
        manifest.dataset.depth is not None
        or bool(manifest.execution_timing.depth_required)
        or manifest.execution_timing.min_execution_reality_level_for_promotion == "l2_depth_walk_no_queue"
        or any(scenario.type == "depth_walk" for scenario in manifest.execution_model.scenarios)
    )


def _load_sqlite_dataset_range(
    *,
    db_path: str | Path,
    manifest: ExperimentManifest,
    split_name: str,
    date_range: DateRange,
) -> DatasetSnapshot:
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT ts, open, high, low, close, volume
            FROM candles
            WHERE pair=? AND interval=? AND ts >= ? AND ts <= ?
            ORDER BY ts ASC
            """,
            (
                manifest.market,
                manifest.interval,
                date_range.start_ts_ms(),
                date_range.end_ts_ms(),
            ),
        ).fetchall()
    finally:
        conn.close()
    candles = tuple(
        Candle(
            ts=int(row[0]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5] or 0.0),
        )
        for row in rows
    )
    top_of_book_spec = manifest.dataset.top_of_book
    return DatasetSnapshot(
        snapshot_id=manifest.dataset.snapshot_id,
        source=manifest.dataset.source,
        market=manifest.market,
        interval=manifest.interval,
        split_name=split_name,
        date_range=date_range,
        candles=candles,
        source_uri=manifest.dataset.source_uri,
        source_content_hash=manifest.dataset.source_content_hash,
        source_schema_hash=manifest.dataset.source_schema_hash,
        locator=manifest.dataset.locator,
        options=manifest.dataset.options,
        top_of_book_requested=top_of_book_spec is not None,
        top_of_book_required=bool(top_of_book_spec.required) if top_of_book_spec is not None else False,
        top_of_book_missing_policy=top_of_book_spec.missing_policy if top_of_book_spec is not None else None,
        top_of_book_source=top_of_book_spec.source if top_of_book_spec is not None else None,
        top_of_book_join_tolerance_ms=top_of_book_spec.join_tolerance_ms if top_of_book_spec is not None else None,
        top_of_book_min_coverage_pct=top_of_book_spec.min_coverage_pct if top_of_book_spec is not None else 100.0,
    )


def build_dataset_quality_report(
    *,
    db_path: str | Path,
    snapshot: DatasetSnapshot,
) -> DatasetQualityReport:
    adapter = default_dataset_adapter_registry().resolve(snapshot.source)
    return adapter.quality_report(snapshot=snapshot, context=DatasetLoadContext(db_path=db_path))


def _build_source_agnostic_dataset_quality_report(
    *,
    db_path: str | Path | None,
    snapshot: DatasetSnapshot,
    adapter_name: str = "source_agnostic",
    adapter_version: str = "1",
    adapter_provenance: dict[str, Any] | None = None,
) -> DatasetQualityReport:
    interval_ms = _interval_ms(snapshot.interval)
    start_ts = snapshot.date_range.start_ts_ms()
    end_ts = snapshot.date_range.end_ts_ms()
    expected_count = _expected_bucket_count(start_ts=start_ts, end_ts=end_ts, interval_ms=interval_ms)
    candles = snapshot.candles
    actual_ts = [candle.ts for candle in candles]
    actual_expected_ts = {
        ts
        for ts in actual_ts
        if _is_expected_bucket(ts, start_ts=start_ts, end_ts=end_ts, interval_ms=interval_ms)
    }
    missing_count, missing_ranges, missing_sample = _scan_missing_buckets(
        start_ts=start_ts,
        end_ts=end_ts,
        interval_ms=interval_ms,
        present_expected_ts=actual_expected_ts,
    )
    duplicate_key_count = _duplicate_key_count_from_snapshot(snapshot=snapshot)
    non_monotonic = sum(1 for prev, curr in zip(actual_ts, actual_ts[1:]) if curr <= prev)
    interval_mismatch = sum(
        1
        for prev, curr in zip(actual_ts, actual_ts[1:])
        if curr > prev and (curr - prev) != interval_ms
    )
    ohlc_violations = 0
    non_positive_prices = 0
    negative_volume = 0
    for candle in candles:
        if not (
            candle.low <= candle.open <= candle.high
            and candle.low <= candle.close <= candle.high
            and candle.low <= candle.high
        ):
            ohlc_violations += 1
        if candle.open <= 0.0 or candle.high <= 0.0 or candle.low <= 0.0 or candle.close <= 0.0:
            non_positive_prices += 1
        if candle.volume < 0.0:
            negative_volume += 1

    reasons: list[str] = []
    if missing_count:
        reasons.append("missing_candles")
    if duplicate_key_count:
        reasons.append("duplicate_candle_keys")
    if non_monotonic:
        reasons.append("non_monotonic_timestamps")
    if interval_mismatch:
        reasons.append("interval_mismatch")
    if ohlc_violations:
        reasons.append("ohlc_invariant_violation")
    if non_positive_prices:
        reasons.append("non_positive_price")
    if negative_volume:
        reasons.append("negative_volume")
    if actual_ts and (min(actual_ts) < start_ts or max(actual_ts) > end_ts):
        reasons.append("timestamp_outside_split_range")
    unexpected_count = sum(
        1
        for ts in actual_ts
        if not _is_expected_bucket(ts, start_ts=start_ts, end_ts=end_ts, interval_ms=interval_ms)
    )
    if unexpected_count:
        reasons.append("unexpected_candle_bucket")

    actual_count = len(candles)
    present_expected_count = len(actual_expected_ts)
    coverage_pct = (present_expected_count / expected_count * 100.0) if expected_count else 0.0
    if (
        db_path is not None
        and snapshot.source == "sqlite_candles"
        and (snapshot.orderbook_depth_source in {None, "orderbook_depth_levels"})
    ):
        depth_summary = default_dataset_adapter_registry().resolve_depth("orderbook_depth_levels").quality_summary(
            snapshot=snapshot,
            context=DatasetLoadContext(db_path=db_path),
        )
    elif snapshot.orderbook_depth_snapshots:
        depth_summary = _orderbook_depth_summary_from_snapshot(snapshot=snapshot)
    else:
        depth_summary = _empty_orderbook_depth_summary()
    depth_rows_available = bool(depth_summary["l2_depth_rows_available"])
    depth_complete_snapshots_available = bool(depth_summary["l2_depth_complete_snapshots_available"])
    depth_provenance = snapshot.orderbook_depth_adapter_provenance or {}
    depth_provenance_hash = sha256_prefixed(depth_provenance) if depth_provenance else None
    payload: dict[str, Any] = {
        "schema_version": 2,
        "artifact_type": "dataset_quality_report",
        "dataset_source": snapshot.source,
        "adapter_name": adapter_name,
        "adapter_version": adapter_version,
        "source": snapshot.source,
        "market": snapshot.market,
        "interval": snapshot.interval,
        "snapshot_id": snapshot.snapshot_id,
        "split_name": snapshot.split_name,
        "start_ts": snapshot.date_range.start_ts_ms(),
        "end_ts": snapshot.date_range.end_ts_ms(),
        "expected_candle_count": expected_count,
        "actual_candle_count": actual_count,
        "present_expected_bucket_count": present_expected_count,
        "coverage_pct": round(coverage_pct, 8),
        "missing_bucket_count": missing_count,
        "missing_bucket_ranges": missing_ranges,
        "missing_bucket_sample": missing_sample,
        "duplicate_key_count": duplicate_key_count,
        "non_monotonic_ts_count": non_monotonic,
        "interval_mismatch_count": interval_mismatch,
        "unexpected_bucket_count": unexpected_count,
        "ohlc_violation_count": ohlc_violations,
        "non_positive_price_count": non_positive_prices,
        "negative_volume_count": negative_volume,
        "first_ts": actual_ts[0] if actual_ts else None,
        "last_ts": actual_ts[-1] if actual_ts else None,
        "db_schema_fingerprint": _db_schema_fingerprint(db_path) if db_path is not None and snapshot.source == "sqlite_candles" else None,
        "dataset_content_hash": snapshot.content_hash(),
        "canonical_snapshot_hash": snapshot.content_hash(),
        "source_content_hash": snapshot.source_content_hash or snapshot.content_hash(),
        "source_content_hash_status": (
            "present" if snapshot.source_content_hash else "derived_from_materialized_snapshot"
        ),
        "source_schema_hash": (
            snapshot.source_schema_hash
            or (
                _db_schema_fingerprint(db_path)
                if db_path is not None and snapshot.source == "sqlite_candles"
                else "not_applicable:source_schema_unavailable"
            )
        ),
        "source_hash_status": "present" if snapshot.source_content_hash else "derived_from_materialized_snapshot",
        "source_schema_hash_status": (
            "present"
            if snapshot.source_schema_hash
            or (snapshot.source == "sqlite_candles" and db_path is not None)
            else "not_applicable"
        ),
        "source_locator_policy": (
            "runtime_db_path_excluded_from_dataset_hash"
            if snapshot.source == "sqlite_candles"
            else "source_locator_excluded_from_dataset_hash"
        ),
        "adapter_provenance": adapter_provenance or snapshot.adapter_provenance or {},
        "adapter_provenance_hash": sha256_prefixed(adapter_provenance or snapshot.adapter_provenance or {}),
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
            "top_of_book_available": any(quote is not None for quote in snapshot.top_of_book_quotes),
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
        "l2_depth_requested": bool(snapshot.orderbook_depth_requested),
        "l2_depth_required": bool(snapshot.orderbook_depth_required),
        "l2_depth_source": snapshot.orderbook_depth_source,
        "l2_depth_source_content_hash": depth_summary.get("l2_depth_content_hash"),
        "l2_depth_source_schema_hash": (
            _db_table_schema_fingerprint(db_path, "orderbook_depth_levels")
            if db_path is not None and snapshot.orderbook_depth_source in {None, "orderbook_depth_levels"}
            else snapshot.orderbook_depth_source_schema_hash
        ),
        "l2_depth_adapter_provenance": depth_provenance,
        "l2_depth_adapter_provenance_hash": depth_provenance_hash,
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
    if snapshot.top_of_book_requested:
        _add_top_of_book_quality_fields(payload=payload, snapshot=snapshot)
    payload["content_hash"] = sha256_prefixed(payload)
    return DatasetQualityReport(payload=payload)


def _duplicate_key_count_from_snapshot(*, snapshot: DatasetSnapshot) -> int:
    counts: dict[int, int] = {}
    for candle in snapshot.candles:
        counts[int(candle.ts)] = counts.get(int(candle.ts), 0) + 1
    return sum(count - 1 for count in counts.values() if count > 1)


def combined_dataset_fingerprint(snapshots: tuple[DatasetSnapshot, ...]) -> str:
    return sha256_prefixed([snapshot.fingerprint_payload() for snapshot in snapshots])


def combined_dataset_quality_hash(reports: tuple[DatasetQualityReport, ...]) -> str:
    return sha256_prefixed([report.payload for report in reports])


def _split_range(manifest: ExperimentManifest, split_name: str) -> DateRange:
    if split_name == "train":
        return manifest.dataset.split.train
    if split_name == "validation":
        return manifest.dataset.split.validation
    if split_name == "final_holdout" and manifest.dataset.split.final_holdout is not None:
        return manifest.dataset.split.final_holdout
    raise ValueError(f"unknown or unavailable dataset split: {split_name}")


def _interval_ms(interval: str) -> int:
    try:
        return interval_to_minute_unit(interval) * 60_000
    except ValueError as exc:
        raise ManifestValidationError(f"unsupported dataset interval for quality report: {interval}") from exc


def _expected_bucket_count(*, start_ts: int, end_ts: int, interval_ms: int) -> int:
    if end_ts < start_ts:
        return 0
    return ((end_ts - start_ts) // interval_ms) + 1


def _is_expected_bucket(ts: int, *, start_ts: int, end_ts: int, interval_ms: int) -> bool:
    return start_ts <= ts <= end_ts and (ts - start_ts) % interval_ms == 0


def _scan_missing_buckets(
    *,
    start_ts: int,
    end_ts: int,
    interval_ms: int,
    present_expected_ts: set[int],
    max_ranges: int = 20,
    max_sample: int = 20,
) -> tuple[int, list[dict[str, int]], list[int]]:
    missing_count = 0
    sample: list[int] = []
    ranges: list[dict[str, int]] = []
    active_start: int | None = None
    active_prev: int | None = None
    active_count = 0

    for ts in range(start_ts, end_ts + 1, interval_ms):
        if ts in present_expected_ts:
            if active_start is not None and len(ranges) < max_ranges:
                ranges.append(
                    {"start_ts": active_start, "end_ts": active_prev or active_start, "bucket_count": active_count}
                )
            active_start = None
            active_prev = None
            active_count = 0
            continue
        missing_count += 1
        if len(sample) < max_sample:
            sample.append(ts)
        if active_start is None:
            active_start = ts
            active_count = 1
        else:
            active_count += 1
        active_prev = ts

    if active_start is not None and len(ranges) < max_ranges:
        ranges.append(
            {"start_ts": active_start, "end_ts": active_prev or active_start, "bucket_count": active_count}
        )
    return missing_count, ranges, sample


def _compact_missing_ranges(missing_ts: list[int], interval_ms: int, *, max_ranges: int = 20) -> list[dict[str, int]]:
    if not missing_ts:
        return []
    ranges: list[dict[str, int]] = []
    start = missing_ts[0]
    prev = missing_ts[0]
    count = 1
    for ts in missing_ts[1:]:
        if ts == prev + interval_ms:
            prev = ts
            count += 1
            continue
        ranges.append({"start_ts": start, "end_ts": prev, "bucket_count": count})
        start = prev = ts
        count = 1
    ranges.append({"start_ts": start, "end_ts": prev, "bucket_count": count})
    return ranges[:max_ranges]


def _db_schema_fingerprint(db_path: str | Path) -> str:
    return _db_table_schema_fingerprint(db_path, "candles")


def _db_table_schema_fingerprint(db_path: str | Path, table_name: str) -> str:
    normalized_table = str(table_name)
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        table_info = [tuple(row) for row in conn.execute(f"PRAGMA table_info({normalized_table})").fetchall()]
        index_list = [tuple(row) for row in conn.execute(f"PRAGMA index_list({normalized_table})").fetchall()]
        index_info = {
            str(index[1]): [tuple(row) for row in conn.execute(f"PRAGMA index_info({str(index[1])})").fetchall()]
            for index in index_list
        }
    finally:
        conn.close()
    return sha256_prefixed(
        {
            "table": normalized_table,
            "table_info": table_info,
            "index_list": index_list,
            "index_info": index_info,
        }
    )


def _orderbook_depth_summary(*, db_path: str | Path, snapshot: DatasetSnapshot) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        return summarize_orderbook_depth_evidence(
            conn,
            pair=snapshot.market,
            start_ts=snapshot.date_range.start_ts_ms(),
            end_ts=snapshot.date_range.end_ts_ms(),
        )
    finally:
        conn.close()


def _orderbook_depth_summary_from_snapshot(*, snapshot: DatasetSnapshot) -> dict[str, Any]:
    snapshots = snapshot.orderbook_depth_snapshots
    if not snapshots:
        return _empty_orderbook_depth_summary()
    row_count = sum(len(item.bids) + len(item.asks) for item in snapshots)
    sources = sorted({str(item.source) for item in snapshots})
    payload = [_depth_snapshot_payload(item) for item in sorted(snapshots, key=lambda item: (int(item.ts), str(item.source)))]
    return {
        "l2_depth_rows_available": row_count > 0,
        "l2_depth_complete_snapshots_available": True,
        "l2_depth_snapshot_count": len(snapshots),
        "l2_depth_row_count": row_count,
        "l2_depth_first_ts": min(int(item.ts) for item in snapshots),
        "l2_depth_last_ts": max(int(item.ts) for item in snapshots),
        "l2_depth_sources": sources,
        "l2_depth_content_hash": sha256_prefixed(payload),
    }


def _empty_orderbook_depth_summary() -> dict[str, Any]:
    return {
        "l2_depth_rows_available": False,
        "l2_depth_complete_snapshots_available": False,
        "l2_depth_snapshot_count": 0,
        "l2_depth_row_count": 0,
        "l2_depth_first_ts": None,
        "l2_depth_last_ts": None,
        "l2_depth_sources": [],
        "l2_depth_content_hash": None,
    }


def _load_top_of_book_quotes(
    *,
    db_path: str | Path,
    market: str,
    candles: tuple[Candle, ...],
    join_tolerance_ms: int,
    quote_source: str | None,
) -> tuple[TopOfBookQuote | None, ...]:
    if not candles:
        return ()
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='orderbook_top_snapshots'"
        ).fetchone()
        if table is None:
            return tuple(None for _ in candles)
        tolerance = int(join_tolerance_ms)
        start_ts = min(int(candle.ts) for candle in candles) - tolerance
        end_ts = max(int(candle.ts) for candle in candles) + tolerance
        params: list[object] = [market, start_ts, end_ts]
        source_predicate = ""
        if quote_source is not None:
            source_predicate = "AND source=?"
            params.append(quote_source)
        rows = conn.execute(
            f"""
            SELECT ts, pair, bid_price, ask_price, spread_bps, source, observed_at_epoch_sec
            FROM orderbook_top_snapshots
            WHERE pair=?
              AND ts >= ?
              AND ts <= ?
              {source_predicate}
            ORDER BY ts ASC, source ASC
            """,
            tuple(params),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return tuple(None for _ in candles)
    quotes = tuple(_top_of_book_quote_from_row(row) for row in rows)
    quote_timestamps = tuple(int(quote.ts) for quote in quotes)
    out: list[TopOfBookQuote | None] = []
    for candle in candles:
        candle_ts = int(candle.ts)
        window_start = candle_ts - int(join_tolerance_ms)
        window_end = candle_ts + int(join_tolerance_ms)
        start_index = bisect_left(quote_timestamps, window_start)
        end_index = bisect_right(quote_timestamps, window_end)
        if start_index >= end_index:
            out.append(None)
            continue
        selected = min(
            (quotes[index] for index in range(start_index, end_index)),
            key=lambda quote: (abs(int(quote.ts) - candle_ts), int(quote.ts), str(quote.source)),
        )
        out.append(
            TopOfBookQuote(
                ts=selected.ts,
                pair=selected.pair,
                bid_price=selected.bid_price,
                ask_price=selected.ask_price,
                spread_bps=selected.spread_bps,
                source=selected.source,
                observed_at_epoch_sec=selected.observed_at_epoch_sec,
                matched_candle_ts=candle_ts,
                age_ms=abs(int(selected.ts) - candle_ts),
            )
        )
    return tuple(out)


def _top_of_book_quote_from_row(row: Any) -> TopOfBookQuote:
    return TopOfBookQuote(
        ts=int(row[0]),
        pair=str(row[1]),
        bid_price=float(row[2]),
        ask_price=float(row[3]),
        spread_bps=float(row[4]),
        source=str(row[5]),
        observed_at_epoch_sec=(None if row[6] is None else float(row[6])),
        matched_candle_ts=None,
        age_ms=None,
    )


def _load_top_of_book_event_quotes(
    *,
    db_path: str | Path,
    market: str,
    interval: str,
    candles: tuple[Candle, ...],
    quote_source: str | None,
    execution_quote_lookahead_ms: int,
) -> tuple[TopOfBookQuote, ...]:
    if not candles:
        return ()
    start_ts = int(candles[0].ts)
    end_ts = int(candles[-1].ts) + _interval_ms(interval) + int(execution_quote_lookahead_ms)
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='orderbook_top_snapshots'"
        ).fetchone()
        if table is None:
            return ()
        params: list[object] = [market, start_ts, end_ts]
        source_predicate = ""
        if quote_source is not None:
            source_predicate = "AND source=?"
            params.append(quote_source)
        rows = conn.execute(
            f"""
            SELECT ts, pair, bid_price, ask_price, spread_bps, source, observed_at_epoch_sec
            FROM orderbook_top_snapshots
            WHERE pair=?
              AND ts >= ?
              AND ts <= ?
              {source_predicate}
            ORDER BY ts ASC, source ASC
            """,
            tuple(params),
        ).fetchall()
    finally:
        conn.close()
    return tuple(
        TopOfBookQuote(
            ts=int(row[0]),
            pair=str(row[1]),
            bid_price=float(row[2]),
            ask_price=float(row[3]),
            spread_bps=float(row[4]),
            source=str(row[5]),
            observed_at_epoch_sec=(None if row[6] is None else float(row[6])),
            matched_candle_ts=None,
            age_ms=None,
        )
        for row in rows
    )


def _load_orderbook_depth_event_snapshots(
    *,
    db_path: str | Path,
    market: str,
    interval: str,
    candles: tuple[Candle, ...],
    source: str | None,
    execution_depth_lookahead_ms: int,
) -> tuple[OrderbookDepthSnapshot, ...]:
    if not candles:
        return ()
    start_ts = int(candles[0].ts)
    end_ts = int(candles[-1].ts) + _interval_ms(interval) + int(execution_depth_lookahead_ms)
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='orderbook_depth_levels'"
        ).fetchone()
        if table is None:
            return ()
        params: list[object] = [market, start_ts, end_ts]
        source_predicate = ""
        if source is not None:
            source_predicate = "AND source=?"
            params.append(source)
        rows = conn.execute(
            f"""
            SELECT ts, pair, source, observed_at_epoch_sec, side, level_index, price, size
            FROM orderbook_depth_levels
            WHERE pair=?
              AND ts >= ?
              AND ts <= ?
              {source_predicate}
            ORDER BY ts ASC, source ASC, side ASC, level_index ASC
            """,
            tuple(params),
        ).fetchall()
    finally:
        conn.close()
    grouped: dict[tuple[int, str, str, float | None], dict[str, list[tuple[float, float]]]] = {}
    for row in rows:
        key = (
            int(row[0]),
            str(row[1]),
            str(row[2]),
            None if row[3] is None else float(row[3]),
        )
        side = str(row[4])
        grouped.setdefault(key, {"bid": [], "ask": []}).setdefault(side, []).append((float(row[6]), float(row[7])))
    snapshots: list[OrderbookDepthSnapshot] = []
    for (ts, pair, snapshot_source, observed), sides in sorted(grouped.items()):
        if not sides.get("bid") or not sides.get("ask"):
            continue
        snapshots.append(
            build_orderbook_depth_snapshot(
                ts=ts,
                pair=pair,
                bid_levels=sides["bid"],
                ask_levels=sides["ask"],
                source=snapshot_source,
                observed_at_epoch_sec=observed,
            )
        )
    return tuple(snapshots)


def _depth_snapshot_payload(snapshot: OrderbookDepthSnapshot) -> dict[str, object]:
    return {
        "ts": int(snapshot.ts),
        "pair": snapshot.pair,
        "source": snapshot.source,
        "observed_at_epoch_sec": snapshot.observed_at_epoch_sec,
        "bids": [(level.price, level.size) for level in snapshot.bids],
        "asks": [(level.price, level.size) for level in snapshot.asks],
    }


def _add_top_of_book_quality_fields(*, payload: dict[str, Any], snapshot: DatasetSnapshot) -> None:
    expected = len(snapshot.candles)
    joined = sum(1 for quote in snapshot.top_of_book_quotes if quote is not None)
    missing_sample = [
        candle.ts
        for candle, quote in zip(snapshot.candles, snapshot.top_of_book_quotes)
        if quote is None
    ][:20]
    coverage_pct = (joined / expected * 100.0) if expected else 0.0
    reasons: list[str] = []
    if joined < expected:
        reasons.append("top_of_book_missing")
    if coverage_pct < float(snapshot.top_of_book_min_coverage_pct):
        reasons.append("top_of_book_coverage_below_threshold")
    gate_status = "PASS"
    if reasons:
        gate_status = "FAIL" if snapshot.top_of_book_required or snapshot.top_of_book_missing_policy == "fail" else "WARN"
    if gate_status == "FAIL":
        existing_reasons = list(payload.get("quality_gate_reasons") or [])
        existing_reasons.extend(reasons)
        payload["quality_gate_reasons"] = existing_reasons
        payload["quality_gate_status"] = "FAIL"
    top_provenance = snapshot.top_of_book_adapter_provenance or {}
    payload.update(
        {
            "top_of_book_requested": True,
            "top_of_book_required": bool(snapshot.top_of_book_required),
            "top_of_book_missing_policy": snapshot.top_of_book_missing_policy,
            "top_of_book_source": snapshot.top_of_book_source or "sqlite_orderbook_top_snapshots",
            "top_of_book_join_tolerance_ms": snapshot.top_of_book_join_tolerance_ms,
            "top_of_book_expected_signal_count": expected,
            "top_of_book_joined_count": joined,
            "top_of_book_missing_count": expected - joined,
            "top_of_book_missing_sample": missing_sample,
            "top_of_book_coverage_pct": round(coverage_pct, 8),
            "top_of_book_gate_status": gate_status,
            "top_of_book_gate_reasons": reasons,
            "top_of_book_source_content_hash": _top_of_book_content_hash(snapshot),
            "top_of_book_source_schema_hash": snapshot.top_of_book_source_schema_hash,
            "top_of_book_adapter_name": top_provenance.get("adapter_name"),
            "top_of_book_adapter_version": top_provenance.get("adapter_version"),
            "top_of_book_adapter_provenance": top_provenance,
            "top_of_book_adapter_provenance_hash": sha256_prefixed(top_provenance) if top_provenance else None,
            "top_of_book_join_policy": "nearest_quote_within_tolerance",
            "top_of_book_quote_age_policy": "absolute_distance_to_candle_ts_lte_join_tolerance_ms",
        }
    )


def _top_of_book_content_hash(snapshot: DatasetSnapshot) -> str | None:
    if not snapshot.top_of_book_requested:
        return None
    return sha256_prefixed(
        {
            "candle_quotes": [
                quote.as_tuple() if quote is not None else None
                for quote in snapshot.top_of_book_quotes
            ],
            "event_quotes": [quote.as_tuple() for quote in snapshot.top_of_book_event_quotes],
        }
    )


class SQLiteCandleAdapter:
    source = "sqlite_candles"
    adapter_name = "sqlite_candle_adapter"
    adapter_version = "1"
    supported_capabilities = frozenset({"candles"})
    supported_top_of_book_sources = frozenset()
    supported_depth_sources = frozenset()
    supports_sqlite_streaming_quality_scan = True

    def load_range(
        self,
        *,
        manifest: ExperimentManifest,
        split_name: str,
        date_range: DateRange,
        context: DatasetLoadContext,
    ) -> DatasetSnapshot:
        if context.db_path is None:
            raise ValueError("sqlite_dataset_adapter_db_path_missing")
        return _load_sqlite_dataset_range(
            db_path=context.db_path,
            manifest=manifest,
            split_name=split_name,
            date_range=date_range,
        )

    def quality_report(
        self,
        *,
        snapshot: DatasetSnapshot,
        context: DatasetLoadContext,
    ) -> DatasetQualityReport:
        if context.db_path is None:
            raise ValueError("sqlite_dataset_adapter_db_path_missing")
        schema_hash = _db_schema_fingerprint(context.db_path)
        registry = default_dataset_adapter_registry()
        top_adapter = (
            registry.resolve_top_of_book(snapshot.top_of_book_source)
            if snapshot.top_of_book_requested and snapshot.top_of_book_source is not None
            else None
        )
        depth_source = snapshot.orderbook_depth_source or "orderbook_depth_levels"
        depth_adapter = registry.resolve_depth(depth_source)
        top_schema_hash = (
            _db_table_schema_fingerprint(context.db_path, "orderbook_top_snapshots")
            if snapshot.top_of_book_requested and snapshot.top_of_book_source == "sqlite_orderbook_top_snapshots"
            else snapshot.top_of_book_source_schema_hash
        )
        provenance = {
            "candle": {
                "dataset_source": self.source,
                "adapter_name": self.adapter_name,
                "adapter_version": self.adapter_version,
            },
            "sqlite": {
                "source_locator_policy": "runtime_db_path_excluded_from_dataset_quality_hash",
                "db_schema_fingerprint": schema_hash,
                "tables": _sqlite_present_tables(context.db_path),
                "scan_method": "snapshot_materialized_with_sqlite_depth_summary",
            },
            "top_of_book": (
                {
                    "source": snapshot.top_of_book_source,
                    "requested": snapshot.top_of_book_requested,
                    "adapter_name": top_adapter.adapter_name if top_adapter is not None else None,
                    "adapter_version": top_adapter.adapter_version if top_adapter is not None else None,
                }
                if snapshot.top_of_book_requested
                else None
            ),
            "depth": {
                "source": depth_source,
                "adapter_name": depth_adapter.adapter_name,
                "adapter_version": depth_adapter.adapter_version,
                "snapshot_count": len(snapshot.orderbook_depth_snapshots),
            },
        }
        report = _build_source_agnostic_dataset_quality_report(
            db_path=context.db_path,
            snapshot=snapshot,
            adapter_name=self.adapter_name,
            adapter_version=self.adapter_version,
            adapter_provenance=provenance,
        )
        report.payload["source_schema_hash"] = schema_hash
        report.payload["source_schema_hash_status"] = "present"
        if snapshot.top_of_book_requested:
            report.payload["top_of_book_source_schema_hash"] = top_schema_hash
            report.payload["top_of_book_adapter_provenance"] = snapshot.top_of_book_adapter_provenance or report.payload.get("top_of_book_adapter_provenance")
            report.payload["top_of_book_adapter_provenance_hash"] = sha256_prefixed(report.payload["top_of_book_adapter_provenance"] or {})
        if snapshot.orderbook_depth_requested:
            report.payload["l2_depth_adapter_provenance"] = snapshot.orderbook_depth_adapter_provenance or {}
            report.payload["l2_depth_adapter_provenance_hash"] = sha256_prefixed(report.payload["l2_depth_adapter_provenance"])
        report.payload["scan_method"] = "sqlite_adapter_snapshot_scan"
        report.payload["content_hash"] = sha256_prefixed({k: v for k, v in report.payload.items() if k != "content_hash"})
        return report

    def provenance(
        self,
        *,
        manifest: ExperimentManifest,
        context: DatasetLoadContext,
    ) -> dict[str, Any]:
        schema_hash = _db_schema_fingerprint(context.db_path) if context.db_path is not None else None
        return {
            "dataset_source": manifest.dataset.source,
            "adapter_name": self.adapter_name,
            "adapter_version": self.adapter_version,
            "source_locator": "runtime_db_path_excluded_from_dataset_hash",
            "source_content_hash": manifest.dataset.source_content_hash,
            "source_schema_hash": manifest.dataset.source_schema_hash or schema_hash,
            "provenance_policy": "sqlite_compatibility_adapter",
        }


class SQLiteTopOfBookAdapter:
    source = "sqlite_orderbook_top_snapshots"
    adapter_name = "sqlite_top_of_book_adapter"
    adapter_version = "1"

    def load_candle_quotes(
        self,
        *,
        manifest: ExperimentManifest,
        candles: tuple[Candle, ...],
        context: DatasetLoadContext,
    ) -> tuple[TopOfBookQuote | None, ...]:
        if context.db_path is None:
            raise ValueError("sqlite_top_of_book_adapter_db_path_missing")
        spec = manifest.dataset.top_of_book
        if spec is None:
            return ()
        return _load_top_of_book_quotes(
            db_path=context.db_path,
            market=manifest.market,
            candles=candles,
            join_tolerance_ms=spec.join_tolerance_ms,
            quote_source=spec.quote_source,
        )

    def load_event_quotes(
        self,
        *,
        manifest: ExperimentManifest,
        candles: tuple[Candle, ...],
        execution_quote_lookahead_ms: int,
        context: DatasetLoadContext,
    ) -> tuple[TopOfBookQuote, ...]:
        if context.db_path is None:
            raise ValueError("sqlite_top_of_book_adapter_db_path_missing")
        spec = manifest.dataset.top_of_book
        if spec is None:
            return ()
        return _load_top_of_book_event_quotes(
            db_path=context.db_path,
            market=manifest.market,
            interval=manifest.interval,
            candles=candles,
            quote_source=spec.quote_source,
            execution_quote_lookahead_ms=execution_quote_lookahead_ms,
        )

    def provenance(
        self,
        *,
        manifest: ExperimentManifest,
        context: DatasetLoadContext,
    ) -> dict[str, Any]:
        return {
            "top_of_book_source": self.source,
            "adapter_name": self.adapter_name,
            "adapter_version": self.adapter_version,
            "quote_source": manifest.dataset.top_of_book.quote_source if manifest.dataset.top_of_book else None,
            "provenance_policy": "sqlite_top_of_book_compatibility_adapter",
        }


class SQLiteOrderbookDepthAdapter:
    source = "orderbook_depth_levels"
    adapter_name = "sqlite_orderbook_depth_adapter"
    adapter_version = "1"

    def load_event_snapshots(
        self,
        *,
        manifest: ExperimentManifest,
        candles: tuple[Candle, ...],
        execution_depth_lookahead_ms: int,
        context: DatasetLoadContext,
    ) -> tuple[OrderbookDepthSnapshot, ...]:
        if context.db_path is None:
            raise ValueError("sqlite_orderbook_depth_adapter_db_path_missing")
        spec = manifest.dataset.depth
        options = spec.options if spec is not None else {}
        source_filter = options.get("quote_source") or options.get("source_filter")
        parsed_source_filter = str(source_filter).strip() if source_filter is not None else None
        if parsed_source_filter == "":
            parsed_source_filter = None
        return _load_orderbook_depth_event_snapshots(
            db_path=context.db_path,
            market=manifest.market,
            interval=manifest.interval,
            candles=candles,
            source=parsed_source_filter,
            execution_depth_lookahead_ms=execution_depth_lookahead_ms,
        )

    def quality_summary(
        self,
        *,
        snapshot: DatasetSnapshot,
        context: DatasetLoadContext,
    ) -> dict[str, Any]:
        if context.db_path is not None and snapshot.source == "sqlite_candles":
            return _orderbook_depth_summary(db_path=context.db_path, snapshot=snapshot)
        return _orderbook_depth_summary_from_snapshot(snapshot=snapshot)

    def provenance(
        self,
        *,
        manifest: ExperimentManifest,
        context: DatasetLoadContext,
    ) -> dict[str, Any]:
        return {
            "depth_source": self.source,
            "adapter_name": self.adapter_name,
            "adapter_version": self.adapter_version,
            "options": dict(manifest.dataset.depth.options) if manifest.dataset.depth is not None else {},
            "provenance_policy": "sqlite_orderbook_depth_compatibility_adapter",
        }


def _sqlite_present_tables(db_path: str | Path) -> list[str]:
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
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


default_dataset_adapter_registry().register(SQLiteCandleAdapter())
default_dataset_adapter_registry().register_top_of_book(SQLiteTopOfBookAdapter())
default_dataset_adapter_registry().register_depth(SQLiteOrderbookDepthAdapter())
