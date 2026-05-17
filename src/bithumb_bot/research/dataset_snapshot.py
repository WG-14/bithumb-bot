from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from bisect import bisect_left
from pathlib import Path
from typing import Any

from bithumb_bot.public_api_minute_candles import interval_to_minute_unit
from bithumb_bot.orderbook_depth_store import has_orderbook_depth_evidence

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
    top_of_book_quotes: tuple[TopOfBookQuote | None, ...] = ()
    top_of_book_event_quotes: tuple[TopOfBookQuote, ...] = ()
    top_of_book_requested: bool = False
    top_of_book_required: bool = False
    top_of_book_missing_policy: str | None = None
    top_of_book_source: str | None = None
    top_of_book_join_tolerance_ms: int | None = None
    top_of_book_min_coverage_pct: float = 100.0

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
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.fingerprint_payload())

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
    top_of_book_quotes: tuple[TopOfBookQuote | None, ...] = ()
    top_of_book_event_quotes: tuple[TopOfBookQuote, ...] = ()
    top_of_book_spec = manifest.dataset.top_of_book
    if top_of_book_spec is not None:
        top_of_book_quotes = _load_top_of_book_quotes(
            db_path=db_path,
            market=manifest.market,
            candles=candles,
            join_tolerance_ms=top_of_book_spec.join_tolerance_ms,
            quote_source=top_of_book_spec.quote_source,
        )
        execution_quote_lookahead_ms = (
            int(manifest.execution_timing.decision_guard_ms)
            + int(max((scenario.latency_ms for scenario in manifest.execution_model.scenarios), default=0))
            + int(manifest.execution_timing.max_quote_wait_ms)
        )
        top_of_book_event_quotes = _load_top_of_book_event_quotes(
            db_path=db_path,
            market=manifest.market,
            interval=manifest.interval,
            candles=candles,
            quote_source=top_of_book_spec.quote_source,
            execution_quote_lookahead_ms=execution_quote_lookahead_ms,
        )
    return DatasetSnapshot(
        snapshot_id=manifest.dataset.snapshot_id,
        source=manifest.dataset.source,
        market=manifest.market,
        interval=manifest.interval,
        split_name=split_name,
        date_range=date_range,
        candles=candles,
        top_of_book_quotes=top_of_book_quotes,
        top_of_book_event_quotes=top_of_book_event_quotes,
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
    duplicate_key_count = _duplicate_key_count(db_path=db_path, snapshot=snapshot)
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
    depth_available = _orderbook_depth_available(db_path=db_path, snapshot=snapshot)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "artifact_type": "dataset_quality_report",
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
        "db_schema_fingerprint": _db_schema_fingerprint(db_path),
        "dataset_content_hash": snapshot.content_hash(),
        "quality_gate_status": "PASS" if not reasons else "FAIL",
        "quality_gate_reasons": reasons,
        "limitations": {
            "orderbook_depth_available": depth_available,
            "l2_depth_evidence_available": depth_available,
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
        "depth_available": depth_available,
        "depth_availability_source": (
            "sqlite_orderbook_depth_levels" if depth_available else "orderbook_depth_levels_missing_or_empty"
        ),
    }
    if snapshot.top_of_book_requested:
        _add_top_of_book_quality_fields(payload=payload, snapshot=snapshot)
    payload["content_hash"] = sha256_prefixed(payload)
    return DatasetQualityReport(payload=payload)


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


def _duplicate_key_count(*, db_path: str | Path, snapshot: DatasetSnapshot) -> int:
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT COUNT(*) - COUNT(DISTINCT ts)
            FROM candles
            WHERE pair=? AND interval=? AND ts >= ? AND ts <= ?
            """,
            (
                snapshot.market,
                snapshot.interval,
                snapshot.date_range.start_ts_ms(),
                snapshot.date_range.end_ts_ms(),
            ),
        ).fetchone()
    finally:
        conn.close()
    return int(rows[0] or 0) if rows else 0


def _db_schema_fingerprint(db_path: str | Path) -> str:
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        table_info = [tuple(row) for row in conn.execute("PRAGMA table_info(candles)").fetchall()]
        index_list = [tuple(row) for row in conn.execute("PRAGMA index_list(candles)").fetchall()]
        index_info = {
            str(index[1]): [tuple(row) for row in conn.execute(f"PRAGMA index_info({str(index[1])})").fetchall()]
            for index in index_list
        }
    finally:
        conn.close()
    return sha256_prefixed(
        {
            "table": "candles",
            "table_info": table_info,
            "index_list": index_list,
            "index_info": index_info,
        }
    )


def _orderbook_depth_available(*, db_path: str | Path, snapshot: DatasetSnapshot) -> bool:
    conn = sqlite3.connect(f"file:{Path(db_path).expanduser().resolve()}?mode=ro", uri=True)
    try:
        return has_orderbook_depth_evidence(
            conn,
            pair=snapshot.market,
            start_ts=snapshot.date_range.start_ts_ms(),
            end_ts=snapshot.date_range.end_ts_ms(),
        )
    finally:
        conn.close()


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
        out: list[TopOfBookQuote | None] = []
        for candle in candles:
            params: list[object] = [
                market,
                candle.ts - int(join_tolerance_ms),
                candle.ts + int(join_tolerance_ms),
            ]
            source_predicate = ""
            if quote_source is not None:
                source_predicate = "AND source=?"
                params.append(quote_source)
            params.append(candle.ts)
            row = conn.execute(
                f"""
                SELECT ts, pair, bid_price, ask_price, spread_bps, source, observed_at_epoch_sec
                FROM orderbook_top_snapshots
                WHERE pair=?
                  AND ts >= ?
                  AND ts <= ?
                  {source_predicate}
                ORDER BY ABS(ts - ?) ASC, ts ASC, source ASC
                LIMIT 1
                """,
                tuple(params),
            ).fetchone()
            if row is None:
                out.append(None)
                continue
            quote_ts = int(row[0])
            out.append(
                TopOfBookQuote(
                    ts=quote_ts,
                    pair=str(row[1]),
                    bid_price=float(row[2]),
                    ask_price=float(row[3]),
                    spread_bps=float(row[4]),
                    source=str(row[5]),
                    observed_at_epoch_sec=(None if row[6] is None else float(row[6])),
                    matched_candle_ts=candle.ts,
                    age_ms=abs(quote_ts - candle.ts),
                )
            )
        return tuple(out)
    finally:
        conn.close()


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
        }
    )
