from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .experiment_manifest import DateRange, ExperimentManifest
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
class DatasetSnapshot:
    snapshot_id: str
    source: str
    market: str
    interval: str
    split_name: str
    date_range: DateRange
    candles: tuple[Candle, ...]

    def fingerprint_payload(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "source": self.source,
            "market": self.market,
            "interval": self.interval,
            "split_name": self.split_name,
            "date_range": self.date_range.as_dict(),
            "candles": [candle.as_tuple() for candle in self.candles],
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.fingerprint_payload())


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
    return DatasetSnapshot(
        snapshot_id=manifest.dataset.snapshot_id,
        source=manifest.dataset.source,
        market=manifest.market,
        interval=manifest.interval,
        split_name=split_name,
        date_range=date_range,
        candles=candles,
    )


def combined_dataset_fingerprint(snapshots: tuple[DatasetSnapshot, ...]) -> str:
    return sha256_prefixed([snapshot.fingerprint_payload() for snapshot in snapshots])


def _split_range(manifest: ExperimentManifest, split_name: str) -> DateRange:
    if split_name == "train":
        return manifest.dataset.split.train
    if split_name == "validation":
        return manifest.dataset.split.validation
    if split_name == "final_holdout" and manifest.dataset.split.final_holdout is not None:
        return manifest.dataset.split.final_holdout
    raise ValueError(f"unknown or unavailable dataset split: {split_name}")
