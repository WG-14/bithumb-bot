from __future__ import annotations

from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange
from bithumb_bot.research.hashing import sha256_prefixed


def _snapshot(close: float) -> DatasetSnapshot:
    return DatasetSnapshot(
        snapshot_id=f"snapshot_{close}",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name="train",
        date_range=DateRange(start="2023-01-01", end="2023-01-01"),
        candles=(Candle(ts=1_700_000_000_000, open=close, high=close, low=close, close=close, volume=1.0),),
    )


def test_dataset_snapshot_content_hash_is_memoized_per_instance(monkeypatch) -> None:
    snapshot = _snapshot(100.0)
    original = DatasetSnapshot.fingerprint_payload
    calls = 0

    def counted(self):
        nonlocal calls
        calls += 1
        return original(self)

    monkeypatch.setattr(DatasetSnapshot, "fingerprint_payload", counted)

    first = snapshot.content_hash()
    second = snapshot.content_hash()

    assert first == second
    assert first == sha256_prefixed(original(snapshot))
    assert calls == 1


def test_dataset_snapshot_content_hash_cache_is_instance_local() -> None:
    first = _snapshot(100.0)
    second = _snapshot(101.0)

    assert first.content_hash() == first.content_hash()
    assert second.content_hash() == second.content_hash()
    assert first.content_hash() != second.content_hash()
