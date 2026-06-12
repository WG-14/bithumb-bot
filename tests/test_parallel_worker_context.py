from __future__ import annotations

import pytest

from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.experiment_manifest import DateRange
from bithumb_bot.research.validation_protocol import _load_worker_task_snapshots
from tests.test_research_memory_admission import _manifest_with_workers


def _snapshot(split_name: str) -> DatasetSnapshot:
    return DatasetSnapshot(
        snapshot_id=f"{split_name}_snapshot",
        source="unit",
        market="KRW-BTC",
        interval="1m",
        split_name=split_name,
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=(Candle(0, 1.0, 1.0, 1.0, 1.0, 1.0),),
    )


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.parallel_e2e
def test_parallel_worker_initializer_context_does_not_include_snapshots() -> None:
    manifest = _manifest_with_workers(2)
    context = {
        "manifest": manifest,
        "db_path": "/tmp/research.sqlite",
        "split_names": ("train", "validation"),
        "dataset_hashes": {"train": "sha256:train", "validation": "sha256:validation"},
        "quality_hashes": {"train": "sha256:qtrain", "validation": "sha256:qvalidation"},
        "manifest_hash": manifest.manifest_hash(),
        "raw_candidate_count": 1,
    }

    forbidden = {"snapshots", "candles", "top_of_book_quotes", "orderbook_depth_snapshots"}
    assert forbidden.isdisjoint(context)
    assert not any(isinstance(value, DatasetSnapshot) for value in context.values())


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.parallel_e2e
def test_parallel_worker_loads_only_required_split(monkeypatch) -> None:
    loaded: list[str] = []

    def fake_load_dataset_split(*, db_path, manifest, split_name):  # type: ignore[no-untyped-def]
        del db_path, manifest
        loaded.append(split_name)
        return _snapshot(split_name)

    monkeypatch.setattr(
        "bithumb_bot.research.validation_protocol.load_dataset_split",
        fake_load_dataset_split,
    )

    snapshots = _load_worker_task_snapshots(
        task={"db_path": "/tmp/research.sqlite", "split_names": ("validation",)},
        manifest=_manifest_with_workers(2),
    )

    assert loaded == ["validation"]
    assert tuple(snapshots) == ("validation",)


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.parallel_e2e
def test_serial_path_can_still_use_in_process_snapshots() -> None:
    snapshots = {"train": _snapshot("train"), "validation": _snapshot("validation")}

    assert isinstance(snapshots["train"], DatasetSnapshot)
    assert snapshots["validation"].candles[0].close == 1.0
