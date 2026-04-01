from __future__ import annotations

import json
from pathlib import Path

import pytest

from bithumb_bot.market_catalog_snapshot import record_market_catalog_snapshot
from bithumb_bot.markets import MarketInfo
from bithumb_bot.paths import PathManager


def _manager_for_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, mode: str) -> PathManager:
    project_root = tmp_path / "repo"
    project_root.mkdir(parents=True, exist_ok=True)
    runtime_root = tmp_path / "runtime"

    monkeypatch.setenv("MODE", mode)
    monkeypatch.setenv("ENV_ROOT", str(runtime_root / "env"))
    monkeypatch.setenv("RUN_ROOT", str(runtime_root / "run"))
    monkeypatch.setenv("DATA_ROOT", str(runtime_root / "data"))
    monkeypatch.setenv("LOG_ROOT", str(runtime_root / "logs"))
    monkeypatch.setenv("BACKUP_ROOT", str(runtime_root / "backup"))
    monkeypatch.setenv("ARCHIVE_ROOT", str(runtime_root / "archive"))
    return PathManager.from_env(project_root)


def test_market_catalog_snapshot_uses_managed_mode_separated_paths(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)

    paper_manager = _manager_for_mode(tmp_path, monkeypatch, mode="paper")
    live_manager = _manager_for_mode(tmp_path, monkeypatch, mode="live")

    record_market_catalog_snapshot(
        path_manager=paper_manager,
        mode="paper",
        source="test",
        markets=[MarketInfo(market="KRW-BTC", market_warning="NONE")],
    )
    record_market_catalog_snapshot(
        path_manager=live_manager,
        mode="live",
        source="test",
        markets=[MarketInfo(market="KRW-BTC", market_warning="NONE")],
    )

    paper_snapshot = paper_manager.derived_path("market_catalog_snapshot", ext="json")
    live_snapshot = live_manager.derived_path("market_catalog_snapshot", ext="json")

    assert paper_snapshot.exists()
    assert live_snapshot.exists()
    assert "/data/paper/" in paper_snapshot.as_posix()
    assert "/data/live/" in live_snapshot.as_posix()


def test_market_catalog_diff_detects_warning_and_listing_changes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    manager = _manager_for_mode(tmp_path, monkeypatch, mode="paper")

    notified: list[str] = []
    monkeypatch.setattr("bithumb_bot.market_catalog_snapshot.notify", lambda msg, **_kwargs: notified.append(msg))

    record_market_catalog_snapshot(
        path_manager=manager,
        mode="paper",
        source="test",
        markets=[MarketInfo(market="KRW-BTC", korean_name="비트코인", english_name="Bitcoin", market_warning="NONE")],
    )

    record_market_catalog_snapshot(
        path_manager=manager,
        mode="paper",
        source="test",
        markets=[
            MarketInfo(market="KRW-BTC", korean_name="비트코인", english_name="Bitcoin", market_warning="CAUTION"),
            MarketInfo(market="KRW-ETH", korean_name="이더리움", english_name="Ethereum", market_warning="NONE"),
        ],
    )

    diff_path = manager.report_path("market_catalog_diff", ext="jsonl")
    assert diff_path.exists()
    lines = [json.loads(line) for line in diff_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 1
    event = lines[0]

    assert event["added_count"] == 1
    assert event["warning_changed_count"] == 1
    assert event["removed_count"] == 0
    assert event["diff"]["added_markets"] == ["KRW-ETH"]
    assert event["diff"]["warning_changed"][0]["market"] == "KRW-BTC"
    assert len(notified) == 1

    # same snapshot should not produce additional diff event
    record_market_catalog_snapshot(
        path_manager=manager,
        mode="paper",
        source="test",
        markets=[
            MarketInfo(market="KRW-BTC", korean_name="비트코인", english_name="Bitcoin", market_warning="CAUTION"),
            MarketInfo(market="KRW-ETH", korean_name="이더리움", english_name="Ethereum", market_warning="NONE"),
        ],
    )
    lines_after = [json.loads(line) for line in diff_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines_after) == 1
