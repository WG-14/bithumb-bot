from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from bithumb_bot.app import main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.historical_backfill import backfill_candles
from bithumb_bot.public_api_minute_candles import MinuteCandle
from bithumb_bot.research.execution_calibration import build_calibration_artifact


class _DummyClient:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def _settings_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    old_db_path = settings.DB_PATH
    old_pair = settings.PAIR
    old_interval = settings.INTERVAL
    old_mode = settings.MODE
    object.__setattr__(settings, "DB_PATH", str(tmp_path / "paper.sqlite"))
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "INTERVAL", "1m")
    object.__setattr__(settings, "MODE", "paper")
    monkeypatch.setenv("MODE", "paper")
    monkeypatch.setattr("bithumb_bot.historical_backfill.canonical_market_id", lambda market: str(market).strip().upper())
    try:
        yield
    finally:
        object.__setattr__(settings, "DB_PATH", old_db_path)
        object.__setattr__(settings, "PAIR", old_pair)
        object.__setattr__(settings, "INTERVAL", old_interval)
        object.__setattr__(settings, "MODE", old_mode)


def _candle(
    utc: str,
    *,
    close: float = 100.0,
    timestamp: int = 1_111_111_199_999,
    market: str = "KRW-BTC",
) -> MinuteCandle:
    return MinuteCandle(
        market=market,
        candle_date_time_utc=utc,
        candle_date_time_kst=utc,
        opening_price=close,
        high_price=close + 1.0,
        low_price=close - 1.0,
        trade_price=close,
        timestamp=timestamp,
        candle_acc_trade_price=10_000.0,
        candle_acc_trade_volume=1.0,
    )


def test_backfill_uses_candle_bucket_timestamp_and_is_idempotent(monkeypatch, _settings_guard) -> None:
    pages = [[_candle("2023-01-01T00:00:00", timestamp=9_999_999_999_999)]]

    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.historical_backfill.fetch_minute_candles", lambda *args, **kwargs: pages[0])

    backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-01")
    backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-01")

    with sqlite3.connect(settings.DB_PATH) as conn:
        rows = conn.execute("SELECT ts, COUNT(*) FROM candles GROUP BY ts").fetchall()

    assert rows == [(1_672_531_200_000, 1)]


def test_backfill_backward_pagination_cursor_moves_to_older_candles(monkeypatch, _settings_guard) -> None:
    calls: list[str | None] = []
    pages = [
        [_candle("2023-01-02T00:01:00"), _candle("2023-01-02T00:00:00")],
        [_candle("2023-01-01T23:59:00")],
        [],
    ]

    def fake_fetch(client, *, market: str, minute_unit: int, count: int, to: str | None = None, max_retries=None):
        calls.append(to)
        return pages.pop(0)

    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.historical_backfill.fetch_minute_candles", fake_fetch)

    backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-02", batch_size=200)

    assert calls[0] == "2023-01-03T00:00:00"
    assert calls[1] == "2023-01-02T00:00:00"


def test_backfill_inclusive_boundary_fallback_continues_and_avoids_duplicate_writes(monkeypatch, _settings_guard) -> None:
    calls: list[str | None] = []
    first_page = [_candle("2023-01-01T00:01:00"), _candle("2023-01-01T00:00:00")]
    pages = [
        first_page,
        first_page,
        [_candle("2022-12-31T23:59:00")],
        [],
    ]
    progress_reasons: list[str | None] = []

    def fake_fetch(client, *, market: str, minute_unit: int, count: int, to: str | None = None, max_retries=None):
        calls.append(to)
        return pages.pop(0)

    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.historical_backfill.fetch_minute_candles", fake_fetch)

    result = backfill_candles(
        market="KRW-BTC",
        interval="1m",
        start="2022-12-31",
        end="2023-01-01",
        progress_callback=lambda progress: progress_reasons.append(progress.reason),
    )

    assert result.progress.status == "COMPLETE"
    assert result.progress.cursor_fallback_count == 1
    assert "cursor_boundary_fallback" in progress_reasons
    assert calls[2] == "2022-12-31T23:59:00"
    with sqlite3.connect(settings.DB_PATH) as conn:
        rows = conn.execute("SELECT ts, COUNT(*) FROM candles GROUP BY ts ORDER BY ts").fetchall()
    assert rows == [
        (1_672_531_140_000, 1),
        (1_672_531_200_000, 1),
        (1_672_531_260_000, 1),
    ]


def test_backfill_duplicate_page_stops_without_infinite_loop(monkeypatch, _settings_guard) -> None:
    page = [_candle("2023-01-02T00:00:00")]
    calls = 0

    def fake_fetch(*args, **kwargs):
        nonlocal calls
        calls += 1
        return page

    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.historical_backfill.fetch_minute_candles", fake_fetch)

    result = backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-02")

    assert calls == 3
    assert result.progress.duplicate_page_count == 2
    assert result.progress.cursor_stall_count == 2
    assert result.progress.cursor_fallback_count == 1
    assert result.progress.status == "INCOMPLETE"
    assert result.progress.reason == "cursor_fallback_no_progress"


def test_backfill_empty_response_stops_cleanly(monkeypatch, _settings_guard) -> None:
    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.historical_backfill.fetch_minute_candles", lambda *args, **kwargs: [])

    result = backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-01")

    assert result.progress.request_count == 1
    assert result.progress.fetched_count == 0
    assert result.coverage["missing_buckets"] == 1440


def test_backfill_market_mismatch_fails_closed(monkeypatch, _settings_guard) -> None:
    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr(
        "bithumb_bot.historical_backfill.fetch_minute_candles",
        lambda *args, **kwargs: [_candle("2023-01-01T00:00:00", market="KRW-ETH")],
    )

    with pytest.raises(ValueError, match="minute candle market mismatch"):
        backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-01")

    assert not Path(settings.DB_PATH).exists()


def test_backfill_dry_run_does_not_write_db(monkeypatch, _settings_guard) -> None:
    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr(
        "bithumb_bot.historical_backfill.fetch_minute_candles",
        lambda *args, **kwargs: [_candle("2023-01-01T00:00:00")],
    )

    result = backfill_candles(
        market="KRW-BTC",
        interval="1m",
        start="2023-01-01",
        end="2023-01-01",
        dry_run=True,
    )

    assert result.progress.written_count == 0
    assert not Path(settings.DB_PATH).exists()


def test_backfill_cli_dry_run_prints_operator_context_without_quality_pass(
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr(
        "bithumb_bot.historical_backfill.fetch_minute_candles",
        lambda *args, **kwargs: [_candle("2023-01-01T00:00:00")],
    )

    rc = main(
        [
            "backfill-candles",
            "--market",
            "KRW-BTC",
            "--interval",
            "1m",
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-01",
            "--dry-run",
        ]
    )
    out = capsys.readouterr().out

    assert rc == 0
    assert "mode=paper" in out
    assert f"db_path={settings.DB_PATH}" in out
    assert "dry_run=1" in out
    assert "env_loaded=" in out
    assert "coverage_status=INCOMPLETE" in out
    assert "dataset_quality status=NOT_EVALUATED_BY_BACKFILL" in out
    assert "next_action=run research-readiness --manifest <manifest> before research-backtest" in out
    assert "quality_gate_status=PASS" not in out
    assert not Path(settings.DB_PATH).exists()


def test_backfill_cli_non_dry_run_empty_response_exits_nonzero(
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.historical_backfill.fetch_minute_candles", lambda *args, **kwargs: [])

    rc = main(
        [
            "backfill-candles",
            "--market",
            "KRW-BTC",
            "--interval",
            "1m",
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-01",
        ]
    )
    out = capsys.readouterr().out

    assert rc == 1
    assert "status=COMPLETE reason=no_older_candles" in out
    assert "coverage_status=INCOMPLETE" in out
    assert "reason=coverage_incomplete_after_backfill" in out
    assert "dataset_quality status=NOT_EVALUATED_BY_BACKFILL" in out
    assert "quality_gate_status=PASS" not in out


def test_backfill_cli_non_dry_run_range_covered_but_missing_buckets_exits_nonzero(
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr(
        "bithumb_bot.historical_backfill.fetch_minute_candles",
        lambda *args, **kwargs: [_candle("2023-01-01T00:00:00")],
    )

    rc = main(
        [
            "backfill-candles",
            "--market",
            "KRW-BTC",
            "--interval",
            "1m",
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-01",
        ]
    )
    out = capsys.readouterr().out

    assert rc == 1
    assert "status=COMPLETE reason=range_covered" in out
    assert "coverage_status=INCOMPLETE" in out
    assert "reason=coverage_incomplete_after_backfill" in out
    assert "dataset_quality status=NOT_EVALUATED_BY_BACKFILL" in out


def test_backfill_cli_dry_run_incomplete_coverage_can_exit_zero_but_prints_not_ready(
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr(
        "bithumb_bot.historical_backfill.fetch_minute_candles",
        lambda *args, **kwargs: [_candle("2023-01-01T00:00:00")],
    )

    rc = main(
        [
            "backfill-candles",
            "--market",
            "KRW-BTC",
            "--interval",
            "1m",
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-01",
            "--dry-run",
        ]
    )
    out = capsys.readouterr().out

    assert rc == 0
    assert "dry_run=1" in out
    assert "coverage_status=INCOMPLETE" in out
    assert "dataset_quality status=NOT_EVALUATED_BY_BACKFILL" in out
    assert "result=DRY_RUN_NOT_READY" in out
    assert "reason=coverage_incomplete_after_backfill" in out
    assert "quality_gate_status=PASS" not in out
    assert not Path(settings.DB_PATH).exists()


def test_backfill_cli_rejects_repo_local_db_path(
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    repo_local_db = Path.cwd() / "tmp" / "operator-paper.sqlite"
    object.__setattr__(settings, "DB_PATH", str(repo_local_db))

    rc = main(
        [
            "backfill-candles",
            "--market",
            "KRW-BTC",
            "--interval",
            "1m",
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-01",
            "--dry-run",
        ]
    )
    out = capsys.readouterr().out

    assert rc == 1
    assert "DB_PATH must be outside repository for backfill-candles" in out


def test_research_readiness_reports_missing_train_candles_and_top_of_book(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        """
        {
          "experiment_id": "readiness_unit",
          "hypothesis": "readiness should fail before research",
          "strategy_name": "sma_with_filter",
          "market": "KRW-BTC",
          "interval": "1m",
          "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "readiness_unit",
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "top_of_book": {
              "source": "sqlite_orderbook_top_snapshots",
              "required": true,
              "missing_policy": "fail",
              "min_coverage_pct": 100
            }
          },
          "parameter_space": {"SMA_SHORT": [1], "SMA_LONG": [2]},
          "execution_model": {
            "type": "fixed_bps",
            "fee_rate": 0.0,
            "slippage_bps": 0.0,
            "calibration_required": false,
            "calibration_strictness": "warn"
          },
          "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 99,
            "min_profit_factor": 0.1,
            "oos_return_must_be_positive": false,
            "parameter_stability_required": false,
            "final_holdout_required_for_promotion": false
          }
        }
        """,
        encoding="utf-8",
    )
    conn = ensure_db(settings.DB_PATH)
    conn.close()

    rc = main(["research-readiness", "--manifest", str(manifest_path)])
    out = capsys.readouterr().out

    assert rc == 1
    assert "split=train expected_candles=1440 present_candles=0 missing=1440" in out
    assert "quality_status=FAIL reasons=missing_candles" in out
    assert "top_of_book=required=1" in out
    assert "status=FAIL" in out
    assert "candle backfill does not satisfy production top-of-book requirements" in out


def _write_manifest(path: Path, *, calibration_required: bool = False, calibration_strictness: str = "warn") -> None:
    path.write_text(
        json.dumps(
            {
                "experiment_id": "readiness_unit",
                "hypothesis": "readiness should mirror research validation",
                "strategy_name": "sma_with_filter",
                "market": "KRW-BTC",
                "interval": "1m",
                "dataset": {
                    "source": "sqlite_candles",
                    "snapshot_id": "readiness_unit",
                    "train": {"start": "2023-01-01", "end": "2023-01-01"},
                    "validation": {"start": "2023-01-02", "end": "2023-01-02"},
                },
                "parameter_space": {"SMA_SHORT": [1], "SMA_LONG": [2]},
                "execution_model": {
                    "type": "fixed_bps",
                    "fee_rate": 0.0,
                    "slippage_bps": 10.0,
                    "latency_ms": 3000,
                    "partial_fill_rate": 0.0,
                    "order_failure_rate": 0.0,
                    "calibration_required": calibration_required,
                    "calibration_strictness": calibration_strictness,
                },
                "acceptance_gate": {
                    "min_trade_count": 1,
                    "max_mdd_pct": 99,
                    "min_profit_factor": 0.1,
                    "oos_return_must_be_positive": False,
                    "parameter_stability_required": False,
                    "final_holdout_required_for_promotion": False,
                },
            }
        ),
        encoding="utf-8",
    )


def _insert_day_candles(db_path: str, day_start_ts: int) -> None:
    conn = ensure_db(db_path)
    try:
        rows = [
            (
                day_start_ts + index * 60_000,
                "KRW-BTC",
                "1m",
                100.0,
                101.0,
                99.0,
                100.5,
                1.0,
            )
            for index in range(1440)
        ]
        conn.executemany(
            """
            INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def test_research_readiness_json_reports_operator_fields_and_top_of_book_policy(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    _insert_day_candles(settings.DB_PATH, 1_672_531_200_000)
    _insert_day_candles(settings.DB_PATH, 1_672_617_600_000)

    rc = main(["research-readiness", "--manifest", str(manifest_path), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["mode"] == "paper"
    assert payload["db_path"] == str(Path(settings.DB_PATH).resolve())
    assert {"train", "validation"} <= set(payload["splits"])
    assert payload["top_of_book"]["status"] == "NOT_REQUESTED"
    assert payload["splits"]["train"]["top_of_book_missing_policy"] is None
    assert payload["execution_calibration"]["status"] == "WARN"
    assert payload["walk_forward"]["status"] == "NOT_REQUIRED"
    assert payload["next_actions"] == ["none"]


def test_research_readiness_calibration_min_sample_matches_research_validation_policy(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path, calibration_required=False, calibration_strictness="warn")
    _insert_day_candles(settings.DB_PATH, 1_672_531_200_000)
    _insert_day_candles(settings.DB_PATH, 1_672_617_600_000)
    artifact = build_calibration_artifact(
        summary={
            "sample_count": 1,
            "p90_slippage_vs_signal_bps": 2.0,
            "p95_slippage_vs_signal_bps": 3.0,
            "p95_submit_to_fill_ms": 100,
            "partial_fill_rate": 0.0,
            "unfilled_rate": 0.0,
            "model_breach_rate": 0.0,
            "quality_gate_status": "PASS",
        },
        market="KRW-BTC",
        interval="1m",
    )
    calibration_path = tmp_path / "calibration.json"
    calibration_path.write_text(json.dumps(artifact), encoding="utf-8")

    rc = main(
        [
            "research-readiness",
            "--manifest",
            str(manifest_path),
            "--execution-calibration",
            str(calibration_path),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    calibration = payload["execution_calibration"]
    assert calibration["status"] == "WARN"
    assert calibration["min_sample_count"] == 30
    assert "execution_calibration_sample_count_below_required" in calibration["reasons"]
    assert calibration["scenario_gates"][0]["min_sample_count"] == 30
