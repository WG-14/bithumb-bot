from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from bithumb_bot.app import main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.historical_backfill import backfill_candles
from bithumb_bot.public_api_minute_candles import MinuteCandle
from bithumb_bot.research.data_plane import (
    build_persistent_missing_candle_classification_artifact,
    build_dataset_quality_report_sql,
    retry_missing_candles_from_artifact,
    write_persistent_missing_candle_classification_artifact,
    write_missing_candle_ranges_artifact,
)
from bithumb_bot.research.execution_calibration import build_calibration_artifact
from bithumb_bot.research.experiment_manifest import load_manifest
from bithumb_bot.research.hashing import sha256_prefixed


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
    kst: str | None = None,
    close: float = 100.0,
    timestamp: int = 1_111_111_199_999,
    market: str = "KRW-BTC",
) -> MinuteCandle:
    return MinuteCandle(
        market=market,
        candle_date_time_utc=utc,
        candle_date_time_kst=kst or _utc_to_kst_naive(utc),
        opening_price=close,
        high_price=close + 1.0,
        low_price=close - 1.0,
        trade_price=close,
        timestamp=timestamp,
        candle_acc_trade_price=10_000.0,
        candle_acc_trade_volume=1.0,
    )


def _utc_to_kst_naive(value: str) -> str:
    return (
        datetime.fromisoformat(value)
        .replace(tzinfo=UTC)
        .astimezone(UTC)
        .replace(tzinfo=UTC)
        + timedelta(hours=9)
    ).replace(tzinfo=None).isoformat(timespec="seconds")


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

    assert calls[0] == "2023-01-03T09:00:00"
    assert calls[1] == "2023-01-02T09:00:00"


def test_backfill_next_cursor_uses_oldest_candle_kst_not_utc(monkeypatch, _settings_guard) -> None:
    calls: list[str | None] = []
    pages = [
        [
            _candle(
                "2023-01-01T23:29:00",
                kst="2023-01-02T08:29:00",
            ),
            _candle(
                "2023-01-01T23:28:00",
                kst="2023-01-02T08:28:00",
            ),
        ],
        [],
    ]

    def fake_fetch(client, *, market: str, minute_unit: int, count: int, to: str | None = None, max_retries=None):
        calls.append(to)
        return pages.pop(0)

    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.historical_backfill.fetch_minute_candles", fake_fetch)

    backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-02")

    assert calls[1] == "2023-01-02T08:28:00"
    assert calls[1] != "2023-01-01T23:28:00"


def test_backfill_no_synthetic_541_minute_gap_between_pages(monkeypatch, _settings_guard) -> None:
    calls: list[str | None] = []
    pages = [
        [
            _candle("2023-01-01T23:29:00", kst="2023-01-02T08:29:00"),
            _candle("2023-01-01T23:28:00", kst="2023-01-02T08:28:00"),
        ],
        [
            _candle("2023-01-01T23:27:00", kst="2023-01-02T08:27:00"),
            _candle("2023-01-01T23:26:00", kst="2023-01-02T08:26:00"),
        ],
        [],
    ]

    def fake_fetch(client, *, market: str, minute_unit: int, count: int, to: str | None = None, max_retries=None):
        calls.append(to)
        return pages.pop(0)

    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.historical_backfill.fetch_minute_candles", fake_fetch)

    result = backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-02")

    assert calls[1] == "2023-01-02T08:28:00"
    with sqlite3.connect(settings.DB_PATH) as conn:
        rows = [row[0] for row in conn.execute("SELECT ts FROM candles ORDER BY ts").fetchall()]
    assert rows == [1_672_615_560_000, 1_672_615_620_000, 1_672_615_680_000, 1_672_615_740_000]
    assert all((right - left) == 60_000 for left, right in zip(rows, rows[1:]))
    assert result.page_gap_summary["top_page_boundary_gaps"][0]["gap_minutes"] == 1


def test_backfill_fallback_cursor_uses_kst_local_time(monkeypatch, _settings_guard) -> None:
    calls: list[str | None] = []
    first_page = [
        _candle("2023-01-01T23:29:00", kst="2023-01-02T08:29:00"),
        _candle("2023-01-01T23:28:00", kst="2023-01-02T08:28:00"),
    ]
    pages = [
        first_page,
        first_page,
        [],
    ]

    def fake_fetch(client, *, market: str, minute_unit: int, count: int, to: str | None = None, max_retries=None):
        calls.append(to)
        return pages.pop(0)

    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr("bithumb_bot.historical_backfill.fetch_minute_candles", fake_fetch)

    backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-02")

    assert calls[2] == "2023-01-02T08:27:00"


def test_backfill_still_stores_utc_bucket_timestamp(monkeypatch, _settings_guard) -> None:
    monkeypatch.setattr("bithumb_bot.historical_backfill.httpx.Client", lambda *args, **kwargs: _DummyClient())
    monkeypatch.setattr(
        "bithumb_bot.historical_backfill.fetch_minute_candles",
        lambda *args, **kwargs: [
            _candle(
                "2023-01-01T23:28:00",
                kst="2023-01-02T08:28:00",
                timestamp=9_999_999_999_999,
            )
        ],
    )

    backfill_candles(market="KRW-BTC", interval="1m", start="2023-01-01", end="2023-01-02")

    with sqlite3.connect(settings.DB_PATH) as conn:
        rows = conn.execute("SELECT ts FROM candles").fetchall()

    assert rows == [(1_672_615_680_000,)]


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
    assert calls[2] == "2023-01-01T08:59:00"
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
                    "label": "readiness_realistic_bithumb_app_fee_0004",
                    "fee_rate": 0.0004,
                    "fee_source": "operator_declared_bithumb_app_fee",
                    "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
                    "slippage_bps": 10.0,
                    "slippage_source": "execution_calibration",
                    "promotable_as_base": True,
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
                "statistical_validation": {
                    "required_for_promotion": True,
                    "benchmark": "cash",
                    "primary_metric": "net_excess_return",
                    "selection_universe": "all_parameter_candidates_all_required_scenarios",
                    "multiple_testing_scope": "experiment_family",
                    "bootstrap": {
                        "method": "metric_centered_max_bootstrap",
                        "n_bootstrap": 100,
                        "block_length_policy": "not_applicable_summary_metric",
                        "seed_policy": "derived_from_selection_universe_hash",
                    },
                    "gates": {
                        "max_reality_check_p_value": 0.05,
                        "max_spa_p_value": None,
                        "min_deflated_sharpe_probability": None,
                        "max_holdout_reuse_count": 0,
                        "max_attempt_index_without_new_hypothesis": 1,
                    },
                },
                "stress_suite": {
                    "required_for_promotion": True,
                    "trade_removal": {
                        "top_n_by_net_pnl": [1],
                        "min_return_retention_pct": 50.0,
                    },
                    "trade_order_monte_carlo": {
                        "iterations": 100,
                        "seed_policy": "derived_from_manifest_candidate_scenario_split_hash",
                        "min_survival_probability": 0.95,
                        "ruin_max_drawdown_pct": 35.0,
                        "min_closed_trades": 3,
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def _safe_production_execution_timing() -> dict[str, object]:
    return {
        "fill_reference_policy": "next_candle_open",
        "allow_same_candle_close_fill": False,
        "min_execution_reality_level_for_promotion": "candle_next_open",
    }


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


def _insert_day_candles_except(db_path: str, day_start_ts: int, excluded_ts: set[int]) -> None:
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
            if day_start_ts + index * 60_000 not in excluded_ts
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


def _rewrite_artifact(path: Path, mutate) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    mutate(payload)
    payload.pop("content_hash", None)
    payload["content_hash"] = sha256_prefixed(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def _remove_content_hash(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.pop("content_hash", None)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _mutate_artifact_without_rehash(path: Path, mutate) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    mutate(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def _write_valid_persistent_missing_classification(tmp_path: Path) -> tuple[Path, Path]:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )
    out_classification = tmp_path / "classification.json"
    write_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        out_path=out_classification,
        generated_at="2026-05-12T00:00:00+00:00",
    )
    return manifest_path, out_classification


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


def test_research_readiness_sql_scan_does_not_materialize_dataset_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    _insert_day_candles(settings.DB_PATH, 1_672_531_200_000)
    _insert_day_candles(settings.DB_PATH, 1_672_617_600_000)
    monkeypatch.setattr(
        "bithumb_bot.research.dataset_snapshot.load_dataset_split",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("snapshot materialization is forbidden")),
    )

    rc = main(["research-readiness", "--manifest", str(manifest_path), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["splits"]["train"]["scan_method"] == "sqlite_streaming"
    assert payload["splits"]["train"]["missing_count"] == 0


def test_sql_readiness_missing_count_matches_small_fixture_quality_path(tmp_path: Path, _settings_guard) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    _insert_day_candles(settings.DB_PATH, 1_672_531_200_000)
    with sqlite3.connect(settings.DB_PATH) as conn:
        conn.execute(
            "DELETE FROM candles WHERE pair='KRW-BTC' AND interval='1m' AND ts IN (?, ?)",
            (1_672_531_200_000 + 10 * 60_000, 1_672_531_200_000 + 11 * 60_000),
        )
    manifest = load_manifest(manifest_path)

    sql_report = build_dataset_quality_report_sql(
        db_path=settings.DB_PATH,
        manifest=manifest,
        split_name="train",
    ).payload

    from bithumb_bot.research.dataset_snapshot import build_dataset_quality_report, load_dataset_split

    snapshot = load_dataset_split(db_path=settings.DB_PATH, manifest=manifest, split_name="train")
    fixture_report = build_dataset_quality_report(db_path=settings.DB_PATH, snapshot=snapshot).payload
    assert sql_report["missing_bucket_count"] == fixture_report["missing_bucket_count"] == 2
    assert sql_report["present_expected_bucket_count"] == fixture_report["present_expected_bucket_count"]


def test_dataset_quality_sql_scan_includes_top_of_book_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw["dataset"]["top_of_book"] = {
        "source": "sqlite_orderbook_top_snapshots",
        "required": True,
        "missing_policy": "fail",
        "min_coverage_pct": 100,
    }
    manifest_path.write_text(json.dumps(raw), encoding="utf-8")
    _insert_day_candles(settings.DB_PATH, 1_672_531_200_000)
    manifest = load_manifest(manifest_path)

    def fake_top_of_book(**kwargs):
        return {
            "top_of_book_requested": True,
            "top_of_book_gate_status": "FAIL",
            "top_of_book_gate_reasons": ["test_top_of_book_default_called"],
        }

    monkeypatch.setattr("bithumb_bot.research.data_plane._top_of_book_split_sql", fake_top_of_book)

    report = build_dataset_quality_report_sql(
        db_path=settings.DB_PATH,
        manifest=manifest,
        split_name="train",
    ).payload

    assert report["top_of_book_requested"] is True
    assert "test_top_of_book_default_called" in report["quality_gate_reasons"]


def test_required_top_of_book_zero_rows_fails_without_per_candle_quote_loader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path, calibration_required=True)
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw["deployment_tier"] = "paper_candidate"
    raw["execution_timing"] = _safe_production_execution_timing()
    raw["dataset"]["top_of_book"] = {
        "source": "sqlite_orderbook_top_snapshots",
        "required": True,
        "missing_policy": "fail",
        "min_coverage_pct": 100,
    }
    manifest_path.write_text(json.dumps(raw), encoding="utf-8")
    _insert_day_candles(settings.DB_PATH, 1_672_531_200_000)
    _insert_day_candles(settings.DB_PATH, 1_672_617_600_000)
    ensure_db(settings.DB_PATH).close()
    monkeypatch.setattr(
        "bithumb_bot.research.dataset_snapshot._load_top_of_book_quotes",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("per-candle top-of-book loop is forbidden")),
    )

    rc = main(["research-readiness", "--manifest", str(manifest_path), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert payload["top_of_book"]["status"] == "FAIL"
    assert "top_of_book_rows_missing" in payload["top_of_book"]["reasons"]


def test_missing_candle_artifact_does_not_call_top_of_book_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw["dataset"]["top_of_book"] = {
        "source": "sqlite_orderbook_top_snapshots",
        "required": True,
        "missing_policy": "fail",
        "min_coverage_pct": 100,
    }
    manifest_path.write_text(json.dumps(raw), encoding="utf-8")
    _insert_day_candles(settings.DB_PATH, 1_672_617_600_000)
    out = tmp_path / "missing_ranges.json"
    monkeypatch.setattr(
        "bithumb_bot.research.data_plane._top_of_book_split_sql",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("top-of-book scan must not run")),
    )

    payload = write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out)

    train = payload["splits"]["train"]
    first = train["ranges"][0]
    assert payload["artifact_type"] == "missing_candle_ranges"
    assert payload["manifest_hash"].startswith("sha256:")
    assert payload["db_path"] == str(Path(settings.DB_PATH).resolve())
    assert first["split"] == "train"
    assert first["start_ts"] == 1_672_531_200_000
    assert first["end_ts"] == 1_672_617_540_000
    assert first["start_utc"].startswith("2023-01-01T00:00:00")
    assert first["start_kst"].startswith("2023-01-01T09:00:00")
    assert first["bucket_count"] == 1440
    assert first["retry_utc_days"] == ["2023-01-01"]
    assert first["classification"] == "untried_missing"
    assert "top_of_book" not in json.dumps(payload)


def test_missing_candle_artifact_contains_hash_paths_timezone_and_retry_plan(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    _insert_day_candles(settings.DB_PATH, 1_672_617_600_000)
    out = tmp_path / "missing_ranges.json"

    payload = write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out)

    train = payload["splits"]["train"]
    first = train["ranges"][0]
    assert out.exists()
    assert payload["manifest_hash"].startswith("sha256:")
    assert payload["db_path"] == str(Path(settings.DB_PATH).resolve())
    assert first["split"] == "train"
    assert first["start_ts"] == 1_672_531_200_000
    assert first["end_ts"] == 1_672_617_540_000
    assert first["start_utc"].startswith("2023-01-01T00:00:00")
    assert first["start_kst"].startswith("2023-01-01T09:00:00")
    assert first["bucket_count"] == 1440
    assert first["retry_utc_days"] == ["2023-01-01"]
    assert first["classification"] == "untried_missing"


def test_retry_missing_marks_persistent_when_coverage_remains_incomplete(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"

    payload = retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )

    assert out_retry.exists()
    assert payload["attempts"][0]["classification"] == "retry_persistent_missing"
    assert payload["attempts"][0]["after"]["missing_buckets"] > 0


def test_retry_missing_marks_recovered_when_backfill_restores_range(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    _insert_day_candles(settings.DB_PATH, 1_672_531_200_000)
    with sqlite3.connect(settings.DB_PATH) as conn:
        conn.execute("DELETE FROM candles WHERE ts=?", (1_672_531_200_000 + 42 * 60_000,))
        conn.commit()
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"

    def fake_backfill(**kwargs):
        with sqlite3.connect(settings.DB_PATH) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, 'KRW-BTC', '1m', 100.0, 101.0, 99.0, 100.0, 1.0)
                """,
                (1_672_531_200_000 + 42 * 60_000,),
            )
            conn.commit()
        return type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )()

    payload = retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1,
        max_attempts=1,
        split="train",
        limit=1,
        out_path=out_retry,
        backfill_func=fake_backfill,
    )

    assert payload["attempts"][0]["classification"] == "retried_recovered"
    assert payload["attempts"][0]["after"]["missing_buckets"] == 0


def test_retry_missing_records_backfill_exception_evidence_for_classification(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"

    def failing_backfill(**kwargs):
        raise RuntimeError("public api transient failure after retries status=503")

    payload = retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=failing_backfill,
    )

    assert out_retry.exists()
    assert payload["artifact_type"] == "missing_candle_retry_attempts"
    assert payload["content_hash"] == sha256_prefixed({key: value for key, value in payload.items() if key != "content_hash"})
    first = payload["attempts"][0]
    assert first["classification"] == "retry_persistent_missing"
    assert first["after"]["missing_buckets"] > 0
    attempt = first["backfill_attempts"][0]
    assert attempt["progress_status"] == "ERROR"
    assert attempt["progress_reason"] == "backfill_exception"
    assert attempt["error_class"] == "RuntimeError"
    assert "status=503" in attempt["error_message"]
    assert attempt["api_unavailable_evidence"] is True
    assert attempt["coverage"] is None

    classification = build_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        generated_at="2026-05-12T00:00:00+00:00",
    )

    classified_range = classification["ranges"][0]
    assert classified_range["classification"] == "api_unavailable_candidate"
    assert classification["summary"]["api_unavailable_candidate"] == 1
    assert classification["summary"]["production_gate_effect"] == "none"
    assert classified_range["gate_effect"] == "none"
    assert any(item["type"] == "api_unavailable_signal" for item in classified_range["evidence"])
    refs = next(item["evidence_refs"] for item in classified_range["evidence"] if item["type"] == "api_unavailable_signal")
    assert any("error_message=" in ref or "api_unavailable_evidence=True" in ref for ref in refs)


def test_persistent_missing_classification_artifact_binds_manifest_db_retry_and_counts(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    missing_ts = 1_672_531_200_000 + 42 * 60_000
    _insert_day_candles_except(settings.DB_PATH, 1_672_531_200_000, {missing_ts})
    _insert_day_candles(settings.DB_PATH, 1_672_617_600_000)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_payload = retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1,
        max_attempts=1,
        split="train",
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )
    out_classification = tmp_path / "classification.json"

    payload = write_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        out_path=out_classification,
        generated_at="2026-05-12T00:00:00+00:00",
    )

    assert out_classification.exists()
    assert payload["artifact_type"] == "persistent_missing_candle_classification"
    assert payload["schema_version"] == 1
    assert payload["manifest_hash"] == load_manifest(manifest_path).manifest_hash()
    assert payload["missing_ranges_hash"].startswith("sha256:")
    assert payload["retry_attempts_hash"] == retry_payload["content_hash"]
    assert payload["db_path"] == str(Path(settings.DB_PATH).resolve())
    assert payload["market"] == "KRW-BTC"
    assert payload["interval"] == "1m"
    assert payload["policy_effect"] == "diagnostic_only_no_gate_relaxation"
    assert payload["summary"]["exchange_gap_candidate"] == 1
    assert payload["summary"]["persistent_range_count"] == 1
    assert payload["summary"]["production_gate_effect"] == "none"
    assert payload["ranges"][0]["gate_effect"] == "none"
    assert payload["limitations"]["synthetic_ohlcv_authorized"] is False
    assert payload["limitations"]["production_gate_relaxed"] is False
    assert payload["content_hash"].startswith("sha256:")


def test_persistent_missing_classification_excludes_recovered_ranges(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    _insert_day_candles(settings.DB_PATH, 1_672_531_200_000)
    with sqlite3.connect(settings.DB_PATH) as conn:
        conn.execute("DELETE FROM candles WHERE ts=?", (1_672_531_200_000 + 7 * 60_000,))
        conn.commit()
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"

    def fake_backfill(**kwargs):
        with sqlite3.connect(settings.DB_PATH) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, 'KRW-BTC', '1m', 100.0, 101.0, 99.0, 100.0, 1.0)
                """,
                (1_672_531_200_000 + 7 * 60_000,),
            )
            conn.commit()
        return type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )()

    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1,
        max_attempts=1,
        split="train",
        limit=1,
        out_path=out_retry,
        backfill_func=fake_backfill,
    )

    payload = build_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        generated_at="2026-05-12T00:00:00+00:00",
    )

    assert payload["ranges"] == []
    assert payload["summary"]["persistent_range_count"] == 0
    assert payload["summary"]["classified_range_count"] == 0


def test_persistent_missing_classification_api_unavailable_preserves_evidence(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "INCOMPLETE", "reason": "retry_exhausted"})(), "coverage": {}},
        )(),
    )
    _rewrite_artifact(
        out_retry,
        lambda payload: payload["attempts"][0].update(
            {
                "probe_evidence": {
                    "error_class": "PublicApiTransientError",
                    "http_status": 503,
                    "endpoint": "/v1/candles/minutes/1",
                    "params_masked": {"market": "KRW-BTC"},
                }
            }
        ),
    )

    payload = build_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        generated_at="2026-05-12T00:00:00+00:00",
    )

    first = payload["ranges"][0]
    assert first["classification"] == "api_unavailable_candidate"
    assert payload["summary"]["api_unavailable_candidate"] == 1
    assert any(item["type"] == "optional_probe_evidence" for item in first["evidence"])
    assert any(item["type"] == "api_unavailable_signal" for item in first["evidence"])


def test_persistent_missing_classification_falls_back_to_unclassified_when_evidence_is_insufficient(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )

    payload = build_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        generated_at="2026-05-12T00:00:00+00:00",
    )

    assert payload["ranges"][0]["classification"] == "unclassified_missing"
    assert payload["summary"]["unclassified_missing"] == 1


def test_missing_ranges_content_hash_mismatch_fails_retry_and_classification_cli(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )
    _mutate_artifact_without_rehash(
        out_missing,
        lambda p: p["splits"]["train"]["ranges"][0].update({"bucket_count": 999}),
    )

    with pytest.raises(ValueError, match="missing ranges content_hash does not match artifact body"):
        retry_missing_candles_from_artifact(
            manifest_path=manifest_path,
            missing_ranges_path=out_missing,
            min_buckets=1,
            max_attempts=1,
            limit=1,
            out_path=tmp_path / "retry.json",
            backfill_func=lambda **kwargs: None,
        )

    rc = main(
        [
            "classify-persistent-missing-candles",
            "--manifest",
            str(manifest_path),
            "--missing-ranges",
            str(out_missing),
            "--retry-attempts",
            str(out_retry),
            "--out",
            str(tmp_path / "classification.json"),
        ]
    )
    out = capsys.readouterr().out
    assert rc == 1
    assert "missing ranges content_hash does not match artifact body" in out


def test_missing_ranges_content_hash_required_for_retry_and_classification_cli(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )
    _remove_content_hash(out_missing)

    with pytest.raises(ValueError, match="missing ranges content_hash is required"):
        retry_missing_candles_from_artifact(
            manifest_path=manifest_path,
            missing_ranges_path=out_missing,
            min_buckets=1,
            max_attempts=1,
            limit=1,
            out_path=tmp_path / "retry.json",
            backfill_func=lambda **kwargs: None,
        )

    rc = main(
        [
            "classify-persistent-missing-candles",
            "--manifest",
            str(manifest_path),
            "--missing-ranges",
            str(out_missing),
            "--retry-attempts",
            str(out_retry),
            "--out",
            str(tmp_path / "classification.json"),
        ]
    )
    out = capsys.readouterr().out
    assert rc == 1
    assert "missing ranges content_hash is required" in out


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda missing_path, retry_path: _rewrite_artifact(missing_path, lambda p: p.update({"manifest_hash": "sha256:bad"})), "manifest_hash"),
        (lambda missing_path, retry_path: _rewrite_artifact(missing_path, lambda p: p.update({"market": "KRW-ETH"})), "market/interval"),
        (lambda missing_path, retry_path: _rewrite_artifact(missing_path, lambda p: p.update({"db_path": "/tmp/wrong.sqlite"})), "db_path"),
        (lambda missing_path, retry_path: _rewrite_artifact(retry_path, lambda p: p.update({"schema_version": 999})), "schema_version"),
        (lambda missing_path, retry_path: _remove_content_hash(retry_path), "content_hash"),
        (lambda missing_path, retry_path: _rewrite_artifact(retry_path, lambda p: p.update({"missing_ranges_hash": "sha256:bad"})), "missing_ranges_hash"),
    ],
)
def test_persistent_missing_classification_fails_closed_on_mismatched_lineage(
    tmp_path: Path,
    _settings_guard,
    mutation,
    match: str,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )
    mutation(out_missing, out_retry)

    with pytest.raises(ValueError, match=match):
        build_persistent_missing_candle_classification_artifact(
            manifest_path=manifest_path,
            missing_ranges_path=out_missing,
            retry_attempts_path=out_retry,
            generated_at="2026-05-12T00:00:00+00:00",
        )


def test_persistent_missing_classification_hash_is_deterministic_and_changes_with_evidence(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )

    first = build_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        generated_at="2026-05-12T00:00:00+00:00",
    )
    second = build_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        generated_at="2026-05-12T00:00:00+00:00",
    )
    assert first["content_hash"] == second["content_hash"]

    _rewrite_artifact(out_retry, lambda p: p["attempts"][0].update({"probe_evidence": {"http_status": 503}}))
    changed = build_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        generated_at="2026-05-12T00:00:00+00:00",
    )
    assert changed["content_hash"] != first["content_hash"]
    assert changed["ranges"][0]["classification"] == "api_unavailable_candidate"


def test_missing_range_kst_early_morning_maps_to_previous_utc_retry_day(
    tmp_path: Path,
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw["dataset"]["train"] = {"start": "2026-04-26", "end": "2026-04-26"}
    raw["dataset"]["validation"] = {"start": "2026-04-27", "end": "2026-04-27"}
    manifest_path.write_text(json.dumps(raw), encoding="utf-8")
    day_start = int(datetime(2026, 4, 26, tzinfo=UTC).timestamp() * 1000)
    missing_start = int(datetime(2026, 4, 26, 16, 1, tzinfo=UTC).timestamp() * 1000)
    missing_end = int(datetime(2026, 4, 26, 20, 29, tzinfo=UTC).timestamp() * 1000)
    conn = ensure_db(settings.DB_PATH)
    try:
        for minute in range(1440):
            ts = day_start + minute * 60_000
            if missing_start <= ts <= missing_end:
                continue
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, 'KRW-BTC', '1m', 100.0, 101.0, 99.0, 100.0, 1.0)
                """,
                (ts,),
            )
        conn.commit()
    finally:
        conn.close()

    payload = write_missing_candle_ranges_artifact(
        manifest_path=manifest_path,
        out_path=tmp_path / "missing_kst.json",
    )
    first = payload["splits"]["train"]["ranges"][0]

    assert first["start_kst"].startswith("2026-04-27T01:01:00")
    assert first["end_kst"].startswith("2026-04-27T05:29:00")
    assert first["retry_utc_days"] == ["2026-04-26"]


def test_research_readiness_fail_closed_reports_separate_production_gate_reasons(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path, calibration_required=True)
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw["deployment_tier"] = "paper_candidate"
    raw["execution_timing"] = _safe_production_execution_timing()
    raw["dataset"]["top_of_book"] = {
        "source": "sqlite_orderbook_top_snapshots",
        "required": True,
        "missing_policy": "fail",
        "min_coverage_pct": 100,
    }
    manifest_path.write_text(json.dumps(raw), encoding="utf-8")

    rc = main(["research-readiness", "--manifest", str(manifest_path), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert payload["readiness_mode"]["readiness_type"] == "production_readiness"
    assert payload["splits"]["train"]["quality_status"] == "FAIL"
    assert "missing_candles" in payload["splits"]["train"]["quality_reasons"]
    assert payload["top_of_book"]["status"] == "FAIL"
    assert payload["execution_calibration"]["status"] == "FAIL"
    assert "execution_calibration_missing" in payload["execution_calibration"]["reasons"]


def test_research_only_candle_diagnostic_is_explicitly_not_production_readiness(
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
    assert payload["readiness_mode"]["readiness_type"] == "research_only_diagnostic"
    assert payload["readiness_mode"]["production_bound"] is False
    assert payload["readiness_mode"]["candle_only_diagnostic"] is True
    assert payload["top_of_book"]["status"] == "NOT_REQUESTED"


def test_diagnostic_only_policy_is_report_metadata_not_gate_relaxation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw["deployment_tier"] = "paper_candidate"
    raw["execution_timing"] = _safe_production_execution_timing()
    raw["dataset_quality_policy"] = {
        "dense_candles_required": False,
        "missing_candle_policy": "diagnostic_only",
        "allow_classified_no_trade_missing": False,
        "require_retry_attempts_for_missing_ranges": True,
        "max_unclassified_missing_buckets": 1440,
    }
    manifest_path.write_text(json.dumps(raw), encoding="utf-8")

    rc = main(["research-readiness", "--manifest", str(manifest_path), "--json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert payload["readiness_mode"]["production_bound"] is True
    assert payload["dataset_quality_policy"]["missing_candle_policy"] == "diagnostic_only"
    assert payload["dataset_quality_policy"]["readiness_gate_effect"] == "metadata_only_no_gate_relaxation"
    assert payload["dataset_quality_policy"]["synthetic_candle_authority"] == "not_allowed"
    assert payload["splits"]["train"]["quality_status"] == "FAIL"
    assert "missing_candles" in payload["splits"]["train"]["quality_reasons"]
    assert payload["status"] == "FAIL"


def test_research_readiness_includes_missing_classification_without_relaxing_gates(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )
    out_classification = tmp_path / "classification.json"
    write_persistent_missing_candle_classification_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        retry_attempts_path=out_retry,
        out_path=out_classification,
        generated_at="2026-05-12T00:00:00+00:00",
    )

    rc = main(
        [
            "research-readiness",
            "--manifest",
            str(manifest_path),
            "--missing-classification",
            str(out_classification),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    assert payload["status"] == "FAIL"
    assert payload["splits"]["train"]["quality_status"] == "FAIL"
    assert "missing_candles" in payload["splits"]["train"]["quality_reasons"]
    section = payload["persistent_missing_classification"]
    assert section["provided"] is True
    assert section["status"] == "DIAGNOSTIC_ONLY"
    assert section["production_gate_effect"] == "none"
    assert section["summary"]["unclassified_missing"] == 1
    assert "PASS" not in section["status"]


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            lambda p: p["summary"].update({"api_unavailable_candidate": 99}),
            "summary count mismatch",
        ),
        (
            lambda p: p["ranges"][0].update({"gate_effect": "relaxed"}),
            "gate_effect must be none",
        ),
        (
            lambda p: p["ranges"][0].update({"classification": "pass_missing"}),
            "unsupported classification",
        ),
        (
            lambda p: p["limitations"].update(
                {"top_of_book_satisfied": True, "execution_calibration_satisfied": True}
            ),
            "top-of-book",
        ),
        (
            lambda p: p["limitations"].update({"execution_calibration_satisfied": True}),
            "execution calibration",
        ),
        (
            lambda p: p["summary"].update({"production_gate_effect": "relaxed"}),
            "production_gate_effect must be none",
        ),
        (
            lambda p: p.update({"missing_ranges_hash": "not-a-hash"}),
            "missing_ranges_hash is required",
        ),
        (
            lambda p: p.update({"retry_attempts_hash": "not-a-hash"}),
            "retry_attempts_hash is required",
        ),
    ],
)
def test_research_readiness_rejects_semantically_unsafe_missing_classification(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
    mutation,
    reason: str,
) -> None:
    manifest_path, out_classification = _write_valid_persistent_missing_classification(tmp_path)
    _rewrite_artifact(out_classification, mutation)

    rc = main(
        [
            "research-readiness",
            "--manifest",
            str(manifest_path),
            "--missing-classification",
            str(out_classification),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 1
    section = payload["persistent_missing_classification"]
    assert section["provided"] is True
    assert section["status"] == "FAIL"
    assert section["production_gate_effect"] == "none"
    assert any(reason in item for item in section["reasons"])
    assert payload["status"] == "FAIL"
    assert payload["splits"]["train"]["quality_status"] == "FAIL"


def test_classify_persistent_missing_cli_output_and_path_policy(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    _settings_guard,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    _write_manifest(manifest_path)
    out_missing = tmp_path / "missing.json"
    write_missing_candle_ranges_artifact(manifest_path=manifest_path, out_path=out_missing)
    out_retry = tmp_path / "retry.json"
    retry_missing_candles_from_artifact(
        manifest_path=manifest_path,
        missing_ranges_path=out_missing,
        min_buckets=1000,
        max_attempts=1,
        limit=1,
        out_path=out_retry,
        backfill_func=lambda **kwargs: type(
            "Result",
            (),
            {"progress": type("Progress", (), {"status": "COMPLETE", "reason": "range_covered"})(), "coverage": {}},
        )(),
    )

    repo_local_out = Path.cwd() / "classification.json"
    rc = main(
        [
            "classify-persistent-missing-candles",
            "--manifest",
            str(manifest_path),
            "--missing-ranges",
            str(out_missing),
            "--retry-attempts",
            str(out_retry),
            "--out",
            str(repo_local_out),
        ]
    )
    assert rc == 1
    assert "outside repository" in capsys.readouterr().out

    out_classification = tmp_path / "classification.json"
    rc = main(
        [
            "classify-persistent-missing-candles",
            "--manifest",
            str(manifest_path),
            "--missing-ranges",
            str(out_missing),
            "--retry-attempts",
            str(out_retry),
            "--out",
            str(out_classification),
        ]
    )
    out = capsys.readouterr().out

    assert rc == 0
    assert out_classification.exists()
    assert "out=" in out
    assert "artifact_hash=sha256:" in out
    assert "unclassified_missing=1" in out
    assert "production_gate_effect=none" in out
    assert "next_action=" in out
    assert "production ready" not in out.lower()
    assert "quality gate pass" not in out.lower()


def test_console_entrypoint_propagates_cli_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys
    from bithumb_bot.bootstrap import run_cli

    monkeypatch.setattr(sys, "argv", ["bithumb-bot", "research-readiness", "--manifest", "missing.json"])
    monkeypatch.setattr("bithumb_bot.bootstrap.bootstrap_argv", lambda argv: argv)
    monkeypatch.setattr("bithumb_bot.observability.configure_runtime_logging", lambda: None)
    monkeypatch.setattr("bithumb_bot.cli.main", lambda: 1)

    with pytest.raises(SystemExit) as exc:
        run_cli()

    assert exc.value.code == 1
