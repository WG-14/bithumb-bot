from __future__ import annotations

import json

from bithumb_bot.analytics_context import (
    classify_gap_bucket,
    classify_overextension_bucket,
    classify_signal_strength_bucket,
    classify_time_bucket,
    classify_volatility_bucket,
    normalize_analysis_context,
    normalize_analysis_context_from_decision_row,
    normalize_analysis_context_from_lifecycle_row,
)
from bithumb_bot.db_core import ensure_db, record_strategy_decision


def test_classify_bucket_functions_cover_expected_ranges() -> None:
    assert classify_time_bucket(ts_ms=1_710_000_000_000) in {"morning", "afternoon", "evening", "overnight"}
    assert classify_signal_strength_bucket(label="strong") == "strong"
    assert classify_signal_strength_bucket(label=None, score=0.6) == "medium"
    assert classify_gap_bucket(gap_ratio=0.0001) == "tiny"
    assert classify_gap_bucket(gap_ratio=0.002) == "medium"
    assert classify_volatility_bucket(volatility_ratio=0.0005) == "very_low"
    assert classify_volatility_bucket(volatility_ratio=0.01) == "high"
    assert classify_overextension_bucket(extension_ratio=0.005) == "normal"
    assert classify_overextension_bucket(extension_ratio=0.03) == "overextended"


def test_normalize_analysis_context_handles_missing_fields_safely() -> None:
    normalized = normalize_analysis_context(context=None, decision_ts=None, candle_ts=None)

    assert normalized["raw"]["gap_ratio"] is None
    assert normalized["raw"]["volatility_ratio"] is None
    assert normalized["raw"]["extension_ratio"] is None
    assert normalized["buckets"]["time_of_day"] == "unknown"
    assert normalized["buckets"]["signal_strength"] == "unknown"
    assert normalized["flags"]["is_overextended"] is False


def test_normalize_analysis_context_from_decision_row(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "analysis-decision.sqlite"))
    try:
        record_strategy_decision(
            conn,
            decision_ts=1_710_000_000_000,
            strategy_name="sma_with_filter",
            signal="HOLD",
            reason="filtered entry",
            candle_ts=1_709_999_940_000,
            market_price=100.0,
            context={
                "signal_strength_label": "weak",
                "features": {
                    "sma_gap_ratio": 0.0012,
                    "volatility_range_ratio": 0.0025,
                    "overextended_abs_return_ratio": 0.025,
                },
            },
        )
        conn.commit()
        row = conn.execute(
            "SELECT decision_ts, candle_ts, context_json FROM strategy_decisions ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    normalized = normalize_analysis_context_from_decision_row(row)
    assert normalized["raw"]["gap_ratio"] == 0.0012
    assert normalized["raw"]["volatility_ratio"] == 0.0025
    assert normalized["raw"]["extension_ratio"] == 0.025
    assert normalized["buckets"]["signal_strength"] == "weak"
    assert normalized["buckets"]["gap"] == "small"
    assert normalized["buckets"]["volatility"] == "low"
    assert normalized["buckets"]["overextension"] == "overextended"
    assert normalized["flags"]["is_overextended"] is True


def test_normalize_analysis_context_from_lifecycle_row_uses_entry_then_exit_context(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "analysis-lifecycle.sqlite"))
    try:
        row = conn.execute(
            """
            SELECT
                1 AS id,
                1710000000000 AS entry_ts,
                1710000600000 AS exit_ts
            """
        ).fetchone()
    finally:
        conn.close()

    assert row is not None

    entry_context = {
        "decision_ts": 1710000000000,
        "candle_ts": 1709999940000,
        "signal_strength_label": "medium",
        "market_observations": {"gap": 0.0035, "volatility": 0.0075, "extension": 0.005},
    }
    exit_context = {
        "decision_ts": 1710000600000,
        "signal_strength_label": "weak",
        "market_observations": {"gap": 0.0002, "volatility": 0.0009, "extension": 0.03},
    }

    normalized = normalize_analysis_context_from_lifecycle_row(
        row,
        entry_context_json=json.dumps(entry_context, ensure_ascii=False),
        exit_context_json=json.dumps(exit_context, ensure_ascii=False),
    )

    assert normalized["raw"]["gap_ratio"] == 0.0035
    assert normalized["buckets"]["gap"] == "large"
    assert normalized["buckets"]["volatility"] == "high"
    assert normalized["buckets"]["overextension"] == "normal"

    fallback = normalize_analysis_context_from_lifecycle_row(
        row,
        entry_context_json=None,
        exit_context_json=json.dumps(exit_context, ensure_ascii=False),
    )
    assert fallback["raw"]["gap_ratio"] == 0.0002
    assert fallback["buckets"]["overextension"] == "overextended"
