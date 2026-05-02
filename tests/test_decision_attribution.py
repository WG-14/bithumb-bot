from __future__ import annotations

import json
import sqlite3

from bithumb_bot.app import main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.decision_attribution import (
    build_decision_attribution_summary_from_db,
    decision_attribution_summary_json,
    normalize_decision_attribution_from_context,
    normalize_decision_attribution_from_row,
    summarize_decision_attributions,
)


def _memory_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE strategy_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_ts INTEGER NOT NULL,
            strategy_name TEXT NOT NULL,
            signal TEXT NOT NULL,
            reason TEXT NOT NULL,
            candle_ts INTEGER,
            market_price REAL,
            confidence REAL,
            context_json TEXT NOT NULL
        )
        """
    )
    return conn


def _insert_context(conn: sqlite3.Connection, *, signal: str = "HOLD", context: object = None) -> None:
    raw_context = context if isinstance(context, str) else json.dumps(context or {}, sort_keys=True)
    conn.execute(
        """
        INSERT INTO strategy_decisions(
            decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1_710_000_000_000, "sma_with_filter", signal, "fixture", 1_710_000_000_000, 100.0, None, raw_context),
    )


def test_buy_candidate_blocked_by_cost_filter_is_attributed_to_cost_and_edge() -> None:
    attribution = normalize_decision_attribution_from_context(
        {
            "raw_signal": "BUY",
            "final_signal": "HOLD",
            "decision_type": "BLOCKED_ENTRY",
            "base_reason": "sma golden cross",
            "entry_reason": "filtered entry: cost_edge",
            "entry_block_reason": "filtered entry: cost_edge",
            "blocked_by_cost_filter": True,
            "gap_ratio": 0.001,
            "filters": {"cost_edge": {"threshold": 0.003}},
            "signal_strength_label": "weak",
            "primary_block_layer": "strategy_filters",
            "primary_block_reason": "cost_edge",
        }
    )
    summary = summarize_decision_attributions([attribution])

    assert summary.candidate_funnel["raw_BUY"] == 1
    assert summary.candidate_funnel["final_BUY"] == 0
    assert summary.filter_ratios["blocked_by_cost_filter_ratio"] == 1.0
    assert summary.edge_stats["gap_lt_required_ratio"] == 1.0
    assert summary.interpretation["primary_issue"] in {
        "entry_edge_insufficient_or_cost_filter_strict",
        "gap_below_required_edge",
        "raw_signal_scarcity",
    }


def test_final_buy_with_no_submit_expectation_is_counted_as_submit_mismatch() -> None:
    summary = summarize_decision_attributions(
        [
            normalize_decision_attribution_from_context(
                {
                    "raw_signal": "BUY",
                    "final_signal": "BUY",
                    "decision_type": "BUY",
                    "entry_reason": "sma golden cross",
                    "submit_expected": False,
                    "execution_block_reason": "missing_order_rule_min_qty",
                }
            )
        ]
    )

    assert summary.candidate_funnel["final_BUY"] == 1
    assert summary.candidate_funnel["submit_expected_BUY"] == 0
    assert summary.submit_mismatch["final_BUY_submit_expected_false"] == 1
    assert summary.filter_ratios["blocked_by_order_rule_ratio"] == 1.0


def test_raw_buy_scarcity_interpretation_for_mostly_hold_decisions() -> None:
    rows = [
        normalize_decision_attribution_from_context(
            {"raw_signal": "HOLD", "final_signal": "HOLD", "decision_type": "HOLD"}
        )
        for _ in range(20)
    ]

    summary = summarize_decision_attributions(rows)

    assert summary.raw_signal_counts["HOLD"] == 20
    assert summary.interpretation["primary_issue"] in {"raw_buy_scarcity", "raw_signal_scarcity"}


def test_malformed_and_missing_context_rows_do_not_crash_and_are_counted() -> None:
    conn = _memory_conn()
    try:
        _insert_context(conn, signal="BUY", context="{bad json")
        conn.execute(
            """
            INSERT INTO strategy_decisions(
                decision_ts, strategy_name, signal, reason, candle_ts, market_price, confidence, context_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "sma_with_filter", "HOLD", "fixture", None, None, None, ""),
        )
        summary = build_decision_attribution_summary_from_db(conn, limit=10)
    finally:
        conn.close()

    assert summary.sample_count == 2
    assert summary.malformed_context_count == 1
    assert summary.context_missing_count == 1
    assert summary.raw_signal_counts["BUY"] == 1
    assert summary.raw_signal_counts["HOLD"] == 1


def test_legacy_partial_context_uses_unknown_defaults_and_keeps_signals() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT ? AS signal, ? AS context_json",
            ("HOLD", json.dumps({"raw_signal": "BUY", "final_signal": "HOLD"})),
        ).fetchone()
        attribution = normalize_decision_attribution_from_row(row)
    finally:
        conn.close()

    assert attribution.raw_signal == "BUY"
    assert attribution.final_signal == "HOLD"
    assert attribution.decision_type == "unknown"
    assert attribution.signal_strength_label == "unknown"
    assert attribution.submit_expected is None


def test_decision_attribution_json_output_shape_is_deterministic() -> None:
    summary = summarize_decision_attributions(
        [
            normalize_decision_attribution_from_context(
                {
                    "raw_signal": "BUY",
                    "final_signal": "BUY",
                    "decision_type": "BUY",
                    "entry_reason": "sma golden cross",
                    "submit_expected": True,
                    "signal_strength_label": "tradable",
                }
            )
        ]
    )

    payload = json.loads(decision_attribution_summary_json(summary))

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["sample_count"] == 1
    assert payload["raw_signal_counts"] == {"BUY": 1}
    assert payload["candidate_funnel"]["submit_expected_BUY"] == 1
    assert payload["malformed_context_count"] == 0
    assert payload["context_missing_count"] == 0


def test_decision_attribution_cli_json_reads_strategy_decisions_context(tmp_path, monkeypatch, capsys) -> None:
    db_path = str(tmp_path / "decision-attribution.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "INTERVAL", "1m")

    conn = ensure_db()
    try:
        _insert_context(
            conn,
            signal="HOLD",
            context={
                "pair": "KRW-BTC",
                "interval": "1m",
                "raw_signal": "BUY",
                "final_signal": "HOLD",
                "decision_type": "BLOCKED_ENTRY",
                "entry_reason": "filtered entry: cost_edge",
                "blocked_by_cost_filter": True,
                "gap_ratio": 0.001,
                "required_edge_ratio": 0.003,
            },
        )
        conn.commit()
    finally:
        conn.close()

    rc = main(["decision-attribution", "--limit", "5", "--pair", "KRW-BTC", "--interval", "1m", "--json"])
    out = capsys.readouterr().out
    payload = json.loads(out)

    assert rc == 0
    assert payload["sample_count"] == 1
    assert payload["candidate_funnel"]["raw_BUY"] == 1
    assert payload["filter_ratios"]["blocked_by_cost_filter_ratio"] == 1.0
