from __future__ import annotations

import json
import sqlite3

from bithumb_bot.app import main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.decision_contract import BLOCK_LAYER_PRIORITY
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
    assert summary.block_reason_counts["strategy_filters.cost_edge"] == 1
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


def test_canonical_all_block_reasons_drive_cost_filter_attribution() -> None:
    attribution = normalize_decision_attribution_from_context(
        {
            "raw_signal": "BUY",
            "final_signal": "HOLD",
            "all_block_reasons": ["strategy_filters.cost_edge"],
            "signal_flow": {
                "primary_block_layer": "strategy_filters",
                "primary_block_reason": "cost_edge",
            },
        }
    )
    summary = summarize_decision_attributions([attribution])

    assert attribution.blocked_by_cost_filter is True
    assert attribution.primary_block_layer == "strategy_filters"
    assert attribution.primary_block_reason == "cost_edge"
    assert attribution.all_block_reasons == ("strategy_filters.cost_edge",)
    assert summary.block_reason_counts["strategy_filters.cost_edge"] == 1
    assert summary.filter_ratios["blocked_by_cost_filter_ratio"] == 1.0


def test_strategy_filter_cost_edge_phrase_variant_is_cost_filter() -> None:
    attribution = normalize_decision_attribution_from_context(
        {
            "raw_signal": "BUY",
            "final_signal": "HOLD",
            "all_block_reasons": ["strategy_filters.filtered entry: cost_edge"],
        }
    )
    summary = summarize_decision_attributions([attribution])

    assert attribution.blocked_by_cost_filter is True
    assert summary.block_reason_counts["strategy_filters.filtered entry: cost_edge"] == 1
    assert summary.filter_ratios["blocked_by_cost_filter_ratio"] == 1.0


def test_strategy_filter_cost_edge_underscore_variant_is_cost_filter() -> None:
    attribution = normalize_decision_attribution_from_context(
        {"all_block_reasons": ["strategy_filters.filtered_entry_cost_edge"]}
    )

    assert attribution.blocked_by_cost_filter is True


def test_pre_trade_economics_edge_variant_is_cost_filter() -> None:
    attribution = normalize_decision_attribution_from_context(
        {"all_block_reasons": ["pre_trade_economics.edge_below_required"]}
    )

    assert attribution.blocked_by_cost_filter is True


def test_non_edge_strategy_filter_is_not_cost_filter() -> None:
    attribution = normalize_decision_attribution_from_context(
        {"all_block_reasons": ["strategy_filters.market_regime_block"]}
    )

    assert attribution.blocked_by_cost_filter is False


def test_primary_block_is_derived_from_all_block_reasons_using_contract_priority() -> None:
    attribution = normalize_decision_attribution_from_context(
        {
            "raw_signal": "BUY",
            "final_signal": "HOLD",
            "all_block_reasons": [
                "strategy_filters.cost_edge",
                "position_gate.already_in_position",
            ],
        }
    )
    expected_layer = min(
        ("strategy_filters", "position_gate"),
        key=lambda layer: BLOCK_LAYER_PRIORITY.index(layer),
    )
    expected_reason = "cost_edge" if expected_layer == "strategy_filters" else "already_in_position"

    assert attribution.primary_block_layer == expected_layer
    assert attribution.primary_block_reason == expected_reason
    assert attribution.blocked_by_cost_filter is True
    assert attribution.blocked_by_position_gate is True


def test_canonical_execution_order_rule_beats_string_fallback() -> None:
    attribution = normalize_decision_attribution_from_context(
        {
            "raw_signal": "BUY",
            "final_signal": "BUY",
            "all_block_reasons": ["execution_order_rule.min_notional"],
            "execution_block_reason": "some unrelated text",
        }
    )

    assert attribution.blocked_by_order_rule is True
    assert attribution.blocked_by_performance_gate is False
    assert attribution.primary_block_layer == "execution_order_rule"
    assert attribution.primary_block_reason == "min_notional"


def test_legacy_string_fallback_still_attributes_order_rule_blocks() -> None:
    attribution = normalize_decision_attribution_from_context(
        {"execution_block_reason": "missing_order_rule_min_qty"}
    )

    assert attribution.all_block_reasons == ()
    assert attribution.blocked_by_order_rule is True


def test_malformed_all_block_reasons_are_ignored_without_crashing() -> None:
    attribution = normalize_decision_attribution_from_context(
        {
            "all_block_reasons": [
                None,
                "",
                {},
                ["bad"],
                "position_gate.already_in_position",
            ]
        }
    )

    assert attribution.all_block_reasons == ("position_gate.already_in_position",)
    assert attribution.primary_block_layer == "position_gate"
    assert attribution.primary_block_reason == "already_in_position"
    assert attribution.blocked_by_position_gate is True


def test_tuple_all_block_reason_shape_is_normalized_and_deduplicated() -> None:
    attribution = normalize_decision_attribution_from_context(
        {
            "signal_flow": {
                "all_block_reasons": [
                    ("strategy_filters", "cost_edge"),
                    "strategy_filters.cost_edge",
                    ("execution_order_rule", "min_notional"),
                ]
            }
        }
    )

    assert attribution.all_block_reasons == (
        "strategy_filters.cost_edge",
        "execution_order_rule.min_notional",
    )


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
    assert payload["schema_quality"] == {
        "all_block_reasons_present_count": 0,
        "primary_all_block_conflict_count": 0,
        "primary_block_present_count": 0,
    }


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
                "all_block_reasons": ["strategy_filters.cost_edge"],
                "signal_flow": {
                    "primary_block_layer": "strategy_filters",
                    "primary_block_reason": "cost_edge",
                },
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
    assert payload["block_layer_counts"] == {"strategy_filters": 1}
    assert payload["block_reason_counts"] == {"strategy_filters.cost_edge": 1}
    assert payload["filter_ratios"]["blocked_by_cost_filter_ratio"] == 1.0
    assert payload["schema_quality"]["all_block_reasons_present_count"] == 1
