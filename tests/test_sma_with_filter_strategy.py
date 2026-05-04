from __future__ import annotations

import json
import importlib
import sqlite3

import pytest

from bithumb_bot.config import settings
from bithumb_bot.dust import classify_dust_residual, dust_qty_gap_tolerance
from bithumb_bot.market_regime import MARKET_REGIME_VERSION
from bithumb_bot.strategy.sma import create_sma_strategy, create_sma_with_filter_strategy


def _build_candle_db(closes: list[float]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE candles (
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            close REAL NOT NULL
        )
        """
    )
    base_ts = 1_700_000_000_000
    for idx, close in enumerate(closes):
        conn.execute(
            "INSERT INTO candles(ts, pair, interval, close) VALUES (?, ?, ?, ?)",
            (base_ts + idx * 60_000, "BTC_KRW", "1m", close),
        )
    conn.commit()
    return conn


def _allowing_policy() -> dict[str, object]:
    return {
        "regime_classifier_version": MARKET_REGIME_VERSION,
        "allowed_regimes": [
            "uptrend_high_vol_unknown",
            "uptrend_normal_vol_unknown",
            "uptrend_low_vol_unknown",
        ],
        "blocked_regimes": [],
        "regime_evidence": {},
    }


def _buy_decision_with_policy(candidate_regime_policy: dict[str, object] | None):
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        return create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.001,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            cost_edge_enabled=True,
            cost_edge_min_ratio=0.0,
            live_fee_rate_estimate=0.0001,
            candidate_regime_policy=candidate_regime_policy,
        ).decide(conn)
    finally:
        conn.close()


def _seed_position_and_dust_state(
    conn: sqlite3.Connection,
    *,
    qty_open: float,
    dust_metadata: dict[str, object],
    position_state: str = "open_exposure",
    executable_lot_count: int = 0,
    dust_tracking_lot_count: int = 0,
) -> None:
    conn.execute(
        """
        CREATE TABLE bot_health (
            id INTEGER PRIMARY KEY,
            last_reconcile_metadata TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE open_position_lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT NOT NULL,
            entry_trade_id INTEGER NOT NULL,
            entry_client_order_id TEXT NOT NULL,
            entry_ts INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            qty_open REAL NOT NULL,
            executable_lot_count INTEGER NOT NULL DEFAULT 0,
            dust_tracking_lot_count INTEGER NOT NULL DEFAULT 0,
            position_semantic_basis TEXT NOT NULL DEFAULT 'lot-native',
            position_state TEXT NOT NULL DEFAULT 'open_exposure'
        )
        """
    )
    conn.execute(
        "INSERT INTO bot_health(id, last_reconcile_metadata) VALUES (1, ?)",
        (json.dumps(dust_metadata, ensure_ascii=False, sort_keys=True),),
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair,
            entry_trade_id,
            entry_client_order_id,
            entry_ts,
            entry_price,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "BTC_KRW",
            1,
            "entry-1",
            1_700_000_000_000,
            40_000_000.0,
            qty_open,
            executable_lot_count,
            dust_tracking_lot_count,
            position_state,
        ),
    )
    conn.commit()


def test_filtered_sma_can_change_trade_signal_to_hold() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        plain = create_sma_strategy(short_n=2, long_n=3, pair="BTC_KRW", interval="1m").decide(conn)
        filtered = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.02,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            market_regime_enabled=False,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert plain is not None
    assert filtered is not None
    assert plain.signal == "BUY"
    assert filtered.signal == "HOLD"
    assert filtered.reason.startswith("filtered entry")


def test_market_regime_allows_trend_entry_and_records_replay_fingerprint() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.001,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            cost_edge_enabled=True,
            cost_edge_min_ratio=0.0,
            live_fee_rate_estimate=0.0001,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "BUY"
    assert decision.context["market_regime"]["regime"] == "trend_up"
    assert decision.context["market_regime"]["allows_entry"] is True
    assert decision.context["signal_flow"]["primary_block_layer"] != "market_regime"
    replay = decision.context["replay_fingerprint"]
    assert replay["strategy_name"] == "sma_with_filter"
    assert replay["decision_contract_version"] == "decision_v2"
    assert replay["pair"] == "BTC_KRW"
    assert replay["interval"] == "1m"
    assert replay["sma_short"] == 2
    assert replay["sma_long"] == 3
    assert replay["regime_feature_version"] == decision.context["market_regime"]["version"]
    assert replay["thresholds"]["sma_filter_gap_min_ratio"] == pytest.approx(0.001)
    assert replay["fee_authority_source"]


def test_decision_context_includes_approved_profile_audit_fields() -> None:
    policy = {
        "live_regime_policy": _allowing_policy(),
        "strategy_profile_hash": "sha256:profile",
        "source_promotion_content_hash": "sha256:promotion",
        "source_promotion_artifact_path": "/runtime/reports/promotion.json",
        "candidate_profile_hash": "sha256:candidate",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "approved_profile_mode": "small_live",
        "approved_profile_verification_ok": True,
        "approved_profile_block_reason": "ok",
    }
    original = settings.APPROVED_STRATEGY_PROFILE_PATH
    try:
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "/runtime/profiles/small_live.json")
        decision = _buy_decision_with_policy(policy)
    finally:
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", original)

    assert decision is not None
    assert decision.context["approved_profile_hash"] == "sha256:profile"
    assert decision.context["approved_profile_path"] == "/runtime/profiles/small_live.json"
    assert decision.context["approved_profile_mode"] == "small_live"
    assert decision.context["approved_profile_verification_ok"] is True
    assert decision.context["approved_profile_block_reason"] == "ok"
    assert decision.context["source_promotion_artifact_path"] == "/runtime/reports/promotion.json"
    assert decision.context["promotion_content_hash"] == "sha256:promotion"
    assert decision.context["candidate_profile_hash"] == "sha256:candidate"
    assert decision.context["manifest_hash"] == "sha256:manifest"
    assert decision.context["dataset_content_hash"] == "sha256:dataset"


def test_replay_fingerprint_preserves_distinct_through_ts_ms() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    last_candle_ts = 1_700_000_000_000 + 4 * 60_000
    through_ts_ms = last_candle_ts + 30_000
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.001,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            cost_edge_enabled=True,
            cost_edge_min_ratio=0.0,
            live_fee_rate_estimate=0.0001,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn, through_ts_ms=through_ts_ms)
    finally:
        conn.close()

    assert decision is not None
    replay = decision.context["replay_fingerprint"]
    assert replay["candle_ts"] == last_candle_ts
    assert replay["through_ts_ms"] == through_ts_ms
    assert replay["strategy_name"] == "sma_with_filter"
    assert replay["decision_contract_version"] == "decision_v2"
    assert replay["regime_feature_version"] == decision.context["market_regime"]["version"]


def test_market_regime_chop_blocks_buy_candidate_before_strategy_filters() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 10.01])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.001,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            cost_edge_enabled=False,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.context["base_signal"] == "BUY"
    assert decision.signal == "HOLD"
    assert decision.context["market_regime"]["regime"] == "chop"
    assert decision.context["signal_flow"]["primary_block_layer"] == "market_regime"
    assert decision.context["signal_flow"]["primary_block_reason"] == "chop_market"
    assert "market_regime.chop_market" in decision.context["signal_flow"]["all_block_reasons"]


def test_cost_edge_block_remains_distinguishable_from_market_regime() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.001,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            cost_edge_enabled=True,
            cost_edge_min_ratio=0.10,
            live_fee_rate_estimate=0.02,
            entry_edge_buffer_ratio=0.0,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.context["market_regime"]["allows_entry"] is True
    assert "cost_edge" in decision.context["blocked_filters"]
    assert decision.context["signal_flow"]["primary_block_layer"] == "strategy_filters"
    assert decision.context["signal_flow"]["primary_block_reason"] == "cost_edge"


def test_candidate_regime_policy_blocks_live_entry_when_current_regime_not_allowed() -> None:
    decision = _buy_decision_with_policy(
        {
            "regime_classifier_version": MARKET_REGIME_VERSION,
            "allowed_regimes": ["downtrend_low_vol_volume_decreasing"],
            "blocked_regimes": ["sideways_low_vol_volume_decreasing"],
            "regime_evidence": {},
        }
    )

    assert decision is not None
    assert decision.context["base_signal"] == "BUY"
    assert decision.signal == "HOLD"
    assert decision.context["candidate_regime_blocked"] is True
    assert decision.context["regime_decision"] == "OFF"
    assert decision.context["regime_block_reason"] == "current_regime_not_in_candidate_allowed_regimes"


def test_missing_candidate_regime_policy_blocks_live_entry_with_auditable_context() -> None:
    decision = _buy_decision_with_policy(None)

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.context["regime_decision"] == "OFF"
    assert decision.context["regime_block_reason"] == "regime_policy_missing"
    assert decision.context["regime_policy_present"] is False
    assert decision.context["regime_policy_valid"] is False
    assert "current_market_regime_snapshot" in decision.context
    assert decision.context["candidate_allowed_regimes"] == []
    assert decision.context["candidate_blocked_regimes"] == []
    assert decision.context["signal_flow"]["primary_block_layer"] == "candidate_regime"


def test_old_candidate_artifact_without_allowed_regimes_blocks_live_entry() -> None:
    decision = _buy_decision_with_policy(
        {
            "regime_classifier_version": MARKET_REGIME_VERSION,
            "regime_evidence": {},
        }
    )

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.context["regime_block_reason"] == "regime_policy_missing_allowed_regimes"
    assert decision.context["regime_policy_present"] is True
    assert decision.context["regime_policy_valid"] is False


def test_invalid_candidate_policy_blocks_live_entry_with_specific_reason() -> None:
    decision = _buy_decision_with_policy({"allowed_regimes": "uptrend_high_vol_unknown"})

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.context["regime_block_reason"] == "regime_policy_missing_classifier_version"
    assert decision.context["regime_policy_valid"] is False


def test_candidate_classifier_version_mismatch_blocks_live_entry() -> None:
    decision = _buy_decision_with_policy(
        {
            "regime_classifier_version": "market_regime_v1",
            "allowed_regimes": ["uptrend_high_vol_unknown"],
            "blocked_regimes": [],
            "regime_evidence": {},
        }
    )

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.context["regime_block_reason"] == "regime_policy_version_mismatch"
    assert decision.context["current_regime_classifier_version"] == MARKET_REGIME_VERSION
    assert decision.context["candidate_regime_classifier_version"] == "market_regime_v1"


def test_candidate_policy_explicitly_blocked_regime_blocks_live_entry() -> None:
    allowed = _allowing_policy()
    decision = _buy_decision_with_policy(
        {
            **allowed,
            "blocked_regimes": ["uptrend_high_vol_unknown"],
        }
    )

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.context["regime_block_reason"] == "current_regime_in_candidate_blocked_regimes"


def test_candidate_policy_allowed_regime_permits_buy_when_other_gates_pass() -> None:
    decision = _buy_decision_with_policy(_allowing_policy())

    assert decision is not None
    assert decision.signal == "BUY"
    assert decision.context["regime_decision"] == "ON"
    assert decision.context["regime_block_reason"] == "none"
    assert decision.context["regime_policy_present"] is True
    assert decision.context["regime_policy_valid"] is True
    assert decision.context["candidate_allowed_regimes"]


def test_candidate_regime_policy_does_not_block_sell_exit(relaxed_test_order_rules) -> None:
    conn = _build_candle_db([11.0, 11.0, 11.0, 11.0, 10.0])
    try:
        _seed_position_and_dust_state(
            conn,
            qty_open=0.0002,
            dust_metadata={},
            executable_lot_count=2,
        )
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            cost_edge_min_ratio=0.0,
            candidate_regime_policy=None,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "SELL"
    assert decision.context["raw_signal"] == "SELL"
    assert decision.context["regime_decision"] == "OFF"
    assert decision.context["regime_block_reason"] == "regime_policy_missing"


def test_gap_filter_blocks_entry_and_writes_context() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.02,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert "gap" in decision.context["blocked_filters"]
    assert decision.context["filters"]["gap"]["passed"] is False
    assert decision.context["features"]["base_signal"] == "BUY"


def test_volatility_filter_blocks_low_range_entry() -> None:
    conn = _build_candle_db([100.0, 100.0, 100.0, 100.0, 100.01])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=5,
            min_volatility_ratio=0.001,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert "volatility" in decision.context["blocked_filters"]
    assert decision.context["filters"]["volatility"]["passed"] is False


def test_overextended_filter_blocks_chasing_entry() -> None:
    conn = _build_candle_db([100.0, 100.0, 100.0, 100.0, 130.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=2,
            overextended_max_return_ratio=0.1,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert "overextended" in decision.context["blocked_filters"]
    assert decision.context["filters"]["overextended"]["passed"] is False


def test_cost_edge_filter_blocks_small_gap_entry_and_records_reason() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.02,
            entry_edge_buffer_ratio=0.005,
            cost_edge_min_ratio=0.0,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.reason.startswith("filtered entry")
    assert "cost_edge" in decision.context["blocked_filters"]
    assert decision.context["filters"]["gap"]["passed"] is True
    assert decision.context["filters"]["volatility"]["passed"] is True
    assert decision.context["filters"]["overextended"]["passed"] is True
    assert decision.context["filters"]["cost_edge"]["passed"] is False
    assert decision.context["filters"]["cost_edge"]["cost_floor_ratio"] == 0.045
    assert decision.context["entry"]["cost_edge_blocked"] is True


def test_cost_edge_filter_allows_entry_when_signal_clears_cost_floor() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            cost_edge_min_ratio=0.0,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "BUY"
    assert decision.context["filters"]["cost_edge"]["passed"] is True
    assert decision.context["entry"]["cost_edge_blocked"] is False


def test_cost_edge_filter_keeps_sell_signal_when_edge_is_sufficient() -> None:
    conn = _build_candle_db([11.0, 11.0, 11.0, 11.0, 10.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            cost_edge_min_ratio=0.0,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.reason == "no_position"
    assert decision.context["filters"]["cost_edge"]["passed"] is True
    assert decision.context["entry"]["cost_edge_blocked"] is False


def test_harmless_dust_effective_flat_keeps_buy_entry_intentable() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    dust = classify_dust_residual(
        broker_qty=0.00009629,
        local_qty=0.00009629,
        min_qty=0.0001,
        min_notional_krw=5000.0,
        latest_price=40_000_000.0,
        partial_flatten_recent=False,
        partial_flatten_reason="not_recent",
        qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
        matched_harmless_resume_allowed=True,
    )
    try:
        _seed_position_and_dust_state(conn, qty_open=0.00009629, dust_metadata=dust.to_metadata())
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
        state_row = conn.execute(
            "SELECT position_state FROM open_position_lots WHERE entry_client_order_id='entry-1'"
        ).fetchone()
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "BUY"
    assert decision.context["raw_signal"] == "BUY"
    assert decision.context["entry_allowed"] is True
    assert decision.context["normalized_exposure_active"] is False
    assert decision.context["entry"]["base_signal"] == "BUY"
    assert decision.context["entry"]["entry_signal"] == "BUY"
    assert decision.context["position_gate"]["effective_flat_due_to_harmless_dust"] is True
    assert decision.context["position_gate"]["entry_allowed"] is True
    assert decision.context["position_gate"]["normalized_exposure_active"] is False
    assert decision.context["position_gate"]["has_executable_exposure"] is False
    assert decision.context["position_gate"]["has_any_position_residue"] is True
    assert decision.context["position_gate"]["has_dust_only_remainder"] is True
    assert decision.context["position_gate"]["open_exposure_qty"] == pytest.approx(0.0)
    assert decision.context["position_gate"]["dust_tracking_qty"] == pytest.approx(0.00009629)
    assert decision.context["position_state"]["raw_holdings"]["classification"] == "harmless_dust"
    assert decision.context["position_state"]["raw_holdings"]["present"] is True
    assert decision.context["position_state"]["normalized_exposure"]["entry_allowed"] is True
    assert decision.context["position_state"]["normalized_exposure"]["normalized_exposure_active"] is False
    assert decision.context["position_state"]["normalized_exposure"]["has_executable_exposure"] is False
    assert decision.context["position_state"]["normalized_exposure"]["has_any_position_residue"] is True
    assert decision.context["position_state"]["normalized_exposure"]["has_dust_only_remainder"] is True
    assert decision.context["position_state"]["normalized_exposure"]["open_exposure_qty"] == pytest.approx(0.0)
    assert decision.context["position_state"]["normalized_exposure"]["dust_tracking_qty"] == pytest.approx(0.00009629)
    assert decision.context["position_gate"]["dust_new_orders_allowed"] is True
    assert decision.context["position_gate"]["dust_resume_allowed"] is True
    assert decision.context["position_gate"]["dust_treat_as_flat"] is True
    assert decision.context["position_gate"]["raw_qty_open"] == pytest.approx(0.0)
    assert "sell_submit_qty" not in decision.context["position_gate"]
    assert "sell_submit_qty_source" not in decision.context["position_gate"]
    assert decision.context["position_state"]["normalized_exposure"]["normalized_exposure_qty"] == pytest.approx(0.0)
    assert state_row[0] == "dust_tracking"


def test_entry_decision_returns_intent_budget_without_final_order_size() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_strategy(short_n=2, long_n=3, pair="BTC_KRW", interval="1m").decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "BUY"
    assert "entry_execution_sizing" not in decision.context
    assert "entry_execution_sizing" not in decision.context["filters"]
    assert "submit_payload_qty" not in decision.context
    assert "position_qty" not in decision.context
    assert "open_exposure_qty" not in decision.context
    assert "dust_tracking_qty" not in decision.context
    assert "reserved_exit_qty" not in decision.context
    assert "sellable_executable_qty" not in decision.context
    assert "normalized_exposure_qty" not in decision.context
    assert "budget_krw" not in decision.context["entry"]["intent"]
    assert "requested_qty" not in decision.context["entry"]["intent"]
    assert "executable_qty" not in decision.context["entry"]["intent"]
    assert decision.context["entry"]["intent"] == {
        "pair": "BTC_KRW",
        "intent": "enter_open_exposure",
        "budget_model": "cash_fraction_capped_by_max_order_krw",
        "budget_fraction_of_cash": pytest.approx(float(settings.BUY_FRACTION)),
        "max_budget_krw": pytest.approx(float(settings.MAX_ORDER_KRW)),
        "requires_execution_sizing": True,
    }


def test_non_executable_exit_stops_at_state_layer_and_does_not_emit_sell() -> None:
    original_rules = {
        "LIVE_MIN_ORDER_QTY": float(settings.LIVE_MIN_ORDER_QTY),
        "LIVE_ORDER_QTY_STEP": float(settings.LIVE_ORDER_QTY_STEP),
        "LIVE_ORDER_MAX_QTY_DECIMALS": int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
        "MIN_ORDER_NOTIONAL_KRW": float(settings.MIN_ORDER_NOTIONAL_KRW),
    }
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    conn = _build_candle_db([11.0, 11.0, 11.0, 11.0, 10.0])
    try:
        _seed_position_and_dust_state(
            conn,
            qty_open=0.00009629,
            dust_metadata={},
            position_state="dust_tracking",
            dust_tracking_lot_count=1,
        )
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
        state_row = conn.execute(
            "SELECT position_state FROM open_position_lots WHERE entry_client_order_id='entry-1'"
        ).fetchone()
    finally:
        conn.close()
        for key, value in original_rules.items():
            object.__setattr__(settings, key, value)

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.reason == "dust_only_remainder"
    assert decision.context["raw_signal"] == "SELL"
    assert decision.context["terminal_state"] == "dust_only"
    assert decision.context["exit_allowed"] is False
    assert decision.context["exit_block_reason"] == "dust_only_remainder"
    assert decision.context["position_state"]["normalized_exposure"]["sellable_executable_qty"] == pytest.approx(0.0)
    assert decision.context["position_state"]["normalized_exposure"]["terminal_state"] == "dust_only"
    assert decision.context["position_state"]["state_interpretation"]["operator_outcome"] == "tracked_unsellable_residual"
    assert decision.context["position_state"]["state_interpretation"]["exit_submit_expected"] is False
    assert decision.context["exit"]["triggered"] is False
    assert decision.context["exit"]["policy"] == "none"
    assert decision.context["exit"]["reason"] == "dust_only_remainder"
    assert decision.context["state_outcome"] == "tracked_unsellable_residual"
    assert decision.context["exit_submit_expected"] is False
    assert state_row[0] == "dust_tracking"


def test_exit_decision_uses_normalized_shared_state_without_last_buy_request_size(
    relaxed_test_order_rules,
) -> None:
    conn = _build_candle_db([11.0, 11.0, 11.0, 11.0, 10.0])
    try:
        _seed_position_and_dust_state(
            conn,
            qty_open=0.0002,
            dust_metadata={},
            executable_lot_count=2,
        )
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "SELL"
    assert decision.context["exit"]["policy"] == "full"
    assert decision.context["position_state"]["normalized_exposure"]["open_exposure_qty"] == pytest.approx(0.0002)
    assert decision.context["position_state"]["normalized_exposure"]["sellable_executable_qty"] == pytest.approx(0.0002)
    assert "last_buy_request_qty" not in decision.context
    assert "last_buy_request_qty" not in decision.context["exit"]


def test_harmless_dust_without_resume_keeps_buy_entry_blocked() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    dust = classify_dust_residual(
        broker_qty=0.00009629,
        local_qty=0.00009629,
        min_qty=0.0001,
        min_notional_krw=5000.0,
        latest_price=40_000_000.0,
        partial_flatten_recent=False,
        partial_flatten_reason="not_recent",
        qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
        matched_harmless_resume_allowed=False,
    )
    dust_metadata = dict(dust.to_metadata())
    dust_metadata["unresolved_open_order_count"] = 1
    try:
        _seed_position_and_dust_state(
            conn,
            qty_open=0.00009629,
            dust_metadata=dust_metadata,
            position_state="dust_tracking",
            dust_tracking_lot_count=1,
        )
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.context["entry"]["base_signal"] == "BUY"
    assert decision.context["entry"]["entry_signal"] == "BUY"
    assert decision.context["position_gate"]["dust_state"] == "harmless_dust"
    assert decision.context["position_gate"]["effective_flat_due_to_harmless_dust"] is False
    assert decision.context["position_gate"]["dust_new_orders_allowed"] is False
    assert decision.context["position_gate"]["dust_resume_allowed"] is False
    assert decision.context["position_gate"]["dust_treat_as_flat"] is True
    assert decision.context["position_gate"]["has_executable_exposure"] is False
    assert decision.context["position_gate"]["has_any_position_residue"] is True
    assert decision.context["position_gate"]["has_non_executable_residue"] is True
    assert decision.context["raw_signal"] == "BUY"
    assert decision.context["final_signal"] == "HOLD"
    assert decision.context["entry_blocked"] is True
    assert decision.context["entry_block_reason"] == "dust_only_remainder"
    assert decision.context["dust_classification"] == "harmless_dust"
    assert decision.context["effective_flat"] is False
    assert decision.context["raw_qty_open"] == pytest.approx(0.00009629)
    assert decision.context["normalized_exposure_active"] is False
    assert decision.context["has_executable_exposure"] is False
    assert decision.context["has_any_position_residue"] is True
    assert decision.context["has_non_executable_residue"] is True
    assert decision.context["position_state"]["normalized_exposure"]["normalized_exposure_qty"] == pytest.approx(0.0)


def test_blocking_dust_still_blocks_buy_entry() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    dust = classify_dust_residual(
        broker_qty=0.000099,
        local_qty=0.000010,
        min_qty=0.0001,
        min_notional_krw=5000.0,
        latest_price=40_000_000.0,
        partial_flatten_recent=False,
        partial_flatten_reason="not_recent",
        qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
        matched_harmless_resume_allowed=False,
    )
    try:
        _seed_position_and_dust_state(
            conn,
            qty_open=0.000099,
            dust_metadata=dust.to_metadata(),
            position_state="dust_tracking",
            dust_tracking_lot_count=1,
        )
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.context["entry"]["base_signal"] == "BUY"
    assert decision.context["entry"]["entry_signal"] == "BUY"
    assert decision.context["position_gate"]["dust_state"] == "blocking_dust"
    assert decision.context["position_gate"]["effective_flat_due_to_harmless_dust"] is False
    assert decision.context["position_gate"]["dust_new_orders_allowed"] is False
    assert decision.context["position_gate"]["dust_resume_allowed"] is False
    assert decision.context["position_gate"]["dust_treat_as_flat"] is False


def test_cost_edge_filter_can_be_disabled_explicitly() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.02,
            entry_edge_buffer_ratio=0.005,
            cost_edge_enabled=False,
            cost_edge_min_ratio=0.05,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "BUY"
    assert "cost_edge" not in decision.context["blocked_filters"]
    assert decision.context["filters"]["cost_edge"]["enabled"] is False
    assert decision.context["filters"]["cost_edge"]["configured_enabled"] is False
    assert decision.context["entry"]["cost_edge_blocked"] is False


def test_cost_edge_min_ratio_relaxation_unblocks_same_market_case() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        strict = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.0,
            cost_edge_enabled=True,
            cost_edge_min_ratio=0.06,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
        relaxed = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.0,
            cost_edge_enabled=True,
            cost_edge_min_ratio=0.0,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert strict is not None
    assert relaxed is not None
    assert strict.signal == "HOLD"
    assert "cost_edge" in strict.context["blocked_filters"]
    assert relaxed.signal == "BUY"
    assert "cost_edge" not in relaxed.context["blocked_filters"]


def test_cost_edge_filter_becomes_more_conservative_when_fee_or_buffer_increase() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        permissive = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            cost_edge_min_ratio=0.0,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
        conservative = create_sma_with_filter_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            min_gap_ratio=0.0,
            volatility_window=3,
            min_volatility_ratio=0.0,
            overextended_lookback=1,
            overextended_max_return_ratio=0.0,
            slippage_bps=0.0,
            live_fee_rate_estimate=0.01,
            entry_edge_buffer_ratio=0.01,
            cost_edge_min_ratio=0.0,
            candidate_regime_policy=_allowing_policy(),
        ).decide(conn)
    finally:
        conn.close()

    assert permissive is not None
    assert conservative is not None
    assert permissive.signal == "BUY"
    assert conservative.signal == "HOLD"
    assert conservative.context["filters"]["cost_edge"]["threshold"] > permissive.context["filters"][
        "cost_edge"
    ]["threshold"]


def test_filtered_strategy_default_thresholds_are_conservative_and_valid() -> None:
    strategy = create_sma_with_filter_strategy(short_n=2, long_n=3, pair="BTC_KRW", interval="1m")

    assert strategy.min_gap_ratio >= 0.001
    assert strategy.min_volatility_ratio >= 0.003
    assert strategy.overextended_max_return_ratio <= 0.02


def test_sma_cross_cost_edge_filter_blocks_weak_entry_and_records_context() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.02,
            entry_edge_buffer_ratio=0.005,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "HOLD"
    assert decision.reason == "filtered entry: cost_edge"
    assert decision.context["raw_signal"] == "BUY"
    assert decision.context["final_signal"] == "HOLD"
    assert decision.context["entry_blocked"] is True
    assert decision.context["entry_block_reason"] == "filtered entry: cost_edge"
    assert decision.context["blocked_by_cost_filter"] is True
    assert decision.context["gap_ratio"] < decision.context["cost_floor_ratio"]
    assert decision.context["filters"]["cost_edge"]["passed"] is False
    assert decision.context["signal_strength_label"] == "weak"
    assert decision.context["signal_strength"]["is_weak_cross"] is True
    assert decision.context["signal_strength"]["preferred_live_strategy"] == "sma_with_filter"


def test_sma_cross_cost_edge_filter_keeps_signal_when_edge_is_sufficient() -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = create_sma_strategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.001,
            entry_edge_buffer_ratio=0.001,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.signal == "BUY"
    assert decision.context["blocked_by_cost_filter"] is False
    assert decision.context["gap_ratio"] > decision.context["cost_floor_ratio"]
    assert decision.context["filters"]["cost_edge"]["passed"] is True
    assert decision.context["signal_strength_label"] == "tradable"
    assert decision.context["signal_strength"]["is_weak_cross"] is False


def test_strategy_entry_slippage_defaults_to_zero_when_env_values_are_unset(monkeypatch) -> None:
    monkeypatch.delenv("STRATEGY_ENTRY_SLIPPAGE_BPS", raising=False)
    monkeypatch.delenv("MAX_MARKET_SLIPPAGE_BPS", raising=False)
    monkeypatch.delenv("SLIPPAGE_BPS", raising=False)
    config_module = importlib.import_module("bithumb_bot.config")
    config_module = importlib.reload(config_module)
    try:
        assert config_module.settings.STRATEGY_ENTRY_SLIPPAGE_BPS == 0.0
    finally:
        importlib.reload(config_module)
