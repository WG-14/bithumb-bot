from __future__ import annotations

from bithumb_bot.market_regime import (
    MARKET_REGIME_VERSION,
    RegimeAcceptanceGate,
    aggregate_regime_coverage,
    aggregate_regime_performance,
    classify_market_regime,
    evaluate_live_regime_policy,
    evaluate_regime_acceptance_gate,
)


def _candles(closes: list[float], volumes: list[float] | None = None) -> list[dict[str, float]]:
    volume_values = volumes or [100.0 for _ in closes]
    return [
        {
            "close": close,
            "high": close * 1.01,
            "low": close * 0.99,
            "volume": volume_values[index],
        }
        for index, close in enumerate(closes)
    ]


def test_shared_classifier_is_deterministic_and_versioned() -> None:
    candles = _candles([100, 101, 102, 103, 104, 105], [100, 100, 100, 150, 160, 170])

    first = classify_market_regime(candles=candles, short_sma=104.0, long_sma=102.0)
    second = classify_market_regime(candles=candles, short_sma=104.0, long_sma=102.0)

    assert first == second
    assert first.version == "market_regime_v2"
    assert first.price_regime == "uptrend"
    assert first.volume_bucket == "volume_increasing"
    assert first.allows_sma_entry is True


def test_classifier_covers_downtrend_sideways_low_vol_and_volume_decreasing() -> None:
    down = classify_market_regime(candles=_candles([105, 104, 103, 102, 101], [200, 190, 100, 80, 70]), short_sma=101, long_sma=103)
    sideways = classify_market_regime(
        candles=[{"close": 100.0, "high": 100.01, "low": 99.99, "volume": 100.0} for _ in range(5)],
        short_sma=100.001,
        long_sma=100.0,
    )

    assert down.price_regime == "downtrend"
    assert down.volume_bucket == "volume_decreasing"
    assert down.allows_sma_entry is False
    assert sideways.price_regime == "sideways"
    assert sideways.volatility_bucket == "low_vol"


def test_classifier_handles_missing_or_zero_volume() -> None:
    snapshot = classify_market_regime(candles=_candles([100, 101, 102], [0, 0, 0]), short_sma=101, long_sma=100)

    assert snapshot.volume_bucket == "unknown"
    assert snapshot.liquidity_bucket == "unknown"


def test_regime_aggregation_handles_zero_loss_and_zero_trade_rows() -> None:
    snapshots = [
        {"price_regime": "uptrend", "volatility_bucket": "normal_vol", "volume_bucket": "volume_normal", "composite_regime": "uptrend_normal_vol_volume_normal"},
        {"price_regime": "sideways", "volatility_bucket": "low_vol", "volume_bucket": "volume_decreasing", "composite_regime": "sideways_low_vol_volume_decreasing"},
    ]
    trades = [
        {
            "side": "SELL",
            "net_pnl": 10.0,
            "fee_total": 1.0,
            "slippage_total": 2.0,
            "entry_regime_snapshot": snapshots[0],
        }
    ]

    coverage = aggregate_regime_coverage(snapshots=snapshots, trades=trades)
    performance = aggregate_regime_performance(trades=trades, coverage=coverage, start_cash=1000.0)

    zero_trade = next(row for row in performance if row.dimension == "composite_regime" and row.regime == "sideways_low_vol_volume_decreasing")
    winner = next(row for row in performance if row.dimension == "composite_regime" and row.regime == "uptrend_normal_vol_volume_normal")
    assert zero_trade.trade_count == 0
    assert zero_trade.profit_factor is None
    assert winner.trade_count == 1
    assert winner.single_trade_dependency_score == 1.0


def test_regime_acceptance_gate_passes_and_fails_specific_conditions() -> None:
    rows = (
        {
            "dimension": "composite_regime",
            "regime": "uptrend_normal_vol_volume_increasing",
            "trade_count": 12,
            "profit_factor": 1.4,
            "expectancy": 100.0,
            "net_pnl": 1200.0,
        },
        {
            "dimension": "composite_regime",
            "regime": "sideways_low_vol_volume_decreasing",
            "trade_count": 0,
            "profit_factor": None,
            "expectancy": None,
            "net_pnl": 0.0,
        },
    )
    gate = RegimeAcceptanceGate(
        required=True,
        min_trade_count_per_required_regime=10,
        required_regimes=("uptrend_normal_vol_volume_increasing",),
        blocked_regimes=("sideways_low_vol_volume_decreasing",),
        min_profit_factor_by_regime={"uptrend_normal_vol_volume_increasing": 1.2},
        max_pnl_dependency_by_single_regime=1.0,
    )

    assert evaluate_regime_acceptance_gate(gate=gate, performance_rows=rows).passed is True

    failed = evaluate_regime_acceptance_gate(
        gate=RegimeAcceptanceGate(
            required=True,
            min_trade_count_per_required_regime=20,
            required_regimes=("uptrend_normal_vol_volume_increasing",),
            blocked_regimes=("sideways_low_vol_volume_decreasing",),
            blocked_regime_max_trade_count=0,
        ),
        performance_rows=rows,
    )
    assert failed.passed is False
    assert any("regime_coverage_failed" in reason for reason in failed.reasons)


def test_regime_acceptance_gate_fails_blocked_leakage_and_profit_dependency() -> None:
    rows = (
        {"dimension": "composite_regime", "regime": "uptrend_normal_vol_volume_normal", "trade_count": 1, "net_pnl": 1000.0, "profit_factor": 2.0},
        {"dimension": "composite_regime", "regime": "sideways_low_vol_volume_decreasing", "trade_count": 1, "net_pnl": -10.0, "profit_factor": 0.5},
    )
    result = evaluate_regime_acceptance_gate(
        gate=RegimeAcceptanceGate(
            required=True,
            blocked_regimes=("sideways_low_vol_volume_decreasing",),
            max_pnl_dependency_by_single_regime=0.5,
        ),
        performance_rows=rows,
    )

    assert result.passed is False
    assert any("blocked_regime_leakage" in reason for reason in result.reasons)
    assert any("regime_profit_dependency_failed" in reason for reason in result.reasons)


def test_live_regime_policy_fails_closed_when_missing_or_not_allowed() -> None:
    snapshot = {"version": "market_regime_v2", "composite_regime": "sideways_low_vol_volume_decreasing"}

    missing = evaluate_live_regime_policy(current_snapshot=snapshot, candidate_policy=None)
    blocked = evaluate_live_regime_policy(
        current_snapshot=snapshot,
        candidate_policy={
            "regime_classifier_version": MARKET_REGIME_VERSION,
            "allowed_regimes": ["uptrend_normal_vol_volume_increasing"],
            "blocked_regimes": ["sideways_low_vol_volume_decreasing"],
        },
    )

    assert missing["allowed"] is False
    assert missing["regime_block_reason"] == "regime_policy_missing"
    assert blocked["allowed"] is False
    assert blocked["regime_block_reason"] == "current_regime_in_candidate_blocked_regimes"


def test_live_regime_policy_normalizes_live_and_promotion_field_names() -> None:
    snapshot = {"version": MARKET_REGIME_VERSION, "composite_regime": "uptrend_high_vol_unknown"}

    live_names = evaluate_live_regime_policy(
        current_snapshot=snapshot,
        candidate_policy={
            "regime_classifier_version": MARKET_REGIME_VERSION,
            "allowed_live_regimes": ["uptrend_high_vol_unknown"],
            "blocked_live_regimes": [],
            "regime_evidence": {"uptrend_high_vol_unknown": {"trade_count": 3}},
        },
    )
    promotion_artifact = evaluate_live_regime_policy(
        current_snapshot=snapshot,
        candidate_policy={
            "live_regime_policy": {
                "regime_classifier_version": MARKET_REGIME_VERSION,
                "allowed_regimes": ["uptrend_high_vol_unknown"],
                "blocked_regimes": [],
            },
            "regime_evidence": {"uptrend_high_vol_unknown": {"trade_count": 3}},
        },
    )

    assert live_names["allowed"] is True
    assert promotion_artifact["allowed"] is True
    assert live_names["candidate_allowed_regimes"] == ["uptrend_high_vol_unknown"]
    assert promotion_artifact["candidate_allowed_regimes"] == ["uptrend_high_vol_unknown"]


def test_live_regime_policy_fails_closed_for_invalid_or_version_mismatch() -> None:
    snapshot = {"version": MARKET_REGIME_VERSION, "composite_regime": "uptrend_high_vol_unknown"}

    missing_allowed = evaluate_live_regime_policy(
        current_snapshot=snapshot,
        candidate_policy={
            "regime_classifier_version": MARKET_REGIME_VERSION,
            "blocked_regimes": [],
        },
    )
    mismatch = evaluate_live_regime_policy(
        current_snapshot=snapshot,
        candidate_policy={
            "regime_classifier_version": "market_regime_v1",
            "allowed_regimes": ["uptrend_high_vol_unknown"],
            "blocked_regimes": [],
        },
    )

    assert missing_allowed["allowed"] is False
    assert missing_allowed["regime_block_reason"] == "regime_policy_missing_allowed_regimes"
    assert mismatch["allowed"] is False
    assert mismatch["regime_block_reason"] == "regime_policy_version_mismatch"
    assert mismatch["current_regime_classifier_version"] == MARKET_REGIME_VERSION
    assert mismatch["candidate_regime_classifier_version"] == "market_regime_v1"
