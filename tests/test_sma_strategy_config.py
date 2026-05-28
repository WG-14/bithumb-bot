from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from bithumb_bot.approved_profile import build_approved_profile
from bithumb_bot.config import settings
from bithumb_bot.execution_reality_contract import build_execution_reality_contract
from bithumb_bot.research.hashing import content_hash_payload, sha256_prefixed
from bithumb_bot.research.promotion_gate import build_candidate_profile
from bithumb_bot.research.strategy_registry import resolve_research_strategy_plugin
from bithumb_bot.research.strategy_spec import (
    StrategySpecError,
    strategy_spec_for_name,
    validate_parameter_space_against_strategy_spec,
)
from bithumb_bot.compat.sma_legacy_adapter import SmaCrossStrategy, create_sma_strategy
from bithumb_bot.strategy_config import (
    normalize_exit_rule_names,
    sma_strategy_config_from_settings,
)
from bithumb_bot.storage_io import write_json_atomic


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


def test_strategy_spec_includes_sma_market_regime_enabled() -> None:
    spec = strategy_spec_for_name("sma_with_filter")

    assert "SMA_MARKET_REGIME_ENABLED" in spec.accepted_parameter_names
    assert "SMA_MARKET_REGIME_ENABLED" in spec.behavior_affecting_parameter_names
    assert spec.default_parameters["SMA_MARKET_REGIME_ENABLED"] is True


def test_research_only_strategy_params_rejected_for_production_bound() -> None:
    with pytest.raises(StrategySpecError, match="SMA_FILTER_LIQUIDITY_WINDOW,SMA_FILTER_VOLUME_WINDOW"):
        validate_parameter_space_against_strategy_spec(
            strategy_name="sma_with_filter",
            deployment_tier="small_live_candidate",
            parameter_space={
                "SMA_SHORT": (2,),
                "SMA_LONG": (4,),
                "SMA_FILTER_VOLUME_WINDOW": (5,),
                "SMA_FILTER_LIQUIDITY_WINDOW": (5,),
            },
        )


def test_research_only_strategy_params_allowed_for_research_only_if_documented() -> None:
    spec = validate_parameter_space_against_strategy_spec(
        strategy_name="sma_with_filter",
        deployment_tier="research_only",
        parameter_space={
            "SMA_SHORT": (2,),
            "SMA_LONG": (4,),
            "SMA_FILTER_VOLUME_WINDOW": (5,),
            "SMA_FILTER_LIQUIDITY_WINDOW": (5,),
        },
    )

    assert set(spec.research_only_parameter_names) == {
        "SMA_FILTER_VOLUME_WINDOW",
        "SMA_FILTER_LIQUIDITY_WINDOW",
    }


@pytest.fixture
def settings_guard():
    names = (
        "SMA_SHORT",
        "SMA_LONG",
        "MODE",
        "LIVE_DRY_RUN",
        "LIVE_REAL_ORDER_ARMED",
        "PAIR",
        "INTERVAL",
        "STRATEGY_EXIT_RULES",
        "STRATEGY_EXIT_STOP_LOSS_RATIO",
        "STRATEGY_EXIT_MAX_HOLDING_MIN",
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO",
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
        "STRATEGY_ENTRY_SLIPPAGE_BPS",
        "LIVE_FEE_RATE_ESTIMATE",
        "ENTRY_EDGE_BUFFER_RATIO",
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
        "SMA_MARKET_REGIME_ENABLED",
        "BUY_FRACTION",
        "MAX_ORDER_KRW",
        "APPROVED_STRATEGY_PROFILE_PATH",
        "STRATEGY_APPROVED_PROFILE_PATH",
        "STRATEGY_CANDIDATE_PROFILE_PATH",
        "EXECUTION_FILL_REFERENCE_POLICY",
        "EXECUTION_MISSING_QUOTE_POLICY",
        "EXECUTION_MIN_REALITY_LEVEL_FOR_PROMOTION",
        "EXECUTION_ALLOW_SAME_CANDLE_CLOSE_FILL",
        "EXECUTION_TOP_OF_BOOK_REQUIRED",
        "EXECUTION_DEPTH_REQUIRED",
        "EXECUTION_TRADE_TICK_REQUIRED",
        "EXECUTION_QUEUE_POSITION_REQUIRED",
        "EXECUTION_MARKET_IMPACT_REQUIRED",
        "EXECUTION_INTRA_CANDLE_PATH_AVAILABLE",
        "EXECUTION_LATENCY_MODEL_TYPE",
        "EXECUTION_LATENCY_MS",
        "EXECUTION_PARTIAL_FILL_MODEL_TYPE",
        "EXECUTION_PARTIAL_FILL_RATE",
        "EXECUTION_ORDER_FAILURE_MODEL_TYPE",
        "EXECUTION_ORDER_FAILURE_RATE",
        "EXECUTION_FEE_SOURCE",
        "EXECUTION_SLIPPAGE_SOURCE",
        "EXECUTION_CALIBRATION_REQUIRED",
        "EXECUTION_CALIBRATION_ARTIFACT_HASH",
    )
    original = {name: getattr(settings, name) for name in names}
    try:
        yield
    finally:
        for name, value in original.items():
            object.__setattr__(settings, name, value)


def _set_matching_runtime_execution_contract_settings() -> None:
    object.__setattr__(settings, "EXECUTION_FILL_REFERENCE_POLICY", "next_candle_open")
    object.__setattr__(settings, "EXECUTION_MISSING_QUOTE_POLICY", "fail")
    object.__setattr__(settings, "EXECUTION_MIN_REALITY_LEVEL_FOR_PROMOTION", "candle_next_open")
    object.__setattr__(settings, "EXECUTION_ALLOW_SAME_CANDLE_CLOSE_FILL", False)
    object.__setattr__(settings, "EXECUTION_TOP_OF_BOOK_REQUIRED", False)
    object.__setattr__(settings, "EXECUTION_DEPTH_REQUIRED", False)
    object.__setattr__(settings, "EXECUTION_TRADE_TICK_REQUIRED", False)
    object.__setattr__(settings, "EXECUTION_QUEUE_POSITION_REQUIRED", False)
    object.__setattr__(settings, "EXECUTION_MARKET_IMPACT_REQUIRED", False)
    object.__setattr__(settings, "EXECUTION_INTRA_CANDLE_PATH_AVAILABLE", False)
    object.__setattr__(settings, "EXECUTION_LATENCY_MODEL_TYPE", "fixed_bps")
    object.__setattr__(settings, "EXECUTION_LATENCY_MS", 0)
    object.__setattr__(settings, "EXECUTION_PARTIAL_FILL_MODEL_TYPE", "fixed_bps")
    object.__setattr__(settings, "EXECUTION_PARTIAL_FILL_RATE", 0.0)
    object.__setattr__(settings, "EXECUTION_ORDER_FAILURE_MODEL_TYPE", "fixed_bps")
    object.__setattr__(settings, "EXECUTION_ORDER_FAILURE_RATE", 0.0)
    object.__setattr__(settings, "EXECUTION_FEE_SOURCE", "operator_declared_test_fee")
    object.__setattr__(settings, "EXECUTION_SLIPPAGE_SOURCE", "test_calibration")
    object.__setattr__(settings, "EXECUTION_CALIBRATION_REQUIRED", True)
    object.__setattr__(settings, "EXECUTION_CALIBRATION_ARTIFACT_HASH", "sha256:calibration")


def _write_paper_profile(tmp_path: Path, *, sma_short: int) -> Path:
    strategy_plugin = resolve_research_strategy_plugin("sma_with_filter")
    parameters = {
        "SMA_SHORT": sma_short,
        "SMA_LONG": int(settings.SMA_LONG),
        "SMA_FILTER_GAP_MIN_RATIO": float(settings.SMA_FILTER_GAP_MIN_RATIO),
        "SMA_FILTER_VOL_WINDOW": int(settings.SMA_FILTER_VOL_WINDOW),
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": float(settings.SMA_FILTER_VOL_MIN_RANGE_RATIO),
        "SMA_FILTER_OVEREXT_LOOKBACK": int(settings.SMA_FILTER_OVEREXT_LOOKBACK),
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": float(settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO),
        "SMA_MARKET_REGIME_ENABLED": bool(settings.SMA_MARKET_REGIME_ENABLED),
        "SMA_COST_EDGE_ENABLED": bool(settings.SMA_COST_EDGE_ENABLED),
        "SMA_COST_EDGE_MIN_RATIO": float(settings.SMA_COST_EDGE_MIN_RATIO),
        "ENTRY_EDGE_BUFFER_RATIO": float(settings.ENTRY_EDGE_BUFFER_RATIO),
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": float(settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO),
        "STRATEGY_EXIT_RULES": str(settings.STRATEGY_EXIT_RULES),
        "STRATEGY_EXIT_STOP_LOSS_RATIO": float(settings.STRATEGY_EXIT_STOP_LOSS_RATIO),
        "STRATEGY_EXIT_MAX_HOLDING_MIN": int(settings.STRATEGY_EXIT_MAX_HOLDING_MIN),
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": float(settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO),
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": float(settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO),
    }
    base_cost_assumption = {
        "label": "paper_profile_base_cost",
        "role": "base",
        "fee_rate": float(settings.LIVE_FEE_RATE_ESTIMATE),
        "fee_source": "operator_declared_test_fee",
        "fee_authority_policy": "runtime_fee_authority_must_match_or_fail",
        "slippage_bps": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        "slippage_source": "test_calibration",
        "promotable_as_base": True,
        "source": "execution_model",
    }
    execution_model = {
        "source": "execution_model",
        "scenario_policy": "single_scenario",
        "calibration_required": True,
        "calibration_strictness": "fail",
        "scenarios": [
            {
                "type": "fixed_bps",
                "fee_rate": float(settings.LIVE_FEE_RATE_ESTIMATE),
                "slippage_bps": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
                "latency_ms": 0,
                "partial_fill_rate": 0.0,
                "order_failure_rate": 0.0,
                "market_order_extra_cost_bps": 0.0,
                "seed": None,
                "source": "execution_model",
                "scenario_policy": "single_scenario",
                "scenario_role": "base",
                "scenario_role_source": "manifest",
                "cost_assumption": base_cost_assumption,
                "model_params_hash": "sha256:model",
            }
        ],
        "model_params_hash": "sha256:model",
    }
    execution_contract = build_execution_reality_contract(
        fill_reference_policy="next_candle_open",
        missing_quote_policy="fail",
        min_execution_reality_level_for_promotion="candle_next_open",
        allow_same_candle_close_fill=False,
        top_of_book_required=False,
        latency_model={"type": "fixed_bps", "latency_ms": 0},
        partial_fill_model={"type": "fixed_bps", "partial_fill_rate": 0.0},
        order_failure_model={"type": "fixed_bps", "order_failure_rate": 0.0},
        fee_source="operator_declared_test_fee",
        slippage_source="test_calibration",
        calibration_required=True,
        calibration_artifact_hash="sha256:calibration",
    )
    candidate = {
        "experiment_id": "paper-exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "strategy_name": "sma_with_filter",
        "strategy_plugin_contract": strategy_plugin.contract_payload(),
        "strategy_plugin_contract_hash": strategy_plugin.contract_hash(),
        "parameter_candidate_id": "candidate_001",
        "parameter_values": parameters,
        "cost_model": {
            "fee_rate": float(settings.LIVE_FEE_RATE_ESTIMATE),
            "slippage_bps": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        },
        "base_cost_assumption": base_cost_assumption,
        "cost_assumption_contract": execution_model,
        "execution_model": execution_model,
        "execution_calibration_required": True,
        "execution_calibration_strictness": "fail",
        "execution_calibration_gate": {
            "status": "PASS",
            "reasons": [],
            "artifact_hash": "sha256:calibration",
            "artifact_hashes": ["sha256:calibration"],
            "scenario_gates": [
                {
                    "status": "PASS",
                    "reasons": [],
                    "artifact_hash": "sha256:calibration",
                    "content_hash_present": True,
                    "market": str(settings.PAIR),
                    "interval": str(settings.INTERVAL),
                    "expected_market": str(settings.PAIR),
                    "expected_interval": str(settings.INTERVAL),
                    "sample_count": 30,
                    "min_sample_count": 30,
                    "quality_gate_status": "PASS",
                }
            ],
        },
        "execution_calibration_artifact_hash": "sha256:calibration",
        "execution_calibration_artifact_hashes": ["sha256:calibration"],
        "execution_reality_contract": execution_contract,
        "execution_contract_hash": execution_contract["execution_contract_hash"],
        "regime_classifier_version": "market_regime_v2",
        "allowed_live_regimes": ["uptrend_normal_vol_unknown"],
        "blocked_live_regimes": ["downtrend_normal_vol_unknown"],
    }
    candidate_hash = sha256_prefixed(build_candidate_profile(candidate))
    promotion = {
        "strategy_name": "sma_with_filter",
        "strategy_profile_source_experiment": "paper-exp",
        "candidate_id": "candidate_001",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "market": str(settings.PAIR),
        "interval": str(settings.INTERVAL),
        "repository_version": "test",
        "candidate_profile": build_candidate_profile(candidate),
        "candidate_profile_hash": candidate_hash,
        "verified_candidate_profile_hash": candidate_hash,
        "live_regime_policy": {
            "regime_classifier_version": "market_regime_v2",
            "allowed_regimes": ["uptrend_normal_vol_unknown"],
            "blocked_regimes": ["downtrend_normal_vol_unknown"],
            "missing_policy_behavior": "fail_closed",
        },
        "generated_at": "2026-05-04T00:00:00+00:00",
    }
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, promotion)
    profile = build_approved_profile(
        promotion=promotion,
        mode="paper",
        source_promotion_path=str(promotion_path),
        market=str(settings.PAIR),
        interval=str(settings.INTERVAL),
        generated_at="2026-05-04T00:00:00+00:00",
    )
    profile_path = tmp_path / "paper_profile.json"
    write_json_atomic(profile_path, profile)
    return profile_path


def test_sma_strategy_config_factory_preserves_settings_defaults(settings_guard) -> None:
    object.__setattr__(settings, "SMA_SHORT", 5)
    object.__setattr__(settings, "SMA_LONG", 13)
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "INTERVAL", "1m")
    object.__setattr__(settings, "STRATEGY_EXIT_RULES", "opposite_cross, max_holding_time")
    object.__setattr__(settings, "STRATEGY_EXIT_STOP_LOSS_RATIO", 0.025)
    object.__setattr__(settings, "STRATEGY_EXIT_MAX_HOLDING_MIN", 45)
    object.__setattr__(settings, "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO", 0.012)
    object.__setattr__(settings, "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO", 0.003)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 7.0)
    object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.0015)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.0007)
    object.__setattr__(settings, "STRATEGY_MIN_EXPECTED_EDGE_RATIO", 0.002)
    object.__setattr__(settings, "BUY_FRACTION", 0.42)
    object.__setattr__(settings, "MAX_ORDER_KRW", 55_000.0)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")
    object.__setattr__(settings, "STRATEGY_APPROVED_PROFILE_PATH", "")
    object.__setattr__(settings, "STRATEGY_CANDIDATE_PROFILE_PATH", "")

    config = sma_strategy_config_from_settings()

    assert config.short_n == 5
    assert config.long_n == 13
    assert config.pair == "BTC_KRW"
    assert config.interval == "1m"
    assert config.exit_rule_names == ("opposite_cross", "max_holding_time")
    assert config.exit_stop_loss_ratio == pytest.approx(0.025)
    assert config.exit_max_holding_min == 45
    assert config.exit_min_take_profit_ratio == pytest.approx(0.012)
    assert config.exit_small_loss_tolerance_ratio == pytest.approx(0.003)
    assert config.slippage_bps == pytest.approx(7.0)
    assert config.live_fee_rate_estimate == pytest.approx(0.0015)
    assert config.entry_edge_buffer_ratio == pytest.approx(0.0007)
    assert config.strategy_min_expected_edge_ratio == pytest.approx(0.002)
    assert config.buy_fraction == pytest.approx(0.42)
    assert config.max_order_krw == pytest.approx(55_000.0)
    assert config.candidate_regime_policy is None


def test_approved_profile_runtime_mismatch_becomes_fail_closed_policy_error(
    settings_guard,
    tmp_path: Path,
) -> None:
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "SMA_SHORT", 5)
    object.__setattr__(settings, "SMA_LONG", 13)
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "INTERVAL", "1m")
    profile_path = _write_paper_profile(tmp_path, sma_short=6)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))
    object.__setattr__(settings, "STRATEGY_CANDIDATE_PROFILE_PATH", "")

    config = sma_strategy_config_from_settings()

    assert config.candidate_regime_policy is not None
    assert config.candidate_regime_policy["_policy_load_error"] == "approved_profile_runtime_mismatch"
    assert config.candidate_regime_policy["approved_profile_path"] == str(profile_path)
    assert config.candidate_regime_policy["legacy_candidate_profile_path_used"] is False
    assert config.candidate_regime_policy["approved_profile_contract_scope"] == "full_approved_profile"
    assert config.candidate_regime_policy["approved_profile_runtime_verified"] is False


def test_approved_profile_selector_is_marked_full_contract_not_legacy(
    settings_guard,
    tmp_path: Path,
) -> None:
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "SMA_SHORT", 5)
    object.__setattr__(settings, "SMA_LONG", 13)
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "INTERVAL", "1m")
    profile_path = _write_paper_profile(tmp_path, sma_short=5)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))
    object.__setattr__(settings, "STRATEGY_APPROVED_PROFILE_PATH", "")
    object.__setattr__(settings, "STRATEGY_CANDIDATE_PROFILE_PATH", "")
    _set_matching_runtime_execution_contract_settings()

    config = sma_strategy_config_from_settings()

    assert config.candidate_regime_policy is not None
    assert config.candidate_regime_policy["legacy_candidate_profile_path_used"] is False
    assert config.candidate_regime_policy.get("legacy_profile_contract_scope") is None
    assert config.candidate_regime_policy["approved_profile_contract_scope"] == "full_approved_profile"
    assert config.candidate_regime_policy.get("legacy_profile_contract_scope") not in {
        "regime_policy_only",
        "full_approved_profile",
    }
    assert config.candidate_regime_policy["approved_profile_verification_ok"] is True
    assert config.candidate_regime_policy["approved_profile_loaded"] is True
    assert config.candidate_regime_policy["approved_profile_schema_hash_valid"] is True
    assert config.candidate_regime_policy["approved_profile_source_verified"] is True
    assert config.candidate_regime_policy["approved_profile_evidence_verified"] is True
    assert config.candidate_regime_policy["approved_profile_runtime_verified"] is True
    assert config.candidate_regime_policy["approved_profile_path"] == str(profile_path.resolve())
    assert config.candidate_regime_policy["approved_profile_hash"]
    assert config.candidate_regime_policy["source_promotion_artifact_path"]
    assert config.candidate_regime_policy["promotion_content_hash"]
    assert config.candidate_regime_policy["candidate_profile_hash"]
    assert config.candidate_regime_policy["manifest_hash"]
    assert config.candidate_regime_policy["dataset_content_hash"]


def test_live_mode_legacy_candidate_path_does_not_satisfy_approved_profile_contract(
    settings_guard,
) -> None:
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")
    object.__setattr__(settings, "STRATEGY_CANDIDATE_PROFILE_PATH", "/runtime/reports/candidate.json")

    config = sma_strategy_config_from_settings()

    assert config.candidate_regime_policy is not None
    assert config.candidate_regime_policy["_policy_load_error"] == "approved_profile_missing"
    assert config.candidate_regime_policy["legacy_candidate_profile_path_used"] is True
    assert config.candidate_regime_policy["legacy_profile_contract_scope"] == "regime_policy_only"
    assert config.candidate_regime_policy["approved_profile_contract_scope"] == "legacy_regime_policy_only"
    assert config.candidate_regime_policy["approved_profile_source_verified"] is False
    assert config.candidate_regime_policy["approved_profile_evidence_verified"] is False
    assert config.candidate_regime_policy["approved_profile_runtime_verified"] is False


def test_legacy_candidate_path_to_approved_profile_is_marked_regime_policy_only(
    settings_guard,
    tmp_path: Path,
) -> None:
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "SMA_SHORT", 5)
    object.__setattr__(settings, "SMA_LONG", 13)
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "INTERVAL", "1m")
    profile_path = _write_paper_profile(tmp_path, sma_short=5)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")
    object.__setattr__(settings, "STRATEGY_APPROVED_PROFILE_PATH", "")
    object.__setattr__(settings, "STRATEGY_CANDIDATE_PROFILE_PATH", str(profile_path))

    config = sma_strategy_config_from_settings()

    assert config.candidate_regime_policy is not None
    assert config.candidate_regime_policy["approved_profile_verification_ok"] is False
    assert (
        config.candidate_regime_policy["approved_profile_block_reason"]
        == "legacy_regime_policy_only_source_not_verified"
    )
    assert config.candidate_regime_policy["legacy_candidate_profile_path_used"] is True
    assert config.candidate_regime_policy["legacy_profile_contract_scope"] == "regime_policy_only"
    assert config.candidate_regime_policy["approved_profile_contract_scope"] == "legacy_regime_policy_only"


def test_existing_sma_constructor_behavior_is_preserved() -> None:
    strategy = SmaCrossStrategy(short_n=2, long_n=3)

    assert strategy.short_n == 2
    assert strategy.long_n == 3
    assert strategy.pair == settings.PAIR
    assert strategy.interval == settings.INTERVAL
    assert tuple(strategy.exit_rule_names) == normalize_exit_rule_names(settings.STRATEGY_EXIT_RULES)
    assert strategy.buy_fraction == pytest.approx(float(settings.BUY_FRACTION))
    assert strategy.max_order_krw == pytest.approx(float(settings.MAX_ORDER_KRW))


def test_sma_from_config_preserves_stable_decision_context_fields() -> None:
    config = replace(
        sma_strategy_config_from_settings(short_n=2, long_n=3),
        pair="BTC_KRW",
        interval="1m",
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        strategy_min_expected_edge_ratio=0.0,
    )
    direct = create_sma_strategy(
        short_n=2,
        long_n=3,
        pair="BTC_KRW",
        interval="1m",
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        strategy_min_expected_edge_ratio=0.0,
    )
    from_config = SmaCrossStrategy.from_config(config)
    conn_a = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    conn_b = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        direct_decision = direct.decide(conn_a)
        config_decision = from_config.decide(conn_b)
    finally:
        conn_a.close()
        conn_b.close()

    assert direct_decision is not None
    assert config_decision is not None
    assert config_decision.context["entry"]["intent"] == direct_decision.context["entry"]["intent"]
    assert config_decision.context["gap_ratio"] == pytest.approx(direct_decision.context["gap_ratio"])
    assert config_decision.context["signal_strength_label"] == direct_decision.context["signal_strength_label"]


def test_entry_intent_uses_config_values_without_mutating_settings(settings_guard) -> None:
    object.__setattr__(settings, "BUY_FRACTION", 0.99)
    object.__setattr__(settings, "MAX_ORDER_KRW", 999_999.0)
    config = replace(
        sma_strategy_config_from_settings(short_n=2, long_n=3),
        pair="BTC_KRW",
        interval="1m",
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        strategy_min_expected_edge_ratio=0.0,
        buy_fraction=0.37,
        max_order_krw=12_345.0,
    )
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = SmaCrossStrategy.from_config(config).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    assert decision.context["entry"]["intent"]["budget_fraction_of_cash"] == pytest.approx(0.37)
    assert decision.context["entry"]["intent"]["max_budget_krw"] == pytest.approx(12_345.0)


def test_position_lot_cost_context_uses_config_values_without_mutating_settings(settings_guard) -> None:
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 99.0)
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.99)
    config = replace(
        sma_strategy_config_from_settings(short_n=2, long_n=3),
        pair="BTC_KRW",
        interval="1m",
        slippage_bps=4.5,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0017,
        strategy_min_expected_edge_ratio=0.0,
    )
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    try:
        decision = SmaCrossStrategy.from_config(config).decide(conn)
    finally:
        conn.close()

    assert decision is not None
    costs = decision.context["position_lot_interpretation_costs"]
    assert costs["exit_slippage_bps"] == pytest.approx(4.5)
    assert costs["exit_buffer_ratio"] == pytest.approx(0.0017)
    assert settings.STRATEGY_ENTRY_SLIPPAGE_BPS == pytest.approx(99.0)
    assert settings.ENTRY_EDGE_BUFFER_RATIO == pytest.approx(0.99)


def test_invalid_sma_short_long_validation_remains() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        with pytest.raises(ValueError, match="short"):
            SmaCrossStrategy(short_n=3, long_n=3).decide(conn)
    finally:
        conn.close()
