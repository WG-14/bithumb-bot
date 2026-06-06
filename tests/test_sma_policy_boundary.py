from __future__ import annotations

import ast
import inspect
import sqlite3
import textwrap
from pathlib import Path
from dataclasses import replace

import pytest

from bithumb_bot.core import sma_policy
from bithumb_bot.core.sma_policy import (
    EntryExecutionIntent,
    ExecutionConstraintSnapshot,
    MarketWindow,
    PositionSnapshot,
    SmaPolicyConfig,
    evaluate_sma_policy,
)
from bithumb_bot.canonical_decision import export_runtime_replay_decisions
from bithumb_bot.market_regime import MARKET_REGIME_VERSION
from bithumb_bot.strategy_plugins.sma_with_filter_events import (
    SmaWithFilterDecisionAdapter,
    build_sma_with_filter_research_events,
)
from bithumb_bot.research.backtest_engine import run_sma_backtest
from bithumb_bot.research import backtest_kernel
from bithumb_bot.research import risk_gate_stage
from bithumb_bot.research import sma_with_filter_plugin
from bithumb_bot.research.backtest_runner import run_plugin_backtest
from bithumb_bot.research.backtest_types import BacktestRunContext
from bithumb_bot.strategy_plugins.sma_with_filter_assembly import (
    MaterializationMode,
    SmaWithFilterPolicyAssembly,
)
from bithumb_bot.strategy_plugins.sma_with_filter_contract import SMA_DECISION_EVIDENCE_CONTRACT
from bithumb_bot.strategy_decision_service import (
    StrategyDecisionService,
    StrategyEvaluationRequest,
)
from bithumb_bot.strategy_decision_input import StrategyDecisionInputBundle
from bithumb_bot.research.strategy_spec import runtime_bound_behavior_parameter_names
from bithumb_bot.research.dataset_snapshot import Candle, DatasetSnapshot
from bithumb_bot.research.decision_event import ResearchDecisionEvent
from bithumb_bot.research.experiment_manifest import (
    DateRange,
    ExecutionTimingPolicy,
    PortfolioPolicy,
    PositionSizingPolicy,
)
from bithumb_bot import engine
from bithumb_bot.runtime_decision_service import (
    compute_legacy_signal_for_diagnostics,
    compute_strategy_decision_for_diagnostics,
)
from bithumb_bot import runtime_position_state_normalizer
from bithumb_bot import runtime_sma_snapshot
from bithumb_bot import runtime_sma_snapshot_builder as runtime_sma
from bithumb_bot import runtime_strategy_decision
from bithumb_bot.runtime_adapters import sma_with_filter as runtime_sma_adapter
from bithumb_bot.runtime_strategy_set import (
    RuntimeDecisionGateway,
    RuntimeMarketScope,
    RuntimeStrategySet,
    RuntimeStrategySpec,
)
from bithumb_bot.strategy_plugins.sma_with_filter_plugin import SMA_WITH_FILTER_PLUGIN
from bithumb_bot.strategy import sma as strategy_sma
from bithumb_bot.strategy.sma import (
    SmaWithFilterStrategy,
    create_sma_with_filter_strategy,
)
from bithumb_bot.compat.sma_legacy_adapter import SmaCrossStrategy
from bithumb_bot.strategy.exit_rules import ExitPolicyConfig, evaluate_sma_exit_policy
from bithumb_bot.strategy.sma_decision_assembler import evaluate_sma_final_decision


def _policy_config() -> SmaPolicyConfig:
    return SmaPolicyConfig(
        strategy_name="sma_with_filter",
        short_n=2,
        long_n=3,
        min_gap_ratio=0.0,
        volatility_window=3,
        min_volatility_ratio=0.0,
        overextended_lookback=1,
        overextended_max_return_ratio=0.0,
        slippage_bps=0.0,
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        cost_edge_min_ratio=0.0,
        market_regime_enabled=False,
        buy_fraction=0.99,
        max_order_krw=100_000.0,
    )


def _market_window() -> MarketWindow:
    closes = (10.0, 10.0, 10.0, 10.0, 11.0)
    return MarketWindow(
        pair="BTC_KRW",
        interval="1m",
        candle_ts=1_700_000_240_000,
        closes=closes,
        prev_s=10.0,
        prev_l=10.0,
        curr_s=10.5,
        curr_l=10.333333333333334,
    )


def _flat_position() -> PositionSnapshot:
    return PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False)


def _open_position(**overrides: object) -> PositionSnapshot:
    payload = {
        "in_position": True,
        "entry_allowed": False,
        "exit_allowed": True,
        "entry_block_reason": "open_exposure",
        "terminal_state": "open_exposure",
        "entry_ts": 1_700_000_000_000,
        "entry_price": 10.0,
        "qty_open": 1.0,
        "holding_time_sec": 60.0,
        "unrealized_pnl": 1.0,
        "unrealized_pnl_ratio": 0.1,
        "raw_qty_open": 1.0,
        "raw_total_asset_qty": 1.0,
        "open_lot_count": 1,
        "sellable_executable_lot_count": 1,
        "effective_flat": False,
        "has_executable_exposure": True,
        "has_any_position_residue": True,
    }
    payload.update(overrides)
    return PositionSnapshot(**payload)  # type: ignore[arg-type]


def _exit_policy_config(**overrides: object) -> ExitPolicyConfig:
    payload = {
        "rule_names": ("stop_loss", "opposite_cross", "max_holding_time"),
        "stop_loss_ratio": 0.05,
        "max_holding_sec": 3_600.0,
        "min_take_profit_ratio": 0.0,
        "small_loss_tolerance_ratio": 0.0,
        "live_fee_rate_estimate": 0.0,
    }
    payload.update(overrides)
    return ExitPolicyConfig(**payload)  # type: ignore[arg-type]


def _allowing_policy() -> dict[str, object]:
    return {
        "regime_classifier_version": MARKET_REGIME_VERSION,
        "allowed_regimes": [
            "uptrend_high_vol_unknown",
            "uptrend_normal_vol_unknown",
            "uptrend_low_vol_unknown",
            "uptrend_high_vol_volume_normal",
            "trend_up",
        ],
        "blocked_regimes": [],
        "regime_evidence": {},
    }


def _runtime_bound_sma_parameters(**overrides: object) -> dict[str, object]:
    params: dict[str, object] = {
        "SMA_SHORT": 2,
        "SMA_LONG": 3,
        "SMA_FILTER_GAP_MIN_RATIO": 0.0,
        "SMA_FILTER_VOL_WINDOW": 3,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
        "SMA_FILTER_OVEREXT_LOOKBACK": 1,
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
        "SMA_MARKET_REGIME_ENABLED": False,
        "SMA_COST_EDGE_ENABLED": False,
        "SMA_COST_EDGE_MIN_RATIO": 0.0,
        "ENTRY_EDGE_BUFFER_RATIO": 0.0,
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": 0.0,
        "STRATEGY_ENTRY_SLIPPAGE_BPS": 0.0,
        "LIVE_FEE_RATE_ESTIMATE": 0.0,
        "STRATEGY_EXIT_RULES": "stop_loss,opposite_cross,max_holding_time",
        "STRATEGY_EXIT_STOP_LOSS_RATIO": 0.05,
        "STRATEGY_EXIT_MAX_HOLDING_MIN": 60,
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0.0,
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0.0,
        "BUY_FRACTION": 0.99,
        "MAX_ORDER_KRW": 100_000.0,
    }
    params.update(overrides)
    return params


def test_evaluate_sma_policy_is_deterministic_for_same_snapshot() -> None:
    first = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    second = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert first == second
    assert first.policy_hash == second.policy_hash
    assert first.raw_signal == "BUY"
    assert first.final_signal == "BUY"


def test_sma_policy_assembly_covers_runtime_bound_parameters_and_payload() -> None:
    assembly = SmaWithFilterPolicyAssembly()
    params = _runtime_bound_sma_parameters(STRATEGY_MIN_EXPECTED_EDGE_RATIO=0.001)
    materialized = assembly.materialize_parameters(params, MaterializationMode.RUNTIME_REPLAY)
    strategy = assembly.build_strategy(
        materialized,
        pair="BTC_KRW",
        interval="1m",
        candidate_regime_policy=_allowing_policy(),
    )
    config = assembly.build_policy_config(
        materialized,
        strategy,
        candidate_regime_policy=_allowing_policy(),
    )
    market = assembly.build_market_snapshot(
        pair="BTC_KRW",
        interval="1m",
        candle_ts=1_700_000_240_000,
        closes=(10.0, 10.0, 10.0, 10.0, 11.0),
        prev_s=10.0,
        prev_l=10.0,
        curr_s=10.5,
        curr_l=10.333333333333334,
    )
    execution = ExecutionConstraintSnapshot(fee_rate_for_decision=0.0)
    exit_config = assembly.build_exit_policy_config(materialized, fee_rate_for_decision=0.0)
    payload = assembly.policy_input_payload(
        materialized=materialized,
        market=market,
        position=_flat_position(),
        policy_config=config,
        execution_context=execution,
        exit_policy_config=exit_config,
    )

    runtime_bound = set(runtime_bound_behavior_parameter_names("sma_with_filter"))
    assert runtime_bound <= set(materialized.values)
    assert runtime_bound <= set(payload["parameters"])
    assert runtime_bound <= set(payload["materialized_parameters"]["values"])
    assert strategy.short_n == materialized.values["SMA_SHORT"]
    assert strategy.long_n == materialized.values["SMA_LONG"]
    assert config.policy_input_payload()["strategy_min_expected_edge_ratio"] == 0.001
    assert config.policy_input_payload()["buy_fraction"] == 0.99
    assert config.policy_input_payload()["max_order_krw"] == 100_000.0
    assert payload["materialized_parameters_hash"].startswith("sha256:")
    assert payload["exit_policy_hash"].startswith("sha256:")


def test_promotion_and_runtime_assembly_have_equivalent_candidate_policy_hashes() -> None:
    assembly = SmaWithFilterPolicyAssembly()
    params = _runtime_bound_sma_parameters()
    decisions = []
    for mode in (MaterializationMode.RESEARCH_PROMOTION, MaterializationMode.RUNTIME_REPLAY):
        materialized = assembly.materialize_parameters(params, mode)
        strategy = assembly.build_strategy(
            materialized,
            pair="BTC_KRW",
            interval="1m",
            candidate_regime_policy=_allowing_policy(),
        )
        decisions.append(
            strategy.decide_snapshot(
                market=assembly.build_market_snapshot(
                    pair="BTC_KRW",
                    interval="1m",
                    candle_ts=1_700_000_240_000,
                    closes=(10.0, 10.0, 10.0, 10.0, 11.0),
                    prev_s=10.0,
                    prev_l=10.0,
                    curr_s=10.5,
                    curr_l=10.333333333333334,
                ),
                position=_flat_position(),
                config=assembly.build_policy_config(
                    materialized,
                    strategy,
                    candidate_regime_policy=_allowing_policy(),
                ),
                execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
                exit_policy_config=assembly.build_exit_policy_config(
                    materialized,
                    fee_rate_for_decision=0.0,
                ),
            )
        )

    research_decision, runtime_decision = decisions
    assert research_decision.policy_input_hash == runtime_decision.policy_input_hash
    assert research_decision.policy_decision_hash == runtime_decision.policy_decision_hash
    assert research_decision.final_signal == runtime_decision.final_signal == "BUY"
    assert research_decision.blocked_filters == runtime_decision.blocked_filters == ()
    status = research_decision.trace["config"]["candidate_regime_policy_status"]
    assert status["candidate_regime_policy_required"] is True
    assert status["candidate_regime_policy_loaded"] is True
    assert status["candidate_regime_policy_valid"] is True
    assert status["candidate_regime_policy_verification_status"] == "verified"
    assert status["candidate_regime_policy_hash"].startswith("sha256:")


def test_candidate_regime_policy_missing_fails_closed_in_comparable_modes() -> None:
    assembly = SmaWithFilterPolicyAssembly()
    materialized = assembly.materialize_parameters(
        _runtime_bound_sma_parameters(SMA_MARKET_REGIME_ENABLED=False),
        MaterializationMode.RESEARCH_PROMOTION,
    )
    strategy = assembly.build_strategy(
        materialized,
        pair="BTC_KRW",
        interval="1m",
        candidate_regime_policy=None,
    )
    decision = strategy.decide_snapshot(
        market=_market_window(),
        position=_flat_position(),
        config=assembly.build_policy_config(materialized, strategy, candidate_regime_policy=None),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=assembly.build_exit_policy_config(materialized, fee_rate_for_decision=0.0),
    )

    assert decision.final_signal == "HOLD"
    assert decision.entry_blocked is True
    assert decision.entry_decision.candidate_regime_triggered is True
    assert decision.entry_decision.candidate_regime_decision["regime_block_reason"] == (
        "regime_policy_missing"
    )
    status = decision.trace["config"]["candidate_regime_policy_status"]
    assert status["candidate_regime_policy_required"] is True
    assert status["candidate_regime_policy_loaded"] is False
    assert status["candidate_regime_policy_valid"] is False
    assert status["candidate_regime_policy_verification_status"] == "fail_closed_missing"


def test_sma_promotion_service_requires_canonical_decision_input_bundle() -> None:
    assembly = SmaWithFilterPolicyAssembly()
    materialized = assembly.materialize_parameters(
        _runtime_bound_sma_parameters(SMA_MARKET_REGIME_ENABLED=False),
        MaterializationMode.RESEARCH_PROMOTION,
    )
    strategy = assembly.build_strategy(
        materialized,
        pair="BTC_KRW",
        interval="1m",
        candidate_regime_policy=_allowing_policy(),
    )
    with pytest.raises(ValueError, match="strategy_evaluation_decision_input_bundle_missing:sma_with_filter"):
        StrategyDecisionService().evaluate(
            StrategyEvaluationRequest(
                strategy_name="sma_with_filter",
                strategy_instance_id="unit",
                mode="research_promotion",
                strategy_policy=strategy,
                market_snapshot=_market_window(),
                position_snapshot=_flat_position(),
                strategy_config=assembly.build_policy_config(
                    materialized,
                    strategy,
                    candidate_regime_policy=_allowing_policy(),
                ),
                execution_constraints=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
                exit_policy_config=assembly.build_exit_policy_config(
                    materialized,
                    fee_rate_for_decision=0.0,
                ),
                rule_sources={},
                approved_profile_hash=None,
                runtime_contract_hash=None,
                plugin_contract_hash=None,
                request_hash=None,
                provenance={
                    "strategy_parameters_hash": "sha256:unit",
                    "approved_profile_hash_unavailable_reason": "unit",
                    "runtime_contract_hash_unavailable_reason": "unit",
                    "plugin_contract_hash_unavailable_reason": "unit",
                    "runtime_decision_request_hash_unavailable_reason": "unit",
                },
                decision_evidence_contract=SMA_DECISION_EVIDENCE_CONTRACT,
            )
        )


def test_live_real_order_service_rejects_unavailable_provenance_before_submit_authority() -> None:
    assembly = SmaWithFilterPolicyAssembly()
    materialized = assembly.materialize_parameters(
        _runtime_bound_sma_parameters(SMA_MARKET_REGIME_ENABLED=False),
        MaterializationMode.LIVE_REAL_ORDER,
    )
    strategy = assembly.build_strategy(
        materialized,
        pair="BTC_KRW",
        interval="1m",
        candidate_regime_policy=_allowing_policy(),
    )
    market = _market_window()
    position = _flat_position()
    config = assembly.build_policy_config(
        materialized,
        strategy,
        candidate_regime_policy=_allowing_policy(),
    )
    execution = ExecutionConstraintSnapshot(
        fee_rate_for_decision=0.0,
        fee_authority={"source": "unit"},
        order_rules={"source": "unit"},
    )
    exit_config = assembly.build_exit_policy_config(materialized, fee_rate_for_decision=0.0)
    bundle = StrategyDecisionInputBundle.build(
        strategy_name="sma_with_filter",
        market=market,
        position=position,
        config=config,
        execution_constraints=execution,
        exit_policy_config=exit_config,
        materialized_parameters_hash="sha256:" + "a" * 64,
        snapshot_projector_version="sma_with_filter_snapshot_projector_v1",
        snapshot_projector_hash="sha256:" + "b" * 64,
        provenance={"unit": True},
    )

    with pytest.raises(
        ValueError,
        match="strategy_evaluation_live_real_order_provenance_missing:.*plugin_contract_hash_unavailable_reason",
    ):
        StrategyDecisionService().evaluate(
            StrategyEvaluationRequest(
                strategy_name="sma_with_filter",
                strategy_instance_id="unit",
                mode="live_real_order",
                strategy_policy=strategy,
                market_snapshot=market,
                position_snapshot=position,
                strategy_config=config,
                execution_constraints=execution,
                exit_policy_config=exit_config,
                rule_sources={},
                approved_profile_hash="sha256:" + "c" * 64,
                runtime_contract_hash="sha256:" + "d" * 64,
                plugin_contract_hash="sha256:" + "e" * 64,
                request_hash="sha256:" + "f" * 64,
                provenance={
                    "strategy_parameters_hash": "sha256:" + "1" * 64,
                    "fee_authority_hash": "sha256:" + "2" * 64,
                    "order_rules_hash": "sha256:" + "3" * 64,
                    "plugin_contract_hash_unavailable_reason": "diagnostic_forbidden_in_live_real_order",
                },
                decision_input_bundle=bundle,
                decision_evidence_contract=SMA_DECISION_EVIDENCE_CONTRACT,
            )
        )


def test_candidate_regime_policy_blocks_buy_equally_in_promotion_and_runtime_replay() -> None:
    assembly = SmaWithFilterPolicyAssembly()
    params = _runtime_bound_sma_parameters(SMA_MARKET_REGIME_ENABLED=False)
    blocking_policy = {
        "regime_classifier_version": MARKET_REGIME_VERSION,
        "allowed_regimes": [],
        "blocked_regimes": [
            "uptrend_high_vol_unknown",
            "uptrend_normal_vol_unknown",
            "uptrend_low_vol_unknown",
        ],
        "missing_policy_behavior": "fail_closed",
    }
    decisions = []
    for mode in (MaterializationMode.RESEARCH_PROMOTION, MaterializationMode.RUNTIME_REPLAY):
        materialized = assembly.materialize_parameters(params, mode)
        strategy = assembly.build_strategy(
            materialized,
            pair="BTC_KRW",
            interval="1m",
            candidate_regime_policy=blocking_policy,
        )
        decisions.append(
            strategy.decide_snapshot(
                market=_market_window(),
                position=_flat_position(),
                config=assembly.build_policy_config(
                    materialized,
                    strategy,
                    candidate_regime_policy=blocking_policy,
                ),
                execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
                exit_policy_config=assembly.build_exit_policy_config(
                    materialized,
                    fee_rate_for_decision=0.0,
                ),
            )
        )

    research_decision, runtime_decision = decisions
    assert research_decision.final_signal == runtime_decision.final_signal == "HOLD"
    assert research_decision.blocked_filters == runtime_decision.blocked_filters
    assert research_decision.entry_decision.candidate_regime_triggered is True
    assert runtime_decision.entry_decision.candidate_regime_triggered is True
    assert research_decision.policy_input_hash == runtime_decision.policy_input_hash
    assert research_decision.policy_decision_hash == runtime_decision.policy_decision_hash


def test_legacy_exploratory_defaults_are_marked_non_runtime_comparable() -> None:
    assembly = SmaWithFilterPolicyAssembly()
    exploratory = assembly.materialize_parameters(
        {"SMA_SHORT": 2, "SMA_LONG": 3},
        MaterializationMode.RESEARCH_EXPLORATORY,
    )
    assert exploratory.runtime_comparable is False
    assert "SMA_FILTER_GAP_MIN_RATIO" in exploratory.legacy_defaults_used

    with pytest.raises(Exception, match="runtime_bound_parameter_missing"):
        assembly.materialize_parameters(
            {"SMA_SHORT": 2, "SMA_LONG": 3},
            MaterializationMode.RESEARCH_PROMOTION,
        )


def test_sma_policy_hash_changes_for_equivalence_inputs() -> None:
    assembly = SmaWithFilterPolicyAssembly()
    params = _runtime_bound_sma_parameters(SMA_MARKET_REGIME_ENABLED=False)

    def _decision(**overrides: object):
        candidate = overrides.pop("candidate_regime_policy", _allowing_policy())
        materialized = assembly.materialize_parameters({**params, **overrides}, MaterializationMode.RUNTIME_REPLAY)
        strategy = assembly.build_strategy(
            materialized,
            pair="BTC_KRW",
            interval="1m",
            candidate_regime_policy=candidate,
        )
        return strategy.decide_snapshot(
            market=_market_window(),
            position=_flat_position(),
            config=assembly.build_policy_config(
                materialized,
                strategy,
                candidate_regime_policy=candidate,
            ),
            execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
            exit_policy_config=assembly.build_exit_policy_config(materialized, fee_rate_for_decision=0.0),
        )

    base = _decision()
    changed_regime = _decision(SMA_MARKET_REGIME_ENABLED=True)
    changed_sizing = _decision(BUY_FRACTION=0.5)
    changed_max_order = _decision(MAX_ORDER_KRW=50_000.0)
    changed_exit = _decision(STRATEGY_EXIT_STOP_LOSS_RATIO=0.10)
    changed_candidate = _decision(
        candidate_regime_policy={**_allowing_policy(), "allowed_regimes": ["sideways_normal_vol_unknown"]}
    )

    assert changed_regime.policy_input_hash != base.policy_input_hash
    assert changed_sizing.policy_input_hash != base.policy_input_hash
    assert changed_max_order.policy_input_hash != base.policy_input_hash
    assert changed_exit.policy_input_hash != base.policy_input_hash
    assert changed_candidate.policy_input_hash != base.policy_input_hash


def test_sma_policy_hash_changes_for_requirement_and_fee_authority_status() -> None:
    assembly = SmaWithFilterPolicyAssembly()
    materialized = assembly.materialize_parameters(
        _runtime_bound_sma_parameters(SMA_MARKET_REGIME_ENABLED=False),
        MaterializationMode.RUNTIME_REPLAY,
    )
    strategy = assembly.build_strategy(
        materialized,
        pair="BTC_KRW",
        interval="1m",
        candidate_regime_policy=_allowing_policy(),
    )
    market = _market_window()
    position = _flat_position()
    exit_config = assembly.build_exit_policy_config(materialized, fee_rate_for_decision=0.0)

    required = strategy.decide_snapshot(
        market=market,
        position=position,
        config=assembly.build_policy_config(
            materialized,
            strategy,
            candidate_regime_policy=_allowing_policy(),
        ),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=exit_config,
    )
    not_required = strategy.decide_snapshot(
        market=market,
        position=position,
        config=assembly.build_policy_config(
            materialized,
            strategy,
            candidate_regime_policy=_allowing_policy(),
            candidate_regime_policy_enforced=False,
        ),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=exit_config,
    )
    degraded_fee = strategy.decide_snapshot(
        market=market,
        position=position,
        config=assembly.build_policy_config(
            materialized,
            strategy,
            candidate_regime_policy=_allowing_policy(),
        ),
        execution_context=ExecutionConstraintSnapshot(
            fee_rate_for_decision=0.0,
            fee_authority_degraded_blocks_entry=True,
            fee_authority={"degraded": True, "degraded_reason": "test"},
        ),
        exit_policy_config=exit_config,
    )

    assert not_required.policy_input_hash != required.policy_input_hash
    assert degraded_fee.policy_input_hash != required.policy_input_hash


def test_sma_assembly_static_guard_blocks_duplicate_policy_construction() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    guarded = (
        repo_root / "src/bithumb_bot/research/sma_with_filter_plugin.py",
        repo_root / "src/bithumb_bot/runtime_adapters/sma_with_filter.py",
        repo_root / "src/bithumb_bot/runtime_sma_snapshot_builder.py",
    )
    forbidden = {
        "SmaPolicyConfig",
        "MarketWindow",
        "ExitPolicyConfig",
        "create_sma_with_filter_strategy",
    }
    violations: list[str] = []
    for path in guarded:
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
                if name in forbidden:
                    violations.append(f"{path.relative_to(repo_root)}:{name}")

    assert violations == []


def test_sma_assembly_authority_is_plugin_owned() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    assembly_path = repo_root / "src/bithumb_bot/strategy_plugins/sma_with_filter_assembly.py"
    shim_path = repo_root / "src/bithumb_bot/research/sma_policy_assembly.py"

    assert assembly_path.exists()
    assert "SmaWithFilterPolicyAssembly" in assembly_path.read_text()
    shim_source = shim_path.read_text()
    assert "Compatibility import" in shim_source
    assert "from bithumb_bot.strategy_plugins.sma_with_filter_assembly import" in shim_source


def test_promotion_grade_plugin_isolates_legacy_exit_callbacks_to_exploratory_scope() -> None:
    contract = SMA_WITH_FILTER_PLUGIN.contract_payload()

    assert contract["decision_payload_adapter_authority_scope"] == (
        "transform_strategy_decision_v2_or_verified_canonical_artifact_only"
    )
    assert contract["exit_signal_context_builder_authority_scope"] == (
        "research_exploratory_compatibility_only"
    )
    assert contract["exit_rule_factory_authority_scope"] == (
        "research_exploratory_compatibility_only"
    )


def test_promotion_runtime_paper_live_paths_do_not_call_exploratory_exit_callbacks() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    guarded = (
        repo_root / "src/bithumb_bot/research/backtest_stage_runner.py",
        repo_root / "src/bithumb_bot/research/strategy_evaluator_stage.py",
        repo_root / "src/bithumb_bot/runtime_sma_snapshot_builder.py",
        repo_root / "src/bithumb_bot/runtime_adapters/sma_with_filter.py",
        repo_root / "src/bithumb_bot/runtime/decision_coordinator.py",
        repo_root / "src/bithumb_bot/runtime/runner.py",
        repo_root / "src/bithumb_bot/run_loop_execution_planner.py",
        repo_root / "src/bithumb_bot/execution_service.py",
    )
    forbidden_attrs = {"exit_signal_context_builder", "exit_rule_factory"}
    violations: list[str] = []
    for path in guarded:
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in forbidden_attrs:
                violations.append(f"{path.relative_to(repo_root)}:{node.attr}")

    assert violations == []


def test_adapter_and_builder_layers_do_not_recreate_sma_signal_authority() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    guarded = (
        repo_root / "src/bithumb_bot/runtime_adapters/sma_with_filter.py",
        repo_root / "src/bithumb_bot/runtime_sma_snapshot_builder.py",
        repo_root / "src/bithumb_bot/research/sma_with_filter_plugin.py",
        repo_root / "src/bithumb_bot/research/strategy_evaluator_stage.py",
    )
    violations: list[str] = []
    for path in guarded:
        source = path.read_text()
        if "curr_s > curr_l" in source:
            violations.append(f"{path.relative_to(repo_root)}:curr_s > curr_l")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "evaluate":
                    owner = getattr(node.func.value, "id", "")
                    if owner == "rule":
                        violations.append(f"{path.relative_to(repo_root)}:rule.evaluate")
            if isinstance(node, ast.Assign):
                targets = [
                    target.id for target in node.targets if isinstance(target, ast.Name)
                ]
                if "final_signal" in targets:
                    violations.append(f"{path.relative_to(repo_root)}:assign final_signal")

    assert violations == []


def test_risk_gate_legacy_exit_reevaluation_is_research_exploratory_only() -> None:
    source = inspect.getsource(risk_gate_stage.DefaultRiskGate)
    assert "research_exploratory_compatibility" in source
    assert '== "research_exploratory"' in source
    assert "and research_exploratory_compatibility" in source


def test_evaluate_sma_policy_has_no_runtime_dependency_imports_or_side_effect_surfaces() -> None:
    source = inspect.getsource(sma_policy)
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])

    assert "sqlite3" not in imported_roots
    assert "time" not in imported_roots
    assert "bithumb_client" not in imported_roots
    assert "notifier" not in imported_roots
    assert "settings" not in source
    assert "conn." not in source
    assert "commit(" not in source


def test_evaluate_sma_policy_open_position_defers_exit_to_wrapper_without_entry_authority() -> None:
    decision = evaluate_sma_policy(
        market=_market_window(),
        position=PositionSnapshot(
            in_position=True,
            entry_allowed=False,
            exit_allowed=True,
            terminal_state="open_exposure",
            qty_open=1.0,
            open_lot_count=1,
            sellable_executable_lot_count=1,
            has_executable_exposure=True,
        ),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert decision.raw_signal == "BUY"
    assert decision.entry_signal == "BUY"
    assert decision.final_signal == "HOLD"
    assert decision.final_reason == "position held: exit policy evaluation required"
    assert decision.trace["position"]["terminal_state"] == "open_exposure"


def test_final_sma_decision_assembler_is_deterministic_and_hashes_policy_material() -> None:
    first = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    second = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    changed = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.001),
        exit_policy_config=_exit_policy_config(),
    )

    assert first == second
    assert first.final_signal == "BUY"
    assert isinstance(first.execution_intent, EntryExecutionIntent)
    assert first.execution_intent.as_dict() == {
        "schema_version": 1,
        "intent_version": 1,
        "side": "BUY",
        "intent": "enter_open_exposure",
        "pair": "BTC_KRW",
        "budget_model": "cash_fraction_capped_by_max_order_krw",
        "budget_fraction_of_cash": 0.99,
        "max_budget_krw": 100_000.0,
        "requires_execution_sizing": True,
    }
    assert first.policy_contract_hash == second.policy_contract_hash
    assert first.policy_input_hash == second.policy_input_hash
    assert first.policy_decision_hash == second.policy_decision_hash
    assert changed.policy_input_hash != first.policy_input_hash


def test_execution_intent_v1_serialization_is_stable() -> None:
    first = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    second = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )

    assert first.execution_intent is not None
    assert second.execution_intent is not None
    assert first.execution_intent.as_dict() == second.execution_intent.as_dict()
    assert first.policy_decision_hash == second.policy_decision_hash
    assert first.as_trace()["execution_intent"] == first.execution_intent.as_dict()


def test_policy_hashes_ignore_transient_fee_authority_timestamps() -> None:
    base_fee_authority = {
        "fee_source": "order_rules",
        "taker_bid_fee_rate": 0.001,
        "taker_ask_fee_rate": 0.001,
        "retrieved_at_sec": 1_700_000_000,
        "expires_at_sec": 1_700_000_300,
    }
    first = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(
            fee_rate_for_decision=0.001,
            fee_authority=base_fee_authority,
        ),
    )
    second = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(
            fee_rate_for_decision=0.001,
            fee_authority={
                **base_fee_authority,
                "retrieved_at_sec": 1_700_000_200,
                "expires_at_sec": 1_700_000_500,
            },
        ),
    )

    assert second.policy_input_hash == first.policy_input_hash
    assert second.policy_decision_hash == first.policy_decision_hash


def test_policy_hashes_normalize_research_runtime_comparable_terminal_states() -> None:
    runtime_flat = evaluate_sma_policy(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    research_flat = evaluate_sma_policy(
        market=_market_window(),
        position=replace(_flat_position(), terminal_state="research_simulated_flat"),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    runtime_open = evaluate_sma_policy(
        market=_market_window(),
        position=replace(_open_position(), terminal_state="open_exposure"),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    research_open = evaluate_sma_policy(
        market=_market_window(),
        position=replace(_open_position(), terminal_state="research_simulated_open_exposure"),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert research_flat.trace["position"]["terminal_state"] == "research_simulated_flat"
    assert runtime_flat.policy_input_hash == research_flat.policy_input_hash
    assert runtime_flat.policy_decision_hash == research_flat.policy_decision_hash
    assert runtime_flat.final_signal == research_flat.final_signal
    assert list(runtime_flat.blocked_filters) == list(research_flat.blocked_filters)
    assert runtime_flat.trace.get("execution_intent") == research_flat.trace.get("execution_intent")
    assert runtime_open.policy_input_hash == research_open.policy_input_hash
    assert runtime_open.policy_decision_hash == research_open.policy_decision_hash
    assert runtime_open.final_signal == research_open.final_signal
    assert list(runtime_open.blocked_filters) == list(research_open.blocked_filters)
    assert runtime_open.trace.get("execution_intent") == research_open.trace.get("execution_intent")

    runtime_final = evaluate_sma_final_decision(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    research_final = evaluate_sma_final_decision(
        market=_market_window(),
        position=replace(_flat_position(), terminal_state="research_simulated_flat"),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )
    assert runtime_final.policy_decision_hash == research_final.policy_decision_hash


def test_final_sma_decision_assembler_owns_opposite_cross_sell() -> None:
    decision = evaluate_sma_final_decision(
        market=MarketWindow(
            pair="BTC_KRW",
            interval="1m",
            candle_ts=1_700_000_240_000,
            closes=(12.0, 12.0, 12.0, 12.0, 11.0),
            prev_s=12.0,
            prev_l=11.5,
            curr_s=11.0,
            curr_l=11.5,
        ),
        position=_open_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(rule_names=("opposite_cross",), stop_loss_ratio=0.0),
    )

    assert decision.raw_signal == "SELL"
    assert decision.final_signal == "SELL"
    assert decision.exit_rule == "opposite_cross"
    assert decision.protective_exit_overrode_entry is False
    assert decision.exit_filter_suppression_prevented is False


def test_final_sma_decision_assembler_owns_protective_stop_loss_override() -> None:
    decision = evaluate_sma_final_decision(
        market=_market_window(),
        position=_open_position(unrealized_pnl=-1.0, unrealized_pnl_ratio=-0.1),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(rule_names=("stop_loss",)),
    )

    assert decision.raw_signal == "BUY"
    assert decision.final_signal == "SELL"
    assert decision.exit_rule == "stop_loss"
    assert decision.protective_exit_overrode_entry is True


def test_final_sma_decision_assembler_owns_protective_max_holding_override() -> None:
    decision = evaluate_sma_final_decision(
        market=_market_window(),
        position=_open_position(holding_time_sec=7_200.0),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(
            rule_names=("max_holding_time",),
            max_holding_sec=3_600.0,
            stop_loss_ratio=0.0,
        ),
    )

    assert decision.raw_signal == "BUY"
    assert decision.final_signal == "SELL"
    assert decision.exit_rule == "max_holding_time"
    assert decision.protective_exit_overrode_entry is True


def test_snapshot_strategy_policy_decides_without_sqlite() -> None:
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    decision = strategy.decide_snapshot(
        market=_market_window(),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert decision.final_signal == "BUY"
    assert decision.policy_hash.startswith("sha256:")


def test_sma_strategy_snapshot_api_returns_final_decision_with_exit_policy() -> None:
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        exit_rule_names=["stop_loss"],
        exit_stop_loss_ratio=0.05,
    )

    entry_only = strategy.decide_entry_snapshot(
        market=_market_window(),
        position=_open_position(unrealized_pnl=-1.0, unrealized_pnl_ratio=-0.1),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )
    final = strategy.decide_snapshot(
        market=_market_window(),
        position=_open_position(unrealized_pnl=-1.0, unrealized_pnl_ratio=-0.1),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert entry_only.raw_signal == "BUY"
    assert final.final_signal == "SELL"
    assert final.exit_rule == "stop_loss"
    assert final.execution_intent is not None
    assert final.execution_intent.side == "SELL"


def _build_candle_db(closes: list[float]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE candles (
            ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            interval TEXT NOT NULL,
            high REAL,
            low REAL,
            volume REAL,
            close REAL NOT NULL
        )
        """
    )
    base_ts = 1_700_000_000_000
    for idx, close in enumerate(closes):
        ts = base_ts + idx * 60_000
        for pair in ("BTC_KRW", "KRW-BTC"):
            conn.execute(
                "INSERT INTO candles(ts, pair, interval, high, low, volume, close) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ts, pair, "1m", close, close, 1.0, close),
            )
    conn.commit()
    return conn


def _dataset_from_closes(closes: list[float]) -> DatasetSnapshot:
    base_ts = 1_700_000_000_000
    candles = tuple(
        Candle(base_ts + index * 60_000, float(close), float(close), float(close), float(close), 1.0)
        for index, close in enumerate(closes)
    )
    return DatasetSnapshot(
        snapshot_id="sma_policy_boundary_unit",
        source="unit",
        market="BTC_KRW",
        interval="1m",
        split_name="validation",
        date_range=DateRange("2026-01-01", "2026-01-02"),
        candles=candles,
    )


def test_live_wrapper_and_research_adapter_share_policy_entry_boundary() -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    conn = _build_candle_db(closes)
    try:
        runtime_decision = runtime_sma.decide_sma_with_filter_snapshot_from_db(
            conn,
            create_sma_with_filter_strategy(
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
                live_fee_rate_estimate=0.0,
                entry_edge_buffer_ratio=0.0,
                cost_edge_enabled=False,
                market_regime_enabled=False,
                candidate_regime_policy=_allowing_policy(),
            ),
        )
    finally:
        conn.close()

    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes(closes))
    research_event = events[-1]
    research_decision = sma_with_filter_plugin.research_policy_decision_builder(
        event=research_event,
        dataset=_dataset_from_closes(closes),
        candle_index=len(closes) - 1,
        position=_flat_position(),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        active_exit_policy={"rules": ()},
        buy_fraction=0.99,
    )

    assert runtime_decision is not None
    assert research_decision is not None
    assert runtime_decision.context["raw_signal"] == research_decision.raw_signal == "BUY"
    assert runtime_decision.context["entry_signal"] == research_decision.entry_signal == "BUY"
    assert runtime_decision.context["final_signal"] == research_decision.final_signal == "BUY"
    assert tuple(runtime_decision.context["blocked_filters"]) == research_decision.blocked_filters == ()
    assert runtime_decision.context["pure_policy_hash"].startswith("sha256:")
    assert "pure_policy_hash" not in research_event.extra_payload


def test_runtime_db_and_research_adapter_policy_input_hashes_are_non_comparable_without_live_constraints() -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    conn = _build_candle_db(closes)
    try:
        runtime_decision = runtime_sma.decide_sma_with_filter_snapshot_from_db(
            conn,
            create_sma_with_filter_strategy(
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
                live_fee_rate_estimate=0.0,
                entry_edge_buffer_ratio=0.0,
                cost_edge_enabled=False,
                market_regime_enabled=False,
                candidate_regime_policy=_allowing_policy(),
            ),
            through_ts_ms=1_700_000_240_000,
        )
    finally:
        conn.close()

    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes(closes))
    research_event = events[-1]
    research_decision = sma_with_filter_plugin.research_policy_decision_builder(
        event=research_event,
        dataset=_dataset_from_closes(closes),
        candle_index=len(closes) - 1,
        position=_flat_position(),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        active_exit_policy={"rules": ()},
        buy_fraction=0.99,
    )

    assert runtime_decision is not None
    assert research_decision is not None
    runtime_trace = runtime_decision.context["pure_policy_trace"]
    research_trace = research_decision.as_trace()
    assert runtime_trace["market"]["pair"] == research_trace["market"]["pair"] == "BTC_KRW"
    assert runtime_trace["market"]["interval"] == research_trace["market"]["interval"] == "1m"
    assert runtime_trace["market"]["candle_ts"] == research_trace["market"]["candle_ts"]
    assert runtime_trace["market"]["last_close"] == research_trace["market"]["last_close"]
    assert runtime_trace["final_signal"] == research_trace["final_signal"] == "BUY"
    assert runtime_trace["final_reason"] == research_trace["final_reason"]
    assert runtime_decision.context["policy_input_hash"].startswith("sha256:")
    assert runtime_decision.context["policy_decision_hash"].startswith("sha256:")
    assert research_decision.policy_input_hash.startswith("sha256:")
    assert research_decision.policy_decision_hash.startswith("sha256:")
    assert runtime_trace["execution_constraints"]["order_rules"] == (
        research_trace["execution_constraints"]["order_rules"]
    )
    assert runtime_trace["execution_constraints"]["fee_authority"] == (
        research_trace["execution_constraints"]["fee_authority"]
    )
    assert research_trace["position"]["terminal_state"] == "flat"


def test_plugin_owned_sma_event_builder_matches_runtime_replay_decision_hash_with_explicit_scope_boundary() -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    candidate_regime_policy = {
        "regime_classifier_version": MARKET_REGIME_VERSION,
        "allowed_regimes": (
            "uptrend_high_vol_unknown",
            "uptrend_high_vol_volume_normal",
        ),
        "blocked_regimes": (),
        "regime_evidence": {},
    }
    parameter_values = _runtime_bound_sma_parameters(
        STRATEGY_EXIT_STOP_LOSS_RATIO=0.0,
        STRATEGY_EXIT_MAX_HOLDING_MIN=0,
    )
    assembly = SmaWithFilterPolicyAssembly()
    materialized = assembly.materialize_parameters(
        parameter_values,
        MaterializationMode.RUNTIME_REPLAY,
    )
    strategy = assembly.build_strategy(
        materialized,
        pair="BTC_KRW",
        interval="1m",
        candidate_regime_policy=candidate_regime_policy,
    )
    conn = _build_candle_db(closes)
    try:
        runtime_decision = runtime_sma.decide_sma_with_filter_snapshot_from_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_240_000,
        )
    finally:
        conn.close()

    dataset = _dataset_from_closes(closes)
    events = build_sma_with_filter_research_events(
        dataset=dataset,
        parameter_values=parameter_values,
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(),
        portfolio_policy=None,
        context=None,
    )
    research_event = events[-1]
    research_decision = sma_with_filter_plugin.research_policy_decision_builder(
        event=research_event,
        dataset=dataset,
        candle_index=len(closes) - 1,
        position=PositionSnapshot(
            in_position=False,
            entry_allowed=True,
            exit_allowed=False,
            exit_block_reason="no_position",
            terminal_state="flat",
            dust_classification="no_dust",
            dust_state="no_dust",
            effective_flat=True,
        ),
        parameter_values=parameter_values,
        fee_rate=0.0,
        slippage_bps=0.0,
        active_exit_policy={
            "rules": ("stop_loss", "opposite_cross", "max_holding_time"),
            "stop_loss": {"stop_loss_ratio": 0.0},
            "max_holding_time": {"max_holding_min": 0},
            "opposite_cross": {
                "min_take_profit_ratio": 0.0,
                "small_loss_tolerance_ratio": 0.0,
            },
        },
        buy_fraction=0.99,
        materialization_mode=MaterializationMode.RUNTIME_REPLAY,
        candidate_regime_policy=candidate_regime_policy,
        candidate_regime_policy_enforced=True,
    )

    assert runtime_decision is not None
    assert research_decision is not None
    runtime_context = runtime_decision.context
    runtime_trace = runtime_context["pure_policy_trace"]
    research_trace = research_decision.as_trace()
    assert runtime_trace["market"]["candle_ts"] == research_trace["market"]["candle_ts"]
    assert runtime_trace["market"]["last_close"] == research_trace["market"]["last_close"]
    assert runtime_trace["market"]["prev_s"] == research_trace["market"]["prev_s"]
    assert runtime_trace["market"]["prev_l"] == research_trace["market"]["prev_l"]
    assert runtime_trace["market"]["curr_s"] == research_trace["market"]["curr_s"]
    assert runtime_trace["market"]["curr_l"] == research_trace["market"]["curr_l"]
    assert runtime_context["policy_decision_hash"] != research_decision.policy_decision_hash
    assert runtime_trace["final_signal"] == research_trace["final_signal"]
    assert runtime_trace["final_reason"] == research_trace["final_reason"]
    assert runtime_trace["execution_intent"] == research_trace["execution_intent"]

    non_comparable_scope = {
        "policy_input_hash": (
            runtime_context["execution_constraints_hash"],
            research_trace["strategy_evaluation_provenance"]["execution_constraints_hash"],
            "runtime_direct_replay_uses_runtime_order_rule_authority_without_research_export_binding",
        ),
        "replay_fingerprint_hash": (
            runtime_context.get("replay_fingerprint_hash"),
            "research_kernel_only_decision_field",
            "runtime_snapshot_builder_exposes_replay_fingerprint_payload_without_top_level_hash",
        ),
        "execution_submit_plan_hash": (
            runtime_context.get("execution_submit_plan_hash"),
            "research_kernel_only_decision_field",
            "runtime_snapshot_replay_lacks_live_readiness_context_for_submit_plan_reconstruction",
        ),
    }
    assert runtime_context["policy_input_hash"] != research_decision.policy_input_hash
    assert non_comparable_scope["policy_input_hash"] == (
        runtime_context["execution_constraints_hash"],
        research_trace["strategy_evaluation_provenance"]["execution_constraints_hash"],
        "runtime_direct_replay_uses_runtime_order_rule_authority_without_research_export_binding",
    )
    assert runtime_context.get("replay_fingerprint_hash")
    assert non_comparable_scope["replay_fingerprint_hash"] == (
        runtime_context.get("replay_fingerprint_hash"),
        "research_kernel_only_decision_field",
        "runtime_snapshot_builder_exposes_replay_fingerprint_payload_without_top_level_hash",
    )
    assert non_comparable_scope["execution_submit_plan_hash"] == (
        None,
        "research_kernel_only_decision_field",
        "runtime_snapshot_replay_lacks_live_readiness_context_for_submit_plan_reconstruction",
    )


def test_sma_research_promotion_backtest_and_runtime_gateway_paths_share_canonical_hashes() -> None:
    closes = [12.0, 11.0, 10.0, 10.0, 12.0]
    parameters = _runtime_bound_sma_parameters(
        STRATEGY_EXIT_STOP_LOSS_RATIO=0.0,
        STRATEGY_EXIT_MAX_HOLDING_MIN=0,
    )
    parameters.pop("BUY_FRACTION", None)
    parameters.pop("MAX_ORDER_KRW", None)
    candidate_regime_policy = _allowing_policy()
    dataset = _dataset_from_closes(closes)
    run = run_plugin_backtest(
        plugin=SMA_WITH_FILTER_PLUGIN,
        dataset=dataset,
        parameter_values=parameters,
        fee_rate=0.0,
        slippage_bps=0.0,
        execution_timing_policy=ExecutionTimingPolicy(),
        context=BacktestRunContext(
            report_detail="full",
            policy_materialization_mode="research_promotion",
            candidate_regime_policy=candidate_regime_policy,
            candidate_regime_policy_drives_research_execution=True,
        ),
    )
    target_ts = 1_700_000_240_000
    research = next(item for item in run.decisions if int(item["candle_ts"]) == target_ts)

    conn = _build_candle_db(closes)
    runtime_parameters = dict(parameters)
    try:
        bundle = RuntimeDecisionGateway().decide_bundle(
            conn,
            strategy_set=RuntimeStrategySet(
                strategies=(
                    RuntimeStrategySpec(
                            strategy_name="sma_with_filter",
                            pair="BTC_KRW",
                            interval="1m",
                            parameters=runtime_parameters,
                            runtime_adapter_config={
                                "candidate_regime_policy": candidate_regime_policy,
                            },
                            parameter_source="path_level_golden_fixture",
                    ),
                ),
                market_scope=RuntimeMarketScope(pair="BTC_KRW", interval="1m"),
                source="path_level_golden_fixture",
            ),
            through_ts_ms=target_ts,
        )
    finally:
        conn.close()

    assert bundle is not None
    runtime_result = bundle.results[0]
    runtime_context = runtime_result.base_context
    runtime_trace = runtime_context["pure_policy_trace"]

    comparable = {
        "policy_input_hash": (research["policy_input_hash"], runtime_context["policy_input_hash"]),
        "policy_decision_hash": (research["policy_decision_hash"], runtime_context["policy_decision_hash"]),
        "policy_contract_hash": (research["policy_contract_hash"], runtime_context["policy_contract_hash"]),
        "decision_input_bundle_hash": (
            research["decision_input_bundle_hash"],
            runtime_context["decision_input_bundle_hash"],
        ),
        "decision_input_contract_hash": (
            research["decision_input_contract_hash"],
            runtime_context["decision_input_contract_hash"],
        ),
        "decision_input_bundle_payload_hash": (
            research["decision_input_bundle_payload_hash"],
            runtime_context["decision_input_bundle_payload_hash"],
        ),
        "market_feature_hash": (
            research["market_feature_hash"],
            runtime_context["market_feature_hash"],
        ),
        "final_exit_decision_input_hash": (
            research["final_exit_decision_input_hash"],
            runtime_context["final_exit_decision_input_hash"],
        ),
        "snapshot_projector_version": (
            research["snapshot_projector_version"],
            runtime_context["snapshot_projector_version"],
        ),
        "snapshot_projector_hash": (
            research["snapshot_projector_hash"],
            runtime_context["snapshot_projector_hash"],
        ),
        "market_snapshot_hash": (research["market_snapshot_hash"], runtime_context["market_snapshot_hash"]),
        "position_snapshot_hash": (
            research["position_snapshot_hash"],
            runtime_context["position_snapshot_hash"],
        ),
        "execution_constraints_hash": (
            research["execution_constraints_hash"],
            runtime_context["execution_constraints_hash"],
        ),
        "policy_config_hash": (research["policy_config_hash"], runtime_context["policy_config_hash"]),
        "exit_policy_config_hash": (
            research["exit_policy_config_hash"],
            runtime_context["exit_policy_config_hash"],
        ),
        "final_signal": (research["final_signal"], runtime_trace["final_signal"]),
        "final_reason": (research["pure_policy_trace"]["final_reason"], runtime_trace["final_reason"]),
        "execution_intent": (research["execution_intent_v2"], runtime_trace["execution_intent"]),
        "exit_rule": (research["exit_rule"] or None, runtime_result.decision.exit_rule),
        "strategy_evaluation_mode": (
            "research_promotion",
            research["strategy_evaluation_provenance"]["strategy_evaluation_mode"],
        ),
        "runtime_strategy_evaluation_mode": (
            "runtime_replay",
            runtime_context["strategy_evaluation_provenance"]["strategy_evaluation_mode"],
        ),
        "decision_boundary": (
            "StrategyDecisionService.evaluate",
            runtime_context["strategy_evaluation_provenance"]["decision_boundary"],
        ),
    }
    mismatches = {
        key: values
        for key, values in comparable.items()
        if values[0] != values[1]
    }
    drift_debug = {
        "previous_cross_state": (
            research["pure_policy_trace"]["market"].get("previous_cross_state"),
            runtime_trace["market"].get("previous_cross_state"),
        ),
        "allow_initial_cross": (
            research["pure_policy_trace"]["market"].get("allow_initial_cross"),
            runtime_trace["market"].get("allow_initial_cross"),
        ),
        "gap_ratio": (
            research["pure_policy_trace"]["market"].get("gap_ratio"),
            runtime_trace["market"].get("gap_ratio"),
        ),
        "volatility_ratio": (
            research["pure_policy_trace"]["market"].get("volatility_ratio"),
            runtime_trace["market"].get("volatility_ratio"),
        ),
        "overextended_ratio": (
            research["pure_policy_trace"]["market"].get("overextended_ratio"),
            runtime_trace["market"].get("overextended_ratio"),
        ),
        "market_regime_snapshot": (
            research["pure_policy_trace"]["market"].get("market_regime_snapshot"),
            runtime_trace["market"].get("market_regime_snapshot"),
        ),
        "candidate_regime_policy_status": (
            research["pure_policy_trace"]["config"].get("candidate_regime_policy_status"),
            runtime_trace["config"].get("candidate_regime_policy_status"),
        ),
        "candidate_regime_decision": (
            research["pure_policy_trace"].get("entry", {}).get("candidate_regime_decision"),
            runtime_trace.get("entry", {}).get("candidate_regime_decision"),
        ),
        "candidate_regime_triggered": (
            research["pure_policy_trace"].get("entry", {}).get("candidate_regime_triggered"),
            runtime_trace.get("entry", {}).get("candidate_regime_triggered"),
        ),
        "position_exit_inputs": (
            research["pure_policy_trace"].get("final_exit_decision_input", {}).get("position"),
            runtime_trace.get("final_exit_decision_input", {}).get("position"),
        ),
    }
    assert mismatches == {}, {"mismatches": mismatches, "drift_debug": drift_debug}
    for drift_field in (
        "previous_cross_state",
        "allow_initial_cross",
        "gap_ratio",
        "volatility_ratio",
        "overextended_ratio",
        "market_regime_snapshot",
    ):
        assert research["pure_policy_trace"]["market"][drift_field] == runtime_trace["market"][drift_field]
    assert research["execution_submit_plan_hash"]
    assert runtime_context["replay_fingerprint_hash"]


def test_research_adapter_does_not_override_policy_first_cross_when_prev_above_unknown() -> None:
    closes = [10.0, 10.0, 10.0, 11.0, 11.0]
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes(closes))

    first = events[0]
    decision = sma_with_filter_plugin.research_policy_decision_builder(
        event=first,
        dataset=_dataset_from_closes(closes),
        candle_index=3,
        position=_flat_position(),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        active_exit_policy={"rules": ()},
        buy_fraction=0.99,
    )
    assert decision is not None
    policy_trace = decision.as_trace()
    assert "prev_above" not in first.extra_payload
    assert policy_trace["market"]["previous_cross_state"] == "unknown"
    assert policy_trace["market"]["allow_initial_cross"] is True
    assert policy_trace["raw_signal"] == "BUY"
    assert policy_trace["entry_signal"] == "BUY"
    assert policy_trace["final_signal"] == "BUY"


def test_policy_can_allow_initial_cross_when_configured() -> None:
    decision = evaluate_sma_policy(
        market=MarketWindow(
            **{
                **_market_window().__dict__,
                "previous_cross_state": "unknown",
                "allow_initial_cross": True,
            }
        ),
        position=_flat_position(),
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
    )

    assert decision.raw_signal == "BUY"
    assert decision.final_signal == "BUY"


def test_shared_sma_exit_policy_is_deterministic_for_runtime_and_research_snapshots() -> None:
    market = MarketWindow(
        pair="BTC_KRW",
        interval="1m",
        candle_ts=1_700_000_240_000,
        closes=(95.0,),
        prev_s=100.0,
        prev_l=99.0,
        curr_s=98.0,
        curr_l=99.0,
    )
    config = ExitPolicyConfig(
        rule_names=("stop_loss", "opposite_cross", "max_holding_time"),
        stop_loss_ratio=0.04,
        max_holding_sec=600.0,
        min_take_profit_ratio=0.0,
        small_loss_tolerance_ratio=0.0,
        live_fee_rate_estimate=0.0,
    )
    runtime_snapshot = PositionSnapshot(
        in_position=True,
        entry_allowed=False,
        exit_allowed=True,
        terminal_state="open_exposure",
        entry_ts=1_700_000_000_000,
        entry_price=100.0,
        qty_open=1.0,
        holding_time_sec=240.0,
        unrealized_pnl=-5.0,
        unrealized_pnl_ratio=-0.05,
        open_lot_count=1,
        sellable_executable_lot_count=1,
        effective_flat=False,
        has_executable_exposure=True,
        has_any_position_residue=True,
    )
    research_snapshot = PositionSnapshot(
        **{
            **runtime_snapshot.__dict__,
            "terminal_state": "research_simulated_open_exposure",
        }
    )

    runtime_exit = evaluate_sma_exit_policy(
        position=runtime_snapshot,
        market=market,
        raw_signal="SELL",
        raw_reason="sma dead cross",
        entry_signal="SELL",
        exit_signal="SELL",
        config=config,
    )
    research_exit = evaluate_sma_exit_policy(
        position=research_snapshot,
        market=market,
        raw_signal="SELL",
        raw_reason="sma dead cross",
        entry_signal="SELL",
        exit_signal="SELL",
        config=config,
    )

    assert runtime_exit == research_exit
    assert runtime_exit.final_signal == "SELL"
    assert runtime_exit.rule == "stop_loss"
    assert runtime_exit.reason == "exit by stop loss"


def test_research_kernel_open_snapshot_matches_live_open_exit_policy_fields() -> None:
    market = MarketWindow(
        pair="BTC_KRW",
        interval="1m",
        candle_ts=1_700_000_240_000,
        closes=(95.0,),
        prev_s=100.0,
        prev_l=99.0,
        curr_s=98.0,
        curr_l=99.0,
    )
    config = ExitPolicyConfig(
        rule_names=("stop_loss", "opposite_cross", "max_holding_time"),
        stop_loss_ratio=0.04,
        max_holding_sec=600.0,
        min_take_profit_ratio=0.0,
        small_loss_tolerance_ratio=0.0,
        live_fee_rate_estimate=0.0,
    )
    research_snapshot = backtest_kernel._research_position_snapshot(
        qty=1.0,
        sellable_qty=1.0,
        pending_buy_qty=0.0,
        pending_sell_qty=0.0,
        entry_ts=1_700_000_000_000,
        entry_price=100.0,
        candle_ts=1_700_000_240_000,
        market_price=95.0,
    )
    live_snapshot = PositionSnapshot(
        **{
            **research_snapshot.__dict__,
            "terminal_state": "open_exposure",
        }
    )

    research_exit = evaluate_sma_exit_policy(
        position=research_snapshot,
        market=market,
        raw_signal="SELL",
        raw_reason="sma dead cross",
        entry_signal="SELL",
        exit_signal="SELL",
        config=config,
    )
    live_exit = evaluate_sma_exit_policy(
        position=live_snapshot,
        market=market,
        raw_signal="SELL",
        raw_reason="sma dead cross",
        entry_signal="SELL",
        exit_signal="SELL",
        config=config,
    )

    assert research_snapshot.terminal_state == "research_simulated_open_exposure"
    assert research_exit.final_signal == live_exit.final_signal == "SELL"
    assert research_exit.rule == live_exit.rule == "stop_loss"
    assert research_exit.reason == live_exit.reason == "exit by stop loss"


def test_runtime_decide_is_read_only_and_normalization_boundary_is_explicit() -> None:
    load_position_source = inspect.getsource(runtime_sma._load_position_context)
    normalizer_source = inspect.getsource(
        runtime_position_state_normalizer.PositionStateNormalizer.normalize_and_persist
    )
    builder_source = inspect.getsource(runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db)
    builder_impl_source = inspect.getsource(
        runtime_sma._build_sma_with_filter_runtime_decision_from_normalized_db_readonly_impl
    )
    orchestration_source = inspect.getsource(runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db)
    runtime_normalization_source = inspect.getsource(
        runtime_sma_adapter.normalize_position_state_before_strategy_decision
    )
    runtime_boundary_source = inspect.getsource(runtime_sma_snapshot.decide_sma_with_filter_snapshot_from_db)
    runtime_boundary_module_source = inspect.getsource(runtime_sma_snapshot)

    assert "mark_harmless_dust_positions" not in load_position_source
    assert "reclassify_non_executable_open_exposure" not in load_position_source
    assert "conn.commit()" not in load_position_source
    assert "mark_harmless_dust_positions" in normalizer_source
    assert "reclassify_non_executable_open_exposure" in normalizer_source
    assert "conn.commit()" in normalizer_source
    assert "readonly_decision_context(" in builder_source
    assert "_load_position_context(" in builder_impl_source
    assert "StrategyDecisionService().evaluate(" in builder_impl_source
    assert "StrategyEvaluationRequest(" in builder_impl_source
    assert "normalize_and_persist(" not in orchestration_source
    assert "normalize_and_persist(" in runtime_normalization_source
    assert "strategy.decide(" not in orchestration_source
    assert "_decide_from_normalized_db(" not in orchestration_source
    assert "build_sma_with_filter_runtime_decision_from_normalized_db(" in orchestration_source
    assert "_runtime_snapshot_from_db(" in runtime_boundary_source
    assert "decide_sma_with_filter_snapshot_from_db as _strategy_snapshot_from_db" not in runtime_boundary_module_source


def test_runtime_snapshot_builder_does_not_import_private_strategy_sma_helpers() -> None:
    tree = ast.parse(inspect.getsource(runtime_sma))
    forbidden_helpers = {
        "_safe_ratio",
        "_sma",
        "_build_entry_decision_context",
        "_build_position_gate_context",
        "_build_position_state_context",
        "_fee_authority_context",
        "_legacy_strategy_decision_from_sma_final_decision",
        "_live_armed_entry_fee_authority_blocks",
        "_resolve_strategy_fee_authority",
    }
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        assert node.module not in {
            ".strategy.sma",
            "bithumb_bot.strategy.sma",
            "strategy.sma",
        }
        if node.module.endswith("strategy.sma"):
            imported = {alias.name for alias in node.names}
            assert imported.isdisjoint(forbidden_helpers)


def test_research_kernel_does_not_import_private_strategy_sma_helpers() -> None:
    source = inspect.getsource(backtest_kernel)
    tree = ast.parse(source)

    assert "from bithumb_bot.strategy import sma as runtime_sma" not in source
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            assert not node.module.endswith("strategy.sma")


def test_runtime_context_owns_sma_legacy_serialization_helpers() -> None:
    builder_source = inspect.getsource(runtime_sma.RuntimeSmaDecisionResult.legacy_strategy_decision)
    strategy_module_source = inspect.getsource(SmaWithFilterStrategy)

    assert "runtime_sma_context" in inspect.getsource(runtime_sma)
    assert "legacy_strategy_decision_from_sma_final_decision(" in builder_source
    assert "_legacy_strategy_decision_from_sma_final_decision(" not in builder_source
    assert "Promotion-grade snapshot SMA strategy" in strategy_module_source
    assert "def decide(" not in strategy_module_source


def test_strategy_sma_is_compatibility_facade_not_implementation_authority() -> None:
    module_source = inspect.getsource(strategy_sma)

    assert "Production-facing SMA strategy facade" in module_source
    assert "import sqlite3" not in module_source
    assert "class SmaWithFilterStrategy" not in module_source
    assert "class SmaCrossStrategy" not in module_source
    assert SmaWithFilterStrategy.__module__ == "bithumb_bot.strategy.sma_policy_strategy"
    assert "evaluate_sma_final_decision(" in inspect.getsource(SmaWithFilterStrategy.decide_snapshot)
    assert "evaluate_sma_policy(" in inspect.getsource(SmaWithFilterStrategy.decide_entry_snapshot)


def _called_function_names(module: object) -> set[str]:
    tree = ast.parse(inspect.getsource(module))
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            names.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            names.add(node.func.attr)
    return names


def test_environment_adapters_do_not_call_final_sma_assembler_directly() -> None:
    forbidden = "evaluate_sma_final_decision"
    assert forbidden not in _called_function_names(runtime_sma)
    assert forbidden not in _called_function_names(sma_with_filter_plugin)
    assert forbidden not in _called_function_names(backtest_kernel)


def test_research_event_adapter_is_non_authoritative_source_boundary() -> None:
    source = inspect.getsource(SmaWithFilterDecisionAdapter.build_events)
    calls = _called_function_names(SmaWithFilterDecisionAdapter)
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes([10.0, 10.0, 10.0, 10.0, 11.0]))

    assert "evaluate_sma_policy" not in calls
    assert "evaluate_sma_final_decision" not in calls
    assert "pure_policy_trace" not in source
    assert events
    event = events[-1]
    assert event.raw_signal == "HOLD"
    assert event.final_signal == "HOLD"
    assert event.reason == "research_event_adapter_non_authoritative"
    assert "entry_decision" not in event.extra_payload
    assert "pure_policy_trace" not in event.extra_payload
    assert "pure_policy_hash" not in event.extra_payload


def test_live_and_research_policy_paths_call_same_strategy_entrypoint(monkeypatch) -> None:
    calls: list[str] = []
    original = SmaWithFilterStrategy.decide_snapshot

    def _counting_decide_snapshot(self, *args, **kwargs):
        calls.append(str(getattr(self, "name", "")))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(SmaWithFilterStrategy, "decide_snapshot", _counting_decide_snapshot)
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    conn = _build_candle_db(closes)
    try:
        runtime_result = runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_240_000,
        )
    finally:
        conn.close()
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes(closes))
    research_result = sma_with_filter_plugin.research_policy_decision_builder(
        event=events[-1],
        dataset=_dataset_from_closes(closes),
        candle_index=len(closes) - 1,
        position=_flat_position(),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        active_exit_policy={"rules": ()},
        buy_fraction=0.99,
    )

    assert runtime_result is not None
    assert research_result is not None
    assert calls == ["sma_with_filter", "sma_with_filter"]


def test_live_research_equivalence_under_identical_policy_snapshots() -> None:
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        exit_rule_names=["stop_loss", "opposite_cross"],
    )
    live_position = _open_position(unrealized_pnl=-1.0, unrealized_pnl_ratio=-0.1)
    research_position = replace(
        live_position,
        terminal_state="research_simulated_open_exposure",
    )
    config = _policy_config()
    execution_context = ExecutionConstraintSnapshot(
        fee_rate_for_decision=0.0,
        fee_authority={"fee_source": "fixture", "taker_bid_fee_rate": 0.0},
        order_rules={"source": "fixture", "min_total": 5_000},
    )
    exit_config = _exit_policy_config(rule_names=("stop_loss", "opposite_cross"))

    live_decision = strategy.decide_snapshot(
        market=_market_window(),
        position=live_position,
        config=config,
        execution_context=execution_context,
        exit_policy_config=exit_config,
    )
    research_decision = strategy.decide_snapshot(
        market=_market_window(),
        position=research_position,
        config=config,
        execution_context=execution_context,
        exit_policy_config=exit_config,
    )

    assert research_decision.final_signal == live_decision.final_signal
    assert research_decision.raw_signal == live_decision.raw_signal
    assert research_decision.entry_signal == live_decision.entry_signal
    assert research_decision.exit_signal == live_decision.exit_signal
    assert research_decision.exit_rule == live_decision.exit_rule
    assert research_decision.execution_intent == live_decision.execution_intent
    assert research_decision.policy_input_hash == live_decision.policy_input_hash
    assert research_decision.policy_decision_hash == live_decision.policy_decision_hash
    assert research_decision.policy_contract_hash == live_decision.policy_contract_hash
    assert research_decision.exit_evaluations == live_decision.exit_evaluations


class _CommitCountingConnection:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.commit_count = 0

    def execute(self, *args, **kwargs):
        return self.conn.execute(*args, **kwargs)

    def commit(self) -> None:
        self.commit_count += 1
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def test_post_normalization_decision_path_does_not_commit(monkeypatch) -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    wrapped = _CommitCountingConnection(_build_candle_db(closes))

    try:
        strategy = create_sma_with_filter_strategy(
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
            live_fee_rate_estimate=0.0,
            entry_edge_buffer_ratio=0.0,
            cost_edge_enabled=False,
            market_regime_enabled=False,
            candidate_regime_policy=_allowing_policy(),
        )
        decision = runtime_sma.build_sma_with_filter_decision_from_normalized_db(
            wrapped,
            strategy,
        )
    finally:
        wrapped.close()

    assert decision is not None
    assert wrapped.commit_count == 0


def test_sma_cross_decide_does_not_run_position_normalizer_or_commit(monkeypatch) -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    wrapped = _CommitCountingConnection(_build_candle_db(closes))

    def _raise_mutating_normalizer(*args, **kwargs):
        raise AssertionError("SmaCrossStrategy.decide must not normalize mutable position state")

    monkeypatch.setattr(
        runtime_position_state_normalizer.PositionStateNormalizer,
        "normalize_and_persist",
        _raise_mutating_normalizer,
    )

    try:
        decision = SmaCrossStrategy(
            short_n=2,
            long_n=3,
            pair="BTC_KRW",
            interval="1m",
            slippage_bps=0.0,
            live_fee_rate_estimate=0.0,
            entry_edge_buffer_ratio=0.0,
            strategy_min_expected_edge_ratio=0.0,
        ).decide(wrapped, through_ts_ms=1_700_000_240_000)
    finally:
        wrapped.close()

    assert decision is not None
    assert wrapped.commit_count == 0


def test_runtime_snapshot_builder_after_normalization_is_read_only() -> None:
    closes = [10.0] * 11 + [11.0]
    conn = _build_candle_db(closes)
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    changes_before = conn.total_changes
    candles_before = conn.execute(
        "SELECT ts, pair, interval, close FROM candles ORDER BY ts"
    ).fetchall()

    try:
        result = runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
        changes_after = conn.total_changes
        candles_after = conn.execute(
            "SELECT ts, pair, interval, close FROM candles ORDER BY ts"
        ).fetchall()
    finally:
        conn.close()

    assert result is not None
    assert changes_after == changes_before
    assert candles_after == candles_before
    assert result.decision.policy_input_hash.startswith("sha256:")
    assert result.decision.policy_decision_hash.startswith("sha256:")
    assert result.decision.policy_hash.startswith("sha256:")
    assert result.replay_fingerprint["strategy_name"] == "sma_with_filter"
    assert result.replay_fingerprint["through_ts_ms"] == 1_700_000_000_000 + 11 * 60_000
    assert result.boundary["normalization_boundary"] == (
        "engine.normalize_position_state_before_strategy_decision"
    )
    assert result.boundary["normalization_updated_count"] is None
    assert result.boundary["decision_boundary_phase"] == "post_normalization_decision"
    assert isinstance(result.boundary["post_normalization_read_only_guard"], dict)
    assert result.boundary["post_decision_total_changes_delta"] == 0
    assert result.runtime_decision_context.as_dict()["boundary"] == result.boundary


def test_post_normalization_read_only_guard_rejects_mutation(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    original_load_signal_rows = runtime_sma._load_signal_rows

    def _mutating_signal_rows(*args, **kwargs):
        conn.execute(
            "INSERT INTO candles(ts, pair, interval, close) VALUES (?, ?, ?, ?)",
            (1_800_000_000_000, "BTC_KRW", "1m", 99.0),
        )
        return original_load_signal_rows(*args, **kwargs)

    monkeypatch.setattr(runtime_sma, "_load_signal_rows", _mutating_signal_rows)

    try:
        with pytest.raises(RuntimeError, match="post_normalization_decision_readonly_violation"):
            runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db(
                conn,
                strategy,
                through_ts_ms=1_700_000_000_000 + 11 * 60_000,
            )
    finally:
        conn.close()



def test_load_position_context_does_not_commit() -> None:
    closes = [10.0, 10.0, 10.0, 10.0, 11.0]
    wrapped = _CommitCountingConnection(_build_candle_db(closes))

    try:
        runtime_sma._load_position_context(
            wrapped,
            pair="BTC_KRW",
            candle_ts=1_700_000_240_000,
            market_price=11.0,
            signal_context={"strategy": "sma_with_filter"},
            slippage_bps=0.0,
            entry_edge_buffer_ratio=0.0,
        )
    finally:
        wrapped.close()

    assert wrapped.commit_count == 0


def test_position_state_normalizer_is_the_commit_boundary(monkeypatch) -> None:
    wrapped = _CommitCountingConnection(_build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0]))
    monkeypatch.setattr(
        runtime_position_state_normalizer,
        "mark_harmless_dust_positions",
        lambda *args, **kwargs: 1,
    )
    monkeypatch.setattr(
        runtime_position_state_normalizer,
        "reclassify_non_executable_open_exposure",
        lambda *args, **kwargs: 0,
    )

    try:
        updated = runtime_position_state_normalizer.PositionStateNormalizer().normalize_and_persist(
            wrapped,
            pair="BTC_KRW",
            market_price=11.0,
            slippage_bps=0.0,
            entry_edge_buffer_ratio=0.0,
        )
    finally:
        wrapped.close()

    assert updated == 1
    assert wrapped.commit_count == 1


def _assert_no_sqlite_mutation_sql_or_commit(functions: tuple[object, ...]) -> None:
    mutating_sql = {
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
        "CREATE",
        "DROP",
        "ALTER",
        "VACUUM",
    }
    for function in functions:
        source = textwrap.dedent(inspect.getsource(function))
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    assert node.func.attr not in {"commit", "executemany", "executescript"}
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                first_token = node.value.strip().split(maxsplit=1)[0].upper() if node.value.strip() else ""
                assert first_token not in mutating_sql


def test_position_normalizer_is_the_only_runtime_decision_mutation_boundary() -> None:
    _assert_no_sqlite_mutation_sql_or_commit(
        (
            sma_policy.evaluate_sma_policy,
            SmaWithFilterStrategy.decide_snapshot,
            runtime_sma._load_signal_rows,
            runtime_sma._load_position_context,
            runtime_sma._policy_position_snapshot,
            runtime_sma._build_sma_with_filter_runtime_decision_from_normalized_db_readonly_impl,
            runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db,
            runtime_sma_snapshot.build_sma_with_filter_replay_bundle,
        )
    )

    normalizer_source = inspect.getsource(
        runtime_position_state_normalizer.PositionStateNormalizer.normalize_and_persist
    )
    orchestration_source = inspect.getsource(runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db)
    runtime_normalization_source = inspect.getsource(
        runtime_sma_adapter.normalize_position_state_before_strategy_decision
    )

    assert "mark_harmless_dust_positions(" in normalizer_source
    assert "reclassify_non_executable_open_exposure(" in normalizer_source
    assert "conn.commit()" in normalizer_source
    assert "normalize_and_persist(" not in orchestration_source
    assert "normalize_and_persist(" in runtime_normalization_source
    assert "build_sma_with_filter_runtime_decision_from_normalized_db(" in orchestration_source


def test_engine_orchestration_normalizes_before_snapshot_decision(monkeypatch) -> None:
    events: list[str] = []
    conn = _build_candle_db([10.0 + 0.01 * idx for idx in range(40)])
    old_pair = engine.settings.PAIR
    old_interval = engine.settings.INTERVAL
    base_ts = 1_700_000_000_000

    from bithumb_bot import runtime_data_provider_sma
    from bithumb_bot.runtime_adapters.sma_with_filter import SmaWithFilterRuntimeDecisionAdapter

    original_load_position_context = runtime_data_provider_sma.load_sma_position_context

    def _normalize(*args, **kwargs):
        events.append("normalize")
        return original_load_position_context(*args, **kwargs)

    def _decide(self, request, feature_snapshot):
        del self, request, feature_snapshot
        events.append("decision")
        return None

    monkeypatch.setattr(
        runtime_data_provider_sma,
        "load_sma_position_context",
        _normalize,
    )
    monkeypatch.setattr(
        SmaWithFilterRuntimeDecisionAdapter,
        "decide_feature_snapshot",
        _decide,
    )

    try:
        object.__setattr__(engine.settings, "PAIR", "BTC_KRW")
        object.__setattr__(engine.settings, "INTERVAL", "1m")
        decision = engine.compute_strategy_decision_snapshot(
            conn,
            through_ts_ms=base_ts + 39 * 60_000,
            strategy_name="sma_with_filter",
        )
    finally:
        object.__setattr__(engine.settings, "PAIR", old_pair)
        object.__setattr__(engine.settings, "INTERVAL", old_interval)
        conn.close()

    assert decision is None
    assert events == ["normalize", "decision"]


def test_snapshot_orchestration_does_not_call_legacy_decide_facade(monkeypatch) -> None:
    conn = _build_candle_db([10.0, 10.0, 10.0, 10.0, 11.0])
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    def _raise_legacy_decide(*args, **kwargs):
        raise AssertionError("legacy decide facade was called")

    def _raise_legacy_normalized_db_decide(*args, **kwargs):
        raise AssertionError("legacy normalized DB strategy method was called")

    monkeypatch.setattr(SmaWithFilterStrategy, "decide", _raise_legacy_decide, raising=False)
    monkeypatch.setattr(
        SmaWithFilterStrategy,
        "_decide_from_normalized_db",
        _raise_legacy_normalized_db_decide,
        raising=False,
    )

    try:
        decision = runtime_sma.decide_sma_with_filter_snapshot_from_db(
            conn,
            strategy,
        )
    finally:
        conn.close()

    assert decision is not None
    assert decision.context["policy_decision_hash"].startswith("sha256:")


def test_runtime_sma_decision_helper_does_not_call_position_normalizer(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    def _raise_mutating_normalizer(*args, **kwargs):
        raise AssertionError("runtime SMA decision helper must be read-only")

    monkeypatch.setattr(
        runtime_position_state_normalizer.PositionStateNormalizer,
        "normalize_and_persist",
        _raise_mutating_normalizer,
    )

    try:
        result = runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
    finally:
        conn.close()

    assert result is not None
    assert result.decision.policy_decision_hash.startswith("sha256:")


def test_replay_bundle_uses_read_only_normalized_builder(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    def _raise_real_normalizer(*args, **kwargs):
        raise AssertionError("real normalizer should not run during replay bundle construction")

    monkeypatch.setattr(
        runtime_position_state_normalizer.PositionStateNormalizer,
        "normalize_and_persist",
        _raise_real_normalizer,
    )

    try:
        bundle = runtime_sma_snapshot.build_sma_with_filter_replay_bundle(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
    finally:
        conn.close()

    assert bundle is not None
    assert bundle["boundary_stages"]["snapshot_builder"] == (
        "runtime_sma_snapshot_builder.build_sma_with_filter_runtime_decision_from_normalized_db"
    )
    assert bundle["boundary_stages"]["pre_decision_normalization"] == (
        "engine.normalize_position_state_before_strategy_decision"
    )
    assert bundle["decision_context_schema_version"] == 1
    assert set(bundle["code_provenance"]) == {
        "schema_version",
        "source",
        "commit_sha",
        "dirty",
        "reason",
    }
    assert bundle["code_provenance"]["source"] in {"git", "unavailable"}
    assert bundle["final_typed_strategy_decision"]["policy_input_hash"] == bundle["policy_input_hash"]
    assert bundle["execution_decision_summary"]["final_signal"] == bundle["final_typed_strategy_decision"]["final_signal"]
    assert bundle["normalization_boundary"] == "engine.normalize_position_state_before_strategy_decision"
    assert bundle["normalization_updated_count"] is None
    assert bundle["decision_boundary_phase"] == "post_normalization_decision"
    assert isinstance(bundle["post_normalization_read_only_guard"], dict)
    assert bundle["post_decision_total_changes_delta"] == 0


def test_replay_decision_uses_read_only_normalizer_and_does_not_mutate_db(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    def _raise_mutating_normalizer(*args, **kwargs):
        raise AssertionError("mutating normalizer should not run during replay")

    monkeypatch.setattr(
        runtime_position_state_normalizer.PositionStateNormalizer,
        "normalize_and_persist",
        _raise_mutating_normalizer,
    )
    changes_before = conn.total_changes

    try:
        bundle = runtime_sma_snapshot.build_sma_with_filter_replay_bundle(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
        changes_after = conn.total_changes
    finally:
        conn.close()

    assert bundle is not None
    assert changes_after == changes_before
    assert {
        "boundary_stages",
        "code_provenance",
        "market_snapshot",
        "position_snapshot",
        "policy_config",
        "execution_constraint_snapshot",
        "policy_input_hash",
        "policy_decision_hash",
        "pure_policy_hash",
        "replay_fingerprint",
        "final_typed_strategy_decision",
        "execution_decision_reconstructable",
        "execution_decision_reconstruction_reason",
    }.issubset(bundle)
    assert bundle["execution_decision_reconstructable"] is False
    assert bundle["execution_decision_reconstruction_reason"] == (
        "live_readiness_context_not_available_in_db_snapshot"
    )
    assert bundle["policy_input_hash"].startswith("sha256:")
    assert bundle["policy_decision_hash"].startswith("sha256:")
    assert bundle["pure_policy_hash"].startswith("sha256:")


def test_compute_signal_uses_direct_sma_with_filter_snapshot_path(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    events: list[str] = []
    original_builder = runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db
    old_pair = engine.settings.PAIR
    old_interval = engine.settings.INTERVAL

    def _raise_legacy_decide(*args, **kwargs):
        raise AssertionError("legacy decide facade was called")

    def _raise_legacy_normalized_db_decide(*args, **kwargs):
        raise AssertionError("legacy normalized DB strategy method was called")

    def _builder(conn, strategy, *, through_ts_ms=None):
        events.append("builder")
        return original_builder(conn, strategy, through_ts_ms=through_ts_ms)

    monkeypatch.setattr(SmaWithFilterStrategy, "decide", _raise_legacy_decide, raising=False)
    monkeypatch.setattr(
        SmaWithFilterStrategy,
        "_decide_from_normalized_db",
        _raise_legacy_normalized_db_decide,
        raising=False,
    )
    monkeypatch.setattr(runtime_sma, "build_sma_with_filter_runtime_decision_from_normalized_db", _builder)

    try:
        object.__setattr__(engine.settings, "PAIR", "BTC_KRW")
        object.__setattr__(engine.settings, "INTERVAL", "1m")
        payload = compute_legacy_signal_for_diagnostics(conn, 2, 3, strategy_name="sma_with_filter")
    finally:
        object.__setattr__(engine.settings, "PAIR", old_pair)
        object.__setattr__(engine.settings, "INTERVAL", old_interval)
        conn.close()

    assert payload is not None
    assert payload["strategy"] == "sma_with_filter"
    assert payload["policy_decision_hash"].startswith("sha256:")
    assert events == ["builder"]


def test_live_sma_handoff_does_not_serialize_legacy_dict_before_execution_summary(
    monkeypatch,
) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    old_pair = engine.settings.PAIR
    old_interval = engine.settings.INTERVAL

    def _raise_legacy_dict(self):
        raise AssertionError("legacy dict serialization should not be the runtime handoff")

    monkeypatch.setattr(runtime_sma.RuntimeSmaDecisionResult, "as_legacy_dict", _raise_legacy_dict)
    parameter_overrides = _runtime_bound_sma_parameters(SMA_SHORT=2, SMA_LONG=3)
    parameter_overrides.pop("BUY_FRACTION", None)
    parameter_overrides.pop("MAX_ORDER_KRW", None)
    try:
        object.__setattr__(engine.settings, "PAIR", "BTC_KRW")
        object.__setattr__(engine.settings, "INTERVAL", "1m")
        handoff = compute_strategy_decision_for_diagnostics(
            conn,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
            strategy_name="sma_with_filter",
            parameter_overrides=parameter_overrides,
        )
    finally:
        object.__setattr__(engine.settings, "PAIR", old_pair)
        object.__setattr__(engine.settings, "INTERVAL", old_interval)
        conn.close()

    assert isinstance(handoff, runtime_sma.RuntimeSmaDecisionResult)
    summary = engine.build_execution_decision_summary(
        decision_context=handoff.legacy_strategy_decision().context,
        readiness_payload={},
        raw_signal=handoff.decision.raw_signal,
        final_signal=handoff.decision.final_signal,
        final_reason=handoff.decision.final_reason,
    )
    assert summary.raw_signal == handoff.decision.raw_signal
    assert summary.final_signal == handoff.decision.final_signal


def test_typed_runtime_sma_result_preserves_policy_hashes_until_legacy_serialization() -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    try:
        result = runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
    finally:
        conn.close()

    assert result is not None
    original_policy_decision_hash = result.decision.policy_decision_hash
    result.base_context["policy_decision_hash"] = "sha256:mutated_legacy_context"
    legacy_payload = result.as_legacy_dict()

    assert result.decision.policy_decision_hash == original_policy_decision_hash
    assert result.policy_hashes.policy_decision_hash == original_policy_decision_hash
    assert result.policy_observability["policy_decision_hash"] == original_policy_decision_hash
    assert legacy_payload["policy_decision_hash"] == original_policy_decision_hash
    assert legacy_payload["pure_policy_trace"]["policy_decision_hash"] == original_policy_decision_hash

    typed_context = result.runtime_decision_context
    serialized_context = typed_context.as_dict()
    serialized_context["policy_decision_hash"] = "sha256:mutated_serialized_context"
    serialized_context["blocked_filters"] = ["mutated"]
    assert typed_context.policy_decision_hash == original_policy_decision_hash
    assert list(typed_context.blocked_filters) == list(result.decision.blocked_filters)
    assert result.decision.policy_decision_hash == original_policy_decision_hash


def test_persistence_context_serializes_typed_policy_and_execution_summary_fields() -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )
    try:
        result = runtime_sma.decide_sma_with_filter_runtime_snapshot_from_db(
            conn,
            strategy,
            through_ts_ms=1_700_000_000_000 + 11 * 60_000,
        )
    finally:
        conn.close()

    assert result is not None
    decision_context = result.legacy_strategy_decision().context
    summary = engine.build_execution_decision_summary(
        decision_context=decision_context,
        readiness_payload={},
        raw_signal=result.decision.raw_signal,
        final_signal=result.decision.final_signal,
        final_reason=result.decision.final_reason,
    )
    persisted = engine.prepare_strategy_decision_persistence_context(
        decision_context=decision_context,
        execution_decision_summary=summary,
        readiness_payload={},
    )

    assert persisted["policy_contract_hash"] == result.decision.policy_contract_hash
    assert persisted["policy_input_hash"] == result.decision.policy_input_hash
    assert persisted["policy_decision_hash"] == result.decision.policy_decision_hash
    assert persisted["execution_decision"]["final_signal"] == summary.final_signal  # type: ignore[index]
    assert persisted["execution_decision"]["final_action"] == summary.final_action  # type: ignore[index]


def test_runtime_replay_export_uses_direct_sma_with_filter_snapshot_path(monkeypatch) -> None:
    conn = _build_candle_db([10.0] * 11 + [11.0])
    events: list[str] = []
    original_builder = runtime_sma.build_sma_with_filter_runtime_decision_from_normalized_db
    strategy = create_sma_with_filter_strategy(
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
        live_fee_rate_estimate=0.0,
        entry_edge_buffer_ratio=0.0,
        cost_edge_enabled=False,
        market_regime_enabled=False,
        candidate_regime_policy=_allowing_policy(),
    )

    def _raise_legacy_decide(*args, **kwargs):
        raise AssertionError("legacy decide facade was called")

    def _raise_legacy_normalized_db_decide(*args, **kwargs):
        raise AssertionError("legacy normalized DB strategy method was called")

    def _builder(conn, strategy, *, through_ts_ms=None):
        events.append("builder")
        return original_builder(conn, strategy, through_ts_ms=through_ts_ms)

    monkeypatch.setattr(SmaWithFilterStrategy, "decide", _raise_legacy_decide, raising=False)
    monkeypatch.setattr(
        SmaWithFilterStrategy,
        "_decide_from_normalized_db",
        _raise_legacy_normalized_db_decide,
        raising=False,
    )
    monkeypatch.setattr(runtime_sma, "build_sma_with_filter_runtime_decision_from_normalized_db", _builder)

    try:
        events_out = export_runtime_replay_decisions(
            conn=conn,
            strategy=strategy,
            through_ts_list=[1_700_000_000_000 + 11 * 60_000],
            market="BTC_KRW",
            interval="1m",
        )
    finally:
        conn.close()

    assert len(events_out) == 1
    assert events_out[0]["strategy_name"] == "sma_with_filter"
    assert events == ["builder"]


def test_research_kernel_reevaluates_policy_with_flat_simulated_position() -> None:
    result = run_sma_backtest(
        dataset=_dataset_from_closes([10.0, 10.0, 10.0, 10.0, 11.0]),
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
    )

    assert result.decisions
    decision = result.decisions[-1]
    assert decision["research_policy_recomputed_with_simulated_position"] is True
    assert decision["research_policy_comparable"] is True
    assert decision["research_policy_position_terminal_state"] == "research_simulated_flat"
    assert decision["pure_policy_trace"]["position"]["terminal_state"] == "research_simulated_flat"
    assert decision["final_signal"] == decision["pure_policy_trace"]["final_signal"] == "BUY"


def test_research_kernel_empty_event_metadata_uses_policy_recomputation_not_event_authority() -> None:
    dataset = _dataset_from_closes([10.0, 10.0, 10.0, 10.0, 11.0])
    event = ResearchDecisionEvent(
        candle_ts=dataset.candles[-1].ts,
        decision_ts=dataset.candles[-1].ts + 60_000,
        strategy_name="sma_with_filter",
        strategy_version="sma_with_filter.research_runtime_contract.v2",
        raw_signal="BUY",
        final_signal="BUY",
        reason="event-first buy must not be authoritative",
        feature_snapshot={},
        strategy_diagnostics={},
        entry_signal="BUY",
        exit_signal="BUY",
        exit_intent={"mode": "evaluate_exit_policy"},
        extra_payload={},
    )

    result = backtest_kernel.run_decision_event_backtest(
        dataset=dataset,
        strategy_name="sma_with_filter",
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        decision_events=(event,),
    )

    assert result.decisions
    decision = result.decisions[-1]
    assert decision["final_signal"] == "BUY"
    assert decision["blocked"] is False
    assert decision["entry_reason"] == "none"
    assert decision["research_policy_recomputed_with_simulated_position"] is True
    assert decision["research_policy_unsupported"] is False
    assert decision["research_policy_comparable"] is True
    assert decision["research_policy_unsupported_reason"] == ""
    assert decision["decision_input_bundle_hash"].startswith("sha256:")


def test_research_kernel_open_position_exit_fields_come_from_policy_decision() -> None:
    dataset = _dataset_from_closes([12.0, 12.0, 12.0, 12.0, 11.0])
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(dataset)
    event = replace(
        events[-1],
        extra_payload={**events[-1].extra_payload, "prev_above": True},
    )

    result = backtest_kernel.run_decision_event_backtest(
        dataset=dataset,
        strategy_name="sma_with_filter",
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        decision_events=(event,),
        portfolio_policy=PortfolioPolicy(
            schema_version=1,
            starting_cash_krw=1_000_000.0,
            quote_currency="KRW",
            initial_position_qty=1.0,
            cash_interest_policy="zero",
            position_sizing=PositionSizingPolicy(
                type="fractional_cash",
                buy_fraction=0.99,
                sell_policy="sell_all_available_position",
                cash_buffer_policy="retain_1_percent_before_fees",
            ),
            source="unit_test",
        ),
    )

    assert result.decisions
    decision = result.decisions[-1]
    assert decision["research_policy_recomputed_with_simulated_position"] is True
    assert decision["final_signal"] == decision["pure_policy_trace"]["final_signal"]
    assert decision["exit_rule"] == decision["pure_policy_trace"]["exit_rule"] == "opposite_cross"
    assert decision["exit_filter_suppression_prevented"] == (
        decision["pure_policy_trace"]["exit_filter_suppression_prevented"]
    )


def test_research_pending_fill_snapshot_is_not_comparable_or_flat() -> None:
    snapshot = backtest_kernel._research_position_snapshot(
        qty=1.0,
        sellable_qty=1.0,
        pending_buy_qty=0.0,
        pending_sell_qty=1.0,
        entry_ts=1_700_000_000_000,
        entry_price=10.0,
        candle_ts=1_700_000_240_000,
        market_price=11.0,
    )

    assert snapshot.terminal_state == "research_pending_fill_not_policy_comparable"
    assert snapshot.entry_allowed is False
    assert snapshot.exit_allowed is False
    assert snapshot.effective_flat is True
    assert snapshot.entry_block_reason == "research_pending_fill_not_policy_comparable"
    assert snapshot.exit_block_reason == "research_pending_fill_not_policy_comparable"


def test_final_sma_decision_harmless_dust_is_explicit_effective_flat_for_entry() -> None:
    position = PositionSnapshot(
        in_position=False,
        entry_allowed=True,
        exit_allowed=False,
        exit_block_reason="dust_only_remainder",
        terminal_state="dust_only",
        raw_qty_open=0.00009629,
        raw_total_asset_qty=0.00009629,
        dust_tracking_lot_count=1,
        dust_classification="harmless_dust",
        dust_state="harmless_dust",
        effective_flat=True,
        has_any_position_residue=True,
        has_non_executable_residue=True,
        has_dust_only_remainder=True,
    )

    decision = evaluate_sma_final_decision(
        market=_market_window(),
        position=position,
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )

    assert decision.final_signal == "BUY"
    assert decision.position_snapshot.terminal_state == "dust_only"
    assert decision.position_snapshot.dust_classification == "harmless_dust"
    assert decision.position_snapshot.effective_flat is True
    assert decision.position_snapshot.has_dust_only_remainder is True


def test_final_sma_decision_blocking_dust_fails_closed_not_flat() -> None:
    position = PositionSnapshot(
        in_position=False,
        entry_allowed=False,
        exit_allowed=False,
        entry_block_reason="blocking_dust_not_tradable",
        exit_block_reason="dust_only_remainder",
        terminal_state="dust_only",
        raw_qty_open=0.0002,
        raw_total_asset_qty=0.0002,
        dust_tracking_lot_count=1,
        dust_classification="blocking_dust",
        dust_state="blocking_dust",
        effective_flat=False,
        has_any_position_residue=True,
        has_non_executable_residue=True,
        has_dust_only_remainder=True,
    )

    decision = evaluate_sma_final_decision(
        market=_market_window(),
        position=position,
        config=_policy_config(),
        execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
        exit_policy_config=_exit_policy_config(),
    )

    assert decision.final_signal == "HOLD"
    assert decision.final_reason == "blocking_dust_not_tradable"
    assert decision.position_snapshot.terminal_state == "dust_only"
    assert decision.position_snapshot.dust_classification == "blocking_dust"
    assert decision.position_snapshot.effective_flat is False


def test_final_sma_decision_unsupported_states_fail_closed_not_flat() -> None:
    unsupported = (
        _open_position(
            exit_allowed=False,
            exit_block_reason="reserved_exit_pending",
            terminal_state="reserved_exit_pending",
            reserved_exit_lot_count=1,
            sellable_executable_lot_count=0,
        ),
        _open_position(
            exit_allowed=False,
            exit_block_reason="no_executable_exit_lot",
            terminal_state="non_executable_position",
            open_lot_count=0,
            sellable_executable_lot_count=0,
            has_executable_exposure=False,
            has_non_executable_residue=True,
        ),
        PositionSnapshot(
            in_position=False,
            entry_allowed=False,
            exit_allowed=False,
            entry_block_reason="authority_missing_recovery_required",
            exit_block_reason="authority_missing_recovery_required",
            terminal_state="authority_gap",
            has_any_position_residue=True,
        ),
        PositionSnapshot(
            in_position=False,
            entry_allowed=False,
            exit_allowed=False,
            entry_block_reason="recovery_required_present",
            exit_block_reason="recovery_required_present",
            terminal_state="recovery_required",
            has_any_position_residue=True,
        ),
    )

    for position in unsupported:
        decision = evaluate_sma_final_decision(
            market=_market_window(),
            position=position,
            config=_policy_config(),
            execution_context=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
            exit_policy_config=_exit_policy_config(),
        )

        assert decision.final_signal == "HOLD"
        assert decision.position_snapshot.terminal_state == position.terminal_state
        assert decision.final_reason in {
            position.entry_block_reason,
            position.exit_block_reason,
            "position held: no exit rule triggered",
        }
        assert decision.position_snapshot.terminal_state != "flat"


def test_research_adapter_placeholder_is_not_full_position_equivalence() -> None:
    events = SmaWithFilterDecisionAdapter(
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_MARKET_REGIME_ENABLED": False,
        },
        fee_rate=0.0,
        slippage_bps=0.0,
        timing_policy=ExecutionTimingPolicy(),
    ).build_events(_dataset_from_closes([10.0, 10.0, 10.0, 10.0, 11.0]))

    assert events[-1].extra_payload["non_authoritative_event_adapter"] is True
    assert "entry_decision" not in events[-1].extra_payload
    assert "pure_policy_hash" not in events[-1].extra_payload
    assert "pure_policy_trace" not in events[-1].extra_payload
