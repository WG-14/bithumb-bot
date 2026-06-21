from __future__ import annotations

import json
import math
import os
from dataclasses import fields, replace
from pathlib import Path

import pytest

from bithumb_bot.approved_profile import build_approved_profile, compute_approved_profile_hash
from bithumb_bot import config
from bithumb_bot.config import settings
from bithumb_bot import notifier
from bithumb_bot.execution_reality_contract import build_execution_reality_contract
from bithumb_bot.decision_equivalence import compute_decision_equivalence_hash
from bithumb_bot.research.hashing import content_hash_payload, sha256_prefixed
from bithumb_bot.research.promotion_gate import build_candidate_profile
from bithumb_bot.research.strategy_registry import (
    resolve_research_strategy_plugin,
    runtime_strategy_parameters_from_settings,
)
from bithumb_bot.research.strategy_spec import materialized_strategy_parameters_hash
from bithumb_bot.strategy_config import _sma_int
from bithumb_bot.runtime_strategy_set import RuntimeDecisionRequestBuilder, RuntimeStrategySpec
from bithumb_bot.risk_contract import RiskPolicy
from bithumb_bot.storage_io import write_json_atomic
from bithumb_bot.broker import order_rules
from bithumb_bot import operator_notification_service
from bithumb_bot.markets import MarketInfo, MarketRegistry
from tests.support.live_auth import TEST_BITHUMB_API_KEY, TEST_BITHUMB_API_SECRET

_REAL_GET_EFFECTIVE_ORDER_RULES = order_rules.get_effective_order_rules


@pytest.fixture(autouse=True)
def _restore_settings():
    old_values = {field.name: getattr(settings, field.name) for field in fields(type(settings))}
    old_cache = dict(order_rules._cached_rules)
    yield
    for key, value in old_values.items():
        object.__setattr__(settings, key, value)
    order_rules._cached_rules.clear()
    order_rules._cached_rules.update(old_cache)




@pytest.fixture(autouse=True)
def _stub_accounts_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [
            {"currency": "KRW", "balance": "1000000", "locked": "0"},
            {"currency": "BTC", "balance": "0.1", "locked": "0"},
        ],
    )

@pytest.fixture(autouse=True)
def _set_live_roots_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    roots = {
        "ENV_ROOT": tmp_path / "env",
        "RUN_ROOT": tmp_path / "run",
        "DATA_ROOT": tmp_path / "data",
        "LOG_ROOT": tmp_path / "logs",
        "BACKUP_ROOT": tmp_path / "backup",
    }
    for key, value in roots.items():
        monkeypatch.setenv(key, str(value.resolve()))


def _set_valid_live_defaults(
    monkeypatch: pytest.MonkeyPatch,
    *,
    db_path: str | None = None,
    stub_order_rules: bool = True,
) -> None:
    data_root = Path(os.environ["DATA_ROOT"])
    run_root = Path(os.environ["RUN_ROOT"])
    resolved_db_path = str(
        Path(db_path).resolve() if db_path is not None else (data_root / "live" / "trades" / "live.sqlite").resolve()
    )
    monkeypatch.setenv("DB_PATH", resolved_db_path)
    monkeypatch.setenv("RUN_LOCK_PATH", str((run_root / "live" / "bithumb-bot.lock").resolve()))
    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    monkeypatch.setenv("BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    monkeypatch.setenv("BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    monkeypatch.delenv("START_CASH_KRW", raising=False)
    monkeypatch.delenv("BUY_FRACTION", raising=False)
    monkeypatch.delenv("FEE_RATE", raising=False)
    monkeypatch.delenv("SLIPPAGE_BPS", raising=False)

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", resolved_db_path)
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "LIVE_ALLOW_ORDER_RULE_FALLBACK", False)
    object.__setattr__(
        settings,
        "LIVE_ORDER_RULE_FALLBACK_PROFILE",
        config.LIVE_ORDER_RULE_FALLBACK_PROFILE_PERSISTED_SNAPSHOT_REQUIRED,
    )
    object.__setattr__(settings, "LIVE_SUBMIT_CONTRACT_PROFILE", config.LIVE_SUBMIT_CONTRACT_PROFILE_V1)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 100000.0)
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "STRATEGY_NAME", "sma_with_filter")
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR", False)
    object.__setattr__(settings, "MARKET_PREFLIGHT_BLOCK_ON_WARNING", False)
    object.__setattr__(settings, "MARKET_PREFLIGHT_WARNING_STATES", "CAUTION")
    _set_matching_runtime_execution_contract_settings()
    monkeypatch.delenv("MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR", raising=False)
    monkeypatch.delenv("MARKET_PREFLIGHT_BLOCK_ON_WARNING", raising=False)
    monkeypatch.delenv("MARKET_PREFLIGHT_WARNING_STATES", raising=False)
    monkeypatch.delenv("MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH", raising=False)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
    )
    object.__setattr__(settings, "MARKET_REGISTRY_CACHE_TTL_SEC", 900.0)
    object.__setattr__(settings, "MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH", False)
    if stub_order_rules:
        monkeypatch.setattr(
            order_rules,
            "get_effective_order_rules",
            lambda _pair: order_rules.RuleResolution(
                rules=order_rules.OrderRules(
                    market_id="KRW-BTC",
                    bid_min_total_krw=5000.0,
                    ask_min_total_krw=5000.0,
                    bid_price_unit=1.0,
                    ask_price_unit=1.0,
                    min_qty=float(settings.LIVE_MIN_ORDER_QTY),
                    qty_step=float(settings.LIVE_ORDER_QTY_STEP),
                    min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
                    max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
                ),
                source={
                    "min_qty": "local_fallback",
                    "qty_step": "local_fallback",
                    "min_notional_krw": "local_fallback",
                    "max_qty_decimals": "local_fallback",
                    "bid_min_total_krw": "chance_doc",
                    "ask_min_total_krw": "chance_doc",
                    "bid_price_unit": "chance_doc",
                    "ask_price_unit": "chance_doc",
                },
            ),
        )
    profile_path = _write_live_profile(Path(os.environ["DATA_ROOT"]).parent, mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))
    object.__setattr__(settings, "STRATEGY_APPROVED_PROFILE_PATH", "")


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


def _candidate_profile_for_current_settings() -> dict[str, object]:
    return runtime_strategy_parameters_from_settings("sma_with_filter", settings)


def _write_live_profile(tmp_path: Path, *, mode: str = "small_live", sma_short: int | None = None) -> Path:
    parameters = _candidate_profile_for_current_settings()
    if sma_short is not None:
        parameters["SMA_SHORT"] = int(sma_short)
    strategy_plugin = resolve_research_strategy_plugin("sma_with_filter")
    base_cost_assumption = {
        "label": "test_runtime_base_cost",
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
        "experiment_id": "live-exp",
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
                    "market": "KRW-BTC",
                    "interval": str(settings.INTERVAL),
                    "expected_market": "KRW-BTC",
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
    decision_report = {
        "schema_version": 2,
        "comparison_contract_version": "canonical_decision_v2",
        "canonical_schema": True,
        "canonical_v2_schema": True,
        "legacy_schema": False,
        "promotion_grade_comparison": True,
        "ok": True,
        "outcome": "PASS_POSITIVE_EQUIVALENCE",
        "reason_codes": [],
        "profile_content_hash": candidate_hash,
        "market": "KRW-BTC",
        "interval": str(settings.INTERVAL),
        "data_fingerprint": candidate["dataset_content_hash"],
        "dataset_content_hash": candidate["dataset_content_hash"],
        "research_decision_count": 1,
        "runtime_decision_count": 1,
        "matched_decision_count": 1,
        "mismatched_decision_count": 0,
        "mismatch_count": 0,
        "missing_research_decisions": [],
        "missing_runtime_decisions": [],
        "mismatches": [],
        "canonical_missing_field_count": 0,
        "canonical_missing_fields_by_decision": {},
        "canonical_incomplete_decision_count": 0,
        "canonical_validation": [],
        "binding_validation": [],
        "artifact_binding_validation": [],
        "research_export_content_hash": "sha256:research",
        "runtime_export_content_hash": "sha256:runtime",
        "research_export_source": "research",
        "runtime_export_source": "runtime_replay",
        "research_export_path": str((tmp_path / "research_decisions.json").resolve()),
        "runtime_export_path": str((tmp_path / "runtime_decisions.json").resolve()),
        "research_strategy_plugin_contract_hash": strategy_plugin.contract_hash(),
        "runtime_strategy_plugin_contract_hash": strategy_plugin.contract_hash(),
        "strategy_decision_contract_version": strategy_plugin.decision_contract_version,
        "repo_owned_export_artifacts": True,
        "legacy_or_unverified_export": False,
        "post_export_canonical_artifact_equivalence": True,
        "claims_scope": {
            "positive_equivalence_state_classes": ["flat_no_dust_no_position"],
            "unsupported_state_classes": [],
            "promotion_claim": "positive_decision_equivalence_for_explicitly_modeled_state_classes_only",
            "full_lifecycle_equivalence_supported": False,
            "submit_plan_equivalence_supported": True,
            "signal_equivalence_supported": True,
            "execution_plan_equivalence_supported": True,
            "position_lifecycle_equivalence_supported": False,
            "fail_closed_unmodeled_state_count": 0,
        },
        "execution_equivalence": {
            "submit_plan_equivalence_supported": True,
            "submit_plan_equivalence_ok": True,
        },
        "state_coverage_matrix": {
            "flat_no_dust_no_position": {
                "research_decision_count": 1,
                "runtime_decision_count": 1,
                "positive_equivalence_supported": True,
                "fail_closed_expected": False,
                "supported_decision_count": 2,
                "unsupported_decision_count": 0,
                "mismatch_count": 0,
                "representative_reason_codes": [],
            }
        },
        "policy_input_hash_coverage": {"ok": True, "checked_decision_count": 2, "missing_by_decision": {}},
        "execution_plan_coverage": {"ok": True, "checked_decision_count": 2, "missing_by_decision": {}},
    }
    decision_report["content_hash"] = compute_decision_equivalence_hash(decision_report)
    decision_report_path = tmp_path / "decision_equivalence_report.json"
    write_json_atomic(decision_report_path, decision_report)
    promotion = {
        "strategy_name": "sma_with_filter",
        "strategy_profile_source_experiment": "live-exp",
        "candidate_id": "candidate_001",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "market": "KRW-BTC",
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
        "decision_equivalence_report_path": str(decision_report_path.resolve()),
        "decision_equivalence_content_hash": decision_report["content_hash"],
        "decision_equivalence_status": "verified",
        "generated_at": "2026-05-04T00:00:00+00:00",
    }
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, promotion)
    profile = build_approved_profile(
        promotion=promotion,
        mode=mode,
        source_promotion_path=str(promotion_path),
        market="KRW-BTC",
        interval=str(settings.INTERVAL),
        generated_at="2026-05-04T00:00:00+00:00",
    )
    risk_policy = RiskPolicy(
        policy_status="enabled",
        missing_policy="fail_closed_for_live",
        source="unit_approved_profile",
    )
    profile["risk_policy"] = risk_policy.as_dict()
    profile["risk_policy_hash"] = risk_policy.policy_hash()
    profile["risk_enforcement_mode"] = "enforced" if mode == "small_live" else "telemetry"
    profile["missing_risk_policy_behavior"] = "fail_closed_for_live"
    profile["profile_content_hash"] = compute_approved_profile_hash(profile)
    path = tmp_path / f"{mode}_profile.json"
    write_json_atomic(path, profile)
    return path


def _write_incomplete_live_profile(
    tmp_path: Path,
    *,
    mode: str,
    missing_parameter: str = "SMA_FILTER_OVEREXT_LOOKBACK",
) -> Path:
    profile_path = _write_live_profile(tmp_path, mode=mode)
    profile = json.loads(profile_path.read_text(encoding="utf-8-sig"))
    profile["strategy_parameters"].pop(missing_parameter)
    profile["effective_strategy_parameters"].pop(missing_parameter)
    profile["effective_strategy_parameters_hash"] = materialized_strategy_parameters_hash(
        profile["effective_strategy_parameters"]
    )
    profile["profile_content_hash"] = compute_approved_profile_hash(profile)
    incomplete_path = tmp_path / f"{mode}_profile_missing_{missing_parameter}.json"
    write_json_atomic(incomplete_path, profile)
    return incomplete_path


def _select_small_live_profile(tmp_path: Path) -> None:
    profile_path = _write_live_profile(tmp_path, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))


def _profile_hash(profile_path: Path) -> str:
    profile = json.loads(profile_path.read_text(encoding="utf-8-sig"))
    return str(profile["profile_content_hash"])


def _set_live_multi_strategy_json(
    monkeypatch: pytest.MonkeyPatch,
    *,
    profile_mode: str,
    second_sma_short: int | None = None,
    include_path: bool = True,
    include_hash: bool = True,
    hash_override: str | None = None,
) -> tuple[Path, Path]:
    base_dir = Path(os.environ["DATA_ROOT"]).parent
    left_profile = _write_live_profile(base_dir, mode=profile_mode)
    right_profile = _write_live_profile(
        base_dir,
        mode=profile_mode,
        sma_short=second_sma_short,
    )
    left_hash = _profile_hash(left_profile)
    right_hash = hash_override or _profile_hash(right_profile)

    def _strategy(instance_id: str, path: Path, profile_hash: str) -> dict[str, object]:
        payload: dict[str, object] = {
            "strategy_name": "sma_with_filter",
            "strategy_instance_id": instance_id,
            "pair": "KRW-BTC",
            "interval": str(settings.INTERVAL),
            "desired_exposure_krw": 50_000.0,
        }
        if include_path:
            payload["approved_profile_path"] = str(path)
        if include_hash:
            payload["approved_profile_hash"] = profile_hash
        return payload

    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": settings.INTERVAL},
            "strategies": [
                _strategy("left", left_profile, left_hash),
                _strategy("right", right_profile, right_hash),
            ],
        }
    )
    monkeypatch.delenv("ACTIVE_STRATEGIES", raising=False)
    monkeypatch.delenv("RUNTIME_STRATEGY_SET_JSON", raising=False)
    monkeypatch.delenv("APPROVED_STRATEGY_PROFILE_PATH", raising=False)
    monkeypatch.delenv("STRATEGY_APPROVED_PROFILE_PATH", raising=False)
    object.__setattr__(settings, "RUNTIME_STRATEGY_SET_JSON", runtime_strategy_set_json)
    object.__setattr__(settings, "ACTIVE_STRATEGIES", "")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")
    object.__setattr__(settings, "STRATEGY_APPROVED_PROFILE_PATH", "")
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    return left_profile, right_profile



def test_live_preflight_skips_paper_mode() -> None:
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    config.validate_live_mode_preflight(settings)
    config.validate_live_real_order_execution_preflight(settings)


def test_live_preflight_validates_active_strategy_set(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("ACTIVE_STRATEGIES", "sma_with_filter,sma_cross")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "runtime_strategy_set_selection_failed" in msg
    assert "runtime_strategy_set_active_strategies_fallback_rejected:live_dry_run" in msg


def test_live_preflight_rejects_multi_active_strategies_without_structured_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("ACTIVE_STRATEGIES", "sma_with_filter,canary_non_sma")
    monkeypatch.delenv("RUNTIME_STRATEGY_SET_JSON", raising=False)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "runtime_strategy_set_selection_failed" in msg
    assert "runtime_strategy_set_active_strategies_fallback_rejected:live_dry_run" in msg


def test_live_multi_strategy_requires_spec_bound_approved_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": settings.INTERVAL},
            "strategies": [
                {"strategy_name": "safe_hold", "strategy_instance_id": "left", "bind_market_scope": True},
                {"strategy_name": "safe_hold", "strategy_instance_id": "right", "bind_market_scope": True},
            ],
        }
    )
    cfg = replace(
        settings,
        MODE="live",
        PAIR="KRW-BTC",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        RUNTIME_STRATEGY_SET_JSON=runtime_strategy_set_json,
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_runtime_strategy_set_selection(cfg)

    assert "live_multi_strategy_requires_spec_bound_approved_profiles" in str(exc.value)


def test_global_profile_selector_rejected_for_live_multi_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": settings.INTERVAL},
            "strategies": [
                {"strategy_name": "safe_hold", "strategy_instance_id": "left", "bind_market_scope": True},
                {"strategy_name": "safe_hold", "strategy_instance_id": "right", "bind_market_scope": True},
            ],
        }
    )
    cfg = replace(
        settings,
        MODE="live",
        PAIR="KRW-BTC",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        APPROVED_STRATEGY_PROFILE_PATH="/runtime/global-profile.json",
        STRATEGY_APPROVED_PROFILE_PATH="",
        RUNTIME_STRATEGY_SET_JSON=runtime_strategy_set_json,
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_runtime_strategy_set_selection(cfg)

    assert "global_profile_selector_rejected_for_live_multi_strategy" in str(exc.value)


def test_live_dry_run_multi_strategy_spec_bound_profiles_without_global_selector_passes_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    _set_live_multi_strategy_json(
        monkeypatch,
        profile_mode="live_dry_run",
        second_sma_short=_sma_int("SMA_SHORT") + 1,
    )
    object.__setattr__(settings, "STRATEGY_NAME", "unsupported_legacy_global_name")

    config.validate_live_dry_run_loop_startup_contract(settings)


def test_live_real_order_multi_strategy_spec_bound_profiles_without_global_selector_passes_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    _set_live_multi_strategy_json(monkeypatch, profile_mode="small_live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "STRATEGY_NAME", "unsupported_legacy_global_name")

    config.validate_live_run_startup_contract(settings)


def test_live_multi_strategy_global_profile_selector_fails_full_startup_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    global_profile, _ = _set_live_multi_strategy_json(monkeypatch, profile_mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(global_profile))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "global_profile_selector_rejected_for_live_multi_strategy" in str(exc.value)


def test_live_multi_strategy_missing_spec_profile_path_fails_full_startup_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    _set_live_multi_strategy_json(
        monkeypatch,
        profile_mode="live_dry_run",
        include_path=False,
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "live_multi_strategy_requires_spec_bound_approved_profiles:path" in str(exc.value)


def test_live_multi_strategy_missing_spec_profile_hash_fails_full_startup_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    _set_live_multi_strategy_json(
        monkeypatch,
        profile_mode="live_dry_run",
        include_hash=False,
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "live_multi_strategy_requires_spec_bound_approved_profiles:hash" in str(exc.value)


def test_live_multi_strategy_spec_profile_hash_mismatch_fails_full_startup_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    _set_live_multi_strategy_json(
        monkeypatch,
        profile_mode="live_dry_run",
        hash_override="sha256:not-the-profile",
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "approved_profile_hash_mismatch_for_runtime_strategy:sma_with_filter" in str(exc.value)


def test_live_multi_strategy_requires_target_delta_execution_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    _set_live_multi_strategy_json(monkeypatch, profile_mode="live_dry_run")
    object.__setattr__(settings, "EXECUTION_ENGINE", "lot_native")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "live_multi_strategy_requires_execution_engine_target_delta" in str(exc.value)


def test_live_multi_strategy_profile_authority_is_observable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    _set_live_multi_strategy_json(monkeypatch, profile_mode="live_dry_run")

    summary = config.live_execution_contract_summary(settings)

    assert summary["runtime_selection_kind"] == "multi_strategy"
    assert summary["runtime_strategy_set_source"] == "RUNTIME_STRATEGY_SET_JSON"
    assert summary["profile_binding_kind"] == "spec_bound_approved_profiles"
    assert summary["startup_gate_authority"] == "RUNTIME_STRATEGY_SET_JSON"
    assert summary["submit_authority_mode"] == "live_dry_run_non_submitting_compat"
    assert str(summary["submit_authority_policy_hash"]).startswith("sha256:")
    assert summary["live_real_order_requires_target_delta"] is False
    assert summary["legacy_lot_native_compat_enabled"] is True
    assert "target_delta" in summary["allowed_submit_plan_sources"]
    binding = summary["runtime_profile_binding"]
    assert isinstance(binding, dict)
    assert binding["global_profile_selector_present"] is False
    assert len(binding["strategy_instance_approved_profile_hashes"]) == 2


def test_live_preflight_rejects_invalid_runtime_strategy_set_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("RUNTIME_STRATEGY_SET_JSON", '{"strategies": "not-a-list"}')
    object.__setattr__(settings, "STRATEGY_NAME", "unsupported_legacy_global_name")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "runtime_strategy_set_selection_failed" in msg
    assert "runtime_strategy_set_json_must_be_list" in msg
    assert "live_strategy_capability_validation_failed" not in msg
    assert "unsupported_legacy_global_name" not in msg


def test_runtime_strategy_set_rejects_multi_pair_until_pair_scoped_runtime_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": settings.INTERVAL},
            "strategies": [
                {
                    "strategy_name": "safe_hold",
                    "pair": "KRW-ETH",
                    "interval": settings.INTERVAL,
                }
            ],
        }
    )
    cfg = replace(
        settings,
        MODE="paper",
        PAIR="KRW-BTC",
        RUNTIME_STRATEGY_SET_JSON=runtime_strategy_set_json,
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_runtime_strategy_set_selection(cfg)

    msg = str(exc.value)
    assert "multi_pair_runtime_unsupported" in msg
    assert "settings_pair=KRW-BTC" in msg
    assert "spec_pair=KRW-ETH" in msg


def test_runtime_strategy_set_preflight_accepts_valid_single_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.delenv("ACTIVE_STRATEGIES", raising=False)
    monkeypatch.delenv("RUNTIME_STRATEGY_SET_JSON", raising=False)

    config.validate_runtime_strategy_set_selection(settings)


def test_live_real_order_execution_preflight_rejects_live_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_real_order_execution_preflight(settings)

    msg = str(exc.value)
    assert "LIVE_DRY_RUN=false is required for MODE=live run" in msg
    assert "LIVE_REAL_ORDER_ARMED=true is required for MODE=live run" in msg


def test_live_real_order_execution_preflight_accepts_armed_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    profile_path = _write_live_profile(Path(os.environ["DATA_ROOT"]).parent, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    config.validate_live_real_order_execution_preflight(settings)


def test_live_real_order_contract_summary_reports_target_delta_only_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)

    summary = config.live_execution_contract_summary(settings)

    assert summary["submit_authority_mode"] == "live_real_order_target_delta_only"
    assert str(summary["submit_authority_policy_hash"]).startswith("sha256:")
    assert summary["live_real_order_requires_target_delta"] is True
    assert summary["legacy_lot_native_compat_enabled"] is False
    assert summary["allowed_submit_plan_sources"] == ["target_delta", "residual_inventory"]
    assert "canonical_target_delta_sizing" in summary["allowed_submit_plan_authorities"]
    assert "configured_strategy_order_size" not in summary["allowed_submit_plan_authorities"]


def test_live_real_order_execution_preflight_rejects_single_strategy_lot_native(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "EXECUTION_ENGINE", "lot_native")
    profile_path = _write_live_profile(Path(os.environ["DATA_ROOT"]).parent, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_real_order_execution_preflight(settings)

    assert "live_real_order_requires_execution_engine_target_delta" in str(exc.value)


def test_live_real_order_preflight_rejects_incomplete_approved_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    profile_path = _write_incomplete_live_profile(tmp_path, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_real_order_execution_preflight(settings)

    assert "profile_missing_required_runtime_bound_parameter:SMA_FILTER_OVEREXT_LOOKBACK" in str(exc.value)


def test_live_real_order_request_builder_rejects_incomplete_approved_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    profile_path = _write_incomplete_live_profile(tmp_path, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(
        ValueError,
        match="profile_missing_required_runtime_bound_parameter:SMA_FILTER_OVEREXT_LOOKBACK",
    ):
        RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec("sma_with_filter", pair="KRW-BTC", interval=str(settings.INTERVAL)),
            through_ts_ms=1_700_000_180_000,
        )


def test_live_real_order_execution_preflight_rejects_armed_live_without_approved_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_real_order_execution_preflight(settings)

    assert "approved_profile_missing" in str(exc.value)


def test_live_armed_preflight_requires_approved_small_live_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "approved_profile_required_for_strategy:sma_with_filter" in str(exc.value)


def test_live_dry_run_preflight_requires_approved_live_dry_run_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "approved_profile_required_for_strategy:sma_with_filter" in str(exc.value)


def test_live_dry_run_preflight_rejects_small_live_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    profile_path = _write_live_profile(Path(os.environ["DATA_ROOT"]).parent, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "profile_mode" in str(exc.value)


def test_live_armed_preflight_rejects_profile_env_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    profile_path = _write_live_profile(tmp_path, mode="small_live", sma_short=_sma_int("SMA_SHORT") + 1)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "approved_profile_runtime_mismatch" in str(exc.value)


def test_live_startup_fails_closed_on_exit_policy_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "STRATEGY_EXIT_MAX_HOLDING_MIN", 0)
    profile_path = _write_live_profile(tmp_path, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))
    object.__setattr__(settings, "STRATEGY_EXIT_MAX_HOLDING_MIN", 10)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "approved_profile_runtime_mismatch" in str(exc.value)


def test_live_armed_preflight_rejects_paper_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    profile_path = _write_live_profile(tmp_path, mode="paper")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "profile_mode" in str(exc.value)


def test_live_dry_run_startup_requires_approved_live_dry_run_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "approved_profile_required_for_strategy:sma_with_filter" in str(exc.value)


def test_live_dry_run_startup_rejects_small_live_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    profile_path = _write_live_profile(tmp_path, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "profile_mode" in str(exc.value)


def test_live_dry_run_startup_rejects_incomplete_approved_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    profile_path = _write_incomplete_live_profile(tmp_path, mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "profile_missing_required_runtime_bound_parameter:SMA_FILTER_OVEREXT_LOOKBACK" in str(exc.value)


def test_live_dry_run_request_builder_rejects_incomplete_approved_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    profile_path = _write_incomplete_live_profile(tmp_path, mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(
        ValueError,
        match="profile_missing_required_runtime_bound_parameter:SMA_FILTER_OVEREXT_LOOKBACK",
    ):
        RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec("sma_with_filter", pair="KRW-BTC", interval=str(settings.INTERVAL)),
            through_ts_ms=1_700_000_180_000,
        )


def test_live_dry_run_startup_accepts_live_dry_run_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    profile_path = _write_live_profile(tmp_path, mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    config.validate_live_dry_run_loop_startup_contract(settings)


def test_live_preflight_rejects_dry_run_when_real_order_armed(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_DRY_RUN=true and LIVE_REAL_ORDER_ARMED=true is ambiguous" in str(exc.value)


def test_live_preflight_preserves_unarmed_non_dry_run_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "live_mode_not_dry_run_or_armed" in str(exc.value)


def test_live_execution_contract_summary_preserves_ambiguous_profile_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)

    summary = config.live_execution_contract_summary(settings)

    approved_profile = summary["approved_profile"]
    assert approved_profile["approved_profile_verification_ok"] is False
    assert approved_profile["approved_profile_block_reason"] == "live_mode_arming_flags_ambiguous"


def test_live_preflight_accepts_approved_profile_alias_selector(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    profile_path = _write_live_profile(tmp_path, mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")
    object.__setattr__(settings, "STRATEGY_APPROVED_PROFILE_PATH", str(profile_path))

    config.validate_live_mode_preflight(settings)


def test_live_execution_contract_emits_safe_env_metadata_and_lints(monkeypatch, tmp_path):
    env_file = tmp_path / "live.env"
    env_file.write_text(
        "\n".join(
            [
                "MODE=live",
                "BITHUMB_API_KEY=raw-key",
                "BITHUMB_API_SECRET=test-secret-for-hs256-min-32-bytes-v1",
                "BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED=true",
            ]
        ),
        encoding="utf-8",
    )
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "BITHUMB_API_KEY", "raw-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "<verified-live_dry_run-profile>")
    object.__setattr__(settings, "STRATEGY_NAME", "sma_with_filter")
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 500)
    monkeypatch.setenv("BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED", "true")
    monkeypatch.setenv("BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET + " ")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.example/test ")
    monkeypatch.setenv("START_CASH_KRW", "1000000")
    monkeypatch.setenv("BITHUMB_ENV_FILE", str(env_file))
    monkeypatch.setenv("BITHUMB_ENV_FILE_LIVE", str(tmp_path / "service-live.env"))

    summary = config.live_execution_contract_summary(
        settings,
        env_summary={
            "source_key": "BITHUMB_ENV_FILE_LIVE",
            "env_file": str(env_file),
            "loaded": True,
            "exists": True,
            "override": False,
        },
    )
    rendered = str(summary)

    assert summary["explicit_env_file"]["mtime_ns"]
    assert summary["explicit_env_file"]["inode"]
    assert summary["api_key_length"] == len("raw-key")
    assert summary["api_key_hash_prefix"]
    assert summary["api_secret_length"] == len(TEST_BITHUMB_API_SECRET)
    assert summary["api_secret_hash_prefix"]
    assert "raw-key" not in rendered
    assert "raw-secret" not in rendered
    assert "approved_profile_placeholder" in summary["live_env_contract_lints"]
    assert "deprecated_ignored_env_key:BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED" in summary["live_env_contract_lints"]
    assert "secret_bearing_key_has_surrounding_whitespace:BITHUMB_API_SECRET" in summary["live_env_contract_lints"]
    assert "secret_bearing_key_has_surrounding_whitespace:SLACK_WEBHOOK_URL" in summary["live_env_contract_lints"]
    assert "paper_only_key_in_live_env:START_CASH_KRW" in summary["live_env_contract_lints"]
    assert "risky_live_limit:MAX_DAILY_ORDER_COUNT>=500" in summary["live_env_contract_lints"]
    assert "live_env_file_source_mismatch:BITHUMB_ENV_FILE!=BITHUMB_ENV_FILE_LIVE" in summary["live_env_contract_lints"]
    lint_findings = summary["live_env_contract_lint_findings"]
    reason_codes = {str(item["reason_code"]) for item in lint_findings}
    assert "APPROVED_PROFILE_PLACEHOLDER" in reason_codes
    assert "DEPRECATED_IGNORED_ENV_KEY" in reason_codes
    assert "SECRET_VALUE_SURROUNDING_WHITESPACE" in reason_codes
    assert "PAPER_ONLY_KEY_IN_LIVE_ENV" in reason_codes
    assert "RISKY_LIVE_LIMIT" in reason_codes
    assert "LIVE_ENV_FILE_SOURCE_MISMATCH" in reason_codes
    for finding in lint_findings:
        assert finding["severity"]
        assert finding["recommended_action"]
        assert "raw-key" not in str(finding)
        assert "raw-secret" not in str(finding)
    config_contract = summary["config_contract"]
    assert config_contract["config_schema_version"] == "config_spec_v1"
    assert str(config_contract["config_spec_hash"]).startswith("sha256:")
    assert str(config_contract["env_example_hash"]).startswith("sha256:")
    assert str(config_contract["generated_docs_hash"]).startswith("sha256:")
    assert str(config_contract["settings_effective_hash"]).startswith("sha256:")
    assert "BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED" in config_contract["deprecated_env_keys"]
    assert "BITHUMB_API_SECRET" in config_contract["settings_explicit_keys"]
    assert "raw-key" not in str(config_contract)
    assert "raw-secret" not in str(config_contract)


def test_live_execution_contract_log_emits_redacted_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    _select_small_live_profile(Path(os.environ["DATA_ROOT"]).parent)
    object.__setattr__(settings, "BITHUMB_API_KEY", "visible-key-length")
    secret_value = "secret-value-must-not-log-32-bytes-ok"
    object.__setattr__(settings, "BITHUMB_API_SECRET", secret_value)

    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        summary = config.log_live_execution_contract(settings, caller="test")

    assert summary["live_dry_run"] is False
    assert "[LIVE_EXECUTION_CONTRACT]" in caplog.text
    assert "fingerprint=" in caplog.text
    assert "live_dry_run=0" in caplog.text
    assert "live_real_order_armed=1" in caplog.text
    assert "api_secret_present=1" in caplog.text
    assert "approved_profile_hash=" in caplog.text
    assert "promotion_content_hash=" in caplog.text
    assert "candidate_profile_hash=" in caplog.text
    assert "manifest_hash=" in caplog.text
    assert "dataset_content_hash=" in caplog.text
    assert "config_schema_version=config_spec_v1" in caplog.text
    assert "config_spec_hash=sha256:" in caplog.text
    assert "env_example_hash=sha256:" in caplog.text
    assert "settings_effective_hash=sha256:" in caplog.text
    assert secret_value not in caplog.text


def test_live_preflight_requires_live_risk_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "MAX_ORDER_KRW must be > 0" in msg
    assert "MAX_DAILY_LOSS_KRW must be > 0" in msg
    assert "MAX_DAILY_ORDER_COUNT must be > 0" in msg


def test_live_preflight_requires_chance_doc_side_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    monkeypatch.setattr(
        order_rules,
        "get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=0.0,
                ask_price_unit=0.0,
                min_qty=float(settings.LIVE_MIN_ORDER_QTY),
                qty_step=float(settings.LIVE_ORDER_QTY_STEP),
                min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
                max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
            ),
            source={
                "min_qty": "local_fallback",
                "qty_step": "local_fallback",
                "min_notional_krw": "local_fallback",
                "max_qty_decimals": "local_fallback",
                "bid_min_total_krw": "unsupported_by_doc",
                "ask_min_total_krw": "missing",
                "bid_price_unit": "unsupported_by_doc",
                "ask_price_unit": "missing",
            },
        ),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "bid_min_total_krw source must be chance_doc for MODE=live" in msg
    assert "ask_min_total_krw source must be chance_doc for MODE=live" in msg


def test_live_preflight_requires_credentials_in_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "BITHUMB_API_KEY is required when MODE=live" in msg
    assert "BITHUMB_API_SECRET is required when MODE=live" in msg


def test_live_preflight_rejects_short_bithumb_api_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    encode_calls = 0

    def _fail_encode(*_args, **_kwargs):
        nonlocal encode_calls
        encode_calls += 1
        raise AssertionError("PyJWT signing must not run for short Bithumb secrets")

    monkeypatch.setattr("bithumb_bot.broker.bithumb._jwt.encode", _fail_encode)
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    short_secret = "short-secret-lint-redact"
    object.__setattr__(settings, "BITHUMB_API_SECRET", short_secret)

    summary = config.live_execution_contract_summary(settings)
    findings = summary["live_env_contract_lint_findings"]
    short_secret_findings = [
        item for item in findings if item["reason_code"] == "AUTH_SECRET_TOO_SHORT"
    ]
    assert short_secret_findings == [
        {
            "reason_code": "AUTH_SECRET_TOO_SHORT",
            "severity": "ERROR",
            "message": "Bithumb API secret is too short for HS256 JWT signing.",
            "recommended_action": "replace_BITHUMB_API_SECRET_with_32_plus_byte_hs256_secret",
            "docs_hint": "docs/config-reference.md",
            "legacy_text": "bithumb_api_secret_too_short:BITHUMB_API_SECRET",
            "details": {
                "key": "BITHUMB_API_SECRET",
                "validation_kind": "jwt_hs256_secret",
                "min_bytes": 32,
                "actual_bytes": len(short_secret.encode("utf-8")),
            },
        }
    ]
    assert "bithumb_api_secret_too_short:BITHUMB_API_SECRET" in summary["live_env_contract_lints"]
    assert short_secret not in str(short_secret_findings)
    assert short_secret not in str(summary["live_env_contract_lint_findings"])
    assert short_secret not in str(summary)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "jwt_hs256_secret validation" in msg
    assert "reason_code=AUTH_SECRET_TOO_SHORT" in msg
    assert "min_bytes=32" in msg
    assert f"actual_bytes={len(short_secret.encode('utf-8'))}" in msg
    assert f"BITHUMB_API_SECRET={short_secret}" not in msg
    assert encode_calls == 0


def test_live_preflight_requires_explicit_arming_for_real_live_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_REAL_ORDER_ARMED=true is required" in str(exc.value)


def test_live_preflight_accepts_real_live_orders_when_explicitly_armed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(tmp_path)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_rejects_local_fallback_profile_in_armed_live_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(
        settings,
        "LIVE_ORDER_RULE_FALLBACK_PROFILE",
        config.LIVE_ORDER_RULE_FALLBACK_PROFILE_ALLOW_LOCAL_FALLBACK,
    )
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_ORDER_RULE_FALLBACK_PROFILE must be 'persisted_snapshot_required'" in str(exc.value)


def test_live_preflight_allows_local_fallback_profile_in_live_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(
        settings,
        "LIVE_ORDER_RULE_FALLBACK_PROFILE",
        config.LIVE_ORDER_RULE_FALLBACK_PROFILE_ALLOW_LOCAL_FALLBACK,
    )

    config.validate_live_mode_preflight(settings)


def test_live_preflight_rejects_invalid_order_rule_fallback_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_ORDER_RULE_FALLBACK_PROFILE", "legacy_bool_combo")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_ORDER_RULE_FALLBACK_PROFILE must be" in str(exc.value)


def test_live_preflight_rejects_invalid_submit_contract_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_SUBMIT_CONTRACT_PROFILE", "legacy_bool_combo")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_SUBMIT_CONTRACT_PROFILE must be" in str(exc.value)


def test_live_preflight_accepts_expected_submit_contract_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_SUBMIT_CONTRACT_PROFILE", config.LIVE_SUBMIT_CONTRACT_PROFILE_V1)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_requires_notifier_delivery_target(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.delenv("NTFY_TOPIC", raising=False)
    monkeypatch.delenv("NOTIFIER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "notifier must be enabled and configured with at least one delivery target" in str(exc.value)


def test_live_preflight_rejects_shared_runtime_roots(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    shared_root = Path(os.environ["DATA_ROOT"]).resolve()
    monkeypatch.setenv("BACKUP_ROOT", str(shared_root))
    object.__setattr__(settings, "BACKUP_ROOT", str(shared_root))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "runtime roots must not overlap or share parent/child paths when MODE=live" in str(exc.value)


def test_live_preflight_accepts_valid_live_configuration(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(tmp_path)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_requires_meaningful_live_price_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS must be a finite value > 0" in str(exc.value)


def test_live_preflight_accepts_meaningful_live_price_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_allows_kill_switch_liquidate_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)

    config.validate_live_mode_preflight(settings)

def test_live_preflight_requires_credentials_even_for_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "BITHUMB_API_KEY is required when MODE=live" in msg
    assert "BITHUMB_API_SECRET is required when MODE=live" in msg


def test_live_preflight_requires_explicit_db_path_for_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.delenv("DB_PATH", raising=False)
    object.__setattr__(settings, "DB_PATH", str((Path(os.environ["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DB_PATH must be explicitly set when MODE=live" in str(exc.value)


def test_live_preflight_rejects_paper_scoped_db_path_for_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    paper_db = str((Path(os.environ["DATA_ROOT"]) / "paper" / "trades" / "paper.sqlite").resolve())
    _set_valid_live_defaults(monkeypatch, db_path=paper_db)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DB_PATH must not point to a paper-scoped path when MODE=live" in str(exc.value)


@pytest.mark.parametrize(("env_key", "env_value"), [("LOG_ROOT", "logs"), ("BACKUP_ROOT", "backup")])
def test_live_preflight_rejects_relative_log_and_backup_roots(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    env_value: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(env_key, env_value)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert f"{env_key} must be an absolute path when MODE=live" in str(exc.value)


@pytest.mark.parametrize(("env_key", "child"), [("DATA_ROOT", "data"), ("LOG_ROOT", "logs"), ("BACKUP_ROOT", "backup")])
def test_live_preflight_rejects_repo_internal_roots(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    child: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(env_key, str((config.PROJECT_ROOT / child).resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert f"{env_key} must be outside repository when MODE=live" in str(exc.value)


def test_live_preflight_rejects_paper_scoped_root_segments(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("DATA_ROOT", str((Path(os.environ["DATA_ROOT"]).parent / "paper" / "data").resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DATA_ROOT must not contain a paper-scoped path segment when MODE=live" in str(exc.value)


@pytest.mark.parametrize("env_key", ["LOG_ROOT", "BACKUP_ROOT"])
def test_live_preflight_rejects_paper_scoped_log_and_backup_segments(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(
        env_key,
        str((Path(os.environ["DATA_ROOT"]).parent / "paper" / env_key.lower()).resolve()),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert f"{env_key} must not contain a paper-scoped path segment when MODE=live" in str(exc.value)


def test_live_preflight_accepts_non_default_live_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    custom_live_db = str((Path(os.environ["DATA_ROOT"]) / "live" / "trades" / "live-prod.sqlite").resolve())
    _set_valid_live_defaults(monkeypatch, db_path=custom_live_db)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_accepts_explicit_non_default_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    custom_live_db = str((Path(os.environ["DATA_ROOT"]) / "live" / "trades" / "live_trading.sqlite").resolve())
    _set_valid_live_defaults(monkeypatch, db_path=custom_live_db)

    config.validate_live_mode_preflight(settings)

@pytest.mark.parametrize(
    ("env_key", "env_value"),
    [
        ("START_CASH_KRW", "1000000"),
        ("BUY_FRACTION", "0.5"),
        ("FEE_RATE", "0.0004"),
        ("SLIPPAGE_BPS", "5"),
    ],
)
def test_live_preflight_rejects_paper_only_env_keys_in_live(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    env_value: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(env_key, env_value)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "paper/test-like config mixing is not allowed when MODE=live" in msg
    assert env_key in msg


def test_live_preflight_accepts_clean_live_env_without_paper_only_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.delenv("START_CASH_KRW", raising=False)
    monkeypatch.delenv("BUY_FRACTION", raising=False)
    monkeypatch.delenv("FEE_RATE", raising=False)
    monkeypatch.delenv("SLIPPAGE_BPS", raising=False)

    config.validate_live_mode_preflight(settings)

def test_live_preflight_requires_notifier_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("NOTIFIER_ENABLED", "false")
    monkeypatch.delenv("NOTIFIER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "notifier must be enabled and configured" in str(exc.value)


def test_live_preflight_accepts_notifier_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/abc")

    config.validate_live_mode_preflight(settings)


def test_live_preflight_paper_mode_notifier_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTIFIER_ENABLED", "false")
    monkeypatch.delenv("NOTIFIER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_fails_when_order_rule_sync_fails_and_manual_rules_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)
    order_rules._cached_rules.clear()

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "min_qty must be > 0" in msg
    assert "qty_step must be > 0" in msg
    assert "min_notional_krw must be > 0" in msg
    assert "max_qty_decimals must be > 0" in msg


def test_live_preflight_surfaces_document_schema_violation_when_manual_rules_are_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch, stub_order_rules=False)
    monkeypatch.setattr(order_rules, "get_effective_order_rules", _REAL_GET_EFFECTIVE_ORDER_RULES)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)
    order_rules._cached_rules.clear()

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: (_ for _ in ()).throw(order_rules.OrderChanceSchemaError("/v1/orders/chance response.market.bid.min_total must be numeric")),
    )
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        config,
        "_dispatch_order_rule_resolution_operator_event",
        lambda resolved: events.append(dict(resolved.operator_event)),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "min_qty must be > 0" in msg
    assert events
    assert events[0]["event_type"] == "order_rule_fallback_used"
    assert "OrderChanceSchemaError" in str(events[0]["reason_detail"])
    assert "response.market.bid.min_total must be numeric" in str(events[0]["reason_detail"])


def test_live_preflight_delivers_order_rule_fallback_event_at_operator_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch, stub_order_rules=False)
    monkeypatch.setattr(order_rules, "get_effective_order_rules", _REAL_GET_EFFECTIVE_ORDER_RULES)
    order_rules._cached_rules.clear()
    object.__setattr__(
        settings,
        "LIVE_ORDER_RULE_FALLBACK_PROFILE",
        config.LIVE_ORDER_RULE_FALLBACK_PROFILE_ALLOW_LOCAL_FALLBACK,
    )

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("exchange unavailable")),
    )
    monkeypatch.setattr(
        order_rules,
        "notify",
        lambda _msg: pytest.fail("domain order-rule resolution must not notify directly"),
        raising=False,
    )

    delivered: list[tuple[str, dict[str, object]]] = []

    class FakeOperatorNotificationService:
        def send_event(self, event_name: str, /, **fields: object) -> None:
            delivered.append((event_name, fields))

    monkeypatch.setattr(
        operator_notification_service,
        "OperatorNotificationService",
        lambda: FakeOperatorNotificationService(),
    )

    domain_resolution = order_rules.get_effective_order_rules("KRW-BTC")
    assert domain_resolution.operator_event["event_type"] == "order_rule_fallback_used"
    assert delivered == []

    order_rules._cached_rules.clear()
    config.validate_live_mode_preflight(settings)

    assert len(delivered) == 1
    event_name, fields = delivered[0]
    assert event_name == "order_rule_fallback_used"
    assert fields["market"] == "KRW-BTC"
    assert fields["source_mode"] == "local_fallback"
    assert fields["reason_code"] == "UNRECOVERABLE"
    assert "RuntimeError: exchange unavailable" in str(fields["reason_detail"])
    assert "order-rule auto-sync unavailable" in str(fields["fallback_risk"])


def test_live_preflight_propagates_pytest_notification_safety_violation_from_order_rule_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch, stub_order_rules=False)
    monkeypatch.setattr(order_rules, "get_effective_order_rules", _REAL_GET_EFFECTIVE_ORDER_RULES)
    order_rules._cached_rules.clear()
    object.__setattr__(
        settings,
        "LIVE_ORDER_RULE_FALLBACK_PROFILE",
        config.LIVE_ORDER_RULE_FALLBACK_PROFILE_ALLOW_LOCAL_FALLBACK,
    )
    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("exchange unavailable")),
    )

    class BlockingOperatorNotificationService:
        def send_event(self, event_name: str, /, **fields: object) -> None:
            raise notifier.PytestNotificationSafetyViolation("blocked notification transport")

    monkeypatch.setattr(
        operator_notification_service,
        "OperatorNotificationService",
        lambda: BlockingOperatorNotificationService(),
    )

    with pytest.raises(notifier.PytestNotificationSafetyViolation, match="blocked notification transport") as exc:
        config.validate_live_mode_preflight(settings)

    assert "failed to resolve order rules" not in str(exc.value)


def test_live_preflight_passes_with_valid_auto_synced_order_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    order_rules._cached_rules.clear()

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: order_rules.OrderRules(
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            max_qty_decimals=4,
        ),
    )

    config.validate_live_mode_preflight(settings)


def test_live_preflight_allows_non_positive_strict_threshold_when_strict_mode_is_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 0.0)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_accepts_positive_strict_threshold_when_strict_mode_is_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 10000.0)

    config.validate_live_mode_preflight(settings)


@pytest.mark.parametrize(
    "invalid_threshold",
    [0.0, -1.0, math.nan, math.inf, -math.inf, "abc"],
)
def test_live_preflight_rejects_invalid_strict_threshold_when_strict_mode_is_on(
    monkeypatch: pytest.MonkeyPatch,
    invalid_threshold: object,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", invalid_threshold)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW" in msg
    assert "LIVE_FILL_FEE_STRICT_MODE=true" in msg


def test_market_preflight_rejects_unsupported_market(monkeypatch: pytest.MonkeyPatch) -> None:
    from bithumb_bot import runtime_strategy_set

    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "PAIR", "KRW-ABC")
    profile_path = _write_live_profile(Path(os.environ["DATA_ROOT"]).parent, mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "unsupported pair" in str(exc.value)


def test_market_preflight_rejects_legacy_alias_in_live_mode_even_when_catalog_supports_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from bithumb_bot import runtime_strategy_set

    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "PAIR", "btc_krw")
    profile_path = _write_live_profile(Path(os.environ["DATA_ROOT"]).parent, mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "invalid configured market format" in str(exc.value)
    assert "MODE=live" in str(exc.value)


def test_market_preflight_blocks_warning_state_in_live_real_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(Path(os.environ["DATA_ROOT"]).parent)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="CAUTION")]),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "market_warning=CAUTION" in str(exc.value)


def test_market_preflight_allows_warning_state_in_dry_run_live_mode(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="CAUTION")]),
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)

    assert "market preflight detected warning state" in caplog.text


def test_market_preflight_blocks_on_catalog_fetch_failure_in_live_real_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(Path(os.environ["DATA_ROOT"]).parent)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(config.MarketCatalogError("catalog down")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "catalog fetch failed" in msg
    assert "endpoint=/v1/market/all" in msg
    assert "isDetails=true" in msg
    assert "mode=live" in msg
    assert "block_on_catalog_error=True" in msg


def test_market_preflight_allows_catalog_fetch_failure_in_dry_run_with_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(config.MarketCatalogError("catalog down")),
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)

    assert "catalog fetch failed" in caplog.text
    assert "endpoint=/v1/market/all" in caplog.text
    assert "isDetails=true" in caplog.text
    assert "mode=live" in caplog.text
    assert "dry_run=True" in caplog.text
    assert "block_on_catalog_error=False" in caplog.text
    assert "schema_drift=True" in caplog.text


def test_market_preflight_warns_and_allows_warning_state_in_paper_mode(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MODE", "paper")
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="CAUTION")]),
    )

    with caplog.at_level("WARNING"):
        config.validate_market_preflight(settings)

    assert "market preflight detected warning state" in caplog.text


def test_market_preflight_warns_and_allows_warning_state_in_dryrun_mode(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MODE", "dryrun")
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="CAUTION")]),
    )

    with caplog.at_level("WARNING"):
        config.validate_market_preflight(settings)

    assert "market preflight detected warning state" in caplog.text


def test_market_preflight_treats_unexpected_market_warning_as_unknown_and_blocks_live_real(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(Path(os.environ["DATA_ROOT"]).parent)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="SUSPICIOUS_NEW_STATE")]),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "market_warning=UNKNOWN" in str(exc.value)


def test_market_runtime_validation_forces_registry_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    calls: list[tuple[bool, float, bool]] = []
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **kwargs: (
            calls.append((bool(kwargs["refresh"]), float(kwargs["ttl_seconds"]), bool(kwargs["is_details"]))),
            MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
        )[1],
    )

    config.validate_market_runtime(settings)

    assert calls == [(True, float(settings.MARKET_REGISTRY_CACHE_TTL_SEC), True)]


def test_live_preflight_blocks_startup_on_accounts_schema_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(Path(os.environ["DATA_ROOT"]).parent)
    monkeypatch.setattr(config, "_fetch_accounts_payload_for_preflight", lambda **_kwargs: {"currency": "KRW"})

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "/v1/accounts REST snapshot preflight validation failed" in msg
    assert "reason=schema mismatch" in msg
    assert "reason_code=ACCOUNTS_SCHEMA_MISMATCH" in msg


def test_live_preflight_blocks_startup_on_required_currency_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(Path(os.environ["DATA_ROOT"]).parent)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )
    monkeypatch.setattr(config, "_flat_start_safety_for_accounts_preflight", lambda: (False, "not_flat_start"))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "/v1/accounts REST snapshot preflight validation failed" in msg
    assert "reason=required currency missing" in msg
    assert "reason_code=ACCOUNTS_REQUIRED_CURRENCY_MISSING" in msg


@pytest.mark.parametrize("mode", ["paper", "dryrun"])
def test_accounts_preflight_diagnostics_are_warning_only_in_non_live_modes(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    mode: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MODE", mode)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "BTC", "balance": "0.2", "locked": "0"}],
    )

    with caplog.at_level("WARNING"):
        config.validate_market_preflight(settings)

    assert f"accounts REST snapshot preflight warning (mode={mode})" in caplog.text
    assert "reason=required currency missing" in caplog.text
    assert "reason_code=ACCOUNTS_REQUIRED_CURRENCY_MISSING" in caplog.text


def test_live_preflight_runs_market_and_accounts_contracts_together(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(tmp_path)
    calls: list[str] = []

    def _market_registry(**_kwargs):
        calls.append("market")
        return MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")])

    def _accounts_payload(**_kwargs):
        calls.append("accounts")
        return [
            {"currency": "KRW", "balance": "1000000", "locked": "0"},
            {"currency": "BTC", "balance": "0.1", "locked": "0"},
        ]

    monkeypatch.setattr(config, "_fetch_market_registry_for_preflight", _market_registry)
    monkeypatch.setattr(config, "_fetch_accounts_payload_for_preflight", _accounts_payload)

    config.validate_live_mode_preflight(settings)

    assert calls == ["market", "accounts"]


def test_market_preflight_rejects_registry_inconsistency_when_market_lookup_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _InconsistentRegistry:
        def require_supported(self, market: str) -> str:
            return market

        def get(self, _market: str) -> None:
            return None

    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(config, "_fetch_market_registry_for_preflight", lambda **_kwargs: _InconsistentRegistry())

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "registry inconsistency" in str(exc.value)


def test_market_preflight_live_defaults_to_forced_registry_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    calls: list[tuple[bool, float, bool]] = []
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MARKET_REGISTRY_CACHE_TTL_SEC", 123.0)
    monkeypatch.delenv("MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH", raising=False)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **kwargs: (
            calls.append((bool(kwargs["refresh"]), float(kwargs["ttl_seconds"]), bool(kwargs["is_details"]))),
            MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
        )[1],
    )

    config.validate_market_preflight(settings)

    assert calls == [(True, 123.0, True)]


def test_market_preflight_paper_defaults_to_cache_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    calls: list[tuple[bool, float, bool]] = []
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "MARKET_REGISTRY_CACHE_TTL_SEC", 456.0)
    monkeypatch.delenv("MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH", raising=False)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **kwargs: (
            calls.append((bool(kwargs["refresh"]), float(kwargs["ttl_seconds"]), bool(kwargs["is_details"]))),
            MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
        )[1],
    )

    config.validate_market_preflight(settings)

    assert calls == [(False, 456.0, True)]


def test_market_preflight_rejects_negative_cache_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MARKET_REGISTRY_CACHE_TTL_SEC", -1.0)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: pytest.fail("market registry fetch should not run when ttl is invalid"),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "MARKET_REGISTRY_CACHE_TTL_SEC must be >= 0" in str(exc.value)


def test_live_preflight_rejects_legacy_broad_scan_env_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("BITHUMB_V1_ORDER_SCAN_MARKET", "KRW-BTC")
    monkeypatch.setenv("BITHUMB_V1_ORDER_SCAN_STATES", "wait,done")
    monkeypatch.setenv("BITHUMB_V1_ORDER_SCAN_LIMIT", "100")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    message = str(exc.value)
    assert "legacy /v1/orders broad-scan env is not allowed" in message
    assert "BITHUMB_V1_ORDER_SCAN_MARKET" in message


def test_accounts_preflight_passes_with_valid_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    config.validate_live_mode_preflight(settings)


def test_accounts_preflight_schema_error_blocks_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(config, "_fetch_accounts_payload_for_preflight", lambda **_kwargs: {"status": "0000"})

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "schema mismatch" in msg
    assert "reason=schema mismatch" in msg
    assert "ACCOUNTS_SCHEMA_MISMATCH" in msg
    assert "row_count=0" in msg
    assert "execution_mode=live_dry_run_unarmed" in msg
    assert "quote_currency=KRW" in msg
    assert "base_currency=BTC" in msg


def test_accounts_preflight_required_currency_missing_blocks_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(Path(os.environ["DATA_ROOT"]).parent)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )
    monkeypatch.setattr(config, "_flat_start_safety_for_accounts_preflight", lambda: (False, "not_flat_start"))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "required currency missing" in msg
    assert "reason=required currency missing" in msg
    assert "row_count=1" in msg
    assert "currencies=KRW" in msg
    assert "ACCOUNTS_REQUIRED_CURRENCY_MISSING" in msg
    assert "missing base currency row 'BTC'" in msg
    assert "execution_mode=live_real_order_path" in msg
    assert "base_currency_missing_policy=block_when_base_currency_row_missing" in msg
    assert "result=fail_real_order_blocked" in msg


def test_accounts_preflight_allows_missing_base_currency_in_live_armed_flat_start(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(tmp_path)
    monkeypatch.setattr(config, "_flat_start_safety_for_accounts_preflight", lambda: (True, "flat_start_safe"))
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)
    assert "ACCOUNTS_BASE_ROW_MISSING_ALLOWED" in caplog.text
    assert "base_currency_missing_policy=allow_flat_start_when_no_open_or_unresolved_exposure" in caplog.text
    assert "flat_start_allowed=1" in caplog.text


def test_accounts_preflight_missing_base_currency_blocks_live_armed_when_not_flat_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(Path(os.environ["DATA_ROOT"]).parent)
    monkeypatch.setattr(config, "_flat_start_safety_for_accounts_preflight", lambda: (False, "local_unresolved_or_open_orders=1"))
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)
    msg = str(exc.value)
    assert "required currency missing" in msg
    assert "flat_start_allowed=0" in msg
    assert "flat_start_reason=local_unresolved_or_open_orders=1" in msg


def test_accounts_preflight_allows_missing_base_currency_in_live_dry_run_unarmed(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)
    assert "ACCOUNTS_BASE_ROW_MISSING_ALLOWED" in caplog.text
    assert "result=pass_no_position_allowed" in caplog.text
    assert "execution_mode=live_dry_run_unarmed" in caplog.text


def test_accounts_preflight_still_requires_quote_currency_in_live_dry_run_unarmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "BTC", "balance": "0.1", "locked": "0"}],
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "required currency missing" in msg
    assert "missing quote currency row 'KRW'" in msg
    assert "ACCOUNTS_REQUIRED_CURRENCY_MISSING" in msg
    assert "execution_mode=live_dry_run_unarmed" in msg
    assert "base_currency_missing_policy=allow_zero_position_start_in_dry_run" in msg


def test_live_preflight_allows_missing_price_unit_sources_with_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", TEST_BITHUMB_API_KEY)
    object.__setattr__(settings, "BITHUMB_API_SECRET", TEST_BITHUMB_API_SECRET)
    _select_small_live_profile(tmp_path)
    monkeypatch.setattr(
        order_rules,
        "get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                market_id="KRW-BTC",
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=0.0,
                ask_price_unit=0.0,
                min_qty=float(settings.LIVE_MIN_ORDER_QTY),
                qty_step=float(settings.LIVE_ORDER_QTY_STEP),
                min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
                max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
            ),
            source={
                "min_qty": "local_fallback",
                "qty_step": "local_fallback",
                "min_notional_krw": "local_fallback",
                "max_qty_decimals": "local_fallback",
                "bid_min_total_krw": "chance_doc",
                "ask_min_total_krw": "chance_doc",
                "bid_price_unit": "missing",
                "ask_price_unit": "missing",
            },
        ),
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)
    assert "optional order-rule source gaps detected" in caplog.text


def test_accounts_preflight_duplicate_currency_blocks_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [
            {"currency": "KRW", "balance": "1000000", "locked": "0"},
            {"currency": "KRW", "balance": "2000", "locked": "0"},
        ],
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "reason=duplicate currency" in msg
    assert "ACCOUNTS_DUPLICATE_CURRENCY" in msg
    assert "duplicate_currencies=KRW" in msg


def test_accounts_preflight_auth_failure_is_classified(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("status=401 unauthorized")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "/v1/accounts REST snapshot preflight authentication failed" in msg
    assert "ACCOUNTS_AUTH_FAILED" in msg


def test_accounts_preflight_permission_failure_is_classified_as_auth_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("status=403 out_of_scope permission denied")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "/v1/accounts REST snapshot preflight authentication failed" in msg
    assert "ACCOUNTS_AUTH_FAILED" in msg
    assert "class=PERMISSION" in msg


def test_accounts_preflight_transport_failure_is_classified(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("transport error timeout")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "/v1/accounts REST snapshot preflight transport failed" in msg
    assert "ACCOUNTS_TRANSPORT_FAILED" in msg


def test_accounts_preflight_unclassified_private_error_is_transport_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("unexpected broker fault")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "/v1/accounts REST snapshot preflight transport failed" in msg
    assert "ACCOUNTS_TRANSPORT_FAILED" in msg
    assert "class=UNRECOVERABLE" in msg
