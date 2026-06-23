from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.broker.balance_source import BalanceSnapshot
from bithumb_bot.broker.base import BrokerBalance
from bithumb_bot.db_core import ensure_db, record_strategy_decision, set_portfolio_breakdown
from bithumb_bot.risk_layer_replay import verify_risk_layer_replay
from bithumb_bot.run_loop_execution_planner import ExecutionPlanner
from bithumb_bot.strategy_policy_contract import (
    EntryExecutionIntent,
    PositionSnapshot,
    StrategyDecisionV2,
)
from bithumb_bot.h74_observation import (
    H74_OBSERVATION_PARAMETERS,
    H74_SOURCE_OBSERVATION_PARAMETERS,
    H74_SOURCE_OBSERVATION_AUTHORITY_ARTIFACT_TYPE,
    H74ObservationAuthorityError,
    build_h74_observation_authority_payload,
    build_h74_source_observation_authority_payload,
    h74_source_observation_risk_policy_hash,
    h74_source_runtime_values_from_settings,
    verify_h74_observation_authority,
    verify_h74_source_observation_authority,
)
from bithumb_bot.config import (
    LiveModeValidationError,
    settings,
    validate_live_strategy_selection,
    validate_runtime_strategy_set_selection,
)
from bithumb_bot.execution_authority import execution_authority_from_payload
from bithumb_bot.research.strategy_spec import runtime_bound_behavior_parameter_names
from bithumb_bot.runtime_strategy_set import (
    ProfileAuthorityContext,
    RuntimeMarketScope,
    RuntimeDecisionRequestBuilder,
    RuntimeStrategySet,
    RuntimeStrategySpec,
)
from dataclasses import replace
import json


class _BalanceSnapshotBroker:
    def __init__(
        self,
        *,
        cash_available: float = 1_000_000.0,
        cash_locked: float = 0.0,
        asset_available: float = 0.0,
        asset_locked: float = 0.0,
        observed_ts_ms: int = 1_704_046_800_000,
        fail: bool = False,
    ) -> None:
        self.cash_available = float(cash_available)
        self.cash_locked = float(cash_locked)
        self.asset_available = float(asset_available)
        self.asset_locked = float(asset_locked)
        self.observed_ts_ms = int(observed_ts_ms)
        self.fail = bool(fail)
        self.snapshot_calls = 0

    def get_balance_snapshot(self) -> BalanceSnapshot:
        self.snapshot_calls += 1
        if self.fail:
            raise RuntimeError("broker snapshot unavailable")
        return BalanceSnapshot(
            source_id="accounts_v1_rest_snapshot",
            observed_ts_ms=self.observed_ts_ms,
            asset_ts_ms=self.observed_ts_ms,
            balance=BrokerBalance(
                cash_available=self.cash_available,
                cash_locked=self.cash_locked,
                asset_available=self.asset_available,
                asset_locked=self.asset_locked,
            ),
        )


def _force_live_strategy_risk_settings(*, db_path: Path) -> dict[str, object]:
    original = {
        "DB_PATH": settings.DB_PATH,
        "MODE": settings.MODE,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "START_CASH_KRW": settings.START_CASH_KRW,
        "PAIR": settings.PAIR,
    }
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50_000.0)
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    return original


def _restore_settings(original: dict[str, object]) -> None:
    for key, value in original.items():
        object.__setattr__(settings, key, value)


def _record_verified_flat_reconcile(*, observed_ts_ms: int = 1_704_046_800_000) -> None:
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_source": "accounts_v1_rest_snapshot",
            "balance_observed_ts_ms": int(observed_ts_ms),
            "broker_cash_available": 1_000_000.0,
            "broker_cash_locked": 0.0,
            "broker_asset_available": 0.0,
            "broker_asset_locked": 0.0,
            "broker_asset_qty": 0.0,
            "dust_residual_present": 0,
        },
        now_epoch_sec=1_704_046_800.0,
    )


def _rehash_authority(payload: dict) -> dict:
    from bithumb_bot.research.hashing import sha256_prefixed

    payload["authority_parameter_hash"] = sha256_prefixed(payload["hash_bound_parameters"])
    payload["authority_content_hash"] = sha256_prefixed(
        {k: v for k, v in payload.items() if k != "authority_content_hash"}
    )
    return payload


def _rehash_source_risk_policy(payload: dict) -> dict:
    payload["risk_policy_hash"] = h74_source_observation_risk_policy_hash(payload["risk_policy"])
    payload["hash_bound_parameters"]["risk_policy_hash"] = payload["risk_policy_hash"]
    return _rehash_authority(payload)


def _source_authority() -> dict:
    return build_h74_source_observation_authority_payload(
        source_candidate_artifact_hash="sha256:source-candidate",
        backtest_report_hash="sha256:backtest",
        validation_run_hash="sha256:validation",
        code_commit_sha="test-commit",
    )


def _source_parameters() -> dict[str, object]:
    return {
        name: H74_SOURCE_OBSERVATION_PARAMETERS[name]
        for name in runtime_bound_behavior_parameter_names("daily_participation_sma")
    }


def _h74_source_cfg(authority_path: Path | str, **overrides) -> object:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_RULES="max_holding_time",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_ORDER_KRW=100_000,
        MAX_DAILY_ORDER_COUNT=2,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(authority_path),
        **overrides,
    )
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(cfg, key, value)
    return cfg


def _flat_readiness_payload() -> dict[str, object]:
    return {
        "residual_inventory_qty": 0.0,
        "residual_inventory_notional_krw": 0.0,
        "residual_inventory_state": "flat",
        "residual_inventory_policy_allows_buy": True,
        "residual_inventory_policy_allows_sell": False,
        "residual_inventory_policy_allows_run": True,
        "cash_available": 1_000_000.0,
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.0,
            "balance_source_stale": False,
        },
        "projection_converged": True,
        "projection_convergence": {"converged": True},
        "broker_portfolio_converged": True,
        "open_order_count": 0,
        "accounting_projection_ok": True,
        "active_fee_accounting_blocker": False,
        "residual_proof_min_qty": 0.0001,
        "residual_proof_min_notional_krw": 5000.0,
    }


class _Readiness:
    def as_dict(self) -> dict[str, object]:
        return _flat_readiness_payload()


def _h74_buy_decision(candle_ts: int) -> StrategyDecisionV2:
    return StrategyDecisionV2(
        strategy_name="daily_participation_sma",
        raw_signal="BUY",
        raw_reason="daily_participation_fallback_allowed",
        entry_signal="BUY",
        entry_reason="daily_participation_fallback_allowed",
        exit_signal="HOLD",
        exit_reason="no_exit",
        final_signal="BUY",
        final_reason="daily_participation_fallback_allowed",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(
            in_position=False,
            entry_allowed=True,
            exit_allowed=False,
            terminal_state="flat",
            dust_state="flat",
            effective_flat=True,
        ),
        execution_intent=EntryExecutionIntent(
            side="BUY",
            intent="enter",
            pair="KRW-BTC",
            requires_execution_sizing=True,
            budget_fraction_of_cash=1.0,
            max_budget_krw=100_000.0,
        ),
        entry_decision=object(),  # type: ignore[arg-type]
        trace={
            "entry_signal_source": "daily_participation_fallback",
            "reason_code": "daily_participation_fallback_allowed",
            "candle_ts": candle_ts,
        },
        policy_hash="sha256:h74-policy",
        policy_contract_hash="sha256:h74-contract",
        policy_input_hash="sha256:h74-input",
        policy_decision_hash="sha256:h74-decision",
    )


def _h74_runtime_bundle(
    *,
    candle_ts: int,
    authority_context: ProfileAuthorityContext,
) -> SimpleNamespace:
    strategy_instance_id = "h74-source-observation"
    spec = RuntimeStrategySpec(
        "daily_participation_sma",
        strategy_instance_id=strategy_instance_id,
        pair="KRW-BTC",
        interval="1m",
        desired_exposure_krw=100_000.0,
        parameters=_source_parameters(),
    )
    strategy_set = RuntimeStrategySet(
        source="RUNTIME_STRATEGY_SET_JSON",
        strategies=(spec,),
        market_scope=RuntimeMarketScope(mode="single_pair", pair="KRW-BTC", interval="1m"),
    )
    decision = _h74_buy_decision(candle_ts)
    base_context = {
        "strategy": "daily_participation_sma",
        "strategy_instance_id": strategy_instance_id,
        "pair": "KRW-BTC",
        "interval": "1m",
        "runtime_decision_request_hash": "sha256:h74-request",
        "strategy_parameters_hash": "sha256:h74-params",
        "approved_profile_hash": None,
        "runtime_contract_hash": "sha256:h74-runtime-contract",
        "plugin_contract_hash": "sha256:h74-plugin-contract",
        "scope_key_hash": "sha256:h74-scope",
        "through_ts_ms": candle_ts,
        "profile_authority_context": authority_context.as_dict(),
    }
    result = SimpleNamespace(
        decision=decision,
        base_context=base_context,
        candle_ts=candle_ts,
        market_price=100_000_000.0,
        policy_hashes={
            "policy_contract_hash": decision.policy_contract_hash,
            "policy_input_hash": decision.policy_input_hash,
            "policy_decision_hash": decision.policy_decision_hash,
        },
        replay_fingerprint={"runtime_decision_request_hash": "sha256:h74-request"},
        boundary={"phase": "h74_live_observation_test"},
        as_legacy_dict=lambda: dict(base_context),
    )
    return SimpleNamespace(
        strategy_set=strategy_set,
        results=(result,),
        candle_ts=candle_ts,
        market_price=100_000_000.0,
        as_dict=lambda: {"schema_version": 1, "results": [result.as_legacy_dict()]},
        content_hash=lambda: "sha256:h74-runtime-bundle",
    )


def test_h74_observation_authority_hash_binds_50k_parameters() -> None:
    payload = build_h74_observation_authority_payload()

    bound = payload["hash_bound_parameters"]
    assert bound["DAILY_PARTICIPATION_MAX_ORDER_KRW"] == 50_000
    assert payload["authority_parameter_hash"].startswith("sha256:")
    verify_h74_observation_authority(payload, runtime_values=H74_OBSERVATION_PARAMETERS)


def test_h74_authority_binds_all_behavior_affecting_parameters() -> None:
    payload = build_h74_observation_authority_payload()
    bound = set(payload["hash_bound_parameters"])
    required = set(runtime_bound_behavior_parameter_names("daily_participation_sma"))

    assert required - bound == set()
    assert bound - required >= {"strategy_name", "market", "interval", "max_daily_order_count", "max_notional_krw"}


def test_h74_authority_rejects_missing_behavior_affecting_parameter() -> None:
    payload = build_h74_observation_authority_payload()
    payload["hash_bound_parameters"].pop("DAILY_PARTICIPATION_BUY_FRACTION")
    from bithumb_bot.research.hashing import sha256_prefixed

    payload["authority_parameter_hash"] = sha256_prefixed(payload["hash_bound_parameters"])
    payload["authority_content_hash"] = sha256_prefixed(
        {k: v for k, v in payload.items() if k != "authority_content_hash"}
    )

    with pytest.raises(H74ObservationAuthorityError, match="DAILY_PARTICIPATION_BUY_FRACTION"):
        verify_h74_observation_authority(payload, runtime_values=H74_OBSERVATION_PARAMETERS)


def test_h74_authority_rejects_runtime_mismatch_for_each_bound_parameter() -> None:
    payload = build_h74_observation_authority_payload()
    for name in runtime_bound_behavior_parameter_names("daily_participation_sma"):
        runtime = dict(H74_OBSERVATION_PARAMETERS)
        current = runtime[name]
        runtime[name] = (not current) if isinstance(current, bool) else f"{current}_changed"
        with pytest.raises(H74ObservationAuthorityError, match=name):
            verify_h74_observation_authority(payload, runtime_values=runtime)


def test_h74_observation_authority_rejects_100k_runtime_mismatch() -> None:
    payload = build_h74_observation_authority_payload()
    runtime = dict(H74_OBSERVATION_PARAMETERS)
    runtime["DAILY_PARTICIPATION_MAX_ORDER_KRW"] = 100_000

    with pytest.raises(H74ObservationAuthorityError, match="DAILY_PARTICIPATION_MAX_ORDER_KRW"):
        verify_h74_observation_authority(payload, runtime_values=runtime)


def test_h74_observation_authority_expires_after_7_days() -> None:
    payload = build_h74_observation_authority_payload(
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)
    )

    with pytest.raises(H74ObservationAuthorityError, match="expired"):
        verify_h74_observation_authority(payload, runtime_values=H74_OBSERVATION_PARAMETERS)


def test_h74_observation_authority_not_accepted_as_promotion_profile() -> None:
    payload = build_h74_observation_authority_payload()

    assert payload["promotion_grade"] is False
    assert payload["research_promotion_evidence"] is False
    assert payload["approved_profile_evidence"] is False


def test_h74_observation_authority_requires_daily_window_09_11() -> None:
    payload = build_h74_observation_authority_payload()
    runtime = dict(H74_OBSERVATION_PARAMETERS)
    runtime["DAILY_PARTICIPATION_WINDOW_START_HOUR_KST"] = 0

    with pytest.raises(H74ObservationAuthorityError, match="DAILY_PARTICIPATION_WINDOW_START_HOUR_KST"):
        verify_h74_observation_authority(payload, runtime_values=runtime)


def test_h74_observation_authority_requires_holding_74() -> None:
    payload = build_h74_observation_authority_payload()
    runtime = dict(H74_OBSERVATION_PARAMETERS)
    runtime["STRATEGY_EXIT_MAX_HOLDING_MIN"] = 75

    with pytest.raises(H74ObservationAuthorityError, match="STRATEGY_EXIT_MAX_HOLDING_MIN"):
        verify_h74_observation_authority(payload, runtime_values=runtime)


def test_live_observation_authority_runtime_hook_rejects_env_mismatch(tmp_path, monkeypatch) -> None:
    authority = build_h74_observation_authority_payload()
    path = tmp_path / "authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("LIVE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_MAX_HOLDING_MIN=75,
        APPROVED_STRATEGY_PROFILE_PATH="",
    )

    with pytest.raises(LiveModeValidationError, match="live_observation_authority_validation_failed"):
        validate_live_strategy_selection(cfg)


def test_h74_observation_authority_does_not_replace_approved_profile(tmp_path, monkeypatch) -> None:
    authority = build_h74_observation_authority_payload()
    path = tmp_path / "authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("LIVE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_DAILY_ORDER_COUNT=1,
        APPROVED_STRATEGY_PROFILE_PATH="",
    )

    with pytest.raises(LiveModeValidationError, match="approved_profile_required_for_strategy:daily_participation_sma"):
        validate_live_strategy_selection(cfg)


def test_live_observation_authority_path_does_not_grant_strategy_run_operation() -> None:
    authority = execution_authority_from_payload(build_h74_observation_authority_payload())

    assert authority.allows("h74_live_observation_50k")
    assert not authority.allows("strategy_run")
    assert authority.risk_authority is False


def test_h74_source_observation_authority_verifies_100k_exact_params() -> None:
    payload = _source_authority()

    assert payload["artifact_type"] == H74_SOURCE_OBSERVATION_AUTHORITY_ARTIFACT_TYPE
    bound = payload["hash_bound_parameters"]
    assert bound["candidate_id"] == "candidate_9738b8d6"
    assert bound["source_candidate_artifact_hash"] == "sha256:source-candidate"
    assert bound["backtest_report_hash"] == "sha256:backtest"
    assert bound["validation_run_hash"] == "sha256:validation"
    assert bound["SMA_FILTER_GAP_MIN_RATIO"] == 0.0002
    assert bound["SMA_FILTER_VOL_MIN_RANGE_RATIO"] == 0.001
    assert bound["SMA_FILTER_OVEREXT_LOOKBACK"] == 5
    assert bound["SMA_FILTER_OVEREXT_MAX_RETURN_RATIO"] == 0.01
    assert bound["ENTRY_EDGE_BUFFER_RATIO"] == 0.0
    assert bound["STRATEGY_EXIT_RULES"] == "max_holding_time"
    assert bound["STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO"] == 0.0008
    assert bound["STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO"] == 0.0005
    assert bound["DAILY_PARTICIPATION_MAX_ORDER_KRW"] == 100_000
    assert bound["DAILY_PARTICIPATION_BUY_FRACTION"] == 1.0
    assert bound["max_entry_notional_krw"] == 100_000
    assert bound["max_daily_entry_count"] == 1
    assert bound["max_daily_total_order_count"] == 2
    assert bound["observation_window_days"] == 7
    assert bound["code_commit_sha"] == "test-commit"
    assert payload["risk_policy_hash"] == h74_source_observation_risk_policy_hash(payload["risk_policy"])
    assert bound["risk_policy_hash"] == payload["risk_policy_hash"]
    assert payload["risk_profile_source"] == "h74_source_live_observation_authority"
    assert payload["risk_enforcement_mode"] == "enforced"
    verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_authority_binds_wsl_h74_behavior_parameters() -> None:
    payload = _source_authority()
    bound = payload["hash_bound_parameters"]
    required = set(runtime_bound_behavior_parameter_names("daily_participation_sma"))
    expected_wsl_h74 = {
        "SMA_SHORT": 10,
        "SMA_LONG": 86,
        "STRATEGY_EXIT_MAX_HOLDING_MIN": 74,
        "DAILY_PARTICIPATION_WINDOW_START_HOUR_KST": 9,
        "DAILY_PARTICIPATION_WINDOW_END_HOUR_KST": 11,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.001,
        "SMA_FILTER_OVEREXT_LOOKBACK": 5,
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.01,
        "ENTRY_EDGE_BUFFER_RATIO": 0.0,
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0.0008,
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0.0005,
    }

    assert required - set(bound) == set()
    for key, expected in expected_wsl_h74.items():
        assert bound[key] == expected


def test_h74_source_observation_verify_rejects_any_behavior_parameter_mismatch() -> None:
    payload = _source_authority()

    for name in runtime_bound_behavior_parameter_names("daily_participation_sma"):
        runtime = dict(H74_SOURCE_OBSERVATION_PARAMETERS)
        current = runtime[name]
        if isinstance(current, bool):
            runtime[name] = not current
        elif isinstance(current, int):
            runtime[name] = current + 1
        elif isinstance(current, float):
            runtime[name] = current + 0.12345
        else:
            runtime[name] = f"{current}_changed"

        with pytest.raises(H74ObservationAuthorityError, match=name):
            verify_h74_source_observation_authority(payload, runtime_values=runtime)


def test_h74_source_observation_runtime_strategy_set_lint_passes_with_generated_authority(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    from bithumb_bot.cli.main import main as cli_main

    authority = _source_authority()
    authority_path = tmp_path / "source-authority.json"
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(authority_path))
    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
            "strategies": [
                {
                    "strategy_name": "daily_participation_sma",
                    "strategy_instance_id": "h74-source-observation",
                    "pair": "KRW-BTC",
                    "interval": "1m",
                    "desired_exposure_krw": 100_000,
                    "parameters": _source_parameters(),
                }
            ],
        }
    )
    cfg = _h74_source_cfg(authority_path, RUNTIME_STRATEGY_SET_JSON=runtime_strategy_set_json)

    assert cli_main(
        ["runtime-strategy-set-lint"],
        context=argparse.Namespace(settings=cfg, printer=print, env_summary=None),
    ) == 0
    assert "runtime_strategy_set_lint_ok" in capsys.readouterr().out


def test_h74_source_observation_runtime_strategy_set_lint_rejects_entry_edge_mismatch(
    tmp_path,
    monkeypatch,
) -> None:
    from bithumb_bot.cli.main import main as cli_main

    authority = _source_authority()
    authority_path = tmp_path / "source-authority.json"
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(authority_path))
    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
            "strategies": [
                {
                    "strategy_name": "daily_participation_sma",
                    "strategy_instance_id": "h74-source-observation",
                    "pair": "KRW-BTC",
                    "interval": "1m",
                    "desired_exposure_krw": 100_000,
                    "parameters": _source_parameters(),
                }
            ],
        }
    )
    cfg = _h74_source_cfg(authority_path, RUNTIME_STRATEGY_SET_JSON=runtime_strategy_set_json)
    object.__setattr__(cfg, "ENTRY_EDGE_BUFFER_RATIO", 0.0005)

    with pytest.raises(Exception) as exc:
        cli_main(
            ["runtime-strategy-set-lint"],
            context=argparse.Namespace(settings=cfg, printer=print, env_summary=None),
        )
    assert exc.type.__name__ == "LiveModeValidationError"
    assert "h74_source_observation_authority_runtime_mismatch:ENTRY_EDGE_BUFFER_RATIO" in str(exc.value)


def test_h74_source_observation_rejects_missing_risk_policy() -> None:
    payload = _source_authority()
    payload.pop("risk_policy")
    _rehash_authority(payload)

    with pytest.raises(H74ObservationAuthorityError, match="risk_policy_missing"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_risk_policy_hash_mismatch() -> None:
    payload = _source_authority()
    payload["risk_policy"]["max_daily_order_count"] = 1
    _rehash_authority(payload)

    with pytest.raises(H74ObservationAuthorityError, match="risk_policy_hash_mismatch"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_disabled_risk_policy() -> None:
    payload = _source_authority()
    payload["risk_policy"]["policy_status"] = "disabled"
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="risk_policy_disabled"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_daily_order_count_above_2() -> None:
    payload = _source_authority()
    payload["risk_policy"]["max_daily_order_count"] = 3
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="daily_order_count_too_high"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_trade_count_above_2() -> None:
    payload = _source_authority()
    payload["risk_policy"]["max_trade_count_per_day"] = 3
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="trade_count_too_high"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_open_positions_not_one() -> None:
    payload = _source_authority()
    payload["risk_policy"]["max_open_positions"] = 2
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="open_positions_invalid"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_unresolved_order_policy_not_block() -> None:
    payload = _source_authority()
    payload["risk_policy"]["unresolved_order_policy"] = "allow"
    _rehash_source_risk_policy(payload)

    with pytest.raises(H74ObservationAuthorityError, match="unresolved_order_policy_invalid"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_50k_authority() -> None:
    with pytest.raises(H74ObservationAuthorityError, match="artifact_type_invalid"):
        verify_h74_source_observation_authority(
            build_h74_observation_authority_payload(),
            runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS,
        )


def test_h74_source_observation_rejects_gap_mismatch_0012_vs_0002() -> None:
    payload = _source_authority()
    runtime = dict(H74_SOURCE_OBSERVATION_PARAMETERS)
    runtime["SMA_FILTER_GAP_MIN_RATIO"] = 0.0012

    with pytest.raises(H74ObservationAuthorityError, match="SMA_FILTER_GAP_MIN_RATIO"):
        verify_h74_source_observation_authority(payload, runtime_values=runtime)


def test_h74_source_observation_rejects_legacy_exit_rules() -> None:
    payload = _source_authority()
    runtime = dict(H74_SOURCE_OBSERVATION_PARAMETERS)
    runtime["STRATEGY_EXIT_RULES"] = "stop_loss,opposite_cross,max_holding_time"

    with pytest.raises(H74ObservationAuthorityError, match="STRATEGY_EXIT_RULES"):
        verify_h74_source_observation_authority(payload, runtime_values=runtime)


def test_h74_source_observation_rejects_expired_authority() -> None:
    payload = build_h74_source_observation_authority_payload(
        source_candidate_artifact_hash="sha256:source-candidate",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        code_commit_sha="test-commit",
    )

    with pytest.raises(H74ObservationAuthorityError, match="expired"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_max_order_above_100000() -> None:
    payload = _source_authority()
    payload["hash_bound_parameters"]["DAILY_PARTICIPATION_MAX_ORDER_KRW"] = 100_001
    _rehash_authority(payload)

    with pytest.raises(H74ObservationAuthorityError, match="above_100000"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_rejects_window_outside_09_11() -> None:
    payload = _source_authority()
    payload["hash_bound_parameters"]["DAILY_PARTICIPATION_WINDOW_START_HOUR_KST"] = 8
    _rehash_authority(payload)

    with pytest.raises(H74ObservationAuthorityError, match="window_start"):
        verify_h74_source_observation_authority(payload, runtime_values=H74_SOURCE_OBSERVATION_PARAMETERS)


def test_h74_source_observation_missing_authority_does_not_replace_approved_profile() -> None:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH="",
    )

    with pytest.raises(LiveModeValidationError, match="approved_profile_required_for_strategy:daily_participation_sma"):
        validate_live_strategy_selection(cfg)


def test_h74_source_observation_allows_live_dry_run_materialization(tmp_path, monkeypatch) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = _h74_source_cfg(path)

    validate_live_strategy_selection(cfg)
    runtime = h74_source_runtime_values_from_settings(cfg)
    assert runtime["max_daily_total_order_count"] == 2
    assert runtime["exit_closeout_not_blocked_by_entry_cap"] is True


def test_h74_source_observation_runtime_strategy_set_selection_passes_without_approved_profile(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
            "strategies": [
                {
                    "strategy_name": "daily_participation_sma",
                    "strategy_instance_id": "h74-source-observation",
                    "pair": "KRW-BTC",
                    "interval": "1m",
                    "desired_exposure_krw": 100_000,
                    "parameters": _source_parameters(),
                }
            ],
        }
    )
    cfg = _h74_source_cfg(path, RUNTIME_STRATEGY_SET_JSON=runtime_strategy_set_json)

    validate_runtime_strategy_set_selection(cfg)


def test_h74_source_observation_selection_rejects_expired_authority(
    tmp_path,
    monkeypatch,
) -> None:
    authority = build_h74_source_observation_authority_payload(
        source_candidate_artifact_hash="sha256:source-candidate",
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        code_commit_sha="test-commit",
    )
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = _h74_source_cfg(path)

    with pytest.raises(LiveModeValidationError, match="h74_source_observation_authority_expired"):
        validate_live_strategy_selection(cfg)


def test_h74_source_observation_selection_rejects_runtime_mismatch(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = _h74_source_cfg(path)
    object.__setattr__(cfg, "SMA_FILTER_GAP_MIN_RATIO", 0.0012)

    with pytest.raises(LiveModeValidationError, match="SMA_FILTER_GAP_MIN_RATIO"):
        validate_live_strategy_selection(cfg)


def test_h74_source_observation_selection_rejects_invalid_risk_policy_authority(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    authority["risk_policy"]["max_daily_order_count"] = 3
    _rehash_source_risk_policy(authority)
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = _h74_source_cfg(path)

    with pytest.raises(LiveModeValidationError, match="daily_order_count_too_high"):
        validate_live_strategy_selection(cfg)


def test_h74_source_observation_live_dry_run_materializes_risk_profile(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_RULES="max_holding_time",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_ORDER_KRW=100_000,
        MAX_DAILY_ORDER_COUNT=2,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(path),
    )
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(cfg, key, value)
    strategy_set = RuntimeStrategySet(
        source="RUNTIME_STRATEGY_SET_JSON",
        strategies=(
            RuntimeStrategySpec(
                "daily_participation_sma",
                pair="KRW-BTC",
                interval="1m",
                parameters=_source_parameters(),
            ),
        ),
    )
    context = ProfileAuthorityContext.for_strategy_set(strategy_set, settings_obj=cfg)

    instance = RuntimeDecisionRequestBuilder(settings_obj=cfg).with_authority_context(
        context
    ).materialize_instance(strategy_set.active_strategies[0])

    assert instance.approved_profile_path is None
    assert instance.approved_profile_hash is None
    assert instance.risk_profile is not None
    assert instance.risk_profile.risk_profile_source == "h74_source_live_observation_authority"
    assert instance.risk_profile.enforcement_mode == "enforced"
    assert instance.risk_profile.policy.policy_status == "enabled"
    assert instance.risk_profile.policy.max_daily_order_count == 2
    assert instance.risk_profile.policy.max_trade_count_per_day == 2
    assert instance.risk_profile.policy.max_open_positions == 1
    assert instance.risk_profile.policy.unresolved_order_policy == "block"
    assert instance.risk_profile.risk_policy_hash == authority["risk_policy_hash"]


def test_h74_source_flat_first_entry_live_observation_reaches_execution_planning(
    tmp_path,
    monkeypatch,
) -> None:
    original_settings = _force_live_strategy_risk_settings(
        db_path=tmp_path / "runtime-state-live.sqlite"
    )
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    conn = None
    try:
        _record_verified_flat_reconcile()
        cfg = _h74_source_cfg(path)
        conn = ensure_db(str(tmp_path / "h74-flat-first-entry.sqlite"))
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        strategy_set = RuntimeStrategySet(
            source="RUNTIME_STRATEGY_SET_JSON",
            strategies=(
                RuntimeStrategySpec(
                    "daily_participation_sma",
                    strategy_instance_id="h74-source-observation",
                    pair="KRW-BTC",
                    interval="1m",
                    desired_exposure_krw=100_000.0,
                    parameters=_source_parameters(),
                ),
            ),
            market_scope=RuntimeMarketScope(mode="single_pair", pair="KRW-BTC", interval="1m"),
        )
        authority_context = ProfileAuthorityContext.for_strategy_set(strategy_set, settings_obj=cfg)
        bundle = _h74_runtime_bundle(
            candle_ts=1_704_046_800_000,
            authority_context=authority_context,
        )
        broker = _BalanceSnapshotBroker()
        planner = ExecutionPlanner(
            settings_obj=cfg,
            readiness_snapshot_builder=lambda _conn: _Readiness(),
            target_state_resolver=lambda *_args, **_kwargs: {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {},
            },
            broker_provider=lambda: broker,
        )

        plan = planner.plan_runtime_strategy_results(conn, bundle, updated_ts=1_704_046_800_000)

        preference = plan.persistence_context["strategy_preferences"][0]
        risk_decision = preference["strategy_risk_decision"]
        risk_evidence = risk_decision["evidence"]
        assert broker.snapshot_calls == 1
        assert risk_decision["status"] == "ALLOW"
        assert risk_decision["status"] != "REQUIRE_RECONCILE"
        assert risk_decision["reason_code"] != "RISK_STATE_MISMATCH"
        assert risk_decision["reason_code"] != "STRATEGY_RISK_STATE_INCOMPLETE"
        assert "missing_required_risk_state" not in risk_evidence
        assert preference["strategy_risk_status"] == "ALLOW"
        assert plan.submit_plan is not None
        assert plan.submit_plan.side == "BUY"
        assert plan.persistence_context["allocation_selected_signal"] == "BUY"
        assert plan.persistence_context["strategy_risk_decision"] == risk_decision
        assert (
            plan.persistence_context["strategy_risk_state_source"]
            == "runtime_db_strategy_instance_ledger"
        )
        assert risk_decision["state_source"] == "runtime_db_strategy_instance_ledger"
        assert isinstance(risk_evidence.get("risk_snapshot_reconstruction"), dict)

        decision_id = record_strategy_decision(
            conn,
            decision_ts=1_704_046_800_000,
            strategy_name="daily_participation_sma",
            signal="BUY",
            reason="unit",
            candle_ts=1_704_046_800_000,
            market_price=100_000_000.0,
            confidence=None,
            context=plan.persistence_context,
        )
        replay = verify_risk_layer_replay(conn, decision_id=decision_id)
        assert replay["strategy_risk_replay_status"] == "pass"
        assert replay["layers"]["strategy"]["source_reconstruction_status"] == "pass"
        assert replay["layers"]["strategy"]["state_source"] == "runtime_db_strategy_instance_ledger"
    finally:
        if conn is not None:
            conn.close()
        _restore_settings(original_settings)


def test_h74_source_authority_position_mode_reaches_execution_payload(
    tmp_path,
    monkeypatch,
) -> None:
    original_settings = _force_live_strategy_risk_settings(
        db_path=tmp_path / "runtime-state-live.sqlite"
    )
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    conn = None
    try:
        _record_verified_flat_reconcile()
        cfg = _h74_source_cfg(path)
        conn = ensure_db(str(tmp_path / "h74-source-authority-payload.sqlite"))
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        strategy_set = RuntimeStrategySet(
            source="RUNTIME_STRATEGY_SET_JSON",
            strategies=(
                RuntimeStrategySpec(
                    "daily_participation_sma",
                    strategy_instance_id="h74-source-observation",
                    pair="KRW-BTC",
                    interval="1m",
                    desired_exposure_krw=100_000.0,
                    parameters=_source_parameters(),
                ),
            ),
            market_scope=RuntimeMarketScope(mode="single_pair", pair="KRW-BTC", interval="1m"),
        )
        bundle = _h74_runtime_bundle(
            candle_ts=1_704_046_800_000,
            authority_context=ProfileAuthorityContext.for_strategy_set(strategy_set, settings_obj=cfg),
        )
        planner = ExecutionPlanner(
            settings_obj=cfg,
            readiness_snapshot_builder=lambda _conn: _Readiness(),
            target_state_resolver=lambda *_args, **_kwargs: {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {},
            },
            broker_provider=lambda: _BalanceSnapshotBroker(),
        )

        plan = planner.plan_runtime_strategy_results(conn, bundle, updated_ts=1_704_046_800_000)

        assert plan.submit_plan is not None
        submit_payload = plan.submit_plan.as_dict()
        assert plan.persistence_context["position_mode"] == "fixed_fill_qty_until_exit"
        assert plan.persistence_context["authority_hash"] == authority["authority_content_hash"]
        assert submit_payload["position_mode"] == "fixed_fill_qty_until_exit"
        assert submit_payload["authority_hash"] == authority["authority_content_hash"]
    finally:
        if conn is not None:
            conn.close()
        _restore_settings(original_settings)


def test_h74_source_live_strategy_risk_fails_closed_when_broker_snapshot_fails(
    tmp_path,
    monkeypatch,
) -> None:
    original_settings = _force_live_strategy_risk_settings(
        db_path=tmp_path / "runtime-state-live.sqlite"
    )
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    conn = None
    try:
        _record_verified_flat_reconcile()
        cfg = _h74_source_cfg(path)
        conn = ensure_db(str(tmp_path / "h74-broker-snapshot-failure.sqlite"))
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        strategy_set = RuntimeStrategySet(
            source="RUNTIME_STRATEGY_SET_JSON",
            strategies=(
                RuntimeStrategySpec(
                    "daily_participation_sma",
                    strategy_instance_id="h74-source-observation",
                    pair="KRW-BTC",
                    interval="1m",
                    desired_exposure_krw=100_000.0,
                    parameters=_source_parameters(),
                ),
            ),
            market_scope=RuntimeMarketScope(mode="single_pair", pair="KRW-BTC", interval="1m"),
        )
        authority_context = ProfileAuthorityContext.for_strategy_set(strategy_set, settings_obj=cfg)
        bundle = _h74_runtime_bundle(
            candle_ts=1_704_046_800_000,
            authority_context=authority_context,
        )
        broker = _BalanceSnapshotBroker(fail=True)
        planner = ExecutionPlanner(
            settings_obj=cfg,
            readiness_snapshot_builder=lambda _conn: _Readiness(),
            target_state_resolver=lambda *_args, **_kwargs: {
                "previous_target_exposure_krw": 0.0,
                "target_policy_metadata": {},
            },
            broker_provider=lambda: broker,
        )

        plan = planner.plan_runtime_strategy_results(conn, bundle, updated_ts=1_704_046_800_000)

        preference = plan.persistence_context["strategy_preferences"][0]
        risk_decision = preference["strategy_risk_decision"]
        assert broker.snapshot_calls == 1
        assert risk_decision["status"] == "REQUIRE_RECONCILE"
        assert risk_decision["reason_code"] == "RISK_STATE_MISMATCH"
        assert preference["strategy_risk_status"] == "REQUIRE_RECONCILE"
        assert plan.submit_plan is None
        assert plan.persistence_context["allocation_primary_block_reason"] == "RISK_STATE_MISMATCH"
    finally:
        if conn is not None:
            conn.close()
        _restore_settings(original_settings)


def test_h74_live_pre_submit_proof_allows_with_verified_broker_snapshot(monkeypatch) -> None:
    from bithumb_bot.risk import DailyLossEvaluation
    from bithumb_bot.risk_contract import RiskPolicy, SubmitPlan
    from bithumb_bot.runtime_risk_engine import RuntimeRiskEngineAdapter

    original_mode = settings.MODE
    object.__setattr__(settings, "MODE", "live")

    def _daily_loss_state(_conn, *, broker=None, **_kwargs) -> DailyLossEvaluation:
        assert broker is not None
        snapshot = broker.get_balance_snapshot()
        asset_qty = float(snapshot.balance.asset_available) + float(snapshot.balance.asset_locked)
        return DailyLossEvaluation(
            blocked=False,
            reason="ok",
            reason_code="OK",
            decision="allow",
            evaluation_ts_ms=1_704_046_800_000,
            day_kst="2024-01-01",
            max_daily_loss_krw=50_000.0,
            start_equity=1_000_000.0,
            current_equity=1_000_000.0,
            loss_today=0.0,
            current_cash_krw=1_000_000.0,
            current_asset_qty=asset_qty,
            mark_price=100_000_000.0,
            mark_price_source="unit",
            details={"current_source": "broker_balance_snapshot"},
        )

    monkeypatch.setattr("bithumb_bot.runtime_risk_engine.evaluate_daily_loss_state", _daily_loss_state)
    monkeypatch.setattr("bithumb_bot.runtime_risk_engine._latest_position_entry_price", lambda _conn: None)
    monkeypatch.setattr("bithumb_bot.runtime_risk_engine._count_orders_today", lambda _conn, _ts: 0)
    monkeypatch.setattr(
        "bithumb_bot.runtime_risk_engine.collect_risky_order_state",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        "bithumb_bot.runtime_risk_engine._record_typed_decision_identity",
        lambda *_args, **_kwargs: None,
    )
    conn = sqlite3.connect(":memory:")
    broker = _BalanceSnapshotBroker(asset_available=0.0, asset_locked=0.0)
    try:
        decision = RuntimeRiskEngineAdapter(conn, policy=RiskPolicy(max_daily_loss_krw=50_000.0)).evaluate_pre_submit(
            plan=SubmitPlan(side="BUY", qty=0.0002, notional_krw=20_000.0, source="target_delta"),
            ts_ms=1_704_046_800_000,
            now_ms=1_704_046_800_000,
            cash=0.0,
            submit_qty=0.0002,
            current_asset_qty=None,
            price=100_000_000.0,
            broker=broker,
            evaluation_origin="live_real_submit_authority_pre_submit",
        )
    finally:
        conn.close()
        object.__setattr__(settings, "MODE", original_mode)

    assert broker.snapshot_calls == 1
    assert decision.status == "ALLOW"
    assert decision.reason_code == "OK"
    assert decision.evidence["current_asset_qty"] == 0.0
    assert decision.evidence["submit_qty"] == 0.0002
    assert decision.evidence["current_asset_qty_source"] == "broker_current_position"


def test_h74_source_observation_other_strategy_still_requires_approved_profile(
    tmp_path,
    monkeypatch,
) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="sma_with_filter",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(path),
    )

    with pytest.raises(LiveModeValidationError, match="approved_profile_required_for_strategy:sma_with_filter"):
        validate_live_strategy_selection(cfg)


def _write_smoke_success(path) -> None:
    path.write_text(
        json.dumps(
            {
                "status": "passed",
                "execution_mode": "live_pipeline_smoke",
                "orders_expected": 10,
                "orders_submitted": 10,
                "manual_intervention_required": False,
                "final": {
                    "broker_qty": 0.0,
                    "portfolio_qty": 0.0,
                    "projected_total_qty": 0.0,
                    "open_order_count": 0,
                    "submit_unknown_count": 0,
                    "recovery_required_count": 0,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_h74_source_real_order_requires_live_pipeline_smoke_evidence(tmp_path, monkeypatch) -> None:
    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_RULES="max_holding_time",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_ORDER_KRW=100_000,
        MAX_DAILY_ORDER_COUNT=2,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(path),
        H74_SOURCE_OBSERVATION_LIVE_PIPELINE_SMOKE_EVIDENCE_PATH="",
    )
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(cfg, key, value)

    with pytest.raises(LiveModeValidationError, match="live_pipeline_smoke_evidence_missing"):
        validate_live_strategy_selection(cfg)


def test_h74_source_real_order_accepts_live_pipeline_smoke_success_evidence(tmp_path, monkeypatch) -> None:
    authority = _source_authority()
    authority_path = tmp_path / "source-authority.json"
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    smoke_path = tmp_path / "smoke-success.json"
    _write_smoke_success(smoke_path)
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(authority_path))
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_LIVE_PIPELINE_SMOKE_EVIDENCE_PATH", str(smoke_path))
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        STRATEGY_NAME="daily_participation_sma",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        STRATEGY_EXIT_RULES="max_holding_time",
        STRATEGY_EXIT_MAX_HOLDING_MIN=74,
        MAX_ORDER_KRW=100_000,
        MAX_DAILY_ORDER_COUNT=2,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        H74_SOURCE_OBSERVATION_AUTHORITY_PATH=str(authority_path),
        H74_SOURCE_OBSERVATION_LIVE_PIPELINE_SMOKE_EVIDENCE_PATH=str(smoke_path),
    )
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(cfg, key, value)

    validate_live_strategy_selection(cfg)


def test_h74_source_observation_policy_does_not_set_approved_profile_ok(
    tmp_path,
    monkeypatch,
) -> None:
    from bithumb_bot.strategy_config import sma_strategy_config_from_settings
    from bithumb_bot.config import settings as live_settings

    authority = _source_authority()
    path = tmp_path / "source-authority.json"
    path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setenv("H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    object.__setattr__(live_settings, "MODE", "live")
    object.__setattr__(live_settings, "LIVE_DRY_RUN", True)
    object.__setattr__(live_settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(live_settings, "STRATEGY_NAME", "daily_participation_sma")
    object.__setattr__(live_settings, "PAIR", "KRW-BTC")
    object.__setattr__(live_settings, "INTERVAL", "1m")
    object.__setattr__(live_settings, "APPROVED_STRATEGY_PROFILE_PATH", "")
    object.__setattr__(live_settings, "STRATEGY_APPROVED_PROFILE_PATH", "")
    object.__setattr__(live_settings, "STRATEGY_CANDIDATE_PROFILE_PATH", "")
    object.__setattr__(live_settings, "H74_SOURCE_OBSERVATION_AUTHORITY_PATH", str(path))
    object.__setattr__(live_settings, "MAX_ORDER_KRW", 100_000)
    object.__setattr__(live_settings, "MAX_DAILY_ORDER_COUNT", 2)
    for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
        if key.isupper():
            object.__setattr__(live_settings, key, value)

    config = sma_strategy_config_from_settings()

    assert config.candidate_regime_policy is not None
    assert config.candidate_regime_policy["h74_observation_authority_verified"] is True
    assert config.candidate_regime_policy["approved_profile_verification_ok"] is False
    assert config.candidate_regime_policy["approved_profile_block_reason"] == "h74_source_observation_authority_used"
    assert config.candidate_regime_policy["approved_profile_contract_scope"] == "h74_source_live_observation_only"
    assert config.candidate_regime_policy["production_approval"] is False
    assert config.candidate_regime_policy["risk_profile_source"] == "h74_source_live_observation_authority"
    assert config.candidate_regime_policy["risk_enforcement_mode"] == "enforced"


def test_h74_source_observation_exit_closeout_not_blocked_by_entry_cap_after_buy() -> None:
    payload = _source_authority()

    assert payload["hash_bound_parameters"]["max_daily_entry_count"] == 1
    assert payload["hash_bound_parameters"]["max_daily_total_order_count"] == 2
    assert payload["hash_bound_parameters"]["exit_closeout_not_blocked_by_entry_cap"] is True
