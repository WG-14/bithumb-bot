from __future__ import annotations

import ast
import argparse
import inspect
import json
import os
import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from bithumb_bot.db_core import ensure_schema
from bithumb_bot.decision_envelope import DecisionEnvelope
from bithumb_bot.config import LiveModeValidationError, settings, validate_runtime_strategy_set_selection
from bithumb_bot.run_loop_execution_planner import ExecutionPlanner
from bithumb_bot.runtime_decision_contract import RuntimeStrategyPolicyHashes
from bithumb_bot.runtime_data_provider import RuntimeDataRequirementResolver, SQLiteRuntimeDataProvider
from bithumb_bot.runtime_adapters.safe_hold import SafeHoldRuntimeDecisionAdapter
from bithumb_bot import runtime_strategy_decision
from bithumb_bot.runtime_strategy_decision import PromotionRuntimeDecisionAdapter, RuntimeDecisionRequest
from bithumb_bot.runtime_strategy_set import (
    ProfileAuthorityContext,
    RuntimeMarketScope,
    RuntimeDecisionGateway,
    RuntimeDecisionRequestBuilder,
    RuntimeStrategyDecisionCollector,
    RuntimeStrategyDecisionResultBundle,
    RuntimeStrategySet,
    RuntimeStrategySetResolver,
    RuntimeStrategySpec,
    normalized_runtime_strategy_set_manifest,
    validate_runtime_strategy_set_market_scope,
    validate_runtime_strategy_set_profile_binding,
)
from bithumb_bot.strategy_plugins.canary_non_sma import CanaryNonSmaRuntimeDecisionAdapter
from bithumb_bot.runtime_adapters.sma_with_filter import SmaWithFilterRuntimeConfig
from bithumb_bot.research.strategy_spec import StrategySpecError, materialize_strategy_parameters, strategy_spec_for_name
from bithumb_bot.strategy_policy_contract import PositionSnapshot, StrategyDecisionV2


class _RuntimeResult:
    def __init__(self, strategy_name: str, candle_ts: int = 1_700_000_180_000) -> None:
        self.decision = StrategyDecisionV2(
            strategy_name=strategy_name,
            raw_signal="HOLD",
            raw_reason="unit",
            entry_signal="HOLD",
            entry_reason="unit",
            exit_signal="HOLD",
            exit_reason="unit",
            final_signal="HOLD",
            final_reason="unit",
            blocked_filters=(),
            entry_blocked=False,
            entry_block_reason=None,
            exit_rule=None,
            exit_evaluations=(),
            protective_exit_overrode_entry=False,
            exit_filter_suppression_prevented=False,
            position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
            execution_intent=None,
            entry_decision=object(),  # type: ignore[arg-type]
            trace={"strategy_name": strategy_name},
            policy_hash="sha256:pure",
            policy_contract_hash="sha256:contract",
            policy_input_hash="sha256:input",
            policy_decision_hash="sha256:decision",
        )
        self.base_context = {"market_price": 10.0, "last_close": 10.0}
        self.candle_ts = candle_ts
        self.market_price = 10.0
        self.replay_fingerprint = {"schema_version": 1, "candle_ts": candle_ts}
        self.boundary = {"decision_boundary_phase": "unit"}
        self.policy_hashes = RuntimeStrategyPolicyHashes(
            {
                "pure_policy_hash": "sha256:pure",
                "policy_contract_hash": "sha256:contract",
                "policy_input_hash": "sha256:input",
                "policy_decision_hash": "sha256:decision",
            }
        )

    def as_legacy_dict(self) -> dict[str, object]:
        return dict(self.base_context)


def _attach_unit_request_metadata(result: _RuntimeResult, request: RuntimeDecisionRequest) -> _RuntimeResult:
    fields = request.observability_fields()
    result.base_context.update(fields)
    result.replay_fingerprint.update(
        {
            "runtime_decision_request_hash": request.request_hash,
            "strategy_instance_id": request.strategy_instance_id,
            "strategy_parameters_hash": request.strategy_parameters_hash,
            "approved_profile_hash": request.approved_profile_hash,
            "runtime_contract_hash": request.runtime_contract_hash,
            "plugin_contract_hash": request.plugin_contract_hash,
            "through_ts_ms": request.through_ts_ms,
        }
    )
    return result


def _adapter_resolver(adapters: dict[str, object]):
    def _resolve(strategy_name: str):
        adapter = adapters.get(str(strategy_name).strip().lower())
        return adapter() if isinstance(adapter, type) else adapter

    return _resolve


def _complete_canary_parameters(**overrides: object) -> dict[str, object]:
    params: dict[str, object] = {
        "CANARY_ORDER_START_INDEX": 0,
        "CANARY_ORDER_SIDE": "BUY",
        "CANARY_ORDER_REASON": "unit_canary",
    }
    params.update(overrides)
    return params


def _canary_spec(**overrides: object) -> RuntimeStrategySpec:
    return RuntimeStrategySpec(
        "canary_non_sma",
        parameters=_complete_canary_parameters(),
        **overrides,
    )


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    for idx in range(4):
        ts = 1_700_000_000_000 + idx * 60_000
        close = 10.0 + idx
        conn.execute(
            """
            INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, "KRW-BTC", "1m", close, close, close, close, 1.0),
        )
    conn.commit()
    return conn


def _decide_adapter_snapshot(conn: sqlite3.Connection, request: RuntimeDecisionRequest, adapter: object):
    spec = RuntimeStrategySpec(
        request.strategy_name,
        pair=request.pair,
        interval=request.interval,
        parameters=dict(request.parameters),
    )
    strategy_set = RuntimeStrategySet(
        source="unit_snapshot_adapter",
        strategies=(spec,),
        market_scope=RuntimeMarketScope(pair=request.pair, interval=request.interval),
    )
    resolver = RuntimeDataRequirementResolver()
    snapshot = SQLiteRuntimeDataProvider(conn, resolver=resolver).snapshot(
        request,
        resolver.resolve_for_strategy_set(strategy_set),
    )
    assert snapshot is not None
    return adapter.decide_feature_snapshot(request, snapshot)


def _complete_sma_parameters(**overrides: object) -> dict[str, object]:
    params: dict[str, object] = {
        "SMA_SHORT": 2,
        "SMA_LONG": 5,
        "SMA_FILTER_GAP_MIN_RATIO": 0.9,
        "SMA_FILTER_VOL_WINDOW": 3,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.8,
        "SMA_FILTER_OVEREXT_LOOKBACK": 4,
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.7,
        "SMA_MARKET_REGIME_ENABLED": False,
        "SMA_COST_EDGE_ENABLED": False,
        "SMA_COST_EDGE_MIN_RATIO": 0.6,
        "ENTRY_EDGE_BUFFER_RATIO": 0.5,
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": 0.4,
        "STRATEGY_ENTRY_SLIPPAGE_BPS": 33,
        "LIVE_FEE_RATE_ESTIMATE": 0.0123,
        "STRATEGY_EXIT_RULES": "opposite_cross",
        "STRATEGY_EXIT_STOP_LOSS_RATIO": 0,
        "STRATEGY_EXIT_MAX_HOLDING_MIN": 11,
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0.22,
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0.11,
    }
    params.update(overrides)
    return params


def test_common_runtime_adapter_protocol_is_request_shaped() -> None:
    params = inspect.signature(PromotionRuntimeDecisionAdapter.decide_feature_snapshot).parameters
    assert "request" in params
    assert "feature_snapshot" in params
    assert "conn" not in params
    assert "short_n" not in params
    assert "long_n" not in params

    for path in (
        Path("src/bithumb_bot/runtime_strategy_decision.py"),
        Path("src/bithumb_bot/runtime_strategy_set.py"),
        Path("src/bithumb_bot/engine.py"),
    ):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in {
                "decide_snapshot",
                "compute_strategy_decision_snapshot",
                "compute_strategy_decision_for_diagnostics",
                "collect",
                "collect_runtime_strategy_decisions",
                "decide_bundle",
                "run_loop",
            }:
                names = {arg.arg for arg in [*node.args.args, *node.args.kwonlyargs]}
                assert "short_n" not in names
                assert "long_n" not in names


def test_production_runtime_modules_have_no_test_only_adapter_registry() -> None:
    forbidden = {
        "_TEST_ONLY_RUNTIME_DECISION_ADAPTERS",
        "_RUNTIME_DECISION_ADAPTERS",
        "register_runtime_decision_adapter",
        "reset_runtime_decision_adapters_for_tests",
    }
    for path in (
        Path("src/bithumb_bot/runtime_strategy_decision.py"),
        Path("src/bithumb_bot/runtime_strategy_set.py"),
        Path("src/bithumb_bot/runtime_decision_service.py"),
        Path("src/bithumb_bot/engine.py"),
    ):
        source = path.read_text(encoding="utf-8-sig")
        assert {token for token in forbidden if token in source} == set()


def test_generic_runtime_and_fingerprint_modules_do_not_embed_sma_diagnostics() -> None:
    forbidden = {
        "diagnostic_sma_windows",
        "SMA_",
        "sma_with_filter",
        "runtime_adapters.sma_with_filter",
        "runtime_sma_snapshot",
        "strategy.sma",
        "short_n",
        "long_n",
        "sma_short",
        "sma_long",
        "settings_compat",
    }
    for path in (
        Path("src/bithumb_bot/config.py"),
        Path("src/bithumb_bot/approved_profile.py"),
        Path("src/bithumb_bot/runtime_strategy_decision.py"),
        Path("src/bithumb_bot/runtime_strategy_set.py"),
        Path("src/bithumb_bot/experiment_fingerprint.py"),
        Path("src/bithumb_bot/decision_contract.py"),
        Path("src/bithumb_bot/runtime_decision_service.py"),
        Path("src/bithumb_bot/runtime/decision_coordinator.py"),
        Path("src/bithumb_bot/runtime/runner.py"),
        Path("src/bithumb_bot/engine.py"),
    ):
        source = path.read_text(encoding="utf-8-sig")
        assert {token for token in forbidden if token in source} == set()
        tree = ast.parse(source)
        literals: set[str] = set()
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                literals.add(node.value)
            elif isinstance(node, ast.Name):
                names.add(node.id)
            elif isinstance(node, ast.arg):
                names.add(node.arg)
        assert {token for token in forbidden if token in literals or token in names} == set()


def test_collector_passes_runtime_decision_request() -> None:
    received: list[RuntimeDecisionRequest] = []

    class _Adapter:
        strategy_name = "canary_non_sma"

        def decide_feature_snapshot(self, request: RuntimeDecisionRequest, feature_snapshot: Any):
            del feature_snapshot
            received.append(request)
            return _RuntimeResult(self.strategy_name)

        def typed_authority_required(self) -> bool:
            return True

    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(_canary_spec(),),
    )
    bundle = RuntimeStrategyDecisionCollector(
        adapter_resolver=_adapter_resolver({"canary_non_sma": _Adapter()}),
    ).collect(
        _conn(),
        strategy_set,
        through_ts_ms=1_700_000_180_000,
    )

    assert bundle is not None
    assert len(received) == 1
    assert isinstance(received[0], RuntimeDecisionRequest)


def test_runtime_decision_entrypoint_accepts_generic_parameter_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: list[RuntimeDecisionRequest] = []

    class _Adapter:
        strategy_name = "canary_non_sma"

        def decide_feature_snapshot(self, request: RuntimeDecisionRequest, feature_snapshot: Any):
            del feature_snapshot
            received.append(request)
            return _RuntimeResult(self.strategy_name)

        def typed_authority_required(self) -> bool:
            return True

    monkeypatch.setattr(
        runtime_strategy_decision,
        "get_runtime_decision_adapter",
        lambda strategy_name: _Adapter()
        if str(strategy_name).strip().lower() == "canary_non_sma"
        else None,
    )

    result = runtime_strategy_decision.compute_strategy_decision_snapshot(
        _conn(),
        strategy_name="canary_non_sma",
        through_ts_ms=1_700_000_180_000,
        parameter_overrides={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "BUY",
            "CANARY_ORDER_REASON": "override_reason",
        },
        parameter_source="runtime_override",
    )

    assert result is not None
    request = received[0]
    assert request.parameter_source == "runtime_strategy_spec"
    assert request.parameters["CANARY_ORDER_REASON"] == "override_reason"
    fields = request.observability_fields()
    assert fields["strategy_parameters_hash"] == request.strategy_parameters_hash
    assert fields["runtime_contract_hash"] == request.runtime_contract_hash
    assert fields["plugin_contract_hash"] == request.plugin_contract_hash
    assert fields["runtime_decision_request_hash"] == request.request_hash


def test_runtime_decision_entrypoint_rejects_positional_diagnostics_for_generic_path() -> None:
    with pytest.raises(TypeError, match="positional_diagnostic_parameters_unsupported"):
        runtime_strategy_decision.compute_strategy_decision_for_diagnostics(
            _conn(),
            2,
            3,
            strategy_name="canary_non_sma",
        )


def test_runtime_decision_entrypoint_rejects_ad_hoc_overrides_in_live_like_mode() -> None:
    old_mode = settings.MODE
    old_profile_path = settings.APPROVED_STRATEGY_PROFILE_PATH
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "/tmp/profile.json")
        with pytest.raises(RuntimeError, match="runtime_parameter_overrides_unapproved:canary_non_sma"):
            runtime_strategy_decision.compute_strategy_decision_snapshot(
                _conn(),
                strategy_name="canary_non_sma",
                parameter_overrides={
                    "CANARY_ORDER_START_INDEX": 0,
                    "CANARY_ORDER_SIDE": "BUY",
                    "CANARY_ORDER_REASON": "unapproved",
                },
            )
    finally:
        object.__setattr__(settings, "MODE", old_mode)
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", old_profile_path)


def test_collector_rejects_dict_returning_adapter() -> None:
    class _Adapter:
        strategy_name = "canary_non_sma"

        def decide_feature_snapshot(self, request: RuntimeDecisionRequest, feature_snapshot: Any):
            del request, feature_snapshot
            return {"signal": "BUY", "reason": "legacy dict"}

        def typed_authority_required(self) -> bool:
            return True

    with pytest.raises(TypeError, match="typed_runtime_decision_required:canary_non_sma"):
        RuntimeStrategyDecisionCollector(
            adapter_resolver=_adapter_resolver({"canary_non_sma": _Adapter()}),
        ).collect(
            _conn(),
            RuntimeStrategySet(source="unit", strategies=(_canary_spec(),)),
            through_ts_ms=1_700_000_180_000,
        )


def test_non_sma_adapters_work_without_sma_parameters() -> None:
    conn = _conn()
    try:
        safe_request = RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec("safe_hold"),
            through_ts_ms=1_700_000_180_000,
        )
        canary_request = RuntimeDecisionRequestBuilder().build_for_spec(
            _canary_spec(),
            through_ts_ms=1_700_000_180_000,
        )
        for request, adapter in (
            (safe_request, SafeHoldRuntimeDecisionAdapter()),
            (canary_request, CanaryNonSmaRuntimeDecisionAdapter()),
        ):
            assert "SMA_SHORT" not in request.parameters
            assert "SMA_LONG" not in request.parameters
            result = _decide_adapter_snapshot(conn, request, adapter)
            assert result is not None
        assert _decide_adapter_snapshot(
            conn,
            safe_request,
            SafeHoldRuntimeDecisionAdapter(),
        ).decision.final_signal == "HOLD"
        assert _decide_adapter_snapshot(
            conn,
            canary_request,
            CanaryNonSmaRuntimeDecisionAdapter(),
        ).decision.final_signal == "BUY"
    finally:
        conn.close()


def test_canary_replay_has_no_dummy_sma_values() -> None:
    source = Path("src/bithumb_bot/strategy_plugins/canary_non_sma.py").read_text(encoding="utf-8")
    assert "short_n=0" not in source
    assert "long_n=0" not in source
    assert "from bithumb_bot.config import settings" not in source
    assert "settings.PAIR" not in source
    assert "settings.INTERVAL" not in source


def test_multi_strategy_parameters_are_independent(monkeypatch: pytest.MonkeyPatch) -> None:
    received: dict[str, RuntimeDecisionRequest] = {}

    class _Adapter:
        def __init__(self, strategy_name: str) -> None:
            self.strategy_name = strategy_name

        def decide_feature_snapshot(self, request: RuntimeDecisionRequest, feature_snapshot: Any):
            del feature_snapshot
            received[self.strategy_name] = request
            return _RuntimeResult(self.strategy_name)

        def typed_authority_required(self) -> bool:
            return True

    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(
            RuntimeStrategySpec(
                "sma_with_filter",
                parameters=_complete_sma_parameters(
                    SMA_SHORT=1,
                    SMA_LONG=2,
                    SMA_FILTER_VOL_WINDOW=1,
                    SMA_FILTER_OVEREXT_LOOKBACK=1,
                ),
            ),
            RuntimeStrategySpec(
                "canary_non_sma",
                parameters={
                    "CANARY_ORDER_START_INDEX": 0,
                    "CANARY_ORDER_SIDE": "BUY",
                    "CANARY_ORDER_REASON": "unit",
                },
            ),
        ),
    )
    RuntimeStrategyDecisionCollector(
        adapter_resolver=_adapter_resolver(
            {
                "canary_non_sma": _Adapter("canary_non_sma"),
                "sma_with_filter": _Adapter("sma_with_filter"),
            }
        ),
    ).collect(_conn(), strategy_set, through_ts_ms=1_700_000_180_000)

    assert set(received) == {"sma_with_filter", "canary_non_sma"}
    assert received["sma_with_filter"].parameters["SMA_SHORT"] == 1
    assert received["sma_with_filter"].parameters["SMA_LONG"] == 2
    assert dict(received["sma_with_filter"].parameters_raw)["SMA_SHORT"] == 1
    assert dict(received["sma_with_filter"].parameters_materialized) == dict(received["sma_with_filter"].parameters)
    assert dict(received["canary_non_sma"].parameters) == {
        "CANARY_ORDER_START_INDEX": 0,
        "CANARY_ORDER_SIDE": "BUY",
        "CANARY_ORDER_REASON": "unit",
    }
    assert received["sma_with_filter"].request_hash != received["canary_non_sma"].request_hash
    assert received["sma_with_filter"].strategy_instance_id != received["canary_non_sma"].strategy_instance_id
    assert received["sma_with_filter"].strategy_parameters_hash != received["canary_non_sma"].strategy_parameters_hash


def test_same_strategy_name_different_instances_do_not_collide() -> None:
    left_spec = RuntimeStrategySpec(
        "canary_non_sma",
        parameters={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "BUY",
            "CANARY_ORDER_REASON": "left",
        },
    )
    right_spec = RuntimeStrategySpec(
        "canary_non_sma",
        parameters={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "BUY",
            "CANARY_ORDER_REASON": "right",
        },
    )
    strategy_set = RuntimeStrategySet(source="unit", strategies=(left_spec, right_spec))
    left_request = RuntimeDecisionRequestBuilder().build_for_spec(
        left_spec,
        through_ts_ms=1_700_000_180_000,
    )
    right_request = RuntimeDecisionRequestBuilder().build_for_spec(
        right_spec,
        through_ts_ms=1_700_000_180_000,
    )

    left = _attach_unit_request_metadata(_RuntimeResult("canary_non_sma"), left_request)
    right = _attach_unit_request_metadata(_RuntimeResult("canary_non_sma"), right_request)

    bundle = RuntimeStrategyDecisionResultBundle(strategy_set=strategy_set, results=(right, left))

    assert left_request.strategy_instance_id != right_request.strategy_instance_id
    assert left_request.strategy_parameters_hash != right_request.strategy_parameters_hash
    assert [item.base_context["strategy_instance_id"] for item in bundle.results] == sorted(
        [left_request.strategy_instance_id, right_request.strategy_instance_id]
    )


def test_runtime_result_bundle_rejects_missing_request_metadata() -> None:
    spec = RuntimeStrategySpec(
        "canary_non_sma",
        parameters={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "BUY",
            "CANARY_ORDER_REASON": "unit",
        },
    )
    result = _RuntimeResult("canary_non_sma")

    with pytest.raises(ValueError, match="runtime_decision_request_metadata_missing"):
        RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(spec,)),
            results=(result,),
        )


@pytest.mark.parametrize(
    "field,value",
    (
        ("strategy_instance_id", "wrong-instance"),
        ("strategy_parameters_hash", "sha256:wrong-parameters"),
        ("approved_profile_hash", "sha256:wrong-profile"),
        ("runtime_contract_hash", "sha256:wrong-runtime-contract"),
        ("plugin_contract_hash", "sha256:wrong-plugin-contract"),
    ),
)
def test_runtime_result_bundle_rejects_mismatched_request_metadata(
    field: str,
    value: object,
) -> None:
    spec = RuntimeStrategySpec(
        "canary_non_sma",
        parameters={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "BUY",
            "CANARY_ORDER_REASON": "unit",
        },
    )
    request = RuntimeDecisionRequestBuilder().build_for_spec(spec, through_ts_ms=1_700_000_180_000)
    result = _attach_unit_request_metadata(_RuntimeResult("canary_non_sma"), request)
    result.base_context[field] = value
    result.replay_fingerprint[field] = value

    with pytest.raises(ValueError, match=f"runtime_decision_request_metadata_mismatch:{field}"):
        RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(spec,)),
            results=(result,),
        )


def test_runtime_result_bundle_rejects_candle_mismatch() -> None:
    spec = RuntimeStrategySpec(
        "canary_non_sma",
        parameters={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "BUY",
            "CANARY_ORDER_REASON": "unit",
        },
    )
    request = RuntimeDecisionRequestBuilder().build_for_spec(spec, through_ts_ms=1_700_000_180_000)
    result = _attach_unit_request_metadata(
        _RuntimeResult("canary_non_sma", candle_ts=1_700_000_240_000),
        request,
    )

    with pytest.raises(ValueError, match="runtime_strategy_candle_mismatch:canary_non_sma"):
        RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(source="unit", strategies=(spec,)),
            results=(result,),
        )


def test_approved_profile_mismatch_fails_before_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    class _Adapter:
        strategy_name = "canary_non_sma"

        def decide_feature_snapshot(self, request: RuntimeDecisionRequest, feature_snapshot: Any):
            del request, feature_snapshot
            nonlocal called
            called = True
            return None

        def typed_authority_required(self) -> bool:
            return True

    from bithumb_bot import runtime_strategy_set
    monkeypatch.setattr(
        runtime_strategy_set,
        "load_approved_profile",
        lambda path: {
            "profile_mode": "paper",
            "profile_content_hash": "sha256:unit",
            "strategy_parameters": {
                "CANARY_ORDER_START_INDEX": 0,
                "CANARY_ORDER_SIDE": "BUY",
                "CANARY_ORDER_REASON": "profile",
            },
        },
    )
    monkeypatch.setattr(
        runtime_strategy_set,
        "diff_profile_to_runtime",
        lambda profile, runtime, profile_path=None: ({"field": "strategy_parameters.CANARY_ORDER_REASON"},),
    )
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(RuntimeStrategySpec("canary_non_sma", approved_profile_path="/tmp/profile.json"),),
    )

    with pytest.raises(RuntimeError, match="approved_profile_runtime_parameter_mismatch"):
        RuntimeStrategyDecisionCollector(
            adapter_resolver=_adapter_resolver({"canary_non_sma": _Adapter()}),
        ).collect(_conn(), strategy_set, through_ts_ms=1_700_000_180_000)

    assert called is False


def test_request_builder_uses_strict_profile_parameters_when_profile_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_params = _complete_sma_parameters(SMA_SHORT=3, SMA_LONG=8)
    settings_params = _complete_sma_parameters(SMA_SHORT=99, SMA_LONG=199)
    profile = {
        "profile_mode": "paper",
        "profile_content_hash": "sha256:profile",
        "strategy_parameters": dict(profile_params),
    }

    from bithumb_bot import runtime_strategy_set

    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: profile)
    monkeypatch.setattr(
        runtime_strategy_set,
        "runtime_contract_from_settings",
        lambda cfg: {
            "mode": "paper",
            "live_dry_run": True,
            "live_real_order_armed": False,
            "profile_selector": "",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "strategy_parameters": dict(settings_params),
        },
    )

    def _diff(profile_payload: dict[str, object], runtime: dict[str, object], profile_path: str | None = None):
        assert profile_payload is profile
        assert runtime["strategy_parameters"]["SMA_SHORT"] == profile_params["SMA_SHORT"]
        assert runtime["strategy_parameters"]["SMA_LONG"] == profile_params["SMA_LONG"]
        assert profile_path == "/tmp/profile.json"
        return ()

    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", _diff)

    request = RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec(
            "sma_with_filter",
            pair="KRW-BTC",
            interval="1m",
            approved_profile_path="/tmp/profile.json",
            approved_profile_hash="sha256:profile",
        ),
        through_ts_ms=1_700_000_180_000,
    )

    assert request.parameters["SMA_SHORT"] == profile_params["SMA_SHORT"]
    assert request.parameters["SMA_LONG"] == profile_params["SMA_LONG"]
    assert dict(request.parameters_raw) == profile_params
    assert dict(request.parameters_materialized) == dict(request.parameters)
    assert request.parameter_source == "approved_profile"
    assert request.approved_profile_hash == "sha256:profile"
    assert request.observability_fields()["parameter_source"] == "approved_profile"


def test_two_approved_profiles_are_spec_bound_not_global_settings_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    left_params = _complete_sma_parameters(SMA_SHORT=3, SMA_LONG=8)
    right_params = _complete_sma_parameters(SMA_SHORT=13, SMA_LONG=34)
    profiles = {
        "/tmp/left_profile.json": {
            "profile_mode": "paper",
            "profile_content_hash": "sha256:left-profile",
            "strategy_parameters": dict(left_params),
        },
        "/tmp/right_profile.json": {
            "profile_mode": "paper",
            "profile_content_hash": "sha256:right-profile",
            "strategy_parameters": dict(right_params),
        },
    }

    from bithumb_bot import runtime_strategy_set

    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: profiles[str(path)])
    monkeypatch.setattr(
        runtime_strategy_set,
        "runtime_contract_from_settings",
        lambda cfg: {
            "mode": "paper",
            "live_dry_run": True,
            "live_real_order_armed": False,
            "profile_selector": "",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "strategy_parameters": _complete_sma_parameters(SMA_SHORT=999, SMA_LONG=1999),
        },
    )

    def _diff(profile_payload: dict[str, object], runtime: dict[str, object], profile_path: str | None = None):
        profile_parameters = profile_payload["strategy_parameters"]
        assert runtime["strategy_parameters"]["SMA_SHORT"] == profile_parameters["SMA_SHORT"]
        assert runtime["strategy_parameters"]["SMA_LONG"] == profile_parameters["SMA_LONG"]
        return ()

    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", _diff)

    left = RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec(
            "sma_with_filter",
            approved_profile_path="/tmp/left_profile.json",
            approved_profile_hash="sha256:left-profile",
        ),
        through_ts_ms=1_700_000_180_000,
    )
    right = RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec(
            "sma_with_filter",
            approved_profile_path="/tmp/right_profile.json",
            approved_profile_hash="sha256:right-profile",
        ),
        through_ts_ms=1_700_000_180_000,
    )

    assert left.parameters["SMA_SHORT"] == 3
    assert right.parameters["SMA_SHORT"] == 13
    assert left.strategy_instance_id != right.strategy_instance_id


def test_request_builder_rejects_explicit_profile_hash_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = {
        "profile_mode": "paper",
        "profile_content_hash": "sha256:actual",
        "strategy_parameters": _complete_sma_parameters(),
    }
    from bithumb_bot import runtime_strategy_set

    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: profile)

    with pytest.raises(RuntimeError, match="approved_profile_hash_mismatch_for_runtime_strategy:sma_with_filter"):
        RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec(
                "sma_with_filter",
                pair="KRW-BTC",
                interval="1m",
                approved_profile_path="/tmp/profile.json",
                approved_profile_hash="sha256:expected",
            ),
            through_ts_ms=1_700_000_180_000,
        )


def test_request_builder_rejects_settings_drift_in_strict_profile_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = {
        "profile_mode": "paper",
        "profile_content_hash": "sha256:profile",
        "strategy_parameters": _complete_sma_parameters(),
    }
    from bithumb_bot import runtime_strategy_set

    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: profile)
    monkeypatch.setattr(
        runtime_strategy_set,
        "runtime_contract_from_settings",
        lambda cfg: {
            "mode": "paper",
            "live_dry_run": True,
            "live_real_order_armed": False,
            "profile_selector": "/tmp/profile.json",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "strategy_parameters": _complete_sma_parameters(SMA_SHORT=99),
        },
    )
    monkeypatch.setattr(
        runtime_strategy_set,
        "diff_profile_to_runtime",
        lambda profile_payload, runtime, profile_path=None: (
            {"field": "strategy_parameters.SMA_SHORT"},
        ),
    )

    with pytest.raises(RuntimeError, match="approved_profile_runtime_parameter_mismatch:sma_with_filter"):
        RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec(
                "sma_with_filter",
                pair="KRW-BTC",
                interval="1m",
                approved_profile_path="/tmp/profile.json",
            ),
            through_ts_ms=1_700_000_180_000,
        )


def test_live_compatible_request_builder_requires_approved_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "APPROVED_STRATEGY_PROFILE_PATH": settings.APPROVED_STRATEGY_PROFILE_PATH,
    }
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_DRY_RUN", True)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")
        monkeypatch.delenv("APPROVED_STRATEGY_PROFILE_PATH", raising=False)
        with pytest.raises(
            RuntimeError,
            match="approved_profile_required_for_live_compatible_runtime_strategy:sma_with_filter",
        ):
            RuntimeDecisionRequestBuilder().build_for_spec(
                RuntimeStrategySpec("sma_with_filter", pair="KRW-BTC", interval="1m"),
                through_ts_ms=1_700_000_180_000,
            )

        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
        with pytest.raises(
            RuntimeError,
            match="approved_profile_required_for_live_compatible_runtime_strategy:sma_with_filter",
        ):
            RuntimeDecisionRequestBuilder().build_for_spec(
                RuntimeStrategySpec("sma_with_filter", pair="KRW-BTC", interval="1m"),
                through_ts_ms=1_700_000_180_000,
            )
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


def test_live_compatible_request_builder_enforces_profile_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
    }
    profile = {
        "profile_mode": "small_live",
        "profile_content_hash": "sha256:profile",
        "strategy_parameters": _complete_sma_parameters(),
    }
    from bithumb_bot import runtime_strategy_set

    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: profile)
    monkeypatch.setattr(
        runtime_strategy_set,
        "runtime_contract_from_settings",
        lambda cfg: {
            "mode": "live",
            "live_dry_run": bool(settings.LIVE_DRY_RUN),
            "live_real_order_armed": bool(settings.LIVE_REAL_ORDER_ARMED),
            "profile_selector": "/tmp/profile.json",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "strategy_parameters": dict(profile["strategy_parameters"]),
        },
    )
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_DRY_RUN", True)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
        with pytest.raises(RuntimeError, match="approved_profile_runtime_parameter_mismatch:sma_with_filter:profile_mode"):
            RuntimeDecisionRequestBuilder().build_for_spec(
                RuntimeStrategySpec(
                    "sma_with_filter",
                    pair="KRW-BTC",
                    interval="1m",
                    approved_profile_path="/tmp/profile.json",
                ),
                through_ts_ms=1_700_000_180_000,
            )

        profile["profile_mode"] = "live_dry_run"
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
        with pytest.raises(RuntimeError, match="approved_profile_runtime_parameter_mismatch:sma_with_filter:profile_mode"):
            RuntimeDecisionRequestBuilder().build_for_spec(
                RuntimeStrategySpec(
                    "sma_with_filter",
                    pair="KRW-BTC",
                    interval="1m",
                    approved_profile_path="/tmp/profile.json",
                ),
                through_ts_ms=1_700_000_180_000,
            )
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


def test_sma_parameters_are_isolated_to_sma_specific_files() -> None:
    for path in (
        Path("src/bithumb_bot/engine.py"),
        Path("src/bithumb_bot/runtime_strategy_set.py"),
    ):
        source = path.read_text(encoding="utf-8-sig")
        assert "SMA_SHORT" not in source
        assert "SMA_LONG" not in source


def test_cli_run_commands_are_strategy_neutral() -> None:
    from bithumb_bot.cli.commands import runtime as runtime_cli

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")
    for spec in runtime_cli.command_specs():
        spec.register_parser(subparsers)
    choices = subparsers.choices

    for command in ("run", "live-dry-run"):
        option_strings = {option for action in choices[command]._actions for option in action.option_strings}
        assert "--short" not in option_strings
        assert "--long" not in option_strings

    for command in ("signal", "explain"):
        option_strings = {option for action in choices[command]._actions for option in action.option_strings}
        assert {"--short", "--long"}.issubset(option_strings)


def test_persisted_decision_context_contains_request_metadata() -> None:
    conn = _conn()
    try:
        request = RuntimeDecisionRequestBuilder().build_for_spec(
            _canary_spec(),
            through_ts_ms=1_700_000_180_000,
        )
        result = _decide_adapter_snapshot(conn, request, CanaryNonSmaRuntimeDecisionAdapter())
        assert result is not None
        from bithumb_bot.runtime_strategy_decision import _attach_runtime_request_metadata

        _attach_runtime_request_metadata(result, request)
        result_bundle = RuntimeStrategyDecisionResultBundle(
            strategy_set=RuntimeStrategySet(
                source="unit",
                strategies=(_canary_spec(),),
            ),
            results=(result,),
        )
        bundle = ExecutionPlanner().plan_runtime_strategy_results(
            conn,
            result_bundle,
            updated_ts=1_700_000_240_000,
        )
        context = bundle.persistence_context
        assert context["strategy_parameters"]
        assert context["strategy_instance_id"] == request.strategy_instance_id
        assert context["strategy_parameters_materialized"]
        assert context["strategy_parameters_hash"]
        assert context["runtime_decision_request_hash"] == request.request_hash
        assert "approved_profile_hash" in context
        assert context["runtime_contract_hash"]
        assert context["through_ts_ms"] == 1_700_000_180_000
        assert context["plugin_contract_hash"]
        assert context["runtime_strategy_set_manifest_hash"]
        assert context["runtime_strategy_result_bundle_hash"]
        assert context["execution_plan_bundle_hash"]
        assert context["runtime_decision_request_hashes"] == [request.request_hash]
        assert context["runtime_strategy_instance_ids"] == [request.strategy_instance_id]
        assert context["runtime_approved_profile_hashes"] == [request.approved_profile_hash]
        assert context["runtime_strategy_parameter_hashes"] == [request.strategy_parameters_hash]
        assert context["runtime_plugin_contract_hashes"] == [request.plugin_contract_hash]
    finally:
        conn.close()


def test_sma_runtime_config_uses_request_parameters_not_global_settings() -> None:
    request = RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec(
            "sma_with_filter",
            parameters=_complete_sma_parameters(),
        ),
        through_ts_ms=1_700_000_180_000,
    )

    original_values = {
        "SMA_SHORT": getattr(settings, "SMA_SHORT", None),
        "SMA_LONG": getattr(settings, "SMA_LONG", None),
        "SMA_FILTER_GAP_MIN_RATIO": getattr(settings, "SMA_FILTER_GAP_MIN_RATIO", None),
        "STRATEGY_EXIT_MAX_HOLDING_MIN": settings.STRATEGY_EXIT_MAX_HOLDING_MIN,
        "LIVE_FEE_RATE_ESTIMATE": settings.LIVE_FEE_RATE_ESTIMATE,
        "STRATEGY_ENTRY_SLIPPAGE_BPS": settings.STRATEGY_ENTRY_SLIPPAGE_BPS,
    }
    try:
        object.__setattr__(settings, "SMA_SHORT", 99)
        object.__setattr__(settings, "SMA_LONG", 199)
        object.__setattr__(settings, "SMA_FILTER_GAP_MIN_RATIO", 0.00001)
        object.__setattr__(settings, "STRATEGY_EXIT_MAX_HOLDING_MIN", 999)
        object.__setattr__(settings, "LIVE_FEE_RATE_ESTIMATE", 0.999)
        object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 999)
        config = SmaWithFilterRuntimeConfig.from_runtime_request(request)
        strategy = config.build_strategy()
    finally:
        for key, value in original_values.items():
            if value is None and hasattr(settings, key):
                object.__delattr__(settings, key)
            else:
                object.__setattr__(settings, key, value)

    assert strategy.short_n == 2
    assert strategy.long_n == 5
    assert strategy.min_gap_ratio == 0.9
    assert strategy.volatility_window == 3
    assert strategy.min_volatility_ratio == 0.8
    assert strategy.cost_edge_enabled is False
    assert strategy.market_regime_enabled is False
    assert strategy.slippage_bps == 33
    assert strategy.live_fee_rate_estimate == 0.0123
    assert strategy.exit_max_holding_min == 11


def test_sma_runtime_config_maps_every_runtime_bound_behavior_parameter() -> None:
    spec = strategy_spec_for_name("sma_with_filter")
    runtime_bound = set(spec.behavior_affecting_parameter_names) - set(spec.research_only_parameter_names)

    assert runtime_bound == set(SmaWithFilterRuntimeConfig.runtime_parameter_names())


def test_sma_runtime_config_missing_behavior_parameter_fails_closed() -> None:
    params = _complete_sma_parameters()
    params.pop("SMA_FILTER_OVEREXT_LOOKBACK")
    with pytest.raises(RuntimeError, match="runtime_strategy_parameters_missing_runtime_bound:sma_with_filter"):
        RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec("sma_with_filter", parameters=params),
            through_ts_ms=1_700_000_180_000,
        )


def test_sma_runtime_replay_strategy_fails_closed_for_incomplete_profile() -> None:
    from bithumb_bot.research.sma_with_filter_plugin import build_runtime_replay_strategy

    profile = {
        "market": "KRW-BTC",
        "interval": "1m",
        "strategy_parameters": _complete_sma_parameters(),
    }
    profile["strategy_parameters"].pop("SMA_FILTER_OVEREXT_LOOKBACK")

    with pytest.raises(RuntimeError, match="sma_runtime_request_behavior_parameter_missing"):
        build_runtime_replay_strategy(profile)


def test_missing_runtime_parameters_use_audited_paper_legacy_settings_compat() -> None:
    request = RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec("canary_non_sma", pair="KRW-BTC", interval="1m"),
        through_ts_ms=1_700_000_180_000,
    )

    assert request.parameter_source == "paper_legacy_compat"
    assert request.runtime_strategy_spec.legacy_compatibility_used is True
    assert request.runtime_strategy_spec.parameter_authority_audit["legacy_fallback"] == (
        "runtime_parameter_adapter.from_settings"
    )


def test_request_builder_uses_strategy_parameters_json_source() -> None:
    cfg = replace(settings, STRATEGY_PARAMETERS_JSON=json.dumps(_complete_canary_parameters()))

    request = RuntimeDecisionRequestBuilder(settings_obj=cfg).build_for_spec(
        RuntimeStrategySpec("canary_non_sma", pair="KRW-BTC", interval="1m"),
        through_ts_ms=1_700_000_180_000,
    )

    assert request.parameter_source == "paper_legacy_compat"
    assert request.runtime_strategy_spec.legacy_compatibility_used is True
    assert dict(request.parameters) == _complete_canary_parameters()


def test_runtime_decision_request_parameter_source_is_normalized() -> None:
    request = RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec(
            "canary_non_sma",
            pair="KRW-BTC",
            interval="1m",
            parameter_source="runtime_override",
            parameters=_complete_canary_parameters(),
        ),
        through_ts_ms=1_700_000_180_000,
    )

    assert request.parameter_source == "runtime_strategy_spec"
    assert request.observability_fields()["parameter_source"] == "runtime_strategy_spec"
    assert request.runtime_strategy_spec.parameter_authority_audit["authority"] == "runtime_strategy_spec"


def test_paper_legacy_compat_is_explicit_when_fallback_is_used() -> None:
    cfg = replace(settings, MODE="paper", STRATEGY_PARAMETERS_JSON=json.dumps(_complete_canary_parameters()))

    request = RuntimeDecisionRequestBuilder(settings_obj=cfg).build_for_spec(
        RuntimeStrategySpec("canary_non_sma", pair="KRW-BTC", interval="1m"),
        through_ts_ms=1_700_000_180_000,
    )

    assert request.parameter_source == "paper_legacy_compat"
    assert request.runtime_strategy_spec.legacy_compatibility_used is True
    assert request.runtime_strategy_spec.parameter_authority_audit["authority"] == "paper_legacy_compat"
    assert request.runtime_strategy_spec.parameter_authority_audit["legacy_fallback"] == "STRATEGY_PARAMETERS_JSON"


def test_runtime_strategy_spec_parameters_are_authoritative_without_settings_fallback() -> None:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        STRATEGY_PARAMETERS_JSON=json.dumps(_complete_canary_parameters(CANARY_ORDER_REASON="settings")),
    )

    request = RuntimeDecisionRequestBuilder(settings_obj=cfg).build_for_spec(
        RuntimeStrategySpec(
            "canary_non_sma",
            pair="KRW-BTC",
            interval="1m",
            parameters=_complete_canary_parameters(CANARY_ORDER_REASON="spec"),
        ),
        through_ts_ms=1_700_000_180_000,
    )

    assert request.parameter_source == "runtime_strategy_spec"
    assert request.parameters["CANARY_ORDER_REASON"] == "spec"


def test_strategy_spec_validates_type_range_enum_and_units() -> None:
    spec = strategy_spec_for_name("canary_non_sma")
    schema = {item.name: item.as_dict() for item in spec.parameter_schema}

    assert schema["CANARY_ORDER_START_INDEX"]["type"] == "int"
    assert schema["CANARY_ORDER_START_INDEX"]["min"] == 0
    assert schema["CANARY_ORDER_START_INDEX"]["unit"] == "candle_index"
    assert schema["CANARY_ORDER_SIDE"]["enum"] == ["BUY", "SELL", "HOLD"]
    materialize_strategy_parameters("canary_non_sma", _complete_canary_parameters())
    with pytest.raises(StrategySpecError, match="CANARY_ORDER_START_INDEX"):
        materialize_strategy_parameters(
            "canary_non_sma",
            _complete_canary_parameters(CANARY_ORDER_START_INDEX=-1),
        )
    with pytest.raises(StrategySpecError, match="CANARY_ORDER_SIDE"):
        materialize_strategy_parameters(
            "canary_non_sma",
            _complete_canary_parameters(CANARY_ORDER_SIDE="WAIT"),
        )


def test_sma_strategy_spec_validates_type_range_enum_and_units() -> None:
    spec = strategy_spec_for_name("sma_with_filter")
    schema = {item.name: item.as_dict() for item in spec.parameter_schema}

    runtime_bound = set(spec.behavior_affecting_parameter_names) - set(spec.research_only_parameter_names)
    assert runtime_bound <= set(schema)
    assert schema["SMA_SHORT"]["type"] == "int"
    assert schema["SMA_SHORT"]["min"] == 1
    assert schema["SMA_SHORT"]["unit"] == "candles"
    assert schema["SMA_MARKET_REGIME_ENABLED"]["type"] == "bool"
    assert schema["STRATEGY_ENTRY_SLIPPAGE_BPS"]["min"] == 0.0
    assert schema["STRATEGY_ENTRY_SLIPPAGE_BPS"]["unit"] == "basis_points"
    assert schema["LIVE_FEE_RATE_ESTIMATE"]["unit"] == "fee_ratio"
    materialize_strategy_parameters("sma_with_filter", _complete_sma_parameters())
    with pytest.raises(StrategySpecError, match="STRATEGY_EXIT_RULES"):
        materialize_strategy_parameters(
            "sma_with_filter",
            _complete_sma_parameters(STRATEGY_EXIT_RULES="opposite_cross,unsupported_exit"),
        )


def test_sma_strategy_spec_rejects_invalid_runtime_bound_parameter_type() -> None:
    with pytest.raises(StrategySpecError, match="SMA_MARKET_REGIME_ENABLED"):
        materialize_strategy_parameters(
            "sma_with_filter",
            _complete_sma_parameters(SMA_MARKET_REGIME_ENABLED="true"),
        )
    with pytest.raises(StrategySpecError, match="SMA_SHORT"):
        materialize_strategy_parameters(
            "sma_with_filter",
            _complete_sma_parameters(SMA_SHORT="2"),
        )


def test_sma_strategy_spec_rejects_invalid_runtime_bound_parameter_range() -> None:
    with pytest.raises(StrategySpecError, match="SMA_SHORT"):
        materialize_strategy_parameters(
            "sma_with_filter",
            _complete_sma_parameters(SMA_SHORT=0),
        )
    with pytest.raises(StrategySpecError, match="LIVE_FEE_RATE_ESTIMATE"):
        materialize_strategy_parameters(
            "sma_with_filter",
            _complete_sma_parameters(LIVE_FEE_RATE_ESTIMATE=-0.0001),
        )


def test_strict_runtime_rejects_global_strategy_parameters_json_fallback() -> None:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_PARAMETERS_JSON=json.dumps(_complete_canary_parameters()),
    )

    with pytest.raises(RuntimeError, match="strict_runtime_rejects_strategy_parameters_json_fallback"):
        RuntimeDecisionRequestBuilder(settings_obj=cfg).build_for_spec(
            RuntimeStrategySpec("canary_non_sma", pair="KRW-BTC", interval="1m"),
            through_ts_ms=1_700_000_180_000,
        )


def test_strict_runtime_rejects_plugin_from_settings_fallback() -> None:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_PARAMETERS_JSON="",
    )

    with pytest.raises(RuntimeError, match="strict_runtime_rejects_plugin_from_settings_fallback"):
        RuntimeDecisionRequestBuilder(settings_obj=cfg).build_for_spec(
            RuntimeStrategySpec("canary_non_sma", pair="KRW-BTC", interval="1m"),
            through_ts_ms=1_700_000_180_000,
        )


def test_live_like_runtime_manifest_rejects_legacy_compatibility_used() -> None:
    cfg = replace(
        settings,
        MODE="paper",
        LIVE_DRY_RUN=True,
        STRATEGY_PARAMETERS_JSON=json.dumps(_complete_canary_parameters()),
    )
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(RuntimeStrategySpec("canary_non_sma", pair="KRW-BTC", interval="1m"),),
    )

    with pytest.raises(RuntimeError, match="runtime_strategy_manifest_legacy_compatibility_rejected"):
        normalized_runtime_strategy_set_manifest(strategy_set=strategy_set, settings_obj=cfg)


def test_new_strategy_plugin_runs_from_runtime_strategy_set_without_config_change(tmp_path: Path) -> None:
    config_source = Path("src/bithumb_bot/config.py").read_text(encoding="utf-8-sig")
    assert "CANARY_ORDER_" not in config_source

    runtime_strategy_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
            "strategies": [
                {
                    "strategy_name": "canary_non_sma",
                    "parameters": _complete_canary_parameters(CANARY_ORDER_REASON="runtime_set_only"),
                }
            ],
        }
    )
    cfg = replace(settings, RUNTIME_STRATEGY_SET_JSON=runtime_strategy_json, STRATEGY_PARAMETERS_JSON="")
    from bithumb_bot.runtime_strategy_set import RuntimeStrategySetResolver

    strategy_set = RuntimeStrategySetResolver(settings_obj=cfg).resolve()
    spec = strategy_set.active_strategies[0]
    request = RuntimeDecisionRequestBuilder(settings_obj=cfg).build_for_spec(
        spec,
        through_ts_ms=1_700_000_180_000,
    )
    conn = _conn()
    try:
        result = _decide_adapter_snapshot(conn, request, CanaryNonSmaRuntimeDecisionAdapter())
    finally:
        conn.close()

    assert request.strategy_name == "canary_non_sma"
    assert request.parameters["CANARY_ORDER_REASON"] == "runtime_set_only"
    assert "SMA_SHORT" not in request.parameters
    assert result is not None
    assert result.decision.strategy_name == "canary_non_sma"


def test_promotion_runtime_paths_do_not_import_legacy_sma_settings_config() -> None:
    for path in (
        Path("src/bithumb_bot/approved_profile.py"),
        Path("src/bithumb_bot/runtime_strategy_set.py"),
        Path("src/bithumb_bot/runtime_strategy_decision.py"),
    ):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        assert "SmaStrategyConfig" not in imported
        assert "sma_strategy_config_from_settings" not in imported


def test_strategy_specific_settings_extension_uses_generic_json_not_new_fields() -> None:
    source = Path("src/bithumb_bot/config.py").read_text(encoding="utf-8")
    assert "CANARY_ORDER_START_INDEX" not in source
    assert "CANARY_ORDER_SIDE" not in source
    assert "CANARY_ORDER_REASON" not in source
    assert "STRATEGY_PARAMETERS_JSON" in source


def test_config_has_no_strategy_specific_parameter_prefix_for_new_strategy() -> None:
    source = Path("src/bithumb_bot/config.py").read_text(encoding="utf-8")

    assert "CANARY_ORDER_START_INDEX" not in source
    assert "CANARY_ORDER_SIDE" not in source
    assert "CANARY_ORDER_REASON" not in source


def test_new_strategy_plugin_materializes_without_settings_attribute() -> None:
    cfg = replace(settings, STRATEGY_PARAMETERS_JSON="")
    request = RuntimeDecisionRequestBuilder(settings_obj=cfg).build_for_spec(
        RuntimeStrategySpec(
            "canary_non_sma",
            parameters=_complete_canary_parameters(CANARY_ORDER_REASON="no_settings_attribute"),
        ),
        through_ts_ms=1_700_000_180_000,
    )

    assert request.parameter_source == "runtime_strategy_spec"
    assert request.parameters["CANARY_ORDER_REASON"] == "no_settings_attribute"
    assert not hasattr(cfg, "CANARY_ORDER_REASON")


def test_promotion_strategy_does_not_require_from_settings_for_strict_runtime() -> None:
    source = Path("src/bithumb_bot/research/strategy_registry.py").read_text(encoding="utf-8")

    assert "strategy promotion runtime capability missing parameter adapter" not in source
    request = RuntimeDecisionRequestBuilder(
        settings_obj=replace(settings, MODE="live", LIVE_DRY_RUN=True, STRATEGY_PARAMETERS_JSON="")
    ).build_for_spec(
        RuntimeStrategySpec("canary_non_sma", parameters=_complete_canary_parameters()),
        through_ts_ms=1_700_000_180_000,
    )
    assert request.parameter_source == "runtime_strategy_spec"


def test_settings_strategy_specific_fields_are_legacy_allowlisted() -> None:
    source = Path("src/bithumb_bot/config.py").read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    settings_class = next(
        node for node in ast.walk(tree) if isinstance(node, ast.ClassDef) and node.name == "Settings"
    )
    field_names = {
        target.id
        for node in settings_class.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
        for target in (node.target,)
    }
    legacy_strategy_specific = {
        "STRATEGY_EXIT_RULES",
        "STRATEGY_EXIT_STOP_LOSS_RATIO",
        "STRATEGY_EXIT_MAX_HOLDING_MIN",
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO",
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO",
    }
    generic_strategy_runtime = {
        "STRATEGY_NAME",
        "ACTIVE_STRATEGIES",
        "RUNTIME_STRATEGY_SET_JSON",
        "STRATEGY_PARAMETERS_JSON",
        "STRATEGY_APPROVED_PROFILE_PATH",
        "APPROVED_STRATEGY_PROFILE_PATH",
        "STRATEGY_CANDIDATE_PROFILE_PATH",
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO",
        "STRATEGY_ENTRY_SLIPPAGE_BPS",
    }
    forbidden_prefixes = (
        "RSI_",
        "BREAKOUT_",
        "MEAN_REVERSION_",
        "CANARY_",
    )
    unexpected_prefixed = sorted(
        name for name in field_names if name.startswith(forbidden_prefixes)
    )
    unexpected_strategy = sorted(
        name
        for name in field_names
        if name.startswith("STRATEGY_")
        and name not in legacy_strategy_specific
        and name not in generic_strategy_runtime
    )
    unexpected_sma = sorted(name for name in field_names if name.startswith("SMA_"))

    assert unexpected_prefixed == []
    assert unexpected_strategy == []
    assert unexpected_sma == []


def test_runtime_strategy_spec_carries_pair_and_interval_into_request() -> None:
    request = RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec(
            "canary_non_sma",
            pair="KRW-ETH",
            interval="5m",
            parameters={
                "CANARY_ORDER_START_INDEX": 0,
                "CANARY_ORDER_SIDE": "BUY",
                "CANARY_ORDER_REASON": "pair_interval_request",
            },
        ),
        through_ts_ms=1_700_000_180_000,
    )

    assert request.pair == "KRW-ETH"
    assert request.interval == "5m"
    assert request.runtime_strategy_spec.as_dict()["interval"] == "5m"


def test_runtime_strategy_set_preflight_rejects_pair_mismatch() -> None:
    cfg = replace(
        settings,
        PAIR="KRW-BTC",
        RUNTIME_STRATEGY_SET_JSON=json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [
                    {
                        "strategy_name": "safe_hold",
                        "pair": "KRW-ETH",
                        "interval": "1m",
                    }
                ]
            }
        ),
    )

    with pytest.raises(Exception) as exc:
        validate_runtime_strategy_set_selection(cfg)
    assert exc.type.__name__ == "LiveModeValidationError"
    assert "runtime_strategy_pair_mismatch" in str(exc.value)


def test_runtime_strategy_set_requires_market_scope_for_structured_contract() -> None:
    cfg = replace(
        settings,
        RUNTIME_STRATEGY_SET_JSON=json.dumps({"strategies": [{"strategy_name": "safe_hold"}]}),
    )

    with pytest.raises(ValueError, match="runtime_strategy_set_market_scope_required"):
        RuntimeStrategySetResolver(settings_obj=cfg).resolve()


def test_runtime_strategy_set_rejects_multi_pair_until_pair_scoped_runtime_exists() -> None:
    cfg = replace(
        settings,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        RUNTIME_STRATEGY_SET_JSON=json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [
                    {"strategy_name": "safe_hold", "strategy_instance_id": "btc", "pair": "KRW-BTC"},
                    {"strategy_name": "safe_hold", "strategy_instance_id": "eth", "pair": "KRW-ETH"},
                ],
            }
        ),
    )

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)
    assert "multi_pair_runtime_unsupported" in str(exc.value)


def test_same_pair_same_interval_multi_strategy_runtime_scope_is_accepted() -> None:
    cfg = replace(settings, PAIR="KRW-BTC", INTERVAL="1m")
    strategy_set = RuntimeStrategySet(
        source="unit",
        market_scope=RuntimeMarketScope(mode="single_pair", pair="KRW-BTC", interval="1m"),
        strategies=(
            RuntimeStrategySpec("safe_hold", strategy_instance_id="hold", pair="KRW-BTC", interval="1m"),
            RuntimeStrategySpec(
                "canary_non_sma",
                strategy_instance_id="canary",
                pair="KRW-BTC",
                interval="1m",
            ),
        ),
    )

    assert validate_runtime_strategy_set_market_scope(strategy_set, cfg) == ()
    validate_runtime_strategy_set_selection(
        replace(
            cfg,
            RUNTIME_STRATEGY_SET_JSON=json.dumps(
                {
                    "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                    "strategies": [
                        {
                            "strategy_name": "safe_hold",
                            "strategy_instance_id": "hold",
                            "pair": "KRW-BTC",
                            "interval": "1m",
                        },
                        {
                            "strategy_name": "canary_non_sma",
                            "strategy_instance_id": "canary",
                            "pair": "KRW-BTC",
                            "interval": "1m",
                        },
                    ],
                }
            ),
        )
    )
    payload = strategy_set.as_dict()
    assert payload["supported_runtime_scope"] == "multi_strategy_single_pair_single_interval"
    assert payload["single_pair_runtime_enforced"] is True
    assert payload["single_interval_runtime_enforced"] is True
    assert payload["multi_pair_portfolio_supported"] is False
    assert payload["multiple_execution_targets_supported"] is False


def test_explicit_multi_pair_portfolio_scope_fails_closed_until_supported() -> None:
    cfg = replace(
        settings,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        RUNTIME_STRATEGY_SET_JSON=json.dumps(
            {
                "market_scope": {"mode": "multi_pair_portfolio", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [{"strategy_name": "safe_hold", "pair": "KRW-BTC"}],
            }
        ),
    )

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)
    assert "multi_pair_runtime_unsupported:market_scope_mode=multi_pair_portfolio" in str(exc.value)


def test_live_like_rejects_list_form_runtime_strategy_set_json() -> None:
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        RUNTIME_STRATEGY_SET_JSON=json.dumps([{"strategy_name": "safe_hold"}]),
    )

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)
    assert "runtime_strategy_set_json_object_required_for_live_like" in str(exc.value)


def test_market_scope_pair_must_match_settings_pair() -> None:
    cfg = replace(
        settings,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        RUNTIME_STRATEGY_SET_JSON=json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-ETH", "interval": "1m"},
                "strategies": [{"strategy_name": "safe_hold", "pair": "KRW-ETH"}],
            }
        ),
    )

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)
    assert "multi_pair_runtime_unsupported" in str(exc.value)
    assert "market_scope_pair=KRW-ETH" in str(exc.value)


def test_strategy_pair_mismatch_reports_multi_pair_runtime_unsupported() -> None:
    cfg = replace(
        settings,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        RUNTIME_STRATEGY_SET_JSON=json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [{"strategy_name": "safe_hold", "pair": "KRW-ETH"}],
            }
        ),
    )

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)
    assert "runtime_strategy_pair_mismatch:multi_pair_runtime_unsupported" in str(exc.value)


def test_strategy_interval_mismatch_reports_single_interval_runtime_unsupported() -> None:
    cfg = replace(
        settings,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        RUNTIME_STRATEGY_SET_JSON=json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [{"strategy_name": "safe_hold", "pair": "KRW-BTC", "interval": "3m"}],
            }
        ),
    )

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)
    assert "runtime_strategy_interval_mismatch:single_interval_runtime_unsupported" in str(exc.value)


def test_market_scope_interval_mismatch_reports_single_interval_runtime_unsupported() -> None:
    cfg = replace(
        settings,
        PAIR="KRW-BTC",
        INTERVAL="1m",
        RUNTIME_STRATEGY_SET_JSON=json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "3m"},
                "strategies": [{"strategy_name": "safe_hold", "pair": "KRW-BTC", "interval": "3m"}],
            }
        ),
    )

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)
    assert "runtime_strategy_interval_mismatch:single_interval_runtime_unsupported" in str(exc.value)
    assert "market_scope_interval=3m" in str(exc.value)


def test_single_pair_runtime_manifest_declares_market_scope() -> None:
    strategy_set = RuntimeStrategySet(
        source="unit",
        market_scope=None,
        strategies=(RuntimeStrategySpec("safe_hold", pair="KRW-BTC", interval="1m"),),
    )
    manifest = normalized_runtime_strategy_set_manifest(strategy_set=strategy_set)

    assert manifest["single_pair_runtime_enforced"] is True
    assert manifest["single_interval_runtime_enforced"] is True
    assert manifest["supported_runtime_scope"] == "multi_strategy_single_pair_single_interval"
    assert manifest["market_scope"]["mode"] == "single_pair"
    assert manifest["market_scope"]["pair"] == "KRW-BTC"
    assert manifest["market_scope"]["single_interval_runtime_enforced"] is True


def test_single_strategy_allows_global_profile_selector_when_hash_matches() -> None:
    cfg = replace(
        settings,
        MODE="live",
        APPROVED_STRATEGY_PROFILE_PATH="/runtime/profile.json",
        STRATEGY_APPROVED_PROFILE_PATH="",
    )
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(RuntimeStrategySpec("safe_hold", strategy_instance_id="only"),),
    )

    assert validate_runtime_strategy_set_profile_binding(strategy_set, cfg) == ()


def test_multi_strategy_profile_hash_mapping_is_manifested(monkeypatch: pytest.MonkeyPatch) -> None:
    from bithumb_bot import runtime_strategy_set

    profiles = {
        "/runtime/left.json": {
            "profile_mode": "paper",
            "profile_content_hash": "sha256:left",
            "strategy_parameters": {},
        },
        "/runtime/right.json": {
            "profile_mode": "paper",
            "profile_content_hash": "sha256:right",
            "strategy_parameters": {},
        },
    }
    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: profiles[str(path)])
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(
            RuntimeStrategySpec(
                "safe_hold",
                strategy_instance_id="left",
                approved_profile_path="/runtime/left.json",
                approved_profile_hash="sha256:left",
            ),
            RuntimeStrategySpec(
                "safe_hold",
                strategy_instance_id="right",
                approved_profile_path="/runtime/right.json",
                approved_profile_hash="sha256:right",
            ),
        ),
    )
    manifest = normalized_runtime_strategy_set_manifest(strategy_set=strategy_set)

    bindings = {
        item["strategy_instance_id"]: item
        for item in manifest["strategy_instance_profile_bindings"]
    }
    assert bindings["left"]["approved_profile_path"] == "/runtime/left.json"
    assert bindings["left"]["approved_profile_hash"] == "sha256:left"
    assert bindings["right"]["approved_profile_path"] == "/runtime/right.json"
    assert bindings["right"]["approved_profile_hash"] == "sha256:right"


def test_profile_selection_authority_is_audited_per_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    from bithumb_bot import runtime_strategy_set

    monkeypatch.setattr(
        runtime_strategy_set,
        "load_approved_profile",
        lambda path: {
            "profile_mode": "paper",
            "profile_content_hash": "sha256:audited",
            "strategy_parameters": _complete_canary_parameters(),
        },
    )
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())
    spec = RuntimeStrategySpec(
        "canary_non_sma",
        strategy_instance_id="audited",
        approved_profile_path="/runtime/audited.json",
        approved_profile_hash="sha256:audited",
    )
    instance = RuntimeDecisionRequestBuilder().materialize_instance(spec)

    assert instance.parameter_authority_audit["authority"] == "approved_profile"
    assert instance.approved_profile_path == "/runtime/audited.json"
    assert instance.approved_profile_hash == "sha256:audited"


def _live_single_strategy_cfg(
    *,
    profile_path: str = "",
    live_dry_run: bool = True,
    live_real_order_armed: bool = False,
    runtime_strategy_set_json: str = "",
) -> object:
    return replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=live_dry_run,
        LIVE_REAL_ORDER_ARMED=live_real_order_armed,
        STRATEGY_NAME="sma_with_filter",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        APPROVED_STRATEGY_PROFILE_PATH=profile_path,
        STRATEGY_APPROVED_PROFILE_PATH="",
        RUNTIME_STRATEGY_SET_JSON=runtime_strategy_set_json,
        ACTIVE_STRATEGIES="",
        STRATEGY_PARAMETERS_JSON="",
    )


@pytest.fixture
def _clear_runtime_strategy_source_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUNTIME_STRATEGY_SET_JSON", raising=False)
    monkeypatch.delenv("ACTIVE_STRATEGIES", raising=False)
    monkeypatch.delenv("STRATEGY_PARAMETERS_JSON", raising=False)
    monkeypatch.delenv("APPROVED_STRATEGY_PROFILE_PATH", raising=False)
    monkeypatch.delenv("STRATEGY_APPROVED_PROFILE_PATH", raising=False)


@pytest.mark.parametrize(
    ("live_dry_run", "live_real_order_armed"),
    ((True, False), (False, True)),
)
def test_live_like_single_strategy_name_preflight_materializes_missing_profile(
    _clear_runtime_strategy_source_env: None,
    live_dry_run: bool,
    live_real_order_armed: bool,
) -> None:
    cfg = _live_single_strategy_cfg(
        live_dry_run=live_dry_run,
        live_real_order_armed=live_real_order_armed,
    )

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)

    msg = str(exc.value)
    assert "source=STRATEGY_NAME" in msg
    assert "approved_profile_required_for_live_compatible_runtime_strategy:sma_with_filter" in msg


def test_live_like_single_strategy_name_preflight_surfaces_profile_hash_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    def _materialize(self: object, spec: RuntimeStrategySpec) -> None:
        raise RuntimeError(f"approved_profile_hash_mismatch_for_runtime_strategy:{spec.strategy_name}")

    monkeypatch.setattr(
        runtime_strategy_set.RuntimeDecisionRequestBuilder,
        "materialize_instance",
        _materialize,
    )

    cfg = _live_single_strategy_cfg(profile_path="/tmp/profile.json")

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)

    msg = str(exc.value)
    assert "source=STRATEGY_NAME" in msg
    assert "runtime_strategy_materialization_failed" in msg
    assert "approved_profile_hash_mismatch_for_runtime_strategy:sma_with_filter" in msg


def test_live_like_single_strategy_name_preflight_rejects_profile_runtime_parameter_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    profile = {
        "profile_mode": "live_dry_run",
        "profile_content_hash": "sha256:profile",
        "strategy_parameters": _complete_sma_parameters(),
    }
    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: profile)
    monkeypatch.setattr(
        runtime_strategy_set,
        "runtime_contract_from_settings",
        lambda cfg: {
            "mode": "live",
            "live_dry_run": True,
            "live_real_order_armed": False,
            "profile_selector": "/tmp/profile.json",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "strategy_parameters": _complete_sma_parameters(SMA_SHORT=99),
        },
    )
    monkeypatch.setattr(
        runtime_strategy_set,
        "diff_profile_to_runtime",
        lambda profile_payload, runtime, profile_path=None: (
            {"field": "strategy_parameters.SMA_SHORT"},
        ),
    )

    cfg = _live_single_strategy_cfg(profile_path="/tmp/profile.json")

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)

    msg = str(exc.value)
    assert "source=STRATEGY_NAME" in msg
    assert "runtime_strategy_materialization_failed" in msg
    assert "approved_profile_runtime_parameter_mismatch:sma_with_filter" in msg


def test_live_like_single_strategy_name_preflight_rejects_missing_runtime_bound_parameters(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    params = _complete_sma_parameters()
    params.pop("SMA_FILTER_OVEREXT_LOOKBACK")
    profile = {
        "profile_mode": "live_dry_run",
        "profile_content_hash": "sha256:profile",
        "strategy_parameters": params,
    }
    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: profile)

    cfg = _live_single_strategy_cfg(profile_path="/tmp/profile.json")

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)

    msg = str(exc.value)
    assert "source=STRATEGY_NAME" in msg
    assert "runtime_strategy_parameters_missing_runtime_bound:sma_with_filter" in msg


def test_live_like_single_strategy_name_preflight_accepts_valid_approved_profile(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    profile_params = _complete_sma_parameters()
    profile = {
        "profile_mode": "live_dry_run",
        "profile_content_hash": "sha256:profile",
        "strategy_parameters": profile_params,
    }
    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: profile)
    monkeypatch.setattr(
        runtime_strategy_set,
        "runtime_contract_from_settings",
        lambda cfg: {
            "mode": "live",
            "live_dry_run": True,
            "live_real_order_armed": False,
            "profile_selector": "",
            "strategy_name": "sma_with_filter",
            "market": "KRW-BTC",
            "interval": "1m",
            "strategy_parameters": dict(profile_params),
        },
    )
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())

    validate_runtime_strategy_set_selection(_live_single_strategy_cfg(profile_path="/tmp/profile.json"))


def test_live_multi_strategy_request_builder_disables_global_profile_fallback(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    loaded: list[str] = []
    monkeypatch.setattr(
        runtime_strategy_set,
        "load_approved_profile",
        lambda path: loaded.append(str(path)) or {
            "profile_mode": "live_dry_run",
            "profile_content_hash": "sha256:global",
            "strategy_parameters": _complete_sma_parameters(),
        },
    )
    cfg = _live_single_strategy_cfg(profile_path="/tmp/global-profile.json")
    spec = RuntimeStrategySpec(
        "sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        approved_profile_hash="sha256:declared",
    )

    with pytest.raises(
        RuntimeError,
        match="spec_bound_approved_profile_path_missing_for_runtime_strategy:sma_with_filter",
    ):
        RuntimeDecisionRequestBuilder(
            settings_obj=cfg,
            require_spec_bound_approved_profile=True,
        ).materialize_instance(spec)

    assert loaded == []


def test_live_multi_strategy_request_builder_requires_spec_hash_even_with_global_profile(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    loaded: list[str] = []
    monkeypatch.setattr(
        runtime_strategy_set,
        "load_approved_profile",
        lambda path: loaded.append(str(path)) or {
            "profile_mode": "live_dry_run",
            "profile_content_hash": "sha256:profile",
            "strategy_parameters": _complete_sma_parameters(),
        },
    )
    cfg = _live_single_strategy_cfg(profile_path="/tmp/global-profile.json")

    with pytest.raises(
        RuntimeError,
        match="spec_bound_approved_profile_hash_missing_for_runtime_strategy:sma_with_filter",
    ):
        RuntimeDecisionRequestBuilder(
            settings_obj=cfg,
            require_spec_bound_approved_profile=True,
        ).materialize_instance(
            RuntimeStrategySpec(
                "sma_with_filter",
                pair="KRW-BTC",
                interval="1m",
                approved_profile_path="/tmp/spec-profile.json",
            )
        )

    assert loaded == []


def test_profile_authority_context_is_typed_and_drives_builder_policy(
    _clear_runtime_strategy_source_env: None,
) -> None:
    strategy_set = RuntimeStrategySet(
        source="RUNTIME_STRATEGY_SET_JSON",
        strategies=(
            RuntimeStrategySpec("canary_non_sma", strategy_instance_id="left"),
            RuntimeStrategySpec("canary_non_sma", strategy_instance_id="right"),
        ),
    )
    cfg = replace(settings, MODE="live", LIVE_DRY_RUN=True, LIVE_REAL_ORDER_ARMED=False)
    context = ProfileAuthorityContext.for_strategy_set(strategy_set, settings_obj=cfg)

    assert context.selection_kind == "multi_strategy"
    assert context.runtime_strategy_set_source == "RUNTIME_STRATEGY_SET_JSON"
    assert context.require_spec_bound_profile is True
    assert context.allow_global_profile_fallback is False

    with pytest.raises(
        RuntimeError,
        match="spec_bound_approved_profile_path_missing_for_runtime_strategy:canary_non_sma",
    ):
        RuntimeDecisionRequestBuilder(settings_obj=cfg).with_authority_context(context).build_for_spec(
            RuntimeStrategySpec("canary_non_sma", strategy_instance_id="left"),
            through_ts_ms=1_700_000_180_000,
        )


def test_live_multi_strategy_collector_materializes_spec_bound_requests_without_global_profile(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    profiles = {
        "/tmp/live-left.json": {
            "profile_mode": "live_dry_run",
            "profile_content_hash": "sha256:left-live",
            "strategy_parameters": _complete_canary_parameters(CANARY_ORDER_REASON="left"),
        },
        "/tmp/live-right.json": {
            "profile_mode": "live_dry_run",
            "profile_content_hash": "sha256:right-live",
            "strategy_parameters": _complete_canary_parameters(CANARY_ORDER_REASON="right"),
        },
    }
    loaded: list[str] = []
    received: list[RuntimeDecisionRequest] = []
    monkeypatch.setattr(
        runtime_strategy_set,
        "load_approved_profile",
        lambda path: loaded.append(str(path)) or profiles[str(path)],
    )
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())

    class _Adapter(CanaryNonSmaRuntimeDecisionAdapter):
        def decide_feature_snapshot(self, request: RuntimeDecisionRequest, feature_snapshot: Any):
            received.append(request)
            return super().decide_feature_snapshot(request, feature_snapshot)

    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="unsupported_legacy_global_name",
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        STRATEGY_PARAMETERS_JSON="",
    )
    strategy_set = RuntimeStrategySet(
        source="RUNTIME_STRATEGY_SET_JSON",
        strategies=(
            RuntimeStrategySpec(
                "canary_non_sma",
                strategy_instance_id="left",
                approved_profile_path="/tmp/live-left.json",
                approved_profile_hash="sha256:left-live",
            ),
            RuntimeStrategySpec(
                "canary_non_sma",
                strategy_instance_id="right",
                approved_profile_path="/tmp/live-right.json",
                approved_profile_hash="sha256:right-live",
            ),
        ),
    )

    bundle = RuntimeStrategyDecisionCollector(
        request_builder=RuntimeDecisionRequestBuilder(settings_obj=cfg),
        adapter_resolver=_adapter_resolver({"canary_non_sma": _Adapter()}),
    ).collect(_conn(), strategy_set, through_ts_ms=1_700_000_180_000)

    assert bundle is not None
    assert set(loaded) == {"/tmp/live-left.json", "/tmp/live-right.json"}
    assert loaded.count("/tmp/live-left.json") >= 1
    assert loaded.count("/tmp/live-right.json") >= 1
    assert {request.approved_profile_path for request in received} == {
        "/tmp/live-left.json",
        "/tmp/live-right.json",
    }
    assert {request.approved_profile_hash for request in received} == {
        "sha256:left-live",
        "sha256:right-live",
    }
    for request in received:
        fields = request.observability_fields()
        assert fields["runtime_selection_kind"] == "multi_strategy"
        assert fields["runtime_strategy_set_source"] == "RUNTIME_STRATEGY_SET_JSON"
        assert fields["profile_binding_kind"] == "spec_bound_approved_profiles"
        assert fields["allow_global_profile_fallback"] is False


def test_live_multi_strategy_collector_rejects_global_profile_substitution(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    loaded: list[str] = []
    monkeypatch.setattr(
        runtime_strategy_set,
        "load_approved_profile",
        lambda path: loaded.append(str(path)) or {
            "profile_mode": "live_dry_run",
            "profile_content_hash": "sha256:global",
            "strategy_parameters": _complete_canary_parameters(),
        },
    )
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="unsupported_legacy_global_name",
        APPROVED_STRATEGY_PROFILE_PATH="/tmp/global.json",
        STRATEGY_APPROVED_PROFILE_PATH="",
        STRATEGY_PARAMETERS_JSON="",
    )
    strategy_set = RuntimeStrategySet(
        source="RUNTIME_STRATEGY_SET_JSON",
        strategies=(
            RuntimeStrategySpec(
                "canary_non_sma",
                strategy_instance_id="left",
                approved_profile_hash="sha256:left",
            ),
            RuntimeStrategySpec(
                "canary_non_sma",
                strategy_instance_id="right",
                approved_profile_path="/tmp/right.json",
                approved_profile_hash="sha256:right",
            ),
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="spec_bound_approved_profile_path_missing_for_runtime_strategy:canary_non_sma",
    ):
        RuntimeStrategyDecisionCollector(
            request_builder=RuntimeDecisionRequestBuilder(settings_obj=cfg),
            adapter_resolver=_adapter_resolver({"canary_non_sma": CanaryNonSmaRuntimeDecisionAdapter()}),
        ).collect(_conn(), strategy_set, through_ts_ms=1_700_000_180_000)

    assert loaded == []


def test_runtime_decision_gateway_resolves_live_multi_strategy_authority_by_default(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    monkeypatch.setattr(
        runtime_strategy_set,
        "load_approved_profile",
        lambda path: {
            "profile_mode": "live_dry_run",
            "profile_content_hash": "sha256:gateway",
            "strategy_parameters": _complete_canary_parameters(CANARY_ORDER_REASON=str(path)),
        },
    )
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())
    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
            "strategies": [
                {
                    "strategy_name": "canary_non_sma",
                    "strategy_instance_id": "left",
                    "approved_profile_path": "/tmp/gateway-left.json",
                    "approved_profile_hash": "sha256:gateway",
                },
                {
                    "strategy_name": "canary_non_sma",
                    "strategy_instance_id": "right",
                    "approved_profile_path": "/tmp/gateway-right.json",
                    "approved_profile_hash": "sha256:gateway",
                },
            ],
        }
    )
    cfg = replace(
        settings,
        MODE="live",
        PAIR="KRW-BTC",
        INTERVAL="1m",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_NAME="unsupported_legacy_global_name",
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        STRATEGY_PARAMETERS_JSON="",
        RUNTIME_STRATEGY_SET_JSON=runtime_strategy_set_json,
        ACTIVE_STRATEGIES="",
    )
    received: list[RuntimeDecisionRequest] = []

    class _Adapter(CanaryNonSmaRuntimeDecisionAdapter):
        def decide_feature_snapshot(self, request: RuntimeDecisionRequest, feature_snapshot: Any):
            received.append(request)
            return super().decide_feature_snapshot(request, feature_snapshot)

    bundle = RuntimeDecisionGateway(
        resolver=RuntimeStrategySetResolver(settings_obj=cfg),
        collector=RuntimeStrategyDecisionCollector(
            adapter_resolver=_adapter_resolver({"canary_non_sma": _Adapter()}),
        ),
    ).decide_bundle(_conn(), through_ts_ms=1_700_000_180_000)

    assert bundle is not None
    assert len(received) == 2
    assert all(request.observability_fields()["runtime_selection_kind"] == "multi_strategy" for request in received)


def test_live_real_order_multi_strategy_builder_uses_small_live_spec_profiles_without_global(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    from bithumb_bot import runtime_strategy_set

    monkeypatch.setattr(
        runtime_strategy_set,
        "load_approved_profile",
        lambda path: {
            "profile_mode": "small_live",
            "profile_content_hash": "sha256:small-live",
            "strategy_parameters": _complete_canary_parameters(),
        },
    )
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *args, **kwargs: ())
    cfg = replace(
        settings,
        MODE="live",
        LIVE_DRY_RUN=False,
        LIVE_REAL_ORDER_ARMED=True,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
        STRATEGY_PARAMETERS_JSON="",
    )
    strategy_set = RuntimeStrategySet(
        source="RUNTIME_STRATEGY_SET_JSON",
        strategies=(
            RuntimeStrategySpec(
                "canary_non_sma",
                strategy_instance_id="left",
                approved_profile_path="/tmp/small-live-left.json",
                approved_profile_hash="sha256:small-live",
            ),
            RuntimeStrategySpec(
                "canary_non_sma",
                strategy_instance_id="right",
                approved_profile_path="/tmp/small-live-right.json",
                approved_profile_hash="sha256:small-live",
            ),
        ),
    )
    context = ProfileAuthorityContext.for_strategy_set(strategy_set, settings_obj=cfg)

    request = RuntimeDecisionRequestBuilder(settings_obj=cfg).with_authority_context(context).build_for_spec(
        strategy_set.active_strategies[0],
        through_ts_ms=1_700_000_180_000,
    )

    assert request.approved_profile_path == "/tmp/small-live-left.json"
    assert request.approved_profile_hash == "sha256:small-live"
    assert request.observability_fields()["allow_global_profile_fallback"] is False


def test_decision_runner_rejects_live_multi_strategy_without_runtime_gateway(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
) -> None:
    runtime_strategy_set_json = json.dumps(
        {
            "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
            "strategies": [
                {
                    "strategy_name": "canary_non_sma",
                    "strategy_instance_id": "left",
                    "approved_profile_path": "/tmp/left.json",
                    "approved_profile_hash": "sha256:left",
                },
                {
                    "strategy_name": "canary_non_sma",
                    "strategy_instance_id": "right",
                    "approved_profile_path": "/tmp/right.json",
                    "approved_profile_hash": "sha256:right",
                },
            ],
        }
    )
    original = {
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "STRATEGY_NAME": settings.STRATEGY_NAME,
        "RUNTIME_STRATEGY_SET_JSON": settings.RUNTIME_STRATEGY_SET_JSON,
        "APPROVED_STRATEGY_PROFILE_PATH": settings.APPROVED_STRATEGY_PROFILE_PATH,
        "STRATEGY_APPROVED_PROFILE_PATH": settings.STRATEGY_APPROVED_PROFILE_PATH,
    }
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_DRY_RUN", True)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
        object.__setattr__(settings, "STRATEGY_NAME", "unsupported_legacy_global_name")
        object.__setattr__(settings, "RUNTIME_STRATEGY_SET_JSON", runtime_strategy_set_json)
        object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")
        object.__setattr__(settings, "STRATEGY_APPROVED_PROFILE_PATH", "")

        with pytest.raises(
            RuntimeError,
            match="decision_runner_live_multi_strategy_requires_runtime_decision_gateway",
        ):
            runtime_strategy_decision.DecisionRunner().decide_snapshot(
                _conn(),
                through_ts_ms=1_700_000_180_000,
            )
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)


@pytest.mark.parametrize(
    "runtime_strategy_set_json",
    (
        "",
        json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [
                    {
                        "strategy_name": "sma_with_filter",
                        "pair": "KRW-BTC",
                        "interval": "1m",
                    }
                ]
            }
        ),
    ),
)
def test_strategy_name_and_runtime_strategy_set_json_preflight_both_materialize(
    monkeypatch: pytest.MonkeyPatch,
    _clear_runtime_strategy_source_env: None,
    runtime_strategy_set_json: str,
) -> None:
    from bithumb_bot import runtime_strategy_set

    calls: list[str] = []

    def _materialize(self: object, spec: RuntimeStrategySpec) -> None:
        calls.append(spec.strategy_name)
        raise RuntimeError("unit_materialization_probe")

    monkeypatch.setattr(
        runtime_strategy_set.RuntimeDecisionRequestBuilder,
        "materialize_instance",
        _materialize,
    )

    cfg = _live_single_strategy_cfg(
        profile_path="/tmp/profile.json",
        runtime_strategy_set_json=runtime_strategy_set_json,
    )

    with pytest.raises(LiveModeValidationError) as exc:
        validate_runtime_strategy_set_selection(cfg)

    assert calls == ["sma_with_filter"]
    assert "unit_materialization_probe" in str(exc.value)


def test_normalized_runtime_strategy_set_manifest_materializes_active_instances() -> None:
    left = RuntimeStrategySpec(
        "canary_non_sma",
        priority=5,
        weight=2.0,
        desired_exposure_krw=50_000.0,
        risk_budget_krw=20_000.0,
        parameters={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "BUY",
            "CANARY_ORDER_REASON": "left",
        },
    )
    right = RuntimeStrategySpec(
        "canary_non_sma",
        priority=10,
        weight=1.0,
        desired_exposure_krw=10_000.0,
        risk_budget_krw=5_000.0,
        parameters={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "HOLD",
            "CANARY_ORDER_REASON": "right",
        },
    )
    manifest = normalized_runtime_strategy_set_manifest(
        strategy_set=RuntimeStrategySet(source="unit", strategies=(left, right)),
    )

    assert manifest["single_pair_runtime_enforced"] is True
    assert manifest["single_interval_runtime_enforced"] is True
    assert manifest["runtime_scope"] == "multi-strategy / single-pair / single-interval runtime"
    assert manifest["runtime_scope_mode"] == "single_pair"
    assert manifest["supported_runtime_scope"] == "multi_strategy_single_pair_single_interval"
    assert manifest["multi_pair_portfolio_supported"] is False
    assert manifest["multiple_execution_targets_supported"] is False
    assert manifest["active_strategy_pairs"] == ["KRW-BTC"]
    assert manifest["active_strategy_intervals"] == ["1m"]
    unsupported = manifest["unsupported_runtime_scopes"]["multi_pair_portfolio"]
    assert unsupported["supported"] is False
    assert unsupported["fail_closed_reason"] == "multi_pair_runtime_unsupported"
    required = unsupported["required_before_enablement"]
    for item in (
        "pair-specific target state",
        "pair-specific runtime data preflight",
        "pair-specific strategy decision bundles or pair-scoped bundle partitioning",
        "pair-specific allocation targets",
        "pair-specific execution plans",
        "pair-specific submit/reconcile loops",
        "cross-pair risk budget semantics",
        "currency-scoped portfolio/accounting ledger or equivalent multi-asset accounting model",
    ):
        assert item in required
    assert manifest["market_scope"]["mode"] == "single_pair"
    assert manifest["execution_config_hash"].startswith("sha256:")
    assert manifest["risk_config_hash"].startswith("sha256:")
    assert manifest["active_strategy_count"] == 2
    assert str(manifest["runtime_strategy_set_manifest_hash"]).startswith("sha256:")
    instances = manifest["active_instances"]
    assert isinstance(instances, list)
    assert {item["strategy_instance_id"] for item in instances} == {
        RuntimeDecisionRequestBuilder().build_for_spec(left, through_ts_ms=123).strategy_instance_id,
        RuntimeDecisionRequestBuilder().build_for_spec(right, through_ts_ms=123).strategy_instance_id,
    }
    for item in instances:
        for key in (
            "strategy_instance_id",
            "strategy_name",
            "pair",
            "interval",
            "priority",
            "weight",
            "desired_exposure_krw",
            "risk_budget_krw",
            "approved_profile_path",
            "approved_profile_hash",
            "parameter_source",
            "parameters_raw",
            "parameters_materialized",
            "strategy_parameters_hash",
            "runtime_contract_hash",
            "plugin_contract_hash",
            "strategy_version",
            "runtime_adapter_config",
            "parameter_authority_audit",
            "legacy_compatibility_used",
            "runtime_decision_request_hash",
            "runtime_decision_request_hash_scope",
        ):
            assert key in item
        assert str(item["runtime_decision_request_hash"]).startswith("sha256:")
        assert item["runtime_decision_request_hash_scope"] == "run_start_blueprint_through_ts_null"
    assert len(manifest["strategy_instance_profile_bindings"]) == 2


def test_runtime_manifest_replays_decision_request_hashes_exactly() -> None:
    spec = RuntimeStrategySpec(
        "canary_non_sma",
        parameters=_complete_canary_parameters(CANARY_ORDER_REASON="manifest_replay"),
    )
    strategy_set = RuntimeStrategySet(source="unit", strategies=(spec,))
    manifest = normalized_runtime_strategy_set_manifest(strategy_set=strategy_set)
    request = RuntimeDecisionRequestBuilder().build_for_spec(spec, through_ts_ms=1_700_000_180_000)

    instance = manifest["active_instances"][0]
    assert instance["strategy_instance_id"] == request.strategy_instance_id
    assert instance["strategy_parameters_hash"] == request.strategy_parameters_hash
    assert instance["runtime_contract_hash"] == request.runtime_contract_hash
    assert instance["plugin_contract_hash"] == request.plugin_contract_hash
    run_start_request = RuntimeDecisionRequestBuilder().build_for_spec(spec, through_ts_ms=None)
    assert instance["runtime_decision_request_hash"] == run_start_request.request_hash
    assert str(manifest["runtime_strategy_set_manifest_hash"]).startswith("sha256:")


def test_decision_bundle_all_results_match_manifest_strategy_instances() -> None:
    left = RuntimeStrategySpec(
        "canary_non_sma",
        strategy_instance_id="left_canary",
        parameters=_complete_canary_parameters(CANARY_ORDER_REASON="left"),
    )
    right = RuntimeStrategySpec("safe_hold", strategy_instance_id="right_hold")
    strategy_set = RuntimeStrategySet(source="unit", strategies=(left, right))
    manifest = normalized_runtime_strategy_set_manifest(strategy_set=strategy_set)
    results = []
    for spec in strategy_set.active_strategies:
        request = RuntimeDecisionRequestBuilder().build_for_spec(spec, through_ts_ms=1_700_000_180_000)
        result = _RuntimeResult(spec.strategy_name)
        result.decision = replace(result.decision, strategy_name=spec.strategy_name)
        results.append(_attach_unit_request_metadata(result, request))

    bundle = RuntimeStrategyDecisionResultBundle(strategy_set=strategy_set, results=tuple(results))
    bundle_payload = bundle.as_dict()

    assert bundle_payload["runtime_strategy_set_manifest_hash"] == manifest["runtime_strategy_set_manifest_hash"]
    assert {item["strategy_instance_id"] for item in bundle_payload["results"]} == {
        item["strategy_instance_id"] for item in manifest["active_instances"]
    }


def test_runtime_strategy_set_dump_cli_validates_and_prints_manifest(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from bithumb_bot.cli.main import main as cli_main

    monkeypatch.setenv(
        "RUNTIME_STRATEGY_SET_JSON",
        json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [
                    {
                        "strategy_name": "safe_hold",
                        "pair": "KRW-BTC",
                        "interval": "1m",
                    }
                ]
            }
        ),
    )
    cfg = replace(settings, MODE="paper", PAIR="KRW-BTC", RUNTIME_STRATEGY_SET_JSON=os.environ["RUNTIME_STRATEGY_SET_JSON"])

    assert cli_main(["runtime-strategy-set-dump"], context=argparse.Namespace(settings=cfg, printer=print, env_summary=None)) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["source"] == "RUNTIME_STRATEGY_SET_JSON"
    assert payload["active_strategy_count"] == 1
    assert payload["active_instances"][0]["strategy_name"] == "safe_hold"


def test_runtime_strategy_set_lint_cli_fails_on_pair_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from bithumb_bot.cli.main import main as cli_main

    monkeypatch.setenv(
        "RUNTIME_STRATEGY_SET_JSON",
        json.dumps(
            {
                "market_scope": {"mode": "single_pair", "pair": "KRW-BTC", "interval": "1m"},
                "strategies": [{"strategy_name": "safe_hold", "pair": "KRW-ETH"}],
            }
        ),
    )
    cfg = replace(settings, MODE="paper", PAIR="KRW-BTC", RUNTIME_STRATEGY_SET_JSON=os.environ["RUNTIME_STRATEGY_SET_JSON"])

    with pytest.raises(Exception) as exc:
        cli_main(["runtime-strategy-set-lint"], context=argparse.Namespace(settings=cfg, printer=print, env_summary=None))
    assert exc.type.__name__ == "LiveModeValidationError"
    assert "runtime_strategy_pair_mismatch" in str(exc.value)
