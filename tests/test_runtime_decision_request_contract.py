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
from bithumb_bot.runtime_adapters.safe_hold import SafeHoldRuntimeDecisionAdapter
from bithumb_bot.runtime_strategy_decision import RuntimeDecisionAdapter, RuntimeDecisionRequest
from bithumb_bot.runtime_strategy_set import (
    RuntimeDecisionRequestBuilder,
    RuntimeStrategyDecisionCollector,
    RuntimeStrategyDecisionResultBundle,
    RuntimeStrategySet,
    RuntimeStrategySpec,
    normalized_runtime_strategy_set_manifest,
)
from bithumb_bot.strategy_plugins.canary_non_sma import CanaryNonSmaRuntimeDecisionAdapter
from bithumb_bot.runtime_adapters.sma_with_filter import SmaWithFilterRuntimeConfig
from bithumb_bot.research.strategy_spec import strategy_spec_for_name
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
    params = inspect.signature(RuntimeDecisionAdapter.decide).parameters
    assert "request" in params
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
                "compute_signal_runtime_handoff",
                "collect",
                "collect_runtime_strategy_decisions",
                "run_loop",
            }:
                names = {arg.arg for arg in [*node.args.args, *node.args.kwonlyargs]}
                assert "short_n" not in names
                assert "long_n" not in names


def test_collector_passes_runtime_decision_request(monkeypatch: pytest.MonkeyPatch) -> None:
    received: list[RuntimeDecisionRequest] = []

    class _Adapter:
        strategy_name = "canary_non_sma"

        def decide(self, conn: Any, request: RuntimeDecisionRequest):
            del conn
            received.append(request)
            return _RuntimeResult(self.strategy_name)

        def typed_authority_required(self) -> bool:
            return True

    from bithumb_bot import runtime_strategy_decision

    runtime_strategy_decision.list_runtime_decision_adapters()
    monkeypatch.setitem(runtime_strategy_decision._RUNTIME_DECISION_ADAPTERS, "canary_non_sma", _Adapter)
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(RuntimeStrategySpec("canary_non_sma"),),
    )
    bundle = RuntimeStrategyDecisionCollector().collect(
        _conn(),
        strategy_set,
        through_ts_ms=1_700_000_180_000,
    )

    assert bundle is not None
    assert len(received) == 1
    assert isinstance(received[0], RuntimeDecisionRequest)


def test_non_sma_adapters_work_without_sma_parameters() -> None:
    conn = _conn()
    try:
        safe_request = RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec("safe_hold"),
            through_ts_ms=1_700_000_180_000,
        )
        canary_request = RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec("canary_non_sma"),
            through_ts_ms=1_700_000_180_000,
        )
        for request, adapter in (
            (safe_request, SafeHoldRuntimeDecisionAdapter()),
            (canary_request, CanaryNonSmaRuntimeDecisionAdapter()),
        ):
            assert "SMA_SHORT" not in request.parameters
            assert "SMA_LONG" not in request.parameters
            result = adapter.decide(conn, request)
            assert result is not None
        assert SafeHoldRuntimeDecisionAdapter().decide(conn, safe_request).decision.final_signal == "HOLD"
        assert CanaryNonSmaRuntimeDecisionAdapter().decide(conn, canary_request).decision.final_signal == "BUY"
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

        def decide(self, conn: Any, request: RuntimeDecisionRequest):
            del conn
            received[self.strategy_name] = request
            return _RuntimeResult(self.strategy_name)

        def typed_authority_required(self) -> bool:
            return True

    from bithumb_bot import runtime_strategy_decision

    runtime_strategy_decision.list_runtime_decision_adapters()
    monkeypatch.setitem(
        runtime_strategy_decision._RUNTIME_DECISION_ADAPTERS,
        "canary_non_sma",
        lambda: _Adapter("canary_non_sma"),
    )
    monkeypatch.setitem(
        runtime_strategy_decision._RUNTIME_DECISION_ADAPTERS,
        "sma_with_filter",
        lambda: _Adapter("sma_with_filter"),
    )
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(
            RuntimeStrategySpec("sma_with_filter", parameters=_complete_sma_parameters(SMA_SHORT=7, SMA_LONG=30)),
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
    RuntimeStrategyDecisionCollector().collect(_conn(), strategy_set, through_ts_ms=1_700_000_180_000)

    assert set(received) == {"sma_with_filter", "canary_non_sma"}
    assert received["sma_with_filter"].parameters["SMA_SHORT"] == 7
    assert received["sma_with_filter"].parameters["SMA_LONG"] == 30
    assert dict(received["sma_with_filter"].parameters_raw)["SMA_SHORT"] == 7
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

    left = _RuntimeResult("canary_non_sma")
    right = _RuntimeResult("canary_non_sma")
    left.base_context["strategy_instance_id"] = left_request.strategy_instance_id
    right.base_context["strategy_instance_id"] = right_request.strategy_instance_id

    bundle = RuntimeStrategyDecisionResultBundle(strategy_set=strategy_set, results=(right, left))

    assert left_request.strategy_instance_id != right_request.strategy_instance_id
    assert left_request.strategy_parameters_hash != right_request.strategy_parameters_hash
    assert [item.base_context["strategy_instance_id"] for item in bundle.results] == sorted(
        [left_request.strategy_instance_id, right_request.strategy_instance_id]
    )


def test_approved_profile_mismatch_fails_before_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    class _Adapter:
        strategy_name = "canary_non_sma"

        def decide(self, conn: Any, request: RuntimeDecisionRequest):
            nonlocal called
            called = True
            return None

        def typed_authority_required(self) -> bool:
            return True

    from bithumb_bot import runtime_strategy_decision, runtime_strategy_set

    monkeypatch.setitem(runtime_strategy_decision._RUNTIME_DECISION_ADAPTERS, "canary_non_sma", _Adapter)
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
        RuntimeStrategyDecisionCollector().collect(_conn(), strategy_set, through_ts_ms=1_700_000_180_000)

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
    assert request.parameter_source == "strict_profile"
    assert request.approved_profile_hash == "sha256:profile"
    assert request.observability_fields()["parameter_source"] == "strict_profile"


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
            RuntimeStrategySpec("canary_non_sma"),
            through_ts_ms=1_700_000_180_000,
        )
        result = CanaryNonSmaRuntimeDecisionAdapter().decide(conn, request)
        assert result is not None
        from bithumb_bot.runtime_strategy_decision import _attach_runtime_request_metadata

        _attach_runtime_request_metadata(result, request)
        bundle = ExecutionPlanner().plan_envelope(
            conn,
            DecisionEnvelope.from_runtime_result(result),
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
        "SMA_SHORT": settings.SMA_SHORT,
        "SMA_LONG": settings.SMA_LONG,
        "SMA_FILTER_GAP_MIN_RATIO": settings.SMA_FILTER_GAP_MIN_RATIO,
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


def test_settings_compat_parameter_source_is_explicit() -> None:
    request = RuntimeDecisionRequestBuilder().build_for_spec(
        RuntimeStrategySpec("canary_non_sma", pair="KRW-BTC", interval="1m"),
        through_ts_ms=1_700_000_180_000,
    )

    assert request.parameter_source == "settings_compat"
    assert request.observability_fields()["parameter_source"] == "settings_compat"


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
        "SMA_SHORT",
        "SMA_LONG",
        "SMA_FILTER_GAP_MIN_RATIO",
        "SMA_FILTER_VOL_WINDOW",
        "SMA_FILTER_VOL_MIN_RANGE_RATIO",
        "SMA_FILTER_OVEREXT_LOOKBACK",
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO",
        "SMA_COST_EDGE_ENABLED",
        "SMA_COST_EDGE_MIN_RATIO",
        "SMA_MARKET_REGIME_ENABLED",
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
    unexpected_sma = sorted(
        name
        for name in field_names
        if name.startswith("SMA_") and name not in legacy_strategy_specific
    )

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

    with pytest.raises(LiveModeValidationError, match="runtime_strategy_pair_mismatch"):
        validate_runtime_strategy_set_selection(cfg)


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
        ):
            assert key in item


def test_runtime_strategy_set_dump_cli_validates_and_prints_manifest(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from bithumb_bot.cli.main import main as cli_main

    monkeypatch.setenv(
        "RUNTIME_STRATEGY_SET_JSON",
        json.dumps(
            {
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
        json.dumps({"strategies": [{"strategy_name": "safe_hold", "pair": "KRW-ETH"}]}),
    )
    cfg = replace(settings, MODE="paper", PAIR="KRW-BTC", RUNTIME_STRATEGY_SET_JSON=os.environ["RUNTIME_STRATEGY_SET_JSON"])

    with pytest.raises(LiveModeValidationError, match="runtime_strategy_pair_mismatch"):
        cli_main(["runtime-strategy-set-lint"], context=argparse.Namespace(settings=cfg, printer=print, env_summary=None))
