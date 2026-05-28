from __future__ import annotations

import ast
import argparse
import inspect
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from bithumb_bot.db_core import ensure_schema
from bithumb_bot.decision_envelope import DecisionEnvelope
from bithumb_bot.run_loop_execution_planner import ExecutionPlanner
from bithumb_bot.runtime_decision_contract import RuntimeStrategyPolicyHashes
from bithumb_bot.runtime_adapters.safe_hold import SafeHoldRuntimeDecisionAdapter
from bithumb_bot.runtime_strategy_decision import RuntimeDecisionAdapter, RuntimeDecisionRequest
from bithumb_bot.runtime_strategy_set import (
    RuntimeDecisionRequestBuilder,
    RuntimeStrategyDecisionCollector,
    RuntimeStrategySet,
    RuntimeStrategySpec,
)
from bithumb_bot.strategy_plugins.canary_non_sma import CanaryNonSmaRuntimeDecisionAdapter
from bithumb_bot.runtime_adapters.sma_with_filter import SmaWithFilterRuntimeConfig
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
            RuntimeStrategySpec("sma_with_filter", parameters={"SMA_SHORT": 7, "SMA_LONG": 30}),
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
    assert dict(received["sma_with_filter"].parameters) == {"SMA_SHORT": 7, "SMA_LONG": 30}
    assert dict(received["canary_non_sma"].parameters) == {
        "CANARY_ORDER_START_INDEX": 0,
        "CANARY_ORDER_SIDE": "BUY",
        "CANARY_ORDER_REASON": "unit",
    }
    assert received["sma_with_filter"].request_hash != received["canary_non_sma"].request_hash


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
    monkeypatch.setattr(runtime_strategy_set, "load_approved_profile", lambda path: {"profile_content_hash": "sha256:unit"})
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
            parameters={
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
            },
        ),
        through_ts_ms=1_700_000_180_000,
    )

    config = SmaWithFilterRuntimeConfig.from_runtime_request(request)
    strategy = config.build_strategy()

    assert strategy.short_n == 2
    assert strategy.long_n == 5
    assert strategy.min_gap_ratio == 0.9
    assert strategy.volatility_window == 3
    assert strategy.min_volatility_ratio == 0.8
    assert strategy.cost_edge_enabled is False
    assert strategy.market_regime_enabled is False
    assert strategy.slippage_bps == 33
    assert strategy.live_fee_rate_estimate == 0.0123


def test_strategy_specific_settings_extension_uses_generic_json_not_new_fields() -> None:
    source = Path("src/bithumb_bot/config.py").read_text(encoding="utf-8")
    assert "CANARY_ORDER_START_INDEX" not in source
    assert "CANARY_ORDER_SIDE" not in source
    assert "CANARY_ORDER_REASON" not in source
    assert "STRATEGY_PARAMETERS_JSON" in source


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
