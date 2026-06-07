from __future__ import annotations

import ast
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest

from bithumb_bot.db_core import ensure_schema
from bithumb_bot import runtime_strategy_decision, runtime_strategy_set
from bithumb_bot.runtime_strategy_decision import RuntimeDecisionRequest
from bithumb_bot.runtime_strategy_set import validate_runtime_decision_result_provenance
from bithumb_bot.strategy_policy_contract import PositionSnapshot, StrategyDecisionV2


def _request() -> RuntimeDecisionRequest:
    return RuntimeDecisionRequest(
        strategy_instance_id="unit:krw-btc:1m",
        strategy_name="canary_non_sma",
        pair="KRW-BTC",
        interval="1m",
        through_ts_ms=1_700_000_000_000,
        parameters={},
        parameters_raw={},
        parameters_materialized={},
        strategy_parameters_hash="sha256:parameters",
        approved_profile_path="/runtime/profile.json",
        approved_profile_hash="sha256:profile",
        runtime_strategy_spec=SimpleNamespace(
            parameter_authority_audit={},
            profile_authority_context={},
            legacy_compatibility_used=False,
        ),
        runtime_contract_hash="sha256:runtime-contract",
        parameter_source="approved_profile",
        plugin_contract_hash="sha256:plugin-contract",
        strategy_version="unit",
        request_hash="sha256:request",
    )


def _decision() -> StrategyDecisionV2:
    return StrategyDecisionV2(
        strategy_name="canary_non_sma",
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
        trace={},
        policy_hash="sha256:pure",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash="sha256:decision",
    )


def _result(request: RuntimeDecisionRequest):
    base = request.observability_fields()
    replay = {
        "schema_version": 1,
        "candle_ts": request.through_ts_ms,
        "runtime_decision_request_hash": request.request_hash,
        "strategy_instance_id": request.strategy_instance_id,
        "scope_key_hash": request.scope_key_hash,
        "strategy_parameters_hash": request.strategy_parameters_hash,
        "approved_profile_hash": request.approved_profile_hash,
        "runtime_contract_hash": request.runtime_contract_hash,
        "plugin_contract_hash": request.plugin_contract_hash,
        "through_ts_ms": request.through_ts_ms,
    }
    return SimpleNamespace(
        decision=_decision(),
        base_context=base,
        candle_ts=request.through_ts_ms,
        market_price=10.0,
        policy_hashes=None,
        replay_fingerprint=replay,
        boundary={},
        as_legacy_dict=lambda: dict(base),
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


def _patch_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Adapter:
        strategy_name = "canary_non_sma"

        def decide_feature_snapshot(
            self,
            request: RuntimeDecisionRequest,
            feature_snapshot: Any,
        ):
            del feature_snapshot
            return _result(request)

        def typed_authority_required(self) -> bool:
            return True

    monkeypatch.setattr(
        runtime_strategy_decision,
        "get_runtime_decision_adapter",
        lambda strategy_name: _Adapter()
        if str(strategy_name).strip().lower() == "canary_non_sma"
        else None,
    )


def _run_decision_runner() -> object | None:
    conn = _conn()
    try:
        return runtime_strategy_decision.DecisionRunner().decide_snapshot(
            conn,
            strategy_name="canary_non_sma",
            through_ts_ms=1_700_000_180_000,
            parameter_overrides={
                "CANARY_ORDER_START_INDEX": 0,
                "CANARY_ORDER_SIDE": "BUY",
                "CANARY_ORDER_REASON": "unit_canary",
            },
            parameter_source="runtime_override",
        )
    finally:
        conn.close()


def test_runtime_result_bundle_rejects_missing_request_hash() -> None:
    request = _request()
    result = _result(request)
    del result.base_context["runtime_decision_request_hash"]

    with pytest.raises(ValueError, match="runtime_decision_request_metadata_missing:runtime_decision_request_hash"):
        validate_runtime_decision_result_provenance(result, request)


def test_runtime_result_bundle_rejects_scope_key_hash_mismatch() -> None:
    request = _request()
    result = _result(request)
    result.base_context["scope_key_hash"] = "sha256:wrong"

    with pytest.raises(ValueError, match="runtime_decision_request_metadata_mismatch:scope_key_hash"):
        validate_runtime_decision_result_provenance(result, request)


def test_runtime_result_bundle_rejects_missing_approved_profile_hash_in_replay_fingerprint() -> None:
    request = _request()
    result = _result(request)
    del result.replay_fingerprint["approved_profile_hash"]

    with pytest.raises(
        ValueError,
        match="runtime_decision_request_metadata_missing:replay_fingerprint.approved_profile_hash",
    ):
        validate_runtime_decision_result_provenance(result, request)


def test_runtime_result_bundle_rejects_missing_plugin_contract_hash() -> None:
    request = _request()
    result = _result(request)
    del result.replay_fingerprint["plugin_contract_hash"]

    with pytest.raises(
        ValueError,
        match="runtime_decision_request_metadata_missing:replay_fingerprint.plugin_contract_hash",
    ):
        validate_runtime_decision_result_provenance(result, request)


def test_runtime_result_bundle_creation_validates_provenance() -> None:
    source = Path("src/bithumb_bot/runtime_strategy_set.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    bundle_class = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "RuntimeStrategyDecisionResultBundle"
    )
    bundle_post_init = next(
        node
        for node in bundle_class.body
        if isinstance(node, ast.FunctionDef) and node.name == "__post_init__"
    )

    assert "validate_runtime_decision_result_provenance(result, request)" in ast.unparse(bundle_post_init)


def test_decision_runner_validates_runtime_decision_provenance_before_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_adapter(monkeypatch)
    seen: list[tuple[object, RuntimeDecisionRequest]] = []
    original = runtime_strategy_set.validate_runtime_decision_result_provenance

    def _record(result: object, request: RuntimeDecisionRequest) -> None:
        seen.append((result, request))
        original(result, request)

    monkeypatch.setattr(runtime_strategy_set, "validate_runtime_decision_result_provenance", _record)

    result = _run_decision_runner()

    assert result is not None
    assert len(seen) >= 1
    assert seen[-1][0] is result


def test_decision_runner_rejects_missing_request_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_adapter(monkeypatch)
    original = runtime_strategy_set._attach_runtime_request_metadata

    def _attach_without_request_hash(result: object, request: RuntimeDecisionRequest) -> None:
        original(result, request)
        result.base_context.pop("runtime_decision_request_hash", None)

    monkeypatch.setattr(runtime_strategy_set, "_attach_runtime_request_metadata", _attach_without_request_hash)

    with pytest.raises(ValueError, match="runtime_decision_request_metadata_missing:runtime_decision_request_hash"):
        _run_decision_runner()


def test_decision_runner_rejects_replay_fingerprint_scope_key_hash_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_adapter(monkeypatch)
    original = runtime_strategy_set._attach_runtime_request_metadata

    def _attach_with_bad_replay_scope_hash(result: object, request: RuntimeDecisionRequest) -> None:
        original(result, request)
        result.replay_fingerprint["scope_key_hash"] = "sha256:wrong"

    monkeypatch.setattr(runtime_strategy_set, "_attach_runtime_request_metadata", _attach_with_bad_replay_scope_hash)

    with pytest.raises(
        ValueError,
        match="runtime_decision_request_metadata_mismatch:replay_fingerprint.scope_key_hash",
    ):
        _run_decision_runner()


@pytest.mark.parametrize(
    ("location", "mutation", "expected"),
    (
        (
            "base_context",
            lambda result: result.base_context.pop("plugin_contract_hash", None),
            "runtime_decision_request_metadata_missing:plugin_contract_hash",
        ),
        (
            "base_context",
            lambda result: result.base_context.__setitem__("plugin_contract_hash", "sha256:wrong"),
            "runtime_decision_request_metadata_mismatch:plugin_contract_hash",
        ),
        (
            "replay_fingerprint",
            lambda result: result.replay_fingerprint.pop("plugin_contract_hash", None),
            "runtime_decision_request_metadata_missing:replay_fingerprint.plugin_contract_hash",
        ),
        (
            "replay_fingerprint",
            lambda result: result.replay_fingerprint.__setitem__("plugin_contract_hash", "sha256:wrong"),
            "runtime_decision_request_metadata_mismatch:replay_fingerprint.plugin_contract_hash",
        ),
    ),
)
def test_decision_runner_rejects_plugin_contract_hash_provenance_errors(
    monkeypatch: pytest.MonkeyPatch,
    location: str,
    mutation: Callable[[Any], object],
    expected: str,
) -> None:
    del location
    _patch_adapter(monkeypatch)
    original = runtime_strategy_set._attach_runtime_request_metadata

    def _attach_with_plugin_contract_error(result: object, request: RuntimeDecisionRequest) -> None:
        original(result, request)
        mutation(result)

    monkeypatch.setattr(runtime_strategy_set, "_attach_runtime_request_metadata", _attach_with_plugin_contract_error)

    with pytest.raises(ValueError, match=expected):
        _run_decision_runner()


def test_production_runtime_modules_do_not_call_runtime_adapters_directly() -> None:
    allowed = {
        ("src/bithumb_bot/runtime_strategy_set.py", "_decide_with_feature_snapshot"),
    }
    production_files = (
        "src/bithumb_bot/runtime_strategy_set.py",
        "src/bithumb_bot/runtime_strategy_decision.py",
        "src/bithumb_bot/runtime_decision_service.py",
        "src/bithumb_bot/runtime_adapter_bootstrap.py",
    )
    violations: list[str] = []
    for path in production_files:
        tree = ast.parse(Path(path).read_text(encoding="utf-8"))
        parents: dict[ast.AST, ast.AST] = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            forbidden_call = None
            if isinstance(func, ast.Attribute) and func.attr in {"decide", "decide_feature_snapshot"}:
                forbidden_call = func.attr
            if (
                isinstance(func, ast.Name)
                and func.id == "feature_decider"
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id == "request"
                and isinstance(node.args[1], ast.Name)
                and node.args[1].id == "feature_snapshot"
            ):
                forbidden_call = "feature_decider(request, feature_snapshot)"
            if forbidden_call is None:
                continue
            parent = parents.get(node)
            function_name = ""
            while parent is not None:
                if isinstance(parent, ast.FunctionDef):
                    function_name = parent.name
                    break
                parent = parents.get(parent)
            if (path, function_name) not in allowed:
                violations.append(f"{path}:{function_name}:{forbidden_call}")

    assert violations == []
