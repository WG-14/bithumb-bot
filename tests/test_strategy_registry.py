from __future__ import annotations

import ast
import json
from pathlib import Path
import inspect
import os
import sqlite3

import pytest

from bithumb_bot import config
from bithumb_bot import engine as engine_module
from bithumb_bot import profile_cli
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.engine import compute_signal
from bithumb_bot.profile_cli import cmd_replay_decision
from bithumb_bot.runtime_sma_snapshot import build_sma_with_filter_replay_bundle
from bithumb_bot.research.strategy_registry import (
    ResearchStrategyRegistryError,
    resolve_research_strategy_plugin,
)
import bithumb_bot.strategy.base as strategy_base
import bithumb_bot.strategy.registry as strategy_registry
from bithumb_bot.strategy.sma import SmaWithFilterStrategy
from bithumb_bot.strategy.sma_legacy_adapter import (
    LEGACY_DB_BOUND_STRATEGY_STATUS,
    LegacySmaWithFilterDbAdapter,
    create_legacy_sma_with_filter_db_adapter,
)
from bithumb_bot.strategy import (
    create_legacy_strategy,
    create_strategy_policy,
    list_legacy_strategies,
    list_strategy_policies,
)
from bithumb_bot.strategy.base import StrategyDecision


def test_registry_default_strategy_available() -> None:
    assert "sma_cross" in list_legacy_strategies()
    assert "sma_cross" not in list_strategy_policies()
    assert "sma_with_filter" in list_strategy_policies()


def test_public_sma_with_filter_name_resolves_to_policy_strategy() -> None:
    from bithumb_bot.strategy import SmaWithFilterStrategy as PublicSmaWithFilterStrategy

    assert PublicSmaWithFilterStrategy.__module__ == "bithumb_bot.strategy.sma_policy_strategy"
    assert SmaWithFilterStrategy.__module__ == "bithumb_bot.strategy.sma_policy_strategy"


def test_legacy_sma_adapter_has_explicit_non_policy_name() -> None:
    import bithumb_bot.strategy.sma_legacy_adapter as legacy_adapter

    assert "class SmaWithFilterStrategy" not in inspect.getsource(legacy_adapter)
    legacy = create_legacy_sma_with_filter_db_adapter(short_n=2, long_n=3)
    assert isinstance(legacy, LegacySmaWithFilterDbAdapter)
    assert hasattr(legacy, "decide")
    assert not hasattr(legacy, "decide_snapshot")


def test_live_policy_registry_cannot_instantiate_legacy_sma_with_filter_adapter() -> None:
    policy = create_strategy_policy("sma_with_filter", short_n=2, long_n=3)

    assert policy.__class__.__module__ == "bithumb_bot.strategy.sma_policy_strategy"
    assert not isinstance(policy, LegacySmaWithFilterDbAdapter)
    assert "sma_with_filter" not in list_legacy_strategies()


def test_db_bound_strategy_protocol_is_explicitly_legacy() -> None:
    legacy_source = inspect.getsource(strategy_base.LegacyDbStrategy)
    policy_source = inspect.getsource(strategy_base.StrategyPolicy)

    assert "Deprecated DB-bound strategy facade" in legacy_source
    assert "compatibility-only" in legacy_source
    assert "decide(" in legacy_source
    assert not hasattr(strategy_base, "Strategy")
    assert "Promotion-grade snapshot strategy interface" in policy_source
    assert "decide_snapshot(" in policy_source


def test_registry_has_separate_policy_and_legacy_creation_paths() -> None:
    policy_annotations = strategy_registry.create_strategy_policy.__annotations__
    legacy_annotations = strategy_registry.create_legacy_strategy.__annotations__

    assert "StrategyPolicy" in str(policy_annotations.get("return"))
    assert "LegacyDbStrategy" in str(legacy_annotations.get("return"))
    assert "StrategyPolicy" not in str(legacy_annotations.get("return"))
    assert LEGACY_DB_BOUND_STRATEGY_STATUS.endswith("not_promotion_grade")


def test_compute_signal_uses_default_strategy_name_from_settings(tmp_path) -> None:
    old_db_path = settings.DB_PATH
    old_strategy_name = settings.STRATEGY_NAME
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "strategy_default.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "STRATEGY_NAME", "sma_with_filter")

    conn = ensure_db()
    base_ts = 1_700_000_000_000
    try:
        closes = [10.0 + 0.2 * idx for idx in range(40)]
        for idx, close in enumerate(closes):
            ts = base_ts + idx * 60_000
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
            )
        conn.commit()

        result = compute_signal(conn, 2, 3)
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        object.__setattr__(settings, "STRATEGY_NAME", old_strategy_name)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert result is not None
    assert result["signal"] in {"BUY", "SELL", "HOLD"}
    assert result["strategy"] == "sma_with_filter"
    assert "reason" in result


def test_compute_signal_routes_sma_with_filter_through_snapshot_orchestration(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old_db_path = settings.DB_PATH
    old_strategy_name = settings.STRATEGY_NAME
    old_env_db_path = os.environ.get("DB_PATH")
    calls: list[str] = []

    original_typed_boundary = engine_module.decide_sma_with_filter_runtime_snapshot_from_db

    def _snapshot_orchestration(conn, strategy, *, through_ts_ms=None, normalizer=None):
        calls.append(strategy.name)
        return original_typed_boundary(
            conn,
            strategy,
            through_ts_ms=through_ts_ms,
            normalizer=normalizer,
        )

    monkeypatch.setattr(
        engine_module,
        "decide_sma_with_filter_runtime_snapshot_from_db",
        _snapshot_orchestration,
    )

    db_path = str(tmp_path / "strategy_snapshot_route.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "STRATEGY_NAME", "sma_with_filter")

    conn = ensure_db()
    base_ts = 1_700_000_000_000
    try:
        closes = [10.0 + 0.2 * idx for idx in range(40)]
        for idx, close in enumerate(closes):
            ts = base_ts + idx * 60_000
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
            )
        conn.commit()

        result = compute_signal(conn, 2, 3)
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        object.__setattr__(settings, "STRATEGY_NAME", old_strategy_name)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert result is not None
    assert calls == ["sma_with_filter"]


def test_live_sma_with_filter_route_does_not_call_legacy_decide(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old_db_path = settings.DB_PATH
    old_mode = settings.MODE
    old_armed = settings.LIVE_REAL_ORDER_ARMED
    old_dry_run = settings.LIVE_DRY_RUN
    old_strategy_name = settings.STRATEGY_NAME
    old_env_db_path = os.environ.get("DB_PATH")

    def _fail_legacy_decide(*args, **kwargs):
        raise AssertionError("legacy Strategy.decide(conn) path called")

    def _fail_legacy_normalized_db(*args, **kwargs):
        raise AssertionError("legacy _decide_from_normalized_db path called")

    monkeypatch.setattr(SmaWithFilterStrategy, "decide", _fail_legacy_decide, raising=False)
    monkeypatch.setattr(
        SmaWithFilterStrategy,
        "_decide_from_normalized_db",
        _fail_legacy_normalized_db,
        raising=False,
    )
    monkeypatch.setattr(
        engine_module,
        "create_legacy_strategy",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("legacy registry creation path called")
        ),
    )
    snapshot_calls: list[str] = []

    def _snapshot_boundary(_conn, strategy, *, through_ts_ms=None):
        snapshot_calls.append(strategy.name)
        return None

    monkeypatch.setattr(
        engine_module,
        "decide_sma_with_filter_runtime_snapshot_from_db",
        _snapshot_boundary,
    )

    db_path = str(tmp_path / "strategy_no_legacy_decide.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "STRATEGY_NAME", "sma_with_filter")

    conn = ensure_db()
    base_ts = 1_700_000_300_000
    try:
        closes = [10.0 + 0.2 * idx for idx in range(40)]
        for idx, close in enumerate(closes):
            ts = base_ts + idx * 60_000
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
            )
        conn.commit()

        result = engine_module.compute_strategy_decision_snapshot(conn, 2, 3)
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        object.__setattr__(settings, "MODE", old_mode)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", old_armed)
        object.__setattr__(settings, "LIVE_DRY_RUN", old_dry_run)
        object.__setattr__(settings, "STRATEGY_NAME", old_strategy_name)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert result is None
    assert snapshot_calls == ["sma_with_filter"]


def test_decision_runner_exposes_typed_strategy_decision_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int, str | None]] = []

    def _fake_impl(conn, short_n, long_n, *, through_ts_ms=None, strategy_name=None):
        calls.append((short_n, long_n, strategy_name))
        return None

    monkeypatch.setattr(engine_module, "_compute_strategy_decision_snapshot_impl", _fake_impl)

    runner = engine_module.DecisionRunner(strategy_name="sma_with_filter")
    with sqlite3.connect(":memory:") as conn:
        result = runner.decide_snapshot(conn, 2, 3, through_ts_ms=123)

    assert result is None
    assert calls == [(2, 3, "sma_with_filter")]


def test_live_real_sma_cross_rejected_before_legacy_strategy_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_mode = settings.MODE
    old_armed = settings.LIVE_REAL_ORDER_ARMED
    old_dry_run = settings.LIVE_DRY_RUN
    old_strategy_name = settings.STRATEGY_NAME
    legacy_calls: list[str] = []

    def _fail_legacy_creation(name: str, **_kwargs):
        legacy_calls.append(name)
        raise AssertionError("legacy DB strategy creation must not be reached")

    monkeypatch.setattr(engine_module, "create_legacy_strategy", _fail_legacy_creation)
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "STRATEGY_NAME", "sma_cross")
    try:
        with sqlite3.connect(":memory:") as conn:
            with pytest.raises(config.LiveModeValidationError, match="plain_sma_live_not_allowed"):
                engine_module.compute_strategy_decision_snapshot(conn, 2, 3)
    finally:
        object.__setattr__(settings, "MODE", old_mode)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", old_armed)
        object.__setattr__(settings, "LIVE_DRY_RUN", old_dry_run)
        object.__setattr__(settings, "STRATEGY_NAME", old_strategy_name)

    assert legacy_calls == []


def test_runtime_replay_bundle_contains_reproducibility_material(tmp_path) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "strategy_replay_bundle.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    base_ts = 1_700_000_400_000
    try:
        closes = [10.0 + 0.2 * idx for idx in range(40)]
        for idx, close in enumerate(closes):
            ts = base_ts + idx * 60_000
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
            )
        conn.commit()

        strategy = create_strategy_policy(
            "sma_with_filter",
            short_n=2,
            long_n=3,
            pair=settings.PAIR,
            interval=settings.INTERVAL,
        )
        changes_before_replay = conn.total_changes
        bundle = build_sma_with_filter_replay_bundle(
            conn,
            strategy,
            through_ts_ms=base_ts + 39 * 60_000,
        )
        changes_after_replay = conn.total_changes
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert bundle is not None
    assert {
        "schema_version",
        "strategy",
        "through_ts_ms",
        "boundary_stages",
        "market_snapshot",
        "position_snapshot",
        "policy_config",
        "execution_constraint_snapshot",
        "policy_input_hash",
        "policy_decision_hash",
        "pure_policy_hash",
        "replay_fingerprint",
        "pure_policy_trace",
        "final_strategy_decision",
        "execution_decision_reconstructable",
        "execution_decision_reconstruction_reason",
        "execution_decision_summary",
    }.issubset(bundle)
    assert changes_after_replay == changes_before_replay
    assert bundle["market_snapshot"]["candle_ts"] == base_ts + 39 * 60_000
    assert bundle["position_snapshot"] is not None
    assert bundle["policy_config"]["short_n"] == 2
    assert str(bundle["policy_input_hash"]).startswith("sha256:")
    assert str(bundle["policy_decision_hash"]).startswith("sha256:")
    assert str(bundle["pure_policy_hash"]).startswith("sha256:")
    assert isinstance(bundle["replay_fingerprint"], dict)
    assert bundle["replay_fingerprint"]["strategy_name"] == "sma_with_filter"
    assert bundle["replay_fingerprint"]["through_ts_ms"] == base_ts + 39 * 60_000
    assert bundle["pure_policy_trace"]
    assert bundle["final_strategy_decision"]["strategy"] == "sma_with_filter"
    assert bundle["execution_decision_reconstructable"] is False
    assert bundle["execution_decision_reconstruction_reason"] == (
        "live_readiness_context_not_available_in_db_snapshot"
    )
    assert bundle["execution_decision_summary"]["execution_engine"] in {"lot_native", "target_delta"}


def test_replay_decision_cli_outputs_single_read_only_replay_bundle(tmp_path, capsys) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "single_replay_decision.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    base_ts = 1_700_000_500_000
    try:
        for idx in range(40):
            close = 10.0 + 0.2 * idx
            ts = base_ts + idx * 60_000
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
            )
        conn.commit()
        changes_before_replay = conn.total_changes
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    rc = cmd_replay_decision(
        db_path=db_path,
        strategy_name="sma_with_filter",
        candle_ts=base_ts + 39 * 60_000,
        as_json=True,
    )
    stdout = capsys.readouterr().out
    out = json.loads(stdout[stdout.index('{\n  "bundle"') :])

    verify_conn = sqlite3.connect(db_path)
    try:
        changes_after_replay = verify_conn.total_changes
    finally:
        verify_conn.close()

    assert rc == 0
    assert out["ok"] is True
    assert out["command"] == "replay-decision"
    bundle = out["bundle"]
    assert bundle["schema_version"] == 1
    assert bundle["boundary_stages"]["snapshot_builder"] == (
        "runtime_sma_snapshot.decide_sma_with_filter_snapshot_from_db"
    )
    assert bundle["market_snapshot"]["candle_ts"] == base_ts + 39 * 60_000
    assert str(bundle["policy_input_hash"]).startswith("sha256:")
    assert str(bundle["policy_decision_hash"]).startswith("sha256:")
    assert str(bundle["pure_policy_hash"]).startswith("sha256:")
    assert bundle["final_strategy_decision"]["strategy"] == "sma_with_filter"
    assert bundle["decision_context_schema_version"] == 1
    assert bundle["code_provenance"] == {"source": "unavailable"}
    assert bundle["final_typed_strategy_decision"]["policy_decision_hash"] == bundle["policy_decision_hash"]
    assert bundle["execution_decision_reconstructable"] is False
    assert bundle["execution_decision_reconstruction_reason"] == (
        "live_readiness_context_not_available_in_db_snapshot"
    )
    assert "execution_decision_summary" in bundle
    assert changes_after_replay == 0
    assert changes_before_replay > 0


def test_compute_signal_allows_strategy_override_for_backtest_compatibility(tmp_path) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "strategy_override.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    base_ts = 1_700_000_100_000
    try:
        for idx, close in enumerate([10.0, 11.0, 12.0, 13.0, 14.0]):
            ts = base_ts + idx * 60_000
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
            )
        conn.commit()

        result = compute_signal(conn, 2, 3, strategy_name="sma_cross")
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert result is not None
    assert result["signal"] in {"BUY", "SELL", "HOLD"}
    assert result["strategy"] == "sma_cross"
    assert "reason" in result


def test_compute_signal_normalizes_strategy_override_name(tmp_path) -> None:
    old_db_path = settings.DB_PATH
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "strategy_override_normalized.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    base_ts = 1_700_000_200_000
    try:
        for idx, close in enumerate([10.0, 11.0, 12.0, 13.0, 14.0]):
            ts = base_ts + idx * 60_000
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
            )
        conn.commit()

        result = compute_signal(conn, 2, 3, strategy_name="  SMA_CROSS ")
    finally:
        conn.close()
        object.__setattr__(settings, "DB_PATH", old_db_path)
        if old_env_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = old_env_db_path

    assert result is not None
    assert result["strategy"] == "sma_cross"


def test_live_compute_signal_rejects_plain_sma_cross_override() -> None:
    old_mode = settings.MODE

    object.__setattr__(settings, "MODE", "live")
    try:
        with sqlite3.connect(":memory:") as conn:
            with pytest.raises(config.LiveModeValidationError) as exc:
                compute_signal(conn, 2, 3, strategy_name="sma_cross")
    finally:
        object.__setattr__(settings, "MODE", old_mode)

    assert "plain_sma_live_not_allowed" in str(exc.value)


def test_live_real_order_boundary_disallows_legacy_db_strategy_fallback() -> None:
    old_mode = settings.MODE
    old_armed = settings.LIVE_REAL_ORDER_ARMED
    old_dry_run = settings.LIVE_DRY_RUN

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    try:
        assert engine_module._legacy_db_strategy_fallback_allowed(
            selected_strategy_name="sma_cross"
        ) is False
        assert engine_module._legacy_db_strategy_fallback_allowed(
            selected_strategy_name="sma_with_filter"
        ) is False
    finally:
        object.__setattr__(settings, "MODE", old_mode)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", old_armed)
        object.__setattr__(settings, "LIVE_DRY_RUN", old_dry_run)


def test_sma_cross_is_excluded_from_research_promotion_plugin_registry() -> None:
    with pytest.raises(ResearchStrategyRegistryError, match="unsupported research strategy: sma_cross"):
        resolve_research_strategy_plugin("sma_cross")


def test_registry_rejects_unknown_strategy_name() -> None:
    with pytest.raises(ValueError, match="strategy_policy_not_registered"):
        create_strategy_policy("does_not_exist")


def test_registry_can_create_filtered_sma_strategy() -> None:
    strategy = create_strategy_policy("sma_with_filter", short_n=2, long_n=3)
    assert strategy.name == "sma_with_filter"
    assert strategy.__class__.__module__ == "bithumb_bot.strategy.sma_policy_strategy"
    assert hasattr(strategy, "decide_snapshot")
    assert not hasattr(strategy, "decide")


def test_policy_registry_excludes_plain_sma_cross() -> None:
    with pytest.raises(ValueError, match="strategy_policy_not_registered:sma_cross"):
        create_strategy_policy("sma_cross", short_n=2, long_n=3)


def test_legacy_registry_can_create_plain_sma_cross_for_compatibility() -> None:
    strategy = create_legacy_strategy("sma_cross", short_n=2, long_n=3)
    assert strategy.name == "sma_cross"
    assert hasattr(strategy, "decide")
    assert not hasattr(strategy, "decide_snapshot")


def test_promotion_grade_modules_do_not_import_sma_legacy_adapter() -> None:
    promotion_modules = [
        Path("src/bithumb_bot/engine.py"),
        Path("src/bithumb_bot/runtime_sma_snapshot.py"),
        Path("src/bithumb_bot/runtime_sma_snapshot_builder.py"),
        Path("src/bithumb_bot/strategy/sma_policy_strategy.py"),
    ]

    for module_path in promotion_modules:
        tree = ast.parse(module_path.read_text(encoding="utf-8-sig"))
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
        assert "sma_legacy_adapter" not in "\n".join(imported)


def test_engine_prepares_persistence_context_and_execution_request() -> None:
    class _Summary:
        def as_dict(self) -> dict[str, object]:
            return {
                "final_action": "REBALANCE_TO_TARGET",
                "submit_expected": True,
                "pre_submit_proof_status": "passed",
                "block_reason": "none",
                "residual_live_sell_mode": "block",
                "residual_buy_sizing_mode": "block",
                "target_shadow_decision": {"target_policy_action": "maintain"},
            }

    context = engine_module.prepare_strategy_decision_persistence_context(
        decision_context={"strategy": "sma_with_filter"},
        execution_decision_summary=_Summary(),
        readiness_payload={"residual_inventory_state": "none"},
        target_policy_metadata={"target_origin": "runtime_state"},
    )

    assert context["execution_decision"]["final_action"] == "REBALANCE_TO_TARGET"  # type: ignore[index]
    assert context["final_action"] == "REBALANCE_TO_TARGET"
    assert context["submit_expected"] is True
    assert context["pre_submit_proof_status"] == "passed"
    assert context["execution_block_reason"] == "none"
    assert context["residual_inventory_state"] == "none"
    assert context["target_policy_action"] == "maintain"
    assert context["target_origin"] == "runtime_state"

    request = engine_module.build_signal_execution_request(
        signal="BUY",
        ts=123,
        market_price=10.0,
        strategy_name="sma_with_filter",
        decision_id=7,
        decision_reason="reason",
        exit_rule_name=None,
        execution_decision_summary=None,
        decision_context=context,
    )
    assert request.signal == "BUY"
    assert request.decision_context is context


def test_engine_no_direct_sma_import() -> None:
    engine_source = Path("src/bithumb_bot/engine.py").read_text()
    assert "from .strategy.sma import" not in engine_source


def test_promotion_live_runtime_modules_do_not_import_strategy_sma_facade() -> None:
    guarded_paths = [
        Path("src/bithumb_bot/app.py"),
        Path("src/bithumb_bot/engine.py"),
        Path("src/bithumb_bot/execution_service.py"),
        Path("src/bithumb_bot/runtime_sma_snapshot.py"),
        Path("src/bithumb_bot/runtime_sma_snapshot_builder.py"),
        Path("src/bithumb_bot/profile_cli.py"),
        Path("src/bithumb_bot/research/backtest_engine.py"),
        Path("src/bithumb_bot/research/backtest_kernel.py"),
        Path("src/bithumb_bot/research/strategy_registry.py"),
    ]
    forbidden = {
        "bithumb_bot.strategy.sma",
        "strategy.sma",
    }
    for path in guarded_paths:
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level:
                    package_parts = path.with_suffix("").parts
                    rel_base = ".".join(package_parts[1 : len(package_parts) - node.level + 1])
                    module = f"{rel_base}.{module}" if module else rel_base
                assert module not in forbidden and not module.endswith(".strategy.sma"), str(path)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name not in forbidden, str(path)


def test_promotion_research_export_requires_explicit_runtime_bound_parameters() -> None:
    with pytest.raises(ValueError, match="promotion_runtime_bound_parameter_missing:"):
        profile_cli._require_explicit_runtime_bound_strategy_parameters(
            strategy_name="sma_with_filter",
            parameter_values={"SMA_SHORT": 2, "SMA_LONG": 3},
        )


def test_promotion_research_export_accepts_explicit_runtime_bound_parameters() -> None:
    profile_cli._require_explicit_runtime_bound_strategy_parameters(
        strategy_name="sma_with_filter",
        parameter_values={
            "SMA_SHORT": 2,
            "SMA_LONG": 3,
            "SMA_FILTER_GAP_MIN_RATIO": 0.0,
            "SMA_FILTER_VOL_WINDOW": 3,
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": 0.0,
            "SMA_MARKET_REGIME_ENABLED": False,
            "SMA_FILTER_OVEREXT_LOOKBACK": 1,
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": 0.0,
            "SMA_COST_EDGE_ENABLED": False,
            "SMA_COST_EDGE_MIN_RATIO": 0.0,
            "ENTRY_EDGE_BUFFER_RATIO": 0.0,
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO": 0.0,
            "STRATEGY_ENTRY_SLIPPAGE_BPS": 0.0,
            "LIVE_FEE_RATE_ESTIMATE": 0.0,
            "STRATEGY_EXIT_RULES": "opposite_cross",
            "STRATEGY_EXIT_STOP_LOSS_RATIO": 0.0,
            "STRATEGY_EXIT_MAX_HOLDING_MIN": 0,
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": 0.0,
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": 0.0,
        },
    )
