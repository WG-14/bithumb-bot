from __future__ import annotations

import ast
import sqlite3
from pathlib import Path

import pytest

from bithumb_bot.db_core import ensure_schema
from bithumb_bot.research.strategy_registry import TEST_TOP_OF_BOOK_REQUIRED_STRATEGY
from bithumb_bot.runtime_data_provider import (
    RuntimeDataRequirementResolver,
    SQLiteRuntimeDataProvider,
    normalize_runtime_data_capability,
)
from bithumb_bot.runtime_strategy_set import (
    RuntimeDecisionRequestBuilder,
    RuntimeStrategyDecisionCollector,
    RuntimeStrategySet,
    RuntimeStrategySpec,
    normalized_runtime_strategy_set_manifest,
)
from bithumb_bot.strategy_plugins.canary_non_sma import CanaryNonSmaRuntimeDecisionAdapter


def _conn(*, with_candles: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    if with_candles:
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


def _canary_spec() -> RuntimeStrategySpec:
    return RuntimeStrategySpec(
        "canary_non_sma",
        pair="KRW-BTC",
        interval="1m",
        parameters={
            "CANARY_ORDER_START_INDEX": 0,
            "CANARY_ORDER_SIDE": "BUY",
            "CANARY_ORDER_REASON": "provider_boundary",
        },
    )


def test_runtime_capability_normalization_aligns_research_aliases() -> None:
    assert normalize_runtime_data_capability("candles") == "candles"
    assert normalize_runtime_data_capability("top_of_book") == "orderbook_top"
    assert normalize_runtime_data_capability("l2_depth_snapshot") == "orderbook_depth"
    assert normalize_runtime_data_capability("trade_ticks") == "trades"
    assert normalize_runtime_data_capability("funding") == "funding"
    assert normalize_runtime_data_capability("open_interest") == "open_interest"


def test_runtime_data_provider_preflight_and_snapshot_are_deterministic() -> None:
    conn = _conn()
    try:
        strategy_set = RuntimeStrategySet(source="unit", strategies=(_canary_spec(),))
        resolver = RuntimeDataRequirementResolver()
        provider = SQLiteRuntimeDataProvider(conn, resolver=resolver)
        report = provider.preflight(strategy_set, through_ts_ms=1_700_000_180_000)
        request = RuntimeDecisionRequestBuilder().build_for_spec(
            _canary_spec(),
            through_ts_ms=1_700_000_180_000,
        )
        requirements = resolver.resolve_for_strategy_set(strategy_set)

        snapshot_a = provider.snapshot(request, requirements)
        snapshot_b = provider.snapshot(request, requirements)
    finally:
        conn.close()

    assert report.ok is True
    assert report.report_hash.startswith("sha256:")
    assert snapshot_a is not None
    assert snapshot_b is not None
    assert snapshot_a.feature_snapshot_hash == snapshot_b.feature_snapshot_hash
    assert snapshot_a.market_snapshot_hash == snapshot_b.market_snapshot_hash
    assert snapshot_a.feature_payload["candle_ts"] == 1_700_000_180_000
    assert snapshot_a.feature_payload["last_close"] == 13.0


def test_missing_required_candles_fail_closed_before_adapter_execution() -> None:
    conn = _conn(with_candles=False)
    calls: list[str] = []

    class _Adapter(CanaryNonSmaRuntimeDecisionAdapter):
        def decide_feature_snapshot(self, request, feature_snapshot):  # type: ignore[no-untyped-def]
            calls.append("called")
            return super().decide_feature_snapshot(request, feature_snapshot)

    try:
        with pytest.raises(RuntimeError, match="runtime_data_requirement_missing:candles"):
            RuntimeStrategyDecisionCollector(
                adapter_resolver=lambda _name: _Adapter(),
            ).collect(
                conn,
                RuntimeStrategySet(source="unit", strategies=(_canary_spec(),)),
                through_ts_ms=1_700_000_180_000,
            )
    finally:
        conn.close()

    assert calls == []


def test_required_orderbook_top_strategy_fails_closed_when_snapshots_absent() -> None:
    conn = _conn()
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(
            RuntimeStrategySpec(
                TEST_TOP_OF_BOOK_REQUIRED_STRATEGY,
                pair="KRW-BTC",
                interval="1m",
                parameters={},
            ),
        ),
    )
    try:
        report = SQLiteRuntimeDataProvider(conn).preflight(
            strategy_set,
            through_ts_ms=1_700_000_180_000,
        )
    finally:
        conn.close()

    assert report.ok is False
    assert "runtime_data_requirement_missing:orderbook_top" in report.reasons
    assert "orderbook_top" in report.as_dict()["capabilities_missing"]


def test_optional_orderbook_top_missing_is_warning_not_failure() -> None:
    conn = _conn()
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(RuntimeStrategySpec("sma_with_filter", pair="KRW-BTC", interval="1m"),),
    )
    try:
        report = SQLiteRuntimeDataProvider(conn).preflight(
            strategy_set,
            through_ts_ms=1_700_000_180_000,
        )
    finally:
        conn.close()

    assert report.ok is True
    assert "runtime_data_optional_missing:orderbook_top" in report.as_dict()["warnings"]


def test_canary_runtime_decision_uses_provider_snapshot_provenance() -> None:
    conn = _conn()
    try:
        bundle = RuntimeStrategyDecisionCollector().collect(
            conn,
            RuntimeStrategySet(source="unit", strategies=(_canary_spec(),)),
            through_ts_ms=1_700_000_180_000,
        )
    finally:
        conn.close()

    assert bundle is not None
    result = bundle.results[0]
    assert result.decision.final_signal == "BUY"
    assert result.base_context["feature_snapshot_hash"].startswith("sha256:")
    assert result.base_context["market_snapshot_hash"].startswith("sha256:")
    provenance = result.base_context["strategy_evaluation_provenance"]
    assert isinstance(provenance, dict)
    assert provenance["feature_snapshot_hash"] == result.base_context["feature_snapshot_hash"]
    assert result.replay_fingerprint["feature_snapshot_hash"] == result.base_context["feature_snapshot_hash"]
    assert bundle.data_availability_report is not None
    assert bundle.data_availability_report.report_hash.startswith("sha256:")


def test_runtime_strategy_set_manifest_includes_provider_evidence() -> None:
    conn = _conn()
    strategy_set = RuntimeStrategySet(source="unit", strategies=(_canary_spec(),))
    try:
        report = SQLiteRuntimeDataProvider(conn).preflight(
            strategy_set,
            through_ts_ms=1_700_000_180_000,
        )
    finally:
        conn.close()

    manifest = normalized_runtime_strategy_set_manifest(
        strategy_set=strategy_set,
        data_availability_report=report,
    )

    assert manifest["runtime_data_contract_hash"].startswith("sha256:")
    assert manifest["runtime_data_availability_report_hash"] == report.report_hash
    assert manifest["provider_contract_hash"].startswith("sha256:")
    assert manifest["runtime_data_db_schema_fingerprint"].startswith("sha256:")
    assert manifest["coverage_by_strategy"]


def test_strategy_plugin_and_runtime_adapter_modules_do_not_own_runtime_data_sql() -> None:
    roots = (Path("src/bithumb_bot/strategy_plugins"), Path("src/bithumb_bot/runtime_adapters"))
    forbidden_tables = {
        "candles",
        "orderbook_top_snapshots",
        "orderbook_depth_levels",
        "trades",
        "funding",
        "open_interest",
    }
    violations: list[str] = []
    for root in roots:
        for path in root.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if node.func.attr == "execute":
                        violations.append(f"{path}:{node.lineno}:execute")
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    upper = node.value.upper()
                    if any(token in upper for token in ("SELECT ", "INSERT ", "UPDATE ", "DELETE ")):
                        if any(table in node.value for table in forbidden_tables):
                            violations.append(f"{path}:{node.lineno}:runtime_sql")

    assert violations == []
