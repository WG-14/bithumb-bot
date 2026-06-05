from __future__ import annotations

import ast
import sqlite3
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_schema
from bithumb_bot.research.strategy_registry import TEST_TOP_OF_BOOK_REQUIRED_STRATEGY
from bithumb_bot.runtime_data_provider import (
    RuntimeStrategyDataRequirements,
    RuntimeDataRequirementResolver,
    SQLiteRuntimeDataProvider,
    normalize_runtime_data_capability,
)
from bithumb_bot.research.strategy_registry import DataCapabilityRequirement
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


def test_common_runtime_files_have_no_concrete_strategy_literals() -> None:
    common_files = (
        Path("src/bithumb_bot/strategy_decision_service.py"),
        Path("src/bithumb_bot/runtime_data_provider.py"),
        Path("src/bithumb_bot/runtime_strategy_decision.py"),
        Path("src/bithumb_bot/runtime_strategy_set.py"),
        Path("src/bithumb_bot/execution_service.py"),
        Path("src/bithumb_bot/run_loop_execution_planner.py"),
    )
    forbidden = ("sma_with_filter", "canary_non_sma")

    violations: list[str] = []
    for path in common_files:
        source = path.read_text(encoding="utf-8")
        for literal in forbidden:
            if literal in source:
                violations.append(f"{path}:{literal}")

    assert violations == []


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


def test_sma_runtime_data_requirements_are_plugin_declared() -> None:
    spec = RuntimeStrategySpec(
        "sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        parameters={
            "SMA_SHORT": 3,
            "SMA_LONG": 8,
            "SMA_FILTER_VOL_WINDOW": 5,
            "SMA_FILTER_OVEREXT_LOOKBACK": 2,
        },
    )
    requirements = RuntimeDataRequirementResolver().resolve_for_strategy_set(
        RuntimeStrategySet(source="unit", strategies=(spec,))
    )

    candles = next(item for item in requirements.required if item.name == "candles")
    assert candles.lookback_rows == 10
    assert candles.closed_candle_required is True
    assert candles.min_coverage_pct == 100.0


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


def test_required_orderbook_top_fails_when_stale_or_malformed() -> None:
    conn = _conn()
    requirements = RuntimeStrategyDataRequirements(
        required=(
            DataCapabilityRequirement("orderbook_top", max_age_ms=1_000),
        ),
        optional=(),
        per_strategy={},
    )
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO orderbook_top_snapshots
            (ts, pair, bid_price, ask_price, spread_bps, source, observed_at_epoch_sec)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1_700_000_000_000, "KRW-BTC", 99.0, 101.0, 200.0, "unit", 1.0),
        )
        stale = SQLiteRuntimeDataProvider(conn).availability_report_for_requirements(
            requirements,
            pair="KRW-BTC",
            interval="1m",
            through_ts_ms=1_700_000_180_000,
        )
        conn.execute("DELETE FROM orderbook_top_snapshots")
        conn.execute(
            """
            INSERT OR REPLACE INTO orderbook_top_snapshots
            (ts, pair, bid_price, ask_price, spread_bps, source, observed_at_epoch_sec)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1_700_000_180_000, "KRW-BTC", 0.0, 101.0, 200.0, "unit", 1.0),
        )
        malformed = SQLiteRuntimeDataProvider(conn).availability_report_for_requirements(
            requirements,
            pair="KRW-BTC",
            interval="1m",
            through_ts_ms=1_700_000_180_000,
        )
    finally:
        conn.close()

    assert "runtime_data_stale:orderbook_top" in stale.reasons
    assert "runtime_data_malformed:orderbook_top" in malformed.reasons


def test_required_orderbook_depth_fails_when_side_coverage_insufficient() -> None:
    conn = _conn()
    requirements = RuntimeStrategyDataRequirements(
        required=(DataCapabilityRequirement("orderbook_depth", min_rows=2, max_age_ms=60_000),),
        optional=(),
        per_strategy={},
    )
    try:
        conn.execute(
            """
            INSERT INTO orderbook_depth_levels(
                ts, pair, side, level_index, price, size,
                cumulative_size, cumulative_notional, source, observed_at_epoch_sec
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1_700_000_180_000, "KRW-BTC", "bid", 0, 99.0, 1.0, 1.0, 99.0, "unit", 1.0),
        )
        report = SQLiteRuntimeDataProvider(conn).availability_report_for_requirements(
            requirements,
            pair="KRW-BTC",
            interval="1m",
            through_ts_ms=1_700_000_180_000,
        )
    finally:
        conn.close()

    assert "runtime_data_depth_insufficient:orderbook_depth" in report.reasons


def test_required_trades_fails_low_density() -> None:
    conn = _conn()
    requirements = RuntimeStrategyDataRequirements(
        required=(
            DataCapabilityRequirement(
                "trades",
                min_rows=3,
                lookback_window_ms=180_000,
                min_density_pct=100.0,
            ),
        ),
        optional=(),
        per_strategy={},
    )
    try:
        conn.execute(
            """
            INSERT INTO trades(ts, pair, interval, side, price, qty, fee, cash_after, asset_after, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1_700_000_180_000, "KRW-BTC", "1m", "BUY", 100.0, 1.0, 0.0, 0.0, 1.0, "unit"),
        )
        report = SQLiteRuntimeDataProvider(conn).availability_report_for_requirements(
            requirements,
            pair="KRW-BTC",
            interval="1m",
            through_ts_ms=1_700_000_180_000,
        )
    finally:
        conn.close()

    assert "runtime_data_coverage_below_threshold:trades" in report.reasons


def test_required_funding_and_open_interest_use_capability_reason_codes() -> None:
    conn = _conn()
    requirements = RuntimeStrategyDataRequirements(
        required=(
            DataCapabilityRequirement("funding", max_age_ms=60_000),
            DataCapabilityRequirement("open_interest", max_age_ms=60_000),
        ),
        optional=(),
        per_strategy={},
    )
    try:
        report = SQLiteRuntimeDataProvider(conn).availability_report_for_requirements(
            requirements,
            pair="KRW-BTC",
            interval="1m",
            through_ts_ms=1_700_000_180_000,
        )
    finally:
        conn.close()

    assert "runtime_data_requirement_missing:funding" in report.reasons
    assert "runtime_data_requirement_missing:open_interest" in report.reasons


def test_sma_required_candle_lookback_insufficient_fails_with_stable_reason() -> None:
    conn = _conn()
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(
            RuntimeStrategySpec(
                "sma_with_filter",
                pair="KRW-BTC",
                interval="1m",
                parameters={
                    "SMA_LONG": 10,
                    "SMA_FILTER_VOL_WINDOW": 2,
                    "SMA_FILTER_OVEREXT_LOOKBACK": 1,
                },
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
    assert "runtime_data_lookback_insufficient:candles" in report.reasons
    coverage = report.as_dict()["coverage_by_capability"]["candles"]
    assert coverage["expected_count"] == 12
    assert coverage["coverage_pct"] < 100.0


def test_required_non_candle_capability_snapshot_materializes_evidence_payload() -> None:
    conn = _conn()
    conn.execute(
        """
        INSERT OR REPLACE INTO orderbook_top_snapshots
        (ts, pair, bid_price, ask_price, spread_bps, source, observed_at_epoch_sec)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1_700_000_180_000, "KRW-BTC", 99.0, 101.0, 200.0, "unit", 1_700_000_180.0),
    )
    conn.commit()
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
    resolver = RuntimeDataRequirementResolver()
    provider = SQLiteRuntimeDataProvider(conn, resolver=resolver)
    try:
        report = provider.preflight(strategy_set, through_ts_ms=1_700_000_180_000)
        request = SimpleNamespace(
            strategy_name=TEST_TOP_OF_BOOK_REQUIRED_STRATEGY,
            pair="KRW-BTC",
            interval="1m",
            through_ts_ms=1_700_000_180_000,
        )
        snapshot = provider.snapshot(
            request,
            resolver.resolve_for_strategy_set(strategy_set),
        )
    finally:
        conn.close()

    assert report.ok is True
    assert snapshot is not None
    orderbook = snapshot.feature_payload["capabilities"]["orderbook_top"]
    assert orderbook["selected_timestamp"] == 1_700_000_180_000
    assert orderbook["source_table_or_stream"] == "orderbook_top_snapshots"
    assert orderbook["payload_hash"].startswith("sha256:")
    assert orderbook["evidence_payload"]["bid_price"] == 99.0


def test_optional_orderbook_top_missing_is_warning_not_failure() -> None:
    conn = _conn()
    strategy_set = RuntimeStrategySet(
        source="unit",
        strategies=(
            RuntimeStrategySpec(
                "sma_with_filter",
                pair="KRW-BTC",
                interval="1m",
                parameters={
                    "SMA_LONG": 2,
                    "SMA_FILTER_VOL_WINDOW": 2,
                    "SMA_FILTER_OVEREXT_LOOKBACK": 1,
                },
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
    replay_metadata = bundle.as_dict()["results"][0]
    assert replay_metadata["feature_snapshot_hash"] == result.base_context["feature_snapshot_hash"]
    assert replay_metadata["runtime_data_availability_report_hash"] == (
        bundle.data_availability_report.report_hash
    )
    assert replay_metadata["provider_contract_hash"] == result.base_context["provider_contract_hash"]
    assert replay_metadata["source_schema_hash"] == result.base_context["source_schema_hash"]


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


def test_live_like_run_start_manifest_splits_cycle_preflight_evidence() -> None:
    strategy_set = RuntimeStrategySet(source="unit", strategies=(_canary_spec(),))

    manifest = normalized_runtime_strategy_set_manifest(
        strategy_set=strategy_set,
        settings_obj=replace(settings, LIVE_DRY_RUN=True),
        data_availability_report=None,
    )

    assert manifest["runtime_data_evidence_scope"] == "decision_cycle"
    assert manifest["runtime_data_availability_report_hash"] is None
    assert "runtime_data_preflight_not_evaluated" not in str(manifest)


def test_live_like_runtime_manifest_rejects_failed_preflight() -> None:
    conn = _conn(with_candles=False)
    strategy_set = RuntimeStrategySet(source="unit", strategies=(_canary_spec(),))
    try:
        report = SQLiteRuntimeDataProvider(conn).preflight(
            strategy_set,
            through_ts_ms=1_700_000_180_000,
        )
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="runtime_data_preflight_gate_failed:FAIL"):
        normalized_runtime_strategy_set_manifest(
            strategy_set=strategy_set,
            settings_obj=replace(settings, LIVE_DRY_RUN=True),
            data_availability_report=report,
        )


def test_strategy_plugin_and_runtime_adapter_modules_do_not_own_runtime_data_sql() -> None:
    roots = (
        Path("src/bithumb_bot/strategy_plugins"),
        Path("src/bithumb_bot/runtime_adapters"),
        Path("src/bithumb_bot"),
    )
    scoped_files = {
        Path("src/bithumb_bot/runtime_sma_snapshot_builder.py"),
        Path("src/bithumb_bot/runtime_sma_snapshot.py"),
        Path("src/bithumb_bot/runtime_strategy_decision.py"),
        Path("src/bithumb_bot/runtime_strategy_set.py"),
    }
    allowed = {
        Path("src/bithumb_bot/runtime_data_provider.py"),
        Path("src/bithumb_bot/runtime_data_provider_sma.py"),
        Path("src/bithumb_bot/runtime_data_access.py"),
        Path("src/bithumb_bot/db_core.py"),
    }
    forbidden_tables = {
        "candles",
        "orderbook_top_snapshots",
        "orderbook_depth_levels",
        "trades",
        "funding",
        "open_interest",
    }
    violations: list[str] = []
    seen: set[Path] = set()
    for root in roots:
        for path in root.rglob("*.py"):
            if path in seen:
                continue
            seen.add(path)
            if path not in scoped_files and not any(str(path).startswith(str(base)) for base in roots[:2]):
                continue
            if path in allowed:
                continue
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
