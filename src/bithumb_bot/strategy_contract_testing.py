from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from bithumb_bot.canonical_decision import export_runtime_replay_decisions
from bithumb_bot.db_core import ensure_schema
from bithumb_bot.research.dataset_snapshot import DatasetSnapshot
from bithumb_bot.research.strategy_registry import ResearchStrategyPlugin
from bithumb_bot.research.strategy_registry import strategy_runtime_capability_issues
from bithumb_bot.runtime_strategy_decision import get_runtime_decision_adapter
from bithumb_bot.runtime_strategy_decision import is_runtime_strategy_decision_result
from bithumb_bot.runtime_data_provider import RuntimeDataRequirementResolver, SQLiteRuntimeDataProvider
from bithumb_bot.runtime_strategy_set import RuntimeDecisionRequestBuilder
from bithumb_bot.runtime_strategy_set import RuntimeMarketScope
from bithumb_bot.runtime_strategy_set import RuntimeStrategySet
from bithumb_bot.runtime_strategy_set import RuntimeStrategySpec


def assert_research_only_contract(plugin: ResearchStrategyPlugin) -> None:
    payload = plugin.contract_payload()

    assert payload["authoring_contract_kind"] == "research_only"
    assert payload["authoring_level"] == "level_1_research_only"
    assert payload["capability_level"] == "research_only"
    assert payload["promotion_grade"] is False
    assert payload["promotion_extension_missing_reason"] == "promotion_extension_missing"
    assert payload["recommended_next_action"] == "promote_strategy_contract"
    assert plugin.runtime_replay_builder is None
    assert plugin.runtime_parameter_adapter is None
    assert plugin.runtime_decision_adapter_factory is None
    assert plugin.policy_assembly_factory is None
    assert plugin.runtime_capabilities.live_dry_run_allowed is False
    assert plugin.runtime_capabilities.live_real_order_allowed is False
    issues = strategy_runtime_capability_issues(
        plugin.name,
        live_dry_run=True,
        live_real_order_armed=True,
        require_promotion_runtime=True,
        require_runtime_replay=True,
        require_runtime_decision_adapter=True,
    )
    assert any(item.startswith(f"promotion_runtime_unsupported_for_strategy:{plugin.name}") for item in issues)
    assert any(item.startswith(f"runtime_replay_unsupported_for_strategy:{plugin.name}") for item in issues)
    assert any(item.startswith(f"live_dry_run_not_allowed_for_strategy:{plugin.name}") for item in issues)


def assert_replay_compatible_contract(
    plugin: ResearchStrategyPlugin,
    *,
    dataset: DatasetSnapshot,
    params: dict[str, Any],
    tmp_path: Path,
) -> None:
    payload = plugin.contract_payload()

    assert payload["authoring_contract_kind"] == "replay_compatible"
    assert payload["authoring_level"] == "level_2_replay_compatible"
    assert payload["capability_level"] == "replay_compatible"
    assert payload["promotion_grade"] is False
    assert plugin.runtime_replay_builder is not None
    assert plugin.runtime_decision_adapter_factory is None
    assert plugin.policy_assembly_factory is None
    assert plugin.runtime_parameter_adapter is None
    assert plugin.runtime_capabilities.replay_decisions_supported is True
    assert plugin.runtime_capabilities.approved_profile_required is False
    assert plugin.runtime_capabilities.live_dry_run_allowed is False
    assert plugin.runtime_capabilities.live_real_order_allowed is False
    plugin.spec.validate_parameters(params)

    db_path = tmp_path / f"{plugin.name}.sqlite"
    through_ts = _seed_replay_db(db_path, dataset)
    conn = sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        before_changes = conn.total_changes
        profile = {
            "market": dataset.market,
            "interval": dataset.interval,
            "strategy_parameters": dict(params),
        }
        strategy_a = plugin.runtime_replay_builder(profile, None)
        strategy_b = plugin.runtime_replay_builder(profile, None)
        decisions_a = export_runtime_replay_decisions(
            conn=conn,
            strategy=strategy_a,
            through_ts_list=[through_ts],
            market=dataset.market,
            interval=dataset.interval,
            dataset_content_hash=dataset.content_hash(),
            db_data_fingerprint="sha256:contract-db",
            strategy_version=plugin.version,
            strategy_decision_contract_version=plugin.decision_contract_version,
        )
        decisions_b = export_runtime_replay_decisions(
            conn=conn,
            strategy=strategy_b,
            through_ts_list=[through_ts],
            market=dataset.market,
            interval=dataset.interval,
            dataset_content_hash=dataset.content_hash(),
            db_data_fingerprint="sha256:contract-db",
            strategy_version=plugin.version,
            strategy_decision_contract_version=plugin.decision_contract_version,
        )
        after_changes = conn.total_changes
    finally:
        conn.close()

    assert after_changes == before_changes
    assert decisions_a == decisions_b
    assert decisions_a
    first = decisions_a[0]
    assert first["policy_contract_hash"].startswith("sha256:")
    assert first["policy_input_hash"].startswith("sha256:")
    assert first["policy_decision_hash"].startswith("sha256:")
    assert first["replay_fingerprint_hash"].startswith("sha256:")

    issues = strategy_runtime_capability_issues(
        plugin.name,
        live_dry_run=True,
        live_real_order_armed=True,
        approved_profile_path="",
        require_promotion_runtime=True,
        require_runtime_replay=True,
        require_runtime_decision_adapter=True,
    )
    assert any(item.startswith(f"promotion_runtime_unsupported_for_strategy:{plugin.name}") for item in issues)
    assert any(item.startswith(f"runtime_decision_adapter_unsupported_for_strategy:{plugin.name}") for item in issues)
    assert any(item.startswith(f"live_dry_run_not_allowed_for_strategy:{plugin.name}") for item in issues)
    assert any(item.startswith(f"live_real_order_not_allowed_for_strategy:{plugin.name}") for item in issues)
    assert not any(item.startswith(f"approved_profile_required_for_strategy:{plugin.name}") for item in issues)


def assert_live_eligible_contract(
    plugin: ResearchStrategyPlugin,
    *,
    tmp_path: Path,
    params: dict[str, Any],
    pair: str,
    interval: str,
) -> None:
    payload = plugin.contract_payload()
    assert payload["authoring_level"] == "level_3_promotion_grade"
    assert payload["legacy_authoring_level_alias"] == "level_3_live_eligible"
    assert payload["capability_level"] in {"live_eligible", "runtime_decision"}
    assert payload["operational_capability"]["live_dry_run_allowed"] is True
    assert payload["operator_verdict"]["targets"]["live_dry_run"]["allowed"] is True
    assert plugin.is_promotion_grade is True
    assert plugin.runtime_capabilities.promotion_runtime_decisions_supported is True
    assert plugin.runtime_decision_adapter_factory is not None
    assert plugin.policy_assembly_factory is not None
    assert plugin.runtime_capabilities.live_dry_run_allowed is True
    assert plugin.runtime_capabilities.approved_profile_required is True
    evidence_contract = payload["decision_evidence_contract"]
    assert evidence_contract["contract_hash"].startswith("sha256:")
    assert (
        evidence_contract["requires_decision_input_bundle"]
        or evidence_contract["required_promotion_provenance_fields"]
    )

    db_path = tmp_path / f"{plugin.name}.sqlite"
    through_ts = _seed_runtime_rows(db_path, pair=pair, interval=interval)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        request = RuntimeDecisionRequestBuilder().build_for_spec(
            RuntimeStrategySpec(
                strategy_name=plugin.name,
                pair=pair,
                interval=interval,
                parameters=dict(params),
                approved_profile_hash="sha256:" + "a" * 64,
                runtime_contract_hash="sha256:" + "b" * 64,
            ),
            through_ts_ms=through_ts,
        )
        adapter = get_runtime_decision_adapter(plugin.name)
        assert adapter is not None
        strategy_set = RuntimeStrategySet(
            strategies=(
                RuntimeStrategySpec(
                    strategy_name=plugin.name,
                    pair=pair,
                    interval=interval,
                    parameters=dict(params),
                    approved_profile_hash="sha256:" + "a" * 64,
                    runtime_contract_hash="sha256:" + "b" * 64,
                ),
            ),
            source="strategy_contract_testing",
            market_scope=RuntimeMarketScope(pair=pair, interval=interval),
        )
        resolver = RuntimeDataRequirementResolver()
        feature_snapshot = SQLiteRuntimeDataProvider(conn, resolver=resolver).snapshot(
            request,
            resolver.resolve_for_strategy_set(strategy_set),
        )
        assert feature_snapshot is not None
        result = adapter.decide_feature_snapshot(request, feature_snapshot)
        assert result is not None
        from bithumb_bot.runtime_strategy_decision import (
            _attach_runtime_feature_snapshot_metadata,
            _attach_runtime_request_metadata,
        )

        _attach_runtime_feature_snapshot_metadata(result, feature_snapshot)
        _attach_runtime_request_metadata(result, request)
    finally:
        conn.close()

    assert is_runtime_strategy_decision_result(result)
    assert result.decision.policy_contract_hash.startswith("sha256:")
    assert result.decision.policy_input_hash.startswith("sha256:")
    assert result.decision.policy_decision_hash.startswith("sha256:")
    assert result.base_context["runtime_decision_request_hash"].startswith("sha256:")
    assert result.base_context["feature_snapshot_hash"].startswith("sha256:")
    assert result.base_context["runtime_data_requirements_hash"].startswith("sha256:")
    assert result.replay_fingerprint["replay_fingerprint_hash"].startswith("sha256:")
    provenance = result.decision.trace.get("strategy_evaluation_provenance")
    assert isinstance(provenance, dict)
    assert provenance["decision_boundary"] == "StrategyDecisionService.evaluate"
    assert provenance["approved_profile_hash"].startswith("sha256:")
    assert provenance["runtime_contract_hash"].startswith("sha256:")
    for field in evidence_contract["required_promotion_provenance_fields"]:
        assert str(provenance.get(field) or "").strip(), field


def _seed_replay_db(path: Path, dataset: DatasetSnapshot) -> int:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_schema(conn)
        for candle in dataset.candles:
            conn.execute(
                """
                INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(candle.ts),
                    dataset.market,
                    dataset.interval,
                    float(candle.open),
                    float(candle.high),
                    float(candle.low),
                    float(candle.close),
                    float(candle.volume),
                ),
            )
        conn.commit()
        return int(dataset.candles[-1].ts)
    finally:
        conn.close()


def _seed_runtime_rows(path: Path, *, pair: str, interval: str) -> int:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        ensure_schema(conn)
        base_ts = 1_700_000_000_000
        for index in range(3):
            close = 100.0 + index
            conn.execute(
                """
                INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (base_ts + index * 60_000, pair, interval, close, close, close, close, 1.0),
            )
        conn.commit()
        return base_ts + 2 * 60_000
    finally:
        conn.close()


__all__ = [
    "assert_live_eligible_contract",
    "assert_replay_compatible_contract",
    "assert_research_only_contract",
]
