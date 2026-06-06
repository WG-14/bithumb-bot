from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot.runtime_data_provider import RuntimeFeatureSnapshot
from bithumb_bot.runtime_strategy_decision import _project_runtime_feature_snapshot
from bithumb_bot.runtime_strategy_set import ParameterAuthorityResolver, RuntimeStrategySpec


def test_production_runtime_modules_do_not_import_legacy_parameter_fallback_directly() -> None:
    allowed = {"src/bithumb_bot/runtime_strategy_set.py"}
    production_files = (
        "src/bithumb_bot/runtime_strategy_decision.py",
        "src/bithumb_bot/runtime_decision_service.py",
        "src/bithumb_bot/runtime_strategy_set.py",
        "src/bithumb_bot/runtime_adapter_bootstrap.py",
        "src/bithumb_bot/runtime_data_provider.py",
    )
    violations = [
        path
        for path in production_files
        if "legacy_compat.runtime_parameters" in Path(path).read_text(encoding="utf-8")
        and path not in allowed
    ]

    assert violations == []


def test_production_live_modules_do_not_import_research_compatibility_planning() -> None:
    production_files = (
        "src/bithumb_bot/execution_service.py",
        "src/bithumb_bot/submit_authority_policy.py",
        "src/bithumb_bot/broker/live.py",
        "src/bithumb_bot/run_loop_execution_planner.py",
    )
    violations = [
        path
        for path in production_files
        if "research.execution_planning" in Path(path).read_text(encoding="utf-8")
        or "_research_execution_submit_plan" in Path(path).read_text(encoding="utf-8")
        or "strategy_parameters_json_fallback" in Path(path).read_text(encoding="utf-8")
        or "settings_derived_fallback" in Path(path).read_text(encoding="utf-8")
    ]

    assert violations == []


def test_legacy_parameter_fallback_module_is_explicitly_paper_compatibility() -> None:
    source = Path("src/bithumb_bot/legacy_compat/runtime_parameters.py").read_text(encoding="utf-8")

    assert "PAPER_LEGACY_PARAMETER_SOURCE" in source
    assert "paper_legacy_compat" in source
    assert "STRATEGY_PARAMETERS_JSON" in source
    assert "runtime_parameter_adapter.from_settings" in source


def _settings_with_strategy_parameters_json(raw_json: str) -> SimpleNamespace:
    return SimpleNamespace(
        MODE="paper",
        LIVE_DRY_RUN=True,
        LIVE_REAL_ORDER_ARMED=False,
        STRATEGY_PARAMETERS_JSON=raw_json,
        APPROVED_STRATEGY_PROFILE_PATH="",
        STRATEGY_APPROVED_PROFILE_PATH="",
    )


@pytest.mark.parametrize(
    "authority_scope",
    ["promotion", "runtime_replay", "live_dry_run", "live_real_order"],
)
def test_non_paper_authority_scopes_reject_strategy_parameters_json_fallback(
    authority_scope: str,
) -> None:
    resolver = ParameterAuthorityResolver(
        settings_obj=_settings_with_strategy_parameters_json(
            '{"CANARY_ORDER_START_INDEX":0,"CANARY_ORDER_SIDE":"BUY","CANARY_ORDER_REASON":"unit"}'
        ),
        authority_scope=authority_scope,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="strict_runtime_rejects_strategy_parameters_json_fallback"):
        resolver.resolve(
            RuntimeStrategySpec("canary_non_sma", parameters=None),
            profile=None,
            approved_profile_path=None,
            approved_profile_hash=None,
        )


def test_paper_scope_allows_and_audits_strategy_parameters_json_fallback() -> None:
    resolver = ParameterAuthorityResolver(
        settings_obj=_settings_with_strategy_parameters_json(
            '{"CANARY_ORDER_START_INDEX":0,"CANARY_ORDER_SIDE":"BUY","CANARY_ORDER_REASON":"unit"}'
        ),
        authority_scope="paper_legacy",
    )

    authority = resolver.resolve(
        RuntimeStrategySpec("canary_non_sma", parameters=None),
        profile=None,
        approved_profile_path=None,
        approved_profile_hash=None,
    )

    assert authority.parameter_source == "paper_legacy_compat"
    assert authority.legacy_compatibility_used is True
    assert dict(authority.source_audit_metadata)["authority_scope"] == "paper_legacy"
    assert dict(authority.source_audit_metadata)["legacy_fallback"] == "STRATEGY_PARAMETERS_JSON"


def test_db_bound_projector_signature_is_rejected_before_projection() -> None:
    calls: list[str] = []

    class _DbBoundProjector:
        def project_feature_snapshot(self, conn, request, feature_snapshot):  # type: ignore[no-untyped-def]
            calls.append("called")
            return feature_snapshot

    with pytest.raises(RuntimeError, match="promotion_runtime_adapter_db_bound_projector_forbidden:unit"):
        _project_runtime_feature_snapshot(
            adapter=_DbBoundProjector(),
            request=SimpleNamespace(strategy_name="unit"),
            feature_snapshot=RuntimeFeatureSnapshot({"feature_payload": {}, "feature_snapshot_hash": "sha256:x"}),
        )

    assert calls == []


def test_builtin_sma_adapter_no_longer_exposes_db_bound_projector() -> None:
    from bithumb_bot.runtime_adapters.sma_with_filter import SmaWithFilterRuntimeDecisionAdapter

    snapshot = RuntimeFeatureSnapshot({"feature_payload": {}, "feature_snapshot_hash": "sha256:x"})
    projected = _project_runtime_feature_snapshot(
        adapter=SmaWithFilterRuntimeDecisionAdapter(),
        request=SimpleNamespace(strategy_name="sma_with_filter"),
        feature_snapshot=snapshot,
    )

    assert projected is snapshot
