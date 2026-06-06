from __future__ import annotations

import ast
from importlib import import_module
import json
import sqlite3
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from bithumb_bot import profile_cli, runtime_adapter_bootstrap, runtime_strategy_decision
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.research.strategy_registry import (
    ResearchStrategyPlugin,
    ResearchStrategyRegistryError,
    RuntimeParameterAdapter,
    StrategyRuntimeCapabilities,
    list_research_strategy_plugins,
    reload_research_strategy_plugins_for_tests,
    resolve_research_strategy_plugin,
    strategy_runtime_capability_issues,
)
from bithumb_bot.research.strategy_spec import StrategySpec
from bithumb_bot.strategy_evidence_contract import DecisionEvidenceContract
from bithumb_bot.strategy_plugin_inventory import build_strategy_plugin_inventory
from bithumb_bot.strategy_plugins.builtin_manifest import (
    BuiltinStrategyPluginExport,
    iter_builtin_strategy_plugin_exports,
)


DYNAMIC_PLUGIN_NAME = "dynamic_entrypoint_unit"
BUILTIN_PLUGIN_EXPORT_ALLOWLIST: dict[str, str] = {}


@dataclass(frozen=True)
class _FakeEntryPoint:
    name: str
    value: str
    plugin: ResearchStrategyPlugin

    def load(self) -> object:
        return self.plugin


@dataclass(frozen=True)
class _DynamicRuntimeDecisionAdapter:
    strategy_name: str = DYNAMIC_PLUGIN_NAME

    def decide_feature_snapshot(
        self,
        request: Any,
        feature_snapshot: Any,
    ) -> None:
        del request, feature_snapshot
        return None

    def typed_authority_required(self) -> bool:
        return True


@dataclass(frozen=True)
class _DynamicRuntimeReplayStrategy:
    name: str = DYNAMIC_PLUGIN_NAME

    def decide_runtime_snapshot(
        self,
        conn: Any,
        *,
        through_ts_ms: int | None = None,
    ) -> None:
        del conn, through_ts_ms
        return None


@dataclass(frozen=True)
class _DynamicPolicyAssembly:
    strategy_name: str = DYNAMIC_PLUGIN_NAME
    decision_contract_version: str = "dynamic_entrypoint_unit.decision.v1"

    def materialize_parameters(self, raw: dict[str, Any]) -> dict[str, Any]:
        if raw:
            raise ValueError("dynamic_entrypoint_unit_parameters_unsupported")
        return {}


def _dynamic_runner(*args: Any, **kwargs: Any) -> Any:
    del args, kwargs
    raise AssertionError("dynamic discovery runner should not execute in these tests")


def _dynamic_runtime_replay_builder(
    profile: dict[str, Any],
    candidate_regime_policy: dict[str, Any] | None = None,
) -> _DynamicRuntimeReplayStrategy:
    del profile, candidate_regime_policy
    return _DynamicRuntimeReplayStrategy()


def _dynamic_parameters_from_env(_env: dict[str, str]) -> dict[str, Any]:
    return {}


def _dynamic_parameters_from_settings(_cfg: object) -> dict[str, Any]:
    return {}


def _dynamic_runtime_adapter_factory() -> _DynamicRuntimeDecisionAdapter:
    return _DynamicRuntimeDecisionAdapter()


def _dynamic_policy_assembly_factory() -> _DynamicPolicyAssembly:
    return _DynamicPolicyAssembly()


def _dynamic_plugin(
    name: str = DYNAMIC_PLUGIN_NAME,
    *,
    runtime_supported: bool = True,
) -> ResearchStrategyPlugin:
    spec = StrategySpec(
        strategy_name=name,
        strategy_version="dynamic_entrypoint_unit.contract.v1",
        accepted_parameter_names=(),
        required_parameter_names=(),
        behavior_affecting_parameter_names=(),
        metadata_only_parameter_names=(),
        research_only_parameter_names=(),
        default_parameters={},
        decision_contract_version="dynamic_entrypoint_unit.decision.v1",
        required_data=("candles",),
        optional_data=(),
        exit_policy_schema={"schema_version": 1, "rules": ()},
    )
    return ResearchStrategyPlugin(
        name=name,
        version=spec.strategy_version,
        spec=spec,
        required_data=spec.required_data,
        optional_data=spec.optional_data,
        runner=_dynamic_runner,
        research_event_builder=lambda **_: (),
        runtime_replay_builder=_dynamic_runtime_replay_builder if runtime_supported else None,
        runtime_parameter_adapter=(
            RuntimeParameterAdapter(
                from_env=_dynamic_parameters_from_env,
                from_settings=_dynamic_parameters_from_settings,
                env_keys=(),
            )
            if runtime_supported
            else None
        ),
        decision_contract_version=spec.decision_contract_version,
        diagnostics_namespace=name,
        runtime_decision_adapter_factory=_dynamic_runtime_adapter_factory if runtime_supported else None,
        policy_assembly_factory=_dynamic_policy_assembly_factory if runtime_supported else None,
        runtime_capabilities=StrategyRuntimeCapabilities(
            promotion_runtime_decisions_supported=runtime_supported,
            runtime_replay_supported=runtime_supported,
            research_only=not runtime_supported,
            baseline_only=False,
            live_dry_run_allowed=runtime_supported,
            live_real_order_allowed=False,
            approved_profile_required=runtime_supported,
            fail_closed_reason=(
                "dynamic_plugin_runtime_unsupported"
                if not runtime_supported
                else "dynamic_plugin_capability_missing"
            ),
        ),
        decision_evidence_contract=DecisionEvidenceContract(
            required_promotion_provenance_fields=("policy_input_hash",),
        ),
    )


def _normalize_plugin(plugin: object) -> ResearchStrategyPlugin:
    if isinstance(plugin, ResearchStrategyPlugin):
        return plugin
    adapter = getattr(plugin, "to_research_strategy_plugin", None)
    if callable(adapter):
        normalized = adapter()
        if isinstance(normalized, ResearchStrategyPlugin):
            return normalized
    raise TypeError(f"test_expected_research_strategy_plugin:{type(plugin).__name__}")


def _load_builtin_export(plugin_export: BuiltinStrategyPluginExport) -> object:
    module = import_module(plugin_export.module)
    return getattr(module, plugin_export.object_name)


def _builtin_export_object_paths() -> set[str]:
    return {plugin_export.object_path for plugin_export in iter_builtin_strategy_plugin_exports()}


def _iter_public_plugin_export_paths() -> set[str]:
    root = Path("src/bithumb_bot/strategy_plugins")
    export_paths: set[str] = set()
    for path in sorted(root.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=path.as_posix())
        module = f"bithumb_bot.strategy_plugins.{path.stem}"
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, ast.AnnAssign):
                targets = (node.target,)
            else:
                continue
            for target in targets:
                if isinstance(target, ast.Name) and _is_public_plugin_export_name(target.id):
                    export_paths.add(f"{module}:{target.id}")
    return export_paths


def _is_public_plugin_export_name(name: str) -> bool:
    if name.startswith("_"):
        return False
    return name in {"STRATEGY_PLUGIN", "STRATEGY_PLUGINS"} or name.endswith("_PLUGIN")


def _dynamic_real_order_plugin_with_incomplete_contract() -> ResearchStrategyPlugin:
    spec = StrategySpec(
        strategy_name="dynamic_real_order_unit",
        strategy_version="dynamic_real_order_unit.contract.v1",
        accepted_parameter_names=(),
        required_parameter_names=(),
        behavior_affecting_parameter_names=(),
        metadata_only_parameter_names=(),
        research_only_parameter_names=(),
        default_parameters={},
        decision_contract_version="dynamic_real_order_unit.decision.v1",
        required_data=("candles",),
        optional_data=(),
        exit_policy_schema={"schema_version": 1, "rules": ()},
    )
    return ResearchStrategyPlugin(
        name=spec.strategy_name,
        version=spec.strategy_version,
        spec=spec,
        required_data=spec.required_data,
        optional_data=spec.optional_data,
        runner=_dynamic_runner,
        research_event_builder=lambda **_: (),
        runtime_replay_builder=_dynamic_runtime_replay_builder,
        runtime_parameter_adapter=RuntimeParameterAdapter(
            from_env=_dynamic_parameters_from_env,
            from_settings=_dynamic_parameters_from_settings,
            env_keys=(),
        ),
        decision_contract_version=spec.decision_contract_version,
        diagnostics_namespace=spec.strategy_name,
        runtime_decision_adapter_factory=_dynamic_runtime_adapter_factory,
        policy_assembly_factory=_dynamic_policy_assembly_factory,
        runtime_capabilities=StrategyRuntimeCapabilities(
            promotion_runtime_decisions_supported=True,
            runtime_replay_supported=True,
            research_only=False,
            baseline_only=False,
            live_dry_run_allowed=True,
            live_real_order_allowed=True,
            approved_profile_required=True,
            fail_closed_reason="dynamic_plugin_capability_missing",
        ),
        decision_evidence_contract=DecisionEvidenceContract(
            required_promotion_provenance_fields=("policy_input_hash",),
        ),
    )


@pytest.fixture(autouse=True)
def _restore_plugin_and_runtime_registries(monkeypatch: pytest.MonkeyPatch) -> None:
    yield
    monkeypatch.undo()
    from bithumb_bot.strategy_plugins import iter_builtin_strategy_plugins

    reload_research_strategy_plugins_for_tests(providers=(iter_builtin_strategy_plugins,))
    runtime_adapter_bootstrap.reset_runtime_decision_adapter_bootstrap_for_tests()


def test_builtin_manifest_exports_are_discoverable_and_hash_stable() -> None:
    import bithumb_bot.strategy_plugins as strategy_plugins

    reload_research_strategy_plugins_for_tests(providers=(strategy_plugins.iter_builtin_strategy_plugins,))

    listed = {plugin.name: plugin for plugin in list_research_strategy_plugins()}
    assert listed

    for plugin_export in iter_builtin_strategy_plugin_exports():
        manifest_plugin = _normalize_plugin(_load_builtin_export(plugin_export))
        listed_plugin = listed[manifest_plugin.name]
        resolved = resolve_research_strategy_plugin(manifest_plugin.name)

        assert listed_plugin.name == manifest_plugin.name
        assert resolved.name == manifest_plugin.name
        assert resolved.contract_hash() == manifest_plugin.contract_hash()
        assert resolved.contract_hash() == sha256_prefixed(resolved.contract_payload())


def test_strategy_plugin_inventory_is_read_only_deterministic_and_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import bithumb_bot.db_core as db_core
    import bithumb_bot.strategy_plugins as strategy_plugins

    def _db_forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("strategy plugin inventory must not open the trading DB")

    monkeypatch.setattr(sqlite3, "connect", _db_forbidden)
    monkeypatch.setattr(db_core, "ensure_db", _db_forbidden)
    reload_research_strategy_plugins_for_tests(providers=(strategy_plugins.iter_builtin_strategy_plugins,))

    inventory = build_strategy_plugin_inventory()
    second_inventory = build_strategy_plugin_inventory()
    listed = {plugin.name: plugin for plugin in list_research_strategy_plugins()}
    manifest_names = {
        _normalize_plugin(_load_builtin_export(plugin_export)).name
        for plugin_export in iter_builtin_strategy_plugin_exports()
    }

    assert inventory == second_inventory
    assert inventory["schema_version"] == 1
    assert inventory["strategy_count"] == len(inventory["strategies"])
    assert [entry["name"] for entry in inventory["strategies"]] == sorted(
        entry["name"] for entry in inventory["strategies"]
    )
    assert manifest_names <= {entry["name"] for entry in inventory["strategies"]}

    required_keys = {
        "name",
        "version",
        "source",
        "manifest_object_path",
        "authoring_contract_kind",
        "authoring_level",
        "capability_level",
        "contract_hash",
        "strategy_spec_hash",
        "runtime_capabilities",
        "live_eligibility",
        "fail_closed_reason",
        "decision_evidence_contract",
        "required_data",
        "optional_data",
    }
    for entry in inventory["strategies"]:
        plugin = listed[entry["name"]]
        assert required_keys <= set(entry)
        assert entry["source"] == "built_in_manifest"
        assert entry["manifest_object_path"] in _builtin_export_object_paths()
        assert entry["contract_hash"] == plugin.contract_hash()
        assert entry["strategy_spec_hash"] == plugin.spec.spec_hash()
        assert entry["decision_evidence_contract"]["contract_hash"] == (
            plugin.decision_evidence_contract.contract_hash()
        )
        if not entry["live_eligibility"]["dry_run_allowed"] or not entry["live_eligibility"]["real_order_allowed"]:
            assert entry["fail_closed_reason"]
            assert entry["fail_closed_reason"] == plugin.runtime_capabilities.fail_closed_reason

    by_name = {entry["name"]: entry for entry in inventory["strategies"]}
    assert by_name["threshold_research_only"]["authoring_level"] == "level_1_research_only"
    assert by_name["threshold_research_only"]["capability_level"] == "research_only"
    assert by_name["replay_threshold"]["authoring_level"] == "level_2_replay_compatible"
    assert by_name["replay_threshold"]["capability_level"] == "replay_compatible"
    assert by_name["canary_non_sma"]["authoring_level"] == "level_3_live_eligible"
    assert by_name["canary_non_sma"]["capability_level"] == "live_eligible"


def test_strategy_plugin_inventory_cli_is_read_only_json_surface() -> None:
    from types import SimpleNamespace

    from bithumb_bot.cli.context import AppContext
    from bithumb_bot.cli.main import main
    from bithumb_bot.cli.registry import command_registry

    output: list[str] = []
    spec = command_registry()["strategy-plugin-inventory"]

    assert spec.read_only is True
    assert spec.mutating is False
    assert spec.writes_db is False
    assert spec.uses_broker is False
    assert spec.produces_artifact is False
    assert spec.json_output_supported is True

    rc = main(
        ["strategy-plugin-inventory", "--json"],
        context=AppContext(settings=SimpleNamespace(MODE="paper"), printer=output.append),
    )
    payload = json.loads(output[0])

    assert rc == 0
    assert payload == build_strategy_plugin_inventory()


def test_public_builtin_plugin_exports_must_be_registered_in_manifest() -> None:
    public_exports = _iter_public_plugin_export_paths()
    manifest_exports = _builtin_export_object_paths()
    allowlisted_exports = set(BUILTIN_PLUGIN_EXPORT_ALLOWLIST)

    undocumented_allowlist = [
        export_path
        for export_path, reason in BUILTIN_PLUGIN_EXPORT_ALLOWLIST.items()
        if not str(reason).strip()
    ]
    assert undocumented_allowlist == []
    assert public_exports - manifest_exports - allowlisted_exports == set()
    assert manifest_exports <= public_exports


def test_builtin_manifest_iterable_strategy_plugins_are_expanded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import bithumb_bot.strategy_plugins as strategy_plugins
    import bithumb_bot.strategy_plugins.builtin_manifest as builtin_manifest

    module = types.ModuleType("tests.dynamic_builtin_strategy_plugins")
    first = _dynamic_plugin(name="dynamic_builtin_iterable_a")
    second = _dynamic_plugin(name="dynamic_builtin_iterable_b", runtime_supported=False)
    module.STRATEGY_PLUGINS = (first, second)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(
        builtin_manifest,
        "BUILTIN_STRATEGY_PLUGIN_EXPORTS",
        (BuiltinStrategyPluginExport(module.__name__, "STRATEGY_PLUGINS"),),
    )

    reload_research_strategy_plugins_for_tests(providers=(strategy_plugins.iter_builtin_strategy_plugins,))

    listed = {plugin.name for plugin in list_research_strategy_plugins()}
    assert listed == {"dynamic_builtin_iterable_a", "dynamic_builtin_iterable_b"}
    assert resolve_research_strategy_plugin("dynamic_builtin_iterable_a") is first
    assert resolve_research_strategy_plugin("dynamic_builtin_iterable_b") is second


def test_builtin_manifest_callable_authoring_export_is_expanded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import bithumb_bot.strategy_plugins as strategy_plugins
    import bithumb_bot.strategy_plugins.builtin_manifest as builtin_manifest

    module = types.ModuleType("tests.dynamic_builtin_strategy_plugin_callable")
    plugin = _dynamic_plugin(name="dynamic_builtin_callable")

    def _strategy_plugins() -> tuple[ResearchStrategyPlugin, ...]:
        return (plugin,)

    module.STRATEGY_PLUGINS = _strategy_plugins
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(
        builtin_manifest,
        "BUILTIN_STRATEGY_PLUGIN_EXPORTS",
        (BuiltinStrategyPluginExport(module.__name__, "STRATEGY_PLUGINS"),),
    )

    reload_research_strategy_plugins_for_tests(providers=(strategy_plugins.iter_builtin_strategy_plugins,))

    assert resolve_research_strategy_plugin("dynamic_builtin_callable") is plugin


def test_builtin_manifest_runtime_capability_contracts_are_fail_closed() -> None:
    import bithumb_bot.strategy_plugins as strategy_plugins

    reload_research_strategy_plugins_for_tests(providers=(strategy_plugins.iter_builtin_strategy_plugins,))
    runtime_adapter_bootstrap.reset_runtime_decision_adapter_bootstrap_for_tests()

    for plugin_export in iter_builtin_strategy_plugin_exports():
        plugin = resolve_research_strategy_plugin(_normalize_plugin(_load_builtin_export(plugin_export)).name)
        capabilities = plugin.runtime_capabilities

        if capabilities.promotion_runtime_decisions_supported:
            adapter = runtime_strategy_decision.get_runtime_decision_adapter(plugin.name)
            assert adapter is not None
            assert getattr(adapter, "strategy_name") == plugin.name
        else:
            assert runtime_strategy_decision.get_runtime_decision_adapter(plugin.name) is None

        if capabilities.research_only or plugin.authoring_contract_kind in {
            "research_only",
            "replay_compatible",
        }:
            assert capabilities.live_dry_run_allowed is False
            assert capabilities.live_real_order_allowed is False
            issues = strategy_runtime_capability_issues(
                plugin.name,
                live_dry_run=True,
                live_real_order_armed=True,
                approved_profile_path="",
            )
            assert any(issue.startswith(f"live_dry_run_not_allowed_for_strategy:{plugin.name}") for issue in issues)
            assert any(issue.startswith(f"live_real_order_not_allowed_for_strategy:{plugin.name}") for issue in issues)


def test_entry_point_strategy_plugin_is_discovered(monkeypatch: pytest.MonkeyPatch) -> None:
    import bithumb_bot.strategy_plugins as strategy_plugins

    plugin = _dynamic_plugin()
    monkeypatch.setattr(
        strategy_plugins.metadata,
        "entry_points",
        lambda: [_FakeEntryPoint("unit_dynamic", "tests:plugin", plugin)],
    )

    reload_research_strategy_plugins_for_tests()

    assert DYNAMIC_PLUGIN_NAME in {item.name for item in list_research_strategy_plugins()}
    assert resolve_research_strategy_plugin(DYNAMIC_PLUGIN_NAME) is plugin
    payload = plugin.contract_payload()
    assert payload["runtime_capabilities"] == {
        "schema_version": 1,
        "research_supported": True,
        "replay_decisions_supported": True,
        "promotion_export_supported": True,
        "runtime_decision_supported": True,
        "promotion_runtime_decisions_supported": True,
        "runtime_replay_supported": True,
        "research_only": False,
        "baseline_only": False,
        "live_dry_run_allowed": True,
        "live_real_order_allowed": False,
        "approved_profile_required": True,
        "fail_closed_reason": "dynamic_plugin_capability_missing",
    }
    assert payload["live_eligibility"] == {
        "dry_run_allowed": True,
        "real_order_allowed": False,
        "approved_profile_required": True,
        "fail_closed_reason": "dynamic_plugin_capability_missing",
    }
    assert payload["decision_evidence_contract"]["required_promotion_provenance_fields"] == [
        "policy_input_hash"
    ]
    assert payload["decision_evidence_contract"]["required_live_real_order_fields"] == []
    assert payload["decision_evidence_contract"]["required_live_real_order_one_of_field_groups"] == []


def test_dynamic_plugin_incomplete_contract_is_valid_only_when_real_orders_not_claimed() -> None:
    plugin = _dynamic_plugin()

    assert plugin.runtime_capabilities.promotion_runtime_decisions_supported is True
    assert plugin.runtime_capabilities.runtime_replay_supported is True
    assert plugin.runtime_decision_adapter_factory is not None
    assert plugin.policy_assembly_factory is not None
    assert plugin.runtime_capabilities.live_dry_run_allowed is True
    assert plugin.runtime_capabilities.live_real_order_allowed is False
    assert plugin.decision_evidence_contract.required_promotion_provenance_fields == (
        "policy_input_hash",
    )

    with pytest.raises(
        ValueError,
        match="strategy_live_real_order_decision_evidence_contract_incomplete:dynamic_real_order_unit",
    ):
        _dynamic_real_order_plugin_with_incomplete_contract()


def test_discovered_plugin_runtime_adapter_is_bootstrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import bithumb_bot.strategy_plugins as strategy_plugins

    plugin = _dynamic_plugin()
    monkeypatch.setattr(
        strategy_plugins.metadata,
        "entry_points",
        lambda: [_FakeEntryPoint("unit_dynamic", "tests:plugin", plugin)],
    )
    reload_research_strategy_plugins_for_tests()
    runtime_adapter_bootstrap.reset_runtime_decision_adapter_bootstrap_for_tests()

    runtime_adapter_bootstrap.ensure_runtime_decision_adapters_registered()

    adapter = runtime_strategy_decision.get_runtime_decision_adapter(DYNAMIC_PLUGIN_NAME)
    assert isinstance(adapter, _DynamicRuntimeDecisionAdapter)
    assert adapter.typed_authority_required() is True


def test_plugin_adapter_name_mismatch_fails_closed() -> None:
    plugin = _dynamic_plugin(name="dynamic_mismatch_unit")
    reload_research_strategy_plugins_for_tests(providers=(lambda: (plugin,),))

    with pytest.raises(RuntimeError, match="runtime_decision_adapter_name_mismatch:dynamic_mismatch_unit"):
        runtime_strategy_decision.get_runtime_decision_adapter("dynamic_mismatch_unit")


def test_runtime_capabilities_must_be_explicit() -> None:
    spec = StrategySpec(
        strategy_name="missing_capabilities_unit",
        strategy_version="missing_capabilities_unit.contract.v1",
        accepted_parameter_names=(),
        required_parameter_names=(),
        behavior_affecting_parameter_names=(),
        metadata_only_parameter_names=(),
        research_only_parameter_names=(),
        default_parameters={},
        decision_contract_version="missing_capabilities_unit.decision.v1",
        required_data=("candles",),
        optional_data=(),
        exit_policy_schema={"schema_version": 1, "rules": ()},
    )

    with pytest.raises(ValueError, match="strategy runtime capabilities must be explicit"):
        ResearchStrategyPlugin(
            name=spec.strategy_name,
            version=spec.strategy_version,
            spec=spec,
            required_data=spec.required_data,
            optional_data=spec.optional_data,
            runner=_dynamic_runner,
            runtime_replay_builder=_dynamic_runtime_replay_builder,
            runtime_parameter_adapter=RuntimeParameterAdapter(
                from_env=_dynamic_parameters_from_env,
                from_settings=_dynamic_parameters_from_settings,
                env_keys=(),
            ),
            decision_contract_version=spec.decision_contract_version,
            diagnostics_namespace=spec.strategy_name,
            runtime_decision_adapter_factory=_dynamic_runtime_adapter_factory,
        )


def test_dynamic_research_only_plugin_is_valid_research_but_live_fails_by_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import bithumb_bot.strategy_plugins as strategy_plugins
    from bithumb_bot.config import LiveModeValidationError, settings, validate_live_strategy_selection
    from dataclasses import replace

    plugin = _dynamic_plugin(name="dynamic_research_only_unit", runtime_supported=False)
    monkeypatch.setattr(
        strategy_plugins.metadata,
        "entry_points",
        lambda: [_FakeEntryPoint("unit_dynamic_research_only", "tests:plugin", plugin)],
    )
    reload_research_strategy_plugins_for_tests()
    runtime_adapter_bootstrap.reset_runtime_decision_adapter_bootstrap_for_tests()

    resolved = resolve_research_strategy_plugin("dynamic_research_only_unit")
    assert resolved.runtime_capabilities.research_only is True
    assert resolved.runtime_capabilities.promotion_runtime_decisions_supported is False

    with pytest.raises(LiveModeValidationError) as exc:
        validate_live_strategy_selection(
            replace(
                settings,
                MODE="live",
                STRATEGY_NAME="dynamic_research_only_unit",
                LIVE_DRY_RUN=True,
                LIVE_REAL_ORDER_ARMED=False,
            )
        )

    message = str(exc.value)
    assert "live_strategy_capability_validation_failed" in message
    assert "promotion_runtime_unsupported_for_strategy:dynamic_research_only_unit" in message
    assert "dynamic_plugin_runtime_unsupported" in message
    assert runtime_strategy_decision.get_runtime_decision_adapter("dynamic_research_only_unit") is None


def test_generic_runtime_files_do_not_branch_on_dynamic_plugin_name() -> None:
    for path in (
        Path("src/bithumb_bot/engine.py"),
        Path("src/bithumb_bot/profile_cli.py"),
        Path("src/bithumb_bot/runtime_strategy_decision.py"),
        Path("src/bithumb_bot/runtime_adapter_bootstrap.py"),
    ):
        assert DYNAMIC_PLUGIN_NAME not in path.read_text(encoding="utf-8")


def test_duplicate_discovered_plugin_names_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import bithumb_bot.strategy_plugins as strategy_plugins

    first = _dynamic_plugin()
    duplicate = _dynamic_plugin()
    monkeypatch.setattr(
        strategy_plugins.metadata,
        "entry_points",
        lambda: [
            _FakeEntryPoint("unit_dynamic_a", "tests:a", first),
            _FakeEntryPoint("unit_dynamic_b", "tests:b", duplicate),
        ],
    )

    with pytest.raises(ResearchStrategyRegistryError, match="duplicate research strategy plugin name"):
        reload_research_strategy_plugins_for_tests()


def test_discovered_plugin_contract_hash_is_stable_and_exported(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import bithumb_bot.strategy_plugins as strategy_plugins

    plugin = _dynamic_plugin()
    monkeypatch.setattr(
        strategy_plugins.metadata,
        "entry_points",
        lambda: [_FakeEntryPoint("unit_dynamic", "tests:plugin", plugin)],
    )
    reload_research_strategy_plugins_for_tests()

    assert plugin.contract_hash() == plugin.contract_hash()
    assert plugin.contract_hash() == sha256_prefixed(plugin.contract_payload())

    db_path = tmp_path / "paper.sqlite"
    sqlite3.connect(db_path).close()
    profile_path = tmp_path / "profile.json"
    through_ts_path = tmp_path / "through_ts.json"
    out_path = tmp_path / "runtime_replay.json"
    profile_path.write_text("{}", encoding="utf-8")
    through_ts_path.write_text(json.dumps({"through_ts_list": []}), encoding="utf-8")
    monkeypatch.setattr(
        profile_cli,
        "load_approved_profile",
        lambda _path: {
            "strategy_name": DYNAMIC_PLUGIN_NAME,
            "profile_content_hash": "sha256:profile",
            "dataset_content_hash": "sha256:dataset",
            "market": "KRW-BTC",
            "interval": "1m",
        },
    )

    rc = profile_cli.cmd_runtime_replay_decisions(
        profile_path=str(profile_path),
        db_path=str(db_path),
        through_ts_list_path=str(through_ts_path),
        out_path=str(out_path),
    )

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert rc == 0
    assert payload["strategy_plugin_contract"] == plugin.contract_payload()
    assert payload["strategy_plugin_contract_hash"] == plugin.contract_hash()
    assert payload["strategy_decision_contract_version"] == plugin.decision_contract_version
