from __future__ import annotations

from collections.abc import Iterable
import json
from dataclasses import dataclass
from typing import Any, Callable

from .backtest_types import BacktestRun, BacktestRunContext
from .dataset_snapshot import DatasetSnapshot
from .decision_event import ResearchDecisionEvent
from .execution_model import ExecutionModel
from .experiment_manifest import ExecutionTimingPolicy, PortfolioPolicy
from .hashing import sha256_prefixed
from .strategy_spec import (
    StrategySpec,
    materialize_strategy_parameters,
)


ResearchEventBuilder = Callable[..., tuple[ResearchDecisionEvent, ...]]
ResearchParameterMaterializer = Callable[..., dict[str, Any]]


ResearchStrategyRunner = Callable[
    [
        DatasetSnapshot,
        dict[str, Any],
        float,
        float,
        float | None,
        ExecutionModel | None,
        ExecutionTimingPolicy | None,
        PortfolioPolicy | None,
        BacktestRunContext | None,
    ],
    BacktestRun,
]
RuntimeReplayBuilder = Callable[[dict[str, Any], dict[str, Any] | None], Any]
SingleReplayBundleBuilder = Callable[
    [Any, Any, int, dict[str, object] | None],
    dict[str, Any] | None,
]
RuntimeEnvParameterExtractor = Callable[[dict[str, str]], dict[str, Any]]
RuntimeSettingsParameterExtractor = Callable[[object], dict[str, Any]]
DecisionPayloadAdapter = Callable[[dict[str, object], Any], dict[str, object]]
ExitSignalContextBuilder = Callable[[Any], dict[str, object]]
ExitRuleFactory = Callable[
    [
        dict[str, Any],
        dict[str, Any],
        float,
    ],
    list[Any],
]
ResearchPolicyDecisionBuilder = Callable[..., Any]
ResearchExportNormalizer = Callable[
    [
        list[dict[str, object]],
        object,
        dict[str, object],
        dict[str, object],
        str,
    ],
    list[dict[str, object]],
]


class ResearchStrategyRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class RuntimeReplayStrategyAdapter:
    strategy: Any
    runtime_decision_builder: Callable[..., Any]

    @property
    def name(self) -> str:
        return str(getattr(self.strategy, "name", ""))

    def __getattr__(self, name: str) -> Any:
        return getattr(self.strategy, name)

    def decide_runtime_snapshot(
        self,
        conn: Any,
        *,
        through_ts_ms: int | None = None,
    ) -> Any:
        return self.runtime_decision_builder(
            conn,
            self.strategy,
            through_ts_ms=through_ts_ms,
        )


@dataclass(frozen=True)
class DataCapabilityRequirement:
    name: str
    required: bool = True
    min_coverage_pct: float | None = None
    evidence_level: str | None = None
    source: str | None = None
    notes: str | None = None

    def __post_init__(self) -> None:
        normalized = str(self.name or "").strip().lower()
        if not normalized:
            raise ValueError("data capability name must be non-empty")
        object.__setattr__(self, "name", normalized)
        if self.min_coverage_pct is not None:
            coverage = float(self.min_coverage_pct)
            if coverage < 0.0 or coverage > 100.0:
                raise ValueError("data capability min_coverage_pct must be between 0 and 100")
            object.__setattr__(self, "min_coverage_pct", coverage)

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "required": bool(self.required),
        }
        if self.min_coverage_pct is not None:
            payload["min_coverage_pct"] = float(self.min_coverage_pct)
        if self.evidence_level is not None:
            payload["evidence_level"] = str(self.evidence_level)
        if self.source is not None:
            payload["source"] = str(self.source)
        if self.notes is not None:
            payload["notes"] = str(self.notes)
        return payload


@dataclass(frozen=True)
class ResearchStrategyDataRequirements:
    required_data: tuple[str, ...]
    optional_data: tuple[str, ...] = ()
    unsupported_without: tuple[str, ...] = ()
    capabilities: tuple[DataCapabilityRequirement, ...] = ()

    def normalized_capabilities(self) -> tuple[DataCapabilityRequirement, ...]:
        return normalized_data_capabilities(
            required_data=self.required_data,
            optional_data=self.optional_data,
            capabilities=self.capabilities,
        )

    def capability_contract_payload(self) -> dict[str, Any]:
        capabilities = self.normalized_capabilities()
        return {
            "schema_version": 1,
            "required_data": list(self.required_data),
            "optional_data": list(self.optional_data),
            "capabilities": [capability.as_dict() for capability in capabilities],
        }


def normalized_data_capabilities(
    *,
    required_data: tuple[str, ...],
    optional_data: tuple[str, ...] = (),
    capabilities: tuple[DataCapabilityRequirement, ...] = (),
) -> tuple[DataCapabilityRequirement, ...]:
    by_name: dict[str, DataCapabilityRequirement] = {}
    for raw_name in required_data:
        name = str(raw_name).strip().lower()
        if name:
            by_name[name] = DataCapabilityRequirement(name=name, required=True)
    for raw_name in optional_data:
        name = str(raw_name).strip().lower()
        if name and name not in by_name:
            by_name[name] = DataCapabilityRequirement(name=name, required=False)
    for capability in capabilities:
        by_name[capability.name] = capability
    return tuple(by_name[name] for name in sorted(by_name))


@dataclass(frozen=True)
class RuntimeParameterAdapter:
    from_env: RuntimeEnvParameterExtractor
    from_settings: RuntimeSettingsParameterExtractor
    env_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class StrategyRuntimeCapabilities:
    promotion_runtime_decisions_supported: bool
    runtime_replay_supported: bool
    research_only: bool = False
    baseline_only: bool = False
    live_dry_run_allowed: bool = False
    live_real_order_allowed: bool = False
    approved_profile_required: bool = True
    fail_closed_reason: str = "strategy_runtime_capability_missing"

    def __post_init__(self) -> None:
        reason = str(self.fail_closed_reason or "").strip().lower()
        if not reason:
            raise ValueError("strategy runtime capability fail_closed_reason must be non-empty")
        object.__setattr__(self, "fail_closed_reason", reason)
        if bool(self.research_only) or bool(self.baseline_only):
            if self.promotion_runtime_decisions_supported:
                raise ValueError("research-only or baseline-only strategy cannot support promotion runtime decisions")
            if self.live_dry_run_allowed or self.live_real_order_allowed:
                raise ValueError("research-only or baseline-only strategy cannot be live eligible")
        if self.live_real_order_allowed and not self.live_dry_run_allowed:
            raise ValueError("live real-order eligibility requires live dry-run eligibility")
        if self.live_dry_run_allowed and not self.promotion_runtime_decisions_supported:
            raise ValueError("live dry-run eligibility requires promotion runtime decision support")
        if self.live_real_order_allowed and not self.approved_profile_required:
            raise ValueError("live real-order eligibility requires an approved profile")

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "promotion_runtime_decisions_supported": bool(self.promotion_runtime_decisions_supported),
            "runtime_replay_supported": bool(self.runtime_replay_supported),
            "research_only": bool(self.research_only),
            "baseline_only": bool(self.baseline_only),
            "live_dry_run_allowed": bool(self.live_dry_run_allowed),
            "live_real_order_allowed": bool(self.live_real_order_allowed),
            "approved_profile_required": bool(self.approved_profile_required),
            "fail_closed_reason": self.fail_closed_reason,
        }


@dataclass(frozen=True)
class ResearchStrategyPlugin:
    name: str
    version: str
    spec: StrategySpec
    required_data: tuple[str, ...]
    optional_data: tuple[str, ...]
    runner: ResearchStrategyRunner
    runtime_replay_builder: RuntimeReplayBuilder | None
    runtime_parameter_adapter: RuntimeParameterAdapter | None
    decision_contract_version: str
    diagnostics_namespace: str
    research_event_builder: ResearchEventBuilder | None = None
    research_parameter_materializer: ResearchParameterMaterializer | None = None
    decision_payload_adapter: DecisionPayloadAdapter | None = None
    exit_signal_context_builder: ExitSignalContextBuilder | None = None
    exit_rule_factory: ExitRuleFactory | None = None
    research_policy_decision_builder: ResearchPolicyDecisionBuilder | None = None
    research_export_normalizer: ResearchExportNormalizer | None = None
    runtime_decision_adapter_factory: Callable[[], Any] | None = None
    single_replay_bundle_builder: SingleReplayBundleBuilder | None = None
    policy_assembly_factory: Callable[[], Any] | None = None
    runtime_capabilities: StrategyRuntimeCapabilities | None = None
    research_runnable: bool = True
    authoring_contract_kind: str = "legacy_research_strategy_plugin"
    promotion_extension_payload: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.runtime_capabilities is None:
            raise ValueError(f"strategy runtime capabilities must be explicit: {self.name}")
        decision_contract_version = str(self.decision_contract_version or "").strip()
        if not decision_contract_version:
            raise ValueError(f"strategy decision contract version missing: {self.name}")
        if bool(self.research_runnable) and self.research_event_builder is None:
            raise ValueError(f"research event builder missing: {self.name}")
        if self.runtime_capabilities.runtime_replay_supported != (self.runtime_replay_builder is not None):
            raise ValueError(f"strategy runtime replay capability mismatch: {self.name}")
        if self.runtime_capabilities.promotion_runtime_decisions_supported:
            if self.runtime_parameter_adapter is None:
                raise ValueError(f"strategy promotion runtime capability missing parameter adapter: {self.name}")
            if self.runtime_decision_adapter_factory is None:
                raise ValueError(f"strategy promotion runtime capability missing adapter: {self.name}")
            if self.policy_assembly_factory is None:
                raise ValueError(f"strategy promotion runtime capability missing policy assembly: {self.name}")
        if self.runtime_capabilities.live_dry_run_allowed and self.runtime_decision_adapter_factory is None:
            raise ValueError(f"strategy live dry-run capability missing adapter: {self.name}")
        if self.runtime_capabilities.live_real_order_allowed and self.runtime_decision_adapter_factory is None:
            raise ValueError(f"strategy live real-order capability missing adapter: {self.name}")

    def contract_payload(self) -> dict[str, Any]:
        data_requirements = ResearchStrategyDataRequirements(
            required_data=self.required_data,
            optional_data=self.optional_data,
        )
        return {
            "name": self.name,
            "version": self.version,
            "strategy_spec_hash": self.spec.spec_hash(),
            "required_data": list(self.required_data),
            "optional_data": list(self.optional_data),
            "data_capability_contract": data_requirements.capability_contract_payload(),
            "behavior_affecting_parameter_names": list(self.spec.behavior_affecting_parameter_names),
            "runner_module": self.runner.__module__,
            "runner_qualname": self.runner.__qualname__,
            "research_runnable": bool(self.research_runnable),
            "authoring_contract_kind": self.authoring_contract_kind,
            "promotion_grade": self.is_promotion_grade,
            "promotion_extension": self.promotion_extension_payload,
            "promotion_extension_missing_reason": (
                None if self.is_promotion_grade else self.runtime_capabilities.fail_closed_reason
            ),
            "recommended_next_action": (
                "none" if self.is_promotion_grade else "promote_strategy_contract"
            ),
            "research_event_builder_supported": self.research_event_builder is not None,
            "research_event_builder_module": (
                self.research_event_builder.__module__ if self.research_event_builder is not None else None
            ),
            "research_event_builder_qualname": (
                self.research_event_builder.__qualname__ if self.research_event_builder is not None else None
            ),
            "research_parameter_materializer_supported": self.research_parameter_materializer is not None,
            "research_parameter_materializer_module": (
                self.research_parameter_materializer.__module__
                if self.research_parameter_materializer is not None
                else None
            ),
            "research_parameter_materializer_qualname": (
                self.research_parameter_materializer.__qualname__
                if self.research_parameter_materializer is not None
                else None
            ),
            "runtime_replay_supported": self.runtime_replay_builder is not None,
            "runtime_replay_builder_module": (
                self.runtime_replay_builder.__module__ if self.runtime_replay_builder is not None else None
            ),
            "runtime_replay_builder_qualname": (
                self.runtime_replay_builder.__qualname__ if self.runtime_replay_builder is not None else None
            ),
            "runtime_parameter_adapter_supported": self.runtime_parameter_adapter is not None,
            "runtime_parameter_env_keys": (
                list(self.runtime_parameter_adapter.env_keys)
                if self.runtime_parameter_adapter is not None
                else []
            ),
            "runtime_parameter_from_env_module": (
                self.runtime_parameter_adapter.from_env.__module__
                if self.runtime_parameter_adapter is not None
                else None
            ),
            "runtime_parameter_from_env_qualname": (
                self.runtime_parameter_adapter.from_env.__qualname__
                if self.runtime_parameter_adapter is not None
                else None
            ),
            "runtime_parameter_from_settings_module": (
                self.runtime_parameter_adapter.from_settings.__module__
                if self.runtime_parameter_adapter is not None
                else None
            ),
            "runtime_parameter_from_settings_qualname": (
                self.runtime_parameter_adapter.from_settings.__qualname__
                if self.runtime_parameter_adapter is not None
                else None
            ),
            "decision_contract_version": self.decision_contract_version,
            "diagnostics_namespace": self.diagnostics_namespace,
            "decision_payload_adapter_supported": self.decision_payload_adapter is not None,
            "decision_payload_adapter_module": (
                self.decision_payload_adapter.__module__ if self.decision_payload_adapter is not None else None
            ),
            "decision_payload_adapter_qualname": (
                self.decision_payload_adapter.__qualname__ if self.decision_payload_adapter is not None else None
            ),
            "exit_signal_context_builder_supported": self.exit_signal_context_builder is not None,
            "exit_signal_context_builder_module": (
                self.exit_signal_context_builder.__module__ if self.exit_signal_context_builder is not None else None
            ),
            "exit_signal_context_builder_qualname": (
                self.exit_signal_context_builder.__qualname__
                if self.exit_signal_context_builder is not None
                else None
            ),
            "exit_rule_factory_supported": self.exit_rule_factory is not None,
            "exit_rule_factory_module": (
                self.exit_rule_factory.__module__ if self.exit_rule_factory is not None else None
            ),
            "exit_rule_factory_qualname": (
                self.exit_rule_factory.__qualname__ if self.exit_rule_factory is not None else None
            ),
            "research_policy_decision_builder_supported": self.research_policy_decision_builder is not None,
            "research_policy_decision_builder_module": (
                self.research_policy_decision_builder.__module__
                if self.research_policy_decision_builder is not None
                else None
            ),
            "research_policy_decision_builder_qualname": (
                self.research_policy_decision_builder.__qualname__
                if self.research_policy_decision_builder is not None
                else None
            ),
            "research_export_normalizer_supported": self.research_export_normalizer is not None,
            "research_export_normalizer_module": (
                self.research_export_normalizer.__module__ if self.research_export_normalizer is not None else None
            ),
            "research_export_normalizer_qualname": (
                self.research_export_normalizer.__qualname__
                if self.research_export_normalizer is not None
                else None
            ),
            "runtime_decision_adapter_supported": self.runtime_decision_adapter_factory is not None,
            "runtime_capabilities": self.runtime_capabilities.as_dict(),
            "runtime_decision_adapter_module": (
                self.runtime_decision_adapter_factory.__module__
                if self.runtime_decision_adapter_factory is not None
                else None
            ),
            "runtime_decision_adapter_qualname": (
                self.runtime_decision_adapter_factory.__qualname__
                if self.runtime_decision_adapter_factory is not None
                else None
            ),
            "single_replay_bundle_supported": self.single_replay_bundle_builder is not None,
            "single_replay_bundle_builder_module": (
                self.single_replay_bundle_builder.__module__
                if self.single_replay_bundle_builder is not None
                else None
            ),
            "single_replay_bundle_builder_qualname": (
                self.single_replay_bundle_builder.__qualname__
                if self.single_replay_bundle_builder is not None
                else None
            ),
            "policy_assembly_supported": self.policy_assembly_factory is not None,
            "policy_assembly_module": (
                self.policy_assembly_factory.__module__
                if self.policy_assembly_factory is not None
                else None
            ),
            "policy_assembly_qualname": (
                self.policy_assembly_factory.__qualname__
                if self.policy_assembly_factory is not None
                else None
            ),
            "decision_assembly_contract": {
                "schema_version": 1,
                "promotion_runtime_decisions_supported": bool(
                    self.runtime_capabilities.promotion_runtime_decisions_supported
                ),
                "runtime_parameter_adapter_required": bool(
                    self.runtime_capabilities.promotion_runtime_decisions_supported
                ),
                "runtime_decision_adapter_required": bool(
                    self.runtime_capabilities.promotion_runtime_decisions_supported
                ),
                "policy_assembly_required": bool(
                    self.runtime_capabilities.promotion_runtime_decisions_supported
                ),
                "decision_contract_version": self.decision_contract_version,
                "runtime_capabilities": self.runtime_capabilities.as_dict(),
            },
        }

    def contract_hash(self) -> str:
        return sha256_prefixed(self.contract_payload())

    @property
    def is_promotion_grade(self) -> bool:
        if self.promotion_extension_payload is not None:
            return True
        return (
            bool(self.runtime_capabilities.promotion_runtime_decisions_supported)
            and self.runtime_parameter_adapter is not None
            and self.runtime_decision_adapter_factory is not None
            and self.policy_assembly_factory is not None
        )


def _legacy_inferred_runtime_capabilities(plugin: ResearchStrategyPlugin) -> StrategyRuntimeCapabilities:
    promotion_supported = plugin.runtime_decision_adapter_factory is not None
    return StrategyRuntimeCapabilities(
        promotion_runtime_decisions_supported=promotion_supported,
        runtime_replay_supported=plugin.runtime_replay_builder is not None,
        research_only=not promotion_supported,
        baseline_only=not promotion_supported,
        live_dry_run_allowed=promotion_supported,
        live_real_order_allowed=False,
        approved_profile_required=True,
        fail_closed_reason=(
            "legacy_plugin_capability_inferred_runtime_adapter_missing"
            if not promotion_supported
            else "legacy_plugin_capability_inferred_live_real_order_not_allowed"
        ),
    )


def strategy_runtime_capability_issues(
    strategy_name: str,
    *,
    live_dry_run: bool,
    live_real_order_armed: bool,
    approved_profile_path: str = "",
    require_promotion_runtime: bool = True,
    require_runtime_replay: bool = False,
    require_runtime_decision_adapter: bool = True,
) -> tuple[str, ...]:
    key = str(strategy_name or "").strip().lower()
    try:
        plugin = resolve_research_strategy_plugin(key)
    except ResearchStrategyRegistryError:
        return (f"strategy_plugin_not_registered:{key}",)

    capabilities = plugin.runtime_capabilities
    issues: list[str] = []
    if require_promotion_runtime and not capabilities.promotion_runtime_decisions_supported:
        issues.append(f"promotion_runtime_unsupported_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}")
    if require_runtime_replay and not capabilities.runtime_replay_supported:
        issues.append(f"runtime_replay_unsupported_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}")
    if require_runtime_decision_adapter and plugin.runtime_decision_adapter_factory is None:
        issues.append(f"runtime_decision_adapter_unsupported_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}")
    if bool(live_dry_run) and not capabilities.live_dry_run_allowed:
        issues.append(f"live_dry_run_not_allowed_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}")
    if bool(live_real_order_armed) and not capabilities.live_real_order_allowed:
        issues.append(f"live_real_order_not_allowed_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}")
    if (
        (bool(live_dry_run) or bool(live_real_order_armed))
        and capabilities.approved_profile_required
        and not str(approved_profile_path or "").strip()
    ):
        issues.append(f"approved_profile_required_for_strategy:{plugin.name}")
    return tuple(issues)


TEST_TOP_OF_BOOK_REQUIRED_STRATEGY = "__test_top_of_book_required__"


def research_strategy_data_requirements(strategy_name: str) -> ResearchStrategyDataRequirements:
    if strategy_name == TEST_TOP_OF_BOOK_REQUIRED_STRATEGY:
        return ResearchStrategyDataRequirements(
            required_data=("candles", "top_of_book"),
            capabilities=(
                DataCapabilityRequirement(
                    name="top_of_book",
                    required=True,
                    min_coverage_pct=100.0,
                    evidence_level="best_bid_ask",
                    source="sqlite_orderbook_top_snapshots",
                    notes="private test hook for required top-of-book preflight",
                ),
            ),
        )
    plugin = resolve_research_strategy_plugin(strategy_name)
    return ResearchStrategyDataRequirements(
        required_data=plugin.required_data,
        optional_data=plugin.optional_data,
    )


def resolve_research_strategy_plugin(strategy_name: str) -> ResearchStrategyPlugin:
    _ensure_discovered_strategy_plugins_loaded()
    key = str(strategy_name or "").strip().lower()
    try:
        return _RESEARCH_STRATEGY_PLUGINS[key]
    except KeyError as exc:
        raise ResearchStrategyRegistryError(f"unsupported research strategy: {key}") from exc


def resolve_research_strategy(strategy_name: str) -> ResearchStrategyRunner:
    if strategy_name == TEST_TOP_OF_BOOK_REQUIRED_STRATEGY:
        return _run_private_required_data_test_hook
    return resolve_research_strategy_plugin(strategy_name).runner


def runtime_strategy_parameters_from_env(strategy_name: str, env: dict[str, str]) -> dict[str, Any]:
    plugin = resolve_research_strategy_plugin(strategy_name)
    generic = _strategy_parameters_json_from_env(env)
    if generic is not None:
        parameters = materialize_strategy_parameters(plugin.name, generic)
        _assert_runtime_parameters_accepted(plugin=plugin, parameters=parameters)
        return parameters
    if plugin.runtime_parameter_adapter is None:
        raise ResearchStrategyRegistryError(f"runtime parameter extraction unsupported: {plugin.name}")
    parameters = plugin.runtime_parameter_adapter.from_env(env)
    _assert_runtime_parameters_accepted(plugin=plugin, parameters=parameters)
    return parameters


def runtime_strategy_parameters_from_settings(strategy_name: str, cfg: object) -> dict[str, Any]:
    plugin = resolve_research_strategy_plugin(strategy_name)
    generic = _strategy_parameters_json_from_settings(cfg)
    if generic is not None:
        parameters = materialize_strategy_parameters(plugin.name, generic)
        _assert_runtime_parameters_accepted(plugin=plugin, parameters=parameters)
        return parameters
    if plugin.runtime_parameter_adapter is None:
        raise ResearchStrategyRegistryError(f"runtime parameter extraction unsupported: {plugin.name}")
    parameters = plugin.runtime_parameter_adapter.from_settings(cfg)
    _assert_runtime_parameters_accepted(plugin=plugin, parameters=parameters)
    return parameters


def runtime_strategy_parameter_env_keys(strategy_name: str) -> tuple[str, ...]:
    plugin = resolve_research_strategy_plugin(strategy_name)
    if plugin.runtime_parameter_adapter is None:
        return ()
    return tuple(plugin.runtime_parameter_adapter.env_keys)


def _strategy_parameters_json_from_env(env: dict[str, str]) -> dict[str, Any] | None:
    raw = str(env.get("STRATEGY_PARAMETERS_JSON") or "").strip()
    return _parse_strategy_parameters_json(raw) if raw else None


def _strategy_parameters_json_from_settings(cfg: object) -> dict[str, Any] | None:
    raw = str(getattr(cfg, "STRATEGY_PARAMETERS_JSON", "") or "").strip()
    return _parse_strategy_parameters_json(raw) if raw else None


def _parse_strategy_parameters_json(raw: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ResearchStrategyRegistryError(f"strategy_parameters_json_invalid:{exc}") from exc
    if not isinstance(payload, dict):
        raise ResearchStrategyRegistryError("strategy_parameters_json_must_be_object")
    return {str(key): value for key, value in payload.items()}


def _assert_runtime_parameters_accepted(
    *,
    plugin: ResearchStrategyPlugin,
    parameters: dict[str, Any],
) -> None:
    unexpected = sorted(set(parameters) - set(plugin.spec.accepted_parameter_names))
    if unexpected:
        joined = ",".join(unexpected)
        raise ResearchStrategyRegistryError(f"runtime parameter extraction returned unsupported keys:{plugin.name}:{joined}")


def _run_private_required_data_test_hook(
    dataset: DatasetSnapshot,
    parameter_values: dict[str, Any],
    fee_rate: float,
    slippage_bps: float,
    parameter_stability_score: float | None = None,
    execution_model: ExecutionModel | None = None,
    execution_timing_policy: ExecutionTimingPolicy | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
    context: BacktestRunContext | None = None,
) -> BacktestRun:
    del (
        dataset,
        parameter_values,
        fee_rate,
        slippage_bps,
        parameter_stability_score,
        execution_model,
        execution_timing_policy,
        portfolio_policy,
        context,
    )
    raise ResearchStrategyRegistryError("private required-data test hook should fail before backtest execution")


_RESEARCH_STRATEGY_PLUGINS: dict[str, ResearchStrategyPlugin] = {}
_DISCOVERED_STRATEGY_PLUGINS_LOADED = False


def register_research_strategy_plugin(
    plugin: Any,
    *,
    replace: bool = False,
) -> None:
    plugin = _normalize_research_strategy_plugin(plugin)
    key = str(plugin.name or "").strip().lower()
    if not key:
        raise ResearchStrategyRegistryError("research strategy plugin name must be non-empty")
    existing = _RESEARCH_STRATEGY_PLUGINS.get(key)
    if existing is not None and not replace:
        raise ResearchStrategyRegistryError(f"duplicate research strategy plugin name: {key}")
    _RESEARCH_STRATEGY_PLUGINS[key] = plugin


def list_research_strategy_plugins() -> tuple[ResearchStrategyPlugin, ...]:
    _ensure_discovered_strategy_plugins_loaded()
    return tuple(_RESEARCH_STRATEGY_PLUGINS[name] for name in sorted(_RESEARCH_STRATEGY_PLUGINS))


def _ensure_discovered_strategy_plugins_loaded() -> None:
    global _DISCOVERED_STRATEGY_PLUGINS_LOADED
    if _DISCOVERED_STRATEGY_PLUGINS_LOADED:
        return
    from bithumb_bot.strategy_plugins import iter_discovered_strategy_plugins

    _load_strategy_plugins_from_provider(iter_discovered_strategy_plugins)
    _DISCOVERED_STRATEGY_PLUGINS_LOADED = True


def _normalize_research_strategy_plugin(plugin: Any) -> ResearchStrategyPlugin:
    if isinstance(plugin, ResearchStrategyPlugin):
        return plugin
    adapter = getattr(plugin, "to_research_strategy_plugin", None)
    if callable(adapter):
        normalized = adapter()
        if isinstance(normalized, ResearchStrategyPlugin):
            return normalized
    raise TypeError(f"research_strategy_plugin_invalid_type:{type(plugin).__name__}")


def _load_strategy_plugins_from_provider(
    provider: Callable[[], Iterable[Any]] | Callable[[], Any],
) -> None:
    for plugin in provider():
        register_research_strategy_plugin(_normalize_research_strategy_plugin(plugin))


def reload_research_strategy_plugins_for_tests(
    providers: tuple[Callable[[], Iterable[ResearchStrategyPlugin]], ...] | None = None,
) -> None:
    """Reset plugin registry state for tests that monkeypatch discovery."""
    global _RESEARCH_STRATEGY_PLUGINS, _DISCOVERED_STRATEGY_PLUGINS_LOADED
    _RESEARCH_STRATEGY_PLUGINS = {}
    _DISCOVERED_STRATEGY_PLUGINS_LOADED = False
    if providers is None:
        _ensure_discovered_strategy_plugins_loaded()
        return
    for provider in providers:
        _load_strategy_plugins_from_provider(provider)
    _DISCOVERED_STRATEGY_PLUGINS_LOADED = True
