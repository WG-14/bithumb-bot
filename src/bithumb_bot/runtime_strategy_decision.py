from __future__ import annotations

import inspect
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Callable, Mapping, Protocol, runtime_checkable

from .config import settings, validate_live_strategy_selection
from .runtime_scope import RuntimeScopeKey
from .runtime_data_provider import RuntimeFeatureSnapshot
from .strategy_policy_contract import StrategyDecisionV2


@dataclass(frozen=True)
class RuntimeDecisionRequest:
    strategy_instance_id: str
    strategy_name: str
    pair: str
    interval: str
    through_ts_ms: int | None
    parameters: Mapping[str, object]
    parameters_raw: Mapping[str, object]
    parameters_materialized: Mapping[str, object]
    strategy_parameters_hash: str
    approved_profile_path: str | None
    approved_profile_hash: str | None
    runtime_strategy_spec: object
    runtime_contract_hash: str | None
    parameter_source: str
    plugin_contract_hash: str | None
    strategy_version: str | None
    request_hash: str
    exit_policy_hash: str | None = None
    exit_policy_contract_hash: str | None = None
    exit_policy_source: str | None = None
    exit_policy_materialization_mode: str | None = None
    exit_policy_config_hash: str | None = None
    runtime_scope_key: RuntimeScopeKey | None = None
    scope_key_hash: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_instance_id", str(self.strategy_instance_id or "").strip())
        object.__setattr__(self, "strategy_name", str(self.strategy_name or "").strip().lower())
        object.__setattr__(self, "pair", str(self.pair or "").strip())
        object.__setattr__(self, "interval", str(self.interval or "").strip())
        object.__setattr__(
            self,
            "parameters",
            MappingProxyType({str(key): value for key, value in dict(self.parameters or {}).items()}),
        )
        object.__setattr__(
            self,
            "parameters_raw",
            MappingProxyType({str(key): value for key, value in dict(self.parameters_raw or {}).items()}),
        )
        object.__setattr__(
            self,
            "parameters_materialized",
            MappingProxyType(
                {str(key): value for key, value in dict(self.parameters_materialized or {}).items()}
            ),
        )
        scope_key = self.runtime_scope_key
        if scope_key is None:
            scope_key = RuntimeScopeKey(
                pair=self.pair,
                interval=self.interval,
                strategy_instance_id=self.strategy_instance_id,
                strategy_name=self.strategy_name,
                runtime_contract_hash=str(
                    self.runtime_contract_hash or "paper_legacy_compat:runtime_contract_hash_missing"
                ),
                approved_profile_hash=str(
                    self.approved_profile_hash or "paper_legacy_compat:approved_profile_hash_missing"
                ),
                strategy_parameters_hash=self.strategy_parameters_hash,
            )
        object.__setattr__(self, "runtime_scope_key", scope_key)
        object.__setattr__(self, "scope_key_hash", self.scope_key_hash or scope_key.scope_key_hash())

    def observability_fields(self) -> dict[str, object]:
        spec = self.runtime_strategy_spec
        authority_audit = (
            spec.parameter_authority_audit
            if hasattr(spec, "parameter_authority_audit")
            else {}
        )
        profile_authority = (
            spec.profile_authority_context
            if hasattr(spec, "profile_authority_context")
            else {}
        )
        legacy_used = (
            bool(spec.legacy_compatibility_used)
            if hasattr(spec, "legacy_compatibility_used")
            else False
        )
        return {
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "strategy_parameters": dict(self.parameters),
            "strategy_parameters_raw": dict(self.parameters_raw),
            "strategy_parameters_materialized": dict(self.parameters_materialized),
            "strategy_parameters_hash": self.strategy_parameters_hash,
            "approved_profile_path": self.approved_profile_path,
            "approved_profile_hash": self.approved_profile_hash,
            "runtime_contract_hash": self.runtime_contract_hash,
            "runtime_scope_key": self.runtime_scope_key.as_dict() if self.runtime_scope_key else None,
            "scope_key_hash": self.scope_key_hash,
            "through_ts_ms": self.through_ts_ms,
            "candle_ts": self.through_ts_ms,
            "plugin_contract_hash": self.plugin_contract_hash,
            "exit_policy_hash": self.exit_policy_hash,
            "exit_policy_contract_hash": self.exit_policy_contract_hash,
            "exit_policy_source": self.exit_policy_source,
            "exit_policy_materialization_mode": self.exit_policy_materialization_mode,
            "exit_policy_config_hash": self.exit_policy_config_hash,
            "runtime_decision_request_hash": self.request_hash,
            "request_hash": self.request_hash,
            "parameter_source": self.parameter_source,
            "parameter_authority_audit": dict(authority_audit or {}),
            "profile_authority_context": dict(profile_authority or {}),
            "runtime_selection_kind": dict(profile_authority or {}).get("runtime_selection_kind"),
            "runtime_strategy_set_source": dict(profile_authority or {}).get("runtime_strategy_set_source"),
            "profile_binding_kind": dict(profile_authority or {}).get("profile_binding_kind"),
            "runtime_gate_authority": dict(profile_authority or {}).get("runtime_gate_authority"),
            "allow_global_profile_fallback": dict(profile_authority or {}).get("allow_global_profile_fallback"),
            "require_spec_bound_profile": dict(profile_authority or {}).get("require_spec_bound_profile"),
            "legacy_parameter_compatibility_used": legacy_used,
        }

    def as_dict(self) -> dict[str, object]:
        spec = self.runtime_strategy_spec
        spec_payload = spec.as_dict() if hasattr(spec, "as_dict") else str(spec)
        return {
            "schema_version": 1,
            **self.observability_fields(),
            "strategy": self.strategy_name,
            "pair": self.pair,
            "interval": self.interval,
            "runtime_strategy_spec": spec_payload,
            "runtime_scope_key": self.runtime_scope_key.as_dict() if self.runtime_scope_key else None,
            "scope_key_hash": self.scope_key_hash,
        }


@runtime_checkable
class RuntimeStrategyDecisionResult(Protocol):
    decision: object
    base_context: dict[str, object]
    candle_ts: int
    market_price: float
    policy_hashes: object | None
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object]

    def as_legacy_dict(self) -> dict[str, object]: ...


class LegacyDiagnosticRuntimeDecisionAdapter(Protocol):
    strategy_name: str

    def decide(
        self,
        conn,
        request: RuntimeDecisionRequest,
    ) -> RuntimeStrategyDecisionResult | None: ...


class PromotionRuntimeDecisionAdapter(Protocol):
    strategy_name: str

    def decide_feature_snapshot(
        self,
        request: RuntimeDecisionRequest,
        feature_snapshot: RuntimeFeatureSnapshot,
    ) -> RuntimeStrategyDecisionResult | None: ...

    def typed_authority_required(self) -> bool: ...


class RuntimeDecisionAdapter(PromotionRuntimeDecisionAdapter, Protocol):
    """Deprecated alias for connection-free promotion adapters."""


RuntimeDecisionAdapterFactory = Callable[[], PromotionRuntimeDecisionAdapter]

_DERIVED_RUNTIME_DECISION_ADAPTER_CACHE: dict[tuple[str, str, str], PromotionRuntimeDecisionAdapter] = {}


def _normalize_name(name: str) -> str:
    key = str(name or "").strip().lower()
    if not key:
        raise ValueError("runtime strategy name must not be empty")
    return key


def list_runtime_decision_adapters() -> tuple[str, ...]:
    from .research.strategy_registry import list_research_strategy_plugins

    return tuple(
        sorted(
            plugin.name
            for plugin in list_research_strategy_plugins()
            if plugin.runtime_capabilities.promotion_runtime_decisions_supported
            and plugin.runtime_decision_adapter_factory is not None
        )
    )


def get_runtime_decision_adapter(name: str) -> PromotionRuntimeDecisionAdapter | None:
    from .research.strategy_registry import ResearchStrategyRegistryError, resolve_research_strategy_plugin

    key = _normalize_name(name)
    try:
        plugin = resolve_research_strategy_plugin(key)
    except ResearchStrategyRegistryError:
        return None
    if not plugin.runtime_capabilities.promotion_runtime_decisions_supported:
        return None
    factory = plugin.runtime_decision_adapter_factory
    if factory is None:
        return None
    contract_hash = plugin.contract_hash()
    cache_key = (plugin.name, contract_hash, "plugin")
    cached = _DERIVED_RUNTIME_DECISION_ADAPTER_CACHE.get(cache_key)
    if cached is not None:
        return cached
    adapter = factory()
    adapter_name = _normalize_name(getattr(adapter, "strategy_name", ""))
    if adapter_name != plugin.name:
        raise RuntimeError(f"runtime_decision_adapter_name_mismatch:{plugin.name}:{adapter_name}")
    if not promotion_adapter_supports_feature_snapshot(adapter):
        raise RuntimeError(f"runtime_decision_feature_snapshot_required:{plugin.name}")
    if _has_db_bound_decide_method(adapter):
        raise RuntimeError(f"promotion_runtime_adapter_db_bound_decide_forbidden:{plugin.name}")
    _DERIVED_RUNTIME_DECISION_ADAPTER_CACHE.clear()
    _DERIVED_RUNTIME_DECISION_ADAPTER_CACHE[cache_key] = adapter
    return adapter


def is_runtime_strategy_decision_result(value: object) -> bool:
    if not isinstance(value, RuntimeStrategyDecisionResult):
        return False
    return isinstance(getattr(value, "decision", None), StrategyDecisionV2)


def production_runtime_strategy_missing_error(selected_strategy_name: str) -> RuntimeError:
    return RuntimeError(f"runtime_decision_adapter_not_registered:{selected_strategy_name}")


def promotion_grade_typed_runtime_decision_required(
    *,
    selected_strategy_name: str,
) -> bool:
    adapter = get_runtime_decision_adapter(selected_strategy_name)
    if adapter is None:
        return _production_missing_adapter_requires_typed_handoff()
    return adapter.typed_authority_required()


def typed_runtime_handoff_failure_reason(
    signal_handoff: object,
    *,
    selected_strategy_name: str,
) -> str | None:
    if not promotion_grade_typed_runtime_decision_required(
        selected_strategy_name=selected_strategy_name,
    ):
        return None
    if is_runtime_strategy_decision_result(signal_handoff):
        return None
    if get_runtime_decision_adapter(selected_strategy_name) is None:
        return "runtime_decision_adapter_not_registered"
    return "typed_runtime_decision_required"


def legacy_db_strategy_fallback_allowed(*, selected_strategy_name: str) -> bool:
    return False


def _production_missing_adapter_requires_typed_handoff() -> bool:
    mode = str(settings.MODE or "").strip().lower()
    if mode == "live":
        return True
    if str(getattr(settings, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip():
        return True
    return True


@dataclass(frozen=True)
class DecisionRunner:
    """Production runtime strategy decision runner.

    Missing adapters fail closed. Legacy DB-bound strategy execution is exposed
    only by the explicit compatibility API in ``run_loop_compatibility``.
    """

    strategy_name: str | None = None

    def decide_snapshot(
        self,
        conn,
        *,
        through_ts_ms: int | None = None,
        strategy_name: str | None = None,
        parameter_overrides: Mapping[str, object] | None = None,
        parameter_source: str = "runtime_override",
        runtime_strategy_spec: object | None = None,
    ) -> RuntimeStrategyDecisionResult | None:
        from .runtime_strategy_set import (
            ProfileAuthorityContext,
            RuntimeMarketScope,
            RuntimeStrategyDecisionCollector,
            RuntimeStrategySet,
            RuntimeStrategySetResolver,
            RuntimeStrategySpec,
        )

        resolved_strategy_set = None
        live_mode = str(settings.MODE or "").strip().lower() == "live"
        if live_mode:
            try:
                resolved_strategy_set = RuntimeStrategySetResolver().resolve()
            except Exception as exc:
                raise RuntimeError(
                    f"runtime_strategy_set_selection_failed: resolve_failed:{type(exc).__name__}:{exc}"
                ) from exc
            if resolved_strategy_set.multi_strategy_enabled and runtime_strategy_spec is None:
                raise RuntimeError(
                    "decision_runner_live_multi_strategy_requires_runtime_decision_gateway"
                )

        if runtime_strategy_spec is not None:
            if not isinstance(runtime_strategy_spec, RuntimeStrategySpec):
                raise TypeError("runtime_strategy_spec_invalid")
            spec = runtime_strategy_spec
            selected_strategy_name = spec.strategy_name
        else:
            selected_strategy_name = str(
                strategy_name or self.strategy_name or settings.STRATEGY_NAME
            ).strip().lower()
            parameters = dict(parameter_overrides or {})
            if parameters:
                _reject_unapproved_runtime_overrides(selected_strategy_name)
            spec = RuntimeStrategySpec(
                strategy_name=selected_strategy_name,
                parameters=parameters or None,
                parameter_source=parameter_source if parameters else None,
            )
        authority_context = None
        if resolved_strategy_set is not None and resolved_strategy_set.multi_strategy_enabled:
            authority_context = ProfileAuthorityContext.for_strategy_set(resolved_strategy_set)
            from .research.strategy_registry import strategy_runtime_capability_issues

            issues = strategy_runtime_capability_issues(
                selected_strategy_name,
                live_dry_run=bool(settings.LIVE_DRY_RUN),
                live_real_order_armed=bool(settings.LIVE_REAL_ORDER_ARMED),
                approved_profile_path=str(spec.approved_profile_path or "").strip(),
                require_promotion_runtime=True,
                require_runtime_replay=True,
                require_runtime_decision_adapter=True,
            )
            if issues:
                raise RuntimeError(
                    "live_runtime_strategy_capability_validation_failed:"
                    f"{selected_strategy_name}:reasons=" + ",".join(issues)
                )
        else:
            validate_live_strategy_selection(
                replace(settings, STRATEGY_NAME=selected_strategy_name)
            )
        adapter = get_runtime_decision_adapter(selected_strategy_name)
        if adapter is None:
            raise production_runtime_strategy_missing_error(selected_strategy_name)

        strategy_set = RuntimeStrategySet(
            strategies=(spec,),
            source="DecisionRunner",
            market_scope=RuntimeMarketScope(pair=spec.pair, interval=spec.interval),
        )
        collector = RuntimeStrategyDecisionCollector()
        request_builder = (
            collector.request_builder.with_authority_context(authority_context)
            if authority_context is not None
            else collector.request_builder
        )
        bundle = RuntimeStrategyDecisionCollector(
            request_builder=request_builder,
            adapter_resolver=get_runtime_decision_adapter,
        ).collect(
            conn,
            strategy_set,
            through_ts_ms=through_ts_ms,
        )
        if bundle is None:
            return None
        if len(bundle.results) != 1:
            raise RuntimeError("decision_runner_single_result_required")
        return bundle.results[0]


def compute_strategy_decision_snapshot(
    conn,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
    parameter_overrides: Mapping[str, object] | None = None,
    parameter_source: str = "runtime_override",
    runtime_strategy_spec: object | None = None,
) -> RuntimeStrategyDecisionResult | None:
    return DecisionRunner(strategy_name=strategy_name).decide_snapshot(
        conn,
        through_ts_ms=through_ts_ms,
        parameter_overrides=parameter_overrides,
        parameter_source=parameter_source,
        runtime_strategy_spec=runtime_strategy_spec,
    )


def compute_strategy_decision_for_diagnostics(
    conn,
    *diagnostic_parameters: int,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
    parameter_overrides: Mapping[str, object] | None = None,
    parameter_source: str = "runtime_override",
    runtime_strategy_spec: object | None = None,
) -> RuntimeStrategyDecisionResult | None:
    if diagnostic_parameters:
        raise TypeError("positional_diagnostic_parameters_unsupported")
    return compute_strategy_decision_snapshot(
        conn,
        through_ts_ms=through_ts_ms,
        strategy_name=strategy_name,
        parameter_overrides=parameter_overrides,
        parameter_source=parameter_source,
        runtime_strategy_spec=runtime_strategy_spec,
    )


def compute_legacy_signal_for_diagnostics(
    conn,
    *diagnostic_parameters: int,
    through_ts_ms: int | None = None,
    strategy_name: str | None = None,
):
    """Return a legacy dict for explicit CLI/diagnostic callers only.

    Production runtime authority must use RuntimeDecisionGateway and never this
    compatibility serialization.
    """
    if diagnostic_parameters:
        selected_strategy_name = str(strategy_name or settings.STRATEGY_NAME or "").strip().lower()
        validate_live_strategy_selection(replace(settings, STRATEGY_NAME=selected_strategy_name))
        adapter = get_runtime_decision_adapter(selected_strategy_name)
        if adapter is None:
            raise production_runtime_strategy_missing_error(selected_strategy_name)
        legacy_adapter = getattr(adapter, "legacy_diagnostic_signal", None)
        if legacy_adapter is None or not callable(legacy_adapter):
            raise TypeError(f"positional_diagnostic_parameters_unsupported:{selected_strategy_name}")
        return legacy_adapter(conn, *diagnostic_parameters, through_ts_ms=through_ts_ms)
    result = compute_strategy_decision_for_diagnostics(
        conn,
        through_ts_ms=through_ts_ms,
        strategy_name=strategy_name,
    )
    if result is None:
        return None
    payload = result.as_legacy_dict()
    payload.setdefault("strategy", result.decision.strategy_name)
    return payload


def _reject_unapproved_runtime_overrides(selected_strategy_name: str) -> None:
    mode = str(settings.MODE or "").strip().lower()
    approved_profile_path = str(getattr(settings, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
    approved_profile_alias = str(getattr(settings, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
    live_like = mode == "live" or bool(approved_profile_path or approved_profile_alias)
    if live_like:
        raise RuntimeError(f"runtime_parameter_overrides_unapproved:{selected_strategy_name}")


def _attach_runtime_request_metadata(
    result: RuntimeStrategyDecisionResult,
    request: RuntimeDecisionRequest,
) -> None:
    fields = request.observability_fields()
    if isinstance(result.base_context, dict):
        result.base_context.update(fields)
    if isinstance(result.replay_fingerprint, dict):
        result.replay_fingerprint.update(
            {
                **fields,
                "runtime_decision_request_hash": request.request_hash,
                "strategy_instance_id": request.strategy_instance_id,
                "strategy_parameters_hash": request.strategy_parameters_hash,
                "approved_profile_hash": request.approved_profile_hash,
                "runtime_contract_hash": request.runtime_contract_hash,
                "runtime_scope_key": request.runtime_scope_key.as_dict() if request.runtime_scope_key else None,
                "scope_key_hash": request.scope_key_hash,
                "plugin_contract_hash": request.plugin_contract_hash,
                "through_ts_ms": request.through_ts_ms,
            }
        )
    if isinstance(result.boundary, dict):
        result.boundary.update(
            {
                "runtime_decision_request_hash": request.request_hash,
                "strategy_instance_id": request.strategy_instance_id,
                "strategy_parameters_hash": request.strategy_parameters_hash,
                "scope_key_hash": request.scope_key_hash,
            }
        )


def promotion_adapter_supports_feature_snapshot(adapter: object) -> bool:
    return callable(getattr(adapter, "decide_feature_snapshot", None))


def _project_runtime_feature_snapshot(
    *,
    adapter: object,
    request: RuntimeDecisionRequest,
    feature_snapshot: RuntimeFeatureSnapshot,
) -> RuntimeFeatureSnapshot | None:
    projector = getattr(adapter, "project_feature_snapshot", None)
    if not callable(projector):
        return feature_snapshot
    if _has_db_bound_projector_method(adapter):
        raise RuntimeError(f"promotion_runtime_adapter_db_bound_projector_forbidden:{request.strategy_name}")
    projected = projector(request, feature_snapshot)
    if projected is None:
        return None
    if not isinstance(projected, RuntimeFeatureSnapshot):
        raise TypeError(f"runtime_feature_snapshot_projector_invalid:{request.strategy_name}")
    return projected


def _has_db_bound_decide_method(adapter: object) -> bool:
    return callable(getattr(adapter, "decide", None)) or callable(
        getattr(adapter, "decide_database_snapshot", None)
    )


def _has_db_bound_projector_method(adapter: object) -> bool:
    projector = getattr(adapter, "project_feature_snapshot", None)
    if not callable(projector):
        return False
    try:
        signature = inspect.signature(projector)
    except (TypeError, ValueError):
        return True
    positional = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind
        in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        }
    ]
    return len(positional) != 2


def _attach_runtime_feature_snapshot_metadata(
    result: RuntimeStrategyDecisionResult,
    feature_snapshot: RuntimeFeatureSnapshot,
) -> None:
    payload = feature_snapshot.as_dict()
    fields = {
        "runtime_data_requirements_hash": payload.get("runtime_data_requirements_hash"),
        "runtime_data_contract_hash": payload.get("runtime_data_contract_hash"),
        "provider_contract_hash": payload.get("provider_contract_hash"),
        "runtime_data_availability_report_hash": payload.get("runtime_data_availability_report_hash"),
        "source_schema_hash": payload.get("source_schema_hash"),
        "feature_snapshot_hash": payload.get("feature_snapshot_hash"),
        "runtime_data_market_snapshot_hash": payload.get("market_snapshot_hash"),
        "runtime_scope_key": payload.get("runtime_scope_key"),
        "scope_key_hash": payload.get("scope_key_hash"),
        "source_schema_hash_by_scope": payload.get("source_schema_hash_by_scope"),
    }
    if isinstance(result.base_context, dict):
        result.base_context.update(fields)
        result.base_context.setdefault("market_snapshot_hash", payload.get("market_snapshot_hash"))
    if isinstance(result.replay_fingerprint, dict):
        result.replay_fingerprint.update(fields)
        result.replay_fingerprint.setdefault("market_snapshot_hash", payload.get("market_snapshot_hash"))
    if isinstance(result.boundary, dict):
        result.boundary.update(fields)
        result.boundary.setdefault("market_snapshot_hash", payload.get("market_snapshot_hash"))
