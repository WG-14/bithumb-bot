from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Callable, Literal, Mapping

from .approved_profile import (
    ApprovedProfileError,
    PROFILE_HASH_FIELD,
    approved_profile_path_from_env,
    diff_profile_to_runtime,
    expected_profile_modes_for_runtime,
    load_approved_profile,
    runtime_contract_from_settings,
)
from .config import settings, validate_live_strategy_selection
from .decision_equivalence import sha256_prefixed
from .research.strategy_registry import (
    ResearchStrategyRegistryError,
    resolve_research_strategy_plugin,
)
from .research.strategy_spec import (
    exit_policy_from_parameters,
    materialize_strategy_parameters,
    materialized_strategy_parameters_hash,
    runtime_bound_behavior_parameter_names,
)
from .runtime_strategy_decision import (
    PromotionRuntimeDecisionAdapter,
    RuntimeDecisionRequest,
    RuntimeStrategyDecisionResult,
    _attach_runtime_request_metadata,
    _attach_runtime_feature_snapshot_metadata,
    _project_runtime_feature_snapshot,
    get_runtime_decision_adapter,
    is_runtime_strategy_decision_result,
    promotion_adapter_supports_feature_snapshot,
    production_runtime_strategy_missing_error,
)
from .submit_authority_policy import submit_authority_policy_from_settings
from .risk_decision import (
    RISK_BUDGET_LEGACY_MARKER,
    RISK_BUDGET_SEMANTICS,
    build_risk_decision_artifact,
)
from .strategy_risk_profile import (
    StrategyRiskProfile,
    strategy_risk_profile_from_profile_payload,
)
from .runtime_data_provider import (
    RuntimeDataAvailabilityReport,
    RuntimeDataRequirementResolver,
    RuntimeFeatureSnapshot,
    RuntimeStrategyDataRequirements,
    SQLiteRuntimeDataProvider,
    runtime_data_provider_contract_hash,
)

RuntimeAuthorityScope = Literal[
    "paper_legacy",
    "promotion",
    "runtime_replay",
    "live_dry_run",
    "live_real_order",
]


def normalize_runtime_authority_scope(value: str | None) -> RuntimeAuthorityScope:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return "paper_legacy"
    if normalized not in {
        "paper_legacy",
        "promotion",
        "runtime_replay",
        "live_dry_run",
        "live_real_order",
    }:
        raise ValueError(f"runtime_authority_scope_unsupported:{normalized}")
    return normalized  # type: ignore[return-value]


def runtime_authority_scope_from_settings(settings_obj: object = settings) -> RuntimeAuthorityScope:
    mode = str(getattr(settings_obj, "MODE", "") or "").strip().lower()
    if mode == "live":
        if bool(getattr(settings_obj, "LIVE_REAL_ORDER_ARMED", False)) and not bool(
            getattr(settings_obj, "LIVE_DRY_RUN", True)
        ):
            return "live_real_order"
        return "live_dry_run"
    if str(getattr(settings_obj, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip():
        return "promotion"
    if str(getattr(settings_obj, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip():
        return "promotion"
    return "paper_legacy"


def _fallback_source_hash(source: str, payload: Mapping[str, object]) -> str:
    return sha256_prefixed(
        {
            "paper_legacy_compat": True,
            "fallback_source": source,
            "fallback_payload": dict(payload),
        }
    )


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _resolve_plugin_or_none(strategy_name: str) -> object | None:
    try:
        return resolve_research_strategy_plugin(strategy_name)
    except ResearchStrategyRegistryError:
        return None


def _plugin_accepts_empty_runtime_parameters(plugin: object | None) -> bool:
    if plugin is None:
        return False
    capabilities = getattr(plugin, "runtime_capabilities", None)
    spec = getattr(plugin, "spec", None)
    return (
        capabilities is not None
        and spec is not None
        and bool(getattr(capabilities, "runtime_decision_supported", False))
        and not bool(getattr(capabilities, "approved_profile_required", True))
        and not bool(getattr(capabilities, "live_dry_run_allowed", False))
        and not bool(getattr(capabilities, "live_real_order_allowed", False))
        and getattr(plugin, "runtime_parameter_adapter", None) is not None
        and not tuple(getattr(spec, "accepted_parameter_names", ()) or ())
    )


SUPPORTED_RUNTIME_SCOPE = "multi_strategy_single_pair_single_interval"
RUNTIME_SCOPE_DESCRIPTION = "multi-strategy / single-pair / single-interval runtime"
RUNTIME_SCOPE_MODE = "single_pair"
UNSUPPORTED_MULTI_PAIR_SCOPE = "multi_pair_portfolio"
UNSUPPORTED_MULTI_INTERVAL_SCOPE = "multi_interval_runtime"
MULTI_PAIR_RUNTIME_UNSUPPORTED_REASON = "multi_pair_runtime_unsupported"
SINGLE_INTERVAL_RUNTIME_UNSUPPORTED_REASON = "single_interval_runtime_unsupported"
MULTI_PAIR_REQUIRED_BEFORE_ENABLEMENT = (
    "pair-scoped runtime shards",
    "pair-specific target state",
    "pair-specific runtime data preflight",
    "pair-specific strategy decision bundles or pair-scoped bundle partitioning",
    "pair-specific allocation targets",
    "pair-specific execution plans",
    "pair-specific submit/reconcile loops",
    "cross-pair risk budget semantics",
    "currency-scoped portfolio/accounting ledger or equivalent multi-asset accounting model",
)


def runtime_scope_contract() -> dict[str, object]:
    return {
        "runtime_scope": RUNTIME_SCOPE_DESCRIPTION,
        "runtime_scope_mode": RUNTIME_SCOPE_MODE,
        "supported_runtime_scope": SUPPORTED_RUNTIME_SCOPE,
        "blocked_layer": "runtime_scope_validation",
        "required_migration": "RuntimeScopeV2",
        "target_position_state_scope": "pair_only",
        "execution_plan_scope": "single_target",
        "portfolio_ledger_scope": "single_asset",
        "unsupported_runtime_scope": [
            UNSUPPORTED_MULTI_PAIR_SCOPE,
            UNSUPPORTED_MULTI_INTERVAL_SCOPE,
        ],
        "single_pair_runtime_enforced": True,
        "single_interval_runtime_enforced": True,
        "multi_pair_portfolio_supported": False,
        "multi_interval_runtime_supported": False,
        "multiple_execution_targets_supported": False,
        "multi_pair_portfolio_fail_closed_reason": MULTI_PAIR_RUNTIME_UNSUPPORTED_REASON,
        "multi_interval_runtime_fail_closed_reason": SINGLE_INTERVAL_RUNTIME_UNSUPPORTED_REASON,
    }


def _settings_for_authority_context(
    authority_context: "ProfileAuthorityContext",
    *,
    fallback: object = settings,
) -> object:
    updates: dict[str, object] = {}
    if authority_context.runtime_mode:
        updates["MODE"] = authority_context.runtime_mode
    if authority_context.live_dry_run is not None:
        updates["LIVE_DRY_RUN"] = bool(authority_context.live_dry_run)
    if authority_context.live_real_order_armed is not None:
        updates["LIVE_REAL_ORDER_ARMED"] = bool(authority_context.live_real_order_armed)
    return replace(fallback, **updates) if updates else fallback


@dataclass(frozen=True)
class RuntimeStrategySpec:
    strategy_name: str
    strategy_instance_id: str | None = None
    enabled: bool = True
    pair: str | None = None
    interval: str | None = None
    priority: int = 100
    weight: float = 1.0
    desired_exposure_krw: float | None = None
    max_target_exposure_krw: float | None = None
    risk_budget_krw: float | None = None
    risk_policy: Mapping[str, object] | None = None
    risk_policy_hash: str | None = None
    risk_snapshot: Mapping[str, object] | None = None
    parameters: Mapping[str, object] | None = None
    runtime_adapter_config: Mapping[str, object] | None = None
    source_audit: Mapping[str, object] | None = None
    approved_profile_path: str | None = None
    approved_profile_hash: str | None = None
    parameter_source: str | None = None
    runtime_contract_hash: str | None = None
    strategy_version: str | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        name = str(self.strategy_name or "").strip().lower()
        if not name:
            raise ValueError("runtime_strategy_name_missing")
        pair = str(self.pair or getattr(settings, "PAIR", "") or "").strip()
        if not pair:
            raise ValueError("runtime_strategy_pair_missing")
        interval = str(self.interval or getattr(settings, "INTERVAL", "") or "").strip()
        if not interval:
            raise ValueError("runtime_strategy_interval_missing")
        weight = float(self.weight)
        if weight <= 0.0:
            raise ValueError("runtime_strategy_weight_must_be_positive")
        risk_budget = _optional_float(self.risk_budget_krw)
        max_target_exposure = _optional_float(self.max_target_exposure_krw)
        if risk_budget is not None and risk_budget < 0.0:
            raise ValueError("runtime_strategy_risk_budget_must_be_non_negative")
        if max_target_exposure is not None and max_target_exposure < 0.0:
            raise ValueError("runtime_strategy_max_target_exposure_must_be_non_negative")
        raw_desired_exposure = self.desired_exposure_krw
        if raw_desired_exposure is None and (self.pair is None or self.interval is None):
            raw_desired_exposure = getattr(settings, "TARGET_EXPOSURE_KRW", None)
            if raw_desired_exposure is None:
                raw_desired_exposure = getattr(settings, "MAX_ORDER_KRW", None)
        desired_exposure = _optional_float(raw_desired_exposure)
        if desired_exposure is not None and desired_exposure < 0.0:
            raise ValueError("runtime_strategy_desired_exposure_must_be_non_negative")
        object.__setattr__(self, "strategy_name", name)
        object.__setattr__(self, "pair", pair)
        object.__setattr__(self, "interval", interval)
        if self.source_audit is not None:
            object.__setattr__(self, "source_audit", dict(self.source_audit))
        elif self.pair is None or self.interval is None:
            object.__setattr__(
                self,
                "source_audit",
                {
                    "legacy_compatibility_used": True,
                    "paper_legacy_compat": True,
                    "pair_source": "settings.PAIR",
                    "interval_source": "settings.INTERVAL",
                    "market_scope_source": "settings",
                    "fallback_source_hash": _fallback_source_hash(
                        "runtime_strategy_spec_settings_market_scope",
                        {
                            "strategy_name": name,
                            "pair": pair,
                            "interval": interval,
                        },
                    ),
                },
            )
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "weight", weight)
        object.__setattr__(self, "desired_exposure_krw", desired_exposure)
        object.__setattr__(self, "max_target_exposure_krw", max_target_exposure)
        object.__setattr__(self, "risk_budget_krw", risk_budget)
        object.__setattr__(
            self,
            "risk_policy",
            MappingProxyType({str(key): value for key, value in dict(self.risk_policy or {}).items()})
            if self.risk_policy is not None
            else None,
        )
        object.__setattr__(
            self,
            "risk_snapshot",
            MappingProxyType({str(key): value for key, value in dict(self.risk_snapshot or {}).items()})
            if self.risk_snapshot is not None
            else None,
        )
        object.__setattr__(
            self,
            "parameters",
            MappingProxyType({str(key): value for key, value in dict(self.parameters or {}).items()}),
        )
        if self.approved_profile_path is not None:
            object.__setattr__(self, "approved_profile_path", str(self.approved_profile_path).strip() or None)
        if self.approved_profile_hash is not None:
            object.__setattr__(self, "approved_profile_hash", str(self.approved_profile_hash).strip() or None)
        if self.parameter_source is not None:
            object.__setattr__(self, "parameter_source", str(self.parameter_source).strip() or None)
        if self.runtime_contract_hash is not None:
            object.__setattr__(self, "runtime_contract_hash", str(self.runtime_contract_hash).strip() or None)
        if self.risk_policy_hash is not None:
            object.__setattr__(self, "risk_policy_hash", str(self.risk_policy_hash).strip() or None)
        if self.strategy_version is not None:
            object.__setattr__(self, "strategy_version", str(self.strategy_version).strip() or None)
        if self.strategy_instance_id is not None:
            object.__setattr__(self, "strategy_instance_id", str(self.strategy_instance_id).strip() or None)
        object.__setattr__(
            self,
            "runtime_adapter_config",
            MappingProxyType(
                {str(key): value for key, value in dict(self.runtime_adapter_config or {}).items()}
            ),
        )
        object.__setattr__(
            self,
            "source_audit",
            MappingProxyType({str(key): value for key, value in dict(self.source_audit or {}).items()}),
        )

    def as_dict(self) -> dict[str, object]:
        risk_decision = build_risk_decision_artifact(
            risk_budget_krw=self.risk_budget_krw,
            max_target_exposure_krw=self.max_target_exposure_krw,
            exposure_cap_source="max_target_exposure_krw"
            if self.max_target_exposure_krw is not None
            else "none",
            decision_context="runtime_strategy_spec",
        )
        return {
            "schema_version": int(self.schema_version),
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_name": self.strategy_name,
            "enabled": bool(self.enabled),
            "pair": self.pair,
            "interval": self.interval,
            "priority": int(self.priority),
            "weight": float(self.weight),
            "desired_exposure_krw": self.desired_exposure_krw,
            "max_target_exposure_krw": self.max_target_exposure_krw,
            "risk_budget_krw": self.risk_budget_krw,
            "strategy_risk_policy": (
                None if self.risk_policy is None else dict(self.risk_policy)
            ),
            "strategy_risk_policy_hash": self.risk_policy_hash,
            "strategy_risk_snapshot": (
                None if self.risk_snapshot is None else dict(self.risk_snapshot)
            ),
            "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
            "exposure_boundary_artifact": risk_decision,
            "exposure_boundary_artifact_hash": risk_decision["exposure_boundary_artifact_hash"],
            "legacy_non_authoritative_exposure_risk_decision": risk_decision,
            "legacy_non_authoritative_exposure_risk_decision_hash": risk_decision[
                "exposure_boundary_artifact_hash"
            ],
            "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
            "parameters": dict(self.parameters or {}),
            "runtime_adapter_config": dict(self.runtime_adapter_config or {}),
            "source_audit": dict(self.source_audit or {}),
            "approved_profile_path": self.approved_profile_path,
            "approved_profile_hash": self.approved_profile_hash,
            "parameter_source": self.parameter_source,
            "runtime_contract_hash": self.runtime_contract_hash,
            "strategy_version": self.strategy_version,
        }


@dataclass(frozen=True)
class RuntimeMarketScope:
    mode: str = "single_pair"
    pair: str | None = None
    interval: str | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        mode = str(self.mode or "").strip().lower() or "single_pair"
        if mode not in {"single_pair", "multi_pair_portfolio"}:
            raise ValueError(f"runtime_market_scope_mode_unsupported:{mode}")
        pair = str(self.pair or "").strip()
        interval = str(self.interval or "").strip()
        if not pair:
            raise ValueError("runtime_market_scope_pair_missing")
        if not interval:
            raise ValueError("runtime_market_scope_interval_missing")
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "pair", pair)
        object.__setattr__(self, "interval", interval)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "mode": self.mode,
            "pair": self.pair,
            "interval": self.interval,
            **runtime_scope_contract(),
        }


@dataclass(frozen=True)
class ParameterAuthority:
    raw_parameters: Mapping[str, object]
    materialized_parameters: Mapping[str, object]
    parameter_source: str
    approved_profile_path: str | None
    approved_profile_hash: str | None
    strategy_parameters_hash: str
    source_audit_metadata: Mapping[str, object]
    legacy_compatibility_used: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "raw_parameters",
            MappingProxyType({str(key): value for key, value in dict(self.raw_parameters).items()}),
        )
        object.__setattr__(
            self,
            "materialized_parameters",
            MappingProxyType(
                {str(key): value for key, value in dict(self.materialized_parameters).items()}
            ),
        )
        object.__setattr__(
            self,
            "source_audit_metadata",
            MappingProxyType(
                {str(key): value for key, value in dict(self.source_audit_metadata).items()}
            ),
        )


def _runtime_strategy_identity_hash(payload: Mapping[str, object]) -> str:
    return sha256_prefixed({"runtime_strategy_instance_identity": dict(payload)})


def derive_strategy_instance_id(
    spec: RuntimeStrategySpec,
    *,
    strategy_parameters_hash: str | None = None,
) -> str:
    explicit = str(spec.strategy_instance_id or "").strip()
    if explicit:
        return explicit
    parameter_identity_hash = (
        str(spec.approved_profile_hash or "").strip()
        or str(strategy_parameters_hash or "").strip()
    )
    if not parameter_identity_hash:
        try:
            parameter_identity_hash = materialized_strategy_parameters_hash(
                materialize_strategy_parameters(spec.strategy_name, dict(spec.parameters or {}))
            )
        except Exception:
            parameter_identity_hash = materialized_strategy_parameters_hash(dict(spec.parameters or {}))
    digest = _runtime_strategy_identity_hash(
        {
            "strategy_name": spec.strategy_name,
            "pair": spec.pair,
            "interval": spec.interval,
            "parameter_identity_hash": parameter_identity_hash,
        }
    )
    return (
        f"{spec.strategy_name}:{str(spec.pair).lower()}:"
        f"{str(spec.interval).lower()}:{digest.removeprefix('sha256:')[:16]}"
    )


@dataclass(frozen=True)
class RuntimeStrategyInstance:
    spec: RuntimeStrategySpec
    strategy_instance_id: str
    parameters_raw: Mapping[str, object]
    parameters_materialized: Mapping[str, object]
    strategy_parameters_hash: str
    parameter_source: str
    approved_profile_path: str | None
    approved_profile_hash: str | None
    runtime_contract_hash: str | None
    plugin_contract_hash: str | None
    strategy_version: str | None
    runtime_contract: Mapping[str, object]
    runtime_adapter_config: Mapping[str, object]
    risk_profile: StrategyRiskProfile | None = None
    parameter_authority_audit: Mapping[str, object] | None = None
    profile_authority_context: Mapping[str, object] | None = None
    legacy_compatibility_used: bool = False
    schema_version: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "strategy_instance_id", str(self.strategy_instance_id).strip())
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
        object.__setattr__(
            self,
            "runtime_contract",
            MappingProxyType({str(key): value for key, value in dict(self.runtime_contract or {}).items()}),
        )
        object.__setattr__(
            self,
            "runtime_adapter_config",
            MappingProxyType(
                {str(key): value for key, value in dict(self.runtime_adapter_config or {}).items()}
            ),
        )
        object.__setattr__(
            self,
            "profile_authority_context",
            MappingProxyType(
                {str(key): value for key, value in dict(self.profile_authority_context or {}).items()}
            ),
        )

    @property
    def strategy_name(self) -> str:
        return self.spec.strategy_name

    @property
    def pair(self) -> str:
        return str(self.spec.pair)

    @property
    def interval(self) -> str:
        return str(self.spec.interval)

    def as_dict(self) -> dict[str, object]:
        risk_decision = build_risk_decision_artifact(
            risk_budget_krw=self.spec.risk_budget_krw,
            max_target_exposure_krw=self.spec.max_target_exposure_krw,
            exposure_cap_source="max_target_exposure_krw"
            if self.spec.max_target_exposure_krw is not None
            else "none",
            decision_context="runtime_strategy_instance",
        )
        return {
            "schema_version": int(self.schema_version),
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_name": self.strategy_name,
            "pair": self.pair,
            "interval": self.interval,
            "priority": int(self.spec.priority),
            "weight": float(self.spec.weight),
            "desired_exposure_krw": self.spec.desired_exposure_krw,
            "max_target_exposure_krw": self.spec.max_target_exposure_krw,
            "risk_budget_krw": self.spec.risk_budget_krw,
            "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
            "exposure_boundary_artifact": risk_decision,
            "exposure_boundary_artifact_hash": risk_decision["exposure_boundary_artifact_hash"],
            "legacy_non_authoritative_exposure_risk_decision": risk_decision,
            "legacy_non_authoritative_exposure_risk_decision_hash": risk_decision[
                "exposure_boundary_artifact_hash"
            ],
            "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
            "parameter_source": self.parameter_source,
            "parameters_raw": dict(self.parameters_raw),
            "parameters_materialized": dict(self.parameters_materialized),
            "strategy_parameters_hash": self.strategy_parameters_hash,
            "approved_profile_path": self.approved_profile_path,
            "approved_profile_hash": self.approved_profile_hash,
            "runtime_contract_hash": self.runtime_contract_hash,
            "plugin_contract_hash": self.plugin_contract_hash,
            "strategy_version": self.strategy_version,
            "runtime_adapter_config": dict(self.runtime_adapter_config),
            "strategy_risk_profile": (
                None if self.risk_profile is None else self.risk_profile.as_dict()
            ),
            "strategy_risk_profile_hash": (
                None if self.risk_profile is None else self.risk_profile.profile_hash()
            ),
            "strategy_risk_policy_hash": (
                None if self.risk_profile is None else self.risk_profile.risk_policy_hash
            ),
            "parameter_authority_audit": dict(self.parameter_authority_audit or {}),
            "profile_authority_context": dict(self.profile_authority_context or {}),
            "legacy_compatibility_used": bool(self.legacy_compatibility_used),
        }


@dataclass(frozen=True)
class RuntimeStrategySet:
    strategies: tuple[RuntimeStrategySpec, ...]
    source: str
    market_scope: RuntimeMarketScope | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        active = tuple(item for item in self.strategies if item.enabled)
        if not active:
            raise ValueError("runtime_strategy_set_empty")
        scope = self.market_scope
        if scope is None:
            scope = RuntimeMarketScope(
                pair=str(getattr(settings, "PAIR", "")),
                interval=str(getattr(settings, "INTERVAL", "")),
            )
        seen: set[str] = set()
        for item in active:
            key = derive_strategy_instance_id(item)
            if key in seen:
                raise ValueError(f"runtime_strategy_duplicate_instance:{key}")
            seen.add(key)
        object.__setattr__(
            self,
            "strategies",
            tuple(
                sorted(
                    self.strategies,
                    key=lambda item: (str(item.pair), item.priority, derive_strategy_instance_id(item)),
                )
            ),
        )
        object.__setattr__(self, "market_scope", scope)

    @property
    def active_strategies(self) -> tuple[RuntimeStrategySpec, ...]:
        return tuple(item for item in self.strategies if item.enabled)

    @property
    def multi_strategy_enabled(self) -> bool:
        return len(self.active_strategies) > 1

    def spec_for_strategy(self, strategy_name: str) -> RuntimeStrategySpec | None:
        normalized = str(strategy_name or "").strip().lower()
        matches = tuple(spec for spec in self.active_strategies if spec.strategy_name == normalized)
        if len(matches) > 1:
            raise ValueError(f"runtime_strategy_name_ambiguous:{normalized}")
        return matches[0] if matches else None

    def spec_for_instance(self, strategy_instance_id: str) -> RuntimeStrategySpec | None:
        normalized = str(strategy_instance_id or "").strip()
        for spec in self.active_strategies:
            if derive_strategy_instance_id(spec) == normalized or spec.strategy_instance_id == normalized:
                return spec
        return None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "source": self.source,
            "market_scope": self.market_scope.as_dict() if self.market_scope else None,
            **runtime_scope_contract(),
            "multi_strategy_enabled": self.multi_strategy_enabled,
            "strategies": [item.as_dict() for item in self.strategies],
            "active_strategies": [item.as_dict() for item in self.active_strategies],
        }


@dataclass(frozen=True)
class ProfileAuthorityContext:
    selection_kind: Literal["single_strategy", "multi_strategy"]
    runtime_strategy_set_source: str
    require_spec_bound_profile: bool
    allow_global_profile_fallback: bool
    expected_profile_modes: tuple[str, ...] | None = None
    runtime_mode: str | None = None
    live_dry_run: bool | None = None
    live_real_order_armed: bool | None = None
    authority_scope: RuntimeAuthorityScope = "paper_legacy"

    def __post_init__(self) -> None:
        selection_kind = str(self.selection_kind or "").strip()
        if selection_kind not in {"single_strategy", "multi_strategy"}:
            raise ValueError(f"runtime_selection_kind_unsupported:{selection_kind}")
        source = str(self.runtime_strategy_set_source or "").strip()
        if not source:
            raise ValueError("runtime_strategy_set_source_missing")
        if self.require_spec_bound_profile and self.allow_global_profile_fallback:
            raise ValueError("profile_authority_context_ambiguous")
        modes = None
        if self.expected_profile_modes is not None:
            modes = tuple(sorted(str(item).strip() for item in self.expected_profile_modes if str(item).strip()))
        object.__setattr__(self, "selection_kind", selection_kind)
        object.__setattr__(self, "runtime_strategy_set_source", source)
        object.__setattr__(self, "expected_profile_modes", modes)
        if self.runtime_mode is not None:
            object.__setattr__(self, "runtime_mode", str(self.runtime_mode).strip().lower() or None)
        object.__setattr__(self, "authority_scope", normalize_runtime_authority_scope(self.authority_scope))

    @classmethod
    def for_strategy_set(
        cls,
        strategy_set: RuntimeStrategySet,
        *,
        settings_obj: object = settings,
        expected_profile_modes: set[str] | tuple[str, ...] | None = None,
    ) -> "ProfileAuthorityContext":
        selection_kind: Literal["single_strategy", "multi_strategy"] = (
            "multi_strategy" if strategy_set.multi_strategy_enabled else "single_strategy"
        )
        live_mode = str(getattr(settings_obj, "MODE", "") or "").strip().lower() == "live"
        require_spec_bound = live_mode and selection_kind == "multi_strategy"
        modes = None if expected_profile_modes is None else tuple(str(item) for item in expected_profile_modes)
        return cls(
            selection_kind=selection_kind,
            runtime_strategy_set_source=str(strategy_set.source),
            require_spec_bound_profile=require_spec_bound,
            allow_global_profile_fallback=not require_spec_bound,
            expected_profile_modes=modes,
            runtime_mode=str(getattr(settings_obj, "MODE", "") or "").strip().lower() or None,
            live_dry_run=bool(getattr(settings_obj, "LIVE_DRY_RUN", False)),
            live_real_order_armed=bool(getattr(settings_obj, "LIVE_REAL_ORDER_ARMED", False)),
            authority_scope=runtime_authority_scope_from_settings(settings_obj),
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "ProfileAuthorityContext":
        modes = payload.get("expected_profile_modes")
        expected_modes = None
        if isinstance(modes, (list, tuple, set)):
            expected_modes = tuple(str(item) for item in modes)
        return cls(
            selection_kind=str(
                payload.get("selection_kind")
                or payload.get("runtime_selection_kind")
                or "single_strategy"
            ),  # type: ignore[arg-type]
            runtime_strategy_set_source=str(payload.get("runtime_strategy_set_source") or "unknown"),
            require_spec_bound_profile=bool(payload.get("require_spec_bound_profile", False)),
            allow_global_profile_fallback=bool(payload.get("allow_global_profile_fallback", True)),
            expected_profile_modes=expected_modes,
            runtime_mode=str(payload.get("runtime_mode") or "").strip().lower() or None,
            live_dry_run=(
                bool(payload["live_dry_run"]) if "live_dry_run" in payload else None
            ),
            live_real_order_armed=(
                bool(payload["live_real_order_armed"]) if "live_real_order_armed" in payload else None
            ),
            authority_scope=str(payload.get("authority_scope") or "paper_legacy"),  # type: ignore[arg-type]
        )

    @classmethod
    def builder_default(cls) -> "ProfileAuthorityContext":
        return cls(
            selection_kind="single_strategy",
            runtime_strategy_set_source="builder_default",
            require_spec_bound_profile=False,
            allow_global_profile_fallback=True,
            runtime_mode=str(getattr(settings, "MODE", "") or "").strip().lower() or None,
            live_dry_run=bool(getattr(settings, "LIVE_DRY_RUN", False)),
            live_real_order_armed=bool(getattr(settings, "LIVE_REAL_ORDER_ARMED", False)),
            authority_scope=runtime_authority_scope_from_settings(settings),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "selection_kind": self.selection_kind,
            "runtime_selection_kind": self.selection_kind,
            "runtime_strategy_set_source": self.runtime_strategy_set_source,
            "require_spec_bound_profile": bool(self.require_spec_bound_profile),
            "allow_global_profile_fallback": bool(self.allow_global_profile_fallback),
            "expected_profile_modes": (
                None if self.expected_profile_modes is None else list(self.expected_profile_modes)
            ),
            "runtime_mode": self.runtime_mode,
            "live_dry_run": self.live_dry_run,
            "live_real_order_armed": self.live_real_order_armed,
            "authority_scope": self.authority_scope,
            "profile_binding_kind": (
                "spec_bound_approved_profiles"
                if self.require_spec_bound_profile
                else "global_approved_profile_selector_allowed"
            ),
            "runtime_gate_authority": (
                "RUNTIME_STRATEGY_SET_JSON"
                if self.selection_kind == "multi_strategy"
                else "STRATEGY_NAME"
            ),
        }


class RuntimeStrategySetResolver:
    """Resolve the runtime strategy set without changing legacy defaults."""

    def __init__(
        self,
        *,
        env_getter: Callable[[str], str | None] | None = None,
        settings_obj: object = settings,
        authority_scope: RuntimeAuthorityScope | str | None = None,
    ) -> None:
        self._env_getter = env_getter or os.getenv
        self._settings = settings_obj
        self._authority_scope = normalize_runtime_authority_scope(
            str(authority_scope) if authority_scope is not None else runtime_authority_scope_from_settings(settings_obj)
        )

    def resolve(self) -> RuntimeStrategySet:
        raw_json = str(
            self._env_getter("RUNTIME_STRATEGY_SET_JSON")
            or getattr(self._settings, "RUNTIME_STRATEGY_SET_JSON", "")
            or ""
        ).strip()
        if raw_json:
            specs, market_scope, structured_runtime_contract = self._load_json_strategy_set(raw_json)
            return RuntimeStrategySet(
                strategies=tuple(
                    self._spec_from_mapping(
                        item,
                        market_scope=market_scope,
                        structured_runtime_contract=structured_runtime_contract,
                    )
                    for item in specs
                ),
                source="RUNTIME_STRATEGY_SET_JSON",
                market_scope=market_scope,
            )
        raw_active = str(
            self._env_getter("ACTIVE_STRATEGIES")
            or getattr(self._settings, "ACTIVE_STRATEGIES", "")
            or ""
        ).strip()
        if raw_active:
            if self._authority_scope != "paper_legacy":
                raise ValueError(
                    f"runtime_strategy_set_active_strategies_fallback_rejected:{self._authority_scope}"
                )
            return RuntimeStrategySet(
                strategies=tuple(
                    self._default_spec(name.strip())
                    for name in raw_active.split(",")
                    if name.strip()
                ),
                source="ACTIVE_STRATEGIES",
                market_scope=RuntimeMarketScope(
                    pair=str(getattr(self._settings, "PAIR", "")),
                    interval=str(getattr(self._settings, "INTERVAL", "")),
                ),
            )
        return RuntimeStrategySet(
            strategies=(self._default_spec(str(getattr(self._settings, "STRATEGY_NAME", ""))),),
            source="STRATEGY_NAME",
            market_scope=RuntimeMarketScope(
                pair=str(getattr(self._settings, "PAIR", "")),
                interval=str(getattr(self._settings, "INTERVAL", "")),
            ),
        )

    def _load_json_strategy_set(
        self,
        raw_json: str,
    ) -> tuple[list[Mapping[str, object]], RuntimeMarketScope, bool]:
        payload = json.loads(raw_json)
        live_like = (
            str(getattr(self._settings, "MODE", "") or "").strip().lower() == "live"
            or bool(getattr(self._settings, "LIVE_DRY_RUN", False))
            or str(getattr(self._settings, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
            or str(getattr(self._settings, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
        )
        if isinstance(payload, Mapping):
            structured_runtime_contract = True
            strategies_payload = payload.get("strategies", ())
            if "strategies" in payload and not isinstance(strategies_payload, list):
                raise ValueError("runtime_strategy_set_json_must_be_list")
            if "market_scope" not in payload:
                raise ValueError("runtime_strategy_set_market_scope_required")
            raw_scope = payload.get("market_scope", {})
            if raw_scope is not None and not isinstance(raw_scope, Mapping):
                raise ValueError("runtime_market_scope_must_be_object")
            market_scope_payload = raw_scope if isinstance(raw_scope, Mapping) else {}
            payload = strategies_payload
        elif live_like:
            raise ValueError("runtime_strategy_set_json_object_required_for_live_like")
        else:
            structured_runtime_contract = False
            market_scope_payload = {
                "mode": "single_pair",
                "pair": str(getattr(self._settings, "PAIR", "")),
                "interval": str(getattr(self._settings, "INTERVAL", "")),
                "source": "paper_legacy_list_form_settings_fallback",
            }
        if not isinstance(payload, list):
            raise ValueError("runtime_strategy_set_json_must_be_list")
        specs: list[Mapping[str, object]] = []
        for item in payload:
            if not isinstance(item, Mapping):
                raise ValueError("runtime_strategy_set_json_item_must_be_object")
            specs.append(item)
        missing_scope = [
            key for key in ("pair", "interval") if not str(market_scope_payload.get(key) or "").strip()
        ]
        if missing_scope:
            raise ValueError(f"runtime_strategy_set_market_scope_missing:{','.join(missing_scope)}")
        return (
            specs,
            RuntimeMarketScope(
                mode=str(market_scope_payload.get("mode", "single_pair")),
                pair=str(market_scope_payload.get("pair") or ""),
                interval=str(market_scope_payload.get("interval") or ""),
            ),
            structured_runtime_contract,
        )

    def _default_spec(self, strategy_name: str) -> RuntimeStrategySpec:
        target = getattr(self._settings, "TARGET_EXPOSURE_KRW", None)
        target_source = "TARGET_EXPOSURE_KRW"
        if target is None:
            target = getattr(self._settings, "MAX_ORDER_KRW", None)
            target_source = "MAX_ORDER_KRW"
        return RuntimeStrategySpec(
            strategy_name=strategy_name,
            pair=str(getattr(self._settings, "PAIR", "")),
            interval=str(getattr(self._settings, "INTERVAL", "")),
            desired_exposure_krw=_optional_float(target),
            source_audit={
                "authority_scope": self._authority_scope,
                "legacy_compatibility_used": True,
                "paper_legacy_compat": True,
                "pair_source": "settings.PAIR",
                "interval_source": "settings.INTERVAL",
                "market_scope_source": "settings",
                "target_exposure_source": target_source,
                "allocation_target_source": target_source,
                "fallback_source_hash": _fallback_source_hash(
                    "settings_default_spec",
                    {
                        "strategy_name": strategy_name,
                        "pair": str(getattr(self._settings, "PAIR", "")),
                        "interval": str(getattr(self._settings, "INTERVAL", "")),
                        "target_source": target_source,
                        "target": target,
                    },
                ),
            },
        )

    def _spec_from_mapping(
        self,
        payload: Mapping[str, object],
        *,
        market_scope: RuntimeMarketScope,
        structured_runtime_contract: bool,
    ) -> RuntimeStrategySpec:
        if "strategy_name" not in payload and "name" in payload:
            payload = {**dict(payload), "strategy_name": payload["name"]}
        name = str(payload.get("strategy_name", ""))
        explicit_pair = str(payload.get("pair") or "").strip()
        explicit_interval = str(payload.get("interval") or "").strip()
        bind_scope = bool(
            payload.get("bind_market_scope")
            or payload.get("use_market_scope")
            or payload.get("market_scope_bound")
        )
        if structured_runtime_contract:
            pair = explicit_pair or str(market_scope.pair)
            interval = explicit_interval or str(market_scope.interval)
            source_audit = {
                "legacy_compatibility_used": False,
                "authority_scope": self._authority_scope,
                "pair_source": "runtime_strategy_spec.pair" if explicit_pair else "market_scope_binding",
                "interval_source": "runtime_strategy_spec.interval" if explicit_interval else "market_scope_binding",
                "market_scope_source": "RUNTIME_STRATEGY_SET_JSON.market_scope",
                "market_scope_binding_explicit": bool(bind_scope),
                "target_exposure_source": (
                    "runtime_strategy_spec.desired_exposure_krw"
                    if "desired_exposure_krw" in payload
                    else "missing"
                ),
                "allocation_target_source": (
                    "runtime_strategy_spec.desired_exposure_krw"
                    if "desired_exposure_krw" in payload
                    else "missing"
                ),
            }
        else:
            default = self._default_spec(name)
            pair = explicit_pair or str(default.pair)
            interval = explicit_interval or str(default.interval)
            source_audit = {
                **dict(default.source_audit or {}),
                "authority_scope": self._authority_scope,
                "pair_source": "runtime_strategy_spec.pair" if explicit_pair else "settings.PAIR",
                "interval_source": "runtime_strategy_spec.interval" if explicit_interval else "settings.INTERVAL",
                "market_scope_source": "paper_legacy_compatibility",
            }
        default = self._default_spec(name)
        return RuntimeStrategySpec(
            strategy_name=str(payload.get("strategy_name", default.strategy_name)),
            strategy_instance_id=(
                str(payload.get("strategy_instance_id") or payload.get("instance_id") or "").strip()
                or default.strategy_instance_id
            ),
            enabled=bool(payload.get("enabled", default.enabled)),
            pair=pair,
            interval=interval,
            priority=int(payload.get("priority", default.priority)),
            weight=float(payload.get("weight", default.weight)),
            desired_exposure_krw=payload.get("desired_exposure_krw", default.desired_exposure_krw),
            max_target_exposure_krw=payload.get(
                "max_target_exposure_krw",
                payload.get("exposure_cap_krw", default.max_target_exposure_krw),
            ),
            risk_budget_krw=payload.get("risk_budget_krw", default.risk_budget_krw),
            risk_policy=(
                payload.get("risk_policy")
                if isinstance(payload.get("risk_policy"), Mapping)
                else default.risk_policy
            ),
            risk_policy_hash=(
                str(payload.get("risk_policy_hash") or payload.get("strategy_risk_policy_hash") or "").strip()
                or default.risk_policy_hash
            ),
            risk_snapshot=(
                payload.get("risk_snapshot")
                if isinstance(payload.get("risk_snapshot"), Mapping)
                else default.risk_snapshot
            ),
            parameters=payload.get("parameters") if isinstance(payload.get("parameters"), Mapping) else None,
            runtime_adapter_config=(
                payload.get("runtime_adapter_config")
                if isinstance(payload.get("runtime_adapter_config"), Mapping)
                else None
            ),
            source_audit=source_audit,
            approved_profile_path=(
                str(payload.get("approved_profile_path") or payload.get("profile_path") or "").strip()
                or default.approved_profile_path
            ),
            approved_profile_hash=(
                str(payload.get("approved_profile_hash") or payload.get("profile_hash") or "").strip()
                or default.approved_profile_hash
            ),
            parameter_source=(
                str(payload.get("parameter_source") or "").strip()
                or default.parameter_source
            ),
            runtime_contract_hash=(
                str(payload.get("runtime_contract_hash") or "").strip()
                or default.runtime_contract_hash
            ),
            strategy_version=(
                str(payload.get("strategy_version") or "").strip()
                or default.strategy_version
            ),
        )


@dataclass(frozen=True)
class ParameterAuthorityResolver:
    settings_obj: object = settings
    authority_scope: RuntimeAuthorityScope = "paper_legacy"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "authority_scope",
            normalize_runtime_authority_scope(self.authority_scope),
        )

    def resolve(
        self,
        spec: RuntimeStrategySpec,
        *,
        profile: Mapping[str, object] | None,
        approved_profile_path: str | None,
        approved_profile_hash: str | None,
    ) -> ParameterAuthority:
        if profile is not None:
            raw_parameters = dict(profile["strategy_parameters"])
            parameter_source = "approved_profile"
            legacy_used = False
            audit = {
                "authority": "approved_profile",
                "authority_scope": self.authority_scope,
                "parameter_source": parameter_source,
                "approved_profile_path": approved_profile_path,
                "approved_profile_hash": approved_profile_hash,
                "legacy_compatibility_used": False,
            }
        else:
            raw_parameters, parameter_source, legacy_used, audit = self._raw_parameters_for_spec(spec)
        materialized = self._materialize_parameters(spec, raw_parameters)
        strategy_parameters_hash = materialized_strategy_parameters_hash(materialized)
        return ParameterAuthority(
            raw_parameters=raw_parameters,
            materialized_parameters=materialized,
            parameter_source=parameter_source,
            approved_profile_path=approved_profile_path,
            approved_profile_hash=approved_profile_hash,
            strategy_parameters_hash=strategy_parameters_hash,
            source_audit_metadata=audit,
            legacy_compatibility_used=legacy_used,
        )

    def _raw_parameters_for_spec(
        self,
        spec: RuntimeStrategySpec,
    ) -> tuple[dict[str, object], str, bool, dict[str, object]]:
        if spec.parameters:
            source = "runtime_strategy_spec"
            return (
                dict(spec.parameters),
                source,
                False,
                {
                    "authority": "runtime_strategy_spec",
                    "authority_scope": self.authority_scope,
                    "parameter_source": source,
                    "legacy_compatibility_used": False,
                },
            )
        plugin = _resolve_plugin_or_none(spec.strategy_name)
        if _plugin_accepts_empty_runtime_parameters(plugin):
            source = "runtime_strategy_spec"
            return (
                {},
                source,
                False,
                {
                    "authority": "runtime_builtin",
                    "authority_scope": self.authority_scope,
                    "parameter_source": source,
                    "legacy_compatibility_used": False,
                },
            )
        strict = self._strict_runtime_mode()
        raw_json = str(getattr(self.settings_obj, "STRATEGY_PARAMETERS_JSON", "") or "").strip()
        if raw_json:
            if strict:
                raise RuntimeError(
                    f"strict_runtime_rejects_strategy_parameters_json_fallback:{spec.strategy_name}"
                )
            if str(spec.parameter_source or "").strip() not in {"", "paper_legacy_compat"}:
                raise RuntimeError(
                    f"runtime_strategy_parameters_missing:{spec.strategy_name}"
                )
            try:
                payload = json.loads(raw_json)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"strategy_parameters_json_invalid:{exc}") from exc
            if not isinstance(payload, Mapping):
                raise RuntimeError("strategy_parameters_json_must_be_object")
            source = "paper_legacy_compat"
            payload = {str(key): value for key, value in payload.items()}
            return (
                payload,
                source,
                True,
                {
                    "authority": "paper_legacy_compat",
                    "authority_scope": self.authority_scope,
                    "parameter_source": source,
                    "legacy_fallback": "STRATEGY_PARAMETERS_JSON",
                    "legacy_compatibility_used": True,
                    "paper_legacy_compat": True,
                    "fallback_source_hash": _fallback_source_hash(
                        "STRATEGY_PARAMETERS_JSON",
                        payload,
                    ),
                },
            )
        if plugin is not None and plugin.runtime_parameter_adapter is not None:
            if strict:
                raise RuntimeError(
                    "approved_profile_required_for_live_compatible_runtime_strategy:"
                    f"{spec.strategy_name};"
                    f"strict_runtime_rejects_plugin_from_settings_fallback:{spec.strategy_name}"
                )
            source = "paper_legacy_compat"
            payload = dict(plugin.runtime_parameter_adapter.from_settings(self.settings_obj))
            return (
                payload,
                source,
                True,
                {
                    "authority": "paper_legacy_compat",
                    "authority_scope": self.authority_scope,
                    "parameter_source": source,
                    "legacy_fallback": "runtime_parameter_adapter.from_settings",
                    "legacy_compatibility_used": True,
                    "paper_legacy_compat": True,
                    "fallback_source_hash": _fallback_source_hash(
                        "runtime_parameter_adapter.from_settings",
                        payload,
                    ),
                },
            )
        raise RuntimeError(f"runtime_strategy_parameters_missing:{spec.strategy_name}")

    def _materialize_parameters(
        self,
        spec: RuntimeStrategySpec,
        raw_parameters: Mapping[str, object],
    ) -> dict[str, object]:
        plugin = _resolve_plugin_or_none(spec.strategy_name)
        if _plugin_accepts_empty_runtime_parameters(plugin):
            if raw_parameters:
                raise RuntimeError(f"runtime_strategy_parameters_unsupported:{spec.strategy_name}")
            return {}
        raw = {str(key): value for key, value in dict(raw_parameters or {}).items()}
        required_runtime_bound = set(runtime_bound_behavior_parameter_names(spec.strategy_name))
        missing_runtime_bound = sorted(required_runtime_bound - set(raw))
        if missing_runtime_bound:
            raise RuntimeError(
                f"runtime_strategy_parameters_missing_runtime_bound:{spec.strategy_name}:"
                + ",".join(missing_runtime_bound)
            )
        parameters = materialize_strategy_parameters(spec.strategy_name, raw)
        try:
            plugin = resolve_research_strategy_plugin(spec.strategy_name)
        except ResearchStrategyRegistryError as exc:
            raise RuntimeError(f"runtime_strategy_plugin_unsupported:{spec.strategy_name}") from exc
        unexpected = sorted(set(parameters) - set(plugin.spec.accepted_parameter_names))
        if unexpected:
            raise RuntimeError(
                f"runtime_strategy_parameters_unsupported:{spec.strategy_name}:"
                + ",".join(unexpected)
            )
        return parameters

    def _strict_runtime_mode(self) -> bool:
        if self.authority_scope != "paper_legacy":
            return True
        if str(getattr(self.settings_obj, "MODE", "") or "").strip().lower() == "live":
            return True
        if str(getattr(self.settings_obj, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip():
            return True
        if str(getattr(self.settings_obj, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip():
            return True
        return False


@dataclass(frozen=True)
class RuntimeDecisionRequestBuilder:
    settings_obj: object = settings
    require_spec_bound_approved_profile: bool = False
    profile_authority_context: ProfileAuthorityContext | None = None
    authority_scope: RuntimeAuthorityScope | str | None = None

    def _authority_context(self) -> ProfileAuthorityContext:
        if self.profile_authority_context is not None:
            return self.profile_authority_context
        if self.authority_scope is not None:
            scope = normalize_runtime_authority_scope(str(self.authority_scope))
        else:
            scope = runtime_authority_scope_from_settings(self.settings_obj)
        require_spec_bound = bool(self.require_spec_bound_approved_profile)
        if not require_spec_bound:
            context = ProfileAuthorityContext.builder_default()
            return replace(context, authority_scope=scope)
        return ProfileAuthorityContext(
            selection_kind="multi_strategy",
            runtime_strategy_set_source="explicit_builder_requirement",
            require_spec_bound_profile=True,
            allow_global_profile_fallback=False,
            authority_scope=scope,
        )

    def with_authority_context(
        self,
        authority_context: ProfileAuthorityContext,
    ) -> "RuntimeDecisionRequestBuilder":
        return replace(
            self,
            profile_authority_context=authority_context,
            require_spec_bound_approved_profile=authority_context.require_spec_bound_profile,
        )

    def materialize_instance(
        self,
        spec: RuntimeStrategySpec,
    ) -> RuntimeStrategyInstance:
        plugin = resolve_research_strategy_plugin(spec.strategy_name)
        cfg = replace(self.settings_obj, STRATEGY_NAME=spec.strategy_name)
        authority_context = self._authority_context()
        live_like = str(getattr(self.settings_obj, "MODE", "") or "").strip().lower() == "live"
        live_real_order = bool(
            str(getattr(self.settings_obj, "MODE", "") or "").strip().lower() == "live"
            and bool(getattr(self.settings_obj, "LIVE_REAL_ORDER_ARMED", False))
            and not bool(getattr(self.settings_obj, "LIVE_DRY_RUN", True))
        )
        if live_like and spec.risk_snapshot is not None:
            raise RuntimeError(f"static_risk_snapshot_rejected_for_live_authority:{spec.strategy_name}")
        spec_profile_path = str(spec.approved_profile_path or "").strip()
        spec_profile_hash = str(spec.approved_profile_hash or "").strip()
        if authority_context.require_spec_bound_profile:
            if not spec_profile_path:
                raise RuntimeError(
                    f"spec_bound_approved_profile_path_missing_for_runtime_strategy:{spec.strategy_name}"
                )
            if not spec_profile_hash:
                raise RuntimeError(
                    f"spec_bound_approved_profile_hash_missing_for_runtime_strategy:{spec.strategy_name}"
                )
            approved_profile_path = spec_profile_path
        else:
            if not authority_context.allow_global_profile_fallback and not spec_profile_path:
                raise RuntimeError(
                    f"global_profile_fallback_rejected_for_runtime_strategy:{spec.strategy_name}"
                )
            approved_profile_path = (
                spec_profile_path
                or (
                    str(getattr(self.settings_obj, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
                    if authority_context.allow_global_profile_fallback
                    else ""
                )
                or (
                    str(getattr(self.settings_obj, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
                    if authority_context.allow_global_profile_fallback
                    else ""
                )
                or (approved_profile_path_from_env() if authority_context.allow_global_profile_fallback else None)
                or None
            )
        profile = None
        if approved_profile_path:
            profile = load_approved_profile(approved_profile_path)
            profile_hash = str(profile.get(PROFILE_HASH_FIELD) or "")
            if spec.approved_profile_hash and spec.approved_profile_hash != profile_hash:
                raise RuntimeError(
                    f"approved_profile_hash_mismatch_for_runtime_strategy:{spec.strategy_name}"
                )
            approved_profile_hash = profile_hash
        else:
            approved_profile_hash = spec.approved_profile_hash

        authority = ParameterAuthorityResolver(
            settings_obj=self.settings_obj,
            authority_scope=authority_context.authority_scope,
        ).resolve(
            spec,
            profile=profile,
            approved_profile_path=approved_profile_path,
            approved_profile_hash=approved_profile_hash,
        )
        parameters = dict(authority.materialized_parameters)
        strategy_parameters_hash = authority.strategy_parameters_hash
        try:
            settings_runtime_contract = runtime_contract_from_settings(cfg)
        except (ResearchStrategyRegistryError, ApprovedProfileError):
            if not _plugin_accepts_empty_runtime_parameters(plugin):
                raise
            settings_runtime_contract = {
                "schema_version": 1,
                "mode": str(getattr(cfg, "MODE", "")),
                "strategy_name": spec.strategy_name,
                "market": str(spec.pair or getattr(cfg, "PAIR", "")),
                "interval": str(spec.interval or getattr(cfg, "INTERVAL", "")),
                "strategy_parameters": {},
            }
        runtime_contract = dict(settings_runtime_contract)
        runtime_contract["strategy_name"] = spec.strategy_name
        runtime_contract["market"] = str(spec.pair or getattr(cfg, "PAIR", ""))
        runtime_contract["interval"] = str(spec.interval or getattr(cfg, "INTERVAL", ""))
        runtime_contract["strategy_parameters"] = dict(parameters)
        if approved_profile_path and not str(runtime_contract.get("profile_selector") or "").strip():
            runtime_contract["profile_selector"] = approved_profile_path
        if _plugin_accepts_empty_runtime_parameters(plugin):
            runtime_contract["exit_policy"] = dict(plugin.spec.exit_policy_schema)
        else:
            runtime_contract["exit_policy"] = exit_policy_from_parameters(spec.strategy_name, dict(parameters))
        runtime_contract["exit_policy_hash"] = sha256_prefixed(runtime_contract["exit_policy"])

        if profile is not None:
            expected_modes, mode_reason = expected_profile_modes_for_runtime(runtime_contract)
            if expected_modes is not None and len(expected_modes) == 0:
                raise RuntimeError(
                    f"approved_profile_runtime_mode_invalid:{spec.strategy_name}:{mode_reason or 'unknown'}"
                )
            if expected_modes is not None and str(profile.get("profile_mode") or "") not in expected_modes:
                raise RuntimeError(
                    "approved_profile_runtime_parameter_mismatch:"
                    f"{spec.strategy_name}:profile_mode"
                )
            mismatches = diff_profile_to_runtime(
                profile,
                runtime_contract,
                profile_path=approved_profile_path,
            )
            if mismatches:
                fields = ",".join(str(item.get("field") or "unknown") for item in mismatches)
                raise RuntimeError(f"approved_profile_runtime_parameter_mismatch:{spec.strategy_name}:{fields}")

        runtime_contract_hash = spec.runtime_contract_hash or sha256_prefixed(runtime_contract)
        plugin_contract_hash = plugin.contract_hash() if hasattr(plugin, "contract_hash") else None
        strategy_version = spec.strategy_version or getattr(plugin, "version", None)
        strategy_instance_id = derive_strategy_instance_id(
            spec,
            strategy_parameters_hash=strategy_parameters_hash,
        )
        risk_profile = strategy_risk_profile_from_profile_payload(
            strategy_instance_id=strategy_instance_id,
            strategy_name=spec.strategy_name,
            pair=str(spec.pair),
            interval=str(spec.interval),
            profile_payload=profile,
            approved_runtime_profile_path=approved_profile_path,
            approved_runtime_profile_hash=approved_profile_hash,
            inline_risk_policy=spec.risk_policy,
            declared_risk_policy_hash=spec.risk_policy_hash,
            live_like=live_like,
            live_real_order=live_real_order,
        )
        return RuntimeStrategyInstance(
            spec=spec,
            strategy_instance_id=strategy_instance_id,
            parameters_raw=dict(authority.raw_parameters),
            parameters_materialized=parameters,
            strategy_parameters_hash=strategy_parameters_hash,
            parameter_source=authority.parameter_source,
            approved_profile_path=approved_profile_path,
            approved_profile_hash=approved_profile_hash,
            runtime_contract_hash=runtime_contract_hash,
            plugin_contract_hash=plugin_contract_hash,
            strategy_version=strategy_version,
            runtime_contract=runtime_contract,
            runtime_adapter_config=dict(spec.runtime_adapter_config or {}),
            risk_profile=risk_profile,
            parameter_authority_audit=dict(authority.source_audit_metadata),
            profile_authority_context=authority_context.as_dict(),
            legacy_compatibility_used=authority.legacy_compatibility_used,
        )

    def build_for_spec(
        self,
        spec: RuntimeStrategySpec,
        *,
        through_ts_ms: int | None,
    ) -> RuntimeDecisionRequest:
        instance = self.materialize_instance(spec)
        cfg = replace(self.settings_obj, STRATEGY_NAME=spec.strategy_name)
        request_payload = {
            "schema_version": 1,
            "strategy_instance_id": instance.strategy_instance_id,
            "strategy_name": spec.strategy_name,
            "pair": spec.pair,
            "interval": str(spec.interval or getattr(cfg, "INTERVAL", "")),
            "through_ts_ms": through_ts_ms,
            "parameters": dict(instance.parameters_materialized),
            "parameters_raw": dict(instance.parameters_raw),
            "parameters_materialized": dict(instance.parameters_materialized),
            "strategy_parameters_hash": instance.strategy_parameters_hash,
            "approved_profile_path": instance.approved_profile_path,
            "approved_profile_hash": instance.approved_profile_hash,
            "runtime_contract_hash": instance.runtime_contract_hash,
            "plugin_contract_hash": instance.plugin_contract_hash,
            "strategy_version": instance.strategy_version,
            "runtime_strategy_spec": instance.as_dict(),
            "parameter_source": instance.parameter_source,
            "runtime_adapter_config": dict(instance.runtime_adapter_config),
        }
        request_hash = sha256_prefixed(request_payload)
        return RuntimeDecisionRequest(
            strategy_instance_id=instance.strategy_instance_id,
            strategy_name=spec.strategy_name,
            pair=str(spec.pair),
            interval=str(spec.interval or getattr(cfg, "INTERVAL", "")),
            through_ts_ms=through_ts_ms,
            parameters=dict(instance.parameters_materialized),
            parameters_raw=dict(instance.parameters_raw),
            parameters_materialized=dict(instance.parameters_materialized),
            strategy_parameters_hash=instance.strategy_parameters_hash,
            approved_profile_path=instance.approved_profile_path,
            approved_profile_hash=instance.approved_profile_hash,
            runtime_strategy_spec=instance,
            runtime_contract_hash=instance.runtime_contract_hash,
            parameter_source=instance.parameter_source,
            plugin_contract_hash=instance.plugin_contract_hash,
            strategy_version=instance.strategy_version,
            request_hash=request_hash,
        )


@dataclass(frozen=True)
class RuntimeStrategyDecisionResultBundle:
    strategy_set: RuntimeStrategySet
    results: tuple[RuntimeStrategyDecisionResult, ...]
    data_availability_report: RuntimeDataAvailabilityReport | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        if len(self.results) != len(self.strategy_set.active_strategies):
            raise ValueError(
                "runtime_strategy_result_set_mismatch:"
                f"expected={len(self.strategy_set.active_strategies)}:actual={len(self.results)}"
            )
        result_instances = [self._result_instance_id(result) for result in self.results]
        if len(set(result_instances)) != len(result_instances):
            raise ValueError("runtime_strategy_result_duplicate_instance")
        for result in self.results:
            result_instance_id = _result_strategy_instance_id(result)
            result_name = str(getattr(result.decision, "strategy_name", "")).strip().lower()
            spec = self.strategy_set.spec_for_instance(result_instance_id)
            if spec is None:
                spec = self.strategy_set.spec_for_strategy(result_name)
            if spec is None:
                raise ValueError(f"runtime_strategy_result_set_mismatch:extra={result_name}")
            base_context = getattr(result, "base_context", {})
            through_ts = (
                base_context.get("through_ts_ms")
                if isinstance(base_context, Mapping) and "through_ts_ms" in base_context
                else int(result.candle_ts)
            )
            base_context = getattr(result, "base_context", {})
            context_payload = (
                base_context.get("profile_authority_context")
                if isinstance(base_context, Mapping)
                else None
            )
            if isinstance(context_payload, Mapping):
                authority_context = ProfileAuthorityContext.from_mapping(context_payload)
                builder = RuntimeDecisionRequestBuilder(
                    settings_obj=_settings_for_authority_context(authority_context)
                ).with_authority_context(authority_context)
            else:
                authority_context = ProfileAuthorityContext.for_strategy_set(self.strategy_set)
                builder = (
                    RuntimeDecisionRequestBuilder(
                        settings_obj=_settings_for_authority_context(authority_context)
                    ).with_authority_context(authority_context)
                    if authority_context.require_spec_bound_profile
                    else RuntimeDecisionRequestBuilder()
                )
            request = builder.build_for_spec(
                spec,
                through_ts_ms=None if through_ts is None else int(through_ts),
            )
            validate_runtime_decision_result_provenance(result, request)
        candle_ts_values = {int(result.candle_ts) for result in self.results}
        if len(candle_ts_values) != 1:
            raise ValueError("runtime_strategy_results_must_share_candle")
        object.__setattr__(
            self,
            "results",
            tuple(sorted(self.results, key=self._result_instance_id)),
        )

    @property
    def candle_ts(self) -> int:
        return int(self.results[0].candle_ts)

    @property
    def market_price(self) -> float:
        return float(self.results[0].market_price)

    def _result_instance_id(self, result: RuntimeStrategyDecisionResult) -> str:
        explicit = _result_strategy_instance_id(result)
        if explicit != str(getattr(result.decision, "strategy_name", "")).strip().lower():
            return explicit
        spec = self.strategy_set.spec_for_strategy(explicit)
        return derive_strategy_instance_id(spec) if spec is not None else explicit

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "authority_label": "RuntimeStrategyDecisionResultBundle",
            "strategy_set": self.strategy_set.as_dict(),
            "runtime_strategy_set_manifest_hash": runtime_strategy_set_manifest_hash(
                self.strategy_set,
            ),
            "runtime_data_availability_report": (
                None if self.data_availability_report is None else self.data_availability_report.as_dict()
            ),
            "runtime_data_availability_report_hash": (
                None if self.data_availability_report is None else self.data_availability_report.report_hash
            ),
            "result_count": len(self.results),
            "results": [
                _runtime_result_replay_metadata(result)
                for result in self.results
            ],
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


def _result_strategy_instance_id(result: RuntimeStrategyDecisionResult) -> str:
    base_context = getattr(result, "base_context", {})
    if isinstance(base_context, Mapping):
        value = str(base_context.get("strategy_instance_id") or "").strip()
        if value:
            return value
    replay_fingerprint = getattr(result, "replay_fingerprint", {})
    if isinstance(replay_fingerprint, Mapping):
        value = str(replay_fingerprint.get("strategy_instance_id") or "").strip()
        if value:
            return value
    return str(getattr(result.decision, "strategy_name", "")).strip().lower()


_REQUIRED_REQUEST_METADATA_FIELDS = (
    "runtime_decision_request_hash",
    "strategy_instance_id",
    "strategy_parameters_hash",
    "approved_profile_hash",
    "runtime_contract_hash",
    "plugin_contract_hash",
    "through_ts_ms",
)


def _runtime_result_replay_metadata(result: RuntimeStrategyDecisionResult) -> dict[str, object]:
    context = getattr(result, "base_context", {})
    replay = getattr(result, "replay_fingerprint", {})
    base = dict(context) if isinstance(context, Mapping) else {}
    replay_payload = dict(replay) if isinstance(replay, Mapping) else {}
    return {
        "strategy_name": str(getattr(result.decision, "strategy_name", "")).strip().lower(),
        "candle_ts": int(result.candle_ts),
        "runtime_decision_request_hash": base.get("runtime_decision_request_hash"),
        "strategy_instance_id": base.get("strategy_instance_id"),
        "strategy_parameters_hash": base.get("strategy_parameters_hash"),
        "approved_profile_hash": base.get("approved_profile_hash"),
        "runtime_contract_hash": base.get("runtime_contract_hash"),
        "plugin_contract_hash": base.get("plugin_contract_hash"),
        "through_ts_ms": base.get("through_ts_ms"),
        "runtime_data_contract_hash": base.get("runtime_data_contract_hash")
        or replay_payload.get("runtime_data_contract_hash"),
        "provider_contract_hash": base.get("provider_contract_hash")
        or replay_payload.get("provider_contract_hash"),
        "runtime_data_availability_report_hash": base.get("runtime_data_availability_report_hash")
        or replay_payload.get("runtime_data_availability_report_hash"),
        "source_schema_hash": base.get("source_schema_hash")
        or replay_payload.get("source_schema_hash"),
        "feature_snapshot_hash": base.get("feature_snapshot_hash")
        or replay_payload.get("feature_snapshot_hash"),
        "replay_fingerprint_hash": sha256_prefixed(replay_payload),
    }


def runtime_strategy_set_manifest_hash(
    strategy_set: RuntimeStrategySet,
    *,
    data_availability_report: RuntimeDataAvailabilityReport | None = None,
) -> str:
    try:
        return str(
            normalized_runtime_strategy_set_manifest(
                strategy_set=strategy_set,
                data_availability_report=data_availability_report,
            )[
                "runtime_strategy_set_manifest_hash"
            ]
        )
    except (ResearchStrategyRegistryError, RuntimeError, ValueError):
        return sha256_prefixed(
            {
                "schema_version": 1,
                "authority_label": "RuntimeStrategySetManifest",
                "authority_scope": "operator_reproducibility_manifest",
                "fallback": "unmaterialized_strategy_set",
                "strategy_set": strategy_set.as_dict(),
            }
        )


def _metadata_value(payload: Mapping[str, object], field: str) -> object:
    if field == "runtime_decision_request_hash":
        return payload.get(field) or payload.get("request_hash")
    return payload.get(field)


def validate_runtime_decision_result_provenance(
    result: RuntimeStrategyDecisionResult,
    request: RuntimeDecisionRequest,
) -> None:
    if not is_runtime_strategy_decision_result(result):
        raise TypeError(f"typed_runtime_decision_required:{request.strategy_name}")
    result_name = str(getattr(result.decision, "strategy_name", "")).strip().lower()
    if result_name != request.strategy_name:
        raise ValueError("runtime_strategy_result_set_mismatch:strategy_name")
    if request.through_ts_ms is not None and int(result.candle_ts) != int(request.through_ts_ms):
        raise ValueError(f"runtime_strategy_candle_mismatch:{request.strategy_name}")

    base_context = getattr(result, "base_context", {})
    if not isinstance(base_context, Mapping):
        raise ValueError("runtime_decision_request_metadata_missing:base_context")
    expected = request.observability_fields()
    for field in _REQUIRED_REQUEST_METADATA_FIELDS:
        if field not in base_context:
            raise ValueError(f"runtime_decision_request_metadata_missing:{field}")
        if _metadata_value(base_context, field) != expected.get(field):
            raise ValueError(f"runtime_decision_request_metadata_mismatch:{field}")

    replay = getattr(result, "replay_fingerprint", {})
    if not isinstance(replay, Mapping):
        raise ValueError("runtime_decision_request_metadata_missing:replay_fingerprint")
    for field in _REQUIRED_REQUEST_METADATA_FIELDS:
        if field not in replay:
            raise ValueError(f"runtime_decision_request_metadata_missing:replay_fingerprint.{field}")
        if _metadata_value(replay, field) != expected.get(field):
            raise ValueError(f"runtime_decision_request_metadata_mismatch:replay_fingerprint.{field}")
    replay_candle_ts = replay.get("candle_ts")
    if replay_candle_ts is not None and int(replay_candle_ts) != int(result.candle_ts):
        raise ValueError(f"runtime_strategy_candle_mismatch:{request.strategy_name}")


RuntimeDecisionAdapterResolver = Callable[[str], PromotionRuntimeDecisionAdapter | None]


@dataclass(frozen=True)
class RuntimeStrategyDecisionCollector:
    request_builder: RuntimeDecisionRequestBuilder = RuntimeDecisionRequestBuilder()
    adapter_resolver: RuntimeDecisionAdapterResolver = get_runtime_decision_adapter
    requirement_resolver: RuntimeDataRequirementResolver = RuntimeDataRequirementResolver()

    def collect(
        self,
        conn,
        strategy_set: RuntimeStrategySet,
        *,
        through_ts_ms: int | None,
    ) -> RuntimeStrategyDecisionResultBundle | None:
        authority_context = ProfileAuthorityContext.for_strategy_set(
            strategy_set,
            settings_obj=self.request_builder.settings_obj,
        )
        request_builder = self.request_builder.with_authority_context(authority_context)
        prepared: list[tuple[RuntimeStrategySpec, PromotionRuntimeDecisionAdapter, RuntimeDecisionRequest]] = []
        materialized_specs: list[RuntimeStrategySpec] = []
        for spec in strategy_set.active_strategies:
            self._validate_strategy_capability(spec, authority_context)
            adapter = self.adapter_resolver(spec.strategy_name)
            if adapter is None:
                raise production_runtime_strategy_missing_error(spec.strategy_name)
            adapter_name = str(getattr(adapter, "strategy_name", "") or "").strip().lower()
            if adapter_name != spec.strategy_name:
                raise RuntimeError(
                    f"runtime_decision_adapter_name_mismatch:{spec.strategy_name}:{adapter_name}"
                )
            request = request_builder.build_for_spec(spec, through_ts_ms=through_ts_ms)
            if not promotion_adapter_supports_feature_snapshot(adapter):
                raise RuntimeError(f"runtime_decision_feature_snapshot_required:{spec.strategy_name}")
            materialized_spec = replace(
                spec,
                parameters=dict(request.parameters),
                parameter_source=request.parameter_source,
                approved_profile_path=request.approved_profile_path,
                approved_profile_hash=request.approved_profile_hash,
                runtime_contract_hash=request.runtime_contract_hash,
                strategy_version=request.strategy_version,
            )
            materialized_specs.append(materialized_spec)
            prepared.append((materialized_spec, adapter, request))
        materialized_strategy_set = RuntimeStrategySet(
            strategies=tuple(materialized_specs),
            source=strategy_set.source,
            market_scope=strategy_set.market_scope,
        )
        results: list[RuntimeStrategyDecisionResult] = []
        data_provider = SQLiteRuntimeDataProvider(conn, resolver=self.requirement_resolver)
        data_availability_report = data_provider.preflight(
            materialized_strategy_set,
            through_ts_ms=through_ts_ms,
        )
        if not data_availability_report.ok:
            raise RuntimeError(";".join(data_availability_report.reasons))
        for spec, adapter, request in prepared:
            requirements = self.requirement_resolver.resolve_for_strategy_set(
                RuntimeStrategySet(strategies=(spec,), source=strategy_set.source, market_scope=strategy_set.market_scope)
            )
            feature_snapshot = data_provider.snapshot(request, requirements)
            if feature_snapshot is None:
                return None
            feature_snapshot = _project_runtime_feature_snapshot(
                adapter=adapter,
                conn=conn,
                request=request,
                feature_snapshot=feature_snapshot,
            )
            if feature_snapshot is None:
                return None
            result = _decide_with_feature_snapshot(
                adapter=adapter,
                conn=conn,
                request=request,
                feature_snapshot=feature_snapshot,
            )
            if result is None:
                return None
            if not is_runtime_strategy_decision_result(result):
                raise TypeError(f"typed_runtime_decision_required:{spec.strategy_name}")
            _attach_runtime_feature_snapshot_metadata(result, feature_snapshot)
            _attach_runtime_request_metadata(result, request)
            validate_runtime_decision_result_provenance(result, request)
            results.append(result)
        return RuntimeStrategyDecisionResultBundle(
            strategy_set=strategy_set,
            results=tuple(results),
            data_availability_report=data_availability_report,
        )

    def _validate_strategy_capability(
        self,
        spec: RuntimeStrategySpec,
        authority_context: ProfileAuthorityContext,
    ) -> None:
        settings_obj = self.request_builder.settings_obj
        if str(getattr(settings_obj, "MODE", "") or "").strip().lower() != "live":
            return
        if authority_context.selection_kind == "single_strategy":
            validate_live_strategy_selection(replace(settings_obj, STRATEGY_NAME=spec.strategy_name))
            return
        if authority_context.require_spec_bound_profile:
            if not str(spec.approved_profile_path or "").strip():
                raise RuntimeError(
                    f"spec_bound_approved_profile_path_missing_for_runtime_strategy:{spec.strategy_name}"
                )
            if not str(spec.approved_profile_hash or "").strip():
                raise RuntimeError(
                    f"spec_bound_approved_profile_hash_missing_for_runtime_strategy:{spec.strategy_name}"
                )
        from .research.strategy_registry import strategy_runtime_capability_issues

        issues = strategy_runtime_capability_issues(
            spec.strategy_name,
            live_dry_run=bool(getattr(settings_obj, "LIVE_DRY_RUN", False)),
            live_real_order_armed=bool(getattr(settings_obj, "LIVE_REAL_ORDER_ARMED", False)),
            approved_profile_path=str(spec.approved_profile_path or "").strip(),
            require_promotion_runtime=True,
            require_runtime_replay=True,
            require_runtime_decision_adapter=True,
        )
        if issues:
            raise RuntimeError(
                "live_runtime_strategy_capability_validation_failed:"
                f"{spec.strategy_name}:reasons=" + ",".join(issues)
            )


def _decide_with_feature_snapshot(
    *,
    adapter: PromotionRuntimeDecisionAdapter,
    conn: object,
    request: RuntimeDecisionRequest,
    feature_snapshot: RuntimeFeatureSnapshot,
) -> RuntimeStrategyDecisionResult | None:
    del conn
    feature_decider = getattr(adapter, "decide_feature_snapshot", None)
    if callable(feature_decider):
        return feature_decider(request, feature_snapshot)
    raise RuntimeError(f"runtime_decision_feature_snapshot_required:{request.strategy_name}")


@dataclass(frozen=True)
class RuntimeDecisionGateway:
    resolver: RuntimeStrategySetResolver = RuntimeStrategySetResolver()
    collector: RuntimeStrategyDecisionCollector = RuntimeStrategyDecisionCollector()

    def decide_bundle(
        self,
        conn,
        *,
        strategy_set: RuntimeStrategySet | None = None,
        through_ts_ms: int | None,
    ) -> RuntimeStrategyDecisionResultBundle | None:
        resolved = strategy_set or self.resolver.resolve()
        collector = self.collector
        resolver_settings = getattr(self.resolver, "_settings", settings)
        if collector.request_builder.settings_obj is settings and resolver_settings is not settings:
            collector = replace(
                collector,
                request_builder=replace(
                    collector.request_builder,
                    settings_obj=resolver_settings,
                ),
            )
        return collector.collect(
            conn,
            resolved,
            through_ts_ms=through_ts_ms,
        )


def active_runtime_strategy_set() -> RuntimeStrategySet:
    return RuntimeStrategySetResolver().resolve()


def collect_runtime_strategy_decisions(
    conn,
    *,
    through_ts_ms: int | None,
    strategy_set: RuntimeStrategySet | None = None,
) -> RuntimeStrategyDecisionResultBundle | None:
    return RuntimeDecisionGateway().decide_bundle(
        conn,
        strategy_set=strategy_set,
        through_ts_ms=through_ts_ms,
    )


def _stable_settings_hash(settings_obj: object, field_names: tuple[str, ...]) -> str:
    return sha256_prefixed(
        {
            name: getattr(settings_obj, name, None)
            for name in field_names
        }
    )


def execution_config_hash(settings_obj: object = settings) -> str:
    field_names = (
            "EXECUTION_ENGINE",
            "EXECUTION_FILL_REFERENCE_POLICY",
            "EXECUTION_DECISION_GUARD_MS",
            "EXECUTION_MAX_QUOTE_WAIT_MS",
            "EXECUTION_MISSING_QUOTE_POLICY",
            "EXECUTION_MIN_REALITY_LEVEL_FOR_PROMOTION",
            "EXECUTION_ALLOW_SAME_CANDLE_CLOSE_FILL",
            "EXECUTION_QUOTE_SOURCE",
            "EXECUTION_QUOTE_AGE_LIMIT_MS",
            "EXECUTION_TOP_OF_BOOK_REQUIRED",
            "EXECUTION_TOP_OF_BOOK_IS_FULL_DEPTH",
            "EXECUTION_DEPTH_REQUIRED",
            "EXECUTION_TRADE_TICK_REQUIRED",
            "EXECUTION_QUEUE_POSITION_REQUIRED",
            "EXECUTION_MARKET_IMPACT_REQUIRED",
            "EXECUTION_INTRA_CANDLE_PATH_AVAILABLE",
            "EXECUTION_REALITY_LEVEL",
            "EXECUTION_LATENCY_MODEL_TYPE",
            "EXECUTION_LATENCY_MS",
            "EXECUTION_PARTIAL_FILL_MODEL_TYPE",
            "EXECUTION_PARTIAL_FILL_RATE",
            "EXECUTION_ORDER_FAILURE_MODEL_TYPE",
            "EXECUTION_ORDER_FAILURE_RATE",
            "EXECUTION_FEE_SOURCE",
            "EXECUTION_SLIPPAGE_SOURCE",
            "EXECUTION_CALIBRATION_REQUIRED",
            "EXECUTION_CALIBRATION_ARTIFACT_HASH",
    )
    return sha256_prefixed(
        {
            "settings": {name: getattr(settings_obj, name, None) for name in field_names},
            "submit_authority_policy_hash": submit_authority_policy_from_settings(
                settings_obj
            ).content_hash(),
        }
    )


def risk_config_hash(settings_obj: object = settings) -> str:
    return _stable_settings_hash(
        settings_obj,
        (
            "MAX_ORDER_KRW",
            "TARGET_EXPOSURE_KRW",
            "MAX_DAILY_LOSS_KRW",
            "MAX_DAILY_ORDER_COUNT",
            "MAX_ORDERBOOK_SPREAD_BPS",
            "MAX_MARKET_SLIPPAGE_BPS",
            "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS",
            "TARGET_HOLD_POLICY",
            "REQUIRE_BROKER_LOCAL_CONVERGENCE",
            "BLOCK_ON_OPEN_ORDER",
            "BLOCK_ON_SUBMIT_UNKNOWN",
            "RESIDUAL_INVENTORY_MODE",
            "RESIDUAL_LIVE_SELL_MODE",
            "RESIDUAL_BUY_SIZING_MODE",
        ),
    )


def validate_runtime_strategy_set_market_scope(
    strategy_set: RuntimeStrategySet,
    settings_obj: object = settings,
) -> tuple[str, ...]:
    issues: list[str] = []
    settings_pair = str(getattr(settings_obj, "PAIR", "") or "").strip()
    settings_interval = str(getattr(settings_obj, "INTERVAL", "") or "").strip()
    scope = strategy_set.market_scope or RuntimeMarketScope(pair=settings_pair, interval=settings_interval)
    if scope.mode != "single_pair":
        issues.append(f"{MULTI_PAIR_RUNTIME_UNSUPPORTED_REASON}:market_scope_mode={scope.mode}")
    if scope.pair != settings_pair:
        issues.append(
            f"runtime_strategy_pair_mismatch:{MULTI_PAIR_RUNTIME_UNSUPPORTED_REASON}:"
            f"settings_pair={settings_pair}:market_scope_pair={scope.pair}"
        )
    if scope.interval != settings_interval:
        issues.append(
            f"runtime_strategy_interval_mismatch:{SINGLE_INTERVAL_RUNTIME_UNSUPPORTED_REASON}:"
            f"settings_interval={settings_interval}:market_scope_interval={scope.interval}"
        )
    for spec in strategy_set.active_strategies:
        if str(spec.pair) != settings_pair:
            issues.append(
                f"runtime_strategy_pair_mismatch:{spec.strategy_name}:{MULTI_PAIR_RUNTIME_UNSUPPORTED_REASON}:"
                f"settings_pair={settings_pair}:spec_pair={spec.pair}:strategy={spec.strategy_name}"
            )
        if str(spec.interval) != settings_interval:
            issues.append(
                f"runtime_strategy_interval_mismatch:{spec.strategy_name}:{SINGLE_INTERVAL_RUNTIME_UNSUPPORTED_REASON}:"
                f"settings_interval={settings_interval}:spec_interval={spec.interval}:strategy={spec.strategy_name}"
            )
    return tuple(issues)


def validate_runtime_strategy_set_profile_binding(
    strategy_set: RuntimeStrategySet,
    settings_obj: object = settings,
) -> tuple[str, ...]:
    if str(getattr(settings_obj, "MODE", "") or "").strip().lower() != "live":
        return ()
    if not strategy_set.multi_strategy_enabled:
        return ()
    global_profile = (
        str(getattr(settings_obj, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
        or str(getattr(settings_obj, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
        or str(approved_profile_path_from_env() or "").strip()
    )
    issues: list[str] = []
    if global_profile:
        issues.append("global_profile_selector_rejected_for_live_multi_strategy")
    for spec in strategy_set.active_strategies:
        instance_id = derive_strategy_instance_id(spec)
        if not str(spec.approved_profile_path or "").strip():
            issues.append(
                f"{instance_id}:multi_strategy_requires_spec_bound_approved_profile:"
                "live_multi_strategy_requires_spec_bound_approved_profiles:path"
            )
        if not str(spec.approved_profile_hash or "").strip():
            issues.append(
                f"{instance_id}:multi_strategy_requires_spec_bound_approved_profile:"
                "live_multi_strategy_requires_spec_bound_approved_profiles:hash"
            )
    return tuple(issues)


def normalized_runtime_strategy_set_manifest(
    *,
    strategy_set: RuntimeStrategySet | None = None,
    settings_obj: object = settings,
    data_availability_report: RuntimeDataAvailabilityReport | None = None,
) -> dict[str, object]:
    """Return the materialized active strategy-set manifest used by startup linting.

    This is an operator/reporting payload only. It does not create runtime
    artifacts and it does not replace typed request, allocation, or submit-plan
    authority.
    """
    resolved = strategy_set or RuntimeStrategySetResolver(settings_obj=settings_obj).resolve()
    profile_issues = validate_runtime_strategy_set_profile_binding(resolved, settings_obj)
    if profile_issues:
        raise RuntimeError("; ".join(profile_issues))
    market_issues = validate_runtime_strategy_set_market_scope(resolved, settings_obj)
    if market_issues:
        raise RuntimeError("; ".join(market_issues))
    authority_context = ProfileAuthorityContext.for_strategy_set(
        resolved,
        settings_obj=settings_obj,
    )
    builder = RuntimeDecisionRequestBuilder(settings_obj=settings_obj)
    if authority_context.require_spec_bound_profile:
        builder = builder.with_authority_context(authority_context)
    active_instances = tuple(builder.materialize_instance(spec) for spec in resolved.active_strategies)
    live_like_runtime = (
        str(getattr(settings_obj, "MODE", "") or "").strip().lower() == "live"
        or bool(getattr(settings_obj, "LIVE_DRY_RUN", False))
    )
    if live_like_runtime:
        legacy_instances = [
            instance.strategy_instance_id
            for instance in active_instances
            if bool(instance.legacy_compatibility_used)
            or str(instance.parameter_source or "").strip() == "paper_legacy_compat"
        ]
        if legacy_instances:
            raise RuntimeError(
                "runtime_strategy_manifest_legacy_compatibility_rejected:"
                + ",".join(legacy_instances)
            )
        if data_availability_report is not None and (
            data_availability_report.status in {"", "FAIL", "NOT_EVALUATED"}
            or not data_availability_report.ok
        ):
            reasons = ",".join(data_availability_report.reasons) or "runtime_data_preflight_failed"
            raise RuntimeError(f"runtime_data_preflight_gate_failed:{data_availability_report.status}:{reasons}")
    run_start_requests = tuple(
        builder.build_for_spec(spec, through_ts_ms=None)
        for spec in resolved.active_strategies
    )
    run_start_request_hashes = {
        request.strategy_instance_id: request.request_hash
        for request in run_start_requests
    }
    market_scope = resolved.market_scope or RuntimeMarketScope(
        pair=str(getattr(settings_obj, "PAIR", "")),
        interval=str(getattr(settings_obj, "INTERVAL", "")),
    )
    submit_authority_policy = submit_authority_policy_from_settings(settings_obj)
    risk_decision = build_risk_decision_artifact(
        decision_context="runtime_strategy_set_manifest"
    )
    payload = {
        "schema_version": 1,
        "authority_label": "RuntimeStrategySetManifest",
        "authority_scope": "operator_reproducibility_manifest",
        "source": resolved.source,
        "runtime_selection_kind": (
            "multi_strategy" if resolved.multi_strategy_enabled else "single_strategy"
        ),
        "profile_binding_kind": (
            "spec_bound_approved_profiles"
            if authority_context.require_spec_bound_profile
            else "global_approved_profile_selector"
        ),
        "global_profile_selector_present": bool(
            str(getattr(settings_obj, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
            or str(getattr(settings_obj, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
            or str(approved_profile_path_from_env() or "").strip()
        ),
        "startup_gate_authority": (
            "RUNTIME_STRATEGY_SET_JSON"
            if resolved.multi_strategy_enabled
            else "STRATEGY_NAME"
        ),
        "runtime_gate_authority": authority_context.as_dict()["runtime_gate_authority"],
        "runtime_pair": str(getattr(settings_obj, "PAIR", "")),
        "runtime_interval": str(getattr(settings_obj, "INTERVAL", "")),
        **runtime_scope_contract(),
        "unsupported_runtime_scopes": {
            "multi_pair_portfolio": {
                "supported": False,
                "fail_closed_reason": MULTI_PAIR_RUNTIME_UNSUPPORTED_REASON,
                "required_before_enablement": list(MULTI_PAIR_REQUIRED_BEFORE_ENABLEMENT),
            },
            "multi_interval_runtime": {
                "supported": False,
                "fail_closed_reason": SINGLE_INTERVAL_RUNTIME_UNSUPPORTED_REASON,
                "required_before_enablement": [
                    "interval-scoped runtime data preflight",
                    "interval-scoped strategy decision bundles",
                    "interval-scoped allocation and execution planning",
                ],
            },
        },
        **submit_authority_policy.as_dict(),
        "submit_authority_policy_hash": submit_authority_policy.content_hash(),
        "risk_budget_semantics": RISK_BUDGET_SEMANTICS,
        "exposure_boundary_artifact": risk_decision,
        "exposure_boundary_artifact_hash": risk_decision["exposure_boundary_artifact_hash"],
        "legacy_non_authoritative_exposure_risk_decision": risk_decision,
        "legacy_non_authoritative_exposure_risk_decision_hash": risk_decision[
            "exposure_boundary_artifact_hash"
        ],
        "risk_budget_legacy_marker": RISK_BUDGET_LEGACY_MARKER,
        "market_scope": market_scope.as_dict(),
        "multi_strategy_enabled": resolved.multi_strategy_enabled,
        "active_strategy_count": len(active_instances),
        "active_strategy_pairs": sorted({instance.pair for instance in active_instances}),
        "active_strategy_intervals": sorted({instance.interval for instance in active_instances}),
        "active_instances": [
            {
                **instance.as_dict(),
                "runtime_decision_request_hash": run_start_request_hashes[instance.strategy_instance_id],
                "runtime_decision_request_hash_scope": "run_start_blueprint_through_ts_null",
            }
            for instance in active_instances
        ],
        "execution_config_hash": execution_config_hash(settings_obj),
        "risk_config_hash": risk_config_hash(settings_obj),
    }
    payload.update(
        _runtime_data_manifest_evidence(
            resolved,
            data_availability_report=data_availability_report,
        )
    )
    payload["strategy_instance_profile_bindings"] = [
        {
            "strategy_instance_id": instance.strategy_instance_id,
            "approved_profile_path": instance.approved_profile_path,
            "approved_profile_hash": instance.approved_profile_hash,
            "strategy_risk_profile_hash": (
                None if instance.risk_profile is None else instance.risk_profile.profile_hash()
            ),
            "strategy_risk_policy_hash": (
                None if instance.risk_profile is None else instance.risk_profile.risk_policy_hash
            ),
            "strategy_risk_enforcement_mode": (
                None if instance.risk_profile is None else instance.risk_profile.enforcement_mode
            ),
            "plugin_contract_hash": instance.plugin_contract_hash,
            "strategy_parameters_hash": instance.strategy_parameters_hash,
            "runtime_contract_hash": instance.runtime_contract_hash,
        }
        for instance in active_instances
    ]
    payload["runtime_strategy_set_manifest_hash"] = sha256_prefixed(payload)
    return payload


def _runtime_data_manifest_evidence(
    strategy_set: RuntimeStrategySet,
    *,
    data_availability_report: RuntimeDataAvailabilityReport | None,
) -> dict[str, object]:
    requirements = RuntimeDataRequirementResolver().resolve_for_strategy_set(strategy_set)
    if data_availability_report is None:
        return {
            "runtime_data_evidence_scope": "decision_cycle",
            "runtime_data_preflight_required_scope": "decision_cycle",
            "runtime_data_contract_hash": requirements.content_hash(),
            "runtime_data_availability_report_hash": None,
            "provider_contract_hash": runtime_data_provider_contract_hash(),
            "runtime_data_db_schema_fingerprint": None,
            "source_schema_hash": None,
            "runtime_data_status": "cycle_specific",
            "runtime_data_requirements_hash": requirements.content_hash(),
            "coverage_by_strategy": {
                key: {
                    "strategy_name": value.get("strategy_name"),
                    "required": list(value.get("required") or ()),
                    "optional": list(value.get("optional") or ()),
                    "requirements_hash": value.get("requirements_hash"),
                }
                for key, value in requirements.per_strategy.items()
            },
        }
    report = data_availability_report.as_dict()
    return {
        "runtime_data_evidence_scope": "decision_cycle",
        "runtime_data_preflight_required_scope": "decision_cycle",
        "runtime_data_contract_hash": requirements.content_hash(),
        "runtime_data_availability_report_hash": data_availability_report.report_hash,
        "provider_contract_hash": report.get("provider_contract_hash") or runtime_data_provider_contract_hash(),
        "runtime_data_provider_name": report.get("provider_name"),
        "runtime_data_provider_version": report.get("provider_version"),
        "runtime_data_preflight_status": report.get("status"),
        "runtime_data_preflight_reasons": list(report.get("reasons") or []),
        "runtime_data_preflight_warnings": list(report.get("warnings") or []),
        "coverage_by_strategy": dict(report.get("per_strategy_status") or {}),
        "runtime_data_coverage_by_capability": dict(report.get("coverage_by_capability") or {}),
        "runtime_data_source_tables_or_streams": list(report.get("source_tables_or_streams") or []),
        "runtime_data_db_schema_fingerprint": report.get("db_schema_fingerprint"),
        "runtime_data_source_schema_hash": report.get("source_schema_hash"),
        "runtime_data_requirements": requirements.as_dict(),
    }
