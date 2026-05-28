from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Callable, Mapping

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
    runtime_strategy_parameters_from_settings,
)
from .research.strategy_spec import (
    exit_policy_from_parameters,
    materialize_strategy_parameters,
    materialized_strategy_parameters_hash,
    runtime_bound_behavior_parameter_names,
)
from .runtime_strategy_decision import (
    RuntimeDecisionRequest,
    RuntimeStrategyDecisionResult,
    _attach_runtime_request_metadata,
    get_runtime_decision_adapter,
    is_runtime_strategy_decision_result,
    production_runtime_strategy_missing_error,
)


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


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
    risk_budget_krw: float | None = None
    parameters: Mapping[str, object] | None = None
    runtime_adapter_config: Mapping[str, object] | None = None
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
        pair = str(self.pair or settings.PAIR).strip()
        if not pair:
            raise ValueError("runtime_strategy_pair_missing")
        interval = str(self.interval or settings.INTERVAL).strip()
        if not interval:
            raise ValueError("runtime_strategy_interval_missing")
        weight = float(self.weight)
        if weight <= 0.0:
            raise ValueError("runtime_strategy_weight_must_be_positive")
        risk_budget = _optional_float(self.risk_budget_krw)
        if risk_budget is not None and risk_budget < 0.0:
            raise ValueError("runtime_strategy_risk_budget_must_be_non_negative")
        desired_exposure = _optional_float(self.desired_exposure_krw)
        if desired_exposure is not None and desired_exposure < 0.0:
            raise ValueError("runtime_strategy_desired_exposure_must_be_non_negative")
        object.__setattr__(self, "strategy_name", name)
        object.__setattr__(self, "pair", pair)
        object.__setattr__(self, "interval", interval)
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "weight", weight)
        object.__setattr__(self, "desired_exposure_krw", desired_exposure)
        object.__setattr__(self, "risk_budget_krw", risk_budget)
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

    def as_dict(self) -> dict[str, object]:
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
            "risk_budget_krw": self.risk_budget_krw,
            "parameters": dict(self.parameters or {}),
            "runtime_adapter_config": dict(self.runtime_adapter_config or {}),
            "approved_profile_path": self.approved_profile_path,
            "approved_profile_hash": self.approved_profile_hash,
            "parameter_source": self.parameter_source,
            "runtime_contract_hash": self.runtime_contract_hash,
            "strategy_version": self.strategy_version,
        }


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
        return {
            "schema_version": int(self.schema_version),
            "strategy_instance_id": self.strategy_instance_id,
            "strategy_name": self.strategy_name,
            "pair": self.pair,
            "interval": self.interval,
            "priority": int(self.spec.priority),
            "weight": float(self.spec.weight),
            "desired_exposure_krw": self.spec.desired_exposure_krw,
            "risk_budget_krw": self.spec.risk_budget_krw,
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
        }


@dataclass(frozen=True)
class RuntimeStrategySet:
    strategies: tuple[RuntimeStrategySpec, ...]
    source: str
    schema_version: int = 1

    def __post_init__(self) -> None:
        active = tuple(item for item in self.strategies if item.enabled)
        if not active:
            raise ValueError("runtime_strategy_set_empty")
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
            "multi_strategy_enabled": self.multi_strategy_enabled,
            "strategies": [item.as_dict() for item in self.strategies],
            "active_strategies": [item.as_dict() for item in self.active_strategies],
        }


class RuntimeStrategySetResolver:
    """Resolve the runtime strategy set without changing legacy defaults."""

    def __init__(
        self,
        *,
        env_getter: Callable[[str], str | None] | None = None,
        settings_obj: object = settings,
    ) -> None:
        self._env_getter = env_getter or os.getenv
        self._settings = settings_obj

    def resolve(self) -> RuntimeStrategySet:
        raw_json = str(
            self._env_getter("RUNTIME_STRATEGY_SET_JSON")
            or getattr(self._settings, "RUNTIME_STRATEGY_SET_JSON", "")
            or ""
        ).strip()
        if raw_json:
            return RuntimeStrategySet(
                strategies=tuple(self._spec_from_mapping(item) for item in self._load_json_specs(raw_json)),
                source="RUNTIME_STRATEGY_SET_JSON",
            )
        raw_active = str(
            self._env_getter("ACTIVE_STRATEGIES")
            or getattr(self._settings, "ACTIVE_STRATEGIES", "")
            or ""
        ).strip()
        if raw_active:
            return RuntimeStrategySet(
                strategies=tuple(
                    self._default_spec(name.strip())
                    for name in raw_active.split(",")
                    if name.strip()
                ),
                source="ACTIVE_STRATEGIES",
            )
        return RuntimeStrategySet(
            strategies=(self._default_spec(str(getattr(self._settings, "STRATEGY_NAME", ""))),),
            source="STRATEGY_NAME",
        )

    def _load_json_specs(self, raw_json: str) -> list[Mapping[str, object]]:
        payload = json.loads(raw_json)
        if isinstance(payload, Mapping):
            payload = payload.get("strategies", ())
        if not isinstance(payload, list):
            raise ValueError("runtime_strategy_set_json_must_be_list")
        specs: list[Mapping[str, object]] = []
        for item in payload:
            if not isinstance(item, Mapping):
                raise ValueError("runtime_strategy_set_json_item_must_be_object")
            specs.append(item)
        return specs

    def _default_spec(self, strategy_name: str) -> RuntimeStrategySpec:
        target = getattr(self._settings, "TARGET_EXPOSURE_KRW", None)
        if target is None:
            target = getattr(self._settings, "MAX_ORDER_KRW", None)
        return RuntimeStrategySpec(
            strategy_name=strategy_name,
            pair=str(getattr(self._settings, "PAIR", "")),
            interval=str(getattr(self._settings, "INTERVAL", "")),
            desired_exposure_krw=_optional_float(target),
        )

    def _spec_from_mapping(self, payload: Mapping[str, object]) -> RuntimeStrategySpec:
        if "strategy_name" not in payload and "name" in payload:
            payload = {**dict(payload), "strategy_name": payload["name"]}
        default = self._default_spec(str(payload.get("strategy_name", "")))
        return RuntimeStrategySpec(
            strategy_name=str(payload.get("strategy_name", default.strategy_name)),
            strategy_instance_id=(
                str(payload.get("strategy_instance_id") or payload.get("instance_id") or "").strip()
                or default.strategy_instance_id
            ),
            enabled=bool(payload.get("enabled", default.enabled)),
            pair=str(payload.get("pair", default.pair)),
            interval=str(payload.get("interval", default.interval)),
            priority=int(payload.get("priority", default.priority)),
            weight=float(payload.get("weight", default.weight)),
            desired_exposure_krw=payload.get("desired_exposure_krw", default.desired_exposure_krw),
            risk_budget_krw=payload.get("risk_budget_krw", default.risk_budget_krw),
            parameters=payload.get("parameters") if isinstance(payload.get("parameters"), Mapping) else None,
            runtime_adapter_config=(
                payload.get("runtime_adapter_config")
                if isinstance(payload.get("runtime_adapter_config"), Mapping)
                else None
            ),
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
class RuntimeDecisionRequestBuilder:
    settings_obj: object = settings

    def materialize_instance(
        self,
        spec: RuntimeStrategySpec,
    ) -> RuntimeStrategyInstance:
        try:
            plugin = resolve_research_strategy_plugin(spec.strategy_name)
        except (ResearchStrategyRegistryError, ApprovedProfileError):
            if spec.strategy_name != "safe_hold":
                raise
            plugin = None
        cfg = replace(self.settings_obj, STRATEGY_NAME=spec.strategy_name)
        approved_profile_path = (
            spec.approved_profile_path
            or str(getattr(self.settings_obj, "APPROVED_STRATEGY_PROFILE_PATH", "") or "").strip()
            or str(getattr(self.settings_obj, "STRATEGY_APPROVED_PROFILE_PATH", "") or "").strip()
            or approved_profile_path_from_env()
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
            raw_parameters = dict(profile["strategy_parameters"])
            parameter_source = spec.parameter_source or "strict_profile"
        else:
            self._require_profile_for_live_compatible_runtime(spec)
            approved_profile_hash = spec.approved_profile_hash
            raw_parameters, parameter_source = self._parameters_for_spec(spec)

        parameters = self._materialize_parameters(spec, raw_parameters)
        strategy_parameters_hash = materialized_strategy_parameters_hash(parameters)
        try:
            settings_runtime_contract = runtime_contract_from_settings(cfg)
        except (ResearchStrategyRegistryError, ApprovedProfileError):
            if spec.strategy_name != "safe_hold":
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
        if spec.strategy_name == "safe_hold":
            runtime_contract["exit_policy"] = {"schema_version": 1, "rules": (), "strategy_name": "safe_hold"}
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
        return RuntimeStrategyInstance(
            spec=spec,
            strategy_instance_id=strategy_instance_id,
            parameters_raw=raw_parameters,
            parameters_materialized=parameters,
            strategy_parameters_hash=strategy_parameters_hash,
            parameter_source=parameter_source,
            approved_profile_path=approved_profile_path,
            approved_profile_hash=approved_profile_hash,
            runtime_contract_hash=runtime_contract_hash,
            plugin_contract_hash=plugin_contract_hash,
            strategy_version=strategy_version,
            runtime_contract=runtime_contract,
            runtime_adapter_config=dict(spec.runtime_adapter_config or {}),
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

    def _parameters_for_spec(self, spec: RuntimeStrategySpec) -> tuple[dict[str, object], str]:
        if spec.parameters:
            return dict(spec.parameters), spec.parameter_source or "runtime_strategy_spec"
        if spec.strategy_name == "safe_hold":
            return {}, spec.parameter_source or "runtime_builtin_no_parameters"
        return (
            runtime_strategy_parameters_from_settings(spec.strategy_name, self.settings_obj),
            spec.parameter_source or "settings_compat",
        )

    def _materialize_parameters(
        self,
        spec: RuntimeStrategySpec,
        raw_parameters: Mapping[str, object],
    ) -> dict[str, object]:
        if spec.strategy_name == "safe_hold":
            if raw_parameters:
                raise RuntimeError("runtime_strategy_parameters_unsupported:safe_hold")
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

    def _require_profile_for_live_compatible_runtime(self, spec: RuntimeStrategySpec) -> None:
        mode = str(getattr(self.settings_obj, "MODE", "") or "").strip().lower()
        if mode != "live":
            return
        live_dry_run = bool(getattr(self.settings_obj, "LIVE_DRY_RUN", False))
        live_real_order_armed = bool(getattr(self.settings_obj, "LIVE_REAL_ORDER_ARMED", False))
        if live_dry_run or live_real_order_armed:
            raise RuntimeError(
                f"approved_profile_required_for_live_compatible_runtime_strategy:{spec.strategy_name}"
            )


@dataclass(frozen=True)
class RuntimeStrategyDecisionResultBundle:
    strategy_set: RuntimeStrategySet
    results: tuple[RuntimeStrategyDecisionResult, ...]
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
            try:
                base_context = getattr(result, "base_context", {})
                through_ts = (
                    base_context.get("through_ts_ms")
                    if isinstance(base_context, Mapping) and "through_ts_ms" in base_context
                    else int(result.candle_ts)
                )
                request = RuntimeDecisionRequestBuilder().build_for_spec(
                    spec,
                    through_ts_ms=None if through_ts is None else int(through_ts),
                )
            except ResearchStrategyRegistryError:
                validate_runtime_decision_result_bundle_provenance(result, spec)
            else:
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
            "runtime_strategy_set_manifest_hash": runtime_strategy_set_manifest_hash(self.strategy_set),
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
        "replay_fingerprint_hash": sha256_prefixed(replay_payload),
    }


def runtime_strategy_set_manifest_hash(strategy_set: RuntimeStrategySet) -> str:
    try:
        return str(
            normalized_runtime_strategy_set_manifest(strategy_set=strategy_set)[
                "runtime_strategy_set_manifest_hash"
            ]
        )
    except ResearchStrategyRegistryError:
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


def validate_runtime_decision_result_bundle_provenance(
    result: RuntimeStrategyDecisionResult,
    spec: RuntimeStrategySpec,
) -> None:
    if not is_runtime_strategy_decision_result(result):
        raise TypeError(f"typed_runtime_decision_required:{spec.strategy_name}")
    result_name = str(getattr(result.decision, "strategy_name", "")).strip().lower()
    if result_name != spec.strategy_name:
        raise ValueError("runtime_strategy_result_set_mismatch:strategy_name")
    base_context = getattr(result, "base_context", {})
    if not isinstance(base_context, Mapping):
        raise ValueError("runtime_decision_request_metadata_missing:base_context")
    for field in _REQUIRED_REQUEST_METADATA_FIELDS:
        if field not in base_context:
            raise ValueError(f"runtime_decision_request_metadata_missing:{field}")
    instance_id = str(base_context.get("strategy_instance_id") or "").strip()
    if instance_id != derive_strategy_instance_id(spec):
        raise ValueError("runtime_decision_request_metadata_mismatch:strategy_instance_id")
    through_ts_ms = base_context.get("through_ts_ms")
    if through_ts_ms is not None and int(through_ts_ms) != int(result.candle_ts):
        raise ValueError(f"runtime_strategy_candle_mismatch:{spec.strategy_name}")


class RuntimeStrategyDecisionCollector:
    request_builder: RuntimeDecisionRequestBuilder = RuntimeDecisionRequestBuilder()

    def collect(
        self,
        conn,
        strategy_set: RuntimeStrategySet,
        *,
        through_ts_ms: int | None,
    ) -> RuntimeStrategyDecisionResultBundle | None:
        results: list[RuntimeStrategyDecisionResult] = []
        for spec in strategy_set.active_strategies:
            adapter = get_runtime_decision_adapter(spec.strategy_name)
            validate_live_strategy_selection(replace(settings, STRATEGY_NAME=spec.strategy_name))
            if adapter is None:
                raise production_runtime_strategy_missing_error(spec.strategy_name)
            request = self.request_builder.build_for_spec(spec, through_ts_ms=through_ts_ms)
            result = adapter.decide(conn, request)
            if result is None:
                return None
            if not is_runtime_strategy_decision_result(result):
                raise TypeError(f"typed_runtime_decision_required:{spec.strategy_name}")
            _attach_runtime_request_metadata(result, request)
            validate_runtime_decision_result_provenance(result, request)
            results.append(result)
        return RuntimeStrategyDecisionResultBundle(strategy_set=strategy_set, results=tuple(results))


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
        return self.collector.collect(
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


def normalized_runtime_strategy_set_manifest(
    *,
    strategy_set: RuntimeStrategySet | None = None,
    settings_obj: object = settings,
) -> dict[str, object]:
    """Return the materialized active strategy-set manifest used by startup linting.

    This is an operator/reporting payload only. It does not create runtime
    artifacts and it does not replace typed request, allocation, or submit-plan
    authority.
    """
    resolved = strategy_set or RuntimeStrategySetResolver(settings_obj=settings_obj).resolve()
    builder = RuntimeDecisionRequestBuilder(settings_obj=settings_obj)
    active_instances = tuple(builder.materialize_instance(spec) for spec in resolved.active_strategies)
    payload = {
        "schema_version": 1,
        "authority_label": "RuntimeStrategySetManifest",
        "authority_scope": "operator_reproducibility_manifest",
        "source": resolved.source,
        "runtime_pair": str(getattr(settings_obj, "PAIR", "")),
        "single_pair_runtime_enforced": True,
        "multi_strategy_enabled": resolved.multi_strategy_enabled,
        "active_strategy_count": len(active_instances),
        "active_instances": [instance.as_dict() for instance in active_instances],
    }
    payload["runtime_strategy_set_manifest_hash"] = sha256_prefixed(payload)
    return payload
