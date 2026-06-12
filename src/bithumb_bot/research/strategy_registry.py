from __future__ import annotations

from collections.abc import Iterable, Mapping
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
from ..strategy_evidence_contract import DecisionEvidenceContract
from ..strategy_evidence_contract import GENERIC_DECISION_EVIDENCE_CONTRACT
from ..runtime_data_capabilities import normalize_runtime_data_capability


ResearchEventBuilder = Callable[..., Iterable[ResearchDecisionEvent]]
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
RuntimeFeatureSnapshotBuilder = Callable[..., Any]
RuntimeEnvParameterExtractor = Callable[[dict[str, str]], dict[str, Any]]
RuntimeSettingsParameterExtractor = Callable[[object], dict[str, Any]]
DecisionPayloadAdapter = Callable[[dict[str, object], Any], dict[str, object]]
DiagnosticCountBuilder = Callable[[dict[str, object]], dict[str, Any]]
ExitSignalContextBuilder = Callable[[Any], dict[str, object]]
ExitRuleFactory = Callable[
    [
        dict[str, Any],
        dict[str, Any],
        float,
    ],
    list[Any],
]
ExitPolicyMaterializer = Callable[[str, dict[str, Any]], "ExitPolicyMaterialization | Mapping[str, Any]"]
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
RuntimeDataRequirementBuilder = Callable[[object | None], "ResearchStrategyDataRequirements"]


def generic_diagnostics_count_builder(payload: dict[str, object]) -> dict[str, Any]:
    raw_signal = str(payload.get("raw_signal") or "").upper()
    final_signal = str(payload.get("final_signal") or "").upper()
    entry_signal = str(payload.get("entry_signal") or "").upper()
    blocked_filters = tuple(str(item) for item in payload.get("blocked_filters") or ())
    defaults: dict[str, int] = {
        "raw_signal_count": 0,
        "final_signal_count": 0,
        "entry_signal_count": 0,
        "exit_signal_count": 0,
    }
    for blocked_filter in blocked_filters:
        defaults[f"blocked_filter_distribution.{blocked_filter}"] = 0
    entry_reason = str(payload.get("entry_reason") or "").strip()
    exit_reason = str(payload.get("exit_reason") or payload.get("exit_rule") or "").strip()
    if entry_reason:
        defaults[f"entry_reason_distribution.{entry_reason}"] = 0
    if exit_reason:
        defaults[f"exit_reason_distribution.{exit_reason}"] = 0
    counts: dict[str, int] = {}
    if raw_signal in {"BUY", "SELL"}:
        counts["raw_signal_count"] = 1
    if final_signal in {"BUY", "SELL"}:
        counts["final_signal_count"] = 1
    if entry_signal == "BUY":
        counts["entry_signal_count"] = 1
    if str(payload.get("exit_signal") or "").upper() == "SELL":
        counts["exit_signal_count"] = 1
    for blocked_filter in blocked_filters:
        key = f"blocked_filter_distribution.{blocked_filter}"
        counts[key] = counts.get(key, 0) + 1
    if entry_reason and final_signal == "BUY":
        counts[f"entry_reason_distribution.{entry_reason}"] = 1
    if exit_reason and final_signal == "SELL":
        counts[f"exit_reason_distribution.{exit_reason}"] = 1
    return {
        "strategy_diagnostics_namespace": payload.get("strategy_diagnostics_namespace"),
        "strategy_diagnostic_count_defaults": defaults,
        "strategy_diagnostic_counts": counts,
    }


class ResearchStrategyRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class ExitPolicyMaterialization:
    exit_policy: dict[str, Any]
    exit_policy_hash: str
    exit_policy_contract_hash: str
    exit_policy_config: dict[str, Any]
    exit_policy_config_hash: str
    exit_policy_source: str
    exit_policy_materialization_mode: str

    def __post_init__(self) -> None:
        if not isinstance(self.exit_policy, dict):
            raise TypeError("exit_policy_materialization_policy_must_be_dict")
        if not str(self.exit_policy_hash or "").startswith("sha256:"):
            raise ValueError("exit_policy_materialization_hash_missing")
        if not str(self.exit_policy_contract_hash or "").startswith("sha256:"):
            raise ValueError("exit_policy_materialization_contract_hash_missing")
        if not isinstance(self.exit_policy_config, dict):
            raise TypeError("exit_policy_materialization_config_must_be_dict")
        if not str(self.exit_policy_config_hash or "").startswith("sha256:"):
            raise ValueError("exit_policy_materialization_config_hash_missing")
        source = str(self.exit_policy_source or "").strip()
        if not source:
            raise ValueError("exit_policy_materialization_source_missing")
        mode = str(self.exit_policy_materialization_mode or "").strip()
        if not mode:
            raise ValueError("exit_policy_materialization_mode_missing")
        object.__setattr__(self, "exit_policy", dict(self.exit_policy))
        object.__setattr__(self, "exit_policy_config", dict(self.exit_policy_config))
        object.__setattr__(self, "exit_policy_source", source)
        object.__setattr__(self, "exit_policy_materialization_mode", mode)

    def as_dict(self) -> dict[str, Any]:
        return {
            "exit_policy": dict(self.exit_policy),
            "exit_policy_hash": self.exit_policy_hash,
            "exit_policy_contract_hash": self.exit_policy_contract_hash,
            "exit_policy_config": dict(self.exit_policy_config),
            "exit_policy_config_hash": self.exit_policy_config_hash,
            "exit_policy_source": self.exit_policy_source,
            "exit_policy_materialization_mode": self.exit_policy_materialization_mode,
        }


def normalize_exit_policy_materialization(
    result: ExitPolicyMaterialization | Mapping[str, Any],
    *,
    strategy_name: str,
    materializer: ExitPolicyMaterializer | None,
    default_source: str,
    default_mode: str,
) -> ExitPolicyMaterialization:
    if isinstance(result, ExitPolicyMaterialization):
        return result
    if not isinstance(result, Mapping):
        raise TypeError(f"exit_policy_materializer_result_invalid:{strategy_name}")
    exit_policy = result.get("exit_policy")
    if not isinstance(exit_policy, Mapping):
        raise ValueError(f"exit_policy_materializer_policy_missing:{strategy_name}")
    exit_policy_config = result.get("exit_policy_config")
    if not isinstance(exit_policy_config, Mapping):
        exit_policy_config = exit_policy
    source = str(result.get("exit_policy_source") or default_source)
    mode = str(result.get("exit_policy_materialization_mode") or default_mode)
    contract_payload = {
        "schema_version": 1,
        "strategy_name": strategy_name,
        "materializer_module": materializer.__module__ if materializer is not None else None,
        "materializer_qualname": materializer.__qualname__ if materializer is not None else None,
        "exit_policy_source": source,
    }
    return ExitPolicyMaterialization(
        exit_policy=dict(exit_policy),
        exit_policy_hash=str(result.get("exit_policy_hash") or sha256_prefixed(dict(exit_policy))),
        exit_policy_contract_hash=str(
            result.get("exit_policy_contract_hash") or sha256_prefixed(contract_payload)
        ),
        exit_policy_config=dict(exit_policy_config),
        exit_policy_config_hash=str(
            result.get("exit_policy_config_hash") or sha256_prefixed(dict(exit_policy_config))
        ),
        exit_policy_source=source,
        exit_policy_materialization_mode=mode,
    )


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
    lookback_rows: int | None = None
    closed_candle_required: bool = False
    max_age_ms: int | None = None
    min_rows: int | None = None
    lookback_window_ms: int | None = None
    min_density_pct: float | None = None
    freshness_policy: str | None = None

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
        if self.lookback_rows is not None:
            rows = int(self.lookback_rows)
            if rows < 1:
                raise ValueError("data capability lookback_rows must be positive")
            object.__setattr__(self, "lookback_rows", rows)
        for field in ("max_age_ms", "min_rows", "lookback_window_ms"):
            value = getattr(self, field)
            if value is None:
                continue
            normalized_int = int(value)
            if normalized_int < 1:
                raise ValueError(f"data capability {field} must be positive")
            object.__setattr__(self, field, normalized_int)
        if self.min_density_pct is not None:
            density = float(self.min_density_pct)
            if density < 0.0 or density > 100.0:
                raise ValueError("data capability min_density_pct must be between 0 and 100")
            object.__setattr__(self, "min_density_pct", density)

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
        if self.lookback_rows is not None:
            payload["lookback_rows"] = int(self.lookback_rows)
        if self.closed_candle_required:
            payload["closed_candle_required"] = True
        if self.max_age_ms is not None:
            payload["max_age_ms"] = int(self.max_age_ms)
        if self.min_rows is not None:
            payload["min_rows"] = int(self.min_rows)
        if self.lookback_window_ms is not None:
            payload["lookback_window_ms"] = int(self.lookback_window_ms)
        if self.min_density_pct is not None:
            payload["min_density_pct"] = float(self.min_density_pct)
        if self.freshness_policy is not None:
            payload["freshness_policy"] = str(self.freshness_policy)
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
        name = normalize_runtime_data_capability(str(raw_name))
        if name:
            by_name[name] = DataCapabilityRequirement(name=name, required=True)
    for raw_name in optional_data:
        name = normalize_runtime_data_capability(str(raw_name))
        if name and name not in by_name:
            by_name[name] = DataCapabilityRequirement(name=name, required=False)
    for capability in capabilities:
        normalized_name = normalize_runtime_data_capability(capability.name)
        by_name[normalized_name] = DataCapabilityRequirement(
            name=normalized_name,
            required=capability.required,
            min_coverage_pct=capability.min_coverage_pct,
            evidence_level=capability.evidence_level,
            source=capability.source,
            notes=capability.notes,
            lookback_rows=capability.lookback_rows,
            closed_candle_required=capability.closed_candle_required,
            max_age_ms=capability.max_age_ms,
            min_rows=capability.min_rows,
            lookback_window_ms=capability.lookback_window_ms,
            min_density_pct=capability.min_density_pct,
            freshness_policy=capability.freshness_policy,
        )
    return tuple(by_name[name] for name in sorted(by_name))


@dataclass(frozen=True)
class RuntimeParameterAdapter:
    """Legacy runtime parameter compatibility adapter.

    `from_settings` is paper_legacy_compat_only. Promotion, live dry-run, and
    live real-order runtime authority must come from approved profiles or
    structured runtime strategy specs, not settings-derived strategy fields.
    """

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
    accepts_empty_runtime_parameters: bool = False
    fail_closed_reason: str = "strategy_runtime_capability_missing"
    research_supported: bool | None = None
    replay_decisions_supported: bool | None = None
    promotion_export_supported: bool | None = None
    runtime_decision_supported: bool | None = None

    def __post_init__(self) -> None:
        reason = str(self.fail_closed_reason or "").strip().lower()
        if not reason:
            raise ValueError("strategy runtime capability fail_closed_reason must be non-empty")
        object.__setattr__(self, "fail_closed_reason", reason)
        if self.research_supported is None:
            object.__setattr__(self, "research_supported", not bool(self.baseline_only))
        if self.replay_decisions_supported is None:
            object.__setattr__(self, "replay_decisions_supported", bool(self.runtime_replay_supported))
        if self.promotion_export_supported is None:
            object.__setattr__(
                self,
                "promotion_export_supported",
                bool(self.promotion_runtime_decisions_supported),
            )
        if self.runtime_decision_supported is None:
            object.__setattr__(
                self,
                "runtime_decision_supported",
                bool(self.promotion_runtime_decisions_supported),
            )
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
            "research_supported": bool(self.research_supported),
            "replay_decisions_supported": bool(self.replay_decisions_supported),
            "promotion_export_supported": bool(self.promotion_export_supported),
            "runtime_decision_supported": bool(self.runtime_decision_supported),
            "promotion_runtime_decisions_supported": bool(self.promotion_runtime_decisions_supported),
            "runtime_replay_supported": bool(self.runtime_replay_supported),
            "research_only": bool(self.research_only),
            "baseline_only": bool(self.baseline_only),
            "live_dry_run_allowed": bool(self.live_dry_run_allowed),
            "live_real_order_allowed": bool(self.live_real_order_allowed),
            "approved_profile_required": bool(self.approved_profile_required),
            "accepts_empty_runtime_parameters": bool(self.accepts_empty_runtime_parameters),
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
    exit_policy_materializer: ExitPolicyMaterializer | None = None
    research_policy_decision_builder: ResearchPolicyDecisionBuilder | None = None
    research_export_normalizer: ResearchExportNormalizer | None = None
    diagnostics_count_builder: DiagnosticCountBuilder | None = None
    runtime_decision_adapter_factory: Callable[[], Any] | None = None
    runtime_feature_snapshot_builder: RuntimeFeatureSnapshotBuilder | None = None
    single_replay_bundle_builder: SingleReplayBundleBuilder | None = None
    policy_assembly_factory: Callable[[], Any] | None = None
    runtime_capabilities: StrategyRuntimeCapabilities | None = None
    research_runnable: bool = True
    authoring_contract_kind: str = "legacy_research_strategy_plugin"
    promotion_extension_payload: dict[str, Any] | None = None
    decision_evidence_contract: DecisionEvidenceContract = GENERIC_DECISION_EVIDENCE_CONTRACT
    runtime_data_requirement_builder: RuntimeDataRequirementBuilder | None = None

    def __post_init__(self) -> None:
        if self.runtime_capabilities is None:
            raise ValueError(f"strategy runtime capabilities must be explicit: {self.name}")
        if self.diagnostics_count_builder is None:
            object.__setattr__(self, "diagnostics_count_builder", generic_diagnostics_count_builder)
        decision_contract_version = str(self.decision_contract_version or "").strip()
        if not decision_contract_version:
            raise ValueError(f"strategy decision contract version missing: {self.name}")
        if bool(self.research_runnable) and self.research_event_builder is None:
            raise ValueError(f"research event builder missing: {self.name}")
        if self.runtime_capabilities.runtime_replay_supported != (self.runtime_replay_builder is not None):
            raise ValueError(f"strategy runtime replay capability mismatch: {self.name}")
        if self.runtime_capabilities.promotion_runtime_decisions_supported:
            if self.runtime_decision_adapter_factory is None:
                raise ValueError(f"strategy promotion runtime capability missing adapter: {self.name}")
            if self.policy_assembly_factory is None:
                raise ValueError(f"strategy promotion runtime capability missing policy assembly: {self.name}")
        if self.runtime_capabilities.live_dry_run_allowed and self.runtime_decision_adapter_factory is None:
            raise ValueError(f"strategy live dry-run capability missing adapter: {self.name}")
        if self.runtime_capabilities.live_real_order_allowed and self.runtime_decision_adapter_factory is None:
            raise ValueError(f"strategy live real-order capability missing adapter: {self.name}")
        if not isinstance(self.decision_evidence_contract, DecisionEvidenceContract):
            raise TypeError(f"strategy decision evidence contract invalid: {self.name}")
        if self.runtime_capabilities.live_real_order_allowed:
            _validate_live_real_order_evidence_contract(
                strategy_name=self.name,
                capabilities=self.runtime_capabilities,
                contract=self.decision_evidence_contract,
            )
        elif self.runtime_capabilities.live_dry_run_allowed:
            evidence_payload = self.decision_evidence_contract.payload_without_hash()
            if (
                not bool(evidence_payload["requires_decision_input_bundle"])
                and not evidence_payload["required_promotion_provenance_fields"]
            ):
                raise ValueError(f"strategy live-eligible decision evidence contract missing: {self.name}")
        if (
            (self.runtime_capabilities.live_dry_run_allowed or self.runtime_capabilities.live_real_order_allowed)
            and self.exit_rule_factory is not None
            and self.exit_policy_materializer is None
        ):
            raise ValueError(f"strategy live-eligible exit policy materializer missing: {self.name}")
        if (
            self.runtime_capabilities.promotion_runtime_decisions_supported
            and _exit_policy_schema_has_strategy_owned_rules(self.spec.exit_policy_schema)
            and self.exit_policy_materializer is None
        ):
            raise ValueError(f"strategy promotion exit policy materializer missing: {self.name}")

    def contract_payload(self) -> dict[str, Any]:
        data_requirements = ResearchStrategyDataRequirements(
            required_data=self.required_data,
            optional_data=self.optional_data,
        )
        authoring_level = _authoring_level_for_contract_kind(self.authoring_contract_kind)
        operational_capability = _operational_capability_for_plugin(self)
        operator_verdict = _operator_verdict_for_plugin(self, operational_capability)
        parameter_authority = _parameter_authority_summary_for_plugin(self)
        runtime_scope = _supported_runtime_scope_payload()
        legacy_authoring_alias = (
            "level_3_live_eligible" if authoring_level == "level_3_promotion_grade" else None
        )
        return {
            "name": self.name,
            "strategy_name": self.name,
            "version": self.version,
            "strategy_spec_hash": self.spec.spec_hash(),
            "required_data": list(self.required_data),
            "optional_data": list(self.optional_data),
            "runtime_data_requirements": data_requirements.capability_contract_payload(),
            "data_capability_contract": data_requirements.capability_contract_payload(),
            "runtime_data_requirement_builder_supported": self.runtime_data_requirement_builder is not None,
            "diagnostics_contract": {
                "schema_version": 1,
                "strategy_diagnostic_count_defaults_supported": self.diagnostics_count_builder is not None,
                "strategy_diagnostic_counts_supported": self.diagnostics_count_builder is not None,
                "strategy_diagnostics_namespace": self.diagnostics_namespace,
                "minimum_fields": [
                    "strategy_diagnostic_count_defaults",
                    "strategy_diagnostic_counts",
                    "strategy_diagnostics_namespace",
                    "raw_signal_count",
                    "final_signal_count",
                    "entry_signal_count",
                    "blocked_filter_distribution",
                ],
            },
            "runtime_feature_snapshot_builder_supported": self.runtime_feature_snapshot_builder is not None,
            "runtime_data_requirement_builder_module": (
                self.runtime_data_requirement_builder.__module__
                if self.runtime_data_requirement_builder is not None
                else None
            ),
            "runtime_data_requirement_builder_qualname": (
                self.runtime_data_requirement_builder.__qualname__
                if self.runtime_data_requirement_builder is not None
                else None
            ),
            "decision_evidence_contract": self.decision_evidence_contract.as_dict(),
            "behavior_affecting_parameter_names": list(self.spec.behavior_affecting_parameter_names),
            "runner_module": self.runner.__module__,
            "runner_qualname": self.runner.__qualname__,
            "research_runnable": bool(self.research_runnable),
            "authoring_contract_kind": self.authoring_contract_kind,
            "authoring_level": authoring_level,
            "canonical_authoring_level": authoring_level,
            "legacy_authoring_level_alias": legacy_authoring_alias,
            "capability_level": _capability_level_for_runtime_capabilities(self.runtime_capabilities),
            "operational_capability": operational_capability,
            "operator_verdict": operator_verdict,
            "supported_runtime_scope": runtime_scope,
            "parameter_authority": parameter_authority,
            "legacy_fallback": {
                "present": bool(parameter_authority["legacy_fallback_present"]),
                "sources": list(parameter_authority["legacy_fallback_sources"]),
                "allowed_in_live": False,
                "promotion_live_authority_scope": "forbidden",
            },
            "required_evidence_summary": _required_evidence_summary_for_plugin(self),
            "promotion_grade": self.is_promotion_grade,
            "promotion_eligible": bool(self.runtime_capabilities.promotion_export_supported),
            "runtime_decision_eligible": bool(self.runtime_capabilities.runtime_decision_supported),
            "runtime_decision_supported": bool(operational_capability["runtime_decision_supported"]),
            "live_dry_run_allowed": bool(operational_capability["live_dry_run_allowed"]),
            "live_real_order_allowed": bool(operational_capability["live_real_order_allowed"]),
            "approved_profile_required": bool(self.runtime_capabilities.approved_profile_required),
            "risk_profile_required": bool(
                self.runtime_capabilities.live_dry_run_allowed
                or self.runtime_capabilities.live_real_order_allowed
            ),
            "promotion_evidence_required": bool(self.runtime_capabilities.promotion_export_supported),
            "fail_closed_reason": self.runtime_capabilities.fail_closed_reason,
            "live_eligibility": {
                "dry_run_allowed": bool(self.runtime_capabilities.live_dry_run_allowed),
                "real_order_allowed": bool(self.runtime_capabilities.live_real_order_allowed),
                "approved_profile_required": bool(self.runtime_capabilities.approved_profile_required),
                "fail_closed_reason": self.runtime_capabilities.fail_closed_reason,
            },
            "promotion_extension": self.promotion_extension_payload,
            "promotion_extension_missing_reason": (
                None if self.is_promotion_grade else self.runtime_capabilities.fail_closed_reason
            ),
            "recommended_next_action": (
                _recommended_next_action_for_runtime_capabilities(self.runtime_capabilities)
            ),
            "next_required_action": (
                _recommended_next_action_for_runtime_capabilities(self.runtime_capabilities)
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
            "runtime_parameter_adapter_authority_scope": "paper_legacy_compat_only",
            "strategy_parameters_json_fallback_authority_scope": "paper_legacy_compat_only",
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
            "decision_payload_adapter_authority_scope": (
                "transform_strategy_decision_v2_or_verified_canonical_artifact_only"
                if self.decision_payload_adapter is not None
                else "unsupported"
            ),
            "decision_payload_adapter_module": (
                self.decision_payload_adapter.__module__ if self.decision_payload_adapter is not None else None
            ),
            "decision_payload_adapter_qualname": (
                self.decision_payload_adapter.__qualname__ if self.decision_payload_adapter is not None else None
            ),
            "exit_signal_context_builder_supported": self.exit_signal_context_builder is not None,
            "exit_signal_context_builder_authority_scope": (
                "research_exploratory_compatibility_only"
                if self.exit_signal_context_builder is not None
                else "unsupported"
            ),
            "exit_signal_context_builder_module": (
                self.exit_signal_context_builder.__module__ if self.exit_signal_context_builder is not None else None
            ),
            "exit_signal_context_builder_qualname": (
                self.exit_signal_context_builder.__qualname__
                if self.exit_signal_context_builder is not None
                else None
            ),
            "exit_rule_factory_supported": self.exit_rule_factory is not None,
            "exit_rule_factory_authority_scope": (
                "research_exploratory_compatibility_only"
                if self.exit_rule_factory is not None
                else "unsupported"
            ),
            "exit_rule_factory_module": (
                self.exit_rule_factory.__module__ if self.exit_rule_factory is not None else None
            ),
            "exit_rule_factory_qualname": (
                self.exit_rule_factory.__qualname__ if self.exit_rule_factory is not None else None
            ),
            "exit_policy_materializer_supported": self.exit_policy_materializer is not None,
            "exit_policy_materializer_authority_scope": (
                "promotion_profile_runtime_live_authority"
                if self.exit_policy_materializer is not None
                else "unsupported"
            ),
            "exit_policy_materializer_module": (
                self.exit_policy_materializer.__module__
                if self.exit_policy_materializer is not None
                else None
            ),
            "exit_policy_materializer_qualname": (
                self.exit_policy_materializer.__qualname__
                if self.exit_policy_materializer is not None
                else None
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
                "production_decision_entry": "decide_feature_snapshot(request, feature_snapshot)",
                "db_bound_decision_methods_allowed_in_promotion_live": False,
                "forbidden_production_decision_methods": [
                    "decide(conn, ...)",
                    "decide_database_snapshot(conn, ...)",
                    "project_feature_snapshot(conn, request, feature_snapshot)",
                ],
                "feature_snapshot_projector_signature": "project_feature_snapshot(request, feature_snapshot)",
                "db_bound_feature_snapshot_projector_allowed_in_promotion_live": False,
                "promotion_runtime_decisions_supported": bool(
                    self.runtime_capabilities.promotion_runtime_decisions_supported
                ),
                "runtime_parameter_adapter_required": False,
                "runtime_parameter_adapter_authority_scope": "paper_legacy_compat_only",
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
        if self.promotion_extension_payload is not None and self.authoring_contract_kind in {
            "promotion_grade",
            "live_eligible",
        }:
            return True
        return (
            bool(self.runtime_capabilities.promotion_runtime_decisions_supported)
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


_LIVE_REAL_ORDER_REQUIRED_CONTRACT_FIELDS = frozenset(
    {
        "decision_input_bundle_hash",
        "decision_input_contract_hash",
        "decision_input_bundle_payload_hash",
        "market_feature_hash",
        "final_exit_decision_input_hash",
        "snapshot_projector_version",
        "snapshot_projector_hash",
    }
)

_LIVE_REAL_ORDER_REQUIRED_ONE_OF_GROUPS = frozenset(
    {
        frozenset({"fee_authority_hash", "fee_authority_payload_hash"}),
        frozenset({"order_rules_hash", "order_rules_payload_hash"}),
    }
)


def _validate_live_real_order_evidence_contract(
    *,
    strategy_name: str,
    capabilities: StrategyRuntimeCapabilities,
    contract: DecisionEvidenceContract,
) -> None:
    missing: list[str] = []
    if not bool(capabilities.approved_profile_required):
        missing.append("approved_profile_required")
    if not str(contract.snapshot_projector_contract or "").strip():
        missing.append("snapshot_projector_contract")
    live_fields = set(contract.required_live_real_order_fields)
    missing_fields = sorted(_LIVE_REAL_ORDER_REQUIRED_CONTRACT_FIELDS - live_fields)
    missing.extend(missing_fields)
    declared_groups = frozenset(
        frozenset(group) for group in contract.required_live_real_order_one_of_field_groups
    )
    missing_groups = sorted(
        "|".join(sorted(group))
        for group in _LIVE_REAL_ORDER_REQUIRED_ONE_OF_GROUPS
        if group not in declared_groups
    )
    missing.extend(f"one_of({group})" for group in missing_groups)
    if not live_fields and not declared_groups:
        missing.append("generic_or_empty_contract")
    if missing:
        raise ValueError(
            "strategy_live_real_order_decision_evidence_contract_incomplete:"
            + strategy_name
            + ":"
            + ",".join(sorted(set(missing)))
        )


def _exit_policy_schema_has_strategy_owned_rules(exit_policy_schema: Mapping[str, Any]) -> bool:
    rules = tuple(str(rule).strip().lower() for rule in exit_policy_schema.get("rules") or ())
    common = {"stop_loss", "max_holding_time"}
    return any(rule and rule not in common for rule in rules)


def _authoring_level_for_contract_kind(kind: str) -> str:
    normalized = str(kind or "").strip().lower()
    if normalized == "research_only":
        return "level_1_research_only"
    if normalized == "replay_compatible":
        return "level_2_replay_compatible"
    if normalized in {"promotion_grade", "live_eligible"}:
        return "level_3_promotion_grade"
    return "internal_legacy_normalized"


def _capability_level_for_runtime_capabilities(capabilities: StrategyRuntimeCapabilities) -> str:
    if capabilities.live_dry_run_allowed or capabilities.live_real_order_allowed:
        return "live_eligible"
    if capabilities.runtime_decision_supported:
        return "runtime_decision"
    if capabilities.replay_decisions_supported:
        return "replay_compatible"
    if capabilities.research_supported:
        return "research_only"
    return "unsupported"


def _recommended_next_action_for_runtime_capabilities(
    capabilities: StrategyRuntimeCapabilities,
) -> str:
    if capabilities.live_dry_run_allowed or capabilities.live_real_order_allowed:
        return "none"
    if capabilities.runtime_decision_supported:
        return "none"
    if capabilities.replay_decisions_supported and not capabilities.runtime_decision_supported:
        return "add_live_eligible_contract_for_runtime_or_live"
    if capabilities.research_supported:
        return "promote_strategy_contract"
    return "do_not_promote_runtime_special_case"


def _supported_runtime_scope_payload() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "runtime_scope": "multi-strategy / single-pair / single-interval runtime",
        "supported_runtime_scope": "multi_strategy_single_pair_single_interval",
        "single_pair_runtime_supported": True,
        "single_interval_runtime_supported": True,
        "multi_pair_portfolio_supported": False,
        "multi_interval_runtime_supported": False,
        "unsupported_runtime_scope": [
            "multi_pair_portfolio",
            "multi_interval_runtime",
        ],
        "multi_pair_portfolio_fail_closed_reason": "multi_pair_runtime_unsupported",
        "multi_interval_runtime_fail_closed_reason": "single_interval_runtime_unsupported",
    }


def _parameter_authority_summary_for_plugin(plugin: ResearchStrategyPlugin) -> dict[str, Any]:
    fallback_sources = ["STRATEGY_PARAMETERS_JSON"]
    if plugin.runtime_parameter_adapter is not None:
        fallback_sources.append("runtime_parameter_adapter.from_settings")
    return {
        "schema_version": 1,
        "production_allowed_sources": [
            "approved_profile",
            "runtime_strategy_spec",
        ],
        "promotion_live_allowed_sources": [
            "approved_profile",
            "runtime_strategy_spec",
        ],
        "legacy_fallback_present": True,
        "legacy_fallback_sources": fallback_sources,
        "legacy_fallback_allowed_in_live": False,
        "strategy_parameters_json_authority_scope": "paper_legacy_compat_only",
        "settings_derived_fallback_authority_scope": "paper_legacy_compat_only",
        "approved_profile_required": bool(plugin.runtime_capabilities.approved_profile_required),
    }


def _required_evidence_summary_for_plugin(plugin: ResearchStrategyPlugin) -> dict[str, Any]:
    evidence = plugin.decision_evidence_contract.as_dict()
    return {
        "schema_version": 1,
        "approved_profile_required": bool(plugin.runtime_capabilities.approved_profile_required),
        "runtime_replay_required_for_live": bool(
            plugin.runtime_capabilities.live_dry_run_allowed
            or plugin.runtime_capabilities.live_real_order_allowed
        ),
        "runtime_decision_adapter_required": bool(
            plugin.runtime_capabilities.runtime_decision_supported
            or plugin.runtime_capabilities.live_dry_run_allowed
            or plugin.runtime_capabilities.live_real_order_allowed
        ),
        "policy_assembly_required": bool(plugin.runtime_capabilities.runtime_decision_supported),
        "decision_evidence_contract_hash": evidence["contract_hash"],
        "required_promotion_provenance_fields": list(
            evidence["required_promotion_provenance_fields"]
        ),
        "required_live_real_order_fields": list(evidence["required_live_real_order_fields"]),
        "required_live_real_order_one_of_field_groups": list(
            evidence["required_live_real_order_one_of_field_groups"]
        ),
    }


def _operational_capability_for_plugin(plugin: ResearchStrategyPlugin) -> dict[str, Any]:
    capabilities = plugin.runtime_capabilities
    runtime_decision_supported = (
        bool(capabilities.runtime_decision_supported)
        and plugin.runtime_decision_adapter_factory is not None
        and plugin.policy_assembly_factory is not None
    )
    return {
        "schema_version": 1,
        "research_backtest_supported": bool(capabilities.research_supported)
        and bool(plugin.research_runnable)
        and plugin.research_event_builder is not None,
        "runtime_replay_supported": bool(capabilities.runtime_replay_supported)
        and plugin.runtime_replay_builder is not None,
        "runtime_decision_supported": runtime_decision_supported,
        "live_dry_run_allowed": bool(capabilities.live_dry_run_allowed)
        and runtime_decision_supported,
        "live_real_order_allowed": bool(capabilities.live_real_order_allowed)
        and runtime_decision_supported,
        "approved_profile_required": bool(capabilities.approved_profile_required),
        "capability_level": _capability_level_for_runtime_capabilities(capabilities),
        "fail_closed_reason": capabilities.fail_closed_reason,
    }


def _operator_verdict_for_plugin(
    plugin: ResearchStrategyPlugin,
    operational_capability: dict[str, Any],
) -> dict[str, Any]:
    capabilities = plugin.runtime_capabilities
    targets = {
        "research_backtest": _target_verdict(
            allowed=bool(operational_capability["research_backtest_supported"]),
            blocked_reasons=[f"research_backtest_unsupported_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}"],
            next_required_action="do_not_promote"
            if not bool(capabilities.research_supported)
            else "none",
        ),
        "runtime_replay": _target_verdict(
            allowed=bool(operational_capability["runtime_replay_supported"]),
            blocked_reasons=[f"runtime_replay_unsupported_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}"],
            next_required_action="add_replay_compatible_contract"
            if bool(capabilities.research_supported)
            else "do_not_promote",
        ),
        "runtime_decision": _target_verdict(
            allowed=bool(operational_capability["runtime_decision_supported"]),
            blocked_reasons=_runtime_decision_blocked_reasons(plugin),
            next_required_action="add_live_eligible_contract_for_runtime_or_live",
        ),
        "live_dry_run": _target_verdict(
            allowed=bool(operational_capability["live_dry_run_allowed"]),
            blocked_reasons=_live_target_blocked_reasons(
                plugin,
                live_dry_run=True,
                live_real_order_armed=False,
                require_promotion_runtime=True,
                require_runtime_replay=True,
                require_runtime_decision_adapter=True,
            ),
            next_required_action=(
                "supply_approved_profile"
                if capabilities.live_dry_run_allowed and capabilities.approved_profile_required
                else "add_live_dry_run_capability"
            ),
        ),
        "live_real_order": _target_verdict(
            allowed=bool(operational_capability["live_real_order_allowed"]),
            blocked_reasons=_live_target_blocked_reasons(
                plugin,
                live_dry_run=True,
                live_real_order_armed=True,
                require_promotion_runtime=True,
                require_runtime_replay=True,
                require_runtime_decision_adapter=True,
            ),
            next_required_action=(
                "supply_approved_profile"
                if capabilities.live_real_order_allowed and capabilities.approved_profile_required
                else "add_live_real_order_eligible_contract"
            ),
        ),
    }
    return {
        "schema_version": 1,
        "targets": targets,
    }


def _target_verdict(
    *,
    allowed: bool,
    blocked_reasons: Iterable[str],
    next_required_action: str,
) -> dict[str, Any]:
    reasons = tuple(str(reason) for reason in blocked_reasons if str(reason).strip())
    return {
        "status": "allowed" if allowed else "blocked",
        "allowed": bool(allowed),
        "blocked_reasons": [] if allowed else list(reasons),
        "next_required_action": "none" if allowed else next_required_action,
    }


def _runtime_decision_blocked_reasons(plugin: ResearchStrategyPlugin) -> tuple[str, ...]:
    reasons: list[str] = []
    capabilities = plugin.runtime_capabilities
    if not capabilities.promotion_runtime_decisions_supported:
        reasons.append(f"promotion_runtime_unsupported_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}")
    if plugin.runtime_decision_adapter_factory is None:
        reasons.append(f"runtime_decision_adapter_unsupported_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}")
    if plugin.policy_assembly_factory is None:
        reasons.append(f"policy_assembly_unsupported_for_strategy:{plugin.name}:{capabilities.fail_closed_reason}")
    return tuple(reasons)


def _live_target_blocked_reasons(
    plugin: ResearchStrategyPlugin,
    *,
    live_dry_run: bool,
    live_real_order_armed: bool,
    require_promotion_runtime: bool,
    require_runtime_replay: bool,
    require_runtime_decision_adapter: bool,
) -> tuple[str, ...]:
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
    if (bool(live_dry_run) or bool(live_real_order_armed)) and capabilities.approved_profile_required:
        issues.append(f"approved_profile_required_for_strategy:{plugin.name}")
    return tuple(issues)


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


def research_strategy_data_requirements(
    strategy_name: str,
    *,
    runtime_strategy_spec: object | None = None,
) -> ResearchStrategyDataRequirements:
    if strategy_name == TEST_TOP_OF_BOOK_REQUIRED_STRATEGY:
        return ResearchStrategyDataRequirements(
            required_data=("candles", "top_of_book"),
            capabilities=(
                DataCapabilityRequirement(
                    name="orderbook_top",
                    required=True,
                    min_coverage_pct=100.0,
                    evidence_level="best_bid_ask",
                    source="sqlite_orderbook_top_snapshots",
                    notes="private test hook for required top-of-book preflight",
                    max_age_ms=120_000,
                    freshness_policy="max_age",
                ),
            ),
        )
    plugin = resolve_research_strategy_plugin(strategy_name)
    if plugin.runtime_data_requirement_builder is not None:
        return plugin.runtime_data_requirement_builder(runtime_strategy_spec)
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
    from bithumb_bot.legacy_compat.runtime_parameters import settings_derived_fallback

    parameters = settings_derived_fallback(plugin.runtime_parameter_adapter, cfg).raw_parameters
    _assert_runtime_parameters_accepted(plugin=plugin, parameters=parameters)
    return parameters


def runtime_strategy_parameter_env_keys(strategy_name: str) -> tuple[str, ...]:
    plugin = resolve_research_strategy_plugin(strategy_name)
    if plugin.runtime_parameter_adapter is None:
        return ()
    return tuple(plugin.runtime_parameter_adapter.env_keys)


def _strategy_parameters_json_from_env(env: dict[str, str]) -> dict[str, Any] | None:
    raw = str(env.get("STRATEGY_PARAMETERS_JSON") or "").strip()
    return _paper_legacy_strategy_parameters_json(raw)


def _strategy_parameters_json_from_settings(cfg: object) -> dict[str, Any] | None:
    raw = str(getattr(cfg, "STRATEGY_PARAMETERS_JSON", "") or "").strip()
    return _paper_legacy_strategy_parameters_json(raw)


def _paper_legacy_strategy_parameters_json(raw: str) -> dict[str, Any] | None:
    if not raw:
        return None
    from bithumb_bot.legacy_compat.runtime_parameters import strategy_parameters_json_fallback

    try:
        fallback = strategy_parameters_json_fallback(raw)
    except RuntimeError as exc:
        raise ResearchStrategyRegistryError(str(exc)) from exc
    return None if fallback is None else dict(fallback.raw_parameters)


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
