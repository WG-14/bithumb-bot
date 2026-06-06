from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping, Protocol

from .decision_equivalence import sha256_prefixed
from .runtime_data_capabilities import (
    RUNTIME_DATA_CAPABILITY_NAMES,
    RUNTIME_DATA_CAPABILITY_TABLES,
    normalize_runtime_data_capability,
)
from .research.strategy_registry import (
    DataCapabilityRequirement,
    ResearchStrategyDataRequirements,
    research_strategy_data_requirements,
)


RUNTIME_DATA_PROVIDER_NAME = "sqlite_runtime_data_provider"
RUNTIME_DATA_PROVIDER_VERSION = "1"
RUNTIME_DATA_CONTRACT_SCHEMA_VERSION = 1
SINGLE_INTERVAL_DECISION_CLOCK_POLICY = "single_interval_same_closed_candle_fail_closed_v1"


@dataclass(frozen=True)
class DecisionClockPolicy:
    policy_name: str = SINGLE_INTERVAL_DECISION_CLOCK_POLICY
    supported_interval_mode: str = "single_interval"
    mixed_interval_policy: str = "fail_closed"
    mixed_interval_fail_closed_reason: str = "single_interval_runtime_unsupported"
    freshness_policy: str = "per_scope_required_capabilities_fail_closed"
    stale_or_missing_scope_policy: str = "fail_closed"
    schema_version: int = 1

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": int(self.schema_version),
            "policy_name": self.policy_name,
            "supported_interval_mode": self.supported_interval_mode,
            "mixed_interval_policy": self.mixed_interval_policy,
            "mixed_interval_fail_closed_reason": self.mixed_interval_fail_closed_reason,
            "freshness_policy": self.freshness_policy,
            "stale_or_missing_scope_policy": self.stale_or_missing_scope_policy,
        }

    def evaluate_intervals(self, intervals: object) -> dict[str, object]:
        normalized = sorted({str(item or "").strip() for item in tuple(intervals or ()) if str(item or "").strip()})
        mixed = len(normalized) > 1
        return {
            **self.as_dict(),
            "intervals": normalized,
            "status": "FAIL" if mixed else "PASS",
            "reason": self.mixed_interval_fail_closed_reason if mixed else "single_interval_policy_satisfied",
        }


DEFAULT_DECISION_CLOCK_POLICY = DecisionClockPolicy()


def decision_clock_policy_payload() -> dict[str, object]:
    return DEFAULT_DECISION_CLOCK_POLICY.as_dict()

def runtime_data_provider_contract_payload() -> dict[str, object]:
    return {
        "schema_version": RUNTIME_DATA_CONTRACT_SCHEMA_VERSION,
        "provider_name": RUNTIME_DATA_PROVIDER_NAME,
        "provider_version": RUNTIME_DATA_PROVIDER_VERSION,
        "capabilities": list(RUNTIME_DATA_CAPABILITY_NAMES),
        "capability_tables": {
            name: list(RUNTIME_DATA_CAPABILITY_TABLES[name]) for name in RUNTIME_DATA_CAPABILITY_NAMES
        },
        "unsupported_required_capability_policy": "fail_closed",
        "optional_missing_policy": "warn_by_default",
        "snapshot_contract": "RuntimeFeatureSnapshot.v1",
        "decision_clock_policy": decision_clock_policy_payload(),
    }


def runtime_data_provider_contract_hash() -> str:
    return sha256_prefixed(runtime_data_provider_contract_payload())


@dataclass(frozen=True)
class RuntimeDataCapabilityCoverage:
    capability: str
    status: str
    row_count: int = 0
    first_ts: int | None = None
    last_ts: int | None = None
    selected_ts: int | None = None
    coverage_pct: float | None = None
    source_tables_or_streams: tuple[str, ...] = ()
    reason: str | None = None
    min_coverage_pct: float | None = None
    expected_count: int | None = None
    closed_candle_required: bool = False
    max_age_ms: int | None = None
    min_rows: int | None = None
    lookback_window_ms: int | None = None
    min_density_pct: float | None = None
    freshness_policy: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "capability": self.capability,
            "status": self.status,
            "row_count": int(self.row_count),
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "selected_ts": self.selected_ts,
            "coverage_pct": self.coverage_pct,
            "source_tables_or_streams": list(self.source_tables_or_streams),
            "reason": self.reason,
            "min_coverage_pct": self.min_coverage_pct,
            "expected_count": self.expected_count,
            "closed_candle_required": bool(self.closed_candle_required),
            "max_age_ms": self.max_age_ms,
            "min_rows": self.min_rows,
            "lookback_window_ms": self.lookback_window_ms,
            "min_density_pct": self.min_density_pct,
            "freshness_policy": self.freshness_policy,
        }


@dataclass(frozen=True)
class RuntimeStrategyDataRequirements:
    required: tuple[DataCapabilityRequirement, ...]
    optional: tuple[DataCapabilityRequirement, ...]
    per_strategy: Mapping[str, dict[str, object]]
    unsupported_required: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "per_strategy",
            MappingProxyType({str(key): dict(value) for key, value in self.per_strategy.items()}),
        )

    @property
    def required_names(self) -> tuple[str, ...]:
        return tuple(capability.name for capability in self.required)

    @property
    def optional_names(self) -> tuple[str, ...]:
        return tuple(capability.name for capability in self.optional)

    @property
    def all_names(self) -> tuple[str, ...]:
        return tuple(sorted(set(self.required_names) | set(self.optional_names)))

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "required": [item.as_dict() for item in self.required],
            "optional": [item.as_dict() for item in self.optional],
            "unsupported_required": list(self.unsupported_required),
            "per_strategy": {key: dict(value) for key, value in self.per_strategy.items()},
        }

    def content_hash(self) -> str:
        return sha256_prefixed(self.as_dict())


@dataclass(frozen=True)
class RuntimeDataAvailabilityReport:
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))

    @property
    def status(self) -> str:
        return str(self.payload.get("status") or "")

    @property
    def reasons(self) -> tuple[str, ...]:
        return tuple(str(item) for item in self.payload.get("reasons") or ())

    @property
    def report_hash(self) -> str:
        return str(self.payload.get("report_hash") or "")

    @property
    def ok(self) -> bool:
        return self.status == "PASS"

    def as_dict(self) -> dict[str, object]:
        return dict(self.payload)


@dataclass(frozen=True)
class RuntimeFeatureSnapshot:
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))

    @property
    def feature_payload(self) -> dict[str, object]:
        value = self.payload.get("feature_payload")
        return dict(value) if isinstance(value, Mapping) else {}

    @property
    def feature_snapshot_hash(self) -> str:
        return str(self.payload.get("feature_snapshot_hash") or "")

    @property
    def market_snapshot_hash(self) -> str:
        return str(self.payload.get("market_snapshot_hash") or "")

    def as_dict(self) -> dict[str, object]:
        return dict(self.payload)


class RuntimeDataProvider(Protocol):
    def preflight(
        self,
        strategy_set: object,
        *,
        through_ts_ms: int | None,
    ) -> RuntimeDataAvailabilityReport: ...

    def snapshot(
        self,
        request: object,
        requirements: RuntimeStrategyDataRequirements,
    ) -> RuntimeFeatureSnapshot | None: ...


@dataclass(frozen=True)
class RuntimeDataRequirementResolver:
    optional_missing_fails: bool = False

    def resolve_for_strategy_set(self, strategy_set: object) -> RuntimeStrategyDataRequirements:
        required_by_name: dict[str, DataCapabilityRequirement] = {}
        optional_by_name: dict[str, DataCapabilityRequirement] = {}
        unsupported_required: list[str] = []
        per_strategy: dict[str, dict[str, object]] = {}
        for spec in tuple(getattr(strategy_set, "active_strategies", ()) or ()):
            strategy_name = str(getattr(spec, "strategy_name", "") or "").strip().lower()
            instance_id = str(getattr(spec, "strategy_instance_id", "") or "").strip()
            if not instance_id:
                try:
                    from .runtime_strategy_set import derive_strategy_instance_id

                    instance_id = derive_strategy_instance_id(spec)
                except Exception:
                    instance_id = strategy_name
            research_requirements = research_strategy_data_requirements(
                strategy_name,
                runtime_strategy_spec=spec,
            )
            normalized = self._normalize_research_requirements(
                research_requirements,
                spec=spec,
                strategy_name=strategy_name,
            )
            strategy_required = []
            strategy_optional = []
            for capability in normalized.required:
                strategy_required.append(capability.name)
                if capability.name not in RUNTIME_DATA_CAPABILITY_NAMES:
                    unsupported_required.append(capability.name)
                required_by_name[capability.name] = capability
                optional_by_name.pop(capability.name, None)
            for capability in normalized.optional:
                strategy_optional.append(capability.name)
                if capability.name not in required_by_name:
                    optional_by_name[capability.name] = capability
            per_strategy[instance_id] = {
                "strategy_name": strategy_name,
                "required": sorted(strategy_required),
                "optional": sorted(strategy_optional),
                "requirements_hash": normalized.content_hash(),
            }
        return RuntimeStrategyDataRequirements(
            required=tuple(required_by_name[name] for name in sorted(required_by_name)),
            optional=tuple(optional_by_name[name] for name in sorted(optional_by_name)),
            unsupported_required=tuple(sorted(set(unsupported_required))),
            per_strategy=per_strategy,
        )

    def _normalize_research_requirements(
        self,
        requirements: ResearchStrategyDataRequirements,
        *,
        spec: object | None = None,
        strategy_name: str = "",
    ) -> RuntimeStrategyDataRequirements:
        required: dict[str, DataCapabilityRequirement] = {}
        optional: dict[str, DataCapabilityRequirement] = {}
        for capability in requirements.normalized_capabilities():
            normalized_name = normalize_runtime_data_capability(capability.name)
            normalized_capability = DataCapabilityRequirement(
                name=normalized_name,
                required=bool(capability.required),
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
            if normalized_capability.required:
                required[normalized_name] = normalized_capability
                optional.pop(normalized_name, None)
            elif normalized_name not in required:
                optional[normalized_name] = normalized_capability
        return RuntimeStrategyDataRequirements(
            required=tuple(required[name] for name in sorted(required)),
            optional=tuple(optional[name] for name in sorted(optional)),
            per_strategy={},
        )


@dataclass(frozen=True)
class SQLiteRuntimeDataProvider:
    conn: sqlite3.Connection
    provider_name: str = RUNTIME_DATA_PROVIDER_NAME
    provider_version: str = RUNTIME_DATA_PROVIDER_VERSION
    resolver: RuntimeDataRequirementResolver = RuntimeDataRequirementResolver()

    @property
    def provider_contract_hash(self) -> str:
        return runtime_data_provider_contract_hash()

    def preflight(
        self,
        strategy_set: object,
        *,
        through_ts_ms: int | None,
    ) -> RuntimeDataAvailabilityReport:
        requirements = self.resolver.resolve_for_strategy_set(strategy_set)
        report = self.availability_report_for_requirements(
            requirements,
            pair=str(getattr(getattr(strategy_set, "market_scope", None), "pair", "") or ""),
            interval=str(getattr(getattr(strategy_set, "market_scope", None), "interval", "") or ""),
            through_ts_ms=through_ts_ms,
        )
        payload = report.as_dict()
        scope_payload = self._scope_coverage_payload(
            strategy_set=strategy_set,
            requirements=requirements,
            through_ts_ms=through_ts_ms,
        )
        payload.update(scope_payload)
        payload["report_hash"] = sha256_prefixed(
            {key: value for key, value in payload.items() if key != "report_hash"}
        )
        return RuntimeDataAvailabilityReport(payload)

    def availability_report_for_requirements(
        self,
        requirements: RuntimeStrategyDataRequirements,
        *,
        pair: str,
        interval: str,
        through_ts_ms: int | None,
    ) -> RuntimeDataAvailabilityReport:
        requirement_by_name = {
            item.name: item for item in tuple(requirements.required) + tuple(requirements.optional)
        }
        coverage = {
            capability: self._coverage_for_capability(
                capability,
                requirement=requirement_by_name.get(capability),
                pair=pair,
                interval=interval,
                through_ts_ms=through_ts_ms,
            )
            for capability in requirements.all_names
        }
        reasons: list[str] = []
        for capability in requirements.unsupported_required:
            reasons.append(f"runtime_data_capability_unsupported:{capability}")
        for capability in requirements.required:
            observed = coverage.get(capability.name)
            if observed is None or observed.status != "PASS":
                reason = (
                    observed.reason
                    if observed is not None and observed.reason
                    else f"runtime_data_requirement_missing:{capability.name}"
                )
                reasons.append(reason)
                continue
            if (
                capability.min_coverage_pct is not None
                and observed.coverage_pct is not None
                and float(observed.coverage_pct) < float(capability.min_coverage_pct)
            ):
                reasons.append(f"runtime_data_coverage_below_threshold:{capability.name}")
        optional_warnings = []
        for capability in requirements.optional:
            observed = coverage.get(capability.name)
            if observed is None or observed.status != "PASS":
                optional_warnings.append(f"runtime_data_optional_missing:{capability.name}")
        payload: dict[str, object] = {
            "schema_version": 1,
            "provider_name": self.provider_name,
            "provider_version": self.provider_version,
            "provider_contract_hash": self.provider_contract_hash,
            "through_ts_ms": through_ts_ms,
            "status": "FAIL" if reasons else "PASS",
            "reasons": sorted(set(reasons)),
            "warnings": sorted(set(optional_warnings)),
            "capabilities_present": sorted(
                capability for capability, item in coverage.items() if item.status == "PASS"
            ),
            "capabilities_missing": sorted(
                capability for capability, item in coverage.items() if item.status != "PASS"
            ),
            "coverage_by_capability": {
                capability: item.as_dict() for capability, item in sorted(coverage.items())
            },
            "source_tables_or_streams": sorted(
                {
                    table
                    for item in coverage.values()
                    for table in item.source_tables_or_streams
                }
            ),
            "db_schema_fingerprint": self._db_schema_fingerprint(),
            "source_schema_hash": self._db_schema_fingerprint(),
            "per_strategy_requirements": {
                key: dict(value) for key, value in requirements.per_strategy.items()
            },
            "per_strategy_status": self._per_strategy_status(
                requirements=requirements,
                coverage=coverage,
            ),
            "runtime_data_requirements_hash": requirements.content_hash(),
        }
        payload["report_hash"] = sha256_prefixed(payload)
        return RuntimeDataAvailabilityReport(payload)

    def snapshot(
        self,
        request: object,
        requirements: RuntimeStrategyDataRequirements,
    ) -> RuntimeFeatureSnapshot | None:
        pair = str(getattr(request, "pair", "") or "").strip()
        interval = str(getattr(request, "interval", "") or "").strip()
        through_ts_ms = getattr(request, "through_ts_ms", None)
        report = self.availability_report_for_requirements(
            requirements,
            pair=pair,
            interval=interval,
            through_ts_ms=None if through_ts_ms is None else int(through_ts_ms),
        )
        if not report.ok:
            return None
        feature_payload: dict[str, object] = {}
        capability_snapshots: dict[str, object] = {}
        if "candles" in requirements.all_names:
            candle_requirement = next(
                (item for item in tuple(requirements.required) + tuple(requirements.optional) if item.name == "candles"),
                None,
            )
            candle_rows = self._candle_rows(
                pair=pair,
                interval=interval,
                through_ts_ms=through_ts_ms,
                limit=None if candle_requirement is None else candle_requirement.lookback_rows,
            )
            if candle_requirement is not None and len(candle_rows) < int(candle_requirement.lookback_rows or 1):
                return None
            candle = self._latest_candle(pair=pair, interval=interval, through_ts_ms=through_ts_ms)
            if candle is None:
                return None
            candle_ts, close, candle_index = candle
            candle_payload = {
                "selected_timestamp": int(candle_ts),
                "selected_ts": int(candle_ts),
                "staleness_ms": (
                    None
                    if through_ts_ms is None
                    else max(0, int(through_ts_ms) - int(candle_ts))
                ),
                "source_table_or_stream": "candles",
                "source_tables_or_streams": ["candles"],
                "rows": candle_rows,
            }
            candle_payload["payload_hash"] = sha256_prefixed(candle_payload)
            capability_snapshots["candles"] = candle_payload
            feature_payload.update(
                {
                    "candle_ts": int(candle_ts),
                    "market_price": float(close),
                    "last_close": float(close),
                    "candle_index": int(candle_index),
                    "candle": {
                        "ts": int(candle_ts),
                        "close": float(close),
                        "candle_index": int(candle_index),
                    },
                }
            )
        for capability in requirements.all_names:
            if capability == "candles":
                continue
            snapshot = self._generic_capability_snapshot(
                capability,
                pair=pair,
                through_ts_ms=through_ts_ms,
            )
            if snapshot is None:
                if capability in requirements.required_names:
                    return None
                continue
            capability_snapshots[capability] = snapshot
        if capability_snapshots:
            feature_payload["capabilities"] = capability_snapshots
        decision_candle_ts = (
            int(feature_payload["candle_ts"])
            if "candle_ts" in feature_payload
            else (None if through_ts_ms is None else int(through_ts_ms))
        )
        market_material = {
            "pair": pair,
            "interval": interval,
            "decision_candle_ts": decision_candle_ts,
            "market_price": feature_payload.get("market_price"),
            "last_close": feature_payload.get("last_close"),
        }
        coverage = dict(report.payload.get("coverage_by_capability") or {})
        requirements_hash = requirements.content_hash()
        scope_key = getattr(request, "runtime_scope_key", None)
        scope_payload = (
            scope_key.with_hash_payload()
            if hasattr(scope_key, "with_hash_payload")
            else {}
        )
        strategy_instance_id = str(scope_payload.get("strategy_instance_id") or "").strip()
        preflight_scope_id = (
            f"{strategy_instance_id}:{pair}:{interval}"
            if strategy_instance_id
            else f"{pair}:{interval}"
        )
        payload: dict[str, object] = {
            "schema_version": 1,
            "pair": pair,
            "interval": interval,
            "through_ts_ms": through_ts_ms,
            "decision_candle_ts": decision_candle_ts,
            "capabilities_present": list(report.payload.get("capabilities_present") or []),
            "capabilities_missing": list(report.payload.get("capabilities_missing") or []),
            "coverage_by_capability": coverage,
            "source_tables_or_streams": list(report.payload.get("source_tables_or_streams") or []),
            "db_schema_fingerprint": report.payload.get("db_schema_fingerprint"),
            "source_schema_hash": report.payload.get("source_schema_hash"),
            "provider_name": self.provider_name,
            "provider_version": self.provider_version,
            "provider_contract_hash": self.provider_contract_hash,
            "runtime_data_requirements_hash": requirements_hash,
            "runtime_data_contract_hash": requirements_hash,
            "runtime_data_availability_report_hash": report.report_hash,
            "runtime_scope_key": scope_payload or None,
            "scope_key_hash": scope_payload.get("scope_key_hash"),
            "preflight_scope_id": preflight_scope_id,
            "coverage_by_scope": {
                str(scope_payload.get("scope_key_hash") or f"{pair}:{interval}"): coverage
            },
            "selected_candle_by_scope": {
                str(scope_payload.get("scope_key_hash") or f"{pair}:{interval}"): feature_payload.get("candle")
            },
            "source_schema_hash_by_scope": {
                str(scope_payload.get("scope_key_hash") or f"{pair}:{interval}"): report.payload.get("source_schema_hash")
            },
            "freshness_by_scope": {
                str(scope_payload.get("scope_key_hash") or f"{pair}:{interval}"): {
                    "staleness_ms": (
                        None
                        if through_ts_ms is None or decision_candle_ts is None
                        else max(0, int(through_ts_ms) - int(decision_candle_ts))
                    ),
                    "decision_clock_policy": DEFAULT_DECISION_CLOCK_POLICY.policy_name,
                    "decision_clock_policy_payload": DEFAULT_DECISION_CLOCK_POLICY.as_dict(),
                }
            },
            "staleness_ms": (
                None
                if through_ts_ms is None or decision_candle_ts is None
                else max(0, int(through_ts_ms) - int(decision_candle_ts))
            ),
            "feature_payload": feature_payload,
        }
        payload["market_snapshot_hash"] = sha256_prefixed(market_material)
        payload["feature_snapshot_hash"] = sha256_prefixed(payload)
        return RuntimeFeatureSnapshot(payload)

    def _scope_coverage_payload(
        self,
        *,
        strategy_set: object,
        requirements: RuntimeStrategyDataRequirements,
        through_ts_ms: int | None,
    ) -> dict[str, object]:
        coverage_by_scope: dict[str, object] = {}
        selected_candle_by_scope: dict[str, object] = {}
        source_schema_hash_by_scope: dict[str, object] = {}
        freshness_by_scope: dict[str, object] = {}
        scope_identity_by_scope: dict[str, object] = {}
        schema_hash = self._db_schema_fingerprint()
        for spec in tuple(getattr(strategy_set, "active_strategies", ()) or ()):
            instance_id = str(getattr(spec, "strategy_instance_id", "") or "").strip()
            if not instance_id:
                instance_id = str(getattr(spec, "strategy_name", "") or "strategy").strip().lower()
            pair = str(getattr(spec, "pair", "") or getattr(getattr(strategy_set, "market_scope", None), "pair", "") or "")
            interval = str(getattr(spec, "interval", "") or getattr(getattr(strategy_set, "market_scope", None), "interval", "") or "")
            scope_id = f"{instance_id}:{pair}:{interval}"
            scope_keys = [scope_id]
            scope_key_payload: dict[str, object] | None = None
            try:
                from .runtime_scope import RuntimeScopeKey

                key = RuntimeScopeKey(
                    pair=pair,
                    interval=interval,
                    strategy_instance_id=instance_id,
                    strategy_name=str(getattr(spec, "strategy_name", "") or ""),
                    runtime_contract_hash=str(getattr(spec, "runtime_contract_hash", "") or ""),
                    approved_profile_hash=str(getattr(spec, "approved_profile_hash", "") or ""),
                    strategy_parameters_hash=str(getattr(spec, "strategy_parameters_hash", "") or ""),
                )
                scope_key_payload = key.with_hash_payload()
                scope_keys.append(key.scope_key_hash())
            except ValueError:
                scope_key_payload = None
            requirement_by_name = {
                item.name: item for item in tuple(requirements.required) + tuple(requirements.optional)
            }
            coverage = {
                capability: self._coverage_for_capability(
                    capability,
                    requirement=requirement_by_name.get(capability),
                    pair=pair,
                    interval=interval,
                    through_ts_ms=through_ts_ms,
                ).as_dict()
                for capability in requirements.all_names
            }
            candle = coverage.get("candles") if isinstance(coverage, Mapping) else None
            selected_candle = (
                None
                if not isinstance(candle, Mapping)
                else {
                    "selected_ts": candle.get("selected_ts"),
                    "status": candle.get("status"),
                    "reason": candle.get("reason"),
                }
            )
            freshness = {
                "decision_clock_policy": DEFAULT_DECISION_CLOCK_POLICY.policy_name,
                "decision_clock_policy_payload": DEFAULT_DECISION_CLOCK_POLICY.as_dict(),
                "freshness_policy": DEFAULT_DECISION_CLOCK_POLICY.freshness_policy,
                "stale_or_missing_scope_policy": DEFAULT_DECISION_CLOCK_POLICY.stale_or_missing_scope_policy,
                "status": "PASS"
                if all(
                    coverage.get(item.name, {}).get("status") == "PASS"
                    for item in requirements.required
                    if isinstance(coverage.get(item.name), Mapping)
                )
                else "FAIL",
            }
            for key in scope_keys:
                coverage_by_scope[key] = coverage
                selected_candle_by_scope[key] = selected_candle
                source_schema_hash_by_scope[key] = schema_hash
                freshness_by_scope[key] = freshness
                scope_identity_by_scope[key] = {
                    "preflight_scope_id": scope_id,
                    "runtime_scope_key": scope_key_payload,
                    "scope_key_hash": None if scope_key_payload is None else scope_key_payload.get("scope_key_hash"),
                }
        return {
            "coverage_by_scope": coverage_by_scope,
            "selected_candle_by_scope": selected_candle_by_scope,
            "source_schema_hash_by_scope": source_schema_hash_by_scope,
            "freshness_by_scope": freshness_by_scope,
            "scope_identity_by_scope": scope_identity_by_scope,
            "decision_clock_policy": DEFAULT_DECISION_CLOCK_POLICY.policy_name,
            "decision_clock_policy_payload": DEFAULT_DECISION_CLOCK_POLICY.as_dict(),
            "decision_clock_policy_evaluation": DEFAULT_DECISION_CLOCK_POLICY.evaluate_intervals(
                str(getattr(spec, "interval", "") or getattr(getattr(strategy_set, "market_scope", None), "interval", "") or "")
                for spec in tuple(getattr(strategy_set, "active_strategies", ()) or ())
            ),
        }

    def _candle_rows(
        self,
        *,
        pair: str,
        interval: str,
        through_ts_ms: int | None,
        limit: int | None,
    ) -> list[dict[str, object]]:
        columns = self._table_columns("candles")
        if not columns:
            return []
        select_columns = [name for name in ("ts", "open", "high", "low", "close", "volume") if name in columns]
        if "ts" not in select_columns or "close" not in select_columns:
            return []
        query = f"SELECT {', '.join(select_columns)} FROM candles WHERE pair=? AND interval=?"
        params: list[object] = [pair, interval]
        if through_ts_ms is not None:
            query += " AND ts<=?"
            params.append(int(through_ts_ms))
        query += " ORDER BY ts DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))
        rows = self.conn.execute(query, tuple(params)).fetchall()
        payload = [
            {
                name: (
                    int(row[name]) if name == "ts" and hasattr(row, "keys") else row[name]
                )
                for name in select_columns
            }
            if hasattr(row, "keys")
            else {name: row[idx] for idx, name in enumerate(select_columns)}
            for row in rows
        ]
        return list(reversed(payload))

    def _generic_capability_snapshot(
        self,
        capability: str,
        *,
        pair: str,
        through_ts_ms: int | None,
    ) -> dict[str, object] | None:
        table = RUNTIME_DATA_CAPABILITY_TABLES[capability][0]
        columns = self._table_columns(table)
        if "ts" not in columns:
            return None
        where = []
        params: list[object] = []
        if "pair" in columns and pair:
            where.append("pair=?")
            params.append(pair)
        if through_ts_ms is not None:
            where.append("ts<=?")
            params.append(int(through_ts_ms))
        clause = " WHERE " + " AND ".join(where) if where else ""
        row = self.conn.execute(
            f"SELECT * FROM {table}{clause} ORDER BY ts DESC LIMIT 1",
            tuple(params),
        ).fetchone()
        if row is None:
            return None
        payload = {name: row[name] if hasattr(row, "keys") else row[idx] for idx, name in enumerate(columns)}
        selected_ts = int(payload.get("ts") or 0)
        material = {
            "selected_timestamp": selected_ts,
            "selected_ts": selected_ts,
            "staleness_ms": (
                None if through_ts_ms is None else max(0, int(through_ts_ms) - selected_ts)
            ),
            "source_table_or_stream": table,
            "source_tables_or_streams": [table],
            "evidence_payload": payload,
        }
        material["payload_hash"] = sha256_prefixed(material)
        return material

    def _per_strategy_status(
        self,
        *,
        requirements: RuntimeStrategyDataRequirements,
        coverage: Mapping[str, RuntimeDataCapabilityCoverage],
    ) -> dict[str, dict[str, object]]:
        payload: dict[str, dict[str, object]] = {}
        for instance_id, item in requirements.per_strategy.items():
            required = tuple(str(name) for name in item.get("required") or ())
            missing = sorted(
                name for name in required if coverage.get(name) is None or coverage[name].status != "PASS"
            )
            payload[instance_id] = {
                "strategy_name": item.get("strategy_name"),
                "status": "FAIL" if missing else "PASS",
                "required": list(required),
                "optional": list(item.get("optional") or ()),
                "missing_required": missing,
                "requirements_hash": item.get("requirements_hash"),
            }
        return payload

    def _coverage_for_capability(
        self,
        capability: str,
        *,
        requirement: DataCapabilityRequirement | None = None,
        pair: str,
        interval: str,
        through_ts_ms: int | None,
    ) -> RuntimeDataCapabilityCoverage:
        if capability not in RUNTIME_DATA_CAPABILITY_NAMES:
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="UNSUPPORTED",
                reason=f"runtime_data_capability_unsupported:{capability}",
            )
        if capability == "candles":
            return self._candle_coverage(
                pair=pair,
                interval=interval,
                through_ts_ms=through_ts_ms,
                requirement=requirement,
            )
        return self._generic_ts_coverage(
            capability,
            pair=pair,
            through_ts_ms=through_ts_ms,
            requirement=requirement,
        )

    def _candle_coverage(
        self,
        *,
        pair: str,
        interval: str,
        through_ts_ms: int | None,
        requirement: DataCapabilityRequirement | None = None,
    ) -> RuntimeDataCapabilityCoverage:
        table = "candles"
        if not self._table_exists(table):
            return RuntimeDataCapabilityCoverage(
                capability="candles",
                status="MISSING",
                source_tables_or_streams=(table,),
                reason="runtime_data_requirement_missing:candles",
            )
        where = "pair=? AND interval=?"
        params: list[object] = [pair, interval]
        if through_ts_ms is not None:
            where += " AND ts<=?"
            params.append(int(through_ts_ms))
        row = self.conn.execute(
            f"SELECT COUNT(*), MIN(ts), MAX(ts) FROM {table} WHERE {where}",
            tuple(params),
        ).fetchone()
        count = int(row[0] or 0) if row is not None else 0
        if count <= 0:
            return RuntimeDataCapabilityCoverage(
                capability="candles",
                status="MISSING",
                row_count=0,
                source_tables_or_streams=(table,),
                reason="runtime_data_requirement_missing:candles",
                expected_count=(
                    None if requirement is None or requirement.lookback_rows is None else int(requirement.lookback_rows)
                ),
                closed_candle_required=bool(requirement.closed_candle_required if requirement else False),
            )
        first_ts = int(row[1])
        last_ts = int(row[2])
        expected_count = int(requirement.lookback_rows or 1) if requirement is not None else 1
        coverage_pct = min(100.0, (float(count) / float(expected_count)) * 100.0)
        if requirement is not None and requirement.closed_candle_required and through_ts_ms is not None:
            selected = self._latest_candle(pair=pair, interval=interval, through_ts_ms=through_ts_ms)
            if selected is None:
                return RuntimeDataCapabilityCoverage(
                    capability="candles",
                    status="MISSING",
                    row_count=count,
                    first_ts=first_ts,
                    last_ts=last_ts,
                    source_tables_or_streams=(table,),
                    reason="runtime_data_closed_candle_unavailable:candles",
                    expected_count=expected_count,
                    coverage_pct=coverage_pct,
                    closed_candle_required=True,
                )
        if count < expected_count:
            return RuntimeDataCapabilityCoverage(
                capability="candles",
                status="INSUFFICIENT",
                row_count=count,
                first_ts=first_ts,
                last_ts=last_ts,
                selected_ts=last_ts,
                coverage_pct=coverage_pct,
                source_tables_or_streams=(table,),
                reason="runtime_data_lookback_insufficient:candles",
                expected_count=expected_count,
                min_coverage_pct=None if requirement is None else requirement.min_coverage_pct,
                closed_candle_required=bool(requirement.closed_candle_required if requirement else False),
            )
        return RuntimeDataCapabilityCoverage(
            capability="candles",
            status="PASS",
            row_count=count,
            first_ts=first_ts,
            last_ts=last_ts,
            selected_ts=last_ts,
            coverage_pct=coverage_pct,
            source_tables_or_streams=(table,),
            expected_count=expected_count,
            min_coverage_pct=None if requirement is None else requirement.min_coverage_pct,
            closed_candle_required=bool(requirement.closed_candle_required if requirement else False),
        )

    def _generic_ts_coverage(
        self,
        capability: str,
        *,
        pair: str,
        through_ts_ms: int | None,
        requirement: DataCapabilityRequirement | None = None,
    ) -> RuntimeDataCapabilityCoverage:
        table = RUNTIME_DATA_CAPABILITY_TABLES[capability][0]
        if not self._table_exists(table):
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="MISSING",
                source_tables_or_streams=(table,),
                reason=f"runtime_data_requirement_missing:{capability}",
            )
        columns = self._table_columns(table)
        if "ts" not in columns:
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="MISSING",
                source_tables_or_streams=(table,),
                reason=f"runtime_data_requirement_missing:{capability}",
            )
        where = []
        params: list[object] = []
        if "pair" in columns and pair:
            where.append("pair=?")
            params.append(pair)
        if through_ts_ms is not None:
            where.append("ts<=?")
            params.append(int(through_ts_ms))
        clause = " WHERE " + " AND ".join(where) if where else ""
        min_ts_for_window: int | None = None
        if through_ts_ms is not None and requirement is not None and requirement.lookback_window_ms is not None:
            min_ts_for_window = int(through_ts_ms) - int(requirement.lookback_window_ms)
            where.append("ts>=?")
            params.append(min_ts_for_window)
            clause = " WHERE " + " AND ".join(where) if where else ""
        if capability == "orderbook_depth":
            return self._orderbook_depth_coverage(
                table=table,
                columns=columns,
                clause=clause,
                params=tuple(params),
                capability=capability,
                through_ts_ms=through_ts_ms,
                requirement=requirement,
            )
        row = self.conn.execute(
            f"SELECT COUNT(*), MIN(ts), MAX(ts) FROM {table}{clause}",
            tuple(params),
        ).fetchone()
        count = int(row[0] or 0) if row is not None else 0
        if count <= 0:
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="MISSING",
                row_count=0,
                source_tables_or_streams=(table,),
                reason=f"runtime_data_requirement_missing:{capability}",
            )
        first_ts = int(row[1])
        last_ts = int(row[2])
        if capability == "orderbook_top":
            malformed_reason = self._orderbook_top_malformed_reason(
                table=table,
                columns=columns,
                clause=clause,
                params=tuple(params),
            )
            if malformed_reason:
                return RuntimeDataCapabilityCoverage(
                    capability=capability,
                    status="MALFORMED",
                    row_count=count,
                    first_ts=first_ts,
                    last_ts=last_ts,
                    selected_ts=last_ts,
                    source_tables_or_streams=(table,),
                    reason=malformed_reason,
                )
        stale_reason = self._stale_reason(
            capability=capability,
            selected_ts=last_ts,
            through_ts_ms=through_ts_ms,
            requirement=requirement,
        )
        if stale_reason:
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="STALE",
                row_count=count,
                first_ts=first_ts,
                last_ts=last_ts,
                selected_ts=last_ts,
                source_tables_or_streams=(table,),
                reason=stale_reason,
                max_age_ms=None if requirement is None else requirement.max_age_ms,
                freshness_policy=None if requirement is None else requirement.freshness_policy,
            )
        min_rows = self._required_min_rows(requirement)
        coverage_pct = self._coverage_pct(count=count, expected_count=min_rows)
        density_reason = self._density_reason(
            capability=capability,
            count=count,
            requirement=requirement,
            min_ts=min_ts_for_window,
            max_ts=through_ts_ms,
        )
        if count < min_rows or density_reason:
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="INSUFFICIENT",
                row_count=count,
                first_ts=first_ts,
                last_ts=last_ts,
                selected_ts=last_ts,
                coverage_pct=coverage_pct,
                source_tables_or_streams=(table,),
                reason=density_reason or f"runtime_data_coverage_below_threshold:{capability}",
                expected_count=min_rows,
                min_coverage_pct=None if requirement is None else requirement.min_coverage_pct,
                min_rows=min_rows,
                lookback_window_ms=None if requirement is None else requirement.lookback_window_ms,
                min_density_pct=None if requirement is None else requirement.min_density_pct,
            )
        return RuntimeDataCapabilityCoverage(
            capability=capability,
            status="PASS",
            row_count=count,
            first_ts=first_ts,
            last_ts=last_ts,
            selected_ts=last_ts,
            coverage_pct=coverage_pct,
            source_tables_or_streams=(table,),
            expected_count=min_rows,
            min_coverage_pct=None if requirement is None else requirement.min_coverage_pct,
            max_age_ms=None if requirement is None else requirement.max_age_ms,
            min_rows=min_rows,
            lookback_window_ms=None if requirement is None else requirement.lookback_window_ms,
            min_density_pct=None if requirement is None else requirement.min_density_pct,
            freshness_policy=None if requirement is None else requirement.freshness_policy,
        )

    def _orderbook_top_malformed_reason(
        self,
        *,
        table: str,
        columns: tuple[str, ...],
        clause: str,
        params: tuple[object, ...],
    ) -> str | None:
        if "bid_price" not in columns or "ask_price" not in columns:
            return "runtime_data_malformed:orderbook_top"
        row = self.conn.execute(
            f"SELECT bid_price, ask_price FROM {table}{clause} ORDER BY ts DESC LIMIT 1",
            params,
        ).fetchone()
        if row is None:
            return "runtime_data_requirement_missing:orderbook_top"
        bid = row["bid_price"] if hasattr(row, "keys") else row[0]
        ask = row["ask_price"] if hasattr(row, "keys") else row[1]
        try:
            bid_f = float(bid)
            ask_f = float(ask)
        except (TypeError, ValueError):
            return "runtime_data_malformed:orderbook_top"
        if bid_f <= 0.0 or ask_f <= 0.0 or bid_f > ask_f:
            return "runtime_data_malformed:orderbook_top"
        return None

    def _orderbook_depth_coverage(
        self,
        *,
        table: str,
        columns: tuple[str, ...],
        clause: str,
        params: tuple[object, ...],
        capability: str,
        through_ts_ms: int | None,
        requirement: DataCapabilityRequirement | None,
    ) -> RuntimeDataCapabilityCoverage:
        if "side" not in columns:
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="MISSING",
                source_tables_or_streams=(table,),
                reason=f"runtime_data_requirement_missing:{capability}",
            )
        row = self.conn.execute(
            f"""
            SELECT COUNT(*) AS row_count, MIN(ts) AS first_ts, MAX(ts) AS last_ts,
                   SUM(CASE WHEN side='bid' THEN 1 ELSE 0 END) AS bid_count,
                   SUM(CASE WHEN side='ask' THEN 1 ELSE 0 END) AS ask_count
            FROM {table}{clause}
            """,
            params,
        ).fetchone()
        count = int(row[0] or 0) if row is not None else 0
        if count <= 0:
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="MISSING",
                row_count=0,
                source_tables_or_streams=(table,),
                reason=f"runtime_data_requirement_missing:{capability}",
            )
        first_ts = int(row[1])
        last_ts = int(row[2])
        bid_count = int(row[3] or 0)
        ask_count = int(row[4] or 0)
        min_rows = self._required_min_rows(requirement)
        if bid_count <= 0 or ask_count <= 0 or count < min_rows:
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="INSUFFICIENT",
                row_count=count,
                first_ts=first_ts,
                last_ts=last_ts,
                selected_ts=last_ts,
                coverage_pct=self._coverage_pct(count=count, expected_count=min_rows),
                source_tables_or_streams=(table,),
                reason="runtime_data_depth_insufficient:orderbook_depth",
                expected_count=min_rows,
                min_rows=min_rows,
            )
        stale_reason = self._stale_reason(
            capability=capability,
            selected_ts=last_ts,
            through_ts_ms=through_ts_ms,
            requirement=requirement,
        )
        if stale_reason:
            return RuntimeDataCapabilityCoverage(
                capability=capability,
                status="STALE",
                row_count=count,
                first_ts=first_ts,
                last_ts=last_ts,
                selected_ts=last_ts,
                source_tables_or_streams=(table,),
                reason=stale_reason,
                max_age_ms=None if requirement is None else requirement.max_age_ms,
            )
        return RuntimeDataCapabilityCoverage(
            capability=capability,
            status="PASS",
            row_count=count,
            first_ts=first_ts,
            last_ts=last_ts,
            selected_ts=last_ts,
            coverage_pct=self._coverage_pct(count=count, expected_count=min_rows),
            source_tables_or_streams=(table,),
            expected_count=min_rows,
            min_rows=min_rows,
        )

    def _required_min_rows(self, requirement: DataCapabilityRequirement | None) -> int:
        if requirement is None:
            return 1
        if requirement.min_rows is not None:
            return int(requirement.min_rows)
        if requirement.lookback_rows is not None:
            return int(requirement.lookback_rows)
        return 1

    def _coverage_pct(self, *, count: int, expected_count: int) -> float:
        return min(100.0, (float(count) / float(max(1, expected_count))) * 100.0)

    def _stale_reason(
        self,
        *,
        capability: str,
        selected_ts: int,
        through_ts_ms: int | None,
        requirement: DataCapabilityRequirement | None,
    ) -> str | None:
        if through_ts_ms is None or requirement is None or requirement.max_age_ms is None:
            return None
        age_ms = int(through_ts_ms) - int(selected_ts)
        if age_ms > int(requirement.max_age_ms):
            return f"runtime_data_stale:{capability}"
        return None

    def _density_reason(
        self,
        *,
        capability: str,
        count: int,
        requirement: DataCapabilityRequirement | None,
        min_ts: int | None,
        max_ts: int | None,
    ) -> str | None:
        if requirement is None or requirement.min_density_pct is None:
            return None
        expected = self._required_min_rows(requirement)
        pct = self._coverage_pct(count=count, expected_count=expected)
        if pct < float(requirement.min_density_pct):
            return f"runtime_data_coverage_below_threshold:{capability}"
        if min_ts is not None and max_ts is not None and count <= 0:
            return f"runtime_data_coverage_below_threshold:{capability}"
        return None

    def _latest_candle(
        self,
        *,
        pair: str,
        interval: str,
        through_ts_ms: int | None,
    ) -> tuple[int, float, int] | None:
        query = "SELECT ts, close FROM candles WHERE pair=? AND interval=?"
        params: list[object] = [pair, interval]
        if through_ts_ms is not None:
            query += " AND ts<=?"
            params.append(int(through_ts_ms))
        query += " ORDER BY ts DESC LIMIT 1"
        row = self.conn.execute(query, tuple(params)).fetchone()
        if row is None:
            return None
        candle_ts = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
        close = float(row["close"]) if hasattr(row, "keys") else float(row[1])
        count_row = self.conn.execute(
            "SELECT COUNT(*) FROM candles WHERE pair=? AND interval=? AND ts<=?",
            (pair, interval, candle_ts),
        ).fetchone()
        candle_index = int(count_row[0]) - 1 if count_row is not None else 0
        return candle_ts, close, max(0, candle_index)

    def _table_exists(self, table: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name=? LIMIT 1",
            (table,),
        ).fetchone()
        return row is not None

    def _table_columns(self, table: str) -> tuple[str, ...]:
        if not self._table_exists(table):
            return ()
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return tuple(str(row[1]) for row in rows)

    def _db_schema_fingerprint(self) -> str:
        rows = self.conn.execute(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE type IN ('table', 'index', 'view', 'trigger')
            ORDER BY type, name
            """
        ).fetchall()
        payload = [
            {
                "type": str(row[0]),
                "name": str(row[1]),
                "tbl_name": str(row[2]),
                "sql": str(row[3] or ""),
            }
            for row in rows
        ]
        return sha256_prefixed(payload)
