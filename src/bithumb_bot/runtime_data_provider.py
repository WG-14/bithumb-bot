from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping, Protocol

from .decision_equivalence import sha256_prefixed
from .research.strategy_registry import (
    DataCapabilityRequirement,
    ResearchStrategyDataRequirements,
    research_strategy_data_requirements,
)


RUNTIME_DATA_PROVIDER_NAME = "sqlite_runtime_data_provider"
RUNTIME_DATA_PROVIDER_VERSION = "1"
RUNTIME_DATA_CONTRACT_SCHEMA_VERSION = 1

RUNTIME_DATA_CAPABILITY_NAMES = (
    "candles",
    "orderbook_top",
    "orderbook_depth",
    "trades",
    "funding",
    "open_interest",
)

_CAPABILITY_ALIASES = {
    "candle": "candles",
    "candles": "candles",
    "ohlcv": "candles",
    "top_of_book": "orderbook_top",
    "orderbook_top": "orderbook_top",
    "orderbook_top_snapshot": "orderbook_top",
    "orderbook_top_snapshots": "orderbook_top",
    "l2_depth_snapshot": "orderbook_depth",
    "l2_depth": "orderbook_depth",
    "depth": "orderbook_depth",
    "orderbook_depth": "orderbook_depth",
    "orderbook_depth_levels": "orderbook_depth",
    "trade_ticks": "trades",
    "trades": "trades",
    "funding": "funding",
    "funding_rates": "funding",
    "open_interest": "open_interest",
}

_CAPABILITY_TABLES = {
    "candles": ("candles",),
    "orderbook_top": ("orderbook_top_snapshots",),
    "orderbook_depth": ("orderbook_depth_levels",),
    "trades": ("trades",),
    "funding": ("funding",),
    "open_interest": ("open_interest",),
}


def normalize_runtime_data_capability(name: str) -> str:
    normalized = str(name or "").strip().lower()
    normalized = normalized.replace("-", "_").replace(" ", "_")
    if not normalized:
        raise ValueError("runtime_data_capability_missing")
    return _CAPABILITY_ALIASES.get(normalized, normalized)


def runtime_data_provider_contract_payload() -> dict[str, object]:
    return {
        "schema_version": RUNTIME_DATA_CONTRACT_SCHEMA_VERSION,
        "provider_name": RUNTIME_DATA_PROVIDER_NAME,
        "provider_version": RUNTIME_DATA_PROVIDER_VERSION,
        "capabilities": list(RUNTIME_DATA_CAPABILITY_NAMES),
        "capability_tables": {
            name: list(_CAPABILITY_TABLES[name]) for name in RUNTIME_DATA_CAPABILITY_NAMES
        },
        "unsupported_required_capability_policy": "fail_closed",
        "optional_missing_policy": "warn_by_default",
        "snapshot_contract": "RuntimeFeatureSnapshot.v1",
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
            research_requirements = research_strategy_data_requirements(strategy_name)
            normalized = self._normalize_research_requirements(research_requirements)
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
        return self.availability_report_for_requirements(
            requirements,
            pair=str(getattr(getattr(strategy_set, "market_scope", None), "pair", "") or ""),
            interval=str(getattr(getattr(strategy_set, "market_scope", None), "interval", "") or ""),
            through_ts_ms=through_ts_ms,
        )

    def availability_report_for_requirements(
        self,
        requirements: RuntimeStrategyDataRequirements,
        *,
        pair: str,
        interval: str,
        through_ts_ms: int | None,
    ) -> RuntimeDataAvailabilityReport:
        coverage = {
            capability: self._coverage_for_capability(
                capability,
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
        if "candles" in requirements.all_names:
            candle = self._latest_candle(pair=pair, interval=interval, through_ts_ms=through_ts_ms)
            if candle is None:
                return None
            candle_ts, close, candle_index = candle
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
            "runtime_data_availability_report_hash": report.report_hash,
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
            return self._candle_coverage(pair=pair, interval=interval, through_ts_ms=through_ts_ms)
        return self._generic_ts_coverage(
            capability,
            pair=pair,
            through_ts_ms=through_ts_ms,
        )

    def _candle_coverage(
        self,
        *,
        pair: str,
        interval: str,
        through_ts_ms: int | None,
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
            )
        first_ts = int(row[1])
        last_ts = int(row[2])
        return RuntimeDataCapabilityCoverage(
            capability="candles",
            status="PASS",
            row_count=count,
            first_ts=first_ts,
            last_ts=last_ts,
            selected_ts=last_ts,
            coverage_pct=100.0,
            source_tables_or_streams=(table,),
        )

    def _generic_ts_coverage(
        self,
        capability: str,
        *,
        pair: str,
        through_ts_ms: int | None,
    ) -> RuntimeDataCapabilityCoverage:
        table = _CAPABILITY_TABLES[capability][0]
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
        return RuntimeDataCapabilityCoverage(
            capability=capability,
            status="PASS",
            row_count=count,
            first_ts=int(row[1]),
            last_ts=int(row[2]),
            selected_ts=int(row[2]),
            coverage_pct=100.0,
            source_tables_or_streams=(table,),
        )

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
