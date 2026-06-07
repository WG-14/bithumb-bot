from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any, Mapping

from .. import runtime_state
from ..decision_equivalence import sha256_prefixed
from ..runtime_data_provider import RuntimeDataAvailabilityReport, RuntimeDataRequirementResolver, SQLiteRuntimeDataProvider
from .runtime_checkpoint import CheckpointDecision


@dataclass(frozen=True)
class RuntimeDataCyclePreflight:
    status: str
    reason_code: str | None
    latest_candle_ts: int | None
    latest_close: float | None
    closed_candle_ts: int | None
    incomplete_candle_ts: int | None
    candle_age_sec: float | None
    stale_cutoff_sec: int
    closed_candle_allowed: bool
    runtime_data_availability_report_hash: str | None
    coverage_by_scope: Mapping[str, object]
    selected_candle_by_scope: Mapping[str, object]
    freshness_by_scope: Mapping[str, object]
    runtime_data_preflight_reasons: tuple[str, ...] = ()
    runtime_data_preflight_warnings: tuple[str, ...] = ()
    checkpoint_decision: CheckpointDecision | None = None
    notification_event_hashes: tuple[str | None, ...] = ()
    sync_observed_epoch_sec: float | None = None

    @property
    def ok(self) -> bool:
        return self.status == "PASS" and self.closed_candle_allowed

    def as_dict(self) -> dict[str, object]:
        payload = {
            "artifact_type": "runtime_data_cycle_preflight",
            "schema_version": 1,
            "status": self.status,
            "reason_code": self.reason_code,
            "latest_candle_ts": self.latest_candle_ts,
            "closed_candle_ts": self.closed_candle_ts,
            "incomplete_candle_ts": self.incomplete_candle_ts,
            "candle_age_sec": self.candle_age_sec,
            "stale_cutoff_sec": self.stale_cutoff_sec,
            "closed_candle_allowed": bool(self.closed_candle_allowed),
            "runtime_data_availability_report_hash": self.runtime_data_availability_report_hash,
            "runtime_data_preflight_reasons": list(self.runtime_data_preflight_reasons),
            "runtime_data_preflight_warnings": list(self.runtime_data_preflight_warnings),
            "coverage_by_scope": dict(self.coverage_by_scope),
            "selected_candle_by_scope": dict(self.selected_candle_by_scope),
            "freshness_by_scope": dict(self.freshness_by_scope),
            "checkpoint_decision": (
                None
                if self.checkpoint_decision is None
                else {
                    "status": self.checkpoint_decision.status,
                    "allowed": self.checkpoint_decision.allowed,
                    "cycle_id": self.checkpoint_decision.cycle_id,
                    "reason": self.checkpoint_decision.reason,
                    "candle_ts": self.checkpoint_decision.candle_ts,
                }
            ),
            "notification_event_hashes": [item for item in self.notification_event_hashes if item],
            "sync_observed_epoch_sec": self.sync_observed_epoch_sec,
        }
        payload["decision_hash"] = sha256_prefixed(payload)
        return payload


@dataclass(frozen=True)
class RuntimeDataCyclePreflightProvider:
    container: object
    runtime_checkpoint: object
    runtime_events: object

    def evaluate(
        self,
        *,
        strategy_set: object,
        now_epoch_sec: float,
        interval_sec: int,
    ) -> RuntimeDataCyclePreflight:
        c = self.container
        c.market_sync(quiet=True)
        sync_observed_epoch_sec = c.clock()
        conn = c.db_factory()
        try:
            row = c.candle_reader(
                conn,
                pair=c.settings_obj.PAIR,
                interval=c.settings_obj.INTERVAL,
            )
            closed_row, incomplete_ts = c.closed_candle_selector(
                conn,
                pair=c.settings_obj.PAIR,
                interval=c.settings_obj.INTERVAL,
                interval_sec=interval_sec,
                now_ms=int(sync_observed_epoch_sec * 1000),
            )
            through_ts_ms = None if closed_row is None else _row_ts(closed_row)
            if not isinstance(conn, sqlite3.Connection):
                data_report = RuntimeDataAvailabilityReport(
                    {
                        "schema_version": 1,
                        "provider_name": "sqlite_runtime_data_provider",
                        "provider_version": "1",
                        "through_ts_ms": through_ts_ms,
                        "status": "PASS",
                        "reasons": [],
                        "warnings": ["runtime_data_preflight_skipped_non_sqlite_compat_connection"],
                        "capabilities_present": [],
                        "capabilities_missing": [],
                        "coverage_by_capability": {},
                        "coverage_by_scope": {},
                        "selected_candle_by_scope": {},
                        "freshness_by_scope": {},
                        "report_hash": sha256_prefixed(
                            {
                                "schema_version": 1,
                                "provider_name": "sqlite_runtime_data_provider",
                                "provider_version": "1",
                                "through_ts_ms": through_ts_ms,
                                "status": "PASS",
                                "warnings": ["runtime_data_preflight_skipped_non_sqlite_compat_connection"],
                            }
                        ),
                    }
                )
            else:
                try:
                    data_report = SQLiteRuntimeDataProvider(
                        conn,
                        resolver=RuntimeDataRequirementResolver(),
                    ).preflight(
                        strategy_set,
                        through_ts_ms=through_ts_ms,
                    )
                except Exception as exc:
                    reason = f"runtime_data_preflight_error:{type(exc).__name__}"
                    data_report = RuntimeDataAvailabilityReport(
                        {
                            "schema_version": 1,
                            "provider_name": "sqlite_runtime_data_provider",
                            "provider_version": "1",
                            "through_ts_ms": through_ts_ms,
                            "status": "FAIL",
                            "reasons": [reason],
                            "warnings": [],
                            "capabilities_present": [],
                            "capabilities_missing": [],
                            "coverage_by_capability": {},
                            "coverage_by_scope": {},
                            "selected_candle_by_scope": {},
                            "freshness_by_scope": {},
                            "error": f"{type(exc).__name__}: {exc}",
                            "report_hash": sha256_prefixed(
                                {
                                    "schema_version": 1,
                                    "provider_name": "sqlite_runtime_data_provider",
                                    "provider_version": "1",
                                    "through_ts_ms": through_ts_ms,
                                    "status": "FAIL",
                                    "reasons": [reason],
                                    "error": f"{type(exc).__name__}: {exc}",
                                }
                            ),
                        }
                    )
        finally:
            conn.close()
        stale_cutoff_sec = int(interval_sec) * 2
        if row is None:
            runtime_state.set_last_candle_observation(
                status="missing_after_sync",
                age_sec=None,
                sync_epoch_sec=sync_observed_epoch_sec,
                candle_ts_ms=None,
                detail="sync completed but latest candle row was not found",
            )
            return RuntimeDataCyclePreflight(
                status="FAIL",
                reason_code="no_candles_after_sync",
                latest_candle_ts=None,
                latest_close=None,
                closed_candle_ts=None,
                incomplete_candle_ts=None,
                candle_age_sec=None,
                stale_cutoff_sec=stale_cutoff_sec,
                closed_candle_allowed=False,
                runtime_data_availability_report_hash=data_report.report_hash,
                runtime_data_preflight_reasons=_report_tuple(data_report, "reasons"),
                runtime_data_preflight_warnings=_report_tuple(data_report, "warnings"),
                coverage_by_scope=_payload_mapping(data_report, "coverage_by_scope"),
                selected_candle_by_scope=_payload_mapping(data_report, "selected_candle_by_scope"),
                freshness_by_scope=_payload_mapping(data_report, "freshness_by_scope"),
                sync_observed_epoch_sec=sync_observed_epoch_sec,
            )
        last_ts = _row_ts(row)
        last_close = _row_close(row)
        candle_age_sec = max(0.0, (c.clock() * 1000 - last_ts) / 1000)
        runtime_state.set_last_candle_observation(
            status="ok",
            age_sec=candle_age_sec,
            sync_epoch_sec=sync_observed_epoch_sec,
            candle_ts_ms=last_ts,
            detail=(
                None
                if incomplete_ts is None
                else f"latest candle ts={incomplete_ts} still open; using latest fully closed candle"
            ),
        )
        if candle_age_sec > stale_cutoff_sec:
            return RuntimeDataCyclePreflight(
                status="FAIL",
                reason_code="stale_candle_detected",
                latest_candle_ts=last_ts,
                latest_close=last_close,
                closed_candle_ts=None if closed_row is None else _row_ts(closed_row),
                incomplete_candle_ts=None if incomplete_ts is None else int(incomplete_ts),
                candle_age_sec=candle_age_sec,
                stale_cutoff_sec=stale_cutoff_sec,
                closed_candle_allowed=False,
                runtime_data_availability_report_hash=data_report.report_hash,
                runtime_data_preflight_reasons=_report_tuple(data_report, "reasons"),
                runtime_data_preflight_warnings=_report_tuple(data_report, "warnings"),
                coverage_by_scope=_payload_mapping(data_report, "coverage_by_scope"),
                selected_candle_by_scope=_payload_mapping(data_report, "selected_candle_by_scope"),
                freshness_by_scope=_payload_mapping(data_report, "freshness_by_scope"),
                sync_observed_epoch_sec=sync_observed_epoch_sec,
            )
        checkpoint_decision = self.runtime_checkpoint.evaluate_closed_candle(
            closed_row=closed_row,
            incomplete_ts=incomplete_ts,
            last_processed_candle_ts_ms=runtime_state.snapshot().last_processed_candle_ts_ms,
            close_guard_ms=_close_guard_ms(interval_sec),
        )
        if not data_report.ok:
            return RuntimeDataCyclePreflight(
                status="FAIL",
                reason_code="runtime_data_preflight_failed",
                latest_candle_ts=last_ts,
                latest_close=last_close,
                closed_candle_ts=checkpoint_decision.candle_ts,
                incomplete_candle_ts=None if incomplete_ts is None else int(incomplete_ts),
                candle_age_sec=candle_age_sec,
                stale_cutoff_sec=stale_cutoff_sec,
                closed_candle_allowed=bool(checkpoint_decision.allowed),
                runtime_data_availability_report_hash=data_report.report_hash,
                runtime_data_preflight_reasons=_report_tuple(data_report, "reasons"),
                runtime_data_preflight_warnings=_report_tuple(data_report, "warnings"),
                coverage_by_scope=_payload_mapping(data_report, "coverage_by_scope"),
                selected_candle_by_scope=_payload_mapping(data_report, "selected_candle_by_scope"),
                freshness_by_scope=_payload_mapping(data_report, "freshness_by_scope"),
                checkpoint_decision=checkpoint_decision,
                sync_observed_epoch_sec=sync_observed_epoch_sec,
            )
        return RuntimeDataCyclePreflight(
            status="PASS",
            reason_code=None if checkpoint_decision.allowed else checkpoint_decision.cycle_id,
            latest_candle_ts=last_ts,
            latest_close=last_close,
            closed_candle_ts=checkpoint_decision.candle_ts,
            incomplete_candle_ts=None if incomplete_ts is None else int(incomplete_ts),
            candle_age_sec=candle_age_sec,
            stale_cutoff_sec=stale_cutoff_sec,
            closed_candle_allowed=bool(checkpoint_decision.allowed),
            runtime_data_availability_report_hash=data_report.report_hash,
            runtime_data_preflight_reasons=_report_tuple(data_report, "reasons"),
            runtime_data_preflight_warnings=_report_tuple(data_report, "warnings"),
            coverage_by_scope=_payload_mapping(data_report, "coverage_by_scope"),
            selected_candle_by_scope=_payload_mapping(data_report, "selected_candle_by_scope"),
            freshness_by_scope=_payload_mapping(data_report, "freshness_by_scope"),
            checkpoint_decision=checkpoint_decision,
            sync_observed_epoch_sec=sync_observed_epoch_sec,
        )


def _row_ts(row: object) -> int:
    return int(row["ts"]) if hasattr(row, "keys") else int(row[0])


def _row_close(row: object) -> float:
    return float(row["close"] if hasattr(row, "keys") else row[1])


def _payload_mapping(report: RuntimeDataAvailabilityReport, key: str) -> Mapping[str, object]:
    value = report.as_dict().get(key)
    return dict(value) if isinstance(value, Mapping) else {}


def _report_tuple(report: RuntimeDataAvailabilityReport, key: str) -> tuple[str, ...]:
    return tuple(str(item) for item in report.as_dict().get(key) or ())


def _close_guard_ms(interval_sec: int) -> int:
    interval_ms = max(1, int(interval_sec)) * 1000
    return max(2_000, min(30_000, interval_ms // 20))


__all__ = ["RuntimeDataCyclePreflight", "RuntimeDataCyclePreflightProvider"]
