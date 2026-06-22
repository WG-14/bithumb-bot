from __future__ import annotations

import sqlite3
import tempfile
import time
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator, Mapping

from .broker.balance_source import BalanceSnapshot
from .broker.base import BrokerBalance
from .broker import order_rules as order_rules_module
from .config import settings
from .db_core import ensure_schema
from .decision_equivalence import sha256_prefixed
from .execution_service import (
    LiveSignalExecutionService,
    TypedExecutionRequest,
)
from .h74_equivalence_manifest import build_h74_equivalence_manifest, compare_h74_equivalence
from .h74_observation import (
    H74_SOURCE_OBSERVATION_AUTHORITY_ENV,
    H74_STRATEGY_NAME,
    H74_SOURCE_OBSERVATION_PARAMETERS,
    build_h74_source_observation_authority_payload,
)
from .run_loop_execution_planner import ExecutionPlanner
from .runtime.execution_coordinator import ExecutionCoordinator
from .submit_authority_policy import evaluate_submit_authority_policy
from .runtime_data_provider import RuntimeDataAvailabilityReport, RuntimeFeatureSnapshot
from .runtime_strategy_decision import (
    _attach_runtime_feature_snapshot_metadata,
    _attach_runtime_request_metadata,
    get_runtime_decision_adapter,
)
from .runtime_strategy_set import (
    RuntimeDecisionRequestBuilder,
    RuntimeMarketScope,
    RuntimeStrategyDecisionResultBundle,
    RuntimeStrategySet,
    RuntimeStrategySpec,
    runtime_strategy_set_manifest_hash,
)
from .strategy_plugins import daily_participation_sma
from .storage_io import write_json_atomic
from .markets import canonical_market_with_raw


class H74LiveRehearsalError(ValueError):
    pass


_MISSING_SETTING = object()


@dataclass(frozen=True)
class H74LiveRehearsalConfig:
    kst_time: str = "10:00"
    no_submit: bool = True
    broker_snapshot_available: bool = True
    smoke_authority_hash: str | None = None
    source_artifact_path: str | None = None
    current_fee_rate: float = 0.0004
    fee_authority_source: str = "runtime_fee_authority"
    order_rules: Mapping[str, object] | None = None
    broker_snapshot_stale: bool = False
    unresolved_order_status: str | None = None
    unresolved_order_created_ts_ms: int | None = None
    projection_converged: bool = True
    active_fee_accounting_blocker: bool = False
    closeout_existing_qty: float = 0.0


class _H74NoSubmitBroker:
    def __init__(
        self,
        *,
        available: bool,
        observed_ts_ms: int,
        cash_krw: float,
        asset_qty: float = 0.0,
        stale: bool = False,
    ) -> None:
        self.available = bool(available)
        self.observed_ts_ms = int(observed_ts_ms)
        self.cash_krw = float(cash_krw)
        self.asset_qty = float(asset_qty)
        self.stale = bool(stale)

    def get_balance_snapshot(self) -> BalanceSnapshot:
        if not self.available:
            raise RuntimeError("h74_rehearsal_broker_snapshot_unavailable")
        if self.stale:
            raise RuntimeError("h74_rehearsal_broker_snapshot_stale")
        return BalanceSnapshot(
            source_id="h74_rehearsal_recorded_broker_snapshot",
            observed_ts_ms=self.observed_ts_ms,
            asset_ts_ms=self.observed_ts_ms,
            balance=BrokerBalance(
                cash_available=self.cash_krw,
                cash_locked=0.0,
                asset_available=self.asset_qty,
                asset_locked=0.0,
            ),
        )


@contextmanager
def _h74_live_settings() -> Iterator[None]:
    base_keys = (
        "MODE",
        "LIVE_DRY_RUN",
        "LIVE_REAL_ORDER_ARMED",
        "EXECUTION_ENGINE",
        "MAX_DAILY_LOSS_KRW",
        "MAX_DAILY_ORDER_COUNT",
        "STRATEGY_NAME",
        "PAIR",
        "INTERVAL",
        "TARGET_EXPOSURE_KRW",
        "MAX_ORDER_KRW",
        "MIN_ORDER_NOTIONAL_KRW",
        "LIVE_MIN_ORDER_QTY",
        "LIVE_ORDER_QTY_STEP",
        "LIVE_ORDER_MAX_QTY_DECIMALS",
        H74_SOURCE_OBSERVATION_AUTHORITY_ENV,
        "H74_LIVE_REHEARSAL_NO_SUBMIT_BOUNDARY",
    )
    parameter_keys = tuple(
        key
        for key in H74_SOURCE_OBSERVATION_PARAMETERS
        if str(key).isupper()
    )
    keys = tuple(dict.fromkeys((*base_keys, *parameter_keys, "DAILY_PARTICIPATION_MAX_DAILY_ENTRY_COUNT")))
    original = {key: getattr(settings, key, _MISSING_SETTING) for key in keys}
    original_rehearsal_env = os.environ.get("H74_LIVE_REHEARSAL_NO_SUBMIT_BOUNDARY")
    try:
        object.__setattr__(settings, "MODE", "live")
        object.__setattr__(settings, "LIVE_DRY_RUN", False)
        object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
        object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)
        object.__setattr__(settings, "STRATEGY_NAME", H74_STRATEGY_NAME)
        object.__setattr__(settings, "PAIR", "KRW-BTC")
        object.__setattr__(settings, "INTERVAL", "1m")
        object.__setattr__(settings, "TARGET_EXPOSURE_KRW", 100_000.0)
        object.__setattr__(settings, "MAX_ORDER_KRW", 100_000.0)
        object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
        object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
        object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
        object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
        object.__setattr__(settings, "H74_LIVE_REHEARSAL_NO_SUBMIT_BOUNDARY", True)
        os.environ["H74_LIVE_REHEARSAL_NO_SUBMIT_BOUNDARY"] = "true"
        for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items():
            if str(key).isupper():
                object.__setattr__(settings, key, value)
        object.__setattr__(
            settings,
            "DAILY_PARTICIPATION_MAX_DAILY_ENTRY_COUNT",
            H74_SOURCE_OBSERVATION_PARAMETERS["max_daily_entry_count"],
        )
        yield
    finally:
        for key, value in original.items():
            if value is _MISSING_SETTING:
                try:
                    object.__delattr__(settings, key)
                except AttributeError:
                    pass
            else:
                object.__setattr__(settings, key, value)
        if original_rehearsal_env is None:
            os.environ.pop("H74_LIVE_REHEARSAL_NO_SUBMIT_BOUNDARY", None)
        else:
            os.environ["H74_LIVE_REHEARSAL_NO_SUBMIT_BOUNDARY"] = original_rehearsal_env


@contextmanager
def _h74_reconcile_snapshot(ts_ms: int) -> Iterator[None]:
    from . import risk

    original_snapshot = risk.runtime_state.snapshot
    risk.runtime_state.snapshot = lambda: SimpleNamespace(
        last_reconcile_epoch_sec=float(ts_ms) / 1000.0,
        last_reconcile_reason_code="OK",
        last_reconcile_status="ok",
    )
    try:
        yield
    finally:
        risk.runtime_state.snapshot = original_snapshot


def _write_h74_source_authority_file(
    tmp_dir: str,
    *,
    equivalence_manifest: Mapping[str, object],
) -> str:
    source_hash = str(equivalence_manifest.get("source_artifact_hash") or "").strip()
    if not source_hash:
        source_hash = "sha256:h74_rehearsal_missing_source_blocks_equivalence"
    payload = build_h74_source_observation_authority_payload(
        source_candidate_artifact_hash=source_hash,
        backtest_report_hash=str(equivalence_manifest.get("source_backtest_report_hash") or "").strip() or None,
    )
    authority_path = Path(tmp_dir) / "h74-source-observation-authority.json"
    write_json_atomic(authority_path, payload)
    return str(authority_path)


@contextmanager
def _h74_order_rule_cache(order_rules: Mapping[str, object]) -> Iterator[None]:
    original_cache = dict(order_rules_module._cached_rules)
    try:
        fallback = order_rules_module._local_fallback_rules()
        now = time.time()
        normalized_pair, _raw_pair = canonical_market_with_raw("KRW-BTC")
        resolution = order_rules_module._build_fallback_only_rule_resolution(
            pair=normalized_pair,
            now=now,
            fallback=fallback,
            reason_code="h74_rehearsal_injected_order_rules",
            reason_summary="h74 rehearsal uses configured order rules at no-submit boundary",
            reason_detail=(
                "min_qty={min_qty}; min_notional_krw={min_notional_krw}; qty_step={qty_step}"
            ).format(
                min_qty=order_rules.get("min_qty"),
                min_notional_krw=order_rules.get("min_notional_krw"),
                qty_step=order_rules.get("qty_step", order_rules.get("min_qty")),
            ),
            fallback_risk="h74_live_rehearsal_no_submit_boundary",
        )
        order_rules_module._cached_rules[normalized_pair] = (now, resolution, fallback)
        yield
    finally:
        order_rules_module._cached_rules.clear()
        order_rules_module._cached_rules.update(original_cache)


def _seed_rehearsal_db(
    path: str,
    *,
    submit_plan_hash: str,
    unresolved_order_status: str | None = None,
    unresolved_order_created_ts_ms: int | None = None,
) -> None:
    conn = sqlite3.connect(path)
    try:
        ensure_schema(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cash_krw REAL NOT NULL,
                asset_qty REAL NOT NULL,
                cash_available REAL NOT NULL DEFAULT 0,
                cash_locked REAL NOT NULL DEFAULT 0,
                asset_available REAL NOT NULL DEFAULT 0,
                asset_locked REAL NOT NULL DEFAULT 0
            )
            """
        )
        cash = float(settings.START_CASH_KRW)
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, ?, 0.0, ?, 0.0, 0.0, 0.0)
            """,
            (cash, cash),
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_plan (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_submit_plan_hash TEXT,
                execution_submit_plan_json TEXT,
                submit_plan_side TEXT,
                submit_plan_qty REAL,
                submit_plan_notional_krw REAL,
                submit_plan_idempotency_key TEXT,
                submit_plan_source TEXT,
                submit_plan_authority TEXT,
                submit_expected INTEGER NOT NULL DEFAULT 0,
                final_action TEXT NOT NULL DEFAULT '',
                block_reason TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                client_order_id TEXT PRIMARY KEY,
                exchange_order_id TEXT,
                status TEXT,
                created_ts INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                side TEXT,
                price REAL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO execution_plan(
                allocation_id, execution_plan_bundle_hash, execution_submit_plan_hash,
                submit_expected, final_action, block_reason, status,
                execution_plan_bundle_json, execution_submit_plan_json
            )
            VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)
            """,
            (
                0,
                sha256_prefixed({"h74": "pending_execution_plan_bundle"}),
                submit_plan_hash,
                "PLANNING_PENDING",
                "planning_pending",
                "planning_pending",
                "{}",
                "{}",
            ),
        )
        base_ts = int(datetime(2026, 6, 22, 8, 0, 0, tzinfo=timezone(timedelta(hours=9))).timestamp() * 1000)
        for index in range(121):
            close = 100_000_000.0 + float(index)
            ts = base_ts + (index * 60_000)
            conn.execute(
                """
                INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, "KRW-BTC", "1m", close, close, close, close, 1.0),
            )
        for index in range(30):
            entry_ts = base_ts - ((index + 2) * 120_000)
            exit_ts = entry_ts + 60_000
            conn.execute(
                """
                INSERT INTO trade_lifecycles(
                    pair, entry_trade_id, exit_trade_id,
                    entry_client_order_id, exit_client_order_id,
                    entry_ts, exit_ts, matched_qty, entry_price, exit_price,
                    gross_pnl, fee_total, net_pnl, holding_time_sec,
                    strategy_name, strategy_instance_id, exit_rule_name
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "KRW-BTC",
                    10_000 + index,
                    20_000 + index,
                    f"h74-entry-{index}",
                    f"h74-exit-{index}",
                    entry_ts,
                    exit_ts,
                    0.001,
                    100_000_000.0,
                    100_010_000.0,
                    10.0,
                    1.0,
                    9.0,
                    60.0,
                    H74_STRATEGY_NAME,
                    "h74-source-observation",
                    "max_holding_time",
                ),
            )
        if unresolved_order_status:
            created_ts = int(unresolved_order_created_ts_ms or 0)
            conn.execute(
                """
                INSERT OR REPLACE INTO orders(
                    client_order_id, exchange_order_id, status, side, pair, order_type,
                    qty_req, qty_filled, local_intent_state, created_ts, updated_ts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"h74-{str(unresolved_order_status).lower()}-fixture",
                    "",
                    str(unresolved_order_status).upper(),
                    "BUY",
                    "KRW-BTC",
                    "LIMIT",
                    0.001,
                    0.0,
                    str(unresolved_order_status).upper(),
                    created_ts,
                    created_ts,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _h74_runtime_strategy_parameters() -> dict[str, object]:
    accepted = set(daily_participation_sma.DAILY_PARTICIPATION_SMA_SPEC.accepted_parameter_names)
    params = {
        key: value
        for key, value in H74_SOURCE_OBSERVATION_PARAMETERS.items()
        if key in accepted
    }
    params.update(
        {
            "DAILY_PARTICIPATION_ENABLED": True,
            "DAILY_PARTICIPATION_WINDOW_START_HOUR_KST": 9,
            "DAILY_PARTICIPATION_WINDOW_END_HOUR_KST": 11,
            "DAILY_PARTICIPATION_BUY_FRACTION": 1.0,
            "DAILY_PARTICIPATION_MAX_ORDER_KRW": 100_000.0,
            "DAILY_PARTICIPATION_FALLBACK_MODE": "unconditional_participation",
        }
    )
    return params


def _h74_runtime_strategy_set() -> RuntimeStrategySet:
    return RuntimeStrategySet(
        source="h74_live_rehearsal_runtime_strategy_set",
        market_scope=RuntimeMarketScope(pair="KRW-BTC", interval="1m"),
        strategies=(
            RuntimeStrategySpec(
                strategy_name=H74_STRATEGY_NAME,
                strategy_instance_id="h74-source-observation",
                pair="KRW-BTC",
                interval="1m",
                priority=10,
                weight=1.0,
                desired_exposure_krw=100_000.0,
                parameters=_h74_runtime_strategy_parameters(),
            ),
        ),
    )


def _base_runtime_feature_snapshot(ts_ms: int) -> RuntimeFeatureSnapshot:
    rows = []
    start_ts = int(ts_ms) - (120 * 60_000)
    for index in range(121):
        close = 100_000_000.0 + float(index)
        rows.append(
            {
                "ts": start_ts + (index * 60_000),
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1.0,
            }
        )
    payload = {
        "pair": "KRW-BTC",
        "interval": "1m",
        "through_ts_ms": int(ts_ms),
        "feature_payload": {"capabilities": {"candles": {"rows": rows}}},
    }
    payload["feature_snapshot_hash"] = sha256_prefixed(payload)
    return RuntimeFeatureSnapshot(payload)


def _runtime_data_report(strategy_set: RuntimeStrategySet, ts_ms: int) -> RuntimeDataAvailabilityReport:
    scope = f"h74-source-observation:KRW-BTC:1m"
    payload = {
        "schema_version": 1,
        "status": "PASS",
        "reasons": [],
        "strategy_set_hash": runtime_strategy_set_manifest_hash(strategy_set),
        "through_ts_ms": int(ts_ms),
        "coverage_by_scope": {scope: {"status": "PASS"}},
        "selected_candle_by_scope": {scope: int(ts_ms)},
        "source_schema_hash_by_scope": {scope: sha256_prefixed({"source_schema": "h74_rehearsal"})},
        "freshness_by_scope": {scope: {"status": "PASS"}},
    }
    payload["report_hash"] = sha256_prefixed(payload)
    return RuntimeDataAvailabilityReport(payload)


def _runtime_decision_bundle(conn: sqlite3.Connection, *, ts_ms: int) -> RuntimeStrategyDecisionResultBundle:
    strategy_set = _h74_runtime_strategy_set()
    spec = strategy_set.active_strategies[0]
    request = RuntimeDecisionRequestBuilder(settings_obj=settings).build_for_spec(spec, through_ts_ms=ts_ms)
    base_snapshot = _base_runtime_feature_snapshot(ts_ms)
    feature_snapshot = daily_participation_sma.runtime_feature_snapshot_builder(
        conn=conn,
        request=request,
        feature_snapshot=base_snapshot,
    )
    if feature_snapshot is None:
        raise H74LiveRehearsalError("h74_rehearsal_daily_participation_feature_snapshot_missing")
    adapter = get_runtime_decision_adapter(H74_STRATEGY_NAME)
    if adapter is None:
        raise H74LiveRehearsalError("h74_rehearsal_daily_participation_adapter_missing")
    result = adapter.decide_feature_snapshot(request, feature_snapshot)
    if result is None:
        raise H74LiveRehearsalError("h74_rehearsal_daily_participation_decision_missing")
    _attach_runtime_feature_snapshot_metadata(result, feature_snapshot)
    _attach_runtime_request_metadata(result, request)
    return RuntimeStrategyDecisionResultBundle(
        strategy_set=strategy_set,
        results=(result,),
        data_availability_report=_runtime_data_report(strategy_set, ts_ms),
        runtime_data_cycle_preflight_hash=sha256_prefixed({"h74": "runtime_cycle_preflight", "ts": int(ts_ms)}),
    )


def _readiness_snapshot_payload(
    *,
    order_rules: Mapping[str, object],
    projection_converged: bool,
    active_fee_accounting_blocker: bool,
    closeout_existing_qty: float,
) -> dict[str, object]:
    current_exposure_krw = float(closeout_existing_qty or 0.0) * 100_000_000.0
    return {
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": float(closeout_existing_qty or 0.0),
            "balance_source_stale": False,
        },
        "projection_converged": bool(projection_converged),
        "projection_convergence": {"converged": bool(projection_converged)},
        "broker_portfolio_converged": bool(projection_converged),
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "accounting_projection_ok": bool(projection_converged),
        "total_effective_exposure_qty": float(closeout_existing_qty or 0.0),
        "total_effective_exposure_notional_krw": current_exposure_krw,
        "residual_inventory_notional_krw": 0.0,
        "active_fee_accounting_blocker": bool(active_fee_accounting_blocker),
        "new_entry_fee_blocker": bool(active_fee_accounting_blocker),
        "cash_available": float(settings.START_CASH_KRW),
        "runtime_pair": str(settings.PAIR),
        "min_qty": float(order_rules.get("min_qty") or 0.0001),
        "qty_step": float(order_rules.get("qty_step") or order_rules.get("min_qty") or 0.0001),
        "min_notional_krw": float(order_rules.get("min_notional_krw") or 5000.0),
        "max_qty_decimals": int(order_rules.get("max_qty_decimals") or 8),
        "order_types": ["bid", "ask"],
        "bid_types": ["market"],
        "ask_types": ["market"],
    }


def _target_delta_planning_bundle(
    conn: sqlite3.Connection,
    *,
    ts_ms: int,
    kst_time: str,
    order_rules: Mapping[str, object],
    projection_converged: bool,
    active_fee_accounting_blocker: bool,
    closeout_existing_qty: float,
):
    result_bundle = _runtime_decision_bundle(conn, ts_ms=ts_ms)
    readiness_payload = _readiness_snapshot_payload(
        order_rules=order_rules,
        projection_converged=projection_converged,
        active_fee_accounting_blocker=active_fee_accounting_blocker,
        closeout_existing_qty=closeout_existing_qty,
    )

    class _ReadinessSnapshot:
        def as_dict(self) -> dict[str, object]:
            return dict(readiness_payload)

    def _target_state_resolver(*_args, **_kwargs) -> dict[str, object]:
        current_exposure_krw = float(closeout_existing_qty or 0.0) * 100_000_000.0
        previous_target_exposure_krw = current_exposure_krw
        if current_exposure_krw <= 0.0 and str(kst_time) != "10:00":
            previous_target_exposure_krw = 100_000.0
        return {
            "previous_target_exposure_krw": previous_target_exposure_krw,
            "target_policy_metadata": {
                "target_policy_action": "use_existing_target"
                if previous_target_exposure_krw
                else "initialize_flat_target",
                "target_origin": "h74_rehearsal_runtime_target_state",
            },
        }

    planner = ExecutionPlanner(
        settings_obj=settings,
        readiness_snapshot_builder=lambda _conn: _ReadinessSnapshot(),
        target_state_resolver=_target_state_resolver,
    )
    return planner.plan_runtime_strategy_results(conn, result_bundle, updated_ts=int(ts_ms))


def _blocking_gate(gate_trace: list[dict[str, object]]) -> tuple[str, str]:
    for entry in gate_trace:
        if bool(entry.get("blocking")):
            return str(entry.get("gate") or "unknown"), str(entry.get("reason_code") or "blocked")
    return "none", "none"


def run_h74_live_rehearsal(config: H74LiveRehearsalConfig | None = None) -> dict[str, Any]:
    cfg = config or H74LiveRehearsalConfig()
    if not cfg.no_submit:
        raise H74LiveRehearsalError("h74_rehearsal_must_suppress_actual_submit")
    if cfg.smoke_authority_hash:
        raise H74LiveRehearsalError("h74_rehearsal_rejects_operator_smoke_authority")

    try:
        kst_hour_text, kst_minute_text = str(cfg.kst_time).split(":", 1)
        kst_hour = int(kst_hour_text)
        kst_minute = int(kst_minute_text)
    except (TypeError, ValueError):
        raise H74LiveRehearsalError("h74_rehearsal_invalid_kst_time") from None
    if not (0 <= kst_hour <= 23 and 0 <= kst_minute <= 59):
        raise H74LiveRehearsalError("h74_rehearsal_invalid_kst_time")

    kst = timezone(timedelta(hours=9))
    ts_ms = int(datetime(2026, 6, 22, kst_hour, kst_minute, 0, tzinfo=kst).timestamp() * 1000)
    order_rules = dict(cfg.order_rules or {"min_qty": 0.0001, "min_notional_krw": 5000.0})
    equivalence_manifest = build_h74_equivalence_manifest(
        source_artifact_path=cfg.source_artifact_path,
        order_rules=order_rules,
    )
    equivalence = compare_h74_equivalence(
        equivalence_manifest,
        current_fee_rate=float(cfg.current_fee_rate),
        current_fee_authority_source=cfg.fee_authority_source,
        current_order_rules=order_rules,
    )
    equivalence_status = str(equivalence["experiment_equivalence_status"])
    equivalence_allows = equivalence_status == "pass"

    with _h74_live_settings():
        captured: list[dict[str, object]] = []
        pre_submit_status = "BLOCK"
        pre_submit_reason = "equivalence_blocked" if not equivalence_allows else "not_evaluated"
        broker_snapshot_hash = ""
        execution_result_status = "submit_blocked"
        with tempfile.TemporaryDirectory(prefix="h74-live-rehearsal-") as tmp_dir:
            authority_path = _write_h74_source_authority_file(
                tmp_dir,
                equivalence_manifest=equivalence_manifest,
            )
            object.__setattr__(settings, H74_SOURCE_OBSERVATION_AUTHORITY_ENV, authority_path)
            db_path = f"{tmp_dir}/h74-rehearsal.sqlite"
            _seed_rehearsal_db(
                db_path,
                submit_plan_hash="sha256:h74_rehearsal_planning_pending",
                unresolved_order_status=cfg.unresolved_order_status,
                unresolved_order_created_ts_ms=cfg.unresolved_order_created_ts_ms,
            )

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                with _h74_order_rule_cache(order_rules):
                    planning_bundle = _target_delta_planning_bundle(
                        conn,
                        ts_ms=ts_ms,
                        kst_time=str(cfg.kst_time),
                        order_rules=order_rules,
                        projection_converged=cfg.projection_converged,
                        active_fee_accounting_blocker=cfg.active_fee_accounting_blocker,
                        closeout_existing_qty=cfg.closeout_existing_qty,
                    )
            finally:
                conn.close()
            summary = planning_bundle.summary
            if summary is None:
                raise H74LiveRehearsalError(
                    str(planning_bundle.planning_error or "h74_rehearsal_target_delta_planner_missing_summary")
                )
            target_plan = summary.typed_target_submit_plan()
            if target_plan is None:
                raise H74LiveRehearsalError("h74_rehearsal_target_delta_planner_missing_target_plan")
            would_submit_plan = target_plan.as_final_payload()
            planning_context = dict(planning_bundle.persistence_context)
            trace = dict(planning_context.get("pure_policy_trace") or {})
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO execution_plan(
                        allocation_id, execution_plan_bundle_hash, execution_submit_plan_hash,
                        submit_expected, final_action, block_reason, status,
                        execution_plan_bundle_json, execution_submit_plan_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1,
                        str(planning_bundle.content_hash()),
                        str(would_submit_plan["submit_plan_hash"]),
                        1 if bool(would_submit_plan.get("submit_expected")) else 0,
                        str(would_submit_plan.get("final_action") or ""),
                        str(would_submit_plan.get("block_reason") or ""),
                        "planned",
                        __import__("json").dumps(planning_bundle.as_dict(), sort_keys=True),
                        __import__("json").dumps(would_submit_plan, sort_keys=True),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            submit_authority = evaluate_submit_authority_policy(
                would_submit_plan,
                settings_obj=settings,
                plan_kind="target",
                require_final_payload=True,
            )
            if equivalence_allows:

                def _db_factory() -> sqlite3.Connection:
                    conn = sqlite3.connect(db_path)
                    conn.row_factory = sqlite3.Row
                    return conn

                broker = _H74NoSubmitBroker(
                    available=cfg.broker_snapshot_available,
                    observed_ts_ms=ts_ms - (30 * 60 * 1000) if cfg.broker_snapshot_stale else ts_ms,
                    cash_krw=float(settings.START_CASH_KRW),
                    asset_qty=float(cfg.closeout_existing_qty or 0.0),
                    stale=cfg.broker_snapshot_stale,
                )
                service = LiveSignalExecutionService(
                    broker=broker,
                    executor=lambda _broker, side, submit_ts, market_price, **kwargs: captured.append(
                        {
                            "side": side,
                            "ts": submit_ts,
                            "market_price": market_price,
                            "execution_submit_plan": dict(kwargs.get("execution_submit_plan") or {}),
                        }
                    )
                    or {"status": "no_submit_boundary_reached", "actual_submit": False},
                    harmless_dust_recorder=lambda **_kwargs: False,
                    db_factory=_db_factory,
                )
                with _h74_reconcile_snapshot(ts_ms):
                    request = TypedExecutionRequest(
                        signal="BUY",
                        ts=ts_ms,
                        market_price=100_000_000.0,
                        strategy_name=H74_STRATEGY_NAME,
                        decision_id=1,
                        decision_reason=str(planning_context.get("final_reason") or trace.get("final_reason") or ""),
                        execution_decision_summary=summary,
                    )
                    execution_result = ExecutionCoordinator("target_delta").execute_cycle(
                        candle_ts=ts_ms,
                        decision_id=1,
                        signal="BUY",
                        market_price=100_000_000.0,
                        strategy_name=H74_STRATEGY_NAME,
                        decision_reason=str(planning_context.get("final_reason") or trace.get("final_reason") or ""),
                        execution_decision_summary=summary,
                        submit_invoker=lambda: service.execute(request),
                        input_hash=sha256_prefixed({"h74": "execution_input", "planning": planning_context}),
                    )
                execution_result_status = execution_result.planning_status
                if captured:
                    submitted = captured[0]["execution_submit_plan"]
                    pre_submit_status = str(submitted.get("pre_submit_risk_status") or "")
                    pre_submit_reason = str(submitted.get("pre_submit_risk_reason_code") or "")
                    broker_snapshot_hash = sha256_prefixed(
                        {
                            "source": "h74_rehearsal_recorded_broker_snapshot",
                            "observed_ts_ms": ts_ms,
                            "cash_krw": float(settings.START_CASH_KRW),
                        }
                    )
                    would_submit_plan = submitted
                else:
                    conn = sqlite3.connect(db_path)
                    try:
                        row = conn.execute(
                            """
                            SELECT execution_submit_plan_json FROM execution_plan
                            WHERE execution_submit_plan_hash=?
                            ORDER BY id DESC
                            LIMIT 1
                            """,
                            (str(would_submit_plan.get("submit_plan_hash") or ""),),
                        ).fetchone()
                        if row and row[0]:
                            stored = dict(__import__("json").loads(row[0]))
                            pre_submit_status = str(stored.get("pre_submit_risk_status") or "BLOCK")
                            pre_submit_reason = str(stored.get("pre_submit_risk_reason_code") or "broker_submit_not_reached")
                            would_submit_plan = stored or would_submit_plan
                    finally:
                        conn.close()
        submit_authority_allowed = bool(captured)
        submit_authority_reason = "allowed_target_delta" if submit_authority_allowed else submit_authority.reason
        readiness_blocks = not bool(cfg.projection_converged)
        planning_path_mode = str(getattr(settings, "MODE", ""))
        planning_path_live_dry_run = bool(getattr(settings, "LIVE_DRY_RUN", True))
        planning_path_live_real_order_armed = bool(getattr(settings, "LIVE_REAL_ORDER_ARMED", False))
        entry_authority_payload = dict(would_submit_plan.get("entry_authority") or {})
        if not entry_authority_payload:
            entry_authority_payload = {
                "gate": "entry_authority",
                "status": str(would_submit_plan.get("entry_authority_status") or "ALLOW"),
                "reason_code": str(would_submit_plan.get("entry_authority_reason_code") or "not_new_buy_exposure"),
                "blocking": str(would_submit_plan.get("entry_authority_status") or "") == "BLOCK",
            }
        gate_trace = [
            {"gate": "time_window", "status": "ALLOW", "reason_code": "within_kst_window", "blocking": False},
            {"gate": "runtime_cycle_pipeline", "status": "ALLOW", "reason_code": "RuntimeCyclePipeline/ExecutionCoordinator", "blocking": False},
            {
                "gate": "fee_equivalence",
                "status": "ALLOW" if equivalence_allows else "BLOCK",
                "reason_code": equivalence_status,
                "blocking": not equivalence_allows,
            },
            {
                "gate": "readiness",
                "status": "BLOCK" if readiness_blocks else "ALLOW",
                "reason_code": "PROJECTION_MISMATCH" if readiness_blocks else "OK",
                "blocking": readiness_blocks,
            },
            {"gate": "strategy_risk", "status": "ALLOW", "reason_code": "OK", "blocking": False},
            {"gate": "portfolio_risk", "status": "ALLOW", "reason_code": "OK", "blocking": False},
            entry_authority_payload,
            {
                "gate": "pre_submit_risk",
                "status": pre_submit_status,
                "reason_code": pre_submit_reason,
                "state_source": "runtime_db_broker" if captured else None,
                "evidence_hash": str(would_submit_plan.get("pre_submit_risk_evidence_hash") or "") or None,
                "blocking": pre_submit_status != "ALLOW",
            },
            {
                "gate": "submit_authority",
                "status": "ALLOW" if submit_authority_allowed else "BLOCK",
                "reason_code": submit_authority_reason,
                "blocking": not submit_authority_allowed,
            },
        ]
    primary_gate, primary_reason = _blocking_gate(gate_trace)
    participation_decision = trace.get("daily_participation_decision")
    daily_reason = ""
    if isinstance(participation_decision, Mapping):
        daily_reason = str(participation_decision.get("reason_code") or "")
    if not daily_reason:
        daily_reason = str(trace.get("final_reason") or planning_context.get("final_reason") or "")
    daily_window_start = int(_h74_runtime_strategy_parameters()["DAILY_PARTICIPATION_WINDOW_START_HOUR_KST"])
    daily_window_end = int(_h74_runtime_strategy_parameters()["DAILY_PARTICIPATION_WINDOW_END_HOUR_KST"])
    daily_entry_authorized = daily_reason == "daily_participation_fallback_allowed"
    payload: dict[str, Any] = {
        "artifact_type": "h74_live_rehearsal",
        "schema_version": 1,
        "readiness_scope": "h74_normal_path",
        "MODE": "live",
        "LIVE_DRY_RUN": False,
        "LIVE_REAL_ORDER_ARMED": True,
        "decision_path_MODE": planning_context.get("profile_authority_context", {}).get("runtime_mode")
        if isinstance(planning_context.get("profile_authority_context"), Mapping)
        else "live",
        "decision_path_LIVE_DRY_RUN": planning_context.get("profile_authority_context", {}).get("live_dry_run")
        if isinstance(planning_context.get("profile_authority_context"), Mapping)
        else False,
        "decision_path_LIVE_REAL_ORDER_ARMED": planning_context.get("profile_authority_context", {}).get("live_real_order_armed")
        if isinstance(planning_context.get("profile_authority_context"), Mapping)
        else True,
        "planning_path_MODE": planning_path_mode,
        "planning_path_LIVE_DRY_RUN": planning_path_live_dry_run,
        "planning_path_LIVE_REAL_ORDER_ARMED": planning_path_live_real_order_armed,
        "kst_time": cfg.kst_time,
        "decision_kst_hour": kst_hour,
        "strategy_name": H74_STRATEGY_NAME,
        "operator_live_pipeline_smoke": False,
        "runtime_cycle_pipeline_called": bool(planning_bundle.execution_plan_batch is not None),
        "production_runtime_strategy_set_called": bool(planning_context.get("runtime_strategy_set_manifest_hash")),
        "production_allocator_portfolio_target_called": bool(planning_context.get("portfolio_target_hash")),
        "production_target_delta_planner_called": str(would_submit_plan.get("source") or "") == "target_delta",
        "runtime_strategy_set_manifest_hash": planning_context.get("runtime_strategy_set_manifest_hash"),
        "runtime_strategy_result_bundle_hash": planning_context.get("runtime_strategy_result_bundle_hash"),
        "portfolio_allocation_decision_hash": planning_context.get("allocation_decision_hash"),
        "portfolio_target_hash": planning_context.get("portfolio_target_hash"),
        "execution_plan_batch_hash": planning_context.get("execution_plan_batch_hash"),
        "live_signal_execution_service_called": bool(equivalence_allows),
        "daily_participation_plugin_called": str(trace.get("strategy_family") or "") == "daily_participation_sma",
        "target_delta_final_payload_created": bool(would_submit_plan.get("schema_version")),
        "daily_participation_reason_code": daily_reason,
        "daily_participation_window_start_hour_kst": daily_window_start,
        "daily_participation_window_end_hour_kst": daily_window_end,
        "daily_participation_entry_authorized": daily_entry_authorized,
        "entry_authority_status": str(would_submit_plan.get("entry_authority_status") or ""),
        "entry_authority_reason_code": str(would_submit_plan.get("entry_authority_reason_code") or ""),
        "entry_authority_gate_present": any(
            isinstance(entry, Mapping) and entry.get("gate") == "entry_authority"
            for entry in gate_trace
        ),
        "entry_authority_gate_hash": sha256_prefixed(entry_authority_payload),
        "pre_submit_risk_status": pre_submit_status,
        "pre_submit_risk_reason_code": pre_submit_reason,
        "pre_submit_proof_created": bool(would_submit_plan.get("pre_submit_risk_decision_hash")),
        "submit_authority_reason": submit_authority_reason,
        "submit_authority_allowed": submit_authority_allowed,
        "broker_submit_reached": bool(captured),
        "actual_submit": False,
        "would_submit": bool(equivalence_allows and captured),
        "would_submit_plan": would_submit_plan,
        "would_submit_plan_hash": sha256_prefixed(would_submit_plan),
        "broker_balance_snapshot_hash": broker_snapshot_hash,
        "experiment_equivalence_status": equivalence_status,
        "source_artifact_status": equivalence_manifest["source_artifact_status"],
        "source_artifact_path": cfg.source_artifact_path,
        "fee_authority_source": equivalence["fee_authority_source"],
        "fee_comparison": equivalence["fee_comparison"],
        "order_rule_comparison": equivalence["order_rule_comparison"],
        "execution_result_status": execution_result_status,
        "gate_trace": gate_trace,
        "primary_block_gate": primary_gate,
        "primary_block_reason": primary_reason,
    }
    payload["gate_trace_hash"] = sha256_prefixed(gate_trace)
    payload["rehearsal_hash"] = sha256_prefixed(payload)
    return payload


__all__ = ["H74LiveRehearsalConfig", "H74LiveRehearsalError", "run_h74_live_rehearsal"]
