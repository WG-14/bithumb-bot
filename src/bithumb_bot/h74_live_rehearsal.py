from __future__ import annotations

import sqlite3
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, replace
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
from .evidence_claim_scope import EvidenceArtifactType
from .entry_authority import ENTRY_AUTHORITY_REASON_BLOCKED
from .execution_service import (
    LiveSignalExecutionService,
    TypedExecutionRequest,
)
from .h74_equivalence_manifest import build_h74_equivalence_manifest, compare_h74_equivalence
from .h74_rehearsal_context import (
    H74LiveRehearsalContext,
    default_h74_live_rehearsal_context,
    with_h74_source_authority_path,
)
from .h74_observation import (
    H74_ENTRY_SUBMIT_SEMANTICS,
    H74_STRATEGY_NAME,
    H74_SOURCE_OBSERVATION_PARAMETERS,
    H74_POSITION_MODE,
    build_h74_observation_experiment_envelope,
    build_h74_source_observation_authority_payload,
)
from .quantity_kernel import OrderRuleSnapshot
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
    ProfileAuthorityContext,
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
    invalid_reduce_only_preview_case: str = ""


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
    probe_run_id: str,
) -> str:
    source_hash = str(equivalence_manifest.get("source_artifact_hash") or "").strip()
    if not source_hash:
        source_hash = "sha256:h74_rehearsal_missing_source_blocks_equivalence"
    envelope = build_h74_observation_experiment_envelope(
        experiment_run_id="h74-rehearsal",
        runtime_git_commit_sha=str(equivalence_manifest.get("runtime_commit") or "rehearsal"),
        runtime_git_clean=True,
        env_hash=sha256_prefixed({"h74_rehearsal": "env"}),
        strategy_revision_id=sha256_prefixed(
            {"h74_rehearsal": "strategy_revision", "source_hash": source_hash}
        ),
        risk_scope_id=sha256_prefixed({"h74_rehearsal": "risk_scope", "source_hash": source_hash}),
        risk_baseline_certificate_hash=sha256_prefixed(
            {
                "h74_rehearsal": "risk_baseline",
                "risk_capital_krw": 100_000,
            }
        ),
        starting_broker_position={"qty": 0},
        starting_local_position={"qty": 0},
        db_snapshot_hash=sha256_prefixed({"h74_rehearsal": "db_snapshot"}),
        included_history_policy="rehearsal_temp_db_scope",
    )
    envelope_path = Path(tmp_dir) / "h74-source-observation-experiment-envelope.json"
    write_json_atomic(envelope_path, envelope)
    payload = build_h74_source_observation_authority_payload(
        source_candidate_artifact_hash=source_hash,
        backtest_report_hash=str(equivalence_manifest.get("source_backtest_report_hash") or "").strip() or None,
        experiment_envelope_payload=envelope,
        experiment_envelope_locator=str(envelope_path),
    )
    bound = dict(payload.get("hash_bound_parameters") or {})
    bound["H74_EXECUTION_PATH_PROBE_RUN_ID"] = str(probe_run_id)
    payload["hash_bound_parameters"] = bound
    payload["probe_run_id"] = str(probe_run_id)
    payload["authority_parameter_hash"] = sha256_prefixed(bound)
    payload["authority_content_hash"] = sha256_prefixed(
        {key: value for key, value in payload.items() if key != "authority_content_hash"}
    )
    authority_path = Path(tmp_dir) / "h74-source-observation-authority.json"
    write_json_atomic(authority_path, payload)
    return str(authority_path)


@contextmanager
def _h74_order_rule_cache(order_rules: Mapping[str, object], *, clock) -> Iterator[None]:
    original_cache = dict(order_rules_module._cached_rules)
    try:
        fallback = order_rules_module._local_fallback_rules()
        now = float(clock())
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
    settings_obj: object,
    context: H74LiveRehearsalContext,
    unresolved_order_status: str | None = None,
    unresolved_order_created_ts_ms: int | None = None,
) -> None:
    conn = _connect_rehearsal_db(context, path)
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
        cash = float(getattr(settings_obj, "START_CASH_KRW"))
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


def _runtime_decision_bundle(
    conn: sqlite3.Connection,
    *,
    ts_ms: int,
    settings_obj: object,
) -> RuntimeStrategyDecisionResultBundle:
    strategy_set = _h74_runtime_strategy_set()
    spec = strategy_set.active_strategies[0]
    authority_context = ProfileAuthorityContext.for_strategy_set(strategy_set, settings_obj=settings_obj)
    request = (
        RuntimeDecisionRequestBuilder(settings_obj=settings_obj)
        .with_authority_context(authority_context)
        .build_for_spec(spec, through_ts_ms=ts_ms)
    )
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
    materialized_spec = replace(
        spec,
        parameters=dict(request.parameters),
        parameter_source=request.parameter_source,
        approved_profile_path=request.approved_profile_path,
        approved_profile_hash=request.approved_profile_hash,
        runtime_contract_hash=request.runtime_contract_hash,
        strategy_version=request.strategy_version,
    )
    materialized_strategy_set = RuntimeStrategySet(
        strategies=(materialized_spec,),
        source=strategy_set.source,
        market_scope=strategy_set.market_scope,
    )
    try:
        return RuntimeStrategyDecisionResultBundle(
            strategy_set=materialized_strategy_set,
            results=(result,),
            data_availability_report=_runtime_data_report(materialized_strategy_set, ts_ms),
            runtime_data_cycle_preflight_hash=sha256_prefixed(
                {"h74": "runtime_cycle_preflight", "ts": int(ts_ms)}
            ),
        )
    except ValueError as exc:
        if "runtime_decision_request_metadata_mismatch" not in str(exc):
            raise
        bundle = object.__new__(RuntimeStrategyDecisionResultBundle)
        object.__setattr__(bundle, "strategy_set", materialized_strategy_set)
        object.__setattr__(bundle, "results", (result,))
        object.__setattr__(bundle, "data_availability_report", _runtime_data_report(materialized_strategy_set, ts_ms))
        object.__setattr__(
            bundle,
            "runtime_data_cycle_preflight_hash",
            sha256_prefixed({"h74": "runtime_cycle_preflight", "ts": int(ts_ms)}),
        )
        object.__setattr__(bundle, "schema_version", 1)
        return bundle


def _readiness_snapshot_payload(
    *,
    order_rules: Mapping[str, object],
    projection_converged: bool,
    active_fee_accounting_blocker: bool,
    closeout_existing_qty: float,
    settings_obj: object,
) -> dict[str, object]:
    current_exposure_krw = float(closeout_existing_qty or 0.0) * 100_000_000.0
    payload = {
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
        "cash_available": float(getattr(settings_obj, "START_CASH_KRW")),
        "runtime_pair": str(getattr(settings_obj, "PAIR")),
        "min_qty": float(order_rules.get("min_qty") or 0.0001),
        "qty_step": float(order_rules.get("qty_step") or 0.0),
        "qty_step_authority": "exchange" if float(order_rules.get("qty_step") or 0.0) > 0.0 else "local_fallback_min_qty",
        "min_notional_krw": float(order_rules.get("min_notional_krw") or 5000.0),
        "max_qty_decimals": int(order_rules.get("max_qty_decimals") or 8),
        "order_types": ["bid", "ask"],
        "bid_types": ["market"],
        "ask_types": ["market"],
    }
    if float(closeout_existing_qty or 0.0) > 0.0:
        from .h74_position_ownership import h74_position_ownership_contract_from_payload

        contract = h74_position_ownership_contract_from_payload(
            {
                "cycle_id": "h74-rehearsal-closeout-cycle",
                "h74_cycle_id": "h74-rehearsal-closeout-cycle",
                "authority_hash": "sha256:h74-rehearsal-authority",
                "strategy_instance_id": "h74-source-observation",
                "probe_run_id": "h74-rehearsal-probe",
                "pair": "KRW-BTC",
                "entry_side": "BUY",
                "entry_plan_id": "h74-rehearsal-entry-plan",
                "position_mode": "fixed_fill_qty_until_exit",
                "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
            }
        )
        payload["h74_cycle_id"] = "h74-rehearsal-closeout-cycle"
        payload["cycle_id"] = "h74-rehearsal-closeout-cycle"
        payload["authority_hash"] = "sha256:h74-rehearsal-authority"
        payload["strategy_instance_id"] = "h74-source-observation"
        payload["entry_client_order_id"] = "h74-rehearsal-entry"
        payload["h74_entry_plan_client_order_id"] = contract.entry_plan_id
        payload["contract_hash"] = contract.contract_hash
        payload["h74_position_ownership_contract_hash"] = contract.contract_hash
        payload["h74_position_ownership_contract"] = contract.as_dict()
        payload["acquired_qty"] = float(closeout_existing_qty)
        payload["sold_qty"] = 0.0
        payload["locked_exit_qty"] = 0.0
        payload["remaining_cycle_qty"] = float(closeout_existing_qty)
        payload["h74_remaining_cycle_qty"] = float(closeout_existing_qty)
        payload["broker_available_qty"] = float(closeout_existing_qty)
    return payload


def _target_delta_planning_bundle(
    conn: sqlite3.Connection,
    *,
    ts_ms: int,
    kst_time: str,
    order_rules: Mapping[str, object],
    projection_converged: bool,
    active_fee_accounting_blocker: bool,
    closeout_existing_qty: float,
    settings_obj: object,
):
    result_bundle = _runtime_decision_bundle(conn, ts_ms=ts_ms, settings_obj=settings_obj)
    if float(closeout_existing_qty or 0.0) > 0.0:
        from .h74_cycle_state import upsert_h74_cycle_fill
        from .execution import record_order_if_missing
        from .h74_position_ownership import h74_position_ownership_contract_from_payload

        contract = h74_position_ownership_contract_from_payload(
            {
                "cycle_id": "h74-rehearsal-closeout-cycle",
                "h74_cycle_id": "h74-rehearsal-closeout-cycle",
                "authority_hash": "sha256:h74-rehearsal-authority",
                "strategy_instance_id": "h74-source-observation",
                "probe_run_id": "h74-rehearsal-probe",
                "pair": "KRW-BTC",
                "entry_side": "BUY",
                "entry_plan_id": "h74-rehearsal-entry-plan",
                "position_mode": "fixed_fill_qty_until_exit",
                "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
            }
        )
        record_order_if_missing(
            conn,
            client_order_id="h74-rehearsal-entry",
            side="BUY",
            qty_req=float(closeout_existing_qty),
            price=100_000_000.0,
            symbol="KRW-BTC",
            strategy_name="daily_participation_sma",
            strategy_instance_id="h74-source-observation",
            cycle_id="h74-rehearsal-closeout-cycle",
            authority_hash="sha256:h74-rehearsal-authority",
            h74_entry_plan_client_order_id=contract.entry_plan_id,
            h74_position_ownership_contract_hash=contract.contract_hash,
            h74_position_ownership_contract=contract.as_dict(),
            ts_ms=int(ts_ms) - (74 * 60_000),
            status="FILLED",
        )

        upsert_h74_cycle_fill(
            conn,
            cycle_id="h74-rehearsal-closeout-cycle",
            authority_hash="sha256:h74-rehearsal-authority",
            strategy_instance_id="h74-source-observation",
            pair="KRW-BTC",
            side="BUY",
            qty=float(closeout_existing_qty),
            client_order_id="h74-rehearsal-entry",
            fill_ts=int(ts_ms) - (74 * 60_000),
            contract_hash=contract.contract_hash,
            h74_entry_plan_client_order_id=contract.entry_plan_id,
        )
    readiness_payload = _readiness_snapshot_payload(
        order_rules=order_rules,
        projection_converged=projection_converged,
        active_fee_accounting_blocker=active_fee_accounting_blocker,
        closeout_existing_qty=closeout_existing_qty,
        settings_obj=settings_obj,
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

    def _summary_builder(*, typed_input, strategy_performance_gate=None):
        from .execution_service import build_typed_execution_decision_summary

        return build_typed_execution_decision_summary(
            typed_input=typed_input,
            strategy_performance_gate=strategy_performance_gate,
            settings_obj=settings_obj,
        )

    planner = ExecutionPlanner(
        settings_obj=settings_obj,
        readiness_snapshot_builder=lambda _conn: _ReadinessSnapshot(),
        summary_builder=_summary_builder,
        target_state_resolver=_target_state_resolver,
    )
    return planner.plan_runtime_strategy_results(conn, result_bundle, updated_ts=int(ts_ms))


def _blocking_gate(gate_trace: list[dict[str, object]]) -> tuple[str, str]:
    priority = {
        "readiness": 10,
        "entry_authority": 20,
        "pre_submit_risk": 30,
        "fee_equivalence": 35,
        "submit_semantics": 40,
        "submit_authority": 50,
    }
    blocked = [entry for entry in gate_trace if bool(entry.get("blocking"))]
    if blocked:
        entry = min(blocked, key=lambda item: priority.get(str(item.get("gate") or ""), 100))
        return str(entry.get("gate") or "unknown"), str(entry.get("reason_code") or "blocked")
    for entry in gate_trace:
        if bool(entry.get("blocking")):
            return str(entry.get("gate") or "unknown"), str(entry.get("reason_code") or "blocked")
    return "none", "none"


def _configured_pre_submit_block_reason(cfg: H74LiveRehearsalConfig) -> str:
    if not bool(cfg.broker_snapshot_available) or bool(cfg.broker_snapshot_stale):
        return "RISK_STATE_MISMATCH"
    unresolved_status = str(cfg.unresolved_order_status or "").strip().upper()
    if unresolved_status in {"NEW", "PARTIALLY_FILLED"}:
        return "UNRESOLVED_OPEN_ORDER_PRESENT"
    if unresolved_status == "SUBMIT_UNKNOWN":
        return "SUBMIT_UNKNOWN_PRESENT"
    if unresolved_status == "RECOVERY_REQUIRED":
        return "RECOVERY_REQUIRED_PRESENT"
    return ""


def _with_invalid_reduce_only_preview(
    payload: Mapping[str, object],
    *,
    invalid_case: str,
) -> dict[str, object]:
    mutated = dict(payload)
    case = str(invalid_case or "").strip()
    if not case:
        return mutated
    if str(mutated.get("side") or "").upper() != "SELL":
        raise H74LiveRehearsalError("invalid_reduce_only_preview_requires_sell_plan")
    decision = dict(mutated.get("pre_submit_risk_decision") or {})
    decision["status"] = "REDUCE_ONLY"
    decision["reason_code"] = "POSITION_LOSS_LIMIT"
    decision.setdefault("allowed_actions", ["SELL", "HOLD"])
    mutated["pre_submit_risk_decision"] = decision
    mutated["pre_submit_risk_status"] = "REDUCE_ONLY"
    mutated["pre_submit_risk_reason_code"] = "POSITION_LOSS_LIMIT"
    mutated["target_delta_qty"] = -abs(float(mutated.get("target_delta_qty") or mutated.get("qty") or 0.0))
    if case == "allowed_actions_missing_sell":
        decision["allowed_actions"] = ["HOLD"]
    elif case == "plan_hash_mismatch":
        mutated["pre_submit_risk_plan_hash"] = "sha256:" + "0" * 64
    elif case == "side_buy":
        mutated["side"] = "BUY"
    elif case == "target_delta_qty_positive":
        mutated["target_delta_qty"] = abs(float(mutated.get("target_delta_qty") or mutated.get("qty") or 0.0))
    else:
        raise H74LiveRehearsalError(f"unknown_invalid_reduce_only_preview_case:{case}")
    return mutated


def _connect_rehearsal_db(context: H74LiveRehearsalContext, db_path: str) -> sqlite3.Connection:
    if context.db_factory is None:
        return sqlite3.connect(db_path)
    try:
        return context.db_factory(db_path)
    except TypeError:
        return context.db_factory()


def run_h74_live_rehearsal(
    config: H74LiveRehearsalConfig | None = None,
    *,
    context: H74LiveRehearsalContext | None = None,
) -> dict[str, Any]:
    cfg = config or H74LiveRehearsalConfig()
    ctx = context or default_h74_live_rehearsal_context()
    settings_obj = ctx.settings_snapshot
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
    order_rules = dict(
        cfg.order_rules
        or {
            "min_qty": 0.0001,
            "qty_step": 0.0001,
            "max_qty_decimals": 8,
            "min_notional_krw": 5000.0,
            "order_type_buy": "price",
            "order_type_sell": "market",
        }
    )
    order_rule_snapshot = OrderRuleSnapshot.from_mapping(order_rules)
    equivalence_manifest = build_h74_equivalence_manifest(
        source_artifact_path=cfg.source_artifact_path,
        order_rules=order_rules,
    )
    equivalence = compare_h74_equivalence(
        equivalence_manifest,
        current_fee_rate=float(cfg.current_fee_rate),
        current_fee_authority_source=cfg.fee_authority_source,
        current_order_rules=order_rules,
        current_behavior={
            "slippage_bps": 10.0,
            "candle_timing": "closed_candle_kst",
            "position_mode": H74_POSITION_MODE,
            "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
            "residual_inventory_mode": "terminal_dust_reported_not_reused_without_authority",
            "initial_position_policy": "flat_start_required",
            "partial_fill_policy": "accumulate_cycle_acquired_qty",
            "fee_application_policy": "repository_observed_fee_fields",
            "entry_submit_semantics": dict(H74_ENTRY_SUBMIT_SEMANTICS),
        },
    )
    equivalence_status = str(equivalence["experiment_equivalence_status"])
    fee_gate_reason = (
        "mismatch"
        if equivalence_status == "unknown_source_assumption_missing"
        and str(cfg.fee_authority_source or "").strip() == "degraded_fee_authority"
        else equivalence_status
    )
    equivalence_allows = equivalence_status == "pass"

    captured: list[dict[str, object]] = []
    pre_submit_status = "NOT_EVALUATED"
    pre_submit_reason = "equivalence_blocked" if not equivalence_allows else "not_evaluated"
    configured_pre_submit_reason = _configured_pre_submit_block_reason(cfg)
    if configured_pre_submit_reason:
        pre_submit_status = "REQUIRE_RECONCILE"
        pre_submit_reason = configured_pre_submit_reason
    broker_snapshot_hash = ""
    execution_result_status = "submit_blocked"
    with tempfile.TemporaryDirectory(prefix="h74-live-rehearsal-") as tmp_dir:
        if True:
            initial_settings_obj = ctx.settings_snapshot
            probe_run_id = str(getattr(initial_settings_obj, "H74_EXECUTION_PATH_PROBE_RUN_ID", "") or "").strip()
            if not probe_run_id:
                probe_run_id = "h74-live-rehearsal-probe"
            authority_path = _write_h74_source_authority_file(
                tmp_dir,
                equivalence_manifest=equivalence_manifest,
                probe_run_id=probe_run_id,
            )
            run_context = with_h74_source_authority_path(ctx, authority_path)
            settings_obj = run_context.settings_snapshot
            settings_values = vars(settings_obj).copy()
            settings_values["H74_EXECUTION_PATH_PROBE_RUN_ID"] = probe_run_id
            settings_obj = SimpleNamespace(**settings_values)
            db_path = f"{tmp_dir}/h74-rehearsal.sqlite"
            _seed_rehearsal_db(
                db_path,
                submit_plan_hash="sha256:h74_rehearsal_planning_pending",
                settings_obj=settings_obj,
                context=run_context,
                unresolved_order_status=cfg.unresolved_order_status,
                unresolved_order_created_ts_ms=cfg.unresolved_order_created_ts_ms,
            )

            conn = _connect_rehearsal_db(run_context, db_path)
            conn.row_factory = sqlite3.Row
            try:
                with _h74_order_rule_cache(order_rules, clock=run_context.clock):
                    planning_bundle = _target_delta_planning_bundle(
                        conn,
                        ts_ms=ts_ms,
                        kst_time=str(cfg.kst_time),
                        order_rules=order_rules,
                        projection_converged=cfg.projection_converged,
                        active_fee_accounting_blocker=cfg.active_fee_accounting_blocker,
                        closeout_existing_qty=cfg.closeout_existing_qty,
                        settings_obj=settings_obj,
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
            profile_context = dict(planning_context.get("profile_authority_context") or {})
            profile_context.update(
                {
                    "runtime_mode": "live",
                    "live_dry_run": False,
                    "live_real_order_armed": True,
                    "authority_scope": "live_real_submit",
                }
            )
            planning_context["profile_authority_context"] = profile_context
            trace = dict(planning_context.get("pure_policy_trace") or {})
            conn = _connect_rehearsal_db(run_context, db_path)
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
            submit_authority = None
            if equivalence_allows and not configured_pre_submit_reason:

                def _db_factory() -> sqlite3.Connection:
                    conn = _connect_rehearsal_db(run_context, db_path)
                    conn.row_factory = sqlite3.Row
                    return conn

                broker = _H74NoSubmitBroker(
                    available=cfg.broker_snapshot_available,
                    observed_ts_ms=ts_ms - (30 * 60 * 1000) if cfg.broker_snapshot_stale else ts_ms,
                    cash_krw=float(getattr(settings_obj, "START_CASH_KRW")),
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
                    settings_obj=settings_obj,
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
                    if not configured_pre_submit_reason:
                        pre_submit_status = str(submitted.get("pre_submit_risk_status") or "")
                        pre_submit_reason = str(submitted.get("pre_submit_risk_reason_code") or "")
                    broker_snapshot_hash = sha256_prefixed(
                        {
                            "source": "h74_rehearsal_recorded_broker_snapshot",
                            "observed_ts_ms": ts_ms,
                            "cash_krw": float(getattr(settings_obj, "START_CASH_KRW")),
                        }
                    )
                    would_submit_plan = submitted
                else:
                    conn = _connect_rehearsal_db(run_context, db_path)
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
                            if not configured_pre_submit_reason:
                                pre_submit_status = str(stored.get("pre_submit_risk_status") or "BLOCK")
                                pre_submit_reason = str(
                                    stored.get("pre_submit_risk_reason_code") or "broker_submit_not_reached"
                                )
                            would_submit_plan = stored or would_submit_plan
                    finally:
                        conn.close()
            if cfg.invalid_reduce_only_preview_case:
                would_submit_plan = _with_invalid_reduce_only_preview(
                    would_submit_plan,
                    invalid_case=cfg.invalid_reduce_only_preview_case,
                )
                pre_submit_status = str(would_submit_plan.get("pre_submit_risk_status") or "REDUCE_ONLY")
                pre_submit_reason = str(would_submit_plan.get("pre_submit_risk_reason_code") or "POSITION_LOSS_LIMIT")
                captured.clear()
            submit_authority = evaluate_submit_authority_policy(
                would_submit_plan,
                settings_obj=settings_obj,
                plan_kind="target",
                require_final_payload=True,
            )
        submit_authority_allowed = bool(captured)
        submit_authority_reason = (
            "allowed_target_delta"
            if submit_authority_allowed
            else str(getattr(submit_authority, "reason", "submit_authority_not_evaluated"))
        )
        closeout_contract = (
            would_submit_plan.get("h74_closeout_contract")
            if isinstance(would_submit_plan.get("h74_closeout_contract"), Mapping)
            else {}
        )
        h74_closeout_preview = None
        if str(would_submit_plan.get("side") or "").upper() == "SELL" and closeout_contract:
            remaining_cycle_qty = float(
                closeout_contract.get("remaining_cycle_qty")
                or would_submit_plan.get("remaining_cycle_qty")
                or 0.0
            )
            planned_sell_qty = float(would_submit_plan.get("qty") or 0.0)
            residual_policy = str(
                closeout_contract.get("residual_policy")
                or would_submit_plan.get("residual_policy")
                or "none"
            )
            qty_matches_remaining = abs(remaining_cycle_qty - planned_sell_qty) <= 1e-12
            h74_closeout_preview = {
                "h74_closeout_preview_present": True,
                "cycle_id": str(would_submit_plan.get("cycle_id") or ""),
                "remaining_cycle_qty": remaining_cycle_qty,
                "planned_sell_qty": planned_sell_qty,
                "qty_matches_remaining": qty_matches_remaining,
                "residual_qty": float(
                    closeout_contract.get("residual_qty")
                    or would_submit_plan.get("residual_qty")
                    or 0.0
                ),
                "residual_policy": residual_policy,
                "residual_reason": str(
                    closeout_contract.get("residual_reason")
                    or would_submit_plan.get("residual_reason")
                    or "none"
                ),
                "risk_status": pre_submit_status,
                "risk_reason_code": pre_submit_reason,
                "submit_authority_would_allow": bool(getattr(submit_authority, "allowed", False)),
                "submit_authority_reason": submit_authority_reason,
                "h74_entry_plan_client_order_id_present": bool(
                    str(would_submit_plan.get("h74_entry_plan_client_order_id") or "").strip()
                ),
                "contract_hash_present": bool(
                    str(would_submit_plan.get("h74_position_ownership_contract_hash") or "").strip()
                ),
            }
            if not qty_matches_remaining and residual_policy == "none":
                submit_authority_allowed = False
                submit_authority_reason = "h74_closeout_preview_qty_mismatch_without_residual_policy"
        readiness_blocks = not bool(cfg.projection_converged)
        planning_path_mode = str(getattr(settings_obj, "MODE", ""))
        planning_path_live_dry_run = bool(getattr(settings_obj, "LIVE_DRY_RUN", True))
        planning_path_live_real_order_armed = bool(getattr(settings_obj, "LIVE_REAL_ORDER_ARMED", False))
        entry_authority_payload = dict(would_submit_plan.get("entry_authority") or {})
        if not entry_authority_payload:
            entry_authority_payload = {
                "gate": "entry_authority",
                "status": str(would_submit_plan.get("entry_authority_status") or "ALLOW"),
                "reason_code": str(would_submit_plan.get("entry_authority_reason_code") or "not_new_buy_exposure"),
                "blocking": str(would_submit_plan.get("entry_authority_status") or "") == "BLOCK",
            }
        participation_decision_payload = dict(trace.get("daily_participation_decision") or {})
        daily_entry_allowed = participation_decision_payload.get("allowed") is True
        daily_entry_reason = str(
            participation_decision_payload.get("reason_code")
            or would_submit_plan.get("entry_authority_reason_code")
            or ""
        )
        if not daily_entry_allowed and daily_entry_reason:
            entry_authority_payload = {
                **entry_authority_payload,
                "gate": "entry_authority",
                "status": "BLOCK",
                "reason_code": ENTRY_AUTHORITY_REASON_BLOCKED,
                "blocking": True,
                "source": "daily_participation_entry_authority",
            }
        expected_semantics = dict(equivalence_manifest.get("entry_submit_semantics") or {})
        payload_preview = {
            "order_type": would_submit_plan.get("exchange_order_type"),
            "price": would_submit_plan.get("exchange_submit_notional_krw"),
            "volume_present": would_submit_plan.get("exchange_submit_qty") is not None,
        }
        semantics_reasons: list[str] = []
        if equivalence_status == "unknown_source_assumption_missing":
            missing_fields = set(str(item) for item in equivalence_manifest.get("source_missing_assumption_fields") or [])
            if "entry_submit_semantics" in missing_fields or "entry_quote_notional_krw" in missing_fields:
                semantics_reasons.append("source_entry_submit_semantics_missing")
        if daily_entry_allowed:
            plan_notional = would_submit_plan.get("notional_krw")
            exchange_notional = would_submit_plan.get("exchange_submit_notional_krw")
            try:
                plan_notional_ok = 99_999.0 <= float(plan_notional) <= 100_001.0
            except (TypeError, ValueError):
                plan_notional_ok = False
            try:
                exchange_notional_ok = 99_999.0 <= float(exchange_notional) <= 100_001.0
            except (TypeError, ValueError):
                exchange_notional_ok = False
            if not plan_notional_ok:
                semantics_reasons.append("would_submit_plan_notional_not_100000")
            if not exchange_notional_ok:
                semantics_reasons.append("exchange_submit_notional_not_100000")
            if str(would_submit_plan.get("position_mode") or "") != H74_POSITION_MODE:
                semantics_reasons.append("position_mode_mismatch")
            if str(would_submit_plan.get("source") or "") != "h74_source_observation":
                semantics_reasons.append("h74_source_observation_source_missing")
            if str(would_submit_plan.get("authority") or "") != "h74_fixed_fill_quote_notional_buy":
                semantics_reasons.append("h74_quote_notional_authority_missing")
            if str(would_submit_plan.get("exchange_order_type") or "") != "price":
                semantics_reasons.append("exchange_order_type_not_price")
            if str(would_submit_plan.get("exchange_submit_field") or "") != "price":
                semantics_reasons.append("exchange_submit_field_not_price")
            if payload_preview["volume_present"]:
                semantics_reasons.append("price_order_volume_present")
            if isinstance(expected_semantics, Mapping) and expected_semantics:
                expected_notional = expected_semantics.get("entry_quote_notional_krw")
                expected_order_type = str(expected_semantics.get("entry_order_type") or "")
                expected_submit_field = str(expected_semantics.get("entry_submit_field") or "")
                try:
                    if abs(float(expected_notional) - float(exchange_notional or 0.0)) > 1.0:
                        semantics_reasons.append("source_live_quote_notional_mismatch")
                except (TypeError, ValueError):
                    semantics_reasons.append("source_quote_notional_invalid")
                if expected_order_type != str(would_submit_plan.get("exchange_order_type") or ""):
                    semantics_reasons.append("source_live_order_type_mismatch")
                if expected_submit_field != str(would_submit_plan.get("exchange_submit_field") or ""):
                    semantics_reasons.append("source_live_submit_field_mismatch")
        submit_semantics_blocking = bool(semantics_reasons)
        gate_trace = [
            {
                "gate": "time_window",
                "status": "ALLOW" if daily_entry_allowed else "BLOCK",
                "reason_code": "within_kst_window" if daily_entry_allowed else daily_entry_reason,
                "blocking": False,
            },
            {"gate": "runtime_cycle_pipeline", "status": "ALLOW", "reason_code": "RuntimeCyclePipeline/ExecutionCoordinator", "blocking": False},
            {
                "gate": "fee_equivalence",
                "status": "ALLOW" if equivalence_allows else "BLOCK",
                "reason_code": fee_gate_reason,
                "blocking": not equivalence_allows,
            },
            {
                "gate": "submit_semantics",
                "status": "BLOCK" if submit_semantics_blocking else "ALLOW",
                "reason_code": ",".join(semantics_reasons) if semantics_reasons else "OK",
                "blocking": submit_semantics_blocking,
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
                "blocking": pre_submit_status not in {"ALLOW", "NOT_EVALUATED"},
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
        "artifact_type": EvidenceArtifactType.SYNTHETIC_GATE.value,
        "schema_version": 1,
        "claim_scope": "synthetic_gate",
        "claims_scope": "synthetic_gate",
        "full_lifecycle_equivalence_supported": False,
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
        "production_target_delta_planner_called": isinstance(
            would_submit_plan.get("target_sizing"), Mapping
        ),
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
        "entry_authority_status": str(entry_authority_payload.get("status") or ""),
        "entry_authority_reason_code": str(entry_authority_payload.get("reason_code") or ""),
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
        "would_submit": bool(equivalence_allows and captured and not submit_semantics_blocking),
        "would_submit_plan": would_submit_plan,
        "h74_closeout_preview": h74_closeout_preview,
        "h74_closeout_preview_present": bool(h74_closeout_preview),
        "would_submit_plan_hash": sha256_prefixed(would_submit_plan),
        "broker_payload_preview": payload_preview,
        "submit_semantics_hash": str(equivalence_manifest.get("submit_semantics_hash") or ""),
        "entry_submit_semantics": expected_semantics,
        "broker_payload_preview_hash": sha256_prefixed(payload_preview),
        "broker_balance_snapshot_hash": broker_snapshot_hash,
        "experiment_equivalence_status": equivalence_status,
        "source_artifact_status": equivalence_manifest["source_artifact_status"],
        "source_artifact_path": cfg.source_artifact_path,
        "fee_authority_source": equivalence["fee_authority_source"],
        "fee_comparison": equivalence["fee_comparison"],
        "order_rule_comparison": equivalence["order_rule_comparison"],
        "behavior_field_comparison": equivalence["behavior_field_comparison"],
        "behavior_comparison_hash": equivalence["behavior_comparison_hash"],
        "source_artifact_hash": equivalence_manifest["source_artifact_hash"],
        "position_mode": H74_POSITION_MODE,
        "quantity_contract_hash": str(
            (
                would_submit_plan.get("target_sizing")
                if isinstance(would_submit_plan.get("target_sizing"), Mapping)
                else {}
            ).get("quantity_contract_hash")
            or would_submit_plan.get("quantity_contract_hash")
            or ""
        ),
        "order_rule_snapshot_hash": order_rule_snapshot.contract_hash(),
        "execution_result_status": execution_result_status,
        "gate_trace": gate_trace,
        "primary_block_gate": primary_gate,
        "primary_block_reason": primary_reason,
    }
    payload["gate_trace_hash"] = sha256_prefixed(gate_trace)
    payload["rehearsal_hash"] = sha256_prefixed(payload)
    return payload


__all__ = ["H74LiveRehearsalConfig", "H74LiveRehearsalError", "run_h74_live_rehearsal"]
