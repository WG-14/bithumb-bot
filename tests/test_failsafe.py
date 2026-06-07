from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from bithumb_bot import engine as engine_module
from bithumb_bot import runtime_state
from bithumb_bot.compat import engine_legacy as engine_legacy_module
from bithumb_bot.broker.base import BrokerBalance, BrokerOrder, BrokerRejectError
from bithumb_bot.broker.balance_source import _default_flat_start_safety_check
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.decision_equivalence import sha256_prefixed
from bithumb_bot.risk_contract import RiskPolicy, RiskSnapshot, build_risk_decision
from bithumb_bot.runtime_compat import get_health_status
from bithumb_bot.runtime.app_container import create_default_runtime_app
from bithumb_bot.runtime.runner import Runner
from bithumb_bot.runtime_scope import RuntimeScopeKey
from bithumb_bot.research.strategy_registry import runtime_strategy_parameters_from_settings
from bithumb_bot.execution_service import (
    ExecutionDecisionSummary,
    ExecutionSubmitPlan,
    LiveSignalExecutionService,
    SignalExecutionRequest,
    build_signal_execution_service,
    build_execution_decision_summary,
    build_residual_sell_candidate,
    build_residual_sell_presubmit_proof,
)
from bithumb_bot.marketdata import _get_with_retry
from bithumb_bot.public_api_orderbook import BestQuote
from bithumb_bot.strategy_policy_contract import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2
from bithumb_bot.target_position import TargetPositionState

_RUN_LOOP_RUNNER: Runner | None = None


def run_loop() -> None:
    assert _RUN_LOOP_RUNNER is not None
    _apply_legacy_test_overrides(_RUN_LOOP_RUNNER)
    _RUN_LOOP_RUNNER.run_one_cycle()


def _apply_legacy_test_overrides(runner: Runner) -> None:
    import bithumb_bot.recovery as recovery_module
    import bithumb_bot.runtime.safety_controller as safety_controller_module

    app = runner.container
    safety_controller_module.evaluate_daily_loss_breach = engine_legacy_module.evaluate_daily_loss_breach
    safety_controller_module.evaluate_position_loss_breach = engine_legacy_module.evaluate_position_loss_breach

    class _NotifyProxy:
        def send_event(self, event_name: str, **fields: object) -> None:
            import bithumb_bot.notifier as notifier_module

            payload = " ".join([f"event={event_name}", *(f"{k}={v}" for k, v in fields.items())])
            notifier_module.notify(payload)
            if engine_legacy_module.notify is not notifier_module.notify:
                engine_legacy_module.notify(payload)

        def send_message(self, message: str) -> None:
            import bithumb_bot.notifier as notifier_module

            notifier_module.notify(message)
            if engine_legacy_module.notify is not notifier_module.notify:
                engine_legacy_module.notify(message)

    notification_service = _NotifyProxy()
    safety_controller = replace(
        app.safety_controller,
        flatten_position=lambda **kwargs: engine_legacy_module.flatten_btc_position(**kwargs),
        exposure_snapshot=engine_legacy_module._get_exposure_snapshot,
        legacy_cancel_open_orders=getattr(engine_legacy_module, "_attempt_open_order_cancellation", None),
    )
    runner.container = replace(
        app,
        live_executor=engine_legacy_module.live_execute_signal,
        paper_executor=engine_legacy_module.paper_execute,
        harmless_dust_recorder=engine_legacy_module.record_harmless_dust_exit_suppression,
        validate_market_runtime=engine_legacy_module.validate_market_runtime,
        interval_parser=engine_legacy_module.parse_interval_sec,
        market_sync=engine_legacy_module.cmd_sync,
        notification_service=notification_service,
        notification_adapter=app.notification_adapter.__class__(notification_service),
        safety_controller=safety_controller,
        decision_coordinator=replace(
            app.decision_coordinator,
            record_strategy_decision_fn=engine_legacy_module.record_strategy_decision,
        ),
        startup_controller=replace(
            app.startup_controller,
            startup_gate_evaluator=engine_legacy_module.evaluate_startup_safety_gate,
            broker_factory=engine_legacy_module.BithumbBroker,
            initial_reconcile=recovery_module.reconcile_with_broker,
            halt_on_startup_failure=lambda *, reason_code, reason, unresolved: safety_controller.apply(
                safety_controller.evaluate_halt(
                    runner_module_halt_reason(reason_code, reason),
                    unresolved=bool(unresolved),
                )
            ),
        ),
        broker_factory=engine_legacy_module.BithumbBroker,
        reconcile_with_broker=recovery_module.reconcile_with_broker,
    )
    runner.execution_service = runner.container.execution_service_factory(
        mode=runner.container.settings_obj.MODE,
        broker=runner.broker,
        paper_executor=runner.container.paper_executor,
        live_executor=runner.container.live_executor,
        harmless_dust_recorder=runner.container.harmless_dust_recorder,
    )


def runner_module_halt_reason(reason_code: str, reason: str):
    from bithumb_bot.runtime.safety_controller import HaltReason

    return HaltReason(reason_code, reason)


class _NoSleepScheduler:
    def sleep(self, _seconds: float) -> None:
        return None


class _Notifications:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []
        self.messages: list[str] = []

    def send_event(self, event_name: str, **fields: object) -> None:
        self.events.append((event_name, fields))

    def send_message(self, message: str) -> None:
        self.messages.append(message)


def _unit_runtime_strategy_set_manifest(**_kwargs):
    payload = {
        "schema_version": 1,
        "authority_label": "RuntimeStrategySetManifest",
        "authority_scope": "operator_reproducibility_manifest",
        "source": "unit",
        "runtime_pair": "KRW-BTC",
        "runtime_interval": "1m",
        "single_pair_runtime_enforced": True,
        "market_scope": {
            "schema_version": 1,
            "mode": "single_pair",
            "pair": "KRW-BTC",
            "interval": "1m",
        },
        "multi_strategy_enabled": False,
        "active_strategy_count": 1,
        "active_strategy_pairs": ["KRW-BTC"],
        "active_strategy_intervals": ["1m"],
        "active_instances": [
            {
                "strategy_instance_id": "unit",
                "strategy_name": "sma_with_filter",
                "parameter_source": "runtime_strategy_spec",
                "legacy_compatibility_used": False,
                "runtime_decision_request_hash": "sha256:unit-request",
                "runtime_decision_request_hash_scope": "run_start_blueprint_through_ts_null",
            }
        ],
        "strategy_instance_profile_bindings": [],
        "execution_config_hash": "sha256:unit-execution",
        "risk_config_hash": "sha256:unit-risk",
    }
    payload["runtime_strategy_set_manifest_hash"] = sha256_prefixed(payload)
    return payload


def _unit_live_risk_policy() -> RiskPolicy:
    return RiskPolicy(
        source="unit_approved_profile",
    )


def _unit_pre_submit_risk_decision(plan) -> object:
    policy = RiskPolicy(source="unit_pre_submit")
    snapshot = RiskSnapshot(
        evaluation_ts_ms=123,
        mark_price=1.0,
        broker_local_mismatch=False,
        unresolved_order_blocked=False,
        unresolved_order_reason_code="OK",
        unresolved_order_reason="ok",
        state_source="unit_pre_submit",
        evidence={"execution_submit_plan_hash": str(plan.evidence.get("execution_submit_plan_hash") or "")},
    )
    return build_risk_decision(
        evaluation_point="pre_submit",
        status="ALLOW",
        reason_code="OK",
        reason="unit pre-submit risk allow",
        allowed_actions=("SUBMIT",),
        recommended_action="SUBMIT",
        snapshot=snapshot,
        policy=policy,
        evidence={
            "execution_submit_plan_hash": str(plan.evidence.get("execution_submit_plan_hash") or ""),
            "execution_submit_plan_source": str(plan.evidence.get("execution_submit_plan_source") or ""),
            "execution_submit_plan_authority": str(plan.evidence.get("execution_submit_plan_authority") or ""),
        },
    )


def _submit_plan_payload(plan: object | None) -> dict[str, object]:
    assert plan is not None
    as_dict = getattr(plan, "as_dict", None)
    assert callable(as_dict)
    payload = as_dict()
    assert isinstance(payload, dict)
    return payload


def _persist_execution_plan_for_submit_plan(plan: ExecutionSubmitPlan) -> None:
    submit_hash = plan.content_hash()
    payload = _submit_plan_payload(plan)
    conn = ensure_db()
    try:
        bundle_hash = f"unit-bundle:{submit_hash}"
        allocation_hash = f"unit-allocation:{submit_hash}"
        conn.execute(
            """
            INSERT OR IGNORE INTO runtime_strategy_decision_bundle(
                candle_ts, pair, interval, strategy_set_manifest_hash,
                bundle_hash, result_count, created_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (123, str(settings.PAIR), str(settings.INTERVAL), "sha256:unit", bundle_hash, 0, 123),
        )
        bundle_row = conn.execute(
            "SELECT id FROM runtime_strategy_decision_bundle WHERE bundle_hash=?",
            (bundle_hash,),
        ).fetchone()
        assert bundle_row is not None
        conn.execute(
            """
            INSERT OR IGNORE INTO portfolio_allocation_decision(
                bundle_id, allocation_decision_hash, allocation_input_hash,
                allocator_config_hash, strategy_contribution_hash, selected_signal,
                selected_priority, authoritative, primary_block_reason, reason,
                conflict_resolution_json, allocation_decision_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(bundle_row["id"]),
                allocation_hash,
                "sha256:unit-input",
                "sha256:unit-config",
                "sha256:unit-contribution",
                "SELL",
                0,
                1,
                "none",
                "unit_residual_submit_plan_fixture",
                "{}",
                "{}",
            ),
        )
        allocation_row = conn.execute(
            "SELECT id FROM portfolio_allocation_decision WHERE allocation_decision_hash=?",
            (allocation_hash,),
        ).fetchone()
        assert allocation_row is not None
        conn.execute(
            """
            INSERT OR IGNORE INTO execution_plan(
                allocation_id, portfolio_target_hash, execution_plan_bundle_hash,
                execution_submit_plan_hash, submit_plan_side, submit_plan_qty,
                submit_plan_notional_krw, submit_plan_idempotency_key,
                submit_plan_source, submit_plan_authority, submit_expected,
                final_action, block_reason, status, execution_plan_bundle_json,
                execution_submit_plan_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(allocation_row["id"]),
                None,
                bundle_hash,
                submit_hash,
                str(payload.get("side") or ""),
                None if payload.get("qty") is None else float(payload.get("qty") or 0.0),
                None
                if payload.get("notional_krw") is None
                else float(payload.get("notional_krw") or 0.0),
                payload.get("idempotency_key"),
                str(payload.get("source") or ""),
                str(payload.get("authority") or ""),
                1 if bool(payload.get("submit_expected")) else 0,
                str(payload.get("final_action") or ""),
                str(payload.get("block_reason") or ""),
                "unit_test_pending_final_payload",
                "{}",
                "{}",
            ),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path):
    from bithumb_bot.config import settings as current_settings
    import bithumb_bot.db_core as db_core_module
    import bithumb_bot.compat.engine_legacy as engine_settings_module
    import bithumb_bot.execution_service as execution_service_module
    import bithumb_bot.operator_commands as operator_commands_module
    import bithumb_bot.run_loop_execution_planner as run_loop_execution_planner_module
    import bithumb_bot.runtime_state as runtime_state_module
    import bithumb_bot.broker.balance_source as balance_source_module

    globals()["settings"] = current_settings
    balance_source_module.settings = current_settings
    db_core_module.settings = current_settings
    engine_settings_module.settings = current_settings
    execution_service_module.settings = current_settings
    operator_commands_module.settings = current_settings
    run_loop_execution_planner_module.settings = current_settings
    runtime_state_module.settings = current_settings
    old_settings = {
        "DB_PATH": settings.DB_PATH,
        "MODE": settings.MODE,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "MAX_ORDER_KRW": settings.MAX_ORDER_KRW,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "MAX_DAILY_ORDER_COUNT": settings.MAX_DAILY_ORDER_COUNT,
        "MAX_OPEN_ORDER_AGE_SEC": settings.MAX_OPEN_ORDER_AGE_SEC,
        "KILL_SWITCH": settings.KILL_SWITCH,
        "KILL_SWITCH_LIQUIDATE": settings.KILL_SWITCH_LIQUIDATE,
        "BITHUMB_API_KEY": settings.BITHUMB_API_KEY,
        "BITHUMB_API_SECRET": settings.BITHUMB_API_SECRET,
        "EXECUTION_ENGINE": settings.EXECUTION_ENGINE,
        "TARGET_EXPOSURE_KRW": settings.TARGET_EXPOSURE_KRW,
        "LIVE_PERFORMANCE_GATE_ENABLED": settings.LIVE_PERFORMANCE_GATE_ENABLED,
        "RESIDUAL_LIVE_SELL_MODE": settings.RESIDUAL_LIVE_SELL_MODE,
        "RESIDUAL_BUY_SIZING_MODE": settings.RESIDUAL_BUY_SIZING_MODE,
        "APPROVED_STRATEGY_PROFILE_PATH": settings.APPROVED_STRATEGY_PROFILE_PATH,
    }
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "failsafe.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", False)

    ensure_db().close()
    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
    runtime_state.set_startup_gate_reason(None)

    yield

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
    runtime_state.set_startup_gate_reason(None)

    for key, value in old_settings.items():
        object.__setattr__(settings, key, value)

    if old_env_db_path is None:
        os.environ.pop("DB_PATH", None)
    else:
        os.environ["DB_PATH"] = old_env_db_path


def _set_tmp_db(tmp_path, monkeypatch: pytest.MonkeyPatch | None = None):
    db_path = str(tmp_path / "live_loop.sqlite")
    if monkeypatch is not None:
        monkeypatch.setenv("DB_PATH", db_path)
    else:
        os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    ensure_db().close()
    return db_path


def _set_live_runtime_paths(
    monkeypatch: pytest.MonkeyPatch,
    *,
    base_dir: Path,
    db_path: Path | None = None,
) -> None:
    roots = {
        "ENV_ROOT": (base_dir / "env").resolve(),
        "RUN_ROOT": (base_dir / "run").resolve(),
        "DATA_ROOT": (base_dir / "data").resolve(),
        "LOG_ROOT": (base_dir / "logs").resolve(),
        "BACKUP_ROOT": (base_dir / "backup").resolve(),
    }
    for key, value in roots.items():
        monkeypatch.setenv(key, str(value))
    monkeypatch.setenv("RUN_LOCK_PATH", str((roots["RUN_ROOT"] / "live" / "bithumb-bot.lock").resolve()))
    live_db_path = (
        db_path.resolve()
        if db_path is not None
        else (roots["DATA_ROOT"] / "live" / "trades" / "live.sqlite").resolve()
    )
    monkeypatch.setenv("DB_PATH", str(live_db_path))
    object.__setattr__(settings, "DB_PATH", str(live_db_path))


def _insert_order(*, status: str, client_order_id: str, created_ts: int) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, exchange_order_id, status, side, price,
                qty_req, qty_filled, created_ts, updated_ts, last_error
            )
            VALUES (?, NULL, ?, 'BUY', NULL, 0.01, 0.0, ?, ?, NULL)
            """,
            (client_order_id, status, created_ts, created_ts),
        )
        conn.commit()
    finally:
        conn.close()
class _LoopConn:
    def __init__(
        self,
        *,
        open_order_created_ts: int | None = None,
        asset_qty: float = 0.0,
        target_state: TargetPositionState | None = None,
    ):
        self.open_order_created_ts = open_order_created_ts
        self.asset_qty = float(asset_qty)
        self.marked_recovery_required = 0
        self.target_state = target_state
        self.runtime_dependency_manifest_rows: dict[str, dict[str, object]] = {}
        self.runtime_strategy_set_manifest_rows: dict[str, dict[str, object]] = {}
        self.runtime_strategy_decision_bundle_rows: dict[str, dict[str, object]] = {}
        self.runtime_strategy_decision_result_rows: dict[tuple[int, str], dict[str, object]] = {}
        self.portfolio_allocation_rows: dict[str, dict[str, object]] = {}
        self.portfolio_target_rows: dict[str, dict[str, object]] = {}
        self.execution_plan_batch_rows: dict[str, dict[str, object]] = {}
        self.execution_plan_rows: dict[tuple[int, str], dict[str, object]] = {}
        self.strategy_virtual_target_state_rows: dict[tuple[str, str, str, str], str] = {}
        self.budget_lock_rows: dict[str, dict[str, object]] = {}
        self.order_lock_rows: dict[str, dict[str, object]] = {}
        self.in_transaction = False

    def execute(self, query, params=None):
        q = " ".join(str(query).split())

        if "FROM sqlite_master" in q and "budget_locks" in q and "order_locks" in q:
            return _Rows([{"name": "budget_locks"}, {"name": "order_locks"}])

        if "FROM target_position_state" in q:
            if self.target_state is None:
                return _Rows(None)
            state = self.target_state
            return _Rows(
                {
                    "pair": state.pair,
                    "target_exposure_krw": state.target_exposure_krw,
                    "target_qty": state.target_qty,
                    "last_signal": state.last_signal,
                    "last_decision_id": state.last_decision_id,
                    "last_reference_price": state.last_reference_price,
                    "updated_ts": state.updated_ts,
                    "target_origin": state.target_origin,
                    "adoption_reason": state.adoption_reason,
                    "adopted_broker_qty": state.adopted_broker_qty,
                    "adopted_broker_exposure_krw": state.adopted_broker_exposure_krw,
                    "created_from_signal": state.created_from_signal,
                    "actual_target_authority": state.actual_target_authority,
                    "actual_target_authority_scope": state.actual_target_authority_scope,
                    "actual_target_source": state.actual_target_source,
                    "runtime_strategy_set_manifest_hash": state.runtime_strategy_set_manifest_hash,
                    "runtime_strategy_decision_bundle_hash": state.runtime_strategy_decision_bundle_hash,
                    "portfolio_allocation_decision_hash": state.portfolio_allocation_decision_hash,
                    "portfolio_target_hash": state.portfolio_target_hash,
                    "execution_plan_batch_hash": state.execution_plan_batch_hash,
                    "execution_submit_plan_hash": state.execution_submit_plan_hash,
                    "actual_target_provenance_hash": state.actual_target_provenance_hash,
                    "actual_target_provenance_json": state.actual_target_provenance_json,
                }
            )

        if "INSERT INTO target_position_state" in q:
            assert params is not None
            self.target_state = TargetPositionState(
                pair=str(params[0]),
                target_exposure_krw=float(params[1]),
                target_qty=float(params[2]),
                last_signal=str(params[3]),
                last_decision_id=(None if params[4] is None else int(params[4])),
                last_reference_price=float(params[5]),
                updated_ts=int(params[6]),
                target_origin=str(params[7] if len(params) > 7 else ""),
                adoption_reason=str(params[8] if len(params) > 8 else ""),
                adopted_broker_qty=(
                    None if len(params) <= 9 or params[9] is None else float(params[9])
                ),
                adopted_broker_exposure_krw=(
                    None if len(params) <= 10 or params[10] is None else float(params[10])
                ),
                created_from_signal=str(params[11] if len(params) > 11 else ""),
                actual_target_authority=str(
                    params[12] if len(params) > 12 else "allocator_derived_pair_actual_target"
                ),
                actual_target_authority_scope=str(
                    params[13] if len(params) > 13 else "pair"
                ),
                actual_target_source=str(params[14] if len(params) > 14 else ""),
                runtime_strategy_set_manifest_hash=str(params[15] if len(params) > 15 else ""),
                runtime_strategy_decision_bundle_hash=str(params[16] if len(params) > 16 else ""),
                portfolio_allocation_decision_hash=str(params[17] if len(params) > 17 else ""),
                portfolio_target_hash=str(params[18] if len(params) > 18 else ""),
                execution_plan_batch_hash=str(params[19] if len(params) > 19 else ""),
                execution_submit_plan_hash=str(params[20] if len(params) > 20 else ""),
                actual_target_provenance_hash=str(params[21] if len(params) > 21 else ""),
                actual_target_provenance_json=str(params[22] if len(params) > 22 else "{}"),
            )
            return _Rows(None, rowcount=1)

        if "SELECT state_json FROM strategy_virtual_target_state" in q:
            assert params is not None
            key = (str(params[0]), str(params[1]), str(params[2]), str(params[3]))
            state_json = self.strategy_virtual_target_state_rows.get(key)
            return _Rows(None if state_json is None else {"state_json": state_json})

        if "INSERT INTO strategy_virtual_target_state" in q:
            assert params is not None
            key = (str(params[0]), str(params[2]), str(params[3]), str(params[4]))
            self.strategy_virtual_target_state_rows[key] = str(params[12])
            return _Rows(None, rowcount=1)

        if "INSERT OR IGNORE INTO runtime_strategy_set_manifest" in q:
            assert params is not None
            manifest_hash = str(params[0])
            if manifest_hash not in self.runtime_strategy_set_manifest_rows:
                self.runtime_strategy_set_manifest_rows[manifest_hash] = {
                    "id": len(self.runtime_strategy_set_manifest_rows) + 1,
                    "manifest_hash": manifest_hash,
                    "source": params[1],
                    "market_scope_json": params[2],
                    "active_strategy_count": params[3],
                    "single_pair_runtime_enforced": params[4],
                    "execution_config_hash": params[5],
                    "risk_config_hash": params[6],
                    "manifest_json": params[7],
                    "created_ts": params[8],
                }
            return _Rows(None, rowcount=1)

        if "SELECT id FROM runtime_strategy_set_manifest WHERE manifest_hash=?" in q:
            assert params is not None
            row = self.runtime_strategy_set_manifest_rows.get(str(params[0]))
            return _Rows(None if row is None else {"id": row["id"]})

        if "INSERT OR IGNORE INTO runtime_dependency_manifest" in q:
            assert params is not None
            manifest_hash = str(params[0])
            if manifest_hash not in self.runtime_dependency_manifest_rows:
                self.runtime_dependency_manifest_rows[manifest_hash] = {
                    "id": len(self.runtime_dependency_manifest_rows) + 1,
                    "manifest_hash": manifest_hash,
                }
            return _Rows(None, rowcount=1)

        if "SELECT id FROM runtime_dependency_manifest WHERE manifest_hash=?" in q:
            assert params is not None
            row = self.runtime_dependency_manifest_rows.get(str(params[0]))
            return _Rows(None if row is None else {"id": row["id"]})

        if "INSERT OR IGNORE INTO runtime_strategy_decision_bundle" in q:
            assert params is not None
            bundle_hash = str(params[5])
            if bundle_hash not in self.runtime_strategy_decision_bundle_rows:
                self.runtime_strategy_decision_bundle_rows[bundle_hash] = {
                    "id": len(self.runtime_strategy_decision_bundle_rows) + 1,
                    "runtime_strategy_set_manifest_id": params[3],
                    "strategy_set_manifest_hash": params[4],
                }
            return _Rows(None, rowcount=1)

        if "SELECT id FROM runtime_strategy_decision_bundle WHERE bundle_hash=?" in q:
            assert params is not None
            row = self.runtime_strategy_decision_bundle_rows.get(str(params[0]))
            return _Rows(None if row is None else {"id": row["id"]})

        if "INSERT OR IGNORE INTO runtime_strategy_decision_result" in q:
            assert params is not None
            key = (int(params[0]), str(params[1]))
            if key not in self.runtime_strategy_decision_result_rows:
                self.runtime_strategy_decision_result_rows[key] = {
                    "id": len(self.runtime_strategy_decision_result_rows) + 1,
                }
            return _Rows(None, rowcount=1)

        if "SELECT id FROM runtime_strategy_decision_result WHERE bundle_id=? AND strategy_instance_id=?" in q:
            assert params is not None
            row = self.runtime_strategy_decision_result_rows.get((int(params[0]), str(params[1])))
            return _Rows(None if row is None else {"id": row["id"]})

        if "SELECT runtime_strategy_set_manifest_id, strategy_set_manifest_hash FROM runtime_strategy_decision_bundle WHERE id=?" in q:
            assert params is not None
            row = next(
                (
                    item
                    for item in self.runtime_strategy_decision_bundle_rows.values()
                    if int(item["id"]) == int(params[0])
                ),
                None,
            )
            return _Rows(row)

        if "INSERT OR IGNORE INTO portfolio_allocation_decision" in q:
            assert params is not None
            allocation_hash = str(params[3])
            if allocation_hash not in self.portfolio_allocation_rows:
                self.portfolio_allocation_rows[allocation_hash] = {
                    "id": len(self.portfolio_allocation_rows) + 1,
                    "runtime_strategy_set_manifest_id": params[1],
                    "runtime_strategy_set_manifest_hash": params[2],
                }
            return _Rows(None, rowcount=1)

        if "SELECT id FROM portfolio_allocation_decision WHERE allocation_decision_hash=?" in q:
            assert params is not None
            row = self.portfolio_allocation_rows.get(str(params[0]))
            return _Rows(None if row is None else {"id": row["id"]})

        if "INSERT OR IGNORE INTO strategy_contribution" in q:
            return _Rows(None, rowcount=1)

        if "INSERT OR IGNORE INTO portfolio_target" in q:
            assert params is not None
            target_hash = str(params[6])
            if target_hash not in self.portfolio_target_rows:
                self.portfolio_target_rows[target_hash] = {
                    "id": len(self.portfolio_target_rows) + 1,
                    "pair": params[1],
                    "final_portfolio_target_hash": target_hash,
                }
            return _Rows(None, rowcount=1)

        if "SELECT id FROM portfolio_target WHERE final_portfolio_target_hash=?" in q:
            assert params is not None
            row = self.portfolio_target_rows.get(str(params[0]))
            return _Rows(None if row is None else {"id": row["id"]})

        if "SELECT runtime_strategy_set_manifest_id, runtime_strategy_set_manifest_hash FROM portfolio_allocation_decision WHERE id=?" in q:
            assert params is not None
            row = next(
                (
                    item
                    for item in self.portfolio_allocation_rows.values()
                    if int(item["id"]) == int(params[0])
                ),
                None,
            )
            return _Rows(row)

        if "INSERT OR IGNORE INTO execution_plan_batch" in q:
            assert params is not None
            batch_hash = str(params[0])
            if batch_hash not in self.execution_plan_batch_rows:
                self.execution_plan_batch_rows[batch_hash] = {
                    "batch_hash": batch_hash,
                    "batch_id": str(params[1]),
                }
            return _Rows(None, rowcount=1)

        if "INSERT OR IGNORE INTO budget_locks" in q:
            assert params is not None
            lock_hash = str(params[0])
            if lock_hash not in self.budget_lock_rows:
                self.budget_lock_rows[lock_hash] = {
                    "lock_hash": lock_hash,
                    "status": str(params[4]),
                    "evidence_hash": str(params[7]),
                }
            return _Rows(None, rowcount=1)

        if "SELECT lock_hash, status, evidence_hash FROM budget_locks WHERE lock_hash=?" in q:
            assert params is not None
            return _Rows(self.budget_lock_rows.get(str(params[0])))

        if "INSERT OR IGNORE INTO order_locks" in q:
            assert params is not None
            lock_hash = str(params[0])
            if lock_hash not in self.order_lock_rows:
                self.order_lock_rows[lock_hash] = {
                    "lock_hash": lock_hash,
                    "status": str(params[4]),
                    "evidence_hash": str(params[7]),
                }
            return _Rows(None, rowcount=1)

        if "SELECT lock_hash, status, evidence_hash FROM order_locks WHERE lock_hash=?" in q:
            assert params is not None
            return _Rows(self.order_lock_rows.get(str(params[0])))

        if "INSERT OR IGNORE INTO execution_plan" in q:
            assert params is not None
            key = (int(params[0]), str(params[4]))
            if key not in self.execution_plan_rows:
                self.execution_plan_rows[key] = {
                    "id": len(self.execution_plan_rows) + 1,
                    "execution_submit_plan_hash": str(params[5]),
                    "execution_submit_plan_json": params[17],
                }
            return _Rows(None, rowcount=1)

        if "UPDATE execution_plan" in q and "WHERE execution_submit_plan_hash=?" in q:
            assert params is not None
            submit_hash = str(params[11])
            for row in self.execution_plan_rows.values():
                if str(row.get("execution_submit_plan_hash") or "") == submit_hash:
                    row["execution_submit_plan_json"] = params[0]
                    row["execution_submit_plan_hash"] = str(params[1])
                    return _Rows(None, rowcount=1)
            return _Rows(None, rowcount=0)

        if "SELECT id FROM execution_plan WHERE allocation_id=? AND execution_plan_bundle_hash=?" in q:
            assert params is not None
            row = self.execution_plan_rows.get((int(params[0]), str(params[1])))
            return _Rows(None if row is None else {"id": row["id"]})

        if "SELECT execution_submit_plan_json FROM execution_plan WHERE id=?" in q:
            assert params is not None
            row = next(
                (
                    item
                    for item in self.execution_plan_rows.values()
                    if int(item["id"]) == int(params[0])
                ),
                None,
            )
            return _Rows(None if row is None else {"execution_submit_plan_json": row["execution_submit_plan_json"]})

        if "SELECT final_portfolio_target_hash" in q and "FROM portfolio_target" in q:
            return _Rows(
                [
                    {"final_portfolio_target_hash": str(row["final_portfolio_target_hash"])}
                    for row in self.portfolio_target_rows.values()
                ]
            )

        if "FROM candles" in q:
            return _Rows({"ts": 10_000, "close": 100.0})

        if "COUNT(*) AS open_count" in q:
            if self.open_order_created_ts is None:
                return _Rows({"open_count": 0, "oldest_created_ts": None})
            return _Rows({"open_count": 1, "oldest_created_ts": self.open_order_created_ts})

        if "COUNT(*) AS open_order_count" in q:
            return _Rows(
                {
                    "open_order_count": 0 if self.open_order_created_ts is None else 1,
                    "recovery_required_count": 0,
                }
            )

        if "FROM portfolio" in q:
            return _Rows({"cash_krw": 100000.0, "asset_qty": self.asset_qty})

        if "SELECT COUNT(*) AS cnt FROM orders WHERE status='SUBMIT_UNKNOWN'" in q:
            return _Rows({"cnt": 0})

        if "SELECT COUNT(*) AS cnt FROM orders WHERE status='RECOVERY_REQUIRED'" in q:
            return _Rows({"cnt": 0})

        if "SELECT COUNT(*) AS cnt FROM orders WHERE status='ACCOUNTING_PENDING'" in q:
            return _Rows({"cnt": 0})

        if "status='SUBMIT_UNKNOWN'" in q and "exchange_order_id" in q:
            return _Rows({"cnt": 0})

        if "client_order_id LIKE 'remote_%'" in q:
            return _Rows({"cnt": 0})

        if (
            "COALESCE(SUM(MAX(qty_req - qty_filled, 0.0)), 0.0) AS reserved_exit_qty" in q
            and "FROM orders" in q
            and "side='SELL'" in q
        ):
            return _Rows({"reserved_exit_qty": 0.0})

        if (
            "SELECT DISTINCT" in q
            and "FROM open_position_lots" in q
            and "lot_semantic_version" in q
        ):
            if self.asset_qty <= 1e-12:
                return _Rows([])
            return _Rows(
                [
                    {
                        "lot_semantic_version": 1,
                        "internal_lot_size": 0.0001,
                        "lot_min_qty": 0.0001,
                        "lot_qty_step": 0.0001,
                        "lot_min_notional_krw": 5000.0,
                        "lot_max_qty_decimals": 8,
                        "lot_rule_source_mode": "ledger",
                    }
                ]
            )

        if "FROM open_position_lots" in q and "SUM(" in q:
            if "executable_lot_count" in q and "dust_tracking_lot_count, 0) = 0" in q:
                return _Rows((self.asset_qty, 1 if self.asset_qty > 1e-12 else 0))
            if "dust_tracking_lot_count" in q and "executable_lot_count, 0) = 0" in q:
                return _Rows((0.0, 0))

        if (
            "AS pending_submit_count" in q
            and "AS accounting_pending_count" in q
            and "AS submit_unknown_count" in q
            and "AS recovery_required_count" in q
            and "AS stale_new_partial_count" in q
            and "FROM orders" in q
        ):
            if self.open_order_created_ts is not None:
                return _Rows(
                    {
                        "pending_submit_count": 0,
                        "accounting_pending_count": 0,
                        "submit_unknown_count": 0,
                        "recovery_required_count": 0,
                        "stale_new_partial_count": 0,
                    }
                )

            real_conn = ensure_db()
            try:
                row = real_conn.execute(query, params or ()).fetchone()
            finally:
                real_conn.close()

            if row is None:
                return _Rows(
                    {
                        "pending_submit_count": 0,
                        "accounting_pending_count": 0,
                        "submit_unknown_count": 0,
                        "recovery_required_count": 0,
                        "stale_new_partial_count": 0,
                    }
                )

            return _Rows(
                {
                    "pending_submit_count": row["pending_submit_count"] or 0,
                    "accounting_pending_count": row["accounting_pending_count"] or 0,
                    "submit_unknown_count": row["submit_unknown_count"] or 0,
                    "recovery_required_count": row["recovery_required_count"] or 0,
                    "stale_new_partial_count": row["stale_new_partial_count"] or 0,
                }
            )

        if "COUNT(*) AS repair_count" in q and "FROM fee_gap_accounting_repairs" in q:
            return _Rows({"repair_count": 0})

        if "COUNT(*) AS repair_count" in q and "FROM position_authority_repairs" in q:
            return _Rows({"repair_count": 0})

        if "FROM fee_gap_accounting_repairs" in q and "ORDER BY event_ts DESC" in q:
            return _Rows(None)

        if "FROM external_position_adjustments" in q and "COUNT(*) AS adjustment_count" in q:
            return _Rows({"adjustment_count": 0, "asset_qty_total": 0.0, "cash_total": 0.0})

        if "FROM external_position_adjustments" in q and "ORDER BY event_ts DESC" in q:
            return _Rows(None)

        if "FROM broker_fill_observations" in q:
            return _Rows([])

        if "INSERT INTO strategy_decisions" in q:
            return _Rows(None, rowcount=1, lastrowid=42)

        if "SET status='RECOVERY_REQUIRED'" in q:
            if self.open_order_created_ts is None:
                self.marked_recovery_required = 0
                return _Rows(None, rowcount=0)
            self.marked_recovery_required = 1
            return _Rows(None, rowcount=1)

        if "SELECT client_order_id, exchange_order_id" in q and "WHERE status IN" in q:
            if self.open_order_created_ts is None:
                return _Rows(None)
            return _Rows({"client_order_id": "open_1", "exchange_order_id": "ex-open-1"})

        raise AssertionError(f"unexpected query: {query}")

    def commit(self):
        return None

    def close(self):
        return None


class _Rows:
    def __init__(self, row, rowcount: int = 0, lastrowid: int = 1):
        self._row = row
        self.rowcount = rowcount
        self.lastrowid = lastrowid

    def fetchone(self):
        return self._row

    def fetchall(self):
        if self._row is None:
            return []
        if isinstance(self._row, list):
            return self._row
        return [self._row]


def _position_snapshot_from_context(
    signal: str,
    base_context: dict[str, object],
) -> PositionSnapshot:
    position_state = base_context.get("position_state")
    normalized_exposure = (
        position_state.get("normalized_exposure")
        if isinstance(position_state, dict)
        else None
    )
    if not isinstance(normalized_exposure, dict):
        return PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False)

    open_exposure_qty = float(normalized_exposure.get("open_exposure_qty") or 0.0)
    raw_total_asset_qty = float(normalized_exposure.get("raw_total_asset_qty") or 0.0)
    dust_tracking_qty = float(normalized_exposure.get("dust_tracking_qty") or 0.0)
    sellable_executable_lot_count = int(
        normalized_exposure.get("sellable_executable_lot_count") or 0
    )
    exit_allowed = bool(normalized_exposure.get("exit_allowed"))
    exit_block_reason = str(normalized_exposure.get("exit_block_reason") or "")
    has_executable_exposure = sellable_executable_lot_count > 0
    has_non_executable_residue = dust_tracking_qty > 0.0
    has_any_position_residue = raw_total_asset_qty > 0.0 or open_exposure_qty > 0.0
    has_dust_only_remainder = (
        has_non_executable_residue
        and not has_executable_exposure
        and exit_block_reason == "dust_only_remainder"
    )
    terminal_state = "dust_only" if has_dust_only_remainder else "open_exposure" if has_executable_exposure else "flat"
    return PositionSnapshot(
        in_position=has_executable_exposure,
        entry_allowed=not has_executable_exposure,
        exit_allowed=exit_allowed,
        exit_block_reason=exit_block_reason,
        terminal_state=terminal_state,
        qty_open=open_exposure_qty,
        raw_qty_open=open_exposure_qty,
        raw_total_asset_qty=raw_total_asset_qty,
        open_lot_count=sellable_executable_lot_count,
        dust_tracking_lot_count=1 if has_non_executable_residue else 0,
        sellable_executable_lot_count=sellable_executable_lot_count,
        dust_classification="harmless_dust" if has_dust_only_remainder else "",
        dust_state=terminal_state if has_non_executable_residue else "",
        effective_flat=not has_executable_exposure,
        has_executable_exposure=has_executable_exposure,
        has_any_position_residue=has_any_position_residue,
        has_non_executable_residue=has_non_executable_residue,
        has_dust_only_remainder=has_dust_only_remainder,
    )


class _RuntimeDecisionResult:
    def __init__(
        self,
        *,
        signal: str = "BUY",
        candle_ts: int = 9000,
        price: float = 100.0,
        reason: str | None = None,
        base_context: dict[str, object] | None = None,
    ):
        self.base_context = {
            "market_price": price,
            "last_close": price,
            "position_state": {"normalized_exposure": {"sellable_executable_lot_count": 0}},
        }
        if base_context is not None:
            self.base_context.update(base_context)
        execution_intent = None
        if signal == "BUY":
            execution_intent = EntryExecutionIntent(
                side="BUY",
                intent="enter",
                pair=settings.PAIR,
                requires_execution_sizing=True,
                budget_fraction_of_cash=1.0,
                max_budget_krw=float(settings.MAX_ORDER_KRW),
            )
        self.decision = StrategyDecisionV2(
            strategy_name=str(settings.STRATEGY_NAME),
            raw_signal=signal,
            raw_reason=reason or f"unit {signal.lower()}",
            entry_signal=signal,
            entry_reason=reason or f"unit {signal.lower()}",
            exit_signal=signal,
            exit_reason=reason or f"unit {signal.lower()}",
            final_signal=signal,
            final_reason=reason or f"unit {signal.lower()}",
            blocked_filters=(),
            entry_blocked=False,
            entry_block_reason=None,
            exit_rule=None,
            exit_evaluations=(),
            protective_exit_overrode_entry=False,
            exit_filter_suppression_prevented=False,
            position_snapshot=_position_snapshot_from_context(signal, self.base_context),
            execution_intent=execution_intent,
            entry_decision=object(),
            trace={"final_signal": signal},
            policy_hash="sha256:failsafe-policy",
            policy_contract_hash="sha256:failsafe-contract",
            policy_input_hash="sha256:failsafe-input",
            policy_decision_hash="sha256:failsafe-decision",
        )
        self.candle_ts = candle_ts
        self.market_price = price
        self.replay_fingerprint = {"schema_version": 1, "candle_ts": candle_ts}
        self.boundary = {"decision_boundary_phase": "unit_failsafe"}
        self.policy_hashes = None

    def as_legacy_dict(self) -> dict[str, object]:
        return dict(self.base_context)


class _RuntimeDecisionBundle:
    def __init__(self, result: _RuntimeDecisionResult, strategy_set) -> None:
        self.results = (result,)
        self.strategy_set = strategy_set

    @property
    def candle_ts(self) -> int:
        return int(self.results[0].candle_ts)

    @property
    def market_price(self) -> float:
        return float(self.results[0].market_price)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "authority_label": "RuntimeStrategyDecisionResultBundle",
            "result_count": 1,
            "results": [dict(self.results[0].base_context)],
        }

    def content_hash(self) -> str:
        return "sha256:unit-runtime-decision-bundle"


def _install_runtime_gateway(monkeypatch_or_factory, result_factory=None):
    global _RUN_LOOP_RUNNER
    if result_factory is None:
        result_factory = monkeypatch_or_factory
    class _Gateway:
        def decide_bundle(self, conn, *, strategy_set=None, through_ts_ms=None, **_kwargs):
            from bithumb_bot.runtime_strategy_set import derive_strategy_instance_id

            result = result_factory(conn, through_ts_ms=through_ts_ms)
            spec = (
                None
                if strategy_set is None
                else strategy_set.spec_for_strategy(result.decision.strategy_name)
            )
            strategy_instance_id = (
                derive_strategy_instance_id(spec)
                if spec is not None
                else str(result.decision.strategy_name)
            )
            scope_key = RuntimeScopeKey(
                pair=settings.PAIR,
                interval=settings.INTERVAL,
                strategy_instance_id=strategy_instance_id,
                strategy_name=result.decision.strategy_name,
                runtime_contract_hash="sha256:unit-runtime-contract",
                approved_profile_hash="sha256:unit-approved-profile",
                strategy_parameters_hash="sha256:unit-parameters",
            )
            result.base_context.update(
                {
                    "runtime_decision_request_hash": "sha256:unit-runtime-request",
                    "strategy_instance_id": strategy_instance_id,
                    "strategy_parameters_hash": "sha256:unit-parameters",
                    "approved_profile_hash": "sha256:unit-approved-profile",
                    "runtime_contract_hash": "sha256:unit-runtime-contract",
                    "plugin_contract_hash": "sha256:unit-plugin-contract",
                    "runtime_scope_key": scope_key.as_dict(),
                    "scope_key_hash": scope_key.scope_key_hash(),
                    "through_ts_ms": through_ts_ms,
                }
            )
            result.replay_fingerprint.update(
                {
                    "runtime_decision_request_hash": "sha256:unit-runtime-request",
                    "strategy_instance_id": strategy_instance_id,
                    "strategy_parameters_hash": "sha256:unit-parameters",
                    "approved_profile_hash": "sha256:unit-approved-profile",
                    "runtime_contract_hash": "sha256:unit-runtime-contract",
                    "plugin_contract_hash": "sha256:unit-plugin-contract",
                    "runtime_scope_key": scope_key.as_dict(),
                    "scope_key_hash": scope_key.scope_key_hash(),
                    "through_ts_ms": through_ts_ms,
                }
            )
            return _RuntimeDecisionBundle(result, strategy_set)

    if _RUN_LOOP_RUNNER is not None:
        _RUN_LOOP_RUNNER.container = replace(
            _RUN_LOOP_RUNNER.container,
            decision_coordinator=replace(
                _RUN_LOOP_RUNNER.container.decision_coordinator,
                decision_gateway_factory=_Gateway,
            ),
        )
    return _Gateway


def _runtime_result_from_payload(payload: dict[str, object]) -> _RuntimeDecisionResult:
    signal = str(payload.get("signal") or payload.get("final_signal") or "HOLD").upper()
    candle_ts = int(payload.get("ts") or 9000)
    price = float(payload.get("last_close") or payload.get("market_price") or 100.0)
    reason = str(payload.get("reason") or f"unit {signal.lower()}")
    base_context = dict(payload)
    base_context["market_price"] = price
    base_context["last_close"] = price
    return _RuntimeDecisionResult(
        signal=signal,
        candle_ts=candle_ts,
        price=price,
        reason=reason,
        base_context=base_context,
    )


class _DummyBroker:
    def get_open_orders(self):
        return []

    def cancel_order(self, *args, **kwargs):
        return None

class _FlattenFailBroker(_DummyBroker):
    def get_balance(self):
        return BrokerBalance(
            cash_available=100_000.0,
            cash_locked=0.0,
            asset_available=1.0,
            asset_locked=0.0,
        )

    def place_order(self, *args, **kwargs):
        raise RuntimeError("place_order boom")

def _prepare_run_loop(
    monkeypatch,
    open_order_created_ts=None,
    asset_qty: float = 0.0,
    target_state: TargetPositionState | None = None,
):
    global _RUN_LOOP_RUNNER
    monkeypatch.setattr("bithumb_bot.config.notifier_is_configured", lambda: True)
    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_age_sec(None)
    runtime_state.set_startup_gate_reason(None)
    runtime_state.reset_candle_processing_state()

    resolved_db_path = str(Path(settings.DB_PATH).resolve())
    monkeypatch.setenv("DB_PATH", resolved_db_path)
    object.__setattr__(settings, "DB_PATH", resolved_db_path)
    resolved_db = Path(resolved_db_path).resolve()
    seeded_live_safe_db = None if "paper" in resolved_db.parts else resolved_db
    _set_live_runtime_paths(
        monkeypatch,
        base_dir=resolved_db.parent / "live-runtime",
        db_path=seeded_live_safe_db,
    )

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "STRATEGY_NAME", "sma_with_filter")
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "KILL_SWITCH", False)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "/tmp/unit-approved-profile.json")

    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)

    validate_live_mode_preflight = lambda _cfg: None
    validate_runtime_strategy_set_selection = lambda _cfg: None
    validate_market_runtime = lambda _cfg: None
    runtime_strategy_set_manifest_provider = _unit_runtime_strategy_set_manifest
    risk_policy = _unit_live_risk_policy()
    import bithumb_bot.runtime_strategy_set as runtime_strategy_set

    monkeypatch.setattr(
        runtime_strategy_set,
        "load_approved_profile",
        lambda _path: {
            "profile_mode": "small_live",
            "profile_content_hash": "sha256:unit-profile",
            "strategy_parameters": runtime_strategy_parameters_from_settings(
                "sma_with_filter",
                settings,
            ),
            "risk_policy": risk_policy.as_dict(),
            "risk_policy_hash": risk_policy.policy_hash(),
            "risk_enforcement_mode": "enforced",
            "missing_risk_policy_behavior": "fail_closed_for_live",
        },
    )
    monkeypatch.setattr(runtime_strategy_set, "diff_profile_to_runtime", lambda *_args, **_kwargs: ())
    monkeypatch.setattr(runtime_strategy_set, "expected_profile_modes_for_runtime", lambda _runtime: (("small_live",), "ok"))
    monkeypatch.setattr(
        "bithumb_bot.runtime_risk_engine.RuntimeRiskEngineAdapter.evaluate_pre_submit",
        lambda self, *, plan, **_kwargs: _unit_pre_submit_risk_decision(plan),
    )
    monkeypatch.setattr(
        "bithumb_bot.run_loop_execution_planner.runtime_strategy_set_manifest_hash",
        lambda _strategy_set: "sha256:unit-runtime-strategy-set",
    )
    interval_parser = lambda _: 1
    market_sync = lambda quiet=True: None
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.validate_market_runtime", validate_market_runtime)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.parse_interval_sec", interval_parser)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.cmd_sync", market_sync)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.BithumbBroker", lambda: _DummyBroker())
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (False, "ok"),
    )
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (False, "ok"),
    )
    closed_candle_selector = lambda _conn, **_kwargs: ({"ts": 9000, "close": 100.0}, None)
    gateway_factory = _install_runtime_gateway(
        lambda _conn, **_kwargs: _RuntimeDecisionResult(signal="BUY", candle_ts=9000),
    )

    loop_conn = _LoopConn(
        open_order_created_ts=open_order_created_ts,
        asset_qty=asset_qty,
        target_state=target_state,
    )
    monkeypatch.setattr("bithumb_bot.runtime_data_access.ensure_db", lambda: loop_conn)
    monkeypatch.setattr("bithumb_bot.execution_service.ensure_db", lambda: loop_conn)
    monkeypatch.setattr("bithumb_bot.flatten.ensure_db", lambda: loop_conn)
    monkeypatch.setattr("bithumb_bot.flatten.init_portfolio", lambda _conn: None)
    broker_factory = lambda: _DummyBroker()
    monkeypatch.setattr(
        "bithumb_bot.runtime.safety_controller.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (False, "ok"),
    )
    monkeypatch.setattr(
        "bithumb_bot.runtime.safety_controller.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (False, "ok"),
    )

    ticks = iter([10.0, 11.0, 11.0, 11.0, 11.0, 11.0])
    clock = lambda: next(ticks, 11.0)
    app = create_default_runtime_app(settings)
    notifications = _Notifications()
    app = replace(
        app,
        db_factory=lambda: loop_conn,
        clock=clock,
        scheduler=_NoSleepScheduler(),
        broker_factory=broker_factory,
        market_sync=market_sync,
        candle_reader=lambda _conn, **_kwargs: {"ts": 9000, "close": 100.0},
        closed_candle_selector=closed_candle_selector,
        notification_service=notifications,
        notification_adapter=app.notification_adapter.__class__(notifications),
        validate_live_mode_preflight=validate_live_mode_preflight,
        validate_runtime_strategy_set_selection=validate_runtime_strategy_set_selection,
        validate_market_runtime=validate_market_runtime,
        interval_parser=interval_parser,
        runtime_strategy_set_manifest_provider=runtime_strategy_set_manifest_provider,
    )
    app = replace(
        app,
        decision_coordinator=replace(
            app.decision_coordinator,
            db_factory=lambda: loop_conn,
            decision_gateway_factory=gateway_factory,
        ),
        startup_controller=replace(app.startup_controller, broker_factory=broker_factory),
    )
    _RUN_LOOP_RUNNER = Runner(app)
    return loop_conn


def test_run_loop_live_broker_error_halts_instead_of_crash(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.live_execute_signal",
        lambda broker, signal, ts, px: (_ for _ in ()).throw(BrokerRejectError("reject")),
    )

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.notifier.notify", lambda msg: notifications.append(msg))

    run_loop()

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.retry_at_epoch_sec is None
    assert state.halt_new_orders_blocked is False
    assert state.halt_reason_code is None


def test_flat_start_safety_check_avoids_self_lock_when_writer_transaction_open():
    writer_conn = ensure_db()
    try:
        writer_conn.execute("BEGIN IMMEDIATE")
        allowed, reason = _default_flat_start_safety_check()
    finally:
        writer_conn.rollback()
        writer_conn.close()

    assert isinstance(allowed, bool)
    assert isinstance(reason, str)


def test_flat_start_safety_check_blocks_local_dust_position_without_broker_confirmation(monkeypatch):
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            ) VALUES (1, 1000000.0, 0.00009629, 1000000.0, 0.0, 0.00009629, 0.0)
            """
        )
        conn.execute(
            "INSERT INTO candles(pair, interval, ts, open, high, low, close, volume) VALUES ('KRW-BTC', '1m', 1, 100000000, 100000000, 100000000, 100000000, 1.0)"
        )
        conn.commit()
    finally:
        conn.close()

    class _Resolved:
        class rules:
            min_qty = 0.0001
            min_notional_krw = 5000.0

    monkeypatch.setattr("bithumb_bot.broker.order_rules.get_effective_order_rules", lambda _pair: _Resolved())

    allowed, reason = _default_flat_start_safety_check()

    assert allowed is False
    assert "flat_start_requires_operator_review" in reason
    assert "state=blocking_dust" in reason
    assert "broker_qty=0.00000000" in reason
    assert "local_qty=0.00009629" in reason
    assert "min_qty=0.00010000" in reason
    assert "qty_below_min(broker=0 local=1)" in reason


def test_run_loop_surfaces_market_preflight_error_during_live_startup(monkeypatch):
    _prepare_run_loop(monkeypatch)
    called = {"n": 0}

    def _market_runtime(_cfg):
        called["n"] += 1
        raise ValueError("market gate")

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.validate_market_runtime", _market_runtime)

    with pytest.raises(Exception) as exc:
        run_loop()

    assert "market gate" in str(exc.value)
    assert called["n"] == 1


def test_run_loop_reconcile_error_halts_instead_of_crash(monkeypatch):
    _prepare_run_loop(monkeypatch)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.notifier.notify", lambda msg: notifications.append(msg))

    calls = {"n": 0}

    def _reconcile(_broker):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise RuntimeError("reconcile boom")

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", _reconcile, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda broker, signal, ts, px: None)

    run_loop()

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason is not None
    assert "reconcile failed" in state.last_disable_reason
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "POST_TRADE_RECONCILE_FAILED"
    assert state.halt_state_unresolved is True
    halted = [n for n in notifications if "event=trading_halted" in n and "reason_code=POST_TRADE_RECONCILE_FAILED" in n]
    assert halted
    assert any("symbol=" in n for n in halted)
    assert any(
        "operator_next_action=run reconcile, validate order state, then run recovery-report before resume" in n
        for n in halted
    )
    assert any(
        "operator_hint_command=uv run python bot.py reconcile && uv run python bot.py recovery-report" in n
        for n in halted
    )


def test_run_loop_periodically_reconciles_when_open_order_exists(monkeypatch):
    _prepare_run_loop(monkeypatch, open_order_created_ts=10_500)

    calls = {"n": 0}

    def _reconcile(_broker):
        calls["n"] += 1

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", _reconcile, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop()

    assert calls["n"] == 2


def test_run_loop_stale_open_order_halts_and_marks_recovery_required(monkeypatch):
    loop_conn = _prepare_run_loop(monkeypatch, open_order_created_ts=0)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 5)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop()

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason is not None
    assert "stale unresolved open order" in state.last_disable_reason
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "STALE_OPEN_ORDER"
    assert state.halt_state_unresolved is True
    assert loop_conn.marked_recovery_required == 1


def test_run_loop_unresolved_open_order_blocks_new_trading(monkeypatch):
    _prepare_run_loop(monkeypatch, open_order_created_ts=10_500)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    called = {"n": 0}

    def _live_execute(*_args, **_kwargs):
        called["n"] += 1

    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", _live_execute)

    run_loop()

    assert called["n"] == 0
    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.halt_new_orders_blocked is False


def test_run_loop_startup_recovery_gate_halts_when_unresolved_state_exists(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(status="RECOVERY_REQUIRED", client_order_id="startup_block", created_ts=1)
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    called = {"n": 0}

    def _live_execute(*_args, **_kwargs):
        called["n"] += 1

    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", _live_execute)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.notify", lambda msg: notifications.append(msg))

    run_loop()

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason is not None
    assert state.last_disable_reason.startswith("startup safety gate:")
    assert state.halt_reason_code == "STARTUP_SAFETY_GATE"
    assert state.halt_new_orders_blocked is True
    assert state.halt_state_unresolved is True
    assert called["n"] == 0
    assert any(
        "event=startup_gate_blocked" in n and "reason_code=STARTUP_BLOCKED" in n and "timestamp=" in n
        for n in notifications
    )
    assert any("operator_action_required=1" in n for n in notifications if "event=startup_gate_blocked" in n)
    startup = [n for n in notifications if "event=startup_gate_blocked" in n]
    assert any("operator_next_action=operator must reconcile unresolved orders before startup" in n for n in startup)
    assert any("operator_compact_summary=halt_reason=STARTUP_SAFETY_GATE" in n for n in startup)
    assert any("open_order_count=" in n for n in startup)
    assert any("position_summary=" in n for n in startup)
    assert any("reason_code=STARTUP_SAFETY_GATE" in n for n in notifications)
    halted = [n for n in notifications if "event=trading_halted" in n and "alert_kind=halt" in n]
    assert halted
    assert any("halt_open_orders_present=1" in n for n in halted)
    assert any("operator_action_required=1" in n for n in halted)
    assert any("unresolved_order_count=" in n for n in halted)
    assert any("position_may_remain=" in n for n in halted)
    assert any("operator_next_action=" in n for n in halted)


def test_run_loop_startup_safety_gate_halts_when_unresolved_open_order_exists(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    _insert_order(status="NEW", client_order_id="startup_unresolved", created_ts=1)
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop()

    state = runtime_state.snapshot()
    health = get_health_status()
    assert state.trading_enabled is False
    assert health["startup_gate_reason"] is not None
    assert "unresolved_open_orders=1" in str(health["startup_gate_reason"])


def test_run_loop_startup_recovery_gate_allows_clean_startup(monkeypatch, tmp_path):
    _set_tmp_db(tmp_path)
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    called = {"n": 0}

    def _live_execute(*_args, **_kwargs):
        called["n"] += 1

    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", _live_execute)

    run_loop()

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert state.retry_at_epoch_sec is None
    assert called["n"] == 0


def test_run_loop_live_harmless_dust_sell_suppresses_before_live_execution(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.00009193)

    _install_runtime_gateway(
        monkeypatch,
        lambda _conn, **_kwargs: _runtime_result_from_payload(
            {
                "ts": 1000,
                "last_close": 100_000_000.0,
                "curr_s": 1.0,
                "curr_l": 0.5,
                "signal": "SELL",
                "position_state": {
                    "normalized_exposure": {
                        "raw_total_asset_qty": 0.00009193,
                        "open_exposure_qty": 0.0,
                        "dust_tracking_qty": 0.00009193,
                        "sellable_executable_qty": 0.0,
                        "sellable_executable_lot_count": 0,
                        "exit_allowed": False,
                        "exit_block_reason": "dust_only_remainder",
                    }
                },
            }
        ),
    )

    suppression_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.record_harmless_dust_exit_suppression",
        lambda **kwargs: suppression_calls.append(kwargs) or True,
    )
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.live_execute_signal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not reach live execution")),
    )
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_partial_flatten_recent": 0,
            "dust_partial_flatten_reason": "flatten_not_recent",
            "dust_qty_gap_tolerance": 0.00005,
            "dust_qty_gap_small": 1,
            "dust_broker_qty": 0.00009193,
            "dust_local_qty": 0.00009193,
            "dust_delta_qty": 0.0,
            "dust_min_qty": 0.0001,
            "dust_min_notional_krw": 5000.0,
            "dust_latest_price": 100_000_000.0,
            "dust_broker_notional_krw": 9193.0,
            "dust_local_notional_krw": 9193.0,
            "dust_broker_qty_is_dust": 1,
            "dust_local_qty_is_dust": 1,
            "dust_broker_notional_is_dust": 0,
            "dust_local_notional_is_dust": 0,
            "dust_residual_summary": (
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
        },
    )

    run_loop()

    state = runtime_state.snapshot()
    assert state.trading_enabled is True
    assert suppression_calls
    assert suppression_calls[0]["signal"] == "SELL"
    assert suppression_calls[0]["side"] == "SELL"
    assert suppression_calls[0]["requested_qty"] == pytest.approx(0.00009193)
    assert suppression_calls[0]["market_price"] == pytest.approx(100_000_000.0)
    assert suppression_calls[0]["submit_qty_source"] == "position_state.normalized_exposure.sellable_executable_lot_count"


def test_run_loop_live_sell_does_not_presuppress_when_canonical_sell_authority_is_executable(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.00049193)

    _install_runtime_gateway(
        monkeypatch,
        lambda _conn, **_kwargs: _runtime_result_from_payload(
            {
                "ts": 1000,
                "last_close": 100_000_000.0,
                "curr_s": 1.0,
                "curr_l": 0.5,
                "signal": "SELL",
                "position_state": {
                    "normalized_exposure": {
                        "raw_total_asset_qty": 0.00049193,
                        "open_exposure_qty": 0.0004,
                        "dust_tracking_qty": 0.00009193,
                        "sellable_executable_qty": 0.0004,
                        "sellable_executable_lot_count": 1,
                        "exit_allowed": True,
                        "exit_block_reason": "none",
                    }
                },
            }
        ),
    )

    suppression_calls: list[dict[str, object]] = []
    live_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.record_harmless_dust_exit_suppression",
        lambda **kwargs: suppression_calls.append(kwargs) or True,
    )
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.live_execute_signal",
        lambda *_args, **_kwargs: live_calls.__setitem__("n", live_calls["n"] + 1) or None,
    )
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    runtime_state.record_reconcile_result(
        success=True,
        metadata={
            "dust_classification": "harmless_dust",
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_effective_flat": 1,
        },
    )

    run_loop()


def test_residual_sell_candidate_is_modeled_separately_from_strategy_sell_authority() -> None:
    context = {
        "signal": "SELL",
        "sellable_executable_lot_count": 0,
        "residual_inventory_mode": "track",
        "residual_inventory_state": "RESIDUAL_INVENTORY_TRACKED",
        "residual_inventory_policy_allows_sell": True,
        "residual_inventory": {
            "residual_qty": 0.0004998,
            "residual_notional_krw": 6497.4,
            "residual_classes": [
                "DEGRADED_RECOVERY_RESIDUAL",
                "LEDGER_SPLIT_RESIDUAL",
                "NEAR_LOT_RESIDUAL",
                "PORTFOLIO_ANCHOR_RESIDUAL",
                "TRUE_DUST",
            ],
            "exchange_sellable": True,
        },
        "projection_converged": True,
        "accounting_projection_ok": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "locked_qty": 0.0,
        "active_fee_accounting_blocker": False,
        "min_qty": 0.0001,
        "min_notional_krw": 5000.0,
        "idempotency_scope": "live_client_order_id_generator",
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.0004998,
            "balance_source_stale": False,
            "asset_locked": 0.0,
        },
    }

    candidate = build_residual_sell_candidate(context)
    proof = build_residual_sell_presubmit_proof(context)

    assert candidate is not None
    assert candidate.source == "residual_inventory"
    assert candidate.qty == pytest.approx(0.0004998)
    assert candidate.exchange_sellable is True
    assert candidate.allowed_by_policy is True
    assert context["sellable_executable_lot_count"] == 0
    assert proof.passed is True

    decision = build_execution_decision_summary(
        decision_context={
            **context,
            "raw_signal": "SELL",
            "final_signal": "HOLD",
            "has_dust_only_remainder": True,
            "exit_block_reason": "dust_only_remainder",
        },
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )

    assert decision.final_action == "CLOSE_RESIDUAL_CANDIDATE"
    assert decision.submit_expected is False
    assert decision.pre_submit_proof_status == "passed"
    assert decision.block_reason == "residual_live_sell_mode_telemetry"
    assert decision.strategy_sell_candidate is None
    assert decision.residual_sell_candidate is not None
    assert decision.residual_sell_candidate["qty"] == pytest.approx(0.0004998)
    assert decision.residual_submit_plan is not None
    assert _submit_plan_payload(decision.residual_submit_plan)["side"] == "SELL"
    assert _submit_plan_payload(decision.residual_submit_plan)["source"] == "residual_inventory"


def test_residual_sell_candidate_is_absent_for_unsellable_tracked_tiny_dust() -> None:
    context = {
        "signal": "SELL",
        "sellable_executable_lot_count": 0,
        "residual_inventory_mode": "track",
        "residual_inventory_state": "RESIDUAL_INVENTORY_TRACKED",
        "residual_inventory_policy_allows_sell": False,
        "residual_inventory": {
            "residual_qty": 0.00009998,
            "residual_notional_krw": 1299.74,
            "residual_classes": ["TRUE_DUST"],
            "exchange_sellable": False,
        },
        "projection_converged": True,
        "accounting_projection_ok": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "locked_qty": 0.0,
        "active_fee_accounting_blocker": False,
        "min_qty": 0.0001,
        "min_notional_krw": 5000.0,
        "idempotency_scope": "live_client_order_id_generator",
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.00009998,
            "balance_source_stale": False,
            "asset_locked": 0.0,
        },
    }

    candidate = build_residual_sell_candidate(context)
    proof = build_residual_sell_presubmit_proof(context)

    assert candidate is None
    assert proof.passed is False
    assert "missing_residual_sell_candidate" in proof.reasons

    decision = build_execution_decision_summary(
        decision_context={
            **context,
            "raw_signal": "SELL",
            "final_signal": "HOLD",
            "has_dust_only_remainder": True,
            "exit_block_reason": "dust_only_remainder",
        },
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )

    assert decision.final_action == "HOLD_TRACKED_DUST"
    assert decision.submit_expected is False
    assert decision.residual_sell_candidate is None
    assert decision.block_reason == "below_min_qty_or_min_notional"


def test_residual_sell_proof_fails_closed_for_submit_unknown() -> None:
    context = {
        "signal": "SELL",
        "residual_inventory_mode": "track",
        "residual_inventory_state": "RESIDUAL_INVENTORY_TRACKED",
        "residual_inventory_policy_allows_sell": True,
        "residual_inventory": {
            "residual_qty": 0.0004998,
            "residual_notional_krw": 57_816.0,
            "residual_classes": ["SELLABLE_RESIDUAL"],
            "exchange_sellable": True,
        },
        "projection_converged": True,
        "accounting_projection_ok": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 1,
        "locked_qty": 0.0,
        "active_fee_accounting_blocker": False,
        "min_qty": 0.0001,
        "min_notional_krw": 5000.0,
        "idempotency_scope": "live_client_order_id_generator",
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.0004998,
            "balance_source_stale": False,
            "asset_locked": 0.0,
        },
    }

    proof = build_residual_sell_presubmit_proof(context)
    decision = build_execution_decision_summary(
        decision_context={**context, "raw_signal": "SELL", "final_signal": "HOLD"},
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )

    assert proof.passed is False
    assert "submit_unknown_count_nonzero" in proof.reasons
    assert decision.final_action == "BLOCK_UNRESOLVED_RESIDUAL"
    assert decision.submit_expected is False
    assert decision.pre_submit_proof_status == "failed"
    assert decision.block_reason == "submit_unknown_count_nonzero"


class _ResidualFakeBroker:
    def __init__(self) -> None:
        self.orders: list[dict[str, object]] = []

    def place_order(self, *, client_order_id: str, side: str, qty: float, price: float | None = None, **_kwargs):
        self.orders.append({"client_order_id": client_order_id, "side": side, "qty": qty, "price": price})
        return BrokerOrder(
            client_order_id=client_order_id,
            exchange_order_id="ex-residual-1",
            side=side,
            status="open",
            price=price,
            qty_req=qty,
            qty_filled=0.0,
            created_ts=123,
            updated_ts=123,
            raw={},
        )


def _ec2_residual_context() -> dict[str, object]:
    return {
        "raw_signal": "SELL",
        "final_signal": "HOLD",
        "sellable_executable_lot_count": 0,
        "sellable_executable_qty": 0.0,
        "has_dust_only_remainder": True,
        "exit_block_reason": "dust_only_remainder",
        "residual_inventory_mode": "track",
        "residual_inventory_state": "RESIDUAL_INVENTORY_TRACKED",
        "residual_inventory_policy_allows_run": True,
        "residual_inventory_policy_allows_buy": True,
        "residual_inventory_policy_allows_sell": True,
        "residual_inventory": {
            "residual_qty": 0.0004998,
            "residual_notional_krw": 57_816.0,
            "residual_classes": ["SELLABLE_RESIDUAL"],
            "exchange_sellable": True,
        },
        "projection_converged": True,
        "projection_convergence": {"converged": True, "portfolio_qty": 0.0004998, "projected_total_qty": 0.0004998},
        "accounting_projection_ok": True,
        "open_order_count": 0,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "locked_qty": 0.0,
        "active_fee_accounting_blocker": False,
        "min_qty": 0.0001,
        "min_notional_krw": 5000.0,
        "idempotency_scope": "live_client_order_id_generator",
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.0004998,
            "balance_source_stale": False,
            "asset_locked": 0.0,
        },
        "total_effective_exposure_notional_krw": 57_816.0,
        "residual_inventory_notional_krw": 57_816.0,
    }


def test_residual_sell_policy_dry_run_builds_plan_without_broker_submit() -> None:
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "dry_run")
    decision = build_execution_decision_summary(
        decision_context=_ec2_residual_context(),
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert decision.pre_submit_proof_status == "passed"
    assert decision.submit_expected is False
    assert decision.block_reason == "residual_live_sell_mode_dry_run"
    assert decision.residual_submit_plan is not None
    assert _submit_plan_payload(decision.residual_submit_plan)["qty"] == pytest.approx(0.0004998)
    assert _submit_plan_payload(decision.residual_submit_plan)["would_submit_pipeline"] == "standard"
    assert _submit_plan_payload(decision.residual_submit_plan)["would_source"] == "residual_inventory"
    assert _submit_plan_payload(decision.residual_submit_plan)["would_authority"] == "residual_inventory_policy"
    assert _submit_plan_payload(decision.residual_submit_plan)["would_submit_side"] == "SELL"
    assert _submit_plan_payload(decision.residual_submit_plan)["would_submit_qty"] == pytest.approx(0.0004998)

    broker = _ResidualFakeBroker()
    service = LiveSignalExecutionService(broker=broker, executor=lambda *_a, **_k: None, harmless_dust_recorder=lambda **_k: False)
    service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_679_000.0,
            decision_context={"execution_decision": decision.as_dict()},
            execution_decision_summary=decision,
        )
    )
    assert broker.orders == []


def test_residual_sell_intent_key_is_stable_for_same_decision() -> None:
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "enabled")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    context = _ec2_residual_context() | {"candle_ts": 1_700_000_000_000}

    first = build_execution_decision_summary(
        decision_context=context,
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    second = build_execution_decision_summary(
        decision_context=context,
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )

    assert first.residual_submit_plan is not None
    assert second.residual_submit_plan is not None
    assert _submit_plan_payload(first.residual_submit_plan)["idempotency_key"] == _submit_plan_payload(
        second.residual_submit_plan
    )["idempotency_key"]
    assert _submit_plan_payload(first.residual_submit_plan)["intent_type"] == "residual_close"
    assert _submit_plan_payload(first.residual_submit_plan)["strategy_context"] == "residual_inventory_policy"


def test_residual_sell_policy_enabled_submits_residual_qty_without_strategy_lot_authority() -> None:
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "enabled")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    decision = build_execution_decision_summary(
        decision_context=_ec2_residual_context(),
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert decision.submit_expected is True
    assert decision.strategy_sell_candidate is None
    assert decision.residual_submit_plan is not None
    _persist_execution_plan_for_submit_plan(decision.residual_submit_plan)
    assert _submit_plan_payload(decision.residual_submit_plan)["authority"] == "residual_inventory_policy"

    broker = _ResidualFakeBroker()
    executor_calls: list[dict[str, object]] = []

    def _standard_pipeline_executor(*_args, **kwargs):
        executor_calls.append(dict(kwargs))
        return {"source": "residual_inventory", "authority": "residual_inventory_policy"}

    service = LiveSignalExecutionService(
        broker=broker,
        executor=_standard_pipeline_executor,
        harmless_dust_recorder=lambda **_k: False,
    )
    trade = service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_679_000.0,
            decision_context={"execution_decision": decision.as_dict()},
            execution_decision_summary=decision,
        )
    )
    assert trade is not None
    assert broker.orders == []
    assert len(executor_calls) == 1
    assert executor_calls[0]["execution_submit_plan"]["side"] == "SELL"
    assert executor_calls[0]["execution_submit_plan"]["source"] == "residual_inventory"
    assert executor_calls[0]["execution_submit_plan"]["authority"] == "residual_inventory_policy"
    assert executor_calls[0]["execution_submit_plan"]["qty"] == pytest.approx(0.0004998)
    assert trade["source"] == "residual_inventory"


def test_target_delta_live_service_uses_target_plan_without_residual_mode() -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "telemetry")
    decision = build_execution_decision_summary(
        decision_context={
            "raw_signal": "SELL",
            "market_price": 115_000_000.0,
            "sellable_executable_lot_count": 0,
            "exit_allowed": False,
            "exit_block_reason": "dust_only_remainder",
        },
        readiness_payload={
            "broker_position_evidence": {
                "broker_qty_known": True,
                "broker_qty": 0.0004998,
                "balance_source_stale": False,
            },
            "projection_converged": True,
            "projection_convergence": {"converged": True},
            "broker_portfolio_converged": True,
            "open_order_count": 0,
            "unresolved_open_order_count": 0,
            "recovery_required_count": 0,
            "submit_unknown_count": 0,
            "accounting_projection_ok": True,
            "active_fee_accounting_blocker": False,
            "residual_proof_min_qty": 0.0001,
            "residual_proof_min_notional_krw": 5000.0,
        },
        raw_signal="SELL",
        final_signal="HOLD",
        previous_target_exposure_krw=0.0,
    )
    assert decision.target_submit_plan is not None
    assert decision.residual_submit_plan is None

    executor_calls: list[dict[str, object]] = []

    def _standard_pipeline_executor(*_args, **kwargs):
        executor_calls.append(dict(kwargs))
        return {"source": "target_delta", "authority": "target_position_delta"}

    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=_standard_pipeline_executor,
        harmless_dust_recorder=lambda **_k: False,
    )
    execution_decision = decision.as_dict()
    target_submit_plan = execution_decision["target_submit_plan"]
    assert isinstance(target_submit_plan, dict)
    target_submit_plan.update(
        {
            "portfolio_target_authoritative": True,
            "portfolio_target_hash": "sha256:unit-portfolio-target",
            "allocation_decision_hash": "sha256:unit-allocation",
            "strategy_contribution_hash": "sha256:unit-contribution",
        }
    )
    trade = service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_000_000.0,
            decision_context={"execution_decision": execution_decision},
        )
    )

    assert trade == {"source": "target_delta", "authority": "target_position_delta"}
    assert len(executor_calls) == 1
    assert executor_calls[0]["execution_submit_plan"]["source"] == "target_delta"
    assert executor_calls[0]["execution_submit_plan"]["authority"] == "canonical_target_delta_sizing"
    assert executor_calls[0]["execution_submit_plan"]["side"] == "SELL"
    assert executor_calls[0]["execution_submit_plan"]["target_desired_qty"] == pytest.approx(0.0004998)
    assert executor_calls[0]["execution_submit_plan"]["qty"] == pytest.approx(0.0004)


@pytest.mark.parametrize(
    "target_plan",
    [
        None,
        {
            "source": "residual_inventory",
            "authority": "residual_inventory_policy",
            "side": "SELL",
            "qty": 0.0004998,
            "submit_expected": True,
            "block_reason": "none",
        },
        {
            "source": "target_delta",
            "authority": "target_position_delta",
            "side": "SELL",
            "qty": 0.0004998,
            "submit_expected": False,
            "block_reason": "none",
        },
        {
            "source": "target_delta",
            "authority": "target_position_delta",
            "side": "SELL",
            "qty": 0.0004998,
            "submit_expected": True,
            "block_reason": "delta_below_exchange_min",
        },
        {
            "source": "target_delta",
            "authority": "target_position_delta",
            "side": "NONE",
            "qty": 0.0004998,
            "submit_expected": True,
            "block_reason": "none",
        },
        {
            "source": "target_delta",
            "authority": "target_position_delta",
            "side": "SELL",
            "qty": 0.0,
            "submit_expected": True,
            "block_reason": "none",
        },
    ],
)
def test_target_delta_live_service_blocks_invalid_target_plan_without_fallback(target_plan) -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "enabled")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)

    executor_calls: list[dict[str, object]] = []
    recorder_calls: list[dict[str, object]] = []

    decision: dict[str, object] = {
        "residual_submit_plan": {
            "source": "residual_inventory",
            "authority": "residual_inventory_policy",
            "side": "SELL",
            "qty": 0.0004998,
            "submit_expected": True,
            "block_reason": "none",
        }
    }
    if target_plan is not None:
        decision["target_submit_plan"] = target_plan

    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=lambda *_args, **kwargs: executor_calls.append(dict(kwargs)) or {"status": "unexpected"},
        harmless_dust_recorder=lambda **kwargs: recorder_calls.append(dict(kwargs)) or True,
    )
    trade = service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_000_000.0,
            decision_context={
                "execution_decision": decision,
                "sellable_executable_lot_count": 0,
                "exit_allowed": False,
                "exit_block_reason": "dust_only_remainder",
                "raw_total_asset_qty": 0.0004998,
                "position_state": {
                    "normalized_exposure": {
                        "sellable_executable_lot_count": 0,
                        "sellable_executable_qty": 0.0,
                        "exit_allowed": False,
                        "exit_block_reason": "dust_only_remainder",
                        "raw_total_asset_qty": 0.0004998,
                    }
                },
            },
        )
    )

    assert trade is None
    assert executor_calls == []
    assert recorder_calls == []


def _valid_target_submit_plan() -> dict[str, object]:
    return {
        "side": "SELL",
        "source": "target_delta",
        "authority": "canonical_target_delta_sizing",
        "final_action": "REBALANCE_TO_TARGET",
        "qty": 0.0004,
        "notional_krw": 46_000.0,
        "target_exposure_krw": 0.0,
        "current_effective_exposure_krw": 57_500.0,
        "delta_krw": -46_000.0,
        "submit_expected": True,
        "pre_submit_proof_status": "passed",
        "block_reason": "none",
        "idempotency_key": "target-delta-test-key",
    }


def _valid_residual_submit_plan() -> dict[str, object]:
    return {
        "side": "SELL",
        "source": "residual_inventory",
        "authority": "residual_inventory_policy",
        "final_action": "CLOSE_RESIDUAL_CANDIDATE",
        "qty": 0.0004998,
        "notional_krw": 57_816.0,
        "target_exposure_krw": None,
        "current_effective_exposure_krw": None,
        "delta_krw": None,
        "submit_expected": True,
        "pre_submit_proof_status": "passed",
        "block_reason": "none",
        "idempotency_key": "residual-close-test-key",
    }


def _typed_plan(payload: dict[str, object]) -> ExecutionSubmitPlan:
    return ExecutionSubmitPlan(
        side=str(payload["side"]),
        source=str(payload["source"]),
        authority=str(payload["authority"]),
        final_action=str(payload["final_action"]),
        qty=payload["qty"],  # type: ignore[arg-type]
        notional_krw=payload["notional_krw"],  # type: ignore[arg-type]
        target_exposure_krw=payload["target_exposure_krw"],  # type: ignore[arg-type]
        current_effective_exposure_krw=payload["current_effective_exposure_krw"],  # type: ignore[arg-type]
        delta_krw=payload["delta_krw"],  # type: ignore[arg-type]
        submit_expected=bool(payload["submit_expected"]),
        pre_submit_proof_status=str(payload["pre_submit_proof_status"]),
        block_reason=str(payload["block_reason"]),
        idempotency_key=payload["idempotency_key"],  # type: ignore[arg-type]
    )


def _typed_target_execution_summary() -> ExecutionDecisionSummary:
    return ExecutionDecisionSummary(
        raw_signal="BUY",
        final_signal="BUY",
        final_action="REBALANCE_TO_TARGET",
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=0.0,
        current_effective_exposure_krw=57_500.0,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=None,
        residual_live_sell_mode="block",
        residual_buy_sizing_mode="block",
        residual_submit_plan=None,
        buy_submit_plan=None,
        target_shadow_decision=None,
        target_submit_plan=_typed_plan(_valid_target_submit_plan()),
    )


@pytest.mark.parametrize(
    ("case_id", "mutate", "expected_reason"),
    [
        (
            "missing_required_field",
            lambda plan: plan.pop("notional_krw"),
            "target_submit_plan_schema_missing_fields:notional_krw",
        ),
        (
            "missing_block_reason",
            lambda plan: plan.update({"block_reason": ""}),
            "target_submit_plan_schema_missing_block_reason",
        ),
        (
            "invalid_side",
            lambda plan: plan.update({"side": "CANCEL"}),
            "target_submit_plan_schema_invalid_side:CANCEL",
        ),
        (
            "submit_expected_failed_proof",
            lambda plan: plan.update({"pre_submit_proof_status": "failed"}),
            "target_submit_plan_schema_submit_expected_with_failed_proof",
        ),
    ],
)
def test_target_submit_plan_schema_failure_blocks_broker_submit_without_fallback(
    caplog,
    case_id,
    mutate,
    expected_reason,
) -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    target_plan = _valid_target_submit_plan()
    mutate(target_plan)
    executor_calls: list[dict[str, object]] = []

    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=lambda *_args, **kwargs: executor_calls.append(dict(kwargs)) or {"status": "unexpected"},
        harmless_dust_recorder=lambda **_k: False,
    )
    result = service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_000_000.0,
            decision_context={
                "execution_decision": {
                    "target_submit_plan": target_plan,
                    "residual_submit_plan": _valid_residual_submit_plan(),
                }
            },
        )
    )

    assert case_id
    assert result is None
    assert executor_calls == []
    assert expected_reason in caplog.text


@pytest.mark.parametrize(
    ("case_id", "mutate", "expected_reason"),
    [
        (
            "missing_required_field",
            lambda plan: plan.pop("current_effective_exposure_krw"),
            "residual_submit_plan_schema_missing_fields:current_effective_exposure_krw",
        ),
        (
            "missing_block_reason",
            lambda plan: plan.update({"block_reason": ""}),
            "residual_submit_plan_schema_missing_block_reason",
        ),
        (
            "invalid_side",
            lambda plan: plan.update({"side": "CANCEL"}),
            "residual_submit_plan_schema_invalid_side:CANCEL",
        ),
        (
            "submit_expected_failed_proof",
            lambda plan: plan.update({"pre_submit_proof_status": "failed"}),
            "residual_submit_plan_schema_submit_expected_with_failed_proof",
        ),
    ],
)
def test_residual_submit_plan_schema_failure_blocks_broker_submit_without_fallback(
    caplog,
    case_id,
    mutate,
    expected_reason,
) -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "lot_native")
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "enabled")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    residual_plan = _valid_residual_submit_plan()
    mutate(residual_plan)
    executor_calls: list[dict[str, object]] = []

    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=lambda *_args, **kwargs: executor_calls.append(dict(kwargs)) or {"status": "unexpected"},
        harmless_dust_recorder=lambda **_k: False,
    )
    result = service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_000_000.0,
            decision_context={"execution_decision": {"residual_submit_plan": residual_plan}},
        )
    )

    assert case_id
    assert result is None
    assert executor_calls == []
    assert expected_reason in caplog.text


def test_live_real_order_missing_submit_plan_blocks_legacy_signal_fallback(caplog) -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "lot_native")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    executor_calls: list[dict[str, object]] = []

    def _legacy_lot_native_executor(_broker, signal, ts, market_price, **kwargs):
        executor_calls.append(
            {
                "signal": signal,
                "ts": ts,
                "market_price": market_price,
                "execution_submit_plan": kwargs.get("execution_submit_plan"),
            }
        )
        return {"status": "submitted"}

    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=_legacy_lot_native_executor,
        harmless_dust_recorder=lambda **_k: False,
    )
    with pytest.raises(TypeError, match="decision_context_not_execution_authority"):
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=115_000_000.0,
            decision_context={"execution_decision": {}},
        )

    assert executor_calls == []


def test_live_real_order_rejects_raw_dict_submit_plan_even_when_summary_present(caplog) -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    raw_summary = object.__new__(ExecutionDecisionSummary)
    for key, value in {
        "raw_signal": "BUY",
        "final_signal": "BUY",
        "final_action": "REBALANCE_TO_TARGET",
        "submit_expected": True,
        "pre_submit_proof_status": "passed",
        "block_reason": "none",
        "strategy_sell_candidate": None,
        "residual_sell_candidate": None,
        "target_exposure_krw": 0.0,
        "current_effective_exposure_krw": 57_500.0,
        "tracked_residual_exposure_krw": None,
        "buy_delta_krw": None,
        "residual_live_sell_mode": "block",
        "residual_buy_sizing_mode": "block",
        "residual_submit_plan": None,
        "buy_submit_plan": None,
        "target_shadow_decision": None,
        "target_submit_plan": _valid_target_submit_plan(),
        "pre_trade_economics": None,
        "signal_flow": None,
    }.items():
        object.__setattr__(raw_summary, key, value)
    executor_calls: list[dict[str, object]] = []
    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=lambda *_args, **kwargs: executor_calls.append(dict(kwargs)) or {"status": "unexpected"},
        harmless_dust_recorder=lambda **_k: False,
    )

    result = service.execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=115_000_000.0,
            decision_context={},
            execution_decision_summary=raw_summary,
        )
    )

    assert result is None
    assert executor_calls == []
    assert "live_real_order_missing_typed_submit_plan:target_submit_plan" in caplog.text


def test_live_real_order_rejects_dict_only_submit_plan_even_with_valid_shape(caplog) -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    executor_calls: list[dict[str, object]] = []
    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=lambda *_args, **kwargs: executor_calls.append(dict(kwargs)) or {"status": "unexpected"},
        harmless_dust_recorder=lambda **_k: False,
    )

    with pytest.raises(TypeError, match="decision_context_not_execution_authority"):
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=115_000_000.0,
            decision_context={
                "execution_decision": {
                    "execution_engine": "target_delta",
                    "final_action": "REBALANCE_TO_TARGET",
                    "submit_expected": True,
                    "pre_submit_proof_status": "passed",
                    "block_reason": "none",
                    "target_submit_plan": _valid_target_submit_plan(),
                }
            },
        )

    assert executor_calls == []


def test_live_real_order_rejects_typed_summary_serialized_context_mismatch(caplog) -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    summary = _typed_target_execution_summary()
    serialized = summary.as_dict()
    assert isinstance(serialized["target_submit_plan"], dict)
    serialized["target_submit_plan"]["submit_expected"] = False  # type: ignore[index]
    executor_calls: list[dict[str, object]] = []
    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=lambda *_args, **kwargs: executor_calls.append(dict(kwargs)) or {"status": "unexpected"},
        harmless_dust_recorder=lambda **_k: False,
    )

    result = service.execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=115_000_000.0,
            decision_context={"execution_decision": serialized},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert executor_calls == []
    assert "execution_decision_summary_context_mismatch" in caplog.text


def test_paper_compatibility_can_still_consume_legacy_raw_target_plan() -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    executor_calls: list[dict[str, object]] = []
    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=lambda *_args, **kwargs: executor_calls.append(dict(kwargs)) or {"status": "submitted"},
        harmless_dust_recorder=lambda **_k: False,
    )

    result = service.execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=115_000_000.0,
            decision_context={"execution_decision": {"target_submit_plan": _valid_target_submit_plan()}},
        )
    )

    assert result == {"status": "submitted"}
    assert executor_calls[0]["execution_submit_plan"] == _valid_target_submit_plan()


def test_valid_unconsumed_explicit_submit_plan_does_not_fall_through_to_legacy_signal(
    caplog,
) -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "lot_native")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    executor_calls: list[dict[str, object]] = []
    summary = _typed_target_execution_summary()
    assert summary.target_submit_plan is not None
    _persist_execution_plan_for_submit_plan(summary.target_submit_plan)

    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=lambda *_args, **kwargs: executor_calls.append(dict(kwargs)) or {"status": "unexpected"},
        harmless_dust_recorder=lambda **_k: False,
    )
    result = service.execute(
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=115_000_000.0,
            decision_context={},
            execution_decision_summary=summary,
        )
    )

    assert result is None
    assert executor_calls == []
    assert "explicit_submit_plan_not_consumed" in caplog.text


def test_typed_submit_expectation_ignores_mutated_serialized_execution_context() -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    summary = _typed_target_execution_summary()
    context = engine_module.prepare_strategy_decision_persistence_context(
        decision_context={"strategy": "sma_with_filter"},
        execution_decision_summary=summary,
        readiness_payload={},
    )
    assert isinstance(context["execution_decision"], dict)
    assert isinstance(context["execution_decision"]["target_submit_plan"], dict)  # type: ignore[index]
    context["execution_decision"]["target_submit_plan"]["submit_expected"] = False  # type: ignore[index]

    expectation = engine_module.resolve_typed_execution_submit_expectation(summary)

    assert expectation.submit_expected is True


@pytest.mark.parametrize(
    ("decision_context", "expected_reason"),
    [
        ("not-a-context", "live_real_order_missing_typed_execution_summary"),
        ({"execution_decision": "not-a-decision"}, "decision_context_not_execution_authority"),
        (
            {"execution_decision": {"target_submit_plan": "not-a-plan"}},
            "decision_context_not_execution_authority",
        ),
        (
            {"execution_decision": {"residual_submit_plan": "not-a-plan"}},
            "decision_context_not_execution_authority",
        ),
    ],
)
def test_malformed_execution_context_blocks_legacy_signal_fallback(
    caplog,
    decision_context,
    expected_reason,
) -> None:
    object.__setattr__(settings, "EXECUTION_ENGINE", "lot_native")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    executor_calls: list[dict[str, object]] = []

    service = LiveSignalExecutionService(
        broker=_ResidualFakeBroker(),
        executor=lambda *_args, **kwargs: executor_calls.append(dict(kwargs)) or {"status": "unexpected"},
        harmless_dust_recorder=lambda **_k: False,
    )
    with pytest.raises(TypeError, match=expected_reason):
        SignalExecutionRequest(
            signal="BUY",
            ts=123,
            market_price=115_000_000.0,
            decision_context=decision_context,
        )

    assert executor_calls == []


def _target_state(*, exposure_krw: float) -> TargetPositionState:
    return TargetPositionState(
        pair=settings.PAIR,
        target_exposure_krw=float(exposure_krw),
        target_qty=0.0 if exposure_krw <= 0.0 else exposure_krw / 100_000_000.0,
        last_signal="SELL" if exposure_krw <= 0.0 else "BUY",
        last_decision_id=7,
        last_reference_price=100_000_000.0,
        updated_ts=1000,
    )


def _target_delta_readiness(
    *,
    broker_qty: float,
    open_order_count: int = 0,
) -> dict[str, object]:
    return {
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": broker_qty,
            "balance_source_stale": False,
        },
        "projection_converged": True,
        "projection_convergence": {"converged": True},
        "broker_portfolio_converged": True,
        "open_order_count": open_order_count,
        "unresolved_open_order_count": 0,
        "recovery_required_count": 0,
        "submit_unknown_count": 0,
        "accounting_projection_ok": True,
        "active_fee_accounting_blocker": False,
        "residual_proof_min_qty": 0.0001,
        "residual_proof_min_notional_krw": 5000.0,
    }


@pytest.mark.parametrize(
    (
        "case_id",
        "persisted_target",
        "broker_qty",
        "open_order_count",
        "expected_forwarded_side",
        "expected_qty",
        "expected_block_reason",
        "expected_dust_classification",
    ),
    [
        (
            "restart_target_zero_executable_leftover",
            _target_state(exposure_krw=0.0),
            0.0004998,
            0,
            "SELL",
            0.0004,
            "none",
            "executable_delta",
        ),
        (
            "restart_target_zero_true_dust",
            _target_state(exposure_krw=0.0),
            0.00000004,
            0,
            None,
            0.0,
            "delta_below_exchange_min",
            "true_dust",
        ),
        (
            "restart_hold_buys_only_missing_delta",
            _target_state(exposure_krw=100_000.0),
            0.0004,
            0,
            "BUY",
            0.0006,
            "none",
            "executable_delta",
        ),
        (
            "adopted_target_hold_maintains_position",
            TargetPositionState(
                pair=settings.PAIR,
                target_exposure_krw=49_980.0,
                target_qty=0.0004998,
                last_signal="HOLD",
                last_decision_id=7,
                last_reference_price=100_000_000.0,
                updated_ts=1000,
                target_origin="adopted_existing_position",
                adoption_reason="safe_converged_executable_broker_position",
                adopted_broker_qty=0.0004998,
                adopted_broker_exposure_krw=49_980.0,
                created_from_signal="HOLD",
            ),
            0.0004998,
            0,
            None,
            0.0,
            "delta_below_exchange_min",
            "true_dust",
        ),
        (
            "hold_without_persisted_target_adopts_executable_broker_position",
            None,
            0.0004998,
            0,
            None,
            0.0,
            "delta_below_exchange_min",
            "true_dust",
        ),
        (
            "hold_without_persisted_target_initializes_flat",
            None,
            0.0,
            0,
            None,
            0.0,
            "delta_below_exchange_min",
            "true_dust",
        ),
        (
            "unsafe_readiness_blocks_target_delta_submit",
            _target_state(exposure_krw=0.0),
            0.0004998,
            1,
            None,
            0.0,
            "open_order_count_nonzero",
            "unknown",
        ),
    ],
)
def test_run_loop_target_delta_persisted_target_state_reaches_live_execution(
    monkeypatch,
    case_id,
    persisted_target,
    broker_qty,
    open_order_count,
    expected_forwarded_side,
    expected_qty,
    expected_block_reason,
    expected_dust_classification,
) -> None:
    loop_conn = _prepare_run_loop(
        monkeypatch,
        asset_qty=broker_qty,
        target_state=persisted_target,
    )
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "telemetry")
    object.__setattr__(settings, "TARGET_EXPOSURE_KRW", None)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", False)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.evaluate_startup_safety_gate", lambda: None)
    _install_runtime_gateway(
        monkeypatch,
        lambda _conn, **_kwargs: _runtime_result_from_payload(
            {
                "ts": 9000,
                "last_close": 100_000_000.0,
                "curr_s": 1.0,
                "curr_l": 1.0,
                "signal": "HOLD",
                "raw_signal": "HOLD",
                "reason": case_id,
            }
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.runtime_service_factories.compute_runtime_readiness_snapshot",
        lambda _conn: SimpleNamespace(
            as_dict=lambda: _target_delta_readiness(
                broker_qty=broker_qty,
                open_order_count=open_order_count,
            )
        ),
    )

    recorded_contexts: list[dict[str, object]] = []
    executor_calls: list[dict[str, object]] = []

    def _record_strategy_decision(_conn, **kwargs):
        recorded_contexts.append(dict(kwargs["context"]))
        return 42

    def _capture_live_execution(_broker, side, ts, market_price, **kwargs):
        executor_calls.append(
            {
                "side": side,
                "ts": ts,
                "market_price": market_price,
                "execution_submit_plan": kwargs.get("execution_submit_plan"),
            }
        )
        return None

    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.record_strategy_decision", _record_strategy_decision)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", _capture_live_execution)

    run_loop()

    assert len(recorded_contexts) == 1
    context = recorded_contexts[0]
    execution_decision = context["execution_decision"]
    assert isinstance(execution_decision, dict)
    target_plan = execution_decision["target_submit_plan"]
    assert isinstance(target_plan, dict)
    assert target_plan["block_reason"] == expected_block_reason
    assert target_plan["target_dust_classification"] == expected_dust_classification
    if case_id == "hold_without_persisted_target_adopts_executable_broker_position":
        assert target_plan["target_policy_action"] == "adopt_existing_broker_position"
        assert target_plan["target_origin"] == "adopted_existing_position"
        assert target_plan["target_delta_side"] == "NONE"
        assert context["target_adopted_broker_qty"] == pytest.approx(0.0004998)
        assert loop_conn.target_state is not None
        assert loop_conn.target_state.target_origin == "adopted_existing_position"
        assert loop_conn.target_state.target_qty == pytest.approx(0.0004998)
    if case_id == "hold_without_persisted_target_initializes_flat":
        assert target_plan["target_policy_action"] == "initialize_flat_target"
        assert loop_conn.target_state is not None
        assert loop_conn.target_state.target_origin == "flat_start"

    if expected_forwarded_side is None:
        assert executor_calls == []
        assert target_plan["submit_expected"] is False
    else:
        assert len(executor_calls) == 1
        forwarded = executor_calls[0]["execution_submit_plan"]
        assert isinstance(forwarded, dict)
        assert executor_calls[0]["side"] == expected_forwarded_side
        assert forwarded["source"] == "target_delta"
        assert forwarded["authority"] == "canonical_target_delta_sizing"
        assert forwarded["side"] == expected_forwarded_side
        assert forwarded["qty"] == pytest.approx(expected_qty)
        assert forwarded["invariant_status"] == "passed"
        assert forwarded["submit_expected"] is True
        assert forwarded["block_reason"] == "none"

    if expected_block_reason == "none":
        assert loop_conn.target_state is not None
        assert loop_conn.target_state.last_signal == "HOLD"


def test_run_loop_target_delta_missing_target_holding_btc_adopts_without_closeout(monkeypatch) -> None:
    loop_conn = _prepare_run_loop(
        monkeypatch,
        asset_qty=0.0004998,
        target_state=None,
    )
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "telemetry")
    object.__setattr__(settings, "TARGET_EXPOSURE_KRW", None)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.evaluate_startup_safety_gate", lambda: None)
    _install_runtime_gateway(
        monkeypatch,
        lambda _conn, **_kwargs: _runtime_result_from_payload(
            {
                "ts": 9000,
                "last_close": 114_120_000.0,
                "curr_s": 1.0,
                "curr_l": 1.0,
                "signal": "HOLD",
                "raw_signal": "HOLD",
                "reason": "regression missing target must not close broker btc",
            }
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.runtime_service_factories.compute_runtime_readiness_snapshot",
        lambda _conn: SimpleNamespace(as_dict=lambda: _target_delta_readiness(broker_qty=0.0004998)),
    )

    recorded_contexts: list[dict[str, object]] = []
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.record_strategy_decision",
        lambda _conn, **kwargs: recorded_contexts.append(dict(kwargs["context"])) or 42,
    )
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.live_execute_signal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("startup adoption must not submit")),
    )

    run_loop()

    assert len(recorded_contexts) == 1
    context = recorded_contexts[0]
    execution_decision = context["execution_decision"]
    assert isinstance(execution_decision, dict)
    target_plan = execution_decision["target_submit_plan"]
    assert isinstance(target_plan, dict)
    assert target_plan["target_policy_action"] == "adopt_existing_broker_position"
    assert target_plan["target_origin"] == "adopted_existing_position"
    assert target_plan["target_delta_side"] == "NONE"
    assert target_plan["side"] != "SELL"
    assert target_plan["submit_expected"] is False
    assert execution_decision["final_action"] != "REBALANCE_TO_TARGET"
    assert loop_conn.target_state is not None
    assert loop_conn.target_state.target_origin == "adopted_existing_position"
    assert loop_conn.target_state.target_qty == pytest.approx(0.0004998)
    assert loop_conn.target_state.target_exposure_krw == pytest.approx(0.0004998 * 114_120_000.0)


def test_run_loop_target_delta_adopted_target_strategy_sell_submits_delta_sell(monkeypatch) -> None:
    loop_conn = _prepare_run_loop(
        monkeypatch,
        asset_qty=0.0004998,
        target_state=TargetPositionState(
            pair=settings.PAIR,
            target_exposure_krw=49_980.0,
            target_qty=0.0004998,
            last_signal="HOLD",
            last_decision_id=7,
            last_reference_price=100_000_000.0,
            updated_ts=1000,
            target_origin="adopted_existing_position",
            adoption_reason="safe_converged_executable_broker_position",
            adopted_broker_qty=0.0004998,
            adopted_broker_exposure_krw=49_980.0,
            created_from_signal="HOLD",
        ),
    )
    object.__setattr__(settings, "EXECUTION_ENGINE", "target_delta")
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "telemetry")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", False)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.evaluate_startup_safety_gate", lambda: None)
    _install_runtime_gateway(
        monkeypatch,
        lambda _conn, **_kwargs: _runtime_result_from_payload(
            {
                "ts": 9000,
                "last_close": 100_000_000.0,
                "curr_s": 0.5,
                "curr_l": 1.0,
                "signal": "SELL",
                "raw_signal": "SELL",
                "reason": "strategy sell after adoption",
            }
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.runtime_service_factories.compute_runtime_readiness_snapshot",
        lambda _conn: SimpleNamespace(as_dict=lambda: _target_delta_readiness(broker_qty=0.0004998)),
    )
    executor_calls: list[dict[str, object]] = []
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.record_strategy_decision", lambda _conn, **_kwargs: 42)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.live_execute_signal",
        lambda _broker, side, ts, market_price, **kwargs: executor_calls.append(
            {"side": side, "execution_submit_plan": kwargs.get("execution_submit_plan")}
        ),
    )
    monkeypatch.setattr(
        "bithumb_bot.runtime.runner.live_execute_signal",
        lambda _broker, side, ts, market_price, **kwargs: executor_calls.append(
            {"side": side, "execution_submit_plan": kwargs.get("execution_submit_plan")}
        ),
    )

    run_loop()

    assert executor_calls == []
    assert loop_conn.target_state is not None
    assert loop_conn.target_state.target_origin == "adopted_existing_position"
    assert loop_conn.target_state.target_exposure_krw == pytest.approx(49_980.0)
    assert loop_conn.target_state.last_signal == "HOLD"


def test_default_live_service_wrapper_preserves_residual_execution_submit_plan(monkeypatch) -> None:
    from bithumb_bot.broker import live as live_module

    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "enabled")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    decision = build_execution_decision_summary(
        decision_context=_ec2_residual_context(),
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert decision.residual_submit_plan is not None
    _persist_execution_plan_for_submit_plan(decision.residual_submit_plan)
    captured: dict[str, object] = {}

    def _capture_live_execute_signal(*_args, **kwargs):
        captured.update(kwargs)
        return {"status": "captured"}

    monkeypatch.setattr(live_module, "live_execute_signal", _capture_live_execute_signal)

    broker = _ResidualFakeBroker()
    service = build_signal_execution_service(mode="live", broker=broker)
    assert service is not None
    result = service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_679_000.0,
            decision_context={"execution_decision": decision.as_dict()},
            execution_decision_summary=decision,
        )
    )

    assert result == {"status": "captured"}
    execution_submit_plan = captured["execution_submit_plan"]
    assert isinstance(execution_submit_plan, dict)
    assert execution_submit_plan["source"] == "residual_inventory"
    assert execution_submit_plan["authority"] == "residual_inventory_policy"
    assert execution_submit_plan["side"] == "SELL"
    assert execution_submit_plan["qty"] == pytest.approx(0.0004998)


def test_residual_enabled_executor_typeerror_fails_closed_without_retry() -> None:
    object.__setattr__(settings, "RESIDUAL_LIVE_SELL_MODE", "enabled")
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    decision = build_execution_decision_summary(
        decision_context=_ec2_residual_context(),
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert decision.residual_submit_plan is not None
    _persist_execution_plan_for_submit_plan(decision.residual_submit_plan)
    calls: list[dict[str, object]] = []

    def _legacy_executor(*_args, **kwargs):
        calls.append(dict(kwargs))
        if "execution_submit_plan" in kwargs:
            raise TypeError("unexpected keyword argument 'execution_submit_plan'")
        raise AssertionError("residual submit plan was silently dropped")

    broker = _ResidualFakeBroker()
    service = LiveSignalExecutionService(
        broker=broker,
        executor=_legacy_executor,
        harmless_dust_recorder=lambda **_k: False,
    )
    result = service.execute(
        SignalExecutionRequest(
            signal="SELL",
            ts=123,
            market_price=115_679_000.0,
            decision_context={"execution_decision": decision.as_dict()},
            execution_decision_summary=decision,
        )
    )

    assert len(calls) == 1
    assert calls[0]["execution_submit_plan"]["source"] == "residual_inventory"
    assert result is not None
    assert result["status"] == "blocked"
    assert result["reason"] == "executor_missing_execution_submit_plan_support"
    assert broker.orders == []


def test_residual_sell_proof_missing_accounting_projection_fails_closed() -> None:
    context = _ec2_residual_context()
    context.pop("accounting_projection_ok")
    context["projection_converged"] = True
    proof = build_residual_sell_presubmit_proof(context)
    assert proof.passed is False
    assert "missing_accounting_projection_ok" in proof.reasons


def test_residual_sell_proof_accounting_projection_false_fails_closed() -> None:
    context = _ec2_residual_context() | {
        "projection_converged": True,
        "accounting_projection_ok": False,
    }
    proof = build_residual_sell_presubmit_proof(context)
    assert proof.passed is False
    assert "accounting_projection_not_ok" in proof.reasons


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ({"broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0004998, "balance_source_stale": True, "asset_locked": 0.0}}, "broker_evidence_stale"),
        ({"broker_position_evidence": {"broker_qty_known": False, "broker_qty": 0.0004998, "balance_source_stale": False, "asset_locked": 0.0}}, "broker_qty_unknown"),
        ({"broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0001, "balance_source_stale": False, "asset_locked": 0.0}}, "broker_qty_below_candidate_qty"),
        ({"locked_qty": 0.0001}, "locked_qty_nonzero"),
        ({"open_order_count": 1}, "open_order_count_nonzero"),
        ({"unresolved_open_order_count": 1}, "unresolved_open_order_count_nonzero"),
        ({"recovery_required_count": 1}, "recovery_required_count_nonzero"),
        ({"submit_unknown_count": 1}, "submit_unknown_count_nonzero"),
        ({"accounting_projection_ok": False}, "accounting_projection_not_ok"),
        ({"min_qty": 0.001}, "qty_below_min_qty"),
        ({"min_notional_krw": 100_000.0}, "notional_below_min_notional"),
        ({"active_fee_accounting_blocker": True}, "active_fee_accounting_blocker"),
        ({"residual_sell_candidate": {"qty": 0.0004998, "notional": 57_816.0, "source": "residual_inventory", "classes": ["SELLABLE_RESIDUAL"], "exchange_sellable": True, "allowed_by_policy": False, "requires_final_pre_submit_proof": True}}, "candidate_policy_blocked"),
    ],
)
def test_residual_sell_proof_failure_reasons_are_explicit(mutation: dict[str, object], reason: str) -> None:
    context = _ec2_residual_context()
    context.update(mutation)
    proof = build_residual_sell_presubmit_proof(context)
    decision = build_execution_decision_summary(
        decision_context=context,
        raw_signal="SELL",
        final_signal="HOLD",
        final_reason="dust_only_remainder",
    )
    assert proof.passed is False
    assert reason in proof.reasons
    assert decision.final_action == "BLOCK_UNRESOLVED_RESIDUAL"
    assert decision.submit_expected is False
    assert decision.pre_submit_proof_status == "failed"
    assert decision.block_reason == reason


def test_residual_buy_sizing_modes_telemetry_and_delta() -> None:
    context = _ec2_residual_context() | {
        "raw_signal": "BUY",
        "final_signal": "BUY",
        "market_price": 115_679_000.0,
        "total_effective_exposure_notional_krw": 57_816.0,
        "residual_inventory_notional_krw": 57_816.0,
    }
    object.__setattr__(settings, "MAX_ORDER_KRW", 100_000.0)
    object.__setattr__(settings, "RESIDUAL_BUY_SIZING_MODE", "telemetry")
    telemetry = build_execution_decision_summary(decision_context=context, raw_signal="BUY", final_signal="BUY")
    assert telemetry.buy_delta_krw == pytest.approx(42_184.0)
    assert telemetry.submit_expected is True
    assert telemetry.buy_submit_plan is not None
    assert _submit_plan_payload(telemetry.buy_submit_plan)["notional_krw"] == pytest.approx(100_000.0)
    assert telemetry.block_reason == "residual_buy_sizing_mode_telemetry"

    object.__setattr__(settings, "RESIDUAL_BUY_SIZING_MODE", "delta")
    delta = build_execution_decision_summary(decision_context=context, raw_signal="BUY", final_signal="BUY")
    assert delta.buy_submit_plan is not None
    assert _submit_plan_payload(delta.buy_submit_plan)["notional_krw"] == pytest.approx(42_184.0)
    assert delta.submit_expected is True
    assert delta.block_reason == "none"

    covered = build_execution_decision_summary(
        decision_context=context | {"total_effective_exposure_notional_krw": 120_000.0},
        raw_signal="BUY",
        final_signal="BUY",
    )
    assert covered.final_action == "HOLD_TARGET_ALREADY_COVERED"
    assert covered.submit_expected is False
    assert covered.block_reason == "tracked_residual_exposure_covers_target"

    below_min = build_execution_decision_summary(
        decision_context=context | {"total_effective_exposure_notional_krw": 96_000.0},
        raw_signal="BUY",
        final_signal="BUY",
    )
    assert below_min.final_action == "BLOCK_ORDER_RULE"
    assert below_min.submit_expected is False
    assert below_min.block_reason == "buy_delta_below_min_notional"


def test_run_loop_kill_switch_halts_with_risk_open_reason_and_cancel_attempt(monkeypatch):
    _prepare_run_loop(monkeypatch)
    object.__setattr__(settings, "KILL_SWITCH", True)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)

    cancel_calls = {"n": 0}

    def _cancel(_broker, trigger: str):
        cancel_calls["n"] += 1
        assert trigger == "kill-switch"
        return True

    monkeypatch.setattr("bithumb_bot.compat.engine_legacy._attempt_open_order_cancellation", _cancel)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy._get_exposure_snapshot", lambda _now_ms: (False, True))

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.notify", lambda msg: notifications.append(msg))

    run_loop()

    assert cancel_calls["n"] == 1
    halted = [n for n in notifications if "event=trading_halted" in n and "reason_code=KILL_SWITCH" in n]
    assert halted
    assert any("operator_compact_summary=halt_reason=KILL_SWITCH" in n for n in halted)
    assert any("open_order_count=" in n for n in halted)
    assert any("position_summary=" in n for n in halted)
    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_operator_action_required is True
    assert state.halt_state_unresolved is True
    assert state.last_disable_reason is not None
    assert "risk_open_exposure_remains" in state.last_disable_reason


def test_run_loop_kill_switch_liquidate_with_open_position_triggers_flatten(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.05)
    object.__setattr__(settings, "KILL_SWITCH", True)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy._get_exposure_snapshot", lambda _now_ms: (False, True))

    run_loop()

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_policy_auto_liquidate_positions is True
    assert state.last_flatten_position_status == "dry_run"
    assert state.last_flatten_position_summary is not None
    assert '"trigger": "kill-switch"' in state.last_flatten_position_summary
    assert "flatten_status=dry_run" in str(state.last_disable_reason)


def test_run_loop_kill_switch_liquidate_with_no_position_enters_safe_halt(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.0)
    object.__setattr__(settings, "KILL_SWITCH", True)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop()

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_new_orders_blocked is True
    assert state.halt_state_unresolved is False
    assert state.last_flatten_position_status == "no_position"
    assert "flatten_status=no_position" in str(state.last_disable_reason)


def test_run_loop_kill_switch_liquidate_flatten_failure_is_persisted(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.02)
    object.__setattr__(settings, "KILL_SWITCH", True)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy._get_exposure_snapshot", lambda _now_ms: (False, True))
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.BithumbBroker", lambda: _FlattenFailBroker())
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    run_loop()

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "KILL_SWITCH"
    assert state.halt_policy_auto_liquidate_positions is True
    assert state.halt_state_unresolved is True
    assert state.last_flatten_position_status == "failed"
    assert state.last_flatten_position_summary is not None
    assert "place_order boom" in state.last_flatten_position_summary
    assert "flatten_status=failed" in str(state.last_disable_reason)

def test_run_loop_daily_loss_breach_halts_persistently(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (True, "daily loss limit exceeded (60,000/50,000 KRW)"),
    )

    called = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.live_execute_signal",
        lambda *_args, **_kwargs: called.__setitem__("n", called["n"] + 1),
    )

    run_loop()

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.last_disable_reason is not None
    assert "daily loss limit exceeded" in state.last_disable_reason
    assert state.halt_reason_code == "DAILY_LOSS_LIMIT"
    assert state.halt_new_orders_blocked is True
    assert called["n"] == 0


def test_run_loop_daily_loss_breach_attempts_open_order_cancel(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.02)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (True, "daily loss limit exceeded (60,000/50,000 KRW)"),
    )

    cancel_calls = {"n": 0}
    flatten_calls = {"n": 0}

    def _cancel(_broker, trigger: str):
        cancel_calls["n"] += 1
        assert trigger == "daily-loss-halt"
        return True

    monkeypatch.setattr("bithumb_bot.compat.engine_legacy._attempt_open_order_cancellation", _cancel)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.flatten_btc_position",
        lambda *_args, **_kwargs: (
            flatten_calls.__setitem__("n", flatten_calls["n"] + 1)
            or {"status": "dry_run", "qty": 0.02}
        ),
    )
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop()

    assert cancel_calls["n"] == 1
    assert flatten_calls["n"] == 1


def test_run_loop_daily_loss_breach_has_no_auto_resume(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (True, "daily loss limit exceeded (60,000/50,000 KRW)"),
    )

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.notify", lambda msg: notifications.append(msg))
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop()

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert all("attempting auto-resume" not in n for n in notifications)
    halted = [n for n in notifications if "event=trading_halted" in n and "reason_code=DAILY_LOSS_LIMIT" in n]
    assert halted
    assert any("symbol=" in n for n in halted)
    assert any(
        "operator_next_action=review risk breach details, verify exposure, then run recovery-report" in n
        for n in halted
    )


def test_run_loop_stale_open_order_emits_recovery_and_cancel_failure_alerts(monkeypatch):
    _prepare_run_loop(monkeypatch, open_order_created_ts=0)
    object.__setattr__(settings, "MAX_OPEN_ORDER_AGE_SEC", 5)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy._attempt_open_order_cancellation", lambda *_args, **_kwargs: False)

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.notify", lambda msg: notifications.append(msg))

    run_loop()

    marked = [n for n in notifications if "event=recovery_required_marked" in n and "reason_code=STALE_OPEN_ORDER" in n]
    assert marked
    assert any("symbol=" in n for n in marked)
    assert any("latest_client_order_id=" in n for n in marked)
    assert any(
        "operator_hint_command=uv run python bot.py reconcile && uv run python bot.py recovery-report" in n
        for n in marked
    )
    assert any("operator_compact_summary=halt_reason=STALE_OPEN_ORDER" in n for n in marked)
    assert any(
        "operator_recommended_commands=uv run python bot.py reconcile | uv run python bot.py recover-order --client-order-id <id>"
        in n
        for n in marked
    )
    assert any("event=trading_halted" in n and "reason_code=STALE_OPEN_ORDER" in n for n in notifications)


def test_attempt_open_order_cancellation_failure_emits_reason_code(monkeypatch):
    err = RuntimeError("boom")
    monkeypatch.setattr(
        "bithumb_bot.recovery.cancel_open_orders_with_broker",
        lambda _broker: (_ for _ in ()).throw(err),
        raising=False,
    )

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.notifier.notify", lambda msg: notifications.append(msg))

    from bithumb_bot.runtime_compat import _attempt_open_order_cancellation

    ok = _attempt_open_order_cancellation(object(), trigger="kill-switch")

    assert ok is False
    assert any("event=panic_cleanup" in n for n in notifications)
    assert any(
        "reason_code=CANCEL_FAILURE" in n and "cancel_detail_code=CANCEL_OPEN_ORDERS_ERROR" in n
        for n in notifications
    )


class _CleanupRevalidateBroker:
    def __init__(self, *, open_orders_seq, position_seq):
        self._open_orders_seq = list(open_orders_seq)
        self._position_seq = list(position_seq)
        self.open_order_calls = 0
        self.balance_calls = 0

    def get_open_orders(
        self,
        *,
        exchange_order_ids: list[str] | tuple[str, ...] | None = None,
        client_order_ids: list[str] | tuple[str, ...] | None = None,
    ):
        self.open_order_calls += 1
        idx = min(self.open_order_calls - 1, len(self._open_orders_seq) - 1)
        open_present = bool(self._open_orders_seq[idx])
        return [object()] if open_present else []

    def get_balance(self):
        self.balance_calls += 1
        idx = min(self.balance_calls - 1, len(self._position_seq) - 1)
        position_present = bool(self._position_seq[idx])
        return BrokerBalance(
            cash_available=100_000.0,
            cash_locked=0.0,
            asset_available=0.01 if position_present else 0.0,
            asset_locked=0.0,
        )


def test_cleanup_revalidation_recovers_safe_state_after_initial_uncertainty(monkeypatch):
    from bithumb_bot.runtime_compat import _revalidate_cleanup_state_after_failure

    broker = _CleanupRevalidateBroker(open_orders_seq=[True, False], position_seq=[True, False])

    reconcile_calls = {"n": 0}

    def _reconcile(_broker):
        reconcile_calls["n"] += 1

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", _reconcile, raising=False)

    safe, detail = _revalidate_cleanup_state_after_failure(
        broker,
        trigger="unit-test",
        max_attempts=2,
    )

    assert safe is True
    assert "attempts=2/2" in detail
    assert reconcile_calls["n"] == 2


def test_cleanup_revalidation_ambiguous_state_remains_halted(monkeypatch):
    from bithumb_bot.runtime_compat import _revalidate_cleanup_state_after_failure

    class _AmbiguousBroker:
        def get_open_orders(
            self,
            *,
            exchange_order_ids: list[str] | tuple[str, ...] | None = None,
            client_order_ids: list[str] | tuple[str, ...] | None = None,
        ):
            raise RuntimeError("open orders unavailable")

        def get_balance(self):
            raise RuntimeError("balance unavailable")

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)

    conn = ensure_db()
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('reval_ambiguous_1','ex-reval-1','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    safe, detail = _revalidate_cleanup_state_after_failure(
        _AmbiguousBroker(),
        trigger="unit-test",
        max_attempts=2,
    )

    assert safe is False
    assert "open_orders_present=unknown" in detail
    assert "position_present=unknown" in detail
    assert "errors=" in detail


def test_cleanup_revalidation_is_bounded_by_max_attempts(monkeypatch):
    from bithumb_bot.runtime_compat import _revalidate_cleanup_state_after_failure

    broker = _CleanupRevalidateBroker(open_orders_seq=[True], position_seq=[True])
    conn = ensure_db()
    conn.execute(
        """
        INSERT INTO orders(client_order_id, exchange_order_id, status, side, price, qty_req, qty_filled, created_ts, updated_ts, last_error)
        VALUES ('reval_bounded_1','ex-reval-2','NEW','BUY',100.0,0.1,0,1000,1000,NULL)
        """
    )
    conn.commit()
    conn.close()

    reconcile_calls = {"n": 0}

    def _reconcile(_broker):
        reconcile_calls["n"] += 1

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", _reconcile, raising=False)

    safe, detail = _revalidate_cleanup_state_after_failure(
        broker,
        trigger="unit-test",
        max_attempts=2,
    )

    assert safe is False
    assert "attempts=2/2" in detail
    assert reconcile_calls["n"] == 2
    assert broker.open_order_calls == 2
    assert broker.balance_calls == 2


class _DummyClient:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, path, params=None):
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _response(status_code: int) -> httpx.Response:
    req = httpx.Request("GET", "https://api.bithumb.com/v1/test")
    return httpx.Response(status_code=status_code, request=req, json={"ok": True})


def test_get_with_retry_retries_on_429(monkeypatch):
    import bithumb_bot.marketdata as marketdata_module

    sleeps: list[float] = []
    monkeypatch.setattr(marketdata_module.time, "sleep", lambda sec: sleeps.append(sec))
    monkeypatch.setattr("bithumb_bot.marketdata.random.uniform", lambda a, b: 0.0)

    client = _DummyClient([_response(429), _response(503), _response(200)])
    result = _get_with_retry(client, "/v1/test")

    assert result.status_code == 200
    assert len(sleeps) == 2


def test_health_status_contains_runtime_flags():
    runtime_state.set_error_count(3)
    runtime_state.set_last_candle_observation(
        status="ok",
        age_sec=12.5,
        sync_epoch_sec=1700000000.0,
        candle_ts_ms=1700000000000,
    )
    runtime_state.disable_trading_until(999.0)

    health = get_health_status()

    assert health["error_count"] == 3
    assert health["last_candle_age_sec"] == 12.5
    assert health["last_candle_status"] == "ok"
    assert health["last_candle_sync_epoch_sec"] == 1700000000.0
    assert health["last_candle_ts_ms"] == 1700000000000
    assert health["last_candle_status_detail"] is None
    assert health["trading_enabled"] is False
    assert health["retry_at_epoch_sec"] == 999.0
    assert health["last_disable_reason"] is None
    assert health["halt_new_orders_blocked"] is False
    assert health["halt_reason_code"] is None
    assert health["halt_state_unresolved"] is False
    assert int(health["unresolved_open_order_count"]) >= 0
    assert int(health["recovery_required_count"]) >= 0
    if int(health["unresolved_open_order_count"]) == 0:
        assert health["oldest_unresolved_order_age_sec"] is None
    assert health["last_reconcile_status"] in (None, "ok", "error")
    if health["last_reconcile_status"] != "error":
        assert health["last_reconcile_error"] is None

    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_observation(
        status="waiting_first_sync",
        age_sec=None,
        sync_epoch_sec=None,
        candle_ts_ms=None,
        detail="test cleanup",
    )
    runtime_state.set_startup_gate_reason(None)


def test_run_loop_position_loss_breach_triggers_halt(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.03)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )

    flatten_calls = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.flatten_btc_position",
        lambda *_args, **_kwargs: (
            flatten_calls.__setitem__("n", flatten_calls["n"] + 1)
            or {"status": "dry_run", "qty": 0.03}
        ),
    )

    run_loop()

    state = runtime_state.snapshot()
    assert state.trading_enabled is False
    assert state.retry_at_epoch_sec == float("inf")
    assert state.halt_new_orders_blocked is True
    assert state.halt_reason_code == "POSITION_LOSS_LIMIT"
    assert state.last_disable_reason is not None
    assert "position loss threshold breached" in state.last_disable_reason
    assert "flatten_status=dry_run" in state.last_disable_reason
    assert flatten_calls["n"] == 1


def test_run_loop_position_loss_breach_uses_executable_exposure_qty(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.00009629)
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECENT_FILL_APPLIED",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1,
            "dust_classification": "harmless_dust",
            "dust_policy_reason": "matched_harmless_dust_resume_allowed",
            "dust_residual_summary": (
                "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
                "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
                "allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed"
            ),
            "dust_broker_qty": 0.00009629,
            "dust_local_qty": 0.00009629,
            "dust_effective_flat": 1,
            "remote_open_order_found": 0,
            "submit_unknown_unresolved": 0,
        },
    )

    captured_qty: list[float] = []

    def _capture_position_loss_breach(_conn, *, qty: float, price: float):
        captured_qty.append(qty)
        return False, "ok"

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.evaluate_position_loss_breach", _capture_position_loss_breach)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop()

    assert captured_qty
    assert captured_qty[0] == 0.0


def test_run_loop_daily_loss_breach_with_no_position_records_no_position_flatten(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.0)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_daily_loss_breach",
        lambda *_args, **_kwargs: (True, "daily loss limit exceeded (60,000/50,000 KRW)"),
    )
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)

    run_loop()

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "DAILY_LOSS_LIMIT"
    assert state.halt_state_unresolved is False
    assert "flatten_status=no_position" in str(state.last_disable_reason)


def test_run_loop_position_loss_breach_flatten_failure_marks_unresolved(monkeypatch):
    _prepare_run_loop(monkeypatch, asset_qty=0.03)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.live_execute_signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.BithumbBroker", lambda: _FlattenFailBroker())
    monkeypatch.setattr("bithumb_bot.flatten.fetch_orderbook_top", lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0))
    monkeypatch.setattr(
        "bithumb_bot.broker.live.fetch_orderbook_top",
        lambda _pair: BestQuote(market="KRW-BTC", bid_price=100_000_000.0, ask_price=100_010_000.0),
    )

    run_loop()

    state = runtime_state.snapshot()
    assert state.halt_reason_code == "POSITION_LOSS_LIMIT"
    assert state.halt_state_unresolved is True
    assert "flatten_status=failed" in str(state.last_disable_reason)


def test_run_loop_position_loss_breach_blocks_new_orders(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )

    called = {"n": 0}
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.live_execute_signal",
        lambda *_args, **_kwargs: called.__setitem__("n", called["n"] + 1),
    )

    run_loop()

    assert called["n"] == 0


def test_run_loop_position_loss_breach_sends_halt_notification(monkeypatch):
    _prepare_run_loop(monkeypatch)

    monkeypatch.setattr("bithumb_bot.recovery.reconcile_with_broker", lambda _broker: None, raising=False)
    monkeypatch.setattr(
        "bithumb_bot.compat.engine_legacy.evaluate_position_loss_breach",
        lambda *_args, **_kwargs: (True, "position loss threshold breached (8.00%/5.00%, entry=100, mark=92)"),
    )

    notifications: list[str] = []
    monkeypatch.setattr("bithumb_bot.compat.engine_legacy.notify", lambda msg: notifications.append(msg))

    run_loop()

    halted = [n for n in notifications if "event=trading_halted" in n and "reason_code=POSITION_LOSS_LIMIT" in n]
    assert halted
    assert any("symbol=" in n for n in halted)
    assert any(
        "operator_next_action=review risk breach details, verify exposure, then run recovery-report" in n
        for n in halted
    )
