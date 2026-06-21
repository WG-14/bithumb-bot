from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.db_core import ensure_db
from bithumb_bot.execution_service import ExecutionSubmitPlan
from bithumb_bot.run_loop_execution_planner import (
    ExecutionPlanner,
    _build_execution_plan_batch_for_runtime_pair,
    resolve_target_position_state_for_run_loop,
)
from bithumb_bot.runtime_strategy_set import RuntimeStrategySet, RuntimeStrategySpec
from bithumb_bot.strategy_policy_contract import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2


def _count(conn, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _readiness_payload() -> dict[str, object]:
    return {
        "residual_inventory_qty": 0.0,
        "residual_inventory_notional_krw": 0.0,
        "residual_inventory_state": "flat",
        "residual_inventory_policy_allows_buy": True,
        "residual_inventory_policy_allows_sell": False,
        "residual_inventory_policy_allows_run": True,
        "cash_available": 1_000_000.0,
        "broker_position_evidence": {
            "broker_qty_known": True,
            "broker_qty": 0.0,
            "balance_source_stale": False,
        },
        "projection_converged": True,
        "projection_convergence": {"converged": True},
        "broker_portfolio_converged": True,
        "open_order_count": 0,
        "accounting_projection_ok": True,
        "active_fee_accounting_blocker": False,
        "residual_proof_min_qty": 0.0001,
        "residual_proof_min_notional_krw": 5000.0,
    }


def _decision(final_signal: str = "BUY") -> StrategyDecisionV2:
    return StrategyDecisionV2(
        strategy_name="unit",
        raw_signal=final_signal,
        raw_reason=f"raw {final_signal}",
        entry_signal=final_signal,
        entry_reason=f"entry {final_signal}",
        exit_signal=final_signal,
        exit_reason=f"exit {final_signal}",
        final_signal=final_signal,
        final_reason=f"final {final_signal}",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        execution_intent=EntryExecutionIntent(
            side=final_signal,
            intent="enter" if final_signal == "BUY" else "hold",
            pair="KRW-BTC",
            requires_execution_sizing=True,
            budget_fraction_of_cash=1.0,
            max_budget_krw=10000.0,
        ),
        entry_decision=object(),  # type: ignore[arg-type]
        trace={"final_signal": final_signal},
        policy_hash="sha256:policy",
        policy_contract_hash="sha256:contract",
        policy_input_hash="sha256:input",
        policy_decision_hash=f"sha256:decision-{final_signal}",
    )


def _runtime_result(*, signal: str = "BUY", instance_id: str = "unit") -> SimpleNamespace:
    decision = _decision(signal)
    base_context = {
        "strategy": "unit",
        "signal": signal,
        "reason": decision.final_reason,
        "strategy_instance_id": instance_id,
        "runtime_decision_request_hash": "sha256:request",
        "strategy_parameters_hash": "sha256:params",
        "approved_profile_hash": None,
        "runtime_contract_hash": "sha256:runtime-contract",
        "plugin_contract_hash": "sha256:plugin-contract",
        "scope_key_hash": "sha256:scope",
        "through_ts_ms": 123,
    }
    return SimpleNamespace(
        decision=decision,
        base_context=base_context,
        candle_ts=123,
        market_price=100_000_000.0,
        policy_hashes={
            "policy_contract_hash": decision.policy_contract_hash,
            "policy_input_hash": decision.policy_input_hash,
            "policy_decision_hash": decision.policy_decision_hash,
        },
        replay_fingerprint={"runtime_decision_request_hash": "sha256:request"},
        boundary={"phase": "unit"},
        as_legacy_dict=lambda: dict(base_context),
    )


def _runtime_bundle(*, signal: str = "BUY") -> SimpleNamespace:
    spec = RuntimeStrategySpec(
        "unit",
        strategy_instance_id="unit",
        pair="KRW-BTC",
        interval="1m",
        desired_exposure_krw=10000.0,
    )
    strategy_set = RuntimeStrategySet(source="unit", strategies=(spec,))
    result = _runtime_result(signal=signal, instance_id="unit")
    return SimpleNamespace(
        strategy_set=strategy_set,
        results=(result,),
        candle_ts=123,
        market_price=100_000_000.0,
        as_dict=lambda: {"schema_version": 1, "results": [result.as_legacy_dict()]},
        content_hash=lambda: "sha256:runtime-bundle",
    )


class _Readiness:
    def as_dict(self) -> dict[str, object]:
        return _readiness_payload()


class _Summary:
    def __init__(self, submit_plan: ExecutionSubmitPlan | None) -> None:
        self._submit_plan = submit_plan
        self.submit_expected = submit_plan is not None
        self.block_reason = "" if submit_plan is not None else "submit_not_expected"

    def as_dict(self) -> dict[str, object]:
        return {
            "final_action": "BUY" if self._submit_plan is not None else "HOLD",
            "submit_expected": self._submit_plan is not None,
            "pre_submit_proof_status": "not_required",
            "block_reason": "",
            "target_submit_plan": (
                None
                if self._submit_plan is None
                else {
                    **self._submit_plan.as_dict(),
                    "submit_plan_hash": self._submit_plan.content_hash(),
                }
            ),
        }

    def typed_target_submit_plan(self):
        return self._submit_plan

    def typed_residual_submit_plan(self):
        return None

    def typed_buy_submit_plan(self):
        return None


def _planner(*, submit: bool = True) -> ExecutionPlanner:
    submit_plan = (
        ExecutionSubmitPlan(
            side="BUY",
            source="test",
            authority="test",
            final_action="BUY",
            qty=0.001,
            notional_krw=10000.0,
            target_exposure_krw=10000.0,
            current_effective_exposure_krw=0.0,
            delta_krw=10000.0,
            submit_expected=True,
            pre_submit_proof_status="not_required",
            block_reason="",
            idempotency_key="planner-full-path-buy",
            pair="KRW-BTC",
        )
        if submit
        else None
    )
    return ExecutionPlanner(
        settings_obj=SimpleNamespace(
            PAIR="KRW-BTC",
            INTERVAL="1m",
            MODE="paper",
            EXECUTION_ENGINE="target_delta",
            MAX_ORDER_KRW=10000.0,
            TARGET_EXPOSURE_KRW=10000.0,
            LIVE_REAL_ORDER_ARMED=False,
            LIVE_DRY_RUN=True,
            APPROVED_STRATEGY_PROFILE_PATH="",
            STRATEGY_APPROVED_PROFILE_PATH="",
            MAX_OPEN_ORDER_AGE_SEC=300,
        ),
        readiness_snapshot_builder=lambda _conn: _Readiness(),
        summary_builder=lambda **_kwargs: _Summary(submit_plan),
    )


def test_plan_runtime_strategy_results_does_not_insert_target_state(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "planner-target.sqlite"))

    result = resolve_target_position_state_for_run_loop(
        conn,
        readiness_payload=_readiness_payload(),
        reference_price=100_000_000.0,
        raw_signal="HOLD",
        updated_ts=1,
        settings_obj=SimpleNamespace(PAIR="KRW-BTC", EXECUTION_ENGINE="target_delta"),
        runtime_pair="KRW-BTC",
    )

    assert _count(conn, "target_position_state") == 0
    assert isinstance(result["target_policy_metadata"], dict)
    assert isinstance(result["target_policy_metadata"].get("target_state_update_intent"), dict)
    conn.close()


def test_buy_submit_plan_returns_budget_lock_intent_without_budget_lock_row(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "planner-buy-lock.sqlite"))
    context = {"runtime_pair": "KRW-BTC", "portfolio_target_hash": "target-hash"}
    submit_plan = ExecutionSubmitPlan(
        side="BUY",
        source="test",
        authority="test",
        final_action="BUY",
        qty=0.001,
        notional_krw=10000.0,
        target_exposure_krw=10000.0,
        current_effective_exposure_krw=0.0,
        delta_krw=10000.0,
        submit_expected=True,
        pre_submit_proof_status="not_required",
        block_reason="",
        idempotency_key="buy-key",
        pair="KRW-BTC",
    )

    batch = _build_execution_plan_batch_for_runtime_pair(
        conn,
        context=context,
        submit_plan=submit_plan,
        updated_ts=1,
    )

    assert _count(conn, "budget_locks") == 0
    assert context["lock_intents"][0]["lock_kind"] == "budget"
    assert batch.pair_plans[0].lock_status == "intent_pending_persistence"
    conn.close()


def test_plan_runtime_strategy_results_does_not_insert_virtual_target_state(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "planner-full-virtual.sqlite"))
    before = _count(conn, "strategy_virtual_target_state")

    bundle = _planner(submit=False).plan_runtime_strategy_results(
        conn,
        _runtime_bundle(signal="BUY"),
        updated_ts=1,
    )

    assert _count(conn, "strategy_virtual_target_state") == before
    intents = bundle.persistence_context.get("virtual_target_state_update_intents")
    assert isinstance(intents, list)
    assert len(intents) == 1
    assert intents[0]["strategy_instance_id"] == "unit"
    conn.close()


def test_plan_runtime_strategy_results_does_not_change_target_or_lock_tables_full_path(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "planner-full-path.sqlite"))
    tables = (
        "target_position_state",
        "strategy_virtual_target_state",
        "budget_locks",
        "order_locks",
    )
    before = {table: _count(conn, table) for table in tables}

    bundle = _planner(submit=True).plan_runtime_strategy_results(
        conn,
        _runtime_bundle(signal="BUY"),
        updated_ts=1,
    )

    after = {table: _count(conn, table) for table in tables}
    assert after == before
    assert isinstance(bundle.persistence_context.get("target_state_update_intent"), dict)
    assert isinstance(bundle.persistence_context.get("virtual_target_state_update_intents"), list)
    assert isinstance(bundle.persistence_context.get("lock_intents"), list)
    assert bundle.persistence_context["lock_intents"][0]["lock_kind"] == "budget"
    conn.close()


def test_sell_submit_plan_returns_order_lock_intent_without_order_lock_row(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "planner-sell-lock.sqlite"))
    context = {"runtime_pair": "KRW-BTC", "portfolio_target_hash": "target-hash"}
    submit_plan = ExecutionSubmitPlan(
        side="SELL",
        source="test",
        authority="test",
        final_action="SELL",
        qty=0.001,
        notional_krw=None,
        target_exposure_krw=0.0,
        current_effective_exposure_krw=10000.0,
        delta_krw=-10000.0,
        submit_expected=True,
        pre_submit_proof_status="not_required",
        block_reason="",
        idempotency_key="sell-key",
        pair="KRW-BTC",
    )

    batch = _build_execution_plan_batch_for_runtime_pair(
        conn,
        context=context,
        submit_plan=submit_plan,
        updated_ts=1,
    )

    assert _count(conn, "order_locks") == 0
    assert context["lock_intents"][0]["lock_kind"] == "order"
    assert batch.pair_plans[0].lock_status == "intent_pending_persistence"
    conn.close()


def test_planner_source_has_no_forbidden_write_calls() -> None:
    source = "src/bithumb_bot/run_loop_execution_planner.py"
    text = open(source, encoding="utf-8").read()
    forbidden = (
        "upsert_target_position_state",
        "upsert_strategy_virtual_target_state",
        "create_or_get_budget_lock",
        "create_or_get_order_lock",
        "conn.commit",
    )
    for needle in forbidden:
        assert needle not in text
