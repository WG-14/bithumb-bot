from __future__ import annotations

from dataclasses import dataclass
import os

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.engine import _close_guard_ms, _is_closed_candle, _select_latest_closed_candle, run_loop
from bithumb_bot.execution_service import ExecutionDecisionSummary, ExecutionSubmitPlan
from bithumb_bot.run_loop_execution_planner import ExecutionPlanBundle
from bithumb_bot.strategy_policy_contract import EntryExecutionIntent, PositionSnapshot, StrategyDecisionV2
from bithumb_bot.compat.sma_legacy_adapter import compute_signal


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path):
    from bithumb_bot.config import settings as current_settings
    import bithumb_bot.db_core as db_core_module
    import bithumb_bot.engine as engine_settings_module
    import bithumb_bot.runtime_state as runtime_state_module

    globals()["settings"] = current_settings
    db_core_module.settings = current_settings
    engine_settings_module.settings = current_settings
    runtime_state_module.settings = current_settings
    old_db_path = settings.DB_PATH
    old_mode = settings.MODE
    old_env_db_path = os.environ.get("DB_PATH")

    db_path = str(tmp_path / "candle_stability.sqlite")
    os.environ["DB_PATH"] = db_path
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "paper")
    ensure_db().close()
    runtime_state.enable_trading()
    runtime_state.set_error_count(0)
    runtime_state.set_last_candle_observation(
        status="waiting_first_sync",
        age_sec=None,
        sync_epoch_sec=None,
        candle_ts_ms=None,
        detail=None,
    )
    runtime_state.set_startup_gate_reason(None)

    yield

    runtime_state.enable_trading()
    object.__setattr__(settings, "DB_PATH", old_db_path)
    object.__setattr__(settings, "MODE", old_mode)
    if old_env_db_path is None:
        os.environ.pop("DB_PATH", None)
    else:
        os.environ["DB_PATH"] = old_env_db_path


def _insert_candle(ts_ms: int, close: float) -> None:
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO candles(ts, pair, interval, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts_ms, settings.PAIR, settings.INTERVAL, close, close, close, close, 1.0),
        )
        conn.commit()
    finally:
        conn.close()


@dataclass
class _RuntimeDecisionResult:
    decision: StrategyDecisionV2
    base_context: dict[str, object]
    candle_ts: int
    market_price: float
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object]
    policy_hashes: None = None

    def as_legacy_dict(self) -> dict[str, object]:
        return dict(self.base_context)


def _runtime_handoff(*, candle_ts: int, price: float, final_signal: str) -> _RuntimeDecisionResult:
    execution_intent = None
    if final_signal == "BUY":
        execution_intent = EntryExecutionIntent(
            side="BUY",
            intent="enter",
            pair=settings.PAIR,
            requires_execution_sizing=True,
            budget_fraction_of_cash=1.0,
            max_budget_krw=float(settings.MAX_ORDER_KRW),
        )
    decision = StrategyDecisionV2(
        strategy_name=str(settings.STRATEGY_NAME),
        raw_signal=final_signal,
        raw_reason=f"unit {final_signal.lower()}",
        entry_signal=final_signal,
        entry_reason=f"unit {final_signal.lower()}",
        exit_signal=final_signal,
        exit_reason=f"unit {final_signal.lower()}",
        final_signal=final_signal,
        final_reason=f"unit {final_signal.lower()}",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        execution_intent=execution_intent,
        entry_decision=object(),
        trace={"final_signal": final_signal},
        policy_hash="sha256:candle-stability-policy",
        policy_contract_hash="sha256:candle-stability-contract",
        policy_input_hash="sha256:candle-stability-input",
        policy_decision_hash="sha256:candle-stability-decision",
    )
    return _RuntimeDecisionResult(
        decision=decision,
        base_context={
            "market_price": price,
            "last_close": price,
            "position_state": {"normalized_exposure": {"sellable_executable_lot_count": 0}},
        },
        candle_ts=candle_ts,
        market_price=price,
        replay_fingerprint={"schema_version": 1, "candle_ts": candle_ts},
        boundary={"decision_boundary_phase": "unit_closed_candle"},
    )


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


def _install_runtime_gateway(monkeypatch, result_factory) -> None:
    class _Gateway:
        def decide_bundle(self, conn, *, strategy_set=None, through_ts_ms=None):
            result = result_factory(conn, through_ts_ms=through_ts_ms)
            result.base_context.update(
                {
                    "runtime_decision_request_hash": "sha256:unit-runtime-request",
                    "strategy_instance_id": str(result.decision.strategy_name),
                    "strategy_parameters_hash": "sha256:unit-parameters",
                    "approved_profile_hash": None,
                    "runtime_contract_hash": "sha256:unit-runtime-contract",
                    "plugin_contract_hash": "sha256:unit-plugin-contract",
                    "through_ts_ms": through_ts_ms,
                }
            )
            result.replay_fingerprint.update(
                {
                    "runtime_decision_request_hash": "sha256:unit-runtime-request",
                    "strategy_instance_id": str(result.decision.strategy_name),
                    "strategy_parameters_hash": "sha256:unit-parameters",
                    "approved_profile_hash": None,
                    "runtime_contract_hash": "sha256:unit-runtime-contract",
                    "plugin_contract_hash": "sha256:unit-plugin-contract",
                    "through_ts_ms": through_ts_ms,
                }
            )
            return _RuntimeDecisionBundle(result, strategy_set)

    monkeypatch.setattr("bithumb_bot.engine.RuntimeDecisionGateway", _Gateway)

    class _Planner:
        def plan_runtime_strategy_results(self, _conn, result_bundle, *, updated_ts: int):
            del updated_ts
            result = result_bundle.results[0]
            if result.decision.final_signal == "BUY":
                return _buy_execution_plan()
            return _hold_execution_plan(result)

    monkeypatch.setattr("bithumb_bot.engine.run_loop_execution_planner", lambda **_kwargs: _Planner())


def _buy_execution_plan() -> ExecutionPlanBundle:
    plan = ExecutionSubmitPlan(
        side="BUY",
        source="target_delta",
        authority="canonical_target_delta_sizing",
        final_action="ENTER_STRATEGY_POSITION",
        qty=0.02,
        notional_krw=2.0,
        target_exposure_krw=2.0,
        current_effective_exposure_krw=0.0,
        delta_krw=2.0,
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        idempotency_key="unit-closed-candle-buy",
    )
    summary = ExecutionDecisionSummary(
        raw_signal="BUY",
        final_signal="BUY",
        final_action="ENTER_STRATEGY_POSITION",
        submit_expected=True,
        pre_submit_proof_status="passed",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=2.0,
        current_effective_exposure_krw=0.0,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=2.0,
        residual_live_sell_mode="telemetry",
        residual_buy_sizing_mode="telemetry",
        residual_submit_plan=None,
        buy_submit_plan=plan,
        target_shadow_decision=None,
        target_submit_plan=None,
    )
    return ExecutionPlanBundle(
        summary=summary,
        submit_plan=plan,
        persistence_context={
            "ts": 0,
            "last_close": 100.0,
            "execution_decision": summary.as_dict(),
            "final_action": "ENTER_STRATEGY_POSITION",
            "submit_expected": True,
            "pre_submit_proof_status": "passed",
            "execution_plan_bundle_present": True,
            "submit_plan_source": plan.source,
            "submit_plan_authority": plan.authority,
            "decision_authority_source": "DecisionEnvelope.strategy_decision",
            "decision_envelope_present": True,
            "persistence_context_authoritative": 0,
        },
        readiness_payload={},
        target_policy_metadata={},
    )


def _hold_execution_plan(result: _RuntimeDecisionResult) -> ExecutionPlanBundle:
    summary = ExecutionDecisionSummary(
        raw_signal=result.decision.raw_signal,
        final_signal=result.decision.final_signal,
        final_action="HOLD",
        submit_expected=False,
        pre_submit_proof_status="not_required",
        block_reason="none",
        strategy_sell_candidate=None,
        residual_sell_candidate=None,
        target_exposure_krw=0.0,
        current_effective_exposure_krw=0.0,
        tracked_residual_exposure_krw=None,
        buy_delta_krw=None,
        residual_live_sell_mode="telemetry",
        residual_buy_sizing_mode="telemetry",
        residual_submit_plan=None,
        buy_submit_plan=None,
        target_shadow_decision=None,
        target_submit_plan=None,
    )
    return ExecutionPlanBundle(
        summary=summary,
        submit_plan=None,
        persistence_context={
            **result.base_context,
            "ts": result.candle_ts,
            "last_close": result.market_price,
            "execution_decision": summary.as_dict(),
            "final_action": "HOLD",
            "submit_expected": False,
            "pre_submit_proof_status": "not_required",
            "execution_plan_bundle_present": True,
            "decision_authority_source": "DecisionEnvelope.strategy_decision",
            "decision_envelope_present": True,
            "persistence_context_authoritative": 0,
        },
        readiness_payload={},
        target_policy_metadata={},
    )


def test_last_processed_candle_ts_persists_to_bot_health() -> None:
    runtime_state.mark_processed_candle(candle_ts_ms=1_700_000_000_000, now_epoch_sec=1_700_000_100.0)

    conn = ensure_db()
    try:
        row = conn.execute(
            "SELECT last_processed_candle_ts_ms, last_candle_status FROM bot_health WHERE id=1"
        ).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert int(row["last_processed_candle_ts_ms"]) == 1_700_000_000_000
    assert str(row["last_candle_status"]) == "processed_closed"
    assert runtime_state.snapshot().last_processed_candle_ts_ms == 1_700_000_000_000


def test_compute_signal_through_ts_excludes_newer_candles() -> None:
    base_ts = 1_700_000_000_000
    for idx, close in enumerate([10.0, 11.0, 12.0, 13.0, 14.0, 50.0]):
        _insert_candle(base_ts + idx * 60_000, close)

    conn = ensure_db()
    try:
        latest = compute_signal(conn, 2, 3)
        bounded = compute_signal(conn, 2, 3, through_ts_ms=base_ts + 4 * 60_000)
    finally:
        conn.close()

    assert latest is not None
    assert bounded is not None
    assert latest["ts"] == base_ts + 5 * 60_000
    assert bounded["ts"] == base_ts + 4 * 60_000
    assert latest["last_close"] == 50.0
    assert bounded["last_close"] == 14.0


def test_compute_signal_ignores_open_candle_tail_when_bounded(monkeypatch) -> None:
    base_ts = 1_700_000_000_000
    # The last candle is intentionally "open" and would flip the moving-average
    # signal if the strategy were allowed to see it.
    closes = [100.0, 100.0, 100.0, 100.0, 100.0, 200.0]
    for idx, close in enumerate(closes):
        _insert_candle(base_ts + idx * 60_000, close)

    # Force the strategy's default closed-only cutoff to land before the open tail.
    monkeypatch.setattr(
        "bithumb_bot.compat.sma_legacy_adapter.time.time",
        lambda: (base_ts + 5 * 60_000 + 3_100) / 1000,
    )

    conn = ensure_db()
    try:
        unbounded = compute_signal(conn, 2, 3)
        bounded = compute_signal(conn, 2, 3, through_ts_ms=base_ts + 4 * 60_000)
    finally:
        conn.close()

    assert unbounded is not None
    assert bounded is not None
    assert unbounded["ts"] == base_ts + 4 * 60_000
    assert bounded["ts"] == base_ts + 4 * 60_000
    assert unbounded["signal"] == "HOLD"
    assert bounded["signal"] == "HOLD"


def test_select_latest_closed_candle_skips_open_tail() -> None:
    _insert_candle(0, 100.0)
    _insert_candle(60_000, 101.0)

    conn = ensure_db()
    try:
        closed_row, incomplete_ts = _select_latest_closed_candle(
            conn,
            pair=settings.PAIR,
            interval=settings.INTERVAL,
            interval_sec=60,
            now_ms=65_000,
        )
    finally:
        conn.close()

    assert closed_row is not None
    assert int(closed_row["ts"]) == 0
    assert incomplete_ts == 60_000


def test_is_closed_candle_boundary_before_and_after_close_guard() -> None:
    interval_sec = 60
    candle_start_ts_ms = 0
    guard_ms = _close_guard_ms(interval_sec)
    close_ready_ts_ms = candle_start_ts_ms + interval_sec * 1000 + guard_ms

    assert _is_closed_candle(
        candle_ts_ms=candle_start_ts_ms,
        now_ms=close_ready_ts_ms - 1,
        interval_sec=interval_sec,
    ) is False
    assert _is_closed_candle(
        candle_ts_ms=candle_start_ts_ms,
        now_ms=close_ready_ts_ms,
        interval_sec=interval_sec,
    ) is True


def test_select_latest_closed_candle_consistent_with_to_exclusive_snapshot_cutoff() -> None:
    # to=00:02:00 fetches candles strictly before the candle containing "to".
    # For 1m bars, a candle is eligible only if start + 1m <= to.
    _insert_candle(0, 100.0)  # [00:00, 00:01)
    _insert_candle(60_000, 101.0)  # [00:01, 00:02)
    _insert_candle(120_000, 102.0)  # [00:02, 00:03) -> excluded by to=00:02:00

    interval_sec = 60
    to_ms = 120_000
    guard_ms = _close_guard_ms(interval_sec)
    now_ms = to_ms + guard_ms

    conn = ensure_db()
    try:
        closed_row, incomplete_ts = _select_latest_closed_candle(
            conn,
            pair=settings.PAIR,
            interval=settings.INTERVAL,
            interval_sec=interval_sec,
            now_ms=now_ms,
        )
    finally:
        conn.close()

    assert closed_row is not None
    assert int(closed_row["ts"]) == 60_000
    assert incomplete_ts == 120_000


def test_run_loop_logs_duplicate_and_incomplete_candle_and_skips_reprocessing(monkeypatch, caplog):
    closed_ts = 0
    open_ts = 60_000
    _insert_candle(closed_ts, 100.0)
    _insert_candle(open_ts, 101.0)
    runtime_state.mark_processed_candle(candle_ts_ms=closed_ts, now_epoch_sec=1.0)

    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 60)
    _install_runtime_gateway(
        monkeypatch,
        lambda *_args, **_kwargs: pytest.fail("duplicate candle should not reach runtime gateway"),
    )

    times = iter([64.0, 65.0, 65.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(times, 65.0))
    sleep_calls = {"n": 0}

    def _sleep(_sec: float) -> None:
        sleep_calls["n"] += 1
        if sleep_calls["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)

    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        run_loop()

    output = caplog.text
    assert "[SKIP] incomplete/open candle" in output
    assert f"candle_ts={open_ts}" in output
    assert "[SKIP] duplicate candle" in output
    assert f"last_processed_candle_ts={closed_ts}" in output
    assert runtime_state.snapshot().last_processed_candle_ts_ms == closed_ts


def test_run_loop_processes_latest_closed_candle_and_persists_it(monkeypatch, caplog):
    closed_ts = 0
    open_ts = 60_000
    _insert_candle(closed_ts, 100.0)
    _insert_candle(open_ts, 101.0)

    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 60)
    _install_runtime_gateway(
        monkeypatch,
        lambda _conn, *, through_ts_ms=None: _runtime_handoff(
            candle_ts=through_ts_ms,
            price=100.0,
            final_signal="HOLD",
        ),
    )

    times = iter([64.0, 65.0, 65.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(times, 65.0))
    sleep_calls = {"n": 0}

    def _sleep(_sec: float) -> None:
        sleep_calls["n"] += 1
        if sleep_calls["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)
    monkeypatch.setattr("bithumb_bot.engine.paper_execute", lambda *_args, **_kwargs: pytest.fail("HOLD should not execute"))

    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        run_loop()

    output = caplog.text
    assert "[RUN] processed closed candle" in output
    assert f"candle_ts={closed_ts}" in output
    assert runtime_state.snapshot().last_processed_candle_ts_ms == closed_ts


def test_run_loop_does_not_mark_candle_processed_when_decision_persistence_fails(
    monkeypatch,
    caplog,
):
    closed_ts = 0
    open_ts = 60_000
    _insert_candle(closed_ts, 100.0)
    _insert_candle(open_ts, 101.0)

    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 60)
    _install_runtime_gateway(
        monkeypatch,
        lambda _conn, *, through_ts_ms=None: _runtime_handoff(
            candle_ts=through_ts_ms,
            price=100.0,
            final_signal="HOLD",
        ),
    )

    def _record_failure(*_args, **_kwargs):
        raise RuntimeError("unit persistence failure")

    monkeypatch.setattr("bithumb_bot.engine.record_strategy_decision", _record_failure)
    times = iter([64.0, 65.0, 65.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(times, 65.0))
    sleep_calls = {"n": 0}

    def _sleep(_sec: float) -> None:
        sleep_calls["n"] += 1
        if sleep_calls["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)

    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        run_loop()

    output = caplog.text
    assert "[WARN] strategy decision persistence failed" in output
    assert "decision_persistence_failed_retryable" in output
    assert "[RUN] processed closed candle" not in output
    assert runtime_state.snapshot().last_processed_candle_ts_ms != closed_ts


def test_run_loop_uses_closed_candle_for_signal_and_trade_log_correlation(monkeypatch, caplog):
    closed_ts = 0
    open_ts = 60_000
    _insert_candle(closed_ts, 100.0)
    _insert_candle(open_ts, 200.0)

    monkeypatch.setattr("bithumb_bot.engine.cmd_sync", lambda quiet=True: None)
    monkeypatch.setattr("bithumb_bot.engine.parse_interval_sec", lambda _: 60)

    def _decide(_conn, *, through_ts_ms=None):
        assert through_ts_ms == closed_ts
        return _runtime_handoff(candle_ts=through_ts_ms, price=100.0, final_signal="BUY")

    _install_runtime_gateway(monkeypatch, _decide)
    monkeypatch.setattr(
        "bithumb_bot.engine.paper_execute",
        lambda _signal, _ts, _price, **_kwargs: {
            "ts": closed_ts,
            "signal_ts": closed_ts,
            "candle_ts": closed_ts,
            "client_order_id": "paper-closed-log",
            "exchange_order_id": "ex-closed-log",
            "side": "BUY",
            "qty": 0.02,
            "filled_qty": 0.02,
            "submit_qty": 0.02,
            "price": 100.0,
            "fee": 1.0,
            "cash": 999.0,
            "asset": 0.02,
            "post_trade_cash": 999.0,
            "post_trade_asset": 0.02,
        },
    )

    times = iter([64.0, 65.0, 65.0])
    monkeypatch.setattr("bithumb_bot.engine.time.time", lambda: next(times, 65.0))
    sleep_calls = {"n": 0}

    def _sleep(_sec: float) -> None:
        sleep_calls["n"] += 1
        if sleep_calls["n"] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("bithumb_bot.engine.time.sleep", _sleep)
    plan_bundle = _buy_execution_plan()

    class _Planner:
        def plan_envelope(self, *_args, **_kwargs):
            return plan_bundle

        def plan_runtime_strategy_results(self, *_args, **_kwargs):
            return plan_bundle

    monkeypatch.setattr("bithumb_bot.engine.run_loop_execution_planner", lambda **_kwargs: _Planner())

    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        run_loop()

    output = caplog.text
    assert "[RUN] processed closed candle" in output
    assert "[RUN] trade_applied" in output
    assert "client_order_id=paper-closed-log" in output
    assert "exchange_order_id=ex-closed-log" in output
    assert "signal_ts=0" in output
    assert "submit_qty=0.020" in output
    assert "filled_qty=0.020" in output
    assert "post_trade_cash=999" in output
    assert "post_trade_asset=0.02000000" in output
