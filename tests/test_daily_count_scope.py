from __future__ import annotations

import sqlite3

from bithumb_bot.risk import DailyLossEvaluation
from bithumb_bot.risk_contract import RiskPolicy, SubmitPlan
from bithumb_bot.runtime.daily_participation_count_provider import (
    build_runtime_daily_count_snapshot_from_sqlite,
)
from bithumb_bot.runtime_risk_engine import RuntimeRiskEngineAdapter
from bithumb_bot.strategy.daily_participation_policy import DailyParticipationPolicyConfig


DECISION_TS = 1_704_046_800_000


def _daily_config() -> DailyParticipationPolicyConfig:
    return DailyParticipationPolicyConfig(
        enabled=True,
        timezone="Asia/Seoul",
        count_basis="intent",
        window_start_hour=0,
        window_end_hour=24,
        buy_fraction=0.05,
        max_order_krw=10_000.0,
    )


def test_operator_smoke_order_does_not_consume_h74_participation_count() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE orders (
            client_order_id TEXT NOT NULL,
            side TEXT NOT NULL,
            status TEXT NOT NULL,
            created_ts INTEGER NOT NULL,
            pair TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            strategy_instance_id TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO orders(client_order_id, side, status, created_ts, pair, strategy_name, strategy_instance_id)
        VALUES ('smoke-1', 'BUY', 'FILLED', ?, 'KRW-BTC', 'operator_live_pipeline_smoke', 'operator-smoke')
        """,
        (DECISION_TS - 1_000,),
    )

    snapshot = build_runtime_daily_count_snapshot_from_sqlite(
        conn=conn,
        config=_daily_config(),
        decision_ts=DECISION_TS,
        pair="KRW-BTC",
        strategy_instance_id="h74-source-observation",
        strategy_name="daily_participation_sma",
    )

    assert snapshot.count_for_kst_day == 0
    assert snapshot.strategy_instance_id == "h74-source-observation"
    conn.close()


def test_risk_daily_order_count_scope_matches_policy(monkeypatch) -> None:
    def _daily_loss_state(*_args, **_kwargs) -> DailyLossEvaluation:
        return DailyLossEvaluation(
            blocked=False,
            reason="ok",
            reason_code="OK",
            decision="allow",
            evaluation_ts_ms=DECISION_TS,
            day_kst="2024-01-01",
            max_daily_loss_krw=50_000.0,
            start_equity=1_000_000.0,
            current_equity=1_000_000.0,
            loss_today=0.0,
            current_cash_krw=1_000_000.0,
            current_asset_qty=0.0,
            mark_price=100_000_000.0,
            mark_price_source="unit",
            details={},
        )

    monkeypatch.setattr("bithumb_bot.runtime_risk_engine.evaluate_daily_loss_state", _daily_loss_state)
    monkeypatch.setattr("bithumb_bot.runtime_risk_engine._latest_position_entry_price", lambda _conn: None)
    monkeypatch.setattr("bithumb_bot.runtime_risk_engine._count_orders_today", lambda _conn, _ts: 1)
    monkeypatch.setattr(
        "bithumb_bot.runtime_risk_engine.collect_risky_order_state",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        "bithumb_bot.runtime_risk_engine._record_typed_decision_identity",
        lambda *_args, **_kwargs: None,
    )
    conn = sqlite3.connect(":memory:")
    try:
        decision = RuntimeRiskEngineAdapter(
            conn,
            policy=RiskPolicy(max_daily_loss_krw=50_000.0, max_daily_order_count=2),
        ).evaluate_pre_submit(
            plan=SubmitPlan(side="BUY", qty=0.0002, notional_krw=20_000.0, source="target_delta"),
            ts_ms=DECISION_TS,
            now_ms=DECISION_TS,
            cash=0.0,
            submit_qty=0.0002,
            current_asset_qty=None,
            price=100_000_000.0,
            broker=object(),
            evaluation_origin="live_real_submit_authority_pre_submit",
        )
    finally:
        conn.close()

    assert decision.evidence["daily_order_count_scope"] == "account_global"
    assert decision.evidence["daily_order_count_source"] == "orders.created_ts_kst_day"
    assert decision.effective_limits["max_daily_order_count"] == 2
