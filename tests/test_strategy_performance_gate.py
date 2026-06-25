from __future__ import annotations

import json

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.strategy_performance import (
    fetch_strategy_performance_summary,
    evaluate_strategy_performance_gate,
)
from bithumb_bot.research.metrics_contract import ClosedTradeRecord, EquityPoint, ExecutionRecord, build_metrics_v2


def _insert_lifecycle(
    conn,
    *,
    idx: int,
    net_pnl: float,
    fee_total: float = 10.0,
    exit_rule_name: str = "opposite_cross",
    strategy_instance_id: str | None = None,
    runtime_strategy_set_manifest_hash: str | None = None,
) -> None:
    gross_pnl = float(net_pnl) + float(fee_total)
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            id, pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
            entry_fill_id, exit_fill_id, entry_ts, exit_ts, matched_qty, entry_price, exit_price,
            gross_pnl, fee_total, net_pnl, holding_time_sec, strategy_name, strategy_instance_id,
            runtime_strategy_set_manifest_hash, exit_rule_name
        ) VALUES (?, 'KRW-BTC', ?, ?, ?, ?, NULL, NULL, ?, ?, 0.0004, 100.0, 100.0, ?, ?, ?, 60.0, 'sma_with_filter', ?, ?, ?)
        """,
        (
            idx,
            idx,
            idx,
            f"entry-{idx}",
            f"exit-{idx}",
            1_710_000_000_000 + idx,
            1_710_000_060_000 + idx,
            gross_pnl,
            fee_total,
            net_pnl,
            strategy_instance_id,
            runtime_strategy_set_manifest_hash,
            exit_rule_name,
        ),
    )


def test_strategy_performance_summary_exposes_fee_adjusted_breakdown(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "performance-summary.sqlite"))
    try:
        _insert_lifecycle(conn, idx=1, net_pnl=-50.0, fee_total=45.0)
        _insert_lifecycle(conn, idx=2, net_pnl=20.0, fee_total=45.0, exit_rule_name="max_holding_time")
        conn.commit()

        summary = fetch_strategy_performance_summary(conn, strategy_name="sma_with_filter", pair="KRW-BTC")
    finally:
        conn.close()

    assert summary.sample_count == 2
    assert summary.net_pnl == -30.0
    assert summary.fee_total == 90.0
    assert summary.expectancy_per_trade == -15.0
    assert summary.by_exit_rule_name["opposite_cross"]["net_pnl"] == -50.0
    assert summary.traded_notional_total == 0.16
    assert summary.fee_drag_ratio == 90.0 / 0.16
    assert summary.fee_drag_ratio_basis == "traded_notional"
    assert summary.fee_to_gross_pnl_ratio == 90.0 / 60.0
    assert summary.fee_to_gross_pnl_ratio_basis == "gross_pnl_abs"
    payload = summary.as_dict()
    assert payload["fee_drag_ratio_basis"] == "traded_notional"
    assert payload["fee_to_gross_pnl_ratio"] == summary.fee_to_gross_pnl_ratio
    assert payload["fee_to_gross_pnl_ratio_basis"] == "gross_pnl_abs"
    json.dumps(payload, allow_nan=False)


def test_research_metrics_contract_matches_runtime_closed_lifecycle_core_definitions(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "performance-parity.sqlite"))
    try:
        _insert_lifecycle(conn, idx=1, net_pnl=100.0, fee_total=10.0)
        _insert_lifecycle(conn, idx=2, net_pnl=-50.0, fee_total=10.0)
        conn.commit()
        runtime = fetch_strategy_performance_summary(conn, strategy_name="sma_with_filter", pair="KRW-BTC")
    finally:
        conn.close()
    research = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=1050.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(EquityPoint(ts=0, equity=1000.0, cash=1000.0, asset_qty=0.0), EquityPoint(ts=1, equity=1050.0, cash=1050.0, asset_qty=0.0)),
        position_intervals=(),
        closed_trades=(
            ClosedTradeRecord(exit_ts=1, net_pnl=100.0, return_pct=10.0),
            ClosedTradeRecord(exit_ts=2, net_pnl=-50.0, return_pct=-5.0),
        ),
        execution_records=(
            ExecutionRecord(side="SELL", status="filled", filled_qty=1.0, price=110.0, fee=10.0),
            ExecutionRecord(side="SELL", status="filled", filled_qty=1.0, price=50.0, fee=10.0),
        ),
    )

    assert research.trade_quality.closed_trade_count == runtime.sample_count
    assert research.trade_quality.expectancy_per_trade_krw == runtime.expectancy_per_trade
    assert research.trade_quality.win_rate == runtime.win_rate
    assert research.trade_quality.profit_factor == runtime.profit_factor


def test_fee_drag_ratios_are_not_compared_without_matching_basis(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "performance-basis.sqlite"))
    try:
        _insert_lifecycle(conn, idx=1, net_pnl=990.0, fee_total=10.0)
        conn.commit()
        runtime = fetch_strategy_performance_summary(conn, strategy_name="sma_with_filter", pair="KRW-BTC")
    finally:
        conn.close()
    research = build_metrics_v2(
        starting_cash=1000.0,
        final_cash=1990.0,
        final_asset_qty=0.0,
        final_mark_price=0.0,
        equity_curve=(
            EquityPoint(ts=0, equity=1000.0, cash=1000.0, asset_qty=0.0),
            EquityPoint(ts=1, equity=1990.0, cash=1990.0, asset_qty=0.0),
        ),
        position_intervals=(),
        closed_trades=(ClosedTradeRecord(exit_ts=1, net_pnl=990.0, return_pct=99.0),),
        execution_records=(
            ExecutionRecord(side="BUY", status="filled", filled_qty=0.0004, price=100.0, fee=5.0),
            ExecutionRecord(side="SELL", status="filled", filled_qty=0.0004, price=100.0, fee=5.0),
        ),
    )

    assert research.cost_execution.fee_drag_ratio_basis == runtime.fee_drag_ratio_basis
    assert research.cost_execution.fee_drag_ratio == runtime.fee_drag_ratio
    assert runtime.fee_to_gross_pnl_ratio_basis == "gross_pnl_abs"
    assert runtime.fee_to_gross_pnl_ratio != runtime.fee_drag_ratio


def test_strategy_performance_gate_blocks_negative_expectancy(tmp_path) -> None:
    old_min_sample = settings.LIVE_PERFORMANCE_GATE_MIN_SAMPLE
    old_enabled = settings.LIVE_PERFORMANCE_GATE_ENABLED
    conn = ensure_db(str(tmp_path / "performance-gate.sqlite"))
    try:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", True)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", 2)
        _insert_lifecycle(conn, idx=1, net_pnl=-50.0)
        _insert_lifecycle(conn, idx=2, net_pnl=10.0)
        conn.commit()

        gate = evaluate_strategy_performance_gate(conn, strategy_name="sma_with_filter", pair="KRW-BTC")
    finally:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", old_min_sample)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", old_enabled)
        conn.close()

    assert gate.enabled is True
    assert gate.allowed is False
    assert "STRATEGY_EXPECTANCY_NEGATIVE" in gate.reason_code
    assert gate.thresholds["max_fee_drag_ratio_basis"] == "gross_pnl_abs"
    assert gate.summary.fee_to_gross_pnl_ratio_basis == "gross_pnl_abs"


def test_strategy_performance_gate_fee_threshold_uses_gross_pnl_compatibility_basis(tmp_path) -> None:
    old_min_sample = settings.LIVE_PERFORMANCE_GATE_MIN_SAMPLE
    old_enabled = settings.LIVE_PERFORMANCE_GATE_ENABLED
    old_min_expectancy = settings.LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW
    old_min_net = settings.LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW
    old_min_profit_factor = settings.LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR
    old_max_fee_drag = settings.LIVE_PERFORMANCE_GATE_MAX_FEE_DRAG_RATIO
    conn = ensure_db(str(tmp_path / "performance-gate-fee.sqlite"))
    try:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", True)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", 1)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW", 0.0)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW", 0.0)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR", 1.0)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MAX_FEE_DRAG_RATIO", "0.009")
        _insert_lifecycle(conn, idx=1, net_pnl=990.0, fee_total=10.0)
        conn.commit()

        gate = evaluate_strategy_performance_gate(conn, strategy_name="sma_with_filter", pair="KRW-BTC")
    finally:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", old_min_sample)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", old_enabled)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW", old_min_expectancy)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW", old_min_net)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR", old_min_profit_factor)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MAX_FEE_DRAG_RATIO", old_max_fee_drag)
        conn.close()

    assert gate.allowed is False
    assert "STRATEGY_FEE_DRAG_EXCESSIVE" in gate.reason_code
    assert "fee_to_gross_pnl_ratio=" in gate.reason
    assert "basis=gross_pnl_abs" in gate.reason


def test_strategy_performance_gate_fails_closed_on_insufficient_sample(tmp_path) -> None:
    old_min_sample = settings.LIVE_PERFORMANCE_GATE_MIN_SAMPLE
    old_enabled = settings.LIVE_PERFORMANCE_GATE_ENABLED
    conn = ensure_db(str(tmp_path / "performance-gate-insufficient.sqlite"))
    try:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", True)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", 3)
        _insert_lifecycle(conn, idx=1, net_pnl=100.0)
        conn.commit()

        gate = evaluate_strategy_performance_gate(conn, strategy_name="sma_with_filter", pair="KRW-BTC")
    finally:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", old_min_sample)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", old_enabled)
        conn.close()

    assert gate.allowed is False
    assert "STRATEGY_SAMPLE_INSUFFICIENT" in gate.reason_code


def test_strategy_performance_summary_prefers_strategy_instance_id_scope(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "performance-instance-scope.sqlite"))
    try:
        _insert_lifecycle(conn, idx=1, net_pnl=-500.0, strategy_instance_id="losing-instance")
        _insert_lifecycle(conn, idx=2, net_pnl=100.0, strategy_instance_id="winning-instance")
        _insert_lifecycle(conn, idx=3, net_pnl=120.0, strategy_instance_id="winning-instance")
        conn.commit()

        summary = fetch_strategy_performance_summary(
            conn,
            strategy_instance_id="winning-instance",
            strategy_name="sma_with_filter",
            pair="KRW-BTC",
        )
    finally:
        conn.close()

    assert summary.sample_count == 2
    assert summary.net_pnl == 220.0
    assert summary.filter_scope["strategy_instance_id_filter_applied"] is True
    assert summary.filter_scope["filter_precedence"] == "strategy_instance_id"


def test_strategy_performance_gate_instance_scope_blocks_only_selected_instance(tmp_path) -> None:
    old_values = {
        "LIVE_PERFORMANCE_GATE_ENABLED": settings.LIVE_PERFORMANCE_GATE_ENABLED,
        "LIVE_PERFORMANCE_GATE_MIN_SAMPLE": settings.LIVE_PERFORMANCE_GATE_MIN_SAMPLE,
        "LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW": settings.LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW,
        "LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW": settings.LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW,
        "LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR": settings.LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR,
    }
    conn = ensure_db(str(tmp_path / "performance-instance-gate.sqlite"))
    try:
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_ENABLED", True)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_SAMPLE", 2)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW", 0.0)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW", 0.0)
        object.__setattr__(settings, "LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR", 1.0)
        _insert_lifecycle(conn, idx=1, net_pnl=-500.0, strategy_instance_id="losing-instance")
        _insert_lifecycle(conn, idx=2, net_pnl=-100.0, strategy_instance_id="losing-instance")
        _insert_lifecycle(conn, idx=3, net_pnl=100.0, strategy_instance_id="winning-instance")
        _insert_lifecycle(conn, idx=4, net_pnl=120.0, strategy_instance_id="winning-instance")
        conn.commit()

        winning = evaluate_strategy_performance_gate(
            conn,
            strategy_instance_id="winning-instance",
            strategy_name="sma_with_filter",
            pair="KRW-BTC",
        )
        losing = evaluate_strategy_performance_gate(
            conn,
            strategy_instance_id="losing-instance",
            strategy_name="sma_with_filter",
            pair="KRW-BTC",
        )
    finally:
        for key, value in old_values.items():
            object.__setattr__(settings, key, value)
        conn.close()

    assert winning.allowed is True
    assert winning.summary.sample_count == 2
    assert winning.summary.filter_scope["strategy_instance_id_filter_applied"] is True
    assert losing.allowed is False
    assert "STRATEGY_EXPECTANCY_NEGATIVE" in losing.reason_code
    assert losing.summary.sample_count == 2


def test_operator_intervention_can_be_reported_separately_from_owner_pnl(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "operator-intervention.sqlite"))
    try:
        conn.execute(
            """
            INSERT INTO trade_lifecycles(
                pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
                entry_ts, exit_ts, matched_qty, entry_price, exit_price, gross_pnl, fee_total,
                net_pnl, holding_time_sec, strategy_name, strategy_instance_id,
                owner_strategy_name, owner_strategy_instance_id, owner_risk_scope_id,
                exit_actor, exit_authority, operator_intervention
            ) VALUES ('KRW-BTC', 1, 2, 'entry', 'operator_flatten-1', 1, 2, 1, 100, 90, -10, 0, -10, 1,
                'operator_flatten', 'H74', 'daily_participation_sma', 'H74', 'H74',
                'operator', 'operator_flatten', 1)
            """
        )
        conn.commit()

        owner = fetch_strategy_performance_summary(conn, strategy_name="daily_participation_sma", pair="KRW-BTC")
        operator_rows = conn.execute(
            "SELECT COUNT(*) AS c FROM trade_lifecycles WHERE exit_actor='operator' AND operator_intervention=1"
        ).fetchone()
    finally:
        conn.close()

    assert owner.sample_count == 1
    assert owner.net_pnl == -10.0
    assert operator_rows["c"] == 1
