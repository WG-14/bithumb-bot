from __future__ import annotations

from bithumb_bot.db_core import ensure_db
from bithumb_bot.h74_observation import build_h74_source_observation_authority_payload
from bithumb_bot.reason_codes import DRAWDOWN_UNDEFINED_NO_CAPITAL_BASE
from bithumb_bot.strategy_risk_state import StrategyRiskStateProvider


def _insert_lifecycle(conn, *, pnl: float, scope: str = "cabccc", ts: int = 10) -> None:
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
            entry_ts, exit_ts, matched_qty, entry_price, exit_price, gross_pnl, fee_total,
            net_pnl, holding_time_sec, strategy_name, strategy_instance_id,
            owner_strategy_name, owner_strategy_instance_id, owner_risk_scope_id, risk_scope_id
        ) VALUES ('KRW-BTC', 1, 2, 'e', 'x', 1, ?, 1, 100, 90, ?, 0, ?, 1,
            'daily_participation_sma', ?, 'daily_participation_sma', ?, ?, ?)
        """,
        (ts, pnl, pnl, scope, scope, scope, scope),
    )


def test_negative_first_lifecycle_without_capital_is_undefined(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "drawdown.sqlite"))
    _insert_lifecycle(conn, pnl=-119.09)

    snapshot = StrategyRiskStateProvider(conn).snapshot(
        strategy_instance_id="cabccc",
        strategy_name="daily_participation_sma",
        pair="KRW-BTC",
        interval="1m",
        as_of_ts_ms=20,
        mark_price=100.0,
        risk_scope_id="cabccc",
    )

    assert snapshot.current_drawdown_metric is not None
    assert snapshot.current_drawdown_metric.state == "undefined"
    assert snapshot.current_drawdown_metric.reason_code == DRAWDOWN_UNDEFINED_NO_CAPITAL_BASE


def test_negative_first_lifecycle_with_100k_capital_is_not_100pct(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "drawdown-capital.sqlite"))
    _insert_lifecycle(conn, pnl=-119.094392175687, ts=10)
    _insert_lifecycle(conn, pnl=-44.3705214677908, ts=11)
    conn.execute(
        "INSERT INTO strategy_risk_capital_basis(risk_scope_id, strategy_instance_id, capital_krw, capital_basis) VALUES (?, ?, ?, ?)",
        ("cabccc", "cabccc", 100_000.0, "fixed_observation_notional"),
    )

    snapshot = StrategyRiskStateProvider(conn).snapshot(
        strategy_instance_id="cabccc",
        strategy_name="daily_participation_sma",
        pair="KRW-BTC",
        interval="1m",
        as_of_ts_ms=20,
        mark_price=100.0,
        risk_scope_id="cabccc",
    )

    assert snapshot.current_drawdown_metric is not None
    assert snapshot.current_drawdown_metric.state == "valid"
    assert float(snapshot.current_drawdown_metric.value or 0.0) < 1.0


def test_h74_observation_policy_declares_risk_capital_basis() -> None:
    payload = build_h74_source_observation_authority_payload(source_candidate_artifact_hash="sha256:" + "a" * 64)
    bound = payload["hash_bound_parameters"]

    assert bound["risk_capital_basis"] == "fixed_observation_notional"
    assert bound["risk_capital_krw"] == 100_000.0
