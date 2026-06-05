from __future__ import annotations

import ast
from pathlib import Path

import pytest

from bithumb_bot.reason_codes import POSITION_LOSS_LIMIT
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, set_portfolio_breakdown
from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest
from bithumb_bot.risk import DAILY_LOSS_LIMIT_REASON_CODE, RISK_STATE_MISMATCH, fetch_recent_risk_evaluations
from bithumb_bot.risk_contract import RiskPolicy, RiskSnapshot, SubmitPlan
from bithumb_bot.risk_policy_engine import RiskPolicyEngine
from bithumb_bot.runtime_risk_engine import RuntimeRiskEngineAdapter


ROOT = Path(__file__).resolve().parents[1]


def _policy() -> RiskPolicy:
    return RiskPolicy(
        max_daily_loss_krw=30_000.0,
        max_position_loss_pct=10.0,
        max_daily_order_count=2,
        max_trade_count_per_day=4,
        max_drawdown_pct=20.0,
        cooldown_after_loss_min=15,
        kill_switch=False,
        max_open_positions=1,
        source="test_policy",
    )


def _snapshot(**overrides: object) -> RiskSnapshot:
    payload = {
        "evaluation_ts_ms": 1,
        "mark_price": 100.0,
        "current_equity": 1_000_000.0,
        "baseline_equity": 1_000_000.0,
        "loss_today": 0.0,
        "current_cash_krw": 1_000_000.0,
        "current_asset_qty": 0.0,
        "position_entry_price": None,
        "state_source": "test_vector",
    }
    payload.update(overrides)
    return RiskSnapshot(**payload)  # type: ignore[arg-type]


def _decisions(snapshot: RiskSnapshot, *, pre_submit: bool = False):
    policy = _policy()
    engines = {
        "research": RiskPolicyEngine(policy),
        "paper": RiskPolicyEngine(policy),
        "live": RiskPolicyEngine(policy),
    }
    if pre_submit:
        plan = SubmitPlan(side="BUY", qty=0.1, source="test")
        return {name: engine.evaluate_pre_submit(plan, snapshot) for name, engine in engines.items()}
    return {name: engine.evaluate_pre_decision(snapshot) for name, engine in engines.items()}


@pytest.mark.parametrize(
    ("snapshot", "reason_code", "pre_submit"),
    [
        (
            _snapshot(current_equity=950_000.0, baseline_equity=1_000_000.0, loss_today=50_000.0),
            DAILY_LOSS_LIMIT_REASON_CODE,
            False,
        ),
        (
            _snapshot(current_asset_qty=1.0, position_entry_price=100.0, mark_price=80.0),
            POSITION_LOSS_LIMIT,
            False,
        ),
        (
            _snapshot(broker_local_mismatch=True, recovery_risk_mismatch_reason="broker/local mismatch"),
            RISK_STATE_MISMATCH,
            False,
        ),
        (
            _snapshot(
                unresolved_order_blocked=True,
                unresolved_order_reason_code="UNRESOLVED_OPEN_ORDER_PRESENT",
                unresolved_order_reason="unresolved open order exists",
            ),
            "UNRESOLVED_OPEN_ORDER_PRESENT",
            True,
        ),
        (_snapshot(duplicate_entry=True, current_asset_qty=0.5), "DUPLICATE_ENTRY", False),
        (_snapshot(daily_order_count=2), "MAX_DAILY_ORDER_COUNT", False),
        (_snapshot(daily_trade_count=4), "MAX_TRADE_COUNT_PER_DAY", False),
        (_snapshot(current_drawdown_pct=20.0), "MAX_DRAWDOWN_PCT", False),
        (_snapshot(minutes_since_last_loss=5.0), "COOLDOWN_AFTER_LOSS", False),
    ],
)
def test_risk_parity_vectors_share_decision_identity(
    snapshot: RiskSnapshot,
    reason_code: str,
    pre_submit: bool,
) -> None:
    decisions = _decisions(snapshot, pre_submit=pre_submit)
    first = next(iter(decisions.values()))

    assert first.reason_code == reason_code
    for decision in decisions.values():
        assert decision.reason_code == first.reason_code
        assert decision.status == first.status
        assert decision.allowed_actions == first.allowed_actions
        assert decision.risk_input_hash == first.risk_input_hash
        assert decision.risk_policy_hash == first.risk_policy_hash


def test_risk_decision_identity_fields_are_trace_compatible() -> None:
    decision = RiskPolicyEngine(_policy()).evaluate_pre_decision(
        _snapshot(current_equity=950_000.0, baseline_equity=1_000_000.0, loss_today=50_000.0)
    )

    fields = decision.identity_fields()

    assert set(fields) == {
        "risk_input_hash",
        "risk_policy_hash",
        "risk_decision_hash",
        "risk_reason_code",
        "risk_status",
        "risk_evaluation_point",
        "risk_state_source",
        "effective_risk_limits",
    }
    assert fields["risk_decision_hash"] == decision.risk_decision_hash
    assert fields["risk_reason_code"] == DAILY_LOSS_LIMIT_REASON_CODE


def test_strategy_level_risk_policy_schema_contains_required_fields() -> None:
    payload = _policy().as_dict()

    assert payload["max_daily_loss_krw"] == pytest.approx(30_000.0)
    assert payload["max_daily_order_count"] == 2
    assert payload["max_trade_count_per_day"] == 4
    assert payload["max_drawdown_pct"] == pytest.approx(20.0)
    assert payload["cooldown_after_loss_min"] == 15


def test_runtime_risk_evaluation_records_typed_decision_identity(tmp_path) -> None:
    original = {
        "DB_PATH": settings.DB_PATH,
        "MODE": settings.MODE,
        "START_CASH_KRW": settings.START_CASH_KRW,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "MAX_POSITION_LOSS_PCT": settings.MAX_POSITION_LOSS_PCT,
        "MAX_DAILY_ORDER_COUNT": settings.MAX_DAILY_ORDER_COUNT,
        "KILL_SWITCH": settings.KILL_SWITCH,
    }
    try:
        db_path = tmp_path / "risk.sqlite"
        object.__setattr__(settings, "DB_PATH", str(db_path))
        object.__setattr__(settings, "MODE", "paper")
        object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
        object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 30_000.0)
        object.__setattr__(settings, "MAX_POSITION_LOSS_PCT", 0.0)
        object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)
        object.__setattr__(settings, "KILL_SWITCH", False)
        conn = ensure_db(str(db_path))
        set_portfolio_breakdown(
            conn,
            cash_available=1_000_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )
        RuntimeRiskEngineAdapter(conn).evaluate_buy_intent(
            ts_ms=1_800_000_000_000,
            cash=1_000_000.0,
            qty=0.0,
            price=100.0,
            evaluation_origin="test_runtime_risk_identity_seed",
        )
        set_portfolio_breakdown(
            conn,
            cash_available=950_000.0,
            cash_locked=0.0,
            asset_available=0.0,
            asset_locked=0.0,
        )

        decision = RuntimeRiskEngineAdapter(conn).evaluate_buy_intent(
            ts_ms=1_800_000_000_000,
            cash=950_000.0,
            qty=0.0,
            price=100.0,
            evaluation_origin="test_runtime_risk_identity",
        )
        recent = fetch_recent_risk_evaluations(conn, limit=1)[0]
    finally:
        for key, value in original.items():
            object.__setattr__(settings, key, value)

    assert decision.reason_code == DAILY_LOSS_LIMIT_REASON_CODE
    assert recent["risk_input_hash"] == decision.risk_input_hash
    assert recent["risk_policy_hash"] == decision.risk_policy_hash
    assert recent["risk_decision_hash"] == decision.risk_decision_hash
    assert recent["risk_status"] == decision.status
    assert recent["risk_evaluation_point"] == "pre_decision"
    assert recent["effective_risk_limits"]["max_daily_loss_krw"] == 30_000.0


def test_unresolved_order_gate_classification_is_reused_from_one_decision() -> None:
    decision = RiskPolicyEngine(_policy()).evaluate_pre_submit(
        SubmitPlan(side="BUY", qty=0.1, source="test"),
        _snapshot(
            unresolved_order_blocked=True,
            unresolved_order_reason_code="SUBMIT_UNKNOWN_PRESENT",
            unresolved_order_reason="submit-unknown unresolved order exists",
            evidence={"unresolved_order_gate": {"evaluated_once": True}},
        ),
    )

    assert decision.reason_code == "SUBMIT_UNKNOWN_PRESENT"
    assert decision.evidence["unresolved_order_gate"]["reason_code"] == "SUBMIT_UNKNOWN_PRESENT"  # type: ignore[index]


def test_production_manifest_without_risk_policy_fails_closed() -> None:
    payload = _production_manifest()
    payload.pop("risk_policy", None)

    with pytest.raises(ManifestValidationError, match="risk_policy is required"):
        parse_manifest(payload)


def test_research_manifest_missing_risk_policy_is_disabled_explicit() -> None:
    payload = _production_manifest()
    payload["deployment_tier"] = "research_only"
    payload.pop("risk_policy", None)

    manifest = parse_manifest(payload)

    assert manifest.risk_policy.policy_status == "disabled_explicit"


def test_tuple_guardrails_are_not_called_from_runtime_execution_paths() -> None:
    blocked_calls = {
        "evaluate_buy_guardrails",
        "evaluate_order_submission_halt",
        "evaluate_unresolved_order_gate",
    }
    checked = [
        ROOT / "src/bithumb_bot/broker/live.py",
        ROOT / "src/bithumb_bot/broker/paper.py",
        ROOT / "src/bithumb_bot/broker/live_submission_execution.py",
        ROOT / "src/bithumb_bot/operator_commands.py",
    ]
    violations: list[str] = []
    for path in checked:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in blocked_calls:
                violations.append(f"{path.relative_to(ROOT)}:{node.lineno}:{node.func.id}")

    assert violations == []


def _production_manifest() -> dict[str, object]:
    return {
        "experiment_id": "risk_policy_test",
        "hypothesis": "Risk policy is explicit.",
        "strategy_name": "sma_with_filter",
        "market": "KRW-BTC",
        "interval": "1m",
        "deployment_tier": "paper_candidate",
        "dataset": {
            "source": "sqlite_candles",
            "snapshot_id": "candles_v1",
            "top_of_book": {"required": True, "missing_policy": "fail"},
            "train": {"start": "2023-01-01", "end": "2023-01-01"},
            "validation": {"start": "2023-01-02", "end": "2023-01-02"},
            "final_holdout": {"start": "2023-01-03", "end": "2023-01-03"},
        },
        "parameter_space": {
            "SMA_SHORT": [2],
            "SMA_LONG": [4],
            "SMA_FILTER_GAP_MIN_RATIO": [0.0],
            "SMA_FILTER_VOL_WINDOW": [10],
            "SMA_FILTER_VOL_MIN_RANGE_RATIO": [0.0],
            "SMA_FILTER_OVEREXT_LOOKBACK": [3],
            "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": [0.02],
            "SMA_MARKET_REGIME_ENABLED": [True],
            "SMA_COST_EDGE_ENABLED": [True],
            "SMA_COST_EDGE_MIN_RATIO": [0.0],
            "ENTRY_EDGE_BUFFER_RATIO": [0.0005],
            "STRATEGY_MIN_EXPECTED_EDGE_RATIO": [0.0],
            "STRATEGY_ENTRY_SLIPPAGE_BPS": [0.0],
            "LIVE_FEE_RATE_ESTIMATE": [0.001],
            "STRATEGY_EXIT_RULES": ["stop_loss,opposite_cross,max_holding_time"],
            "STRATEGY_EXIT_STOP_LOSS_RATIO": [0.0],
            "STRATEGY_EXIT_MAX_HOLDING_MIN": [0],
            "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": [0.0],
            "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": [0.0],
        },
        "cost_model": {"fee_rate": 0.001, "slippage_bps": [0]},
        "portfolio_policy": {
            "schema_version": 1,
            "starting_cash_krw": 1_000_000,
            "quote_currency": "KRW",
            "initial_position_qty": 0.0,
            "cash_interest_policy": "zero",
            "position_sizing": {
                "type": "fractional_cash",
                "buy_fraction": 0.99,
                "sell_policy": "sell_all_available_position",
                "cash_buffer_policy": "retain_1_percent_before_fees",
                "min_order_krw": None,
                "max_order_krw": None,
                "rounding_policy": "engine_float_no_exchange_lot_rounding",
            },
            "source": "manifest",
        },
        "risk_policy": {
            "schema_version": 1,
            "max_daily_loss_krw": 30000,
            "max_position_loss_pct": 10.0,
            "max_daily_order_count": 20,
            "kill_switch": False,
            "max_open_positions": 1,
            "unresolved_order_policy": "block",
            "missing_policy": "fail_closed_for_promotion",
        },
        "acceptance_gate": {
            "min_trade_count": 1,
            "max_mdd_pct": 50,
            "min_profit_factor": 1.0,
            "oos_return_must_be_positive": True,
            "parameter_stability_required": False,
        },
    }
