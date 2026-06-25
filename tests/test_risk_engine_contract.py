from __future__ import annotations

import ast
from pathlib import Path

import pytest

from bithumb_bot.reason_codes import POSITION_LOSS_LIMIT
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db, record_strategy_decision, set_portfolio_breakdown
from bithumb_bot.research.experiment_manifest import ManifestValidationError, parse_manifest
from bithumb_bot.risk import DAILY_LOSS_LIMIT_REASON_CODE, RISK_STATE_MISMATCH, fetch_recent_risk_evaluations
from bithumb_bot.risk_contract import RiskMetric, RiskPolicy, RiskSnapshot, SubmitPlan
from bithumb_bot.risk_policy_engine import RiskPolicyEngine
from bithumb_bot.runtime_risk_engine import RuntimeRiskEngineAdapter
from bithumb_bot.strategy_risk_profile import strategy_risk_profile_from_profile_payload
from bithumb_bot.strategy_risk_state import StrategyRiskStateProvider, missing_required_risk_state


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
        (
            _snapshot(
                current_drawdown_metric=RiskMetric(
                    value=20.0,
                    unit="percent_point",
                    scope="risk_scope",
                    denominator_kind="allocated_capital",
                    denominator_value=100_000.0,
                    sample_count=1,
                    state="valid",
                    source_table="trade_lifecycles",
                    formula_version="unit",
                )
            ),
            "MAX_DRAWDOWN_PCT",
            False,
        ),
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
        assert decision.risk_evidence_hash == first.risk_evidence_hash


def test_risk_decision_identity_fields_are_trace_compatible() -> None:
    decision = RiskPolicyEngine(_policy()).evaluate_pre_decision(
        _snapshot(current_equity=950_000.0, baseline_equity=1_000_000.0, loss_today=50_000.0)
    )

    fields = decision.identity_fields()

    assert set(fields) == {
        "risk_input_hash",
        "risk_policy_hash",
        "risk_evidence_hash",
        "risk_decision_hash",
        "risk_reason_code",
        "risk_status",
        "risk_evaluation_point",
        "risk_state_source",
        "effective_risk_limits",
    }
    assert fields["risk_decision_hash"] == decision.risk_decision_hash
    assert fields["risk_evidence_hash"] == decision.risk_evidence_hash
    assert fields["risk_reason_code"] == DAILY_LOSS_LIMIT_REASON_CODE


def test_strategy_level_risk_policy_schema_contains_required_fields() -> None:
    payload = _policy().as_dict()

    assert payload["max_daily_loss_krw"] == pytest.approx(30_000.0)
    assert payload["max_daily_order_count"] == 2
    assert payload["max_trade_count_per_day"] == 4
    assert payload["max_drawdown_pct"] == pytest.approx(20.0)
    assert payload["cooldown_after_loss_min"] == 15


def test_typed_strategy_risk_profile_binds_policy_hash_and_scope() -> None:
    profile = strategy_risk_profile_from_profile_payload(
        strategy_instance_id="sma:unit",
        strategy_name="sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        profile_payload={
            "risk_policy": _policy().as_dict(),
            "risk_enforcement_mode": "enforced",
            "missing_risk_policy_behavior": "fail_closed_for_live",
        },
        approved_runtime_profile_path="/runtime/profile.json",
        approved_runtime_profile_hash="sha256:profile",
        live_like=True,
        live_real_order=True,
    )

    assert profile is not None
    assert profile.strategy_instance_id == "sma:unit"
    assert profile.policy == _policy()
    assert profile.risk_policy_hash == _policy().policy_hash()
    assert profile.profile_hash().startswith("sha256:")
    assert profile.as_dict()["approved_runtime_profile_hash"] == "sha256:profile"


def test_strategy_risk_state_provider_derives_reproducible_snapshot_and_blocks_count_limit(tmp_path) -> None:
    db_path = tmp_path / "strategy-risk.sqlite"
    conn = ensure_db(str(db_path))
    set_portfolio_breakdown(
        conn,
        cash_available=1_000_000.0,
        cash_locked=0.0,
        asset_available=0.0,
        asset_locked=0.0,
    )
    decision_id = record_strategy_decision(
        conn,
        decision_ts=1_800_000_000_000,
        strategy_name="sma_with_filter",
        signal="BUY",
        reason="unit",
        candle_ts=1_800_000_000_000,
        market_price=100.0,
        context={
            "strategy_instance_id": "sma:unit",
            "pair": "KRW-BTC",
            "interval": "1m",
        },
    )
    conn.execute(
        """
        INSERT INTO orders(
            client_order_id, side, qty_req, qty_filled, status,
            entry_decision_id, created_ts, updated_ts
        )
        VALUES ('order-1', 'BUY', 0.1, 0.0, 'FILLED', ?, ?, ?)
        """,
        (decision_id, 1_800_000_000_000, 1_800_000_000_000),
    )
    conn.commit()
    policy = RiskPolicy(max_daily_order_count=1, source="unit")
    provider = StrategyRiskStateProvider(conn)

    first = provider.snapshot(
        strategy_instance_id="sma:unit",
        strategy_name="sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        as_of_ts_ms=1_800_000_000_001,
        mark_price=100.0,
        policy=policy,
        enforced=True,
    )
    second = provider.snapshot(
        strategy_instance_id="sma:unit",
        strategy_name="sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        as_of_ts_ms=1_800_000_000_001,
        mark_price=100.0,
        policy=policy,
        enforced=True,
    )
    decision = RiskPolicyEngine(policy).evaluate_pre_decision(first)

    assert first.input_hash() == second.input_hash()
    assert first.state_source == "runtime_db_strategy_instance_ledger"
    assert str(first.evidence["risk_state_evidence_hash"]).startswith("sha256:")
    assert first.evidence["scope"] == "strategy_instance"
    assert decision.reason_code == "MAX_DAILY_ORDER_COUNT"
    assert decision.status == "BLOCK"


def test_strategy_risk_state_provider_does_not_share_same_pair_instance_state(tmp_path) -> None:
    db_path = tmp_path / "strategy-risk-scope.sqlite"
    conn = ensure_db(str(db_path))
    alpha_decision_id = record_strategy_decision(
        conn,
        decision_ts=1_800_000_000_000,
        strategy_name="sma_with_filter",
        signal="BUY",
        reason="unit",
        candle_ts=1_800_000_000_000,
        market_price=100.0,
        context={"strategy_instance_id": "alpha", "pair": "KRW-BTC", "interval": "1m"},
    )
    beta_decision_id = record_strategy_decision(
        conn,
        decision_ts=1_800_000_000_000,
        strategy_name="sma_with_filter",
        signal="BUY",
        reason="unit",
        candle_ts=1_800_000_000_000,
        market_price=100.0,
        context={"strategy_instance_id": "beta", "pair": "KRW-BTC", "interval": "1m"},
    )
    conn.execute(
        """
        INSERT INTO orders(
            client_order_id, side, qty_req, qty_filled, status,
            entry_decision_id, created_ts, updated_ts
        )
        VALUES ('alpha-order', 'BUY', 0.1, 0.0, 'FILLED', ?, ?, ?)
        """,
        (alpha_decision_id, 1_800_000_000_000, 1_800_000_000_000),
    )
    conn.execute(
        """
        INSERT INTO open_position_lots(
            pair, entry_trade_id, entry_client_order_id, entry_ts, entry_price,
            qty_open, executable_lot_count, position_state, entry_decision_id
        )
        VALUES ('KRW-BTC', 1, 'alpha-order', ?, 100.0, 0.25, 1, 'open_exposure', ?)
        """,
        (1_800_000_000_000, alpha_decision_id),
    )
    conn.execute(
        """
        INSERT INTO trades(
            ts, pair, interval, side, price, qty, fee, cash_after, asset_after,
            entry_decision_id, strategy_name
        )
        VALUES (?, 'KRW-BTC', '1m', 'BUY', 100.0, 0.25, 0.0, 0.0, 0.25, ?, 'sma_with_filter')
        """,
        (1_800_000_000_000, alpha_decision_id),
    )
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            entry_trade_id, exit_trade_id, pair, entry_client_order_id, exit_client_order_id,
            entry_ts, exit_ts, matched_qty, entry_price, exit_price, gross_pnl, fee_total,
            net_pnl, holding_time_sec, strategy_name, strategy_instance_id
        )
        VALUES (1, 2, 'KRW-BTC', 'alpha-order', 'alpha-sell', ?, ?, 0.25, 100.0, 90.0,
                -10.0, 0.0, -10.0, 60.0, 'sma_with_filter', 'alpha')
        """,
        (1_800_000_000_000, 1_800_000_060_000),
    )
    conn.commit()
    provider = StrategyRiskStateProvider(conn)
    policy = RiskPolicy(
        max_daily_order_count=1,
        max_trade_count_per_day=1,
        max_daily_loss_krw=1.0,
        source="unit",
    )

    alpha = provider.snapshot(
        strategy_instance_id="alpha",
        strategy_name="sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        as_of_ts_ms=1_800_000_120_000,
        mark_price=100.0,
        policy=policy,
        enforced=True,
    )
    beta = provider.snapshot(
        strategy_instance_id="beta",
        strategy_name="sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        as_of_ts_ms=1_800_000_120_000,
        mark_price=100.0,
        policy=policy,
        enforced=True,
    )

    assert beta_decision_id != alpha_decision_id
    assert alpha.daily_order_count == 1
    assert beta.daily_order_count == 0
    assert alpha.daily_trade_count == 1
    assert beta.daily_trade_count == 0
    assert alpha.current_asset_qty == pytest.approx(0.25)
    assert beta.current_asset_qty == pytest.approx(0.0)
    assert alpha.position_entry_price == pytest.approx(100.0)
    assert beta.position_entry_price is None
    assert alpha.current_drawdown_metric is not None
    assert alpha.current_drawdown_metric.state == "undefined"
    assert beta.current_drawdown_metric is not None
    assert beta.current_drawdown_metric.state == "undefined"
    assert alpha.loss_today == pytest.approx(10.0)
    assert beta.loss_today == pytest.approx(0.0)
    assert alpha.evidence["state_derivation"]["position_entry_price"]["scope"] == "strategy_instance"
    assert alpha.evidence["state_derivation"]["current_drawdown_pct"]["scope"] == "risk_scope"


def test_enforced_strategy_risk_state_allows_flat_without_position_entry_price(tmp_path) -> None:
    db_path = tmp_path / "strategy-risk-missing-position.sqlite"
    conn = ensure_db(str(db_path))
    record_strategy_decision(
        conn,
        decision_ts=1_800_000_000_000,
        strategy_name="sma_with_filter",
        signal="BUY",
        reason="unit",
        candle_ts=1_800_000_000_000,
        market_price=100.0,
        context={"strategy_instance_id": "alpha", "pair": "KRW-BTC", "interval": "1m"},
    )
    conn.commit()
    policy = RiskPolicy(max_position_loss_pct=1.0, source="unit")
    snapshot = StrategyRiskStateProvider(conn).snapshot(
        strategy_instance_id="alpha",
        strategy_name="sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        as_of_ts_ms=1_800_000_120_000,
        mark_price=90.0,
        policy=policy,
        enforced=True,
    )

    assert snapshot.current_asset_qty == pytest.approx(0.0)
    assert snapshot.position_entry_price is None
    assert missing_required_risk_state(policy, snapshot) == ()
    assert "missing_required_risk_state" not in snapshot.evidence


def test_enforced_strategy_risk_state_fails_closed_without_scoped_position_state(tmp_path) -> None:
    db_path = tmp_path / "strategy-risk-position-table-missing.sqlite"
    conn = ensure_db(str(db_path))
    record_strategy_decision(
        conn,
        decision_ts=1_800_000_000_000,
        strategy_name="sma_with_filter",
        signal="BUY",
        reason="unit",
        candle_ts=1_800_000_000_000,
        market_price=100.0,
        context={"strategy_instance_id": "alpha", "pair": "KRW-BTC", "interval": "1m"},
    )
    conn.execute("DROP TABLE open_position_lots")
    conn.commit()
    policy = RiskPolicy(max_position_loss_pct=1.0, source="unit")
    snapshot = StrategyRiskStateProvider(conn).snapshot(
        strategy_instance_id="alpha",
        strategy_name="sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        as_of_ts_ms=1_800_000_120_000,
        mark_price=90.0,
        policy=policy,
        enforced=True,
    )

    assert snapshot.current_asset_qty is None
    assert snapshot.position_entry_price is None
    assert missing_required_risk_state(policy, snapshot) == ("position_loss_state",)
    assert snapshot.evidence["missing_required_risk_state"] == ["position_loss_state"]
    assert snapshot.evidence["missing_required_risk_state_behavior"] == "fail_closed"


def test_strategy_risk_state_treats_no_prior_loss_as_no_active_cooldown(tmp_path) -> None:
    db_path = tmp_path / "strategy-risk-no-prior-loss.sqlite"
    conn = ensure_db(str(db_path))
    record_strategy_decision(
        conn,
        decision_ts=1_800_000_000_000,
        strategy_name="sma_with_filter",
        signal="BUY",
        reason="unit",
        candle_ts=1_800_000_000_000,
        market_price=100.0,
        context={"strategy_instance_id": "alpha", "pair": "KRW-BTC", "interval": "1m"},
    )
    conn.commit()
    policy = RiskPolicy(cooldown_after_loss_min=15, source="unit")
    snapshot = StrategyRiskStateProvider(conn).snapshot(
        strategy_instance_id="alpha",
        strategy_name="sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        as_of_ts_ms=1_800_000_120_000,
        mark_price=100.0,
        policy=policy,
        enforced=True,
    )

    cooldown_evidence = snapshot.evidence["state_derivation"]["minutes_since_last_loss"]
    assert snapshot.minutes_since_last_loss is None
    assert cooldown_evidence["source_state"] == "no_prior_loss"
    assert missing_required_risk_state(policy, snapshot) == ()
    assert RiskPolicyEngine(policy).evaluate_pre_decision(snapshot).status == "ALLOW"


def test_enforced_strategy_risk_state_reports_missing_required_fields() -> None:
    policy = RiskPolicy(max_trade_count_per_day=1, max_drawdown_pct=5.0, source="unit")
    snapshot = _snapshot(daily_trade_count=None, current_drawdown_pct=None)

    assert missing_required_risk_state(policy, snapshot) == (
        "daily_trade_count",
        "current_drawdown_metric",
    )


def test_max_drawdown_block_uses_typed_metric_contract() -> None:
    decision = RiskPolicyEngine(_policy()).evaluate_pre_decision(
        _snapshot(
            current_drawdown_metric=RiskMetric(
                value=25.0,
                unit="percent_point",
                scope="risk_scope",
                denominator_kind="allocated_capital",
                denominator_value=100_000.0,
                sample_count=3,
                state="valid",
                source_table="trade_lifecycles",
                formula_version="unit",
            )
        )
    )

    assert decision.reason_code == "MAX_DRAWDOWN_PCT"
    assert decision.evidence["drawdown_metric_comparison"]["metric"]["state"] == "valid"


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
    assert recent["risk_evidence_hash"] == decision.risk_evidence_hash
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


def test_risk_budget_krw_not_used_in_arithmetic_authority_paths() -> None:
    allowed_paths = {
        ROOT / "src/bithumb_bot/risk_decision.py",
        ROOT / "src/bithumb_bot/strategy_preference.py",
        ROOT / "src/bithumb_bot/portfolio_allocation.py",
        ROOT / "src/bithumb_bot/runtime_strategy_set.py",
        ROOT / "src/bithumb_bot/db_core.py",
    }
    violations: list[str] = []
    for path in (ROOT / "src/bithumb_bot").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "risk_budget_krw" not in text:
            continue
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, ast.BinOp):
                segment = ast.get_source_segment(text, node) or ""
                if "risk_budget_krw" in segment and path not in allowed_paths:
                    violations.append(f"{path.relative_to(ROOT)}:{node.lineno}:{segment}")
            if isinstance(node, ast.Call):
                segment = ast.get_source_segment(text, node) or ""
                if (
                    "risk_budget_krw" in segment
                    and any(name in segment for name in ("min(", "max(", "float("))
                    and path not in allowed_paths
                ):
                    violations.append(f"{path.relative_to(ROOT)}:{node.lineno}:{segment}")
    assert violations == []


def test_non_live_missing_runtime_risk_policy_materializes_disabled_profile() -> None:
    profile = strategy_risk_profile_from_profile_payload(
        strategy_instance_id="research:unit",
        strategy_name="sma_with_filter",
        pair="KRW-BTC",
        interval="1m",
        profile_payload=None,
        approved_runtime_profile_path=None,
        approved_runtime_profile_hash=None,
        live_like=False,
        live_real_order=False,
    )

    assert profile is not None
    payload = profile.as_dict()
    assert payload["risk_profile_source"] == "research_missing_policy_explicit"
    assert payload["enforcement_mode"] == "telemetry"
    assert payload["missing_policy_behavior"] == "disabled_explicit"
    risk_policy = payload["risk_policy"]
    assert isinstance(risk_policy, dict)
    assert risk_policy["policy_status"] == "disabled_explicit"
    assert risk_policy["missing_policy"] == "disabled_explicit"
    assert risk_policy["source"] == "research_missing_policy_explicit"


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
