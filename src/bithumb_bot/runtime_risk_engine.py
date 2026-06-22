from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Mapping

from .config import settings
from .oms import collect_risky_order_state
from .risk import (
    _count_orders_today,
    _latest_position_entry_price,
    daily_loss_reason_code_from_reason,
    evaluate_daily_loss_state,
)
from .risk_contract import RiskDecision, RiskPolicy, RiskSnapshot, SubmitPlan
from .risk_policy_engine import RiskPolicyEngine
from .strategy_risk_profile import risk_policy_from_mapping


def settings_risk_policy() -> RiskPolicy:
    return RiskPolicy(
        schema_version=1,
        max_daily_loss_krw=float(settings.MAX_DAILY_LOSS_KRW),
        max_position_loss_pct=float(settings.MAX_POSITION_LOSS_PCT),
        max_daily_order_count=int(settings.MAX_DAILY_ORDER_COUNT),
        kill_switch=bool(settings.KILL_SWITCH),
        max_open_positions=int(settings.MAX_OPEN_POSITIONS),
        unresolved_order_policy="block",
        policy_status="enabled",
        source="runtime_settings",
    )


@dataclass(frozen=True)
class EffectivePreSubmitRiskPolicy:
    policy: RiskPolicy
    risk_policy_source: str
    strategy_instance_ids: tuple[str, ...] = ()
    strategy_risk_profile_hashes: tuple[str, ...] = ()
    strategy_risk_policy_hashes: tuple[str, ...] = ()
    portfolio_risk_policy_hash: str | None = None
    operational_risk_policy_hash: str | None = None
    residual_risk_policy_hash: str | None = None
    composition_rule: str = "settings_fallback_no_plan_bound_strategy_policy"

    def evidence_fields(self) -> dict[str, object]:
        return {
            "risk_policy_source": self.risk_policy_source,
            "pre_submit_risk_policy_source": self.risk_policy_source,
            "pre_submit_risk_policy_composition_rule": self.composition_rule,
            "strategy_instance_ids": list(self.strategy_instance_ids),
            "strategy_risk_profile_hashes": list(self.strategy_risk_profile_hashes),
            "strategy_risk_policy_hashes": list(self.strategy_risk_policy_hashes),
            "portfolio_risk_policy_hash": self.portfolio_risk_policy_hash,
            "operational_risk_policy_hash": self.operational_risk_policy_hash,
            "residual_risk_policy_hash": self.residual_risk_policy_hash,
            "effective_pre_submit_risk_policy": self.policy.as_dict(),
            "effective_pre_submit_risk_policy_hash": self.policy.policy_hash(),
        }


def resolve_effective_pre_submit_risk_policy(
    submit_payload: Mapping[str, object],
) -> EffectivePreSubmitRiskPolicy:
    strategy_profiles = _strategy_risk_profiles_from_submit_payload(submit_payload)
    source = str(submit_payload.get("source") or "").strip()
    pre_submit_required = bool(submit_payload.get("pre_submit_risk_required"))
    if (
        not strategy_profiles
        and source == "target_delta"
        and pre_submit_required
    ):
        raise ValueError("pre_submit_strategy_risk_profiles_missing_for_target_delta")
    if strategy_profiles:
        policies: list[RiskPolicy] = []
        instance_ids: list[str] = []
        profile_hashes: list[str] = []
        policy_hashes: list[str] = []
        for profile in strategy_profiles:
            raw_policy = profile.get("risk_policy")
            if not isinstance(raw_policy, Mapping):
                raise ValueError("pre_submit_strategy_risk_policy_missing")
            policy = risk_policy_from_mapping(raw_policy)
            policy_hash = policy.policy_hash()
            declared_policy_hash = str(profile.get("risk_policy_hash") or "").strip()
            if declared_policy_hash and declared_policy_hash != policy_hash:
                raise ValueError("pre_submit_strategy_risk_policy_hash_mismatch")
            policies.append(policy)
            instance_id = str(profile.get("strategy_instance_id") or "").strip()
            if instance_id:
                instance_ids.append(instance_id)
            profile_hash = str(profile.get("strategy_risk_profile_hash") or profile.get("profile_hash") or "").strip()
            if profile_hash:
                profile_hashes.append(profile_hash)
            policy_hashes.append(policy_hash)
        return EffectivePreSubmitRiskPolicy(
            policy=_compose_most_restrictive_strategy_policies(policies),
            risk_policy_source="strategy_risk_profiles",
            strategy_instance_ids=tuple(sorted(set(instance_ids))),
            strategy_risk_profile_hashes=tuple(sorted(set(profile_hashes))),
            strategy_risk_policy_hashes=tuple(sorted(set(policy_hashes))),
            portfolio_risk_policy_hash=(
                str(submit_payload.get("portfolio_risk_policy_hash") or "").strip() or None
            ),
            composition_rule="most_restrictive_selected_strategy_policy",
        )
    explicit_policy = _explicit_non_target_pre_submit_policy(submit_payload)
    if explicit_policy is not None:
        policy, policy_source, declared_hash, composition_rule = explicit_policy
        policy_hash = policy.policy_hash()
        if declared_hash and declared_hash != policy_hash:
            raise ValueError(f"pre_submit_{policy_source}_policy_hash_mismatch")
        return EffectivePreSubmitRiskPolicy(
            policy=policy,
            risk_policy_source=policy_source,
            portfolio_risk_policy_hash=(
                policy_hash
                if policy_source == "portfolio_risk_policy"
                else str(submit_payload.get("portfolio_risk_policy_hash") or "").strip() or None
            ),
            operational_risk_policy_hash=(
                policy_hash if policy_source == "operational_risk_policy" else None
            ),
            residual_risk_policy_hash=(
                policy_hash if policy_source == "residual_risk_policy" else None
            ),
            composition_rule=composition_rule,
        )
    if source == "residual_inventory" and pre_submit_required:
        raise ValueError("pre_submit_explicit_residual_risk_policy_missing")
    if pre_submit_required:
        raise ValueError("pre_submit_runtime_settings_fallback_rejected_for_live_real")
    return EffectivePreSubmitRiskPolicy(
        policy=settings_risk_policy(),
        risk_policy_source="runtime_settings_fallback",
        portfolio_risk_policy_hash=(
            str(submit_payload.get("portfolio_risk_policy_hash") or "").strip() or None
        ),
    )


def _explicit_non_target_pre_submit_policy(
    submit_payload: Mapping[str, object],
) -> tuple[RiskPolicy, str, str | None, str] | None:
    candidates = (
        (
            "residual_risk_policy",
            "residual_risk_policy_hash",
            "residual_risk_policy",
            "explicit_residual_pre_submit_policy",
        ),
        (
            "operational_risk_policy",
            "operational_risk_policy_hash",
            "operational_risk_policy",
            "explicit_operational_pre_submit_policy",
        ),
        (
            "portfolio_risk_policy",
            "portfolio_risk_policy_hash",
            "portfolio_risk_policy",
            "explicit_portfolio_pre_submit_policy",
        ),
    )
    for policy_field, hash_field, source, rule in candidates:
        raw_policy = submit_payload.get(policy_field)
        if not isinstance(raw_policy, Mapping):
            continue
        return (
            risk_policy_from_mapping(raw_policy),
            source,
            str(submit_payload.get(hash_field) or "").strip() or None,
            rule,
        )
    return None


def _strategy_risk_profiles_from_submit_payload(
    submit_payload: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    raw_profiles = submit_payload.get("strategy_risk_profiles")
    profiles: list[Mapping[str, object]] = []
    if isinstance(raw_profiles, list | tuple):
        profiles.extend(item for item in raw_profiles if isinstance(item, Mapping))
    raw_profile = submit_payload.get("strategy_risk_profile")
    if isinstance(raw_profile, Mapping):
        profiles.append(raw_profile)
    return tuple(profiles)


def _positive_min(values: list[float | int]) -> float:
    positives = [float(value) for value in values if float(value) > 0.0]
    return min(positives) if positives else 0.0


def _compose_most_restrictive_strategy_policies(policies: list[RiskPolicy]) -> RiskPolicy:
    if not policies:
        return settings_risk_policy()
    enabled = [policy for policy in policies if policy.policy_status != "disabled_explicit"]
    source_policies = enabled or policies
    unresolved_policy = (
        "block"
        if any(policy.unresolved_order_policy == "block" for policy in source_policies)
        else str(source_policies[0].unresolved_order_policy)
    )
    return RiskPolicy(
        schema_version=1,
        max_daily_loss_krw=_positive_min([policy.max_daily_loss_krw for policy in source_policies]),
        max_position_loss_pct=_positive_min([policy.max_position_loss_pct for policy in source_policies]),
        max_daily_order_count=int(_positive_min([policy.max_daily_order_count for policy in source_policies])),
        max_trade_count_per_day=int(
            _positive_min([policy.max_trade_count_per_day for policy in source_policies])
        ),
        max_drawdown_pct=_positive_min([policy.max_drawdown_pct for policy in source_policies]),
        cooldown_after_loss_min=int(
            _positive_min([policy.cooldown_after_loss_min for policy in source_policies])
        ),
        kill_switch=any(policy.kill_switch for policy in source_policies),
        max_open_positions=max(1, min(max(1, int(policy.max_open_positions)) for policy in source_policies)),
        unresolved_order_policy=unresolved_policy,
        policy_status=("enabled" if enabled else "disabled_explicit"),
        missing_policy=(
            "fail_closed_for_live"
            if any(policy.missing_policy == "fail_closed_for_live" for policy in source_policies)
            else str(source_policies[0].missing_policy)
        ),
        source="composed_selected_strategy_risk_profiles",
    )


@dataclass(frozen=True)
class RuntimeRiskEngineAdapter:
    conn: sqlite3.Connection
    policy: RiskPolicy | None = None

    def evaluate_buy_intent(
        self,
        *,
        ts_ms: int,
        cash: float,
        qty: float,
        price: float,
        broker: object | None = None,
        mark_price_source: str = "market_price",
        evaluation_origin: str = "buy_guardrails",
    ) -> RiskDecision:
        policy = self.policy or settings_risk_policy()
        snapshot = self._snapshot(
            ts_ms=ts_ms,
            now_ms=ts_ms,
            cash=cash,
            qty=qty,
            price=price,
            broker=broker,
            mark_price_source=mark_price_source,
            evaluation_origin=evaluation_origin,
            include_unresolved_order_gate=False,
            duplicate_entry=float(qty) > 1e-12,
        )
        decision = RiskPolicyEngine(policy).evaluate_pre_decision(snapshot)
        _record_typed_decision_identity(
            self.conn,
            decision=decision,
            evaluation_ts_ms=int(ts_ms),
            evaluation_origin=evaluation_origin,
        )
        return decision

    def evaluate_pre_submit(  # broker= is required by live real-order callers.
        self,
        *,
        plan: SubmitPlan,
        ts_ms: int,
        now_ms: int,
        cash: float,
        submit_qty: float | None = None,
        current_asset_qty: float | None = None,
        qty: float | None = None,
        price: float,
        broker: object | None = None,
        mark_price_source: str = "market_price",
        evaluation_origin: str = "submission_halt",
    ) -> RiskDecision:
        policy = self.policy or settings_risk_policy()
        snapshot = self._snapshot(
            ts_ms=ts_ms,
            now_ms=now_ms,
            cash=cash,
            submit_qty=submit_qty,
            current_asset_qty=current_asset_qty,
            legacy_qty=qty,
            price=price,
            broker=broker,
            mark_price_source=mark_price_source,
            evaluation_origin=evaluation_origin,
            include_unresolved_order_gate=True,
            duplicate_entry=False,
        )
        decision = RiskPolicyEngine(policy).evaluate_pre_submit(plan, snapshot)  # broker=not_applicable_pure_policy
        _record_typed_decision_identity(
            self.conn,
            decision=decision,
            evaluation_ts_ms=int(ts_ms),
            evaluation_origin=evaluation_origin,
        )
        return decision

    def _snapshot(
        self,
        *,
        ts_ms: int,
        now_ms: int,
        cash: float,
        submit_qty: float | None,
        current_asset_qty: float | None,
        legacy_qty: float | None,
        price: float,
        broker: object | None,
        mark_price_source: str,
        evaluation_origin: str,
        include_unresolved_order_gate: bool,
        duplicate_entry: bool,
    ) -> RiskSnapshot:
        del cash
        daily = evaluate_daily_loss_state(
            self.conn,
            ts_ms=int(ts_ms),
            price=float(price),
            broker=broker,
            mark_price_source=mark_price_source,
            evaluation_origin=evaluation_origin,
        )
        mismatch = daily.reason_code == "RISK_STATE_MISMATCH" or daily_loss_reason_code_from_reason(
            daily.reason
        ) == "RISK_STATE_MISMATCH"
        if daily.current_asset_qty is not None:
            resolved_current_asset_qty = float(daily.current_asset_qty)
            current_asset_qty_source = "broker_current_position"
        elif current_asset_qty is not None:
            resolved_current_asset_qty = float(current_asset_qty)
            current_asset_qty_source = "explicit_current_position"
        elif legacy_qty is not None:
            resolved_current_asset_qty = float(legacy_qty)
            current_asset_qty_source = "legacy_qty_compatibility"
        else:
            resolved_current_asset_qty = 0.0
            current_asset_qty_source = "missing_default_zero"
        if submit_qty is None:
            submit_qty = float(plan.qty)
        unresolved_blocked = False
        unresolved_reason_code = "OK"
        unresolved_reason = "ok"
        unresolved_state: dict[str, object] = {}
        if include_unresolved_order_gate:
            unresolved_state = dict(
                collect_risky_order_state(
                    self.conn,
                    now_ms=int(now_ms),
                    max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
                )
            )
            unresolved_blocked, unresolved_reason_code, unresolved_reason = _classify_unresolved_state(
                unresolved_state,
                max_open_order_age_sec=int(settings.MAX_OPEN_ORDER_AGE_SEC),
            )
        return RiskSnapshot(
            evaluation_ts_ms=int(ts_ms),
            mark_price=float(price),
            current_equity=daily.current_equity,
            baseline_equity=daily.start_equity,
            loss_today=daily.loss_today,
            current_cash_krw=daily.current_cash_krw,
            current_asset_qty=float(resolved_current_asset_qty),
            position_entry_price=_latest_position_entry_price(self.conn),
            broker_local_mismatch=bool(mismatch),
            recovery_risk_mismatch_reason=daily.reason if mismatch else None,
            duplicate_entry=bool(duplicate_entry),
            daily_order_count=_count_orders_today(self.conn, int(ts_ms)),
            unresolved_order_blocked=bool(unresolved_blocked),
            unresolved_order_reason_code=str(unresolved_reason_code),
            unresolved_order_reason=str(unresolved_reason),
            state_source="runtime_db_broker",
            evidence={
                "daily_loss_evaluation": {
                    "reason_code": daily.reason_code,
                    "decision": daily.decision,
                    "day_kst": daily.day_kst,
                    "mark_price_source": daily.mark_price_source,
                },
                "current_asset_qty_source": current_asset_qty_source,
                "submit_plan_qty_source": "submit_plan.qty",
                "submit_qty": float(submit_qty),
                "current_asset_qty": float(resolved_current_asset_qty),
                "daily_order_count_scope": "account_global",
                "daily_order_count_source": "orders.created_ts_kst_day",
                "unresolved_order_gate": {
                    "blocked": bool(unresolved_blocked),
                    "reason_code": str(unresolved_reason_code),
                    "reason": str(unresolved_reason),
                    "state": unresolved_state,
                    "evaluated_once": bool(include_unresolved_order_gate),
                },
            },
        )


def _classify_unresolved_state(
    state: dict[str, Any],
    *,
    max_open_order_age_sec: int,
) -> tuple[bool, str, str]:
    if int(state.get("submit_unknown_count") or 0) > 0:
        return True, "SUBMIT_UNKNOWN_PRESENT", "submit-unknown unresolved order exists"
    if int(state.get("accounting_pending_count") or 0) > 0:
        return True, "ACCOUNTING_PENDING_PRESENT", "accounting-pending order exists"
    if int(state.get("recovery_required_count") or 0) > 0:
        return True, "RECOVERY_REQUIRED_PRESENT", "recovery-required order exists"
    open_count = int(state.get("unresolved_open_order_count") or 0)
    if open_count <= 0:
        return False, "OK", "ok"
    age_sec = float(state.get("oldest_unresolved_open_order_age_sec") or 0.0)
    max_age_sec = max(1, int(max_open_order_age_sec))
    if age_sec > max_age_sec:
        return (
            True,
            "STALE_UNRESOLVED_OPEN_ORDER",
            f"stale unresolved open order exists: age={age_sec:.1f}s > {max_age_sec}s",
        )
    return True, "UNRESOLVED_OPEN_ORDER_PRESENT", "unresolved open order exists"


def _record_typed_decision_identity(
    conn: sqlite3.Connection,
    *,
    decision: RiskDecision,
    evaluation_ts_ms: int,
    evaluation_origin: str,
) -> None:
    _ensure_typed_risk_columns(conn)
    had_tx = conn.in_transaction
    conn.execute(
        """
        UPDATE risk_evaluations
        SET
            risk_input_hash=?,
            risk_policy_hash=?,
            risk_evidence_hash=?,
            risk_decision_hash=?,
            risk_reason_code=?,
            risk_status=?,
            risk_evaluation_point=?,
            risk_state_source=?,
            effective_risk_limits_json=?
        WHERE id = (
            SELECT id
            FROM risk_evaluations
            WHERE evaluation_ts_ms=? AND evaluation_origin=?
            ORDER BY id DESC
            LIMIT 1
        )
        """,
        (
            decision.risk_input_hash,
            decision.risk_policy_hash,
            decision.risk_evidence_hash,
            decision.risk_decision_hash,
            decision.reason_code,
            decision.status,
            decision.evaluation_point,
            decision.state_source,
            json.dumps(decision.effective_limits, ensure_ascii=False, sort_keys=True),
            int(evaluation_ts_ms),
            str(evaluation_origin),
        ),
    )
    if not had_tx:
        conn.commit()


def _ensure_typed_risk_columns(conn: sqlite3.Connection) -> None:
    columns = {
        str(row["name"]) if hasattr(row, "keys") else str(row[1])
        for row in conn.execute("PRAGMA table_info(risk_evaluations)").fetchall()
    }
    for name, ddl in (
        ("risk_input_hash", "risk_input_hash TEXT"),
        ("risk_policy_hash", "risk_policy_hash TEXT"),
        ("risk_evidence_hash", "risk_evidence_hash TEXT"),
        ("risk_decision_hash", "risk_decision_hash TEXT"),
        ("risk_reason_code", "risk_reason_code TEXT"),
        ("risk_status", "risk_status TEXT"),
        ("risk_evaluation_point", "risk_evaluation_point TEXT"),
        ("risk_state_source", "risk_state_source TEXT"),
        ("effective_risk_limits_json", "effective_risk_limits_json TEXT"),
    ):
        if name not in columns:
            conn.execute(f"ALTER TABLE risk_evaluations ADD COLUMN {ddl}")
