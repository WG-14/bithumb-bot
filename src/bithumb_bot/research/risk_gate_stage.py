from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.canonical_decision import canonical_payload_hash
from bithumb_bot.risk import PureRiskInput, evaluate_pure_risk

from . import backtest_support as support
from .backtest_stages import RiskGateDecision


@dataclass(frozen=True)
class RiskMarketSnapshot:
    candle_ts: int
    close: float

    @classmethod
    def from_mapping(cls, payload: dict[str, object]) -> RiskMarketSnapshot:
        return cls(
            candle_ts=int(payload.get("candle_ts") or 0),
            close=float(payload.get("close") or 0.0),
        )


@dataclass(frozen=True)
class RiskPortfolioSnapshot:
    qty: float
    pending_buy_qty: float
    pending_sell_qty: float
    sellable_qty: float

    @classmethod
    def from_mapping(cls, payload: dict[str, object]) -> RiskPortfolioSnapshot:
        return cls(
            qty=float(payload.get("qty") or 0.0),
            pending_buy_qty=float(payload.get("pending_buy_qty") or 0.0),
            pending_sell_qty=float(payload.get("pending_sell_qty") or 0.0),
            sellable_qty=float(payload.get("sellable_qty") or 0.0),
        )


@dataclass(frozen=True)
class RiskGateContext:
    strategy_plugin: Any
    event: Any
    active_exit_policy: dict[str, object]
    parameter_values: dict[str, object]
    fee_rate: float
    strategy_envelope: Any
    pure_risk_input: PureRiskInput | None = None
    current_equity: float | None = None
    baseline_equity: float | None = None
    loss_today: float | None = None
    max_daily_loss_krw: float = 0.0
    current_cash: float | None = None
    max_position_loss_pct: float = 0.0
    broker_local_mismatch: bool = False
    recovery_risk_mismatch_reason: str | None = None

    @classmethod
    def from_mapping(cls, payload: dict[str, object]) -> RiskGateContext:
        return cls(
            strategy_plugin=payload["strategy_plugin"],
            event=payload["event"],
            active_exit_policy=dict(payload["active_exit_policy"]),  # type: ignore[arg-type]
            parameter_values=dict(payload["parameter_values"]),  # type: ignore[arg-type]
            fee_rate=float(payload["fee_rate"]),
            strategy_envelope=payload["strategy_envelope"],
            pure_risk_input=payload.get("pure_risk_input"),  # type: ignore[arg-type]
            current_equity=_float_or_none(payload.get("current_equity")),
            baseline_equity=_float_or_none(payload.get("baseline_equity")),
            loss_today=_float_or_none(payload.get("loss_today")),
            max_daily_loss_krw=float(payload.get("max_daily_loss_krw") or 0.0),
            current_cash=_float_or_none(payload.get("current_cash")),
            max_position_loss_pct=float(payload.get("max_position_loss_pct") or 0.0),
            broker_local_mismatch=bool(payload.get("broker_local_mismatch")),
            recovery_risk_mismatch_reason=(
                str(payload.get("recovery_risk_mismatch_reason"))
                if payload.get("recovery_risk_mismatch_reason") is not None
                else None
            ),
        )


@dataclass(frozen=True)
class DefaultRiskGate:
    """Exit-policy and research risk admission boundary."""

    def run(self, state: Any) -> Any:
        return state

    def evaluate(
        self,
        strategy_decision: Any | None,
        position_snapshot: Any,
        market_snapshot: dict[str, object] | RiskMarketSnapshot,
        portfolio_snapshot: dict[str, object] | RiskPortfolioSnapshot,
        risk_context: dict[str, object] | RiskGateContext,
    ) -> RiskGateDecision:
        from bithumb_bot.strategy.exit_rules import merge_exit_rules

        market = (
            market_snapshot
            if isinstance(market_snapshot, RiskMarketSnapshot)
            else RiskMarketSnapshot.from_mapping(market_snapshot)
        )
        portfolio = (
            portfolio_snapshot
            if isinstance(portfolio_snapshot, RiskPortfolioSnapshot)
            else RiskPortfolioSnapshot.from_mapping(portfolio_snapshot)
        )
        context = (
            risk_context
            if isinstance(risk_context, RiskGateContext)
            else RiskGateContext.from_mapping(risk_context)
        )
        plugin = context.strategy_plugin
        event = context.event
        strategy_envelope = context.strategy_envelope
        raw_signal = str(strategy_envelope.provenance.get("raw_signal") or "HOLD").upper()
        raw_reason = str(strategy_envelope.provenance.get("raw_reason") or event.reason)
        entry_signal = str(strategy_envelope.provenance.get("entry_signal") or raw_signal).upper()
        unsupported_reason = str(strategy_envelope.unsupported_reason or "")
        policy_drives_execution = True
        if strategy_decision is not None and policy_drives_execution:
            requested_action = str(strategy_decision.final_signal or "HOLD").upper()
        elif unsupported_reason:
            requested_action = "HOLD"
        else:
            requested_action = str(event.final_signal or "HOLD").upper()
        action = requested_action
        blocked = bool(unsupported_reason)
        block_reason = (
            str(strategy_decision.final_reason)
            if strategy_decision is not None and policy_drives_execution
            else unsupported_reason or str(event.reason)
        )
        pure_risk_input = context.pure_risk_input
        if pure_risk_input is None:
            pure_risk_input = PureRiskInput(
                evaluation_ts_ms=market.candle_ts,
                current_equity=context.current_equity,
                baseline_equity=context.baseline_equity,
                loss_today=context.loss_today,
                max_daily_loss_krw=context.max_daily_loss_krw,
                mark_price=market.close,
                current_cash_krw=context.current_cash,
                current_asset_qty=portfolio.qty,
                position_entry_price=_float_or_none(getattr(position_snapshot, "entry_price", None)),
                max_position_loss_pct=context.max_position_loss_pct,
                broker_local_mismatch=context.broker_local_mismatch,
                recovery_risk_mismatch_reason=context.recovery_risk_mismatch_reason,
            )
        pure_risk = evaluate_pure_risk(pure_risk_input)
        if pure_risk.blocked:
            action = "HOLD"
            blocked = True
            block_reason = pure_risk.reason_code
        exit_evaluations: list[dict[str, object]] = []
        exit_rule = str((event.exit_intent or {}).get("exit_rule") or "") if event.exit_intent else ""
        exit_reason = str((event.exit_intent or {}).get("exit_reason") or "") if event.exit_intent else ""
        evaluates_exit_policy = bool(strategy_envelope.provenance.get("evaluates_exit_policy"))
        if (
            evaluates_exit_policy
            and strategy_decision is None
            and not unsupported_reason
            and not pure_risk.blocked
        ):
            action = "BUY" if requested_action == "BUY" else "HOLD"
            if portfolio.sellable_qty > 1e-12:
                position = support.ResearchPositionContext(
                    in_position=True,
                    entry_ts=getattr(position_snapshot, "entry_ts", None),
                    entry_price=getattr(position_snapshot, "entry_price", None),
                    qty_open=portfolio.sellable_qty,
                    holding_time_sec=float(getattr(position_snapshot, "holding_time_sec", 0.0) or 0.0),
                    unrealized_pnl=float(getattr(position_snapshot, "unrealized_pnl", 0.0) or 0.0),
                    unrealized_pnl_ratio=float(
                        getattr(position_snapshot, "unrealized_pnl_ratio", 0.0) or 0.0
                    ),
                )
                common_exit_rules = support.create_exit_rules(
                    rule_names=list(context.active_exit_policy.get("common_rules") or ()),
                    stop_loss_ratio=float(
                        dict(context.active_exit_policy.get("stop_loss") or {}).get("stop_loss_ratio", 0.0)
                    ),
                    max_holding_sec=float(
                        dict(context.active_exit_policy.get("max_holding_time") or {}).get("max_holding_min", 0.0)
                    )
                    * 60.0,
                )
                strategy_exit_rules = []
                if plugin.exit_rule_factory is not None:
                    strategy_exit_rules = plugin.exit_rule_factory(
                        context.active_exit_policy,
                        context.parameter_values,
                        context.fee_rate,
                    )
                exit_rules = merge_exit_rules(common_exit_rules, strategy_exit_rules)
                common_exit_rule_names = {rule.name for rule in common_exit_rules}
                strategy_exit_rule_names = {rule.name for rule in strategy_exit_rules}
                for rule in exit_rules:
                    strategy_signal_context = (
                        plugin.exit_signal_context_builder(event)
                        if plugin.exit_signal_context_builder is not None
                        else {}
                    )
                    result = rule.evaluate(
                        position=position,
                        candle_ts=market.candle_ts,
                        market_price=market.close,
                        signal_context={
                            "base_signal": raw_signal,
                            "base_reason": raw_reason,
                            "entry_signal": entry_signal,
                            "exit_signal": event.exit_signal or raw_signal,
                            **strategy_signal_context,
                        },
                    )
                    exit_evaluations.append(
                        {
                            "rule": rule.name,
                            "rule_source": _exit_rule_source(
                                rule_name=rule.name,
                                common_exit_rule_names=common_exit_rule_names,
                                strategy_exit_rule_names=strategy_exit_rule_names,
                            ),
                            "triggered": bool(result.should_exit),
                            "reason": result.reason,
                            "context": result.context,
                        }
                    )
                    if result.should_exit:
                        action = "SELL"
                        exit_rule = rule.name
                        exit_reason = result.reason
                        break
        if action == "BUY" and (portfolio.qty > 1e-12 or portfolio.pending_buy_qty > 1e-12):
            action = "HOLD"
            blocked = True
            block_reason = "buy_blocked_existing_position_or_pending_buy"
        elif action == "SELL" and portfolio.sellable_qty <= 1e-12:
            action = "HOLD"
            blocked = True
            block_reason = "sell_blocked_no_sellable_qty"
        elif action not in {"BUY", "SELL", "HOLD"}:
            raise ValueError(f"unsupported_decision_event_final_signal:{event.final_signal}")
        if strategy_decision is not None:
            exit_evaluations = [dict(item) for item in strategy_decision.exit_evaluations]
            exit_rule = str(strategy_decision.exit_rule or "")
            exit_reason = strategy_decision.exit_reason
        reason_code = block_reason if blocked or action == "HOLD" else "none"
        evidence_payload = {
            "stage": "risk_gate",
            "requested_action": requested_action,
            "final_signal": action,
            "reason_code": reason_code,
            "exit_rule": exit_rule,
            "exit_reason": exit_reason,
            "exit_evaluations": exit_evaluations,
        }
        return RiskGateDecision(
            allow=action in {"BUY", "SELL"} and not blocked,
            block=bool(blocked or action == "HOLD"),
            override_to_sell=bool(requested_action != "SELL" and action == "SELL"),
            final_signal=action,
            reason_code=reason_code,
            evidence_hash=canonical_payload_hash(evidence_payload),
            exit_rule=exit_rule,
            exit_reason=exit_reason,
            exit_evaluations=tuple(exit_evaluations),
            payload=evidence_payload,
        )


def _exit_rule_source(
    *,
    rule_name: str,
    common_exit_rule_names: set[str],
    strategy_exit_rule_names: set[str],
) -> str:
    in_common = rule_name in common_exit_rule_names
    in_strategy = rule_name in strategy_exit_rule_names
    if in_common and in_strategy:
        return "common_risk_and_plugin"
    if in_common:
        return "common_risk"
    if in_strategy:
        return "plugin"
    return "unknown"


def _float_or_none(value: object) -> float | None:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed


__all__ = ["DefaultRiskGate", "RiskGateContext", "RiskMarketSnapshot", "RiskPortfolioSnapshot"]
