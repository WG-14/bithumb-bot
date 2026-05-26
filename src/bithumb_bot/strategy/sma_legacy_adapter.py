from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from statistics import fmean
from typing import Any

from ..config import settings
from ..market_regime import evaluate_live_regime_policy
from ..dust import (
    NormalizedExposure,
    PositionStateModel,
)
from ..broker.order_rules import get_effective_order_rules
from ..core.sma_policy import (
    MarketWindow,
    PositionSnapshot,
    StrategyDecisionV2,
)
from ..decision_contract import apply_decision_contract
from ..fee_authority import (
    FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON,
    FeeAuthoritySnapshot,
)
from ..strategy_config import (
    SmaStrategyConfig,
    normalize_exit_rule_names,
    sma_strategy_config_from_settings,
)
from ..runtime_position_state_normalizer import PositionStateNormalizer, load_last_reconcile_metadata
from .. import runtime_sma_context
from ..utils_time import parse_interval_sec
from .base import PositionContext, StrategyDecision
from .exit_rules import ExitPolicyConfig, evaluate_sma_exit_policy
from .sma_decision_assembler import evaluate_sma_final_decision
from ..sma_decision import evaluate_entry_edge_filter, evaluate_sma_entry_decision


LEGACY_DB_BOUND_STRATEGY_STATUS = "db_bound_smoke_compatibility_only_not_promotion_grade"


# Currently implemented protective exits that can override raw BUY entry intent.
# Add future active risk exits here, such as take_profit, trailing_stop, or
# momentum_timeout, when they become implemented exit rules.
PROTECTIVE_EXIT_RULE_NAMES = frozenset({"stop_loss", "max_holding_time"})


def _load_signal_rows(
    conn: sqlite3.Connection,
    *,
    pair: str,
    interval: str,
    through_ts_ms: int | None,
):
    """Compatibility shim; runtime_sma_snapshot_builder owns this DB read."""
    from ..runtime_sma_snapshot_builder import _load_signal_rows as _runtime_loader

    return _runtime_loader(
        conn,
        pair=pair,
        interval=interval,
        through_ts_ms=through_ts_ms,
    )


def _closed_candle_cutoff_ts_ms(*, interval_sec: int, now_ms: int | None = None) -> int | None:
    """Compatibility shim; runtime_sma_snapshot_builder owns closed-candle selection."""
    from ..runtime_sma_snapshot_builder import _closed_candle_cutoff_ts_ms as _runtime_cutoff

    if now_ms is None:
        now_ms = int(time.time() * 1000)
    return _runtime_cutoff(interval_sec=interval_sec, now_ms=now_ms)



def _sma(values: list[float], n: int, end: int) -> float:
    return runtime_sma_context.sma(values, n, end)


def _safe_ratio(numerator: float, denominator: float) -> float:
    return runtime_sma_context.safe_ratio(numerator, denominator)


def _compute_gap_ratio(*, curr_s: float, curr_l: float) -> float:
    return abs(_safe_ratio(curr_s - curr_l, curr_l))


def _base_signal(*, prev_s: float, prev_l: float, curr_s: float, curr_l: float) -> tuple[str, str]:
    if prev_s <= prev_l and curr_s > curr_l:
        return "BUY", "sma golden cross"
    if prev_s >= prev_l and curr_s < curr_l:
        return "SELL", "sma dead cross"
    return "HOLD", "sma no crossover"


def _resolve_exit_rule_names(raw: str) -> list[str]:
    return list(normalize_exit_rule_names(raw or ""))


def _load_last_reconcile_metadata(conn: sqlite3.Connection) -> str | None:
    return load_last_reconcile_metadata(conn)


def _compute_entry_cost_floor_ratio(*, slippage_bps: float, live_fee_rate_estimate: float, buffer_ratio: float) -> float:
    slippage_ratio = max(0.0, float(slippage_bps)) / 10_000.0
    roundtrip_fee_ratio = 2.0 * max(0.0, float(live_fee_rate_estimate))
    return roundtrip_fee_ratio + slippage_ratio + max(0.0, float(buffer_ratio))


def _compute_required_entry_edge_ratio(
    *,
    slippage_bps: float,
    live_fee_rate_estimate: float,
    edge_buffer_ratio: float,
    strategy_min_expected_edge_ratio: float,
) -> tuple[float, float]:
    cost_floor_ratio = _compute_entry_cost_floor_ratio(
        slippage_bps=slippage_bps,
        live_fee_rate_estimate=live_fee_rate_estimate,
        buffer_ratio=edge_buffer_ratio,
    )
    return cost_floor_ratio, max(cost_floor_ratio, max(0.0, float(strategy_min_expected_edge_ratio)))


def _pair_order_rules(pair: str):
    return get_effective_order_rules(pair).rules


def _resolve_strategy_fee_authority(
    *,
    pair: str,
    config_fallback_fee_rate: float,
) -> FeeAuthoritySnapshot:
    return runtime_sma_context.resolve_strategy_fee_authority(
        pair=pair,
        config_fallback_fee_rate=float(config_fallback_fee_rate),
    )


def _fee_authority_context(fee_authority: FeeAuthoritySnapshot) -> dict[str, object]:
    return runtime_sma_context.fee_authority_context(fee_authority)


def _build_entry_intent_context(
    *,
    pair: str,
    buy_fraction: float,
    max_order_krw: float,
) -> dict[str, Any]:
    return runtime_sma_context.build_entry_intent_context(
        pair=pair,
        buy_fraction=buy_fraction,
        max_order_krw=max_order_krw,
    )


def _build_entry_decision_context(
    *,
    pair: str,
    base_signal: str,
    base_reason: str,
    entry_signal: str,
    entry_reason: str,
    buy_fraction: float,
    max_order_krw: float,
) -> dict[str, Any]:
    return runtime_sma_context.build_entry_decision_context(
        pair=pair,
        base_signal=base_signal,
        base_reason=base_reason,
        entry_signal=entry_signal,
        entry_reason=entry_reason,
        buy_fraction=buy_fraction,
        max_order_krw=max_order_krw,
    )


def _build_exit_decision_context(
    *,
    exposure: NormalizedExposure,
    triggered: bool,
    reason: str,
    rule: str | None,
    evaluations: list[dict[str, Any]],
) -> dict[str, Any]:
    return runtime_sma_context.build_exit_decision_context(
        exposure=exposure,
        triggered=triggered,
        reason=reason,
        rule=rule,
        evaluations=evaluations,
    )


def _build_position_state_context(position_state: PositionStateModel) -> dict[str, Any]:
    return runtime_sma_context.build_position_state_context(position_state)


def _build_position_gate_context(
    exposure: NormalizedExposure,
    *,
    order_rules: dict[str, object] | None = None,
) -> dict[str, Any]:
    return runtime_sma_context.build_position_gate_context(exposure, order_rules=order_rules)


def _has_tracked_open_exposure(exposure: NormalizedExposure) -> bool:
    return bool(exposure.normalized_exposure_active)


def _evaluate_entry_edge_filter(
    *,
    base_signal: str,
    gap_ratio: float,
    slippage_bps: float,
    live_fee_rate_estimate: float,
    edge_buffer_ratio: float,
    strategy_min_expected_edge_ratio: float,
    filter_enabled: bool = True,
) -> tuple[bool, dict[str, float | bool]]:
    return evaluate_entry_edge_filter(
        base_signal=base_signal,
        gap_ratio=gap_ratio,
        slippage_bps=slippage_bps,
        live_fee_rate_estimate=live_fee_rate_estimate,
        edge_buffer_ratio=edge_buffer_ratio,
        strategy_min_expected_edge_ratio=strategy_min_expected_edge_ratio,
        filter_enabled=filter_enabled,
    )


def _live_armed_entry_fee_authority_blocks(fee_authority: FeeAuthoritySnapshot) -> bool:
    return runtime_sma_context.live_armed_entry_fee_authority_blocks(fee_authority)


def _resolve_signal_strength_label(
    *,
    base_signal: str,
    expected_edge_ratio: float,
    required_edge_ratio: float,
) -> str:
    if base_signal not in ("BUY", "SELL"):
        return "neutral"
    if expected_edge_ratio < required_edge_ratio:
        return "weak"
    return "tradable"


def _load_position_context(
    conn: sqlite3.Connection,
    *,
    pair: str,
    candle_ts: int,
    market_price: float,
    signal_context: dict[str, Any],
    slippage_bps: float,
    entry_edge_buffer_ratio: float,
):
    """Compatibility shim; runtime_sma_snapshot_builder owns this boundary."""
    from ..runtime_sma_snapshot_builder import _load_position_context as _runtime_loader

    return _runtime_loader(
        conn,
        pair=pair,
        candle_ts=candle_ts,
        market_price=market_price,
        signal_context=signal_context,
        slippage_bps=slippage_bps,
        entry_edge_buffer_ratio=entry_edge_buffer_ratio,
    )


def _policy_position_snapshot(
    *,
    position: PositionContext,
    exposure: NormalizedExposure,
) -> PositionSnapshot:
    """Compatibility shim; runtime_sma_snapshot_builder owns this boundary."""
    from ..runtime_sma_snapshot_builder import _policy_position_snapshot as _runtime_snapshot

    return _runtime_snapshot(position=position, exposure=exposure)


def _apply_entry_exit_policy(
    *,
    base_signal: str,
    base_reason: str,
    base_context: dict[str, Any],
    position: PositionContext,
    exposure: NormalizedExposure,
    position_state: PositionStateModel,
    exit_policy_config: ExitPolicyConfig,
    raw_signal: str | None = None,
    raw_reason: str | None = None,
    exit_signal: str | None = None,
    exit_reason: str | None = None,
) -> StrategyDecision:
    resolved_raw_signal = str(raw_signal or base_context.get("base_signal") or base_signal).upper()
    resolved_raw_reason = str(raw_reason or base_context.get("base_reason") or base_reason)
    resolved_entry_signal = str(base_signal).upper()
    resolved_entry_reason = str(base_reason)
    resolved_exit_signal = str(exit_signal or resolved_raw_signal).upper()
    resolved_exit_reason = str(exit_reason or resolved_raw_reason)
    allow_harmless_dust_exit_evaluation = bool(
        exposure.dust_classification == "harmless_dust"
        and not exposure.harmless_dust_effective_flat
        and position.in_position
    )

    def _annotate_decision_context(
        context: dict[str, Any],
        *,
        raw_signal: str,
        final_signal: str,
        final_reason: str,
    ) -> dict[str, Any]:
        entry = context.get("entry") if isinstance(context.get("entry"), dict) else {}
        exit_context = context.get("exit") if isinstance(context.get("exit"), dict) else {}
        entry_signal = str(entry.get("entry_signal", raw_signal)).strip().upper() or raw_signal
        filtered_entry = raw_signal == "BUY" and raw_signal != entry_signal
        exit_rule_name = str(exit_context.get("rule") or "").strip().lower()
        protective_exit_overrode_entry = bool(
            raw_signal == "BUY"
            and position.in_position
            and final_signal == "SELL"
            and exit_rule_name in PROTECTIVE_EXIT_RULE_NAMES
        )
        entry_blocked = raw_signal == "BUY" and final_signal == "HOLD"
        raw_filter_would_block = bool(context.get("raw_filter_would_block", context.get("entry_filter_blocked", False)))
        exit_filter_suppression_prevented = bool(
            raw_signal == "SELL"
            and position.in_position
            and position_state.normalized_exposure.exit_allowed
            and raw_filter_would_block
            and resolved_exit_signal == "SELL"
        )
        entry_block_reason: str | None = None
        if entry_blocked:
            if filtered_entry:
                entry_block_reason = str(entry.get("entry_reason") or context.get("reason") or "").strip() or None
            else:
                entry_block_reason = str(final_reason or "").strip() or None
        order_rules = context.get("order_rules") if isinstance(context.get("order_rules"), dict) else {}
        context["position_gate"] = _build_position_gate_context(
            position_state.normalized_exposure,
            order_rules=order_rules,
        )
        context["position_state"] = _build_position_state_context(position_state)
        normalized_state = context["position_state"]["normalized_exposure"]
        state_interpretation = context["position_state"]["state_interpretation"]
        context["raw_signal"] = raw_signal
        context["final_signal"] = final_signal
        context["exit_signal"] = resolved_exit_signal
        context["exit_reason_raw"] = resolved_exit_reason
        context["raw_filter_would_block"] = raw_filter_would_block
        context["entry_blocked"] = entry_blocked
        context["protective_exit_overrode_entry"] = protective_exit_overrode_entry
        context["exit_filter_suppression_prevented"] = exit_filter_suppression_prevented
        context["entry_block_reason"] = entry_block_reason
        context["dust_classification"] = str(normalized_state["dust_classification"])
        context["entry_allowed"] = bool(normalized_state["entry_allowed"])
        context["effective_flat"] = bool(normalized_state["effective_flat"])
        context["raw_qty_open"] = float(normalized_state["raw_qty_open"])
        context["raw_total_asset_qty"] = float(normalized_state["raw_total_asset_qty"])
        context["normalized_exposure_active"] = bool(normalized_state["normalized_exposure_active"])
        context["has_executable_exposure"] = bool(normalized_state.get("has_executable_exposure", False))
        context["has_any_position_residue"] = bool(normalized_state.get("has_any_position_residue", False))
        context["has_non_executable_residue"] = bool(normalized_state.get("has_non_executable_residue", False))
        context["has_dust_only_remainder"] = bool(normalized_state.get("has_dust_only_remainder", False))
        context["exit_allowed"] = bool(normalized_state["exit_allowed"])
        context["exit_block_reason"] = str(normalized_state["exit_block_reason"])
        context["terminal_state"] = str(normalized_state["terminal_state"])
        context["state_outcome"] = str(state_interpretation["operator_outcome"])
        context["exit_submit_expected"] = bool(state_interpretation["exit_submit_expected"])
        return apply_decision_contract(context)

    if resolved_exit_signal == "SELL" and not exposure.exit_allowed and not allow_harmless_dust_exit_evaluation:
        context = _annotate_decision_context(
            dict(base_context),
            raw_signal=resolved_raw_signal,
            final_signal="HOLD",
            final_reason=str(exposure.exit_block_reason or "exit_blocked_by_position_state"),
        )
        context["exit"] = _build_exit_decision_context(
            exposure=exposure,
            triggered=False,
            reason=str(exposure.exit_block_reason or "exit_blocked_by_position_state"),
            rule=None,
            evaluations=[],
        )
        return StrategyDecision(
            signal="HOLD",
            reason=str(exposure.exit_block_reason or "exit_blocked_by_position_state"),
            context=context,
        )

    if position.in_position and not exposure.exit_allowed and not allow_harmless_dust_exit_evaluation:
        context = _annotate_decision_context(
            dict(base_context),
            raw_signal=resolved_raw_signal,
            final_signal="HOLD",
            final_reason=str(exposure.exit_block_reason or "exit_blocked_by_position_state"),
        )
        context["exit"] = _build_exit_decision_context(
            exposure=exposure,
            triggered=False,
            reason=str(exposure.exit_block_reason or "exit_blocked_by_position_state"),
            rule=None,
            evaluations=[],
        )
        return StrategyDecision(
            signal="HOLD",
            reason=str(exposure.exit_block_reason or "exit_blocked_by_position_state"),
            context=context,
        )

    if position.in_position:
        exit_decision = evaluate_sma_exit_policy(
            position=_policy_position_snapshot(position=position, exposure=exposure),
            market=MarketWindow(
                pair=str(base_context.get("pair") or ""),
                interval=str(base_context.get("interval") or ""),
                candle_ts=int(base_context["ts"]),
                closes=(float(base_context["last_close"]),),
                prev_s=float(base_context["prev_s"]),
                prev_l=float(base_context["prev_l"]),
                curr_s=float(base_context["curr_s"]),
                curr_l=float(base_context["curr_l"]),
            ),
            raw_signal=resolved_raw_signal,
            raw_reason=resolved_raw_reason,
            entry_signal=resolved_entry_signal,
            exit_signal=resolved_exit_signal,
            config=exit_policy_config,
        )
        exit_results = [dict(item) for item in exit_decision.evaluations]
        if exit_decision.triggered:
            context = dict(base_context)
            context["position"] = position.as_dict()
            context["exit"] = _build_exit_decision_context(
                exposure=exposure,
                triggered=True,
                reason=exit_decision.reason,
                rule=exit_decision.rule,
                evaluations=exit_results,
            )
            context = _annotate_decision_context(
                context,
                raw_signal=resolved_raw_signal,
                final_signal="SELL",
                final_reason=exit_decision.reason,
            )
            return StrategyDecision(signal="SELL", reason=exit_decision.reason, context=context)

        context = dict(base_context)
        context["position"] = position.as_dict()
        context["exit"] = _build_exit_decision_context(
            exposure=exposure,
            triggered=False,
            reason="no exit rule triggered",
            rule=None,
            evaluations=exit_results,
        )
        context = _annotate_decision_context(
            context,
            raw_signal=resolved_raw_signal,
            final_signal="HOLD",
            final_reason="position held: no exit rule triggered",
        )
        return StrategyDecision(signal="HOLD", reason="position held: no exit rule triggered", context=context)

    if resolved_entry_signal == "BUY" and not exposure.entry_allowed:
        context = _annotate_decision_context(
            dict(base_context),
            raw_signal=resolved_raw_signal,
            final_signal="HOLD",
            final_reason=str(exposure.entry_block_reason or "entry_blocked_by_position_state"),
        )
        return StrategyDecision(
            signal="HOLD",
            reason=str(exposure.entry_block_reason or "entry_blocked_by_position_state"),
            context=context,
        )

    if not position.in_position:
        context = _annotate_decision_context(
            dict(base_context),
            raw_signal=resolved_raw_signal,
            final_signal=resolved_entry_signal,
            final_reason=resolved_entry_reason,
        )
        return StrategyDecision(signal=resolved_entry_signal, reason=resolved_entry_reason, context=context)


def _legacy_strategy_decision_from_sma_final_decision(
    *,
    decision: StrategyDecisionV2,
    base_context: dict[str, Any],
    position: PositionContext,
    exposure: NormalizedExposure,
    position_state: PositionStateModel,
) -> StrategyDecision:
    return runtime_sma_context.legacy_strategy_decision_from_sma_final_decision(
        decision=decision,
        base_context=base_context,
        position=position,
        exposure=exposure,
        position_state=position_state,
    )


@dataclass(frozen=True)
class SmaCrossStrategy:
    short_n: int
    long_n: int
    pair: str = settings.PAIR
    interval: str = settings.INTERVAL
    exit_rule_names: list[str] = field(
        default_factory=lambda: _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
    )
    exit_stop_loss_ratio: float = settings.STRATEGY_EXIT_STOP_LOSS_RATIO
    exit_max_holding_min: int = settings.STRATEGY_EXIT_MAX_HOLDING_MIN
    exit_min_take_profit_ratio: float = settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
    exit_small_loss_tolerance_ratio: float = settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO
    slippage_bps: float = settings.STRATEGY_ENTRY_SLIPPAGE_BPS
    live_fee_rate_estimate: float = settings.LIVE_FEE_RATE_ESTIMATE
    entry_edge_buffer_ratio: float = settings.ENTRY_EDGE_BUFFER_RATIO
    strategy_min_expected_edge_ratio: float = settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO
    buy_fraction: float = settings.BUY_FRACTION
    max_order_krw: float = settings.MAX_ORDER_KRW

    name: str = "sma_cross"
    legacy_status: str = "legacy_db_bound_smoke_only_not_promotion_grade"

    @classmethod
    def from_config(cls, config: SmaStrategyConfig) -> "SmaCrossStrategy":
        return cls(
            short_n=int(config.short_n),
            long_n=int(config.long_n),
            pair=str(config.pair),
            interval=str(config.interval),
            exit_rule_names=list(config.exit_rule_names),
            exit_stop_loss_ratio=float(config.exit_stop_loss_ratio),
            exit_max_holding_min=int(config.exit_max_holding_min),
            exit_min_take_profit_ratio=float(config.exit_min_take_profit_ratio),
            exit_small_loss_tolerance_ratio=float(config.exit_small_loss_tolerance_ratio),
            slippage_bps=float(config.slippage_bps),
            live_fee_rate_estimate=float(config.live_fee_rate_estimate),
            entry_edge_buffer_ratio=float(config.entry_edge_buffer_ratio),
            strategy_min_expected_edge_ratio=float(config.strategy_min_expected_edge_ratio),
            buy_fraction=float(config.buy_fraction),
            max_order_krw=float(config.max_order_krw),
        )

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None:
        # Legacy DB-bound compatibility path. Live mode rejects this strategy;
        # promotion-grade equivalence work should use sma_with_filter.
        if self.short_n >= self.long_n:
            raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

        interval_sec = parse_interval_sec(self.interval)
        signal_through_ts_ms = through_ts_ms
        if signal_through_ts_ms is None:
            signal_through_ts_ms = _closed_candle_cutoff_ts_ms(interval_sec=interval_sec)
            if signal_through_ts_ms is None:
                return None

        rows = _load_signal_rows(
            conn,
            pair=self.pair,
            interval=self.interval,
            through_ts_ms=signal_through_ts_ms,
        )
        if len(rows) < self.long_n + 2:
            return None

        closes = [float(r[1]) for r in rows]
        ts_list = [int(r[0]) for r in rows]
        end_prev = len(closes) - 1
        end_curr = len(closes)

        prev_s = _sma(closes, self.short_n, end_prev)
        prev_l = _sma(closes, self.long_n, end_prev)
        curr_s = _sma(closes, self.short_n, end_curr)
        curr_l = _sma(closes, self.long_n, end_curr)

        base_signal, base_reason = _base_signal(prev_s=prev_s, prev_l=prev_l, curr_s=curr_s, curr_l=curr_l)
        gap_ratio = _compute_gap_ratio(curr_s=curr_s, curr_l=curr_l)
        fee_authority = _resolve_strategy_fee_authority(
            pair=self.pair,
            config_fallback_fee_rate=float(self.live_fee_rate_estimate),
        )
        fee_rate_for_decision = float(fee_authority.taker_roundtrip_fee_rate / 2)
        edge_filter_triggered, edge_filter_details = _evaluate_entry_edge_filter(
            base_signal=base_signal,
            gap_ratio=gap_ratio,
            slippage_bps=float(self.slippage_bps),
            live_fee_rate_estimate=fee_rate_for_decision,
            edge_buffer_ratio=float(self.entry_edge_buffer_ratio),
            strategy_min_expected_edge_ratio=float(self.strategy_min_expected_edge_ratio),
        )
        entry_signal = base_signal
        entry_reason = base_reason
        if base_signal == "BUY" and edge_filter_triggered:
            entry_signal = "HOLD"
            entry_reason = "filtered entry: cost_edge"
        if base_signal == "BUY" and _live_armed_entry_fee_authority_blocks(fee_authority):
            entry_signal = "HOLD"
            entry_reason = FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON
        signal_strength_label = _resolve_signal_strength_label(
            base_signal=base_signal,
            expected_edge_ratio=float(edge_filter_details["expected_edge_ratio"]),
            required_edge_ratio=float(edge_filter_details["required_edge_ratio"]),
        )
        signal_context = {
            "strategy": self.name,
            "base_signal": base_signal,
            "base_reason": base_reason,
            "entry_signal": entry_signal,
            "entry_reason": entry_reason,
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
        }
        PositionStateNormalizer().normalize_and_persist(
            conn,
            pair=self.pair,
            market_price=float(closes[-1]),
            slippage_bps=float(self.slippage_bps),
            entry_edge_buffer_ratio=float(self.entry_edge_buffer_ratio),
        )
        position, exposure, position_state, order_rules_snapshot = _load_position_context(
            conn,
            pair=self.pair,
            candle_ts=ts_list[-1],
            market_price=float(closes[-1]),
            signal_context=signal_context,
            slippage_bps=float(self.slippage_bps),
            entry_edge_buffer_ratio=float(self.entry_edge_buffer_ratio),
        )
        exit_policy_config = ExitPolicyConfig(
            rule_names=tuple(self.exit_rule_names),
            max_holding_sec=float(self.exit_max_holding_min) * 60.0,
            min_take_profit_ratio=float(self.exit_min_take_profit_ratio),
            live_fee_rate_estimate=fee_rate_for_decision,
            small_loss_tolerance_ratio=float(self.exit_small_loss_tolerance_ratio),
            stop_loss_ratio=float(self.exit_stop_loss_ratio),
        )
        base_context = {
            "ts": ts_list[-1],
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
            "last_close": float(closes[-1]),
            "strategy": self.name,
            "pair": self.pair,
            "interval": self.interval,
            "gap_ratio": gap_ratio,
            "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
            "position_lot_interpretation_costs": {
                "exit_slippage_bps": float(self.slippage_bps),
                "exit_buffer_ratio": float(self.entry_edge_buffer_ratio),
            },
            "blocked_by_cost_filter": bool(edge_filter_triggered),
            "blocked_by_fee_authority": bool(entry_reason == FEE_AUTHORITY_LIVE_ENTRY_BLOCK_REASON),
            "fee_authority": _fee_authority_context(fee_authority),
            "signal_strength_label": signal_strength_label,
            "signal_strength": {
                "label": signal_strength_label,
                "gap_ratio": gap_ratio,
                "required_edge_ratio": float(edge_filter_details["required_edge_ratio"]),
                "is_weak_cross": bool(signal_strength_label == "weak"),
                # NOTE: sma_cross는 단순 교차 전략이며, 실거래 우선 전략은 sma_with_filter다.
                "preferred_live_strategy": "sma_with_filter",
            },
            "entry": _build_entry_decision_context(
                pair=self.pair,
                base_signal=base_signal,
                base_reason=base_reason,
                entry_signal=entry_signal,
                entry_reason=entry_reason,
                buy_fraction=float(self.buy_fraction),
                max_order_krw=float(self.max_order_krw),
            ),
            "order_rules": order_rules_snapshot,
            "position_gate": _build_position_gate_context(
                position_state.normalized_exposure,
                order_rules=order_rules_snapshot,
            ),
            "position_state": _build_position_state_context(position_state),
            "filters": {
                "cost_edge": {
                    "enabled": bool(edge_filter_details["enabled"]),
                    "passed": not bool(edge_filter_details["blocked"]),
                    "value": float(edge_filter_details["expected_edge_ratio"]),
                    "threshold": float(edge_filter_details["required_edge_ratio"]),
                    "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
                    "roundtrip_fee_ratio": float(edge_filter_details["roundtrip_fee_ratio"]),
                    "slippage_ratio": float(edge_filter_details["slippage_ratio"]),
                    "buffer_ratio": float(edge_filter_details["buffer_ratio"]),
                    "min_expected_edge_ratio": float(edge_filter_details["min_expected_edge_ratio"]),
                    "fee_authority_source": fee_authority.fee_source,
                    "fee_authority_degraded": bool(fee_authority.degraded),
                }
            },
        }
        return _apply_entry_exit_policy(
            base_signal=entry_signal,
            base_reason=entry_reason,
            base_context=base_context,
            position=position,
            exposure=exposure,
            position_state=position_state,
            exit_policy_config=exit_policy_config,
            raw_signal=base_signal,
            raw_reason=base_reason,
            exit_signal=base_signal,
            exit_reason=base_reason,
        )


@dataclass(frozen=True)
class LegacySmaWithFilterDbAdapter:
    short_n: int
    long_n: int
    pair: str = settings.PAIR
    interval: str = settings.INTERVAL
    min_gap_ratio: float = settings.SMA_FILTER_GAP_MIN_RATIO
    volatility_window: int = settings.SMA_FILTER_VOL_WINDOW
    min_volatility_ratio: float = settings.SMA_FILTER_VOL_MIN_RANGE_RATIO
    overextended_lookback: int = settings.SMA_FILTER_OVEREXT_LOOKBACK
    overextended_max_return_ratio: float = settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO
    slippage_bps: float = settings.STRATEGY_ENTRY_SLIPPAGE_BPS
    live_fee_rate_estimate: float = settings.LIVE_FEE_RATE_ESTIMATE
    entry_edge_buffer_ratio: float = settings.ENTRY_EDGE_BUFFER_RATIO
    cost_edge_enabled: bool = settings.SMA_COST_EDGE_ENABLED
    cost_edge_min_ratio: float = settings.SMA_COST_EDGE_MIN_RATIO
    market_regime_enabled: bool = settings.SMA_MARKET_REGIME_ENABLED
    exit_rule_names: list[str] = field(
        default_factory=lambda: _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
    )
    exit_stop_loss_ratio: float = settings.STRATEGY_EXIT_STOP_LOSS_RATIO
    exit_max_holding_min: int = settings.STRATEGY_EXIT_MAX_HOLDING_MIN
    exit_min_take_profit_ratio: float = settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
    exit_small_loss_tolerance_ratio: float = settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO
    buy_fraction: float = settings.BUY_FRACTION
    max_order_krw: float = settings.MAX_ORDER_KRW
    candidate_regime_policy: dict[str, object] | None = None

    name: str = "sma_with_filter"

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None:
        # Deprecated DB-bound compatibility facade. Runtime orchestration must
        # call PositionStateNormalizer explicitly before entering this read-only
        # snapshot path.
        from ..runtime_sma_snapshot_builder import build_sma_with_filter_decision_from_normalized_db

        return build_sma_with_filter_decision_from_normalized_db(
            conn,
            self,
            through_ts_ms=through_ts_ms,
        )

    def _decide_from_normalized_db(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None:
        # Deprecated compatibility wrapper for tests or older imports. The
        # runtime authority path calls runtime_sma_snapshot_builder directly.
        from ..runtime_sma_snapshot_builder import build_sma_with_filter_decision_from_normalized_db

        return build_sma_with_filter_decision_from_normalized_db(
            conn,
            self,
            through_ts_ms=through_ts_ms,
        )


def build_sma_with_filter_decision_from_normalized_db(
    conn: sqlite3.Connection,
    strategy: LegacySmaWithFilterDbAdapter,
    *,
    through_ts_ms: int | None = None,
) -> StrategyDecision | None:
    """Compatibility shim; runtime_sma_snapshot_builder owns this boundary."""
    from ..runtime_sma_snapshot_builder import (
        build_sma_with_filter_decision_from_normalized_db as _runtime_builder,
    )

    return _runtime_builder(conn, strategy, through_ts_ms=through_ts_ms)


def decide_sma_with_filter_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: LegacySmaWithFilterDbAdapter,
    *,
    through_ts_ms: int | None = None,
    normalizer: PositionStateNormalizer | None = None,
) -> StrategyDecision | None:
    """Compatibility shim; runtime_sma_snapshot_builder owns this boundary."""
    from ..runtime_sma_snapshot_builder import (
        decide_sma_with_filter_snapshot_from_db as _runtime_snapshot_from_db,
    )

    return _runtime_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
        normalizer=normalizer,
    )


def _resolve_signal_through_ts_ms(*, interval: str, through_ts_ms: int | None) -> int | None:
    interval_sec = parse_interval_sec(interval)
    signal_through_ts_ms = through_ts_ms
    if signal_through_ts_ms is None:
        signal_through_ts_ms = _closed_candle_cutoff_ts_ms(interval_sec=interval_sec)
        if signal_through_ts_ms is None:
            return None
    return int(signal_through_ts_ms)


def _latest_signal_close(
    conn: sqlite3.Connection,
    *,
    pair: str,
    interval: str,
    through_ts_ms: int,
) -> float | None:
    try:
        row = conn.execute(
            """
            SELECT close
            FROM candles
            WHERE pair=? AND interval=? AND ts <= ?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (pair, interval, int(through_ts_ms)),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None or row[0] is None:
        return None
    return float(row[0])


def create_sma_strategy(
    *,
    short_n: int | None = None,
    long_n: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
    exit_rule_names: list[str] | None = None,
    exit_stop_loss_ratio: float | None = None,
    exit_max_holding_min: int | None = None,
    exit_min_take_profit_ratio: float | None = None,
    exit_small_loss_tolerance_ratio: float | None = None,
    slippage_bps: float | None = None,
    entry_edge_buffer_ratio: float | None = None,
    strategy_min_expected_edge_ratio: float | None = None,
    live_fee_rate_estimate: float | None = None,
    buy_fraction: float | None = None,
    max_order_krw: float | None = None,
) -> SmaCrossStrategy:
    settings_config = sma_strategy_config_from_settings(short_n=short_n, long_n=long_n)
    config = SmaStrategyConfig(
        short_n=int(settings.SMA_SHORT if short_n is None else short_n),
        long_n=int(settings.SMA_LONG if long_n is None else long_n),
        pair=settings_config.pair if pair is None else str(pair),
        interval=settings_config.interval if interval is None else str(interval),
        exit_rule_names=(
            settings_config.exit_rule_names
            if exit_rule_names is None
            else normalize_exit_rule_names(exit_rule_names)
        ),
        exit_stop_loss_ratio=float(
            settings_config.exit_stop_loss_ratio
            if exit_stop_loss_ratio is None
            else exit_stop_loss_ratio
        ),
        exit_max_holding_min=int(
            settings_config.exit_max_holding_min
            if exit_max_holding_min is None
            else exit_max_holding_min
        ),
        exit_min_take_profit_ratio=float(
            settings_config.exit_min_take_profit_ratio
            if exit_min_take_profit_ratio is None
            else exit_min_take_profit_ratio
        ),
        exit_small_loss_tolerance_ratio=float(
            settings_config.exit_small_loss_tolerance_ratio
            if exit_small_loss_tolerance_ratio is None
            else exit_small_loss_tolerance_ratio
        ),
        slippage_bps=float(
            settings_config.slippage_bps if slippage_bps is None else slippage_bps
        ),
        entry_edge_buffer_ratio=float(
            settings_config.entry_edge_buffer_ratio
            if entry_edge_buffer_ratio is None
            else entry_edge_buffer_ratio
        ),
        strategy_min_expected_edge_ratio=float(
            settings_config.strategy_min_expected_edge_ratio
            if strategy_min_expected_edge_ratio is None
            else strategy_min_expected_edge_ratio
        ),
        live_fee_rate_estimate=float(
            settings_config.live_fee_rate_estimate
            if live_fee_rate_estimate is None
            else live_fee_rate_estimate
        ),
        buy_fraction=float(settings_config.buy_fraction if buy_fraction is None else buy_fraction),
        max_order_krw=float(settings_config.max_order_krw if max_order_krw is None else max_order_krw),
    )
    return SmaCrossStrategy.from_config(config)


def create_legacy_sma_with_filter_db_adapter(
    *,
    short_n: int | None = None,
    long_n: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
    min_gap_ratio: float | None = None,
    volatility_window: int | None = None,
    min_volatility_ratio: float | None = None,
    overextended_lookback: int | None = None,
    overextended_max_return_ratio: float | None = None,
    slippage_bps: float | None = None,
    live_fee_rate_estimate: float | None = None,
    entry_edge_buffer_ratio: float | None = None,
    strategy_min_expected_edge_ratio: float | None = None,
    cost_edge_enabled: bool | None = None,
    cost_edge_min_ratio: float | None = None,
    market_regime_enabled: bool | None = None,
    candidate_regime_policy: dict[str, object] | None = None,
    exit_rule_names: list[str] | None = None,
    exit_stop_loss_ratio: float | None = None,
    exit_max_holding_min: int | None = None,
    exit_min_take_profit_ratio: float | None = None,
    exit_small_loss_tolerance_ratio: float | None = None,
) -> LegacySmaWithFilterDbAdapter:
    settings_config = sma_strategy_config_from_settings(short_n=short_n, long_n=long_n)
    return LegacySmaWithFilterDbAdapter(
        short_n=int(settings.SMA_SHORT if short_n is None else short_n),
        long_n=int(settings.SMA_LONG if long_n is None else long_n),
        pair=settings.PAIR if pair is None else str(pair),
        interval=settings.INTERVAL if interval is None else str(interval),
        min_gap_ratio=float(
            settings.SMA_FILTER_GAP_MIN_RATIO if min_gap_ratio is None else min_gap_ratio
        ),
        volatility_window=int(
            settings.SMA_FILTER_VOL_WINDOW if volatility_window is None else volatility_window
        ),
        min_volatility_ratio=float(
            settings.SMA_FILTER_VOL_MIN_RANGE_RATIO
            if min_volatility_ratio is None
            else min_volatility_ratio
        ),
        overextended_lookback=int(
            settings.SMA_FILTER_OVEREXT_LOOKBACK
            if overextended_lookback is None
            else overextended_lookback
        ),
        overextended_max_return_ratio=float(
            settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO
            if overextended_max_return_ratio is None
            else overextended_max_return_ratio
        ),
        slippage_bps=float(
            settings.STRATEGY_ENTRY_SLIPPAGE_BPS if slippage_bps is None else slippage_bps
        ),
        live_fee_rate_estimate=float(
            settings.LIVE_FEE_RATE_ESTIMATE
            if live_fee_rate_estimate is None
            else live_fee_rate_estimate
        ),
        entry_edge_buffer_ratio=float(
            settings.ENTRY_EDGE_BUFFER_RATIO
            if entry_edge_buffer_ratio is None
            else entry_edge_buffer_ratio
        ),
        cost_edge_enabled=(
            bool(settings.SMA_COST_EDGE_ENABLED) if cost_edge_enabled is None else bool(cost_edge_enabled)
        ),
        cost_edge_min_ratio=float(
            (
                settings.SMA_COST_EDGE_MIN_RATIO
                if cost_edge_min_ratio is None and strategy_min_expected_edge_ratio is None
                else strategy_min_expected_edge_ratio
                if cost_edge_min_ratio is None
                else cost_edge_min_ratio
            )
        ),
        market_regime_enabled=(
            bool(settings.SMA_MARKET_REGIME_ENABLED)
            if market_regime_enabled is None
            else bool(market_regime_enabled)
        ),
        candidate_regime_policy=(
            settings_config.candidate_regime_policy
            if candidate_regime_policy is None
            else candidate_regime_policy
        ),
        exit_rule_names=(
            _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
            if exit_rule_names is None
            else [str(name).strip().lower() for name in exit_rule_names if str(name).strip()]
        ),
        exit_stop_loss_ratio=float(
            settings.STRATEGY_EXIT_STOP_LOSS_RATIO
            if exit_stop_loss_ratio is None
            else exit_stop_loss_ratio
        ),
        exit_max_holding_min=int(
            settings.STRATEGY_EXIT_MAX_HOLDING_MIN
            if exit_max_holding_min is None
            else exit_max_holding_min
        ),
        exit_min_take_profit_ratio=float(
            settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
            if exit_min_take_profit_ratio is None
            else exit_min_take_profit_ratio
        ),
        exit_small_loss_tolerance_ratio=float(
            settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO
            if exit_small_loss_tolerance_ratio is None
            else exit_small_loss_tolerance_ratio
        ),
    )


def compute_signal(
    conn: sqlite3.Connection,
    short_n: int,
    long_n: int,
    *,
    through_ts_ms: int | None = None,
) -> dict[str, Any] | None:
    decision = create_sma_strategy(short_n=short_n, long_n=long_n).decide(
        conn,
        through_ts_ms=through_ts_ms,
    )
    if decision is None:
        return None
    return decision.as_dict()
