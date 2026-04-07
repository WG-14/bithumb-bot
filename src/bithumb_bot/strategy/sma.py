from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from statistics import fmean
from typing import Any

from ..config import settings
from ..dust import (
    NormalizedExposure,
    build_dust_display_context,
    build_position_state_model,
    PositionStateModel,
)
from ..lifecycle import OPEN_POSITION_STATE, mark_harmless_dust_positions
from .base import PositionContext, StrategyDecision
from .exit_rules import ExitRule, create_exit_rules


def _load_signal_rows(
    conn: sqlite3.Connection,
    *,
    pair: str,
    interval: str,
    through_ts_ms: int | None,
) -> list[sqlite3.Row | tuple[Any, ...]]:
    query = "SELECT ts, close FROM candles WHERE pair=? AND interval=?"
    params: list[object] = [pair, interval]
    if through_ts_ms is not None:
        query += " AND ts <= ?"
        params.append(int(through_ts_ms))
    query += " ORDER BY ts ASC"
    return conn.execute(query, tuple(params)).fetchall()


def _sma(values: list[float], n: int, end: int) -> float:
    return sum(values[end - n : end]) / n


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _compute_gap_ratio(*, curr_s: float, curr_l: float) -> float:
    return abs(_safe_ratio(curr_s - curr_l, curr_l))


def _base_signal(*, prev_s: float, prev_l: float, curr_s: float, curr_l: float) -> tuple[str, str]:
    if prev_s <= prev_l and curr_s > curr_l:
        return "BUY", "sma golden cross"
    if prev_s >= prev_l and curr_s < curr_l:
        return "SELL", "sma dead cross"
    return "HOLD", "sma no crossover"


def _resolve_exit_rule_names(raw: str) -> list[str]:
    return [token.strip().lower() for token in str(raw or "").split(",") if token.strip()]


def _load_last_reconcile_metadata(conn: sqlite3.Connection) -> str | None:
    try:
        row = conn.execute(
            "SELECT last_reconcile_metadata FROM bot_health WHERE id=1"
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None or row[0] is None:
        return None
    return str(row[0])


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
    cost_floor_ratio, required_edge_ratio = _compute_required_entry_edge_ratio(
        slippage_bps=slippage_bps,
        live_fee_rate_estimate=live_fee_rate_estimate,
        edge_buffer_ratio=edge_buffer_ratio,
        strategy_min_expected_edge_ratio=strategy_min_expected_edge_ratio,
    )
    expected_edge_ratio = max(0.0, float(gap_ratio))
    signal_eligible = base_signal in ("BUY", "SELL")
    enabled = bool(filter_enabled) and signal_eligible
    blocked = enabled and expected_edge_ratio < required_edge_ratio
    return blocked, {
        "enabled": enabled,
        "configured_enabled": bool(filter_enabled),
        "signal_eligible": signal_eligible,
        "blocked": blocked,
        "expected_edge_ratio": expected_edge_ratio,
        "required_edge_ratio": required_edge_ratio,
        "cost_floor_ratio": cost_floor_ratio,
        "roundtrip_fee_ratio": 2.0 * max(0.0, float(live_fee_rate_estimate)),
        "slippage_ratio": max(0.0, float(slippage_bps)) / 10_000.0,
        "buffer_ratio": max(0.0, float(edge_buffer_ratio)),
        "min_expected_edge_ratio": max(0.0, float(strategy_min_expected_edge_ratio)),
    }


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
) -> tuple[PositionContext, NormalizedExposure, PositionStateModel]:
    dust_context = build_dust_display_context(_load_last_reconcile_metadata(conn))
    try:
        if mark_harmless_dust_positions(
            conn,
            pair=pair,
            dust_metadata=dust_context,
        ) > 0:
            conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        row = conn.execute(
            """
            SELECT
                MIN(entry_ts) AS entry_ts,
                SUM(entry_price * qty_open) / NULLIF(SUM(qty_open), 0.0) AS avg_entry_price,
                SUM(qty_open) AS qty_open
            FROM open_position_lots
            WHERE pair=? AND position_state=? AND qty_open > 1e-12
            """,
            (pair, OPEN_POSITION_STATE),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None

    dust_tracking_qty = 0.0
    try:
        dust_row = conn.execute(
            """
            SELECT COALESCE(SUM(qty_open), 0.0) AS dust_tracking_qty
            FROM open_position_lots
            WHERE pair=? AND position_state=? AND qty_open > 1e-12
            """,
            (pair, "dust_tracking"),
        ).fetchone()
        if dust_row is not None and dust_row[0] is not None:
            dust_tracking_qty = max(0.0, float(dust_row[0]))
    except sqlite3.OperationalError:
        dust_tracking_qty = 0.0

    if row is None or row[0] is None or row[2] is None:
        position_state = build_position_state_model(
            raw_qty_open=0.0,
            metadata_raw=dust_context.classification,
            raw_total_asset_qty=float(dust_tracking_qty),
            open_exposure_qty=0.0,
            dust_tracking_qty=float(dust_tracking_qty),
        )
        exposure = position_state.normalized_exposure
        return (
            PositionContext(
                in_position=exposure.normalized_exposure_active,
                qty_open=exposure.raw_qty_open,
                recent_signal_context=dict(signal_context),
            ),
            exposure,
            position_state,
        )

    entry_ts = int(row[0])
    entry_price = float(row[1])
    qty_open = float(row[2])
    position_state = build_position_state_model(
        raw_qty_open=qty_open,
        metadata_raw=dust_context.classification,
        raw_total_asset_qty=qty_open + float(dust_tracking_qty),
        open_exposure_qty=qty_open,
        dust_tracking_qty=float(dust_tracking_qty),
    )
    exposure = position_state.normalized_exposure
    holding_time_sec = max(0.0, (int(candle_ts) - entry_ts) / 1000.0)
    unrealized_pnl = (float(market_price) - entry_price) * qty_open
    unrealized_pnl_ratio = _safe_ratio(float(market_price) - entry_price, entry_price)

    return (
        PositionContext(
            in_position=exposure.normalized_exposure_active,
            entry_ts=entry_ts,
            entry_price=entry_price,
            qty_open=qty_open,
            holding_time_sec=holding_time_sec,
            unrealized_pnl=unrealized_pnl,
            unrealized_pnl_ratio=unrealized_pnl_ratio,
            recent_signal_context=dict(signal_context),
        ),
        exposure,
        position_state,
    )


def _apply_entry_exit_policy(
    *,
    base_signal: str,
    base_reason: str,
    base_context: dict[str, Any],
    position: PositionContext,
    exit_rules: list[ExitRule],
) -> StrategyDecision:
    def _annotate_decision_context(
        context: dict[str, Any],
        *,
        raw_signal: str,
        final_signal: str,
        final_reason: str,
        position: PositionContext,
    ) -> dict[str, Any]:
        entry = context.get("entry") if isinstance(context.get("entry"), dict) else {}
        position_gate = context.get("position_gate") if isinstance(context.get("position_gate"), dict) else {}
        position_state = context.get("position_state") if isinstance(context.get("position_state"), dict) else {}
        normalized_state = (
            position_state.get("normalized_exposure")
            if isinstance(position_state.get("normalized_exposure"), dict)
            else {}
        )
        entry_signal = str(entry.get("entry_signal", raw_signal)).strip().upper() or raw_signal
        filtered_entry = raw_signal in {"BUY", "SELL"} and raw_signal != entry_signal
        entry_blocked = raw_signal in {"BUY", "SELL"} and final_signal != raw_signal
        entry_block_reason: str | None = None
        if entry_blocked:
            if filtered_entry:
                entry_block_reason = str(entry.get("entry_reason") or context.get("reason") or "").strip() or None
            else:
                entry_block_reason = str(final_reason or "").strip() or None
        dust_classification = str(
            normalized_state.get(
                "dust_classification",
                position_gate.get("dust_classification", position_gate.get("dust_state", "")),
            )
        ).strip()
        entry_allowed = bool(
            normalized_state.get(
                "entry_allowed",
                position_gate.get(
                    "entry_allowed",
                    position_gate.get(
                        "effective_flat_due_to_harmless_dust",
                        position_gate.get("dust_treat_as_flat"),
                    ),
                ),
            )
        )
        effective_flat = bool(
            normalized_state.get(
                "effective_flat",
                position_gate.get("effective_flat_due_to_harmless_dust", position_gate.get("dust_treat_as_flat")),
            )
        )
        raw_qty_open = float(
            normalized_state.get(
                "raw_qty_open",
                position_gate.get("raw_qty_open", position.qty_open),
            )
        )
        open_exposure_qty = float(
            normalized_state.get(
                "open_exposure_qty",
                position_gate.get("open_exposure_qty", raw_qty_open),
            )
        )
        dust_tracking_qty = float(
            normalized_state.get(
                "dust_tracking_qty",
                position_gate.get("dust_tracking_qty", 0.0),
            )
        )
        normalized_exposure_active = bool(
            normalized_state.get(
                "normalized_exposure_active",
                position_gate.get(
                    "normalized_exposure_active",
                    open_exposure_qty > 1e-12 and not entry_allowed,
                ),
            )
        )
        normalized_exposure_qty = float(
            normalized_state.get(
                "normalized_exposure_qty",
                position_gate.get(
                    "normalized_exposure_qty",
                    open_exposure_qty if normalized_exposure_active else 0.0,
                ),
            )
        )

        context["raw_signal"] = raw_signal
        context["final_signal"] = final_signal
        context["entry_blocked"] = entry_blocked
        context["entry_block_reason"] = entry_block_reason
        context["dust_classification"] = dust_classification
        context["entry_allowed"] = entry_allowed
        context["effective_flat"] = effective_flat
        context["raw_qty_open"] = raw_qty_open
        context["open_exposure_qty"] = open_exposure_qty
        context["dust_tracking_qty"] = dust_tracking_qty
        context["normalized_exposure_active"] = normalized_exposure_active
        context["normalized_exposure_qty"] = normalized_exposure_qty
        context["decision_summary"] = {
            "raw_signal": raw_signal,
            "final_signal": final_signal,
            "entry_blocked": entry_blocked,
            "entry_block_reason": entry_block_reason,
            "dust_classification": dust_classification,
            "entry_allowed": entry_allowed,
            "effective_flat": effective_flat,
            "raw_qty_open": raw_qty_open,
            "open_exposure_qty": open_exposure_qty,
            "dust_tracking_qty": dust_tracking_qty,
            "normalized_exposure_active": normalized_exposure_active,
            "normalized_exposure_qty": normalized_exposure_qty,
        }
        return context

    if not position.in_position:
        context = _annotate_decision_context(
            dict(base_context),
            raw_signal=str(base_context.get("entry", {}).get("base_signal", base_signal)),
            final_signal=base_signal,
            final_reason=base_reason,
            position=position,
        )
        return StrategyDecision(signal=base_signal, reason=base_reason, context=context)

    exit_results: list[dict[str, Any]] = []
    for rule in exit_rules:
        rule_result = rule.evaluate(
            position=position,
            candle_ts=int(base_context["ts"]),
            market_price=float(base_context["last_close"]),
            signal_context={
                "base_signal": base_signal,
                "base_reason": base_reason,
                "curr_s": base_context["curr_s"],
                "curr_l": base_context["curr_l"],
            },
        )
        exit_results.append(
            {
                "rule": rule.name,
                "triggered": bool(rule_result.should_exit),
                "reason": rule_result.reason,
                "context": rule_result.context,
            }
        )
        if rule_result.should_exit:
            context = dict(base_context)
            context["position"] = position.as_dict()
            context["exit"] = {
                "triggered": True,
                "rule": rule.name,
                "reason": rule_result.reason,
                "evaluations": exit_results,
            }
            context = _annotate_decision_context(
                context,
                raw_signal=str(base_context.get("entry", {}).get("base_signal", base_signal)),
                final_signal="SELL",
                final_reason=rule_result.reason,
                position=position,
            )
            return StrategyDecision(signal="SELL", reason=rule_result.reason, context=context)

    context = dict(base_context)
    context["position"] = position.as_dict()
    context["exit"] = {
        "triggered": False,
        "rule": None,
        "reason": "no exit rule triggered",
        "evaluations": exit_results,
    }
    context = _annotate_decision_context(
        context,
        raw_signal=str(base_context.get("entry", {}).get("base_signal", base_signal)),
        final_signal="HOLD",
        final_reason="position held: no exit rule triggered",
        position=position,
    )
    return StrategyDecision(signal="HOLD", reason="position held: no exit rule triggered", context=context)


@dataclass(frozen=True)
class SmaCrossStrategy:
    short_n: int
    long_n: int
    pair: str = settings.PAIR
    interval: str = settings.INTERVAL
    exit_rule_names: list[str] = field(
        default_factory=lambda: _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
    )
    exit_max_holding_min: int = settings.STRATEGY_EXIT_MAX_HOLDING_MIN
    exit_min_take_profit_ratio: float = settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
    exit_small_loss_tolerance_ratio: float = settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO
    slippage_bps: float = settings.STRATEGY_ENTRY_SLIPPAGE_BPS
    live_fee_rate_estimate: float = settings.LIVE_FEE_RATE_ESTIMATE
    entry_edge_buffer_ratio: float = settings.ENTRY_EDGE_BUFFER_RATIO
    strategy_min_expected_edge_ratio: float = settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO

    name: str = "sma_cross"

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None:
        if self.short_n >= self.long_n:
            raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

        rows = _load_signal_rows(
            conn,
            pair=self.pair,
            interval=self.interval,
            through_ts_ms=through_ts_ms,
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
        edge_filter_triggered, edge_filter_details = _evaluate_entry_edge_filter(
            base_signal=base_signal,
            gap_ratio=gap_ratio,
            slippage_bps=float(self.slippage_bps),
            live_fee_rate_estimate=float(self.live_fee_rate_estimate),
            edge_buffer_ratio=float(self.entry_edge_buffer_ratio),
            strategy_min_expected_edge_ratio=float(self.strategy_min_expected_edge_ratio),
        )
        entry_signal = base_signal
        entry_reason = base_reason
        if edge_filter_triggered:
            entry_signal = "HOLD"
            entry_reason = "filtered entry: cost_edge"
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
        position, exposure, position_state = _load_position_context(
            conn,
            pair=self.pair,
            candle_ts=ts_list[-1],
            market_price=float(closes[-1]),
            signal_context=signal_context,
        )
        exit_rules = create_exit_rules(
            rule_names=self.exit_rule_names,
            max_holding_sec=float(self.exit_max_holding_min) * 60.0,
            min_take_profit_ratio=float(self.exit_min_take_profit_ratio),
            live_fee_rate_estimate=float(self.live_fee_rate_estimate),
            small_loss_tolerance_ratio=float(self.exit_small_loss_tolerance_ratio),
        )
        base_context = {
            "ts": ts_list[-1],
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
            "last_close": float(closes[-1]),
            "strategy": self.name,
            "gap_ratio": gap_ratio,
            "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
            "blocked_by_cost_filter": bool(edge_filter_triggered),
            "signal_strength_label": signal_strength_label,
            "signal_strength": {
                "label": signal_strength_label,
                "gap_ratio": gap_ratio,
                "required_edge_ratio": float(edge_filter_details["required_edge_ratio"]),
                "is_weak_cross": bool(signal_strength_label == "weak"),
                # NOTE: sma_cross는 단순 교차 전략이며, 실거래 우선 전략은 sma_with_filter다.
                "preferred_live_strategy": "sma_with_filter",
            },
            "entry": {
                "base_signal": base_signal,
                "base_reason": base_reason,
                "entry_signal": entry_signal,
                "entry_reason": entry_reason,
            },
            "position_gate": {
                **exposure.as_dict(),
            },
            "position_state": {
                "raw_holdings": position_state.raw_holdings.as_dict(),
                "normalized_exposure": exposure.as_dict(),
                "operator_diagnostics": {
                    "state": exposure.dust_operator_view.state,
                    "state_label": exposure.dust_operator_view.state_label,
                    "operator_action": exposure.dust_operator_view.operator_action,
                    "operator_message": exposure.dust_operator_view.operator_message,
                    "broker_local_match": bool(exposure.dust_operator_view.broker_local_match),
                    "new_orders_allowed": bool(exposure.dust_operator_view.new_orders_allowed),
                    "resume_allowed": bool(exposure.dust_operator_view.resume_allowed),
                    "treat_as_flat": bool(exposure.dust_operator_view.treat_as_flat),
                },
            },
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
                }
            },
        }
        return _apply_entry_exit_policy(
            base_signal=entry_signal,
            base_reason=entry_reason,
            base_context=base_context,
            position=position,
            exit_rules=exit_rules,
        )


@dataclass(frozen=True)
class SmaWithFilterStrategy:
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
    exit_rule_names: list[str] = field(
        default_factory=lambda: _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
    )
    exit_max_holding_min: int = settings.STRATEGY_EXIT_MAX_HOLDING_MIN
    exit_min_take_profit_ratio: float = settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO
    exit_small_loss_tolerance_ratio: float = settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO

    name: str = "sma_with_filter"

    def decide(
        self,
        conn: sqlite3.Connection,
        *,
        through_ts_ms: int | None = None,
    ) -> StrategyDecision | None:
        if self.short_n >= self.long_n:
            raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

        min_rows = max(
            self.long_n + 2,
            int(self.volatility_window),
            int(self.overextended_lookback) + 1,
        )
        rows = _load_signal_rows(
            conn,
            pair=self.pair,
            interval=self.interval,
            through_ts_ms=through_ts_ms,
        )
        if len(rows) < min_rows:
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

        vol_window = max(1, int(self.volatility_window))
        vol_closes = closes[-vol_window:]
        vol_mean = fmean(vol_closes)
        volatility_ratio = _safe_ratio((max(vol_closes) - min(vol_closes)), vol_mean)

        overext_lookback = max(1, int(self.overextended_lookback))
        base_close = closes[-1 - overext_lookback]
        overextended_ratio = abs(_safe_ratio(closes[-1] - base_close, base_close))

        gap_filter_enabled = float(self.min_gap_ratio) > 0
        volatility_filter_enabled = float(self.min_volatility_ratio) > 0
        overextended_filter_enabled = float(self.overextended_max_return_ratio) > 0

        gap_triggered = gap_filter_enabled and gap_ratio < float(self.min_gap_ratio)
        volatility_triggered = (
            volatility_filter_enabled and volatility_ratio < float(self.min_volatility_ratio)
        )
        overextended_triggered = (
            overextended_filter_enabled
            and overextended_ratio > float(self.overextended_max_return_ratio)
        )
        edge_filter_triggered, edge_filter_details = _evaluate_entry_edge_filter(
            base_signal=base_signal,
            gap_ratio=gap_ratio,
            slippage_bps=float(self.slippage_bps),
            live_fee_rate_estimate=float(self.live_fee_rate_estimate),
            edge_buffer_ratio=float(self.entry_edge_buffer_ratio),
            strategy_min_expected_edge_ratio=float(self.cost_edge_min_ratio),
            filter_enabled=bool(self.cost_edge_enabled),
        )

        blocked_filters = []
        if gap_triggered:
            blocked_filters.append("gap")
        if volatility_triggered:
            blocked_filters.append("volatility")
        if overextended_triggered:
            blocked_filters.append("overextended")
        if edge_filter_triggered:
            blocked_filters.append("cost_edge")

        should_filter_entry = base_signal in ("BUY", "SELL")
        entry_signal = base_signal
        entry_reason = base_reason
        if should_filter_entry and blocked_filters:
            entry_signal = "HOLD"
            entry_reason = f"filtered entry: {', '.join(blocked_filters)}"

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
        position, exposure, position_state = _load_position_context(
            conn,
            pair=self.pair,
            candle_ts=ts_list[-1],
            market_price=float(closes[-1]),
            signal_context=signal_context,
        )
        exit_rules = create_exit_rules(
            rule_names=self.exit_rule_names,
            max_holding_sec=float(self.exit_max_holding_min) * 60.0,
            min_take_profit_ratio=float(self.exit_min_take_profit_ratio),
            live_fee_rate_estimate=float(self.live_fee_rate_estimate),
            small_loss_tolerance_ratio=float(self.exit_small_loss_tolerance_ratio),
        )

        base_context = {
            "ts": ts_list[-1],
            "last_close": float(closes[-1]),
            "strategy": self.name,
            "prev_s": prev_s,
            "prev_l": prev_l,
            "curr_s": curr_s,
            "curr_l": curr_l,
            "features": {
                "prev_s": prev_s,
                "prev_l": prev_l,
                "curr_s": curr_s,
                "curr_l": curr_l,
                "sma_gap_ratio": gap_ratio,
                "volatility_range_ratio": volatility_ratio,
                "overextended_abs_return_ratio": overextended_ratio,
                "base_signal": base_signal,
                "base_reason": base_reason,
            },
            "position_gate": {
                **exposure.as_dict(),
            },
            "position_state": {
                "raw_holdings": exposure.raw_holdings.as_dict(),
                "normalized_exposure": exposure.as_dict(),
                "operator_diagnostics": {
                    "state": exposure.dust_operator_view.state,
                    "state_label": exposure.dust_operator_view.state_label,
                    "operator_action": exposure.dust_operator_view.operator_action,
                    "operator_message": exposure.dust_operator_view.operator_message,
                    "broker_local_match": bool(exposure.dust_operator_view.broker_local_match),
                    "new_orders_allowed": bool(exposure.dust_operator_view.new_orders_allowed),
                    "resume_allowed": bool(exposure.dust_operator_view.resume_allowed),
                    "treat_as_flat": bool(exposure.dust_operator_view.treat_as_flat),
                },
            },
            "filters": {
                "gap": {
                    "enabled": gap_filter_enabled,
                    "passed": not gap_triggered,
                    "threshold": float(self.min_gap_ratio),
                    "value": gap_ratio,
                },
                "volatility": {
                    "enabled": volatility_filter_enabled,
                    "passed": not volatility_triggered,
                    "window": vol_window,
                    "threshold": float(self.min_volatility_ratio),
                    "value": volatility_ratio,
                },
                "overextended": {
                    "enabled": overextended_filter_enabled,
                    "passed": not overextended_triggered,
                    "lookback": overext_lookback,
                    "threshold": float(self.overextended_max_return_ratio),
                    "value": overextended_ratio,
                },
                "cost_edge": {
                    "enabled": bool(edge_filter_details["enabled"]),
                    "configured_enabled": bool(edge_filter_details["configured_enabled"]),
                    "signal_eligible": bool(edge_filter_details["signal_eligible"]),
                    "passed": not bool(edge_filter_details["blocked"]),
                    "value": float(edge_filter_details["expected_edge_ratio"]),
                    "threshold": float(edge_filter_details["required_edge_ratio"]),
                    "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
                    "roundtrip_fee_ratio": float(edge_filter_details["roundtrip_fee_ratio"]),
                    "slippage_ratio": float(edge_filter_details["slippage_ratio"]),
                    "buffer_ratio": float(edge_filter_details["buffer_ratio"]),
                    "min_expected_edge_ratio": float(edge_filter_details["min_expected_edge_ratio"]),
                },
            },
            "filter_blocked": bool(should_filter_entry and blocked_filters),
            "blocked_filters": blocked_filters,
            "gap_ratio": gap_ratio,
            "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
            "blocked_by_cost_filter": bool(should_filter_entry and edge_filter_triggered),
            "entry": {
                "base_signal": base_signal,
                "base_reason": base_reason,
                "entry_signal": entry_signal,
                "entry_reason": entry_reason,
                "cost_edge_blocked": bool(should_filter_entry and edge_filter_triggered),
            },
        }

        return _apply_entry_exit_policy(
            base_signal=entry_signal,
            base_reason=entry_reason,
            base_context=base_context,
            position=position,
            exit_rules=exit_rules,
        )


def create_sma_strategy(
    *,
    short_n: int | None = None,
    long_n: int | None = None,
    pair: str | None = None,
    interval: str | None = None,
    exit_rule_names: list[str] | None = None,
    exit_max_holding_min: int | None = None,
    exit_min_take_profit_ratio: float | None = None,
    exit_small_loss_tolerance_ratio: float | None = None,
    slippage_bps: float | None = None,
    entry_edge_buffer_ratio: float | None = None,
    strategy_min_expected_edge_ratio: float | None = None,
    live_fee_rate_estimate: float | None = None,
) -> SmaCrossStrategy:
    return SmaCrossStrategy(
        short_n=int(settings.SMA_SHORT if short_n is None else short_n),
        long_n=int(settings.SMA_LONG if long_n is None else long_n),
        pair=settings.PAIR if pair is None else str(pair),
        interval=settings.INTERVAL if interval is None else str(interval),
        exit_rule_names=(
            _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
            if exit_rule_names is None
            else [str(name).strip().lower() for name in exit_rule_names if str(name).strip()]
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
        slippage_bps=float(
            settings.STRATEGY_ENTRY_SLIPPAGE_BPS if slippage_bps is None else slippage_bps
        ),
        entry_edge_buffer_ratio=float(
            settings.ENTRY_EDGE_BUFFER_RATIO
            if entry_edge_buffer_ratio is None
            else entry_edge_buffer_ratio
        ),
        strategy_min_expected_edge_ratio=float(
            settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO
            if strategy_min_expected_edge_ratio is None
            else strategy_min_expected_edge_ratio
        ),
        live_fee_rate_estimate=float(
            settings.LIVE_FEE_RATE_ESTIMATE
            if live_fee_rate_estimate is None
            else live_fee_rate_estimate
        ),
    )


def create_sma_with_filter_strategy(
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
    exit_rule_names: list[str] | None = None,
    exit_max_holding_min: int | None = None,
    exit_min_take_profit_ratio: float | None = None,
    exit_small_loss_tolerance_ratio: float | None = None,
) -> SmaWithFilterStrategy:
    return SmaWithFilterStrategy(
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
        exit_rule_names=(
            _resolve_exit_rule_names(settings.STRATEGY_EXIT_RULES)
            if exit_rule_names is None
            else [str(name).strip().lower() for name in exit_rule_names if str(name).strip()]
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
