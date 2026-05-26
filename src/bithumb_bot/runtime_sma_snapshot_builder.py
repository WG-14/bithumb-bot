from __future__ import annotations

import sqlite3
import time
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .broker.order_rules import get_effective_order_rules
from .canonical_decision import order_rules_snapshot_payload
from .config import settings
from .core.sma_policy import (
    ExecutionConstraintSnapshot,
    MarketWindow,
    PositionSnapshot,
    SmaPolicyConfig,
    StrategyDecisionV2,
)
from .decision_contract import build_replay_fingerprint
from .dust import build_dust_display_context, build_position_state_model
from .fee_authority import build_fee_authority_snapshot
from .lifecycle import (
    OPEN_POSITION_STATE,
    summarize_position_lots,
    summarize_reserved_exit_qty,
)
from .runtime_position_state_normalizer import (
    PositionStateNormalizer,
    load_last_reconcile_metadata,
)
from .runtime_sma_context import (
    build_entry_decision_context,
    build_position_gate_context,
    build_position_state_context,
    fee_authority_context,
    legacy_strategy_decision_from_sma_final_decision,
    live_armed_entry_fee_authority_blocks,
    resolve_strategy_fee_authority,
    safe_ratio,
    sma,
)
from .strategy.base import PositionContext, StrategyDecision
from .strategy.exit_rules import ExitPolicyConfig
from .strategy.sma_decision_assembler import evaluate_sma_final_decision


@dataclass(frozen=True)
class RuntimeSmaPolicyHashes:
    pure_policy_hash: str
    policy_contract_hash: str
    policy_input_hash: str
    policy_decision_hash: str

    def as_dict(self) -> dict[str, str]:
        return {
            "pure_policy_hash": self.pure_policy_hash,
            "policy_contract_hash": self.policy_contract_hash,
            "policy_input_hash": self.policy_input_hash,
            "policy_decision_hash": self.policy_decision_hash,
        }


@dataclass(frozen=True)
class RuntimeSmaReplayFingerprint:
    payload: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return dict(self.payload)


@dataclass(frozen=True)
class RuntimeSmaDecisionContext:
    """Typed authority-critical SMA decision context.

    ``as_dict`` is for persistence and replay observability. Execution
    authority stays with typed policy and execution objects.
    """

    policy_contract_hash: str
    policy_input_hash: str
    policy_decision_hash: str
    pure_policy_hash: str
    pure_policy_trace: dict[str, object]
    final_signal: str
    final_reason: str
    blocked_filters: tuple[str, ...]
    entry_blocked: bool
    entry_block_reason: str
    execution_intent: dict[str, object] | None
    replay_fingerprint: dict[str, object]

    @classmethod
    def from_decision(
        cls,
        *,
        decision: StrategyDecisionV2,
        replay_fingerprint: dict[str, object],
    ) -> "RuntimeSmaDecisionContext":
        trace = decision.as_trace()
        raw_intent = trace.get("execution_intent")
        return cls(
            policy_contract_hash=decision.policy_contract_hash,
            policy_input_hash=decision.policy_input_hash,
            policy_decision_hash=decision.policy_decision_hash,
            pure_policy_hash=decision.policy_hash,
            pure_policy_trace=deepcopy(trace),
            final_signal=decision.final_signal,
            final_reason=decision.final_reason,
            blocked_filters=tuple(str(item) for item in decision.blocked_filters),
            entry_blocked=bool(decision.entry_blocked),
            entry_block_reason=str(decision.entry_block_reason or ""),
            execution_intent=dict(raw_intent) if isinstance(raw_intent, dict) else None,
            replay_fingerprint=dict(replay_fingerprint),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "policy_contract_hash": self.policy_contract_hash,
            "policy_input_hash": self.policy_input_hash,
            "policy_decision_hash": self.policy_decision_hash,
            "pure_policy_hash": self.pure_policy_hash,
            "pure_policy_trace": deepcopy(self.pure_policy_trace),
            "final_signal": self.final_signal,
            "final_reason": self.final_reason,
            "blocked_filters": list(self.blocked_filters),
            "entry_blocked": bool(self.entry_blocked),
            "entry_block_reason": self.entry_block_reason,
            "execution_intent": (
                None if self.execution_intent is None else deepcopy(self.execution_intent)
            ),
            "replay_fingerprint": deepcopy(self.replay_fingerprint),
        }


@dataclass(frozen=True)
class RuntimeSmaDecisionResult:
    """Typed SMA runtime decision before legacy persistence serialization."""

    decision: StrategyDecisionV2
    base_context: dict[str, Any]
    position: PositionContext
    exposure: object
    position_state: object
    candle_ts: int
    market_price: float
    replay_fingerprint: dict[str, object]

    def __post_init__(self) -> None:
        # ``base_context`` is legacy serialization material. Keep a private copy
        # boundary so callers cannot mutate the originally supplied mapping after
        # construction and accidentally alter this result's compatibility payload.
        object.__setattr__(self, "base_context", dict(self.base_context))
        object.__setattr__(self, "replay_fingerprint", dict(self.replay_fingerprint))

    @property
    def policy_hashes(self) -> RuntimeSmaPolicyHashes:
        return RuntimeSmaPolicyHashes(
            pure_policy_hash=self.decision.policy_hash,
            policy_contract_hash=self.decision.policy_contract_hash,
            policy_input_hash=self.decision.policy_input_hash,
            policy_decision_hash=self.decision.policy_decision_hash,
        )

    @property
    def replay_fingerprint_snapshot(self) -> RuntimeSmaReplayFingerprint:
        return RuntimeSmaReplayFingerprint(self.replay_fingerprint)

    @property
    def runtime_decision_context(self) -> RuntimeSmaDecisionContext:
        return RuntimeSmaDecisionContext.from_decision(
            decision=self.decision,
            replay_fingerprint=self.replay_fingerprint,
        )

    @property
    def policy_observability(self) -> dict[str, object]:
        context = self.runtime_decision_context.as_dict()
        return {
            **self.policy_hashes.as_dict(),
            "pure_policy_trace": context["pure_policy_trace"],
            "final_signal": context["final_signal"],
            "final_reason": context["final_reason"],
            "blocked_filters": context["blocked_filters"],
            "entry_blocked": context["entry_blocked"],
            "entry_block_reason": context["entry_block_reason"],
            "exit_rule": self.decision.exit_rule,
            "exit_evaluations": [dict(item) for item in self.decision.exit_evaluations],
            "execution_intent": context["execution_intent"],
            "replay_fingerprint": context["replay_fingerprint"],
        }

    def _authoritative_policy_context(self) -> dict[str, object]:
        return {
            "pure_policy_hash": self.decision.policy_hash,
            "policy_contract_hash": self.decision.policy_contract_hash,
            "policy_input_hash": self.decision.policy_input_hash,
            "policy_decision_hash": self.decision.policy_decision_hash,
            "pure_policy_trace": self.decision.as_trace(),
        }

    def legacy_strategy_decision(self) -> StrategyDecision:
        return legacy_strategy_decision_from_sma_final_decision(
            decision=self.decision,
            base_context={**dict(self.base_context), **self._authoritative_policy_context()},
            position=self.position,
            exposure=self.exposure,
            position_state=self.position_state,
        )

    def as_legacy_dict(self) -> dict[str, Any]:
        payload = self.legacy_strategy_decision().as_dict()
        payload.update(self._authoritative_policy_context())
        return payload


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


def _closed_candle_cutoff_ts_ms(*, interval_sec: int, now_ms: int | None = None) -> int | None:
    """Return the latest candle start timestamp that is safely closed now."""
    interval_ms = max(1, int(interval_sec)) * 1000
    close_guard_ms = max(2_000, min(30_000, interval_ms // 20))
    current_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    cutoff_ts_ms = current_ms - interval_ms - close_guard_ms
    return cutoff_ts_ms if cutoff_ts_ms >= 0 else None


def _load_position_context(
    conn: sqlite3.Connection,
    *,
    pair: str,
    candle_ts: int,
    market_price: float,
    signal_context: dict[str, Any],
    slippage_bps: float,
    entry_edge_buffer_ratio: float,
) -> tuple[PositionContext, object, object, dict[str, object]]:
    dust_context = build_dust_display_context(load_last_reconcile_metadata(conn))
    resolution = get_effective_order_rules(pair)
    rules = resolution.rules
    order_rules_snapshot = order_rules_snapshot_payload(resolution, pair=pair)
    fee_authority = build_fee_authority_snapshot(resolution)
    reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=pair)
    try:
        row = conn.execute(
            """
            SELECT
                MIN(entry_ts) AS entry_ts,
                SUM(entry_price * qty_open) / NULLIF(SUM(qty_open), 0.0) AS avg_entry_price,
                SUM(qty_open) AS qty_open
            FROM open_position_lots
            WHERE pair=? AND position_state=? AND qty_open > 1e-12
              AND COALESCE(position_semantic_basis, '')='lot-native'
              AND COALESCE(executable_lot_count, 0) > 0
              AND COALESCE(dust_tracking_lot_count, 0) = 0
            """,
            (pair, OPEN_POSITION_STATE),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None

    if row is None or row[0] is None or row[2] is None:
        lot_snapshot = summarize_position_lots(conn, pair=pair)
        lot_definition = getattr(lot_snapshot, "lot_definition", None)
        tracked_qty = float(lot_snapshot.raw_total_asset_qty)
        raw_qty_open = (
            tracked_qty
            if (
                tracked_qty > 1e-12
                and dust_context.classification.classification == "harmless_dust"
                and not dust_context.effective_flat_due_to_harmless_dust
            )
            else 0.0
        )
        position_state = build_position_state_model(
            raw_qty_open=raw_qty_open,
            metadata_raw=dust_context.classification,
            raw_total_asset_qty=tracked_qty,
            open_exposure_qty=0.0,
            dust_tracking_qty=lot_snapshot.dust_tracking_qty,
            reserved_exit_qty=reserved_exit_qty,
            open_lot_count=lot_snapshot.open_lot_count,
            dust_tracking_lot_count=lot_snapshot.dust_tracking_lot_count,
            internal_lot_size=(None if lot_definition is None else lot_definition.internal_lot_size),
            market_price=float(market_price),
            min_qty=(
                float(rules.min_qty)
                if lot_definition is None or lot_definition.min_qty is None
                else lot_definition.min_qty
            ),
            qty_step=(
                float(rules.qty_step)
                if lot_definition is None or lot_definition.qty_step is None
                else lot_definition.qty_step
            ),
            min_notional_krw=(
                float(rules.min_notional_krw)
                if lot_definition is None or lot_definition.min_notional_krw is None
                else lot_definition.min_notional_krw
            ),
            max_qty_decimals=(
                int(rules.max_qty_decimals)
                if lot_definition is None or lot_definition.max_qty_decimals is None
                else lot_definition.max_qty_decimals
            ),
            exit_fee_ratio=float(fee_authority.taker_ask_fee_rate),
            exit_slippage_bps=float(slippage_bps),
            exit_buffer_ratio=float(entry_edge_buffer_ratio),
        )
        exposure = position_state.normalized_exposure
        return (
            PositionContext(
                in_position=bool(exposure.normalized_exposure_active),
                qty_open=float(exposure.normalized_exposure_qty),
                recent_signal_context=dict(signal_context),
            ),
            exposure,
            position_state,
            order_rules_snapshot,
        )

    entry_ts = int(row[0])
    entry_price = float(row[1])
    tracked_open_qty = float(row[2])
    lot_snapshot = summarize_position_lots(conn, pair=pair)
    lot_definition = getattr(lot_snapshot, "lot_definition", None)
    position_state = build_position_state_model(
        raw_qty_open=tracked_open_qty,
        metadata_raw=dust_context.classification,
        raw_total_asset_qty=float(lot_snapshot.raw_total_asset_qty),
        open_exposure_qty=tracked_open_qty,
        dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
        reserved_exit_qty=reserved_exit_qty,
        open_lot_count=int(lot_snapshot.open_lot_count),
        dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
        internal_lot_size=(None if lot_definition is None else lot_definition.internal_lot_size),
        market_price=float(market_price),
        min_qty=(
            float(rules.min_qty)
            if lot_definition is None or lot_definition.min_qty is None
            else lot_definition.min_qty
        ),
        qty_step=(
            float(rules.qty_step)
            if lot_definition is None or lot_definition.qty_step is None
            else lot_definition.qty_step
        ),
        min_notional_krw=(
            float(rules.min_notional_krw)
            if lot_definition is None or lot_definition.min_notional_krw is None
            else lot_definition.min_notional_krw
        ),
        max_qty_decimals=(
            int(rules.max_qty_decimals)
            if lot_definition is None or lot_definition.max_qty_decimals is None
            else lot_definition.max_qty_decimals
        ),
        exit_fee_ratio=float(fee_authority.taker_ask_fee_rate),
        exit_slippage_bps=float(slippage_bps),
        exit_buffer_ratio=float(entry_edge_buffer_ratio),
    )
    exposure = position_state.normalized_exposure
    holding_time_sec = max(0.0, (int(candle_ts) - entry_ts) / 1000.0)
    unrealized_pnl = (float(market_price) - entry_price) * tracked_open_qty
    unrealized_pnl_ratio = safe_ratio(float(market_price) - entry_price, entry_price)

    return (
        PositionContext(
            in_position=bool(exposure.normalized_exposure_active),
            entry_ts=entry_ts,
            entry_price=entry_price,
            qty_open=tracked_open_qty,
            holding_time_sec=holding_time_sec,
            unrealized_pnl=unrealized_pnl,
            unrealized_pnl_ratio=unrealized_pnl_ratio,
            recent_signal_context=dict(signal_context),
        ),
        exposure,
        position_state,
        order_rules_snapshot,
    )


def _policy_position_snapshot(
    *,
    position: PositionContext,
    exposure: object,
) -> PositionSnapshot:
    return PositionSnapshot(
        in_position=bool(position.in_position),
        entry_allowed=bool(exposure.entry_allowed),
        exit_allowed=bool(exposure.exit_allowed),
        entry_block_reason=str(exposure.entry_block_reason or ""),
        exit_block_reason=str(exposure.exit_block_reason or ""),
        terminal_state=str(exposure.terminal_state),
        entry_ts=position.entry_ts,
        entry_price=position.entry_price,
        qty_open=float(position.qty_open),
        holding_time_sec=float(position.holding_time_sec),
        unrealized_pnl=float(position.unrealized_pnl),
        unrealized_pnl_ratio=float(position.unrealized_pnl_ratio),
        raw_qty_open=float(exposure.raw_qty_open),
        raw_total_asset_qty=float(exposure.raw_total_asset_qty),
        open_lot_count=int(exposure.open_lot_count),
        dust_tracking_lot_count=int(exposure.dust_tracking_lot_count),
        reserved_exit_lot_count=int(exposure.reserved_exit_lot_count),
        sellable_executable_lot_count=int(exposure.sellable_executable_lot_count),
        dust_classification=str(exposure.dust_classification),
        dust_state=str(exposure.dust_state),
        effective_flat=bool(exposure.effective_flat),
        has_executable_exposure=bool(exposure.has_executable_exposure),
        has_any_position_residue=bool(exposure.has_any_position_residue),
        has_non_executable_residue=bool(exposure.has_non_executable_residue),
        has_dust_only_remainder=bool(exposure.has_dust_only_remainder),
    )


def build_sma_with_filter_runtime_decision_from_normalized_db(
    conn: sqlite3.Connection,
    strategy: object,
    *,
    through_ts_ms: int | None = None,
) -> RuntimeSmaDecisionResult | None:
    """Read normalized DB state and return the typed final SMA runtime decision."""
    from .utils_time import parse_interval_sec

    if int(strategy.short_n) >= int(strategy.long_n):
        raise ValueError("short는 long보다 작아야 해. 예: short=7 long=30")

    min_rows = max(
        int(strategy.long_n) + 2,
        int(strategy.volatility_window),
        int(strategy.overextended_lookback) + 1,
    )
    interval_sec = parse_interval_sec(str(strategy.interval))
    signal_through_ts_ms = through_ts_ms
    if signal_through_ts_ms is None:
        signal_through_ts_ms = _closed_candle_cutoff_ts_ms(interval_sec=interval_sec)
        if signal_through_ts_ms is None:
            return None

    rows = _load_signal_rows(
        conn,
        pair=strategy.pair,
        interval=strategy.interval,
        through_ts_ms=signal_through_ts_ms,
    )
    if len(rows) < min_rows:
        return None

    closes = [float(r[1]) for r in rows]
    ts_list = [int(r[0]) for r in rows]

    end_prev = len(closes) - 1
    end_curr = len(closes)

    prev_s = sma(closes, int(strategy.short_n), end_prev)
    prev_l = sma(closes, int(strategy.long_n), end_prev)
    curr_s = sma(closes, int(strategy.short_n), end_curr)
    curr_l = sma(closes, int(strategy.long_n), end_curr)

    fee_authority = resolve_strategy_fee_authority(
        pair=strategy.pair,
        config_fallback_fee_rate=float(strategy.live_fee_rate_estimate),
    )
    fee_rate_for_decision = float(fee_authority.taker_roundtrip_fee_rate / 2)
    signal_context = {
        "strategy": strategy.name,
        "prev_s": prev_s,
        "prev_l": prev_l,
        "curr_s": curr_s,
        "curr_l": curr_l,
    }
    position, exposure, position_state, order_rules_snapshot = _load_position_context(
        conn,
        pair=strategy.pair,
        candle_ts=ts_list[-1],
        market_price=float(closes[-1]),
        signal_context=signal_context,
        slippage_bps=float(strategy.slippage_bps),
        entry_edge_buffer_ratio=float(strategy.entry_edge_buffer_ratio),
    )
    market_snapshot = MarketWindow(
        pair=strategy.pair,
        interval=strategy.interval,
        candle_ts=int(ts_list[-1]),
        closes=tuple(float(value) for value in closes),
        prev_s=float(prev_s),
        prev_l=float(prev_l),
        curr_s=float(curr_s),
        curr_l=float(curr_l),
        through_ts_ms=signal_through_ts_ms,
    )
    position_snapshot = _policy_position_snapshot(position=position, exposure=exposure)
    policy_config = SmaPolicyConfig(
        strategy_name=strategy.name,
        short_n=int(strategy.short_n),
        long_n=int(strategy.long_n),
        min_gap_ratio=float(strategy.min_gap_ratio),
        volatility_window=int(strategy.volatility_window),
        min_volatility_ratio=float(strategy.min_volatility_ratio),
        overextended_lookback=int(strategy.overextended_lookback),
        overextended_max_return_ratio=float(strategy.overextended_max_return_ratio),
        slippage_bps=float(strategy.slippage_bps),
        live_fee_rate_estimate=float(strategy.live_fee_rate_estimate),
        entry_edge_buffer_ratio=float(strategy.entry_edge_buffer_ratio),
        cost_edge_enabled=bool(strategy.cost_edge_enabled),
        cost_edge_min_ratio=float(strategy.cost_edge_min_ratio),
        market_regime_enabled=bool(strategy.market_regime_enabled),
        buy_fraction=float(strategy.buy_fraction),
        max_order_krw=float(strategy.max_order_krw),
        candidate_regime_policy=strategy.candidate_regime_policy,
        require_candidate_regime_policy=True,
    )
    execution_snapshot = ExecutionConstraintSnapshot(
        fee_rate_for_decision=fee_rate_for_decision,
        fee_authority_degraded_blocks_entry=live_armed_entry_fee_authority_blocks(fee_authority),
        fee_authority=fee_authority_context(fee_authority),
        order_rules=order_rules_snapshot,
    )
    exit_policy_config = ExitPolicyConfig(
        rule_names=tuple(strategy.exit_rule_names),
        max_holding_sec=float(strategy.exit_max_holding_min) * 60.0,
        min_take_profit_ratio=float(strategy.exit_min_take_profit_ratio),
        live_fee_rate_estimate=fee_rate_for_decision,
        small_loss_tolerance_ratio=float(strategy.exit_small_loss_tolerance_ratio),
        stop_loss_ratio=float(strategy.exit_stop_loss_ratio),
    )
    final_policy_decision = evaluate_sma_final_decision(
        market=market_snapshot,
        position=position_snapshot,
        config=policy_config,
        execution_context=execution_snapshot,
        exit_policy_config=exit_policy_config,
    )
    policy_decision = final_policy_decision
    entry_decision = policy_decision.entry_decision
    base_signal = policy_decision.raw_signal
    base_reason = policy_decision.raw_reason
    entry_signal = policy_decision.entry_signal
    entry_reason = policy_decision.entry_reason
    gap_ratio = entry_decision.gap_ratio
    volatility_ratio = entry_decision.volatility_ratio
    overextended_ratio = entry_decision.overextended_ratio
    edge_filter_details = entry_decision.edge_filter_details
    edge_filter_triggered = entry_decision.edge_filter_triggered
    blocked_filters = list(policy_decision.blocked_filters)
    market_regime_triggered = entry_decision.market_regime_triggered
    candidate_regime_triggered = entry_decision.candidate_regime_triggered
    candidate_regime_decision = entry_decision.candidate_regime_decision
    market_regime = entry_decision.market_regime
    vol_window = max(1, int(strategy.volatility_window))
    overext_lookback = max(1, int(strategy.overextended_lookback))
    raw_filter_would_block = bool(entry_decision.raw_filter_would_block)
    entry_blocked_by_filter = bool(entry_decision.entry_blocked)
    should_filter_entry = base_signal == "BUY"

    base_context = {
        "ts": ts_list[-1],
        "last_close": float(closes[-1]),
        "strategy": strategy.name,
        "pair": strategy.pair,
        "interval": strategy.interval,
        "approved_profile_hash": (
            strategy.candidate_regime_policy.get("strategy_profile_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_path": settings.APPROVED_STRATEGY_PROFILE_PATH or None,
        "approved_profile_mode": (
            strategy.candidate_regime_policy.get("approved_profile_mode")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_verification_ok": (
            strategy.candidate_regime_policy.get("approved_profile_verification_ok")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_block_reason": (
            strategy.candidate_regime_policy.get("approved_profile_block_reason")
            or strategy.candidate_regime_policy.get("_policy_load_error")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_loaded": (
            strategy.candidate_regime_policy.get("approved_profile_loaded")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_schema_hash_valid": (
            strategy.candidate_regime_policy.get("approved_profile_schema_hash_valid")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_source_verified": (
            strategy.candidate_regime_policy.get("approved_profile_source_verified")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_evidence_verified": (
            strategy.candidate_regime_policy.get("approved_profile_evidence_verified")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_runtime_verified": (
            strategy.candidate_regime_policy.get("approved_profile_runtime_verified")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "approved_profile_contract_scope": (
            strategy.candidate_regime_policy.get("approved_profile_contract_scope")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "legacy_candidate_profile_path_used": (
            strategy.candidate_regime_policy.get("legacy_candidate_profile_path_used")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "legacy_profile_contract_scope": (
            strategy.candidate_regime_policy.get("legacy_profile_contract_scope")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "source_promotion_artifact_path": (
            strategy.candidate_regime_policy.get("source_promotion_artifact_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "promotion_content_hash": (
            strategy.candidate_regime_policy.get("source_promotion_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_profile_hash": (
            strategy.candidate_regime_policy.get("candidate_profile_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "manifest_hash": (
            strategy.candidate_regime_policy.get("manifest_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "dataset_content_hash": (
            strategy.candidate_regime_policy.get("dataset_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "lineage_hash": (
            strategy.candidate_regime_policy.get("lineage_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "legacy_compatibility_used": (
            strategy.candidate_regime_policy.get("legacy_compatibility_used")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "paper_validation_evidence_path": (
            strategy.candidate_regime_policy.get("paper_validation_evidence_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "paper_validation_evidence_content_hash": (
            strategy.candidate_regime_policy.get("paper_validation_evidence_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "live_readiness_evidence_path": (
            strategy.candidate_regime_policy.get("live_readiness_evidence_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "live_readiness_evidence_content_hash": (
            strategy.candidate_regime_policy.get("live_readiness_evidence_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "decision_equivalence_report_path": (
            strategy.candidate_regime_policy.get("decision_equivalence_report_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "decision_equivalence_content_hash": (
            strategy.candidate_regime_policy.get("decision_equivalence_content_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_applied_in_research": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_applied_in_research")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_required_for_live": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_required_for_live")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_equivalence_required": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_equivalence_required")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_equivalence_evidence_hash": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_equivalence_evidence_hash")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_equivalence_evidence_path": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_equivalence_evidence_path")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_equivalence_evidence_status": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_equivalence_evidence_status")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_limitation_reasons": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_limitation_reasons")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "candidate_regime_policy_next_action": (
            strategy.candidate_regime_policy.get("candidate_regime_policy_next_action")
            if isinstance(strategy.candidate_regime_policy, dict)
            else None
        ),
        "base_signal": base_signal,
        "base_reason": base_reason,
        "entry_signal": entry_signal,
        "entry_reason": entry_reason,
        "pure_policy_hash": final_policy_decision.policy_hash,
        "pure_policy_trace": final_policy_decision.as_trace(),
        "policy_contract_hash": final_policy_decision.policy_contract_hash,
        "policy_input_hash": final_policy_decision.policy_input_hash,
        "policy_decision_hash": final_policy_decision.policy_decision_hash,
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
        "market_regime": market_regime,
        "current_market_regime_snapshot": market_regime,
        "current_regime": candidate_regime_decision.get("current_regime"),
        "current_regime_classifier_version": candidate_regime_decision.get("current_regime_classifier_version"),
        "candidate_regime_classifier_version": candidate_regime_decision.get("candidate_regime_classifier_version"),
        "candidate_allowed_regimes": list(candidate_regime_decision.get("candidate_allowed_regimes") or ()),
        "candidate_blocked_regimes": list(candidate_regime_decision.get("candidate_blocked_regimes") or ()),
        "regime_decision": candidate_regime_decision.get("regime_decision"),
        "regime_block_reason": candidate_regime_decision.get("regime_block_reason"),
        "regime_policy_source": candidate_regime_decision.get("regime_policy_source"),
        "regime_policy_present": bool(candidate_regime_decision.get("regime_policy_present")),
        "regime_policy_valid": bool(candidate_regime_decision.get("regime_policy_valid")),
        "order_rules": order_rules_snapshot,
        "position_gate": build_position_gate_context(
            position_state.normalized_exposure,
            order_rules=order_rules_snapshot,
        ),
        "position_state": build_position_state_context(position_state),
        "fee_authority": fee_authority_context(fee_authority),
        "filters": {
            "gap": {
                "enabled": entry_decision.gap_filter_enabled,
                "passed": not entry_decision.gap_triggered,
                "threshold": float(strategy.min_gap_ratio),
                "value": gap_ratio,
            },
            "volatility": {
                "enabled": entry_decision.volatility_filter_enabled,
                "passed": not entry_decision.volatility_triggered,
                "window": vol_window,
                "threshold": float(strategy.min_volatility_ratio),
                "value": volatility_ratio,
            },
            "overextended": {
                "enabled": entry_decision.overextended_filter_enabled,
                "passed": not entry_decision.overextended_triggered,
                "lookback": overext_lookback,
                "threshold": float(strategy.overextended_max_return_ratio),
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
                "fee_authority_source": fee_authority.fee_source,
                "fee_authority_degraded": bool(fee_authority.degraded),
            },
        },
        "filter_blocked": bool(should_filter_entry and blocked_filters),
        "raw_filter_would_block": bool(raw_filter_would_block),
        "entry_blocked": bool(entry_blocked_by_filter),
        "entry_filter_blocked": bool(raw_filter_would_block),
        "market_regime_blocked": bool(market_regime_triggered),
        "candidate_regime_blocked": bool(candidate_regime_triggered),
        "decision_type": (
            "BLOCKED_ENTRY"
            if base_signal == "BUY" and (blocked_filters or market_regime_triggered or candidate_regime_triggered)
            else base_signal
        ),
        "blocked_filters": blocked_filters,
        "gap_ratio": gap_ratio,
        "cost_floor_ratio": float(edge_filter_details["cost_floor_ratio"]),
        "position_lot_interpretation_costs": {
            "exit_slippage_bps": float(strategy.slippage_bps),
            "exit_buffer_ratio": float(strategy.entry_edge_buffer_ratio),
        },
        "blocked_by_cost_filter": bool(should_filter_entry and edge_filter_triggered),
        "blocked_by_fee_authority": bool("fee_authority_degraded" in blocked_filters),
        "entry": {
            **build_entry_decision_context(
                pair=strategy.pair,
                base_signal=base_signal,
                base_reason=base_reason,
                entry_signal=entry_signal,
                entry_reason=entry_reason,
                buy_fraction=float(strategy.buy_fraction),
                max_order_krw=float(strategy.max_order_krw),
            ),
            "cost_edge_blocked": bool(should_filter_entry and edge_filter_triggered),
            "blocked_filters": blocked_filters,
            "filter_blocked": bool(should_filter_entry and blocked_filters),
            "raw_filter_would_block": bool(raw_filter_would_block),
            "entry_blocked": bool(entry_blocked_by_filter),
            "raw_filter_blocked": bool(raw_filter_would_block),
        },
    }
    thresholds = {
        "sma_filter_gap_min_ratio": float(strategy.min_gap_ratio),
        "sma_filter_vol_window": int(vol_window),
        "sma_filter_vol_min_range_ratio": float(strategy.min_volatility_ratio),
        "sma_filter_overext_lookback": int(overext_lookback),
        "sma_filter_overext_max_return_ratio": float(strategy.overextended_max_return_ratio),
        "sma_cost_edge_enabled": bool(strategy.cost_edge_enabled),
        "sma_cost_edge_min_ratio": float(strategy.cost_edge_min_ratio),
        "entry_edge_buffer_ratio": float(strategy.entry_edge_buffer_ratio),
        "market_regime_enabled": bool(strategy.market_regime_enabled),
        "candidate_regime_policy_configured": bool(candidate_regime_decision.get("regime_policy_present")),
    }
    replay_fingerprint = build_replay_fingerprint(
        strategy_name=strategy.name,
        pair=strategy.pair,
        interval=strategy.interval,
        candle_ts=int(ts_list[-1]),
        through_ts_ms=None if signal_through_ts_ms is None else int(signal_through_ts_ms),
        short_n=int(strategy.short_n),
        long_n=int(strategy.long_n),
        thresholds=thresholds,
        fee_authority=fee_authority_context(fee_authority),
        slippage_bps=float(strategy.slippage_bps),
        regime_version=str(market_regime.get("version") or ""),
    )
    base_context["replay_fingerprint"] = replay_fingerprint

    return RuntimeSmaDecisionResult(
        decision=final_policy_decision,
        base_context=base_context,
        position=position,
        exposure=exposure,
        position_state=position_state,
        candle_ts=int(ts_list[-1]),
        market_price=float(closes[-1]),
        replay_fingerprint=replay_fingerprint,
    )


def build_sma_with_filter_decision_from_normalized_db(
    conn: sqlite3.Connection,
    strategy: object,
    *,
    through_ts_ms: int | None = None,
) -> StrategyDecision | None:
    """Compatibility serializer for legacy callers expecting StrategyDecision."""
    result = build_sma_with_filter_runtime_decision_from_normalized_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
    )
    return None if result is None else result.legacy_strategy_decision()


def decide_sma_with_filter_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: object,
    *,
    through_ts_ms: int | None = None,
    normalizer: PositionStateNormalizer | None = None,
) -> StrategyDecision | None:
    """Compatibility serializer for legacy callers expecting StrategyDecision."""
    result = decide_sma_with_filter_runtime_snapshot_from_db(
        conn,
        strategy,
        through_ts_ms=through_ts_ms,
        normalizer=normalizer,
    )
    return None if result is None else result.legacy_strategy_decision()


def decide_sma_with_filter_runtime_snapshot_from_db(
    conn: sqlite3.Connection,
    strategy: object,
    *,
    through_ts_ms: int | None = None,
    normalizer: PositionStateNormalizer | None = None,
) -> RuntimeSmaDecisionResult | None:
    """Live/runtime orchestration boundary for typed sma_with_filter decisions."""
    signal_through_ts_ms = _resolve_signal_through_ts_ms(
        interval=strategy.interval,
        through_ts_ms=through_ts_ms,
    )
    if signal_through_ts_ms is None:
        return None
    market_price = _latest_signal_close(
        conn,
        pair=strategy.pair,
        interval=strategy.interval,
        through_ts_ms=signal_through_ts_ms,
    )
    if market_price is not None:
        (normalizer or PositionStateNormalizer()).normalize_and_persist(
            conn,
            pair=strategy.pair,
            market_price=float(market_price),
            slippage_bps=float(strategy.slippage_bps),
            entry_edge_buffer_ratio=float(strategy.entry_edge_buffer_ratio),
        )
    return build_sma_with_filter_runtime_decision_from_normalized_db(
        conn,
        strategy,
        through_ts_ms=signal_through_ts_ms,
    )


def _resolve_signal_through_ts_ms(*, interval: str, through_ts_ms: int | None) -> int | None:
    from .utils_time import parse_interval_sec

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
