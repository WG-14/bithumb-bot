from __future__ import annotations

from dataclasses import dataclass

from bithumb_bot.config import settings
from bithumb_bot.decision_equivalence import sha256_prefixed
from bithumb_bot.runtime_decision_contract import RuntimeStrategyPolicyHashes
from bithumb_bot.runtime_strategy_decision import RuntimeStrategyDecisionResult
from bithumb_bot.strategy_policy_contract import PositionSnapshot, StrategyDecisionV2


SAFE_HOLD_STRATEGY_NAME = "safe_hold"
SAFE_HOLD_POLICY_CONTRACT_VERSION = "safe_hold_runtime_policy_v1"


@dataclass(frozen=True)
class SafeHoldRuntimeDecisionResult:
    decision: StrategyDecisionV2
    base_context: dict[str, object]
    candle_ts: int
    market_price: float
    replay_fingerprint: dict[str, object]
    boundary: dict[str, object]
    policy_hashes: RuntimeStrategyPolicyHashes

    def as_legacy_dict(self) -> dict[str, object]:
        payload = dict(self.base_context)
        payload.setdefault("strategy", self.decision.strategy_name)
        payload.setdefault("signal", self.decision.final_signal)
        payload.setdefault("reason", self.decision.final_reason)
        payload.setdefault("final_signal", self.decision.final_signal)
        payload.setdefault("final_reason", self.decision.final_reason)
        payload.setdefault("raw_signal", self.decision.raw_signal)
        payload.setdefault("raw_reason", self.decision.raw_reason)
        payload.setdefault("ts", int(self.candle_ts))
        payload.setdefault("last_close", float(self.market_price))
        payload.update(self.policy_hashes.as_dict())
        payload.setdefault("replay_fingerprint", dict(self.replay_fingerprint))
        payload.setdefault("boundary", dict(self.boundary))
        return payload


def _latest_runtime_candle(conn, *, through_ts_ms: int | None) -> tuple[int, float] | None:
    if through_ts_ms is None:
        row = conn.execute(
            """
            SELECT ts, close
            FROM candles
            WHERE pair=? AND interval=?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (settings.PAIR, settings.INTERVAL),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT ts, close
            FROM candles
            WHERE pair=? AND interval=? AND ts<=?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (settings.PAIR, settings.INTERVAL, int(through_ts_ms)),
        ).fetchone()
    if row is None:
        return None
    candle_ts = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
    close = float(row["close"]) if hasattr(row, "keys") else float(row[1])
    return candle_ts, close


def _safe_hold_decision(*, candle_ts: int, market_price: float) -> SafeHoldRuntimeDecisionResult:
    policy_input = {
        "schema_version": 1,
        "strategy_name": SAFE_HOLD_STRATEGY_NAME,
        "pair": str(settings.PAIR),
        "interval": str(settings.INTERVAL),
        "candle_ts": int(candle_ts),
        "market_price": float(market_price),
        "final_signal": "HOLD",
    }
    policy_contract = {
        "schema_version": 1,
        "contract": SAFE_HOLD_POLICY_CONTRACT_VERSION,
        "can_submit_orders": False,
    }
    policy_decision = {
        "schema_version": 1,
        "final_signal": "HOLD",
        "final_reason": "safe_hold_no_order_policy",
        "execution_intent": None,
    }
    policy_hash = sha256_prefixed(
        {
            "policy_contract": policy_contract,
            "policy_input": policy_input,
            "policy_decision": policy_decision,
        }
    )
    policy_contract_hash = sha256_prefixed(policy_contract)
    policy_input_hash = sha256_prefixed(policy_input)
    policy_decision_hash = sha256_prefixed(policy_decision)
    replay_fingerprint = {
        "schema_version": 1,
        "strategy_name": SAFE_HOLD_STRATEGY_NAME,
        "policy_contract_version": SAFE_HOLD_POLICY_CONTRACT_VERSION,
        "policy_contract_hash": policy_contract_hash,
        "policy_input_hash": policy_input_hash,
        "policy_decision_hash": policy_decision_hash,
        "candle_ts": int(candle_ts),
        "market_price": float(market_price),
    }
    boundary = {
        "schema_version": 1,
        "decision_boundary_phase": "safe_hold_runtime_decision",
        "typed_authority": "StrategyDecisionV2",
        "order_submission_possible": False,
    }
    decision = StrategyDecisionV2(
        strategy_name=SAFE_HOLD_STRATEGY_NAME,
        raw_signal="HOLD",
        raw_reason="safe_hold_no_order_policy",
        entry_signal="HOLD",
        entry_reason="safe_hold_no_order_policy",
        exit_signal="HOLD",
        exit_reason="safe_hold_no_order_policy",
        final_signal="HOLD",
        final_reason="safe_hold_no_order_policy",
        blocked_filters=(),
        entry_blocked=False,
        entry_block_reason=None,
        exit_rule=None,
        exit_evaluations=(),
        protective_exit_overrode_entry=False,
        exit_filter_suppression_prevented=False,
        position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
        execution_intent=None,
        entry_decision=object(),  # type: ignore[arg-type]
        trace={
            "strategy_name": SAFE_HOLD_STRATEGY_NAME,
            "final_signal": "HOLD",
            "final_reason": "safe_hold_no_order_policy",
            "order_submission_possible": False,
        },
        policy_hash=policy_hash,
        policy_contract_hash=policy_contract_hash,
        policy_input_hash=policy_input_hash,
        policy_decision_hash=policy_decision_hash,
    )
    base_context = {
        "market_price": float(market_price),
        "last_close": float(market_price),
        "strategy": SAFE_HOLD_STRATEGY_NAME,
        "signal": "HOLD",
        "reason": "safe_hold_no_order_policy",
        "final_signal": "HOLD",
        "final_reason": "safe_hold_no_order_policy",
        "observability_context_authoritative": 0,
        "non_authoritative_observability_payload": True,
        "boundary": dict(boundary),
        "replay_fingerprint": dict(replay_fingerprint),
    }
    return SafeHoldRuntimeDecisionResult(
        decision=decision,
        base_context=base_context,
        candle_ts=int(candle_ts),
        market_price=float(market_price),
        replay_fingerprint=replay_fingerprint,
        boundary=boundary,
        policy_hashes=RuntimeStrategyPolicyHashes(
            {
                "pure_policy_hash": policy_hash,
                "policy_contract_hash": policy_contract_hash,
                "policy_input_hash": policy_input_hash,
                "policy_decision_hash": policy_decision_hash,
            }
        ),
    )


@dataclass(frozen=True)
class SafeHoldRuntimeDecisionAdapter:
    strategy_name: str = SAFE_HOLD_STRATEGY_NAME

    def decide(
        self,
        conn,
        request,
    ) -> RuntimeStrategyDecisionResult | None:
        candle = _latest_runtime_candle(conn, through_ts_ms=request.through_ts_ms)
        if candle is None:
            return None
        candle_ts, market_price = candle
        return _safe_hold_decision(candle_ts=candle_ts, market_price=market_price)

    def typed_authority_required(self) -> bool:
        return True
