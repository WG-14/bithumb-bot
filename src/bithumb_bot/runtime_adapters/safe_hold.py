from __future__ import annotations

from dataclasses import dataclass

from bithumb_bot.config import settings
from bithumb_bot.decision_equivalence import sha256_prefixed
from bithumb_bot.runtime_decision_contract import RuntimeStrategyPolicyHashes
from bithumb_bot.runtime_strategy_decision import RuntimeStrategyDecisionResult
from bithumb_bot.strategy_decision_service import StrategyDecisionService, StrategyEvaluationRequest
from bithumb_bot.strategy_policy_contract import (
    ExecutionConstraintSnapshot,
    PositionSnapshot,
    StrategyDecisionV2,
)


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
    return _evaluate_safe_hold_decision(
        candle_ts=candle_ts,
        market_price=market_price,
        request=None,
    )


@dataclass(frozen=True)
class SafeHoldPolicy:
    name: str = SAFE_HOLD_STRATEGY_NAME

    def decide_snapshot(
        self,
        *,
        market: object,
        position: PositionSnapshot,
        config: object,
        execution_context: ExecutionConstraintSnapshot,
        exit_policy_config: object | None = None,
        rule_sources: dict[str, str] | None = None,
    ) -> StrategyDecisionV2:
        del position, config, execution_context, exit_policy_config, rule_sources
        market_payload = dict(market) if isinstance(market, dict) else {}
        candle_ts = int(market_payload.get("candle_ts") or 0)
        market_price = float(market_payload.get("market_price") or 0.0)
        hashes = _safe_hold_policy_hash_material(candle_ts=candle_ts, market_price=market_price)
        return StrategyDecisionV2(
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
            policy_hash=str(hashes["policy_hash"]),
            policy_contract_hash=str(hashes["policy_contract_hash"]),
            policy_input_hash=str(hashes["policy_input_hash"]),
            policy_decision_hash=str(hashes["policy_decision_hash"]),
        )


def _safe_hold_policy_hash_material(*, candle_ts: int, market_price: float) -> dict[str, object]:
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
    return {
        "policy_input": policy_input,
        "policy_contract": policy_contract,
        "policy_decision": policy_decision,
        "policy_hash": policy_hash,
        "policy_contract_hash": policy_contract_hash,
        "policy_input_hash": policy_input_hash,
        "policy_decision_hash": policy_decision_hash,
    }


def _evaluate_safe_hold_decision(
    *,
    candle_ts: int,
    market_price: float,
    request: object | None,
) -> SafeHoldRuntimeDecisionResult:
    hashes = _safe_hold_policy_hash_material(candle_ts=candle_ts, market_price=market_price)
    policy_contract_hash = str(hashes["policy_contract_hash"])
    policy_input_hash = str(hashes["policy_input_hash"])
    policy_decision_hash = str(hashes["policy_decision_hash"])
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
    request_fields = (
        request.observability_fields()
        if request is not None and hasattr(request, "observability_fields")
        else {}
    )
    if isinstance(request_fields, dict):
        replay_fingerprint.update(
            {
                key: value
                for key, value in request_fields.items()
                if key
                in {
                    "runtime_decision_request_hash",
                    "strategy_instance_id",
                    "strategy_parameters_hash",
                    "approved_profile_hash",
                    "runtime_contract_hash",
                    "plugin_contract_hash",
                    "through_ts_ms",
                }
            }
        )
    boundary = {
        "schema_version": 1,
        "decision_boundary_phase": "StrategyDecisionService.evaluate",
        "typed_authority": "StrategyDecisionV2",
        "order_submission_possible": False,
    }
    provenance = {
        **request_fields,
        "decision_boundary": "StrategyDecisionService.evaluate",
        "snapshot_builder": "runtime_adapters.safe_hold",
        "replay_fingerprint": replay_fingerprint,
        "strategy_parameters_hash": request_fields.get("strategy_parameters_hash") or sha256_prefixed({}),
        "approved_profile_hash_unavailable_reason": "safe_hold_approved_profile_not_required",
        "plugin_contract_hash_unavailable_reason": "safe_hold_direct_compatibility_call"
        if not request_fields.get("plugin_contract_hash")
        else "",
        "runtime_contract_hash_unavailable_reason": "safe_hold_direct_compatibility_call"
        if not request_fields.get("runtime_contract_hash")
        else "",
        "runtime_decision_request_hash_unavailable_reason": "safe_hold_direct_compatibility_call"
        if not request_fields.get("runtime_decision_request_hash")
        else "",
        "code_provenance": {
            "policy_module": "bithumb_bot.runtime_adapters.safe_hold",
            "policy_class": "SafeHoldPolicy",
        },
    }
    result = StrategyDecisionService().evaluate(
        StrategyEvaluationRequest(
            strategy_name=SAFE_HOLD_STRATEGY_NAME,
            strategy_instance_id=(
                str(request_fields.get("strategy_instance_id") or SAFE_HOLD_STRATEGY_NAME)
            ),
            mode="runtime_replay",
            strategy_policy=SafeHoldPolicy(),
            market_snapshot={"candle_ts": int(candle_ts), "market_price": float(market_price)},
            position_snapshot=PositionSnapshot(in_position=False, entry_allowed=True, exit_allowed=False),
            strategy_config={},
            execution_constraints=ExecutionConstraintSnapshot(fee_rate_for_decision=0.0),
            exit_policy_config=None,
            rule_sources={},
            approved_profile_hash=(
                str(request_fields.get("approved_profile_hash") or "") or None
            ),
            runtime_contract_hash=(
                str(request_fields.get("runtime_contract_hash") or "") or None
            ),
            plugin_contract_hash=(
                str(request_fields.get("plugin_contract_hash") or "") or None
            ),
            request_hash=(
                str(request_fields.get("runtime_decision_request_hash") or "") or None
            ),
            provenance=provenance,
        )
    )
    result.decision.trace["strategy_evaluation_provenance"] = dict(result.provenance)
    result.decision.trace["replay_fingerprint_hash"] = result.replay_fingerprint_hash
    replay_fingerprint = dict(result.replay_fingerprint)
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
        "strategy_evaluation_provenance": dict(result.provenance),
        **request_fields,
    }
    return SafeHoldRuntimeDecisionResult(
        decision=result.decision,
        base_context=base_context,
        candle_ts=int(candle_ts),
        market_price=float(market_price),
        replay_fingerprint=replay_fingerprint,
        boundary=boundary,
        policy_hashes=RuntimeStrategyPolicyHashes(
            {
                "pure_policy_hash": str(hashes["policy_hash"]),
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
        return _evaluate_safe_hold_decision(
            candle_ts=candle_ts,
            market_price=market_price,
            request=request,
        )

    def typed_authority_required(self) -> bool:
        return True
