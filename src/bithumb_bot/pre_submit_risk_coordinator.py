from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Mapping

from .db_core import update_execution_plan_final_submit_payload
from .execution_plan_batch import build_pre_submit_risk_finalization_artifact
from .risk_contract import SubmitPlan
from .runtime_risk_engine import RuntimeRiskEngineAdapter, resolve_effective_pre_submit_risk_policy
from .submit_authority_policy import operational_pre_submit_risk_approval_error


@dataclass(frozen=True)
class PreSubmitRiskResult:
    payload: dict[str, object]
    allowed: bool
    persistence_status: str
    reason: str


class PreSubmitRiskCoordinator:
    """Execution-phase pre-submit proof generation.

    DB and broker access belongs here, never in submit-plan serialization.
    The caller owns the SQLite connection and transaction boundary.
    """

    def evaluate_and_persist(
        self,
        conn: sqlite3.Connection,
        *,
        payload: Mapping[str, object],
        broker: object | None,
        ts_ms: int,
        market_price: float,
        field_name: str,
    ) -> PreSubmitRiskResult:
        from .execution_service import (
            execution_submit_plan_payload_hash,
            _pre_submit_risk_required_for_live_real,
        )

        final_payload = dict(payload)
        if not _pre_submit_risk_required_for_live_real(final_payload):
            return PreSubmitRiskResult(
                payload=final_payload,
                allowed=True,
                persistence_status="not_required",
                reason="pre_submit_risk_not_required",
            )

        expected_hash = str(final_payload.get("submit_plan_hash") or "").strip()
        if not expected_hash:
            expected_hash = execution_submit_plan_payload_hash(final_payload)
            final_payload["submit_plan_hash"] = expected_hash

        existing_approval_error = operational_pre_submit_risk_approval_error(
            final_payload,
            expected_submit_plan_hash=expected_hash,
        )
        if existing_approval_error is None:
            return PreSubmitRiskResult(
                payload=final_payload,
                allowed=True,
                persistence_status="already_approved",
                reason="pre_submit_risk_already_approved",
            )

        side = str(final_payload.get("side") or "").strip().upper() or "UNKNOWN"
        effective_policy = resolve_effective_pre_submit_risk_policy(final_payload)
        decision = RuntimeRiskEngineAdapter(conn, policy=effective_policy.policy).evaluate_pre_submit(  # broker=live_execution_service
            plan=SubmitPlan(
                side=side,
                qty=float(final_payload.get("qty") or 0.0),
                notional_krw=(
                    None
                    if final_payload.get("notional_krw") is None
                    else float(final_payload.get("notional_krw") or 0.0)
                ),
                source=str(final_payload.get("source") or "execution_submit_plan"),
                evidence={
                    "execution_submit_plan_hash": expected_hash,
                    "execution_submit_plan_source": str(final_payload.get("source") or ""),
                    "execution_submit_plan_authority": str(final_payload.get("authority") or ""),
                    "plan_kind": field_name,
                    "submit_plan_qty_source": "submit_plan.qty",
                    "current_asset_qty_source": "broker_current_position",
                    **effective_policy.evidence_fields(),
                },
            ),
            ts_ms=int(ts_ms),
            now_ms=int(ts_ms),
            cash=0.0,
            submit_qty=float(final_payload.get("qty") or 0.0),
            current_asset_qty=None,
            price=float(market_price),
            broker=broker,
            evaluation_origin="live_real_submit_authority_pre_submit",
        )

        proof_fields = {
            **effective_policy.evidence_fields(),
            "pre_submit_risk_decision": decision.as_dict(),
            "pre_submit_risk_status": decision.status,
            "pre_submit_risk_decision_hash": decision.risk_decision_hash,
            "pre_submit_risk_policy_hash": decision.risk_policy_hash,
            "effective_pre_submit_risk_policy_hash": decision.risk_policy_hash,
            "pre_submit_risk_input_hash": decision.risk_input_hash,
            "pre_submit_risk_evidence_hash": decision.risk_evidence_hash,
            "pre_submit_risk_plan_hash": expected_hash,
            "pre_submit_risk_reason_code": decision.reason_code,
            "pre_submit_risk_state_source": decision.state_source,
            "pre_submit_risk_state_source_detail": "runtime_db_broker",
            "pre_submit_risk_evidence": dict(decision.evidence),
        }
        final_payload.update(proof_fields)
        approval_error = operational_pre_submit_risk_approval_error(
            proof_fields,
            expected_submit_plan_hash=expected_hash,
        )
        allowed = approval_error is None
        persistence_status = (
            "final_broker_bound_payload" if allowed else "post_proof_submit_skipped"
        )
        if not allowed:
            final_payload.update(
                {
                    "final_submit_payload_persistence_status": persistence_status,
                    "final_submit_payload_skip_reason": str(approval_error),
                }
            )
        final_payload["content_hash"] = execution_submit_plan_payload_hash(final_payload)
        finalization = build_pre_submit_risk_finalization_artifact(final_payload)
        final_payload["pre_submit_risk_finalization_artifact"] = finalization
        final_payload["pre_submit_risk_finalization_hash"] = finalization[
            "pre_submit_risk_finalization_hash"
        ]
        persist_result = update_execution_plan_final_submit_payload(
            conn,
            final_submit_payload=final_payload,
            persistence_status=persistence_status,
        )
        if not bool(persist_result.get("updated")):
            raise RuntimeError(
                str(persist_result.get("reason") or "execution_plan_final_submit_payload_not_bound")
            )
        return PreSubmitRiskResult(
            payload=final_payload,
            allowed=allowed,
            persistence_status=persistence_status,
            reason="allowed" if allowed else str(approval_error),
        )
