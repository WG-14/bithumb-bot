from __future__ import annotations

import json

import pytest

from bithumb_bot.broker.live_submission_execution import _merge_h74_submit_identity
from bithumb_bot.broker.live_submit_orchestrator import (
    _build_context,
    _plan_submit_attempt,
    _validate_explicit_submit_plan,
    run_standard_submit_pipeline_with_evidence,
)
from bithumb_bot.db_core import ensure_db
from bithumb_bot.h74_submit_identity import H74SubmitIdentityError
from tests.test_h74_live_submit_ownership import _ownership, _request


class _DispatchForbiddenBroker:
    def place_order(self, **_kwargs):
        raise AssertionError("dispatch must not be reached")


def _decision_observability() -> dict[str, object]:
    ownership = _ownership()
    return {
        "h74_fixed_position_contract_active": True,
        "cycle_id": ownership.cycle_id,
        "h74_cycle_id": ownership.h74_cycle_id,
        "strategy_instance_id": ownership.strategy_instance_id,
        "authority_hash": ownership.authority_hash,
        "h74_execution_path_probe_run_id": ownership.probe_run_id,
        "h74_entry_plan_client_order_id": ownership.entry_plan_id,
        "h74_position_ownership_contract_hash": ownership.contract_hash,
        "h74_position_ownership_contract": ownership.as_dict(),
    }


def _projected_request(conn):
    submit_observability, identity = _merge_h74_submit_identity(
        submit_observability_fields={"h74_fixed_position_contract_active": True},
        decision_observability=_decision_observability(),
    )
    assert identity is not None
    metadata = identity.as_order_metadata()
    base = _request(conn)
    return base.__class__(
        **{
            **base.__dict__,
            "submit_observability_fields": submit_observability,
            "strategy_instance_id": metadata["strategy_instance_id"],
            "cycle_id": metadata["cycle_id"],
            "authority_hash": metadata["authority_hash"],
            "probe_run_id": metadata["probe_run_id"],
            "h74_cycle_id": metadata["h74_cycle_id"],
            "h74_entry_plan_client_order_id": metadata["h74_entry_plan_client_order_id"],
            "h74_position_ownership_contract_hash": metadata["h74_position_ownership_contract_hash"],
            "h74_position_ownership_contract": metadata["h74_position_ownership_contract"],
            "h74_submit_identity": identity,
        }
    )


def test_live_submission_merges_h74_identity_into_submit_observability_fields() -> None:
    submit_observability, identity = _merge_h74_submit_identity(
        submit_observability_fields={"submit_qty_source": "test"},
        decision_observability=_decision_observability(),
    )

    assert identity is not None
    assert submit_observability["cycle_id"] == "cycle-1"
    assert submit_observability["h74_cycle_id"] == "cycle-1"
    assert submit_observability["h74_entry_plan_client_order_id"] == "h74_entry_plan_123"
    assert submit_observability["h74_position_ownership_contract"]["entry_plan_id"] == "h74_entry_plan_123"


def test_live_submission_request_and_observability_use_same_h74_identity(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "identity.sqlite"))
    request = _projected_request(conn)

    assert request.h74_submit_identity is not None
    assert request.cycle_id == request.submit_observability_fields["cycle_id"]
    assert request.h74_position_ownership_contract_hash == request.submit_observability_fields[
        "h74_position_ownership_contract_hash"
    ]


def test_live_submission_rejects_missing_h74_contract_hash_before_dispatch() -> None:
    decision = _decision_observability()
    decision.pop("h74_position_ownership_contract_hash")

    with pytest.raises(H74SubmitIdentityError, match="contract_hash"):
        _merge_h74_submit_identity(
            submit_observability_fields={"h74_fixed_position_contract_active": True},
            decision_observability=decision,
        )


def test_h74_identity_propagates_from_decision_observability_to_request(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "identity.sqlite"))
    request = _projected_request(conn)

    assert _validate_explicit_submit_plan(request=request) is request.submit_plan
    assert request.h74_entry_plan_client_order_id == "h74_entry_plan_123"


def test_h74_identity_propagates_to_planning_submit_evidence(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "identity.sqlite"))
    request = _projected_request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)

    event = conn.execute(
        """
        SELECT submit_evidence FROM order_events
        WHERE client_order_id=? AND submit_phase='planning'
        ORDER BY id DESC LIMIT 1
        """,
        (request.client_order_id,),
    ).fetchone()
    evidence = json.loads(event["submit_evidence"])
    row = conn.execute(
        """
        SELECT cycle_id, h74_entry_plan_client_order_id,
               h74_position_ownership_contract_hash, h74_position_ownership_contract
        FROM orders WHERE client_order_id=?
        """,
        (request.client_order_id,),
    ).fetchone()

    assert evidence["cycle_id"] == "cycle-1"
    assert evidence["h74_cycle_id"] == "cycle-1"
    assert evidence["h74_entry_plan_client_order_id"] == "h74_entry_plan_123"
    assert evidence["h74_position_ownership_contract"]["entry_plan_id"] == "h74_entry_plan_123"
    assert row["cycle_id"] == "cycle-1"
    assert row["h74_entry_plan_client_order_id"] == "h74_entry_plan_123"
    assert row["h74_position_ownership_contract_hash"] == request.h74_position_ownership_contract_hash
    assert json.loads(row["h74_position_ownership_contract"])["entry_plan_id"] == "h74_entry_plan_123"


def test_h74_identity_propagates_to_failed_order_row_before_dispatch(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "identity.sqlite"))
    request = _projected_request(conn)
    bad_plan = request.submit_plan.__class__(**{**request.submit_plan.__dict__, "phase_result": "bad"})
    bad_request = request.__class__(**{**request.__dict__, "submit_plan": bad_plan})

    result = run_standard_submit_pipeline_with_evidence(
        broker=_DispatchForbiddenBroker(),
        request=bad_request,
    )

    row = conn.execute(
        """
        SELECT status, cycle_id, h74_entry_plan_client_order_id,
               h74_position_ownership_contract_hash, h74_position_ownership_contract
        FROM orders WHERE client_order_id=?
        """,
        (request.client_order_id,),
    ).fetchone()
    event = conn.execute(
        """
        SELECT submit_evidence FROM order_events
        WHERE client_order_id=? AND submit_phase='planning'
        ORDER BY id DESC LIMIT 1
        """,
        (request.client_order_id,),
    ).fetchone()
    evidence = json.loads(event["submit_evidence"])

    assert result is None
    assert row["status"] == "FAILED"
    assert row["cycle_id"] == "cycle-1"
    assert row["h74_entry_plan_client_order_id"] == "h74_entry_plan_123"
    assert row["h74_position_ownership_contract_hash"] == request.h74_position_ownership_contract_hash
    assert json.loads(row["h74_position_ownership_contract"])["entry_plan_id"] == "h74_entry_plan_123"
    assert evidence["cycle_id"] == "cycle-1"
    assert evidence["h74_cycle_id"] == "cycle-1"
    assert evidence["h74_position_ownership_contract"]["entry_plan_id"] == "h74_entry_plan_123"
