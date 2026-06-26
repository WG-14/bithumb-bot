from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from bithumb_bot.broker.live_submit_orchestrator import (
    LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE,
    StandardSubmitPipelineRequest,
    _build_context,
    _plan_submit_attempt,
    _validate_explicit_submit_plan,
)
from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.db_core import ensure_db
from bithumb_bot.execution_models import OrderIntent, SubmitPlan, SubmitPriceTickPolicy
from bithumb_bot.h74_position_ownership import H74PositionOwnershipContract
from bithumb_bot.h74_submit_identity import H74SubmitIdentity


def _submit_plan() -> SubmitPlan:
    intent = OrderIntent(
        client_order_id="h74-live-buy",
        market="KRW-BTC",
        side="BUY",
        normalized_side="bid",
        qty=0.0008,
        price=None,
        created_ts=1,
    )
    return SubmitPlan(
        intent=intent,
        rules=SimpleNamespace(),
        requested_qty=0.0008,
        exchange_constrained_qty=0.0008,
        lifecycle_executable_qty=0.0008,
        submitted_qty=0.0008,
        rejected_qty_remainder=0.0,
        unused_budget_krw=0.0,
        submit_qty_authority="non_authoritative_preview",
        lifecycle_non_executable_reason=None,
        chance_validation_order_type="price",
        chance_supported_order_types=("price",),
        exchange_submit_field="price",
        exchange_order_type="price",
        exchange_submit_price=None,
        exchange_submit_volume=None,
        exchange_submit_notional_krw=100_000.0,
        submit_contract_context={},
        submit_price_tick_policy=SubmitPriceTickPolicy(False, 0.0, "not_applicable"),
        effective_market_price=100_000_000.0,
        lot_rules=SimpleNamespace(),
        qty_split=SimpleNamespace(),
        internal_lot_qty=0.0001,
        exchange_submit_qty=0.0008,
        plan_id="plan-1",
    )


def _ownership() -> H74PositionOwnershipContract:
    return H74PositionOwnershipContract(
        cycle_id="cycle-1",
        h74_cycle_id="cycle-1",
        strategy_instance_id="h74-source-observation",
        authority_hash="sha256:a",
        probe_run_id="probe-run-1",
        pair="KRW-BTC",
        entry_side="BUY",
        entry_plan_id="h74_entry_plan_123",
        position_mode="fixed_fill_qty_until_exit",
        hold_policy="hold_acquired_fill_qty_until_max_holding_exit",
    )


def _request(conn, *, cycle_id: str | None = "cycle-1") -> StandardSubmitPipelineRequest:
    ownership = _ownership()
    identity = H74SubmitIdentity.from_ownership_contract(ownership)
    submit_observability = {
        "h74_fixed_position_contract_active": True,
        "h74_position_ownership_contract_hash": ownership.contract_hash,
        "cycle_id": ownership.cycle_id,
        "h74_cycle_id": ownership.h74_cycle_id,
        "strategy_instance_id": ownership.strategy_instance_id,
        "authority_hash": ownership.authority_hash,
        "h74_execution_path_probe_run_id": ownership.probe_run_id,
        "position_mode": ownership.position_mode,
        "hold_policy": ownership.hold_policy,
        "entry_plan_id": ownership.entry_plan_id,
        "h74_entry_plan_client_order_id": ownership.entry_plan_id,
        "h74_position_ownership_contract": ownership.as_dict(),
    }
    return StandardSubmitPipelineRequest(
        conn=conn,
        submit_plan=_submit_plan(),
        signal="BUY",
        client_order_id="h74-live-buy",
        submit_attempt_id="attempt-1",
        side="BUY",
        order_qty=0.0008,
        position_qty=0.0008,
        qty=0.0008,
        ts=1,
        intent_key="intent-1",
        market_price=100_000_000.0,
        raw_total_asset_qty=0.0,
        open_exposure_qty=0.0,
        dust_tracking_qty=0.0,
        effective_rules=SimpleNamespace(),
        submit_qty_source="non_authoritative_preview",
        position_state_source="test",
        reference_price=100_000_000.0,
        top_of_book_summary=None,
        strategy_name="daily_participation_sma",
        decision_id=1,
        decision_reason="unit",
        exit_rule_name=None,
        order_type="price",
        contract_profile=LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE,
        payload_hash="sha256:payload",
        internal_lot_size=0.0001,
        effective_min_trade_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5_000.0,
        intended_lot_count=8,
        executable_lot_count=8,
        final_intended_qty=0.0008,
        final_submitted_qty=0.0008,
        decision_reason_code="unit",
        submit_truth_source_fields={},
        submit_observability_fields=submit_observability,
        sell_observability={},
        strategy_instance_id=ownership.strategy_instance_id,
        cycle_id=cycle_id,
        authority_hash=ownership.authority_hash,
        probe_run_id=ownership.probe_run_id,
        h74_cycle_id=cycle_id,
        h74_entry_plan_client_order_id=ownership.entry_plan_id,
        h74_position_ownership_contract_hash=ownership.contract_hash,
        h74_position_ownership_contract=ownership.as_dict(),
        h74_submit_identity=identity if cycle_id == ownership.cycle_id else None,
    )


def test_h74_live_submit_request_rejects_missing_cycle_id_before_dispatch(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn, cycle_id=None)

    with pytest.raises(BrokerRejectError, match="h74_cycle_ownership_required_for_entry"):
        _validate_explicit_submit_plan(request=request)


def test_h74_order_event_submit_evidence_contains_ownership_contract_hash(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)

    order_row = conn.execute(
        "SELECT cycle_id, strategy_instance_id, authority_hash, probe_run_id FROM orders WHERE client_order_id=?",
        ("h74-live-buy",),
    ).fetchone()
    event_row = conn.execute(
        """
        SELECT submit_evidence FROM order_events
        WHERE client_order_id=? AND submit_phase='planning'
        ORDER BY id DESC LIMIT 1
        """,
        ("h74-live-buy",),
    ).fetchone()
    evidence = json.loads(event_row["submit_evidence"])

    assert order_row["cycle_id"] == "cycle-1"
    assert order_row["strategy_instance_id"] == "h74-source-observation"
    assert order_row["authority_hash"] == "sha256:a"
    assert order_row["probe_run_id"] == "probe-run-1"
    assert evidence["cycle_id"] == "cycle-1"
    assert evidence["h74_cycle_id"] == "cycle-1"
    assert evidence["h74_position_ownership_contract_hash"].startswith("sha256:")
    assert evidence["h74_position_ownership_contract"]["entry_plan_id"] == "h74_entry_plan_123"
    assert evidence["h74_entry_plan_client_order_id"] == "h74_entry_plan_123"


def test_h74_order_row_persists_ownership_contract_hash(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)

    order_row = conn.execute(
        """
        SELECT h74_position_ownership_contract_hash
        FROM orders
        WHERE client_order_id=?
        """,
        ("h74-live-buy",),
    ).fetchone()

    assert order_row["h74_position_ownership_contract_hash"] == request.h74_position_ownership_contract_hash


def test_h74_submit_request_carries_full_ownership_contract(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    assert request.h74_position_ownership_contract["entry_plan_id"] == "h74_entry_plan_123"
    assert request.h74_entry_plan_client_order_id == "h74_entry_plan_123"
    assert request.client_order_id != request.h74_entry_plan_client_order_id


def test_h74_order_row_persists_full_ownership_contract(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)

    row = conn.execute(
        """
        SELECT h74_position_ownership_contract
        FROM orders
        WHERE client_order_id=?
        """,
        (request.client_order_id,),
    ).fetchone()
    contract_json = json.loads(row["h74_position_ownership_contract"])
    assert contract_json["entry_plan_id"] == "h74_entry_plan_123"
    assert contract_json["contract_hash"] == request.h74_position_ownership_contract_hash


def test_h74_order_row_persists_h74_entry_plan_client_order_id(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)

    row = conn.execute(
        """
        SELECT h74_entry_plan_client_order_id
        FROM orders
        WHERE client_order_id=?
        """,
        (request.client_order_id,),
    ).fetchone()
    assert row["h74_entry_plan_client_order_id"] == "h74_entry_plan_123"


def test_h74_order_row_contract_hash_matches_contract_json(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)

    row = conn.execute(
        """
        SELECT h74_position_ownership_contract_hash, h74_position_ownership_contract
        FROM orders
        WHERE client_order_id=?
        """,
        (request.client_order_id,),
    ).fetchone()
    contract_json = json.loads(row["h74_position_ownership_contract"])
    assert row["h74_position_ownership_contract_hash"] == contract_json["contract_hash"]


def test_h74_order_event_submit_evidence_contains_full_ownership_contract(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)

    event_row = conn.execute(
        """
        SELECT submit_evidence FROM order_events
        WHERE client_order_id=? AND submit_phase='planning'
        ORDER BY id DESC LIMIT 1
        """,
        (request.client_order_id,),
    ).fetchone()
    evidence = json.loads(event_row["submit_evidence"])
    assert evidence["h74_position_ownership_contract"]["entry_plan_id"] == "h74_entry_plan_123"


def test_h74_order_event_submit_evidence_contains_h74_entry_plan_client_order_id(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)

    event_row = conn.execute(
        """
        SELECT submit_evidence FROM order_events
        WHERE client_order_id=? AND submit_phase='planning'
        ORDER BY id DESC LIMIT 1
        """,
        (request.client_order_id,),
    ).fetchone()
    evidence = json.loads(event_row["submit_evidence"])
    assert evidence["h74_entry_plan_client_order_id"] == "h74_entry_plan_123"


def test_h74_guard_activates_when_request_has_ownership_hash_even_without_active_flag(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)
    observability = dict(request.submit_observability_fields)
    observability.pop("h74_fixed_position_contract_active")
    request = request.__class__(**{**request.__dict__, "submit_observability_fields": observability})

    assert _validate_explicit_submit_plan(request=request) is request.submit_plan


def test_h74_guard_accepts_entry_plan_id_different_from_client_order_id_when_contract_matches(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    assert request.client_order_id != request.h74_entry_plan_client_order_id
    assert _validate_explicit_submit_plan(request=request) is request.submit_plan


def test_h74_guard_rejects_contract_hash_mismatch_before_dispatch(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)
    bad = request.__class__(**{**request.__dict__, "h74_position_ownership_contract_hash": "sha256:mismatch"})

    with pytest.raises(BrokerRejectError, match="h74_cycle_ownership"):
        _validate_explicit_submit_plan(request=bad)


def test_h74_order_event_submit_evidence_contains_cycle_id_and_h74_cycle_id(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)
    event_row = conn.execute(
        """
        SELECT submit_evidence FROM order_events
        WHERE client_order_id=? AND submit_phase='planning'
        ORDER BY id DESC LIMIT 1
        """,
        (request.client_order_id,),
    ).fetchone()
    evidence = json.loads(event_row["submit_evidence"])

    assert evidence["cycle_id"] == "cycle-1"
    assert evidence["h74_cycle_id"] == "cycle-1"


def test_h74_guard_rejects_cycle_alias_mismatch_before_dispatch(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)
    bad = request.__class__(**{**request.__dict__, "h74_cycle_id": "cycle-other"})

    with pytest.raises(BrokerRejectError, match="mismatch:cycle_id"):
        _validate_explicit_submit_plan(request=bad)


def test_h74_guard_diagnostic_identifies_missing_submit_observability_cycle_id(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)
    observability = dict(request.submit_observability_fields)
    observability.pop("cycle_id")
    bad = request.__class__(**{**request.__dict__, "submit_observability_fields": observability})

    with pytest.raises(BrokerRejectError) as excinfo:
        _validate_explicit_submit_plan(request=bad)

    message = str(excinfo.value)
    assert "plan_missing:cycle_id" in message
    assert '"request_value_present":true' in message
    assert '"submit_observability_value_present":false' in message


def test_h74_guard_diagnostic_identifies_plan_payload_and_request_values(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)
    observability = dict(request.submit_observability_fields)
    observability["h74_entry_plan_client_order_id"] = "different-entry"
    bad = request.__class__(**{**request.__dict__, "submit_observability_fields": observability})

    with pytest.raises(BrokerRejectError) as excinfo:
        _validate_explicit_submit_plan(request=bad)

    message = str(excinfo.value)
    assert "mismatch:h74_entry_plan_client_order_id" in message
    assert '"request_value":"h74_entry_plan_123"' in message
    assert '"submit_observability_value":"different-entry"' in message


def test_h74_order_event_order_row_identity_matches_request(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _request(conn)

    context = _build_context(request=request, submit_plan=_validate_explicit_submit_plan(request=request))
    _plan_submit_attempt(context=context)
    row = conn.execute(
        """
        SELECT cycle_id, h74_entry_plan_client_order_id, h74_position_ownership_contract_hash
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

    assert row["cycle_id"] == evidence["cycle_id"] == request.cycle_id
    assert (
        row["h74_entry_plan_client_order_id"]
        == evidence["h74_entry_plan_client_order_id"]
        == request.h74_entry_plan_client_order_id
    )
    assert (
        row["h74_position_ownership_contract_hash"]
        == evidence["h74_position_ownership_contract_hash"]
        == request.h74_position_ownership_contract_hash
    )
