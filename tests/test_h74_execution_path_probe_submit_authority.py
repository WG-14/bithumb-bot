from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from bithumb_bot.execution_service import (
    _h74_execution_path_probe_authority_allows_submit,
    build_execution_decision_summary,
)
from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.live_submit_orchestrator import _validate_explicit_submit_plan
from bithumb_bot.experiment_execution_contract import POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT
from bithumb_bot.h74_observation import (
    H74_SOURCE_OBSERVATION_PARAMETERS,
    H74_SOURCE_VARIANT_OBSERVATION_AUTHORITY_ARTIFACT_TYPE,
    build_h74_observation_experiment_envelope,
    build_h74_source_observation_authority_payload,
    build_h74_source_variant_observation_authority_payload,
)
from bithumb_bot.h74_pre_submit_evidence import build_h74_pre_submit_evidence_bundle
from bithumb_bot.h74_position_ownership import H74PositionOwnershipError, h74_position_ownership_contract_from_payload
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.run_loop_execution_planner import _h74_entry_cycle_fields
from tests.test_h74_live_submit_ownership import _request as _live_submit_request


pytestmark = pytest.mark.fast_regression


def _envelope() -> dict[str, object]:
    return build_h74_observation_experiment_envelope(
        experiment_run_id="probe-exp",
        runtime_git_commit_sha="commit",
        runtime_git_clean=True,
        env_hash="sha256:" + "1" * 64,
        strategy_revision_id="sha256:" + "2" * 64,
        risk_scope_id="sha256:" + "3" * 64,
        risk_baseline_certificate_hash="sha256:" + "4" * 64,
        starting_broker_position={"qty": 0},
        starting_local_position={"qty": 0},
        db_snapshot_hash="sha256:" + "5" * 64,
        included_history_policy="declared_live_history_scope",
    )


def _source_authority() -> dict[str, object]:
    return build_h74_source_observation_authority_payload(
        source_candidate_artifact_hash="sha256:source",
        backtest_report_hash="sha256:backtest",
        validation_run_hash="sha256:validation",
        code_commit_sha="commit",
        experiment_envelope_payload=_envelope(),
    )


def _variant_authority() -> dict[str, object]:
    payload = build_h74_source_variant_observation_authority_payload(
        base_authority=_source_authority(),
        variant_overrides={
            "DAILY_PARTICIPATION_WINDOW_START_HOUR_KST": 0,
            "DAILY_PARTICIPATION_WINDOW_END_HOUR_KST": 24,
        },
        experiment_envelope_payload=_envelope(),
    )
    bound = dict(payload["hash_bound_parameters"])
    bound["H74_EXECUTION_PATH_PROBE_RUN_ID"] = "probe-run-1"
    payload["hash_bound_parameters"] = bound
    payload["probe_run_id"] = "probe-run-1"
    payload["authority_content_hash"] = sha256_prefixed(
        {key: value for key, value in payload.items() if key != "authority_content_hash"}
    )
    return payload


def _write_authority(tmp_path, payload: dict[str, object]) -> str:
    path = tmp_path / "h74-authority.json"
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return str(path)


def _settings(authority_path: str, evidence_path: str = "", **overrides: object) -> SimpleNamespace:
    values = dict(H74_SOURCE_OBSERVATION_PARAMETERS)
    values.update(
        {
            "MODE": "live",
            "LIVE_DRY_RUN": False,
            "LIVE_REAL_ORDER_ARMED": True,
            "EXECUTION_ENGINE": "target_delta",
            "STRATEGY_NAME": "daily_participation_sma",
            "PAIR": "KRW-BTC",
            "INTERVAL": "1m",
            "MAX_ORDER_KRW": 100_000.0,
            "MAX_DAILY_ORDER_COUNT": 2,
            "H74_SOURCE_OBSERVATION_AUTHORITY_PATH": authority_path,
            "H74_EXECUTION_PATH_PROBE_RUN_ID": "probe-run-1",
            "H74_EXECUTION_PATH_PROBE_PRE_SUBMIT_EVIDENCE_PATH": evidence_path,
            "H74_READINESS_CERTIFICATE_PATH": "",
            "POSITION_MODE": POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
            "TARGET_EXPOSURE_KRW": None,
            "TARGET_HOLD_POLICY": "maintain_previous_target",
            "TARGET_EXECUTION_SHADOW": False,
            "LIVE_ORDER_MAX_QTY_DECIMALS": 8,
            "MIN_NET_EDGE_KRW": 0.0,
            "MIN_MARGIN_AFTER_COST_RATIO": 0.0,
            "PRE_TRADE_ECONOMICS_BLOCKING_ENABLED": False,
        }
    )
    values["DAILY_PARTICIPATION_WINDOW_START_HOUR_KST"] = 0
    values["DAILY_PARTICIPATION_WINDOW_END_HOUR_KST"] = 24
    values.update(overrides)
    return SimpleNamespace(**values)


def _payload(**overrides: object) -> dict[str, object]:
    try:
        contract = h74_position_ownership_contract_from_payload(
            {
                "cycle_id": overrides.get("cycle_id", "h74-cycle-1"),
                "h74_cycle_id": overrides.get("h74_cycle_id", overrides.get("cycle_id", "h74-cycle-1")),
                "strategy_instance_id": overrides.get("strategy_instance_id", "h74-source-observation"),
                "authority_hash": overrides.get("authority_hash", "sha256:authority"),
                "probe_run_id": "probe-run-1",
                "pair": "KRW-BTC",
                "entry_side": "BUY",
                "entry_plan_id": overrides.get("h74_entry_plan_client_order_id", "h74-entry-plan-1"),
                "position_mode": POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
                "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
            }
        )
    except H74PositionOwnershipError:
        contract = None
    payload = {
        "strategy": "daily_participation_sma",
        "h74_fixed_position_contract_active": True,
        "h74_execution_path_probe_run_id": "probe-run-1",
        "cycle_id": "h74-cycle-1",
        "h74_cycle_id": "h74-cycle-1",
        "strategy_instance_id": "h74-source-observation",
        "authority_hash": "sha256:authority",
        "runtime_pair": "KRW-BTC",
        "h74_entry_plan_client_order_id": "h74-entry-plan-1",
        "position_mode": POSITION_MODE_FIXED_FILL_QTY_UNTIL_EXIT,
        "hold_policy": "hold_acquired_fill_qty_until_max_holding_exit",
    }
    if contract is not None:
        payload["h74_position_ownership_contract_hash"] = contract.contract_hash
        payload["h74_position_ownership_contract"] = contract.as_dict()
    payload.update(overrides)
    return payload


def _rehash(payload: dict[str, object]) -> dict[str, object]:
    payload["authority_content_hash"] = sha256_prefixed(
        {key: value for key, value in payload.items() if key != "authority_content_hash"}
    )
    return payload


def _rehash_bundle(payload: dict[str, object]) -> dict[str, object]:
    payload["pre_submit_evidence_hash"] = sha256_prefixed(
        {key: value for key, value in payload.items() if key != "pre_submit_evidence_hash"}
    )
    return payload


def _evidence_bundle(authority: dict[str, object], cfg: SimpleNamespace) -> dict[str, object]:
    return build_h74_pre_submit_evidence_bundle(
        authority_payload=authority,
        settings_obj=cfg,
        env_hash=str(authority["env_hash"]),
        risk_baseline_certificate_hash="sha256:" + "7" * 64,
        db_snapshot_hash="sha256:" + "8" * 64,
        starting_broker_position={"qty": 0},
        starting_local_position={"qty": 0},
        flat_start_proof={"flat": True},
        disk_capacity_path="/tmp",
    )


def _write_bundle(tmp_path, payload: dict[str, object]) -> str:
    path = tmp_path / "h74-pre-submit-evidence.json"
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return str(path)


def _valid_cfg(tmp_path, authority: dict[str, object] | None = None) -> SimpleNamespace:
    authority_payload = authority or _variant_authority()
    authority_path = _write_authority(tmp_path, authority_payload)
    cfg_without_evidence = _settings(authority_path)
    evidence_path = _write_bundle(tmp_path, _evidence_bundle(authority_payload, cfg_without_evidence))
    return _settings(authority_path, evidence_path)


def test_h74_production_missing_certificate_without_probe_authority_fails_closed(tmp_path) -> None:
    cfg = _settings("")

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_no_window_variant_probe_authority_allows_missing_certificate_branch(tmp_path) -> None:
    cfg = _valid_cfg(tmp_path)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is True


def test_h74_no_window_variant_probe_authority_allows_settings_without_daily_max_order_attr(tmp_path) -> None:
    cfg = _valid_cfg(tmp_path)
    delattr(cfg, "DAILY_PARTICIPATION_MAX_ORDER_KRW")

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is True


def test_h74_probe_authority_requires_probe_run_id(tmp_path) -> None:
    authority_path = _write_authority(tmp_path, _variant_authority())
    cfg = _settings(authority_path, H74_EXECUTION_PATH_PROBE_RUN_ID="")

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_authority_alignment_requires_strategy_instance_id_for_fixed_position(tmp_path) -> None:
    authority = _variant_authority()
    authority.pop("strategy_instance_id", None)
    bound = dict(authority["hash_bound_parameters"])
    bound.pop("strategy_instance_id", None)
    authority["hash_bound_parameters"] = bound
    authority_path = _write_authority(tmp_path, authority)
    cfg = _settings(authority_path)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_probe_authority_requires_payload_probe_run_id(tmp_path) -> None:
    cfg = _valid_cfg(tmp_path)

    assert (
        _h74_execution_path_probe_authority_allows_submit(
            _payload(h74_execution_path_probe_run_id=""),
            cfg,
        )
        is False
    )


def test_h74_probe_authority_requires_authority_probe_run_id(tmp_path) -> None:
    authority = _variant_authority()
    authority.pop("probe_run_id", None)
    bound = dict(authority["hash_bound_parameters"])
    bound.pop("H74_EXECUTION_PATH_PROBE_RUN_ID", None)
    bound.pop("probe_run_id", None)
    authority["hash_bound_parameters"] = bound
    authority["authority_content_hash"] = sha256_prefixed(
        {key: value for key, value in authority.items() if key != "authority_content_hash"}
    )
    authority_path = _write_authority(tmp_path, authority)
    cfg = _settings(authority_path)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_probe_authority_rejects_mismatched_payload_probe_run_id(tmp_path) -> None:
    cfg = _valid_cfg(tmp_path)

    assert (
        _h74_execution_path_probe_authority_allows_submit(
            _payload(h74_execution_path_probe_run_id="other"),
            cfg,
        )
        is False
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("artifact_type", "h74_source_live_observation_authority"),
        ("authority_type", "h74_source_live_observation_authority"),
        ("contract_scope", "h74_source_live_observation_only"),
        ("acceptance_track", "production_readiness"),
        ("probe_scope", "full_runtime"),
        ("production_approval", True),
        ("equivalence_to_source_candidate", True),
    ),
)
def test_h74_probe_authority_rejects_wrong_variant_metadata(tmp_path, field, value) -> None:
    authority = _variant_authority()
    authority[field] = value
    authority_path = _write_authority(tmp_path, _rehash(authority))
    cfg = _settings(authority_path)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("DAILY_PARTICIPATION_WINDOW_START_HOUR_KST", 9),
        ("DAILY_PARTICIPATION_WINDOW_END_HOUR_KST", 11),
        ("SMA_SHORT", 11),
        ("SMA_LONG", 87),
        ("STRATEGY_EXIT_MAX_HOLDING_MIN", 75),
        ("DAILY_PARTICIPATION_MAX_ORDER_KRW", 100_001),
    ),
)
def test_h74_probe_authority_rejects_wrong_runtime_alignment(tmp_path, field, value) -> None:
    authority_path = _write_authority(tmp_path, _variant_authority())
    cfg = _settings(authority_path, **{field: value})

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_probe_authority_rejects_max_order_above_100000(tmp_path) -> None:
    authority_path = _write_authority(tmp_path, _variant_authority())
    cfg = _settings(authority_path, MAX_ORDER_KRW=100_001)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_probe_authority_rejects_authority_bound_order_cap_above_100000(tmp_path) -> None:
    authority = _variant_authority()
    bound = dict(authority["hash_bound_parameters"])
    bound["max_entry_notional_krw"] = 100_001
    authority["hash_bound_parameters"] = bound
    authority_path = _write_authority(tmp_path, _rehash(authority))
    cfg = _settings(authority_path)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_probe_authority_rejects_missing_pre_submit_evidence_path(tmp_path) -> None:
    authority_path = _write_authority(tmp_path, _variant_authority())
    cfg = _settings(authority_path)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_probe_authority_rejects_pre_submit_evidence_hash_mismatch(tmp_path) -> None:
    authority = _variant_authority()
    authority_path = _write_authority(tmp_path, authority)
    cfg_without_evidence = _settings(authority_path)
    evidence = _evidence_bundle(authority, cfg_without_evidence)
    evidence["flat_start_proof"] = {"flat": False}
    evidence_path = _write_bundle(tmp_path, evidence)
    cfg = _settings(authority_path, evidence_path)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_probe_authority_rejects_evidence_authority_hash_mismatch(tmp_path) -> None:
    authority = _variant_authority()
    authority_path = _write_authority(tmp_path, authority)
    cfg_without_evidence = _settings(authority_path)
    evidence = _evidence_bundle(authority, cfg_without_evidence)
    evidence["authority_hash"] = "sha256:" + "0" * 64
    evidence_path = _write_bundle(tmp_path, _rehash_bundle(evidence))
    cfg = _settings(authority_path, evidence_path)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


@pytest.mark.parametrize(
    "evidence_update",
    (
        {"flat_start_proof": {"flat": False}},
        {"flat_start_proof": {}},
        {"db_snapshot_hash": "", "db_snapshot_locator": ""},
        {"authority_env_alignment": {"ok": False}},
    ),
)
def test_h74_probe_authority_rejects_invalid_pre_submit_evidence_content(tmp_path, evidence_update) -> None:
    authority = _variant_authority()
    authority_path = _write_authority(tmp_path, authority)
    cfg_without_evidence = _settings(authority_path)
    evidence = _evidence_bundle(authority, cfg_without_evidence)
    evidence.update(evidence_update)
    evidence_path = _write_bundle(tmp_path, _rehash_bundle(evidence))
    cfg = _settings(authority_path, evidence_path)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


@pytest.mark.parametrize(
    ("settings_overrides", "payload_overrides"),
    (
        ({"LIVE_DRY_RUN": True}, {}),
        ({"LIVE_REAL_ORDER_ARMED": False}, {}),
        ({"H74_LIVE_REHEARSAL_NO_SUBMIT_BOUNDARY": True}, {}),
        ({}, {"strategy": "safe_hold"}),
        ({}, {"h74_fixed_position_contract_active": False}),
    ),
)
def test_h74_probe_authority_rejects_runtime_mode_and_contract_mismatches(
    tmp_path,
    settings_overrides,
    payload_overrides,
) -> None:
    cfg = _valid_cfg(tmp_path)
    values = vars(cfg).copy()
    values.update(settings_overrides)
    cfg = SimpleNamespace(**values)

    assert _h74_execution_path_probe_authority_allows_submit(_payload(**payload_overrides), cfg) is False


def test_h74_source_production_authority_remains_distinct_from_no_window_probe(tmp_path) -> None:
    authority = _source_authority()
    assert authority["artifact_type"] != H74_SOURCE_VARIANT_OBSERVATION_AUTHORITY_ARTIFACT_TYPE
    authority_path = _write_authority(tmp_path, authority)
    cfg = _settings(
        authority_path,
        DAILY_PARTICIPATION_WINDOW_START_HOUR_KST=9,
        DAILY_PARTICIPATION_WINDOW_END_HOUR_KST=11,
    )

    assert _h74_execution_path_probe_authority_allows_submit(_payload(), cfg) is False


def test_h74_no_window_probe_full_execution_summary_allows_target_submit_without_certificate(
    tmp_path,
    monkeypatch,
) -> None:
    cfg = _valid_cfg(tmp_path)
    monkeypatch.setattr(
        "bithumb_bot.execution_service.validate_h74_readiness_certificate",
        lambda *_args, **_kwargs: pytest.fail("certificate validator must not run"),
    )
    original_read_text = Path.read_text

    def guarded_read_text(path: Path, *args: object, **kwargs: object) -> str:
        if str(path) == ".":
            pytest.fail("empty certificate path must not be read")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr("bithumb_bot.execution_service.Path.read_text", guarded_read_text)

    summary = build_execution_decision_summary(
        decision_context={
            **_payload(),
            "runtime_pair": "KRW-BTC",
            "market_price": 100_000_000.0,
            "residual_proof_min_qty": 0.00001,
            "residual_proof_min_notional_krw": 5_000.0,
            "min_qty": 0.00001,
            "qty_step": 0.00000001,
            "min_notional_krw": 5_000.0,
            "bid_min_total_krw": 5_000.0,
            "strategy_risk_status": "ALLOW",
            "strategy_risk_reason_code": "none",
        },
        readiness_payload={
            "broker_position_evidence": {
                "broker_qty_known": True,
                "broker_qty": 0.0,
                "balance_source_stale": False,
            },
            "projection_converged": True,
            "projection_convergence": {"converged": True},
            "broker_portfolio_converged": True,
            "open_order_count": 0,
            "unresolved_open_order_count": 0,
            "recovery_required_count": 0,
            "submit_unknown_count": 0,
            "accounting_projection_ok": True,
            "active_fee_accounting_blocker": False,
        },
        raw_signal="BUY",
        final_signal="BUY",
        previous_target_exposure_krw=0.0,
        settings_obj=cfg,
    )

    assert summary.target_submit_plan is not None
    plan = summary.target_submit_plan.as_dict()
    assert plan["submit_expected"] is True
    assert plan["block_reason"] == "none"
    assert plan["final_action"] == "REBALANCE_TO_TARGET"
    assert plan["source"] == "h74_source_observation"
    assert plan["sizing_mode"] == "quote_notional"
    assert plan["exchange_order_type"] == "price"
    assert plan["exchange_submit_field"] == "price"
    assert plan["quote_notional_krw"] == 100_000.0
    assert plan["h74_execution_path_probe_run_id"] == "probe-run-1"
    assert plan["h74_position_ownership_contract_hash"].startswith("sha256:")


@pytest.mark.parametrize(
    ("field", "reason_field"),
    (
        ("strategy_instance_id", "strategy_instance_id"),
        ("authority_hash", "authority_hash"),
    ),
)
def test_h74_fixed_buy_requires_ownership_before_submit(tmp_path, field, reason_field) -> None:
    cfg = _valid_cfg(tmp_path)
    payload = _payload(**{field: "", "h74_cycle_id": "" if field == "cycle_id" else "h74-cycle-1"})

    summary = build_execution_decision_summary(
        decision_context={
            **payload,
            "runtime_pair": "KRW-BTC",
            "market_price": 100_000_000.0,
            "residual_proof_min_qty": 0.00001,
            "residual_proof_min_notional_krw": 5_000.0,
            "min_qty": 0.00001,
            "qty_step": 0.00000001,
            "min_notional_krw": 5_000.0,
            "bid_min_total_krw": 5_000.0,
            "strategy_risk_status": "ALLOW",
            "strategy_risk_reason_code": "none",
        },
        readiness_payload={
            "broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0},
            "projection_converged": True,
            "projection_convergence": {"converged": True},
            "broker_portfolio_converged": True,
            "open_order_count": 0,
            "unresolved_open_order_count": 0,
            "recovery_required_count": 0,
            "submit_unknown_count": 0,
            "accounting_projection_ok": True,
            "active_fee_accounting_blocker": False,
        },
        raw_signal="BUY",
        final_signal="BUY",
        previous_target_exposure_krw=0.0,
        settings_obj=cfg,
    )

    assert summary.target_submit_plan is not None
    plan = summary.target_submit_plan.as_dict()
    assert plan["submit_expected"] is False
    assert plan["final_action"] == "BLOCK_PORTFOLIO_TARGET_AUTHORITY"
    assert plan["block_reason"].startswith("h74_cycle_ownership_required_for_entry")
    assert reason_field in plan["block_reason"]


def test_h74_fixed_buy_pre_dispatch_requires_full_ownership_contract(tmp_path) -> None:
    from bithumb_bot.db_core import ensure_db

    conn = ensure_db(str(tmp_path / "live-submit.sqlite"))
    request = _live_submit_request(conn)
    hash_only_request = request.__class__(
        **{
            **request.__dict__,
            "h74_position_ownership_contract": None,
            "h74_entry_plan_client_order_id": None,
        }
    )

    with pytest.raises(BrokerRejectError, match="h74_cycle_ownership_required_for_entry"):
        _validate_explicit_submit_plan(request=hash_only_request)


def test_h74_fixed_buy_requires_cycle_id_before_submit(tmp_path) -> None:
    cfg = _valid_cfg(tmp_path)
    payload = _payload(cycle_id="", h74_cycle_id="")

    summary = build_execution_decision_summary(
        decision_context={
            **payload,
            "runtime_pair": "KRW-BTC",
            "market_price": 100_000_000.0,
            "residual_proof_min_qty": 0.00001,
            "residual_proof_min_notional_krw": 5_000.0,
            "min_qty": 0.00001,
            "qty_step": 0.00000001,
            "min_notional_krw": 5_000.0,
            "bid_min_total_krw": 5_000.0,
            "strategy_risk_status": "ALLOW",
            "strategy_risk_reason_code": "none",
        },
        readiness_payload={
            "broker_position_evidence": {"broker_qty_known": True, "broker_qty": 0.0},
            "projection_converged": True,
            "projection_convergence": {"converged": True},
            "broker_portfolio_converged": True,
            "open_order_count": 0,
            "unresolved_open_order_count": 0,
            "recovery_required_count": 0,
            "submit_unknown_count": 0,
            "accounting_projection_ok": True,
            "active_fee_accounting_blocker": False,
        },
        raw_signal="BUY",
        final_signal="BUY",
        previous_target_exposure_krw=0.0,
        settings_obj=cfg,
    )

    assert summary.target_submit_plan is not None
    plan = summary.target_submit_plan.as_dict()
    assert plan["submit_expected"] is False
    assert plan["final_action"] == "BLOCK_PORTFOLIO_TARGET_AUTHORITY"
    assert plan["block_reason"].startswith("h74_cycle_ownership_required_for_entry")


def test_h74_entry_cycle_fields_returns_canonical_submit_identity() -> None:
    payload = _h74_entry_cycle_fields(planning_context=_payload(runtime_pair="KRW-BTC"), updated_ts=123)

    assert payload["cycle_id"] == payload["h74_cycle_id"]
    assert payload["h74_entry_plan_client_order_id"] == payload["h74_position_ownership_contract"]["entry_plan_id"]
    assert payload["h74_position_ownership_contract_hash"] == payload["h74_position_ownership_contract"]["contract_hash"]


def test_h74_entry_cycle_fields_includes_cycle_id_and_h74_cycle_id_aliases() -> None:
    payload = _h74_entry_cycle_fields(planning_context=_payload(runtime_pair="KRW-BTC"), updated_ts=124)

    assert payload["cycle_id"]
    assert payload["h74_cycle_id"]


def test_h74_entry_cycle_fields_contract_hash_matches_contract_json() -> None:
    payload = _h74_entry_cycle_fields(planning_context=_payload(runtime_pair="KRW-BTC"), updated_ts=125)

    contract = payload["h74_position_ownership_contract"]
    assert isinstance(contract, dict)
    assert payload["h74_position_ownership_contract_hash"] == contract["contract_hash"]
