from __future__ import annotations

import pytest

from tests.test_h74_execution_path_probe_submit_authority import _settings, _variant_authority, _write_authority

from bithumb_bot.h74_authority_alignment import validate_h74_authority_env_alignment
from bithumb_bot.h74_observation import H74ObservationAuthorityError


def test_h74_authority_contract_missing_strategy_instance_id_blocks_live_probe(tmp_path) -> None:
    authority = _variant_authority()
    authority.pop("strategy_instance_id", None)
    bound = dict(authority["hash_bound_parameters"])
    bound.pop("strategy_instance_id", None)
    authority["hash_bound_parameters"] = bound
    cfg = _settings(_write_authority(tmp_path, authority))

    with pytest.raises(H74ObservationAuthorityError, match="h74_authority_contract_incomplete:strategy_instance_id"):
        validate_h74_authority_env_alignment(authority, settings_obj=cfg)


def test_h74_authority_contract_missing_partial_fill_policy_blocks_live_probe(tmp_path) -> None:
    authority = _variant_authority()
    authority.pop("partial_fill_policy", None)
    bound = dict(authority["hash_bound_parameters"])
    bound.pop("partial_fill_policy", None)
    authority["hash_bound_parameters"] = bound
    cfg = _settings(_write_authority(tmp_path, authority))

    with pytest.raises(H74ObservationAuthorityError, match="h74_authority_contract_incomplete:partial_fill_policy"):
        validate_h74_authority_env_alignment(authority, settings_obj=cfg)


@pytest.mark.parametrize(
    ("top_field", "bound_field", "reason_field"),
    [
        ("position_mode", "position_mode", "position_mode"),
        ("hold_policy", "hold_policy", "hold_policy"),
        ("authority_content_hash", "authority_content_hash", "authority_content_hash"),
    ],
)
def test_h74_authority_contract_missing_required_fixed_position_field_blocks_live_probe(
    tmp_path,
    top_field: str,
    bound_field: str,
    reason_field: str,
) -> None:
    authority = _variant_authority()
    authority.pop(top_field, None)
    bound = dict(authority["hash_bound_parameters"])
    bound.pop(bound_field, None)
    authority["hash_bound_parameters"] = bound
    cfg = _settings(_write_authority(tmp_path, authority))

    with pytest.raises(H74ObservationAuthorityError, match=f"h74_authority_contract_incomplete:{reason_field}"):
        validate_h74_authority_env_alignment(authority, settings_obj=cfg)


def test_h74_authority_contract_max_order_mismatch_blocks_live_probe(tmp_path) -> None:
    authority = _variant_authority()
    cfg = _settings(
        _write_authority(tmp_path, authority),
        MAX_ORDER_KRW=99_000.0,
        DAILY_PARTICIPATION_MAX_ORDER_KRW=99_000.0,
    )

    with pytest.raises(H74ObservationAuthorityError, match="h74_authority_contract_mismatch:max_order_krw"):
        validate_h74_authority_env_alignment(authority, settings_obj=cfg)


def test_h74_authority_contract_contains_required_fixed_position_fields(tmp_path) -> None:
    authority = _variant_authority()
    cfg = _settings(_write_authority(tmp_path, authority))

    result = validate_h74_authority_env_alignment(authority, settings_obj=cfg)

    assert result.ok is True
    for field in ("strategy_instance_id", "position_mode", "hold_policy", "partial_fill_policy", "authority_content_hash"):
        assert authority[field]
    assert authority["hash_bound_parameters"]["DAILY_PARTICIPATION_MAX_ORDER_KRW"] == pytest.approx(100_000.0)
    assert cfg.H74_EXECUTION_PATH_PROBE_RUN_ID == "probe-run-1"
