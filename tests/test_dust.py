from __future__ import annotations

import pytest

from bithumb_bot.dust import build_dust_operator_view, classify_dust_residual, dust_qty_gap_tolerance


@pytest.mark.parametrize(
    (
        "broker_qty",
        "local_qty",
        "latest_price",
        "partial_flatten_recent",
        "expected_present",
        "expected_allow_resume",
        "expected_policy_reason",
        "expected_state",
        "expected_broker_local_match",
        "expected_treat_as_flat",
    ),
    [
        (
            0.00009629,
            0.00009629,
            40_000_000.0,
            True,
            True,
            True,
            "dust_residual_allowed_for_resume",
            "effective_flat_dust",
            True,
            True,
        ),
        (
            0.00009629,
            0.00009629,
            40_000_000.0,
            False,
            True,
            True,
            "dust_residual_allowed_for_resume",
            "effective_flat_dust",
            True,
            True,
        ),
        (
            0.0,
            0.00009629,
            40_000_000.0,
            False,
            True,
            False,
            "dust_residual_requires_operator_review",
            "manual_review_required",
            False,
            False,
        ),
        (
            0.00009629,
            0.0,
            40_000_000.0,
            False,
            True,
            False,
            "dust_residual_requires_operator_review",
            "manual_review_required",
            False,
            False,
        ),
        (
            0.00009629,
            0.00009629,
            100_000_000.0,
            False,
            True,
            False,
            "dust_residual_requires_operator_review",
            "manual_review_required",
            True,
            False,
        ),
        (
            0.0,
            0.0,
            40_000_000.0,
            False,
            False,
            False,
            "no_dust_residual",
            "none",
            True,
            True,
        ),
    ],
    ids=[
        "matched_dust_recent_partial_flatten",
        "matched_dust_notional_also_dust",
        "local_only_dust_mismatch",
        "broker_only_dust_mismatch",
        "qty_dust_but_notional_tradeable",
        "fully_flat_no_dust",
    ],
)
def test_dust_classification_and_operator_view_matrix(
    broker_qty: float,
    local_qty: float,
    latest_price: float,
    partial_flatten_recent: bool,
    expected_present: bool,
    expected_allow_resume: bool,
    expected_policy_reason: str,
    expected_state: str,
    expected_broker_local_match: bool,
    expected_treat_as_flat: bool,
) -> None:
    dust = classify_dust_residual(
        broker_qty=broker_qty,
        local_qty=local_qty,
        min_qty=0.0001,
        min_notional_krw=5000.0,
        latest_price=latest_price,
        partial_flatten_recent=partial_flatten_recent,
        partial_flatten_reason="test_case",
        qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
    )
    view = build_dust_operator_view(dust)

    assert dust.present is expected_present
    assert dust.allow_resume is expected_allow_resume
    assert dust.policy_reason == expected_policy_reason
    assert view.state == expected_state
    assert view.broker_local_match is expected_broker_local_match
    assert view.treat_as_flat is expected_treat_as_flat
    assert view.resume_allowed is expected_allow_resume if expected_present else True
    assert view.new_orders_allowed is expected_allow_resume if expected_present else True

    if expected_present:
        assert "policy_reason=" in dust.summary
        assert "allow_resume=" in dust.summary
    else:
        assert dust.effective_flat is True
        assert view.operator_action == "none"
