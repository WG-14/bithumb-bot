from __future__ import annotations

import pytest

from bithumb_bot.dust import build_dust_operator_view, classify_dust_residual, dust_qty_gap_tolerance


@pytest.mark.parametrize(
    (
        "broker_qty",
        "local_qty",
        "latest_price",
        "partial_flatten_recent",
        "matched_harmless_resume_allowed",
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
            False,
            True,
            False,
            "matched_harmless_dust_operator_review_required",
            "matched_harmless_dust",
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
            True,
            "matched_harmless_dust_resume_allowed",
            "matched_harmless_dust",
            True,
            True,
        ),
        (
            0.00009629,
            0.00009629,
            40_000_000.0,
            False,
            False,
            True,
            False,
            "matched_harmless_dust_operator_review_required",
            "matched_harmless_dust",
            True,
            True,
        ),
        (
            0.00009900,
            0.00001000,
            40_000_000.0,
            False,
            False,
            True,
            False,
            "dangerous_dust_operator_review_required",
            "dangerous_dust",
            False,
            False,
        ),
        (
            0.00009629,
            0.0,
            40_000_000.0,
            False,
            False,
            True,
            False,
            "dangerous_dust_operator_review_required",
            "dangerous_dust",
            False,
            False,
        ),
        (
            0.00009629,
            0.00009629,
            100_000_000.0,
            False,
            False,
            True,
            False,
            "dangerous_dust_operator_review_required",
            "dangerous_dust",
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
            False,
            "no_dust_residual",
            "none",
            True,
            True,
        ),
    ],
    ids=[
        "matched_dust_recent_partial_flatten",
        "matched_dust_resume_allowed",
        "matched_dust_notional_also_dust",
        "dust_on_both_sides_but_gap_too_large",
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
    matched_harmless_resume_allowed: bool,
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
        matched_harmless_resume_allowed=matched_harmless_resume_allowed,
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


def test_dust_operator_view_recovers_detail_from_summary_only_metadata() -> None:
    view = build_dust_operator_view(
        {
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_residual_summary": (
                "broker_qty=0.00009193 local_qty=0.00009193 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
                "allow_resume=0 effective_flat=0 policy_reason=dangerous_dust_operator_review_required"
            ),
            "dust_latest_price": 100000000.0,
        }
    )

    assert view.broker_qty == pytest.approx(0.00009193)
    assert view.local_qty == pytest.approx(0.00009193)
    assert view.delta_qty == pytest.approx(0.0)
    assert view.min_qty == pytest.approx(0.0001)
    assert view.min_notional_krw == pytest.approx(5000.0)
    assert view.broker_local_match is True
    assert view.broker_qty_below_min is True
    assert view.local_qty_below_min is True
    assert view.broker_notional_below_min is False
    assert view.local_notional_below_min is False


@pytest.mark.parametrize(
    (
        "summary",
        "latest_price",
        "expected_qty",
        "expected_resume_allowed",
        "expected_state",
        "expected_treat_as_flat",
        "expected_notional_below_min",
    ),
    [
        (
            "broker_qty=0.00009193 local_qty=0.00009193 delta=0.00000000 "
            "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
            "allow_resume=0 effective_flat=0 policy_reason=dangerous_dust_operator_review_required",
            100000000.0,
            0.00009193,
            False,
            "dangerous_dust",
            False,
            False,
        ),
        (
            "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
            "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
            "classification=matched_harmless_dust matched_harmless=1 broker_local_match=1 allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed",
            40000000.0,
            0.00009629,
            True,
            "matched_harmless_dust",
            True,
            True,
        ),
    ],
    ids=["blocked_matched_dust", "resume_safe_matched_dust"],
)
def test_dust_operator_view_keeps_summary_and_detail_consistent(
    summary: str,
    latest_price: float,
    expected_qty: float,
    expected_resume_allowed: bool,
    expected_state: str,
    expected_treat_as_flat: bool,
    expected_notional_below_min: bool,
) -> None:
    view = build_dust_operator_view(
        {
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 1 if expected_resume_allowed else 0,
            "dust_policy_reason": (
                "matched_harmless_dust_resume_allowed"
                if expected_resume_allowed
                else "dangerous_dust_operator_review_required"
            ),
            "dust_residual_summary": summary,
            "dust_latest_price": latest_price,
        }
    )

    assert f"broker_qty={expected_qty:.8f}" in summary
    assert f"local_qty={expected_qty:.8f}" in summary
    assert view.broker_qty == pytest.approx(expected_qty)
    assert view.local_qty == pytest.approx(expected_qty)
    assert view.delta_qty == pytest.approx(0.0)
    assert view.broker_local_match is True
    assert view.resume_allowed is expected_resume_allowed
    assert view.new_orders_allowed is expected_resume_allowed
    assert view.state == expected_state
    assert view.treat_as_flat is expected_treat_as_flat
    assert view.broker_qty_below_min is True
    assert view.local_qty_below_min is True
    assert view.broker_notional_below_min is expected_notional_below_min
    assert view.local_notional_below_min is expected_notional_below_min
    assert f"broker_qty={expected_qty:.8f}" in view.compact_summary
    assert f"local_qty={expected_qty:.8f}" in view.compact_summary
    assert (
        f"resume_allowed={1 if expected_resume_allowed else 0}" in view.compact_summary
    )
    assert "broker_local_match=1" in view.compact_summary
