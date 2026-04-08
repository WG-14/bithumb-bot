from __future__ import annotations

import pytest

from bithumb_bot.dust import (
    DUST_TRACKING_LOT_STATE,
    OPEN_EXPOSURE_LOT_STATE,
    build_dust_display_context,
    build_dust_operator_view,
    build_normalized_exposure,
    build_position_state_model,
    classify_dust_residual,
    dust_qty_gap_tolerance,
    is_strictly_below_min_qty,
    lot_state_quantity_contract,
    lot_state_qty_boundary_rule,
    lot_state_sell_submission_allowed,
    lot_state_sell_submit_includes_dust_tracking,
    lot_state_sell_submit_qty_source,
    lot_state_strategy_qty_source,
    should_treat_as_flat_for_entry_gate,
)


pytestmark = pytest.mark.fast_regression


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
            "harmless_dust",
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
            "harmless_dust",
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
            "harmless_dust",
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
            "blocking_dust",
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
            "blocking_dust",
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
            "matched_harmless_dust_operator_review_required",
            "harmless_dust",
            True,
            True,
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
            "no_dust",
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
            "dust_policy_reason": "matched_harmless_dust_operator_review_required",
            "dust_residual_summary": (
                "broker_qty=0.00009193 local_qty=0.00009193 delta=0.00000000 "
                "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
            "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
            "allow_resume=0 effective_flat=1 "
            "policy_reason=matched_harmless_dust_operator_review_required"
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
    assert view.resume_allowed is True
    assert view.new_orders_allowed is True
    assert view.operator_action == "harmless_dust_tracked_resume_allowed"
    assert "tracked only" in view.operator_message
    assert "resume/new orders are allowed" in view.operator_message


def test_matched_dust_operator_message_does_not_imply_mismatch_or_recovery_concern() -> None:
    view = build_dust_operator_view(
        classify_dust_residual(
            broker_qty=0.00009193,
            local_qty=0.00009193,
            min_qty=0.0001,
            min_notional_krw=5000.0,
            latest_price=100_000_000.0,
            partial_flatten_recent=False,
            partial_flatten_reason="not_recent",
            qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
            matched_harmless_resume_allowed=False,
        )
    )

    assert view.state == "harmless_dust"
    assert "matches across broker/local state" in view.operator_message
    assert "below minimum tradable quantity" in view.operator_message
    assert "mismatch" not in view.operator_message.lower()
    assert "recovery concern" not in view.operator_message.lower()


def test_matched_dust_resume_safe_operator_view_marks_residual_as_tracked_only() -> None:
    view = build_dust_operator_view(
        classify_dust_residual(
            broker_qty=0.00009629,
            local_qty=0.00009629,
            min_qty=0.0001,
            min_notional_krw=5000.0,
            latest_price=40_000_000.0,
            partial_flatten_recent=False,
            partial_flatten_reason="not_recent",
            qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
            matched_harmless_resume_allowed=True,
        )
    )

    assert view.resume_allowed is True
    assert view.new_orders_allowed is True
    assert view.treat_as_flat is True
    assert view.operator_action == "harmless_dust_tracked_resume_allowed"
    assert "tracked only" in view.operator_message
    assert "resume/new orders are allowed" in view.operator_message


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
            "classification=harmless_dust harmless_dust=1 broker_local_match=1 "
            "allow_resume=0 effective_flat=1 submit_unknown_count=1 "
            "policy_reason=matched_harmless_dust_operator_review_required",
            100000000.0,
            0.00009193,
            False,
            "harmless_dust",
            True,
            False,
        ),
        (
            "broker_qty=0.00009629 local_qty=0.00009629 delta=0.00000000 "
            "min_qty=0.00010000 min_notional_krw=5000.0 qty_gap_small=1 "
            "classification=harmless_dust harmless_dust=1 broker_local_match=1 allow_resume=1 effective_flat=1 policy_reason=matched_harmless_dust_resume_allowed",
            40000000.0,
            0.00009629,
            True,
            "harmless_dust",
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
                else "matched_harmless_dust_operator_review_required"
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


def test_dust_display_context_exposes_effective_flat_due_to_harmless_dust() -> None:
    context = build_dust_display_context(
        classify_dust_residual(
            broker_qty=0.00009193,
            local_qty=0.00009193,
            min_qty=0.0001,
            min_notional_krw=5000.0,
            latest_price=100_000_000.0,
            partial_flatten_recent=False,
            partial_flatten_reason="not_recent",
            qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
            matched_harmless_resume_allowed=True,
        )
    )

    assert context.classification.classification == "harmless_dust"
    assert context.effective_flat_due_to_harmless_dust is True
    assert context.fields["effective_flat_due_to_harmless_dust"] is True


def test_flat_entry_gate_reuses_effective_flat_truth_for_harmless_dust() -> None:
    context = build_dust_display_context(
        classify_dust_residual(
            broker_qty=0.00009193,
            local_qty=0.00009193,
            min_qty=0.0001,
            min_notional_krw=5000.0,
            latest_price=100_000_000.0,
            partial_flatten_recent=False,
            partial_flatten_reason="not_recent",
            qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
            matched_harmless_resume_allowed=True,
        )
    )

    assert should_treat_as_flat_for_entry_gate(context) is True


def test_flat_entry_gate_keeps_blocking_dust_conservative() -> None:
    context = build_dust_display_context(
        classify_dust_residual(
            broker_qty=0.000099,
            local_qty=0.000010,
            min_qty=0.0001,
            min_notional_krw=5000.0,
            latest_price=40_000_000.0,
            partial_flatten_recent=False,
            partial_flatten_reason="not_recent",
            qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
            matched_harmless_resume_allowed=False,
        )
    )

    assert should_treat_as_flat_for_entry_gate(context) is False
    assert context.fields["dust_effective_flat"] is False


def test_normalized_exposure_reuses_shared_dust_truth_for_harmless_dust() -> None:
    exposure = build_normalized_exposure(
        raw_qty_open=0.00009629,
        dust_context=build_dust_display_context(
            classify_dust_residual(
                broker_qty=0.00009629,
                local_qty=0.00009629,
                min_qty=0.0001,
                min_notional_krw=5000.0,
                latest_price=40_000_000.0,
                partial_flatten_recent=False,
                partial_flatten_reason="not_recent",
                qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
                matched_harmless_resume_allowed=True,
            )
        ),
    )

    assert exposure.dust_classification == "harmless_dust"
    assert exposure.harmless_dust_effective_flat is True
    assert exposure.effective_flat is True
    assert exposure.entry_allowed is True
    assert exposure.normalized_exposure_active is True
    assert exposure.normalized_exposure_qty == pytest.approx(0.00009629)
    assert exposure.as_dict()["normalized_exposure_active"] is True


def test_normalized_exposure_keeps_blocking_dust_active_and_entry_blocked() -> None:
    exposure = build_normalized_exposure(
        raw_qty_open=0.000099,
        dust_context=build_dust_display_context(
            classify_dust_residual(
                broker_qty=0.000099,
                local_qty=0.000010,
                min_qty=0.0001,
                min_notional_krw=5000.0,
                latest_price=40_000_000.0,
                partial_flatten_recent=False,
                partial_flatten_reason="not_recent",
                qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
                matched_harmless_resume_allowed=False,
            )
        ),
    )

    assert exposure.dust_classification == "blocking_dust"
    assert exposure.harmless_dust_effective_flat is False
    assert exposure.effective_flat is False
    assert exposure.entry_allowed is False
    assert exposure.normalized_exposure_active is True
    assert exposure.normalized_exposure_qty == pytest.approx(0.000099)


def test_position_state_model_exposes_separate_raw_normalized_and_operator_layers() -> None:
    dust = classify_dust_residual(
        broker_qty=0.00009629,
        local_qty=0.00009629,
        min_qty=0.0001,
        min_notional_krw=5000.0,
        latest_price=40_000_000.0,
        partial_flatten_recent=False,
        partial_flatten_reason="not_recent",
        qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
        matched_harmless_resume_allowed=True,
    )

    model = build_position_state_model(raw_qty_open=0.00009629, metadata_raw=dust)

    assert model.raw_holdings.classification == "harmless_dust"
    assert model.raw_holdings.broker_qty == pytest.approx(0.00009629)
    assert model.normalized_exposure.dust_classification == "harmless_dust"
    assert model.normalized_exposure.effective_flat is True
    assert model.normalized_exposure.normalized_exposure_active is True
    assert model.operator_diagnostics.state == "harmless_dust"
    assert model.operator_diagnostics.treat_as_flat is True
    assert model.fields["raw_holdings"]["broker_local_match"] is True
    assert model.fields["normalized_exposure"]["normalized_exposure_qty"] == pytest.approx(0.00009629)
    assert model.fields["normalized_exposure"]["sell_submit_qty"] == pytest.approx(0.00009629)
    assert model.fields["normalized_exposure"]["sell_submit_qty_source"] == "position_state.normalized_exposure.open_exposure_qty"
    assert model.fields["operator_diagnostics"]["resume_allowed"] is True


def test_lot_state_quantity_contract_exposes_boundary_and_sell_submission_rules() -> None:
    contract = lot_state_quantity_contract()

    assert contract[OPEN_EXPOSURE_LOT_STATE]["meaning"] == "real strategy-visible position"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["strategy_qty_source"] == "open_exposure_qty"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["sell_submit_qty_source"] == "position_state.normalized_exposure.open_exposure_qty"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["sell_submit_includes_dust_tracking"] is False
    assert contract[DUST_TRACKING_LOT_STATE]["meaning"] == "operator tracking residual"
    assert contract[DUST_TRACKING_LOT_STATE]["strategy_qty_source"] == "dust_tracking_qty"
    assert contract[DUST_TRACKING_LOT_STATE]["sell_submit_qty_source"] == "excluded_from_sell_qty"
    assert contract[DUST_TRACKING_LOT_STATE]["sell_submit_includes_dust_tracking"] is False



def test_is_strictly_below_min_qty_respects_exact_min_boundary() -> None:
    assert is_strictly_below_min_qty(qty_open=0.00009999, min_qty=0.0001) is True
    assert is_strictly_below_min_qty(qty_open=0.0001, min_qty=0.0001) is False
    assert is_strictly_below_min_qty(qty_open=0.00010001, min_qty=0.0001) is False
    assert is_strictly_below_min_qty(qty_open=0.0, min_qty=0.0001) is False


def test_normalized_exposure_sell_submit_qty_ignores_dust_tracking_qty() -> None:
    exposure = build_normalized_exposure(
        raw_qty_open=0.0002,
        dust_context=build_dust_display_context(
            classify_dust_residual(
                broker_qty=0.0002,
                local_qty=0.0002,
                min_qty=0.0001,
                min_notional_krw=5000.0,
                latest_price=40_000_000.0,
                partial_flatten_recent=False,
                partial_flatten_reason="not_recent",
                qty_gap_tolerance=dust_qty_gap_tolerance(min_qty=0.0001, default_abs_tolerance=1e-8),
                matched_harmless_resume_allowed=True,
            )
        ),
        raw_total_asset_qty=0.0003,
        open_exposure_qty=0.0002,
        dust_tracking_qty=0.0001,
    )

    assert exposure.open_exposure_qty == pytest.approx(0.0002)
    assert exposure.dust_tracking_qty == pytest.approx(0.0001)
    assert exposure.sell_submit_qty == pytest.approx(0.0002)
    assert exposure.sell_submit_qty_source == "position_state.normalized_exposure.open_exposure_qty"
    assert exposure.as_dict()["sell_submit_qty"] == pytest.approx(0.0002)
    assert exposure.as_dict()["sell_submit_qty_source"] == "position_state.normalized_exposure.open_exposure_qty"


def test_lot_state_quantity_contract_routes_open_exposure_and_dust_tracking_separately() -> None:
    contract = lot_state_quantity_contract()

    assert contract[OPEN_EXPOSURE_LOT_STATE]["meaning"] == "real strategy-visible position"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["strategy_qty_source"] == "open_exposure_qty"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["sell_submit_qty_source"] == "position_state.normalized_exposure.open_exposure_qty"
    assert contract[OPEN_EXPOSURE_LOT_STATE]["sell_submission_allowed"] is True
    assert contract[OPEN_EXPOSURE_LOT_STATE]["qty_boundary_rule"] == (
        "qty_open >= min_qty remains open_exposure; SELL uses open_exposure_qty only"
    )
    assert contract[OPEN_EXPOSURE_LOT_STATE]["sell_submit_includes_dust_tracking"] is False
    assert contract[DUST_TRACKING_LOT_STATE]["meaning"] == "operator tracking residual"
    assert contract[DUST_TRACKING_LOT_STATE]["strategy_qty_source"] == "dust_tracking_qty"
    assert contract[DUST_TRACKING_LOT_STATE]["sell_submit_qty_source"] == "excluded_from_sell_qty"
    assert contract[DUST_TRACKING_LOT_STATE]["sell_submission_allowed"] is False
    assert contract[DUST_TRACKING_LOT_STATE]["qty_boundary_rule"] == (
        "qty_open < min_qty is tracked here; SELL submission excludes dust_tracking by default"
    )
    assert contract[DUST_TRACKING_LOT_STATE]["sell_submit_includes_dust_tracking"] is False


def test_lot_state_quantity_helpers_make_the_route_contract_explicit() -> None:
    assert lot_state_strategy_qty_source(OPEN_EXPOSURE_LOT_STATE) == "open_exposure_qty"
    assert lot_state_sell_submit_qty_source(OPEN_EXPOSURE_LOT_STATE) == (
        "position_state.normalized_exposure.open_exposure_qty"
    )
    assert lot_state_sell_submission_allowed(OPEN_EXPOSURE_LOT_STATE) is True
    assert lot_state_sell_submit_includes_dust_tracking(OPEN_EXPOSURE_LOT_STATE) is False
    assert lot_state_strategy_qty_source(DUST_TRACKING_LOT_STATE) == "dust_tracking_qty"
    assert lot_state_sell_submit_qty_source(DUST_TRACKING_LOT_STATE) == "excluded_from_sell_qty"
    assert lot_state_sell_submission_allowed(DUST_TRACKING_LOT_STATE) is False
    assert lot_state_sell_submit_includes_dust_tracking(DUST_TRACKING_LOT_STATE) is False
    assert lot_state_qty_boundary_rule(OPEN_EXPOSURE_LOT_STATE) == (
        "qty_open >= min_qty remains open_exposure; SELL uses open_exposure_qty only"
    )
    assert lot_state_qty_boundary_rule(DUST_TRACKING_LOT_STATE) == (
        "qty_open < min_qty is tracked here; SELL submission excludes dust_tracking by default"
    )
