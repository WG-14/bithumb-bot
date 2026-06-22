from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.flatten import flatten_btc_position


class _Broker:
    pass


def _readiness(
    disposition: str = "TRACKED_NON_EXECUTABLE",
    *,
    flatten_allowed: bool = False,
    recommended_action: str = "none",
    manual_exchange_action_required: bool = False,
):
    residual_disposition = SimpleNamespace(
        disposition=disposition,
        reason_codes=(
            "sub_min_qty_residual_tracked"
            if disposition == "TRACKED_NON_EXECUTABLE"
            else "residual_state_inconsistent"
            if disposition == "BLOCKING_INCONSISTENT"
            else "qty_below_minimum"
        ),
        flatten_allowed=flatten_allowed,
        operator_action_required=disposition != "TRACKED_NON_EXECUTABLE",
        recommended_action=recommended_action,
        manual_exchange_action_required=manual_exchange_action_required,
        quantity_rule_authority="persisted_exchange_snapshot",
        qty_step_authority_level="persisted_exchange_snapshot",
        quantity_rule_source_mode="persisted_exchange_snapshot",
        quantity_contract_complete=True,
        broker_local_projection_state="converged",
    )
    normalized = SimpleNamespace(
        open_exposure_qty=0.0,
        terminal_state="dust_only",
        sellable_executable_lot_count=0,
    )
    return SimpleNamespace(
        residual_disposition=residual_disposition,
        residual_inventory=SimpleNamespace(residual_qty=0.00009996),
        position_state=SimpleNamespace(normalized_exposure=normalized),
        recovery_required_count=0,
        open_order_count=0,
    )


def test_flatten_reports_tracked_non_executable_residual_without_manual_closeout(tmp_path, monkeypatch):
    db_path = tmp_path / "flatten-residual.sqlite"
    original_db_path = settings.DB_PATH
    original_pair = settings.PAIR
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    ensure_db(str(db_path)).close()
    monkeypatch.setattr("bithumb_bot.flatten.compute_runtime_readiness_snapshot", lambda _conn: _readiness())
    try:
        result = flatten_btc_position(broker=_Broker(), dry_run=True, trigger="operator")
    finally:
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "PAIR", original_pair)

    assert result["status"] == "tracked_non_executable_residual"
    assert result["submit_expected"] is False
    assert result["closeout_allowed"] is False
    assert result["flatten_required"] is False
    assert result["recommended_action"] == "none"
    assert result["manual_exchange_action_required"] is False


def test_flatten_blocks_inconsistent_residual_with_manual_review(tmp_path, monkeypatch):
    db_path = tmp_path / "flatten-inconsistent-residual.sqlite"
    original_db_path = settings.DB_PATH
    original_pair = settings.PAIR
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    ensure_db(str(db_path)).close()
    monkeypatch.setattr(
        "bithumb_bot.flatten.compute_runtime_readiness_snapshot",
        lambda _conn: _readiness("BLOCKING_INCONSISTENT", recommended_action="review_recovery_report"),
    )
    try:
        result = flatten_btc_position(broker=_Broker(), dry_run=True, trigger="operator")
    finally:
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "PAIR", original_pair)

    assert result["status"] == "blocked"
    assert result["submit_expected"] is False
    assert result["recommended_action"] == "review_recovery_report"
    assert result["manual_exchange_action_required"] is False
    assert result["residual_disposition"] == "BLOCKING_INCONSISTENT"


def test_flatten_closeout_candidate_requires_qty_and_notional_minimums(tmp_path, monkeypatch):
    db_path = tmp_path / "flatten-closeout-candidate.sqlite"
    original_db_path = settings.DB_PATH
    original_pair = settings.PAIR
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    ensure_db(str(db_path)).close()
    monkeypatch.setattr(
        "bithumb_bot.flatten.compute_runtime_readiness_snapshot",
        lambda _conn: _readiness(
            "CLOSEOUT_EXECUTABLE",
            flatten_allowed=False,
            recommended_action="refresh_order_rules_or_review_quantity_settings",
        ),
    )
    try:
        result = flatten_btc_position(broker=_Broker(), dry_run=True, trigger="operator")
    finally:
        object.__setattr__(settings, "DB_PATH", original_db_path)
        object.__setattr__(settings, "PAIR", original_pair)

    assert result["status"] == "blocked"
    assert result["submit_expected"] is False
    assert result["recommended_action"] == "refresh_order_rules_or_review_quantity_settings"
    assert result["manual_exchange_action_required"] is False
    assert result["residual_disposition"] == "CLOSEOUT_EXECUTABLE"
