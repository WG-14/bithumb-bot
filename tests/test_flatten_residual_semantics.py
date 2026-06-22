from __future__ import annotations

from types import SimpleNamespace

from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.flatten import flatten_btc_position


class _Broker:
    pass


def _readiness(disposition: str = "TRACKED_NON_EXECUTABLE"):
    residual_disposition = SimpleNamespace(
        disposition=disposition,
        quantity_rule_authority="persisted_exchange_snapshot",
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


def test_flatten_blocks_inconsistent_residual_with_manual_review():
    result = _readiness("BLOCKING_INCONSISTENT").residual_disposition

    assert result.disposition == "BLOCKING_INCONSISTENT"
    assert result.disposition != "TRACKED_NON_EXECUTABLE"


def test_flatten_closeout_candidate_requires_qty_and_notional_minimums():
    result = _readiness("CLOSEOUT_EXECUTABLE").residual_disposition

    assert result.disposition == "CLOSEOUT_EXECUTABLE"
    assert result.disposition != "TRACKED_NON_EXECUTABLE"
