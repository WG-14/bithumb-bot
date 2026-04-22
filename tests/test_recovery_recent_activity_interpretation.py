from __future__ import annotations

from bithumb_bot.broker.base import BrokerFill, BrokerOrder
from bithumb_bot.execution import record_order_if_missing
from bithumb_bot.db_core import ensure_db
from bithumb_bot.recovery import _apply_recent_fills, _interpret_submit_unknown_recent_activity


def _local_submit_unknown_row(*, qty_req: float = 1.0) -> dict[str, str | float]:
    return {
        "client_order_id": "submit_timeout_restart",
        "exchange_order_id": "",
        "side": "BUY",
        "qty_req": qty_req,
    }


def _timeout_submit_context(*, qty: float = 1.0) -> dict[str, str | float | bool]:
    return {
        "submit_attempt_id": "attempt_timeout_meta",
        "timeout_submit_unknown": True,
        "preflight_side": "BUY",
        "preflight_qty": qty,
    }


def test_interpret_submit_unknown_recent_activity_single_match_success() -> None:
    result = _interpret_submit_unknown_recent_activity(
        local_row=_local_submit_unknown_row(),
        submit_attempt_context=_timeout_submit_context(),
        recent_orders=[
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id="ex-submit-unknown-strong",
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=100,
                updated_ts=110,
            )
        ],
        recent_fills=[],
    )

    assert result.outcome == "success"
    assert result.candidate_count == 1
    assert result.matched_exchange_order_id == "ex-submit-unknown-strong"


def test_interpret_submit_unknown_recent_activity_no_match_insufficient_evidence() -> None:
    result = _interpret_submit_unknown_recent_activity(
        local_row=_local_submit_unknown_row(),
        submit_attempt_context=_timeout_submit_context(),
        recent_orders=[],
        recent_fills=[],
    )

    assert result.outcome == "insufficient_evidence"
    assert result.candidate_count == 0
    assert result.matched_exchange_order_id is None


def test_interpret_submit_unknown_recent_activity_multiple_candidates_ambiguous() -> None:
    result = _interpret_submit_unknown_recent_activity(
        local_row=_local_submit_unknown_row(),
        submit_attempt_context=_timeout_submit_context(),
        recent_orders=[
            BrokerOrder("submit_timeout_restart", "ex-a", "BUY", "CANCELED", 100.0, 1.0, 0.0, 100, 110),
            BrokerOrder("submit_timeout_restart", "ex-b", "BUY", "FAILED", 100.0, 1.0, 0.0, 101, 111),
        ],
        recent_fills=[],
    )

    assert result.outcome == "ambiguous"
    assert result.candidate_count == 2
    assert result.matched_exchange_order_id is None


def test_interpret_submit_unknown_recent_activity_partial_fill_evidence_detected() -> None:
    result = _interpret_submit_unknown_recent_activity(
        local_row=_local_submit_unknown_row(qty_req=1.0),
        submit_attempt_context=_timeout_submit_context(qty=1.0),
        recent_orders=[
            BrokerOrder("submit_timeout_restart", "ex-partial", "BUY", "PARTIAL", 100.0, 1.0, 0.4, 100, 110),
        ],
        recent_fills=[
            BrokerFill(
                client_order_id="submit_timeout_restart",
                fill_id="fill-1",
                fill_ts=111,
                price=100.0,
                qty=0.4,
                fee=0.0,
                exchange_order_id="ex-partial",
            )
        ],
    )

    assert result.outcome == "success"
    assert result.candidate_count == 1
    assert len(result.matched_fills) == 1
    assert result.has_partial_fill_evidence is True


def test_interpret_submit_unknown_recent_activity_accepts_client_id_when_exchange_id_missing() -> None:
    result = _interpret_submit_unknown_recent_activity(
        local_row=_local_submit_unknown_row(),
        submit_attempt_context=_timeout_submit_context(),
        recent_orders=[
            BrokerOrder(
                client_order_id="submit_timeout_restart",
                exchange_order_id=None,
                side="BUY",
                status="CANCELED",
                price=100.0,
                qty_req=1.0,
                qty_filled=0.0,
                created_ts=100,
                updated_ts=110,
            )
        ],
        recent_fills=[],
    )

    assert result.outcome == "success"
    assert result.candidate_count == 1
    assert result.matched_exchange_order_id is None
    assert result.matched_order is not None
    assert result.matched_order.status == "CANCELED"


def test_interpret_submit_unknown_recent_activity_fill_only_evidence_resolves_with_timeout_metadata() -> None:
    result = _interpret_submit_unknown_recent_activity(
        local_row=_local_submit_unknown_row(qty_req=1.0),
        submit_attempt_context=_timeout_submit_context(qty=1.0),
        recent_orders=[],
        recent_fills=[
            BrokerFill(
                client_order_id="submit_timeout_restart",
                fill_id="fill-only",
                fill_ts=111,
                price=100.0,
                qty=0.4,
                fee=0.0,
                exchange_order_id="ex-fill-only",
            )
        ],
    )

    assert result.outcome == "success"
    assert result.candidate_count == 1
    assert result.matched_exchange_order_id == "ex-fill-only"
    assert len(result.matched_fills) == 1
    assert result.has_partial_fill_evidence is True


def test_apply_recent_fills_allows_client_id_linkage_for_non_submit_unknown_when_exchange_missing(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "recent_fill_client_match.sqlite"))
    try:
        record_order_if_missing(
            conn,
            client_order_id="cid_delayed_fill",
            side="BUY",
            qty_req=1.0,
            price=100.0,
            ts_ms=100,
            status="NEW",
        )
        conn.execute(
            "UPDATE orders SET exchange_order_id='ex-known' WHERE client_order_id='cid_delayed_fill'"
        )
        conn.commit()

        applied, conflicts, blocked_invalid_price, fee_pending_updates = _apply_recent_fills(
            conn,
            [
                BrokerFill(
                    client_order_id="cid_delayed_fill",
                    fill_id="fill-delayed",
                    fill_ts=120,
                    price=100.0,
                    qty=1.0,
                    fee=0.0,
                    exchange_order_id=None,
                )
            ],
            trusted_open_exchange_ids=set(),
        )
        row = conn.execute(
            "SELECT status, qty_filled, last_error FROM orders WHERE client_order_id='cid_delayed_fill'"
        ).fetchone()
    finally:
        conn.close()

    assert applied is True
    assert conflicts == []
    assert blocked_invalid_price == 0
    assert fee_pending_updates is None
    assert row is not None
    assert row["status"] == "FILLED"
    assert float(row["qty_filled"]) == 1.0
    assert row["last_error"] is None
