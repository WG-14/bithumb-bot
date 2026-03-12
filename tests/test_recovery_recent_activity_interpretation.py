from __future__ import annotations

from bithumb_bot.broker.base import BrokerFill, BrokerOrder
from bithumb_bot.recovery import _interpret_submit_unknown_recent_activity


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
