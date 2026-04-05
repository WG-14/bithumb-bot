from __future__ import annotations

from bithumb_bot.oms import MAX_CLIENT_ORDER_ID_LENGTH, build_client_order_id


def test_build_client_order_id_live_format_and_max_length() -> None:
    client_order_id = build_client_order_id(
        mode="live",
        side="BUY",
        intent_ts=1775367720000,
        submit_attempt_id="attempt_f70fd9a0eca948de",
    )

    assert client_order_id.startswith("live_1775367720000_buy_")
    assert len(client_order_id) <= MAX_CLIENT_ORDER_ID_LENGTH


def test_build_client_order_id_same_intent_ts_is_unique_per_attempt() -> None:
    first = build_client_order_id(
        mode="live",
        side="BUY",
        intent_ts=1775367720000,
        submit_attempt_id="attempt_11111111",
    )
    second = build_client_order_id(
        mode="live",
        side="BUY",
        intent_ts=1775367720000,
        submit_attempt_id="attempt_22222222",
    )

    assert first != second
    assert len(first) <= MAX_CLIENT_ORDER_ID_LENGTH
    assert len(second) <= MAX_CLIENT_ORDER_ID_LENGTH


def test_build_client_order_id_supports_paper_and_dryrun_modes() -> None:
    paper_id = build_client_order_id(
        mode="paper",
        side="SELL",
        intent_ts=1775367720000,
        nonce="paper_nonce_abcdef12",
    )
    dryrun_id = build_client_order_id(
        mode="dryrun",
        side="BUY",
        intent_ts=1775367720000,
        nonce="dry_nonce_abcdef12",
    )

    assert paper_id.startswith("paper_1775367720000_sell_")
    assert dryrun_id.startswith("dryru_1775367720000_buy_")
    assert len(paper_id) <= MAX_CLIENT_ORDER_ID_LENGTH
    assert len(dryrun_id) <= MAX_CLIENT_ORDER_ID_LENGTH
