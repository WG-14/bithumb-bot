from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from bithumb_bot import runtime_state
from bithumb_bot.broker.base import BrokerOrder
from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.live_submission_execution import (
    reconcile_apply_fills_and_refresh,
    submit_live_order_and_confirm,
)
from bithumb_bot.broker.live_submit_orchestrator import (
    LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE,
    StandardSubmitPipelineRequest,
)
from bithumb_bot.config import runtime_code_provenance, settings
from bithumb_bot.db_core import ensure_db, init_portfolio
from bithumb_bot.execution_models import OrderIntent, SubmitPlan, SubmitPriceTickPolicy
from bithumb_bot.fee_observation import fee_accounting_status
from bithumb_bot.lifecycle import summarize_position_lots


class _ScriptedBithumbBroker(BithumbBroker):
    def __init__(self) -> None:
        super().__init__()
        self.dry_run = False
        self.payloads: dict[str, dict[str, object]] = {}
        self.private_calls: list[tuple[str, dict[str, object]]] = []
        self.submit_calls = 0
        self.cancel_calls = 0
        self.flatten_calls = 0

    def set_order_payload(self, payload: dict[str, object]) -> None:
        copied = deepcopy(payload)
        self.payloads[str(copied["uuid"])] = copied
        self.payloads[str(copied["client_order_id"])] = copied

    def place_order(self, *, client_order_id: str, side: str, qty: float, price=None, submit_plan=None, **_kwargs):
        self.submit_calls += 1
        exchange_order_id = f"ex-{client_order_id}"
        return BrokerOrder(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            side=side,
            status="NEW",
            price=price,
            qty_req=float(qty),
            qty_filled=0.0,
            created_ts=1_800_000_000_000,
            updated_ts=1_800_000_000_000,
            raw={"uuid": exchange_order_id, "client_order_id": client_order_id},
            submit_contract_context=getattr(submit_plan, "submit_contract_context", None),
        )

    def cancel_order(self, **_kwargs):
        self.cancel_calls += 1
        raise AssertionError("settlement fixture must not cancel")

    def _get_private(self, endpoint, params, retry_safe=False):
        self.private_calls.append((str(endpoint), dict(params)))
        if endpoint != "/v1/order":
            raise AssertionError(f"unexpected endpoint {endpoint}")
        key = str(params.get("uuid") or params.get("client_order_id") or "")
        if key not in self.payloads:
            raise AssertionError(f"missing scripted payload for {key}")
        return deepcopy(self.payloads[key])


@pytest.fixture(autouse=True)
def _clear_runtime_code_provenance_cache():
    runtime_code_provenance.cache_clear()
    yield
    runtime_code_provenance.cache_clear()


def _configure_live_fixture(tmp_path, monkeypatch):
    runtime_code_provenance.cache_clear()
    monkeypatch.setenv("BITHUMB_DEPLOY_COMMIT_SHA", "test-live-settlement-clean")
    monkeypatch.setenv("BITHUMB_DEPLOY_DIRTY", "false")
    db_path = tmp_path / "live_equivalent.sqlite"
    object.__setattr__(settings, "DB_PATH", str(db_path))
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "INTERVAL", "1m")
    object.__setattr__(settings, "START_CASH_KRW", 1_000_000.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW", 1.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_RATIO_MIN", 0.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_RATIO_MAX", 0.01)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 100_000.0)
    object.__setattr__(settings, "LIVE_INTERNAL_LOT_SIZE", 0.0001)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 8)
    object.__setattr__(settings, "LIVE_MIN_NOTIONAL_KRW", 5_000.0)
    object.__setattr__(settings, "BITHUMB_API_KEY", "fixture-api-key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "fixture-api-secret-32-bytes-minimum")
    runtime_state.enable_trading()
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="SCRIPTED_LIVE_EQUIVALENT_FIXTURE",
        metadata={
            "broker_qty_known": True,
            "broker_asset_qty": 0.0,
            "broker_asset_available": 0.0,
            "broker_asset_locked": 0.0,
            "balance_observed_ts_ms": 1_800_000_000_000,
        },
        now_epoch_sec=1.0,
    )
    monkeypatch.setattr("bithumb_bot.notifier.notify", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("bithumb_bot.broker.live.notify", lambda *_args, **_kwargs: None)
    conn = ensure_db(str(db_path))
    init_portfolio(conn)
    return conn


def _submit_plan(*, client_order_id: str, side: str, qty: float, price: float) -> SubmitPlan:
    normalized_side = "bid" if side == "BUY" else "ask"
    intent = OrderIntent(
        client_order_id=client_order_id,
        market="KRW-BTC",
        side=side,
        normalized_side=normalized_side,
        qty=float(qty),
        price=None,
        created_ts=1_800_000_000_000,
    )
    rules = SimpleNamespace(min_qty=0.0001, qty_step=0.0001, min_notional_krw=5_000.0)
    return SubmitPlan(
        intent=intent,
        rules=rules,
        requested_qty=float(qty),
        exchange_constrained_qty=float(qty),
        lifecycle_executable_qty=float(qty),
        submitted_qty=float(qty),
        rejected_qty_remainder=0.0,
        unused_budget_krw=0.0,
        submit_qty_authority="scripted_live_equivalent_fixture",
        lifecycle_non_executable_reason=None,
        chance_validation_order_type="market",
        chance_supported_order_types=("market", "price"),
        exchange_submit_field="volume",
        exchange_order_type="market",
        exchange_submit_price=None,
        exchange_submit_volume=float(qty),
        exchange_submit_notional_krw=float(price) * float(qty),
        submit_contract_context={"fixture": "scripted_v1_order"},
        submit_price_tick_policy=SubmitPriceTickPolicy(applies=False, price_unit=1.0, reason="market_order"),
        effective_market_price=float(price),
        lot_rules=rules,
        qty_split={"submitted_qty": float(qty)},
        internal_lot_qty=0.0001,
        exchange_submit_qty=float(qty),
        plan_id=f"{client_order_id}:plan",
    )


def _request(conn, *, client_order_id: str, side: str, qty: float, price: float) -> StandardSubmitPipelineRequest:
    internal_lot_size = float(settings.LIVE_INTERNAL_LOT_SIZE)
    lot_count = int(float(qty) / internal_lot_size) if internal_lot_size > 0 else 0
    sell_observability = {}
    if side == "SELL":
        sell_observability = {
            "sell_open_exposure_qty": 0.0 if lot_count <= 0 else float(qty),
            "sell_dust_tracking_qty": float(qty) if lot_count <= 0 else 0.0,
            "raw_total_asset_qty": float(qty),
            "observed_position_qty": float(qty),
            "clean_account_after_sell": True,
        }
    return StandardSubmitPipelineRequest(
        conn=conn,
        submit_plan=_submit_plan(client_order_id=client_order_id, side=side, qty=qty, price=price),
        signal=side,
        client_order_id=client_order_id,
        submit_attempt_id=f"{client_order_id}:attempt",
        side=side,
        order_qty=float(qty),
        position_qty=float(qty) if side == "SELL" else 0.0,
        qty=float(qty),
        ts=1_800_000_000_000,
        intent_key=f"{client_order_id}:intent",
        market_price=float(price),
        raw_total_asset_qty=float(qty) if side == "SELL" else 0.0,
        open_exposure_qty=float(qty) if side == "SELL" and lot_count > 0 else 0.0,
        dust_tracking_qty=float(qty) if side == "SELL" and lot_count <= 0 else 0.0,
        effective_rules=SimpleNamespace(min_qty=0.0001, qty_step=0.0001, min_notional_krw=5_000.0),
        submit_qty_source="scripted_live_equivalent_fixture",
        position_state_source="scripted_live_equivalent_fixture",
        reference_price=float(price),
        top_of_book_summary={"bid": price - 1.0, "ask": price + 1.0, "spread": 2.0},
        strategy_name="target_delta",
        decision_id=1,
        decision_reason="scripted_live_equivalent_fixture",
        exit_rule_name="target_delta" if side == "SELL" else None,
        order_type="market",
        contract_profile=LIVE_STANDARD_SUBMIT_CONTRACT_PROFILE,
        payload_hash=f"sha256:{client_order_id}",
        internal_lot_size=internal_lot_size,
        effective_min_trade_qty=0.0001,
        qty_step=0.0001,
        min_notional_krw=5_000.0,
        intended_lot_count=lot_count,
        executable_lot_count=lot_count,
        final_intended_qty=float(qty),
        final_submitted_qty=float(qty),
        decision_reason_code="target_delta_rebalance",
        submit_truth_source_fields={"submit_qty_source_truth_source": "scripted_fixture_submit_plan"},
        submit_observability_fields=sell_observability,
        sell_observability=sell_observability,
    )


def _payload(
    *,
    client_order_id: str,
    side: str,
    qty: float,
    price: float,
    paid_fee: float | None,
    trades: list[dict[str, object]],
) -> dict[str, object]:
    exchange_order_id = f"ex-{client_order_id}"
    payload = {
        "uuid": exchange_order_id,
        "client_order_id": client_order_id,
        "market": "KRW-BTC",
        "ord_type": "market",
        "side": "bid" if side == "BUY" else "ask",
        "price": str(price),
        "volume": f"{qty:.8f}",
        "remaining_volume": "0",
        "executed_volume": f"{qty:.8f}",
        "executed_funds": f"{price * qty:.8f}",
        "created_at": "2024-01-01T00:00:00+00:00",
        "updated_at": "2024-01-01T00:00:01+00:00",
        "state": "done",
        "trades": trades,
    }
    if paid_fee is not None:
        payload["paid_fee"] = f"{paid_fee:.8f}"
    return payload


def _trade(*, fill_id: str, qty: float, price: float, fee: float | None = None, seconds: int = 0):
    row = {
        "uuid": fill_id,
        "price": str(price),
        "volume": f"{qty:.8f}",
        "funds": f"{price * qty:.8f}",
        "created_at": f"2024-01-01T00:00:0{seconds}+00:00",
    }
    if fee is not None:
        row["fee"] = f"{fee:.8f}"
    return row


def _submit_and_apply(conn, broker: _ScriptedBithumbBroker, *, client_order_id: str, side: str, qty: float, price: float):
    submission = submit_live_order_and_confirm(
        broker=broker,
        request=_request(conn, client_order_id=client_order_id, side=side, qty=qty, price=price),
        intent_key=f"{client_order_id}:intent",
        strategy_name="target_delta",
        decision_id=1,
        decision_reason="scripted_live_equivalent_fixture",
        exit_rule_name="target_delta" if side == "SELL" else None,
    )
    assert submission is not None
    from bithumb_bot.broker import live as live_module

    trade = reconcile_apply_fills_and_refresh(live_module, broker=broker, submission=submission)
    conn.commit()
    return trade


def _table_count(conn, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def test_recorded_single_fill_delayed_paid_fee_settles_through_broker_parser_without_manual_repair(tmp_path, monkeypatch) -> None:
    conn = _configure_live_fixture(tmp_path, monkeypatch)
    broker = _ScriptedBithumbBroker()
    try:
        pending = _payload(
            client_order_id="recorded-buy",
            side="BUY",
            qty=0.0002,
            price=100_000_000.0,
            paid_fee=None,
            trades=[_trade(fill_id="recorded-buy-fill", qty=0.0002, price=100_000_000.0)],
        )
        complete = _payload(
            client_order_id="recorded-buy",
            side="BUY",
            qty=0.0002,
            price=100_000_000.0,
            paid_fee=10.0,
            trades=[_trade(fill_id="recorded-buy-fill", qty=0.0002, price=100_000_000.0)],
        )
        broker.set_order_payload(pending)
        pending_fills = broker.get_fills(client_order_id="recorded-buy", exchange_order_id="ex-recorded-buy", parse_mode="salvage")
        assert [fee_accounting_status(fee=f.fee, fee_status=f.fee_status, price=f.price, qty=f.qty) for f in pending_fills] == ["fee_pending"]

        broker.set_order_payload(complete)
        trade = _submit_and_apply(conn, broker, client_order_id="recorded-buy", side="BUY", qty=0.0002, price=100_000_000.0)

        assert trade is not None
        assert _table_count(conn, "orders") == 1
        assert _table_count(conn, "fills") == 1
        assert _table_count(conn, "trades") == 1
        assert _table_count(conn, "portfolio") == 1
        assert _table_count(conn, "broker_fill_observations") == 1
        lot = summarize_position_lots(conn, pair=str(settings.PAIR))
        assert lot.open_lot_count == 2
        assert lot.raw_open_exposure_qty == pytest.approx(0.0002)
        observation = conn.execute("SELECT accounting_status, fee_source, fee_provenance FROM broker_fill_observations").fetchone()
        assert observation["accounting_status"] == "accounting_complete"
        assert observation["fee_source"] == "order_level_paid_fee"
        assert "order_level_paid_fee" in observation["fee_provenance"]
        assert _table_count(conn, "fee_pending_accounting_repairs") == 0
        assert _table_count(conn, "position_authority_repairs") == 0
    finally:
        conn.close()


def test_order_level_paid_fee_finalized_observation_policy_is_explicit(tmp_path, monkeypatch) -> None:
    conn = _configure_live_fixture(tmp_path, monkeypatch)
    broker = _ScriptedBithumbBroker()
    try:
        broker.set_order_payload(
            _payload(
                client_order_id="policy-a-buy",
                side="BUY",
                qty=0.0002,
                price=100_000_000.0,
                paid_fee=10.0,
                trades=[_trade(fill_id="policy-a-buy-fill", qty=0.0002, price=100_000_000.0)],
            )
        )
        _submit_and_apply(conn, broker, client_order_id="policy-a-buy", side="BUY", qty=0.0002, price=100_000_000.0)

        observation = conn.execute(
            """
            SELECT accounting_status, source, fee_source, fee_provenance
            FROM broker_fill_observations
            WHERE client_order_id='policy-a-buy'
            """
        ).fetchone()
        assert observation is not None
        assert observation["accounting_status"] == "accounting_complete"
        assert observation["source"] in {"live_application_fee_finalized", "live_application_fee_rate_warning"}
        assert observation["fee_source"] == "order_level_paid_fee"
        assert "order_level_paid_fee" in observation["fee_provenance"]
        assert conn.execute(
            "SELECT fee_accounting_status FROM fills WHERE client_order_id='policy-a-buy'"
        ).fetchone()[0] == "fee_finalized"
    finally:
        conn.close()


def test_order_level_paid_fee_finalized_records_observation_if_policy_records(tmp_path, monkeypatch) -> None:
    test_order_level_paid_fee_finalized_observation_policy_is_explicit(tmp_path, monkeypatch)


def test_recorded_multi_fill_paid_fee_allocates_through_broker_parser_only_when_complete(tmp_path, monkeypatch) -> None:
    conn = _configure_live_fixture(tmp_path, monkeypatch)
    broker = _ScriptedBithumbBroker()
    try:
        incomplete = _payload(
            client_order_id="recorded-multi",
            side="BUY",
            qty=0.0003,
            price=100_000_000.0,
            paid_fee=None,
            trades=[
                _trade(fill_id="recorded-multi-a", qty=0.0001, price=100_000_000.0),
                _trade(fill_id="recorded-multi-b", qty=0.0002, price=100_000_000.0, seconds=1),
            ],
        )
        complete = _payload(
            client_order_id="recorded-multi",
            side="BUY",
            qty=0.0003,
            price=100_000_000.0,
            paid_fee=15.0,
            trades=[
                _trade(fill_id="recorded-multi-a", qty=0.0001, price=100_000_000.0),
                _trade(fill_id="recorded-multi-b", qty=0.0002, price=100_000_000.0, seconds=1),
            ],
        )
        broker.set_order_payload(incomplete)
        incomplete_fills = broker.get_fills(client_order_id="recorded-multi", exchange_order_id="ex-recorded-multi", parse_mode="salvage")
        assert len(incomplete_fills) == 2
        assert all(
            fee_accounting_status(fee=f.fee, fee_status=f.fee_status, price=f.price, qty=f.qty) == "fee_pending"
            for f in incomplete_fills
        )

        broker.set_order_payload(complete)
        complete_fills = broker.get_fills(client_order_id="recorded-multi", exchange_order_id="ex-recorded-multi")
        assert len(complete_fills) == 2
        assert all(f.fee_source == "order_level_paid_fee" for f in complete_fills)
        assert sum(float(f.fee or 0.0) for f in complete_fills) == pytest.approx(15.0)

        _submit_and_apply(conn, broker, client_order_id="recorded-multi", side="BUY", qty=0.0003, price=100_000_000.0)
        assert _table_count(conn, "fills") == 1
        assert _table_count(conn, "trades") == 1
        assert _table_count(conn, "broker_fill_observations") == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM broker_fill_observations WHERE accounting_status='accounting_complete' AND fee_source='order_level_paid_fee'"
        ).fetchone()[0] == 2
    finally:
        conn.close()


def test_recorded_all_dust_terminal_close_projects_flat_through_live_application_path(tmp_path, monkeypatch) -> None:
    conn = _configure_live_fixture(tmp_path, monkeypatch)
    broker = _ScriptedBithumbBroker()
    try:
        price = 100_000_000.0
        dust_qty = 0.00005
        broker.set_order_payload(
            _payload(
                client_order_id="dust-buy",
                side="BUY",
                qty=dust_qty,
                price=price,
                paid_fee=2.5,
                trades=[_trade(fill_id="dust-buy-fill", qty=dust_qty, price=price)],
            )
        )
        _submit_and_apply(conn, broker, client_order_id="dust-buy", side="BUY", qty=dust_qty, price=price)
        buy_lot = summarize_position_lots(conn, pair=str(settings.PAIR))
        assert buy_lot.dust_tracking_qty == pytest.approx(dust_qty)
        assert buy_lot.open_lot_count == 0

        broker.set_order_payload(
            _payload(
                client_order_id="dust-sell",
                side="SELL",
                qty=dust_qty,
                price=price,
                paid_fee=2.5,
                trades=[_trade(fill_id="dust-sell-fill", qty=dust_qty, price=price)],
            )
        )
        _submit_and_apply(conn, broker, client_order_id="dust-sell", side="SELL", qty=dust_qty, price=price)
        sell_lot = summarize_position_lots(conn, pair=str(settings.PAIR))
        assert sell_lot.raw_total_asset_qty == pytest.approx(0.0)
        assert conn.execute("SELECT asset_available + asset_locked FROM portfolio WHERE id=1").fetchone()[0] == pytest.approx(0.0)
    finally:
        conn.close()


def assert_recorded_smoke_five_round_trips_touches_orders_fills_trades_portfolio_lots(tmp_path, monkeypatch) -> None:
    conn = _configure_live_fixture(tmp_path, monkeypatch)
    broker = _ScriptedBithumbBroker()
    try:
        qty = 0.0002
        price = 100_000_000.0
        for index in range(5):
            buy_id = f"roundtrip-buy-{index}"
            sell_id = f"roundtrip-sell-{index}"
            broker.set_order_payload(
                _payload(
                    client_order_id=buy_id,
                    side="BUY",
                    qty=qty,
                    price=price,
                    paid_fee=10.0,
                    trades=[_trade(fill_id=f"{buy_id}-fill", qty=qty, price=price)],
                )
            )
            _submit_and_apply(conn, broker, client_order_id=buy_id, side="BUY", qty=qty, price=price)
            after_buy = summarize_position_lots(conn, pair=str(settings.PAIR))
            assert after_buy.open_lot_count == 2
            assert after_buy.raw_open_exposure_qty == pytest.approx(qty)

            broker.set_order_payload(
                _payload(
                    client_order_id=sell_id,
                    side="SELL",
                    qty=qty,
                    price=price,
                    paid_fee=10.0,
                    trades=[_trade(fill_id=f"{sell_id}-fill", qty=qty, price=price)],
                )
            )
            _submit_and_apply(conn, broker, client_order_id=sell_id, side="SELL", qty=qty, price=price)
            after_sell = summarize_position_lots(conn, pair=str(settings.PAIR))
            assert after_sell.raw_total_asset_qty == pytest.approx(0.0)
            assert after_sell.open_lot_count == 0

        assert _table_count(conn, "orders") == 10
        assert conn.execute("SELECT COUNT(*) FROM orders WHERE side='BUY'").fetchone()[0] == 5
        assert conn.execute("SELECT COUNT(*) FROM orders WHERE side='SELL'").fetchone()[0] == 5
        assert _table_count(conn, "order_events") >= 30
        assert _table_count(conn, "fills") == 10
        assert _table_count(conn, "broker_fill_observations") == 10
        assert conn.execute(
            "SELECT COUNT(*) FROM broker_fill_observations "
            "WHERE accounting_status='accounting_complete' "
            "AND fee_source='order_level_paid_fee'"
        ).fetchone()[0] == 10
        assert _table_count(conn, "trades") == 10
        assert _table_count(conn, "portfolio") == 1
        assert conn.execute("SELECT COUNT(*) FROM open_position_lots WHERE qty_open > 1e-12").fetchone()[0] == 0
        assert _table_count(conn, "trade_lifecycles") == 5
        assert _table_count(conn, "fee_pending_accounting_repairs") == 0
        assert _table_count(conn, "position_authority_repairs") == 0
        assert broker.submit_calls == 10
        assert broker.cancel_calls == 0
        assert len(broker.private_calls) == 20
        assert {
            str(params.get("uuid") or params.get("client_order_id"))
            for _endpoint, params in broker.private_calls
        } == (
            {f"ex-roundtrip-buy-{index}" for index in range(5)}
            | {f"ex-roundtrip-sell-{index}" for index in range(5)}
        )
        assert all(endpoint == "/v1/order" for endpoint, _params in broker.private_calls)
    finally:
        conn.close()


def test_recorded_smoke_five_round_trips_touches_orders_fills_trades_portfolio_lots_with_non_vacuous_lot_assertions(
    tmp_path, monkeypatch
) -> None:
    assert_recorded_smoke_five_round_trips_touches_orders_fills_trades_portfolio_lots(tmp_path, monkeypatch)
