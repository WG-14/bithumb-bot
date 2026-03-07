from __future__ import annotations

import httpx
import pytest

from bithumb_bot.broker.bithumb import BithumbBroker
from bithumb_bot.broker.base import BrokerRejectError, BrokerTemporaryError
from bithumb_bot.config import settings


class _SequencedClient:
    actions: list[object] = []
    calls = 0

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, endpoint: str, data: dict[str, str], headers: dict[str, str]):
        type(self).calls += 1
        action = type(self).actions.pop(0)
        if isinstance(action, Exception):
            raise action
        return action



def _mk_response(status_code: int, payload: dict) -> httpx.Response:
    req = httpx.Request("POST", "https://api.bithumb.com/private")
    return httpx.Response(status_code, json=payload, request=req)


def _configure_live():
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "k")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "s")


def test_private_timeout_is_temporary_error(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [httpx.ReadTimeout("timeout")]
    _SequencedClient.calls = 0
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerTemporaryError):
        broker._post_private("/info/balance", {"currency": "BTC"}, retry_safe=False)


def test_private_business_reject_is_reject_error(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [_mk_response(200, {"status": "5600", "message": "invalid"})]
    _SequencedClient.calls = 0
    monkeypatch.setattr("httpx.Client", _SequencedClient)

    broker = BithumbBroker()
    with pytest.raises(BrokerRejectError):
        broker._post_private("/info/balance", {"currency": "BTC"}, retry_safe=False)


def test_private_safe_call_retries_temporary_error(monkeypatch):
    _configure_live()
    _SequencedClient.actions = [
        httpx.ConnectError("down"),
        _mk_response(200, {"status": "0000", "data": {}}),
    ]
    _SequencedClient.calls = 0
    sleeps: list[float] = []
    monkeypatch.setattr("httpx.Client", _SequencedClient)
    monkeypatch.setattr("bithumb_bot.broker.bithumb.time.sleep", lambda sec: sleeps.append(sec))

    broker = BithumbBroker()
    data = broker._post_private("/info/balance", {"currency": "BTC"}, retry_safe=True)

    assert data["status"] == "0000"
    assert _SequencedClient.calls == 2
    assert sleeps == [0.2]


def test_balance_parses_available_and_locked(monkeypatch):
    _configure_live()
    broker = BithumbBroker()

    monkeypatch.setattr(
        broker,
        "_post_private",
        lambda endpoint, payload, retry_safe=False: {
            "status": "0000",
            "data": {
                "available_krw": "1000",
                "in_use_krw": "25",
                "available_btc": "0.1",
                "in_use_btc": "0.02",
            },
        },
    )

    bal = broker.get_balance()

    assert bal.cash_available == 1000.0
    assert bal.cash_locked == 25.0
    assert bal.asset_available == 0.1
    assert bal.asset_locked == 0.02
