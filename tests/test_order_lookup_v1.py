from __future__ import annotations

import pytest

from bithumb_bot.broker.base import BrokerRejectError
from bithumb_bot.broker.order_lookup_v1 import build_lookup_params


def test_build_lookup_params_uses_uuid_when_only_uuid_provided() -> None:
    assert build_lookup_params(client_order_id=None, exchange_order_id="ex-1") == {"uuid": "ex-1"}


def test_build_lookup_params_uses_client_order_id_when_only_client_order_id_provided() -> None:
    assert build_lookup_params(client_order_id="cid-1", exchange_order_id=None) == {"client_order_id": "cid-1"}


def test_build_lookup_params_prefers_uuid_when_both_identifiers_provided() -> None:
    assert build_lookup_params(client_order_id="cid-1", exchange_order_id="ex-1") == {"uuid": "ex-1"}


def test_build_lookup_params_rejects_missing_identifiers() -> None:
    with pytest.raises(ValueError, match=r"requires exchange_order_id\(uuid\) or client_order_id"):
        build_lookup_params(client_order_id=None, exchange_order_id=None)


def test_build_lookup_params_rejects_invalid_client_order_id_when_uuid_missing() -> None:
    with pytest.raises(BrokerRejectError, match="contains invalid characters"):
        build_lookup_params(client_order_id="invalid id", exchange_order_id=None)
