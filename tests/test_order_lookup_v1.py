from __future__ import annotations

import pytest

from bithumb_bot.broker.base import BrokerIdentifierMismatchError, BrokerRejectError, BrokerSchemaError
from bithumb_bot.broker.order_lookup_v1 import (
    build_lookup_params,
    ensure_identifier_consistency,
    require_order_payload_dict,
    resolve_identifiers,
    resolve_requested_identifiers,
)


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


def test_require_order_payload_dict_rejects_non_dict_payload() -> None:
    with pytest.raises(BrokerSchemaError, match="expected object payload actual=list"):
        require_order_payload_dict([], context="order lookup response")


def test_ensure_identifier_consistency_rejects_identifier_mismatch() -> None:
    requested = resolve_requested_identifiers(client_order_id="cid-1", exchange_order_id="ex-1")
    response = resolve_identifiers({"uuid": "ex-other", "client_order_id": "cid-1"})
    with pytest.raises(BrokerIdentifierMismatchError, match="exchange_order_id mismatch"):
        ensure_identifier_consistency(requested=requested, response=response, context="order lookup response")


def test_ensure_identifier_consistency_rejects_missing_response_identifiers_when_required() -> None:
    requested = resolve_requested_identifiers(client_order_id="cid-1", exchange_order_id="ex-1")
    response = resolve_identifiers({})
    with pytest.raises(BrokerSchemaError, match="missing both uuid and client_order_id"):
        ensure_identifier_consistency(
            requested=requested,
            response=response,
            context="order lookup response",
            require_response_identifier=True,
        )
