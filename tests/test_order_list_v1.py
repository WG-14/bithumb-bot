from __future__ import annotations

import pytest

from bithumb_bot.broker.bithumb import BithumbPrivateAPI
from bithumb_bot.broker.order_list_v1 import build_order_list_params


def test_build_order_list_params_accepts_uuids_only() -> None:
    params = build_order_list_params(uuids=["uuid-1", "uuid-2"])
    assert params == {
        "uuids": ["uuid-1", "uuid-2"],
        "page": 1,
        "order_by": "desc",
    }


def test_build_order_list_params_accepts_client_order_ids_only() -> None:
    params = build_order_list_params(client_order_ids=["cid-1", "cid-2"], state="done", page=2, order_by="asc")
    assert params == {
        "client_order_ids": ["cid-1", "cid-2"],
        "state": "done",
        "page": 2,
        "order_by": "asc",
    }


def test_build_order_list_params_rejects_missing_identifiers() -> None:
    with pytest.raises(ValueError, match="requires uuids or client_order_ids"):
        build_order_list_params()


def test_build_order_list_params_rejects_identifier_length_over_limit() -> None:
    with pytest.raises(ValueError, match="allows at most 100 items"):
        build_order_list_params(uuids=[f"uuid-{idx}" for idx in range(101)])


def test_build_order_list_params_rejects_invalid_state() -> None:
    with pytest.raises(ValueError, match="state must be one of"):
        build_order_list_params(uuids=["uuid-1"], state="unknown")


def test_build_order_list_params_rejects_zero_page() -> None:
    with pytest.raises(ValueError, match="page must be between 1"):
        build_order_list_params(uuids=["uuid-1"], page=0)


def test_build_order_list_params_rejects_invalid_order_by() -> None:
    with pytest.raises(ValueError, match="order_by must be one of"):
        build_order_list_params(uuids=["uuid-1"], order_by="latest")


def test_build_order_list_params_query_string_remains_query_hash_compatible() -> None:
    params = build_order_list_params(uuids=["uuid-1", "uuid-2"], state="wait", page=3, order_by="desc")
    query = BithumbPrivateAPI._query_string(params)
    assert query == "page=3&order_by=desc&uuids[]=uuid-1&uuids[]=uuid-2&state=wait"
    claims = BithumbPrivateAPI._query_hash_claims(params)
    assert claims["query_hash"]
    assert claims["query_hash_alg"] == "SHA512"


def test_build_order_list_params_client_order_ids_array_is_consistent_for_auth_hash() -> None:
    params = build_order_list_params(client_order_ids=["cid-1", "cid-2"], state="done")
    query = BithumbPrivateAPI._query_string(params)
    assert query == "page=1&order_by=desc&client_order_ids[]=cid-1&client_order_ids[]=cid-2&state=done"
    claims = BithumbPrivateAPI._query_hash_claims(params)
    assert claims["query_hash"]
