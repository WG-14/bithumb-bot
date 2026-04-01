from __future__ import annotations

from dataclasses import dataclass

from .order_lookup_v1 import V1_ORDER_STATES, clean_identifier
from .order_payloads import validate_client_order_id

_ORDER_BY_VALUES = {"asc", "desc"}
_MAX_IDENTIFIER_COUNT = 100
_MAX_PAGE = 10_000


@dataclass(frozen=True)
class OrderListQuery:
    uuids: tuple[str, ...] = ()
    client_order_ids: tuple[str, ...] = ()
    state: str | None = None
    page: int = 1
    order_by: str = "desc"

    def to_params(self) -> dict[str, object]:
        params: dict[str, object] = {
            "page": self.page,
            "order_by": self.order_by,
        }
        if self.uuids:
            params["uuids"] = list(self.uuids)
        if self.client_order_ids:
            params["client_order_ids"] = list(self.client_order_ids)
        if self.state is not None:
            params["state"] = self.state
        return params


def _validate_identifier_list(values: list[str], *, field_name: str) -> tuple[str, ...]:
    if len(values) > _MAX_IDENTIFIER_COUNT:
        raise ValueError(f"{field_name} allows at most {_MAX_IDENTIFIER_COUNT} items")
    out: list[str] = []
    for raw in values:
        cleaned = clean_identifier(raw)
        if not cleaned:
            raise ValueError(f"{field_name} must not include empty identifiers")
        out.append(cleaned)
    return tuple(out)


def build_order_list_params(
    *,
    uuids: list[str] | tuple[str, ...] | None = None,
    client_order_ids: list[str] | tuple[str, ...] | None = None,
    state: str | None = None,
    page: int = 1,
    order_by: str = "desc",
) -> dict[str, object]:
    uuid_values = _validate_identifier_list(list(uuids or []), field_name="uuids")
    client_values = _validate_identifier_list(
        [validate_client_order_id(value) for value in list(client_order_ids or [])],
        field_name="client_order_ids",
    )
    if not uuid_values and not client_values:
        raise ValueError("order list lookup requires uuids or client_order_ids")

    normalized_state = clean_identifier(state).lower() if state is not None else None
    if normalized_state is not None and normalized_state not in V1_ORDER_STATES:
        raise ValueError(f"state must be one of {sorted(V1_ORDER_STATES)}")

    normalized_page = int(page)
    if normalized_page < 1 or normalized_page > _MAX_PAGE:
        raise ValueError(f"page must be between 1 and {_MAX_PAGE}")

    normalized_order_by = clean_identifier(order_by).lower()
    if normalized_order_by not in _ORDER_BY_VALUES:
        raise ValueError(f"order_by must be one of {sorted(_ORDER_BY_VALUES)}")

    return OrderListQuery(
        uuids=uuid_values,
        client_order_ids=client_values,
        state=normalized_state,
        page=normalized_page,
        order_by=normalized_order_by,
    ).to_params()


def build_legacy_order_scan_params(*, market: str, state: str, limit: int) -> dict[str, object]:
    normalized_market = clean_identifier(market)
    if not normalized_market:
        raise ValueError("market is required")
    normalized_state = clean_identifier(state).lower()
    if normalized_state not in V1_ORDER_STATES:
        raise ValueError(f"state must be one of {sorted(V1_ORDER_STATES)}")
    normalized_limit = int(limit)
    if normalized_limit < 1:
        raise ValueError("limit must be >= 1")
    # TODO: remove legacy market/state scan once all /v1/orders callers migrate to documented identifier lookup.
    return {
        "market": normalized_market,
        "state": normalized_state,
        "limit": normalized_limit,
    }
