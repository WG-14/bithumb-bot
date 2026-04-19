from __future__ import annotations

from .base import BrokerFill, BrokerIdentifierMismatchError, BrokerOrder, BrokerRejectError, BrokerSchemaError, BrokerTemporaryError
from .order_list_v1 import build_order_list_params, build_recovery_order_list_params, parse_v1_order_list_row
from .order_lookup_v1 import (
    build_lookup_params as build_v1_order_lookup_params,
    ensure_identifier_consistency as ensure_v1_identifier_consistency,
    require_order_payload_dict as require_v1_order_payload_dict,
    require_known_state as require_v1_known_state,
    resolve_requested_identifiers as resolve_v1_requested_identifiers,
    resolve_identifiers as resolve_v1_order_identifiers,
    status_from_state as v1_status_from_state,
)


def get_order(
    broker,
    *,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    classify_private_api_error,
) -> BrokerOrder:
    now = broker._now_millis()
    requested = resolve_v1_requested_identifiers(
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
    )
    params = build_v1_order_lookup_params(
        client_order_id=requested.client_order_id,
        exchange_order_id=requested.exchange_order_id,
    )

    exid = requested.exchange_order_id or f"dry_{requested.client_order_id}"
    if broker.dry_run:
        return BrokerOrder(requested.client_order_id, exid, "BUY", "NEW", None, 0.0, 0.0, now, now)

    response_client_order_id = ""
    response_exchange_order_id = ""
    try:
        payload = broker._get_private("/v1/order", params, retry_safe=True)
        data = require_v1_order_payload_dict(payload, context="order lookup response")
        broker._journal_read_summary(path="/v1/order", data=data)
        response_has_identifier = any(broker._clean_identifier(data.get(key)) for key in ("uuid", "client_order_id"))
        if not response_has_identifier:
            raise BrokerSchemaError("order lookup response schema mismatch: missing both uuid and client_order_id in response")

        resolved_ids = resolve_v1_order_identifiers(
            data,
            fallback_client_order_id=requested.client_order_id,
        )
        response_client_order_id = resolved_ids.client_order_id
        response_exchange_order_id = resolved_ids.exchange_order_id
        ensure_v1_identifier_consistency(
            requested=requested,
            response=resolved_ids,
            context="order lookup response",
            require_response_identifier=True,
        )

        normalized = broker._normalize_v1_order_row_strict(data)
    except (BrokerSchemaError, BrokerIdentifierMismatchError, BrokerTemporaryError, BrokerRejectError) as exc:
        broker._log_v1_myorder_lookup_failure(
            stage="get_order",
            retry_safe=True,
            requested_client_order_id=requested.client_order_id,
            requested_exchange_order_id=requested.exchange_order_id,
            response_client_order_id=response_client_order_id,
            response_exchange_order_id=response_exchange_order_id,
            reason=f"{classify_private_api_error(exc)[0]}:{exc}",
        )
        raise

    state = normalized.state
    qty_req = float(normalized.volume)
    qty_filled = float(normalized.executed_volume)
    status = v1_status_from_state(state=state, qty_req=qty_req, qty_filled=qty_filled)
    order_raw = broker._raw_v1_order_fields(data)
    return BrokerOrder(
        client_order_id=response_client_order_id,
        exchange_order_id=response_exchange_order_id,
        side=str(normalized.side),
        status=status,
        price=float(normalized.price) if normalized.price is not None else None,
        qty_req=qty_req,
        qty_filled=qty_filled,
        created_ts=int(normalized.created_ts),
        updated_ts=int(normalized.updated_ts),
        raw=order_raw,
    )


def get_open_orders(
    broker,
    *,
    exchange_order_ids: list[str] | tuple[str, ...] | None = None,
    client_order_ids: list[str] | tuple[str, ...] | None = None,
) -> list[BrokerOrder]:
    if broker.dry_run:
        return []
    if not exchange_order_ids and not client_order_ids:
        raise BrokerRejectError(
            "open order lookup is identifier-scoped by bot policy; /v1/orders broad market/state scans are reserved for recovery via get_recent_orders_for_recovery"
        )
    data = broker._get_private(
        "/v1/orders",
        build_order_list_params(
            uuids=exchange_order_ids,
            client_order_ids=client_order_ids,
            state="wait",
            page=1,
            order_by="desc",
        ),
        retry_safe=True,
    )
    broker._journal_read_summary(path="/v1/orders(open_orders)", data=data)
    if not isinstance(data, list):
        raise BrokerRejectError(f"unexpected /v1/orders payload type: {type(data).__name__}")

    out: list[BrokerOrder] = []
    exchange_ids_count = len(exchange_order_ids or [])
    client_ids_count = len(client_order_ids or [])
    for row in data:
        if not isinstance(row, dict):
            raise BrokerRejectError("/v1/orders schema mismatch: each row must be object")
        try:
            normalized = parse_v1_order_list_row(row)
        except BrokerRejectError as exc:
            broker._log_v1_orders_parse_failure(
                endpoint="/v1/orders",
                state="wait",
                exchange_ids_count=exchange_ids_count,
                client_ids_count=client_ids_count,
                row=row,
                reason=str(exc),
            )
            raise
        qty_req, qty_filled = broker._v1_list_quantities(normalized)
        status = v1_status_from_state(state=normalized.state, qty_req=qty_req, qty_filled=qty_filled)
        broker._log_v1_orders_price_resolution(
            endpoint="/v1/orders",
            state="wait",
            exchange_ids_count=exchange_ids_count,
            client_ids_count=client_ids_count,
            row=row,
            normalized=normalized,
        )
        out.append(
            BrokerOrder(
                client_order_id=normalized.client_order_id,
                exchange_order_id=normalized.uuid,
                side=normalized.side,
                status=status,
                price=normalized.price,
                qty_req=qty_req,
                qty_filled=qty_filled,
                created_ts=int(normalized.created_ts),
                updated_ts=int(normalized.updated_ts),
                raw=broker._raw_v1_order_fields(row),
            )
        )
    return out


def get_fills(
    broker,
    *,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    parse_mode: str = "strict",
    classify_private_api_error,
) -> list[BrokerFill]:
    if broker.dry_run:
        return []
    normalized_parse_mode = str(parse_mode or "strict").strip().lower()
    if normalized_parse_mode not in {"strict", "salvage"}:
        raise BrokerRejectError(f"unsupported fill parse_mode={parse_mode!r}")
    strict_fee_parse = normalized_parse_mode == "strict"

    requested = resolve_v1_requested_identifiers(
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
    )

    if not (requested.exchange_order_id or requested.client_order_id):
        raise BrokerRejectError(
            "fill lookup requires identifiers; /v1/order does not support broad recent fill scans without uuid/client_order_id"
        )
    params = build_v1_order_lookup_params(
        client_order_id=requested.client_order_id,
        exchange_order_id=requested.exchange_order_id,
    )
    response_client_order_id = ""
    response_exchange_order_id = ""
    try:
        payload = broker._get_private("/v1/order", params, retry_safe=True)
        data = require_v1_order_payload_dict(payload, context="fill lookup response")
        broker._journal_read_summary(path="/v1/order(fills)", data=data)
        response_has_identifier = any(broker._clean_identifier(data.get(key)) for key in ("uuid", "client_order_id"))
        if not response_has_identifier:
            raise BrokerSchemaError("fill lookup response schema mismatch: missing both uuid and client_order_id in response")

        response_ids = resolve_v1_order_identifiers(
            data,
            fallback_client_order_id=requested.client_order_id,
        )
        response_client_order_id = response_ids.client_order_id
        response_exchange_order_id = response_ids.exchange_order_id
        ensure_v1_identifier_consistency(
            requested=requested,
            response=response_ids,
            context="fill lookup response",
            require_response_identifier=True,
            enforce_client_match_with_exchange_lookup=True,
        )
    except (BrokerSchemaError, BrokerIdentifierMismatchError, BrokerTemporaryError, BrokerRejectError) as exc:
        broker._log_v1_myorder_lookup_failure(
            stage="get_fills",
            retry_safe=True,
            requested_client_order_id=requested.client_order_id,
            requested_exchange_order_id=requested.exchange_order_id,
            response_client_order_id=response_client_order_id,
            response_exchange_order_id=response_exchange_order_id,
            reason=f"{classify_private_api_error(exc)[0]}:{exc}",
        )
        raise

    fills: list[BrokerFill] = []
    requires_removed_legacy_scan = False
    for row in [data]:
        if row.get("trades") not in (None, "") and not isinstance(row.get("trades"), list):
            raise BrokerRejectError("/v1/order schema mismatch: trades must be a list when present")
        require_v1_known_state(row.get("state"), context="/v1/order")
        normalized = broker._normalize_v1_order_row_lenient_for_fills(row)
        trades = normalized["trades"] if isinstance(normalized["trades"], list) else []
        if trades:
            for index, trade in enumerate(trades):
                if not isinstance(trade, dict):
                    continue
                qty = broker._strict_optional_number(trade, "volume", context="/v1/order.trades")
                price = broker._resolve_fill_price(trade, normalized_row=normalized)
                if qty is None or qty <= 0:
                    continue
                if price is None:
                    raise BrokerRejectError("/v1/order.trades schema mismatch: missing required numeric field 'price'")
                fee_observation = broker._extract_fill_fee_observation(
                    trade,
                    context="trade",
                    qty=qty,
                    price=price,
                    strict=strict_fee_parse,
                )
                ts_raw = trade.get("created_at")
                ts = broker._strict_parse_ts(ts_raw, field_name="created_at", context="/v1/order.trades")
                trade_client_order_id, _ = broker._resolve_order_identifiers(
                    trade,
                    fallback_client_order_id=requested.client_order_id or row.get("client_order_id") or "",
                )
                fills.append(
                    BrokerFill(
                        client_order_id=trade_client_order_id,
                        fill_id=str(trade.get("uuid") or trade.get("id") or f"{row.get('uuid') or ''}:{index}:{ts}"),
                        fill_ts=ts,
                        price=float(price),
                        qty=float(qty),
                        fee=fee_observation.fee,
                        exchange_order_id=str(row.get("uuid") or ""),
                        fee_status=fee_observation.status,
                        parse_warnings=((fee_observation.warning,) if fee_observation.warning else ()),
                        raw=broker._sanitize_debug_value(trade),
                    )
                )
            continue

        qty_filled = float(normalized["executed_volume"])
        if qty_filled <= 0:
            requires_removed_legacy_scan = True
            continue
        price = broker._resolve_fill_price(row, normalized_row=normalized)
        if price is None:
            requires_removed_legacy_scan = True
            continue
        updated_raw = row.get("updated_at")
        created_raw = row.get("created_at")
        try:
            if updated_raw not in (None, ""):
                ts = broker._strict_parse_ts(updated_raw, field_name="updated_at", context="/v1/order")
            elif created_raw not in (None, ""):
                ts = broker._strict_parse_ts(created_raw, field_name="created_at", context="/v1/order")
            else:
                raise BrokerRejectError("/v1/order schema mismatch: missing required timestamp field 'created_at'")
        except BrokerRejectError:
            requires_removed_legacy_scan = True
            continue
        fee_observation = broker._extract_fill_fee_observation(
            row,
            context="aggregate",
            qty=qty_filled,
            price=price,
            strict=False,
        )
        aggregate_client_order_id, aggregate_exchange_order_id = broker._resolve_order_identifiers(
            row,
            fallback_client_order_id=requested.client_order_id or "",
            fallback_exchange_order_id=str(normalized.get("uuid") or ""),
        )
        fills.append(
            BrokerFill(
                client_order_id=aggregate_client_order_id,
                fill_id=f"{row.get('uuid') or ''}:aggregate:{ts}",
                fill_ts=ts,
                price=float(price),
                qty=qty_filled,
                fee=fee_observation.fee,
                exchange_order_id=aggregate_exchange_order_id,
                fee_status=fee_observation.status,
                parse_warnings=((fee_observation.warning,) if fee_observation.warning else ()),
                raw=broker._sanitize_debug_value(row),
            )
        )
    if not fills and requires_removed_legacy_scan:
        raise BrokerRejectError(
            "fill lookup requires /v1/order trade payload completeness; broad /v1/orders done scan fallback is disabled"
        )
    return fills


def get_recent_orders(
    broker,
    *,
    limit: int = 100,
    exchange_order_ids: list[str] | tuple[str, ...] | None = None,
    client_order_ids: list[str] | tuple[str, ...] | None = None,
) -> list[BrokerOrder]:
    lim = max(0, int(limit))
    if lim == 0:
        return []
    if not exchange_order_ids and not client_order_ids:
        raise BrokerRejectError(
            "recent order lookup is identifier-scoped by bot policy; /v1/orders broad market/state scans are reserved for recovery via get_recent_orders_for_recovery"
        )

    snapshots: dict[str, BrokerOrder] = {}
    exchange_ids_count = len(exchange_order_ids or [])
    client_ids_count = len(client_order_ids or [])
    for state, journal_path in (("wait", "/v1/orders(open_orders)"), ("done", "/v1/orders(done)"), ("cancel", "/v1/orders(cancel)")):
        data = broker._get_private(
            "/v1/orders",
            build_order_list_params(
                uuids=exchange_order_ids,
                client_order_ids=client_order_ids,
                state=state,
                page=1,
                order_by="desc",
                limit=min(lim, 100),
            ),
            retry_safe=True,
        )
        broker._journal_read_summary(path=journal_path, data=data)
        if not isinstance(data, list):
            raise BrokerRejectError(f"unexpected /v1/orders payload type: {type(data).__name__}")
        for row in data:
            if not isinstance(row, dict):
                raise BrokerRejectError("/v1/orders schema mismatch: each row must be object")
            try:
                normalized = parse_v1_order_list_row(row)
            except BrokerRejectError as exc:
                broker._log_v1_orders_parse_failure(
                    endpoint="/v1/orders",
                    state=state,
                    exchange_ids_count=exchange_ids_count,
                    client_ids_count=client_ids_count,
                    row=row,
                    reason=str(exc),
                )
                raise
            qty_req, qty_filled = broker._v1_list_quantities(normalized)
            broker._log_v1_orders_price_resolution(
                endpoint="/v1/orders",
                state=state,
                exchange_ids_count=exchange_ids_count,
                client_ids_count=client_ids_count,
                row=row,
                normalized=normalized,
            )
            order = BrokerOrder(
                client_order_id=normalized.client_order_id,
                exchange_order_id=normalized.uuid,
                side=normalized.side,
                status=v1_status_from_state(state=normalized.state, qty_req=qty_req, qty_filled=qty_filled),
                price=normalized.price,
                qty_req=qty_req,
                qty_filled=qty_filled,
                created_ts=int(normalized.created_ts),
                updated_ts=int(normalized.updated_ts),
                raw=broker._raw_v1_order_fields(row),
            )
            snapshot_key = str(order.exchange_order_id or order.client_order_id or "")
            if snapshot_key:
                snapshots[snapshot_key] = order

    out = list(snapshots.values())
    out.sort(key=lambda order: int(order.updated_ts), reverse=True)
    return out[:lim]


def get_recent_orders_for_recovery(
    broker,
    *,
    limit: int = 100,
    market: str,
    page_size: int | None = None,
) -> list[BrokerOrder]:
    lim = max(0, int(limit))
    if lim == 0:
        return []

    conservative_page_size = min(max(1, lim), max(1, int(page_size or 30)), 30)
    recovery_states: tuple[tuple[str, ...], ...] = (("wait", "done", "cancel"), ("watch",))
    snapshots: dict[str, BrokerOrder] = {}

    for states in recovery_states:
        page = 1
        while len(snapshots) < lim:
            params = build_recovery_order_list_params(
                market=market,
                states=states,
                page=page,
                order_by="desc",
                limit=conservative_page_size,
            )
            data = broker._get_private("/v1/orders", params, retry_safe=True)
            broker._journal_read_summary(path=f"/v1/orders(recovery:{'+'.join(states)})", data=data)
            if not isinstance(data, list):
                raise BrokerRejectError(f"unexpected /v1/orders payload type: {type(data).__name__}")
            if not data:
                break
            for row in data:
                if not isinstance(row, dict):
                    raise BrokerRejectError("/v1/orders schema mismatch: each row must be object")
                try:
                    normalized = parse_v1_order_list_row(row)
                except BrokerRejectError as exc:
                    broker._log_v1_orders_parse_failure(
                        endpoint="/v1/orders",
                        state="+".join(states),
                        exchange_ids_count=0,
                        client_ids_count=0,
                        row=row,
                        reason=str(exc),
                    )
                    raise
                qty_req, qty_filled = broker._v1_list_quantities(normalized)
                broker._log_v1_orders_price_resolution(
                    endpoint="/v1/orders",
                    state="+".join(states),
                    exchange_ids_count=0,
                    client_ids_count=0,
                    row=row,
                    normalized=normalized,
                )
                order = BrokerOrder(
                    client_order_id=normalized.client_order_id,
                    exchange_order_id=normalized.uuid,
                    side=normalized.side,
                    status=v1_status_from_state(state=normalized.state, qty_req=qty_req, qty_filled=qty_filled),
                    price=normalized.price,
                    qty_req=qty_req,
                    qty_filled=qty_filled,
                    created_ts=int(normalized.created_ts),
                    updated_ts=int(normalized.updated_ts),
                    raw=broker._raw_v1_order_fields(row),
                )
                snapshot_key = str(order.exchange_order_id or order.client_order_id or "")
                if snapshot_key:
                    snapshots[snapshot_key] = order
            if len(data) < conservative_page_size:
                break
            page += 1

    out = list(snapshots.values())
    out.sort(key=lambda order: int(order.updated_ts), reverse=True)
    return out[:lim]
