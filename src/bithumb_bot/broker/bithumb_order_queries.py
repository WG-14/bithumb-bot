from __future__ import annotations

from ..config import settings
from ..fee_observation import (
    MultiFillTradeEvidence,
    classify_fee_evaluation,
    validate_multi_fill_order_level_paid_fee_allocation,
    validate_single_fill_order_level_paid_fee,
)
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


_ORDER_LEVEL_FEE_KEYS = ("fee", "paid_fee", "commission", "trade_fee", "transaction_fee", "fee_amount")
_ORDER_LEVEL_AGGREGATE_FEE_KEYS = ("paid_fee", "reserved_fee", "remaining_fee", "fee", "commission", "trade_fee", "transaction_fee", "fee_amount")


def _order_level_fee_candidate(
    broker,
    row: dict[str, object],
    *,
    trades: list[object],
) -> tuple[float | None, str | None]:
    present_keys = [key for key in _ORDER_LEVEL_FEE_KEYS if key in row]
    for key in present_keys:
        fee = broker._to_float(row.get(key), default=None)
        if fee is None:
            continue
        if fee < 0:
            continue
        if len(trades) != 1:
            return None, f"order_level_fee_candidate_ambiguous:{key}"
        return float(fee), f"order_level_fee_candidate:{key}"
    if present_keys:
        return None, f"order_level_fee_candidate_unparseable:{present_keys[0]}"
    return None, None


def _trade_fee_warnings(
    broker,
    row: dict[str, object],
    *,
    trade_fee: float | None,
    trades: list[object],
) -> tuple[str, ...]:
    candidate_fee, candidate_warning = _order_level_fee_candidate(broker, row, trades=trades)
    if candidate_warning is None:
        return ()
    if candidate_fee is None:
        return (candidate_warning,)
    if trade_fee is None:
        return (candidate_warning,)
    if abs(float(trade_fee) - float(candidate_fee)) > 1e-12:
        return (f"order_level_fee_disagrees:{candidate_warning.split(':', 1)[-1]}",)
    return ()


def _build_order_level_paid_fee_evaluation(
    broker,
    *,
    row: dict[str, object],
    trade: dict[str, object],
    qty: float | None,
    price: float | None,
    trades: list[object],
    client_order_id: str,
    exchange_order_id: str,
) -> object | None:
    if "paid_fee" not in row:
        return None
    fill_funds = None
    if qty is not None and price is not None:
        fill_funds = price * qty
    if "funds" in trade:
        try:
            fill_funds = float(trade.get("funds"))
        except (TypeError, ValueError):
            fill_funds = fill_funds
    return validate_single_fill_order_level_paid_fee(
        paid_fee=row.get("paid_fee"),
        fill_qty=qty,
        fill_price=price,
        fill_funds=fill_funds,
        order_executed_volume=broker._to_float(row.get("executed_volume"), default=None),
        order_executed_funds=broker._to_float(row.get("executed_funds"), default=None),
        single_fill_evidence=(len(trades) == 1),
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        fill_id=str(trade.get("uuid") or trade.get("id") or ""),
    )


def _build_multi_fill_order_level_paid_fee_allocations(
    broker,
    *,
    row: dict[str, object],
    trades: list[object],
    client_order_id: str,
    exchange_order_id: str,
):
    trade_evidence: list[MultiFillTradeEvidence] = []
    for trade in trades:
        if not isinstance(trade, dict):
            return None
        qty = broker._strict_optional_number(trade, "volume", context="/v1/order.trades")
        price = broker._resolve_fill_price(trade, normalized_row=row)
        if qty is None or qty <= 0 or price is None:
            return None
        funds = broker._to_float(trade.get("funds"), default=None)
        if funds is None:
            funds = float(qty) * float(price)
        trade_evidence.append(
            MultiFillTradeEvidence(
                fill_id=str(trade.get("uuid") or trade.get("id") or ""),
                qty=float(qty),
                price=float(price),
                funds=float(funds),
            )
        )
    return validate_multi_fill_order_level_paid_fee_allocation(
        paid_fee=row.get("paid_fee"),
        trades=trade_evidence,
        order_executed_volume=broker._to_float(row.get("executed_volume"), default=None),
        order_executed_funds=broker._to_float(row.get("executed_funds"), default=None),
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
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
        multi_fill_order_level_allocation = None
        if trades and len(trades) > 1 and "paid_fee" in row:
            multi_fill_order_level_allocation = _build_multi_fill_order_level_paid_fee_allocations(
                broker,
                row=row,
                trades=trades,
                client_order_id=(requested.client_order_id or row.get("client_order_id") or ""),
                exchange_order_id=str(row.get("uuid") or ""),
            )
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
                    strict=False,
                )
                fee = fee_observation.fee
                fee_status = fee_observation.status
                fee_source = "trade_level_fee" if fee_status == "complete" else "missing"
                fee_confidence = "authoritative" if fee_status == "complete" else "ambiguous"
                fee_provenance = "trade_level_fee_present" if fee_status == "complete" else "missing_fee_field"
                fee_validation_reason = "accounting_complete" if fee_status == "complete" else fee_status
                fee_validation_checks: dict[str, bool] | None = None
                warnings = [fee_observation.warning] if fee_observation.warning else []
                candidate_fee = None
                candidate_warning = None
                allocated_evaluation = None
                if multi_fill_order_level_allocation is not None:
                    allocated_evaluation = multi_fill_order_level_allocation.evaluations_by_fill_id.get(
                        str(trade.get("uuid") or trade.get("id") or "")
                    )
                current_evaluation = classify_fee_evaluation(
                    fee=fee,
                    fee_status=fee_status,
                    price=price,
                    qty=qty,
                    material_notional_threshold=float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW),
                    fee_source=fee_source,
                    fee_confidence=fee_confidence,
                    provenance=fee_provenance,
                    reason=fee_validation_reason,
                    checks=fee_validation_checks,
                )
                if allocated_evaluation is not None and current_evaluation.accounting_status != "accounting_complete":
                    candidate_fee, candidate_warning = _order_level_fee_candidate(broker, row, trades=trades)
                    if candidate_warning:
                        warnings.append(candidate_warning)
                    fee = allocated_evaluation.fee
                    fee_status = allocated_evaluation.fee_status
                    fee_source = allocated_evaluation.fee_source
                    fee_confidence = allocated_evaluation.fee_confidence
                    fee_provenance = allocated_evaluation.provenance
                    fee_validation_reason = allocated_evaluation.reason
                    fee_validation_checks = allocated_evaluation.checks
                elif fee is None and fee_status in {"missing", "empty", "invalid", "zero_reported", "unparseable"}:
                    candidate_fee, candidate_warning = _order_level_fee_candidate(broker, row, trades=trades)
                    if candidate_warning:
                        warnings.append(candidate_warning)
                    if candidate_fee is not None:
                        evaluation = _build_order_level_paid_fee_evaluation(
                            broker,
                            row=row,
                            trade=trade,
                            qty=qty,
                            price=price,
                            trades=trades,
                            client_order_id=(requested.client_order_id or row.get("client_order_id") or ""),
                            exchange_order_id=str(row.get("uuid") or ""),
                        )
                        if evaluation is not None:
                            fee = evaluation.fee
                            fee_status = evaluation.fee_status
                            fee_source = evaluation.fee_source
                            fee_confidence = evaluation.fee_confidence
                            fee_provenance = evaluation.provenance
                            fee_validation_reason = evaluation.reason
                            fee_validation_checks = evaluation.checks
                        else:
                            fee = candidate_fee
                            fee_status = "order_level_candidate"
                            fee_source = "order_level_paid_fee"
                            fee_confidence = "ambiguous"
                            fee_provenance = "order_level_fee_candidate"
                            fee_validation_reason = "order_level_fee_candidate"
                    elif strict_fee_parse:
                        warning_suffix = ""
                        if fee_observation.warning and ":" in fee_observation.warning:
                            warning_suffix = f" '{fee_observation.warning.split(':', 1)[1]}'"
                        if fee_status == "missing":
                            raise BrokerRejectError(
                                "/v1/order.trade schema mismatch: missing fee field for materially sized fill"
                            )
                        if fee_status == "empty":
                            raise BrokerRejectError(
                                f"/v1/order.trade schema mismatch: empty fee field{warning_suffix} for materially sized fill"
                            )
                        if fee_status == "zero_reported":
                            raise BrokerRejectError(
                                f"/v1/order.trade schema mismatch: zero fee field{warning_suffix} for materially sized fill"
                            )
                        raise BrokerRejectError(
                            f"/v1/order.trade schema mismatch: {fee_status} fee field{warning_suffix} for materially sized fill"
                        )
                else:
                    warnings.extend(_trade_fee_warnings(broker, row, trade_fee=fee, trades=trades))
                evaluation = classify_fee_evaluation(
                    fee=fee,
                    fee_status=fee_status,
                    price=price,
                    qty=qty,
                    material_notional_threshold=float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW),
                    fee_source=fee_source,
                    fee_confidence=fee_confidence,
                    provenance=fee_provenance,
                    reason=fee_validation_reason,
                    checks=fee_validation_checks,
                )
                order_fee_fields = {key: row.get(key) for key in _ORDER_LEVEL_FEE_KEYS if key in row}
                raw_fill_payload = (
                    {"trade": trade, "order_fee_fields": order_fee_fields}
                    if order_fee_fields
                    else trade
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
                        fee=fee,
                        exchange_order_id=str(row.get("uuid") or ""),
                        fee_status=evaluation.fee_status,
                        fee_source=evaluation.fee_source,
                        fee_confidence=evaluation.fee_confidence,
                        fee_provenance=evaluation.provenance,
                        fee_validation_reason=evaluation.reason,
                        fee_validation_checks=evaluation.checks,
                        parse_warnings=tuple(warnings),
                        raw=broker._sanitize_debug_value(raw_fill_payload),
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
        aggregate_fee_status = fee_observation.status
        aggregate_fee_source = "trade_level_fee" if aggregate_fee_status == "complete" else "missing"
        aggregate_fee_confidence = "authoritative" if aggregate_fee_status == "complete" else "ambiguous"
        aggregate_fee_provenance = "aggregate_trade_level_fee_present" if aggregate_fee_status == "complete" else "missing_fee_field"
        aggregate_fee_reason = "accounting_complete" if aggregate_fee_status == "complete" else aggregate_fee_status
        aggregate_fee_checks: dict[str, bool] | None = None
        aggregate_warnings = [fee_observation.warning] if fee_observation.warning else []
        aggregate_fee_keys = [key for key in _ORDER_LEVEL_AGGREGATE_FEE_KEYS if key in row]
        if fee_observation.fee is not None and fee_observation.status == "complete" and aggregate_fee_keys:
            aggregate_fee_status = "order_level_candidate"
            aggregate_fee_source = "order_level_paid_fee"
            aggregate_fee_confidence = "ambiguous"
            aggregate_fee_provenance = "order_level_paid_fee_aggregate"
            aggregate_fee_reason = "multi_fill_order_level_fee_ambiguous"
            aggregate_fee_checks = {
                "single_fill": False,
                "paid_fee_present": True,
                "executed_volume_match": True,
                "executed_funds_match": True,
                "expected_fee_rate_match": False,
                "identifiers_match": bool(
                    str((requested.client_order_id or row.get("client_order_id") or "")).strip()
                    and str(normalized.get("uuid") or "").strip()
                ),
                "material_notional_suspicious": True,
            }
            aggregate_warnings.append(f"order_level_fee_candidate:{aggregate_fee_keys[0]}")
        aggregate_evaluation = classify_fee_evaluation(
            fee=fee_observation.fee,
            fee_status=aggregate_fee_status,
            price=price,
            qty=qty_filled,
            material_notional_threshold=float(settings.LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW),
            fee_source=aggregate_fee_source,
            fee_confidence=aggregate_fee_confidence,
            provenance=aggregate_fee_provenance,
            reason=aggregate_fee_reason,
            checks=aggregate_fee_checks,
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
                fee_status=aggregate_evaluation.fee_status,
                fee_source=aggregate_evaluation.fee_source,
                fee_confidence=aggregate_evaluation.fee_confidence,
                fee_provenance=aggregate_evaluation.provenance,
                fee_validation_reason=aggregate_evaluation.reason,
                fee_validation_checks=aggregate_evaluation.checks,
                parse_warnings=tuple(aggregate_warnings),
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
