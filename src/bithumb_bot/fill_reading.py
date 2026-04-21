from __future__ import annotations

import logging
from enum import Enum

from .broker.base import Broker, BrokerFill

_LOG = logging.getLogger(__name__)


class FillReadPolicy(str, Enum):
    ACCOUNTING_STRICT = "accounting_strict"
    OBSERVATION_SALVAGE = "observation_salvage"


def get_broker_fills(
    broker: Broker,
    *,
    client_order_id: str | None = None,
    exchange_order_id: str | None = None,
    policy: FillReadPolicy,
) -> list[BrokerFill]:
    """Read broker fills with an explicit accounting or observation policy."""
    parse_mode = "strict" if policy == FillReadPolicy.ACCOUNTING_STRICT else "salvage"
    try:
        return broker.get_fills(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            parse_mode=parse_mode,
        )
    except TypeError as exc:
        if "parse_mode" not in str(exc):
            raise
        _LOG.warning(
            "broker_get_fills_parse_mode_compatibility_fallback policy=%s "
            "client_order_id=%s exchange_order_id=%s error=%s",
            policy.value,
            client_order_id or "-",
            exchange_order_id or "-",
            exc,
        )
        return broker.get_fills(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
        )
