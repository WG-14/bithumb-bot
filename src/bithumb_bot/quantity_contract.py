from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


QtyStepAuthorityLevel = Literal[
    "exchange_hard",
    "persisted_exchange_snapshot",
    "local_fallback",
    "operator_policy",
    "unknown",
]


_EXCHANGE_HARD_SOURCES = {"chance_doc", "exchange", "orders_chance", "exchange_hard"}
_LOCAL_FALLBACK_SOURCES = {
    "local_fallback",
    "settings.LIVE_ORDER_QTY_STEP",
    "settings.LIVE_MIN_ORDER_QTY",
    "settings.MIN_ORDER_NOTIONAL_KRW",
    "settings.LIVE_ORDER_MAX_QTY_DECIMALS",
}


@dataclass(frozen=True)
class ExchangeQuantityContract:
    market: str
    min_qty: float
    min_qty_source: str
    min_notional_krw: float
    min_notional_source: str
    max_qty_decimals: int
    max_qty_decimals_source: str
    exchange_qty_step: float | None
    exchange_qty_step_source: str
    configured_qty_step: float | None
    configured_qty_step_source: str
    qty_step_authority_level: QtyStepAuthorityLevel

    @classmethod
    def from_rule_resolution(cls, resolution, *, market: str | None = None) -> "ExchangeQuantityContract":
        rules = resolution.rules
        source = dict(getattr(resolution, "source", {}) or {})
        source_mode = str(getattr(resolution, "source_mode", "") or "")
        snapshot_persisted = bool(getattr(resolution, "snapshot_persisted", False))
        resolved_market = str(market or getattr(rules, "market_id", "") or "")
        qty_step_source = str(source.get("qty_step") or "unknown")
        authority = qty_step_authority_level_for_source(
            qty_step_source,
            source_mode=source_mode,
            snapshot_persisted=snapshot_persisted,
        )
        qty_step = float(getattr(rules, "qty_step", 0.0) or 0.0)
        exchange_qty_step = qty_step if authority in {"exchange_hard", "persisted_exchange_snapshot"} else None
        configured_qty_step = qty_step if authority in {"local_fallback", "operator_policy", "unknown"} else None
        return cls(
            market=resolved_market,
            min_qty=float(getattr(rules, "min_qty", 0.0) or 0.0),
            min_qty_source=str(source.get("min_qty") or "unknown"),
            min_notional_krw=float(getattr(rules, "min_notional_krw", 0.0) or 0.0),
            min_notional_source=str(source.get("min_notional_krw") or "unknown"),
            max_qty_decimals=int(getattr(rules, "max_qty_decimals", 0) or 0),
            max_qty_decimals_source=str(source.get("max_qty_decimals") or "unknown"),
            exchange_qty_step=exchange_qty_step,
            exchange_qty_step_source=qty_step_source if exchange_qty_step is not None else "missing",
            configured_qty_step=configured_qty_step,
            configured_qty_step_source=qty_step_source if configured_qty_step is not None else "missing",
            qty_step_authority_level=authority,
        )

    @classmethod
    def local_fallback(
        cls,
        *,
        market: str,
        min_qty: float,
        min_notional_krw: float,
        max_qty_decimals: int,
        configured_qty_step: float,
    ) -> "ExchangeQuantityContract":
        return cls(
            market=market,
            min_qty=float(min_qty),
            min_qty_source="local_fallback",
            min_notional_krw=float(min_notional_krw),
            min_notional_source="local_fallback",
            max_qty_decimals=int(max_qty_decimals),
            max_qty_decimals_source="local_fallback",
            exchange_qty_step=None,
            exchange_qty_step_source="missing",
            configured_qty_step=float(configured_qty_step),
            configured_qty_step_source="local_fallback",
            qty_step_authority_level="local_fallback",
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "min_qty": self.min_qty,
            "min_qty_source": self.min_qty_source,
            "min_notional_krw": self.min_notional_krw,
            "min_notional_source": self.min_notional_source,
            "max_qty_decimals": self.max_qty_decimals,
            "max_qty_decimals_source": self.max_qty_decimals_source,
            "exchange_qty_step": self.exchange_qty_step,
            "exchange_qty_step_source": self.exchange_qty_step_source,
            "configured_qty_step": self.configured_qty_step,
            "configured_qty_step_source": self.configured_qty_step_source,
            "qty_step_source": self.exchange_qty_step_source
            if self.exchange_qty_step is not None
            else self.configured_qty_step_source,
            "qty_step_authority_level": self.qty_step_authority_level,
            "quantity_rule_source_mode": self.quantity_rule_source_mode,
            "quantity_contract_complete": self.quantity_contract_complete,
            "quantity_contract_recommended_action": self.quantity_contract_recommended_action,
        }

    @property
    def quantity_rule_source_mode(self) -> str:
        sources = {
            self.min_qty_source,
            self.min_notional_source,
            self.max_qty_decimals_source,
            self.exchange_qty_step_source,
            self.configured_qty_step_source,
        }
        if "unknown" in sources:
            return "unknown"
        if "local_fallback" in sources:
            return "local_fallback"
        if self.qty_step_authority_level == "persisted_exchange_snapshot":
            return "persisted_exchange_snapshot"
        return "exchange"

    @property
    def quantity_contract_complete(self) -> bool:
        return (
            self.min_qty > 0
            and self.min_notional_krw > 0
            and self.max_qty_decimals > 0
            and self.qty_step_authority_level != "unknown"
        )

    @property
    def quantity_contract_recommended_action(self) -> str | None:
        if self.qty_step_authority_level == "unknown":
            return "refresh_order_rules_or_review_quantity_settings"
        if not self.quantity_contract_complete:
            return "refresh_order_rules_or_review_quantity_settings"
        if self.qty_step_authority_level == "local_fallback":
            return "review_local_quantity_fallback_before_real_submit"
        return None


def qty_step_authority_level_for_source(
    source: str,
    *,
    source_mode: str = "",
    snapshot_persisted: bool = False,
) -> QtyStepAuthorityLevel:
    normalized = str(source or "unknown")
    if snapshot_persisted and normalized in _EXCHANGE_HARD_SOURCES:
        return "persisted_exchange_snapshot"
    if normalized in _EXCHANGE_HARD_SOURCES:
        return "exchange_hard"
    if normalized in _LOCAL_FALLBACK_SOURCES or normalized.startswith("settings."):
        return "local_fallback"
    if normalized == "operator_policy":
        return "operator_policy"
    if source_mode == "local_fallback":
        return "local_fallback"
    return "unknown"


def should_enforce_qty_step_as_hard_rule(
    contract: ExchangeQuantityContract,
    *,
    command_intent: str,
) -> bool:
    if contract.qty_step_authority_level == "exchange_hard":
        return True
    if contract.qty_step_authority_level == "persisted_exchange_snapshot":
        return command_intent != "operator_clean_account_closeout"
    return False
