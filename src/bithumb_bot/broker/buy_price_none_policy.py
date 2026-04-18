from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace

from .base import BrokerRejectError

BUY_PRICE_NONE_ALIAS_POLICY = "market_to_price_alias_disabled"


def _normalize_chance_side(side: str) -> str:
    token = str(side or "").strip().upper()
    if token in {"BUY", "BID"}:
        return "bid"
    if token in {"SELL", "ASK"}:
        return "ask"
    raise BrokerRejectError(f"unsupported order side: {side}")


def _normalize_chance_order_type(order_type: str) -> str:
    token = str(order_type or "").strip().lower()
    if token in {"limit", "price", "market"}:
        return token
    raise BrokerRejectError(f"unsupported order_type: {order_type}")


def raw_supported_order_types_for_chance_validation(*, side: str, rules) -> tuple[str, ...]:
    normalized_side = _normalize_chance_side(side)
    side_specific_attr = "bid_types" if normalized_side == "bid" else "ask_types"
    raw_supported_types = getattr(rules, side_specific_attr, ()) or getattr(rules, "order_types", ()) or ()
    supported_types = {str(item).strip().lower() for item in raw_supported_types if str(item).strip()}
    return tuple(sorted(supported_types))


def supported_order_types_for_chance_validation(*, side: str, rules) -> tuple[str, ...]:
    return raw_supported_order_types_for_chance_validation(side=side, rules=rules)


def buy_price_none_alias_policy() -> str:
    return BUY_PRICE_NONE_ALIAS_POLICY


@dataclass(frozen=True)
class BuyPriceNoneResolution:
    allowed: bool
    resolved_order_type: str
    decision_basis: str
    alias_used: bool
    alias_policy: str
    block_reason: str
    raw_supported_types: tuple[str, ...]
    support_source: str


@dataclass(frozen=True)
class BuyPriceNoneSubmitContract:
    resolution: BuyPriceNoneResolution
    chance_validation_order_type: str
    chance_supported_order_types: tuple[str, ...]
    exchange_submit_field: str
    exchange_order_type: str
    exchange_submit_notional_krw: float | None = None
    exchange_submit_qty: float | None = None
    internal_executable_qty: float | None = None

    @property
    def contract_id(self) -> str:
        payload = {
            "chance_validation_order_type": self.chance_validation_order_type,
            "chance_supported_order_types": self.chance_supported_order_types,
            "exchange_submit_field": self.exchange_submit_field,
            "exchange_order_type": self.exchange_order_type,
            "allowed": self.resolution.allowed,
            "decision_basis": self.resolution.decision_basis,
            "alias_used": self.resolution.alias_used,
            "alias_policy": self.resolution.alias_policy,
            "block_reason": self.resolution.block_reason,
            "raw_supported_types": self.resolution.raw_supported_types,
            "support_source": self.resolution.support_source,
            "resolved_order_type": self.resolution.resolved_order_type,
        }
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:12]

    @property
    def resolved_contract(self) -> str:
        return (
            f"validation_order_type={self.chance_validation_order_type} "
            f"exchange_order_type={self.exchange_order_type} "
            f"submit_field={self.exchange_submit_field}"
        )

    def with_execution_fields(
        self,
        *,
        exchange_submit_notional_krw: float | None = None,
        exchange_submit_qty: float | None = None,
        internal_executable_qty: float | None = None,
    ) -> "BuyPriceNoneSubmitContract":
        return replace(
            self,
            exchange_submit_notional_krw=exchange_submit_notional_krw,
            exchange_submit_qty=exchange_submit_qty,
            internal_executable_qty=internal_executable_qty,
        )

    def as_context(self) -> dict[str, object]:
        decision_outcome = "pass" if self.resolution.allowed else "block"
        return {
            "chance_validation_order_type": self.chance_validation_order_type,
            "chance_supported_order_types": list(self.chance_supported_order_types),
            "buy_price_none_allowed": bool(self.resolution.allowed),
            "buy_price_none_decision_outcome": decision_outcome,
            "buy_price_none_decision_basis": self.resolution.decision_basis,
            "buy_price_none_alias_used": bool(self.resolution.alias_used),
            "buy_price_none_alias_policy": self.resolution.alias_policy,
            "buy_price_none_block_reason": self.resolution.block_reason,
            "buy_price_none_support_source": self.resolution.support_source,
            "buy_price_none_raw_supported_types": list(self.resolution.raw_supported_types),
            "buy_price_none_resolved_order_type": self.resolution.resolved_order_type,
            "buy_price_none_resolved_contract": self.resolved_contract,
            "buy_price_none_contract_id": self.contract_id,
            "allowed": bool(self.resolution.allowed),
            "decision_outcome": decision_outcome,
            "decision_basis": self.resolution.decision_basis,
            "alias_used": bool(self.resolution.alias_used),
            "alias_policy": self.resolution.alias_policy,
            "block_reason": self.resolution.block_reason,
            "support_source": self.resolution.support_source,
            "raw_buy_supported_types": list(self.resolution.raw_supported_types),
            "resolved_order_type": self.resolution.resolved_order_type,
            "resolved_contract": self.resolved_contract,
            "contract_id": self.contract_id,
            "submit_field": self.exchange_submit_field,
            "exchange_submit_field": self.exchange_submit_field,
            "exchange_order_type": self.exchange_order_type,
            "exchange_submit_notional_krw": self.exchange_submit_notional_krw,
            "exchange_submit_qty": self.exchange_submit_qty,
            "internal_executable_qty": self.internal_executable_qty,
        }


@dataclass(frozen=True)
class BuyPriceNoneSubmitPolicy:
    chance_validation_order_type: str
    chance_supported_order_types: tuple[str, ...]
    exchange_submit_field: str
    exchange_order_type: str


def resolve_buy_price_none_resolution(*, rules) -> BuyPriceNoneResolution:
    raw_supported_types = raw_supported_order_types_for_chance_validation(side="BUY", rules=rules)
    support_source = "bid_types" if getattr(rules, "bid_types", ()) else "order_types"
    alias_policy = buy_price_none_alias_policy()
    if "price" in raw_supported_types:
        return BuyPriceNoneResolution(
            allowed=True,
            resolved_order_type="price",
            decision_basis="raw",
            alias_used=False,
            alias_policy=alias_policy,
            block_reason="",
            raw_supported_types=raw_supported_types,
            support_source=support_source,
        )
    if "market" in raw_supported_types:
        return BuyPriceNoneResolution(
            allowed=False,
            resolved_order_type="price",
            decision_basis="raw",
            alias_used=False,
            alias_policy=alias_policy,
            block_reason="buy_price_none_requires_explicit_price_support",
            raw_supported_types=raw_supported_types,
            support_source=support_source,
        )
    return BuyPriceNoneResolution(
        allowed=False,
        resolved_order_type="price",
        decision_basis="raw",
        alias_used=False,
        alias_policy=alias_policy,
        block_reason="buy_price_none_unsupported",
        raw_supported_types=raw_supported_types,
        support_source=support_source,
    )


def resolve_buy_price_none_submit_policy(
    *,
    rules,
    resolution: BuyPriceNoneResolution | None = None,
) -> BuyPriceNoneSubmitPolicy:
    buy_resolution = resolution or resolve_buy_price_none_resolution(rules=rules)
    return BuyPriceNoneSubmitPolicy(
        chance_validation_order_type=buy_resolution.resolved_order_type,
        chance_supported_order_types=supported_order_types_for_chance_validation(side="BUY", rules=rules),
        exchange_submit_field="price",
        exchange_order_type=buy_resolution.resolved_order_type,
    )


def build_buy_price_none_submit_contract(
    *,
    rules,
    resolution: BuyPriceNoneResolution | None = None,
) -> BuyPriceNoneSubmitContract:
    buy_resolution = resolution or resolve_buy_price_none_resolution(rules=rules)
    submit_policy = resolve_buy_price_none_submit_policy(
        rules=rules,
        resolution=buy_resolution,
    )
    return BuyPriceNoneSubmitContract(
        resolution=buy_resolution,
        chance_validation_order_type=submit_policy.chance_validation_order_type,
        chance_supported_order_types=submit_policy.chance_supported_order_types,
        exchange_submit_field=submit_policy.exchange_submit_field,
        exchange_order_type=submit_policy.exchange_order_type,
    )


def serialize_buy_price_none_submit_contract(
    contract: BuyPriceNoneSubmitContract,
    *,
    market: str | None = None,
    order_side: str | None = None,
) -> dict[str, object]:
    context = contract.as_context()
    if market is not None:
        context["market"] = market
    if order_side is not None:
        context["order_side"] = order_side
    return context


def build_buy_price_none_submit_contract_context(
    *,
    rules,
    resolution: BuyPriceNoneResolution | None = None,
) -> dict[str, object]:
    return serialize_buy_price_none_submit_contract(
        build_buy_price_none_submit_contract(
            rules=rules,
            resolution=resolution,
        )
    )


def validate_buy_price_none_submit_contract(*, submit_contract: BuyPriceNoneSubmitContract) -> None:
    if submit_contract.resolution.allowed:
        return
    raise BrokerRejectError(
        "/v1/orders/chance rejected BUY price=None before submit: "
        f"reason={submit_contract.resolution.block_reason} "
        f"support_source={submit_contract.resolution.support_source} "
        f"raw_supported_types={sorted(set(submit_contract.resolution.raw_supported_types))}"
    )


def validate_order_chance_support(
    *,
    rules,
    side: str,
    order_type: str,
    buy_price_none_resolution: BuyPriceNoneResolution | None = None,
) -> None:
    normalized_side = _normalize_chance_side(side)
    normalized_order_type = _normalize_chance_order_type(order_type)

    supported_sides = tuple(
        str(item).strip().lower()
        for item in getattr(rules, "order_sides", ()) or ()
        if str(item).strip()
    )
    if supported_sides and normalized_side not in supported_sides:
        raise BrokerRejectError(
            "/v1/orders/chance rejected order side before submit: "
            f"side={str(side).strip().upper()} supported={sorted(set(supported_sides))}"
        )

    if normalized_side == "bid" and normalized_order_type == "price":
        buy_resolution = buy_price_none_resolution or resolve_buy_price_none_resolution(rules=rules)
        if buy_resolution.allowed:
            return
        raise BrokerRejectError(
            "/v1/orders/chance rejected BUY price=None before submit: "
            f"reason={buy_resolution.block_reason} "
            f"support_source={buy_resolution.support_source} "
            f"raw_supported_types={sorted(set(buy_resolution.raw_supported_types))}"
        )

    supported_types = supported_order_types_for_chance_validation(side=side, rules=rules)
    if supported_types and normalized_order_type not in supported_types:
        raise BrokerRejectError(
            "/v1/orders/chance rejected order type before submit: "
            f"order_type={normalized_order_type} supported={sorted(set(supported_types))}"
        )


def validate_buy_price_none_order_chance_contract(
    *,
    rules,
    submit_contract: BuyPriceNoneSubmitContract,
) -> None:
    normalized_order_type = _normalize_chance_order_type(
        submit_contract.chance_validation_order_type
    )
    if normalized_order_type != submit_contract.resolution.resolved_order_type:
        raise BrokerRejectError(
            "BUY price=None submit contract chance validation mismatch before submit: "
            f"chance_validation_order_type={submit_contract.chance_validation_order_type!r} "
            f"resolved_order_type={submit_contract.resolution.resolved_order_type!r}"
        )

    validate_order_chance_support(
        rules=rules,
        side="BUY",
        order_type=submit_contract.chance_validation_order_type,
        buy_price_none_resolution=submit_contract.resolution,
    )


def buy_price_none_submit_contract_mismatch(
    *,
    expected: dict[str, object] | None,
    actual: dict[str, object] | None,
) -> str | None:
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        return "submit contract missing"
    if dict(expected) == dict(actual):
        return None
    all_keys = sorted(set(expected) | set(actual))
    mismatches: list[str] = []
    for key in all_keys:
        expected_value = expected.get(key)
        actual_value = actual.get(key)
        if expected_value != actual_value:
            mismatches.append(f"{key}: expected={expected_value!r} actual={actual_value!r}")
    return "; ".join(mismatches) if mismatches else "submit contract mismatch"


def build_buy_price_none_diagnostic_fields(
    *,
    rules,
    resolution: BuyPriceNoneResolution | None = None,
    submit_contract: BuyPriceNoneSubmitContract | None = None,
) -> dict[str, object]:
    contract = submit_contract or build_buy_price_none_submit_contract(
        rules=rules,
        resolution=resolution,
    )
    submit_context = serialize_buy_price_none_submit_contract(contract)
    return {
        "raw_bid_types": [str(item) for item in getattr(rules, "bid_types", ()) or ()],
        "raw_order_types": [str(item) for item in getattr(rules, "order_types", ()) or ()],
        "raw_buy_supported_types": list(submit_context["raw_buy_supported_types"]),
        "support_source": submit_context["support_source"],
        "resolved_order_type": submit_context["resolved_order_type"],
        "resolved_contract": submit_context["resolved_contract"],
        "contract_id": submit_context["contract_id"],
        "submit_field": submit_context["submit_field"],
        "allowed": bool(submit_context["allowed"]),
        "decision_outcome": submit_context["decision_outcome"],
        "decision_basis": submit_context["decision_basis"],
        "alias_used": bool(submit_context["alias_used"]),
        "alias_policy": submit_context["alias_policy"],
        "block_reason": (str(submit_context["block_reason"]) or "-"),
    }
