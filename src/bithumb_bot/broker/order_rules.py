from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, field, replace
from typing import Any

from ..config import settings
from ..db_core import ensure_db, fetch_latest_order_rule_snapshot, record_order_rule_snapshot
from ..markets import ExchangeMarketCodeError, canonical_market_id, canonical_market_with_raw, parse_documented_market_code
from ..notifier import notify
from .base import BrokerRejectError
from .bithumb import BithumbBroker, classify_private_api_error

_CACHE_TTL_SEC = 300.0
_cached_rules: dict[str, tuple[float, "RuleResolution", "DerivedOrderConstraints"]] = {}
KNOWN_RULE_SOURCES = frozenset(
    {
        "chance_doc",
        "local_fallback",
        "merged",
        "unsupported_by_doc",
        "missing",
    }
)
TRACKED_CHANCE_CONTRACT_FIELDS = ("order_types", "bid_types", "ask_types", "order_sides")

# BUY price=None stays fail-closed by default: a raw "market" capability is
# not treated as a compatible alias for the required exchange "price" submit
# field unless a future compatibility mode is added explicitly and safely gated.
BUY_PRICE_NONE_ALIAS_POLICY = "market_to_price_alias_disabled"
BUY_PRICE_NONE_ALIAS_POLICY_COMPAT = "market_to_price_alias_enabled"


@dataclass(frozen=True)
class OrderChanceSide:
    price_unit: float | None
    min_total: float


@dataclass(frozen=True)
class OrderChanceResponse:
    market_id: str
    order_types: tuple[str, ...]
    bid_types: tuple[str, ...]
    ask_types: tuple[str, ...]
    order_sides: tuple[str, ...]
    bid: OrderChanceSide
    ask: OrderChanceSide
    bid_fee: float
    ask_fee: float
    maker_bid_fee: float
    maker_ask_fee: float


@dataclass(frozen=True)
class DerivedOrderConstraints:
    market_id: str = ""
    bid_min_total_krw: float = 0.0
    ask_min_total_krw: float = 0.0
    bid_price_unit: float = 0.0
    ask_price_unit: float = 0.0
    order_types: tuple[str, ...] = ()
    bid_types: tuple[str, ...] = ()
    ask_types: tuple[str, ...] = ()
    order_sides: tuple[str, ...] = ()
    bid_fee: float = 0.0
    ask_fee: float = 0.0
    maker_bid_fee: float = 0.0
    maker_ask_fee: float = 0.0
    min_qty: float = 0.0
    qty_step: float = 0.0
    min_notional_krw: float = 0.0
    max_qty_decimals: int = 0


@dataclass(frozen=True)
class ExchangeDerivedConstraints:
    market_id: str = ""
    bid_min_total_krw: float = 0.0
    ask_min_total_krw: float = 0.0
    bid_price_unit: float = 0.0
    ask_price_unit: float = 0.0
    order_types: tuple[str, ...] = ()
    bid_types: tuple[str, ...] = ()
    ask_types: tuple[str, ...] = ()
    order_sides: tuple[str, ...] = ()
    bid_fee: float = 0.0
    ask_fee: float = 0.0
    maker_bid_fee: float = 0.0
    maker_ask_fee: float = 0.0


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

# Raw chance capability stays in BuyPriceNoneResolution; this contract is the
# executable submit boundary that validation, payload routing, and diagnostics share.
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


@dataclass(frozen=True)
class LocalFallbackConstraints:
    min_qty: float = 0.0
    qty_step: float = 0.0
    min_notional_krw: float = 0.0
    max_qty_decimals: int = 0


@dataclass(frozen=True)
class RuleResolution:
    rules: DerivedOrderConstraints
    source: dict[str, str]
    exchange_source: dict[str, str] = field(default_factory=dict)
    local_fallback_source: dict[str, str] = field(default_factory=dict)
    fallback_used: bool = False
    fallback_reason_code: str = ""
    fallback_reason_summary: str = ""
    fallback_reason_detail: str = ""
    fallback_risk: str = ""
    retrieved_at_sec: float = 0.0
    expires_at_sec: float = 0.0
    stale: bool = False
    source_mode: str = "exchange"
    snapshot_persisted: bool = False
    chance_contract_change: "ChanceContractChange | None" = None

    def is_stale(self, *, now_sec: float | None = None) -> bool:
        if self.stale:
            return True
        if self.expires_at_sec <= 0:
            return False
        current = float(time.time() if now_sec is None else now_sec)
        return current >= float(self.expires_at_sec)


class OrderChanceSchemaError(RuntimeError):
    """Raised when /v1/orders/chance response violates documented schema."""


class OrderChanceMarketMismatchError(OrderChanceSchemaError):
    """Raised when /v1/orders/chance response market does not match request market."""


@dataclass(frozen=True)
class ChanceContractChange:
    detected: bool
    changed_fields: dict[str, dict[str, tuple[str, ...]]] = field(default_factory=dict)
    previous_snapshot: dict[str, tuple[str, ...]] = field(default_factory=dict)
    current_snapshot: dict[str, tuple[str, ...]] = field(default_factory=dict)
    previous_fetched_ts: int = 0


def side_min_total_krw(*, rules: DerivedOrderConstraints, side: str) -> float:
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY":
        if float(rules.bid_min_total_krw) > 0:
            return float(rules.bid_min_total_krw)
    elif normalized_side == "SELL":
        if float(rules.ask_min_total_krw) > 0:
            return float(rules.ask_min_total_krw)
    return float(rules.min_notional_krw)


def side_price_unit(*, rules: DerivedOrderConstraints, side: str) -> float:
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY":
        return float(rules.bid_price_unit)
    if normalized_side == "SELL":
        return float(rules.ask_price_unit)
    return 0.0


def normalize_limit_price_for_side(*, price: float, side: str, rules: DerivedOrderConstraints) -> float:
    value = float(price)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"limit price must be > 0 (got {price})")

    unit = side_price_unit(rules=rules, side=side)
    if not math.isfinite(unit) or unit <= 0:
        return value

    steps = value / unit
    epsilon = 1e-12
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "SELL":
        aligned_steps = math.ceil(steps - epsilon)
    else:
        aligned_steps = math.floor(steps + epsilon)
    return float(aligned_steps * unit)


def required_rule_issues(rules: DerivedOrderConstraints) -> list[str]:
    issues: list[str] = []
    if not math.isfinite(float(rules.min_qty)) or float(rules.min_qty) <= 0:
        issues.append(f"min_qty must be > 0 (got {rules.min_qty})")
    if not math.isfinite(float(rules.qty_step)) or float(rules.qty_step) <= 0:
        issues.append(f"qty_step must be > 0 (got {rules.qty_step})")
    if not math.isfinite(float(rules.min_notional_krw)) or float(rules.min_notional_krw) <= 0:
        issues.append(f"min_notional_krw must be > 0 (got {rules.min_notional_krw})")
    if int(rules.max_qty_decimals) <= 0:
        issues.append(f"max_qty_decimals must be > 0 (got {rules.max_qty_decimals})")
    return issues


def rule_source_for(field: str, source: dict[str, str] | None) -> str:
    if not source:
        return "missing"
    normalized = str(source.get(field, "")).strip() or "missing"
    if normalized == "manual_config":
        # legacy source label -> canonical source label
        normalized = "local_fallback"
    return normalized if normalized in KNOWN_RULE_SOURCES else "missing"


def required_rule_source_issues(
    source: dict[str, str] | None, *, require_price_unit_sources: bool = True
) -> list[str]:
    issues: list[str] = []
    doc_required_fields: tuple[str, ...] = (
        "bid_min_total_krw",
        "ask_min_total_krw",
    )
    if require_price_unit_sources:
        doc_required_fields += ("bid_price_unit", "ask_price_unit")
    for field in doc_required_fields:
        field_source = rule_source_for(field, source)
        if field_source != "chance_doc":
            issues.append(
                f"{field} source must be chance_doc for MODE=live "
                f"(got {field_source})"
            )
    return issues


def optional_rule_source_warnings(source: dict[str, str] | None) -> list[str]:
    warnings: list[str] = []
    for field in ("bid_price_unit", "ask_price_unit"):
        field_source = rule_source_for(field, source)
        if field_source != "chance_doc":
            warnings.append(
                f"{field} source is {field_source}; limit price tick normalization may be pass-through"
            )
    return warnings


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


def raw_supported_order_types_for_chance_validation(*, side: str, rules: DerivedOrderConstraints) -> tuple[str, ...]:
    normalized_side = _normalize_chance_side(side)
    side_specific_attr = "bid_types" if normalized_side == "bid" else "ask_types"
    raw_supported_types = getattr(rules, side_specific_attr, ()) or getattr(rules, "order_types", ()) or ()
    supported_types = {str(item).strip().lower() for item in raw_supported_types if str(item).strip()}
    return tuple(sorted(supported_types))


def buy_price_none_alias_policy() -> str:
    if bool(settings.BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED):
        return BUY_PRICE_NONE_ALIAS_POLICY_COMPAT
    return BUY_PRICE_NONE_ALIAS_POLICY


def resolve_buy_price_none_resolution(*, rules: DerivedOrderConstraints) -> BuyPriceNoneResolution:
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
        if bool(settings.BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED):
            return BuyPriceNoneResolution(
                allowed=True,
                resolved_order_type="price",
                decision_basis="alias_policy",
                alias_used=True,
                alias_policy=alias_policy,
                block_reason="",
                raw_supported_types=raw_supported_types,
                support_source=support_source,
            )
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


def build_buy_price_none_submit_contract_context(
    *,
    rules: DerivedOrderConstraints,
    resolution: BuyPriceNoneResolution | None = None,
) -> dict[str, object]:
    """Return the canonical BUY price=None submit-context evidence.

    BUY price=None permission is fail-closed and must come from one raw
    exchange-declared resolution. This helper keeps validation, payload
    routing, and operator-facing submit evidence on the same contract.
    """

    return serialize_buy_price_none_submit_contract(
        build_buy_price_none_submit_contract(
            rules=rules,
            resolution=resolution,
        )
    )


def build_buy_price_none_submit_contract(
    *,
    rules: DerivedOrderConstraints,
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


def resolve_buy_price_none_submit_policy(
    *,
    rules: DerivedOrderConstraints,
    resolution: BuyPriceNoneResolution | None = None,
) -> BuyPriceNoneSubmitPolicy:
    buy_resolution = resolution or resolve_buy_price_none_resolution(rules=rules)
    return BuyPriceNoneSubmitPolicy(
        chance_validation_order_type=buy_resolution.resolved_order_type,
        chance_supported_order_types=supported_order_types_for_chance_validation(side="BUY", rules=rules),
        exchange_submit_field="price",
        exchange_order_type=buy_resolution.resolved_order_type,
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


def validate_buy_price_none_submit_contract(*, submit_contract: BuyPriceNoneSubmitContract) -> None:
    if submit_contract.resolution.allowed:
        return
    raise BrokerRejectError(
        "/v1/orders/chance rejected BUY price=None before submit: "
        f"reason={submit_contract.resolution.block_reason} "
        f"support_source={submit_contract.resolution.support_source} "
        f"raw_supported_types={sorted(set(submit_contract.resolution.raw_supported_types))}"
    )


def validate_buy_price_none_order_chance_contract(
    *,
    rules: DerivedOrderConstraints,
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
    rules: DerivedOrderConstraints,
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


def supported_order_types_for_chance_validation(*, side: str, rules: DerivedOrderConstraints) -> tuple[str, ...]:
    return raw_supported_order_types_for_chance_validation(side=side, rules=rules)


def validate_order_chance_support(
    *,
    rules: DerivedOrderConstraints,
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


def _local_fallback_constraints() -> LocalFallbackConstraints:
    return LocalFallbackConstraints(
        min_qty=float(settings.LIVE_MIN_ORDER_QTY),
        qty_step=float(settings.LIVE_ORDER_QTY_STEP),
        min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
        max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
    )


def _local_fallback_rules() -> DerivedOrderConstraints:
    fallback = _local_fallback_constraints()
    return DerivedOrderConstraints(
        min_qty=fallback.min_qty,
        qty_step=fallback.qty_step,
        min_notional_krw=fallback.min_notional_krw,
        max_qty_decimals=fallback.max_qty_decimals,
    )


def _require_dict(payload: dict[str, Any], key: str, *, where: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be object")
    return value


def _require_non_empty_str(payload: dict[str, Any], key: str, *, where: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be non-empty string")
    return value.strip()


def _require_non_empty_list(payload: dict[str, Any], key: str, *, where: str) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list) or len(value) == 0:
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be non-empty array")
    return value


def _require_positive_number(payload: dict[str, Any], key: str, *, where: str) -> float:
    raw = payload.get(key)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be numeric") from None
    if not math.isfinite(value) or value <= 0:
        raise OrderChanceSchemaError(f"/v1/orders/chance {where}.{key} must be > 0")
    return value


def _optional_positive_number(payload: dict[str, Any], key: str, *, where: str) -> float | None:
    if key not in payload or payload.get(key) is None:
        return None
    return _require_positive_number(payload, key, where=where)


def parse_order_chance_response(payload: dict[str, Any], *, requested_market: str) -> OrderChanceResponse:
    fee_values = {
        fee_field: _require_positive_number(payload, fee_field, where="response")
        for fee_field in ("bid_fee", "ask_fee", "maker_bid_fee", "maker_ask_fee")
    }

    market = _require_dict(payload, "market", where="response")
    market_id = parse_documented_market_code(_require_non_empty_str(market, "id", where="response.market"))
    normalized_requested_market = parse_documented_market_code(requested_market)
    if market_id != normalized_requested_market:
        raise OrderChanceMarketMismatchError(
            "/v1/orders/chance response.market.id mismatch: "
            f"requested={normalized_requested_market} response={market_id}"
        )

    _require_non_empty_list(market, "order_types", where="response.market")
    _require_non_empty_list(market, "bid_types", where="response.market")
    _require_non_empty_list(market, "ask_types", where="response.market")
    _require_non_empty_list(market, "order_sides", where="response.market")

    bid = _require_dict(market, "bid", where="response.market")
    ask = _require_dict(market, "ask", where="response.market")

    return OrderChanceResponse(
        market_id=market_id,
        order_types=tuple(str(item) for item in _require_non_empty_list(market, "order_types", where="response.market")),
        bid_types=tuple(str(item) for item in _require_non_empty_list(market, "bid_types", where="response.market")),
        ask_types=tuple(str(item) for item in _require_non_empty_list(market, "ask_types", where="response.market")),
        order_sides=tuple(str(item) for item in _require_non_empty_list(market, "order_sides", where="response.market")),
        bid=OrderChanceSide(
            price_unit=_optional_positive_number(bid, "price_unit", where="response.market.bid"),
            min_total=_require_positive_number(bid, "min_total", where="response.market.bid"),
        ),
        ask=OrderChanceSide(
            price_unit=_optional_positive_number(ask, "price_unit", where="response.market.ask"),
            min_total=_require_positive_number(ask, "min_total", where="response.market.ask"),
        ),
        bid_fee=fee_values["bid_fee"],
        ask_fee=fee_values["ask_fee"],
        maker_bid_fee=fee_values["maker_bid_fee"],
        maker_ask_fee=fee_values["maker_ask_fee"],
    )


def _exchange_rule_source_map(exchange: ExchangeDerivedConstraints) -> dict[str, str]:
    return {
        "market_id": "chance_doc",
        "bid_min_total_krw": "chance_doc",
        "ask_min_total_krw": "chance_doc",
        "bid_price_unit": "chance_doc" if exchange.bid_price_unit > 0 else "missing",
        "ask_price_unit": "chance_doc" if exchange.ask_price_unit > 0 else "missing",
        "order_types": "chance_doc",
        "bid_types": "chance_doc",
        "ask_types": "chance_doc",
        "order_sides": "chance_doc",
        "bid_fee": "chance_doc",
        "ask_fee": "chance_doc",
        "maker_bid_fee": "chance_doc",
        "maker_ask_fee": "chance_doc",
    }


def _fallback_rule_source_map() -> dict[str, str]:
    return {
        "min_qty": "local_fallback",
        "qty_step": "local_fallback",
        "min_notional_krw": "local_fallback",
        "max_qty_decimals": "local_fallback",
    }


def derive_order_rules_from_chance(response: OrderChanceResponse) -> ExchangeDerivedConstraints:
    return ExchangeDerivedConstraints(
        market_id=response.market_id,
        bid_min_total_krw=response.bid.min_total,
        ask_min_total_krw=response.ask.min_total,
        bid_price_unit=float(response.bid.price_unit or 0.0),
        ask_price_unit=float(response.ask.price_unit or 0.0),
        order_types=response.order_types,
        bid_types=response.bid_types,
        ask_types=response.ask_types,
        order_sides=response.order_sides,
        bid_fee=response.bid_fee,
        ask_fee=response.ask_fee,
        maker_bid_fee=response.maker_bid_fee,
        maker_ask_fee=response.maker_ask_fee,
    )


def fetch_exchange_order_rules(pair: str) -> ExchangeDerivedConstraints:
    try:
        market = parse_documented_market_code(pair)
    except ExchangeMarketCodeError as exc:
        raise OrderChanceSchemaError(
            f"/v1/orders/chance request market must be canonical QUOTE-BASE: {pair!r}"
        ) from exc
    payload = BithumbBroker().get_order_chance(market=market)

    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected order rules payload type: {type(payload).__name__}")

    chance = parse_order_chance_response(payload, requested_market=market)
    return derive_order_rules_from_chance(chance)


def _build_fallback_only_rule_resolution(
    *,
    pair: str,
    now: float,
    fallback: DerivedOrderConstraints,
    reason_code: str,
    reason_summary: str,
    reason_detail: str,
    fallback_risk: str,
) -> RuleResolution:
    source = {
        **_fallback_rule_source_map(),
        "market_id": "unsupported_by_doc",
        "bid_min_total_krw": "unsupported_by_doc",
        "ask_min_total_krw": "unsupported_by_doc",
        "bid_price_unit": "unsupported_by_doc",
        "ask_price_unit": "unsupported_by_doc",
        "order_types": "unsupported_by_doc",
        "bid_types": "unsupported_by_doc",
        "ask_types": "unsupported_by_doc",
        "order_sides": "unsupported_by_doc",
        "bid_fee": "unsupported_by_doc",
        "ask_fee": "unsupported_by_doc",
        "maker_bid_fee": "unsupported_by_doc",
        "maker_ask_fee": "unsupported_by_doc",
        "ruleset": "merged",
        "exchange_source_json": json.dumps({}, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        "local_fallback_source_json": json.dumps(
            _fallback_rule_source_map(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    resolution = RuleResolution(
        rules=fallback,
        source=source,
        exchange_source={},
        local_fallback_source=_fallback_rule_source_map(),
        fallback_used=True,
        fallback_reason_code=reason_code,
        fallback_reason_summary=reason_summary,
        fallback_reason_detail=reason_detail,
        fallback_risk=fallback_risk,
        retrieved_at_sec=now,
        expires_at_sec=now + _CACHE_TTL_SEC,
        stale=False,
        source_mode="local_fallback",
    )
    if settings.MODE == "live":
        import logging

        logging.getLogger(__name__).warning(
            "live order rule snapshot fallback engaged pair=%s retrieved_at_sec=%.3f expires_at_sec=%.3f reason_code=%s reason=%s source_min_qty=%s source_qty_step=%s source_min_notional=%s source_max_qty_decimals=%s",
            pair,
            resolution.retrieved_at_sec,
            resolution.expires_at_sec,
            reason_code,
            reason_summary,
            resolution.source.get("min_qty", "missing"),
            resolution.source.get("qty_step", "missing"),
            resolution.source.get("min_notional_krw", "missing"),
            resolution.source.get("max_qty_decimals", "missing"),
        )
    return resolution


def _build_merged_rule_resolution(
    *,
    pair: str,
    now: float,
    exchange: ExchangeDerivedConstraints,
    fallback: DerivedOrderConstraints,
) -> RuleResolution:
    merged = DerivedOrderConstraints(
        market_id=exchange.market_id,
        bid_min_total_krw=exchange.bid_min_total_krw,
        ask_min_total_krw=exchange.ask_min_total_krw,
        bid_price_unit=exchange.bid_price_unit,
        ask_price_unit=exchange.ask_price_unit,
        order_types=exchange.order_types,
        bid_types=exchange.bid_types,
        ask_types=exchange.ask_types,
        order_sides=exchange.order_sides,
        bid_fee=exchange.bid_fee,
        ask_fee=exchange.ask_fee,
        maker_bid_fee=exchange.maker_bid_fee,
        maker_ask_fee=exchange.maker_ask_fee,
        min_qty=fallback.min_qty,
        qty_step=fallback.qty_step,
        min_notional_krw=fallback.min_notional_krw,
        max_qty_decimals=fallback.max_qty_decimals,
    )
    exchange_source = _exchange_rule_source_map(exchange)
    local_fallback_source = _fallback_rule_source_map()
    source = {
        **exchange_source,
        **local_fallback_source,
        "ruleset": "merged",
        "exchange_source_json": json.dumps(
            exchange_source,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "local_fallback_source_json": json.dumps(
            local_fallback_source,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    resolution = RuleResolution(
        rules=merged,
        source=source,
        exchange_source=exchange_source,
        local_fallback_source=local_fallback_source,
        retrieved_at_sec=now,
        expires_at_sec=now + _CACHE_TTL_SEC,
        stale=False,
        source_mode="merged",
    )
    if settings.MODE == "live":
        import logging

        logging.getLogger(__name__).info(
            "live order rule snapshot refreshed pair=%s retrieved_at_sec=%.3f expires_at_sec=%.3f source=exchange source_min_qty=%s source_qty_step=%s source_min_notional=%s source_max_qty_decimals=%s",
            pair,
            resolution.retrieved_at_sec,
            resolution.expires_at_sec,
            resolution.source.get("min_qty", "missing"),
            resolution.source.get("qty_step", "missing"),
            resolution.source.get("min_notional_krw", "missing"),
            resolution.source.get("max_qty_decimals", "missing"),
        )
    return resolution


def get_effective_order_rules(pair: str) -> RuleResolution:
    normalized_pair, _raw_pair = canonical_market_with_raw(pair)
    now = time.time()
    fallback = _local_fallback_rules()

    cached = _cached_rules.get(normalized_pair)
    if cached and now - cached[0] < _CACHE_TTL_SEC and cached[2] == fallback:
        cached_resolution = cached[1]
        if (
            cached_resolution.fallback_used
            and settings.MODE == "live"
            and not bool(settings.LIVE_DRY_RUN)
            and not bool(settings.LIVE_ALLOW_ORDER_RULE_FALLBACK)
        ):
            raise BrokerRejectError(
                f"live order rule snapshot unavailable for {pair}; cached fallback is disabled"
            )
        return cached_resolution
    try:
        exchange = fetch_exchange_order_rules(normalized_pair)
    except Exception as exc:
        fallback_issues = required_rule_issues(fallback)
        code, summary = classify_private_api_error(exc)
        detail = f"{type(exc).__name__}: {exc}"
        fallback_risk = (
            "order-rule auto-sync unavailable; side minimum totals, fees, and tick-size normalization "
            "may stay on local fallback until /v1/orders/chance succeeds again"
        )
        if fallback_issues and settings.MODE == "live" and not bool(settings.LIVE_DRY_RUN):
            raise BrokerRejectError(
                f"live order rule fallback invalid for {pair}: " + "; ".join(fallback_issues)
            ) from exc
        if settings.MODE == "live" and not bool(settings.LIVE_DRY_RUN) and not bool(settings.LIVE_ALLOW_ORDER_RULE_FALLBACK):
            raise BrokerRejectError(
                f"live order rule snapshot unavailable for {pair}; fallback disabled "
                f"(reason_code={code}; reason={summary}; detail={detail})"
            ) from exc
        notify(
            f"[WARN] order rules auto-sync failed for {pair}; using local fallback only "
            f"(reason_code={code}; reason={summary}; detail={detail}; risk={fallback_risk})"
        )
        resolution = _build_fallback_only_rule_resolution(
            pair=pair,
            now=now,
            fallback=fallback,
            reason_code=code,
            reason_summary=summary,
            reason_detail=detail,
            fallback_risk=fallback_risk,
        )
        resolution = _persist_rule_snapshot_if_possible(resolution)
        _cached_rules[normalized_pair] = (now, resolution, fallback)
        return resolution

    resolution = _build_merged_rule_resolution(
        pair=pair,
        now=now,
        exchange=exchange,
        fallback=fallback,
    )
    resolution = _persist_rule_snapshot_if_possible(resolution)
    _cached_rules[normalized_pair] = (now, resolution, fallback)
    return resolution


def get_cached_order_rule_snapshot(pair: str) -> RuleResolution | None:
    cached = _cached_rules.get(pair)
    if not cached:
        return None
    return cached[1]


def _coerce_tracked_contract_tokens(value: object) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    if value is None:
        return ()
    return (str(value),)


def _tracked_chance_contract_snapshot_from_payload(payload: dict[str, object] | None) -> dict[str, tuple[str, ...]]:
    source = payload or {}
    return {
        field: _coerce_tracked_contract_tokens(source.get(field))
        for field in TRACKED_CHANCE_CONTRACT_FIELDS
    }


def _tracked_chance_contract_snapshot_from_rules(rules: DerivedOrderConstraints) -> dict[str, tuple[str, ...]]:
    return _tracked_chance_contract_snapshot_from_payload(
        {
            field: getattr(rules, field, ())
            for field in TRACKED_CHANCE_CONTRACT_FIELDS
        }
    )


def _detect_chance_contract_change(
    *,
    previous_rules_payload: dict[str, object] | None,
    current_rules_payload: dict[str, object],
    previous_fetched_ts: int = 0,
) -> ChanceContractChange | None:
    if previous_rules_payload is None:
        return None
    previous_snapshot = _tracked_chance_contract_snapshot_from_payload(previous_rules_payload)
    current_snapshot = _tracked_chance_contract_snapshot_from_payload(current_rules_payload)
    changed_fields = {
        field: {
            "previous": previous_snapshot[field],
            "current": current_snapshot[field],
        }
        for field in TRACKED_CHANCE_CONTRACT_FIELDS
        if previous_snapshot[field] != current_snapshot[field]
    }
    return ChanceContractChange(
        detected=bool(changed_fields),
        changed_fields=changed_fields,
        previous_snapshot=previous_snapshot,
        current_snapshot=current_snapshot,
        previous_fetched_ts=int(previous_fetched_ts or 0),
    )


def _persist_rule_snapshot_if_possible(resolution: RuleResolution) -> RuleResolution:
    fallback_market, _raw_market = canonical_market_with_raw(settings.PAIR)
    market = str(getattr(resolution.rules, "market_id", "") or parse_documented_market_code(fallback_market))
    rules_payload = {
        field: getattr(resolution.rules, field)
        for field in resolution.rules.__dataclass_fields__.keys()
    }
    source_payload = dict(resolution.source)
    source_payload["source_mode"] = str(resolution.source_mode)
    source_payload["exchange_source_json"] = json.dumps(
        resolution.exchange_source,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    source_payload["local_fallback_source_json"] = json.dumps(
        resolution.local_fallback_source,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    conn = None
    try:
        conn = ensure_db()
        previous_snapshot_record = fetch_latest_order_rule_snapshot(conn, market=market)
        previous_rules_payload = None
        if previous_snapshot_record is not None:
            try:
                previous_rules_payload = json.loads(previous_snapshot_record.rules_json)
            except Exception:
                previous_rules_payload = None
        chance_contract_change = _detect_chance_contract_change(
            previous_rules_payload=previous_rules_payload,
            current_rules_payload=rules_payload,
            previous_fetched_ts=(
                previous_snapshot_record.fetched_ts
                if previous_snapshot_record is not None
                else 0
            ),
        )
        record_order_rule_snapshot(
            conn,
            market=market,
            fetched_ts=int(max(0.0, float(resolution.retrieved_at_sec)) * 1000),
            source_mode=str(resolution.source_mode),
            fallback_used=bool(resolution.fallback_used),
            fallback_reason_code=str(resolution.fallback_reason_code or ""),
            fallback_reason_summary=str(resolution.fallback_reason_summary or ""),
            rules_payload=rules_payload,
            source_payload=source_payload,
        )
        conn.commit()
        return RuleResolution(
            rules=resolution.rules,
            source=resolution.source,
            exchange_source=resolution.exchange_source,
            local_fallback_source=resolution.local_fallback_source,
            fallback_used=resolution.fallback_used,
            fallback_reason_code=resolution.fallback_reason_code,
            fallback_reason_summary=resolution.fallback_reason_summary,
            fallback_reason_detail=resolution.fallback_reason_detail,
            fallback_risk=resolution.fallback_risk,
            retrieved_at_sec=resolution.retrieved_at_sec,
            expires_at_sec=resolution.expires_at_sec,
            stale=resolution.stale,
            source_mode=resolution.source_mode,
            snapshot_persisted=True,
            chance_contract_change=chance_contract_change,
        )
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return resolution
    finally:
        if conn is not None:
            conn.close()

OrderRules = DerivedOrderConstraints
