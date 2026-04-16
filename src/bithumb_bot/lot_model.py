from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Any

from .config import settings

_DECIMAL_ZERO = Decimal("0")
DUST_POSITION_EPS = 1e-12
# Fallback lot modeling floor for dust/executable-lot normalization only.
# It is not the BUY entry permission gate: BUY allow/deny is decided later
# from exchange-constrained executable quantity plus minimum-constraint checks.
STATIC_FALLBACK_INTERNAL_LOT_SIZE = 0.0004


def _decimal_from_number(value: object) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid numeric value: {value}") from exc
    if not parsed.is_finite():
        raise ValueError(f"invalid non-finite numeric value: {value}")
    return parsed


def _decimal_quantizer(*, places: int) -> Decimal | None:
    normalized_places = max(0, int(places))
    if normalized_places <= 0:
        return None
    return Decimal("1").scaleb(-normalized_places)


def _rule_number(rules: Any, name: str, default: float = 0.0) -> float:
    raw = getattr(rules, name, default)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return float(default)
    if not value == value or value in (float("inf"), float("-inf")):
        return float(default)
    return value


def _rule_int(rules: Any, name: str, default: int = 0) -> int:
    raw = getattr(rules, name, default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


def _rule_tuple(rules: Any, name: str) -> tuple[str, ...]:
    raw = getattr(rules, name, ())
    if raw in (None, ""):
        return ()
    if isinstance(raw, (list, tuple)):
        return tuple(str(value) for value in raw)
    return (str(raw),)


def _floor_qty_to_step(*, qty: float, qty_step: float, max_qty_decimals: int) -> float:
    normalized = max(_DECIMAL_ZERO, _decimal_from_number(qty))
    step = max(_DECIMAL_ZERO, _decimal_from_number(qty_step))
    if step > 0:
        normalized = (normalized / step).to_integral_value(rounding=ROUND_FLOOR) * step
    quantizer = _decimal_quantizer(places=int(max_qty_decimals))
    if quantizer is not None:
        normalized = normalized.quantize(quantizer, rounding=ROUND_FLOOR)
    return max(0.0, float(normalized))


def _ceil_qty_to_step(*, qty: float, qty_step: float, max_qty_decimals: int) -> float:
    normalized = max(_DECIMAL_ZERO, _decimal_from_number(qty))
    step = max(_DECIMAL_ZERO, _decimal_from_number(qty_step))
    if step > 0:
        normalized = (normalized / step).to_integral_value(rounding=ROUND_CEILING) * step
    quantizer = _decimal_quantizer(places=int(max_qty_decimals))
    if quantizer is not None:
        normalized = normalized.quantize(quantizer, rounding=ROUND_CEILING)
    return max(0.0, float(normalized))


def derive_internal_lot_size(
    *,
    market_price: float | None,
    min_qty: float,
    qty_step: float,
    min_notional_krw: float,
    max_qty_decimals: int,
    exit_fee_ratio: float = 0.0,
    exit_slippage_bps: float = 0.0,
    exit_buffer_ratio: float = 0.0,
) -> float:
    """Derive the internal lot size used for lot-native modeling.

    This helper preserves a stable internal lot size for executable/dust
    interpretation even when exchange rules are sparse. It does not decide BUY
    permission. BUY allow/deny remains an execution-sizing outcome based on the
    exchange-constrained executable quantity and minimum constraints.
    """
    from .dust import build_executable_lot

    probe = build_executable_lot(
        qty=max(
            0.0,
            float(min_qty) if float(min_qty) > 0 else float(qty_step) if float(qty_step) > 0 else 0.0,
        ),
        market_price=float(market_price) if market_price is not None else None,
        min_qty=float(min_qty),
        qty_step=float(qty_step),
        min_notional_krw=float(min_notional_krw),
        max_qty_decimals=int(max_qty_decimals),
        exit_fee_ratio=float(exit_fee_ratio),
        exit_slippage_bps=float(exit_slippage_bps),
        exit_buffer_ratio=float(exit_buffer_ratio),
    )
    lot_size = max(
        float(probe.effective_min_trade_qty),
        float(probe.qty_step),
        float(min_qty),
    )
    if lot_size <= DUST_POSITION_EPS:
        # Preserve a non-zero modeling lot size when exchange inputs are too
        # sparse to derive one. This fallback does not authorize BUY entry;
        # the BUY gate is enforced later from executable_qty and exchange mins.
        lot_size = max(float(settings.LIVE_MIN_ORDER_QTY or 0.0), STATIC_FALLBACK_INTERNAL_LOT_SIZE)
    elif float(qty_step) > DUST_POSITION_EPS and lot_size <= float(qty_step) * 10.0:
        # Keep a one-step buffer so newly opened exposure stays safely executable
        # after exchange-side fees and rounding.
        lot_size += float(qty_step)
    return _ceil_qty_to_step(qty=lot_size, qty_step=float(qty_step), max_qty_decimals=int(max_qty_decimals))


def quantize_to_lot_count(*, qty: float, lot_size: float, rounding=ROUND_FLOOR) -> int:
    normalized_qty = max(0.0, _decimal_from_number(qty))
    normalized_lot_size = max(0.0, _decimal_from_number(lot_size))
    if normalized_qty <= _DECIMAL_ZERO or normalized_lot_size <= _DECIMAL_ZERO:
        return 0
    if rounding not in {ROUND_FLOOR, ROUND_CEILING}:
        raise ValueError(f"unsupported rounding mode for lot count: {rounding}")
    lot_ratio = normalized_qty / normalized_lot_size
    epsilon = Decimal("1e-9")
    if rounding == ROUND_FLOOR:
        lot_ratio += epsilon
    else:
        lot_ratio -= epsilon
    lot_count = lot_ratio.to_integral_value(rounding=rounding)
    return max(0, int(lot_count))


def lot_count_to_qty(*, lot_count: int, lot_size: float) -> float:
    normalized_count = max(0, int(lot_count))
    normalized_lot_size = max(0.0, float(lot_size))
    if normalized_count <= 0 or normalized_lot_size <= DUST_POSITION_EPS:
        return 0.0
    return float(Decimal(str(normalized_count)) * Decimal(str(normalized_lot_size)))


@dataclass(frozen=True)
class MarketLotRules:
    market_id: str
    lot_size: float
    executable_min_qty: float
    dust_threshold: float
    bid_min_total_krw: float = 0.0
    ask_min_total_krw: float = 0.0
    bid_price_unit: float = 0.0
    ask_price_unit: float = 0.0
    order_types: tuple[str, ...] = ()
    order_sides: tuple[str, ...] = ()
    bid_fee: float = 0.0
    ask_fee: float = 0.0
    maker_bid_fee: float = 0.0
    maker_ask_fee: float = 0.0
    min_qty: float = 0.0
    qty_step: float = 0.0
    min_notional_krw: float = 0.0
    max_qty_decimals: int = 0
    source_mode: str = "derived"

    def quantize_to_lot_count(self, qty: float, *, rounding=ROUND_FLOOR) -> int:
        return quantize_to_lot_count(qty=qty, lot_size=self.lot_size, rounding=rounding)

    def lot_count_to_qty(self, lot_count: int) -> float:
        return lot_count_to_qty(lot_count=lot_count, lot_size=self.lot_size)

    def split_qty(self, qty: float) -> "LotSplit":
        return split_qty_into_executable_and_dust(qty=qty, lot_rules=self)

    def is_executable_exit_qty(self, qty: float) -> bool:
        return is_executable_exit_qty(qty=qty, lot_rules=self)


@dataclass(frozen=True)
class LotSplit:
    requested_qty: float
    lot_size: float
    lot_count: int
    executable_qty: float
    dust_qty: float
    executable_min_qty: float
    dust_threshold: float
    executable: bool
    non_executable_reason: str


def build_market_lot_rules(
    *,
    market_id: str,
    market_price: float | None,
    rules: Any,
    exit_fee_ratio: float = 0.0,
    exit_slippage_bps: float = 0.0,
    exit_buffer_ratio: float = 0.0,
    source_mode: str = "exchange",
) -> MarketLotRules:
    from .dust import build_executable_lot

    probe = build_executable_lot(
        qty=max(
            0.0,
            _rule_number(rules, "min_qty")
            if _rule_number(rules, "min_qty") > 0
            else _rule_number(rules, "qty_step")
            if _rule_number(rules, "qty_step") > 0
            else 0.0,
        ),
        market_price=float(market_price) if market_price is not None else None,
        min_qty=_rule_number(rules, "min_qty"),
        qty_step=_rule_number(rules, "qty_step"),
        min_notional_krw=_rule_number(rules, "min_notional_krw"),
        max_qty_decimals=_rule_int(rules, "max_qty_decimals"),
        exit_fee_ratio=float(exit_fee_ratio),
        exit_slippage_bps=float(exit_slippage_bps),
        exit_buffer_ratio=float(exit_buffer_ratio),
    )
    lot_size = derive_internal_lot_size(
        market_price=market_price,
        min_qty=_rule_number(rules, "min_qty"),
        qty_step=_rule_number(rules, "qty_step"),
        min_notional_krw=_rule_number(rules, "min_notional_krw"),
        max_qty_decimals=_rule_int(rules, "max_qty_decimals"),
        exit_fee_ratio=float(exit_fee_ratio),
        exit_slippage_bps=float(exit_slippage_bps),
        exit_buffer_ratio=float(exit_buffer_ratio),
    )
    dust_threshold = max(float(lot_size), float(probe.effective_min_trade_qty))
    return MarketLotRules(
        market_id=str(market_id),
        lot_size=float(lot_size),
        executable_min_qty=float(probe.effective_min_trade_qty),
        dust_threshold=float(dust_threshold),
        bid_min_total_krw=_rule_number(rules, "bid_min_total_krw"),
        ask_min_total_krw=_rule_number(rules, "ask_min_total_krw"),
        bid_price_unit=_rule_number(rules, "bid_price_unit"),
        ask_price_unit=_rule_number(rules, "ask_price_unit"),
        order_types=_rule_tuple(rules, "order_types"),
        order_sides=_rule_tuple(rules, "order_sides"),
        bid_fee=_rule_number(rules, "bid_fee"),
        ask_fee=_rule_number(rules, "ask_fee"),
        maker_bid_fee=_rule_number(rules, "maker_bid_fee"),
        maker_ask_fee=_rule_number(rules, "maker_ask_fee"),
        min_qty=_rule_number(rules, "min_qty"),
        qty_step=_rule_number(rules, "qty_step"),
        min_notional_krw=_rule_number(rules, "min_notional_krw"),
        max_qty_decimals=_rule_int(rules, "max_qty_decimals"),
        source_mode=str(source_mode or "derived"),
    )


def split_qty_into_executable_and_dust(*, qty: float, lot_rules: MarketLotRules) -> LotSplit:
    requested_qty = max(0.0, float(qty))
    lot_count = quantize_to_lot_count(qty=requested_qty, lot_size=float(lot_rules.lot_size))
    executable_qty = lot_count_to_qty(lot_count=lot_count, lot_size=float(lot_rules.lot_size))
    dust_qty = max(0.0, requested_qty - executable_qty)
    executable = bool(
        requested_qty > DUST_POSITION_EPS
        and lot_count > 0
        and executable_qty >= max(float(lot_rules.executable_min_qty), float(lot_rules.dust_threshold), DUST_POSITION_EPS)
    )
    if not executable:
        if requested_qty <= DUST_POSITION_EPS:
            reason = "no_position"
        elif requested_qty < float(lot_rules.dust_threshold):
            reason = "dust_only_remainder"
        else:
            reason = "no_executable_exit_lot"
        return LotSplit(
            requested_qty=requested_qty,
            lot_size=float(lot_rules.lot_size),
            lot_count=0,
            executable_qty=0.0,
            dust_qty=requested_qty,
            executable_min_qty=float(lot_rules.executable_min_qty),
            dust_threshold=float(lot_rules.dust_threshold),
            executable=False,
            non_executable_reason=reason,
        )

    return LotSplit(
        requested_qty=requested_qty,
        lot_size=float(lot_rules.lot_size),
        lot_count=int(lot_count),
        executable_qty=float(executable_qty),
        dust_qty=float(dust_qty),
        executable_min_qty=float(lot_rules.executable_min_qty),
        dust_threshold=float(lot_rules.dust_threshold),
        executable=True,
        non_executable_reason="executable",
    )


def qty_to_executable_lot_count(*, qty: float, lot_rules: MarketLotRules) -> int:
    split = split_qty_into_executable_and_dust(qty=qty, lot_rules=lot_rules)
    return int(split.lot_count if split.executable else 0)


def is_executable_exit_qty(*, qty: float, lot_rules: MarketLotRules) -> bool:
    return bool(split_qty_into_executable_and_dust(qty=qty, lot_rules=lot_rules).executable)
