from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class QuantitySemantics:
    broker_position_qty: float
    exchange_min_qty: float
    exchange_sellable: bool
    strategy_internal_lot_size: float | None
    strategy_executable_lot_count: int
    strategy_dust_qty: float
    target_delta_closeable: bool
    terminal_closeout_covered_qty: float

    def as_dict(self) -> dict[str, object]:
        return {
            "broker_position_qty": float(self.broker_position_qty),
            "exchange_min_qty": float(self.exchange_min_qty),
            "exchange_sellable": bool(self.exchange_sellable),
            "strategy_internal_lot_size": (
                None
                if self.strategy_internal_lot_size is None
                else float(self.strategy_internal_lot_size)
            ),
            "strategy_executable_lot_count": int(self.strategy_executable_lot_count),
            "strategy_dust_qty": float(self.strategy_dust_qty),
            "target_delta_closeable": bool(self.target_delta_closeable),
            "terminal_closeout_covered_qty": float(self.terminal_closeout_covered_qty),
        }


def build_quantity_semantics(
    *,
    broker_position_qty: float,
    exchange_min_qty: float,
    strategy_internal_lot_size: float | None,
    target_delta_closeout_authorized: bool = False,
    terminal_closeout_covered_qty: float | None = None,
) -> QuantitySemantics:
    qty = max(0.0, float(broker_position_qty or 0.0))
    min_qty = max(0.0, float(exchange_min_qty or 0.0))
    lot_size = (
        None
        if strategy_internal_lot_size is None
        else max(0.0, float(strategy_internal_lot_size or 0.0))
    )
    exchange_sellable = bool(qty > 0.0 and qty + 1e-12 >= min_qty)
    if lot_size is None or lot_size <= 1e-12:
        executable_lots = 0
        dust_qty = qty
    else:
        executable_lots = max(0, int(math.floor((qty + 1e-12) / lot_size)))
        dust_qty = max(0.0, qty - (float(executable_lots) * lot_size))
    covered_qty = qty if terminal_closeout_covered_qty is None else max(
        0.0, float(terminal_closeout_covered_qty or 0.0)
    )
    target_delta_closeable = bool(
        target_delta_closeout_authorized
        and exchange_sellable
        and covered_qty + 1e-12 >= qty
    )
    return QuantitySemantics(
        broker_position_qty=qty,
        exchange_min_qty=min_qty,
        exchange_sellable=exchange_sellable,
        strategy_internal_lot_size=lot_size,
        strategy_executable_lot_count=executable_lots,
        strategy_dust_qty=dust_qty,
        target_delta_closeable=target_delta_closeable,
        terminal_closeout_covered_qty=covered_qty,
    )


__all__ = ["QuantitySemantics", "build_quantity_semantics"]
