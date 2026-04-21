from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import settings
from .db_core import portfolio_asset_total
from .dust import DustDisplayContext, build_dust_display_context, build_position_state_model
from .lifecycle import summarize_position_lots, summarize_reserved_exit_qty


@dataclass(frozen=True)
class CanonicalPositionSnapshot:
    portfolio_asset_qty: float
    reserved_exit_qty: float
    dust_context: DustDisplayContext
    lot_snapshot: Any
    position_state: Any


def build_canonical_position_snapshot(
    conn: Any,
    *,
    metadata_raw: str | dict[str, object] | None,
    pair: str | None = None,
    portfolio_asset_qty: float | None = None,
) -> CanonicalPositionSnapshot:
    pair_text = str(pair or settings.PAIR)
    if portfolio_asset_qty is None:
        portfolio_row = conn.execute(
            "SELECT asset_qty, asset_available, asset_locked FROM portfolio WHERE id=1"
        ).fetchone()
        if portfolio_row is None:
            normalized_portfolio_asset_qty = 0.0
        elif hasattr(portfolio_row, "keys") and "asset_available" in portfolio_row.keys():
            normalized_portfolio_asset_qty = portfolio_asset_total(
                asset_available=float(portfolio_row["asset_available"] or 0.0),
                asset_locked=float(portfolio_row["asset_locked"] or 0.0),
            )
        else:
            normalized_portfolio_asset_qty = float(portfolio_row["asset_qty"] or 0.0)
    else:
        normalized_portfolio_asset_qty = float(portfolio_asset_qty)

    dust_context = build_dust_display_context(metadata_raw)
    lot_snapshot = summarize_position_lots(conn, pair=pair_text)
    lot_definition = getattr(lot_snapshot, "lot_definition", None)
    reserved_exit_qty = summarize_reserved_exit_qty(conn, pair=pair_text)
    position_state = build_position_state_model(
        raw_qty_open=normalized_portfolio_asset_qty,
        metadata_raw=metadata_raw,
        raw_total_asset_qty=max(
            normalized_portfolio_asset_qty,
            float(lot_snapshot.raw_total_asset_qty),
            float(dust_context.raw_holdings.broker_qty),
        ),
        open_exposure_qty=float(lot_snapshot.raw_open_exposure_qty),
        dust_tracking_qty=float(lot_snapshot.dust_tracking_qty),
        reserved_exit_qty=reserved_exit_qty,
        open_lot_count=int(lot_snapshot.open_lot_count),
        dust_tracking_lot_count=int(lot_snapshot.dust_tracking_lot_count),
        internal_lot_size=(None if lot_definition is None else lot_definition.internal_lot_size),
        min_qty=(None if lot_definition is None else lot_definition.min_qty),
        qty_step=(None if lot_definition is None else lot_definition.qty_step),
        min_notional_krw=(None if lot_definition is None else lot_definition.min_notional_krw),
        max_qty_decimals=(None if lot_definition is None else lot_definition.max_qty_decimals),
    )
    return CanonicalPositionSnapshot(
        portfolio_asset_qty=normalized_portfolio_asset_qty,
        reserved_exit_qty=reserved_exit_qty,
        dust_context=dust_context,
        lot_snapshot=lot_snapshot,
        position_state=position_state,
    )
