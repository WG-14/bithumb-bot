from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .config import resolve_db_path, settings
from .db_core import (
    ensure_db,
    init_portfolio,
    normalize_asset_qty,
    normalize_cash_amount,
    portfolio_asset_total,
    portfolio_cash_total,
    set_portfolio_breakdown,
)

EPS = 1e-10


@dataclass(frozen=True)
class RepairTarget:
    fill_id: int
    trade_id: int
    client_order_id: str
    exchange_order_id: str
    expected_fill_uid: str
    expected_qty: float
    expected_price: float


TARGETS: tuple[RepairTarget, ...] = (
    RepairTarget(
        fill_id=14,
        trade_id=14,
        client_order_id="recovery_C0101000002856175729",
        exchange_order_id="C0101000002856175729",
        expected_fill_uid="C0101000002856175729:aggregate:1774230830000",
        expected_qty=9.777e-05,
        expected_price=102247000.0,
    ),
    RepairTarget(
        fill_id=33,
        trade_id=33,
        client_order_id="live_1774265640000_sell_attempt_790346d1402e44d9",
        exchange_order_id="C0101000002857593528",
        expected_fill_uid="C0101000002857593528:aggregate:1774265763000",
        expected_qty=9.709e-05,
        expected_price=103032000.0,
    ),
    RepairTarget(
        fill_id=99,
        trade_id=99,
        client_order_id="recovery_C0101000002861024463",
        exchange_order_id="C0101000002861024463",
        expected_fill_uid="C0101000002861024463:aggregate:1774352767000",
        expected_qty=9.433e-05,
        expected_price=106143000.0,
    ),
)


class RepairValidationError(RuntimeError):
    pass


def _backup_hint(db_path: str, backup_path: str | None) -> tuple[bool, str]:
    if backup_path:
        candidate = Path(backup_path)
        if candidate.exists() and candidate.is_file():
            return True, f"backup file confirmed: {candidate}"
        return False, f"backup file not found: {candidate}"

    db = Path(db_path)
    sibling_candidates = sorted(db.parent.glob(f"{db.name}*.sqlite"))
    if sibling_candidates:
        newest = sibling_candidates[-1]
        return True, f"no --backup-path provided; found nearby backup candidate: {newest}"
    return False, "no --backup-path provided and no nearby backup candidate found"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RepairValidationError(message)


def _validate_targets(conn: sqlite3.Connection) -> list[str]:
    notes: list[str] = []

    for target in TARGETS:
        order = conn.execute(
            """
            SELECT client_order_id, exchange_order_id, side, status, qty_filled
            FROM orders
            WHERE client_order_id=?
            """,
            (target.client_order_id,),
        ).fetchone()
        _require(order is not None, f"target order missing: {target.client_order_id}")
        _require(str(order["exchange_order_id"]) == target.exchange_order_id, f"unexpected exchange_order_id for {target.client_order_id}")
        _require(str(order["side"]) == "SELL", f"order side is not SELL for {target.client_order_id}")
        _require(str(order["status"]) == "FILLED", f"order status is not FILLED for {target.client_order_id}")
        _require(abs(float(order["qty_filled"]) - target.expected_qty) <= EPS, f"order qty_filled mismatch for {target.client_order_id}")

        fill = conn.execute(
            """
            SELECT id, client_order_id, fill_id, price, qty
            FROM fills
            WHERE id=?
            """,
            (target.fill_id,),
        ).fetchone()
        _require(fill is not None, f"target fill missing id={target.fill_id}")
        _require(str(fill["client_order_id"]) == target.client_order_id, f"fill client_order_id mismatch id={target.fill_id}")
        _require(str(fill["fill_id"]) == target.expected_fill_uid, f"fill fill_id mismatch id={target.fill_id}")
        _require(abs(float(fill["qty"]) - target.expected_qty) <= EPS, f"fill qty mismatch id={target.fill_id}")
        _require(abs(float(fill["price"])) <= EPS, f"fill price is not zero (already repaired?) id={target.fill_id}")

        trade = conn.execute(
            """
            SELECT id, side, price, qty, fee, cash_after, asset_after, note
            FROM trades
            WHERE id=?
            """,
            (target.trade_id,),
        ).fetchone()
        _require(trade is not None, f"target trade missing id={target.trade_id}")
        _require(str(trade["side"]) == "SELL", f"trade side is not SELL id={target.trade_id}")
        _require(abs(float(trade["qty"]) - target.expected_qty) <= EPS, f"trade qty mismatch id={target.trade_id}")
        _require(abs(float(trade["asset_after"])) <= EPS, f"trade asset_after is not zero id={target.trade_id}")
        _require(abs(float(trade["price"]) - target.expected_price) <= EPS, f"trade price mismatch id={target.trade_id}")
        note_text = str(trade["note"] or "")
        _require(
            f"reconcile recent exchange_order_id={target.exchange_order_id}" in note_text,
            f"trade note missing expected exchange_order_id marker id={target.trade_id}",
        )

        prev_buy = conn.execute(
            """
            SELECT id, side, qty, asset_after
            FROM trades
            WHERE id < ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (target.trade_id,),
        ).fetchone()
        _require(prev_buy is not None, f"no previous trade before sell id={target.trade_id}")
        _require(str(prev_buy["side"]) == "BUY", f"previous trade is not BUY before sell id={target.trade_id}")
        _require(abs(float(prev_buy["qty"]) - target.expected_qty) <= EPS, f"previous BUY qty mismatch before sell id={target.trade_id}")
        _require(float(prev_buy["asset_after"]) >= target.expected_qty - EPS, f"previous BUY asset_after too small before sell id={target.trade_id}")

        notes.append(
            f"validated target fill_id={target.fill_id} trade_id={target.trade_id} exchange_order_id={target.exchange_order_id}"
        )

    return notes


def _ledger_replay_from_fills(conn: sqlite3.Connection) -> tuple[float, float]:
    cash = normalize_cash_amount(settings.START_CASH_KRW)
    qty = normalize_asset_qty(0.0)
    rows = conn.execute(
        """
        SELECT o.side, f.price, f.qty, f.fee
        FROM fills f
        JOIN orders o ON o.client_order_id = f.client_order_id
        ORDER BY f.fill_ts ASC, f.id ASC
        """
    ).fetchall()
    for row in rows:
        side = str(row["side"])
        price = float(row["price"])
        fill_qty = float(row["qty"])
        fee = float(row["fee"])
        if side == "BUY":
            cash = normalize_cash_amount(cash - ((price * fill_qty) + fee))
            qty = normalize_asset_qty(qty + fill_qty)
        elif side == "SELL":
            cash = normalize_cash_amount(cash + ((price * fill_qty) - fee))
            qty = normalize_asset_qty(qty - fill_qty)
        else:
            raise RepairValidationError(f"invalid side in fills replay: {side}")
    return cash, qty


def _recompute_trade_snapshots(conn: sqlite3.Connection) -> tuple[list[dict[str, float | int]], float, float]:
    cash = normalize_cash_amount(settings.START_CASH_KRW)
    qty = normalize_asset_qty(0.0)
    diffs: list[dict[str, float | int]] = []

    rows = conn.execute(
        """
        SELECT id, side, price, qty, fee, cash_after, asset_after
        FROM trades
        ORDER BY id ASC
        """
    ).fetchall()

    for row in rows:
        trade_id = int(row["id"])
        side = str(row["side"])
        price = float(row["price"])
        trade_qty = float(row["qty"])
        fee = float(row["fee"])

        if side == "BUY":
            cash = normalize_cash_amount(cash - ((price * trade_qty) + fee))
            qty = normalize_asset_qty(qty + trade_qty)
        elif side == "SELL":
            cash = normalize_cash_amount(cash + ((price * trade_qty) - fee))
            qty = normalize_asset_qty(qty - trade_qty)
        else:
            raise RepairValidationError(f"invalid trade side for id={trade_id}: {side}")

        if cash < -1e-6:
            raise RepairValidationError(f"negative cash produced while replaying trades at id={trade_id}: {cash}")
        if qty < -1e-10:
            raise RepairValidationError(f"negative asset produced while replaying trades at id={trade_id}: {qty}")

        old_cash_after = float(row["cash_after"])
        old_asset_after = float(row["asset_after"])
        if not math.isclose(old_cash_after, cash, abs_tol=1e-8) or not math.isclose(old_asset_after, qty, abs_tol=1e-10):
            diffs.append(
                {
                    "trade_id": trade_id,
                    "old_cash_after": old_cash_after,
                    "new_cash_after": cash,
                    "old_asset_after": old_asset_after,
                    "new_asset_after": qty,
                }
            )

    return diffs, cash, qty


def run_repair(*, db_path: str | None = None, apply: bool = False, backup_path: str | None = None, allow_no_backup: bool = False) -> int:
    resolved_db_path = resolve_db_path(db_path or settings.DB_PATH)
    backup_ok, backup_message = _backup_hint(resolved_db_path, backup_path)

    conn = ensure_db(resolved_db_path)
    conn.isolation_level = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        init_portfolio(conn)

        print(f"[REPAIR] db_path={resolved_db_path}")
        print(f"[REPAIR] mode={'APPLY' if apply else 'DRY-RUN'}")
        print(f"[REPAIR] backup_check={backup_message}")
        if apply and not backup_ok and not allow_no_backup:
            raise RepairValidationError("backup confirmation is required for --apply (use --backup-path or --allow-no-backup)")

        pre_replay_cash, pre_replay_qty = _ledger_replay_from_fills(conn)
        portfolio_row = conn.execute(
            "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
        ).fetchone()
        _require(portfolio_row is not None, "portfolio row(id=1) missing")
        pre_portfolio_cash = portfolio_cash_total(
            cash_available=float(portfolio_row["cash_available"]),
            cash_locked=float(portfolio_row["cash_locked"]),
        )
        pre_portfolio_qty = portfolio_asset_total(
            asset_available=float(portfolio_row["asset_available"]),
            asset_locked=float(portfolio_row["asset_locked"]),
        )
        print(
            "[REPAIR] precheck "
            f"replay_cash={pre_replay_cash:.8f} replay_qty={pre_replay_qty:.12f} "
            f"portfolio_cash={pre_portfolio_cash:.8f} portfolio_qty={pre_portfolio_qty:.12f}"
        )

        validation_notes = _validate_targets(conn)
        for note in validation_notes:
            print(f"[REPAIR] {note}")

        fill_diffs: list[dict[str, float | int | str]] = []
        for target in TARGETS:
            row = conn.execute("SELECT price FROM fills WHERE id=?", (target.fill_id,)).fetchone()
            old_price = float(row["price"])
            fill_diffs.append(
                {
                    "fill_id": target.fill_id,
                    "client_order_id": target.client_order_id,
                    "old_price": old_price,
                    "new_price": target.expected_price,
                }
            )
            conn.execute("UPDATE fills SET price=? WHERE id=?", (target.expected_price, target.fill_id))

        trade_diffs, final_trade_cash, final_trade_qty = _recompute_trade_snapshots(conn)
        for diff in trade_diffs:
            conn.execute(
                "UPDATE trades SET cash_after=?, asset_after=? WHERE id=?",
                (float(diff["new_cash_after"]), float(diff["new_asset_after"]), int(diff["trade_id"])),
            )

        set_portfolio_breakdown(
            conn,
            cash_available=final_trade_cash,
            cash_locked=0.0,
            asset_available=final_trade_qty,
            asset_locked=0.0,
        )

        post_replay_cash, post_replay_qty = _ledger_replay_from_fills(conn)
        post_portfolio = conn.execute(
            "SELECT cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked FROM portfolio WHERE id=1"
        ).fetchone()
        _require(post_portfolio is not None, "portfolio row missing after repair")
        post_portfolio_cash = portfolio_cash_total(
            cash_available=float(post_portfolio["cash_available"]),
            cash_locked=float(post_portfolio["cash_locked"]),
        )
        post_portfolio_qty = portfolio_asset_total(
            asset_available=float(post_portfolio["asset_available"]),
            asset_locked=float(post_portfolio["asset_locked"]),
        )

        cash_match = math.isclose(post_replay_cash, post_portfolio_cash, abs_tol=1e-6)
        qty_match = math.isclose(post_replay_qty, post_portfolio_qty, abs_tol=1e-10)
        if not cash_match or not qty_match:
            raise RepairValidationError(
                "post-check mismatch remains: "
                f"replay_cash={post_replay_cash}, portfolio_cash={post_portfolio_cash}, "
                f"replay_qty={post_replay_qty}, portfolio_qty={post_portfolio_qty}"
            )

        latest_trade = conn.execute("SELECT id, cash_after, asset_after FROM trades ORDER BY id DESC LIMIT 1").fetchone()
        if latest_trade is not None:
            _require(
                math.isclose(float(latest_trade["cash_after"]), post_portfolio_cash, abs_tol=1e-8),
                "latest trade cash_after does not match portfolio",
            )
            _require(
                math.isclose(float(latest_trade["asset_after"]), post_portfolio_qty, abs_tol=1e-10),
                "latest trade asset_after does not match portfolio",
            )

        print("[REPAIR] fill price updates:")
        for diff in fill_diffs:
            print(
                "  "
                f"fill_id={diff['fill_id']} client_order_id={diff['client_order_id']} "
                f"price {float(diff['old_price']):.8f} -> {float(diff['new_price']):.8f}"
            )

        print(f"[REPAIR] trade snapshot updates={len(trade_diffs)}")
        for diff in trade_diffs[:20]:
            print(
                "  "
                f"trade_id={int(diff['trade_id'])} cash_after {float(diff['old_cash_after']):.8f} -> {float(diff['new_cash_after']):.8f}, "
                f"asset_after {float(diff['old_asset_after']):.12f} -> {float(diff['new_asset_after']):.12f}"
            )
        if len(trade_diffs) > 20:
            print(f"  ... {len(trade_diffs) - 20} more rows")

        print(
            "[REPAIR] postcheck "
            f"replay_cash={post_replay_cash:.8f} replay_qty={post_replay_qty:.12f} "
            f"portfolio_cash={post_portfolio_cash:.8f} portfolio_qty={post_portfolio_qty:.12f} "
            f"cash_match={cash_match} qty_match={qty_match}"
        )

        if apply:
            conn.execute("COMMIT")
            print("[REPAIR] applied successfully")
        else:
            conn.execute("ROLLBACK")
            print("[REPAIR] dry-run complete (rolled back)")
        return 0
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
