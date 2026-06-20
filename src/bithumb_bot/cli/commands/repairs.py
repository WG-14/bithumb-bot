from __future__ import annotations

import argparse

from bithumb_bot.cli.registry import CommandSpec

from ._helpers import make_spec


def _fee_gap(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_fee_gap_accounting_repair

    cmd_fee_gap_accounting_repair(
        apply=bool(args.apply),
        confirm=bool(args.yes),
        note=str(args.note) if args.note is not None else None,
    )


def _fee_pending(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_fee_pending_accounting_repair

    cmd_fee_pending_accounting_repair(
        client_order_id=str(args.client_order_id),
        fill_id=str(args.fill_id) if args.fill_id is not None else None,
        exchange_order_id=str(args.exchange_order_id) if args.exchange_order_id is not None else None,
        fee=float(args.fee) if args.fee is not None else None,
        fee_provenance=str(args.fee_provenance) if args.fee_provenance is not None else None,
        apply=bool(args.apply),
        confirm=bool(args.yes),
        note=str(args.note) if args.note is not None else None,
    )


def _rebuild_position_authority(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_rebuild_position_authority

    cmd_rebuild_position_authority(
        apply=bool(args.apply),
        confirm=bool(args.yes),
        note=str(args.note) if args.note is not None else None,
        full_projection_rebuild=bool(args.full_projection_rebuild),
        flat_stale_projection_repair=bool(args.flat_stale_projection_repair),
        historical_fragmentation_projection_repair=bool(
            getattr(args, "historical_fragmentation_projection_repair", False)
        ),
        enrich_legacy_operator_closeout_evidence=bool(args.enrich_legacy_operator_closeout_evidence),
        as_json=bool(args.json),
    )


def _external_cash(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_record_external_cash_adjustment

    cmd_record_external_cash_adjustment(
        event_ts=int(args.event_ts),
        delta_amount=float(args.delta_amount),
        source=str(args.source),
        reason=str(args.reason),
        broker_snapshot_basis=str(args.broker_snapshot_basis),
        currency=str(args.currency),
        correlation_metadata=str(args.correlation_metadata) if args.correlation_metadata is not None else None,
        note=str(args.note) if args.note is not None else None,
        adjustment_key=str(args.adjustment_key) if args.adjustment_key is not None else None,
        yes=bool(args.yes),
    )


def _manual_flat(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_manual_flat_accounting_repair

    cmd_manual_flat_accounting_repair(
        apply=bool(args.apply),
        confirm=bool(args.yes),
        note=str(args.note) if args.note is not None else None,
    )


def _external_position(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.operator_commands import cmd_external_position_accounting_repair

    cmd_external_position_accounting_repair(
        apply=bool(args.apply),
        confirm=bool(args.yes),
        note=str(args.note) if args.note is not None else None,
    )


def command_specs() -> list[CommandSpec]:
    common = dict(
        domain="repairs",
        read_only=False,
        mutating=True,
        requires_confirmation=True,
        writes_db=True,
        produces_artifact=True,
        json_output_supported=True,
    )
    return [
        make_spec(
            "fee-gap-accounting-repair",
            handler=_fee_gap,
            help="preview or apply explicit fee-gap accounting recovery",
            description="Record an explicit bounded fee-gap accounting repair for reconcile-detected historical zero-fee fill drift.",
            build=_build_apply_yes_note,
            **common,
        ),
        make_spec(
            "fee-pending-accounting-repair",
            handler=_fee_pending,
            help="finalize a fee-pending observed fill with explicit operator fee evidence",
            description=(
                "Apply a broker_fill_observations fee-pending fill through normal accounting after "
                "the operator supplies explicit fee provenance."
            ),
            build=_build_fee_pending,
            **common,
        ),
        make_spec(
            "rebuild-position-authority",
            handler=_rebuild_position_authority,
            help="preview or rebuild canonical lot authority from accounted BUY fill evidence",
            description=(
                "Rebuild missing lot-native position authority only from already-accounted BUY fills "
                "when no open orders, no existing lots, no SELL history, and portfolio quantity match."
            ),
            build=_build_rebuild_position_authority,
            guard_policy="operator_recovery",
            **common,
        ),
        make_spec(
            "record-external-cash-adjustment",
            handler=_external_cash,
            help="record an external cash adjustment event",
            description="Store a manual or broker-driven cash adjustment as a separate accounting event.",
            build=_build_external_cash,
            **common,
        ),
        make_spec(
            "manual-flat-accounting-repair",
            handler=_manual_flat,
            help="preview or apply a bounded manual-flat accounting repair",
            description="Record an explicit manual-flat accounting repair event after broker/manual flattening and local flat cleanup.",
            build=_build_apply_yes_note,
            **common,
        ),
        make_spec(
            "external-position-accounting-repair",
            handler=_external_position,
            help="preview or apply a replay-compatible external position adjustment",
            description="Record an explicit accounting adjustment after broker/offline position changes have already been reconciled into portfolio truth.",
            build=_build_apply_yes_note,
            **common,
        ),
    ]


def _build_apply_yes_note(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--note")


def _build_fee_pending(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--client-order-id", required=True)
    parser.add_argument("--fill-id")
    parser.add_argument("--exchange-order-id")
    parser.add_argument("--fee", type=float)
    parser.add_argument("--fee-provenance")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--note")


def _build_rebuild_position_authority(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--full-projection-rebuild", action="store_true")
    parser.add_argument("--flat-stale-projection-repair", action="store_true")
    parser.add_argument("--historical-fragmentation-projection-repair", action="store_true")
    parser.add_argument("--enrich-legacy-operator-closeout-evidence", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--note")
    parser.add_argument("--json", action="store_true")


def _build_external_cash(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--event-ts", type=int, required=True)
    parser.add_argument("--delta-amount", type=float, required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--broker-snapshot-basis", required=True)
    parser.add_argument("--currency", default="KRW")
    parser.add_argument("--correlation-metadata")
    parser.add_argument("--note")
    parser.add_argument("--adjustment-key")
    parser.add_argument("--yes", action="store_true")
