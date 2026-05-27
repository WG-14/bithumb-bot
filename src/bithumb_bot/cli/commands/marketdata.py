from __future__ import annotations

import argparse

from bithumb_bot.cli.registry import CommandSpec

from ._helpers import make_spec


def _ticker(_args: argparse.Namespace, _context) -> None:
    from bithumb_bot.marketdata import cmd_ticker

    cmd_ticker()


def _candles(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.marketdata import cmd_candles

    cmd_candles(args.limit)


def _sync(_args: argparse.Namespace, _context) -> None:
    from bithumb_bot.marketdata import cmd_sync

    cmd_sync()


def _sync_orderbook_top(args: argparse.Namespace, _context) -> None:
    from bithumb_bot.marketdata import cmd_sync_orderbook_top

    cmd_sync_orderbook_top(pair=str(args.pair) if args.pair else None)


def _backfill_candles(args: argparse.Namespace, _context) -> int:
    from bithumb_bot.historical_backfill import backfill_candles
    from bithumb_bot.utils_time import kst_str

    def _print_progress(progress):
        oldest = kst_str(progress.oldest_ts) if progress.oldest_ts is not None else "none"
        newest = kst_str(progress.newest_ts) if progress.newest_ts is not None else "none"
        print(
            "[BACKFILL-CANDLES] "
            f"requests={progress.request_count} fetched={progress.fetched_count} "
            f"written={progress.written_count} duplicate_pages={progress.duplicate_page_count} "
            f"cursor_stalls={progress.cursor_stall_count} cursor_fallbacks={progress.cursor_fallback_count} "
            f"oldest={oldest} newest={newest} next_api_cursor={progress.next_cursor or 'none'} "
            f"page_boundary_gap_minutes={progress.page_boundary_gap_minutes if progress.page_boundary_gap_minutes is not None else 'none'} "
            f"status={progress.status} reason={progress.reason or 'none'}"
        )

    try:
        result = backfill_candles(
            market=str(args.market),
            interval=str(args.interval),
            start=str(args.start),
            end=str(args.end),
            batch_size=int(args.batch_size),
            dry_run=bool(args.dry_run),
            progress_callback=_print_progress,
        )
    except Exception as exc:
        print(f"[BACKFILL-CANDLES] error={exc}")
        return 1
    coverage = result.coverage
    env_summary = result.env_summary
    print(
        "[BACKFILL-CANDLES] final "
        f"mode={result.mode} db_path={result.db_path} dry_run={1 if result.dry_run else 0} "
        f"env_file={env_summary.get('env_file')} env_loaded={1 if env_summary.get('loaded') else 0} "
        f"env_exists={1 if env_summary.get('exists') else 0} "
        f"requests={result.progress.request_count} fetched={result.progress.fetched_count} "
        f"written={result.progress.written_count} cursor_fallbacks={result.progress.cursor_fallback_count} "
        f"status={result.progress.status} reason={result.progress.reason or 'none'}"
    )
    page_gap_summary = result.page_gap_summary
    top_gaps = page_gap_summary.get("top_page_boundary_gaps") or []
    formatted_gaps = (
        ";".join(f"{item.get('gap_minutes')}m:{item.get('count')}" for item in top_gaps)
        if top_gaps
        else "none"
    )
    print(
        "[BACKFILL-CANDLES] data_plane_contract "
        f"api_cursor_timezone={page_gap_summary.get('api_cursor_timezone')} "
        f"db_timestamp_timezone={page_gap_summary.get('db_timestamp_timezone')} "
        f"page_boundary_gap_top={formatted_gaps}"
    )
    print(
        "[BACKFILL-CANDLES] coverage "
        f"expected_buckets={coverage['expected_buckets']} "
        f"present_buckets={coverage['present_buckets']} "
        f"missing_buckets={coverage['missing_buckets']} "
        f"coverage_pct={coverage['coverage_pct']} first_ts={coverage['first_ts']} "
        f"last_ts={coverage['last_ts']} coverage_status={coverage['coverage_status']} "
        f"coverage_reasons={','.join(str(item) for item in coverage['coverage_reasons']) if coverage['coverage_reasons'] else 'none'}"
    )
    print(
        "[BACKFILL-CANDLES] dataset_quality "
        f"status={result.dataset_quality_status} next_action={result.next_action}"
    )
    coverage_complete = coverage.get("coverage_status") == "COMPLETE"
    if result.progress.status != "COMPLETE":
        print(
            "[BACKFILL-CANDLES] result=FAIL "
            f"reason={result.progress.reason or 'progress_incomplete'} "
            "next_action=inspect backfill progress, rerun backfill, "
            "then run research-readiness --manifest <manifest>"
        )
        return 1
    if not coverage_complete:
        result_status = "DRY_RUN_NOT_READY" if result.dry_run else "FAIL"
        print(
            f"[BACKFILL-CANDLES] result={result_status} "
            "reason=coverage_incomplete_after_backfill "
            "next_action=inspect missing ranges, rerun backfill, "
            "then run research-readiness --manifest <manifest>"
        )
        return 0 if result.dry_run else 1
    print(
        "[BACKFILL-CANDLES] result=COMPLETE reason=coverage_complete "
        "next_action=run research-readiness --manifest <manifest> before research-backtest"
    )
    return 0


def command_specs() -> list[CommandSpec]:
    return [
        make_spec("ticker", domain="marketdata", handler=_ticker),
        make_spec(
            "candles",
            domain="marketdata",
            handler=_candles,
            build=lambda p: p.add_argument("--limit", type=int, default=5),
        ),
        make_spec("sync", domain="marketdata", handler=_sync, writes_db=True),
        make_spec(
            "sync-orderbook-top",
            domain="marketdata",
            handler=_sync_orderbook_top,
            help="collect one validated public top-of-book snapshot into the configured SQLite DB",
            description="Fetch and persist one best bid/ask snapshot. This stores top-of-book only, not full depth.",
            build=lambda p: p.add_argument("--pair"),
            writes_db=True,
        ),
        make_spec(
            "backfill-candles",
            domain="marketdata",
            handler=_backfill_candles,
            help="backfill historical minute candles into the configured SQLite DB",
            description=(
                "Fetch public Bithumb minute candles backward over an explicit date range "
                "and upsert them into candles using candle_date_time_utc bucket keys."
            ),
            build=_build_backfill_candles_parser,
            read_only=False,
            mutating=True,
            writes_db=True,
            produces_artifact=False,
        ),
    ]


def _build_backfill_candles_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market", required=True)
    parser.add_argument("--interval", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
