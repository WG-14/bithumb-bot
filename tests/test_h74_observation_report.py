from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest

from bithumb_bot.h74_observation_report import build_h74_observation_report
from bithumb_bot.runtime.daily_participation_claims import ensure_daily_participation_claims_schema


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE orders (
            client_order_id TEXT,
            strategy_name TEXT,
            side TEXT,
            status TEXT,
            exit_rule_name TEXT,
            decision_reason TEXT,
            last_error TEXT
        )
        """
    )
    conn.execute("CREATE TABLE fills (client_order_id TEXT, fee REAL)")
    conn.execute("CREATE TABLE daily_participation_claims (status TEXT)")
    return conn


def _ts(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _window_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE orders (
            client_order_id TEXT,
            strategy_name TEXT,
            strategy_instance_id TEXT,
            pair TEXT,
            side TEXT,
            status TEXT,
            exit_rule_name TEXT,
            decision_reason TEXT,
            last_error TEXT,
            created_ts INTEGER,
            authority_hash TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fills (
            client_order_id TEXT,
            fill_ts INTEGER,
            price REAL,
            qty REAL,
            fee REAL,
            reference_price REAL,
            slippage_bps REAL
        )
        """
    )
    return conn


def _interval_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE orders (
            client_order_id TEXT,
            strategy_name TEXT,
            strategy_instance_id TEXT,
            pair TEXT,
            interval TEXT,
            side TEXT,
            status TEXT,
            exit_rule_name TEXT,
            decision_reason TEXT,
            last_error TEXT,
            created_ts INTEGER,
            authority_hash TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fills (
            client_order_id TEXT,
            fill_ts INTEGER,
            price REAL,
            qty REAL,
            fee REAL,
            reference_price REAL,
            slippage_bps REAL
        )
        """
    )
    return conn


def _cycle_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE orders (
            client_order_id TEXT,
            strategy_name TEXT,
            strategy_instance_id TEXT,
            pair TEXT,
            interval TEXT,
            side TEXT,
            status TEXT,
            exit_rule_name TEXT,
            decision_reason TEXT,
            decision_reason_code TEXT,
            entry_authority_source TEXT,
            authority_source TEXT,
            last_error TEXT,
            created_ts INTEGER,
            cycle_id TEXT,
            authority_hash TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE fills (
            client_order_id TEXT,
            fill_ts INTEGER,
            price REAL,
            qty REAL,
            fee REAL,
            reference_price REAL,
            slippage_bps REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE h74_cycle_state (
            cycle_id TEXT PRIMARY KEY,
            authority_hash TEXT NOT NULL,
            strategy_instance_id TEXT NOT NULL,
            pair TEXT NOT NULL,
            state TEXT NOT NULL,
            entry_client_order_id TEXT,
            exit_client_order_id TEXT,
            entry_filled_ts INTEGER,
            scheduled_exit_ts INTEGER,
            acquired_qty REAL NOT NULL DEFAULT 0,
            sold_qty REAL NOT NULL DEFAULT 0,
            locked_exit_qty REAL NOT NULL DEFAULT 0,
            unauthorized_intermediate_order_count INTEGER NOT NULL DEFAULT 0,
            updated_ts INTEGER NOT NULL
        )
        """
    )
    return conn


def _insert_cycle_order(
    conn: sqlite3.Connection,
    cid: str,
    *,
    cycle_id: str = "cycle-1",
    created: str,
    side: str,
    exit_rule: str = "",
    instance: str = "h74:one",
    authority_hash: str = "sha256:auth-a",
) -> None:
    conn.execute(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            cid,
            "daily_participation_sma",
            instance,
            "KRW-BTC",
            "1m",
            side,
            "FILLED",
            exit_rule,
            "",
            "daily_participation_fallback_allowed" if side == "BUY" else "",
            "daily_participation_entry" if side == "BUY" else "",
            "daily_participation_entry" if side == "BUY" else "",
            "",
            _ts(created),
            cycle_id,
            authority_hash,
        ),
    )


def _insert_cycle_fill(
    conn: sqlite3.Connection,
    cid: str,
    *,
    fill_ts: str,
    price: float = 100_000_000.0,
    qty: float = 0.0008,
    fee: float = 10.0,
    reference_price: float | None = None,
) -> None:
    conn.execute(
        "INSERT INTO fills VALUES (?,?,?,?,?,?,?)",
        (cid, _ts(fill_ts), price, qty, fee, reference_price if reference_price is not None else price, None),
    )


def _insert_cycle_state(
    conn: sqlite3.Connection,
    *,
    cycle_id: str = "cycle-1",
    acquired_qty: float = 0.0008,
    sold_qty: float = 0.0008,
    locked_exit_qty: float = 0.0,
) -> None:
    conn.execute(
        """
        INSERT INTO h74_cycle_state(
            cycle_id, authority_hash, strategy_instance_id, pair, state,
            entry_client_order_id, exit_client_order_id, entry_filled_ts,
            scheduled_exit_ts, acquired_qty, sold_qty, locked_exit_qty,
            unauthorized_intermediate_order_count, updated_ts
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            cycle_id,
            "sha256:auth-a",
            "h74:one",
            "KRW-BTC",
            "CLOSED",
            "entry",
            "exit",
            _ts("2026-06-18T00:00:00Z"),
            _ts("2026-06-18T01:14:00Z"),
            acquired_qty,
            sold_qty,
            locked_exit_qty,
            0,
            _ts("2026-06-18T01:14:00Z"),
        ),
    )


def _insert_order(
    conn: sqlite3.Connection,
    cid: str,
    *,
    created: str,
    side: str = "BUY",
    strategy: str = "daily_participation_sma",
    instance: str = "h74:one",
    pair: str = "KRW-BTC",
    status: str = "FILLED",
    exit_rule: str = "",
    reason: str = "",
    error: str = "",
    authority_hash: str = "sha256:auth-a",
) -> None:
    conn.execute(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (cid, strategy, instance, pair, side, status, exit_rule, reason, error, _ts(created), authority_hash),
    )


def _insert_interval_order(
    conn: sqlite3.Connection,
    cid: str,
    *,
    created: str,
    interval: str = "1m",
    side: str = "BUY",
    strategy: str = "daily_participation_sma",
    instance: str = "h74:one",
    pair: str = "KRW-BTC",
    status: str = "FILLED",
    exit_rule: str = "",
    reason: str = "",
    error: str = "",
    authority_hash: str = "sha256:auth-a",
) -> None:
    conn.execute(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            cid,
            strategy,
            instance,
            pair,
            interval,
            side,
            status,
            exit_rule,
            reason,
            error,
            _ts(created),
            authority_hash,
        ),
    )


def _insert_fill(
    conn: sqlite3.Connection,
    cid: str,
    *,
    fill_ts: str,
    price: float = 100.0,
    qty: float = 1.0,
    fee: float = 1.0,
    reference_price: float = 100.0,
    slippage_bps: float | None = None,
) -> None:
    conn.execute(
        "INSERT INTO fills VALUES (?,?,?,?,?,?,?)",
        (cid, _ts(fill_ts), price, qty, fee, reference_price, slippage_bps),
    )


def test_h74_observation_report_includes_daily_counts() -> None:
    conn = _conn()
    conn.execute("INSERT INTO orders VALUES ('b1','daily_participation_sma','BUY','FILLED','','','')")
    conn.execute("INSERT INTO fills VALUES ('b1', 10.0)")

    report = build_h74_observation_report(conn=conn, days=7)

    assert report["daily_buy_intent_count"] == 1
    assert report["daily_buy_filled_count"] == 1
    assert "duplicate_entry_block_count" in report


def test_h74_observation_report_distinguishes_strategy_exit_from_manual_flatten() -> None:
    conn = _conn()
    conn.execute("INSERT INTO orders VALUES ('s1','daily_participation_sma','SELL','FILLED','max_holding_time','','')")
    conn.execute("INSERT INTO orders VALUES ('s2','daily_participation_sma','SELL','FILLED','','manual_flatten','')")

    report = build_h74_observation_report(conn=conn, days=7)

    assert report["max_holding_exit_filled_count"] == 1
    assert report["manual_intervention_count"] == 1


def test_h74_observation_report_flags_duplicate_entry() -> None:
    conn = _conn()
    for index in range(8):
        conn.execute(
            "INSERT INTO orders VALUES (?, 'daily_participation_sma','BUY','FILLED','','','')",
            (f"b{index}",),
        )
        conn.execute("INSERT INTO fills VALUES (?, 0.0)", (f"b{index}",))

    report = build_h74_observation_report(conn=conn, days=7)

    assert report["duplicate_entry_block_count"] == 7


def test_h74_observation_report_includes_broker_local_mismatch_count() -> None:
    conn = _conn()
    conn.execute("INSERT INTO orders VALUES ('x','daily_participation_sma','BUY','FAILED','','','broker/local mismatch')")

    report = build_h74_observation_report(conn=conn, days=7)

    assert report["broker_local_mismatch_count"] == 1


def test_h74_observation_report_does_not_use_backtest_pnl_as_live_pnl() -> None:
    report = build_h74_observation_report(days=7)

    assert report["source_backtest_pnl"] is None
    assert report["live_observed_pnl"] is None


def test_h74_observation_report_filters_to_requested_7_day_window() -> None:
    conn = _window_conn()
    _insert_order(conn, "old", created="2026-06-09T00:00:00Z")
    _insert_fill(conn, "old", fill_ts="2026-06-09T00:01:00Z")
    _insert_order(conn, "inside", created="2026-06-18T00:00:00Z")
    _insert_fill(conn, "inside", fill_ts="2026-06-18T00:01:00Z")

    report = build_h74_observation_report(
        conn=conn,
        days=7,
        now=datetime(2026, 6, 19, tzinfo=timezone.utc),
        strategy_instance_id="h74:one",
    )

    assert report["daily_buy_intent_count"] == 1
    assert report["daily_buy_filled_count"] == 1


def test_h74_observation_report_filters_orders_by_interval_when_column_exists() -> None:
    conn = _interval_conn()
    _insert_interval_order(conn, "target", created="2026-06-18T00:00:00Z", interval="1m")
    _insert_fill(conn, "target", fill_ts="2026-06-18T00:01:00Z", fee=2.0)
    _insert_interval_order(conn, "wrong_interval", created="2026-06-18T00:00:00Z", interval="5m")
    _insert_fill(conn, "wrong_interval", fill_ts="2026-06-18T00:01:00Z", fee=99.0)

    report = build_h74_observation_report(
        conn=conn,
        days=7,
        now=datetime(2026, 6, 19, tzinfo=timezone.utc),
        strategy_instance_id="h74:one",
        pair="KRW-BTC",
        interval="1m",
    )

    assert report["daily_buy_intent_count"] == 1
    assert report["daily_buy_filled_count"] == 1
    assert report["fee_total_krw"] == 2.0
    assert report["interval_scope_applied"] is True
    assert report["interval_scope_unavailable"] == []


def _insert_claim(
    conn: sqlite3.Connection,
    *,
    instance: str = "h74:one",
    pair: str = "KRW-BTC",
    kst_day: str = "2026-06-18",
    policy_hash: str = "sha256:policy-a",
    status: str = "submitted",
    created: str = "2026-06-18T00:00:00Z",
    client_order_id: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO daily_participation_claims(
            strategy_instance_id, pair, kst_day, participation_policy_hash,
            status, retry_allowed, created_ts, updated_ts
        )
        VALUES (?, ?, ?, ?, ?, 0, ?, ?)
        """,
        (instance, pair, kst_day, policy_hash, status, _ts(created), _ts(created)),
    )
    if client_order_id is not None:
        conn.execute(
            """
            UPDATE daily_participation_claims
            SET client_order_id=?
            WHERE strategy_instance_id=?
              AND pair=?
              AND kst_day=?
              AND participation_policy_hash=?
            """,
            (client_order_id, instance, pair, kst_day, policy_hash),
        )


def test_h74_observation_report_filters_claims_to_requested_7_day_window() -> None:
    conn = _window_conn()
    ensure_daily_participation_claims_schema(conn)
    _insert_claim(conn, kst_day="2026-06-09", created="2026-06-09T00:00:00Z")
    _insert_claim(conn, kst_day="2026-06-18", created="2026-06-18T00:00:00Z")

    report = build_h74_observation_report(
        conn=conn,
        observation_start=datetime(2026, 6, 11, 15, tzinfo=timezone.utc),
        observation_end=datetime(2026, 6, 18, 15, tzinfo=timezone.utc),
        strategy_instance_id="h74:one",
        participation_policy_hash="sha256:policy-a",
    )

    assert report["claim_pending_count"] == 1


def test_h74_observation_report_scopes_claims_to_strategy_instance_pair_and_policy() -> None:
    conn = _window_conn()
    ensure_daily_participation_claims_schema(conn)
    _insert_claim(conn, instance="h74:one", pair="KRW-BTC", policy_hash="sha256:policy-a")
    _insert_claim(conn, instance="h74:two", pair="KRW-BTC", policy_hash="sha256:policy-a")
    _insert_claim(conn, instance="h74:one", pair="KRW-ETH", policy_hash="sha256:policy-a")
    _insert_claim(conn, instance="h74:one", pair="KRW-BTC", policy_hash="sha256:policy-b")

    report = build_h74_observation_report(
        conn=conn,
        observation_start=datetime(2026, 6, 11, 15, tzinfo=timezone.utc),
        observation_end=datetime(2026, 6, 18, 15, tzinfo=timezone.utc),
        strategy_instance_id="h74:one",
        participation_policy_hash="sha256:policy-a",
        pair="KRW-BTC",
    )

    assert report["claim_pending_count"] == 1


def test_h74_observation_report_filters_claims_by_interval_or_fails_closed_when_required() -> None:
    conn = _interval_conn()
    ensure_daily_participation_claims_schema(conn)
    _insert_interval_order(conn, "claim_1m", created="2026-06-18T00:00:00Z", interval="1m", status="NEW")
    _insert_claim(conn, kst_day="2026-06-18", policy_hash="sha256:policy-a", client_order_id="claim_1m")
    _insert_interval_order(conn, "claim_5m", created="2026-06-18T00:00:00Z", interval="5m", status="NEW")
    _insert_claim(conn, kst_day="2026-06-17", policy_hash="sha256:policy-a", client_order_id="claim_5m")

    report = build_h74_observation_report(
        conn=conn,
        observation_start=datetime(2026, 6, 11, 15, tzinfo=timezone.utc),
        observation_end=datetime(2026, 6, 18, 15, tzinfo=timezone.utc),
        now=datetime(2026, 6, 19, tzinfo=timezone.utc),
        strategy_instance_id="h74:one",
        participation_policy_hash="sha256:policy-a",
        pair="KRW-BTC",
        interval="1m",
    )

    assert report["claim_pending_count"] == 1
    assert report["interval_scope_applied"] is True
    assert report["interval_scope_unavailable"] == []


def test_h74_observation_report_scopes_to_authority_hash_when_column_exists() -> None:
    conn = _window_conn()
    conn.execute(
        """
        CREATE TABLE daily_participation_claims (
            strategy_instance_id TEXT,
            pair TEXT,
            kst_day TEXT,
            participation_policy_hash TEXT,
            authority_hash TEXT,
            status TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    conn.execute(
        "INSERT INTO daily_participation_claims VALUES (?,?,?,?,?,?,?,?)",
        ("h74:one", "KRW-BTC", "2026-06-18", "sha256:policy-a", "sha256:auth-a", "submitted", _ts("2026-06-18T00:00:00Z"), _ts("2026-06-18T00:00:00Z")),
    )
    conn.execute(
        "INSERT INTO daily_participation_claims VALUES (?,?,?,?,?,?,?,?)",
        ("h74:one", "KRW-BTC", "2026-06-18", "sha256:policy-a", "sha256:auth-b", "submitted", _ts("2026-06-18T00:00:00Z"), _ts("2026-06-18T00:00:00Z")),
    )

    report = build_h74_observation_report(
        conn=conn,
        observation_start=datetime(2026, 6, 11, 15, tzinfo=timezone.utc),
        observation_end=datetime(2026, 6, 18, 15, tzinfo=timezone.utc),
        authority_hash="sha256:auth-a",
        strategy_instance_id="h74:one",
        participation_policy_hash="sha256:policy-a",
    )

    assert report["claim_pending_count"] == 1


def test_h74_observation_report_scopes_to_strategy_instance_and_authority_hash() -> None:
    conn = _window_conn()
    _insert_order(conn, "one", created="2026-06-18T00:00:00Z", instance="h74:one")
    _insert_fill(conn, "one", fill_ts="2026-06-18T00:01:00Z")
    _insert_order(conn, "two", created="2026-06-18T00:00:00Z", instance="h74:two")
    _insert_fill(conn, "two", fill_ts="2026-06-18T00:01:00Z")

    report = build_h74_observation_report(
        conn=conn,
        days=7,
        now=datetime(2026, 6, 19, tzinfo=timezone.utc),
        strategy_instance_id="h74:one",
    )

    assert report["daily_buy_intent_count"] == 1


def test_h74_observation_report_cli_accepts_authority_scope() -> None:
    from bithumb_bot.cli.parser import build_parser
    from bithumb_bot.cli.registry import command_registry

    parser = build_parser(command_registry())
    args = parser.parse_args(
        [
            "h74-observation-report",
            "--days",
            "7",
            "--json",
            "--authority-hash",
            "sha256:auth-a",
            "--from",
            "2026-06-12",
            "--to",
            "2026-06-19",
        ]
    )

    assert args.authority_hash == "sha256:auth-a"
    assert args.from_date == "2026-06-12"
    assert args.to_date == "2026-06-19"


def test_h74_observation_report_cli_passes_interval_to_builder(monkeypatch) -> None:
    from bithumb_bot.cli.commands import reports
    from bithumb_bot.cli.parser import build_parser
    from bithumb_bot.cli.registry import command_registry

    captured: dict[str, object] = {}

    def fake_builder(**kwargs):
        captured.update(kwargs)
        return {"complete": False}

    monkeypatch.setattr("bithumb_bot.h74_observation_report.build_h74_observation_report", fake_builder)
    parser = build_parser(command_registry())
    args = parser.parse_args(
        [
            "h74-observation-report",
            "--days",
            "7",
            "--json",
            "--authority-hash",
            "sha256:auth-a",
            "--from",
            "2026-06-12",
            "--to",
            "2026-06-19",
            "--interval",
            "5m",
        ]
    )

    assert reports._h74_observation_report(args, None) == 0
    assert captured["interval"] == "5m"


def test_h74_observation_report_detects_same_kst_day_duplicate_buy() -> None:
    conn = _window_conn()
    _insert_order(conn, "b1", created="2026-06-18T00:00:00Z")
    _insert_fill(conn, "b1", fill_ts="2026-06-18T00:01:00Z")
    _insert_order(conn, "b2", created="2026-06-18T01:00:00Z")
    _insert_fill(conn, "b2", fill_ts="2026-06-18T01:01:00Z")

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["duplicate_entry_block_count"] == 1


def test_h74_observation_report_excludes_non_h74_fees() -> None:
    conn = _window_conn()
    _insert_order(conn, "h74", created="2026-06-18T00:00:00Z")
    _insert_fill(conn, "h74", fill_ts="2026-06-18T00:01:00Z", fee=2.0)
    _insert_order(conn, "other", created="2026-06-18T00:00:00Z", strategy="sma_with_filter", instance="other")
    _insert_fill(conn, "other", fill_ts="2026-06-18T00:01:00Z", fee=99.0)

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["fee_total_krw"] == 2.0


def test_h74_observation_report_computes_exit_delay_from_rows() -> None:
    conn = _window_conn()
    _insert_order(conn, "s1", created="2026-06-18T00:00:00Z", side="SELL", exit_rule="max_holding_time")
    _insert_fill(conn, "s1", fill_ts="2026-06-18T00:01:30Z")

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["exit_delay_seconds_max"] == 90.0


def test_h74_observation_report_computes_observed_fee_bps() -> None:
    conn = _window_conn()
    _insert_order(conn, "b1", created="2026-06-18T00:00:00Z")
    _insert_fill(conn, "b1", fill_ts="2026-06-18T00:01:00Z", price=100.0, qty=2.0, fee=1.0)

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["observed_fee_bps"] == 50.0


def test_h74_observation_report_computes_slippage_bps() -> None:
    conn = _window_conn()
    _insert_order(conn, "b1", created="2026-06-18T00:00:00Z")
    _insert_fill(conn, "b1", fill_ts="2026-06-18T00:01:00Z", slippage_bps=3.5)

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["slippage_bps_avg"] == 3.5


def test_h74_observation_report_complete_false_before_7_days_elapsed() -> None:
    report = build_h74_observation_report(days=6, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["complete"] is False


def test_h74_observation_report_complete_false_when_interval_scope_unavailable() -> None:
    conn = _window_conn()
    for index, created in enumerate(
        [
            "2026-06-11T15:00:00Z",
            "2026-06-12T15:00:00Z",
            "2026-06-13T15:00:00Z",
            "2026-06-14T15:00:00Z",
            "2026-06-15T15:00:00Z",
            "2026-06-16T15:00:00Z",
            "2026-06-17T15:00:00Z",
        ]
    ):
        cid = f"b{index}"
        _insert_order(conn, cid, created=created, instance="h74:one")
        _insert_fill(conn, cid, fill_ts=created)

    report = build_h74_observation_report(
        conn=conn,
        observation_start=datetime(2026, 6, 11, 15, tzinfo=timezone.utc),
        observation_end=datetime(2026, 6, 18, 15, tzinfo=timezone.utc),
        now=datetime(2026, 6, 19, tzinfo=timezone.utc),
        authority_hash="sha256:auth-a",
        strategy_instance_id="h74:one",
        pair="KRW-BTC",
        interval="1m",
    )

    assert report["covered_kst_days"] == [
        "2026-06-12",
        "2026-06-13",
        "2026-06-14",
        "2026-06-15",
        "2026-06-16",
        "2026-06-17",
        "2026-06-18",
    ]
    assert report["interval_scope_applied"] is False
    assert "orders" in report["interval_scope_unavailable"]
    assert "fills_via_orders" in report["interval_scope_unavailable"]
    assert report["complete"] is False


def test_h74_observation_report_complete_requires_7_distinct_kst_days() -> None:
    conn = _window_conn()
    days = [
        "2026-06-12T00:00:00Z",
        "2026-06-13T00:00:00Z",
        "2026-06-14T00:00:00Z",
        "2026-06-15T00:00:00Z",
        "2026-06-16T00:00:00Z",
        "2026-06-17T00:00:00Z",
        "2026-06-17T01:00:00Z",
    ]
    for index, created in enumerate(days):
        cid = f"b{index}"
        _insert_order(conn, cid, created=created, instance="h74:one")
        _insert_fill(conn, cid, fill_ts=created)

    report = build_h74_observation_report(
        conn=conn,
        observation_start=datetime(2026, 6, 11, 15, tzinfo=timezone.utc),
        observation_end=datetime(2026, 6, 18, 15, tzinfo=timezone.utc),
        now=datetime(2026, 6, 19, tzinfo=timezone.utc),
        authority_hash="sha256:auth-a",
        strategy_instance_id="h74:one",
    )

    assert report["daily_buy_filled_count"] == 7
    assert len(report["covered_kst_days"]) == 6
    assert report["complete"] is False


def test_h74_observation_report_rejects_unscoped_pair_rows_in_strict_h74_scope() -> None:
    conn = _window_conn()
    _insert_order(conn, "target", created="2026-06-18T00:00:00Z", pair="KRW-BTC")
    _insert_fill(conn, "target", fill_ts="2026-06-18T00:01:00Z")
    _insert_order(conn, "null_pair", created="2026-06-18T00:00:00Z", pair=None)
    _insert_fill(conn, "null_pair", fill_ts="2026-06-18T00:01:00Z")
    _insert_order(conn, "empty_pair", created="2026-06-18T00:00:00Z", pair="")
    _insert_fill(conn, "empty_pair", fill_ts="2026-06-18T00:01:00Z")

    report = build_h74_observation_report(
        conn=conn,
        days=7,
        now=datetime(2026, 6, 19, tzinfo=timezone.utc),
        pair="KRW-BTC",
    )

    assert report["daily_buy_intent_count"] == 1


def test_report_uses_cycle_classifier_for_roundtrip_success() -> None:
    conn = _cycle_conn()
    _insert_cycle_order(conn, "entry", created="2026-06-18T00:00:00Z", side="BUY")
    _insert_cycle_fill(conn, "entry", fill_ts="2026-06-18T00:00:00Z")
    _insert_cycle_order(conn, "exit", created="2026-06-18T01:14:00Z", side="SELL", exit_rule="max_holding_time")
    _insert_cycle_fill(conn, "exit", fill_ts="2026-06-18T01:14:00Z")
    _insert_cycle_state(conn)

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["entry_path_sample_count"] == 1
    assert report["cycle_validation_success_count"] == 1
    assert report["h74_backtest_validation_sample_count"] == 1


def test_report_buy_only_counts_entry_sample_not_cycle_success() -> None:
    conn = _cycle_conn()
    _insert_cycle_order(conn, "entry", created="2026-06-18T00:00:00Z", side="BUY")
    _insert_cycle_fill(conn, "entry", fill_ts="2026-06-18T00:00:00Z")
    _insert_cycle_state(conn, sold_qty=0.0)

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["entry_path_sample_count"] == 1
    assert report["cycle_validation_success_count"] == 0
    assert report["h74_backtest_validation_sample_count"] == 0


def test_report_terminal_true_dust_uses_cycle_origin_and_notional() -> None:
    conn = _cycle_conn()
    _insert_cycle_order(conn, "entry", created="2026-06-18T00:00:00Z", side="BUY")
    _insert_cycle_fill(conn, "entry", fill_ts="2026-06-18T00:00:00Z")
    _insert_cycle_order(conn, "exit", created="2026-06-18T01:14:00Z", side="SELL", exit_rule="max_holding_time")
    _insert_cycle_fill(conn, "exit", fill_ts="2026-06-18T01:14:00Z", qty=0.00079)
    _insert_cycle_state(conn, acquired_qty=0.0008, sold_qty=0.00079)

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    residual = report["terminal_residual"]
    assert residual["residual_qty"] > 0
    assert residual["residual_notional_krw"] > 0
    assert residual["origin_cycle_id"] == "cycle-1"
    assert residual["residual_class"] == "EXCHANGE_TRUE_DUST"


def test_report_executable_residual_blocks_cycle_success() -> None:
    conn = _cycle_conn()
    _insert_cycle_order(conn, "entry", created="2026-06-18T00:00:00Z", side="BUY")
    _insert_cycle_fill(conn, "entry", fill_ts="2026-06-18T00:00:00Z")
    _insert_cycle_order(conn, "exit", created="2026-06-18T01:14:00Z", side="SELL", exit_rule="max_holding_time")
    _insert_cycle_fill(conn, "exit", fill_ts="2026-06-18T01:14:00Z", qty=0.0006)
    _insert_cycle_state(conn, acquired_qty=0.0008, sold_qty=0.0006)

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["terminal_residual"]["residual_qty"] == pytest.approx(0.0002)
    assert report["terminal_residual"]["next_cycle_allowed"] is False
    assert report["cycle_validation_success_count"] == 0


def test_report_records_unauthorized_intermediate_order_ids() -> None:
    conn = _cycle_conn()
    _insert_cycle_order(conn, "entry", created="2026-06-18T00:00:00Z", side="BUY")
    _insert_cycle_fill(conn, "entry", fill_ts="2026-06-18T00:00:00Z")
    _insert_cycle_order(conn, "rebalance", created="2026-06-18T00:30:00Z", side="BUY")
    _insert_cycle_fill(conn, "rebalance", fill_ts="2026-06-18T00:30:00Z")
    _insert_cycle_order(conn, "exit", created="2026-06-18T01:14:00Z", side="SELL", exit_rule="max_holding_time")
    _insert_cycle_fill(conn, "exit", fill_ts="2026-06-18T01:14:00Z")
    _insert_cycle_state(conn)

    report = build_h74_observation_report(conn=conn, days=7, now=datetime(2026, 6, 19, tzinfo=timezone.utc))

    assert report["unauthorized_intermediate_order_count"] == 1
    assert report["unauthorized_order_ids"] == ["rebalance"]
    assert report["cycle_validation_success_count"] == 0
