from __future__ import annotations

import json

import pytest

from bithumb_bot.app import main
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db


FORBIDDEN_RESULT_KEY_PARTS = ("pnl", "drawdown", "fee_drag", "profit", "loss")


@pytest.fixture
def configured_db(tmp_path, monkeypatch):
    original = {
        "DB_PATH": settings.DB_PATH,
        "MODE": settings.MODE,
        "PAIR": settings.PAIR,
        "INTERVAL": settings.INTERVAL,
        "ENTRY_EDGE_BUFFER_RATIO": settings.ENTRY_EDGE_BUFFER_RATIO,
        "STRATEGY_ENTRY_SLIPPAGE_BPS": settings.STRATEGY_ENTRY_SLIPPAGE_BPS,
    }
    db_path = str((tmp_path / "strategy-sweep.sqlite").resolve())
    monkeypatch.setenv("MODE", "paper")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "DB_PATH", db_path)
    object.__setattr__(settings, "PAIR", "BTC_KRW")
    object.__setattr__(settings, "INTERVAL", "1m")
    try:
        yield db_path
    finally:
        for name, value in original.items():
            object.__setattr__(settings, name, value)


def _insert_candles(closes: list[float]) -> None:
    conn = ensure_db()
    try:
        base_ts = 1_700_000_000_000
        for idx, close in enumerate(closes):
            conn.execute(
                """
                INSERT INTO candles(ts, pair, interval, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    base_ts + idx * 60_000,
                    "BTC_KRW",
                    "1m",
                    close,
                    close,
                    close,
                    close,
                    1.0,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _sweep_args(*, as_json: bool = True) -> list[str]:
    args = [
        "strategy-sweep",
        "--short",
        "2",
        "--long",
        "3",
        "--edge-buffer",
        "0,0.02",
        "--min-expected-edge",
        "0",
        "--slippage-bps",
        "0",
        "--pair",
        "BTC_KRW",
        "--interval",
        "1m",
    ]
    if as_json:
        args.append("--json")
    return args


def _assert_no_forbidden_keys(payload) -> None:
    if isinstance(payload, dict):
        assert not any(
            forbidden in str(key).lower()
            for key in payload
            for forbidden in FORBIDDEN_RESULT_KEY_PARTS
        )
        for value in payload.values():
            _assert_no_forbidden_keys(value)
    elif isinstance(payload, list):
        for value in payload:
            _assert_no_forbidden_keys(value)


def test_strategy_sweep_cli_json_returns_deterministic_attribution_rows(
    configured_db, capsys
) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])

    rc = main(_sweep_args(as_json=True))
    payload = json.loads(capsys.readouterr().out)
    rows = payload["rows"]

    assert rc == 0
    assert payload["plan"]["grid_count"] == 2
    assert payload["plan"]["candle_count"] == 5
    assert payload["plan"]["full_history"] is True
    assert payload["plan"]["estimated_operations"] == 10
    assert payload["plan"]["max_operations"] == 300_000
    assert payload["plan"]["allow_large_sweep"] is False
    assert len(rows) == 2
    required = {
        "config_id",
        "raw_buy",
        "final_buy",
        "blocked_by_cost_filter_ratio",
        "gap_lt_required_ratio",
        "primary_issue",
    }
    assert required.issubset(rows[0])
    assert rows[1]["final_buy"] < rows[0]["final_buy"]
    _assert_no_forbidden_keys(payload)


def test_strategy_sweep_cli_human_output_includes_operator_fields(
    configured_db, capsys
) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])

    rc = main(_sweep_args(as_json=False))
    out = capsys.readouterr().out

    assert rc == 0
    assert "[STRATEGY-SWEEP]" in out
    assert "mode=decision_attribution_only" in out
    assert "grid_count=2" in out
    assert "candle_count=5" in out
    assert "estimated_operations=10" in out
    assert "max_operations=300000" in out
    assert "raw_BUY" in out
    assert "final_BUY" in out
    assert "cost_block" in out
    assert "primary_issue" in out


@pytest.mark.parametrize(
    ("option", "value", "expected"),
    [
        ("--short", "", "--short requires a non-empty comma-separated list"),
        ("--long", "abc", "--long contains invalid value: abc"),
        ("--edge-buffer", "0,not-a-float", "--edge-buffer contains invalid value: not-a-float"),
    ],
)
def test_strategy_sweep_cli_invalid_list_arguments_fail_cleanly(
    configured_db, capsys, option: str, value: str, expected: str
) -> None:
    args = _sweep_args(as_json=True)
    args[args.index(option) + 1] = value

    with pytest.raises(SystemExit):
        main(args)
    err = capsys.readouterr().err

    assert expected in err


def test_strategy_sweep_cli_does_not_mutate_settings(configured_db, capsys) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])
    object.__setattr__(settings, "ENTRY_EDGE_BUFFER_RATIO", 0.77)
    object.__setattr__(settings, "STRATEGY_ENTRY_SLIPPAGE_BPS", 88.0)

    rc = main(_sweep_args(as_json=True))
    capsys.readouterr()

    assert rc == 0
    assert settings.ENTRY_EDGE_BUFFER_RATIO == pytest.approx(0.77)
    assert settings.STRATEGY_ENTRY_SLIPPAGE_BPS == pytest.approx(88.0)


def test_strategy_sweep_cli_is_read_only_for_trade_state_tables(
    configured_db, capsys
) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])
    conn = ensure_db()
    try:
        conn.execute(
            """
            INSERT INTO orders(
                client_order_id, status, side, qty_req, qty_filled, created_ts, updated_ts
            )
            VALUES ('sentinel-order', 'OPEN', 'BUY', 0.001, 0.0, 1, 1)
            """
        )
        conn.execute(
            """
            INSERT INTO fills(client_order_id, fill_id, fill_ts, price, qty, fee)
            VALUES ('sentinel-order', 'sentinel-fill', 2, 10.0, 0.001, 0.0)
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO portfolio(
                id, cash_krw, asset_qty, cash_available, cash_locked, asset_available, asset_locked
            )
            VALUES (1, 1000.0, 0.001, 1000.0, 0.0, 0.001, 0.0)
            """
        )
        conn.execute(
            """
            INSERT INTO open_position_lots(
                pair, entry_trade_id, entry_client_order_id, entry_fill_id, entry_ts,
                entry_price, qty_open, executable_lot_count, dust_tracking_lot_count,
                lot_semantic_version, internal_lot_size, lot_min_qty, lot_qty_step,
                lot_min_notional_krw, lot_max_qty_decimals, lot_rule_source_mode,
                position_semantic_basis, position_state, entry_fee_total
            )
            VALUES (
                'BTC_KRW', 1, 'sentinel-order', 'sentinel-fill', 2,
                10.0, 0.001, 1, 0,
                1, 0.001, 0.001, 0.001,
                5000.0, 8, 'test',
                'lot-native', 'open_exposure', 0.0
            )
            """
        )
        conn.commit()
        before = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("orders", "fills", "portfolio", "open_position_lots")
        }
    finally:
        conn.close()

    rc = main(_sweep_args(as_json=True))
    capsys.readouterr()

    conn = ensure_db()
    try:
        after = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("orders", "fills", "portfolio", "open_position_lots")
        }
        order_status = conn.execute(
            "SELECT status FROM orders WHERE client_order_id='sentinel-order'"
        ).fetchone()[0]
        fill_id = conn.execute(
            "SELECT fill_id FROM fills WHERE client_order_id='sentinel-order'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert rc == 0
    assert after == before
    assert order_status == "OPEN"
    assert fill_id == "sentinel-fill"


def test_strategy_sweep_cli_json_has_no_pnl_fields(configured_db, capsys) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])

    rc = main(_sweep_args(as_json=True))
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["rows"]
    _assert_no_forbidden_keys(payload)


def test_strategy_sweep_cli_live_requires_execution_boundary(
    configured_db, monkeypatch, capsys
) -> None:
    called = False

    def fail_if_called(**_kwargs):
        nonlocal called
        called = True

    object.__setattr__(settings, "MODE", "live")
    monkeypatch.setattr("bithumb_bot.app.cmd_strategy_sweep", fail_if_called)

    with pytest.raises(SystemExit):
        main(_sweep_args(as_json=True))
    err = capsys.readouterr().err

    assert called is False
    assert (
        "strategy-sweep in live mode requires "
        "--from/--to/--through/--max-candles or --allow-full-history"
    ) in err


def test_strategy_sweep_cli_live_max_candles_allows_execution(
    configured_db, capsys
) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])
    object.__setattr__(settings, "MODE", "live")
    args = _sweep_args(as_json=True) + ["--max-candles", "5000"]

    rc = main(args)
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["plan"]["allowed"] is True
    assert payload["plan"]["max_candles"] == 5000
    assert payload["plan"]["full_history"] is False
    assert len(payload["rows"]) == 2


def test_strategy_sweep_cli_live_large_operations_fail_before_replay(
    configured_db, monkeypatch, capsys
) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])
    object.__setattr__(settings, "MODE", "live")
    called = False

    def fail_if_called(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("sweep replay should not execute")

    monkeypatch.setattr(
        "bithumb_bot.app.run_sma_strategy_sweep_from_candles",
        fail_if_called,
    )
    args = _sweep_args(as_json=True) + ["--max-candles", "5000", "--max-operations", "9"]

    with pytest.raises(SystemExit):
        main(args)
    out = capsys.readouterr().out

    assert called is False
    assert "estimated_operations exceeds max_operations" in out


def test_strategy_sweep_cli_allow_large_sweep_permits_budget_exceedance(
    configured_db, capsys
) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])
    object.__setattr__(settings, "MODE", "live")
    args = _sweep_args(as_json=True) + [
        "--max-candles",
        "5000",
        "--max-operations",
        "9",
        "--allow-large-sweep",
    ]

    rc = main(args)
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["plan"]["estimated_operations"] == 10
    assert payload["plan"]["max_operations"] == 9
    assert payload["plan"]["allow_large_sweep"] is True
    assert payload["plan"]["allowed"] is True


def test_strategy_sweep_cli_max_operations_changes_threshold(
    configured_db, capsys
) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])
    object.__setattr__(settings, "MODE", "live")
    args = _sweep_args(as_json=True) + ["--max-candles", "5000", "--max-operations", "10"]

    rc = main(args)
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["plan"]["estimated_operations"] == 10
    assert payload["plan"]["max_operations"] == 10
    assert payload["plan"]["allow_large_sweep"] is False


def test_strategy_sweep_cli_invalid_max_candles_fails_cleanly(
    configured_db, capsys
) -> None:
    args = _sweep_args(as_json=True) + ["--max-candles", "0"]

    with pytest.raises(SystemExit):
        main(args)
    err = capsys.readouterr().err

    assert "--max-candles must be a positive integer" in err


def test_strategy_sweep_cli_allow_full_history_marks_plan(
    configured_db, capsys
) -> None:
    _insert_candles([10.0, 10.0, 10.0, 10.0, 11.0])
    object.__setattr__(settings, "MODE", "live")
    args = _sweep_args(as_json=True) + ["--allow-full-history"]

    rc = main(args)
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["plan"]["allowed"] is True
    assert payload["plan"]["full_history"] is True
    assert payload["plan"]["max_candles"] is None
