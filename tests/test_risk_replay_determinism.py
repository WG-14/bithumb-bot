from __future__ import annotations

import pytest

from bithumb_bot.db_core import ensure_db
from bithumb_bot.risk_layer_replay import build_risk_replay_input_artifact


def test_same_snapshot_replays_same_risk_decision_hash(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "replay.sqlite"))
    kwargs = {
        "db_snapshot_hash": "sha256:" + "a" * 64,
        "env_hash": "sha256:" + "b" * 64,
        "runtime_scope_id": "scope",
        "risk_scope_id": "risk",
        "candle_ts": 1,
        "mark_price": 100.0,
    }

    first = build_risk_replay_input_artifact(conn, **kwargs)
    second = build_risk_replay_input_artifact(conn, **kwargs)

    assert first["risk_decision_hash"] == second["risk_decision_hash"]


def test_db_history_change_changes_risk_input_hash(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "replay-change.sqlite"))
    kwargs = {
        "db_snapshot_hash": "sha256:" + "a" * 64,
        "env_hash": "sha256:" + "b" * 64,
        "runtime_scope_id": "scope",
        "risk_scope_id": "risk",
        "candle_ts": 1,
        "mark_price": 100.0,
    }
    before = build_risk_replay_input_artifact(conn, **kwargs)
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
            entry_ts, exit_ts, matched_qty, entry_price, exit_price, gross_pnl, fee_total,
            net_pnl, holding_time_sec
        ) VALUES ('KRW-BTC', 1, 2, 'e', 'x', 1, 2, 1, 100, 90, -10, 0, -10, 1)
        """
    )
    after = build_risk_replay_input_artifact(conn, **kwargs)

    assert before["risk_input_hash"] != after["risk_input_hash"]


def test_missing_snapshot_hash_fails_replay_contract(tmp_path) -> None:
    conn = ensure_db(str(tmp_path / "replay-missing.sqlite"))

    with pytest.raises(ValueError, match="risk_replay_db_snapshot_hash_missing"):
        build_risk_replay_input_artifact(
            conn,
            db_snapshot_hash="",
            env_hash="sha256:" + "b" * 64,
            runtime_scope_id="scope",
            risk_scope_id="risk",
            candle_ts=1,
            mark_price=100.0,
        )
