from __future__ import annotations

from bithumb_bot import runtime_state
from bithumb_bot.config import settings
from bithumb_bot.db_core import ensure_db
from bithumb_bot.reporting import (
    fetch_attribution_quality_summary,
    fetch_recovery_attribution_signal_summary,
)


def _insert_lifecycle(
    conn,
    *,
    lifecycle_id: int,
    exit_ts: int,
    entry_decision_id: int | None,
    entry_decision_linkage: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO trade_lifecycles(
            id, pair, entry_trade_id, exit_trade_id, entry_client_order_id, exit_client_order_id,
            entry_fill_id, exit_fill_id, entry_ts, exit_ts, matched_qty, entry_price, exit_price,
            gross_pnl, fee_total, net_pnl, holding_time_sec, strategy_name, entry_decision_id, entry_decision_linkage
        ) VALUES (?, 'KRW-BTC', ?, ?, ?, ?, NULL, NULL, ?, ?, 1.0, 100.0, 101.0, 1.0, 0.1, 0.9, 60.0, 'safety_case', ?, ?)
        """,
        (
            lifecycle_id,
            lifecycle_id,
            lifecycle_id,
            f"entry-{lifecycle_id}",
            f"exit-{lifecycle_id}",
            exit_ts - 60_000,
            exit_ts,
            entry_decision_id,
            entry_decision_linkage,
        ),
    )


def test_recovery_attribution_signals_cover_restart_regression_scenarios(tmp_path, monkeypatch):
    db_path = str(tmp_path / "attribution-quality.sqlite")
    monkeypatch.setenv("DB_PATH", db_path)
    object.__setattr__(settings, "DB_PATH", db_path)

    conn = ensure_db()
    try:
        # 선행 reconcile 기준시각을 고정한다.
        runtime_state.record_reconcile_result(
            success=True,
            reason_code="RECONCILE_OK",
            metadata={"remote_open_order_found": 0},
            now_epoch_sec=1000.0,
        )

        # 1) submit unknown 후 recovery: 복구 기반 degraded linkage
        _insert_lifecycle(
            conn,
            lifecycle_id=1,
            exit_ts=1_001_000,
            entry_decision_id=None,
            entry_decision_linkage="degraded_recovery_submit_unknown",
        )
        # 2) delayed fill discovery: 복구 기반 degraded linkage
        _insert_lifecycle(
            conn,
            lifecycle_id=2,
            exit_ts=1_001_100,
            entry_decision_id=None,
            entry_decision_linkage="degraded_recovery_delayed_fill",
        )
        # 3) partial fill 후 restart: 정상 direct linkage
        _insert_lifecycle(
            conn,
            lifecycle_id=3,
            exit_ts=1_001_200,
            entry_decision_id=3003,
            entry_decision_linkage="direct",
        )
        # 4) legacy incomplete linkage row: legacy 공백 linkage
        _insert_lifecycle(
            conn,
            lifecycle_id=4,
            exit_ts=1_001_300,
            entry_decision_id=None,
            entry_decision_linkage="",
        )
        # 5) ambiguous candidate decisions
        _insert_lifecycle(
            conn,
            lifecycle_id=5,
            exit_ts=1_001_400,
            entry_decision_id=None,
            entry_decision_linkage="ambiguous_multi_candidate",
        )
        conn.commit()

        quality = fetch_attribution_quality_summary(conn)
        signals = fetch_recovery_attribution_signal_summary(conn)
    finally:
        conn.close()

    assert quality.total_trade_count == 5
    assert quality.recovery_derived_attribution_count == 2
    assert quality.ambiguous_linkage_count == 1
    assert quality.reason_buckets["legacy_incomplete_row"] == 1
    assert quality.reason_buckets["multiple_candidate_decisions"] == 1
    assert quality.reason_buckets["recovery_unresolved_linkage"] == 2

    assert signals.recent_recovery_derived_trade_count == 2
    assert signals.unresolved_attribution_count == 4
    assert signals.ambiguous_linkage_after_recent_reconcile is True
    assert signals.last_reconcile_epoch_sec == 1000.0
