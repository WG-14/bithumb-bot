from __future__ import annotations

import json
import os

from bithumb_bot import runtime_state
from bithumb_bot.config import settings


def _set_tmp_db(tmp_path):
    db_path = str((tmp_path / "runtime-state.sqlite").resolve())
    os.environ["DB_PATH"] = db_path
    roots = {
        "ENV_ROOT": (tmp_path / "env").resolve(),
        "RUN_ROOT": (tmp_path / "run").resolve(),
        "DATA_ROOT": (tmp_path / "data").resolve(),
        "LOG_ROOT": (tmp_path / "logs").resolve(),
        "BACKUP_ROOT": (tmp_path / "backup").resolve(),
    }
    for key, value in roots.items():
        os.environ[key] = str(value)
    object.__setattr__(settings, "DB_PATH", db_path)


def test_startup_gate_reason_preserves_reconcile_dust_metadata(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "dust_residual_present": 1,
            "dust_residual_allow_resume": 0,
            "dust_policy_reason": "dangerous_dust_operator_review_required",
            "dust_broker_qty": 0.000099,
            "dust_local_qty": 0.00001,
        },
        now_epoch_sec=123.0,
    )

    runtime_state.set_startup_gate_reason("unresolved_open_orders=1")
    state = runtime_state.snapshot()

    assert state.last_reconcile_reason_code == "STARTUP_GATE_BLOCKED"
    assert state.last_reconcile_metadata is not None
    metadata = json.loads(state.last_reconcile_metadata)
    assert metadata["startup_gate_blocked"] is True
    assert metadata["startup_gate_reason"] == "unresolved_open_orders=1"
    assert metadata["dust_residual_present"] == 1
    assert metadata["dust_policy_reason"] == "dangerous_dust_operator_review_required"


def test_record_reconcile_result_preserves_formal_broker_evidence_under_metadata_clipping(tmp_path):
    _set_tmp_db(tmp_path)
    runtime_state.record_reconcile_result(
        success=True,
        reason_code="RECONCILE_OK",
        metadata={
            "balance_source": "accounts_v1_rest_snapshot",
            "balance_observed_ts_ms": 1_777_191_428_883,
            "balance_asset_ts_ms": 1_777_191_428_883,
            "balance_source_stale": False,
            "balance_source_quote_currency": "KRW",
            "balance_source_base_currency": "BTC",
            "broker_asset_qty": 0.00079982,
            "broker_asset_available": 0.00079982,
            "broker_asset_locked": 0.0,
            "broker_cash_available": 306_916.10493,
            "broker_cash_locked": 0.0,
            "dust_broker_qty": 0.00079982,
            "remote_open_order_found": 0,
            "oversized_detail": "x" * 10_000,
        },
        now_epoch_sec=123.0,
    )

    metadata = json.loads(runtime_state.snapshot().last_reconcile_metadata or "{}")

    assert metadata["balance_source"] == "accounts_v1_rest_snapshot"
    assert metadata["balance_observed_ts_ms"] == 1_777_191_428_883
    assert metadata["balance_source_quote_currency"] == "KRW"
    assert metadata["balance_source_base_currency"] == "BTC"
    assert metadata["broker_asset_qty"] == 0.00079982
    assert metadata["broker_asset_available"] == 0.00079982
    assert metadata["broker_asset_locked"] == 0.0
    assert metadata["broker_cash_available"] == 306_916.10493
    assert metadata["broker_cash_locked"] == 0.0
    assert metadata["oversized_detail"] == "x" * 160
