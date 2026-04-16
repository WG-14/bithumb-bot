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
