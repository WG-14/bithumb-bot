from __future__ import annotations

import time

from bithumb_bot.h74_readiness_certificate import (
    validate_h74_long_run_preflight,
    validate_h74_readiness_certificate,
)


def _certificate(**overrides):
    payload = {
        "status": "pass",
        "positive_rehearsal_kst_10_pass": True,
        "negative_rehearsal_kst_18_blocks_entry": True,
        "entry_authority_gate_present": True,
        "out_of_window_buy_blocked": True,
        "entry_authority_gate_hash": "sha256:entry",
        "env_file_hash": "sha256:env-file-missing",
        "broker_balance_snapshot_hash": "sha256:broker",
        "expires_at_sec": time.time() + 3600,
        "commit_sha": "abc",
        "db_schema_hash": "sha256:schema",
        "order_rule_fee_authority_hash": "sha256:rules",
        "gate_trace_hash": "sha256:gate",
        "would_submit_plan_hash": "sha256:plan",
        "behavior_comparison_hash": "sha256:behavior",
        "contract_hash": "sha256:contract",
    }
    payload.update(overrides)
    return payload


def test_h74_live_start_blocks_without_certificate() -> None:
    result = validate_h74_readiness_certificate(
        {},
        env_file=None,
        broker_balance_snapshot_hash="sha256:broker",
        strict=True,
    )

    assert result["status"] == "invalid"


def test_h74_live_start_blocks_when_certificate_expired() -> None:
    result = validate_h74_readiness_certificate(
        _certificate(expires_at_sec=1),
        env_file=None,
        broker_balance_snapshot_hash="sha256:broker",
        now_sec=2,
    )

    assert "certificate_expired" in result["reasons"]


def test_h74_live_start_blocks_when_env_hash_changed(tmp_path) -> None:
    env_file = tmp_path / "live.env"
    env_file.write_text("MODE=live\n", encoding="utf-8")
    result = validate_h74_readiness_certificate(
        _certificate(env_file_hash="sha256:old"),
        env_file=str(env_file),
        broker_balance_snapshot_hash="sha256:broker",
    )

    assert "env_hash_changed" in result["reasons"]


def test_h74_long_run_preflight_reports_startup_enforced_true() -> None:
    result = validate_h74_long_run_preflight(_certificate())

    assert result["status"] == "pass"
    assert result["run_startup_enforced"] is True


def test_h74_live_start_blocks_when_current_contract_hash_missing() -> None:
    result = validate_h74_readiness_certificate(
        _certificate(contract_hash="sha256:a"),
        env_file=None,
        broker_balance_snapshot_hash="sha256:broker",
        current_commit_sha="abc",
        current_db_schema_hash="sha256:schema",
        current_order_rule_fee_authority_hash="sha256:rules",
        current_gate_trace_hash="sha256:gate",
        current_would_submit_plan_hash="sha256:plan",
        current_behavior_comparison_hash="sha256:behavior",
        strict=True,
    )

    assert result["valid"] is False
    assert "missing_current_contract_hash" in result["reasons"]


def test_h74_live_start_blocks_when_current_hashes_fallback_to_certificate_would_have_passed() -> None:
    cert = _certificate(
        commit_sha="cert-commit",
        order_rule_fee_authority_hash="sha256:cert-rules",
        contract_hash="sha256:cert-contract",
    )
    result = validate_h74_readiness_certificate(
        cert,
        env_file=None,
        broker_balance_snapshot_hash="sha256:broker",
        current_db_schema_hash="sha256:schema",
        current_gate_trace_hash="sha256:gate",
        current_would_submit_plan_hash="sha256:plan",
        current_behavior_comparison_hash="sha256:behavior",
        strict=True,
    )

    assert result["valid"] is False
    assert "missing_current_commit_sha" in result["reasons"]
    assert "missing_current_order_rule_fee_authority_hash" in result["reasons"]
    assert "missing_current_contract_hash" in result["reasons"]
