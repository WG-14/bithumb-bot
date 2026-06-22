from __future__ import annotations

import json

import pytest

from bithumb_bot.h74_live_rehearsal import H74LiveRehearsalConfig, run_h74_live_rehearsal
from bithumb_bot.h74_readiness_certificate import (
    H74ReadinessCertificateError,
    build_h74_readiness_certificate,
    validate_h74_readiness_certificate,
)


def _source_artifact(tmp_path, *, fee_rate: float = 0.0004) -> str:
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            {
                "runtime_base_cost_assumption": {
                    "fee_rate": fee_rate,
                    "fee_source": "research_realistic_bithumb_app_fee",
                    "slippage_bps": 10,
                    "slippage_source": "research_assumption",
                },
                "candle_timing": "closed_candle_kst",
            }
        ),
        encoding="utf-8",
    )
    return str(source)


def _passing_rehearsal(tmp_path) -> dict[str, object]:
    return run_h74_live_rehearsal(
        H74LiveRehearsalConfig(source_artifact_path=_source_artifact(tmp_path))
    )


def _certificate(tmp_path) -> tuple[dict[str, object], dict[str, object], str]:
    env = tmp_path / "live.env"
    env.write_text("MODE=live\n", encoding="utf-8")
    rehearsal = _passing_rehearsal(tmp_path)
    cert = build_h74_readiness_certificate(rehearsal, env_file=str(env), expires_at_sec=9_999_999_999)
    return cert, rehearsal, str(env)


def test_certificate_contains_commit_env_broker_order_rule_hashes(tmp_path) -> None:
    cert, rehearsal, _env = _certificate(tmp_path)

    assert cert["commit_sha"]
    assert cert["env_file_hash"].startswith("sha256:")
    assert cert["db_schema_hash"].startswith("sha256:")
    assert cert["h74_authority_hash"] == rehearsal["rehearsal_hash"]
    assert cert["broker_balance_snapshot_hash"] == rehearsal["broker_balance_snapshot_hash"]
    assert cert["order_rule_fee_authority_hash"].startswith("sha256:")
    assert cert["gate_trace_hash"] == rehearsal["gate_trace_hash"]
    assert cert["would_submit_plan_hash"] == rehearsal["would_submit_plan_hash"]
    assert cert["positive_rehearsal_kst_10_pass"] is True
    assert cert["negative_rehearsal_kst_18_blocks_entry"] is True
    assert cert["entry_authority_gate_present"] is True
    assert str(cert["entry_authority_gate_hash"]).startswith("sha256:")


def test_certificate_requires_negative_entry_rehearsal(tmp_path) -> None:
    env = tmp_path / "live.env"
    env.write_text("MODE=live\n", encoding="utf-8")
    positive = _passing_rehearsal(tmp_path)
    negative = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="18:00", source_artifact_path=_source_artifact(tmp_path))
    )

    cert = build_h74_readiness_certificate(positive, env_file=str(env), negative_rehearsal=negative)

    assert cert["negative_rehearsal_kst_18_blocks_entry"] is True


def test_certificate_fails_when_kst_18_would_submit(tmp_path) -> None:
    positive = _passing_rehearsal(tmp_path)
    negative = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(kst_time="18:00", source_artifact_path=_source_artifact(tmp_path))
    )
    negative["would_submit"] = True

    with pytest.raises(H74ReadinessCertificateError, match="negative_rehearsal_kst_18"):
        build_h74_readiness_certificate(positive, env_file=None, negative_rehearsal=negative)


def test_certificate_contains_entry_authority_gate_hash(tmp_path) -> None:
    cert, _rehearsal, _env = _certificate(tmp_path)

    assert cert["entry_authority_gate_present"] is True
    assert str(cert["entry_authority_gate_hash"]).startswith("sha256:")


def test_certificate_invalid_when_env_hash_changes(tmp_path) -> None:
    cert, rehearsal, env = _certificate(tmp_path)
    with open(env, "w", encoding="utf-8") as handle:
        handle.write("MODE=live\nA=2\n")

    verdict = validate_h74_readiness_certificate(
        cert,
        env_file=env,
        broker_balance_snapshot_hash=str(rehearsal["broker_balance_snapshot_hash"]),
        now_sec=1,
    )

    assert verdict["valid"] is False
    assert "env_hash_changed" in verdict["reasons"]


def test_certificate_not_issued_when_pre_submit_risk_blocks(tmp_path) -> None:
    rehearsal = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            broker_snapshot_available=False,
            source_artifact_path=_source_artifact(tmp_path),
        )
    )

    with pytest.raises(H74ReadinessCertificateError, match="gate_trace_blocking"):
        build_h74_readiness_certificate(rehearsal, env_file=None)


def test_certificate_not_issued_when_equivalence_mismatch(tmp_path) -> None:
    rehearsal = run_h74_live_rehearsal(
        H74LiveRehearsalConfig(
            source_artifact_path=_source_artifact(tmp_path),
            current_fee_rate=0.0025,
        )
    )

    with pytest.raises(H74ReadinessCertificateError, match="experiment_equivalence_not_pass"):
        build_h74_readiness_certificate(rehearsal, env_file=None)


def test_certificate_not_issued_when_gate_trace_has_blocking_gate(tmp_path) -> None:
    rehearsal = _passing_rehearsal(tmp_path)
    rehearsal["gate_trace"] = [*rehearsal["gate_trace"], {"gate": "unit", "blocking": True}]

    with pytest.raises(H74ReadinessCertificateError, match="gate_trace_blocking"):
        build_h74_readiness_certificate(rehearsal, env_file=None)


def test_certificate_invalid_when_commit_changes(tmp_path) -> None:
    cert, rehearsal, env = _certificate(tmp_path)

    verdict = validate_h74_readiness_certificate(
        cert,
        env_file=env,
        broker_balance_snapshot_hash=str(rehearsal["broker_balance_snapshot_hash"]),
        current_commit_sha="different",
        now_sec=1,
    )

    assert verdict["valid"] is False
    assert "commit_sha_changed" in verdict["reasons"]


def test_certificate_invalid_when_order_rule_fee_hash_changes(tmp_path) -> None:
    cert, rehearsal, env = _certificate(tmp_path)

    verdict = validate_h74_readiness_certificate(
        cert,
        env_file=env,
        broker_balance_snapshot_hash=str(rehearsal["broker_balance_snapshot_hash"]),
        current_order_rule_fee_authority_hash="sha256:different",
        now_sec=1,
    )

    assert verdict["valid"] is False
    assert "order_rule_fee_authority_hash_changed" in verdict["reasons"]


def test_certificate_invalid_when_gate_trace_hash_changes(tmp_path) -> None:
    cert, rehearsal, env = _certificate(tmp_path)

    verdict = validate_h74_readiness_certificate(
        cert,
        env_file=env,
        broker_balance_snapshot_hash=str(rehearsal["broker_balance_snapshot_hash"]),
        current_gate_trace_hash="sha256:different",
        now_sec=1,
    )

    assert verdict["valid"] is False
    assert "gate_trace_hash_changed" in verdict["reasons"]


def test_certificate_invalid_when_would_submit_plan_hash_changes(tmp_path) -> None:
    cert, rehearsal, env = _certificate(tmp_path)

    verdict = validate_h74_readiness_certificate(
        cert,
        env_file=env,
        broker_balance_snapshot_hash=str(rehearsal["broker_balance_snapshot_hash"]),
        current_would_submit_plan_hash="sha256:different",
        now_sec=1,
    )

    assert verdict["valid"] is False
    assert "would_submit_plan_hash_changed" in verdict["reasons"]


def test_certificate_invalid_when_db_schema_hash_changes(tmp_path) -> None:
    cert, rehearsal, env = _certificate(tmp_path)

    verdict = validate_h74_readiness_certificate(
        cert,
        env_file=env,
        broker_balance_snapshot_hash=str(rehearsal["broker_balance_snapshot_hash"]),
        current_db_schema_hash="sha256:different",
        now_sec=1,
    )

    assert verdict["valid"] is False
    assert "db_schema_hash_changed" in verdict["reasons"]


def test_certificate_validation_requires_current_commit_db_order_gate_plan_hashes(tmp_path) -> None:
    cert, rehearsal, env = _certificate(tmp_path)

    verdict = validate_h74_readiness_certificate(
        cert,
        env_file=env,
        broker_balance_snapshot_hash=str(rehearsal["broker_balance_snapshot_hash"]),
        current_commit_sha=str(cert["commit_sha"]),
        current_db_schema_hash=str(cert["db_schema_hash"]),
        current_order_rule_fee_authority_hash=str(cert["order_rule_fee_authority_hash"]),
        current_gate_trace_hash=str(cert["gate_trace_hash"]),
        current_would_submit_plan_hash=str(cert["would_submit_plan_hash"]),
        strict=True,
        now_sec=1,
    )

    assert verdict["valid"] is True
    assert verdict["reasons"] == []


def test_certificate_invalid_when_current_hash_argument_missing_in_strict_mode(tmp_path) -> None:
    cert, rehearsal, env = _certificate(tmp_path)

    verdict = validate_h74_readiness_certificate(
        cert,
        env_file=env,
        broker_balance_snapshot_hash=str(rehearsal["broker_balance_snapshot_hash"]),
        current_commit_sha=str(cert["commit_sha"]),
        current_db_schema_hash=None,
        current_order_rule_fee_authority_hash=str(cert["order_rule_fee_authority_hash"]),
        current_gate_trace_hash=str(cert["gate_trace_hash"]),
        current_would_submit_plan_hash=str(cert["would_submit_plan_hash"]),
        strict=True,
        now_sec=1,
    )

    assert verdict["valid"] is False
    assert "missing_current_db_schema_hash" in verdict["reasons"]


def test_certificate_not_issued_when_source_artifact_missing() -> None:
    rehearsal = run_h74_live_rehearsal(H74LiveRehearsalConfig(source_artifact_path=None))

    with pytest.raises(H74ReadinessCertificateError, match="source_artifact_not_loaded"):
        build_h74_readiness_certificate(rehearsal, env_file=None)
