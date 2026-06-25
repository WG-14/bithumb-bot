from __future__ import annotations

import pytest

from bithumb_bot.h74_observation import (
    H74ObservationAuthorityError,
    build_h74_observation_experiment_envelope,
    verify_h74_observation_experiment_envelope,
)


def _envelope(**overrides: object) -> dict[str, object]:
    payload = {
        "experiment_run_id": "exp-1",
        "runtime_git_commit_sha": "abc",
        "runtime_git_clean": True,
        "env_hash": "sha256:" + "1" * 64,
        "strategy_revision_id": "sha256:" + "2" * 64,
        "risk_scope_id": "sha256:" + "3" * 64,
        "risk_baseline_certificate_hash": "sha256:" + "4" * 64,
        "starting_broker_position": {"qty": 0},
        "starting_local_position": {"qty": 0},
        "db_snapshot_hash": "sha256:" + "5" * 64,
        "included_history_policy": "declared_live_history_scope",
    }
    payload.update(overrides)
    return build_h74_observation_experiment_envelope(**payload)  # type: ignore[arg-type]


def test_h74_real_observation_requires_experiment_envelope() -> None:
    with pytest.raises(H74ObservationAuthorityError, match="experiment_run_id"):
        _envelope(experiment_run_id="")


def test_h74_envelope_binds_risk_scope_and_baseline() -> None:
    payload = _envelope()
    verify_h74_observation_experiment_envelope(payload)

    assert payload["risk_scope_id"] == "sha256:" + "3" * 64
    assert payload["risk_baseline_certificate_hash"] == "sha256:" + "4" * 64


def test_h74_envelope_records_included_history_policy() -> None:
    payload = _envelope(included_history_policy="explicit_allowlist")

    assert payload["included_history_policy"] == "explicit_allowlist"
