from __future__ import annotations

import json

from bithumb_bot import app
from bithumb_bot.profile_cli import _candidate_regime_policy_from_approved_profile, cmd_decision_equivalence
from tests.test_decision_equivalence_canonical import _decision


def test_runtime_replay_policy_uses_approved_profile_regime_and_audit_fields() -> None:
    profile = {
        "profile_content_hash": "sha256:profile",
        "profile_mode": "paper",
        "regime_policy": {
            "regime_classifier_version": "market_regime_v2",
            "allowed_regimes": ["uptrend_normal_vol_unknown"],
            "blocked_regimes": [],
        },
        "candidate_profile_hash": "sha256:candidate",
        "manifest_hash": "sha256:manifest",
        "dataset_content_hash": "sha256:dataset",
        "source_promotion_content_hash": "sha256:promotion",
        "lineage_hash": "sha256:lineage",
        "legacy_compatibility_used": False,
    }

    policy = _candidate_regime_policy_from_approved_profile(profile)

    assert policy["live_regime_policy"] == profile["regime_policy"]
    assert policy["strategy_profile_hash"] == "sha256:profile"
    assert policy["approved_profile_hash"] == "sha256:profile"
    assert policy["approved_profile_verification_ok"] is True
    assert policy["approved_profile_block_reason"] == "ok"
    assert policy["candidate_profile_hash"] == "sha256:candidate"
    assert policy["manifest_hash"] == "sha256:manifest"
    assert policy["dataset_content_hash"] == "sha256:dataset"


def test_cli_dispatch_reaches_research_export_decisions(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(**kwargs):
        calls.update(kwargs)
        return 0

    monkeypatch.setattr(app, "cmd_research_export_decisions", fake_cmd)

    assert app.main(
        [
            "research-export-decisions",
            "--manifest",
            "manifest.json",
            "--candidate-id",
            "candidate_001",
            "--split",
            "validation",
            "--profile",
            "profile.json",
            "--out",
            "research.json",
        ]
    ) == 0
    assert calls == {
        "manifest_path": "manifest.json",
        "candidate_id_value": "candidate_001",
        "split": "validation",
        "out_path": "research.json",
        "profile_path": "profile.json",
    }


def test_cli_dispatch_reaches_runtime_replay_decisions(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_cmd(**kwargs):
        calls.update(kwargs)
        return 0

    monkeypatch.setattr(app, "cmd_runtime_replay_decisions", fake_cmd)

    assert app.main(
        [
            "runtime-replay-decisions",
            "--profile",
            "profile.json",
            "--db",
            "paper.sqlite",
            "--through-ts-list",
            "timestamps.json",
            "--out",
            "runtime.json",
        ]
    ) == 0
    assert calls == {
        "profile_path": "profile.json",
        "db_path": "paper.sqlite",
        "through_ts_list_path": "timestamps.json",
        "out_path": "runtime.json",
    }


def test_decision_equivalence_cli_marks_direct_lists_unverified(tmp_path, capsys) -> None:
    research_path = tmp_path / "research_list.json"
    runtime_path = tmp_path / "runtime_list.json"
    research_path.write_text(json.dumps([_decision()], sort_keys=True), encoding="utf-8")
    runtime_path.write_text(json.dumps([_decision()], sort_keys=True), encoding="utf-8")

    rc = cmd_decision_equivalence(
        research_decisions_path=str(research_path),
        runtime_decisions_path=str(runtime_path),
        profile_hash="sha256:profile",
        market="KRW-BTC",
        interval="1m",
        data_fingerprint="sha256:data",
    )

    payload = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert payload["ok"] is False
    assert payload["promotion_grade_comparison"] is False
    assert payload["legacy_or_unverified_export"] is True
    assert payload["recommended_next_action"] == "regenerate_decisions_with_repo_owned_export_commands"
