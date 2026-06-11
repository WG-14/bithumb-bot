from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from bithumb_bot.notifier import AlertSeverity, NotificationResult
from bithumb_bot.paths import PathManager
from bithumb_bot.research import cli as research_cli
from bithumb_bot.research.artifact_store import ArtifactBudgetExceeded
from bithumb_bot.research.cli import _print_report_summary, _print_research_backtest_progress
from bithumb_bot.research.experiment_registry import (
    EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
    append_attempt_completion,
    reserve_research_attempt,
)
from bithumb_bot.research.hashing import content_hash_payload, report_content_hash_payload, sha256_prefixed
from bithumb_bot.research.run_summary import build_research_run_summary


def _manager(tmp_path: Path, monkeypatch) -> PathManager:
    monkeypatch.setenv("MODE", "paper")
    for key in ("ENV_ROOT", "RUN_ROOT", "DATA_ROOT", "LOG_ROOT", "BACKUP_ROOT", "ARCHIVE_ROOT"):
        monkeypatch.setenv(key, str(tmp_path / f"{key.lower()}_root"))
    return PathManager.from_env(Path.cwd())


def _candidate(
    candidate_id: str,
    *,
    gate: str = "FAIL",
    fail_reasons: list[str] | None = None,
    walk_forward_metrics: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "parameter_candidate_id": candidate_id,
        "acceptance_gate_result": gate,
        "gate_fail_reasons": fail_reasons or [],
    }
    if walk_forward_metrics is not None:
        payload["walk_forward_metrics"] = walk_forward_metrics
    return payload


def _report(*, candidates: object, best_candidate_id: str | None = None, gate_result: str = "FAIL") -> dict[str, object]:
    return {
        "experiment_id": "summary_exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snapshot",
        "dataset_content_hash": "sha256:dataset",
        "candidate_count": len(candidates) if isinstance(candidates, list) else 0,
        "best_candidate_id": best_candidate_id,
        "gate_result": gate_result,
        "artifact_paths": {
            "report_path": "/tmp/reports/research/summary_exp/backtest_report.json",
            "derived_path": "/tmp/derived/research/summary_exp/backtest_candidates.json",
        },
        "content_hash": "sha256:report",
        "warnings": [],
        "candidates": candidates,
    }


def _registry_payload() -> dict[str, object]:
    return {
        "run_id": "summary_exp",
        "experiment_family_id": "family_a",
        "hypothesis_id": "hypothesis_a",
        "hypothesis_status": "pre_registered",
        "hypothesis_identity_source": "manifest.hypothesis_id",
        "experiment_family_identity_source": "manifest.experiment_family_id",
        "experiment_id": "summary_exp",
        "manifest_hash": "sha256:manifest",
        "manifest_metadata_hash": "sha256:metadata",
        "dataset_snapshot_id": "snap_001",
        "dataset_content_hash": "sha256:dataset",
        "dataset_quality_hash": "sha256:quality",
        "train_split_hash": "sha256:train",
        "validation_split_hash": "sha256:validation",
        "final_holdout_split_hash": "sha256:holdout",
        "final_holdout_fingerprint": "sha256:holdout-identity",
        "final_holdout_identity_hash": "sha256:holdout-identity",
        "final_holdout_content_hash": "sha256:holdout-content",
        "final_holdout_reuse_key_hash": "sha256:holdout-identity",
        "parameter_space_hash": "sha256:space",
        "parameter_grid_size": 1,
        "candidate_count": None,
        "declared_attempt_index": None,
        "declared_holdout_reuse_count": None,
        "statistical_evidence_hash": None,
        "return_panel_hash": None,
        "promotion_artifact_hash": None,
        "promoted_candidate_id": None,
        "repository_version": "test",
        "command_args_hash": "sha256:args",
    }


def _registry_payload_with(**overrides: object) -> dict[str, object]:
    payload = _registry_payload()
    payload.update(overrides)
    return payload


def _write_registry_validate_artifacts(
    manager: PathManager,
    reservation: dict[str, object],
    completion: dict[str, object],
    *,
    evidence_bound_hash: str = "sha256:pre-completion",
    evidence_row_hash: str | None = None,
    statistical_required: bool = True,
    write_evidence: bool,
    write_panel: bool,
) -> None:
    report_dir = manager.data_dir() / "reports" / "research" / "summary_exp"
    report_dir.mkdir(parents=True, exist_ok=True)
    report = {
        **_registry_payload(),
        "experiment_registry_path": reservation["path"],
        "experiment_registry_prior_hash": reservation["prior_hash"],
        "experiment_registry_row_hash": reservation["row_hash"],
        "experiment_registry_completion_row_hash": completion["row_hash"],
        "experiment_registry_bound_evidence_hash": "sha256:pre-completion",
        "experiment_registry_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        "computed_attempt_index": reservation["computed_attempt_index"],
        "computed_holdout_reuse_count": reservation["computed_holdout_reuse_count"],
        "statistical_validation_required": statistical_required,
        "statistical_evidence_hash": "sha256:final-evidence" if statistical_required else None,
        "return_panel_hash": "sha256:return",
        "candidates": [],
    }
    report["content_hash"] = sha256_prefixed(report_content_hash_payload(report))
    (report_dir / "backtest_report.json").write_text(json.dumps(report, sort_keys=True), encoding="utf-8")
    if write_evidence:
        evidence = {
            **report,
            "artifact_type": "statistical_selection_evidence",
            "experiment_registry_row_hash": evidence_row_hash or report["experiment_registry_row_hash"],
            "experiment_registry_bound_evidence_hash": evidence_bound_hash,
            "content_hash": "sha256:placeholder",
        }
        evidence["content_hash"] = sha256_prefixed(
            content_hash_payload({key: value for key, value in evidence.items() if key != "content_hash"})
        )
        (report_dir / "statistical_selection_evidence.json").write_text(
            json.dumps(evidence, sort_keys=True),
            encoding="utf-8",
        )
    if write_panel:
        panel = {
            "artifact_type": "candidate_return_panel",
            "schema_version": 1,
            "experiment_id": "summary_exp",
            "manifest_hash": "sha256:manifest",
            "dataset_content_hash": "sha256:dataset",
            "dataset_quality_hash": "sha256:quality",
            "split": "validation",
            "return_unit": "trade_return",
            "benchmark": "cash",
            "ordered_time_index": [],
            "ordered_time_index_hash": sha256_prefixed([]),
            "candidate_count": 0,
            "candidate_ids": [],
            "candidate_return_series": [],
            "observation_count": 0,
            "missing_observation_policy": "skip_missing_candidate_trade_returns",
            "limitations": [],
        }
        panel["panel_content_hash"] = sha256_prefixed(content_hash_payload(panel))
        panel["content_hash"] = sha256_prefixed(content_hash_payload(panel))
        report["return_panel_hash"] = panel["content_hash"]
        (report_dir / "candidate_return_panel.json").write_text(json.dumps(panel, sort_keys=True), encoding="utf-8")
        report["content_hash"] = sha256_prefixed(report_content_hash_payload(report))
        (report_dir / "backtest_report.json").write_text(json.dumps(report, sort_keys=True), encoding="utf-8")
        if write_evidence:
            evidence = {
                **report,
                "artifact_type": "statistical_selection_evidence",
                "experiment_registry_row_hash": evidence_row_hash or report["experiment_registry_row_hash"],
                "experiment_registry_bound_evidence_hash": evidence_bound_hash,
                "content_hash": "sha256:placeholder",
            }
            evidence["content_hash"] = sha256_prefixed(
                content_hash_payload({key: value for key, value in evidence.items() if key != "content_hash"})
            )
            (report_dir / "statistical_selection_evidence.json").write_text(
                json.dumps(evidence, sort_keys=True),
                encoding="utf-8",
            )


def test_fail_report_summary_includes_top_fail_reasons() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[
                _candidate("candidate_001", fail_reasons=["profit_factor_failed", "walk_forward_failed"]),
                _candidate("candidate_002", fail_reasons=["profit_factor_failed", "min_trade_count_failed"]),
            ]
        )
    )

    assert summary.top_fail_reasons == {
        "profit_factor_failed": 2,
        "min_trade_count_failed": 1,
        "walk_forward_failed": 1,
    }


def test_research_backtest_progress_prints_parent_serial_stages(capsys) -> None:
    _print_research_backtest_progress(
        {
            "stage": "build_work_tasks_complete",
            "candidate_count": 5,
            "scenario_count": 2,
            "work_task_count": 10,
            "split_count": 3,
            "elapsed_s": 1.25,
        }
    )

    out = capsys.readouterr().out
    assert "stage=build_work_tasks_complete" in out
    assert "elapsed_s=1.25" in out
    assert "work_task_count=10" in out


def test_research_command_finished_helper_emits_success_notification(monkeypatch) -> None:
    calls = []
    sent_result = NotificationResult(
        message="sent",
        severity=AlertSeverity.INFO,
        enabled=False,
        configured=False,
        final_status="skipped_disabled",
    )

    monkeypatch.setattr(research_cli, "monotonic", lambda: 12.5)
    monkeypatch.setattr(
        research_cli,
        "notify",
        lambda msg, *, severity, event_name=None, policy=None, source_command=None: calls.append((msg, severity))
        or sent_result,
    )
    monkeypatch.setattr(research_cli, "_record_notification_result", lambda *args, **kwargs: None)

    result = research_cli._notify_research_command_finished(
        "research-backtest",
        10.0,
        0,
        manifest="manifest.json",
    )

    assert calls == [
        (
            "event=research_command_finished command=research-backtest status=success "
            "exit_code=0 elapsed_sec=2.5 manifest=manifest.json",
            AlertSeverity.INFO,
        )
    ]
    assert result is sent_result


def test_research_command_finished_exposes_notification_result(monkeypatch) -> None:
    expected = NotificationResult(
        message="sent",
        severity=AlertSeverity.INFO,
        enabled=True,
        configured=True,
        attempted_transports=("ntfy",),
        delivered_transports=("ntfy",),
        final_status="delivered",
    )

    monkeypatch.setattr(research_cli, "monotonic", lambda: 12.5)
    monkeypatch.setattr(
        research_cli,
        "notify",
        lambda msg, *, severity, event_name=None, policy=None, source_command=None: expected,
    )
    monkeypatch.setattr(research_cli, "_record_notification_result", lambda *args, **kwargs: None)

    result = research_cli._notify_research_command_finished("research-backtest", 10.0, 0)

    assert result is expected


def test_research_backtest_notifies_on_success_and_failure(monkeypatch) -> None:
    calls = []
    sent_result = NotificationResult(
        message="sent",
        severity=AlertSeverity.INFO,
        enabled=False,
        configured=False,
        final_status="skipped_disabled",
    )

    monkeypatch.setattr(research_cli, "load_manifest", lambda path: SimpleNamespace(deployment_tier="research_only"))
    monkeypatch.setattr(
        research_cli,
        "notify",
        lambda msg, *, severity, event_name=None, policy=None, source_command=None: calls.append((msg, severity))
        or sent_result,
    )
    monkeypatch.setattr(research_cli, "_record_notification_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_cli, "_print_report_summary", lambda label, report: None)
    monkeypatch.setattr(research_cli, "monotonic", lambda: 10.0)
    monkeypatch.setattr(
        research_cli,
        "run_research_backtest",
        lambda **kwargs: {
            "deployment_tier": "research_only",
            "promotion_eligibility_gate_result": "FAIL",
            "promotion_blocking_reasons": ["diagnostic_failure"],
            "diagnostic_only": True,
        },
    )

    assert research_cli.cmd_research_backtest(manifest_path="manifest.json") == 0

    monkeypatch.setattr(research_cli, "load_manifest", lambda path: (_ for _ in ()).throw(ValueError("bad manifest")))

    assert research_cli.cmd_research_backtest(manifest_path="bad.json") == 1
    assert calls[0][1] == AlertSeverity.INFO
    assert "command=research-backtest status=success exit_code=0" in calls[0][0]
    assert calls[1][1] == AlertSeverity.WARN
    assert "command=research-backtest status=failure exit_code=1" in calls[1][0]


def test_pass_report_summary_sets_promotion_allowed() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[_candidate("candidate_001", gate="PASS")],
            best_candidate_id="candidate_001",
            gate_result="PASS",
        )
    )

    assert summary.promotion_allowed is True
    assert summary.next_action == "review_promotion_candidate"


def test_statistical_gate_failure_forces_promotion_disallowed() -> None:
    report = _report(
        candidates=[_candidate("candidate_001", gate="PASS")],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )
    report.update(
        {
            "statistical_validation_required": True,
            "statistical_gate_result": "FAIL",
            "statistical_gate_fail_reasons": ["reality_check_p_value_failed"],
        }
    )

    summary = build_research_run_summary(report)

    assert summary.promotion_allowed is False
    assert summary.next_action == "do_not_promote_review_statistical_selection"


def test_research_run_summary_final_selection_warn_not_promotion_allowed(capsys) -> None:
    report = _report(
        candidates=[_candidate("candidate_001", gate="PASS")],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )
    report.update(
        {
            "final_selection_required": False,
            "final_selection_gate_result": "WARN",
            "final_selection_fail_reasons": ["legacy_implicit_final_rank_policy_v1"],
            "promotion_eligibility_gate_result": "PASS",
        }
    )

    summary = build_research_run_summary(report)

    assert summary.promotion_allowed is False
    assert summary.next_action == "do_not_promote_review_final_selection_contract"

    _print_report_summary("RESEARCH-BACKTEST", report)
    output = capsys.readouterr().out
    assert "promotion_allowed=0" in output
    assert "final_selection_gate_result=WARN" in output
    assert "candidate_final_scores_hash=none" in output
    assert "next_action=do_not_promote_review_final_selection_contract" in output


def test_fail_report_summary_sets_promotion_disallowed() -> None:
    summary = build_research_run_summary(
        _report(candidates=[_candidate("candidate_001", fail_reasons=["profit_factor_failed"])])
    )

    assert summary.promotion_allowed is False


def test_standalone_backtest_summary_discloses_not_full_validation(capsys) -> None:
    report = _report(
        candidates=[_candidate("candidate_001", gate="PASS")],
        best_candidate_id="candidate_001",
        gate_result="FAIL",
    )
    report.update(
        {
            "promotion_eligibility_gate_result": "FAIL",
            "promotion_blocking_reasons": ["walk_forward_required_but_not_executed_in_this_run"],
            "validation_run_complete": False,
            "diagnostic_only": True,
            "standalone_backtest_not_full_validation": True,
            "next_required_stage": "research-walk-forward",
        }
    )

    _print_report_summary("RESEARCH-BACKTEST", report)
    output = capsys.readouterr().out

    assert "validation_run_complete=0" in output
    assert "diagnostic_only=1" in output
    assert "next_required_stage=research-walk-forward" in output
    assert "reason=standalone_backtest_not_full_validation" in output
    assert "promotion_allowed=0" in output


def test_candidate_gate_counts_are_computed_with_unknown_labels() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[
                _candidate("candidate_001", gate="PASS"),
                _candidate("candidate_002", gate="FAIL"),
                _candidate("candidate_003", gate="REVIEW"),
                {"parameter_candidate_id": "candidate_004"},
            ]
        )
    )

    assert summary.candidate_gate_counts == {"PASS": 1, "FAIL": 1, "REVIEW": 1, "UNKNOWN": 1}


def test_walk_forward_report_summary_includes_window_summary() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[
                _candidate(
                    "candidate_001",
                    fail_reasons=["walk_forward_failed"],
                    walk_forward_metrics={
                        "window_count": 5,
                        "pass_window_count": 1,
                        "fail_window_count": 4,
                        "windows": [],
                    },
                )
            ]
        )
    )

    assert summary.walk_forward_window_count == 5
    assert summary.walk_forward_pass_window_count == 1
    assert summary.walk_forward_fail_window_count == 4


def test_walk_forward_report_summary_includes_top_window_fail_reasons() -> None:
    summary = build_research_run_summary(
        _report(
            candidates=[
                _candidate(
                    "candidate_001",
                    fail_reasons=["walk_forward_failed"],
                    walk_forward_metrics={
                        "window_count": 2,
                        "pass_window_count": 0,
                        "fail_window_count": 2,
                        "windows": [
                            {"gate_result": "FAIL", "fail_reasons": ["test_metrics_gate_incompatible"]},
                            {
                                "gate_result": "FAIL",
                                "fail_reasons": ["test_metrics_gate_incompatible", "test_return_not_positive"],
                            },
                        ],
                    },
                )
            ]
        )
    )

    assert summary.top_window_fail_reasons == {
        "test_metrics_gate_incompatible": 2,
        "test_return_not_positive": 1,
    }


def test_empty_or_malformed_candidate_sections_do_not_crash() -> None:
    empty_summary = build_research_run_summary(_report(candidates=[]))
    malformed_summary = build_research_run_summary(_report(candidates={"not": "a-list"}))
    mixed_summary = build_research_run_summary(_report(candidates=["bad-row", {"gate_fail_reasons": "bad"}]))

    assert empty_summary.candidate_gate_counts == {}
    assert empty_summary.next_action == "inspect_dataset_or_manifest"
    assert malformed_summary.candidate_gate_counts == {}
    assert mixed_summary.candidate_gate_counts == {"PASS": 0, "FAIL": 0, "UNKNOWN": 1}


def test_best_candidate_remains_none_when_all_candidates_fail() -> None:
    report = _report(candidates=[_candidate("candidate_001", fail_reasons=["profit_factor_failed"])])
    summary = build_research_run_summary(report)

    assert report["best_candidate_id"] is None
    assert summary.promotion_allowed is False


def test_nearest_failed_candidate_is_separate_from_best_candidate() -> None:
    report = _report(
        candidates=[
            _candidate("candidate_004_1e322c70", fail_reasons=["profit_factor_failed", "walk_forward_missing"])
        ]
    )
    summary = build_research_run_summary(report)

    assert report["best_candidate_id"] is None
    assert summary.nearest_failed_candidate_id == "candidate_004_1e322c70"
    assert summary.nearest_failed_candidate_fail_reasons == ("profit_factor_failed", "walk_forward_missing")


def test_next_action_is_conservative_for_failed_reports() -> None:
    walk_missing = build_research_run_summary(
        _report(candidates=[_candidate("candidate_001", fail_reasons=["walk_forward_missing"])])
    )
    walk_failed = build_research_run_summary(
        _report(candidates=[_candidate("candidate_001", fail_reasons=["walk_forward_failed"])])
    )
    strategy_failed = build_research_run_summary(
        _report(candidates=[_candidate("candidate_001", fail_reasons=["profit_factor_failed"])])
    )

    assert walk_missing.next_action == "run_walk_forward_before_promotion"
    assert walk_failed.next_action == "do_not_promote_review_walk_forward_windows"
    assert strategy_failed.next_action == "do_not_promote_revise_strategy_hypothesis"
    assert strategy_failed.next_action != "review_promotion_candidate"


def test_summary_surfaces_strategy_diagnostics_and_entry_exit_next_action(capsys) -> None:
    report = _report(
        candidates=[
            _candidate(
                "candidate_001",
                fail_reasons=["diagnostic_review_required"],
            )
        ],
        gate_result="FAIL",
    )
    report["candidates"][0]["validation_strategy_diagnostics"] = {
        "raw_sell_filter_blocked_while_in_position_count": 2,
        "exit_reason_distribution": {"opposite_cross": 3, "max_holding_time": 1},
        "p95_mae_pct": -1.5,
        "worst_trade_mae_pct": -3.0,
    }
    report["candidates"][0]["final_holdout_strategy_diagnostics"] = {
        "raw_sell_filter_blocked_while_in_position_count": 1,
        "exit_reason_distribution": {"opposite_cross": 2},
        "p95_mae_pct": -2.0,
        "worst_trade_mae_pct": -4.0,
    }

    summary = build_research_run_summary(report)

    assert summary.top_exit_reasons == {"opposite_cross": 5, "max_holding_time": 1}
    assert summary.validation_raw_sell_filter_blocked_while_in_position_count == 2
    assert summary.final_holdout_raw_sell_filter_blocked_while_in_position_count == 1
    assert summary.validation_p95_mae_pct == -1.5
    assert summary.final_holdout_worst_trade_mae_pct == -4.0
    assert summary.next_action == "review_entry_exit_channel_diagnostics"

    _print_report_summary("RESEARCH-BACKTEST", report)
    output = capsys.readouterr().out

    assert "strategy_diagnostics_summary=" in output
    assert "top_exit_reasons=opposite_cross:5,max_holding_time:1" in output
    assert "validation_raw_sell_filter_blocked_while_in_position_count=2" in output
    assert "final_holdout_raw_sell_filter_blocked_while_in_position_count=1" in output
    assert "validation_p95_mae_pct=-1.5" in output
    assert "final_holdout_worst_trade_mae_pct=-4.0" in output
    assert "next_action=review_entry_exit_channel_diagnostics" in output


def test_cli_summary_prints_non_null_strategy_diagnostics_summary(capsys) -> None:
    report = _report(candidates=[_candidate("candidate_001")], gate_result="FAIL")
    report["candidates"][0]["validation_strategy_diagnostics"] = {
        "raw_signal_count": 2,
        "final_signal_count": 1,
        "blocked_filter_distribution": {"volume_ratio_below_min": 1},
        "exit_reason_distribution": {"take_profit": 1},
        "strategy_diagnostics_namespace": "channel_breakout_with_regime_filter",
    }

    _print_report_summary("RESEARCH-BACKTEST", report)
    output = capsys.readouterr().out

    assert "strategy_diagnostics_summary=" in output
    assert "top_exit_reasons=take_profit:1" in output


def test_strategy_diagnostics_summary_uses_contract_counts(capsys) -> None:
    report = _report(candidates=[_candidate("candidate_001")], gate_result="FAIL")
    report["candidates"][0]["validation_strategy_diagnostics"] = {
        "raw_signal_count": 2,
        "final_signal_count": 1,
        "entry_count": 1,
        "blocked_filter_distribution": {"volume_ratio_below_min": 1},
        "exit_reason_distribution": {"max_holding_time": 1},
        "strategy_diagnostics_namespace": "channel_breakout_with_regime_filter",
    }

    _print_report_summary("RESEARCH-BACKTEST", report)
    output = capsys.readouterr().out

    assert "top_exit_reasons=max_holding_time:1" in output


def test_exploratory_summary_prints_diagnostic_only(capsys) -> None:
    report = _report(candidates=[_candidate("candidate_001", gate="PASS")], gate_result="PASS")
    report["diagnostic_mode"] = "exploratory"
    report["diagnostic_only"] = True
    report["best_candidate_id"] = "candidate_001"

    summary = build_research_run_summary(report)
    _print_report_summary("RESEARCH-BACKTEST", report)
    output = capsys.readouterr().out

    assert summary.promotion_allowed is False
    assert "diagnostic_only=1" in output
    assert "diagnostic_mode=exploratory" in output
    assert "promotion_allowed=0" in output
    assert "next_action=revise_hypothesis_from_exploratory_diagnostics" in output


def test_research_only_strategy_summary_marks_not_live_eligible(capsys) -> None:
    report = _report(candidates=[_candidate("candidate_001")], gate_result="FAIL")
    report["candidates"][0]["strategy_runtime_capabilities"] = {
        "research_only": True,
        "promotion_runtime_decisions_supported": False,
        "runtime_replay_supported": False,
        "live_dry_run_allowed": False,
        "live_real_order_allowed": False,
        "fail_closed_reason": "promotion_extension_missing",
    }
    report["candidates"][0]["promotion_interpretation"] = "research_only_not_live_eligible"

    _print_report_summary("RESEARCH-BACKTEST", report)
    output = capsys.readouterr().out

    assert "research_only_not_live_eligible=1" in output
    assert '"research_only": true' in output


def test_summary_uses_none_for_missing_final_holdout_diagnostics(capsys) -> None:
    report = _report(candidates=[_candidate("candidate_001")], gate_result="FAIL")
    report["candidates"][0]["validation_strategy_diagnostics"] = {
        "raw_sell_filter_blocked_while_in_position_count": 1,
        "exit_reason_distribution": {"opposite_cross": 1},
        "p95_mae_pct": -1.0,
        "worst_trade_mae_pct": -1.5,
    }

    summary = build_research_run_summary(report)

    assert summary.final_holdout_raw_sell_filter_blocked_while_in_position_count is None
    assert summary.final_holdout_p95_mae_pct is None
    assert summary.final_holdout_worst_trade_mae_pct is None

    _print_report_summary("RESEARCH-BACKTEST", report)
    output = capsys.readouterr().out

    assert "final_holdout_raw_sell_filter_blocked_while_in_position_count=None" in output
    assert "final_holdout_p95_mae_pct=None" in output
    assert "final_holdout_worst_trade_mae_pct=None" in output


def test_print_report_summary_renders_operator_diagnostics(capsys) -> None:
    report = _report(
        candidates=[
            _candidate(
                "candidate_004_1e322c70",
                fail_reasons=["profit_factor_failed", "walk_forward_failed"],
                walk_forward_metrics={
                    "window_count": 5,
                    "pass_window_count": 0,
                    "fail_window_count": 5,
                    "windows": [
                        {"gate_result": "FAIL", "fail_reasons": ["test_metrics_gate_incompatible"]},
                    ],
                },
            )
        ]
    )

    _print_report_summary("RESEARCH-WALK-FORWARD", report)

    output = capsys.readouterr().out
    assert "candidate_gate_counts=PASS:0,FAIL:1" in output
    assert "top_fail_reasons=profit_factor_failed:1,walk_forward_failed:1" in output
    assert "promotion_allowed=0" in output
    assert "best_candidate_id=none" in output
    assert "nearest_failed_candidate_id=candidate_004_1e322c70" in output
    assert "walk_forward_window_summary=window_count:5,pass:0,fail:5" in output
    assert "top_window_fail_reasons=test_metrics_gate_incompatible:1" in output
    assert "next_action=do_not_promote_review_walk_forward_windows" in output


def test_print_report_summary_renders_statistical_selection_diagnostics(capsys) -> None:
    report = _report(
        candidates=[_candidate("candidate_001", gate="PASS")],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )
    report.update(
        {
            "statistical_validation_required": True,
            "parameter_grid_size": 5000,
            "search_budget": 5000,
            "attempt_index": 3,
            "holdout_reuse_count": 2,
            "selection_universe_hash": "sha256:selection",
            "statistical_evidence_hash": "sha256:evidence",
            "experiment_registry_bound_evidence_hash": "sha256:pre-completion",
            "experiment_registry_evidence_hash_phase": "pre_completion_evidence_hash",
            "final_holdout_identity_hash": "sha256:identity",
            "final_holdout_content_hash": "sha256:content",
            "final_holdout_reuse_key_hash": "sha256:identity",
            "white_reality_check_p_value": 0.2,
            "statistical_gate_result": "FAIL",
            "statistical_gate_fail_reasons": ["reality_check_p_value_failed"],
        }
    )

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "statistical_validation_required=1" in output
    assert "statistical_parameter_grid_size=5000" in output
    assert "statistical_search_budget=5000" in output
    assert "statistical_attempt_index=3" in output
    assert "statistical_holdout_reuse_count=2" in output
    assert "selection_universe_hash=sha256:selection" in output
    assert "statistical_evidence_hash=sha256:evidence" in output
    assert "experiment_registry_bound_evidence_hash=sha256:pre-completion" in output
    assert "experiment_registry_evidence_hash_phase=pre_completion_evidence_hash" in output
    assert "final_holdout_identity_hash=sha256:identity" in output
    assert "final_holdout_content_hash=sha256:content" in output
    assert "final_holdout_reuse_key_hash=sha256:identity" in output
    assert "white_reality_check_p_value=0.2" in output
    assert "statistical_gate_result=FAIL" in output
    assert "statistical_gate_fail_reasons=reality_check_p_value_failed" in output
    assert "promotion_allowed=0" in output
    assert "next_action=do_not_promote_review_statistical_selection" in output


def test_cli_summary_prints_identity_and_content_holdout_hashes(capsys) -> None:
    report = _report(candidates=[_candidate("candidate_001", gate="PASS")], best_candidate_id="candidate_001")
    report.update(
        {
            "final_holdout_identity_hash": "sha256:identity",
            "final_holdout_content_hash": "sha256:content",
            "final_holdout_reuse_key_hash": "sha256:identity",
        }
    )

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "final_holdout_identity_hash=sha256:identity" in output
    assert "final_holdout_content_hash=sha256:content" in output
    assert "final_holdout_reuse_key_hash=sha256:identity" in output


def test_cli_summary_prints_evidence_binding_phase_and_bound_hash(capsys) -> None:
    report = _report(candidates=[_candidate("candidate_001", gate="PASS")], best_candidate_id="candidate_001")
    report.update(
        {
            "experiment_registry_bound_evidence_hash": "sha256:pre-completion",
            "experiment_registry_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        }
    )

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "experiment_registry_bound_evidence_hash=sha256:pre-completion" in output
    assert "experiment_registry_evidence_hash_phase=pre_completion_evidence_hash" in output


def test_research_registry_validate_reports_validation_scope(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reservation = reserve_research_attempt(manager=manager, base_payload=_registry_payload())
    append_attempt_completion(
        manager=manager,
        reservation=reservation,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:evidence",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )

    status = research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    assert status == 0
    assert payload["validation_scope"] == "registry_only"
    assert payload["artifact_binding_valid"] == "unknown"
    assert payload["warning"] == "artifact_binding_not_checked"
    assert payload["results"][0]["report_loaded"] is False


def test_research_registry_validate_registry_only_does_not_claim_artifact_binding(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reserve_research_attempt(manager=manager, base_payload=_registry_payload())

    research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    assert payload["validation_scope"] == "registry_only"
    assert payload["artifact_binding_valid"] == "unknown"
    assert payload["results"][0]["artifact_binding_valid"] == "unknown"
    assert payload["registry_lifecycle_summary"][0]["artifact_bound"] is False


def test_registry_validate_registry_only_reports_lifecycle_summary_without_artifact_claims(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    first = reserve_research_attempt(manager=manager, base_payload=_registry_payload())
    second = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_retry"))
    append_attempt_completion(
        manager=manager,
        reservation=second,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )

    status = research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    assert status == 0
    assert payload["validation_scope"] == "registry_only"
    assert payload["artifact_binding_valid"] == "unknown"
    assert payload["warning"] == "artifact_binding_not_checked"
    assert [row["row_hash"] for row in payload["registry_lifecycle_summary"]] == [first["row_hash"], second["row_hash"]]
    assert all(row["artifact_bound"] is False for row in payload["registry_lifecycle_summary"])
    assert payload["registry_lifecycle_summary"][0]["incomplete"] is True
    assert payload["registry_lifecycle_summary"][0]["ok"] is False
    assert payload["registry_lifecycle_summary"][0]["row_valid_only"] is True
    assert payload["registry_lifecycle_summary"][0]["lifecycle_complete"] is False
    assert payload["registry_lifecycle_summary"][1]["promotion_permitted"] is True
    assert payload["registry_lifecycle_summary"][1]["lifecycle_complete"] is True
    assert payload["registry_lifecycle_summary"][1]["ok"] is True


def test_registry_lifecycle_summary_incomplete_row_ok_false(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reserve_research_attempt(manager=manager, base_payload=_registry_payload())

    research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    row = payload["registry_lifecycle_summary"][0]
    assert row["registry_row_valid"] is True
    assert row["completion_row_valid"] is True
    assert row["lifecycle_complete"] is False
    assert row["promotion_permitted"] is False
    assert row["ok"] is False
    assert "experiment_registry_incomplete_attempt" in row["reasons"]


def test_registry_lifecycle_summary_incomplete_row_row_valid_only_true(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reserve_research_attempt(manager=manager, base_payload=_registry_payload())

    research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    row = payload["registry_lifecycle_summary"][0]
    assert row["row_valid_only"] is True
    assert row["registry_row_valid"] is True
    assert row["lifecycle_complete"] is False


def test_registry_lifecycle_summary_completed_row_lifecycle_complete_true(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reservation = reserve_research_attempt(manager=manager, base_payload=_registry_payload())
    append_attempt_completion(
        manager=manager,
        reservation=reservation,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )

    research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    row = payload["registry_lifecycle_summary"][0]
    assert row["registry_row_valid"] is True
    assert row["completion_row_valid"] is True
    assert row["lifecycle_complete"] is True
    assert row["promotion_permitted"] is True
    assert row["row_valid_only"] is False
    assert row["ok"] is True


def test_research_registry_validate_full_scope_detects_evidence_binding_mismatch(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reservation = reserve_research_attempt(manager=manager, base_payload=_registry_payload())
    completion = append_attempt_completion(
        manager=manager,
        reservation=reservation,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    _write_registry_validate_artifacts(
        manager,
        reservation,
        completion,
        evidence_bound_hash="sha256:other",
        write_evidence=True,
        write_panel=False,
    )

    status = research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    assert status == 1
    assert payload["validation_scope"] == "registry_and_artifacts"
    assert payload["artifact_binding_valid"] is False
    assert "experiment_registry_statistical_evidence_hash_mismatch" in payload["results"][0]["reasons"]


def test_research_registry_validate_full_scope_detects_missing_evidence(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reservation = reserve_research_attempt(manager=manager, base_payload=_registry_payload())
    completion = append_attempt_completion(
        manager=manager,
        reservation=reservation,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    _write_registry_validate_artifacts(manager, reservation, completion, write_evidence=False, write_panel=False)

    status = research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    assert status == 1
    assert payload["report_loaded"] is True
    assert payload["evidence_loaded"] is False
    assert "statistical_evidence_missing" in payload["results"][0]["reasons"]


def test_research_registry_validate_full_scope_reports_loaded_artifacts(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reservation = reserve_research_attempt(manager=manager, base_payload=_registry_payload())
    completion = append_attempt_completion(
        manager=manager,
        reservation=reservation,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    _write_registry_validate_artifacts(manager, reservation, completion, write_evidence=True, write_panel=True)

    research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    assert payload["validation_scope"] == "registry_and_artifacts"
    assert payload["report_loaded"] is True
    assert payload["evidence_loaded"] is True
    assert payload["return_panel_loaded"] is True


def test_registry_validate_full_scope_validates_only_artifact_bound_row(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    old = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_old"))
    current = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_current"))
    completion = append_attempt_completion(
        manager=manager,
        reservation=current,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    _write_registry_validate_artifacts(manager, current, completion, write_evidence=False, write_panel=False, statistical_required=False)

    research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    rows = payload["registry_lifecycle_summary"]
    assert payload["artifact_bound_row_hash"] == current["row_hash"]
    assert [row["artifact_bound"] for row in rows] == [False, True]
    assert rows[0]["row_hash"] == old["row_hash"]
    assert rows[0]["artifact_binding_valid"] == "unknown"
    assert "experiment_registry_statistical_evidence_hash_mismatch" not in rows[0]["reasons"]


def test_registry_validate_full_scope_reports_extra_incomplete_reservations_separately(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_old"))
    current = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_current"))
    completion = append_attempt_completion(
        manager=manager,
        reservation=current,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    _write_registry_validate_artifacts(manager, current, completion, write_evidence=False, write_panel=False, statistical_required=False)

    research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["registry_lifecycle_summary"][0]["incomplete"] is True
    assert payload["registry_lifecycle_summary"][0]["promotion_permitted"] is False
    assert payload["registry_lifecycle_summary"][0]["ok"] is False
    assert payload["registry_lifecycle_summary"][0]["row_valid_only"] is True
    assert payload["registry_lifecycle_summary"][1]["promotion_permitted"] is True
    assert payload["registry_lifecycle_summary"][1]["ok"] is True


def test_registry_validate_extra_incomplete_non_bound_row_does_not_fail_artifact_bound_validation(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    old = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_old"))
    current = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_current"))
    completion = append_attempt_completion(
        manager=manager,
        reservation=current,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    _write_registry_validate_artifacts(
        manager,
        current,
        completion,
        write_evidence=False,
        write_panel=False,
        statistical_required=False,
    )

    status = research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    rows = payload["registry_lifecycle_summary"]
    assert status == 0
    assert payload["ok"] is True
    assert payload["artifact_binding_valid"] is True
    assert rows[0]["row_hash"] == old["row_hash"]
    assert rows[0]["artifact_bound"] is False
    assert rows[0]["ok"] is False
    assert rows[0]["row_valid_only"] is True
    assert rows[1]["artifact_bound"] is True
    assert rows[1]["ok"] is True
    assert rows[1]["artifact_binding_valid"] is True


def test_registry_validate_artifact_bound_incomplete_row_fails(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reservation = reserve_research_attempt(manager=manager, base_payload=_registry_payload())
    _write_registry_validate_artifacts(
        manager,
        reservation,
        {"row_hash": None},
        write_evidence=False,
        write_panel=False,
        statistical_required=False,
    )

    status = research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    row = payload["registry_lifecycle_summary"][0]
    assert status == 1
    assert payload["ok"] is False
    assert payload["artifact_binding_valid"] is False
    assert row["artifact_bound"] is True
    assert row["registry_row_valid"] is True
    assert row["completion_row_valid"] is True
    assert row["lifecycle_complete"] is False
    assert row["promotion_permitted"] is False
    assert row["ok"] is False
    assert row["row_valid_only"] is True
    assert "experiment_registry_incomplete_attempt" in row["reasons"]


def test_registry_validate_full_scope_does_not_mark_old_rows_ok_using_current_evidence(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    old = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_old"))
    append_attempt_completion(
        manager=manager,
        reservation=old,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:old",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    current = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_current"))
    completion = append_attempt_completion(
        manager=manager,
        reservation=current,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    _write_registry_validate_artifacts(manager, current, completion, write_evidence=False, write_panel=False, statistical_required=False)

    research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    old_summary = payload["registry_lifecycle_summary"][0]
    assert old_summary["row_hash"] == old["row_hash"]
    assert old_summary["artifact_bound"] is False
    assert old_summary["artifact_binding_valid"] == "unknown"


def test_registry_validate_full_scope_reports_artifact_bound_row_hash(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    reservation = reserve_research_attempt(manager=manager, base_payload=_registry_payload())
    completion = append_attempt_completion(
        manager=manager,
        reservation=reservation,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    _write_registry_validate_artifacts(manager, reservation, completion, write_evidence=False, write_panel=False, statistical_required=False)

    research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_bound_row_hash"] == reservation["row_hash"]
    assert payload["registry_lifecycle_summary"][0]["artifact_bound"] is True


def test_registry_validate_full_scope_fails_when_report_and_evidence_bound_rows_disagree(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    report_reservation = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_report"))
    evidence_reservation = reserve_research_attempt(manager=manager, base_payload=_registry_payload_with(run_id="summary_exp_evidence"))
    completion = append_attempt_completion(
        manager=manager,
        reservation=report_reservation,
        updates={
            "candidate_count": 1,
            "return_panel_hash": "sha256:return",
            "statistical_evidence_hash": "sha256:pre-completion",
            "statistical_evidence_hash_phase": EXPERIMENT_REGISTRY_EVIDENCE_HASH_PHASE,
        },
    )
    _write_registry_validate_artifacts(
        manager,
        report_reservation,
        completion,
        evidence_row_hash=str(evidence_reservation["row_hash"]),
        write_evidence=True,
        write_panel=False,
    )

    status = research_cli.cmd_research_registry_validate(experiment_id="summary_exp")

    payload = json.loads(capsys.readouterr().out)
    assert status == 1
    assert payload["artifact_binding_valid"] is False
    assert "experiment_registry_report_evidence_row_hash_mismatch" in payload["artifact_reasons"]


def test_print_report_summary_renders_execution_capability_diagnostics(capsys) -> None:
    report = _report(
        candidates=[_candidate("candidate_001", gate="PASS")],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )
    report.update(
        {
            "execution_capability_contract_hash": "sha256:capability",
            "evidence_tier": "top_of_book_after_decision",
            "unavailable_required_capabilities": ["market_impact_model"],
            "market_impact_required": True,
            "market_impact_model_available": False,
            "top_of_book_is_full_depth": False,
        }
    )

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "execution_capability_contract_hash=sha256:capability" in output
    assert "evidence_tier=top_of_book_after_decision" in output
    assert "unavailable_required_capabilities=market_impact_model" in output
    assert "market_impact_required=True" in output
    assert "market_impact_model_available=False" in output
    assert "top_of_book_is_full_depth=False" in output
    assert "execution_capability_next_action=remove unsupported requirements or add implemented evidence/model support" in output


def test_promote_candidate_summary_renders_execution_capability_diagnostics(capsys, monkeypatch) -> None:
    artifact = {
        "gate_result": "PASS",
        "statistical_validation_required": False,
        "validation_policy_source": "repo_research_validation_policy_v1",
        "validation_policy_required_stage_names": ["readiness", "backtest", "promotion"],
        "effective_walk_forward_required": False,
        "effective_final_holdout_required": False,
        "effective_stress_suite_required": False,
        "effective_statistical_validation_required": False,
        "effective_final_selection_required": True,
        "promotion_grade_limitations": [],
        "promotion_blocking_reasons": [],
        "execution_capability_contract_hash": "sha256:capability",
        "evidence_tier": "candle_next_open",
        "unavailable_required_capabilities": [],
        "market_impact_required": False,
        "market_impact_model_available": False,
        "top_of_book_is_full_depth": False,
        "operator_next_step": "review",
    }
    monkeypatch.setattr(
        research_cli,
        "promote_candidate",
        lambda **_: SimpleNamespace(
            artifact=artifact,
            artifact_path=Path("/tmp/promotion.json"),
            content_hash="sha256:promotion",
        ),
    )

    rc = research_cli.cmd_research_promote_candidate(experiment_id="exp", candidate_id="candidate")

    output = capsys.readouterr().out
    assert rc == 0
    assert "execution_capability_contract_hash=sha256:capability" in output
    assert "evidence_tier=candle_next_open" in output
    assert "unavailable_required_capabilities=none" in output
    assert "market_impact_required=False" in output
    assert "market_impact_model_available=False" in output
    assert "top_of_book_is_full_depth=False" in output
    assert "validation_policy_source=repo_research_validation_policy_v1" in output
    assert "validation_policy_required_stage_names=readiness,backtest,promotion" in output
    assert "effective_walk_forward_required=0" in output
    assert "effective_final_holdout_required=0" in output
    assert "effective_stress_suite_required=0" in output
    assert "effective_statistical_validation_required=0" in output
    assert "effective_final_selection_required=1" in output


def test_print_report_summary_renders_stress_suite_diagnostics(capsys) -> None:
    report = _report(
        candidates=[_candidate("candidate_001", gate="FAIL")],
        best_candidate_id=None,
        gate_result="FAIL",
    )
    report.update(
        {
            "stress_suite_required": True,
            "stress_suite_gate_result": "FAIL",
            "stress_suite_fail_reasons": ["stress_trade_removal_return_retention_failed"],
            "best_validation_stress_suite": {
                "trade_removal": {"status": "FAIL"},
                "period_ablation": {"status": "PASS", "pass_ratio": 1.0},
                "parameter_perturbation": {"status": "FAIL", "pass_ratio": 0.5},
                "trade_order_monte_carlo": {
                    "survival_probability": 0.972,
                    "max_drawdown_pct_p95": 31.2,
                },
            },
        }
    )

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "stress_suite_required=1" in output
    assert "stress_suite_gate_result=FAIL" in output
    assert "stress_suite_fail_reasons=stress_trade_removal_return_retention_failed" in output
    assert "stress_trade_removal_status=FAIL" in output
    assert "stress_period_ablation_status=PASS" in output
    assert "stress_period_ablation_pass_ratio=1.0" in output
    assert "stress_parameter_perturbation_status=FAIL" in output
    assert "stress_parameter_perturbation_pass_ratio=0.5" in output
    assert "stress_monte_carlo_survival_probability=0.972" in output
    assert "stress_monte_carlo_max_drawdown_pct_p95=31.2" in output


def test_all_stress_failed_report_summary_stays_fail_closed(capsys) -> None:
    report = _report(
        candidates=[
            _candidate(
                "candidate_001",
                gate="FAIL",
                fail_reasons=["stress_suite_gate_not_passed"],
            )
        ],
        best_candidate_id=None,
        gate_result="FAIL",
    )
    report.update(
        {
            "stress_suite_required": True,
            "stress_suite_gate_result": "FAIL",
            "stress_suite_fail_reasons": ["stress_monte_carlo_survival_probability_failed"],
            "best_validation_stress_suite": {
                "trade_removal": {"status": "PASS"},
                "trade_order_monte_carlo": {
                    "survival_probability": 0.2,
                    "max_drawdown_pct_p95": 91.0,
                },
            },
        }
    )

    summary = build_research_run_summary(report)
    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert summary.promotion_allowed is False
    assert "promotion_allowed=0" in output
    assert "stress_suite_gate_result=FAIL" in output
    assert "stress_suite_fail_reasons=stress_monte_carlo_survival_probability_failed" in output


def test_print_report_summary_handles_missing_optional_stress_suite(capsys) -> None:
    _print_report_summary("RESEARCH-BACKTEST", _report(candidates=[]))

    output = capsys.readouterr().out
    assert "stress_suite_required=0" in output
    assert "stress_suite_gate_result=none" in output
    assert "stress_trade_removal_status=none" in output


def test_print_report_summary_renders_metrics_v2_for_passing_candidate(capsys) -> None:
    report = _report(
        candidates=[
            {
                **_candidate("candidate_001", gate="PASS"),
                "validation_metrics_v2": {
                    "metrics_schema_version": 2,
                    "return_risk": {"cagr_pct": 12.5, "open_position_at_end": False},
                    "trade_quality": {"expectancy_per_trade_krw": 250.0},
                    "time_exposure": {"exposure_time_pct": 25.0, "avg_holding_time_ms": 600000.0},
                    "cost_execution": {
                        "fee_drag_ratio": 0.001,
                        "fee_drag_ratio_basis": "traded_notional",
                        "slippage_drag_ratio": 0.002,
                        "slippage_drag_ratio_basis": "traded_notional",
                    },
                },
            }
        ],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "metrics_v2_summary=schema=2 cagr_pct=12.5 expectancy_per_trade_krw=250.0" in output
    assert "exposure_time_pct=25.0 avg_holding_time_ms=600000.0 open_position_at_end=False" in output
    assert "fee_drag_ratio=0.001 fee_drag_ratio_basis=traded_notional" in output
    assert "slippage_drag_ratio=0.002 slippage_drag_ratio_basis=traded_notional" in output


def test_print_report_summary_renders_top_of_book_warning_context(capsys) -> None:
    report = _report(
        candidates=[
            _candidate(
                "candidate_001",
                gate="PASS",
                fail_reasons=[],
            )
        ],
        best_candidate_id="candidate_001",
        gate_result="PASS",
    )
    report["warnings"] = ["top_of_book_optional_coverage_warning"]
    report["top_of_book_quality_summary"] = {
        "requested": True,
        "required": False,
        "gate_status": "WARN",
        "coverage_pct": 50.0,
        "joined_quote_count": 10,
        "missing_quote_count": 10,
        "join_tolerance_ms": 3000,
        "affected_splits": [
            {"split_name": "validation", "top_of_book_missing_count": 10},
        ],
        "next_action": (
            "collect orderbook top snapshots with sync-orderbook-top, rerun research-backtest, "
            "and verify top_of_book_coverage_pct"
        ),
    }

    _print_report_summary("RESEARCH-BACKTEST", report)

    output = capsys.readouterr().out
    assert "warnings=top_of_book_optional_coverage_warning" in output
    assert "top_of_book_quote_coverage=requested=1 required=0 gate_status=WARN coverage_pct=50.0" in output
    assert "joined_count=10 missing_count=10 join_tolerance_ms=3000 affected_splits=validation" in output
    assert (
        "top_of_book_limitations=best_bid_ask_only_not_full_depth,"
        "intra_candle_path_unavailable"
    ) in output
    assert (
        "top_of_book_next_action=collect orderbook top snapshots with sync-orderbook-top, "
        "rerun research-backtest, and verify top_of_book_coverage_pct"
    ) in output


def test_research_backtest_progress_lines_are_operator_visible(capsys) -> None:
    _print_research_backtest_progress(
        {
            "stage": "start",
            "manifest_hash": "sha256:manifest",
            "db_path": "/runtime/data/paper/trades/paper.sqlite",
        }
    )
    _print_research_backtest_progress({"stage": "load_split", "split": "train", "candles": 4320})
    _print_research_backtest_progress(
        {
            "stage": "evaluate",
            "scenario": "1/1",
            "candidate": "1/1",
            "split": "validation",
            "candles": 4297,
        }
    )
    _print_research_backtest_progress({"stage": "report_write", "experiment_id": "summary_exp"})
    _print_research_backtest_progress({"stage": "complete", "experiment_id": "summary_exp"})

    output = capsys.readouterr().out
    assert "[RESEARCH-BACKTEST] stage=start" in output
    assert "stage=load_split" in output
    assert "stage=evaluate" in output
    assert "stage=report_write" in output
    assert "stage=complete" in output


def _budget_exception(path: Path) -> ArtifactBudgetExceeded:
    return ArtifactBudgetExceeded(
        reason="artifact_budget_max_artifact_bytes_exceeded",
        observed=144,
        limit=128,
        path=path,
        attempted_write_bytes=64,
        prior_total_bytes=80,
        next_total_bytes=144,
        overwrite_existing_path=True,
        known_file_count=3,
    )


def _assert_artifact_budget_failure_payload(
    *,
    manager: PathManager,
    capsys,
    command_label: str,
) -> None:
    output = capsys.readouterr().out
    assert f"[{command_label}] artifact_budget_failure=" in output
    failure_path = manager.data_dir() / "reports" / "research" / "cli_budget_failure" / "artifact_budget_failure.json"
    assert failure_path.exists()
    payload = json.loads(failure_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["status"] == "ARTIFACT_BUDGET_EXCEEDED"
    assert payload["reason"] == "artifact_budget_max_artifact_bytes_exceeded"
    assert payload["attempted_write_bytes"] == 64
    assert payload["prior_total_bytes"] == 80
    assert payload["next_total_bytes"] == 144
    assert payload["limit"] == 128
    assert payload["path"].endswith("candidate_results/candidate_001.json")
    assert payload["overwrite_existing_path"] is True
    assert payload["known_file_count"] == 3
    assert payload["failure_artifact_ref"] == "reports/research/cli_budget_failure/artifact_budget_failure.json"
    assert payload["failure_artifact_path"] == str(failure_path.resolve())


def test_research_backtest_writes_artifact_budget_failure_payload(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    attempted_path = manager.data_dir() / "derived" / "research" / "cli_budget_failure" / "candidate_results" / "candidate_001.json"

    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    monkeypatch.setattr(
        research_cli,
        "load_manifest",
        lambda _path: SimpleNamespace(experiment_id="cli_budget_failure", deployment_tier="research_only"),
    )
    monkeypatch.setattr(research_cli, "load_calibration_artifact", lambda _path: None)
    monkeypatch.setattr(research_cli, "notify", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        research_cli,
        "run_research_backtest",
        lambda **_kwargs: (_ for _ in ()).throw(_budget_exception(attempted_path)),
    )

    rc = research_cli.cmd_research_backtest(manifest_path="manifest.json")

    assert rc == 1
    _assert_artifact_budget_failure_payload(
        manager=manager,
        capsys=capsys,
        command_label="RESEARCH-BACKTEST",
    )


def test_research_walk_forward_writes_artifact_budget_failure_payload(tmp_path, monkeypatch, capsys) -> None:
    manager = _manager(tmp_path, monkeypatch)
    attempted_path = manager.data_dir() / "derived" / "research" / "cli_budget_failure" / "candidate_results" / "candidate_001.json"

    monkeypatch.setattr(research_cli, "PATH_MANAGER", manager)
    monkeypatch.setattr(
        research_cli,
        "load_manifest",
        lambda _path: SimpleNamespace(experiment_id="cli_budget_failure", deployment_tier="research_only"),
    )
    monkeypatch.setattr(research_cli, "load_calibration_artifact", lambda _path: None)
    monkeypatch.setattr(research_cli, "notify", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        research_cli,
        "run_research_walk_forward",
        lambda **_kwargs: (_ for _ in ()).throw(_budget_exception(attempted_path)),
    )

    rc = research_cli.cmd_research_walk_forward(manifest_path="manifest.json")

    assert rc == 1
    _assert_artifact_budget_failure_payload(
        manager=manager,
        capsys=capsys,
        command_label="RESEARCH-WALK-FORWARD",
    )
