from __future__ import annotations

from pathlib import Path


def _research_validation_doc() -> str:
    return Path("docs/research-validation.md").read_text(encoding="utf-8")


def _experiment_registry_section() -> str:
    doc = _research_validation_doc()
    start = doc.index("## Experiment Attempt Registry")
    end = doc.index("## Artifacts", start)
    return doc[start:end]


def _parallel_research_safety_matrix_section() -> str:
    doc = _research_validation_doc()
    start = doc.index("### Parallel Research Safety Matrix")
    end = doc.index("Workspace controls are", start)
    return doc[start:end]


def _short_clean_revalidation_section() -> str:
    doc = _research_validation_doc()
    start = doc.index("### Short Clean 8 Candidates x 2 Scenarios Revalidation")
    end = doc.index("Full-suite pytest validation should use:", start)
    return doc[start:end]


def test_short_clean_revalidation_runbook_mentions_required_checks() -> None:
    section = _short_clean_revalidation_section()

    for text in (
        "8 candidates",
        "2 scenarios",
        "research_run.report_detail=summary",
        "max_equity_points_retained=0",
        "research-readiness",
        "artifact_write_summary",
        "ArtifactBudgetExceeded",
        "work_unit_complete",
        "max_artifact_bytes",
        "EXPERIMENT_ID",
        "jq -r '.experiment_id' \"$REPORT\"",
        "DATA_ROOT/<mode>/reports/research/<experiment_id>/backtest_report.json",
        "DATA_ROOT/<mode>/derived/research/<experiment_id>/backtest_candidates.json",
        "DATA_ROOT/<mode>/derived/research/<experiment_id>/candidate_results/*.json",
    ):
        assert text in section

    assert "Do not raise\n`max_artifact_bytes` as the default repair" in section
    assert "test \"$(jq -r '.experiment_id' \"$REPORT\")\" = \"$EXPERIMENT_ID\"" in section
    assert "events alone are not success evidence if report writing failed" in section


def test_parallel_research_safety_matrix_records_measured_required_rows() -> None:
    section = _parallel_research_safety_matrix_section()

    for heading in (
        "#### Measured evidence",
        "#### Pending measurements",
        "#### Recommended default",
        "#### Reason for recommendation",
    ):
        assert heading in section

    required_rows = {
        "M1": "PYTEST_XDIST_WORKERS=2 BITHUMB_RESEARCH_MAX_WORKERS=2 ./scripts/run_parallel_research_safety_tests.sh",
        "M2": "PYTEST_XDIST_WORKERS=4 BITHUMB_RESEARCH_MAX_WORKERS=2 ./scripts/run_parallel_research_safety_tests.sh",
        "M3": "PYTEST_XDIST_WORKERS=4 BITHUMB_RESEARCH_MAX_WORKERS=1 ./scripts/run_parallel_research_safety_tests.sh",
    }
    measured = section[
        section.index("#### Measured evidence") : section.index("#### Pending measurements")
    ]

    for matrix_id, command in required_rows.items():
        assert f"| {matrix_id} | `{command}` |" in measured

    assert measured.count("| M1 |") == 1
    assert measured.count("| M2 |") == 1
    assert measured.count("| M3 |") == 1
    assert "DeprecationWarning=0" in measured
    assert "[RUNTIME-ARTIFACT-CHECK] OK" in measured
    assert "effective_process_start_method=spawn" in measured
    assert "outer_parallel_context=pytest-xdist" in measured
    assert "process_budget={" in measured

    for placeholder in ("TODO", "TBD", "__FILL_", "pending local/pipeline measurement"):
        assert placeholder not in measured


def test_parallel_research_safety_matrix_keeps_optional_rows_pending() -> None:
    section = _parallel_research_safety_matrix_section()
    pending = section[
        section.index("#### Pending measurements") : section.index("#### Recommended default")
    ]
    measured = section[
        section.index("#### Measured evidence") : section.index("#### Pending measurements")
    ]

    optional_commands = (
        "PYTEST_XDIST_WORKERS=2 BITHUMB_TOTAL_PROCESS_BUDGET=4 ./scripts/run_parallel_research_safety_tests.sh",
        "PYTEST_XDIST_WORKERS=4 BITHUMB_TOTAL_PROCESS_BUDGET=8 ./scripts/run_parallel_research_safety_tests.sh",
        "PYTEST_XDIST_WORKERS=4 BITHUMB_TOTAL_PROCESS_BUDGET=4 ./scripts/run_parallel_research_safety_tests.sh",
    )

    for command in optional_commands:
        assert command in pending
        assert command not in measured


def test_research_validation_docs_describe_semantic_holdout_identity_not_fingerprint_only() -> None:
    doc = _experiment_registry_section()

    assert "`final_holdout_identity_hash` is the semantic reuse-counting key based on\n  dataset source, market, interval, and final-holdout date range." in doc
    assert "`final_holdout_reuse_key_hash` is the actual key used to compute\n  `computed_holdout_reuse_count`" in doc
    assert "`final_holdout_fingerprint` is retained only as a compatibility alias for the\n  semantic identity hash." in doc
    assert "`final_holdout_content_hash` is the reproducibility/integrity key based on\n  dataset snapshot id, final-holdout split hash, and dataset quality hash." in doc
    assert "does not reset semantic holdout\nreuse for the same market, interval, and date range." in doc


def test_research_validation_docs_describe_pre_content_reservation_and_completion_binding() -> None:
    doc = _experiment_registry_section()

    assert "performs checked registry\nreservation before the final-holdout split is loaded." in doc
    assert "uses the semantic final-holdout identity while\ncontent is still unavailable." in doc
    assert "final_holdout_content_pending_until_completion=true" in doc
    assert "content fields must be\n  bound in completion, evidence, and report artifacts before promotion." in doc
    assert "declared counter mismatches and budget excesses are checked\nunder the registry lock" in doc
    assert "append `counted_attempt=false`, and do not append a\ncounted reservation." in doc


def test_research_validation_docs_describe_bound_evidence_hash_and_final_content_hash_difference() -> None:
    doc = _experiment_registry_section()

    assert "`research_attempt_completed` records the pre-completion statistical evidence\nhash." in doc
    assert "Final evidence stores that value in\n`experiment_registry_bound_evidence_hash`" in doc
    assert "`experiment_registry_evidence_hash_phase=pre_completion_evidence_hash`." in doc
    assert "the final\n`content_hash` and bound evidence hash can intentionally differ." in doc


def test_wsl_runbook_documents_parallel_efficiency() -> None:
    doc = Path("docs/runbooks/wsl-research-backtest.md").read_text(encoding="utf-8")

    assert "Parallelism Depends On Available Work Tasks" in doc
    assert "available_parallel_work_tasks" in doc
    assert "expected_worker_utilization_pct" in doc
    assert "1 / 8 * 100 = 12.5%" in doc
    assert "research-batch --manifest-glob" in doc


def test_research_validation_docs_describe_lifecycle_status_separate_from_statistical_gate() -> None:
    doc = _experiment_registry_section()

    assert "Lifecycle status is append-only and separate from statistical gate result:" in doc
    assert "`IN_PROGRESS` is a counted reservation." in doc
    assert "`COMPLETED` is a completed lifecycle event." in doc
    assert "`ABORTED` is an interrupted counted attempt." in doc
    assert "`REJECTED` is an uncounted preflight rejection." in doc
    assert "Only `COMPLETED` is promotion-permitted." in doc
    assert "`statistical_gate_result` is\n`PASS|FAIL|UNKNOWN` and remains separate evidence" in doc


def test_research_validation_docs_describe_registry_validate_artifact_bound_row_and_lifecycle_summary() -> None:
    doc = _experiment_registry_section()

    assert "validation_scope=registry_only" in doc
    assert "validation_scope=registry_and_artifacts" in doc
    assert "`artifact_bound_row_hash` identifies the reservation row\nreferenced by report/evidence." in doc
    assert "`artifact_binding_valid` and `artifact_reasons`\ndescribe whether that artifact chain binds" in doc
    assert "`registry_lifecycle_summary` lists all reservation rows for the experiment\nseparately" in doc
    assert "`row_valid_only=true` means the row hash is valid but the lifecycle is not\n  promotion-permitted." in doc
    assert "`ok=true` means `registry_row_valid`, `completion_row_valid`, and\n  `lifecycle_complete` are all true." in doc


def test_research_validation_docs_describe_registry_lifecycle_expected_incomplete_and_completed_rows() -> None:
    doc = _experiment_registry_section()

    assert "An incomplete row is expected to report `registry_row_valid=true`,\n`completion_row_valid=true`, `lifecycle_complete=false`" in doc
    assert "`promotion_permitted=false`, `row_valid_only=true`, `ok=false`, and reason\n`experiment_registry_incomplete_attempt`." in doc
    assert "A completed row is expected to report\n`registry_row_valid=true`, `completion_row_valid=true`,\n`lifecycle_complete=true`, `promotion_permitted=true`, `row_valid_only=false`,\nand `ok=true`." in doc


def test_research_validation_docs_list_current_registry_refusal_reasons() -> None:
    doc = _experiment_registry_section()

    for reason in (
        "experiment_registry_bound_evidence_hash_missing",
        "experiment_registry_evidence_hash_phase_mismatch",
        "experiment_registry_statistical_evidence_hash_mismatch",
        "experiment_registry_identity_source_missing",
        "experiment_registry_final_holdout_identity_mismatch",
        "experiment_registry_final_holdout_content_mismatch",
        "experiment_registry_final_holdout_reuse_key_mismatch",
        "experiment_registry_artifact_bound_row_missing",
        "experiment_registry_artifact_bound_row_hash_mismatch",
        "experiment_registry_report_evidence_row_hash_mismatch",
        "artifact_binding_not_checked",
        "attempt_budget_exceeded",
        "holdout_reuse_budget_exceeded",
    ):
        assert reason in doc
