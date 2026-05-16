from __future__ import annotations

from pathlib import Path


def _research_validation_doc() -> str:
    return Path("docs/research-validation.md").read_text(encoding="utf-8")


def _experiment_registry_section() -> str:
    doc = _research_validation_doc()
    start = doc.index("## Experiment Attempt Registry")
    end = doc.index("## Artifacts", start)
    return doc[start:end]


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
