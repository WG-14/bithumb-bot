from __future__ import annotations

from pathlib import Path


def _research_validation_doc() -> str:
    return Path("docs/research-validation.md").read_text(encoding="utf-8")


def test_research_validation_docs_describe_final_holdout_identity_and_content_hashes() -> None:
    doc = _research_validation_doc()

    assert "final_holdout_identity_hash" in doc
    assert "final_holdout_content_hash" in doc
    assert "final_holdout_reuse_key_hash" in doc
    assert "final_holdout_fingerprint" in doc
    assert "final_holdout_content_pending_until_completion=true" in doc


def test_research_validation_docs_describe_bound_evidence_hash_phase() -> None:
    doc = _research_validation_doc()

    assert "experiment_registry_bound_evidence_hash" in doc
    assert "experiment_registry_evidence_hash_phase=pre_completion_evidence_hash" in doc
    assert "pre-completion evidence" in doc
    assert "final `content_hash` is recomputed" in doc


def test_research_validation_docs_describe_registry_validate_scopes() -> None:
    doc = _research_validation_doc()

    assert "validation_scope=registry_only" in doc
    assert "validation_scope=registry_and_artifacts" in doc
    assert "artifact_bound_row_hash" in doc
    assert "artifact_binding_valid" in doc
    assert "registry_lifecycle_summary" in doc


def test_research_validation_docs_list_current_registry_refusal_reasons() -> None:
    doc = _research_validation_doc()

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
