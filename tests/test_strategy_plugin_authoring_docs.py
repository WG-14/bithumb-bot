from __future__ import annotations

from pathlib import Path


DOC = Path("docs/strategy-plugin-authoring.md")


def test_authoring_docs_define_exit_policy_materializer_contract() -> None:
    text = DOC.read_text(encoding="utf-8")

    assert "exit_policy_materializer" in text
    assert "exit_policy_hash" in text
    assert "exit_policy_config_hash" in text
    assert "Custom exit policy PR checklist" in text


def test_authoring_docs_do_not_recommend_core_rule_whitelist_extension() -> None:
    text = DOC.read_text(encoding="utf-8")

    assert "must not be added to" in text or "Do not add strategy-owned custom rule names" in text
    assert "`exit_rule_factory` is scoped to `research_exploratory_compatibility_only`" in text
    assert "research compatibility hooks as live authority" in text
