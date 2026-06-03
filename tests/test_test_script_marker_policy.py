from __future__ import annotations

import re
from pathlib import Path


CANONICAL_FAST_MARKER_EXPR = (
    "not research_e2e and not audit_e2e and not walk_forward_e2e and not parallel_e2e "
    "and not nightly and not slow_research and not memory_sensitive"
)
CANONICAL_RESEARCH_NIGHTLY_MARKER_EXPR = (
    "research_e2e or audit_e2e or walk_forward_e2e or parallel_e2e or nightly "
    "or slow_research or memory_sensitive"
)


def _shell_assignment(path: Path, name: str) -> str:
    text = path.read_text(encoding="utf-8")
    match = re.search(rf'^{name}="([^"]+)"$', text, flags=re.MULTILINE)
    assert match is not None, f"{path} missing {name}"
    return match.group(1)


def test_fast_and_diagnostic_scripts_use_canonical_marker_expressions() -> None:
    active_scripts = [
        Path("scripts/run_fast_pr_tests.sh"),
        Path("run_pytest_diagnostics.sh"),
        Path("run_remaining_test_results.sh"),
    ]

    for script in active_scripts:
        assert _shell_assignment(script, "FAST_MARKER_EXPR") == CANONICAL_FAST_MARKER_EXPR

    for script in [Path("run_pytest_diagnostics.sh"), Path("run_remaining_test_results.sh")]:
        assert _shell_assignment(script, "RESEARCH_NIGHTLY_MARKER_EXPR") == CANONICAL_RESEARCH_NIGHTLY_MARKER_EXPR


def test_research_nightly_script_uses_canonical_marker_expression() -> None:
    text = Path("scripts/run_research_nightly_tests.sh").read_text(encoding="utf-8")

    assert f'-m "{CANONICAL_RESEARCH_NIGHTLY_MARKER_EXPR}"' in text
