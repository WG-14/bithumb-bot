from __future__ import annotations

import re
from pathlib import Path

from tests.policy.research_runner_policy import DEFAULT_FAST_EXCLUDED_RESEARCH_MARKERS


CANONICAL_FAST_MARKER_EXPR = (
    "not research_kernel and not research_e2e and not audit_e2e and not walk_forward_e2e "
    "and not parallel_e2e and not nightly and not slow_research and not memory_sensitive"
)
CANONICAL_RESEARCH_NIGHTLY_MARKER_EXPR = (
    "research_kernel or research_e2e or audit_e2e or walk_forward_e2e or parallel_e2e "
    "or nightly or slow_research or memory_sensitive"
)


def _shell_assignment(path: Path, name: str) -> str:
    text = path.read_text(encoding="utf-8")
    match = re.search(rf'^{name}="([^"]+)"$', text, flags=re.MULTILINE)
    assert match is not None, f"{path} missing {name}"
    return match.group(1)


def _fast_excluded_markers(expr: str) -> set[str]:
    parts = expr.split(" and ")
    markers = []
    for part in parts:
        match = re.fullmatch(r"not ([a-zA-Z0-9_]+)", part)
        assert match is not None, f"unsupported fast marker expression term: {part}"
        markers.append(match.group(1))
    return set(markers)


def _nightly_included_markers(expr: str) -> set[str]:
    parts = expr.split(" or ")
    for part in parts:
        assert re.fullmatch(r"[a-zA-Z0-9_]+", part) is not None, f"unsupported nightly marker term: {part}"
    return set(parts)


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


def test_fast_pr_script_exports_fast_test_tier_before_pytest() -> None:
    text = Path("scripts/run_fast_pr_tests.sh").read_text(encoding="utf-8")

    export_index = text.index("export BITHUMB_TEST_TIER=fast")
    pytest_index = text.index("uv run pytest")

    assert export_index < pytest_index


def test_default_fast_excluded_research_markers_match_script_expressions() -> None:
    expected = set(DEFAULT_FAST_EXCLUDED_RESEARCH_MARKERS)
    assert "research_kernel" in expected

    fast_expr = _shell_assignment(Path("scripts/run_fast_pr_tests.sh"), "FAST_MARKER_EXPR")
    nightly_text = Path("scripts/run_research_nightly_tests.sh").read_text(encoding="utf-8")
    nightly_match = re.search(r'-m "([^"]+)"', nightly_text)
    assert nightly_match is not None

    assert _fast_excluded_markers(fast_expr) == expected
    assert _nightly_included_markers(nightly_match.group(1)) == expected
