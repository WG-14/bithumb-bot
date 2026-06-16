from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RESEARCH_ROOT = REPO_ROOT / "src" / "bithumb_bot" / "research"


def _research_python_files() -> list[Path]:
    return sorted(RESEARCH_ROOT.rglob("*.py"))


def test_no_hardcoded_wsl_worker_count() -> None:
    offenders: list[str] = []
    for path in _research_python_files():
        text = path.read_text(encoding="utf-8")
        if "max_workers = 8" in text or "max_workers: int = 8" in text or "processors = 8" in text:
            offenders.append(str(path.relative_to(REPO_ROOT)))
        if "12 * 1024" in text and path.name != "resource_planner.py":
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []


def test_research_execution_policy_defaults_do_not_force_eight_workers() -> None:
    source = (RESEARCH_ROOT / "experiment_manifest.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    defaults: dict[str, object] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "ResearchExecutionPolicy":
            for stmt in node.body:
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                    if isinstance(stmt.value, ast.Constant):
                        defaults[stmt.target.id] = stmt.value.value
    assert defaults["max_workers"] == 1


def test_candidate_scenario_split_not_unconditional_default() -> None:
    source = (RESEARCH_ROOT / "experiment_manifest.py").read_text(encoding="utf-8")
    assert 'work_unit: str = "candidate_scenario"' in source
    assert 'work_unit: str = "candidate_scenario_split"' not in source


def test_memory_retention_defaults_not_increased_for_cpu_utilization() -> None:
    source = (RESEARCH_ROOT / "experiment_manifest.py").read_text(encoding="utf-8")
    assert "max_decisions_retained: int | None = 0" in source
    assert "max_equity_points_retained: int | None = 0" in source
