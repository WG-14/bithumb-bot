#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


REQUIRED_PR_TEMPLATE_TOKENS = (
    "default-fast workload delta",
    "estimated_strategy_runs",
    "research/nightly workload delta",
    "research_e2e",
    "audit_e2e",
    "walk_forward_e2e",
    "parallel_e2e",
    "research_kernel",
    "slow_research",
    "nightly",
    "memory_sensitive",
    "lower-level contract coverage",
    "no default-fast workload delta",
    "builtin_manifest.py",
    "bithumb_bot.strategy_plugins",
    "strategy-plugin-inventory --json",
    "list_research_strategy_plugins()",
    "resolve_research_strategy_plugin()",
    "common execution, risk, data, research, and runtime core paths remain strategy-neutral",
)

REQUIRED_AUTHORING_DOC_TOKENS = (
    "Level 1",
    "Level 2",
    "Level 3",
    "estimated_strategy_runs",
    "research/nightly workload delta",
    "full default-fast research matrices",
    "lower-level contract",
    "builtin_manifest.py",
    "entry-point group",
    "STRATEGY_PLUGINS",
    "strategy-plugin-inventory --json",
    "list_research_strategy_plugins()",
    "resolve_research_strategy_plugin()",
)


def missing_tokens(path: Path, tokens: tuple[str, ...]) -> list[str]:
    text = path.read_text(encoding="utf-8").lower()
    return [token for token in tokens if token.lower() not in text]


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    checks = {
        repo_root / ".github" / "pull_request_template.md": REQUIRED_PR_TEMPLATE_TOKENS,
        repo_root / "docs" / "strategy-plugin-authoring.md": REQUIRED_AUTHORING_DOC_TOKENS,
    }
    violations: list[str] = []
    for path, tokens in checks.items():
        if not path.exists():
            violations.append(f"{path.relative_to(repo_root)} missing")
            continue
        for token in missing_tokens(path, tokens):
            violations.append(f"{path.relative_to(repo_root)} missing required strategy workload guard text: {token}")
    if violations:
        print("strategy PR workload guard violations:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1
    print("strategy PR workload guard: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
