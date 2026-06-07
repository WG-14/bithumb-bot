#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
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
    "registration path",
    "builtin_manifest",
    "external_entry_point",
    "built-in reason",
    "official_example",
    "canary",
    "fail_safe",
    "maintained_baseline",
    "approved_core_strategy",
    "entry point group",
    "inventory evidence",
    "list_research_strategy_plugins()",
    "resolve_research_strategy_plugin()",
    "common execution, risk, data, research, and runtime core paths remain strategy-neutral",
    "strategy level",
    "level_1_research_only",
    "level_2_replay_compatible",
    "level_3_promotion_grade",
    "not_strategy_related",
    "assert_research_only_contract",
    "assert_replay_compatible_contract",
    "assert_live_eligible_contract",
    "architecture_review_required",
    "architecture_review_complete",
    "architecture review marker",
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
    "Registration Path",
    "Built-in Reason",
    "official_example",
    "canary",
    "fail_safe",
    "maintained_baseline",
    "approved_core_strategy",
    "PLUGIN_REGISTRATION_INTENT",
    "private_helper",
    "STRATEGY_PLUGINS",
    "strategy-plugin-inventory --json",
    "list_research_strategy_plugins()",
    "resolve_research_strategy_plugin()",
    "level_1_research_only",
    "level_2_replay_compatible",
    "level_3_promotion_grade",
    "assert_research_only_contract",
    "assert_replay_compatible_contract",
    "assert_live_eligible_contract",
)

LEVEL_HELPERS = {
    "level_1_research_only": ("assert_research_only_contract",),
    "level_2_replay_compatible": ("assert_replay_compatible_contract",),
    "level_3_promotion_grade": (
        "assert_live_eligible_contract",
        "focused runtime/live gate coverage",
        "equivalent focused runtime/live gate coverage",
    ),
}
LEVEL_TOKENS = tuple(LEVEL_HELPERS) + ("not_strategy_related",)
BUILTIN_REASON_TOKENS = (
    "official_example",
    "canary",
    "fail_safe",
    "maintained_baseline",
    "approved_core_strategy",
)
CORE_PATH_PREFIXES = (
    "src/bithumb_bot/runtime_",
    "src/bithumb_bot/research/",
    "src/bithumb_bot/risk",
    "src/bithumb_bot/execution",
    "src/bithumb_bot/run_loop",
    "src/bithumb_bot/strategy_decision",
    "src/bithumb_bot/runtime_data_provider.py",
)
STRATEGY_ADDITION_FORBIDDEN_CORE_FILES = (
    "src/bithumb_bot/runtime/runner.py",
    "src/bithumb_bot/runtime/decision_coordinator.py",
    "src/bithumb_bot/runtime/execution_coordinator.py",
    "src/bithumb_bot/run_loop_execution_planner.py",
    "src/bithumb_bot/execution_service.py",
    "src/bithumb_bot/runtime_strategy_set.py",
    "src/bithumb_bot/research/backtest_kernel.py",
    "src/bithumb_bot/research/backtest_pipeline.py",
    "src/bithumb_bot/research/backtest_stage_runner.py",
)
STRATEGY_PLUGIN_PREFIX = "src/bithumb_bot/strategy_plugins/"
BUILTIN_MANIFEST = "src/bithumb_bot/strategy_plugins/builtin_manifest.py"


def missing_tokens(path: Path, tokens: tuple[str, ...]) -> list[str]:
    text = path.read_text(encoding="utf-8").lower()
    return [token for token in tokens if token.lower() not in text]


def validate_strategy_pr_evidence(
    *,
    changed_files: tuple[str, ...],
    evidence_text: str,
) -> list[str]:
    text = evidence_text.lower()
    normalized_files = tuple(str(path).replace("\\", "/") for path in changed_files)
    violations: list[str] = []
    strategy_related = any(path.startswith(STRATEGY_PLUGIN_PREFIX) for path in normalized_files)
    core_related = any(path.startswith(prefix) for path in normalized_files for prefix in CORE_PATH_PREFIXES)
    architecture_migration = (
        "architecture_review_required" in text
        or "architecture_review_complete" in text
        or "runtime architecture migration" in text
    )
    if not normalized_files:
        return violations
    declared_levels = [level for level in LEVEL_TOKENS if level in text]
    if strategy_related and not declared_levels:
        violations.append("strategy changes require strategy Level declaration")
    if strategy_related and "not_strategy_related" in declared_levels:
        violations.append("strategy changes cannot be marked not_strategy_related")
    for level, helpers in LEVEL_HELPERS.items():
        if level in text and not any(helper in text for helper in helpers):
            violations.append(f"{level} requires contract helper or equivalent focused test")
    plugin_files = [
        path
        for path in normalized_files
        if path.startswith(STRATEGY_PLUGIN_PREFIX)
        and path.endswith(".py")
        and path != BUILTIN_MANIFEST
        and not path.endswith("_test.py")
    ]
    has_builtin_manifest = BUILTIN_MANIFEST in normalized_files or "builtin_manifest.py" in text
    has_entry_point = "bithumb_bot.strategy_plugins" in text
    declares_builtin_path = "registration path: builtin_manifest" in text or has_builtin_manifest
    declares_external_path = "registration path: external_entry_point" in text or has_entry_point
    if strategy_related:
        if BUILTIN_MANIFEST in normalized_files:
            builtin_reason = _extract_field_value(text, "built-in reason")
            if builtin_reason not in BUILTIN_REASON_TOKENS:
                violations.append("built-in strategy changes require valid Built-in Reason")
                violations.append(
                    "built-in strategy changes require valid Built-in Reason:"
                    f"{BUILTIN_MANIFEST}"
                )
        if declares_external_path and BUILTIN_MANIFEST in normalized_files:
            violations.append("external entry-point strategy changes must not edit built-in manifest")
            violations.append(
                "external entry-point strategy changes must not edit built-in manifest:"
                f"{BUILTIN_MANIFEST}"
            )
    if plugin_files:
        forbidden_core_files = [
            path for path in normalized_files if path in STRATEGY_ADDITION_FORBIDDEN_CORE_FILES
        ]
        if forbidden_core_files and not architecture_migration:
            for path in forbidden_core_files:
                violations.append(
                    "strategy_core_diff_forbidden:"
                    f"{path}:runtime_architecture_migration_evidence_required"
                )
        if not (has_builtin_manifest or has_entry_point):
            violations.append("strategy plugin changes require built-in manifest or external entry-point evidence")
        if not (declares_builtin_path or declares_external_path):
            violations.append("strategy plugin changes require Registration Path evidence")
        if "strategy-plugin-inventory --json" not in text:
            violations.append("strategy plugin changes require inventory evidence")
    if core_related and not (
        "architecture_review_required" in text or "architecture_review_complete" in text
    ):
        violations.append("core runtime/research changes require architecture review marker")
    if "full default-fast research matrix" in text or "full default-fast research matrices added" in text:
        violations.append("default-fast research matrix expansion is not allowed")
    return violations


def _extract_field_value(text: str, field: str) -> str:
    prefix = f"{field}:"
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(prefix):
            return stripped[len(prefix):].strip().split()[0] if stripped[len(prefix):].strip() else ""
    return ""


def _changed_files_from_args(args: argparse.Namespace, repo_root: Path) -> tuple[str, ...]:
    files = list(args.changed_file or ())
    if args.changed_files:
        files.extend(
            line.strip()
            for line in Path(args.changed_files).read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    if files:
        return tuple(files)
    return ()


def _github_event_payload() -> dict[str, object]:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return {}
    path = Path(event_path)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _run_git_changed_files(repo_root: Path, *args: str) -> tuple[str, ...]:
    try:
        result = subprocess.run(
            ("git", *args),
            cwd=repo_root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError):
        return ()
    return tuple(line.strip() for line in result.stdout.splitlines() if line.strip())


def _changed_files_from_github_event(repo_root: Path, payload: dict[str, object]) -> tuple[str, ...]:
    pull_request = payload.get("pull_request")
    if not isinstance(pull_request, dict):
        return ()
    base = pull_request.get("base")
    head = pull_request.get("head")
    base_sha = str(base.get("sha") or "").strip() if isinstance(base, dict) else ""
    head_sha = str(head.get("sha") or "").strip() if isinstance(head, dict) else ""
    base_ref = str(base.get("ref") or "").strip() if isinstance(base, dict) else ""
    candidates: list[tuple[str, ...]] = []
    if base_sha and head_sha:
        candidates.append(("diff", "--name-only", f"{base_sha}...{head_sha}"))
        candidates.append(("diff", "--name-only", f"{base_sha}..{head_sha}"))
    if base_ref:
        candidates.append(("diff", "--name-only", f"origin/{base_ref}...HEAD"))
    candidates.append(("diff", "--name-only", "HEAD^..HEAD"))
    for candidate in candidates:
        files = _run_git_changed_files(repo_root, *candidate)
        if files:
            return files
    return ()


def _changed_files_from_env(repo_root: Path, payload: dict[str, object]) -> tuple[str, ...]:
    raw = os.environ.get("STRATEGY_PR_CHANGED_FILES", "")
    if raw.strip():
        return tuple(line.strip() for line in raw.splitlines() if line.strip())
    if os.environ.get("GITHUB_EVENT_NAME") == "pull_request":
        return _changed_files_from_github_event(repo_root, payload)
    return ()


def _evidence_text_from_args(args: argparse.Namespace, repo_root: Path) -> str:
    if args.evidence_text:
        return str(args.evidence_text)
    if args.evidence_file:
        return Path(args.evidence_file).read_text(encoding="utf-8")
    return (repo_root / ".github" / "pull_request_template.md").read_text(encoding="utf-8")


def _evidence_text_from_env(payload: dict[str, object]) -> str:
    raw = os.environ.get("STRATEGY_PR_EVIDENCE_TEXT")
    if raw is not None:
        return raw
    pull_request = payload.get("pull_request")
    if isinstance(pull_request, dict):
        title = str(pull_request.get("title") or "")
        body = str(pull_request.get("body") or "")
        return "\n".join((title, body))
    return ""


def _explicit_diff_inputs(args: argparse.Namespace) -> bool:
    return bool(args.changed_file or args.changed_files or args.evidence_file or args.evidence_text)


def _explicit_evidence_input(args: argparse.Namespace) -> bool:
    return bool(args.evidence_file or args.evidence_text)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--changed-file", action="append")
    parser.add_argument("--changed-files")
    parser.add_argument("--evidence-file")
    parser.add_argument("--evidence-text")
    parser.add_argument(
        "--require-diff-aware",
        action="store_true",
        help="fail if changed-file/evidence inputs or PR metadata are unavailable",
    )
    return parser.parse_args(argv)


def main() -> int:
    args = _parse_args(sys.argv[1:])
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
    event_payload = _github_event_payload()
    changed_files = _changed_files_from_args(args, repo_root)
    evidence_text = _evidence_text_from_args(args, repo_root) if _explicit_evidence_input(args) else ""
    diff_source = "explicit_args" if _explicit_diff_inputs(args) else "static_only"
    if not changed_files and not _explicit_diff_inputs(args):
        changed_files = _changed_files_from_env(repo_root, event_payload)
        env_evidence = _evidence_text_from_env(event_payload)
        if changed_files:
            evidence_text = env_evidence
            diff_source = "ci_pr_metadata"
    diff_evaluated = bool(changed_files)
    if args.require_diff_aware and not diff_evaluated:
        violations.append("diff-aware strategy PR guard required but changed-file evidence was unavailable")
    if diff_evaluated:
        violations.extend(
            validate_strategy_pr_evidence(
                changed_files=changed_files,
                evidence_text=evidence_text,
            )
        )
    if violations:
        print("strategy PR workload guard violations:", file=sys.stderr)
        for violation in violations:
            print(f"- {violation}", file=sys.stderr)
        return 1
    print("strategy PR workload guard: static docs/templates ok")
    if diff_evaluated:
        print(f"strategy PR workload guard: diff-aware evidence ok ({diff_source}, changed_files={len(changed_files)})")
    else:
        print(
            "strategy PR workload guard: diff-aware evidence skipped "
            "(no changed-file PR metadata or explicit changed-file/evidence arguments)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
