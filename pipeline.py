"""Pipeline that prepares prompts, runs Codex, records status, and publishes changes."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from prompts import (
    CODEX_INSTRUCTIONS,
    CODEX_TEMPLATE,
    DEFAULT_PROMPT,
    NORMALIZE_INSTRUCTIONS,
    NORMALIZE_TEMPLATE,
)

ROOT = Path(__file__).resolve().parent
SRC_ROOT = ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bithumb_bot.notifier import AlertSeverity, format_event, notify
from bithumb_bot.paths import PathManager
from bithumb_bot.storage_io import write_json_atomic, write_text_atomic

REVIEW_PATH = ROOT / "review.txt"
NORMALIZED_PATH = ROOT / "normalized.txt"
CODEX_PROMPT_PATH = ROOT / "codex_prompt.txt"
RESPONSES_URL = "https://api.openai.com/v1/responses"
PATH_MANAGER = PathManager.from_env(ROOT)
STATUS_PATH = PATH_MANAGER.report_path("codex_pipeline_status")


def read_review() -> str:
    """Load the source review text."""
    return REVIEW_PATH.read_text(encoding="utf-8").strip()


def extract_output_text(data: dict[str, Any]) -> str:
    """Extract plain text from a Responses API payload."""
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    parts: list[str] = []
    for item in data.get("output", []):
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())

    if parts:
        return "\n".join(parts)

    raise RuntimeError("Responses API returned no text output.")


def call_responses_api(
    *,
    model: str,
    instructions: str,
    user_input: str,
    api_key: str,
) -> str:
    """Send one Responses API request and return the text output."""
    payload = {
        "model": model,
        "instructions": instructions,
        "input": user_input,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=120.0) as client:
        response = client.post(RESPONSES_URL, headers=headers, json=payload)
        response.raise_for_status()
        return extract_output_text(response.json())


def build_normalization_request(review_text: str) -> tuple[str, str]:
    """Build the first-pass normalization request."""
    instructions = NORMALIZE_INSTRUCTIONS
    user_input = NORMALIZE_TEMPLATE.format(review_text=review_text)
    return instructions, user_input


def build_codex_prompt_request(review_text: str, normalized_text: str) -> tuple[str, str]:
    """Build the second-pass Codex prompt request."""
    instructions = CODEX_INSTRUCTIONS
    user_input = CODEX_TEMPLATE.format(
        default_prompt=DEFAULT_PROMPT,
        review_text=review_text,
        normalized_text=normalized_text,
    )
    return instructions, user_input


def write_text(path: Path, content: str) -> None:
    """Persist a UTF-8 text artifact."""
    write_text_atomic(path, content.rstrip() + "\n")


def run_command(args: list[str], *, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    """Run a subprocess in the repository root."""
    return subprocess.run(
        args,
        cwd=ROOT,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )


def utc_now() -> str:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def ensure_git_ready() -> None:
    """Fail closed when pre-existing local changes would make auto-commit unsafe."""
    if os.environ.get("PIPELINE_ALLOW_DIRTY_GIT", "").strip().lower() in {"1", "true", "yes", "on"}:
        return

    status = run_command(["git", "status", "--porcelain"])
    if status.returncode != 0:
        raise RuntimeError(f"git status failed: {status.stderr.strip() or status.stdout.strip()}")
    if status.stdout.strip():
        raise RuntimeError(
            "git worktree is not clean; refusing automatic add/commit/push. "
            "Set PIPELINE_ALLOW_DIRTY_GIT=1 to override."
        )


def current_branch() -> str:
    """Return the current Git branch name."""
    result = run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    branch = result.stdout.strip()
    if result.returncode != 0 or not branch or branch == "HEAD":
        raise RuntimeError("could not determine current git branch for push")
    return branch


def run_codex(prompt_text: str) -> dict[str, Any]:
    """Run Codex non-interactively against the current repository."""
    model = os.environ.get("CODEX_MODEL", "").strip()
    args = [
        "codex",
        "exec",
        "--full-auto",
        "--sandbox",
        "workspace-write",
        "-C",
        str(ROOT),
        "-",
    ]
    if model:
        args[2:2] = ["--model", model]

    result = run_command(args, input_text=prompt_text)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "codex exec failed")
    return {
        "returncode": result.returncode,
        "stdout_tail": result.stdout.strip()[-4000:],
        "stderr_tail": result.stderr.strip()[-4000:],
    }


def commit_and_push() -> dict[str, str]:
    """Stage repository changes, commit them, and push the current branch."""
    add_result = run_command(["git", "add", "-A"])
    if add_result.returncode != 0:
        raise RuntimeError(add_result.stderr.strip() or "git add failed")

    diff_result = run_command(["git", "diff", "--cached", "--quiet"])
    if diff_result.returncode == 0:
        raise RuntimeError("no staged changes were produced by the Codex run")
    if diff_result.returncode not in {0, 1}:
        raise RuntimeError("git diff --cached --quiet failed")

    commit_message = os.environ.get(
        "PIPELINE_GIT_COMMIT_MESSAGE",
        "chore: apply codex pipeline changes",
    ).strip()
    commit_result = run_command(["git", "commit", "-m", commit_message])
    if commit_result.returncode != 0:
        raise RuntimeError(commit_result.stderr.strip() or commit_result.stdout.strip() or "git commit failed")

    branch = current_branch()
    push_result = run_command(["git", "push", "origin", branch])
    if push_result.returncode != 0:
        raise RuntimeError(push_result.stderr.strip() or push_result.stdout.strip() or "git push failed")

    rev_result = run_command(["git", "rev-parse", "HEAD"])
    commit_sha = rev_result.stdout.strip()
    if rev_result.returncode != 0 or not commit_sha:
        raise RuntimeError("git rev-parse HEAD failed after commit")

    return {
        "branch": branch,
        "commit_sha": commit_sha,
        "commit_message": commit_message,
    }


def write_status(payload: dict[str, Any]) -> None:
    """Write the pipeline status snapshot to the managed reports bucket."""
    write_json_atomic(STATUS_PATH, payload)


def notify_status(payload: dict[str, Any]) -> None:
    """Send a concise status notification through the configured notifier hook."""
    severity = AlertSeverity.INFO if payload.get("status") == "success" else AlertSeverity.CRITICAL
    message = format_event(
        "codex_pipeline_finished",
        severity=severity,
        status=payload.get("status"),
        branch=payload.get("branch"),
        commit_sha=payload.get("commit_sha"),
        status_path=STATUS_PATH,
        error=payload.get("error"),
    )
    notify(message, severity=severity)


def run() -> dict[str, Path]:
    """Execute the full pipeline and save outputs."""
    normalize_model = os.environ.get("OPENAI_NORMALIZE_MODEL", "gpt-5-mini")
    prompt_model = os.environ.get("OPENAI_PROMPT_MODEL", normalize_model)
    status_payload: dict[str, Any] = {
        "timestamp_utc": utc_now(),
        "status": "started",
        "mode": PATH_MANAGER.config.mode,
        "review_path": str(REVIEW_PATH),
        "normalized_path": str(NORMALIZED_PATH),
        "codex_prompt_path": str(CODEX_PROMPT_PATH),
        "status_path": str(STATUS_PATH),
    }
    write_status(status_payload)

    try:
        api_key = os.environ["OPENAI_API_KEY"]
        ensure_git_ready()
        review_text = read_review()

        normalize_instructions, normalize_input = build_normalization_request(review_text)
        normalized_text = call_responses_api(
            model=normalize_model,
            instructions=normalize_instructions,
            user_input=normalize_input,
            api_key=api_key,
        )
        write_text(NORMALIZED_PATH, normalized_text)

        prompt_instructions, prompt_input = build_codex_prompt_request(
            review_text,
            normalized_text,
        )
        codex_prompt_text = call_responses_api(
            model=prompt_model,
            instructions=prompt_instructions,
            user_input=prompt_input,
            api_key=api_key,
        )
        write_text(CODEX_PROMPT_PATH, codex_prompt_text)

        codex_result = run_codex(codex_prompt_text)
        git_result = commit_and_push()

        status_payload.update(
            {
                "timestamp_utc": utc_now(),
                "status": "success",
                "branch": git_result["branch"],
                "commit_sha": git_result["commit_sha"],
                "commit_message": git_result["commit_message"],
                "codex_result": codex_result,
            }
        )
        write_status(status_payload)
        notify_status(status_payload)
        return {
            "review": REVIEW_PATH,
            "normalized": NORMALIZED_PATH,
            "codex_prompt": CODEX_PROMPT_PATH,
            "status": STATUS_PATH,
        }
    except Exception as exc:
        status_payload.update(
            {
                "timestamp_utc": utc_now(),
                "status": "failed",
                "error": str(exc),
            }
        )
        write_status(status_payload)
        notify_status(status_payload)
        raise


def main() -> int:
    outputs = run()
    print(f"review={outputs['review'].name}")
    print(f"normalized={outputs['normalized'].name}")
    print(f"codex_prompt={outputs['codex_prompt'].name}")
    print(f"status={outputs['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
