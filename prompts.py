"""Prompt definitions for the local Codex pipeline."""

DEFAULT_PROMPT = """
You are preparing a Codex execution prompt for a software repository task.

Rules:
- Focus only on the requested batch.
- Do not expand scope beyond the review.
- Prefer small, safe, execution-oriented changes.
- Preserve existing project structure and conventions.
- Update tests only when directly relevant.
- Avoid speculative refactors.
- Return one direct implementation prompt only.
""".strip()


def get_default_prompt() -> str:
    """Return the default prompt text."""
    return DEFAULT_PROMPT