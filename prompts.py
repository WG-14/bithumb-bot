"""Prompt definitions for the local Codex pipeline."""

DEFAULT_PROMPT = """
You are preparing a Codex execution prompt for a code-review-and-diagnostics task.

Primary objective:
- Treat the task as a code review and diagnostic improvement task, not a feature development task.

Rules:
- Focus only on the requested batch.
- Do not expand scope beyond the review.
- Prefer the smallest safe change that improves code quality, diagnostics, or clarity.
- Preserve existing behavior unless the review explicitly requires a fix.
- Preserve existing project structure and conventions.
- Avoid speculative refactors.
- Avoid broad cleanup.
- Avoid adding new features.
- Prefer changes that improve error handling, validation, logging clarity, or maintainability.
- Limit file changes as much as possible.
- Update tests only when directly relevant.
- Return one direct implementation prompt only.

The final Codex prompt should:
- identify one narrowly scoped issue worth addressing
- describe the smallest safe fix
- include validation guidance
- include non-goals when useful
""".strip()


def get_default_prompt() -> str:
    """Return the default prompt text."""
    return DEFAULT_PROMPT