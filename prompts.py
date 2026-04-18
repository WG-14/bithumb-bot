"""Prompt definitions for the local Codex pipeline."""

NORMALIZE_INSTRUCTIONS = """
You rewrite raw review notes into a concise normalized spec.

Rules:
- Return plain text only.
- Preserve the original intent.
- Remove fluff and repetition.
- Keep the result execution-ready.
- Do not expand scope beyond the source review.
""".strip()

NORMALIZE_TEMPLATE = """
Normalize the following review notes into an implementation-ready normalized spec.

Requirements:
- Organize the result so it is directly executable.
- Keep only what is necessary to preserve the requested intent.
- Do not add new scope, ideas, or tasks not present in the source.

Review notes:
{review_text}
""".strip()

CODEX_INSTRUCTIONS = """
You produce a single Codex-ready implementation prompt.

Rules:
- Return plain text only.
- Be direct, specific, and execution-oriented.
- Do not expand scope beyond the provided review and normalized summary.
- Prefer the smallest safe change.
- Include validation guidance.
""".strip()

CODEX_TEMPLATE = """
Base objective:
{default_prompt}

Original review notes:
{review_text}

Normalized summary:
{normalized_text}

Write one Codex prompt that an agent can execute immediately.

Requirements:
- plain text only
- direct and specific wording
- execution-oriented instructions
- no scope expansion
- prefer minimal modifications
- include concrete validation guidance
""".strip()

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
