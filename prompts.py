"""Minimal prompt definitions for the local pipeline draft."""

DEFAULT_PROMPT = "Review the repository inputs and produce a short summary."


def get_default_prompt() -> str:
    """Return the default prompt text."""
    return DEFAULT_PROMPT
