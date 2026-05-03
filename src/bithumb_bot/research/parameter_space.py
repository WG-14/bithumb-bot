from __future__ import annotations

import itertools
from typing import Any

from .hashing import sha256_hex


def iter_parameter_candidates(parameter_space: dict[str, tuple[object, ...]]) -> list[dict[str, Any]]:
    keys = sorted(parameter_space)
    candidates: list[dict[str, Any]] = []
    for values in itertools.product(*(parameter_space[key] for key in keys)):
        candidate = dict(zip(keys, values, strict=True))
        candidates.append(candidate)
    return candidates


def candidate_id(parameter_values: dict[str, Any], index: int) -> str:
    digest = sha256_hex(parameter_values)[:8]
    return f"candidate_{index + 1:03d}_{digest}"
