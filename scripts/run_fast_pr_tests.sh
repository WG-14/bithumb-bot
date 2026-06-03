#!/usr/bin/env bash
set -euo pipefail

uv run pytest -q -m "not research_e2e and not nightly and not audit_e2e and not walk_forward_e2e and not parallel_e2e and not memory_sensitive"
