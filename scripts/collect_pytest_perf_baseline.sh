#!/usr/bin/env bash
set -euo pipefail

uv run python scripts/collect_pytest_perf_baseline.py "$@"
