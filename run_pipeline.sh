#!/usr/bin/env bash
set -euo pipefail

uv run python pipeline.py "$@"
