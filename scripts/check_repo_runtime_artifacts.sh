#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Runtime/test DB artifacts must stay outside repository.
# Allowlist is intentionally explicit and empty by default.
allowlist_regex='^$'

candidates="$({
  git ls-files --cached -- '*.db' '*.sqlite' '*.sqlite3'
  git ls-files --others --exclude-standard -- '*.db' '*.sqlite' '*.sqlite3'
} | sort -u)"

violations=""
if [[ -n "$candidates" ]]; then
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    if [[ "$path" =~ $allowlist_regex ]]; then
      continue
    fi
    violations+="$path"$'\n'
  done <<< "$candidates"
fi

if [[ -n "$violations" ]]; then
  echo "[RUNTIME-ARTIFACT-CHECK] repo-local DB artifacts detected:" >&2
  printf '%s' "$violations" >&2
  echo "[RUNTIME-ARTIFACT-CHECK] Move runtime/test DB files outside repo (PathManager roots or pytest tmp_path)." >&2
  exit 1
fi

echo "[RUNTIME-ARTIFACT-CHECK] OK: no repo-local DB artifacts detected."
