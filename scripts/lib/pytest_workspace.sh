#!/usr/bin/env bash
set -euo pipefail

BITHUMB_PYTEST_WORKSPACE=""
BITHUMB_PYTEST_WORKSPACE_PARENT=""
BITHUMB_PYTEST_SUITE=""
BITHUMB_PYTEST_PREFLIGHT_STAGE=""
BITHUMB_PYTEST_STARTED=0

BITHUMB_PYTEST_BROKER_PRIVATE_ENV_KEYS=(
  BITHUMB_API_KEY
  BITHUMB_API_SECRET
)

BITHUMB_PYTEST_EXTERNAL_NOTIFICATION_ENV_KEYS=(
  NTFY_TOPIC
  NOTIFIER_WEBHOOK_URL
  SLACK_WEBHOOK_URL
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
)

bithumb_pytest_repo_root() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
}

bithumb_pytest_resolve_path() {
  local path="$1"
  python3 - "$path" <<'PY'
from pathlib import Path
import sys
print(Path(sys.argv[1]).expanduser().resolve())
PY
}

bithumb_pytest_refuse_unsafe_path() {
  local target="$1"
  local repo_root="$2"
  if [[ -z "$target" || "$target" == "/" ]]; then
    echo "[PYTEST-WORKSPACE] refusing unsafe cleanup target: ${target:-<empty>}" >&2
    return 1
  fi
  python3 - "$target" "$repo_root" <<'PY'
from pathlib import Path
import sys
target = Path(sys.argv[1]).resolve()
repo = Path(sys.argv[2]).resolve()
if target == repo or repo in target.parents:
    print(f"[PYTEST-WORKSPACE] refusing repo-local cleanup target: {target}", file=sys.stderr)
    raise SystemExit(1)
PY
}

bithumb_pytest_setup_workspace() {
  local suite_name="${1:?suite name required}"
  local repo_root
  repo_root="$(bithumb_pytest_repo_root)"
  local workspace_root="${BITHUMB_PYTEST_WORKSPACE_ROOT:-/tmp/bithumb-bot-pytest-${USER:-user}}"
  workspace_root="$(bithumb_pytest_resolve_path "$workspace_root")"
  bithumb_pytest_refuse_unsafe_path "$workspace_root" "$repo_root"

  local run_id="${BITHUMB_PYTEST_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
  BITHUMB_PYTEST_WORKSPACE_PARENT="$workspace_root"
  BITHUMB_PYTEST_WORKSPACE="$workspace_root/$suite_name/$run_id"
  BITHUMB_PYTEST_SUITE="$suite_name"
  export BITHUMB_PYTEST_RUN_ID="$run_id"
  export BITHUMB_PYTEST_SUITE
  export PYTEST_DEBUG_TEMPROOT="$BITHUMB_PYTEST_WORKSPACE/pytest-debug"
  mkdir -p "$PYTEST_DEBUG_TEMPROOT"
  echo "[PYTEST-WORKSPACE] suite=$suite_name run_id=$run_id"
  echo "[PYTEST-WORKSPACE] root=$BITHUMB_PYTEST_WORKSPACE"
  echo "[PYTEST-WORKSPACE] PYTEST_DEBUG_TEMPROOT=$PYTEST_DEBUG_TEMPROOT"
}

bithumb_pytest_sanitize_unsafe_env() {
  local runner_name="${1:-pytest runner}"
  local key

  for key in "${BITHUMB_PYTEST_BROKER_PRIVATE_ENV_KEYS[@]}"; do
    unset "$key"
  done

  if [[ "${BITHUMB_PYTEST_ALLOW_EXTERNAL_NOTIFICATIONS:-0}" == "1" ]]; then
    echo "[PYTEST-SAFETY] broker-private env disabled for ${runner_name}; external notification env allowed by explicit opt-in"
    return 0
  fi

  export NOTIFIER_ENABLED=false
  for key in "${BITHUMB_PYTEST_EXTERNAL_NOTIFICATION_ENV_KEYS[@]}"; do
    unset "$key"
  done
  echo "[PYTEST-SAFETY] unsafe inherited env disabled for ${runner_name}"
}

bithumb_pytest_preflight_report_path() {
  if [[ -z "${BITHUMB_PYTEST_WORKSPACE:-}" ]]; then
    return 1
  fi
  printf '%s\n' "$BITHUMB_PYTEST_WORKSPACE/preflight_failure.json"
}

bithumb_pytest_workspace_size_bytes() {
  if [[ -z "${BITHUMB_PYTEST_WORKSPACE:-}" || ! -d "$BITHUMB_PYTEST_WORKSPACE" ]]; then
    printf '0\n'
    return 0
  fi
  du -sb "$BITHUMB_PYTEST_WORKSPACE" 2>/dev/null | awk '{print $1}'
}

bithumb_pytest_record_preflight_failure() {
  local stage="${1:?preflight stage required}"
  local status="${2:-1}"
  local command="${3:-}"
  local workspace_size
  workspace_size="$(bithumb_pytest_workspace_size_bytes)"
  local report_path
  report_path="$(bithumb_pytest_preflight_report_path)"
  mkdir -p "$(dirname "$report_path")"
  python3 - "$report_path" "$BITHUMB_PYTEST_SUITE" "$BITHUMB_PYTEST_WORKSPACE" "$stage" "$status" "$command" "$workspace_size" <<'PY'
import json
from pathlib import Path
import sys

report_path = Path(sys.argv[1])
payload = {
    "suite": sys.argv[2],
    "workspace_root": sys.argv[3],
    "failed_stage": sys.argv[4],
    "pytest_started": False,
    "status": "preflight_failed",
    "reason": f"preflight stage failed before pytest: {sys.argv[4]}",
    "command": sys.argv[6],
    "exit_code": int(sys.argv[5]),
    "retained_workspace_size_bytes": int(sys.argv[7] or 0),
}
report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  echo "[PYTEST-PREFLIGHT] failed suite=$BITHUMB_PYTEST_SUITE stage=$stage exit_code=$status"
  echo "[PYTEST-PREFLIGHT] pytest did not start"
  echo "[PYTEST-PREFLIGHT] workspace=$BITHUMB_PYTEST_WORKSPACE retained_size_bytes=${workspace_size:-0}"
  echo "[PYTEST-PREFLIGHT] report=$report_path"
}

bithumb_pytest_run_preflight() {
  local stage="${1:?preflight stage required}"
  shift
  local status
  BITHUMB_PYTEST_PREFLIGHT_STAGE="$stage"
  echo "[PYTEST-PREFLIGHT] start suite=$BITHUMB_PYTEST_SUITE stage=$stage command=$*"
  if "$@"; then
    echo "[PYTEST-PREFLIGHT] ok suite=$BITHUMB_PYTEST_SUITE stage=$stage"
    BITHUMB_PYTEST_PREFLIGHT_STAGE=""
    return 0
  else
    status=$?
    bithumb_pytest_record_preflight_failure "$stage" "$status" "$*"
    BITHUMB_PYTEST_PREFLIGHT_STAGE=""
    return "$status"
  fi
}

bithumb_pytest_mark_pytest_started() {
  BITHUMB_PYTEST_STARTED=1
  export BITHUMB_PYTEST_STARTED
  echo "[PYTEST-WORKSPACE] pytest_started=1 suite=$BITHUMB_PYTEST_SUITE workspace=$BITHUMB_PYTEST_WORKSPACE"
}

bithumb_pytest_workspace_summary() {
  if [[ -z "${BITHUMB_PYTEST_WORKSPACE:-}" || ! -d "$BITHUMB_PYTEST_WORKSPACE" ]]; then
    return 0
  fi
  local bytes
  bytes="$(du -sb "$BITHUMB_PYTEST_WORKSPACE" 2>/dev/null | awk '{print $1}')"
  echo "[PYTEST-WORKSPACE] retained_size_bytes=${bytes:-0} path=$BITHUMB_PYTEST_WORKSPACE"
  find "$BITHUMB_PYTEST_WORKSPACE" -type f -printf '%s %p\n' 2>/dev/null \
    | sort -nr \
    | head -10 \
    | awk '{print "[PYTEST-WORKSPACE] large_file_bytes="$1" path="$2}'
}

bithumb_pytest_cleanup_workspace() {
  local status="${1:-0}"
  local repo_root
  repo_root="$(bithumb_pytest_repo_root)"
  if [[ -z "${BITHUMB_PYTEST_WORKSPACE:-}" ]]; then
    return 0
  fi
  bithumb_pytest_refuse_unsafe_path "$BITHUMB_PYTEST_WORKSPACE" "$repo_root"
  if [[ "${KEEP_BITHUMB_TEST_ARTIFACTS:-0}" == "1" || "$status" != "0" ]]; then
    echo "[PYTEST-WORKSPACE] keeping workspace: $BITHUMB_PYTEST_WORKSPACE"
    bithumb_pytest_workspace_summary
    return 0
  fi
  if [[ "${BITHUMB_PYTEST_SUMMARY_ON_SUCCESS:-0}" == "1" ]]; then
    bithumb_pytest_workspace_summary
  fi
  rm -rf "$BITHUMB_PYTEST_WORKSPACE"
  echo "[PYTEST-WORKSPACE] cleaned workspace: $BITHUMB_PYTEST_WORKSPACE"
}
