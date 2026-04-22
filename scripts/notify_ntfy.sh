#!/usr/bin/env bash
set -euo pipefail

# Send an operator notification through ntfy.
# Usage: NTFY_TOPIC=<topic> ./scripts/notify_ntfy.sh "Title" "default" "Message"

if [[ $# -ne 3 ]]; then
  echo "[NTFY] usage: $0 <title> <priority> <message>" >&2
  exit 2
fi

if [[ -z "${NTFY_TOPIC:-}" ]]; then
  echo "[NTFY] NTFY_TOPIC is required" >&2
  exit 2
fi

title="$1"
priority="$2"
message="$3"
base_url="${NTFY_URL:-https://ntfy.sh}"

curl --fail --show-error --silent \
  -H "Title: ${title}" \
  -H "Priority: ${priority}" \
  --data-binary "${message}" \
  "${base_url%/}/${NTFY_TOPIC}"

printf '\n'
