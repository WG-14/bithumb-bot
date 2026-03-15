#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)"

OUT_DIR="${1:-${SCRIPT_DIR}/rendered}"
mkdir -p "${OUT_DIR}"

BITHUMB_BOT_ROOT="${BITHUMB_BOT_ROOT:-${REPO_ROOT}}"
BITHUMB_ENV_FILE_LIVE="${BITHUMB_ENV_FILE_LIVE:-/etc/bithumb-bot/bithumb-bot.live.env}"
BITHUMB_ENV_FILE_PAPER="${BITHUMB_ENV_FILE_PAPER:-/etc/bithumb-bot/bithumb-bot.paper.env}"

for unit in "${SCRIPT_DIR}"/*.service "${SCRIPT_DIR}"/*.timer; do
  target="${OUT_DIR}/$(basename "${unit}")"
  sed \
    -e "s|@BITHUMB_BOT_ROOT@|${BITHUMB_BOT_ROOT}|g" \
    -e "s|@BITHUMB_ENV_FILE_LIVE@|${BITHUMB_ENV_FILE_LIVE}|g" \
    -e "s|@BITHUMB_ENV_FILE_PAPER@|${BITHUMB_ENV_FILE_PAPER}|g" \
    "${unit}" > "${target}"
done

echo "Rendered systemd units to ${OUT_DIR}"
