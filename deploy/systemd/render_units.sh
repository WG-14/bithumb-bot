#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd -P)"

OUT_DIR="${1:-${SCRIPT_DIR}/rendered}"
mkdir -p "${OUT_DIR}"

BITHUMB_BOT_ROOT="${BITHUMB_BOT_ROOT:-${REPO_ROOT}}"
BITHUMB_ENV_FILE_LIVE="${BITHUMB_ENV_FILE_LIVE:-/etc/bithumb-bot/bithumb-bot.live.env}"
BITHUMB_ENV_FILE_PAPER="${BITHUMB_ENV_FILE_PAPER:-/etc/bithumb-bot/bithumb-bot.paper.env}"
BITHUMB_ENV_ROOT="${BITHUMB_ENV_ROOT:-/var/lib/bithumb-bot/env}"
BITHUMB_RUN_ROOT="${BITHUMB_RUN_ROOT:-/var/lib/bithumb-bot/run}"
BITHUMB_DATA_ROOT="${BITHUMB_DATA_ROOT:-/var/lib/bithumb-bot/data}"
BITHUMB_LOG_ROOT="${BITHUMB_LOG_ROOT:-/var/lib/bithumb-bot/logs}"
BITHUMB_BACKUP_ROOT="${BITHUMB_BACKUP_ROOT:-/var/lib/bithumb-bot/backup}"
BITHUMB_RUN_USER="${BITHUMB_RUN_USER:-$(id -un)}"
DEFAULT_UV_BIN="$(command -v uv || true)"
BITHUMB_UV_BIN="${BITHUMB_UV_BIN:-${DEFAULT_UV_BIN:-uv}}"

for unit in "${SCRIPT_DIR}"/*.service "${SCRIPT_DIR}"/*.timer; do
  target="${OUT_DIR}/$(basename "${unit}")"
  sed \
    -e "s|@BITHUMB_BOT_ROOT@|${BITHUMB_BOT_ROOT}|g" \
    -e "s|@BITHUMB_ENV_FILE_LIVE@|${BITHUMB_ENV_FILE_LIVE}|g" \
    -e "s|@BITHUMB_ENV_FILE_PAPER@|${BITHUMB_ENV_FILE_PAPER}|g" \
    -e "s|@BITHUMB_ENV_ROOT@|${BITHUMB_ENV_ROOT}|g" \
    -e "s|@BITHUMB_RUN_ROOT@|${BITHUMB_RUN_ROOT}|g" \
    -e "s|@BITHUMB_DATA_ROOT@|${BITHUMB_DATA_ROOT}|g" \
    -e "s|@BITHUMB_LOG_ROOT@|${BITHUMB_LOG_ROOT}|g" \
    -e "s|@BITHUMB_BACKUP_ROOT@|${BITHUMB_BACKUP_ROOT}|g" \
    -e "s|@BITHUMB_UV_BIN@|${BITHUMB_UV_BIN}|g" \
    -e "s|@BITHUMB_RUN_USER@|${BITHUMB_RUN_USER}|g" \
    "${unit}" > "${target}"
done

echo "Rendered systemd units to ${OUT_DIR}"
