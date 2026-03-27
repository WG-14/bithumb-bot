#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="${BITHUMB_BOT_ROOT:-$(cd -- "${SCRIPT_DIR}/.." && pwd -P)}"
cd "${REPO_ROOT}"

TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="snapshots/live_${TS}"
mkdir -p "${OUT_DIR}"

echo "Collecting snapshot into ${OUT_DIR}"

{
  echo "== date =="
  date
  echo
  echo "== pwd =="
  pwd
} > "${OUT_DIR}/00_meta.txt"

{
  echo "== bithumb-bot.service =="
  sudo systemctl status bithumb-bot.service --no-pager || true
  echo
  echo "== bithumb-bot-healthcheck.timer =="
  sudo systemctl status bithumb-bot-healthcheck.timer --no-pager || true
  echo
  echo "== bithumb-bot-backup.timer =="
  sudo systemctl status bithumb-bot-backup.timer --no-pager || true
} > "${OUT_DIR}/10_systemd_status.txt"

{
  echo "== journal: bithumb-bot.service =="
  sudo journalctl -u bithumb-bot.service -n 200 --no-pager || true
  echo
  echo "== journal: bithumb-bot-healthcheck.service =="
  sudo journalctl -u bithumb-bot-healthcheck.service -n 100 --no-pager || true
  echo
  echo "== journal: bithumb-bot-backup.service =="
  sudo journalctl -u bithumb-bot-backup.service -n 100 --no-pager || true
} > "${OUT_DIR}/20_journal.txt"

{
  echo "== env.live redacted =="
  grep -E '^[A-Z0-9_]+=' .env.live | sed 's/=.*$/=REDACTED/' || true
} > "${OUT_DIR}/30_env_redacted.txt"

{
  echo "== repo files =="
  find docs scripts deploy/systemd -maxdepth 2 -type f 2>/dev/null | sort
  echo
  echo "== data files =="
  find data -maxdepth 2 -type f 2>/dev/null | sort
  echo
  echo "== backup files =="
  find backups -maxdepth 1 -type f 2>/dev/null | sort | tail -50
} > "${OUT_DIR}/40_files.txt"

echo "Done: ${OUT_DIR}"
