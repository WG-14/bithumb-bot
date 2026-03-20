#!/usr/bin/env bash
set -euo pipefail

cd /home/ec2-user/bithumb-bot

echo "== systemd: bithumb-bot.service =="
sudo systemctl status bithumb-bot.service --no-pager || true
echo

echo "== systemd: healthcheck timer =="
sudo systemctl status bithumb-bot-healthcheck.timer --no-pager || true
echo

echo "== systemd: backup timer =="
sudo systemctl status bithumb-bot-backup.timer --no-pager || true
echo

echo "== recent journal (live) =="
sudo journalctl -u bithumb-bot.service -n 50 --no-pager || true
echo

echo "== recent journal (healthcheck) =="
sudo journalctl -u bithumb-bot-healthcheck.service -n 30 --no-pager || true
echo

echo "== live db files =="
ls -lh data/live.sqlite data/locks/bithumb-bot-run-live.lock 2>/dev/null || true
echo

echo "== backup files =="
ls -1t backups | head -20 || true
