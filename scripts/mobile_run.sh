#!/usr/bin/env bash
set -euo pipefail

export NTFY_TOPIC=bithumb-bot-dnjsckd5025

cd ~/work/bithumb-bot
./scripts/run_codex_pipeline.sh
