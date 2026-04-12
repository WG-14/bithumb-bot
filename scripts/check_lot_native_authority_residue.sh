#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

targets=(
  "src/bithumb_bot/broker/live.py"
  "src/bithumb_bot/decision_context.py"
  "src/bithumb_bot/reporting.py"
  "src/bithumb_bot/recovery.py"
)

forbidden_submit_sources='submit_qty_source["'"'"']?[[:space:]]*[:=][[:space:]]*["'"'"'](observation\.sell_qty_preview|position_state\.raw_total_asset_qty|position_qty|submit_payload_qty)'
forbidden_sell_basis_sources='sell_qty_basis_source["'"'"']?[[:space:]]*[:=][[:space:]]*["'"'"'](position_state\.raw_total_asset_qty|position_qty|submit_payload_qty)'
forbidden_live_shadow_submit_source='submit_qty_source[[:space:]]*=[[:space:]]*str\(exit_sizing\.qty_source\)'

if rg -n --pcre2 "$forbidden_submit_sources" "${targets[@]}"; then
  echo "lot-native residue check failed: suspicious non-canonical SELL submit source literal found" >&2
  exit 1
fi

if rg -n --pcre2 "$forbidden_sell_basis_sources" "${targets[@]}"; then
  echo "lot-native residue check failed: suspicious SELL basis source literal found" >&2
  exit 1
fi

if rg -n --pcre2 "$forbidden_live_shadow_submit_source" "src/bithumb_bot/broker/live.py"; then
  echo "lot-native residue check failed: live SELL submit path must not trust exit_sizing.qty_source as canonical authority" >&2
  exit 1
fi

echo "lot-native residue check passed"
