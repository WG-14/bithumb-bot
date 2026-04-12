#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

targets=(
  "src/bithumb_bot/broker/live.py"
  "src/bithumb_bot/flatten.py"
  "src/bithumb_bot/decision_context.py"
  "src/bithumb_bot/reporting.py"
  "src/bithumb_bot/recovery.py"
)

forbidden_submit_sources='submit_qty_source["'"'"']?[[:space:]]*[:=][[:space:]]*["'"'"'](observation\.sell_qty_preview|position_state\.raw_total_asset_qty|position_qty|submit_payload_qty)'
forbidden_sell_basis_sources='sell_qty_basis_source["'"'"']?[[:space:]]*[:=][[:space:]]*["'"'"'](position_state\.raw_total_asset_qty|position_qty|submit_payload_qty)'
forbidden_live_shadow_submit_source='submit_qty_source[[:space:]]*=[[:space:]]*str\(exit_sizing\.qty_source\)'
forbidden_flatten_qty_authority='normalized_qty[[:space:]]*=[[:space:]]*float\(normalized_exposure\.open_exposure_qty\)'
forbidden_live_boundary_authority_override='replace\([[:space:]]*normalized_exposure[\s\S]{0,500}(exit_allowed=|exit_block_reason=|sellable_executable_lot_count=)'
forbidden_flatten_reserved_exit_rebuild='reserved_exit_lot_count[[:space:]]*=[[:space:]]*min\([[:space:]]*int\(open_lot_count\)[\s\S]{0,400}sellable_executable_lot_count[[:space:]]*=[[:space:]]*max\(0,[[:space:]]*int\(open_lot_count\)[[:space:]]*-[[:space:]]*reserved_exit_lot_count\)'
forbidden_mixed_sell_dust_boundary_definition='def _record_sell_dust_unsellable\([\s\S]{0,500}position_qty:'
forbidden_mixed_sell_dust_boundary_call='(?m)^[[:space:]]+_record_sell_dust_unsellable\([\s\S]{0,500}(position_qty=|submit_qty_source=|raw_total_asset_qty=|open_exposure_qty=|dust_tracking_qty=)'

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

if rg -n --pcre2 "$forbidden_flatten_qty_authority" "src/bithumb_bot/flatten.py"; then
  echo "lot-native residue check failed: flatten SELL submit path must not size directly from normalized_exposure.open_exposure_qty" >&2
  exit 1
fi

if rg -nUP "$forbidden_live_boundary_authority_override" "src/bithumb_bot/broker/live.py"; then
  echo "lot-native residue check failed: live SELL boundary must not override canonical normalized_exposure authority fields" >&2
  exit 1
fi

if rg -nUP "$forbidden_flatten_reserved_exit_rebuild" "src/bithumb_bot/flatten.py"; then
  echo "lot-native residue check failed: flatten SELL boundary must not locally rebuild reserved-exit or sellable-lot authority" >&2
  exit 1
fi

if rg -nUP "$forbidden_mixed_sell_dust_boundary_definition" "src/bithumb_bot/broker/live.py"; then
  echo "lot-native residue check failed: sell dust suppression boundary must not reintroduce mixed position_qty authority inputs" >&2
  exit 1
fi

if rg -nUP "$forbidden_mixed_sell_dust_boundary_call" "src/bithumb_bot/broker/live.py"; then
  echo "lot-native residue check failed: sell dust suppression call sites must pass separated canonical and diagnostic views" >&2
  exit 1
fi

echo "lot-native residue check passed"
