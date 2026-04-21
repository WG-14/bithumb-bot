# Operator Reporting Workflow (Ops / Trade / Strategy Analysis)

Background: This document preserves the existing reporting intent while standardizing terminology.

This document helps operators answer:

- "How many trades actually happened?"
- "Which orders or fills were blocked, delayed, or misread?"

Canonical commands:

- `ops-report`
- `decision-telemetry`
- `fee-diagnostics`
- `strategy-report`
- `cash-drift-report`
- `experiment-report`

## 1. Required Environment

Minimum required value:

- `DB_PATH`: SQLite DB path for the report query

Recommended context values:

- `MODE` (`paper` / `live`)
- `PAIR` (`KRW-BTC` is the canonical market example; legacy alias `BTC_KRW` is still accepted as input)
- `INTERVAL` (`1m` is the canonical example)
- `BITHUMB_ENV_FILE` or `BITHUMB_ENV_FILE_LIVE` for explicit env injection

Path note:

- Do not hardcode DB paths into code.
- Use the explicit env file loading pattern when you need `DB_PATH`.

## 2. Execution Modes

### Local

```bash
MODE=paper DB_PATH=/var/lib/bithumb-bot/data/paper/trades/paper.small.safe.sqlite uv run bithumb-bot ops-report --limit 20
```

### AWS / systemd

```bash
BITHUMB_ENV_FILE=/etc/bithumb-bot/bithumb-bot.live.env uv run bithumb-bot ops-report --limit 50
```

If needed, run directly as the service user:

```bash
sudo -u <service-user> BITHUMB_ENV_FILE=/etc/bithumb-bot/bithumb-bot.live.env uv run bithumb-bot ops-report --limit 50
```

The default output is `stdout`. Redirect to a file only when the output itself is needed as an artifact.

## 3. Current `ops-report` Surface

`ops-report` emits a combined operator snapshot to `stdout` and writes the JSON artifact under `DATA_ROOT/<mode>/reports/ops_report/`.

Current sections and payload groups:

- `[OPS-REPORT]`: top-level operator recovery summary, lot/dust authority view, balance/accounts diagnostics, recent external cash adjustment summary, runtime-state snapshot, and run-lock status.
- `[ORDER-RULE-SNAPSHOT]`: current buy/sell rule values together with their source metadata, or an explicit load failure.
- `[STRATEGY-SUMMARY]`: per-strategy order/fill counts, notionals, fee totals, and the current `pnl_proxy_deprecated` compatibility field.
- `[RECENT-STRATEGY-ORDER-FILL-FLOW]`: recent order-event flow with submission reason, sell-boundary diagnostics, and operator-facing notes.
- `[RECENT-SELL-SUPPRESSIONS]`: recent SELL suppression outcomes including suppression category/detail, lot count, qty boundary inputs, dust/operator action, and summary text.
- `[RECENT-STRATEGY-DECISION-FLOW]`: recent strategy decisions with raw/base/final signals, entry gating, normalized exposure diagnostics, sell-boundary fields, and final reason text.
- `[RECENT-TRADES-OPERATIONS]`: recent trade ledger operations and rolling fee total.
- `[FEE-DIAGNOSTICS-SNAPSHOT]`: compact fee/fill and round-trip diagnostics included alongside the ops snapshot.

Key operator-facing payload surfaces:

- `operator_recovery_summary`
- `recent_sell_suppressions`
- `recent_decision_flow`
- `order_rule_snapshot`
- `balance_source_diagnostics`
- `recent_external_cash_adjustment`
- `runtime_state_snapshot`
- `run_lock`

## 4. Live Read Checklist

1. Run `ops-report`.
2. Check `[OPS-REPORT]`.
   - Review `operator_recovery_summary` first.
   - Confirm the current lot-native exit authority, dust posture, balance/accounts diagnostic, runtime-state snapshot, and `run_lock`.
3. Check `[ORDER-RULE-SNAPSHOT]`.
   - Confirm the current min-qty, step, min-notional, and BUY/SELL rule sources.
4. Check `[RECENT-SELL-SUPPRESSIONS]`.
   - Review recent suppression categories, sell-boundary details, and operator-action text.
5. Check `[RECENT-STRATEGY-DECISION-FLOW]`.
   - Review the latest base/raw/final decision path, entry gating, and sell-boundary diagnostics.
6. Check `[STRATEGY-SUMMARY]`.
   - Compare `order_count` and `fill_count`.
   - Confirm `pnl_proxy = sell_notional - buy_notional - fee_total`.
7. Check `[RECENT-STRATEGY-ORDER-FILL-FLOW]`.
   - Review the latest order events by time.
   - Confirm `submission_reason_code` and `message(note)` are correct.
8. Check `[RECENT-TRADES-OPERATIONS]`.
   - Review `fee`, `cash_after`, `asset_after`, and `note`.

## 3-0. `/v1/accounts` Preflight Interpretation

`broker-diagnose` and `health` print `/v1/accounts` preflight context together with the report output.

- `execution_mode`
  - `live_dry_run_unarmed`: live dry-run path
  - `live_real_order_path`: real-order path
- `quote_currency`, `base_currency`
- `base_currency_missing_policy`
  - `allow_zero_position_start_in_dry_run`
  - `block_when_base_currency_row_missing`
- `preflight_outcome`
  - `pass_no_position_allowed`
  - `fail_real_order_blocked`

If `order_rules_autosync=FALLBACK`, the bot could not use `/v1/orders/chance` and is using local fallback constraints. In `MODE=live`, treat that as a warning to clear before real-order arming.

## 3-1. `health` / `recovery-report` Field Guide

Read `health` and `recovery-report` as status maps, not as a simple green/red stamp.

- `trading_enabled`: the bot currently allows new order intent to proceed.
- `halt_new_orders_blocked`: an explicit stop gate is active.
- `unresolved_open_order_count`: order lifecycle state is still unclear.
- `recovery_required_count`: explicit recovery action is still required.
- `last_reconcile_*`: the most recent reconciliation evidence.
- Read the current lot/dust fields from the emitted status payload, not from older position booleans.
- For entry/flat interpretation, start with:
  - `entry_allowed`
  - `effective_flat`
  - `effective_flat_due_to_harmless_dust`
- Dust terms:
  - Canonical current states are `no_dust`, `harmless_dust`, and `blocking_dust`.
  - `harmless_dust`: a small remainder that is policy-classified as harmless dust.
  - `blocking_dust`: a dust residual that is not policy-approved to resume and requires manual review.
  - `effective flat`: the remainder is treated as flat for the entry gate.
  - `resume allowed` / `new orders allowed`: policy flags that must be true before fresh BUYs are allowed.
- `effective_flat_due_to_harmless_dust` does not prove a literal zero balance.
- `dust_state`, `dust_action` / `operator_action`, `dust_resume_allowed`, `dust_new_orders_allowed`, and `dust_treat_as_flat` should be read together, but they are not the primary SELL/exit authority layer.

### Fee-Pending Fill Recovery

If `recovery-report` shows broker fill observations with `fee_status=missing`,
`fee_status=order_level_candidate`, or `accounting_status=fee_pending`, the
exchange-side fill is observed but the local ledger has not accepted it as
accounted. `order_level_candidate` means the broker omitted trade-level fee
fields while exposing an order-level fee candidate, such as `paid_fee`, in the
same order payload. Treat that as repair evidence, not automatic accounting
truth. Do not use `resume --force` for this state.

Use the fee-pending repair command only after checking broker evidence for the
exact fill fee:

```bash
uv run bithumb-bot fee-pending-accounting-repair --client-order-id <id> --fill-id <fill_id> --fee <fee> --fee-provenance <evidence> --apply --yes
```

The command records an audited fee-pending accounting repair, applies the fill
through normal accounting code, records an accounting-complete broker fill
observation, then rebuilds the lot/lifecycle projections from the accounted
trade sequence. This means operator fee evidence updates `fills` and `trades`
first; `open_position_lots` and `trade_lifecycles` are replayed projections,
not independently patched truth. Trading remains disabled until an explicit
`resume`.
Material live fills still require a positive fee; a zero-fee repair for a
material live fill is refused.
- `dust_broker_qty`, `dust_local_qty`, `dust_delta_qty`, and `dust_broker_local_match` should be read together.
- `dust_min_qty` and `dust_min_notional_krw` are separate sellability gates.
- For exit authority, check the lot-native fields first:
  - `sellable_executable_lot_count`
  - `reserved_exit_lot_count`
  - `exit_allowed`
  - `exit_block_reason`
  - current exposure cross-check fields such as `sellable_executable_qty`, `executable_exposure_qty`, `tracked_dust_qty`, and `normalized_exposure_qty`

## 3-1-1. Preflight Interpretation

- In live dry-run, a missing base row can still yield `pass_no_position_allowed`.
- A missing quote row is a preflight failure.
- In live real-order mode, the same missing-base policy must remain a blocker until explicitly cleared.

## 3-2. Fee Diagnostics

Use `fee-diagnostics` to review live fill fee behavior.

Useful outputs:

- Average fee rate
- Zero-fee fill count
- Mean and median fee bps
- Estimated fee-rate gap
- Recent round-trip fee total
- Gross vs net PnL comparison

Example:

```bash
MODE=live DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.sqlite \
  uv run bithumb-bot fee-diagnostics --fill-limit 200 --roundtrip-limit 100

MODE=live DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.sqlite \
  uv run bithumb-bot fee-diagnostics --fill-limit 200 --roundtrip-limit 100 --json
```

## 4. Strategy Report

`strategy-report` compares realized PnL from `trade_lifecycles`.

Typical metrics:

- `trade_count`
- `win_rate`
- `average_gain`
- `average_loss`
- `realized_gross_pnl`
- `fee_total`
- `realized_net_pnl`
- `expectancy_per_trade`
- Holding-time summary
- Reason-linkage summary

Example:

```bash
MODE=paper DB_PATH=/var/lib/bithumb-bot/data/paper/trades/paper.sqlite uv run bithumb-bot strategy-report

MODE=paper DB_PATH=/var/lib/bithumb-bot/data/paper/trades/paper.sqlite \
  uv run bithumb-bot strategy-report \
  --from-date 2026-03-01 --to-date 2026-03-27 \
  --pair KRW-BTC \
  --group-by strategy_name,exit_rule_name,pair \
  --json
```

## 5. Experiment Report

`experiment-report` is a strategy comparison report that uses ops, health, and recovery context.

Typical metrics:

- `realized_net_pnl`
- `trade_count`
- `win_rate`
- `expectancy_per_trade`
- `max_drawdown_proxy`
- Top-N concentration
- Longest losing streak
- Time-of-day bucket performance
- Market regime bucket performance

Warnings:

- `insufficient sample`
- `concentrated pnl`
- `regime skew`
- `regime pnl skew`

Example:

```bash
MODE=live DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.small.safe.sqlite \
  uv run bithumb-bot experiment-report \
  --from-date 2026-03-01 --to-date 2026-03-31 \
  --sample-threshold 30 \
  --top-n 3 \
  --concentration-threshold 0.60 \
  --regime-skew-threshold 0.70 \
  --regime-pnl-skew-threshold 0.70
```

## 6. Dust Residual Reading Guide

- `accounts_flat_start_allowed` is only an `/v1/accounts` diagnostic.
- `dust_state=harmless_dust` means broker/local dust matches closely enough to be treated as harmless dust.
- `dust_state=blocking_dust` means the remainder is not safely resumable.
- Legacy labels such as `dangerous_dust` may still be normalized from older metadata, but operators should treat `blocking_dust` as the current canonical state name.
- `unresolved_count > 0` or `recovery_required_count > 0` means the state is still recovery-related.
- If harmless dust is being treated as effectively flat, confirm that with `entry_allowed=1`, `effective_flat=1`, and `effective_flat_due_to_harmless_dust=1` rather than relying on older `position.in_position` style state expressions.
- Do not use `dust_state` alone to infer whether a SELL or exit is currently allowed.
- Use this order when you read the fields:
  1. restart gate
  2. lot-native exit authority (`sellable_executable_lot_count`, `reserved_exit_lot_count`, `exit_allowed`, `exit_block_reason`, normalized exposure fields)
  3. dust policy and resume posture
  4. quantity cross-check
  5. exchange minimum cross-check

## 7. Manual App Sell Caution

- Do not rely on manual app sells as the normal dust workflow.
- Prefer `reconcile` plus report comparison first.
- If a manual app sell happened while the bot was stopped, rerun `health`, `recovery-report`, and `ops-report` before restarting.
- Do not use `resume --force` as a shortcut around dust review.

## 8. Summary

When in doubt, prefer:

1. Recovery evidence
2. Storage and path correctness
3. Operator review
4. Resume safety

Note: This report workflow remains a diagnostic workflow, not an execution workflow.
