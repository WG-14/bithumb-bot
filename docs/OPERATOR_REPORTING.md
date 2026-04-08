# Operator Reporting Workflow (Ops / Trade / Strategy Analysis)

Background: This document preserves the existing reporting intent while standardizing terminology.

This document helps operators answer:

- "How many trades actually happened?"
- "Which orders or fills were blocked, delayed, or misread?"

Canonical commands:

- `ops-report`
- `fee-diagnostics`
- `experiment-report`

## 1. Required Environment

Minimum required value:

- `DB_PATH`: SQLite DB path for the report query

Recommended context values:

- `MODE` (`paper` / `live`)
- `PAIR` (`BTC_KRW` is the canonical example)
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

## 3. Live Read Checklist

1. Run `ops-report`.
2. Check `[STRATEGY-SUMMARY]`.
   - Compare `order_count` and `fill_count`.
   - Confirm `pnl_proxy = sell_notional - buy_notional - fee_total`.
3. Check `[RECENT-STRATEGY-ORDER-FILL-FLOW]`.
   - Review the latest order events by time.
   - Confirm `submission_reason_code` and `message(note)` are correct.
4. Check `[RECENT-TRADES-OPERATIONS]`.
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
- Dust terms:
  - `harmless_dust`: a small remainder that is policy-classified as harmless dust.
  - `unsafe dust` / `mismatch dust`: any dust-like residual that is not policy-approved to resume.
  - `effective flat`: the remainder is treated as flat for the entry gate.
  - `resume allowed` / `new orders allowed`: policy flags that must be true before fresh BUYs are allowed.
- `effective_flat_due_to_harmless_dust` does not prove a literal zero balance.
- `dust_state`, `dust_action`, `dust_resume_allowed`, `dust_new_orders_allowed`, and `dust_treat_as_flat` should be read together.
- `strategy.context.position_gate.in_position` is an exposure-state field, not a dust-state field.
- `dust_broker_qty`, `dust_local_qty`, `dust_delta_qty`, and `dust_broker_local_match` should be read together.
- `dust_min_qty` and `dust_min_notional_krw` are separate sellability gates.

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
  --pair BTC_KRW \
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
- `dust_state=dangerous_dust` means the remainder is not safely resumable.
- `unresolved_count > 0` or `recovery_required_count > 0` means the state is still recovery-related.
- If `position.in_position=False` because of harmless dust, the entry gate has already accepted the remainder as flat.
- Use this order when you read the fields:
  1. restart gate
  2. dust policy
  3. quantity cross-check
  4. exchange minimum cross-check

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
