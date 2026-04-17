# Limited Unattended Live Ops Checklist (Bithumb BTC)

Background: This document is a limited live operations checklist and does not imply full 24/7 autonomous operation.

> Current model: explicit live arming, safety-halting, and operator-confirmed resume gates.

## 1. Mode and Path Separation

- [ ] The current session mode is explicitly one of `paper`, `live dry-run`, or `live armed`
- [ ] `paper` and `live` use separate `DB_PATH` values
- [ ] Live dry-run starts with `LIVE_DRY_RUN=true`
- [ ] Real-order mode requires `LIVE_DRY_RUN=false` and `LIVE_REAL_ORDER_ARMED=true`

Example commands:

```bash
MODE=paper DB_PATH=/var/lib/bithumb-bot/data/paper/trades/paper.safe.sqlite uv run bithumb-bot health
MODE=live DB_PATH=/var/lib/bithumb-bot/data/live/trades/live.safe.sqlite LIVE_DRY_RUN=true uv run bithumb-bot health
```

## 2. Live Preflight

```bash
uv run bithumb-bot broker-diagnose
uv run bithumb-bot health
uv run bithumb-bot recovery-report
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report
```

Pass criteria:

- `broker-diagnose` returns `overall=PASS`
- BUY `price=None` / BUY market support is judged from `broker-diagnose`'s `BUY price=None chance resolution` output.
- Confirm the `BUY price=None chance resolution` fields: `allowed`, `resolved_order_type`, `support_source`, `decision_basis`, `alias_used`, and `block_reason`.
- `health` shows no stale-candle or error problem
- `recovery-report` shows unresolved and recovery-required counts cleared

Live safety reminders:

- `DB_PATH` must be explicit in live mode
- `MAX_ORDER_KRW`, `MAX_DAILY_LOSS_KRW`, and `MAX_DAILY_ORDER_COUNT` must be finite positive values
- `MAX_ORDERBOOK_SPREAD_BPS`, `MAX_MARKET_SLIPPAGE_BPS`, and `LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS` must be finite positive values in live mode
- Notifier configuration must be present
- Paper-only settings must remain unset in live mode

## 3. API and Notifier Checks

- [ ] Bithumb API read and order permissions are confirmed
- [ ] Withdraw permissions remain disabled
- [ ] IP whitelist state is understood
- [ ] At least one notifier path is configured
- [ ] Recent health and recovery alerts are visible

## 4. Live Halt, Recovery, and Resume

```bash
# Integrated emergency path
uv run bithumb-bot panic-stop
uv run bithumb-bot panic-stop --flatten

# Manual halt without integrated cleanup
uv run bithumb-bot pause
uv run bithumb-bot cancel-open-orders

# Reconcile the ledger
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report
```

Resume:

```bash
uv run bithumb-bot resume
```

- Use `panic-stop` as the current integrated live emergency command.
- Use `pause` when you need a persistent halt without automatic cancel / flatten cleanup.
- Do not resume until `recovery-report` shows `resume_allowed=1`, `can_resume=true`, and the blocker list is empty.
- Use `resume --force` only after operator review.

Targeted unresolved-order recovery:

```bash
uv run bithumb-bot recover-order --client-order-id <client_id> --exchange-order-id <exchange_id> --dry-run
uv run bithumb-bot recover-order --client-order-id <client_id> --exchange-order-id <exchange_id> --yes
```

- Use `recover-order` only for a specific unresolved live order after reviewing `recovery-report`.
- Run the `--dry-run` preview first. The applied path requires `--yes` and still leaves trading disabled until explicit `resume`.

## 5. Restart / Reconcile Checklist

```bash
uv run bithumb-bot restart-checklist
uv run bithumb-bot health
uv run bithumb-bot recovery-report
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report
uv run bithumb-bot cancel-open-orders
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report
uv run bithumb-bot resume
```

Pass criteria:

- `restart-checklist` reports `safe_to_resume=1`
- `recovery-report` shows unresolved and recovery-required counts cleared, `resume_allowed=1`, and no remaining dust / lot resume blocker
- Live monitoring remains stable for 30 to 60 minutes after resume

## 6. Kill Switch

- `KILL_SWITCH=true`: stop new orders immediately
- `KILL_SWITCH_LIQUIDATE=true`: attempt flattening during kill-switch handling
- After kill-switch handling, verify `health`, `recovery-report`, and `reconcile`

## 7. Healthcheck and Backup

Healthcheck thresholds:

- `HEALTH_MAX_CANDLE_AGE_SEC=180`
- `HEALTH_MAX_ERROR_COUNT=3`
- `HEALTH_MAX_RECONCILE_AGE_SEC` and `HEALTH_MAX_UNRESOLVED_ORDER_AGE_SEC` are not core `.env.example` template defaults today.
- If your operator wrapper or local healthcheck tooling uses reconcile-age or unresolved-order-age thresholds, document them in the runtime env or service config as additional optional operator settings rather than assuming they come from the base template.

Backup verification:

```bash
BACKUP_VERIFY_RESTORE=1 ./scripts/backup_sqlite.sh
python3 tools/verify_sqlite_restore.py /var/lib/bithumb-bot/backup/live/db/<backup_file>.sqlite
```

## 8. systemd Env File Separation

- `bithumb-bot.service` uses `BITHUMB_ENV_FILE=/etc/bithumb-bot/bithumb-bot.live.env`
- `bithumb-bot-healthcheck.service` and `bithumb-bot-backup.service` use the same explicit runtime env file
- The env file must keep DB, notifier, and safety settings aligned

## 9. Pass / Fail

- Pass: paper/live storage remains separated, live preflight passes, and recovery evidence is clear
- Fail: any rule breaks storage separation, live safety, or recovery integrity
