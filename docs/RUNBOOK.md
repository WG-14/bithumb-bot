# RUNBOOK (Bithumb BTC Limited Unattended Operations)

> Scope: Bithumb BTC live operations with limited unattended runtime and explicit human intervention gates.
>
> Background: This runbook preserves the existing operating procedure and serves as a quick reference for limited unattended operations.

## Operating Model Summary

- This is not a fully unattended 24/7 autonomy model.
- The intended model is `systemd`-driven execution with explicit halt and resume gates.
- The runbook prioritizes `reconcile` and operator-confirmed `resume` over automatic continuation.
- If recovery evidence is unclear, use `recovery-report` as the final stop/go reference.

## Mode Classification Before Start

- [ ] `paper`: simulation only. No real orders, no loss exposure.
- [ ] `live dry-run`: live environment checks only. Private reads are allowed, but private writes are blocked (`LIVE_DRY_RUN=true`).
- [ ] `live armed`: real-order mode (`LIVE_DRY_RUN=false`, `LIVE_REAL_ORDER_ARMED=true`).
- [ ] `live not armed`: fail fast if `LIVE_DRY_RUN=false` and `LIVE_REAL_ORDER_ARMED` is not `true`.

Starting assumptions:

- [ ] The current session mode is explicitly one of `paper`, `live dry-run`, or `live armed`.
- [ ] BTC position and balance exposure are understood before arming.
- [ ] Dry-run and live real-order execution are not mixed.

## Conservative Live Preset

For a 1,000,000 KRW account, start conservatively:

- `MAX_ORDER_KRW=30000`
- `MAX_DAILY_LOSS_KRW=20000`
- `MAX_DAILY_ORDER_COUNT=6`
- `KILL_SWITCH=false`
- `KILL_SWITCH_LIQUIDATE=false`
- `LIVE_DRY_RUN=true` for the first live validation pass

Emergency rule:

- If daily loss, halt, or other emergency conditions trip, stop new orders immediately and reconcile before resuming.

## Deployment and Units

- `deploy/systemd/bithumb-bot.service`: main loop (`Restart=always`)
- `deploy/systemd/bithumb-bot-healthcheck.timer`: hourly status check
- `deploy/systemd/bithumb-bot-backup.timer`: periodic SQLite backup
- `scripts/healthcheck.py`: stale candle, error count, and trading-enabled checks
- `scripts/backup_sqlite.sh`: SQLite backup and restore-verify flow

Operating scope:

- Linux and WSL/Linux only for the run lock semantics
- Native Windows is not a supported run-lock execution target

## Installation and Enablement

```bash
sudo mkdir -p /etc/bithumb-bot
sudo cp .env.example /etc/bithumb-bot/bithumb-bot.live.env

RENDER_DIR="$(mktemp -d)"
BITHUMB_BOT_ROOT="$(pwd)" \
BITHUMB_UV_BIN="$(command -v uv)" \
BITHUMB_RUN_USER="$(id -un)" \
./deploy/systemd/render_units.sh "${RENDER_DIR}"
sudo cp "${RENDER_DIR}"/bithumb-bot.service /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-healthcheck.service /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-healthcheck.timer /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-backup.service /etc/systemd/system/
sudo cp "${RENDER_DIR}"/bithumb-bot-backup.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now bithumb-bot.service
sudo systemctl enable --now bithumb-bot-healthcheck.timer
sudo systemctl enable --now bithumb-bot-backup.timer
```

Notes:

- The rendered units point `BITHUMB_ENV_FILE` at the explicit runtime env file.
- The service and timer units must all reference the same explicit env file.
- `healthcheck` is fail-fast. A missing env file must fail the run rather than continue.

## Startup Checklist

### A. Mode, limits, and path checks

1. Confirm the intended `MODE` is set correctly.
2. If `MODE=live`, confirm the following are set to finite positive values:
   - `MAX_ORDER_KRW`
   - `MAX_DAILY_LOSS_KRW`
   - `MAX_DAILY_ORDER_COUNT`
   - `MAX_ORDERBOOK_SPREAD_BPS`
   - `MAX_MARKET_SLIPPAGE_BPS`
   - `LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS`
3. Start in `LIVE_DRY_RUN=true` for the initial live pass.
4. Switch to `LIVE_DRY_RUN=false` and `LIVE_REAL_ORDER_ARMED=true` only after checks pass.
5. Confirm `KILL_SWITCH=false` unless an emergency requires it.
6. Keep `KILL_SWITCH_LIQUIDATE` disabled unless you explicitly need flattening behavior.
7. Keep `.env.example` as a template only. Put real values in the runtime env file.

### B. Broker readiness checks

Run these commands as written:

```bash
uv run bithumb-bot broker-diagnose
uv run bithumb-bot health
uv run bithumb-bot recovery-report
uv run bithumb-bot reconcile
uv run bithumb-bot recovery-report
```

Interpretation:

- `broker-diagnose` must pass before live arming.
- Judge BUY `price=None` / BUY market support from `broker-diagnose`, not from indirect inference in other surfaces.
- In `broker-diagnose`, inspect the `BUY price=None chance resolution` line and confirm:
  `allowed`, `resolved_order_type`, `support_source`, `decision_basis`, `alias_used`, and `block_reason`.
- `health` must show no stale-candle or error-condition problem.
- `recovery-report` must show no unresolved or recovery-required blocker before resume.

### C. Service and log checks

```bash
sudo systemctl restart bithumb-bot.service
sudo systemctl status bithumb-bot.service
sudo journalctl -u bithumb-bot.service -n 100 --no-pager
sudo journalctl -u bithumb-bot.service -f

uv run bithumb-bot health
uv run bithumb-bot recovery-report
```

- Confirm the service is `active (running)`.
- For the rendered `bithumb-bot.service`, `StandardOutput=journal` and `StandardError=journal`.
  Treat `journalctl -u bithumb-bot.service -f` as the canonical live log stream unless the unit is explicitly changed to redirect stdout/stderr to a managed runtime log file.
  File logs remain useful supporting evidence, but a quiet file log does not prove the systemd process is stopped.
- Confirm `last_candle_age_sec`, `error_count`, and `trading_enabled` are healthy.
- Confirm `recovery-report` still shows no unresolved or recovery-required order state.

## Emergency Stop, Pause, and Resume

### A. Integrated emergency stop

```bash
uv run bithumb-bot panic-stop
uv run bithumb-bot panic-stop --flatten
```

- `panic-stop` is the current integrated live emergency path.
- It blocks new orders immediately, cancels open orders, and can optionally attempt flattening with `--flatten`.
- Use `panic-stop --flatten` only when exposure reduction is required and the live situation justifies an explicit flatten attempt.
- After either command, read `health` and `recovery-report` before considering `resume`.

### B. Pause only

```bash
uv run bithumb-bot pause
```

- Use this to block new orders immediately.
- `pause` does not cancel open orders; use it when you need a persistent halt without the integrated cleanup path.
- After pause, read `health`, `recovery-report`, and the most recent logs before resuming.

### C. Decomposed cleanup path

```bash
uv run bithumb-bot cancel-open-orders
```

- Use this after `pause` when live mode needs exchange-side order cleanup without invoking `panic-stop`.
- Follow it with `reconcile` and `recovery-report`.

### D. Resume

```bash
uv run bithumb-bot resume
```

- Never use `resume` until `recovery-report` shows `resume_allowed=1` / `can_resume=true` and the blocker list is clear.
- Check the current dust and lot signals before resuming:
  - `dust_state`, `dust_resume_allowed`, and `dust_treat_as_flat`
  - `open_lot_count`, `dust_tracking_lot_count`, `sellable_executable_lot_count`, and `sellable_executable_qty`
  - `terminal_state` and `exit_block_reason`
- Use `resume --force` only as a last resort and only after operator review.

## Recovery Checklist

1. Check `journalctl -u bithumb-bot.service` for the last live-loop decision or error cause.
2. Run `uv run bithumb-bot recovery-report`.
3. Confirm `unresolved_count` and `recovery_required_count` are both zero.
4. Confirm `[P2] resume_eligibility` shows `resume_allowed=1`, `can_resume=true`, and no active blockers.
5. Review `[P3] dust_residual` and `[P3.1] lot_exposure`.
6. Review recent strategy decision flow for `raw_signal`, `final_signal`, `final_action`, `submit_expected`, `pre_submit_proof`, and `execution_block_reason`.
7. Do not treat a clear unresolved count alone as sufficient if `dust_state=blocking_dust`, `dust_resume_allowed=0`, `sellable_executable_qty=0`, or `exit_block_reason` still indicates a lot/dust boundary blocker.
   If `residual_inventory_state=RESIDUAL_INVENTORY_TRACKED` and `final_action=CLOSE_RESIDUAL_CANDIDATE`, check `RESIDUAL_LIVE_SELL_MODE`, `submit_expected`, `pre_submit_proof`, and `execution_block_reason` before taking action.

### Residual Inventory Policy Verification

Use these commands on EC2 after deployment:

```bash
MODE=live uv run bithumb-bot health | egrep \
  'canonical_state|residual_inventory|new_entry_allowed|exit_allowed|sellable_executable|run_loop|can_resume'

MODE=live uv run bithumb-bot decision-telemetry --limit 20 | egrep \
  'raw_signal|final_signal|final_action|submit_expected|pre_submit|residual|buy_delta|execution_block'

MODE=live uv run bithumb-bot ops-report --limit 50 | egrep \
  'final_action|submit_expected|pre_submit|residual|dust_only|buy_delta|execution_block'

sudo journalctl -u bithumb-bot.service -n 100 --no-pager | egrep \
  'strategy decision|final_action|submit_expected|pre_submit|residual|dust_only|BUY|SELL'
```

Expected residual SELL mode behavior:

- `RESIDUAL_LIVE_SELL_MODE=telemetry`: `final_action=CLOSE_RESIDUAL_CANDIDATE`, `pre_submit_proof=passed`, `submit_expected=0`, `execution_block_reason=residual_live_sell_mode_telemetry`.
- `RESIDUAL_LIVE_SELL_MODE=dry_run`: residual submit plan is built, `submit_expected=0`, `execution_block_reason=residual_live_sell_mode_dry_run`.
- `RESIDUAL_LIVE_SELL_MODE=enabled`: residual submit is allowed only when proof passes, `LIVE_REAL_ORDER_ARMED=true`, and `LIVE_DRY_RUN=false`; otherwise it remains fail-closed with an explicit block reason.

Expected BUY sizing behavior:

- `RESIDUAL_BUY_SIZING_MODE=telemetry`: target/current/delta are reported, but the existing BUY sizing remains unchanged.
- `RESIDUAL_BUY_SIZING_MODE=delta`: BUY sizing uses `target_exposure_krw - current_effective_exposure_krw`; if tracked residual already covers target, no BUY submits.

### Target Position Shadow Mode

- `TARGET_EXECUTION_SHADOW=true` does not change live order behavior.
- It compares the current lot-native execution decision against a target-position decision using current position, target position, and target delta telemetry.
- It is intended to validate whether target-position execution would remove residual/dust deadlocks before any live migration.
- Do not switch live execution to `EXECUTION_ENGINE=target_delta` until shadow telemetry has been reviewed and a separate migration patch proves the submit path.
- Because target state is not persisted yet, HOLD target continuity is reported as `target_state_persistence=not_yet_persisted` and `target_block_reason=missing_persistent_target_state`.
- Compare `raw_signal`, `final_signal`, `final_action`, `submit_expected`, `execution_block_reason`, `residual_live_sell_mode`, and `residual_buy_sizing_mode` against `target_delta_side`, `target_would_submit`, `target_submit_qty`, `target_delta_notional_krw`, `target_block_reason`, and `target_position_truth_state` in `decision-telemetry` or `ops-report`.
8. Run `uv run bithumb-bot reconcile` if state needs to be refreshed.
9. Re-run `health` and confirm `trading_enabled` is healthy, `can_resume=true`, and the current dust indicators do not contradict resume.
10. Resume only after the state is understood.

### Position Authority Projection Drift

If `recovery-report`, `health`, or `audit-ledger` shows either:

- `AUTHORITY_PROJECTION_NON_CONVERGED_PENDING`
- `HISTORICAL_FRAGMENTATION_PROJECTION_DRIFT`

then treat `open_position_lots` as a disposable projection, not as independent truth.
Do not resume or manually edit the table.

Before any live DB mutation:

- Back up the live DB first using the normal backup flow.
- Do not apply any repair unless the preview shows `safe_to_apply=1`.
- The command must remain dry-run unless `--apply --yes` is provided.

Allowed:

- `uv run bithumb-bot rebuild-position-authority --full-projection-rebuild`
- `uv run bithumb-bot rebuild-position-authority --full-projection-rebuild --apply --yes`

Apply only after the dry-run shows:

- accounting projection OK
- broker/portfolio converged
- no remote open orders
- no unresolved or recovery-required orders
- no pending submit / submit unknown orders

Forbidden:

- `uv run bithumb-bot run`
- `uv run bithumb-bot resume`
- manual SQL `DELETE` / `UPDATE` against `open_position_lots`

Operator sequence:

```bash
./scripts/backup_sqlite.sh
MODE=live uv run bithumb-bot rebuild-position-authority --full-projection-rebuild
# apply only if safe_to_apply=1
MODE=live uv run bithumb-bot rebuild-position-authority --full-projection-rebuild --apply --yes --note "operator-reviewed projection rebuild"
MODE=live uv run bithumb-bot audit-ledger
MODE=live uv run bithumb-bot recovery-report
MODE=live uv run bithumb-bot restart-checklist
MODE=live uv run bithumb-bot health
```

Do not resume live until all of the following are true:

- `can_resume=true`
- `safe_to_resume=1`
- `live_ready=1`
- `lot_projection_converged=1`
- the startup safety gate is not blocked

## Post-Change Validation

After a live change, verify:

- `paper` and `live` env files are still separated
- live DB paths remain repository-external
- run lock lives under `run/live/`
- backup paths remain repository-external
- healthcheck, backup, and the main service all read the same explicit live env file

## Backup / Restore

```bash
./scripts/backup_sqlite.sh
python3 tools/verify_sqlite_restore.py /var/lib/bithumb-bot/backup/live/db/<backup_file>.sqlite
```

Note: backup verification is a recovery safety check, not a convenience-only workflow.

## Test Groups

- Fast regression set:
  - `uv run pytest -q -m fast_regression`
- Slow integration/live-like set:
  - `uv run pytest -q -m slow_integration`

Prefer the fast set first. Keep the slow set separate unless you are validating restart, recovery, or live-like execution paths.
