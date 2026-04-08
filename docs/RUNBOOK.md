# RUNBOOK (Bithumb BTC Limited Unattended Operations)

> Scope: Bithumb BTC live operations with limited unattended runtime and explicit human intervention gates.
>
> 배경: 이 문서는 제한적 무인 운용을 위한 기존 운영 절차를 빠르게 확인하기 위한 요약본이다.

## Operating Model Summary

- This is not a fully unattended 24/7 autonomy model.
- The intended model is `systemd`-driven execution with explicit halt and resume gates.
- The runbook prioritizes `reconcile` and operator-confirmed `resume` over automatic continuation.
- If recovery evidence is unclear, use `recovery-report` as the final stop/go reference.

## Mode Classification Before Start

- [ ] `paper`: simulation only. No real orders, no loss exposure.
- [ ] `live + dry-run`: live environment checks only. Private reads are allowed, but private writes are blocked (`LIVE_DRY_RUN=true`).
- [ ] `live + armed`: real-order mode (`LIVE_DRY_RUN=false`, `LIVE_REAL_ORDER_ARMED=true`).
- [ ] `live + not armed`: fail fast if `LIVE_DRY_RUN=false` and `LIVE_REAL_ORDER_ARMED` is not `true`.

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
- `health` must show no stale-candle or error-condition problem.
- `recovery-report` must show no unresolved or recovery-required blocker before resume.

### C. Service and log checks

```bash
sudo systemctl restart bithumb-bot.service
sudo systemctl status bithumb-bot.service
sudo journalctl -u bithumb-bot.service -n 100 --no-pager

uv run bithumb-bot health
uv run bithumb-bot recovery-report
```

- Confirm the service is `active (running)`.
- Confirm `last_candle_age_sec`, `error_count`, and `trading_enabled` are healthy.
- Confirm `recovery-report` still shows no unresolved or recovery-required order state.

## Emergency Stop, Pause, and Resume

### A. Immediate stop

```bash
uv run bithumb-bot pause
```

- Use this to block new orders immediately.
- After pause, read `health`, `recovery-report`, and the most recent logs before resuming.

### B. Cancel open orders

```bash
uv run bithumb-bot cancel-open-orders
```

- Use this only when live mode needs exchange-side order cleanup.
- Follow it with `reconcile` and `recovery-report`.

### C. Resume

```bash
uv run bithumb-bot resume
```

- Never use `resume` until the blocker list is clear.
- Use `resume --force` only as a last resort and only after operator review.

## Recovery Checklist

1. Check `journalctl` for the last error cause.
2. Run `uv run bithumb-bot recovery-report`.
3. Confirm `unresolved_orders` and `recovery_required_orders` are both zero.
4. Run `uv run bithumb-bot reconcile` if state needs to be refreshed.
5. Re-run `health` and confirm `trading_enabled` is healthy.
6. Resume only after the state is understood.

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

Korean note: backup verification is a recovery safety check, not a convenience-only workflow.

## Test Groups

- Fast regression set:
  - `uv run pytest -q -m fast_regression`
- Slow integration/live-like set:
  - `uv run pytest -q -m slow_integration`

Prefer the fast set first. Keep the slow set separate unless you are validating restart, recovery, or live-like execution paths.
