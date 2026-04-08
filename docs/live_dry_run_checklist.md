# Live Dry-Run Checklist

## Purpose

Confirm that live mode is safe when `LIVE_DRY_RUN=true`.

Background: This checklist is for validation only and does not permit real orders.

## Startup Checks

- [ ] `BITHUMB_ENV_FILE` points to the explicit live env file with live DB and run-lock paths
- [ ] `LIVE_DRY_RUN=true`
- [ ] `LIVE_REAL_ORDER_ARMED=false`
- [ ] Notifier configuration is present and valid
- [ ] `/var/lib/bithumb-bot/data/live/trades/live.sqlite` backup is reachable
- [ ] `bithumb-bot.service` is running normally
- [ ] `bithumb-bot-healthcheck.timer` is enabled
- [ ] `bithumb-bot-backup.timer` is enabled

## Runtime Checks

- [ ] `sudo systemctl status bithumb-bot.service`
- [ ] `sudo journalctl -u bithumb-bot.service -n 100 --no-pager`
- [ ] No healthcheck error is present
- [ ] No halt state is present
- [ ] The service can still recover after restart

## During Dry-Run

- [ ] There is no duplicate execution
- [ ] The run lock behaves normally
- [ ] Reconcile does not report an error
- [ ] There are no unexpected unresolved open orders
- [ ] Notifier delivery works
- [ ] The backup timer runs normally

## Minimum Conditions for Switching to Real Orders

- [ ] No unexplained error persists over time
- [ ] systemd restart and reboot behavior is normal
- [ ] Healthcheck remains stable during live execution
- [ ] Incident documentation is complete
- [ ] Rollback and restore evidence is ready

## Pass / Fail Criteria

- Pass: all startup, runtime, and minimum conditions are satisfied.
- Fail: any live-safety, recovery, or separation rule is broken.
