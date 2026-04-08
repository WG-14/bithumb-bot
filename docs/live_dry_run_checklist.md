# Live Dry-Run Checklist

## Purpose

Confirm that live mode is safe when `LIVE_DRY_RUN=true`.

배경: 이 체크리스트는 검증용이며 실주문을 허용하지 않는다.

## Startup Checks

- [ ] `.env.live` uses live DB and lock paths
- [ ] `LIVE_DRY_RUN=true`
- [ ] `LIVE_REAL_ORDER_ARMED=false`
- [ ] Notifier configuration is valid
- [ ] `/var/lib/bithumb-bot/data/live/trades/live.sqlite` backup is possible
- [ ] `bithumb-bot.service` is running normally
- [ ] `bithumb-bot-healthcheck.timer` is enabled
- [ ] `bithumb-bot-backup.timer` is enabled

## Runtime Checks

- [ ] `sudo systemctl status bithumb-bot.service`
- [ ] `sudo journalctl -u bithumb-bot.service -n 100 --no-pager`
- [ ] No healthcheck error is present
- [ ] No halt state is present
- [ ] The service can still recover itself

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
