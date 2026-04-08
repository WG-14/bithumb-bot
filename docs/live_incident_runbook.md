# Live Incident Runbook

## Purpose

This document gives operators a short, English-first checklist for live incidents.

Background: This document preserves the operational intent while providing a short checklist for rapid triage and recovery verification.

## 1. Initial Checks

1. `sudo systemctl status bithumb-bot.service --no-pager`
2. `sudo journalctl -u bithumb-bot.service -n 100 --no-pager`
3. `sudo systemctl status bithumb-bot-healthcheck.timer --no-pager`
4. `./scripts/check_live_runtime.sh`

## 2. When to Stop Immediately

Stop live execution immediately if any of the following are true:

- Repeated exceptions are appearing
- Order, fill, or state corruption is suspected
- Halt state has been re-triggered
- Live order safety looks unclear
- The DB looks inconsistent or damaged

## 3. Stop Procedure

1. Stop the service:
   - `sudo systemctl stop bithumb-bot.service`
2. Confirm the service is stopped:
   - `sudo systemctl status bithumb-bot.service --no-pager`
3. Collect incident evidence:
   - `./scripts/collect_live_snapshot.sh`

## 4. DB Backup After Stop

- Run a fresh DB backup immediately after the incident stop.
- Do not depend on the periodic backup timer alone.

Example:

```bash
cp /var/lib/bithumb-bot/data/live/trades/live.sqlite /var/lib/bithumb-bot/backup/live/db/live.manual.$(date +%Y%m%d_%H%M%S).sqlite
```

## 5. Minimum Forensics

Review the most recent journal evidence for:

- Healthcheck outcome
- Restart-before / restart-after state
- Reconcile-related logs
- Notifier warnings
- Recent DB and backup state

## 6. Safety Checklist

- [ ] Confirm operator acknowledgement
- [ ] Confirm there is no repeated error pattern
- [ ] Confirm env settings are still correct
- [ ] Confirm lock and DB state are still consistent
- [ ] Confirm restore evidence is available if needed

## 7. Recovery Gate

1. `sudo systemctl start bithumb-bot.service`
2. `sudo systemctl status bithumb-bot.service --no-pager`
3. `sudo journalctl -u bithumb-bot.service -n 100 --no-pager`

## 8. Incident Log

Record the following fields:

- Start time
- Symptoms
- Scope
- Root cause
- Decision
- Recovery action

## 9. Dust Residual vs Unresolved Order

- `harmless_dust`: broker/local remainder is matched closely enough to be policy-classified as harmless dust. It can still be a real BTC remainder.
- `unsafe dust` / `mismatch dust`: a dust-like remainder that is not policy-approved to resume, including broker/local mismatch or recovery-unclear residue.
- `effective flat`: the strategy may treat the residual as flat for BUY entry. This is not a literal zero-balance claim.
- `dust_state=harmless_dust` means the remainder may be resume-safe only when the policy also allows resume and new orders.
- `dust_state=dangerous_dust` means the remainder is not safely resumable. Treat it as operator-review required.
- `unresolved_count > 0` or `recovery_required_count > 0` means the problem is still recovery-related, not dust-only.
- Do not restart immediately after a manual app sell until `health`, `recovery-report`, and `ops-report` are re-run.
- If the remaining quantity is below exchange minimums, confirm both quantity and notional minimums before trying another liquidation.

## 10. Manual App Sell Caution

- Do not use manual app sells as the normal dust-handling path.
- Prefer `reconcile` plus report comparison before any manual liquidation retry.
- If the bot was stopped and a manual sell happened in the app, rerun the three reports before restart.
- Do not use `resume --force` as a shortcut around dust review.

## 11. Common Readback Order

Use this order when you need to reason about a live incident:

1. `health`
2. `recovery-report`
3. `ops-report --limit 20`

Note: If the above three outputs disagree, treat the discrepancy as a recovery task, not as a cosmetic issue.
