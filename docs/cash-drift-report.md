# Cash Drift Report

`cash-drift-report` is a read-only operator diagnostic for cash accounting drift.

## Purpose

Use it to separate:
- broker cash
- local cash
- recent external cash adjustments
- unexplained residual delta

## Command

```bash
uv run bithumb-bot cash-drift-report --recent-limit 5
```

## Snapshot output

The command also writes a read-only JSON snapshot to:

```text
data/<mode>/reports/cash_drift_report/cash_drift_report_YYYY-MM-DD.json
```

## Notes

- The snapshot is report-class output, not trading state.
- It is mode-scoped, so paper and live stay separated.
- The recent adjustment list is intended to help long-term drift triage after restarts and repeated reconciles.
