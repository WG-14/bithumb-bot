# Tracked Residue State Model

Verified findings:
- `summarize_position_lots()` zeroed `effective_min_trade_qty` whenever `executable_lot is None`, even when the snapshot still had authoritative lot metadata.
- Several operator/reporting surfaces rebuilt position state from qty plus dust metadata only, without reusing the lot-aware snapshot inputs already used by runtime readiness.
- The recovery model already distinguishes run-loop permission, entry permission, closeout permission, execution flatness, and accounting flatness. The main failure was state construction drift, not total absence of a residue operating model.

Root cause class:
- Primary: dust-only metadata loss during snapshot/state reconstruction.
- Secondary: state-surface divergence from qty-only fallback paths in some operator commands.

Chosen design direction:
- Preserve authoritative lot metadata in dust-only snapshots instead of conflating "not sellable" with "minimum metadata unknown".
- Recover lot-definition metadata from accounted BUY order/fill evidence when `open_position_lots` rows are incomplete.
- Reuse one canonical DB-backed position-state builder in operator/reporting surfaces so health, recovery, flatten, and readiness interpret the same state.

Rejected alternatives:
- Forcing tracked dust to become SELL authority.
- Treating broker dust metadata as the canonical lot boundary source.
- Narrow special-casing around one residual quantity or one incident record set.
