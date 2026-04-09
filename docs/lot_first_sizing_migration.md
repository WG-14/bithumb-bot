# Lot-First Sizing Migration Notes

This update keeps the existing ledger and order tables compatible, but changes the
execution boundary to be lot-first and exact.

## What changed

- Entry and exit sizing now derive order quantity from lot count first.
- The broker rejects direct submit attempts that are not exact lot multiples.
- Dust-only exits are suppressed before order submission instead of being treated
  as broker failures.
- Position snapshots now surface derived lot counts alongside quantity totals.

## Compatibility notes

- No database migration is required for the current storage shape.
- Existing quantity fields remain available.
- New lot-count fields are derived and additive:
  - `open_lot_count`
  - `dust_tracking_lot_count`
  - `reserved_exit_lot_count`
  - `sellable_executable_lot_count`

## Operational impact

- Live and paper execution should continue to work, but any direct call path
  that tries to submit a non-exact quantity will now fail fast.
- This is intentional and prevents silent quantity truncation at the broker
  boundary.
- Dust-only exits should now appear as suppressed decisions such as
  `dust_only_remainder` or `no_executable_exit_lot`, not as submission errors.
