# Live Fill Stability Hardening

## Remaining Risks

- Bithumb may still change undocumented private API behavior, error payloads, or order type rules; startup preflight and order-rule checks reduce that risk but cannot remove exchange-side drift.
- Operators can still deploy different env files across hosts; the startup contract fingerprint makes this visible, but deployment review must still compare the emitted values.
- Tests use mocked HTTP clients for signing and dispatch contract checks; a final real live dry-run remains required before arming production fills.

## Changes Made

- Centralized the live `/v2/orders` endpoint, content type, and dispatch authority in `broker/live_order_contract.py`.
- Blocked direct `BithumbPrivateAPI.request("POST", "/v2/orders", ...)` usage; live order submission must pass through `submit_order()` with validated place-order flow authority.
- Added pre-send checks that the signed request canonical payload still matches the payload being transmitted, and that query hash/content bytes/content type stay aligned.
- Added a single `validate_live_run_startup_contract()` wrapper for live run-loop startup gates.
- Added redacted `[LIVE_EXECUTION_CONTRACT]` startup logging with a stable fingerprint over mode, arming flags, submit contract profile, order-rule fallback profile, managed roots, DB path, run-lock path, API key/secret presence and lengths, and API base.
- Extended auth diagnostics with a canonical `/v2/orders` request preview showing submit path, dispatch authority, content type, payload keys, and query hash inclusion.
- Rejected the ambiguous `LIVE_DRY_RUN=true` plus `LIVE_REAL_ORDER_ARMED=true` runtime combination.

## Future Regressions Now Protected

- Reintroducing raw `/v2/orders` private request dispatch fails before HTTP.
- Mutating a signed order payload after signing fails before HTTP.
- Replacing the canonical submit client with `_private_api.request(...)` fails a regression test.
- Startup without the live run startup contract still routes through the same wrapper from the CLI run path and engine run loop.
- Auth diagnostics now show whether the expected canonical order submit branch is selected.

## Operator Impact

- Live startup logs one new redacted `[LIVE_EXECUTION_CONTRACT]` line with a fingerprint and key runtime flags.
- `LIVE_DRY_RUN=true` and `LIVE_REAL_ORDER_ARMED=true` is now a preflight failure; use dry-run unarmed for diagnostics or dry-run false plus armed true for real orders.
- Direct private write attempts to `/v2/orders` fail loudly instead of acting as a possible alternate submit path.
