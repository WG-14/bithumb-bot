# Order and Fill Stability Hardening Audit

## Remaining Risks

1. **CI drift can hide safety regressions.**
   - Files/functions: `.github/workflows/safety-regression.yml`, `pyproject.toml` marker `fast_regression`.
   - Why it matters: order submit, fill, preflight, and recovery tests can exist locally but stop protecting deployments if the workflow only runs a hand-picked subset.
   - Priority: P0.

2. **Live command entrypoints can bypass intent if dispatch is copied or extended carelessly.**
   - Files/functions: `src/bithumb_bot/app.py::main`, `src/bithumb_bot/app.py::_enforce_live_command_guard`, `src/bithumb_bot/app.py::cmd_run`.
   - Why it matters: a new command that creates, cancels, flattens, or starts live execution without preflight can send orders under stale or contradictory config.
   - Priority: P0.

3. **The `/v2/orders` request contract depends on exact byte-level behavior.**
   - Files/functions: `src/bithumb_bot/broker/bithumb.py::BithumbPrivateAPI._validated_order_submit_auth_context`, `src/bithumb_bot/broker/live_order_contract.py::require_order_submit_transmission_contract`, `src/bithumb_bot/broker/bithumb_client.py::submit_signed_order_request`.
   - Why it matters: a harmless-looking change from `content=` to `json=`, a different JSON serializer, or mismatched `query_hash` canonicalization can cause exchange auth failures or ambiguous submit outcomes.
   - Priority: P0.

4. **Compatibility shims remain a recurring reintroduction risk.**
   - Files/functions: `src/bithumb_bot/broker/bithumb.py::_request_private`, `src/bithumb_bot/broker/bithumb.py::_submit_validated_order_payload`, `src/bithumb_bot/broker/live_order_contract.py::reject_forbidden_order_submit_route`.
   - Why it matters: alternate raw submit paths or `/v1/order(s)` POST paths would split order behavior and make duplicate-submit or unknown-submit handling harder to reason about.
   - Priority: P0.

5. **Deployment config drift remains operationally risky.**
   - Files/functions: `src/bithumb_bot/config.py::live_execution_contract_summary`, `src/bithumb_bot/config.py::live_execution_contract_fingerprint`, `src/bithumb_bot/config.py::log_live_execution_contract`.
   - Why it matters: wrong env files, wrong roots, dry-run/armed contradictions, or fallback profiles can change live behavior without code changes.
   - Priority: P1.

## Implemented Hardening

- Added a central live command guard in `app.main` so live `run` requires the startup contract before dispatch and live write/recovery commands require live preflight before their command bodies run.
- Moved byte-level `/v2/orders` transmission invariants into `live_order_contract.py` so signing, query hash, JSON bytes, `content=`, and `Content-Type` are validated as one reusable contract.
- Added tests that fail before any HTTP request if body bytes, request kwargs, or content type drift.
- Strengthened GitHub Actions by expanding the focused order-submit safety job with the new byte-level contract tests and canonical submit module tests.

## Priority Order

1. Keep CI required on PRs for `order-submit-safety`.
2. Keep all real live submit paths flowing through `BithumbBroker.place_order()` -> `build_place_order_submission_flow()` -> `submit_signed_order_request()` -> `BithumbPrivateAPI.submit_order()`.
3. Treat any new live command as requiring explicit classification in `LIVE_COMMAND_GUARDS` when it can start, submit, cancel, flatten, recover, or mutate live broker/order state.
4. Keep config fingerprints in live logs and compare them during deployment validation.

## Residual Risk

- GitHub branch protection must be configured outside the repository to require the new workflow jobs.
- The repository still has stale `fast_regression`-marked legacy tests that do not pass against the current canonical submit path and live arming model; they should be retired or rewritten before `uv run pytest -q -m fast_regression` can become a required CI gate.
- The central command guard protects `app.main` dispatch; direct imports of low-level helper functions still rely on their existing in-function checks and broker-level fail-closed behavior.
- This patch does not alter exchange-facing order semantics; it hardens validation around the existing canonical path.
