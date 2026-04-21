# Order Submit Root-Risk Report

## Remaining Risks

1. **P0: alternate order-submit routes could be reintroduced.**  
   Files/functions: `src/bithumb_bot/broker/bithumb.py::BithumbPrivateAPI.request`,
   `src/bithumb_bot/broker/bithumb.py::BithumbBroker._request_private`,
   `src/bithumb_bot/broker/live_order_contract.py`.  
   The canonical route is `BithumbBroker.place_order()` ->
   `build_place_order_submission_flow()` -> `BithumbPrivateAPI.submit_order()` ->
   `POST /v2/orders`. Before this patch, direct `POST /v2/orders` was blocked,
   but legacy exact endpoints such as `POST /v1/orders` were not structurally
   denied at the private request layer. A future refactor could have introduced
   a legacy submit path while still passing many payload/signing tests.

2. **P1: deployment drift fingerprinting did not include explicit env-file provenance.**  
   Files/functions: `src/bithumb_bot/config.py::live_execution_contract_summary`,
   `src/bithumb_bot/config.py::log_live_execution_contract`,
   `src/bithumb_bot/app.py::cmd_run`.  
   Runtime logs included roots, DB path, arming state, and API material presence,
   but the live execution contract fingerprint did not include which explicit env
   file was selected and loaded. Two deployments with different env files could
   produce the same contract fingerprint even when env provenance was the cause
   of behavior drift.

3. **P1: structural submit-path invariants were mostly convention plus targeted tests.**  
   Files/functions: `tests/test_bithumb_private_api.py`,
   `tests/test_live_broker.py`, new `tests/test_order_submit_hardening.py`.  
   Existing tests strongly covered signing, JSON body bytes, query hashes, and
   dry-run blocking. The missing layer was a small invariant test that fails if
   source code adds obvious static legacy order-submit POST calls.

4. **P1: CI safety coverage was not declared in-repo.**  
   Files/functions: new `.github/workflows/safety-regression.yml`.  
   Without a workflow, dangerous changes could merge depending on external
   process discipline. A minimal in-repo safety workflow now runs the focused
   order-submit, live-preflight, env-loading, and deployment-contract tests.

## Priority of Fixes

1. Centrally reject legacy/alternate exact order-submit POST endpoints.
2. Include explicit env-file provenance in the live execution contract summary,
   log line, and fingerprint.
3. Add focused regression tests for route rejection and source-level invariant
   scanning.
4. Add minimal CI coverage for the safety-critical regression set.

## BUY Submit Authority Update

The live BUY dispatch contract is now explicitly treated as a planned-submit
contract:

- `SubmitPlan.requested_qty` records the strategy/cash-budget request.
- `SubmitPlan.exchange_constrained_qty` records exchange-step constrained BUY
  quantity.
- `SubmitPlan.submitted_qty` is the canonical broker dispatch quantity.
- `SubmitPlan.lifecycle_executable_qty` is lifecycle ingestion context, not
  dispatch authority.

Direct `POST /v2/orders` remains disabled at the private request layer even when
a caller supplies `client_order_id`; the only permitted order-submit route is
the validated place-order flow carrying the internal order-submit authority
token.

Submit evidence also carries compact runtime identity fields, including the
live execution contract fingerprint, code commit provenance when available,
working-tree dirty marker when available, and explicit env-file selection. These
fields are diagnostic and forensic surfaces only; they do not change order
authority.

## Why This Reduces Fill Instability

- Live submission now has fewer viable bypass surfaces: exact legacy POST order
  endpoints fail before network dispatch.
- The `/v2/orders` signing/body contract remains deterministic and tested:
  canonical query string, `query_hash`, JSON bytes, and `content=` dispatch.
- Startup live logs now make env-file drift visible through the same fingerprint
  operators use for runtime contract comparison.
- Future refactors that add obvious alternate submit calls fail tests quickly.
