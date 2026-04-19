# Live Order Submit Root-Cause Report

## Summary

Live fills could alternate after patches or deployments because the `/v2/orders`
path was not a single deterministic path. The standard live pipeline built a
validated `SubmitPlan`, but final dispatch could still route through broker-level
compatibility overrides. Separately, live dry-run could return synthetic success
at the broker/API boundary, making "no real order sent" look like a successful
submission in local code.

The highest-likelihood root cause is the combination of:

1. compatibility submit overrides that could bypass the canonical private API
   request implementation,
2. dry-run fake success for private writes,
3. deployment/runtime drift between `LIVE_DRY_RUN`, `LIVE_REAL_ORDER_ARMED`, and
   the env file actually loaded by the service.

## Evidence

- `src/bithumb_bot/broker/bithumb_client.py` previously detected overrides of
  `BithumbBroker._submit_validated_order_payload` and `_post_private`; those
  overrides could change the actual `/v2/orders` dispatch behavior after tests,
  patches, or monkey-patched runtime construction.
- `src/bithumb_bot/broker/bithumb.py` previously let `_post_private()` return a
  fake dry-run success payload for private writes, and `BithumbBroker.place_order()`
  returned a synthetic `dry_...` exchange order id when `LIVE_DRY_RUN=true`.
- `src/bithumb_bot/broker/bithumb.py:BithumbPrivateAPI.request()` is the canonical
  signer/dispatcher. It builds the query-hash payload with
  `_canonical_payload_for_query_hash()`, signs JWT claims in
  `_order_submit_auth_context()`, and transmits the exact JSON bytes via
  `content=...`. Any route around this function can diverge from the signed body
  and logging contract.
- `src/bithumb_bot/config.py:validate_live_mode_preflight()` allowed live dry-run
  as a valid live preflight state. That is useful for diagnostics but unsafe as a
  steady-state deployment mode because the service can appear healthy while real
  orders are blocked.

## Affected Files And Functions

- Env/config: `bootstrap.py:load_explicit_env_file`, `bootstrap.py:bootstrap_argv`,
  `config.py:Settings`, `config.py:validate_live_mode_preflight`,
  `config.py:validate_live_real_order_execution_preflight`.
- Run entry: `app.py:cmd_run`, `engine.py:run_loop`.
- Submit planning: `live_submission_execution.py:execute_live_submission_and_application`,
  `live_submit_planning.py:build_live_submit_plan`, `order_submit.py:plan_place_order`.
- Payload construction: `order_payloads.py:build_order_payload_from_plan`,
  `order_payloads.py:validate_order_submit_payload`.
- Submit orchestration: `live_submit_orchestrator.py:run_standard_submit_pipeline`,
  `_validate_explicit_submit_plan`, `_plan_submit_attempt`, `_dispatch_submit_attempt`,
  `_confirm_submit_attempt`.
- Broker/API boundary: `bithumb.py:BithumbBroker.place_order`,
  `bithumb.py:BithumbPrivateAPI.request`.
- Signing/dispatch: `bithumb.py:_canonical_payload_for_query_hash`,
  `_query_string`, `_order_submit_auth_context`, `_json_body_text`.
- Response parsing: `bithumb_execution.py:execute_signed_order_request`,
  `bithumb_read_models.py:parse_order_confirmation`.

## Why It Alternated After Deployments

The bot could be deployed with the same strategy and ledger state but different
runtime flags or service env file contents. In one deployment, `LIVE_DRY_RUN=false`
and `LIVE_REAL_ORDER_ARMED=true` sent real orders. In another, `LIVE_DRY_RUN=true`
or an override path could produce local success without a real exchange submit.
Because compatibility hooks lived at the final dispatch boundary, patches that
looked unrelated to order construction could still alter the effective submit
path.

## Durable Fix

- `/v2/orders` now uses exactly one canonical implementation:
  `BithumbPrivateAPI.request("POST", "/v2/orders", json_body=...)`.
- Broker-level compatibility submit/post overrides are not used for canonical
  signed order submission.
- Live dry-run or unarmed submit attempts fail explicitly and log
  `dry_run_blocked`.
- The live run loop has an additional real-order execution preflight: live
  diagnostics may still run in dry-run, but the live trading loop cannot start
  unless real-order settings are explicit.
- Order-submit logs now distinguish `real_order_sent`, `dry_run_blocked`,
  `auth_signature_rejected`, and `exchange_rejected`.
