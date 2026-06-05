# Runtime Scope Contract

Current production runtime scope is:

```text
multi_strategy_single_pair_single_interval
```

This means multiple active strategy instances may run in one process only when
they evaluate the same runtime `PAIR` and the same runtime `INTERVAL`.

The current runtime does not support:

- multiple pairs in one process or execution cycle
- multiple intervals in one process or execution cycle
- multiple portfolio targets submitted or reconciled in one execution cycle

`multi_pair_runtime_unsupported` is intentional fail-closed behavior. It is not
a bug to bypass. Removing the validator is unsafe because runtime data preflight,
target state, execution plan persistence, submit/reconcile loops, and accounting
are not multi-pair-safe.

The current runtime envelope is single-pair and single-interval throughout:

- one runtime pair
- one runtime interval
- one closed candle input per decision cycle
- one authoritative `PortfolioTarget`
- one target-delta submit plan

Future multi-pair support requires pair-scoped runtime shards plus a
portfolio-level orchestrator. Each pair shard needs pair-specific target state,
pair-specific runtime data preflight, pair-scoped strategy decision bundles or
bundle partitioning, pair-specific allocation targets, pair-specific execution
plans, and pair-specific submit/reconcile loops. Real multi-pair trading also
requires cross-pair risk budget semantics and a currency-scoped
portfolio/accounting ledger or an equivalent multi-asset accounting model.

Future multi-interval support similarly requires interval-scoped runtime data
preflight, interval-scoped decision bundles, and interval-scoped allocation and
execution planning. Until those boundaries exist, interval mismatches fail
closed with `single_interval_runtime_unsupported`.
