# Runtime Scope Contract

Current production runtime scope is:

```text
multi_strategy_single_pair_single_interval
```

This means multiple active strategy instances may run in one process only when
they evaluate the same runtime `PAIR` and the same runtime `INTERVAL`.
Current multi-strategy support means multiple strategies determine one target
for one runtime pair and one runtime interval. It is not a multi-pair portfolio
runtime.

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
- one runtime strategy result bundle
- one portfolio allocation
- one authoritative `PortfolioTarget`
- one primary `ExecutionSubmitPlan`
- one execution cycle

`target_position_state(pair)` is pair-level actual target state for the current
runtime pair. It is not strategy-instance-level virtual target state, and it is
not interval-level virtual strategy lifecycle state. Before strategy-instance
or interval lifecycle support exists, future work must separate actual
portfolio target state from any strategy virtual target state.

Future multi-pair support requires pair-scoped runtime shards plus a
portfolio-level orchestrator. Each pair shard needs pair-specific target state,
pair-specific runtime data preflight, pair-scoped strategy decision bundles or
bundle partitioning, pair-specific allocation targets, pair-specific execution
plans, and pair-specific submit/reconcile loops. Real multi-pair trading also
requires cross-pair risk budget semantics and a currency-scoped
portfolio/accounting ledger or an equivalent multi-asset accounting model.
The current single-asset aggregate accounting shape, such as
`portfolio(id=1, asset_qty)`, is not sufficient live authority for multi-pair
trading.

Future multi-interval support similarly requires interval-scoped runtime data
preflight, interval-scoped decision bundles, interval-scoped allocation and
execution planning, and an explicit decision-clock/freshness policy. Until
those boundaries exist, interval mismatches fail closed with
`single_interval_runtime_unsupported`.
