## Validation

- [ ] Default PR validation: `./scripts/run_fast_pr_tests.sh`
- [ ] Selector-less full pytest was not used as default local PR validation.

## Strategy Workload Delta

For strategy-related changes, complete every item below. For non-strategy changes, state `not strategy-related`.

- Strategy Level: one of `level_1_research_only`, `level_2_replay_compatible`, `level_3_promotion_grade`, or `not_strategy_related`.
- Registration Path: one of `builtin_manifest`, `external_entry_point`, or `not_strategy_related`.
- Built-in Reason: one of `official_example`, `canary`, `fail_safe`, `maintained_baseline`, `approved_core_strategy`, or `not_applicable`.
- Entry Point Group: `bithumb_bot.strategy_plugins` or `not_applicable`.
- Level contract helper or equivalent focused test: `assert_research_only_contract`, `assert_replay_compatible_contract`, `assert_live_eligible_contract`, or equivalent focused runtime/live gate coverage.
- Inventory Evidence: `strategy-plugin-inventory --json checked` or `not_strategy_related`.
- Default-fast workload delta: `estimated_strategy_runs=<delta>` or `no default-fast workload delta`.
- Research/nightly workload delta: `estimated_strategy_runs=<delta>`, `estimated_tick_events=<delta>`, `estimated_audit_stream_rows=<delta>`, or `no research/nightly workload delta`.
- Workload delta JSON (`workload_delta_json`): include machine-readable `base` and `head` values for `expensive_test_count`, `strategy_count`, `manifest_count`, `strategy_canary_count`, `estimated_strategy_runs`, and `estimated_audit_stream_rows`.
- Strategy canary workload: state `strategy_canary_count delta=<delta>`; values greater than 1 require explicit guard-approved evidence.
- New expensive nodeids: list new expensive nodeids or `none`.
- New E2E reasons: list each new `must_be_e2e_reason` and `e2e_canary_group` or `none`.
- Newly added expensive research tests: list any `research_e2e`, `audit_e2e`, `walk_forward_e2e`, `parallel_e2e`, `research_kernel`, `slow_research`, `nightly`, or `memory_sensitive` tests, or state `none`.
- Real E2E/kernel justification: for each new real E2E/kernel test, explain why lower-level contract coverage is insufficient and confirm inventory/budget metadata was added.

## Strategy Boundary

- [ ] New strategy authoring stays plugin-centered.
- [ ] In-repo built-in strategy plugins updated `src/bithumb_bot/strategy_plugins/builtin_manifest.py` with a valid Built-in Reason, or external strategy packages declared a `bithumb_bot.strategy_plugins` entry point.
- [ ] Strategy appears in `list_research_strategy_plugins()` and resolves through `resolve_research_strategy_plugin()`.
- [ ] Strategy plugin inventory verification: `uv run bithumb-bot strategy-plugin-inventory --json` shows the expected strategy source, contract hash, live eligibility, and fail-closed reason.
- [ ] Strategy plugin validate verification: `uv run bithumb-bot strategy-plugin-validate --strategy <name> --target <target> --json` checked target verdicts where applicable.
- [ ] Live-eligible strategies have runtime decision adapter discovery coverage where applicable.
- [ ] Common execution, risk, data, research, and runtime core paths remain strategy-neutral.
- [ ] Core-neutral confirmation: common execution, risk, data, research, and runtime core paths remain strategy-neutral.
- [ ] Architecture review marker: common execution, risk, data, research, or runtime core path changes include `architecture_review_required` or `architecture_review_complete`.
- [ ] No full default-fast research matrices were added.
