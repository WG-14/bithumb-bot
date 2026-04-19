# Refactor target for bithumb-bot

## Goal
This repository must satisfy the refactor criteria in this document.
The purpose is to stop regressions in the live order path by replacing hidden contracts and mixed responsibilities with an explicit contract-based pipeline.

## Completion criteria
The work is complete only when all of the following are true:

1. Live order submission is driven by explicit objects such as OrderIntent and SubmitPlan.
2. live.py no longer injects or depends on hidden mutable broker state.
3. bithumb.py is no longer a single large file responsible for planning, signing, HTTP submission, and read-model parsing at once.
4. order_rules.py is split so that rule source fetching, rule policy, and BUY market policy are separated.
5. Payload building is driven from SubmitPlan rather than ad hoc side/order_type/price/volume assembly.
6. Submission phases are observable enough that failures can be distinguished by phase.
7. If the criteria are already satisfied, no further patch should be made.

## Priority order
1. Introduce explicit execution models.
2. Remove hidden contract coupling from live execution.
3. Split bithumb.py by responsibility.
4. Split order_rules.py by responsibility.
5. Improve persistence/observability for phase-based submission tracking.

## Required direction
- Prefer small safe patches.
- Preserve existing behavior unless required by this document.
- Avoid broad rewrites unless needed to satisfy the criteria.
- Stop patching and report completion if the criteria are met.

## Important repository-specific findings
- live.py is currently too large and mixes orchestration, validation, suppression, and execution preparation.
- order_rules.py currently mixes exchange rule retrieval, fallback merge, policy, and persistence.
- bithumb.py currently carries too many responsibilities at once.

## Expected target shape
- execution/models.py
- execution/planner.py
- execution/orchestrator.py
- broker/bithumb_client.py
- broker/bithumb_adapter.py
- broker/bithumb_execution.py
- broker/bithumb_read_models.py
- broker/rule_sources.py
- broker/rule_policy.py
- broker/buy_market_policy.py

## Stop condition
If the repository is judged to satisfy the criteria above, stop patching and report:
- why it is considered complete
- what files were checked
- any residual risks or follow-up items

If the repository already satisfies the target criteria, do not make cosmetic or unnecessary changes.