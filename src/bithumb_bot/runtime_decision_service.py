from __future__ import annotations

from .runtime_adapter_bootstrap import ensure_runtime_decision_adapters_registered
from .runtime_strategy_decision import (
    ORIGINAL_COMPUTE_SIGNAL,
    DecisionRunner,
    RuntimeDecisionRequest,
    RuntimeDecisionAdapter,
    RuntimeStrategyDecisionResult,
    compute_signal,
    compute_signal_runtime_handoff,
    compute_strategy_decision_snapshot,
    get_runtime_decision_adapter,
    is_runtime_strategy_decision_result,
    legacy_db_strategy_fallback_allowed,
    list_runtime_decision_adapters,
    promotion_grade_typed_runtime_decision_required,
    register_runtime_decision_adapter,
    typed_runtime_handoff_failure_reason,
)
from .runtime_decision_contract import (
    RuntimeDecisionContext,
    RuntimeReplayFingerprint,
    RuntimeStrategyPolicyHashes,
)

ensure_runtime_decision_adapters_registered()

__all__ = [
    "ORIGINAL_COMPUTE_SIGNAL",
    "DecisionRunner",
    "RuntimeDecisionRequest",
    "RuntimeDecisionAdapter",
    "RuntimeStrategyDecisionResult",
    "RuntimeStrategyPolicyHashes",
    "RuntimeReplayFingerprint",
    "RuntimeDecisionContext",
    "compute_signal",
    "compute_signal_runtime_handoff",
    "compute_strategy_decision_snapshot",
    "get_runtime_decision_adapter",
    "is_runtime_strategy_decision_result",
    "legacy_db_strategy_fallback_allowed",
    "list_runtime_decision_adapters",
    "promotion_grade_typed_runtime_decision_required",
    "register_runtime_decision_adapter",
    "typed_runtime_handoff_failure_reason",
]
