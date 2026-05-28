from __future__ import annotations

from .runtime_adapter_bootstrap import ensure_runtime_decision_adapters_registered
from .runtime_strategy_decision import (
    DecisionRunner,
    RuntimeDecisionRequest,
    RuntimeDecisionAdapter,
    RuntimeStrategyDecisionResult,
    compute_legacy_signal_for_diagnostics,
    compute_strategy_decision_for_diagnostics,
    compute_strategy_decision_snapshot,
    get_runtime_decision_adapter,
    is_runtime_strategy_decision_result,
    legacy_db_strategy_fallback_allowed,
    list_runtime_decision_adapters,
    promotion_grade_typed_runtime_decision_required,
    typed_runtime_handoff_failure_reason,
)
from .runtime_strategy_set import RuntimeDecisionGateway
from .runtime_decision_contract import (
    RuntimeDecisionContext,
    RuntimeReplayFingerprint,
    RuntimeStrategyPolicyHashes,
)

ensure_runtime_decision_adapters_registered()

__all__ = [
    "DecisionRunner",
    "RuntimeDecisionGateway",
    "RuntimeDecisionRequest",
    "RuntimeDecisionAdapter",
    "RuntimeStrategyDecisionResult",
    "RuntimeStrategyPolicyHashes",
    "RuntimeReplayFingerprint",
    "RuntimeDecisionContext",
    "compute_legacy_signal_for_diagnostics",
    "compute_strategy_decision_for_diagnostics",
    "compute_strategy_decision_snapshot",
    "get_runtime_decision_adapter",
    "is_runtime_strategy_decision_result",
    "legacy_db_strategy_fallback_allowed",
    "list_runtime_decision_adapters",
    "promotion_grade_typed_runtime_decision_required",
    "typed_runtime_handoff_failure_reason",
]
