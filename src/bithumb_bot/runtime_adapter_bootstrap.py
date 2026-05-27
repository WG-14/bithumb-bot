from __future__ import annotations

from .runtime_adapters.safe_hold import SAFE_HOLD_STRATEGY_NAME, SafeHoldRuntimeDecisionAdapter
from .research.strategy_registry import list_research_strategy_plugins
from .runtime_strategy_decision import register_runtime_decision_adapter


_REGISTERED = False


def ensure_runtime_decision_adapters_registered() -> None:
    global _REGISTERED
    if _REGISTERED:
        return
    for plugin in list_research_strategy_plugins():
        if plugin.runtime_decision_adapter_factory is not None:
            register_runtime_decision_adapter(plugin.name, plugin.runtime_decision_adapter_factory)
    register_runtime_decision_adapter(SAFE_HOLD_STRATEGY_NAME, SafeHoldRuntimeDecisionAdapter)
    _REGISTERED = True
