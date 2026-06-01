from __future__ import annotations

import time

from ..broker.bithumb import BithumbBroker
from ..config import (
    validate_live_mode_preflight,
    validate_market_preflight,
    validate_market_runtime,
    validate_runtime_strategy_set_selection,
)
from ..db_core import ensure_db, record_strategy_decision
from ..execution_service import (
    build_execution_decision_summary,
    live_execute_signal,
    paper_execute,
    record_harmless_dust_exit_suppression,
)
from ..marketdata import cmd_sync
from ..notifier import notify
from ..risk import evaluate_daily_loss_breach, evaluate_position_loss_breach
from ..runtime_compat import *  # noqa: F403 - explicit legacy compatibility surface.
from ..runtime_decision_service import RuntimeDecisionGateway
from ..runtime_service_factories import run_loop_execution_planner
from ..runtime_strategy_set import normalized_runtime_strategy_set_manifest
from ..utils_time import parse_interval_sec
from ..flatten import flatten_btc_position
from ..runtime.runner import _get_exposure_snapshot


def compute_signal(*_args, **_kwargs):
    raise RuntimeError("legacy_compute_signal_unavailable")


def reconcile_with_broker(broker):
    from ..recovery import reconcile_with_broker as _reconcile_with_broker

    return _reconcile_with_broker(broker)

__all__ = [
    "BithumbBroker",
    "RuntimeDecisionGateway",
    "build_execution_decision_summary",
    "cmd_sync",
    "compute_signal",
    "ensure_db",
    "evaluate_daily_loss_breach",
    "evaluate_position_loss_breach",
    "flatten_btc_position",
    "live_execute_signal",
    "normalized_runtime_strategy_set_manifest",
    "notify",
    "paper_execute",
    "parse_interval_sec",
    "record_harmless_dust_exit_suppression",
    "record_strategy_decision",
    "reconcile_with_broker",
    "run_loop_execution_planner",
    "time",
    "_get_exposure_snapshot",
    "validate_live_mode_preflight",
    "validate_market_preflight",
    "validate_market_runtime",
    "validate_runtime_strategy_set_selection",
]
