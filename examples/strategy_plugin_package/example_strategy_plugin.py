from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bithumb_bot.research.strategy_spec import StrategyParameterSchema
from bithumb_bot.research.strategy_spec import StrategySpec
from bithumb_bot.research.hashing import sha256_prefixed
from bithumb_bot.strategy_authoring import ReplayCompatibleStrategyExtension
from bithumb_bot.strategy_authoring import build_replay_compatible_strategy_plugin
from bithumb_bot.strategy_authoring import research_plugin_from_decide_snapshot
from bithumb_bot.strategy_evidence import StrategyDecisionEvidenceBuilder


LEVEL_1_SPEC = StrategySpec(
    strategy_name="example_external_research_only",
    strategy_version="example_external_research_only.v1",
    accepted_parameter_names=("EXAMPLE_CLOSE_ABOVE",),
    required_parameter_names=("EXAMPLE_CLOSE_ABOVE",),
    behavior_affecting_parameter_names=("EXAMPLE_CLOSE_ABOVE",),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={},
    decision_contract_version="example_external_research_only.decision.v1",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={"schema_version": 1, "rules": ()},
)


def _decide_example_snapshot(
    *,
    candle: Any,
    candle_index: int,
    dataset: Any,
    parameter_values: dict[str, Any],
) -> dict[str, Any]:
    del dataset
    threshold = float(parameter_values["EXAMPLE_CLOSE_ABOVE"])
    close = float(candle.close)
    signal = "BUY" if close > threshold else "HOLD"
    return {
        "signal": signal,
        "reason": "example_close_above" if signal == "BUY" else "example_threshold_not_met",
        "feature_snapshot": {"candle_index": int(candle_index), "close": close},
    }


LEVEL_1_RESEARCH_ONLY_PLUGIN = research_plugin_from_decide_snapshot(
    strategy_name=LEVEL_1_SPEC.strategy_name,
    version=LEVEL_1_SPEC.strategy_version,
    spec=LEVEL_1_SPEC,
    required_data=LEVEL_1_SPEC.required_data,
    decide_snapshot=_decide_example_snapshot,
)


LEVEL_2_SPEC = StrategySpec(
    strategy_name="example_external_replay_compatible",
    strategy_version="example_external_replay_compatible.v1",
    accepted_parameter_names=("EXAMPLE_REPLAY_CLOSE_ABOVE",),
    required_parameter_names=("EXAMPLE_REPLAY_CLOSE_ABOVE",),
    behavior_affecting_parameter_names=("EXAMPLE_REPLAY_CLOSE_ABOVE",),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={},
    decision_contract_version="example_external_replay_compatible.decision.v1",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={"schema_version": 1, "rules": ()},
    parameter_schema=(
        StrategyParameterSchema(
            name="EXAMPLE_REPLAY_CLOSE_ABOVE",
            value_type="float",
            min_value=0.0,
            required=True,
            runtime_bound=True,
            behavior_affecting=True,
        ),
    ),
)


def _materialize_level_2(parameters: dict[str, Any]) -> dict[str, Any]:
    payload = {"EXAMPLE_REPLAY_CLOSE_ABOVE": float(parameters["EXAMPLE_REPLAY_CLOSE_ABOVE"])}
    LEVEL_2_SPEC.validate_parameters(payload)
    return payload


def _level_2_decision_material(*, market: str, interval: str, candle_ts: int, close: float, params: dict[str, Any]) -> dict[str, Any]:
    parameters = _materialize_level_2(params)
    signal = "BUY" if close > parameters["EXAMPLE_REPLAY_CLOSE_ABOVE"] else "HOLD"
    evidence = StrategyDecisionEvidenceBuilder().build(
        strategy_name=LEVEL_2_SPEC.strategy_name,
        policy_contract_material={"schema_version": 1, "strategy_name": LEVEL_2_SPEC.strategy_name},
        policy_input_material={
            "schema_version": 1,
            "market": market,
            "interval": interval,
            "candle_ts": int(candle_ts),
            "close": float(close),
            "parameters": parameters,
        },
        policy_decision_material={"schema_version": 1, "final_signal": signal},
        replay_fingerprint_material={"candle_ts": int(candle_ts), "read_only_replay": True},
        mode="runtime_replay",
    )
    return {"signal": signal, "evidence": evidence}


def _decide_level_2_snapshot(
    *,
    candle: Any,
    candle_index: int,
    dataset: Any,
    parameter_values: dict[str, Any],
) -> dict[str, Any]:
    material = _level_2_decision_material(
        market=str(dataset.market),
        interval=str(dataset.interval),
        candle_ts=int(candle.ts),
        close=float(candle.close),
        params=parameter_values,
    )
    evidence = material["evidence"]
    return {
        "signal": material["signal"],
        "reason": "example_replay_decision",
        "feature_snapshot": {"candle_index": int(candle_index), "close": float(candle.close)},
        "extra_payload": {
            "policy_contract_hash": evidence.policy_contract_hash,
            "policy_input_hash": evidence.policy_input_hash,
            "policy_decision_hash": evidence.policy_decision_hash,
            "replay_fingerprint_hash": evidence.replay_fingerprint_hash,
        },
    }


@dataclass(frozen=True)
class ExampleReplayStrategy:
    name: str = LEVEL_2_SPEC.strategy_name
    market: str = ""
    interval: str = ""
    parameters: dict[str, Any] | None = None

    def decide(self, conn: Any, *, through_ts_ms: int | None = None) -> Any | None:
        from bithumb_bot.strategy.base import StrategyDecision

        row = conn.execute(
            """
            SELECT ts, close FROM candles
            WHERE pair=? AND interval=? AND (? IS NULL OR ts<=?)
            ORDER BY ts DESC LIMIT 1
            """,
            (self.market, self.interval, through_ts_ms, through_ts_ms),
        ).fetchone()
        if row is None:
            return None
        candle_ts = int(row["ts"]) if hasattr(row, "keys") else int(row[0])
        close = float(row["close"]) if hasattr(row, "keys") else float(row[1])
        material = _level_2_decision_material(
            market=self.market,
            interval=self.interval,
            candle_ts=candle_ts,
            close=close,
            params=dict(self.parameters or {}),
        )
        evidence = material["evidence"]
        return StrategyDecision(
            signal=str(material["signal"]),
            reason="example_replay_decision",
            context={
                "strategy": self.name,
                "final_signal": str(material["signal"]),
                "final_reason": "example_replay_decision",
                "policy_contract_hash": evidence.policy_contract_hash,
                "policy_input_hash": evidence.policy_input_hash,
                "policy_decision_hash": evidence.policy_decision_hash,
                "pure_policy_hash": evidence.policy_hash,
                "replay_fingerprint": dict(evidence.replay_fingerprint),
                "replay_fingerprint_hash": evidence.replay_fingerprint_hash,
                "strategy_evaluation_provenance": dict(evidence.strategy_evaluation_provenance),
                "read_only_replay": True,
            },
        )


def _build_level_2_replay_strategy(
    profile: dict[str, Any],
    candidate_regime_policy: dict[str, Any] | None = None,
) -> ExampleReplayStrategy:
    del candidate_regime_policy
    params = profile.get("strategy_parameters") if isinstance(profile.get("strategy_parameters"), dict) else {}
    return ExampleReplayStrategy(
        market=str(profile.get("market") or ""),
        interval=str(profile.get("interval") or ""),
        parameters=_materialize_level_2(dict(params)),
    )


_LEVEL_2_RESEARCH_PLUGIN = research_plugin_from_decide_snapshot(
    strategy_name=LEVEL_2_SPEC.strategy_name,
    version=LEVEL_2_SPEC.strategy_version,
    spec=LEVEL_2_SPEC,
    required_data=LEVEL_2_SPEC.required_data,
    decide_snapshot=_decide_level_2_snapshot,
)


LEVEL_2_REPLAY_COMPATIBLE_PLUGIN = build_replay_compatible_strategy_plugin(
    research=_LEVEL_2_RESEARCH_PLUGIN,
    extension=ReplayCompatibleStrategyExtension(
        runtime_replay_builder=_build_level_2_replay_strategy,
        parameter_materializer=_materialize_level_2,
    ),
)


STRATEGY_OWNED_EXIT_SPEC = StrategySpec(
    strategy_name="example_strategy_owned_exit",
    strategy_version="example_strategy_owned_exit.v1",
    accepted_parameter_names=("EXAMPLE_TRAILING_STOP_RATIO",),
    required_parameter_names=("EXAMPLE_TRAILING_STOP_RATIO",),
    behavior_affecting_parameter_names=("EXAMPLE_TRAILING_STOP_RATIO",),
    metadata_only_parameter_names=(),
    research_only_parameter_names=(),
    default_parameters={},
    decision_contract_version="example_strategy_owned_exit.decision.v1",
    required_data=("candles",),
    optional_data=(),
    exit_policy_schema={
        "schema_version": 1,
        "rules": ("example_trailing_stop",),
        "example_trailing_stop": {"unit": "unrealized_pnl_ratio"},
    },
)


def example_exit_policy_materializer(strategy_name: str, parameters: dict[str, Any]) -> dict[str, object]:
    ratio = float(parameters["EXAMPLE_TRAILING_STOP_RATIO"])
    policy = {
        "schema_version": 1,
        "strategy_name": strategy_name,
        "rules": ["example_trailing_stop"],
        "common_rules": [],
        "strategy_rules": ["example_trailing_stop"],
        "example_trailing_stop": {"enabled": ratio > 0.0, "trailing_stop_ratio": ratio},
    }
    config = {
        "schema_version": 1,
        "strategy_name": strategy_name,
        "example_trailing_stop_ratio": ratio,
    }
    return {
        "exit_policy": policy,
        "exit_policy_hash": sha256_prefixed(policy),
        "exit_policy_contract_hash": sha256_prefixed(
            {
                "schema_version": 1,
                "strategy_name": strategy_name,
                "materializer": "example_exit_policy_materializer",
            }
        ),
        "exit_policy_config": config,
        "exit_policy_config_hash": sha256_prefixed(config),
        "exit_policy_source": "plugin_exit_policy_materializer",
        "exit_policy_materialization_mode": "profile_export",
    }
