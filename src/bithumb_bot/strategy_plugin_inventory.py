from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from bithumb_bot.research.strategy_registry import list_research_strategy_plugins


@dataclass(frozen=True)
class StrategyPluginSource:
    source: str
    manifest_object_path: str | None = None
    entry_point_name: str | None = None
    entry_point_value: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "source": self.source,
            "manifest_object_path": self.manifest_object_path,
            "entry_point_name": self.entry_point_name,
            "entry_point_value": self.entry_point_value,
        }


def build_strategy_plugin_inventory() -> dict[str, Any]:
    """Build a deterministic, read-only inventory of discovered strategy plugins."""

    plugins = list_research_strategy_plugins()
    source_by_name = _strategy_plugin_sources_by_name()
    entries: list[dict[str, Any]] = []
    for plugin in plugins:
        payload = plugin.contract_payload()
        source = source_by_name.get(plugin.name, StrategyPluginSource(source="unknown")).as_dict()
        live_eligibility = dict(payload["live_eligibility"])
        entries.append(
            {
                "name": plugin.name,
                "version": plugin.version,
                "source": source["source"],
                "manifest_object_path": source["manifest_object_path"],
                "entry_point_name": source["entry_point_name"],
                "entry_point_value": source["entry_point_value"],
                "authoring_contract_kind": payload["authoring_contract_kind"],
                "authoring_level": payload["authoring_level"],
                "capability_level": payload["capability_level"],
                "contract_hash": plugin.contract_hash(),
                "strategy_spec_hash": payload["strategy_spec_hash"],
                "runtime_capabilities": payload["runtime_capabilities"],
                "live_eligibility": live_eligibility,
                "fail_closed_reason": live_eligibility["fail_closed_reason"],
                "decision_evidence_contract": {
                    "contract_hash": payload["decision_evidence_contract"]["contract_hash"],
                },
                "required_data": list(plugin.required_data),
                "optional_data": list(plugin.optional_data),
            }
        )
    entries.sort(key=lambda item: str(item["name"]))
    return {
        "schema_version": 1,
        "strategy_count": len(entries),
        "strategies": entries,
    }


def strategy_plugin_inventory_json() -> str:
    return json.dumps(
        build_strategy_plugin_inventory(),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _strategy_plugin_sources_by_name() -> dict[str, StrategyPluginSource]:
    from bithumb_bot.strategy_plugins import coerce_loaded_strategy_plugins, metadata
    from bithumb_bot.strategy_plugins.builtin_manifest import iter_builtin_strategy_plugin_exports

    sources: dict[str, StrategyPluginSource] = {}
    for plugin_export in iter_builtin_strategy_plugin_exports():
        loaded = plugin_export.load()
        for plugin in coerce_loaded_strategy_plugins(loaded):
            sources.setdefault(
                plugin.name,
                StrategyPluginSource(
                    source="built_in_manifest",
                    manifest_object_path=plugin_export.object_path,
                ),
            )

    entry_points = metadata.entry_points()
    if hasattr(entry_points, "select"):
        selected = entry_points.select(group="bithumb_bot.strategy_plugins")
    elif isinstance(entry_points, dict):
        selected = entry_points.get("bithumb_bot.strategy_plugins", ())
    else:
        selected = [
            item
            for item in entry_points
            if str(getattr(item, "group", "bithumb_bot.strategy_plugins"))
            == "bithumb_bot.strategy_plugins"
        ]
    for entry_point in sorted(
        selected,
        key=lambda item: (
            str(getattr(item, "name", "")),
            str(getattr(item, "value", "")),
        ),
    ):
        for plugin in coerce_loaded_strategy_plugins(entry_point.load()):
            sources.setdefault(
                plugin.name,
                StrategyPluginSource(
                    source="entry_point",
                    entry_point_name=str(getattr(entry_point, "name", "")),
                    entry_point_value=str(getattr(entry_point, "value", "")),
                ),
            )
    return sources
