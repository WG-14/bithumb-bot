from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path

from .markets import MarketInfo
from .notifier import AlertSeverity, format_event, notify
from .paths import PathManager
from .storage_io import append_jsonl, write_json_atomic

LOG = logging.getLogger(__name__)


def _snapshot_path(path_manager: PathManager) -> Path:
    return path_manager.derived_path("market_catalog_snapshot", ext="json")


def _diff_event_path(path_manager: PathManager) -> Path:
    return path_manager.report_path("market_catalog_diff", ext="jsonl")


def _normalize_market(item: MarketInfo) -> dict[str, str]:
    return {
        "market": item.market,
        "korean_name": str(item.korean_name or "").strip(),
        "english_name": str(item.english_name or "").strip(),
        "market_warning": str(item.market_warning or "").strip().upper(),
    }


def _build_snapshot(markets: list[MarketInfo]) -> dict[str, dict[str, str]]:
    normalized = [_normalize_market(item) for item in markets]
    normalized.sort(key=lambda row: row["market"])
    return {row["market"]: row for row in normalized}


def _load_previous_snapshot(path: Path) -> dict[str, dict[str, str]] | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    markets = raw.get("markets") if isinstance(raw, dict) else None
    if not isinstance(markets, dict):
        return None
    cleaned: dict[str, dict[str, str]] = {}
    for market, row in markets.items():
        if not isinstance(market, str) or not isinstance(row, dict):
            continue
        cleaned[market] = {
            "market": market,
            "korean_name": str(row.get("korean_name") or "").strip(),
            "english_name": str(row.get("english_name") or "").strip(),
            "market_warning": str(row.get("market_warning") or "").strip().upper(),
        }
    return cleaned


def _compute_diff(
    previous: dict[str, dict[str, str]] | None,
    current: dict[str, dict[str, str]],
) -> dict[str, object]:
    prev = previous or {}
    prev_markets = set(prev.keys())
    cur_markets = set(current.keys())

    added = sorted(cur_markets - prev_markets)
    removed = sorted(prev_markets - cur_markets)

    warning_changed: list[dict[str, str]] = []
    name_changed: list[dict[str, str]] = []

    for market in sorted(cur_markets & prev_markets):
        before = prev[market]
        after = current[market]
        if before.get("market_warning") != after.get("market_warning"):
            warning_changed.append(
                {
                    "market": market,
                    "before": str(before.get("market_warning") or ""),
                    "after": str(after.get("market_warning") or ""),
                }
            )
        if before.get("korean_name") != after.get("korean_name") or before.get("english_name") != after.get("english_name"):
            name_changed.append(
                {
                    "market": market,
                    "before_korean_name": str(before.get("korean_name") or ""),
                    "after_korean_name": str(after.get("korean_name") or ""),
                    "before_english_name": str(before.get("english_name") or ""),
                    "after_english_name": str(after.get("english_name") or ""),
                }
            )

    return {
        "added_markets": added,
        "removed_markets": removed,
        "warning_changed": warning_changed,
        "name_changed": name_changed,
        "changed": bool(added or removed or warning_changed or name_changed),
    }


def record_market_catalog_snapshot(
    *,
    path_manager: PathManager,
    mode: str,
    source: str,
    markets: list[MarketInfo],
) -> None:
    snapshot_path = _snapshot_path(path_manager)
    previous = _load_previous_snapshot(snapshot_path)
    current = _build_snapshot(markets)
    captured_at = datetime.now(timezone.utc).isoformat()
    diff = _compute_diff(previous, current)

    write_json_atomic(
        snapshot_path,
        {
            "captured_at": captured_at,
            "mode": mode,
            "source": source,
            "market_count": len(current),
            "markets": current,
        },
    )

    if previous is None or not bool(diff.get("changed")):
        return

    event = {
        "captured_at": captured_at,
        "mode": mode,
        "source": source,
        "market_count": len(current),
        "added_count": len(diff["added_markets"]),
        "removed_count": len(diff["removed_markets"]),
        "warning_changed_count": len(diff["warning_changed"]),
        "name_changed_count": len(diff["name_changed"]),
        "diff": diff,
    }
    append_jsonl(_diff_event_path(path_manager), event)

    LOG.warning(
        "market catalog changed source=%s mode=%s added=%s removed=%s warning_changed=%s name_changed=%s",
        source,
        mode,
        event["added_count"],
        event["removed_count"],
        event["warning_changed_count"],
        event["name_changed_count"],
    )

    notify(
        format_event(
            "market_catalog_changed",
            severity=AlertSeverity.WARN,
            mode=mode,
            source=source,
            market_count=len(current),
            added_count=event["added_count"],
            removed_count=event["removed_count"],
            warning_changed_count=event["warning_changed_count"],
            name_changed_count=event["name_changed_count"],
        ),
        severity=AlertSeverity.WARN,
    )
