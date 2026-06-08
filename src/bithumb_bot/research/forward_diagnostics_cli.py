from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bithumb_bot.config import PATH_MANAGER, settings
from bithumb_bot.research.experiment_manifest import ManifestValidationError, load_manifest
from bithumb_bot.research.forward_diagnostics import run_forward_diagnostics
from bithumb_bot.research.forward_diagnostics_report import write_forward_diagnostics_report


ALLOWED_SPLITS = frozenset({"train", "validation", "final_holdout"})


def cmd_research_forward_diagnostics(
    *,
    manifest_path: str,
    split_name: str = "train",
    features: tuple[str, ...],
    horizons: tuple[int, ...],
    bucket: str,
    entry_price: str = "next_open",
    min_bucket_count: int = 30,
    out_path: str | None = None,
    as_json: bool = False,
) -> int:
    try:
        split = _normalize_split(split_name)
        feature_names = _normalize_features(features)
        horizon_steps = _normalize_horizons(horizons)
        manifest = load_manifest(manifest_path)
        result_payload = run_forward_diagnostics(
            manifest=manifest,
            db_path=settings.DB_PATH,
            split_name=split,
            feature_names=feature_names,
            horizon_steps=horizon_steps,
            bucket_method=str(bucket),
            entry_price_mode=str(entry_price),
            min_bucket_count=int(min_bucket_count),
        )
        from bithumb_bot.research.forward_diagnostics import ForwardDiagnosticsResult
        result = ForwardDiagnosticsResult(
            experiment_id=str(result_payload["experiment_id"]),
            split_name=str(result_payload["split_name"]),
            feature_names=tuple(str(item) for item in result_payload["feature_names"]),
            horizon_steps=tuple(int(item) for item in result_payload["horizon_steps"]),
            bucket_method=str(result_payload["bucket_method"]),
            entry_price_mode=str(result_payload["entry_price_mode"]),
            sample_count=int(result_payload["sample_count"]),
            target_count=int(result_payload["target_count"]),
            feature_bucket_metrics=tuple(_metric_from_payload(item) for item in result_payload["feature_bucket_metrics"]),
            feature_horizon_metrics=tuple(_metric_from_payload(item) for item in result_payload["feature_horizon_metrics"]),
            warnings=tuple(dict(item) for item in result_payload["warnings"]),
        )
        report = write_forward_diagnostics_report(manager=PATH_MANAGER, manifest=manifest, result=result)
        if out_path:
            _write_explicit_json(Path(out_path), report)
        if as_json:
            print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
        else:
            print(
                "[RESEARCH-FORWARD-DIAGNOSTICS] "
                f"experiment_id={manifest.experiment_id} split={split} "
                f"features={','.join(feature_names)} horizons={','.join(str(item) for item in horizon_steps)} "
                f"report={report['artifact_paths']['report']}"
            )
        return 0
    except (ManifestValidationError, OSError, ValueError, IndexError) as exc:
        print(f"[RESEARCH-FORWARD-DIAGNOSTICS] error={exc}")
        return 1


def _metric_from_payload(payload: Any):
    from bithumb_bot.research.feature_bucket_metrics import FeatureBucketMetric

    data = dict(payload)
    return FeatureBucketMetric(
        feature_name=str(data["feature_name"]),
        bucket_id=str(data["bucket_id"]),
        bucket_label=str(data["bucket_label"]),
        horizon_label=str(data["horizon_label"]),
        count=int(data["count"]),
        mean_forward_return=_optional_float(data.get("mean_forward_return")),
        median_forward_return=_optional_float(data.get("median_forward_return")),
        win_rate=_optional_float(data.get("win_rate")),
        p10_forward_return=_optional_float(data.get("p10_forward_return")),
        p90_forward_return=_optional_float(data.get("p90_forward_return")),
        mean_mfe=_optional_float(data.get("mean_mfe")),
        median_mfe=_optional_float(data.get("median_mfe")),
        mean_mae=_optional_float(data.get("mean_mae")),
        median_mae=_optional_float(data.get("median_mae")),
        mfe_mae_ratio=_optional_float(data.get("mfe_mae_ratio")),
        warnings=tuple(str(item) for item in data.get("warnings", ())),
    )


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _normalize_split(split_name: str) -> str:
    split = str(split_name or "").strip()
    if split not in ALLOWED_SPLITS:
        allowed = ", ".join(sorted(ALLOWED_SPLITS))
        raise ValueError(f"unknown split={split_name!r}; allowed values: {allowed}")
    return split


def _normalize_features(features: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(str(feature).strip() for feature in features if str(feature).strip())
    if not normalized:
        raise ValueError("features must not be empty")
    return normalized


def _normalize_horizons(horizons: tuple[int, ...]) -> tuple[int, ...]:
    normalized = tuple(int(horizon) for horizon in horizons)
    if not normalized:
        raise ValueError("horizons must not be empty")
    if any(horizon <= 0 for horizon in normalized):
        raise ValueError("horizons must be positive")
    return normalized


def _write_explicit_json(path: Path, payload: dict[str, Any]) -> None:
    from bithumb_bot.storage_io import write_json_atomic

    resolved = path.expanduser()
    if not resolved.is_absolute():
        raise ValueError("--out must be an absolute path")
    resolved = resolved.resolve()
    try:
        resolved.relative_to(PATH_MANAGER.project_root.resolve())
    except ValueError:
        pass
    else:
        raise ValueError("--out must not point inside the repository")
    write_json_atomic(resolved, payload)
