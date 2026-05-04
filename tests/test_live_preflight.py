from __future__ import annotations

import math
import os
from pathlib import Path

import pytest

from bithumb_bot.approved_profile import build_approved_profile
from bithumb_bot import config
from bithumb_bot.config import settings
from bithumb_bot.research.hashing import content_hash_payload, sha256_prefixed
from bithumb_bot.research.promotion_gate import build_candidate_profile
from bithumb_bot.storage_io import write_json_atomic
from bithumb_bot.broker import order_rules
from bithumb_bot.markets import MarketInfo, MarketRegistry

_REAL_GET_EFFECTIVE_ORDER_RULES = order_rules.get_effective_order_rules


@pytest.fixture(autouse=True)
def _restore_settings():
    old_values = {
        "MODE": settings.MODE,
        "DB_PATH": settings.DB_PATH,
        "MAX_ORDER_KRW": settings.MAX_ORDER_KRW,
        "MAX_DAILY_LOSS_KRW": settings.MAX_DAILY_LOSS_KRW,
        "MAX_DAILY_ORDER_COUNT": settings.MAX_DAILY_ORDER_COUNT,
        "LIVE_DRY_RUN": settings.LIVE_DRY_RUN,
        "LIVE_REAL_ORDER_ARMED": settings.LIVE_REAL_ORDER_ARMED,
        "LIVE_ALLOW_ORDER_RULE_FALLBACK": settings.LIVE_ALLOW_ORDER_RULE_FALLBACK,
        "LIVE_ORDER_RULE_FALLBACK_PROFILE": settings.LIVE_ORDER_RULE_FALLBACK_PROFILE,
        "LIVE_SUBMIT_CONTRACT_PROFILE": settings.LIVE_SUBMIT_CONTRACT_PROFILE,
        "KILL_SWITCH_LIQUIDATE": settings.KILL_SWITCH_LIQUIDATE,
        "BITHUMB_API_KEY": settings.BITHUMB_API_KEY,
        "BITHUMB_API_SECRET": settings.BITHUMB_API_SECRET,
        "LIVE_MIN_ORDER_QTY": settings.LIVE_MIN_ORDER_QTY,
        "LIVE_ORDER_QTY_STEP": settings.LIVE_ORDER_QTY_STEP,
        "MIN_ORDER_NOTIONAL_KRW": settings.MIN_ORDER_NOTIONAL_KRW,
        "LIVE_ORDER_MAX_QTY_DECIMALS": settings.LIVE_ORDER_MAX_QTY_DECIMALS,
        "MAX_ORDERBOOK_SPREAD_BPS": settings.MAX_ORDERBOOK_SPREAD_BPS,
        "MAX_MARKET_SLIPPAGE_BPS": settings.MAX_MARKET_SLIPPAGE_BPS,
        "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS": settings.LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS,
        "LIVE_FILL_FEE_STRICT_MODE": settings.LIVE_FILL_FEE_STRICT_MODE,
        "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW": settings.LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW,
        "PAIR": settings.PAIR,
        "MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR": settings.MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR,
        "MARKET_PREFLIGHT_BLOCK_ON_WARNING": settings.MARKET_PREFLIGHT_BLOCK_ON_WARNING,
        "MARKET_PREFLIGHT_WARNING_STATES": settings.MARKET_PREFLIGHT_WARNING_STATES,
        "MARKET_REGISTRY_CACHE_TTL_SEC": settings.MARKET_REGISTRY_CACHE_TTL_SEC,
        "MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH": settings.MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH,
        "APPROVED_STRATEGY_PROFILE_PATH": settings.APPROVED_STRATEGY_PROFILE_PATH,
        "STRATEGY_NAME": settings.STRATEGY_NAME,
        "SMA_SHORT": settings.SMA_SHORT,
        "SMA_LONG": settings.SMA_LONG,
        "SMA_FILTER_GAP_MIN_RATIO": settings.SMA_FILTER_GAP_MIN_RATIO,
        "SMA_FILTER_VOL_WINDOW": settings.SMA_FILTER_VOL_WINDOW,
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": settings.SMA_FILTER_VOL_MIN_RANGE_RATIO,
        "SMA_FILTER_OVEREXT_LOOKBACK": settings.SMA_FILTER_OVEREXT_LOOKBACK,
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO,
        "SMA_COST_EDGE_ENABLED": settings.SMA_COST_EDGE_ENABLED,
        "SMA_COST_EDGE_MIN_RATIO": settings.SMA_COST_EDGE_MIN_RATIO,
        "ENTRY_EDGE_BUFFER_RATIO": settings.ENTRY_EDGE_BUFFER_RATIO,
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO,
        "STRATEGY_EXIT_RULES": settings.STRATEGY_EXIT_RULES,
        "STRATEGY_EXIT_MAX_HOLDING_MIN": settings.STRATEGY_EXIT_MAX_HOLDING_MIN,
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO,
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO,
        "LIVE_FEE_RATE_ESTIMATE": settings.LIVE_FEE_RATE_ESTIMATE,
        "STRATEGY_ENTRY_SLIPPAGE_BPS": settings.STRATEGY_ENTRY_SLIPPAGE_BPS,
    }
    old_cache = dict(order_rules._cached_rules)
    yield
    for key, value in old_values.items():
        object.__setattr__(settings, key, value)
    order_rules._cached_rules.clear()
    order_rules._cached_rules.update(old_cache)




@pytest.fixture(autouse=True)
def _stub_accounts_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [
            {"currency": "KRW", "balance": "1000000", "locked": "0"},
            {"currency": "BTC", "balance": "0.1", "locked": "0"},
        ],
    )

@pytest.fixture(autouse=True)
def _set_live_roots_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    roots = {
        "ENV_ROOT": tmp_path / "env",
        "RUN_ROOT": tmp_path / "run",
        "DATA_ROOT": tmp_path / "data",
        "LOG_ROOT": tmp_path / "logs",
        "BACKUP_ROOT": tmp_path / "backup",
    }
    for key, value in roots.items():
        monkeypatch.setenv(key, str(value.resolve()))


def _set_valid_live_defaults(
    monkeypatch: pytest.MonkeyPatch,
    *,
    db_path: str | None = None,
    stub_order_rules: bool = True,
) -> None:
    data_root = Path(os.environ["DATA_ROOT"])
    run_root = Path(os.environ["RUN_ROOT"])
    resolved_db_path = str(
        Path(db_path).resolve() if db_path is not None else (data_root / "live" / "trades" / "live.sqlite").resolve()
    )
    monkeypatch.setenv("DB_PATH", resolved_db_path)
    monkeypatch.setenv("RUN_LOCK_PATH", str((run_root / "live" / "bithumb-bot.lock").resolve()))
    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/ok")
    monkeypatch.setenv("BITHUMB_API_KEY", "key")
    monkeypatch.setenv("BITHUMB_API_SECRET", "secret")
    monkeypatch.delenv("START_CASH_KRW", raising=False)
    monkeypatch.delenv("BUY_FRACTION", raising=False)
    monkeypatch.delenv("FEE_RATE", raising=False)
    monkeypatch.delenv("SLIPPAGE_BPS", raising=False)

    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "DB_PATH", resolved_db_path)
    object.__setattr__(settings, "MAX_ORDER_KRW", 100000.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 50000.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 10)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "LIVE_ALLOW_ORDER_RULE_FALLBACK", False)
    object.__setattr__(
        settings,
        "LIVE_ORDER_RULE_FALLBACK_PROFILE",
        config.LIVE_ORDER_RULE_FALLBACK_PROFILE_PERSISTED_SNAPSHOT_REQUIRED,
    )
    object.__setattr__(settings, "LIVE_SUBMIT_CONTRACT_PROFILE", config.LIVE_SUBMIT_CONTRACT_PROFILE_V1)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0001)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0001)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 5000.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 4)
    object.__setattr__(settings, "MAX_ORDERBOOK_SPREAD_BPS", 100.0)
    object.__setattr__(settings, "MAX_MARKET_SLIPPAGE_BPS", 50.0)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 25.0)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 100000.0)
    object.__setattr__(settings, "PAIR", "KRW-BTC")
    object.__setattr__(settings, "STRATEGY_NAME", "sma_with_filter")
    object.__setattr__(settings, "MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR", False)
    object.__setattr__(settings, "MARKET_PREFLIGHT_BLOCK_ON_WARNING", False)
    object.__setattr__(settings, "MARKET_PREFLIGHT_WARNING_STATES", "CAUTION")
    monkeypatch.delenv("MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR", raising=False)
    monkeypatch.delenv("MARKET_PREFLIGHT_BLOCK_ON_WARNING", raising=False)
    monkeypatch.delenv("MARKET_PREFLIGHT_WARNING_STATES", raising=False)
    monkeypatch.delenv("MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH", raising=False)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
    )
    object.__setattr__(settings, "MARKET_REGISTRY_CACHE_TTL_SEC", 900.0)
    object.__setattr__(settings, "MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH", False)
    if stub_order_rules:
        monkeypatch.setattr(
            order_rules,
            "get_effective_order_rules",
            lambda _pair: order_rules.RuleResolution(
                rules=order_rules.OrderRules(
                    market_id="KRW-BTC",
                    bid_min_total_krw=5000.0,
                    ask_min_total_krw=5000.0,
                    bid_price_unit=1.0,
                    ask_price_unit=1.0,
                    min_qty=float(settings.LIVE_MIN_ORDER_QTY),
                    qty_step=float(settings.LIVE_ORDER_QTY_STEP),
                    min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
                    max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
                ),
                source={
                    "min_qty": "local_fallback",
                    "qty_step": "local_fallback",
                    "min_notional_krw": "local_fallback",
                    "max_qty_decimals": "local_fallback",
                    "bid_min_total_krw": "chance_doc",
                    "ask_min_total_krw": "chance_doc",
                    "bid_price_unit": "chance_doc",
                    "ask_price_unit": "chance_doc",
                },
            ),
        )
    profile_path = _write_live_profile(Path(os.environ["DATA_ROOT"]).parent, mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))


def _candidate_profile_for_current_settings() -> dict[str, object]:
    return {
        "SMA_SHORT": int(settings.SMA_SHORT),
        "SMA_LONG": int(settings.SMA_LONG),
        "SMA_FILTER_GAP_MIN_RATIO": float(settings.SMA_FILTER_GAP_MIN_RATIO),
        "SMA_FILTER_VOL_WINDOW": int(settings.SMA_FILTER_VOL_WINDOW),
        "SMA_FILTER_VOL_MIN_RANGE_RATIO": float(settings.SMA_FILTER_VOL_MIN_RANGE_RATIO),
        "SMA_FILTER_OVEREXT_LOOKBACK": int(settings.SMA_FILTER_OVEREXT_LOOKBACK),
        "SMA_FILTER_OVEREXT_MAX_RETURN_RATIO": float(settings.SMA_FILTER_OVEREXT_MAX_RETURN_RATIO),
        "SMA_COST_EDGE_ENABLED": bool(settings.SMA_COST_EDGE_ENABLED),
        "SMA_COST_EDGE_MIN_RATIO": float(settings.SMA_COST_EDGE_MIN_RATIO),
        "ENTRY_EDGE_BUFFER_RATIO": float(settings.ENTRY_EDGE_BUFFER_RATIO),
        "STRATEGY_MIN_EXPECTED_EDGE_RATIO": float(settings.STRATEGY_MIN_EXPECTED_EDGE_RATIO),
        "STRATEGY_EXIT_RULES": str(settings.STRATEGY_EXIT_RULES),
        "STRATEGY_EXIT_MAX_HOLDING_MIN": int(settings.STRATEGY_EXIT_MAX_HOLDING_MIN),
        "STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO": float(settings.STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO),
        "STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO": float(settings.STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO),
    }


def _write_live_profile(tmp_path: Path, *, mode: str = "small_live", sma_short: int | None = None) -> Path:
    parameters = _candidate_profile_for_current_settings()
    if sma_short is not None:
        parameters["SMA_SHORT"] = int(sma_short)
    candidate = {
        "experiment_id": "live-exp",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "strategy_name": "sma_with_filter",
        "parameter_candidate_id": "candidate_001",
        "parameter_values": parameters,
        "cost_model": {
            "fee_rate": float(settings.LIVE_FEE_RATE_ESTIMATE),
            "slippage_bps": float(settings.STRATEGY_ENTRY_SLIPPAGE_BPS),
        },
        "regime_classifier_version": "market_regime_v2",
        "allowed_live_regimes": ["uptrend_normal_vol_unknown"],
        "blocked_live_regimes": ["downtrend_normal_vol_unknown"],
    }
    candidate_hash = sha256_prefixed(build_candidate_profile(candidate))
    promotion = {
        "strategy_name": "sma_with_filter",
        "strategy_profile_source_experiment": "live-exp",
        "candidate_id": "candidate_001",
        "manifest_hash": "sha256:manifest",
        "dataset_snapshot_id": "snap",
        "dataset_content_hash": "sha256:dataset",
        "market": "KRW-BTC",
        "interval": str(settings.INTERVAL),
        "repository_version": "test",
        "candidate_profile": build_candidate_profile(candidate),
        "candidate_profile_hash": candidate_hash,
        "verified_candidate_profile_hash": candidate_hash,
        "live_regime_policy": {
            "regime_classifier_version": "market_regime_v2",
            "allowed_regimes": ["uptrend_normal_vol_unknown"],
            "blocked_regimes": ["downtrend_normal_vol_unknown"],
            "missing_policy_behavior": "fail_closed",
        },
        "generated_at": "2026-05-04T00:00:00+00:00",
    }
    promotion["content_hash"] = sha256_prefixed(content_hash_payload(promotion))
    promotion_path = tmp_path / "promotion.json"
    write_json_atomic(promotion_path, promotion)
    profile = build_approved_profile(
        promotion=promotion,
        mode=mode,
        source_promotion_path=str(promotion_path),
        market="KRW-BTC",
        interval=str(settings.INTERVAL),
        generated_at="2026-05-04T00:00:00+00:00",
    )
    path = tmp_path / f"{mode}_profile.json"
    write_json_atomic(path, profile)
    return path


def _select_small_live_profile(tmp_path: Path) -> None:
    profile_path = _write_live_profile(tmp_path, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))



def test_live_preflight_skips_paper_mode() -> None:
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    config.validate_live_mode_preflight(settings)
    config.validate_live_real_order_execution_preflight(settings)


def test_live_real_order_execution_preflight_rejects_live_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_real_order_execution_preflight(settings)

    msg = str(exc.value)
    assert "LIVE_DRY_RUN=false is required for MODE=live run" in msg
    assert "LIVE_REAL_ORDER_ARMED=true is required for MODE=live run" in msg


def test_live_real_order_execution_preflight_accepts_armed_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)

    config.validate_live_real_order_execution_preflight(settings)


def test_live_armed_preflight_requires_approved_small_live_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "approved_profile_missing" in str(exc.value)


def test_live_armed_preflight_rejects_profile_env_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    profile_path = _write_live_profile(tmp_path, mode="small_live", sma_short=int(settings.SMA_SHORT) + 1)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "approved_profile_runtime_mismatch" in str(exc.value)


def test_live_armed_preflight_rejects_paper_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    profile_path = _write_live_profile(tmp_path, mode="paper")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "profile_mode_mismatch" in str(exc.value)


def test_live_dry_run_startup_requires_approved_live_dry_run_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "approved_profile_missing" in str(exc.value)


def test_live_dry_run_startup_rejects_small_live_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    profile_path = _write_live_profile(tmp_path, mode="small_live")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_dry_run_loop_startup_contract(settings)

    assert "profile_mode_mismatch" in str(exc.value)


def test_live_dry_run_startup_accepts_live_dry_run_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    profile_path = _write_live_profile(tmp_path, mode="live_dry_run")
    object.__setattr__(settings, "APPROVED_STRATEGY_PROFILE_PATH", str(profile_path))

    config.validate_live_dry_run_loop_startup_contract(settings)


def test_live_preflight_rejects_dry_run_when_real_order_armed(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_DRY_RUN=true and LIVE_REAL_ORDER_ARMED=true is ambiguous" in str(exc.value)


def test_live_execution_contract_log_emits_redacted_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "visible-key-length")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret-value-must-not-log")

    with caplog.at_level("INFO", logger="bithumb_bot.run"):
        summary = config.log_live_execution_contract(settings, caller="test")

    assert summary["live_dry_run"] is False
    assert "[LIVE_EXECUTION_CONTRACT]" in caplog.text
    assert "fingerprint=" in caplog.text
    assert "live_dry_run=0" in caplog.text
    assert "live_real_order_armed=1" in caplog.text
    assert "api_secret_present=1" in caplog.text
    assert "secret-value-must-not-log" not in caplog.text


def test_live_preflight_requires_live_risk_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "MAX_ORDER_KRW must be > 0" in msg
    assert "MAX_DAILY_LOSS_KRW must be > 0" in msg
    assert "MAX_DAILY_ORDER_COUNT must be > 0" in msg


def test_live_preflight_requires_chance_doc_side_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    monkeypatch.setattr(
        order_rules,
        "get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                bid_min_total_krw=0.0,
                ask_min_total_krw=0.0,
                bid_price_unit=0.0,
                ask_price_unit=0.0,
                min_qty=float(settings.LIVE_MIN_ORDER_QTY),
                qty_step=float(settings.LIVE_ORDER_QTY_STEP),
                min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
                max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
            ),
            source={
                "min_qty": "local_fallback",
                "qty_step": "local_fallback",
                "min_notional_krw": "local_fallback",
                "max_qty_decimals": "local_fallback",
                "bid_min_total_krw": "unsupported_by_doc",
                "ask_min_total_krw": "missing",
                "bid_price_unit": "unsupported_by_doc",
                "ask_price_unit": "missing",
            },
        ),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "bid_min_total_krw source must be chance_doc for MODE=live" in msg
    assert "ask_min_total_krw source must be chance_doc for MODE=live" in msg


def test_live_preflight_requires_credentials_in_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "BITHUMB_API_KEY is required when MODE=live" in msg
    assert "BITHUMB_API_SECRET is required when MODE=live" in msg


def test_live_preflight_requires_explicit_arming_for_real_live_orders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_REAL_ORDER_ARMED=true is required" in str(exc.value)


def test_live_preflight_accepts_real_live_orders_when_explicitly_armed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    _select_small_live_profile(tmp_path)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_rejects_local_fallback_profile_in_armed_live_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(
        settings,
        "LIVE_ORDER_RULE_FALLBACK_PROFILE",
        config.LIVE_ORDER_RULE_FALLBACK_PROFILE_ALLOW_LOCAL_FALLBACK,
    )
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_ORDER_RULE_FALLBACK_PROFILE must be 'persisted_snapshot_required'" in str(exc.value)


def test_live_preflight_allows_local_fallback_profile_in_live_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(
        settings,
        "LIVE_ORDER_RULE_FALLBACK_PROFILE",
        config.LIVE_ORDER_RULE_FALLBACK_PROFILE_ALLOW_LOCAL_FALLBACK,
    )

    config.validate_live_mode_preflight(settings)


def test_live_preflight_rejects_invalid_order_rule_fallback_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_ORDER_RULE_FALLBACK_PROFILE", "legacy_bool_combo")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_ORDER_RULE_FALLBACK_PROFILE must be" in str(exc.value)


def test_live_preflight_rejects_invalid_submit_contract_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_SUBMIT_CONTRACT_PROFILE", "legacy_bool_combo")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_SUBMIT_CONTRACT_PROFILE must be" in str(exc.value)


def test_live_preflight_accepts_expected_submit_contract_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_SUBMIT_CONTRACT_PROFILE", config.LIVE_SUBMIT_CONTRACT_PROFILE_V1)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_requires_notifier_delivery_target(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.delenv("NOTIFIER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "notifier must be enabled and configured with at least one delivery target" in str(exc.value)


def test_live_preflight_rejects_shared_runtime_roots(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    shared_root = Path(os.environ["DATA_ROOT"]).resolve()
    monkeypatch.setenv("BACKUP_ROOT", str(shared_root))
    object.__setattr__(settings, "BACKUP_ROOT", str(shared_root))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "runtime roots must not overlap or share parent/child paths when MODE=live" in str(exc.value)


def test_live_preflight_accepts_valid_live_configuration(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    _select_small_live_profile(tmp_path)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_requires_meaningful_live_price_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS", 0.0)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS must be a finite value > 0" in str(exc.value)


def test_live_preflight_accepts_meaningful_live_price_protection(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_allows_kill_switch_liquidate_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "KILL_SWITCH_LIQUIDATE", True)

    config.validate_live_mode_preflight(settings)

def test_live_preflight_requires_credentials_even_for_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    object.__setattr__(settings, "BITHUMB_API_KEY", "")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "BITHUMB_API_KEY is required when MODE=live" in msg
    assert "BITHUMB_API_SECRET is required when MODE=live" in msg


def test_live_preflight_requires_explicit_db_path_for_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.delenv("DB_PATH", raising=False)
    object.__setattr__(settings, "DB_PATH", str((Path(os.environ["DATA_ROOT"]) / "live" / "trades" / "live.sqlite").resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DB_PATH must be explicitly set when MODE=live" in str(exc.value)


def test_live_preflight_rejects_paper_scoped_db_path_for_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    paper_db = str((Path(os.environ["DATA_ROOT"]) / "paper" / "trades" / "paper.sqlite").resolve())
    _set_valid_live_defaults(monkeypatch, db_path=paper_db)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DB_PATH must not point to a paper-scoped path when MODE=live" in str(exc.value)


@pytest.mark.parametrize(("env_key", "env_value"), [("LOG_ROOT", "logs"), ("BACKUP_ROOT", "backup")])
def test_live_preflight_rejects_relative_log_and_backup_roots(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    env_value: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(env_key, env_value)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert f"{env_key} must be an absolute path when MODE=live" in str(exc.value)


@pytest.mark.parametrize(("env_key", "child"), [("DATA_ROOT", "data"), ("LOG_ROOT", "logs"), ("BACKUP_ROOT", "backup")])
def test_live_preflight_rejects_repo_internal_roots(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    child: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(env_key, str((config.PROJECT_ROOT / child).resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert f"{env_key} must be outside repository when MODE=live" in str(exc.value)


def test_live_preflight_rejects_paper_scoped_root_segments(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("DATA_ROOT", str((Path(os.environ["DATA_ROOT"]).parent / "paper" / "data").resolve()))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "DATA_ROOT must not contain a paper-scoped path segment when MODE=live" in str(exc.value)


@pytest.mark.parametrize("env_key", ["LOG_ROOT", "BACKUP_ROOT"])
def test_live_preflight_rejects_paper_scoped_log_and_backup_segments(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(
        env_key,
        str((Path(os.environ["DATA_ROOT"]).parent / "paper" / env_key.lower()).resolve()),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert f"{env_key} must not contain a paper-scoped path segment when MODE=live" in str(exc.value)


def test_live_preflight_accepts_non_default_live_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    custom_live_db = str((Path(os.environ["DATA_ROOT"]) / "live" / "trades" / "live-prod.sqlite").resolve())
    _set_valid_live_defaults(monkeypatch, db_path=custom_live_db)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_accepts_explicit_non_default_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    custom_live_db = str((Path(os.environ["DATA_ROOT"]) / "live" / "trades" / "live_trading.sqlite").resolve())
    _set_valid_live_defaults(monkeypatch, db_path=custom_live_db)

    config.validate_live_mode_preflight(settings)

@pytest.mark.parametrize(
    ("env_key", "env_value"),
    [
        ("START_CASH_KRW", "1000000"),
        ("BUY_FRACTION", "0.5"),
        ("FEE_RATE", "0.0004"),
        ("SLIPPAGE_BPS", "5"),
    ],
)
def test_live_preflight_rejects_paper_only_env_keys_in_live(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    env_value: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv(env_key, env_value)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "paper/test-like config mixing is not allowed when MODE=live" in msg
    assert env_key in msg


def test_live_preflight_accepts_clean_live_env_without_paper_only_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.delenv("START_CASH_KRW", raising=False)
    monkeypatch.delenv("BUY_FRACTION", raising=False)
    monkeypatch.delenv("FEE_RATE", raising=False)
    monkeypatch.delenv("SLIPPAGE_BPS", raising=False)

    config.validate_live_mode_preflight(settings)

def test_live_preflight_requires_notifier_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("NOTIFIER_ENABLED", "false")
    monkeypatch.delenv("NOTIFIER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "notifier must be enabled and configured" in str(exc.value)


def test_live_preflight_accepts_notifier_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("NOTIFIER_ENABLED", "true")
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/abc")

    config.validate_live_mode_preflight(settings)


def test_live_preflight_paper_mode_notifier_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTIFIER_ENABLED", "false")
    monkeypatch.delenv("NOTIFIER_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "MAX_ORDER_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_LOSS_KRW", 0.0)
    object.__setattr__(settings, "MAX_DAILY_ORDER_COUNT", 0)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_fails_when_order_rule_sync_fails_and_manual_rules_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)
    order_rules._cached_rules.clear()

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "min_qty must be > 0" in msg
    assert "qty_step must be > 0" in msg
    assert "min_notional_krw must be > 0" in msg
    assert "max_qty_decimals must be > 0" in msg


def test_live_preflight_surfaces_document_schema_violation_when_manual_rules_are_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch, stub_order_rules=False)
    monkeypatch.setattr(order_rules, "get_effective_order_rules", _REAL_GET_EFFECTIVE_ORDER_RULES)
    object.__setattr__(settings, "LIVE_MIN_ORDER_QTY", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_QTY_STEP", 0.0)
    object.__setattr__(settings, "MIN_ORDER_NOTIONAL_KRW", 0.0)
    object.__setattr__(settings, "LIVE_ORDER_MAX_QTY_DECIMALS", 0)
    order_rules._cached_rules.clear()

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: (_ for _ in ()).throw(order_rules.OrderChanceSchemaError("/v1/orders/chance response.market.bid.min_total must be numeric")),
    )
    warnings: list[str] = []
    monkeypatch.setattr(order_rules, "notify", lambda msg: warnings.append(msg))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "min_qty must be > 0" in msg
    assert warnings
    assert "OrderChanceSchemaError" in warnings[0]
    assert "response.market.bid.min_total must be numeric" in warnings[0]


def test_live_preflight_passes_with_valid_auto_synced_order_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    order_rules._cached_rules.clear()

    monkeypatch.setattr(
        order_rules,
        "fetch_exchange_order_rules",
        lambda _pair: order_rules.OrderRules(
            min_qty=0.0001,
            qty_step=0.0001,
            min_notional_krw=5000.0,
            max_qty_decimals=4,
        ),
    )

    config.validate_live_mode_preflight(settings)


def test_live_preflight_allows_non_positive_strict_threshold_when_strict_mode_is_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", False)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 0.0)

    config.validate_live_mode_preflight(settings)


def test_live_preflight_accepts_positive_strict_threshold_when_strict_mode_is_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", 10000.0)

    config.validate_live_mode_preflight(settings)


@pytest.mark.parametrize(
    "invalid_threshold",
    [0.0, -1.0, math.nan, math.inf, -math.inf, "abc"],
)
def test_live_preflight_rejects_invalid_strict_threshold_when_strict_mode_is_on(
    monkeypatch: pytest.MonkeyPatch,
    invalid_threshold: object,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MODE", True)
    object.__setattr__(settings, "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW", invalid_threshold)

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW" in msg
    assert "LIVE_FILL_FEE_STRICT_MODE=true" in msg


def test_market_preflight_rejects_unsupported_market(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "PAIR", "KRW-ABC")
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "unsupported pair" in str(exc.value)


def test_market_preflight_rejects_legacy_alias_in_live_mode_even_when_catalog_supports_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "PAIR", "btc_krw")
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "invalid configured market format" in str(exc.value)
    assert "MODE=live" in str(exc.value)


def test_market_preflight_blocks_warning_state_in_live_real_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="CAUTION")]),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "market_warning=CAUTION" in str(exc.value)


def test_market_preflight_allows_warning_state_in_dry_run_live_mode(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="CAUTION")]),
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)

    assert "market preflight detected warning state" in caplog.text


def test_market_preflight_blocks_on_catalog_fetch_failure_in_live_real_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(config.MarketCatalogError("catalog down")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "catalog fetch failed" in msg
    assert "endpoint=/v1/market/all" in msg
    assert "isDetails=true" in msg
    assert "mode=live" in msg
    assert "block_on_catalog_error=True" in msg


def test_market_preflight_allows_catalog_fetch_failure_in_dry_run_with_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(config.MarketCatalogError("catalog down")),
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)

    assert "catalog fetch failed" in caplog.text
    assert "endpoint=/v1/market/all" in caplog.text
    assert "isDetails=true" in caplog.text
    assert "mode=live" in caplog.text
    assert "dry_run=True" in caplog.text
    assert "block_on_catalog_error=False" in caplog.text
    assert "schema_drift=True" in caplog.text


def test_market_preflight_warns_and_allows_warning_state_in_paper_mode(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MODE", "paper")
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="CAUTION")]),
    )

    with caplog.at_level("WARNING"):
        config.validate_market_preflight(settings)

    assert "market preflight detected warning state" in caplog.text


def test_market_preflight_warns_and_allows_warning_state_in_dryrun_mode(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MODE", "dryrun")
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="CAUTION")]),
    )

    with caplog.at_level("WARNING"):
        config.validate_market_preflight(settings)

    assert "market preflight detected warning state" in caplog.text


def test_market_preflight_treats_unexpected_market_warning_as_unknown_and_blocks_live_real(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="SUSPICIOUS_NEW_STATE")]),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "market_warning=UNKNOWN" in str(exc.value)


def test_market_runtime_validation_forces_registry_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    calls: list[tuple[bool, float, bool]] = []
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **kwargs: (
            calls.append((bool(kwargs["refresh"]), float(kwargs["ttl_seconds"]), bool(kwargs["is_details"]))),
            MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
        )[1],
    )

    config.validate_market_runtime(settings)

    assert calls == [(True, float(settings.MARKET_REGISTRY_CACHE_TTL_SEC), True)]


def test_live_preflight_blocks_startup_on_accounts_schema_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    monkeypatch.setattr(config, "_fetch_accounts_payload_for_preflight", lambda **_kwargs: {"currency": "KRW"})

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "/v1/accounts REST snapshot preflight validation failed" in msg
    assert "reason=schema mismatch" in msg
    assert "reason_code=ACCOUNTS_SCHEMA_MISMATCH" in msg


def test_live_preflight_blocks_startup_on_required_currency_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )
    monkeypatch.setattr(config, "_flat_start_safety_for_accounts_preflight", lambda: (False, "not_flat_start"))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "/v1/accounts REST snapshot preflight validation failed" in msg
    assert "reason=required currency missing" in msg
    assert "reason_code=ACCOUNTS_REQUIRED_CURRENCY_MISSING" in msg


@pytest.mark.parametrize("mode", ["paper", "dryrun"])
def test_accounts_preflight_diagnostics_are_warning_only_in_non_live_modes(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    mode: str,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MODE", mode)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "BTC", "balance": "0.2", "locked": "0"}],
    )

    with caplog.at_level("WARNING"):
        config.validate_market_preflight(settings)

    assert f"accounts REST snapshot preflight warning (mode={mode})" in caplog.text
    assert "reason=required currency missing" in caplog.text
    assert "reason_code=ACCOUNTS_REQUIRED_CURRENCY_MISSING" in caplog.text


def test_live_preflight_runs_market_and_accounts_contracts_together(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    _select_small_live_profile(tmp_path)
    calls: list[str] = []

    def _market_registry(**_kwargs):
        calls.append("market")
        return MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")])

    def _accounts_payload(**_kwargs):
        calls.append("accounts")
        return [
            {"currency": "KRW", "balance": "1000000", "locked": "0"},
            {"currency": "BTC", "balance": "0.1", "locked": "0"},
        ]

    monkeypatch.setattr(config, "_fetch_market_registry_for_preflight", _market_registry)
    monkeypatch.setattr(config, "_fetch_accounts_payload_for_preflight", _accounts_payload)

    config.validate_live_mode_preflight(settings)

    assert calls == ["market", "accounts"]


def test_market_preflight_rejects_registry_inconsistency_when_market_lookup_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _InconsistentRegistry:
        def require_supported(self, market: str) -> str:
            return market

        def get(self, _market: str) -> None:
            return None

    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(config, "_fetch_market_registry_for_preflight", lambda **_kwargs: _InconsistentRegistry())

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "registry inconsistency" in str(exc.value)


def test_market_preflight_live_defaults_to_forced_registry_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    calls: list[tuple[bool, float, bool]] = []
    object.__setattr__(settings, "MODE", "live")
    object.__setattr__(settings, "MARKET_REGISTRY_CACHE_TTL_SEC", 123.0)
    monkeypatch.delenv("MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH", raising=False)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **kwargs: (
            calls.append((bool(kwargs["refresh"]), float(kwargs["ttl_seconds"]), bool(kwargs["is_details"]))),
            MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
        )[1],
    )

    config.validate_market_preflight(settings)

    assert calls == [(True, 123.0, True)]


def test_market_preflight_paper_defaults_to_cache_reuse(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    calls: list[tuple[bool, float, bool]] = []
    object.__setattr__(settings, "MODE", "paper")
    object.__setattr__(settings, "MARKET_REGISTRY_CACHE_TTL_SEC", 456.0)
    monkeypatch.delenv("MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH", raising=False)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **kwargs: (
            calls.append((bool(kwargs["refresh"]), float(kwargs["ttl_seconds"]), bool(kwargs["is_details"]))),
            MarketRegistry([MarketInfo(market="KRW-BTC", market_warning="NONE")]),
        )[1],
    )

    config.validate_market_preflight(settings)

    assert calls == [(False, 456.0, True)]


def test_market_preflight_rejects_negative_cache_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "MARKET_REGISTRY_CACHE_TTL_SEC", -1.0)
    monkeypatch.setattr(
        config,
        "_fetch_market_registry_for_preflight",
        lambda **_kwargs: pytest.fail("market registry fetch should not run when ttl is invalid"),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    assert "MARKET_REGISTRY_CACHE_TTL_SEC must be >= 0" in str(exc.value)


def test_live_preflight_rejects_legacy_broad_scan_env_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setenv("BITHUMB_V1_ORDER_SCAN_MARKET", "KRW-BTC")
    monkeypatch.setenv("BITHUMB_V1_ORDER_SCAN_STATES", "wait,done")
    monkeypatch.setenv("BITHUMB_V1_ORDER_SCAN_LIMIT", "100")

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    message = str(exc.value)
    assert "legacy /v1/orders broad-scan env is not allowed" in message
    assert "BITHUMB_V1_ORDER_SCAN_MARKET" in message


def test_accounts_preflight_passes_with_valid_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    config.validate_live_mode_preflight(settings)


def test_accounts_preflight_schema_error_blocks_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(config, "_fetch_accounts_payload_for_preflight", lambda **_kwargs: {"status": "0000"})

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "schema mismatch" in msg
    assert "reason=schema mismatch" in msg
    assert "ACCOUNTS_SCHEMA_MISMATCH" in msg
    assert "row_count=0" in msg
    assert "execution_mode=live_dry_run_unarmed" in msg
    assert "quote_currency=KRW" in msg
    assert "base_currency=BTC" in msg


def test_accounts_preflight_required_currency_missing_blocks_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )
    monkeypatch.setattr(config, "_flat_start_safety_for_accounts_preflight", lambda: (False, "not_flat_start"))

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "required currency missing" in msg
    assert "reason=required currency missing" in msg
    assert "row_count=1" in msg
    assert "currencies=KRW" in msg
    assert "ACCOUNTS_REQUIRED_CURRENCY_MISSING" in msg
    assert "missing base currency row 'BTC'" in msg
    assert "execution_mode=live_real_order_path" in msg
    assert "base_currency_missing_policy=block_when_base_currency_row_missing" in msg
    assert "result=fail_real_order_blocked" in msg


def test_accounts_preflight_allows_missing_base_currency_in_live_armed_flat_start(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    _select_small_live_profile(tmp_path)
    monkeypatch.setattr(config, "_flat_start_safety_for_accounts_preflight", lambda: (True, "flat_start_safe"))
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)
    assert "ACCOUNTS_BASE_ROW_MISSING_ALLOWED" in caplog.text
    assert "base_currency_missing_policy=allow_flat_start_when_no_open_or_unresolved_exposure" in caplog.text
    assert "flat_start_allowed=1" in caplog.text


def test_accounts_preflight_missing_base_currency_blocks_live_armed_when_not_flat_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    monkeypatch.setattr(config, "_flat_start_safety_for_accounts_preflight", lambda: (False, "local_unresolved_or_open_orders=1"))
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)
    msg = str(exc.value)
    assert "required currency missing" in msg
    assert "flat_start_allowed=0" in msg
    assert "flat_start_reason=local_unresolved_or_open_orders=1" in msg


def test_accounts_preflight_allows_missing_base_currency_in_live_dry_run_unarmed(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "KRW", "balance": "1000000", "locked": "0"}],
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)
    assert "ACCOUNTS_BASE_ROW_MISSING_ALLOWED" in caplog.text
    assert "result=pass_no_position_allowed" in caplog.text
    assert "execution_mode=live_dry_run_unarmed" in caplog.text


def test_accounts_preflight_still_requires_quote_currency_in_live_dry_run_unarmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", True)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", False)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [{"currency": "BTC", "balance": "0.1", "locked": "0"}],
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "required currency missing" in msg
    assert "missing quote currency row 'KRW'" in msg
    assert "ACCOUNTS_REQUIRED_CURRENCY_MISSING" in msg
    assert "execution_mode=live_dry_run_unarmed" in msg
    assert "base_currency_missing_policy=allow_zero_position_start_in_dry_run" in msg


def test_live_preflight_allows_missing_price_unit_sources_with_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    _set_valid_live_defaults(monkeypatch)
    object.__setattr__(settings, "LIVE_DRY_RUN", False)
    object.__setattr__(settings, "LIVE_REAL_ORDER_ARMED", True)
    object.__setattr__(settings, "BITHUMB_API_KEY", "key")
    object.__setattr__(settings, "BITHUMB_API_SECRET", "secret")
    _select_small_live_profile(tmp_path)
    monkeypatch.setattr(
        order_rules,
        "get_effective_order_rules",
        lambda _pair: order_rules.RuleResolution(
            rules=order_rules.OrderRules(
                market_id="KRW-BTC",
                bid_min_total_krw=5000.0,
                ask_min_total_krw=5000.0,
                bid_price_unit=0.0,
                ask_price_unit=0.0,
                min_qty=float(settings.LIVE_MIN_ORDER_QTY),
                qty_step=float(settings.LIVE_ORDER_QTY_STEP),
                min_notional_krw=float(settings.MIN_ORDER_NOTIONAL_KRW),
                max_qty_decimals=int(settings.LIVE_ORDER_MAX_QTY_DECIMALS),
            ),
            source={
                "min_qty": "local_fallback",
                "qty_step": "local_fallback",
                "min_notional_krw": "local_fallback",
                "max_qty_decimals": "local_fallback",
                "bid_min_total_krw": "chance_doc",
                "ask_min_total_krw": "chance_doc",
                "bid_price_unit": "missing",
                "ask_price_unit": "missing",
            },
        ),
    )

    with caplog.at_level("WARNING"):
        config.validate_live_mode_preflight(settings)
    assert "optional order-rule source gaps detected" in caplog.text


def test_accounts_preflight_duplicate_currency_blocks_live(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: [
            {"currency": "KRW", "balance": "1000000", "locked": "0"},
            {"currency": "KRW", "balance": "2000", "locked": "0"},
        ],
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "reason=duplicate currency" in msg
    assert "ACCOUNTS_DUPLICATE_CURRENCY" in msg
    assert "duplicate_currencies=KRW" in msg


def test_accounts_preflight_auth_failure_is_classified(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("status=401 unauthorized")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "?몄쬆 ?ㅽ뙣" in msg
    assert "ACCOUNTS_AUTH_FAILED" in msg


def test_accounts_preflight_permission_failure_is_classified_as_auth_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("status=403 out_of_scope permission denied")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "?몄쬆 ?ㅽ뙣" in msg
    assert "ACCOUNTS_AUTH_FAILED" in msg
    assert "class=PERMISSION" in msg


def test_accounts_preflight_transport_failure_is_classified(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("transport error timeout")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "transport ?ㅽ뙣" in msg
    assert "ACCOUNTS_TRANSPORT_FAILED" in msg


def test_accounts_preflight_unclassified_private_error_is_transport_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_valid_live_defaults(monkeypatch)
    monkeypatch.setattr(
        config,
        "_fetch_accounts_payload_for_preflight",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("unexpected broker fault")),
    )

    with pytest.raises(config.LiveModeValidationError) as exc:
        config.validate_live_mode_preflight(settings)

    msg = str(exc.value)
    assert "transport ?ㅽ뙣" in msg
    assert "ACCOUNTS_TRANSPORT_FAILED" in msg
    assert "class=UNRECOVERABLE" in msg
