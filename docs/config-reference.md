# Configuration Reference

This file is generated from `src/bithumb_bot/config_spec.py`.
Schema version: `config_spec_v1`
Spec hash: `sha256:8ea24cde5e1e0c6ae87dfbdc9a2048d946e0b7015103afc0463c48b6647f68f7`

| Name | Type | Scope | Default | Live required | Secret | Deprecated/Ignored | Safety | Validation | Description |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ACTIVE_STRATEGIES` | string | common |  | no | no | no | P1 |  | ACTIVE_STRATEGIES runtime configuration. |
| `APPROVED_STRATEGY_PROFILE_PATH` | string | common |  | no | no | no | P1 |  | APPROVED_STRATEGY_PROFILE_PATH runtime configuration. |
| `ARCHIVE_ROOT` | string | common | `<PathManager>` | no | no | no | P1 |  | ARCHIVE_ROOT runtime configuration. |
| `BACKUP_DIR` | string | common |  | no | no | no | P1 |  | BACKUP_DIR runtime configuration. |
| `BACKUP_RETENTION_COUNT` | number | common |  | no | no | no | P1 |  | BACKUP_RETENTION_COUNT runtime configuration. |
| `BACKUP_RETENTION_DAYS` | string | common |  | no | no | no | P1 |  | BACKUP_RETENTION_DAYS runtime configuration. |
| `BACKUP_ROOT` | string | live | `<PathManager>` | yes | no | no | P0 |  | BACKUP_ROOT runtime configuration. |
| `BITHUMB_API_BASE` | string | common |  | no | no | no | P0 |  | BITHUMB_API_BASE runtime configuration. |
| `BITHUMB_API_KEY` | string | live |  | yes | yes | no | P0 |  | BITHUMB_API_KEY runtime configuration. |
| `BITHUMB_API_SECRET` | string | live |  | yes | yes | no | P0 | jwt_hs256_secret, min_live_bytes=32 | BITHUMB_API_SECRET runtime configuration. |
| `BITHUMB_AUTH_DIAGNOSTICS` | string | common |  | no | no | no | P1 |  | BITHUMB_AUTH_DIAGNOSTICS runtime configuration. |
| `BITHUMB_CANCEL_RETRY_ATTEMPTS` | string | common |  | no | no | no | P1 |  | BITHUMB_CANCEL_RETRY_ATTEMPTS runtime configuration. |
| `BITHUMB_CANCEL_RETRY_BACKOFF_SEC` | number | common |  | no | no | no | P1 |  | BITHUMB_CANCEL_RETRY_BACKOFF_SEC runtime configuration. |
| `BITHUMB_DEPLOY_COMMIT_SHA` | string | internal |  | no | no | no | internal |  | BITHUMB_DEPLOY_COMMIT_SHA runtime configuration. |
| `BITHUMB_DEPLOY_DIRTY` | string | internal |  | no | no | no | internal |  | BITHUMB_DEPLOY_DIRTY runtime configuration. |
| `BITHUMB_ENV_FILE` | string | bootstrap |  | no | no | no | P1 |  | BITHUMB_ENV_FILE runtime configuration. |
| `BITHUMB_ENV_FILE_LIVE` | string | bootstrap |  | no | no | no | P1 |  | BITHUMB_ENV_FILE_LIVE runtime configuration. |
| `BITHUMB_ENV_FILE_PAPER` | string | bootstrap |  | no | no | no | P1 |  | BITHUMB_ENV_FILE_PAPER runtime configuration. |
| `BITHUMB_ORDER_RPS_LIMIT` | string | common |  | no | no | no | P1 |  | BITHUMB_ORDER_RPS_LIMIT runtime configuration. |
| `BITHUMB_PRIVATE_RPS_LIMIT` | string | common |  | no | no | no | P1 |  | BITHUMB_PRIVATE_RPS_LIMIT runtime configuration. |
| `BITHUMB_PYTEST_RUN_ID` | string | internal |  | no | no | no | internal |  | BITHUMB_PYTEST_RUN_ID runtime configuration. |
| `BITHUMB_PYTEST_STARTED` | bool | internal |  | no | no | no | internal |  | BITHUMB_PYTEST_STARTED runtime configuration. |
| `BITHUMB_PYTEST_SUITE` | string | internal |  | no | no | no | internal |  | BITHUMB_PYTEST_SUITE runtime configuration. |
| `BITHUMB_PYTEST_SUMMARY_ON_SUCCESS` | bool | internal |  | no | no | no | internal |  | BITHUMB_PYTEST_SUMMARY_ON_SUCCESS runtime configuration. |
| `BITHUMB_RESEARCH_ALLOW_UNSAFE_FORK` | bool | internal |  | no | no | no | internal |  | BITHUMB_RESEARCH_ALLOW_UNSAFE_FORK runtime configuration. |
| `BITHUMB_RESEARCH_MAX_WORKERS` | number | internal |  | no | no | no | internal |  | BITHUMB_RESEARCH_MAX_WORKERS runtime configuration. |
| `BITHUMB_RESEARCH_MP_START_METHOD` | string | internal |  | no | no | no | internal |  | BITHUMB_RESEARCH_MP_START_METHOD runtime configuration. |
| `BITHUMB_TOTAL_PROCESS_BUDGET` | number | internal |  | no | no | no | internal |  | BITHUMB_TOTAL_PROCESS_BUDGET runtime configuration. |
| `BITHUMB_WS_MYASSET_ENABLED` | bool | common |  | no | no | no | P1 |  | BITHUMB_WS_MYASSET_ENABLED runtime configuration. |
| `BITHUMB_WS_MYASSET_RECV_TIMEOUT_SEC` | number | common |  | no | no | no | P1 |  | BITHUMB_WS_MYASSET_RECV_TIMEOUT_SEC runtime configuration. |
| `BITHUMB_WS_MYASSET_STALE_AFTER_MS` | number | common |  | no | no | no | P1 |  | BITHUMB_WS_MYASSET_STALE_AFTER_MS runtime configuration. |
| `BITHUMB_WS_MYASSET_SUBSCRIBE_TICKET` | string | common |  | no | no | no | P1 |  | BITHUMB_WS_MYASSET_SUBSCRIBE_TICKET runtime configuration. |
| `BLOCK_ON_OPEN_ORDER` | string | common |  | no | no | no | P1 |  | BLOCK_ON_OPEN_ORDER runtime configuration. |
| `BLOCK_ON_SUBMIT_UNKNOWN` | string | common |  | no | no | no | P1 |  | BLOCK_ON_SUBMIT_UNKNOWN runtime configuration. |
| `BUY_FRACTION` | string | paper |  | no | no | no | P1 |  | BUY_FRACTION runtime configuration. |
| `BUY_PRICE_NONE_MARKET_TO_PRICE_ALIAS_ENABLED` | bool | common |  | no | no | deprecated, ignored | P1 |  | Deprecated and ignored compatibility gate; runtime behavior remains fail-closed. |
| `COOLDOWN_MIN` | number | common |  | no | no | no | P1 |  | COOLDOWN_MIN runtime configuration. |
| `DATA_ROOT` | string | live | `<PathManager>` | yes | no | no | P0 |  | DATA_ROOT runtime configuration. |
| `DB_BUSY_TIMEOUT_MS` | number | common |  | no | no | no | P0 |  | DB_BUSY_TIMEOUT_MS runtime configuration. |
| `DB_LOCK_RETRY_BACKOFF_MS` | number | common |  | no | no | no | P0 |  | DB_LOCK_RETRY_BACKOFF_MS runtime configuration. |
| `DB_LOCK_RETRY_COUNT` | number | common |  | no | no | no | P0 |  | DB_LOCK_RETRY_COUNT runtime configuration. |
| `DB_PATH` | string | live | `<PathManager>` | yes | no | no | P0 |  | Compatibility override for the mode-specific SQLite trade ledger path. |
| `ENTRY_EDGE_BUFFER_RATIO` | number | common |  | no | no | no | P1 |  | ENTRY_EDGE_BUFFER_RATIO runtime configuration. |
| `ENV_ROOT` | string | live | `<PathManager>` | yes | no | no | P0 |  | ENV_ROOT runtime configuration. |
| `EVERY` | string | common |  | no | no | no | P1 |  | EVERY runtime configuration. |
| `EXECUTION_ALLOW_SAME_CANDLE_CLOSE_FILL` | string | common |  | no | no | no | P1 |  | EXECUTION_ALLOW_SAME_CANDLE_CLOSE_FILL runtime configuration. |
| `EXECUTION_CALIBRATION_ARTIFACT_HASH` | string | common |  | no | no | no | P1 |  | EXECUTION_CALIBRATION_ARTIFACT_HASH runtime configuration. |
| `EXECUTION_CALIBRATION_REQUIRED` | string | common |  | no | no | no | P1 |  | EXECUTION_CALIBRATION_REQUIRED runtime configuration. |
| `EXECUTION_DECISION_GUARD_MS` | number | common |  | no | no | no | P1 |  | EXECUTION_DECISION_GUARD_MS runtime configuration. |
| `EXECUTION_DEPTH_REQUIRED` | string | common |  | no | no | no | P1 |  | EXECUTION_DEPTH_REQUIRED runtime configuration. |
| `EXECUTION_ENGINE` | string | common |  | no | no | no | P1 |  | EXECUTION_ENGINE runtime configuration. |
| `EXECUTION_FEE_SOURCE` | string | common |  | no | no | no | P1 |  | EXECUTION_FEE_SOURCE runtime configuration. |
| `EXECUTION_FILL_REFERENCE_POLICY` | string | common |  | no | no | no | P1 |  | EXECUTION_FILL_REFERENCE_POLICY runtime configuration. |
| `EXECUTION_INTRA_CANDLE_PATH_AVAILABLE` | string | common |  | no | no | no | P1 |  | EXECUTION_INTRA_CANDLE_PATH_AVAILABLE runtime configuration. |
| `EXECUTION_LATENCY_MODEL_TYPE` | string | common |  | no | no | no | P1 |  | EXECUTION_LATENCY_MODEL_TYPE runtime configuration. |
| `EXECUTION_LATENCY_MS` | number | common |  | no | no | no | P1 |  | EXECUTION_LATENCY_MS runtime configuration. |
| `EXECUTION_MARKET_IMPACT_REQUIRED` | string | common |  | no | no | no | P1 |  | EXECUTION_MARKET_IMPACT_REQUIRED runtime configuration. |
| `EXECUTION_MAX_QUOTE_WAIT_MS` | number | common |  | no | no | no | P1 |  | EXECUTION_MAX_QUOTE_WAIT_MS runtime configuration. |
| `EXECUTION_MIN_REALITY_LEVEL_FOR_PROMOTION` | string | common |  | no | no | no | P1 |  | EXECUTION_MIN_REALITY_LEVEL_FOR_PROMOTION runtime configuration. |
| `EXECUTION_MISSING_QUOTE_POLICY` | string | common |  | no | no | no | P1 |  | EXECUTION_MISSING_QUOTE_POLICY runtime configuration. |
| `EXECUTION_ORDER_FAILURE_MODEL_TYPE` | string | common |  | no | no | no | P1 |  | EXECUTION_ORDER_FAILURE_MODEL_TYPE runtime configuration. |
| `EXECUTION_ORDER_FAILURE_RATE` | number | common |  | no | no | no | P1 |  | EXECUTION_ORDER_FAILURE_RATE runtime configuration. |
| `EXECUTION_PARTIAL_FILL_MODEL_TYPE` | string | common |  | no | no | no | P1 |  | EXECUTION_PARTIAL_FILL_MODEL_TYPE runtime configuration. |
| `EXECUTION_PARTIAL_FILL_RATE` | number | common |  | no | no | no | P1 |  | EXECUTION_PARTIAL_FILL_RATE runtime configuration. |
| `EXECUTION_QUEUE_POSITION_REQUIRED` | string | common |  | no | no | no | P1 |  | EXECUTION_QUEUE_POSITION_REQUIRED runtime configuration. |
| `EXECUTION_QUOTE_AGE_LIMIT_MS` | number | common |  | no | no | no | P1 |  | EXECUTION_QUOTE_AGE_LIMIT_MS runtime configuration. |
| `EXECUTION_QUOTE_SOURCE` | string | common |  | no | no | no | P1 |  | EXECUTION_QUOTE_SOURCE runtime configuration. |
| `EXECUTION_REALITY_LEVEL` | string | common |  | no | no | no | P1 |  | EXECUTION_REALITY_LEVEL runtime configuration. |
| `EXECUTION_SLIPPAGE_SOURCE` | string | common |  | no | no | no | P1 |  | EXECUTION_SLIPPAGE_SOURCE runtime configuration. |
| `EXECUTION_TOP_OF_BOOK_IS_FULL_DEPTH` | string | common |  | no | no | no | P1 |  | EXECUTION_TOP_OF_BOOK_IS_FULL_DEPTH runtime configuration. |
| `EXECUTION_TOP_OF_BOOK_REQUIRED` | string | common |  | no | no | no | P1 |  | EXECUTION_TOP_OF_BOOK_REQUIRED runtime configuration. |
| `EXECUTION_TRADE_TICK_REQUIRED` | string | common |  | no | no | no | P1 |  | EXECUTION_TRADE_TICK_REQUIRED runtime configuration. |
| `FEE_RATE` | number | common |  | no | no | no | P1 |  | FEE_RATE runtime configuration. |
| `HEALTH_MAX_CANDLE_AGE_SEC` | number | common |  | no | no | no | P1 |  | HEALTH_MAX_CANDLE_AGE_SEC runtime configuration. |
| `HEALTH_MAX_ERROR_COUNT` | number | common |  | no | no | no | P1 |  | HEALTH_MAX_ERROR_COUNT runtime configuration. |
| `INTERVAL` | string | common |  | no | no | no | P1 |  | INTERVAL runtime configuration. |
| `KILL_SWITCH` | bool | common |  | no | no | no | P0 |  | KILL_SWITCH runtime configuration. |
| `KILL_SWITCH_LIQUIDATE` | bool | common |  | no | no | no | P0 |  | KILL_SWITCH_LIQUIDATE runtime configuration. |
| `LEGACY_DEFAULT_STRATEGY_COMPAT` | string | common |  | no | no | no | P1 |  | LEGACY_DEFAULT_STRATEGY_COMPAT runtime configuration. |
| `LIVE_ALLOW_ORDER_RULE_FALLBACK` | string | live |  | no | no | deprecated, ignored | P0 |  | Deprecated and ignored compatibility gate; use LIVE_ORDER_RULE_FALLBACK_PROFILE. |
| `LIVE_DRY_RUN` | bool | live | `true` | no | no | no | P0 |  | LIVE_DRY_RUN runtime configuration. |
| `LIVE_EXECUTION_QUALITY_GATE_ENABLED` | bool | live |  | no | no | no | P0 |  | LIVE_EXECUTION_QUALITY_GATE_ENABLED runtime configuration. |
| `LIVE_EXECUTION_QUALITY_GATE_MODE` | string | live |  | no | no | no | P0 |  | LIVE_EXECUTION_QUALITY_GATE_MODE runtime configuration. |
| `LIVE_EXECUTION_QUALITY_MAX_MODEL_BREACH_RATE` | number | live |  | no | no | no | P0 |  | LIVE_EXECUTION_QUALITY_MAX_MODEL_BREACH_RATE runtime configuration. |
| `LIVE_EXECUTION_QUALITY_MAX_P90_SLIPPAGE_BPS` | number | live |  | no | no | no | P0 |  | LIVE_EXECUTION_QUALITY_MAX_P90_SLIPPAGE_BPS runtime configuration. |
| `LIVE_EXECUTION_QUALITY_MAX_P95_FULL_FILL_LATENCY_MS` | number | live |  | no | no | no | P0 |  | LIVE_EXECUTION_QUALITY_MAX_P95_FULL_FILL_LATENCY_MS runtime configuration. |
| `LIVE_EXECUTION_QUALITY_MAX_PARTIAL_FILL_RATE` | number | live |  | no | no | no | P0 |  | LIVE_EXECUTION_QUALITY_MAX_PARTIAL_FILL_RATE runtime configuration. |
| `LIVE_EXECUTION_QUALITY_MIN_SAMPLE` | string | live |  | no | no | no | P0 |  | LIVE_EXECUTION_QUALITY_MIN_SAMPLE runtime configuration. |
| `LIVE_FEE_RATE_ESTIMATE` | string | live |  | no | no | no | P0 |  | LIVE_FEE_RATE_ESTIMATE runtime configuration. |
| `LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW` | number | live |  | no | no | no | P0 |  | LIVE_FILL_FEE_ALERT_MIN_NOTIONAL_KRW runtime configuration. |
| `LIVE_FILL_FEE_RATIO_MAX` | string | live |  | no | no | no | P0 |  | LIVE_FILL_FEE_RATIO_MAX runtime configuration. |
| `LIVE_FILL_FEE_RATIO_MIN` | number | live |  | no | no | no | P0 |  | LIVE_FILL_FEE_RATIO_MIN runtime configuration. |
| `LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW` | number | live |  | no | no | no | P0 |  | LIVE_FILL_FEE_STRICT_MIN_NOTIONAL_KRW runtime configuration. |
| `LIVE_FILL_FEE_STRICT_MODE` | string | live |  | no | no | no | P0 |  | LIVE_FILL_FEE_STRICT_MODE runtime configuration. |
| `LIVE_MIN_ORDER_QTY` | string | live |  | no | no | no | P0 |  | LIVE_MIN_ORDER_QTY runtime configuration. |
| `LIVE_ORDER_MAX_QTY_DECIMALS` | string | live |  | no | no | no | P0 |  | LIVE_ORDER_MAX_QTY_DECIMALS runtime configuration. |
| `LIVE_ORDER_QTY_STEP` | string | live |  | no | no | no | P0 |  | LIVE_ORDER_QTY_STEP runtime configuration. |
| `LIVE_ORDER_RULE_FALLBACK_PROFILE` | string | live | `persisted_snapshot_required` | no | no | no | P0 |  | Controls order-rule fallback posture; armed live must use persisted_snapshot_required. |
| `LIVE_PERFORMANCE_GATE_ENABLED` | bool | live |  | no | no | no | P0 |  | LIVE_PERFORMANCE_GATE_ENABLED runtime configuration. |
| `LIVE_PERFORMANCE_GATE_MAX_FEE_DRAG_RATIO` | number | live |  | no | no | no | P0 |  | LIVE_PERFORMANCE_GATE_MAX_FEE_DRAG_RATIO runtime configuration. |
| `LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW` | number | live |  | no | no | no | P0 |  | LIVE_PERFORMANCE_GATE_MIN_EXPECTANCY_KRW runtime configuration. |
| `LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW` | number | live |  | no | no | no | P0 |  | LIVE_PERFORMANCE_GATE_MIN_NET_PNL_KRW runtime configuration. |
| `LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR` | string | live |  | no | no | no | P0 |  | LIVE_PERFORMANCE_GATE_MIN_PROFIT_FACTOR runtime configuration. |
| `LIVE_PERFORMANCE_GATE_MIN_SAMPLE` | string | live |  | no | no | no | P0 |  | LIVE_PERFORMANCE_GATE_MIN_SAMPLE runtime configuration. |
| `LIVE_PERFORMANCE_GATE_RECENT_LIMIT` | string | live |  | no | no | no | P0 |  | LIVE_PERFORMANCE_GATE_RECENT_LIMIT runtime configuration. |
| `LIVE_PERFORMANCE_GATE_SCOPE` | string | live |  | no | no | no | P0 |  | LIVE_PERFORMANCE_GATE_SCOPE runtime configuration. |
| `LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS` | number | live |  | no | no | no | P0 |  | LIVE_PRICE_PROTECTION_MAX_SLIPPAGE_BPS runtime configuration. |
| `LIVE_PRICE_REFERENCE_MAX_AGE_SEC` | number | live |  | no | no | no | P0 |  | LIVE_PRICE_REFERENCE_MAX_AGE_SEC runtime configuration. |
| `LIVE_REAL_ORDER_ARMED` | bool | live | `false` | no | no | no | P0 |  | LIVE_REAL_ORDER_ARMED runtime configuration. |
| `LIVE_SUBMIT_CONTRACT_PROFILE` | string | live | `live_explicit_submit_plan_v1` | no | no | no | P0 |  | LIVE_SUBMIT_CONTRACT_PROFILE runtime configuration. |
| `LOG_ROOT` | string | live | `<PathManager>` | yes | no | no | P0 |  | LOG_ROOT runtime configuration. |
| `MARKET` | string | common | `KRW-BTC` | no | no | no | P1 |  | MARKET runtime configuration. |
| `MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR` | string | common |  | no | no | no | P1 |  | MARKET_PREFLIGHT_BLOCK_ON_CATALOG_ERROR runtime configuration. |
| `MARKET_PREFLIGHT_BLOCK_ON_WARNING` | string | common |  | no | no | no | P1 |  | MARKET_PREFLIGHT_BLOCK_ON_WARNING runtime configuration. |
| `MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH` | string | common |  | no | no | no | P1 |  | MARKET_PREFLIGHT_FORCE_REGISTRY_REFRESH runtime configuration. |
| `MARKET_PREFLIGHT_WARNING_STATES` | string | common |  | no | no | no | P1 |  | MARKET_PREFLIGHT_WARNING_STATES runtime configuration. |
| `MARKET_REGISTRY_CACHE_TTL_SEC` | number | common |  | no | no | no | P1 |  | MARKET_REGISTRY_CACHE_TTL_SEC runtime configuration. |
| `MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC` | number | common |  | no | no | no | P1 |  | MARKET_RUNTIME_REGISTRY_REFRESH_INTERVAL_SEC runtime configuration. |
| `MAX_DAILY_LOSS_KRW` | number | live |  | yes | no | no | P0 |  | MAX_DAILY_LOSS_KRW runtime configuration. |
| `MAX_DAILY_ORDER_COUNT` | number | live |  | yes | no | no | P0 |  | MAX_DAILY_ORDER_COUNT runtime configuration. |
| `MAX_MARKET_SLIPPAGE_BPS` | number | live |  | yes | no | no | P0 |  | MAX_MARKET_SLIPPAGE_BPS runtime configuration. |
| `MAX_OPEN_ORDER_AGE_SEC` | number | common |  | no | no | no | P0 |  | MAX_OPEN_ORDER_AGE_SEC runtime configuration. |
| `MAX_OPEN_POSITIONS` | string | common |  | no | no | no | P0 |  | MAX_OPEN_POSITIONS runtime configuration. |
| `MAX_ORDERBOOK_SPREAD_BPS` | number | live |  | yes | no | no | P0 |  | MAX_ORDERBOOK_SPREAD_BPS runtime configuration. |
| `MAX_ORDER_KRW` | number | live |  | yes | no | no | P0 |  | MAX_ORDER_KRW runtime configuration. |
| `MAX_POSITION_LOSS_PCT` | string | common |  | no | no | no | P0 |  | MAX_POSITION_LOSS_PCT runtime configuration. |
| `MIN_GAP` | string | common |  | no | no | no | P1 |  | MIN_GAP runtime configuration. |
| `MIN_MARGIN_AFTER_COST_RATIO` | number | common |  | no | no | no | P1 |  | MIN_MARGIN_AFTER_COST_RATIO runtime configuration. |
| `MIN_NET_EDGE_KRW` | number | common |  | no | no | no | P1 |  | MIN_NET_EDGE_KRW runtime configuration. |
| `MIN_ORDER_NOTIONAL_KRW` | number | common |  | no | no | no | P1 |  | MIN_ORDER_NOTIONAL_KRW runtime configuration. |
| `MODE` | string | common | `paper` | no | no | no | P1 |  | MODE runtime configuration. |
| `NOTIFIER_DEDUPE_WINDOW_SEC` | number | common | `20` | no | no | no | P1 |  | Notifier duplicate suppression window in seconds. Internal operational setting, documented for reproducibility. |
| `NOTIFIER_ENABLED` | bool | common |  | no | no | no | P1 |  | NOTIFIER_ENABLED runtime configuration. |
| `NOTIFIER_TIMEOUT_SEC` | number | common |  | no | no | no | P1 |  | NOTIFIER_TIMEOUT_SEC runtime configuration. |
| `NOTIFIER_WEBHOOK_URL` | string | common |  | no | yes | no | P1 |  | NOTIFIER_WEBHOOK_URL runtime configuration. |
| `NTFY_PRIORITY_FAILURE` | string | common |  | no | no | no | P1 |  | NTFY_PRIORITY_FAILURE runtime configuration. |
| `NTFY_PRIORITY_SUCCESS` | string | common |  | no | no | no | P1 |  | NTFY_PRIORITY_SUCCESS runtime configuration. |
| `NTFY_SERVER` | string | common |  | no | no | no | P1 |  | NTFY_SERVER runtime configuration. |
| `NTFY_TITLE_PREFIX` | string | common |  | no | no | no | P1 |  | NTFY_TITLE_PREFIX runtime configuration. |
| `NTFY_TOPIC` | string | common |  | no | no | no | P1 |  | NTFY_TOPIC runtime configuration. |
| `OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC` | number | common |  | no | no | no | P1 |  | OPEN_ORDER_RECONCILE_MIN_INTERVAL_SEC runtime configuration. |
| `PAIR` | string | common | `KRW-BTC` | no | no | no | P1 |  | PAIR runtime configuration. |
| `PAPER_EXECUTION_LATENCY_MS` | number | paper |  | no | no | no | P2 |  | PAPER_EXECUTION_LATENCY_MS runtime configuration. |
| `PAPER_EXECUTION_MODEL` | string | paper |  | no | no | no | P2 |  | PAPER_EXECUTION_MODEL runtime configuration. |
| `PAPER_EXECUTION_ORDER_FAILURE_RATE` | number | paper |  | no | no | no | P2 |  | PAPER_EXECUTION_ORDER_FAILURE_RATE runtime configuration. |
| `PAPER_EXECUTION_PARTIAL_FILL_FRACTION` | string | paper |  | no | no | no | P2 |  | PAPER_EXECUTION_PARTIAL_FILL_FRACTION runtime configuration. |
| `PAPER_EXECUTION_PARTIAL_FILL_RATE` | number | paper |  | no | no | no | P2 |  | PAPER_EXECUTION_PARTIAL_FILL_RATE runtime configuration. |
| `PAPER_EXECUTION_STRESS_SEED` | string | paper |  | no | no | no | P2 |  | PAPER_EXECUTION_STRESS_SEED runtime configuration. |
| `PAPER_FEE_RATE` | number | paper |  | no | no | no | P2 |  | PAPER_FEE_RATE runtime configuration. |
| `PAPER_FEE_RATE_ESTIMATE` | string | paper |  | no | no | no | P2 |  | PAPER_FEE_RATE_ESTIMATE runtime configuration. |
| `PRETRADE_BALANCE_BUFFER_BPS` | number | common |  | no | no | no | P1 |  | PRETRADE_BALANCE_BUFFER_BPS runtime configuration. |
| `PRE_TRADE_ECONOMICS_BLOCKING_ENABLED` | bool | common |  | no | no | no | P1 |  | PRE_TRADE_ECONOMICS_BLOCKING_ENABLED runtime configuration. |
| `PYTEST_XDIST_WORKER` | string | internal |  | no | no | no | internal |  | PYTEST_XDIST_WORKER runtime configuration. |
| `PYTEST_XDIST_WORKERS` | number | internal |  | no | no | no | internal |  | PYTEST_XDIST_WORKERS runtime configuration. |
| `PYTEST_XDIST_WORKER_COUNT` | number | internal |  | no | no | no | internal |  | PYTEST_XDIST_WORKER_COUNT runtime configuration. |
| `REQUIRE_BROKER_LOCAL_CONVERGENCE` | string | common |  | no | no | no | P1 |  | REQUIRE_BROKER_LOCAL_CONVERGENCE runtime configuration. |
| `RESIDUAL_BUY_SIZING_MODE` | string | common |  | no | no | no | P1 |  | RESIDUAL_BUY_SIZING_MODE runtime configuration. |
| `RESIDUAL_INVENTORY_MODE` | string | common |  | no | no | no | P1 |  | RESIDUAL_INVENTORY_MODE runtime configuration. |
| `RESIDUAL_LIVE_SELL_MODE` | string | common |  | no | no | no | P1 |  | RESIDUAL_LIVE_SELL_MODE runtime configuration. |
| `RUNTIME_STRATEGY_SET_JSON` | string | common |  | no | no | no | P1 |  | RUNTIME_STRATEGY_SET_JSON runtime configuration. |
| `RUN_LOCK_PATH` | string | common | `<PathManager>` | no | no | no | P0 |  | Compatibility override for the mode-specific run lock path. |
| `RUN_ROOT` | string | live | `<PathManager>` | yes | no | no | P0 |  | RUN_ROOT runtime configuration. |
| `SLACK_WEBHOOK_URL` | string | common |  | no | yes | no | P1 |  | SLACK_WEBHOOK_URL runtime configuration. |
| `SLIPPAGE_BPS` | number | paper |  | no | no | no | P1 |  | SLIPPAGE_BPS runtime configuration. |
| `SMA_COST_EDGE_ENABLED` | bool | common |  | no | no | no | P2 |  | SMA_COST_EDGE_ENABLED runtime configuration. |
| `SMA_COST_EDGE_MIN_RATIO` | number | common |  | no | no | no | P2 |  | SMA_COST_EDGE_MIN_RATIO runtime configuration. |
| `SMA_FILTER_GAP_MIN_RATIO` | number | common |  | no | no | no | P2 |  | SMA_FILTER_GAP_MIN_RATIO runtime configuration. |
| `SMA_FILTER_OVEREXT_LOOKBACK` | string | common |  | no | no | no | P2 |  | SMA_FILTER_OVEREXT_LOOKBACK runtime configuration. |
| `SMA_FILTER_OVEREXT_MAX_RETURN_RATIO` | number | common |  | no | no | no | P2 |  | SMA_FILTER_OVEREXT_MAX_RETURN_RATIO runtime configuration. |
| `SMA_FILTER_VOL_MIN_RANGE_RATIO` | number | common |  | no | no | no | P2 |  | SMA_FILTER_VOL_MIN_RANGE_RATIO runtime configuration. |
| `SMA_FILTER_VOL_WINDOW` | string | common |  | no | no | no | P2 |  | SMA_FILTER_VOL_WINDOW runtime configuration. |
| `SMA_LONG` | string | common |  | no | no | no | P2 |  | SMA_LONG runtime configuration. |
| `SMA_MARKET_REGIME_ENABLED` | bool | common |  | no | no | no | P2 |  | SMA_MARKET_REGIME_ENABLED runtime configuration. |
| `SMA_SHORT` | string | common |  | no | no | no | P2 |  | SMA_SHORT runtime configuration. |
| `SNAPSHOT_ROOT` | string | common | `<PathManager>` | no | no | no | P1 |  | Compatibility override for snapshot output under the backup bucket. |
| `START_CASH_KRW` | number | paper |  | no | no | no | P1 |  | START_CASH_KRW runtime configuration. |
| `STRATEGY_APPROVED_PROFILE_PATH` | string | common |  | no | no | no | P2 |  | STRATEGY_APPROVED_PROFILE_PATH runtime configuration. |
| `STRATEGY_CANDIDATE_PROFILE_PATH` | string | common |  | no | no | no | P2 |  | STRATEGY_CANDIDATE_PROFILE_PATH runtime configuration. |
| `STRATEGY_ENTRY_SLIPPAGE_BPS` | number | common |  | no | no | no | P2 |  | STRATEGY_ENTRY_SLIPPAGE_BPS runtime configuration. |
| `STRATEGY_EXIT_MAX_HOLDING_MIN` | number | common |  | no | no | no | P2 |  | STRATEGY_EXIT_MAX_HOLDING_MIN runtime configuration. |
| `STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO` | number | common |  | no | no | no | P2 |  | STRATEGY_EXIT_MIN_TAKE_PROFIT_RATIO runtime configuration. |
| `STRATEGY_EXIT_RULES` | string | common |  | no | no | no | P2 |  | STRATEGY_EXIT_RULES runtime configuration. |
| `STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO` | number | common |  | no | no | no | P2 |  | STRATEGY_EXIT_SMALL_LOSS_TOLERANCE_RATIO runtime configuration. |
| `STRATEGY_EXIT_STOP_LOSS_RATIO` | number | common |  | no | no | no | P2 |  | STRATEGY_EXIT_STOP_LOSS_RATIO runtime configuration. |
| `STRATEGY_MIN_EXPECTED_EDGE_RATIO` | number | common |  | no | no | no | P2 |  | STRATEGY_MIN_EXPECTED_EDGE_RATIO runtime configuration. |
| `STRATEGY_NAME` | string | common |  | no | no | no | P2 |  | STRATEGY_NAME runtime configuration. |
| `STRATEGY_PARAMETERS_JSON` | string | common |  | no | no | no | P2 |  | STRATEGY_PARAMETERS_JSON runtime configuration. |
| `TARGET_EXECUTION_SHADOW` | bool | common |  | no | no | no | P1 |  | TARGET_EXECUTION_SHADOW runtime configuration. |
| `TARGET_EXPOSURE_KRW` | number | common |  | no | no | no | P1 |  | TARGET_EXPOSURE_KRW runtime configuration. |
| `TARGET_HOLD_POLICY` | string | common |  | no | no | no | P1 |  | TARGET_HOLD_POLICY runtime configuration. |
| `TELEGRAM_BOT_TOKEN` | string | common |  | no | yes | no | P1 |  | TELEGRAM_BOT_TOKEN runtime configuration. |
| `TELEGRAM_CHAT_ID` | string | common |  | no | no | no | P1 |  | TELEGRAM_CHAT_ID runtime configuration. |
| `XDG_STATE_HOME` | string | internal |  | no | no | no | internal |  | XDG_STATE_HOME runtime configuration. |
