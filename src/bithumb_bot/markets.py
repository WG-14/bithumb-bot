from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from threading import Lock
import time
import httpx

from .public_api import PublicApiSchemaError, get_public_json_with_retry


BASE_URL = "https://api.bithumb.com"
MARKET_CATALOG_ENDPOINT = "/v1/market/all"
LOG = logging.getLogger(__name__)


class MarketCatalogError(PublicApiSchemaError):
    """Raised when market catalog fetch/parsing fails."""


class MarketContractDriftError(MarketCatalogError):
    """Raised when exchange market code format drifts from documented schema."""


class UserMarketInputError(ValueError):
    """Raised when user/config market input cannot be parsed."""


class ExchangeMarketCodeError(ValueError):
    """Raised when exchange boundary market code is not canonical QUOTE-BASE."""


class UnsupportedMarketError(ValueError):
    """Raised when a market is not supported by the loaded market catalog."""


@dataclass(frozen=True)
class MarketInfo:
    market: str
    korean_name: str | None = None
    english_name: str | None = None
    market_warning: str | None = None


@dataclass(frozen=True)
class MarketWarningPolicyDecision:
    normalized_warning: str
    is_warning_state: bool
    should_block: bool


class MarketCatalogClient:
    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        timeout: float = 10.0,
        max_retries: int = 3,
        base_backoff_sec: float = 0.2,
        max_backoff_sec: float = 2.0,
        jitter_sec: float = 0.1,
    ) -> None:
        self._base_url = base_url
        self._timeout = timeout
        self._max_retries = max_retries
        self._base_backoff_sec = base_backoff_sec
        self._max_backoff_sec = max_backoff_sec
        self._jitter_sec = jitter_sec

    def fetch_markets(self, *, is_details: bool = False) -> list[MarketInfo]:
        endpoint = MARKET_CATALOG_ENDPOINT
        params = {"isDetails": "true" if is_details else "false"}
        try:
            with httpx.Client(base_url=self._base_url, timeout=self._timeout) as client:
                payload = get_public_json_with_retry(
                    client,
                    endpoint,
                    params=params,
                    max_retries=self._max_retries,
                    base_backoff_sec=self._base_backoff_sec,
                    max_backoff_sec=self._max_backoff_sec,
                    jitter_sec=self._jitter_sec,
                )

            if not isinstance(payload, list):
                raise MarketCatalogError(f"unexpected market catalog payload type: {type(payload).__name__}")
            if not payload:
                raise MarketCatalogError("market catalog is empty")

            items: list[MarketInfo] = []
            for row in payload:
                item = parse_market_catalog_row_details(row) if is_details else parse_market_catalog_row(row)
                items.append(item)
            return items
        except Exception as exc:
            schema_drift = isinstance(exc, (MarketCatalogError, PublicApiSchemaError))
            LOG.warning(
                "market catalog fetch failed endpoint=%s isDetails=%s retries=%s schema_drift=%s error=%s: %s",
                endpoint,
                params["isDetails"],
                max(0, int(self._max_retries)),
                schema_drift,
                type(exc).__name__,
                exc,
            )
            raise


class MarketRegistry:
    def __init__(self, markets: list[MarketInfo]) -> None:
        self._markets: dict[str, MarketInfo] = {m.market: m for m in markets}

    @classmethod
    def from_catalog(cls, *, client: MarketCatalogClient | None = None, is_details: bool = False) -> "MarketRegistry":
        catalog_client = client or MarketCatalogClient()
        return cls(catalog_client.fetch_markets(is_details=is_details))

    def is_supported(self, market: str) -> bool:
        return self.is_supported_canonical(parse_documented_market_code(market))

    def require_supported(self, market: str) -> str:
        canonical = parse_documented_market_code(market)
        return self.require_supported_canonical(canonical)

    def is_supported_canonical(self, market: str) -> bool:
        return market in self._markets

    def require_supported_canonical(self, market: str) -> str:
        if market not in self._markets:
            raise UnsupportedMarketError(f"unsupported market: {market!r} (canonical={market})")
        return market

    def get(self, market: str) -> MarketInfo | None:
        return self._markets.get(parse_documented_market_code(market))

    def items(self) -> list[MarketInfo]:
        return list(self._markets.values())


_market_registry_lock = Lock()
_market_registry_cache_by_detail: dict[bool, MarketRegistry] = {}
_market_registry_cached_at_monotonic_by_detail: dict[bool, float] = {}


def _cache_is_fresh_for_details(*, is_details: bool, now_monotonic: float, ttl_seconds: float | None) -> bool:
    if is_details not in _market_registry_cache_by_detail:
        return False
    if ttl_seconds is None:
        return True
    if ttl_seconds <= 0:
        return False
    cached_at = _market_registry_cached_at_monotonic_by_detail.get(is_details)
    if cached_at is None:
        return False
    return (now_monotonic - cached_at) < ttl_seconds


def get_market_registry(
    *,
    refresh: bool = False,
    client: MarketCatalogClient | None = None,
    is_details: bool = False,
    ttl_seconds: float | None = 900.0,
) -> MarketRegistry:
    """Return cached market registry loaded from /v1/market/all."""
    now_monotonic = time.monotonic()
    if refresh:
        registry = MarketRegistry.from_catalog(client=client, is_details=is_details)
        with _market_registry_lock:
            _market_registry_cache_by_detail[is_details] = registry
            _market_registry_cached_at_monotonic_by_detail[is_details] = now_monotonic
        return registry

    with _market_registry_lock:
        if _cache_is_fresh_for_details(
            is_details=is_details,
            now_monotonic=now_monotonic,
            ttl_seconds=ttl_seconds,
        ):
            return _market_registry_cache_by_detail[is_details]

    registry = MarketRegistry.from_catalog(client=client, is_details=is_details)
    with _market_registry_lock:
        if refresh or not _cache_is_fresh_for_details(
            is_details=is_details,
            now_monotonic=now_monotonic,
            ttl_seconds=ttl_seconds,
        ):
            _market_registry_cache_by_detail[is_details] = registry
            _market_registry_cached_at_monotonic_by_detail[is_details] = now_monotonic
        elif is_details not in _market_registry_cache_by_detail:
            _market_registry_cache_by_detail[is_details] = registry
            _market_registry_cached_at_monotonic_by_detail[is_details] = now_monotonic
        return _market_registry_cache_by_detail[is_details]


def canonical_market_id(market: str, *, registry: MarketRegistry | None = None) -> str:
    """Normalize user input and validate the resulting market against exchange catalog."""
    active_registry = registry or get_market_registry()
    user_market = parse_user_market_input(market)
    return validate_exchange_market_code(user_market, registry=active_registry)


def _as_optional_str(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise MarketCatalogError(f"market catalog field {field!r} must be string or null: type={type(value).__name__}")
    text = value.strip()
    return text or None


_CATALOG_MARKET_CODE_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")


def parse_exchange_market_catalog_code(market: object) -> str:
    if not isinstance(market, str):
        raise MarketCatalogError(f"market catalog field 'market' must be string: type={type(market).__name__}")
    if not market:
        raise MarketCatalogError("missing required field: market (empty)")
    if not _CATALOG_MARKET_CODE_PATTERN.fullmatch(market):
        raise MarketCatalogError(f"invalid market code format: {market!r}")
    return market


def _require_catalog_str_field(*, row: dict[str, object], field: str) -> str:
    if field not in row:
        raise MarketCatalogError(f"missing required field: {field}")
    value = _as_optional_str(row.get(field), field=field)
    if value is None:
        raise MarketCatalogError(f"missing required field: {field} (empty)")
    return value


def parse_market_catalog_row(row: object) -> MarketInfo:
    if not isinstance(row, dict):
        raise MarketCatalogError(f"non-object row: type={type(row).__name__}")
    if "market" not in row:
        raise MarketCatalogError("missing required field: market")
    market = parse_exchange_market_catalog_code(row.get("market"))
    korean_name = _require_catalog_str_field(row=row, field="korean_name")
    english_name = _require_catalog_str_field(row=row, field="english_name")
    return MarketInfo(market=market, korean_name=korean_name, english_name=english_name)


def parse_market_catalog_row_details(row: object) -> MarketInfo:
    if not isinstance(row, dict):
        raise MarketCatalogError(f"non-object row: type={type(row).__name__}")
    if "market" not in row:
        raise MarketCatalogError("missing required field: market")
    market = parse_exchange_market_catalog_code(row.get("market"))
    korean_name = _require_catalog_str_field(row=row, field="korean_name")
    english_name = _require_catalog_str_field(row=row, field="english_name")
    market_warning = _require_catalog_str_field(row=row, field="market_warning")
    return MarketInfo(
        market=market,
        korean_name=korean_name,
        english_name=english_name,
        market_warning=market_warning,
    )


def normalize_market_warning_value(raw_warning: object) -> str:
    token = str(raw_warning or "").strip().upper()
    if not token:
        return "UNKNOWN"
    if token == "NONE":
        return "NONE"
    if token == "CAUTION":
        return "CAUTION"
    return "UNKNOWN"


def evaluate_market_warning_policy(
    *,
    raw_warning: object,
    warning_block_states: set[str],
) -> MarketWarningPolicyDecision:
    normalized_warning = normalize_market_warning_value(raw_warning)
    if normalized_warning == "NONE":
        return MarketWarningPolicyDecision(
            normalized_warning=normalized_warning,
            is_warning_state=False,
            should_block=False,
        )
    should_block = normalized_warning in warning_block_states
    return MarketWarningPolicyDecision(
        normalized_warning=normalized_warning,
        is_warning_state=True,
        should_block=should_block,
    )


def parse_user_market_input(market: str, *, default_quote: str = "KRW") -> str:
    """User-input convenience normalization layer.

    Accepts:
    - canonical exchange id: ``KRW-BTC``
    - legacy alias: ``BTC_KRW``
    - base-only shorthand: ``BTC`` (default quote is applied)
    """
    token = str(market).strip().upper().replace(" ", "")
    if not token:
        raise UserMarketInputError("market must not be empty")

    quote = str(default_quote).strip().upper()
    if not quote:
        raise UserMarketInputError("default_quote must not be empty")

    if "-" in token:
        left, right = _split_pair(token, "-")
        return f"{left}-{right}"

    if "_" in token:
        base, quote_token = _split_pair(token, "_")
        return f"{quote_token}-{base}"

    return f"{quote}-{token}"


def parse_documented_market_code(market: str) -> str:
    token = str(market).strip().upper()
    if not token:
        raise ExchangeMarketCodeError("market must not be empty")
    if " " in token:
        raise ExchangeMarketCodeError(f"invalid exchange market code: {market!r}")
    if token.count("-") != 1:
        raise ExchangeMarketCodeError(
            f"exchange market code must use canonical QUOTE-BASE format, got {market!r}"
        )
    quote, base = _split_pair(token, "-")
    return f"{quote}-{base}"


def validate_exchange_market_code(market: str, *, registry: MarketRegistry) -> str:
    """Core exchange validation layer.

    This function validates only exchange-document market id format (QUOTE-BASE),
    and never applies implicit default quote inference.
    """
    token = parse_documented_market_code(market)
    require_supported_canonical = getattr(registry, "require_supported_canonical", None)
    if callable(require_supported_canonical):
        return require_supported_canonical(token)
    return registry.require_supported(token)


def parse_exchange_market_response_code(
    market: str,
    *,
    requested_market: str | None = None,
) -> str:
    try:
        response_market = parse_documented_market_code(market)
    except ExchangeMarketCodeError as exc:
        raise MarketContractDriftError(f"exchange response market code drift: {market!r}") from exc
    if requested_market is not None:
        expected_market = parse_documented_market_code(requested_market)
        if response_market != expected_market:
            raise MarketContractDriftError(
                "exchange response market code mismatch: "
                f"requested={expected_market!r} response={response_market!r}"
            )
    return response_market


def normalize_market_id_with_registry(market: str, *, registry: MarketRegistry, default_quote: str = "KRW") -> str:
    canonical = parse_user_market_input(market, default_quote=default_quote)
    return validate_exchange_market_code(canonical, registry=registry)


def normalize_market_id(market: str, *, default_quote: str = "KRW") -> str:
    return parse_user_market_input(market, default_quote=default_quote)


def validate_exchange_market_id(market: str, *, registry: MarketRegistry) -> str:
    return validate_exchange_market_code(market, registry=registry)


def _split_pair(token: str, separator: str) -> tuple[str, str]:
    left, right = token.split(separator, 1)
    left = left.strip().upper()
    right = right.strip().upper()
    if not left or not right:
        raise ValueError(f"invalid market format: {token!r}")
    return left, right


def canonical_to_legacy_pair(market: str) -> str:
    quote, base = _split_pair(parse_documented_market_code(market), "-")
    return f"{base}_{quote}"


def canonical_market_with_raw(market: str) -> tuple[str, str | None]:
    raw = str(market).strip()
    canonical = parse_user_market_input(raw)
    if not raw:
        return canonical, None
    if raw.upper() == canonical:
        return canonical, None
    return canonical, raw
