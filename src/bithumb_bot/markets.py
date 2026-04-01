from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
import time
import httpx

from .public_api import PublicApiSchemaError, get_public_json


BASE_URL = "https://api.bithumb.com"


class MarketCatalogError(PublicApiSchemaError):
    """Raised when market catalog fetch/parsing fails."""


class UnsupportedMarketError(ValueError):
    """Raised when a market is not supported by the loaded market catalog."""


@dataclass(frozen=True)
class MarketInfo:
    market: str
    korean_name: str | None = None
    english_name: str | None = None
    market_warning: str | None = None


class MarketCatalogClient:
    def __init__(self, *, base_url: str = BASE_URL, timeout: float = 10.0) -> None:
        self._base_url = base_url
        self._timeout = timeout

    def fetch_markets(self, *, is_details: bool = False) -> list[MarketInfo]:
        params = {"isDetails": "true" if is_details else "false"}
        with httpx.Client(base_url=self._base_url, timeout=self._timeout) as client:
            payload = get_public_json(client, "/v1/market/all", params=params)

        if not isinstance(payload, list):
            raise MarketCatalogError(f"unexpected market catalog payload type: {type(payload).__name__}")
        if not payload:
            raise MarketCatalogError("market catalog is empty")

        items: list[MarketInfo] = []
        for row in payload:
            if not isinstance(row, dict):
                raise MarketCatalogError(f"unexpected market catalog row type: {type(row).__name__}")
            market = _require_catalog_field(row=row, field="market", required=True)
            canonical = normalize_market_id(market)

            korean_name = _require_catalog_field(
                row=row,
                field="korean_name",
                required=is_details,
            )
            english_name = _require_catalog_field(
                row=row,
                field="english_name",
                required=is_details,
            )
            market_warning = _require_catalog_field(
                row=row,
                field="market_warning",
                required=is_details,
            )
            items.append(
                MarketInfo(
                    market=canonical,
                    korean_name=korean_name,
                    english_name=english_name,
                    market_warning=market_warning,
                )
            )
        return items


class MarketRegistry:
    def __init__(self, markets: list[MarketInfo]) -> None:
        self._markets: dict[str, MarketInfo] = {m.market: m for m in markets}

    @classmethod
    def from_catalog(cls, *, client: MarketCatalogClient | None = None, is_details: bool = False) -> "MarketRegistry":
        catalog_client = client or MarketCatalogClient()
        return cls(catalog_client.fetch_markets(is_details=is_details))

    def is_supported(self, market: str) -> bool:
        canonical = normalize_market_id(market)
        return canonical in self._markets

    def require_supported(self, market: str) -> str:
        canonical = normalize_market_id(market)
        if canonical not in self._markets:
            raise UnsupportedMarketError(f"unsupported market: {market!r} (canonical={canonical})")
        return canonical

    def get(self, market: str) -> MarketInfo | None:
        canonical = normalize_market_id(market)
        return self._markets.get(canonical)


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
    return normalize_market_id_with_registry(market, registry=active_registry)


def _as_optional_str(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise MarketCatalogError(f"market catalog field {field!r} must be string or null: type={type(value).__name__}")
    text = value.strip()
    return text or None


def _require_catalog_field(*, row: dict[str, object], field: str, required: bool) -> str | None:
    if field not in row:
        if required:
            raise MarketCatalogError(f"market catalog required field missing: {field!r} row={row}")
        return None
    return _as_optional_str(row.get(field), field=field)


def normalize_market_id(market: str, *, default_quote: str = "KRW") -> str:
    """User-input convenience normalization layer.

    Accepts:
    - canonical exchange id: ``KRW-BTC``
    - legacy alias: ``BTC_KRW``
    - base-only shorthand: ``BTC`` (default quote is applied)
    """
    token = str(market).strip().upper().replace(" ", "")
    if not token:
        raise ValueError("market must not be empty")

    quote = str(default_quote).strip().upper()
    if not quote:
        raise ValueError("default_quote must not be empty")

    if "-" in token:
        left, right = _split_pair(token, "-")
        return f"{left}-{right}"

    if "_" in token:
        base, quote_token = _split_pair(token, "_")
        return f"{quote_token}-{base}"

    return f"{quote}-{token}"


def normalize_market_id_with_registry(market: str, *, registry: MarketRegistry, default_quote: str = "KRW") -> str:
    canonical = normalize_market_id(market, default_quote=default_quote)
    return registry.require_supported(canonical)


def validate_exchange_market_id(market: str, *, registry: MarketRegistry) -> str:
    """Core exchange validation layer.

    This function validates only exchange-document market id format (QUOTE-BASE),
    and never applies implicit default quote inference.
    """
    token = str(market).strip().upper().replace(" ", "")
    if not token:
        raise ValueError("market must not be empty")
    if "-" not in token:
        raise ValueError(
            f"exchange market id must be canonical QUOTE-BASE format, got {market!r}"
        )
    quote, base = _split_pair(token, "-")
    return registry.require_supported(f"{quote}-{base}")


def _split_pair(token: str, separator: str) -> tuple[str, str]:
    left, right = token.split(separator, 1)
    left = left.strip().upper()
    right = right.strip().upper()
    if not left or not right:
        raise ValueError(f"invalid market format: {token!r}")
    return left, right


def canonical_to_legacy_pair(market: str) -> str:
    quote, base = _split_pair(normalize_market_id(market), "-")
    return f"{base}_{quote}"


def canonical_market_with_raw(market: str) -> tuple[str, str | None]:
    raw = str(market).strip()
    canonical = normalize_market_id(raw)
    if not raw:
        return canonical, None
    if raw.upper() == canonical:
        return canonical, None
    return canonical, raw
