from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
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
            market = row.get("market")
            if not isinstance(market, str) or not market.strip():
                raise MarketCatalogError(f"market key missing in catalog row: {row}")
            canonical = normalize_market_id(market)
            items.append(
                MarketInfo(
                    market=canonical,
                    korean_name=_as_optional_str(row.get("korean_name"), field="korean_name"),
                    english_name=_as_optional_str(row.get("english_name"), field="english_name"),
                    market_warning=_as_optional_str(row.get("market_warning"), field="market_warning"),
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
_market_registry_cache: MarketRegistry | None = None


def get_market_registry(*, refresh: bool = False, client: MarketCatalogClient | None = None) -> MarketRegistry:
    """Return cached market registry loaded from /v1/market/all."""
    global _market_registry_cache
    if refresh:
        registry = MarketRegistry.from_catalog(client=client)
        with _market_registry_lock:
            _market_registry_cache = registry
        return registry

    with _market_registry_lock:
        cached = _market_registry_cache
    if cached is not None:
        return cached

    registry = MarketRegistry.from_catalog(client=client)
    with _market_registry_lock:
        if _market_registry_cache is None:
            _market_registry_cache = registry
        return _market_registry_cache


def canonical_market_id(market: str, *, registry: MarketRegistry | None = None) -> str:
    """Normalize and validate market against exchange catalog."""
    active_registry = registry or get_market_registry()
    return normalize_market_id_with_registry(market, registry=active_registry)


def _as_optional_str(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise MarketCatalogError(f"market catalog field {field!r} must be string or null: type={type(value).__name__}")
    text = value.strip()
    return text or None


def normalize_market_id(market: str, *, default_quote: str = "KRW") -> str:
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
