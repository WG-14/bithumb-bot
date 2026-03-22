#!/usr/bin/env python3
"""Minimal repro for Bithumb /v2/orders authentication debugging.

This script intentionally avoids reusing the bot's internal helpers so the
request-signing path can be verified in isolation.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import time
import uuid
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlencode

import base64
import hmac
import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

DEFAULT_BASE_URL = "https://api.bithumb.com"
DEFAULT_ENDPOINT = "/v2/orders"
DEFAULT_FORM_PAIRS: tuple[tuple[str, str], ...] = (
    ("market", "KRW-BTC"),
    ("side", "bid"),
    ("price", "9999"),
    ("order_type", "price"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Minimal standalone repro for Bithumb /v2/orders auth.",
    )
    parser.add_argument(
        "--dry-print-only",
        action="store_true",
        help="Print the canonical payload, hash, and claims summary without sending the request.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="HTTP timeout in seconds when sending the request (default: 10).",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Base URL for the API (default: {DEFAULT_BASE_URL}).",
    )
    return parser.parse_args()


def build_canonical_payload(form_pairs: Iterable[tuple[str, str]]) -> str:
    return urlencode(list(form_pairs), doseq=False, safe="-._~")


def compute_query_hash(canonical_payload: str) -> str:
    return hashlib.sha512(canonical_payload.encode("utf-8")).hexdigest()


def build_claims(access_key: str, query_hash: str) -> dict[str, Any]:
    return {
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "query_hash": query_hash,
        "query_hash_alg": "SHA512",
    }


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def build_authorization_header(secret_key: str, claims: dict[str, Any]) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    encoded_header = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    encoded_claims = _b64url(json.dumps(claims, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{encoded_header}.{encoded_claims}".encode("utf-8")
    signature = hmac.new(secret_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
    token = f"{encoded_header}.{encoded_claims}.{_b64url(signature)}"
    return f"Bearer {token}"


def mask_access_key(access_key: str) -> str:
    if len(access_key) <= 8:
        return "*" * len(access_key)
    return f"{access_key[:4]}...{access_key[-4:]}"


def truncate_value(value: str, visible_prefix: int = 12, visible_suffix: int = 8) -> str:
    if len(value) <= visible_prefix + visible_suffix + 3:
        return value
    return f"{value[:visible_prefix]}...{value[-visible_suffix:]}"


def print_debug_summary(
    *,
    endpoint: str,
    content_type: str,
    canonical_payload: str,
    query_hash: str,
    claims: dict[str, Any],
    authorization_header: str,
    base_url: str,
    dry_run: bool,
) -> None:
    claims_summary = {
        "access_key_prefix": mask_access_key(str(claims["access_key"])),
        "nonce": claims["nonce"],
        "timestamp": claims["timestamp"],
        "query_hash_alg": claims["query_hash_alg"],
    }
    header_summary = {
        "Authorization": f"Bearer {truncate_value(authorization_header.removeprefix('Bearer '))}",
        "Content-Type": content_type,
    }
    print(f"endpoint={endpoint}")
    print(f"base_url={base_url}")
    print(f"dry_run={dry_run}")
    print(f"content_type={content_type}")
    print(f"canonical_payload={canonical_payload}")
    print(f"query_hash={query_hash}")
    print(f"claims_summary={claims_summary}")
    print(f"headers_summary={header_summary}")


def main() -> int:
    args = parse_args()

    access_key = os.getenv("BITHUMB_API_KEY")
    secret_key = os.getenv("BITHUMB_API_SECRET")

    if not access_key or not secret_key:
        print(
            "Missing environment variables. Set BITHUMB_API_KEY and BITHUMB_API_SECRET.",
            file=sys.stderr,
        )
        return 2

    canonical_payload = build_canonical_payload(DEFAULT_FORM_PAIRS)
    request_body = {key: value for key, value in DEFAULT_FORM_PAIRS}
    query_hash = compute_query_hash(canonical_payload)
    claims = build_claims(access_key, query_hash)
    authorization_header = build_authorization_header(secret_key, claims)
    content_type = "application/json"

    print_debug_summary(
        endpoint=DEFAULT_ENDPOINT,
        content_type=content_type,
        canonical_payload=canonical_payload,
        query_hash=query_hash,
        claims=claims,
        authorization_header=authorization_header,
        base_url=args.base_url,
        dry_run=args.dry_print_only,
    )

    if args.dry_print_only:
        return 0

    url = f"{args.base_url.rstrip('/')}{DEFAULT_ENDPOINT}"
    headers = {
        "Authorization": authorization_header,
        "Content-Type": content_type,
    }

    request = Request(
        url=url,
        data=json.dumps(request_body, separators=(",", ":")).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    try:
        with urlopen(request, timeout=args.timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            print(f"status_code={response.status}")
            print(f"response_body={body}")
            return 0
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"status_code={exc.code}")
        print(f"response_body={body}")
        return 0
    except URLError as exc:
        print(f"request_error={exc.__class__.__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
