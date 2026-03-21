from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any


def _base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def encode(payload: dict[str, Any], key: str, algorithm: str = "HS256") -> str:
    if algorithm != "HS256":
        raise ValueError(f"unsupported algorithm: {algorithm}")
    header = {"alg": algorithm, "typ": "JWT"}
    signing_input = (
        _base64url(json.dumps(header, separators=(",", ":")).encode())
        + "."
        + _base64url(json.dumps(payload, separators=(",", ":")).encode())
    )
    signature = hmac.new(key.encode(), signing_input.encode(), hashlib.sha256).digest()
    return signing_input + "." + _base64url(signature)
