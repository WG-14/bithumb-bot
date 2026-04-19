from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass

PRIVATE_REQUEST_RATE_LIMIT_BUCKET = "private"
ORDER_REQUEST_RATE_LIMIT_BUCKET = "order"
ORDER_RATE_LIMIT_ENDPOINTS = {
    "/v1/order",
    "/v1/orders",
    "/v1/orders/chance",
    "/v2/order",
    "/v2/orders",
}
OFFICIAL_PRIVATE_RPS_LIMIT = 140.0
OFFICIAL_ORDER_RPS_LIMIT = 10.0


@dataclass
class BucketThrottleState:
    next_allowed_at: float = 0.0
    penalty_until: float = 0.0


class RequestThrottleCoordinator:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state_by_bucket: dict[str, BucketThrottleState] = {}

    def acquire(self, *, bucket: str, limit_per_sec: float) -> float:
        limit = float(limit_per_sec)
        if not math.isfinite(limit) or limit <= 0:
            return 0.0

        interval = 1.0 / limit
        now = time.monotonic()
        with self._lock:
            state = self._state_by_bucket.setdefault(bucket, BucketThrottleState())
            gate_until = max(state.next_allowed_at, state.penalty_until)
            wait = max(0.0, gate_until - now)
            state.next_allowed_at = max(now, gate_until) + interval
        if wait > 0:
            time.sleep(wait)
        return wait

    def penalize(self, *, bucket: str, delay_sec: float) -> None:
        normalized_delay = max(0.0, float(delay_sec))
        if normalized_delay <= 0.0:
            return
        now = time.monotonic()
        with self._lock:
            state = self._state_by_bucket.setdefault(bucket, BucketThrottleState())
            state.penalty_until = max(state.penalty_until, now + normalized_delay)


def request_bucket_for_endpoint(*, method: str, endpoint: str) -> str:
    normalized_endpoint = str(endpoint or "").split("?", 1)[0]
    if normalized_endpoint in ORDER_RATE_LIMIT_ENDPOINTS:
        return ORDER_REQUEST_RATE_LIMIT_BUCKET
    normalized_method = str(method or "").strip().upper()
    if normalized_method in {"POST", "DELETE"} and normalized_endpoint.startswith("/v2/"):
        return ORDER_REQUEST_RATE_LIMIT_BUCKET
    return PRIVATE_REQUEST_RATE_LIMIT_BUCKET
