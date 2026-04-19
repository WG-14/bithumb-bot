from __future__ import annotations

from bithumb_bot.broker.bithumb_throttle import (
    ORDER_REQUEST_RATE_LIMIT_BUCKET,
    PRIVATE_REQUEST_RATE_LIMIT_BUCKET,
    RequestThrottleCoordinator,
    request_bucket_for_endpoint,
)


def test_request_bucket_for_endpoint_routes_order_endpoints() -> None:
    assert request_bucket_for_endpoint(method="POST", endpoint="/v2/orders") == ORDER_REQUEST_RATE_LIMIT_BUCKET
    assert request_bucket_for_endpoint(method="GET", endpoint="/v1/orders/chance") == ORDER_REQUEST_RATE_LIMIT_BUCKET
    assert request_bucket_for_endpoint(method="GET", endpoint="/v1/accounts") == PRIVATE_REQUEST_RATE_LIMIT_BUCKET


def test_request_throttle_coordinator_penalty_extends_wait() -> None:
    coordinator = RequestThrottleCoordinator()

    first_wait = coordinator.acquire(bucket="order", limit_per_sec=1000.0)
    coordinator.penalize(bucket="order", delay_sec=0.02)
    second_wait = coordinator.acquire(bucket="order", limit_per_sec=1000.0)

    assert first_wait == 0.0
    assert second_wait > 0.0
