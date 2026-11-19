"""
Concurrency tests for in-memory rate limiting.
Validates thread safety, atomic decision-making, and client/endpoint isolation
under simultaneous multi-threaded load.
"""

from concurrent.futures import ThreadPoolExecutor
from rate_limiter import RateLimiter as OriginalRateLimiter
from rate_limiter_optimized import RateLimiter as OptimizedRateLimiter


def run_single_key_concurrency_test(strategy: str, limiter_cls=OriginalRateLimiter):
    """Test 100 concurrent requests to a single (client, endpoint) key with limit=10."""
    limiter = limiter_cls(strategy=strategy)

    try:
        def send_request(_):
            return limiter.check(
                client_id="alice",
                endpoint="/v1/chat",
                limit=10,
                window_ms=60000,
                capacity=10,
                refill_rate=0,
            )

        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(send_request, range(100)))

        allowed = sum(1 for result in results if result[0])
        rejected = sum(1 for result in results if not result[0])
        stats = limiter.stats()

        print(f"\n[{limiter_cls.__name__}] Strategy: {strategy}")
        print(f"Allowed: {allowed}, Rejected: {rejected}")
        print(f"Statistics: {stats}")

        assert allowed == 10, f"Expected 10 allowed, got {allowed}"
        assert rejected == 90, f"Expected 90 rejected, got {rejected}"
        assert stats["total_allowed"] == 10
        assert stats["total_rejected"] == 90
        assert stats["active_keys"] == 1
    finally:
        limiter.close()


def run_multi_key_concurrency_test(strategy: str, limiter_cls=OriginalRateLimiter):
    """Test concurrent requests across multiple clients and endpoints."""
    limiter = limiter_cls(strategy=strategy)

    clients = [f"client_{i}" for i in range(5)]
    endpoints = ["/v1/chat", "/v1/complete"]
    limit_per_key = 5
    requests_per_key = 20

    tasks = []
    for client in clients:
        for ep in endpoints:
            for _ in range(requests_per_key):
                tasks.append((client, ep))

    try:
        def send_req(item):
            client, ep = item
            return limiter.check(
                client_id=client,
                endpoint=ep,
                limit=limit_per_key,
                window_ms=60000,
                capacity=limit_per_key,
                refill_rate=0,
            )

        with ThreadPoolExecutor(max_workers=25) as executor:
            results = list(executor.map(send_req, tasks))

        allowed = sum(1 for res in results if res[0])
        rejected = sum(1 for res in results if not res[0])
        total_keys = len(clients) * len(endpoints)
        expected_allowed = total_keys * limit_per_key
        expected_rejected = total_keys * (requests_per_key - limit_per_key)

        assert allowed == expected_allowed, f"Expected {expected_allowed} allowed, got {allowed}"
        assert rejected == expected_rejected, f"Expected {expected_rejected} rejected, got {rejected}"

        stats = limiter.stats()
        assert stats["total_allowed"] == expected_allowed
        assert stats["total_rejected"] == expected_rejected
        assert stats["active_keys"] == total_keys
    finally:
        limiter.close()


def test_policy_a_concurrency():
    run_single_key_concurrency_test("policy_a", OriginalRateLimiter)


def test_policy_b_concurrency():
    run_single_key_concurrency_test("policy_b", OriginalRateLimiter)


def test_policy_a_multi_key_concurrency():
    run_multi_key_concurrency_test("policy_a", OriginalRateLimiter)


def test_policy_b_multi_key_concurrency():
    run_multi_key_concurrency_test("policy_b", OriginalRateLimiter)


def test_optimized_policy_a_concurrency():
    run_single_key_concurrency_test("policy_a", OptimizedRateLimiter)


def test_optimized_policy_b_concurrency():
    run_single_key_concurrency_test("policy_b", OptimizedRateLimiter)


def test_optimized_multi_key_concurrency():
    run_multi_key_concurrency_test("policy_a", OptimizedRateLimiter)
    run_multi_key_concurrency_test("policy_b", OptimizedRateLimiter)


if __name__ == "__main__":
    test_policy_a_concurrency()
    test_policy_b_concurrency()
    test_policy_a_multi_key_concurrency()
    test_policy_b_multi_key_concurrency()
    test_optimized_policy_a_concurrency()
    test_optimized_policy_b_concurrency()
    test_optimized_multi_key_concurrency()
    print("\nALL CONCURRENCY TESTS PASSED")
