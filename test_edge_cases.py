"""
Edge case and input validation tests for RateLimiter.
Covers zero limits, zero capacities, zero refill rates, negative configurations,
invalid strategies, empty client/endpoint identifiers, and boundary conditions.
"""

import sys
from rate_limiter import RateLimiter as OriginalRateLimiter
from rate_limiter_optimized import RateLimiter as OptimizedRateLimiter


def run_zero_limit_test(limiter_cls):
    """Zero request limit immediately rejects all requests in Policy A."""
    limiter = limiter_cls(strategy="policy_a")
    try:
        allowed, remaining, reset = limiter.check(
            client_id="alice",
            endpoint="/test",
            limit=0,
            window_ms=60000,
            capacity=5,
            refill_rate=1,
        )
        assert allowed is False
        assert remaining == 0
        print(f"PASS: [{limiter_cls.__name__}] Zero request limit")
    finally:
        limiter.close()


def run_zero_capacity_test(limiter_cls):
    """Zero token capacity immediately rejects all requests in Policy B."""
    limiter = limiter_cls(strategy="policy_b")
    try:
        allowed, remaining, reset = limiter.check(
            client_id="alice",
            endpoint="/test",
            limit=5,
            window_ms=60000,
            capacity=0,
            refill_rate=1,
        )
        assert allowed is False
        assert remaining == 0
        print(f"PASS: [{limiter_cls.__name__}] Zero token capacity")
    finally:
        limiter.close()


def run_zero_refill_rate_test(limiter_cls):
    """Zero refill rate allows initial burst, then permanently rejects with sys.maxsize."""
    limiter = limiter_cls(strategy="policy_b")
    try:
        # First 2 should be allowed
        a1, r1, t1 = limiter.check("alice", "/test", 5, 1000, 2, 0.0)
        a2, r2, t2 = limiter.check("alice", "/test", 5, 1000, 2, 0.0)
        assert a1 is True and r1 == 1 and t1 == 0
        assert a2 is True and r2 == 0 and t2 == 0

        # Third should be rejected with reset_after_ms == sys.maxsize
        a3, r3, t3 = limiter.check("alice", "/test", 5, 1000, 2, 0.0)
        assert a3 is False
        assert r3 == 0
        assert t3 == sys.maxsize
        print(f"PASS: [{limiter_cls.__name__}] Zero refill rate")
    finally:
        limiter.close()


def run_negative_values_test(limiter_cls):
    """Negative limit, window, capacity, or refill_rate raise ValueError."""
    limiter_a = limiter_cls(strategy="policy_a")
    limiter_b = limiter_cls(strategy="policy_b")

    invalid_cases = [
        (limiter_a, {"limit": -1, "window_ms": 60000, "capacity": 5, "refill_rate": 1}),
        (limiter_a, {"limit": 5, "window_ms": -1000, "capacity": 5, "refill_rate": 1}),
        (limiter_b, {"limit": 5, "window_ms": 60000, "capacity": -1, "refill_rate": 1}),
        (limiter_b, {"limit": 5, "window_ms": 60000, "capacity": 5, "refill_rate": -1}),
    ]

    try:
        for limiter, config in invalid_cases:
            try:
                limiter.check(client_id="alice", endpoint="/test", **config)
            except ValueError:
                continue
            else:
                raise AssertionError(f"Expected ValueError for {config}")

        print(f"PASS: [{limiter_cls.__name__}] Negative configuration validation")
    finally:
        limiter_a.close()
        limiter_b.close()


def run_invalid_strategy_test(limiter_cls):
    """Unknown strategy string raises ValueError."""
    for invalid_strat in ["policy_c", "sliding_window", "", "UNKNOWN"]:
        try:
            limiter_cls(strategy=invalid_strat)
        except ValueError:
            continue
        else:
            raise AssertionError(f"Expected ValueError for invalid strategy: {invalid_strat}")
    print(f"PASS: [{limiter_cls.__name__}] Invalid strategy validation")


def run_invalid_cleanup_config_test(limiter_cls):
    """Non-positive cleanup_interval_seconds or idle_timeout_seconds raise ValueError."""
    for invalid_val in [0, -1, -0.5]:
        try:
            limiter_cls(strategy="policy_a", cleanup_interval_seconds=invalid_val)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Expected ValueError for cleanup_interval={invalid_val}")

        try:
            limiter_cls(strategy="policy_a", idle_timeout_seconds=invalid_val)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Expected ValueError for idle_timeout={invalid_val}")

    print(f"PASS: [{limiter_cls.__name__}] Invalid cleanup configuration validation")


def run_empty_identifiers_test(limiter_cls):
    """Empty client_id or endpoint string is tracked as an isolated key."""
    for strat in ["policy_a", "policy_b"]:
        limiter = limiter_cls(strategy=strat)
        try:
            # Empty client_id
            first = limiter.check("", "/test", 1, 60000, 1, 0)
            second = limiter.check("", "/test", 1, 60000, 1, 0)
            other_client = limiter.check("alice", "/test", 1, 60000, 1, 0)

            assert first[0] is True, "First empty-client request must be allowed"
            assert second[0] is False, "Second empty-client request must be rejected"
            assert other_client[0] is True, "Different client must not be affected by empty client"

            # Empty endpoint
            first_ep = limiter.check("bob", "", 1, 60000, 1, 0)
            second_ep = limiter.check("bob", "", 1, 60000, 1, 0)
            other_ep = limiter.check("bob", "/other", 1, 60000, 1, 0)

            assert first_ep[0] is True, "First empty-endpoint request must be allowed"
            assert second_ep[0] is False, "Second empty-endpoint request must be rejected"
            assert other_ep[0] is True, "Different endpoint must not be affected by empty endpoint"
        finally:
            limiter.close()

    print(f"PASS: [{limiter_cls.__name__}] Empty client and endpoint identifiers")


# Pytest test cases
def test_zero_limit():
    run_zero_limit_test(OriginalRateLimiter)
    run_zero_limit_test(OptimizedRateLimiter)


def test_zero_capacity():
    run_zero_capacity_test(OriginalRateLimiter)
    run_zero_capacity_test(OptimizedRateLimiter)


def test_zero_refill_rate():
    run_zero_refill_rate_test(OriginalRateLimiter)
    run_zero_refill_rate_test(OptimizedRateLimiter)


def test_negative_values():
    run_negative_values_test(OriginalRateLimiter)
    run_negative_values_test(OptimizedRateLimiter)


def test_invalid_strategy():
    run_invalid_strategy_test(OriginalRateLimiter)
    run_invalid_strategy_test(OptimizedRateLimiter)


def test_invalid_cleanup_config():
    run_invalid_cleanup_config_test(OriginalRateLimiter)
    run_invalid_cleanup_config_test(OptimizedRateLimiter)


def test_empty_identifiers():
    run_empty_identifiers_test(OriginalRateLimiter)
    run_empty_identifiers_test(OptimizedRateLimiter)


if __name__ == "__main__":
    test_zero_limit()
    test_zero_capacity()
    test_zero_refill_rate()
    test_negative_values()
    test_invalid_strategy()
    test_invalid_cleanup_config()
    test_empty_identifiers()
    print("\nALL EDGE CASE TESTS PASSED")
