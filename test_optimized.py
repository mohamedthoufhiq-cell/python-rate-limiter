"""
Tests specifically validating the optimized collections.deque implementation
and ensuring 100% equivalence and contract compatibility with the original baseline.
"""

import math
import sys
import time

from rate_limiter import RateLimiter as OriginalLimiter
from rate_limiter_optimized import RateLimiter as OptimizedLimiter


def test_sliding_window():
    """Deque-based sliding window properly tracks limit, rejection, and expiry recovery."""
    limiter = OptimizedLimiter(strategy="policy_a")

    try:
        for _ in range(5):
            result = limiter.check("alice", "/test", 5, 1000, 5, 1)
            assert result[0] is True

        result = limiter.check("alice", "/test", 5, 1000, 5, 1)
        assert result[0] is False

        time.sleep(1.05)

        result = limiter.check("alice", "/test", 5, 1000, 5, 1)
        assert result[0] is True
        print("PASS: Optimized sliding window")
    finally:
        limiter.close()


def test_token_bucket():
    """Token bucket implementation in optimized module maintains identical behavior."""
    limiter = OptimizedLimiter(strategy="policy_b")

    try:
        for _ in range(5):
            result = limiter.check("alice", "/test", 5, 1000, 5, 0)
            assert result[0] is True

        result = limiter.check("alice", "/test", 5, 1000, 5, 0)
        assert result[0] is False
        print("PASS: Token bucket preserved in optimized module")
    finally:
        limiter.close()


def test_automatic_cleanup():
    """Optimized deque cleanup prunes empty collections and active keys."""
    limiter = OptimizedLimiter(
        strategy="policy_a",
        cleanup_interval_seconds=0.02,
        idle_timeout_seconds=0.05,
    )

    try:
        for i in range(100):
            limiter.check(f"client_{i}", "/test", 5, 100, 5, 1)

        assert limiter.stats()["active_keys"] == 100
        time.sleep(0.25)
        assert limiter.stats()["active_keys"] == 0
        print("PASS: Deque automatic cleanup")
    finally:
        limiter.close()


def test_equivalence_with_original_policy_a():
    """Validate that Original and Optimized yield identical decisions for Policy A."""
    original = OriginalLimiter(strategy="policy_a")
    optimized = OptimizedLimiter(strategy="policy_a")

    try:
        for i in range(15):
            res_orig = original.check("user1", "/api", limit=10, window_ms=500, capacity=0, refill_rate=0)
            res_opt = optimized.check("user1", "/api", limit=10, window_ms=500, capacity=0, refill_rate=0)

            assert res_orig[0] == res_opt[0], f"Step {i}: allowed mismatch ({res_orig[0]} vs {res_opt[0]})"
            assert res_orig[1] == res_opt[1], f"Step {i}: remaining mismatch ({res_orig[1]} vs {res_opt[1]})"
            # Reset time within 5ms of each other due to execution time
            assert abs(res_orig[2] - res_opt[2]) <= 5, f"Step {i}: reset time deviation too large"

        print("PASS: Policy A exact equivalence between Original and Optimized")
    finally:
        original.close()
        optimized.close()


def test_equivalence_with_original_policy_b():
    """Validate that Original and Optimized yield identical decisions for Policy B."""
    original = OriginalLimiter(strategy="policy_b")
    optimized = OptimizedLimiter(strategy="policy_b")

    try:
        for i in range(10):
            res_orig = original.check("user1", "/api", limit=0, window_ms=0, capacity=5, refill_rate=2.0)
            res_opt = optimized.check("user1", "/api", limit=0, window_ms=0, capacity=5, refill_rate=2.0)

            assert res_orig[0] == res_opt[0], f"Step {i}: allowed mismatch"
            assert res_orig[1] == res_opt[1], f"Step {i}: remaining mismatch"
            assert abs(res_orig[2] - res_opt[2]) <= 5, f"Step {i}: reset time deviation too large"

        print("PASS: Policy B exact equivalence between Original and Optimized")
    finally:
        original.close()
        optimized.close()


if __name__ == "__main__":
    test_sliding_window()
    test_token_bucket()
    test_automatic_cleanup()
    test_equivalence_with_original_policy_a()
    test_equivalence_with_original_policy_b()
    print("\nALL OPTIMIZED TESTS PASSED")
