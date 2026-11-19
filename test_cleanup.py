"""
Tests for automatic cleanup, resource management, and thread shutdown.
Validates purging of expired sliding windows, safe preservation of depleted token buckets,
purging of fully restored idle token buckets, and explicit thread termination.
"""

import time
import threading
from rate_limiter import RateLimiter as OriginalRateLimiter
from rate_limiter_optimized import RateLimiter as OptimizedRateLimiter


def run_policy_a_automatic_cleanup(limiter_cls):
    """Expired sliding window entries are purged when active window elapses."""
    limiter = limiter_cls(
        strategy="policy_a",
        cleanup_interval_seconds=0.02,
        idle_timeout_seconds=0.05,
    )
    try:
        for i in range(100):
            limiter.check(
                client_id=f"client_{i}",
                endpoint="/test",
                limit=5,
                window_ms=100,
                capacity=5,
                refill_rate=1,
            )

        assert limiter.stats()["active_keys"] == 100
        time.sleep(0.25)
        assert limiter.stats()["active_keys"] == 0
        print(f"PASS: [{limiter_cls.__name__}] Policy A automatic cleanup")
    finally:
        limiter.close()


def run_policy_b_automatic_cleanup(limiter_cls):
    """Idle token buckets that have fully recovered to capacity are purged."""
    limiter = limiter_cls(
        strategy="policy_b",
        cleanup_interval_seconds=0.02,
        idle_timeout_seconds=0.05,
    )
    try:
        for i in range(100):
            limiter.check(
                client_id=f"client_{i}",
                endpoint="/test",
                limit=5,
                window_ms=100,
                capacity=2,
                refill_rate=100,
            )

        assert limiter.stats()["active_keys"] == 100
        time.sleep(0.25)
        assert limiter.stats()["active_keys"] == 0
        print(f"PASS: [{limiter_cls.__name__}] Policy B automatic cleanup")
    finally:
        limiter.close()


def run_depleted_bucket_is_not_deleted(limiter_cls):
    """Depleted or partially filled buckets must NOT be purged (prevents quota reset bypass)."""
    limiter = limiter_cls(
        strategy="policy_b",
        cleanup_interval_seconds=0.02,
        idle_timeout_seconds=0.05,
    )
    try:
        for _ in range(2):
            limiter.check(
                client_id="alice",
                endpoint="/test",
                limit=5,
                window_ms=1000,
                capacity=2,
                refill_rate=0,
            )

        assert limiter.stats()["active_keys"] == 1
        time.sleep(0.2)
        # Bucket must still exist because current_tokens (0) < capacity (2)
        assert limiter.stats()["active_keys"] == 1
        print(f"PASS: [{limiter_cls.__name__}] Depleted token bucket preserved")
    finally:
        limiter.close()


def run_cleanup_thread_shutdown_test(limiter_cls):
    """Calling close() signals the worker thread and terminates it cleanly."""
    limiter = limiter_cls(strategy="policy_a", cleanup_interval_seconds=1.0)
    worker_thread = limiter._cleanup_thread
    assert worker_thread.is_alive(), "Cleanup thread must be running initially"

    limiter.close()
    assert not worker_thread.is_alive(), "Cleanup thread must stop after close()"

    # Calling close again must be safe and idempotent
    limiter.close()
    print(f"PASS: [{limiter_cls.__name__}] Cleanup thread shutdown")


def run_context_manager_test(limiter_cls):
    """Context manager ensures close() is invoked automatically on block exit."""
    with limiter_cls(strategy="policy_b", cleanup_interval_seconds=1.0) as limiter:
        worker_thread = limiter._cleanup_thread
        assert worker_thread.is_alive()

    assert not worker_thread.is_alive(), "Cleanup thread must terminate upon context manager exit"
    print(f"PASS: [{limiter_cls.__name__}] Context manager automatic cleanup")


# Pytest entries
def test_policy_a_automatic_cleanup():
    run_policy_a_automatic_cleanup(OriginalRateLimiter)
    run_policy_a_automatic_cleanup(OptimizedRateLimiter)


def test_policy_b_automatic_cleanup():
    run_policy_b_automatic_cleanup(OriginalRateLimiter)
    run_policy_b_automatic_cleanup(OptimizedRateLimiter)


def test_depleted_bucket_is_not_deleted():
    run_depleted_bucket_is_not_deleted(OriginalRateLimiter)
    run_depleted_bucket_is_not_deleted(OptimizedRateLimiter)


def test_cleanup_thread_shutdown():
    run_cleanup_thread_shutdown_test(OriginalRateLimiter)
    run_cleanup_thread_shutdown_test(OptimizedRateLimiter)


def test_context_manager():
    run_context_manager_test(OriginalRateLimiter)
    run_context_manager_test(OptimizedRateLimiter)


if __name__ == "__main__":
    test_policy_a_automatic_cleanup()
    test_policy_b_automatic_cleanup()
    test_depleted_bucket_is_not_deleted()
    test_cleanup_thread_shutdown()
    test_context_manager()
    print("\nALL CLEANUP TESTS PASSED")
