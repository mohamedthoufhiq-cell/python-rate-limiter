
import time
from rate_limiter import RateLimiter


def test_policy_a_memory():
    limiter = RateLimiter(strategy="policy_a")

    for i in range(1000):
        limiter.check(
            client_id=f"client_{i}",
            endpoint="/test",
            limit=5,
            window_ms=100,
            capacity=5,
            refill_rate=1,
        )

    print("\nPolicy A before expiry:", limiter.stats())

    time.sleep(0.2)

    limiter.check(
        client_id="new_client",
        endpoint="/test",
        limit=5,
        window_ms=100,
        capacity=5,
        refill_rate=1,
    )

    print("Policy A after expiry:", limiter.stats())


def test_policy_b_memory():
    limiter = RateLimiter(strategy="policy_b")

    for i in range(1000):
        limiter.check(
            client_id=f"client_{i}",
            endpoint="/test",
            limit=5,
            window_ms=100,
            capacity=5,
            refill_rate=1,
        )

    print("\nPolicy B after requests:", limiter.stats())


if __name__ == "__main__":
    test_policy_a_memory()
    test_policy_b_memory()
