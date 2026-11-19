
import time
import statistics
from concurrent.futures import ThreadPoolExecutor

from rate_limiter import RateLimiter


REQUESTS = 20000
WORKERS = 10


def percentile(values, percentile_value):
    sorted_values = sorted(values)
    index = max(
        0,
        int((percentile_value / 100) * len(sorted_values)) - 1
    )
    return sorted_values[index]


def run_benchmark(strategy, concurrent=False):
    limiter = RateLimiter(
        strategy=strategy,
        cleanup_interval_seconds=30,
        idle_timeout_seconds=300,
    )

    latencies = []

    def send_request(_):
        start = time.perf_counter()

        result = limiter.check(
            client_id="benchmark_client",
            endpoint="/benchmark",
            limit=REQUESTS,
            window_ms=60000,
            capacity=REQUESTS,
            refill_rate=0,
        )

        elapsed = time.perf_counter() - start
        return elapsed, result[0]

    try:
        start_time = time.perf_counter()

        if concurrent:
            with ThreadPoolExecutor(max_workers=WORKERS) as executor:
                results = list(
                    executor.map(send_request, range(REQUESTS))
                )
        else:
            results = [
                send_request(i) for i in range(REQUESTS)
            ]

        total_time = time.perf_counter() - start_time

        latencies = [
            elapsed * 1000 for elapsed, _ in results
        ]

        allowed = sum(
            1 for _, success in results if success
        )

        rejected = REQUESTS - allowed

        print("\n" + "=" * 45)
        print(f"Strategy: {strategy}")
        print(
            f"Execution: "
            f"{'Concurrent' if concurrent else 'Sequential'}"
        )
        print(f"Requests: {REQUESTS}")
        print(f"Workers: {WORKERS if concurrent else 1}")
        print(f"Allowed: {allowed}")
        print(f"Rejected: {rejected}")
        print(f"Total time: {total_time:.4f} seconds")
        print(f"Requests per second: {REQUESTS / total_time:.2f}")
        print(
            f"Average latency: "
            f"{statistics.mean(latencies):.4f} ms"
        )
        print(
            f"P95 latency: "
            f"{percentile(latencies, 95):.4f} ms"
        )
        print(f"Statistics: {limiter.stats()}")

    finally:
        limiter.close()


if __name__ == "__main__":
    for strategy in ("policy_a", "policy_b"):
        run_benchmark(strategy, concurrent=False)
        run_benchmark(strategy, concurrent=True)

    print("\nBENCHMARK COMPLETED")
