
import math
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

from rate_limiter import RateLimiter as OriginalLimiter
from rate_limiter_optimized import RateLimiter as OptimizedLimiter


CLIENTS = 50
ENDPOINTS = 4
REQUESTS_PER_KEY = 1000
LIMIT = 500

WORKER_COUNTS = [1, 10, 50]
REPETITIONS = 2


def percentile(values, percentage):
    ordered = sorted(values)
    index = math.ceil((percentage / 100) * len(ordered)) - 1
    return ordered[max(0, index)]


def create_requests():
    requests = []

    for client in range(CLIENTS):
        for endpoint in range(ENDPOINTS):
            for _ in range(REQUESTS_PER_KEY):
                requests.append((
                    f"client_{client}",
                    f"/endpoint_{endpoint}",
                ))

    random.Random(42).shuffle(requests)
    return requests


def run_once(limiter_class, strategy, workers, requests):
    limiter = limiter_class(
        strategy=strategy,
        cleanup_interval_seconds=3600,
        idle_timeout_seconds=7200,
    )

    def send_request(request):
        client_id, endpoint = request

        start = time.perf_counter_ns()

        allowed, remaining, reset_after = limiter.check(
            client_id=client_id,
            endpoint=endpoint,
            limit=LIMIT,
            window_ms=60000,
            capacity=LIMIT,
            refill_rate=0,
        )

        latency = (
            time.perf_counter_ns() - start
        ) / 1_000_000

        return latency, allowed

    try:
        start_time = time.perf_counter()

        if workers == 1:
            results = [
                send_request(item) for item in requests
            ]
        else:
            with ThreadPoolExecutor(
                max_workers=workers
            ) as executor:
                results = list(
                    executor.map(send_request, requests)
                )

        elapsed = time.perf_counter() - start_time

        latencies = [item[0] for item in results]
        allowed = sum(1 for _, result in results if result)
        rejected = len(results) - allowed

        expected_allowed = CLIENTS * ENDPOINTS * LIMIT
        expected_rejected = (
            CLIENTS * ENDPOINTS *
            (REQUESTS_PER_KEY - LIMIT)
        )

        assert allowed == expected_allowed
        assert rejected == expected_rejected

        stats = limiter.stats()

        assert stats["total_allowed"] == allowed
        assert stats["total_rejected"] == rejected

        return {
            "rps": len(requests) / elapsed,
            "average_ms": statistics.mean(latencies),
            "p95_ms": percentile(latencies, 95),
            "allowed": allowed,
            "rejected": rejected,
        }

    finally:
        limiter.close()


def benchmark(name, limiter_class, strategy, workers, requests):
    print(f"\nRunning: {name} | Workers: {workers}")

    # Warm-up using a separate limiter.
    warmup = limiter_class(strategy=strategy)

    try:
        for _ in range(1000):
            warmup.check(
                client_id="warmup",
                endpoint="/warmup",
                limit=500,
                window_ms=60000,
                capacity=500,
                refill_rate=0,
            )
    finally:
        warmup.close()

    runs = []

    for repetition in range(REPETITIONS):
        print(f"Measurement {repetition + 1}/{REPETITIONS}")

        runs.append(
            run_once(
                limiter_class,
                strategy,
                workers,
                requests,
            )
        )

    median_rps = statistics.median(
        [run["rps"] for run in runs]
    )

    median_average = statistics.median(
        [run["average_ms"] for run in runs]
    )

    median_p95 = statistics.median(
        [run["p95_ms"] for run in runs]
    )

    return {
        "name": name,
        "workers": workers,
        "rps": median_rps,
        "average_ms": median_average,
        "p95_ms": median_p95,
        "allowed": runs[-1]["allowed"],
        "rejected": runs[-1]["rejected"],
    }


if __name__ == "__main__":

    requests = create_requests()

    configurations = [
        ("Original Policy A", OriginalLimiter, "policy_a"),
        ("Optimized Policy A", OptimizedLimiter, "policy_a"),
        ("Original Policy B", OriginalLimiter, "policy_b"),
    ]

    all_results = []

    print("=" * 65)
    print("RATE LIMITER BEFORE-AND-AFTER COMPARISON")
    print("=" * 65)

    print(f"Total requests: {len(requests):,}")
    print(f"Clients: {CLIENTS}")
    print(f"Endpoints per client: {ENDPOINTS}")
    print(f"Requests per key: {REQUESTS_PER_KEY:,}")
    print(f"Limit per key: {LIMIT}")
    print(f"Worker counts: {WORKER_COUNTS}")
    print(f"Repetitions: {REPETITIONS}")

    for name, limiter_class, strategy in configurations:
        for workers in WORKER_COUNTS:

            result = benchmark(
                name,
                limiter_class,
                strategy,
                workers,
                requests,
            )

            all_results.append(result)

    print("\n" + "=" * 90)
    print("FINAL COMPARISON")
    print("=" * 90)

    print(
        f"{'Implementation':<22}"
        f"{'Workers':>9}"
        f"{'RPS':>15}"
        f"{'Avg ms':>12}"
        f"{'P95 ms':>12}"
    )

    for result in all_results:
        print(
            f"{result['name']:<22}"
            f"{result['workers']:>9}"
            f"{result['rps']:>15,.2f}"
            f"{result['average_ms']:>12.4f}"
            f"{result['p95_ms']:>12.4f}"
        )

    print("\nCORRECTNESS SUMMARY")

    for result in all_results:
        print(
            f"{result['name']} | "
            f"{result['workers']} workers | "
            f"Allowed: {result['allowed']:,} | "
            f"Rejected: {result['rejected']:,}"
        )

    print("\nALL COMPARISON TESTS COMPLETED")
