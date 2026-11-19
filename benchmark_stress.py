
import math
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

from rate_limiter import RateLimiter


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


def run_once(strategy, workers, requests):
    limiter = RateLimiter(
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

        latency_ms = (time.perf_counter_ns() - start) / 1_000_000

        return latency_ms, allowed

    try:
        start_time = time.perf_counter()

        if workers == 1:
            results = [send_request(item) for item in requests]
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(send_request, requests))

        elapsed = time.perf_counter() - start_time

        latencies = [latency for latency, _ in results]
        allowed = sum(1 for _, result in results if result)
        rejected = len(results) - allowed

        expected_allowed = CLIENTS * ENDPOINTS * LIMIT
        expected_rejected = (
            CLIENTS * ENDPOINTS *
            (REQUESTS_PER_KEY - LIMIT)
        )

        assert allowed == expected_allowed, (
            f"Expected {expected_allowed} allowed, got {allowed}"
        )

        assert rejected == expected_rejected, (
            f"Expected {expected_rejected} rejected, got {rejected}"
        )

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


def benchmark_strategy(strategy, workers, requests):
    print(f"\nRunning warm-up: {strategy}, {workers} workers")

    warmup = RateLimiter(strategy=strategy)

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
        print(f"Measurement {repetition + 1}/{REPETITIONS}...")

        result = run_once(strategy, workers, requests)
        runs.append(result)

    rps = statistics.median([run["rps"] for run in runs])
    avg_latency = statistics.median(
        [run["average_ms"] for run in runs]
    )
    p95_latency = statistics.median(
        [run["p95_ms"] for run in runs]
    )

    print("\n" + "=" * 55)
    print(f"Strategy: {strategy}")
    print(f"Workers: {workers}")
    print(f"Requests: {len(requests):,}")
    print(f"Allowed: {runs[-1]['allowed']:,}")
    print(f"Rejected: {runs[-1]['rejected']:,}")
    print(f"Median throughput: {rps:,.2f} RPS")
    print(f"Median average latency: {avg_latency:.4f} ms")
    print(f"Median P95 latency: {p95_latency:.4f} ms")


if __name__ == "__main__":
    requests = create_requests()

    print("=" * 55)
    print("RATE LIMITER LARGE-SCALE STRESS TEST")
    print("=" * 55)
    print(f"Clients: {CLIENTS}")
    print(f"Endpoints per client: {ENDPOINTS}")
    print(f"Requests per key: {REQUESTS_PER_KEY:,}")
    print(f"Limit per key: {LIMIT:,}")
    print(f"Total requests per run: {len(requests):,}")
    print(f"Worker configurations: {WORKER_COUNTS}")
    print(f"Repetitions: {REPETITIONS}")

    for strategy in ("policy_a", "policy_b"):
        for workers in WORKER_COUNTS:
            benchmark_strategy(strategy, workers, requests)

    print("\nALL STRESS TESTS COMPLETED")
