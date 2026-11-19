
import random
import statistics
import time
import math
from concurrent.futures import ThreadPoolExecutor

from rate_limiter import RateLimiter


CLIENTS = 10
ENDPOINTS = 2
REQUESTS_PER_KEY = 100
LIMIT = 50

WORKER_COUNTS = [1, 5, 10, 20, 50]
REPETITIONS = 3


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
        cleanup_interval_seconds=300,
        idle_timeout_seconds=600,
    )

    latencies = []

    def send_request(request):
        client_id, endpoint = request

        start = time.perf_counter()

        result = limiter.check(
            client_id=client_id,
            endpoint=endpoint,
            limit=LIMIT,
            window_ms=60000,
            capacity=LIMIT,
            refill_rate=0,
        )

        elapsed = time.perf_counter() - start

        return elapsed * 1000, result[0]

    try:
        start_time = time.perf_counter()

        if workers == 1:
            results = [send_request(item) for item in requests]
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(send_request, requests))

        total_time = time.perf_counter() - start_time

        latencies = [item[0] for item in results]

        allowed = sum(1 for _, success in results if success)
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
            "rps": len(requests) / total_time,
            "average_ms": statistics.mean(latencies),
            "p95_ms": percentile(latencies, 95),
            "allowed": allowed,
            "rejected": rejected,
        }

    finally:
        limiter.close()


def benchmark_strategy(strategy, workers):
    requests = create_requests()

    # Warm-up with a separate limiter and separate client.
    warmup = RateLimiter(strategy=strategy)

    try:
        for _ in range(100):
            warmup.check(
                client_id="warmup",
                endpoint="/warmup",
                limit=50,
                window_ms=60000,
                capacity=50,
                refill_rate=0,
            )
    finally:
        warmup.close()

    runs = [
        run_once(strategy, workers, requests)
        for _ in range(REPETITIONS)
    ]

    rps_values = [run["rps"] for run in runs]
    latency_values = [run["average_ms"] for run in runs]
    p95_values = [run["p95_ms"] for run in runs]

    print("\n" + "=" * 55)
    print(f"Strategy: {strategy}")
    print(f"Workers: {workers}")
    print(f"Repetitions: {REPETITIONS}")
    print(f"Requests per run: {len(requests)}")
    print(f"Allowed: {runs[-1]['allowed']}")
    print(f"Rejected: {runs[-1]['rejected']}")
    print(f"Median throughput: {statistics.median(rps_values):.2f} RPS")
    print(f"Average latency: {statistics.mean(latency_values):.4f} ms")
    print(f"Median P95 latency: {statistics.median(p95_values):.4f} ms")


if __name__ == "__main__":
    print("REALISTIC RATE LIMITER BENCHMARK")
    print(f"Clients: {CLIENTS}")
    print(f"Endpoints per client: {ENDPOINTS}")
    print(f"Requests per client-endpoint: {REQUESTS_PER_KEY}")
    print(f"Limit per client-endpoint: {LIMIT}")

    for strategy in ("policy_a", "policy_b"):
        for workers in WORKER_COUNTS:
            benchmark_strategy(strategy, workers)

    print("\nALL BENCHMARK RUNS COMPLETED")
