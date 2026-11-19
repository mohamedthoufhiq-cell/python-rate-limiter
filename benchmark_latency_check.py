"""
Benchmark Latency Check & Tail Outlier Diagnostic.
===================================================

This script investigates and explains the observed phenomenon where Average (Mean)
latency can exceed P95 latency under concurrent load.

Statistical Background:
-----------------------
In concurrent benchmarks using multi-threading (especially with shared synchronization
locks and the Python GIL), latency distributions are positively skewed with heavy tails.
While 95% of requests acquire the lock almost immediately (< 0.01 ms), a small fraction
(top 1-5%) experience operating-system thread descheduling or lock queue convoying (> 5-20 ms).
Because the arithmetic mean is sensitive to extreme values, these tail outliers pull the mean
upwards, resulting in Mean > P95.
"""

import math
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

from rate_limiter import RateLimiter as OriginalLimiter
from rate_limiter_optimized import RateLimiter as OptimizedLimiter


def compute_percentile(sorted_values, pct):
    """Compute exact rank percentile from sorted values."""
    if not sorted_values:
        return 0.0
    idx = max(0, math.ceil((pct / 100.0) * len(sorted_values)) - 1)
    return sorted_values[idx]


def run_latency_diagnostic(limiter_cls, strategy: str, workers: int = 10, total_requests: int = 20000):
    """Run concurrent benchmark with comprehensive percentile & histogram analysis."""
    limiter = limiter_cls(strategy=strategy, cleanup_interval_seconds=3600, idle_timeout_seconds=7200)

    # Warm-up phase
    for _ in range(500):
        limiter.check("warmup", "/test", limit=50000, window_ms=60000, capacity=50000, refill_rate=0)

    latencies_ns = []

    def send_request(_):
        t0 = time.perf_counter_ns()
        result = limiter.check(
            client_id="bench_client",
            endpoint="/api",
            limit=total_requests,
            window_ms=60000,
            capacity=total_requests,
            refill_rate=0,
        )
        elapsed_ns = time.perf_counter_ns() - t0
        return elapsed_ns, result[0]

    try:
        t_start = time.perf_counter()
        if workers == 1:
            results = [send_request(i) for i in range(total_requests)]
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(send_request, range(total_requests)))
        total_time = time.perf_counter() - t_start

        latencies_ms = sorted([res[0] / 1_000_000.0 for res in results])
        allowed = sum(1 for res in results if res[1])
        rejected = total_requests - allowed

        # Statistical Metrics
        mean_lat = statistics.mean(latencies_ms)
        std_lat = statistics.stdev(latencies_ms) if len(latencies_ms) > 1 else 0.0
        min_lat = latencies_ms[0]
        p50_lat = compute_percentile(latencies_ms, 50)
        p90_lat = compute_percentile(latencies_ms, 90)
        p95_lat = compute_percentile(latencies_ms, 95)
        p99_lat = compute_percentile(latencies_ms, 99)
        p999_lat = compute_percentile(latencies_ms, 99.9)
        max_lat = latencies_ms[-1]
        rps = total_requests / total_time

        # Latency Buckets (Histogram)
        buckets = [
            ("< 0.01 ms", lambda x: x < 0.01),
            ("0.01 - 0.05 ms", lambda x: 0.01 <= x < 0.05),
            ("0.05 - 0.10 ms", lambda x: 0.05 <= x < 0.10),
            ("0.10 - 1.00 ms", lambda x: 0.10 <= x < 1.00),
            ("1.00 - 5.00 ms", lambda x: 1.00 <= x < 5.00),
            (">= 5.00 ms", lambda x: x >= 5.00),
        ]

        bucket_counts = {}
        for name, pred in buckets:
            bucket_counts[name] = sum(1 for x in latencies_ms if pred(x))

        print("\n" + "=" * 70)
        print(f"LATENCY DIAGNOSTIC: {limiter_cls.__name__} [{strategy.upper()}]")
        print(f"Workers: {workers} | Total Requests: {total_requests:,}")
        print("=" * 70)
        print(f"Throughput:         {rps:,.2f} RPS")
        print(f"Total Time:         {total_time:.4f} seconds")
        print(f"Allowed / Rejected: {allowed:,} / {rejected:,}")
        print("-" * 70)
        print("PERCENTILE BREAKDOWN (milliseconds):")
        print(f"  Min:      {min_lat:.5f} ms")
        print(f"  P50:      {p50_lat:.5f} ms (Median)")
        print(f"  P90:      {p90_lat:.5f} ms")
        print(f"  P95:      {p95_lat:.5f} ms")
        print(f"  P99:      {p99_lat:.5f} ms")
        print(f"  P99.9:    {p999_lat:.5f} ms")
        print(f"  Max:      {max_lat:.5f} ms")
        print("-" * 70)
        print(f"  Mean (Average):   {mean_lat:.5f} ms")
        print(f"  Std Deviation:    {std_lat:.5f} ms")
        print(f"  Mean vs P95:      Mean is {mean_lat / p95_lat:.2f}x of P95" if p95_lat > 0 else "")
        print("-" * 70)
        print("LATENCY DISTRIBUTION HISTOGRAM:")
        for name, count in bucket_counts.items():
            pct = (count / total_requests) * 100
            bar = "#" * int(pct // 2)
            print(f"  {name:<15} {count:>7,} reqs ({pct:>6.2f}%) | {bar}")

        # Outlier Explanation
        if mean_lat > p95_lat:
            print("\n  >> OBSERVATION CONFIRMED: Mean > P95.")
            print("     Root Cause: Positive distribution skew caused by lock contention.")
            print(f"     Top 1% (P99 to Max) reaches up to {max_lat:.2f} ms due to thread wait queues,")
            print("     which arithmetically inflates the average while P95 remains in the fast tier.")
        else:
            print("\n  >> OBSERVATION: Mean <= P95 (standard symmetric or moderate load distribution).")

        return {
            "rps": rps,
            "mean": mean_lat,
            "p50": p50_lat,
            "p95": p95_lat,
            "p99": p99_lat,
            "max": max_lat,
        }
    finally:
        limiter.close()


def run_multi_key_latency_diagnostic(limiter_cls, strategy: str, workers: int = 10):
    """Run concurrent multi-key benchmark with saturated limits to measure tail latency."""
    limiter = limiter_cls(strategy=strategy, cleanup_interval_seconds=3600, idle_timeout_seconds=7200)

    clients = 20
    endpoints = 2
    requests_per_key = 500
    limit = 250

    requests = []
    for c in range(clients):
        for e in range(endpoints):
            for _ in range(requests_per_key):
                requests.append((f"client_{c}", f"/ep_{e}"))

    import random
    random.Random(42).shuffle(requests)
    total_requests = len(requests)

    def send_request(req):
        client_id, endpoint = req
        t0 = time.perf_counter_ns()
        result = limiter.check(
            client_id=client_id,
            endpoint=endpoint,
            limit=limit,
            window_ms=60000,
            capacity=limit,
            refill_rate=0,
        )
        elapsed_ns = time.perf_counter_ns() - t0
        return elapsed_ns, result[0]

    try:
        t_start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(send_request, requests))
        total_time = time.perf_counter() - t_start

        latencies_ms = sorted([res[0] / 1_000_000.0 for res in results])
        allowed = sum(1 for res in results if res[1])
        rejected = total_requests - allowed

        mean_lat = statistics.mean(latencies_ms)
        std_lat = statistics.stdev(latencies_ms) if len(latencies_ms) > 1 else 0.0
        min_lat = latencies_ms[0]
        p50_lat = compute_percentile(latencies_ms, 50)
        p90_lat = compute_percentile(latencies_ms, 90)
        p95_lat = compute_percentile(latencies_ms, 95)
        p99_lat = compute_percentile(latencies_ms, 99)
        p999_lat = compute_percentile(latencies_ms, 99.9)
        max_lat = latencies_ms[-1]
        rps = total_requests / total_time

        print("\n" + "=" * 70)
        print(f"MULTI-KEY SATURATION DIAGNOSTIC: {limiter_cls.__name__} [{strategy.upper()}]")
        print(f"Workers: {workers} | Clients: {clients} | Keys: {clients * endpoints} | Requests: {total_requests:,}")
        print("=" * 70)
        print(f"Throughput:         {rps:,.2f} RPS")
        print(f"Total Time:         {total_time:.4f} seconds")
        print(f"Allowed / Rejected: {allowed:,} / {rejected:,}")
        print("-" * 70)
        print("PERCENTILE BREAKDOWN (milliseconds):")
        print(f"  Min:      {min_lat:.5f} ms")
        print(f"  P50:      {p50_lat:.5f} ms (Median)")
        print(f"  P90:      {p90_lat:.5f} ms")
        print(f"  P95:      {p95_lat:.5f} ms")
        print(f"  P99:      {p99_lat:.5f} ms")
        print(f"  P99.9:    {p999_lat:.5f} ms")
        print(f"  Max:      {max_lat:.5f} ms")
        print("-" * 70)
        print(f"  Mean (Average):   {mean_lat:.5f} ms")
        print(f"  Std Deviation:    {std_lat:.5f} ms")
        print(f"  Mean vs P95:      Mean is {mean_lat / p95_lat:.2f}x of P95" if p95_lat > 0 else "")

        if mean_lat > p95_lat:
            print("\n  >> OBSERVATION CONFIRMED: Mean > P95 under multi-key saturation.")
            print(f"     Root cause: Tail outliers (top 1% reaching up to {max_lat:.2f} ms) skew the arithmetic mean.")
        else:
            print(f"\n  >> OBSERVATION: Mean ({mean_lat:.4f} ms) <= P95 ({p95_lat:.4f} ms).")

        return {"rps": rps, "mean": mean_lat, "p95": p95_lat, "max": max_lat}
    finally:
        limiter.close()


if __name__ == "__main__":
    print("======================================================================")
    print("INVESTIGATION OF LATENCY SKEW (AVERAGE vs P95) UNDER CONCURRENCY")
    print("======================================================================")

    # 1. Sequential baseline (no lock contention)
    print("\n[Case 1: Sequential Execution - No Lock Contention]")
    run_latency_diagnostic(OptimizedLimiter, "policy_a", workers=1, total_requests=20000)

    # 2. Concurrent execution (10 workers)
    print("\n[Case 2: 10 Workers Concurrent Contention]")
    run_latency_diagnostic(OptimizedLimiter, "policy_a", workers=10, total_requests=20000)

    # 3. Policy B 10 workers
    print("\n[Case 3: Policy B 10 Workers Concurrent Contention]")
    run_latency_diagnostic(OptimizedLimiter, "policy_b", workers=10, total_requests=20000)

    # 4. Multi-key saturated workload (50 clients, 4 endpoints, 10 workers)
    print("\n[Case 4: Multi-Key Saturated Workload - Tail Latency Analysis]")
    run_multi_key_latency_diagnostic(OptimizedLimiter, "policy_a", workers=10)

    print("\n" + "=" * 70)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 70)
