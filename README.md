# High-Performance In-Memory Rate Limiter for LLM API Gateway

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-40%20Passed-brightgreen.svg)]()
[![AsyncIO](https://img.shields.io/badge/Framework-aiohttp-orange.svg)](https://docs.aiohttp.org/)

A production-grade, thread-safe, high-throughput in-memory rate limiter engineered for Large Language Model (LLM) API Gateways. The system protects expensive inference backends against denial-of-service, abusive request bursts, and noisy-neighbor quota exhaustion while guaranteeing isolation across distinct clients and endpoint paths.

---

## Table of Contents

1. [Project Title & Overview](#1-project-title--overview)
2. [Problem Statement](#2-problem-statement)
3. [Objectives](#3-objectives)
4. [Features](#4-features)
5. [Rate-Limiting Fundamentals](#5-rate-limiting-fundamentals)
6. [Sliding Window Algorithm (Policy A)](#6-sliding-window-algorithm-policy-a)
7. [Token Bucket Algorithm (Policy B)](#7-token-bucket-algorithm-policy-b)
8. [Architecture Overview & Flow Diagrams](#8-architecture-overview--flow-diagrams)
9. [Project Structure](#9-project-structure)
10. [Installation Instructions](#10-installation-instructions)
11. [Virtual Environment Setup](#11-virtual-environment-setup)
12. [How to Run the Project](#12-how-to-run-the-project)
13. [API Usage Examples](#13-api-usage-examples)
14. [Configuration Instructions](#14-configuration-instructions)
15. [Testing Instructions](#15-testing-instructions)
16. [Benchmark Instructions](#16-benchmark-instructions)
17. [Performance Comparison & Benchmark Results](#17-performance-comparison--benchmark-results)
18. [Complexity Analysis](#18-complexity-analysis)
19. [Memory Considerations & Garbage Collection](#19-memory-considerations--garbage-collection)
20. [Limitations](#20-limitations)
21. [Future Improvements & Distributed Roadmap](#21-future-improvements--distributed-roadmap)
22. [Technologies Used](#22-technologies-used)

---

## 1. Project Title & Overview

**Project Title:** In-Memory Multi-Policy Rate Limiter for an LLM API Gateway  
**Core Module:** `rate_limiter.py` (baseline) and `rate_limiter_optimized.py` (optimized)  
**API Service:** `server.py` (async HTTP API Gateway built on `aiohttp`)

Modern LLM API gateways (handling endpoints such as `/v1/chat` and `/v1/complete`) must enforce strict request and throughput policies. Because LLM generation is computationally expensive and bounded by upstream accelerator (GPU/TPU) capacity, unconstrained bursts can trigger cascade failures. This project provides dual rate-limiting algorithms with dynamic runtime strategy selection, nanosecond-precision thread synchronization, and automated memory recycling.

---

## 2. Problem Statement

In a multi-tenant LLM gateway, several client applications share common inference pipelines. Without rate limiting:
- A single client can monopolize server threads, starving other tenants.
- Bursts of high-concurrency requests degrade response latencies across the cluster.
- In-memory state tracking can leak unbounded memory as new client identifiers appear.
- Naive algorithm implementations ($O(N)$ list filtering on every incoming check) create severe CPU bottlenecks, dropping gateway throughput from hundreds of thousands of requests per second to barely a few thousand.

---

## 3. Objectives

- **Algorithmic Correctness:** Faithfully implement **Sliding Window Log (Policy A)** and **Token Bucket (Policy B)** with strict compliance to quota semantics and exact millisecond recovery calculations.
- **Client & Endpoint Isolation:** Ensure rate limits are partitioned per `(client_id, endpoint)` tuple.
- **Thread Safety:** Guarantee atomic state checks and counter telemetry under concurrent multi-threaded execution.
- **Performance Optimization:** Eliminate algorithmic bottlenecks by migrating timestamp expiration from $O(N)$ full-list comprehension to $O(1)$ amortized `collections.deque` pop-left mechanics, increasing single-worker throughput by **8.9x** (from 18,409 to 163,614 RPS).
- **Resource Management:** Provide background cleanup threads to safely reclaim stale keys without resetting active quotas prematurely.
- **Production-Ready Gateway:** Deliver a lightweight HTTP API exposing standard `X-RateLimit-*` headers, payload validation, and aggregate telemetry.

---

## 4. Features

- **Dual Rate Limiting Policies:** Choose between Sliding Window Log and Token Bucket via configuration.
- **Atomic Operations:** Thread-safe state mutation and aggregate counter synchronization.
- **Low-Latency Deque Optimization:** Front-eviction mechanics achieving sub-microsecond decision latencies.
- **Automatic Background Cleanup:** Periodic reaper thread removes expired sliding windows and fully recovered idle token buckets while preserving active restrictions.
- **Standardized HTTP Headers:** Returns `X-RateLimit-Remaining`, `X-RateLimit-Reset-Ms`, and `X-RateLimit-Policy` on every response.
- **Resilient Payload Handling:** Safe JSON body validation returning HTTP 400 on malformed payloads without crashing the gateway.
- **Context Manager Support:** Clean resource lifecycle management with Python `with` statement support.
- **Zero Heavy External Dependencies:** Core limiting engine is built strictly using the Python standard library.

---

## 5. Rate-Limiting Fundamentals

Rate limiting throttles the frequency of operations performed by a client. Every check evaluates:
1. **Subject Key:** `(client_id, endpoint)`
2. **Quota Parameters:** Maximum allowed capacity/requests and time window/refill rate.
3. **Decision Return Tuple:** `(allowed: bool, remaining: int, reset_after_ms: int)`

- `allowed`: `True` if the request is permitted; `False` if rejected.
- `remaining`: Number of additional immediate requests permitted after the current decision. If rejected, must be `0`.
- `reset_after_ms`: Time in milliseconds until quota is recovered. For Policy A, time until the oldest counted request expires. For Policy B, `0` if accepted, or wait time until 1 full token refilled if rejected.

---

## 6. Sliding Window Algorithm (Policy A)

Policy A restricts requests to at most `limit` within the last `window_ms` milliseconds.

### Mechanics
1. **Clock Reading:** Read high-precision monotonic time `now = time.monotonic()`.
2. **Eviction:** Discard timestamps older than `now - window_seconds`.
3. **Decision:** If remaining timestamps `< limit`, record `now` and grant access. Otherwise, reject.
4. **Reset Time:** Calculated as `(oldest_timestamp + window_seconds - now) * 1000`.

### Original vs. Optimized Implementation
- **Original (`rate_limiter.py`):** Uses list comprehension:
  ```python
  timestamps[:] = [t for t in timestamps if now - t < window_seconds]
  ```
  Every request scans the entire list of size $N$, incurring $O(N)$ time per check and $O(N^2)$ during traffic bursts.
- **Optimized (`rate_limiter_optimized.py`):** Uses `collections.deque`:
  ```python
  while timestamps and now - timestamps[0] >= window_seconds:
      timestamps.popleft()
  ```
  Because timestamps are chronologically ordered, expired entries are always at the front. Eviction is $O(1)$ amortized per request.

---

## 7. Token Bucket Algorithm (Policy B)

Policy B grants an initial burst allowance of `capacity` tokens, replenishing continuously at `refill_rate` tokens per second.

### Mechanics
1. **Bucket Initialization:** A new `(client_id, endpoint)` key begins with `capacity` tokens.
2. **Continuous Refill:** On subsequent calls:
   $$\text{tokens} = \min(\text{capacity}, \text{tokens} + \Delta t \times \text{refill\_rate})$$
   where $\Delta t = \text{now} - \text{last\_refill}$.
3. **Consumption:** If $\text{tokens} \ge 1$, deduct 1 token, allow request, and return `reset_after_ms = 0`.
4. **Rejection:** If $\text{tokens} < 1$, reject, return `remaining = 0`, and calculate:
   $$\text{reset\_after\_ms} = \left\lceil \frac{1 - \text{tokens}}{\text{refill\_rate}} \times 1000 \right\rceil$$
   If $\text{refill\_rate} = 0$, return `sys.maxsize`.

---

## 8. Architecture Overview & Flow Diagrams

### System Architecture

```mermaid
flowchart TD
    Client[HTTP Client / LLM Consumer] -->|POST /v1/chat\nX-Client-ID: alice| Gateway[aiohttp Gateway Server]
    Gateway --> RateCheck[limiter.check]
    
    subgraph RateLimiter Engine
        RateCheck --> Lock[threading.Lock Acquisition]
        Lock --> StrategyBranch{Active Strategy?}
        StrategyBranch -->|policy_a| SlidingWin[Sliding Window Engine\ncollections.deque]
        StrategyBranch -->|policy_b| TokenBkt[Token Bucket Engine\nContinuous Refill]
        
        SlidingWin --> UpdateCounters[Update stats & counters]
        TokenBkt --> UpdateCounters
        UpdateCounters --> ReleaseLock[Release threading.Lock]
    end
    
    ReleaseLock -->|Decision Allowed| Response200[HTTP 200 OK\nX-RateLimit-Remaining\nX-RateLimit-Reset-Ms]
    ReleaseLock -->|Decision Rejected| Response429[HTTP 429 Too Many Requests\nX-RateLimit-Remaining: 0\nX-RateLimit-Reset-Ms]
    
    subgraph Background Maintenance
        CleanupThread[Cleanup Daemon Thread] -->|Periodically| ReapExpired[Prune Expired Windows & Idle Buckets]
    end
```

### Sliding Window Decision Flow (Policy A)

```mermaid
flowchart TD
    StartA([Incoming Request]) --> ReadClock[Read time.monotonic]
    ReadClock --> DequePrune[Pop timestamps from front where age >= window]
    DequePrune --> CheckCap{len timestamps < limit?}
    CheckCap -->|Yes| AppendNow[Append current timestamp to deque]
    AppendNow --> CalcRemainA[remaining = limit - len timestamps]
    CalcRemainA --> CalcResetA[reset_ms = oldest_ts + window - now]
    CalcResetA --> AllowA([Return: Allowed=True])
    CheckCap -->|No| RejectA[remaining = 0]
    RejectA --> CalcResetA2[reset_ms = oldest_ts + window - now]
    CalcResetA2 --> BlockA([Return: Allowed=False])
```

### Token Bucket Decision Flow (Policy B)

```mermaid
flowchart TD
    StartB([Incoming Request]) --> ReadClockB[Read time.monotonic]
    ReadClockB --> RefillTokens[tokens = min capacity, tokens + elapsed * refill_rate]
    RefillTokens --> CheckTokens{tokens >= 1.0?}
    CheckTokens -->|Yes| DeductToken[tokens = tokens - 1.0]
    DeductToken --> ReturnAllowB[reset_ms = 0\nremaining = floor tokens]
    ReturnAllowB --> AllowB([Return: Allowed=True])
    CheckTokens -->|No| CalcWait[wait = 1.0 - tokens / refill_rate]
    CalcWait --> ReturnBlockB[reset_ms = ceil wait * 1000\nremaining = 0]
    ReturnBlockB --> BlockB([Return: Allowed=False])
```

---

## 9. Project Structure

```text
python-rate-limiter-main/
│
├── rate_limiter.py              # Baseline RateLimiter (list-based Sliding Window)
├── rate_limiter_optimized.py    # Optimized RateLimiter (deque-based Sliding Window)
├── server.py                    # Production-grade aiohttp HTTP Gateway
├── test_rate_limiter.py         # Official public functional unit tests
├── test_optimized.py            # Optimization validation & regression tests
├── test_concurrency.py          # Multi-threaded thread-safety tests
├── test_edge_cases.py           # Zero, negative, invalid, and boundary tests
├── test_cleanup.py              # Automatic reaper and thread shutdown tests
├── test_memory.py               # Memory retention and key pruning tests
├── test_http_api.py             # Full HTTP API test suite (status codes, headers, payloads)
├── test_client.py               # Live integration client smoke test
├── benchmark.py                 # Core latency and throughput benchmark
├── benchmark_comparison.py      # Comparative benchmark: Original vs. Optimized
├── benchmark_realistic.py       # Realistic multi-client benchmark
├── benchmark_stress.py          # Large-scale multi-worker stress benchmark
├── benchmark_latency_check.py   # Latency percentile & tail-outlier diagnostic
├── requirements.txt             # Python dependencies
├── REFLECTION.essay             # In-depth architectural analysis on locking and distributed scale
├── README.md                    # Comprehensive technical documentation
├── PROJECT_REPORT.md            # Formal engineering project completion report
└── .gitignore                   # GitHub-ready exclusion rules
```

---

## 10. Installation Instructions

### Prerequisites
- Python 3.10+ (Tested on Python 3.14.6)
- Git

Clone or open the repository:
```bash
cd python-rate-limiter-main
```

---

## 11. Virtual Environment Setup

Create and activate a virtual environment:

### On Windows (PowerShell)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### On Linux / macOS
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 12. How to Run the Project

### Starting the HTTP Server with Policy A (Sliding Window)
```bash
# Windows PowerShell
$env:STRATEGY="policy_a"; python server.py

# Linux / macOS
STRATEGY=policy_a python3 server.py
```

### Starting the HTTP Server with Policy B (Token Bucket)
```bash
# Windows PowerShell
$env:STRATEGY="policy_b"; python server.py

# Linux / macOS
STRATEGY=policy_b python3 server.py
```

### Selecting the Optimized Engine in the Server
```bash
# Windows PowerShell
$env:STRATEGY="policy_a"; $env:RATE_LIMITER_IMPL="optimized"; python server.py

# Linux / macOS
STRATEGY=policy_a RATE_LIMITER_IMPL=optimized python3 server.py
```

---

## 13. API Usage Examples

### 1. Successful Completion Request
```bash
curl -X POST http://localhost:8080/v1/chat \
  -H "X-Client-ID: alice" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Explain quantum computing in simple terms."}'
```
**Response (HTTP 200):**
```json
{
  "reply": "Hello from /v1/chat",
  "client": "alice",
  "policy": "policy_a"
}
```
**Response Headers:**
```text
X-RateLimit-Remaining: 4
X-RateLimit-Reset-Ms: 10000
X-RateLimit-Policy: policy_a
```

### 2. Rate Limited Request (Quota Exhausted)
```bash
curl -i -X POST http://localhost:8080/v1/chat -H "X-Client-ID: alice"
```
**Response (HTTP 429 Too Many Requests):**
```text
HTTP/1.1 429 Too Many Requests
X-RateLimit-Remaining: 0
X-RateLimit-Reset-Ms: 6420
X-RateLimit-Policy: policy_a
Content-Type: text/plain; charset=utf-8

Rate limit exceeded
```

### 3. Gateway Statistics
```bash
curl -X GET http://localhost:8080/stats
```
**Response (HTTP 200):**
```json
{
  "strategy": "policy_a",
  "total_allowed": 5,
  "total_rejected": 1,
  "active_keys": 1
}
```

---

## 14. Configuration Instructions

The rate limiter can be configured programmatically or via environment variables:

| Variable | Description | Default | Supported Values |
|---|---|---|---|
| `STRATEGY` | Active rate limiting algorithm | `policy_a` | `policy_a`, `policy_b` |
| `RATE_LIMITER_IMPL` | Backend implementation class | `original` | `original`, `optimized` |
| `PORT` | HTTP Server port | `8080` | Any valid TCP port |
| `HOST` | HTTP Server binding interface | `0.0.0.0` | IP Address / Hostname |

### Programmatic Configuration
```python
from rate_limiter_optimized import RateLimiter

limiter = RateLimiter(
    strategy="policy_a",
    cleanup_interval_seconds=30.0,
    idle_timeout_seconds=300.0,
)
```

---

## 15. Testing Instructions

The test suite contains **40 comprehensive automated tests** covering functional correctness, edge cases, concurrency, cleanup lifecycles, regression equivalence, and HTTP gateway integration.

### Run All Tests via pytest
```bash
pytest -v
```

### Run Tests Individually
```bash
python test_rate_limiter.py   # Public unit tests
python test_concurrency.py    # Concurrency and thread safety
python test_edge_cases.py     # Edge cases, zero/negative inputs
python test_cleanup.py        # Automatic cleanup and thread shutdown
python test_optimized.py      # Optimized implementation tests
python test_http_api.py       # Full HTTP API test suite
python test_client.py         # Live HTTP server smoke test
```

---

## 16. Benchmark Instructions

Run the benchmarking suites to verify throughput, percentiles, and scalability:

```bash
# Core sequential and concurrent benchmark
python benchmark.py

# Before-and-after comparison between Original and Optimized implementations
python benchmark_comparison.py

# Realistic multi-client, multi-endpoint benchmark
python benchmark_realistic.py

# Large-scale multi-worker stress test
python benchmark_stress.py

# Latency percentile breakdown and tail-outlier diagnostic
python benchmark_latency_check.py
```

---

## 17. Performance Comparison & Benchmark Results

All benchmarks were executed on an actual system (Windows x64, Python 3.14.6) under identical conditions with **200,000 requests** across 50 clients and 4 endpoints.

### Verified Benchmark Comparison (`benchmark_comparison.py`)

| Implementation | Workers | Throughput (RPS) | Average Latency | P95 Latency | Speedup vs Baseline |
|---|:---:|:---:|:---:|:---:|:---:|
| **Original Policy A** (list) | 1 | 18,409.59 RPS | 0.0533 ms | 0.1111 ms | 1.0x (Baseline) |
| **Original Policy A** (list) | 10 | 6,892.03 RPS | 1.4170 ms | 2.2658 ms | 1.0x (Baseline) |
| **Original Policy A** (list) | 50 | 5,613.46 RPS | 8.8909 ms | 12.4659 ms | 1.0x (Baseline) |
| **Optimized Policy A** (deque) | 1 | **163,614.65 RPS** | **0.0050 ms** | **0.0072 ms** | **8.89x Faster** |
| **Optimized Policy A** (deque) | 10 | **37,611.94 RPS** | **0.0960 ms** | **0.0069 ms** | **5.46x Faster** |
| **Optimized Policy A** (deque) | 50 | **12,281.00 RPS** | **3.8354 ms** | **4.5791 ms** | **2.19x Faster** |
| **Original Policy B** (bucket) | 1 | 240,166.63 RPS | 0.0033 ms | 0.0057 ms | — |
| **Original Policy B** (bucket) | 10 | 31,812.94 RPS | 0.2320 ms | 0.0066 ms | — |
| **Original Policy B** (bucket) | 50 | 11,399.78 RPS | 4.2353 ms | 4.6203 ms | — |

### Correctness Validation
In all runs, exactly **100,000 requests were allowed** and **100,000 requests were rejected**, confirming 100% mathematical and behavioral equivalence between the original and optimized implementations.

### Why Mean Latency Can Exceed P95 Latency
In concurrent multi-threaded workloads, arithmetic mean latency can exceed P95 latency (e.g., Optimized Policy A at 10 workers: Mean 0.0960 ms vs P95 0.0069 ms). This occurs due to:
1. **Positively Skewed Distribution:** 95%+ of requests acquire the lock within microseconds.
2. **Lock Queuing in the Tail:** A small percentage (< 1%) of worker threads experience OS scheduling preemption while waiting on lock acquisition, resulting in tail delays of 5–20 ms.
3. **Mathematical Sensitivity:** Because the arithmetic mean averages all samples linearly, extreme tail outliers pull the mean up, while the 95th percentile strictly ignores the top 5% worst-case values.

---

## 18. Complexity Analysis

| Algorithm & Operation | Original (List-based) | Optimized (Deque-based) | Token Bucket |
|---|:---:|:---:|:---:|
| **Timestamp Insertion** | $O(1)$ amortized | $O(1)$ amortized | N/A |
| **Timestamp Eviction** | $O(N)$ (filters all items) | $O(k)$ ($k$ expired items) | N/A |
| **Worst-Case Eviction** | $O(N)$ every call | $O(1)$ amortized | N/A |
| **Decision Check** | $O(N)$ | $O(1)$ amortized | $O(1)$ worst-case |
| **Reset Calculation** | $O(1)$ | $O(1)$ | $O(1)$ |
| **Periodic Key Cleanup** | $O(K \times N)$ | $O(K \times k)$ | $O(K)$ |
| **Per-Key Memory** | $O(N)$ list overhead | $O(N)$ deque blocks | $O(1)$ constant tuple |

*$N$ = limit (number of timestamps in active window), $K$ = number of tracked keys, $k$ = number of expired timestamps.*

---

## 19. Memory Considerations & Garbage Collection

- **Preallocated Memory in Deque:** Python's `collections.deque` allocates memory in doubly linked 64-element block arrays, avoiding continuous array resizing copies.
- **Safe Key Reaper:** The background reaper thread inspects keys periodically:
  - Policy A keys are removed once all timestamps expire.
  - Policy B keys are removed **only after** natural recovery to full capacity, preventing clients from evading limits by idling.
- **Resource Deallocation:** Instances implement `close()`, `__enter__`, `__exit__`, and `__del__`, guaranteeing cleanup worker threads terminate when discarded.

---

## 20. Limitations

- **Single-Process In-Memory Boundary:** State resides exclusively in the process memory space. If running multiple independent OS worker processes without sticky sessions, quotas are multiplied by the worker count.
- **Node-Local Volatility:** Server restarts wipe current in-memory counters.
- **Global Lock Under Ultra-High Core Counts:** While single-lock contention is imperceptible under normal traffic, scaling beyond 50 concurrent OS threads encounters GIL and lock convoy bottlenecks.

---

## 21. Future Improvements & Distributed Roadmap

1. **Lock Striping / Sharded State:** Partition the `_state` dictionary across $N$ independent locks via `hash(key) % N` to achieve linear multi-core scaling.
2. **Distributed Redis Backend:** Migrate state to Redis Sorted Sets (for Sliding Window) and Redis Hashes (for Token Bucket) executed via atomic server-side Lua scripts.
3. **Local Predictive Leases:** Implement batch reservation protocols where API gateway instances lease token blocks from Redis, serving requests in nanoseconds without cross-network roundtrips.
4. **Adaptive Rate Limiting:** Dynamically adjust quotas based on upstream LLM server response latency and error rates (AIMD / CoDel backpressure).

---

## 22. Technologies Used

- **Language:** Python 3.10+ (tested with Python 3.14.6)
- **Standard Library:** `collections.deque`, `threading`, `time`, `math`, `statistics`, `typing`
- **HTTP Engine:** `aiohttp` (Asynchronous HTTP Server and Client)
- **HTTP Client (Testing):** `httpx`
- **Testing Framework:** `pytest`, `unittest`, `aiohttp.test_utils`
- **Benchmarking:** `concurrent.futures.ThreadPoolExecutor`, `time.perf_counter_ns`

---

## License

This project is licensed under the MIT License.
