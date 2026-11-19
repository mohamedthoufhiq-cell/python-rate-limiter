
import math
import sys
import time
import threading
from typing import Dict, Tuple, TypedDict

Key = Tuple[str, str]
Decision = Tuple[bool, int, int]


class StatsDict(TypedDict):
    strategy: str
    total_allowed: int
    total_rejected: int
    active_keys: int


class RateLimiter:
    def __init__(
        self,
        strategy: str = "policy_a",
        cleanup_interval_seconds: float = 30.0,
        idle_timeout_seconds: float = 300.0,
    ):
        if strategy not in ("policy_a", "policy_b"):
            raise ValueError(f"Unknown strategy: {strategy}")

        if cleanup_interval_seconds <= 0:
            raise ValueError("Cleanup interval must be positive")

        if idle_timeout_seconds <= 0:
            raise ValueError("Idle timeout must be positive")

        self.strategy = strategy
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        self._state: Dict[Key, object] = {}
        self._last_seen: Dict[Key, float] = {}

        # Policy A configuration
        self._window_seconds: Dict[Key, float] = {}

        # Policy B configuration
        self._bucket_capacity: Dict[Key, int] = {}
        self._bucket_refill_rate: Dict[Key, float] = {}

        self._total_allowed = 0
        self._total_rejected = 0

        self._cleanup_interval = cleanup_interval_seconds
        self._idle_timeout = idle_timeout_seconds

        self._cleanup_thread = threading.Thread(
            target=self._cleanup_worker,
            name=f"RateLimiterCleanup-{strategy}",
            daemon=True,
        )
        self._cleanup_thread.start()

    def check(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        with self._lock:
            if self.strategy == "policy_a":
                result = self.policy_a_limit(
                    client_id, endpoint, limit, window_ms
                )
            else:
                result = self.policy_b_limit(
                    client_id, endpoint, capacity, refill_rate
                )

            if result[0]:
                self._total_allowed += 1
            else:
                self._total_rejected += 1

            return result

    def policy_a_limit(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
    ) -> Decision:
        """Sliding Window Rate Limiting."""

        if limit < 0 or window_ms < 0:
            raise ValueError("Limit and window must be non-negative")

        key = (client_id, endpoint)
        now = time.monotonic()
        window_seconds = window_ms / 1000

        timestamps = self._state.setdefault(key, [])

        self._last_seen[key] = now
        self._window_seconds[key] = window_seconds

        timestamps[:] = [
            timestamp
            for timestamp in timestamps
            if now - timestamp < window_seconds
        ]

        if len(timestamps) < limit:
            timestamps.append(now)
            allowed = True
        else:
            allowed = False

        remaining = max(0, limit - len(timestamps))

        if timestamps:
            reset_seconds = (
                timestamps[0] + window_seconds - now
            )
            reset_after_ms = max(
                1, math.ceil(reset_seconds * 1000)
            )
        else:
            reset_after_ms = 0

        return allowed, remaining, reset_after_ms

    def policy_b_limit(
        self,
        client_id: str,
        endpoint: str,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """Token Bucket Rate Limiting."""

        if capacity < 0 or refill_rate < 0:
            raise ValueError(
                "Capacity and refill rate must be non-negative"
            )

        key = (client_id, endpoint)
        now = time.monotonic()

        if key not in self._state:
            tokens = float(capacity)
            last_refill = now
        else:
            tokens, last_refill = self._state[key]

        elapsed = now - last_refill

        tokens = min(
            float(capacity),
            tokens + elapsed * refill_rate,
        )

        allowed = tokens >= 1

        if allowed:
            tokens -= 1
            reset_after_ms = 0
        elif refill_rate > 0:
            wait_seconds = (1 - tokens) / refill_rate
            reset_after_ms = math.ceil(wait_seconds * 1000)
        else:
            reset_after_ms = sys.maxsize

        self._state[key] = (tokens, now)
        self._last_seen[key] = now

        # Save the latest configuration for safe cleanup.
        self._bucket_capacity[key] = capacity
        self._bucket_refill_rate[key] = refill_rate

        remaining = max(0, math.floor(tokens))

        return allowed, remaining, reset_after_ms

    def _cleanup_worker(self):
        """Run automatic cleanup periodically."""

        while not self._stop_event.wait(self._cleanup_interval):
            try:
                self._cleanup_expired()
            except Exception:
                # Keep the cleanup worker alive if one cycle fails.
                continue

    def _cleanup_expired(self):
        """Remove expired windows and safely idle token buckets."""

        now = time.monotonic()

        with self._lock:
            if self.strategy == "policy_a":
                expired_keys = []

                for key, timestamps in list(self._state.items()):
                    window_seconds = self._window_seconds.get(
                        key, 0
                    )

                    timestamps[:] = [
                        timestamp
                        for timestamp in timestamps
                        if now - timestamp < window_seconds
                    ]

                    if not timestamps:
                        expired_keys.append(key)

                for key in expired_keys:
                    self._state.pop(key, None)
                    self._last_seen.pop(key, None)
                    self._window_seconds.pop(key, None)

            else:
                expired_keys = []

                for key, bucket in list(self._state.items()):
                    tokens, last_refill = bucket

                    last_seen = self._last_seen.get(
                        key, last_refill
                    )

                    if now - last_seen < self._idle_timeout:
                        continue

                    capacity = self._bucket_capacity.get(key)
                    if capacity is None:
                        continue
                    refill_rate = self._bucket_refill_rate.get(key, 0.0)

                    elapsed = now - last_refill

                    current_tokens = min(
                        float(capacity),
                        tokens + elapsed * refill_rate,
                    )

                    # Only remove a bucket when it has naturally
                    # recovered to full capacity.
                    if current_tokens >= float(capacity):
                        expired_keys.append(key)

                for key in expired_keys:
                    self._state.pop(key, None)
                    self._last_seen.pop(key, None)
                    self._bucket_capacity.pop(key, None)
                    self._bucket_refill_rate.pop(key, None)

    def stats(self) -> StatsDict:
        """Return aggregate rate limiter statistics."""

        with self._lock:
            return {
                "strategy": self.strategy,
                "total_allowed": self._total_allowed,
                "total_rejected": self._total_rejected,
                "active_keys": len(self._state),
            }

    def close(self):
        """Stop the background cleanup thread."""

        self._stop_event.set()

        if threading.current_thread() is not self._cleanup_thread:
            self._cleanup_thread.join(timeout=2)

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
