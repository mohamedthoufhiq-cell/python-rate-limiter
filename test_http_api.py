"""
HTTP API Integration and Gateway Tests.
Tests routes (/v1/chat, /v1/complete, /stats), rate limit enforcement,
response headers, JSON payload validation, status codes, and policy switching.
Uses aiohttp.test_utils for fast, in-process, deterministic execution.
"""

import json
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase

from server import create_app, LIMIT, WINDOW_MS, CAPACITY, REFILL_RATE
from rate_limiter import RateLimiter as OriginalRateLimiter
from rate_limiter_optimized import RateLimiter as OptimizedRateLimiter


class TestHttpApiPolicyA(AioHTTPTestCase):
    async def get_application(self):
        self.limiter = OriginalRateLimiter(strategy="policy_a")
        return create_app(strategy="policy_a", limiter_instance=self.limiter)

    async def test_successful_requests_and_headers(self):
        """Valid requests receive HTTP 200 and standard rate limit headers."""
        resp = await self.client.request(
            "POST",
            "/v1/chat",
            headers={"X-Client-ID": "alice"},
            json={"prompt": "Hello world"},
        )
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(data["reply"], "Hello from /v1/chat")
        self.assertEqual(data["client"], "alice")
        self.assertEqual(data["policy"], "policy_a")

        self.assertIn("X-RateLimit-Remaining", resp.headers)
        self.assertEqual(resp.headers["X-RateLimit-Remaining"], str(LIMIT - 1))
        self.assertIn("X-RateLimit-Reset-Ms", resp.headers)
        self.assertEqual(resp.headers["X-RateLimit-Policy"], "policy_a")

    async def test_rate_limiting_status_429(self):
        """Exceeding the quota returns HTTP 429 and rate limit headers."""
        client_id = "test_rate_limited"
        # Consume allowed requests
        for _ in range(LIMIT):
            resp = await self.client.request(
                "POST", "/v1/chat", headers={"X-Client-ID": client_id}
            )
            self.assertEqual(resp.status, 200)

        # Next request must be rejected with 429
        rejected_resp = await self.client.request(
            "POST", "/v1/chat", headers={"X-Client-ID": client_id}
        )
        self.assertEqual(rejected_resp.status, 429)
        text = await rejected_resp.text()
        self.assertEqual(text, "Rate limit exceeded")
        self.assertEqual(rejected_resp.headers["X-RateLimit-Remaining"], "0")
        self.assertEqual(rejected_resp.headers["X-RateLimit-Policy"], "policy_a")

    async def test_endpoint_isolation(self):
        """Chat and complete endpoints maintain independent quotas."""
        client_id = "test_endpoints"
        for _ in range(LIMIT):
            resp = await self.client.request(
                "POST", "/v1/chat", headers={"X-Client-ID": client_id}
            )
            self.assertEqual(resp.status, 200)

        # /v1/chat is now exhausted
        chat_resp = await self.client.request(
            "POST", "/v1/chat", headers={"X-Client-ID": client_id}
        )
        self.assertEqual(chat_resp.status, 429)

        # /v1/complete must still allow requests for the same client
        complete_resp = await self.client.request(
            "POST", "/v1/complete", headers={"X-Client-ID": client_id}
        )
        self.assertEqual(complete_resp.status, 200)

    async def test_anonymous_client_fallback(self):
        """Missing X-Client-ID defaults to anonymous."""
        resp = await self.client.request("POST", "/v1/chat")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(data["client"], "anonymous")

    async def test_invalid_json_payload(self):
        """Malformed JSON payload returns HTTP 400."""
        resp = await self.client.request(
            "POST",
            "/v1/chat",
            headers={"Content-Type": "application/json", "X-Client-ID": "test_bad_json"},
            data="not-a-valid-json{{{",
        )
        self.assertEqual(resp.status, 400)
        data = await resp.json()
        self.assertIn("error", data)

    async def test_stats_endpoint(self):
        """GET /stats reports aggregate counters accurately."""
        await self.client.request("POST", "/v1/chat", headers={"X-Client-ID": "user_a"})
        await self.client.request("POST", "/v1/chat", headers={"X-Client-ID": "user_b"})

        resp = await self.client.request("GET", "/stats")
        self.assertEqual(resp.status, 200)
        stats = await resp.json()
        self.assertEqual(stats["strategy"], "policy_a")
        self.assertEqual(stats["total_allowed"], 2)
        self.assertEqual(stats["total_rejected"], 0)
        self.assertEqual(stats["active_keys"], 2)

    async def test_method_not_allowed_and_not_found(self):
        """Invalid HTTP methods return 405 and unknown routes return 404."""
        resp_405 = await self.client.request("GET", "/v1/chat")
        self.assertEqual(resp_405.status, 405)

        resp_404 = await self.client.request("POST", "/v1/nonexistent")
        self.assertEqual(resp_404.status, 404)


class TestHttpApiPolicyB(AioHTTPTestCase):
    async def get_application(self):
        self.limiter = OptimizedRateLimiter(strategy="policy_b")
        return create_app(
            strategy="policy_b",
            limiter_instance=self.limiter,
            impl="optimized",
        )

    async def test_policy_b_burst_and_headers(self):
        """Policy B token bucket operates through HTTP API with optimized implementation."""
        client_id = "token_user"
        for i in range(CAPACITY):
            resp = await self.client.request(
                "POST", "/v1/chat", headers={"X-Client-ID": client_id}
            )
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.headers["X-RateLimit-Policy"], "policy_b")
            self.assertEqual(resp.headers["X-RateLimit-Remaining"], str(CAPACITY - 1 - i))
            self.assertEqual(resp.headers["X-RateLimit-Reset-Ms"], "0")

        # Capacity exhausted
        rejected = await self.client.request(
            "POST", "/v1/chat", headers={"X-Client-ID": client_id}
        )
        self.assertEqual(rejected.status, 429)
        self.assertEqual(rejected.headers["X-RateLimit-Remaining"], "0")
        self.assertTrue(int(rejected.headers["X-RateLimit-Reset-Ms"]) > 0)


if __name__ == "__main__":
    import unittest
    unittest.main()
