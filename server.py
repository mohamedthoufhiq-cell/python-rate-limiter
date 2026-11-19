"""
In-Memory Rate Limiter for an LLM API Gateway
=============================================

Production-grade HTTP API Gateway server implementing rate limiting
for LLM endpoints (/v1/chat and /v1/complete) and statistics (/stats).

Configuration via Environment Variables:
- STRATEGY: "policy_a" (Sliding Window) or "policy_b" (Token Bucket)
- RATE_LIMITER_IMPL: "original" (list-based) or "optimized" (deque-based)
- PORT: HTTP port (default: 8080)
- HOST: HTTP host (default: 0.0.0.0)
"""

import os
from typing import Optional
from aiohttp import web

from rate_limiter import RateLimiter as OriginalRateLimiter
from rate_limiter_optimized import RateLimiter as OptimizedRateLimiter

STRATEGY = os.environ.get("STRATEGY", "policy_a")
if STRATEGY not in {"policy_a", "policy_b"}:
    raise ValueError(f"Unsupported STRATEGY: {STRATEGY}")

RATE_LIMITER_IMPL = os.environ.get("RATE_LIMITER_IMPL", "original").lower()

LIMITER_KEY = web.AppKey("limiter", object)

LIMIT = 5
WINDOW_MS = 10_000
CAPACITY = 5
REFILL_RATE = 0.5


def get_limiter_class(impl: str = RATE_LIMITER_IMPL):
    """Return the RateLimiter implementation class based on configuration."""
    if impl == "optimized":
        return OptimizedRateLimiter
    return OriginalRateLimiter


def create_app(
    strategy: str = STRATEGY,
    limiter_instance: Optional[object] = None,
    impl: str = RATE_LIMITER_IMPL,
) -> web.Application:
    """Application factory for the Rate Limiter HTTP server."""
    if strategy not in {"policy_a", "policy_b"}:
        raise ValueError(f"Unsupported STRATEGY: {strategy}")

    limiter = limiter_instance or get_limiter_class(impl)(strategy=strategy)

    async def handle_completion(request: web.Request) -> web.Response:
        """Handle LLM completion endpoints (/v1/chat and /v1/complete)."""
        client_id = request.headers.get("X-Client-ID", "anonymous")
        endpoint = request.path

        allowed, remaining, reset_after_ms = limiter.check(
            client_id=client_id,
            endpoint=endpoint,
            limit=LIMIT,
            window_ms=WINDOW_MS,
            capacity=CAPACITY,
            refill_rate=REFILL_RATE,
        )

        headers = {
            "X-RateLimit-Remaining": str(remaining),
            "X-RateLimit-Reset-Ms": str(reset_after_ms),
            "X-RateLimit-Policy": strategy,
        }

        if not allowed:
            return web.Response(
                status=429,
                text="Rate limit exceeded",
                headers=headers,
            )

        # Validate JSON payload if a body was provided with JSON content type
        if request.can_read_body and request.content_length:
            if request.content_type == "application/json":
                try:
                    await request.json()
                except Exception:
                    return web.json_response(
                        {"error": "Invalid JSON payload"},
                        status=400,
                        headers=headers,
                    )

        return web.json_response(
            {
                "reply": f"Hello from {endpoint}",
                "client": client_id,
                "policy": strategy,
            },
            headers=headers,
        )

    async def handle_stats(_: web.Request) -> web.Response:
        """Return rate limiter statistics."""
        return web.json_response(limiter.stats())

    async def on_cleanup(app_ref: web.Application):
        """Cleanly close the rate limiter background thread on server shutdown."""
        limiter.close()

    application = web.Application()
    application[LIMITER_KEY] = limiter
    application.on_cleanup.append(on_cleanup)

    application.router.add_post("/v1/chat", handle_completion)
    application.router.add_post("/v1/complete", handle_completion)
    application.router.add_get("/stats", handle_stats)

    return application


# Global default application instance for direct running and backward compatibility
app = create_app()
limiter = app[LIMITER_KEY]

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    host = os.environ.get("HOST", "0.0.0.0")
    print(
        f"Rate limiter server listening on http://{host}:{port} "
        f"(strategy={STRATEGY}, impl={RATE_LIMITER_IMPL})"
    )
    web.run_app(app, host=host, port=port)
