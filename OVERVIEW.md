
# Q1: In-memory rate limiter for an LLM API gateway

## Background

Build an in-memory rate limiter for an LLM API gateway where multiple clients share API resources. The limiter must prevent a single client from consuming excessive capacity while safely handling concurrent requests and shared state.

## Task

Implement a rate limiter with two configurable strategies: **Sliding Window** and **Token Bucket**. The limiter should maintain independent state for each client and resource, correctly determine whether each request should be accepted, and return the remaining capacity and estimated recovery time. The implementation must also maintain accurate statistics and remain correct under concurrent access.

## What This Problem Tests

- Implementing **Sliding Window** and **Token Bucket** rate-limiting algorithms
- Handling time-based expiration and continuous token refills
- Managing **shared state and thread safety** in a concurrent environment
- Applying basic **locking and synchronization** mechanisms
- Maintaining isolated state across clients and resources
- Handling edge cases such as rejected requests, exhausted capacity, and partial recovery
- Translating detailed algorithmic requirements into a reliable engineering implementation

---

## Next step

This page is a short overview. The full problem statement — detailed requirements, which files to edit, and how to run the tests — is in [README.md](./README.md). Read it before you start.
