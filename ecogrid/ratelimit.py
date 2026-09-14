"""Redis-backed fixed-window rate limiting.

Fixed-window rather than sliding-window-log: it is two Redis commands instead of
a sorted-set insert plus a range query, and the burst-at-the-boundary imprecision
is acceptable for defence-in-depth behind the API gateway.

Failure policy: **fail open.** If Redis is unreachable the request is allowed and
a warning is logged. This limiter protects the Platform Core from a misbehaving
internal caller; the gateway is the component that must stay available to absorb
external abuse, and a Redis outage must not turn into a full API outage.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from redis.asyncio import Redis

from ecogrid.config import PlatformSettings

logger = logging.getLogger("ecogrid.ratelimit")


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    reset_after_seconds: int


class RateLimiter:
    """Per-identity fixed-window counter."""

    def __init__(self, redis: Redis, settings: PlatformSettings) -> None:
        self._redis = redis
        self._limit = settings.rate_limit_requests
        self._window = settings.rate_limit_window_seconds
        self._enabled = settings.rate_limit_enabled

    async def check(self, identity: str) -> RateLimitResult:
        """Consume one token for ``identity``."""
        if not self._enabled:
            return RateLimitResult(True, self._limit, self._limit, 0)

        now = time.time()
        # Bucket the clock into fixed windows; all requests in a window share a key.
        window_index = int(now // self._window)
        key = f"ecogrid:ratelimit:{identity}:{window_index}"
        reset_after = int((window_index + 1) * self._window - now) or 1

        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.incr(key, 1)
                pipe.expire(key, self._window + 1)
                used, _ = await pipe.execute()
            used = int(used)
        except Exception as exc:  # noqa: BLE001 — fail open, loudly
            logger.warning(
                "Rate limiter unavailable (%s) — allowing request for %s. Redis being "
                "down must not become an API outage.",
                exc,
                identity,
            )
            return RateLimitResult(True, self._limit, self._limit, 0)

        remaining = max(0, self._limit - used)
        return RateLimitResult(
            allowed=used <= self._limit,
            limit=self._limit,
            remaining=remaining,
            reset_after_seconds=reset_after,
        )
