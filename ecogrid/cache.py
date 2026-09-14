"""Redis-backed hot-read cache for the newest telemetry window.

Scope discipline: PostgreSQL remains the source of truth. This cache exists only
to keep the dashboard's "current grid state" read off the database, which is the
highest-frequency read in the system. A cache miss falls back to PostgreSQL and
repopulates — correctness never depends on Redis being warm or even present.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from redis.asyncio import Redis

from ecogrid.config import PlatformSettings

logger = logging.getLogger("ecogrid.cache")


class TelemetryCache:
    """Single-key cache holding the most recent telemetry window."""

    def __init__(self, redis: Redis, settings: PlatformSettings) -> None:
        self._redis = redis
        self._key = settings.telemetry_cache_key
        self._ttl = settings.telemetry_cache_ttl_seconds

    async def get_latest(self) -> dict[str, Any] | None:
        """Return the cached window, or ``None`` on miss or Redis failure."""
        try:
            raw = await self._redis.get(self._key)
        except Exception as exc:  # noqa: BLE001 — cache must never break a read
            logger.warning("Redis GET failed for %s: %s — falling back to Postgres", self._key, exc)
            return None
        if raw is None:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Discarding corrupt cache entry at %s", self._key)
            await self.invalidate()
            return None
        return payload if isinstance(payload, dict) else None

    async def set_latest_if_newer(self, payload: dict[str, Any]) -> bool:
        """Cache ``payload`` only if its window is newer than what is stored.

        The consumer processes partitions in order but a rebalance can replay an
        older partition, so an unconditional SET would let a stale window
        overwrite a fresh one. Returns True when the cache was updated.
        """
        incoming = _parse_ts(payload.get("window_from"))
        if incoming is None:
            return False

        current = await self.get_latest()
        if current is not None:
            stored = _parse_ts(current.get("window_from"))
            if stored is not None and stored >= incoming:
                return False

        try:
            await self._redis.set(
                self._key,
                json.dumps(payload, default=str),
                ex=self._ttl if self._ttl > 0 else None,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Redis SET failed for %s: %s — cache stays cold", self._key, exc)
            return False
        return True

    async def invalidate(self) -> None:
        """Drop the cached window. Best-effort."""
        try:
            await self._redis.delete(self._key)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Redis DEL failed for %s: %s", self._key, exc)


def _parse_ts(value: object) -> datetime | None:
    """Parse an ISO-8601 timestamp, tolerating a trailing ``Z``."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
