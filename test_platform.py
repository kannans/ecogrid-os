"""Phase 2 Platform Core tests.

Two tiers:

* **Unit** — pure logic, no infrastructure. Always run.
* **Integration** — require the Dockerized PostgreSQL. Auto-skipped when it is
  unreachable, so the suite stays runnable anywhere. These are the tests that
  actually matter: they pin the idempotency contract that makes at-least-once
  delivery safe.

    pytest -q                      # unit + integration (if Postgres is up)
    pytest -q -m "not integration" # unit only
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from ecogrid.cache import TelemetryCache
from ecogrid.config import PlatformSettings
from ecogrid.ratelimit import RateLimiter
from ecogrid.security import (
    Principal,
    Role,
    generate_api_key,
    hash_api_key,
    role_rank,
)

# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class FakeRedis:
    """Minimal in-memory stand-in covering the commands this codebase uses."""

    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.fail = False

    async def get(self, key: str) -> Any:
        if self.fail:
            raise ConnectionError("simulated redis outage")
        return self.store.get(key)

    async def set(self, key: str, value: Any, ex: int | None = None) -> bool:
        if self.fail:
            raise ConnectionError("simulated redis outage")
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        if self.fail:
            raise ConnectionError("simulated redis outage")
        return 1 if self.store.pop(key, None) is not None else 0

    async def ping(self) -> bool:
        if self.fail:
            raise ConnectionError("simulated redis outage")
        return True

    def pipeline(self, transaction: bool = True) -> _FakePipeline:
        return _FakePipeline(self)


class _FakePipeline:
    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._ops: list[tuple[str, tuple[Any, ...]]] = []

    async def __aenter__(self) -> _FakePipeline:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    def incr(self, key: str, amount: int = 1) -> None:
        self._ops.append(("incr", (key, amount)))

    def expire(self, key: str, seconds: int) -> None:
        self._ops.append(("expire", (key, seconds)))

    async def execute(self) -> list[Any]:
        if self._redis.fail:
            raise ConnectionError("simulated redis outage")
        results: list[Any] = []
        for op, args in self._ops:
            if op == "incr":
                key, amount = args
                self._redis.store[key] = int(self._redis.store.get(key, 0)) + amount
                results.append(self._redis.store[key])
            elif op == "expire":
                results.append(True)
        return results


# --------------------------------------------------------------------------- #
# Unit — security
# --------------------------------------------------------------------------- #


def test_hash_api_key_is_deterministic_and_fixed_width() -> None:
    digest = hash_api_key("some-key")
    assert digest == hash_api_key("some-key")
    assert len(digest) == 64
    assert digest != hash_api_key("some-other-key")


def test_generated_key_never_exposes_its_digest() -> None:
    raw, digest, prefix = generate_api_key()
    assert len(raw) >= 32
    assert digest == hash_api_key(raw)
    assert raw.startswith(prefix)
    assert raw not in digest


def test_role_ranking_is_ordered() -> None:
    assert role_rank(Role.VIEWER) < role_rank(Role.OPERATOR) < role_rank(Role.ADMIN)


def test_unknown_role_ranks_lowest_and_does_not_raise() -> None:
    """A corrupted role must fail closed, not crash the request."""
    assert role_rank("superuser") == 0
    assert role_rank("") == 0


def test_principal_capability_hierarchy() -> None:
    viewer = Principal(key_id="1", name="v", role=Role.VIEWER, key_prefix="aaa")
    admin = Principal(key_id="2", name="a", role=Role.ADMIN, key_prefix="bbb")

    assert viewer.has_at_least(Role.VIEWER)
    assert not viewer.has_at_least(Role.OPERATOR)
    assert not viewer.has_at_least(Role.ADMIN)

    assert admin.has_at_least(Role.VIEWER)
    assert admin.has_at_least(Role.OPERATOR)
    assert admin.has_at_least(Role.ADMIN)


# --------------------------------------------------------------------------- #
# Unit — configuration
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("postgresql://u:p@h:5432/d", "postgresql+asyncpg://u:p@h:5432/d"),
        ("postgres://u:p@h:5432/d", "postgresql+asyncpg://u:p@h:5432/d"),
        ("postgresql+asyncpg://u:p@h:5432/d", "postgresql+asyncpg://u:p@h:5432/d"),
    ],
)
def test_dsn_async_driver_is_injected(given: str, expected: str) -> None:
    """Operators write the plain DSN; the async driver is added for them."""
    settings = PlatformSettings(_env_file=None, postgres_dsn=given)  # type: ignore[call-arg]
    assert settings.postgres_dsn == expected


def test_dsn_for_logs_redacts_the_password() -> None:
    settings = PlatformSettings(  # type: ignore[call-arg]
        _env_file=None, postgres_dsn="postgresql://alice:hunter2@db:5432/ecogrid"
    )
    assert "hunter2" not in settings.dsn_for_logs
    assert settings.dsn_for_logs == "postgresql+asyncpg://alice:***@db:5432/ecogrid"


# --------------------------------------------------------------------------- #
# Unit — rate limiting
# --------------------------------------------------------------------------- #


def _limiter(redis: FakeRedis, *, limit: int = 3, enabled: bool = True) -> RateLimiter:
    settings = PlatformSettings(  # type: ignore[call-arg]
        _env_file=None,
        rate_limit_requests=limit,
        rate_limit_window_seconds=60,
        rate_limit_enabled=enabled,
    )
    return RateLimiter(redis, settings)  # type: ignore[arg-type]


async def test_rate_limiter_allows_up_to_limit_then_denies() -> None:
    limiter = _limiter(FakeRedis(), limit=3)
    results = [await limiter.check("key:abc") for _ in range(4)]

    assert [r.allowed for r in results] == [True, True, True, False]
    assert results[0].remaining == 2
    assert results[2].remaining == 0


async def test_rate_limiter_tracks_identities_independently() -> None:
    limiter = _limiter(FakeRedis(), limit=1)
    assert (await limiter.check("key:a")).allowed
    assert not (await limiter.check("key:a")).allowed
    assert (await limiter.check("key:b")).allowed


async def test_rate_limiter_fails_open_when_redis_is_down() -> None:
    """Redis being down must not become an API outage."""
    redis = FakeRedis()
    redis.fail = True
    result = await _limiter(redis, limit=1).check("key:abc")
    assert result.allowed is True


async def test_rate_limiter_disabled_always_allows() -> None:
    limiter = _limiter(FakeRedis(), limit=1, enabled=False)
    for _ in range(10):
        assert (await limiter.check("key:abc")).allowed


# --------------------------------------------------------------------------- #
# Unit — telemetry cache
# --------------------------------------------------------------------------- #


def _cache(redis: FakeRedis, ttl: int = 900) -> TelemetryCache:
    settings = PlatformSettings(  # type: ignore[call-arg]
        _env_file=None, telemetry_cache_ttl_seconds=ttl
    )
    return TelemetryCache(redis, settings)  # type: ignore[arg-type]


async def test_cache_stores_and_returns_latest() -> None:
    redis = FakeRedis()
    cache = _cache(redis)
    assert await cache.get_latest() is None

    payload = {"window_from": "2026-09-14T17:00:00Z", "carbon_index": "moderate"}
    assert await cache.set_latest_if_newer(payload) is True
    assert (await cache.get_latest())["carbon_index"] == "moderate"


async def test_cache_refuses_to_overwrite_a_newer_window_with_an_older_one() -> None:
    """A partition rebalance can replay an older partition.

    An unconditional SET would let a stale window overwrite a fresh one, so the
    dashboard would show the grid going backwards.
    """
    redis = FakeRedis()
    cache = _cache(redis)

    newer = {"window_from": "2026-09-14T18:00:00Z", "marker": "new"}
    older = {"window_from": "2026-09-14T17:00:00Z", "marker": "old"}

    assert await cache.set_latest_if_newer(newer) is True
    assert await cache.set_latest_if_newer(older) is False
    assert (await cache.get_latest())["marker"] == "new"


async def test_cache_accepts_a_genuinely_newer_window() -> None:
    redis = FakeRedis()
    cache = _cache(redis)
    await cache.set_latest_if_newer({"window_from": "2026-09-14T17:00:00Z"})
    assert await cache.set_latest_if_newer({"window_from": "2026-09-14T17:30:00Z"}) is True


async def test_cache_discards_corrupt_entries() -> None:
    redis = FakeRedis()
    cache = _cache(redis)
    redis.store[PlatformSettings().telemetry_cache_key] = "{not json"
    assert await cache.get_latest() is None
    # The corrupt entry must have been evicted, not left to poison later reads.
    assert PlatformSettings().telemetry_cache_key not in redis.store


async def test_cache_survives_redis_failure() -> None:
    redis = FakeRedis()
    redis.fail = True
    cache = _cache(redis)
    assert await cache.get_latest() is None
    assert await cache.set_latest_if_newer({"window_from": "2026-09-14T17:00:00Z"}) is False


# --------------------------------------------------------------------------- #
# Unit — consumer loop lifecycle
# --------------------------------------------------------------------------- #


async def test_consumer_run_reaches_the_loop(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """`run()` must not die before starting.

    Regression: an `assert self._consumer is not None` placed *before* `start()`
    checked state that `start()` creates. It always fired, so the consumer exited
    1 having logged nothing — and the module-level handler swallowed the
    traceback, making it look like an external kill.
    """
    from ecogrid.consumer import TelemetryConsumer

    settings = PlatformSettings(_env_file=None)  # type: ignore[call-arg]
    consumer = TelemetryConsumer(settings)

    calls = {"started": 0, "stopped": 0}

    async def fake_start() -> None:
        calls["started"] += 1
        consumer._stop.set()  # exit the loop immediately

    async def fake_stop() -> None:
        calls["stopped"] += 1

    monkeypatch.setattr(consumer, "start", fake_start)
    monkeypatch.setattr(consumer, "stop", fake_stop)
    monkeypatch.setattr(consumer, "_install_signal_handlers", lambda: None)

    await consumer.run()  # must not raise

    assert calls["started"] == 1
    assert calls["stopped"] == 1


async def test_consumer_stats_summary_is_readable() -> None:
    from ecogrid.consumer import ConsumerStats

    summary = ConsumerStats(consumed=5, inserted=3, revised=1, duplicates=1).summary()
    assert "consumed=5" in summary
    assert "inserted=3" in summary
    assert "duplicates=1" in summary


# --------------------------------------------------------------------------- #
# Integration — the idempotency contract
# --------------------------------------------------------------------------- #

pytestmark_integration = pytest.mark.integration


def _integration_settings() -> PlatformSettings:
    return PlatformSettings()  # reads .env


async def _engine_or_skip() -> Any:
    from ecogrid.db import check_connectivity, create_engine

    settings = _integration_settings()
    engine = create_engine(settings)
    try:
        await check_connectivity(engine)
    except Exception as exc:  # noqa: BLE001
        await engine.dispose()
        pytest.skip(f"PostgreSQL unreachable ({type(exc).__name__}) — integration test skipped")
    return engine


async def _outcome(session: Any, telemetry: Any) -> str:
    """Run the upsert and return just the outcome label."""
    from ecogrid.consumer import upsert_telemetry

    return (await upsert_telemetry(session, telemetry)).outcome


#: Windows deliberately dated in 1990 so integration rows can never collide with
#: real telemetry, and cleanup is a bounded range delete.
_ERA = datetime(1990, 1, 1, tzinfo=timezone.utc)


def _telemetry(window_offset_minutes: int = 0, *, forecast: int = 100, actual: int | None = 110):
    """Build a GridTelemetry via the Phase 1 contract model."""
    from ingest_grid import CarbonIndex, GridTelemetry

    start = _ERA + timedelta(minutes=window_offset_minutes)
    return GridTelemetry(
        window_from=start,
        window_to=start + timedelta(minutes=30),
        forecast_intensity=forecast,
        actual_intensity=actual,
        carbon_index=CarbonIndex.MODERATE,
        generation_mix={"wind": 50.0, "gas": 50.0},
        renewable_percentage=50.0,
        low_carbon_percentage=50.0,
        fossil_percentage=50.0,
        is_forecast_only=actual is None,
        generation_mix_missing=False,
        ingested_at=_ERA,
    )


async def _cleanup(session: Any) -> None:
    from sqlalchemy import delete

    from ecogrid.models import GridTelemetryRow, IngestAudit

    await session.execute(
        delete(GridTelemetryRow).where(GridTelemetryRow.window_from < datetime(2000, 1, 1, tzinfo=timezone.utc))
    )
    await session.execute(delete(IngestAudit).where(IngestAudit.consumer_group == "test-group"))
    await session.commit()


async def _fetch_window(factory: Any, window_from: datetime) -> Any:
    """Read a window through a *fresh* session.

    Reading through the writing session would return the identity-mapped object
    with its previously loaded attributes — with ``expire_on_commit=False`` that
    object is not refreshed, so the assertion would compare stale values. A new
    session also mirrors how a real reader (the API) sees the data.
    """
    from sqlalchemy import select

    from ecogrid.models import GridTelemetryRow

    async with factory() as session:
        return (
            await session.execute(
                select(GridTelemetryRow).where(GridTelemetryRow.window_from == window_from)
            )
        ).scalar_one_or_none()


@pytest.mark.integration
async def test_upsert_inserts_then_suppresses_identical_redelivery() -> None:
    """The core at-least-once guarantee: a redelivered message is a no-op."""
    from ecogrid.consumer import OUTCOME_DUPLICATE, OUTCOME_INSERTED
    from ecogrid.db import create_session_factory

    engine = await _engine_or_skip()
    factory = create_session_factory(engine)
    try:
        async with factory() as session:
            await _cleanup(session)

            telemetry = _telemetry(0)
            assert await _outcome(session, telemetry) == OUTCOME_INSERTED
            await session.commit()

            # Same payload again — must NOT count as a revision.
            assert await _outcome(session, telemetry) == OUTCOME_DUPLICATE
            await session.commit()

        row = await _fetch_window(factory, telemetry.window_from)
        assert row is not None
        assert row.revision_count == 0, "a duplicate delivery must not bump revision_count"
        assert row.actual_intensity == 110

        async with factory() as session:
            await _cleanup(session)
    finally:
        await engine.dispose()


@pytest.mark.integration
async def test_upsert_treats_a_changed_payload_as_a_revision() -> None:
    """A settled `actual` is a genuine revision and must be recorded."""
    from ecogrid.consumer import (
        OUTCOME_DUPLICATE,
        OUTCOME_INSERTED,
        OUTCOME_REVISED,
    )
    from ecogrid.db import create_session_factory

    engine = await _engine_or_skip()
    factory = create_session_factory(engine)
    try:
        async with factory() as session:
            await _cleanup(session)

            # Forecast-only first, then the same window with the actual settled.
            forecast_only = _telemetry(30, forecast=200, actual=None)
            assert await _outcome(session, forecast_only) == OUTCOME_INSERTED
            await session.commit()

            settled = _telemetry(30, forecast=200, actual=195)
            assert await _outcome(session, settled) == OUTCOME_REVISED
            await session.commit()

            # Replaying the settled payload is again a no-op.
            assert await _outcome(session, settled) == OUTCOME_DUPLICATE
            await session.commit()

        row = await _fetch_window(factory, settled.window_from)
        assert row is not None
        assert row.revision_count == 1, "replays must not inflate the revision count"
        assert row.actual_intensity == 195
        assert row.is_forecast_only is False

        async with factory() as session:
            await _cleanup(session)
    finally:
        await engine.dispose()


@pytest.mark.integration
async def test_first_seen_at_survives_revision() -> None:
    """The first observation of a window must be preserved, not overwritten."""
    from ecogrid.db import create_session_factory

    engine = await _engine_or_skip()
    factory = create_session_factory(engine)
    try:
        async with factory() as session:
            await _cleanup(session)
            await _outcome(session, _telemetry(60, forecast=300, actual=None))
            await session.commit()

        first = await _fetch_window(factory, _telemetry(60).window_from)
        assert first is not None
        first_seen = first.first_seen_at

        await asyncio.sleep(0.05)

        async with factory() as session:
            await _outcome(session, _telemetry(60, forecast=300, actual=290))
            await session.commit()

        revised = await _fetch_window(factory, _telemetry(60).window_from)
        assert revised is not None
        assert revised.first_seen_at == first_seen, "first_seen_at must not be rewritten"
        assert revised.last_seen_at > first_seen, "last_seen_at must advance on revision"
        assert revised.revision_count == 1

        async with factory() as session:
            await _cleanup(session)
    finally:
        await engine.dispose()


@pytest.mark.integration
async def test_ingest_audit_offset_never_goes_backwards() -> None:
    """A rebalance can re-deliver lower offsets; the ledger must not regress."""
    from sqlalchemy import select

    from ecogrid.consumer import ConsumerStats, update_ingest_audit
    from ecogrid.db import create_session_factory
    from ecogrid.models import IngestAudit

    engine = await _engine_or_skip()
    settings = _integration_settings().model_copy(update={"kafka_consumer_group": "test-group"})
    factory = create_session_factory(engine)
    try:
        async with factory() as session:
            await _cleanup(session)

            stats = ConsumerStats(consumed=10, inserted=10)
            await update_ingest_audit(session, settings, partition=0, offset=100, stats=stats)
            await session.commit()

            # A lower offset arrives (rebalance / redelivery).
            await update_ingest_audit(session, settings, partition=0, offset=42, stats=stats)
            await session.commit()

        async with factory() as session:
            row = (
                await session.execute(
                    select(IngestAudit).where(IngestAudit.consumer_group == "test-group")
                )
            ).scalar_one()
            assert row.last_offset == 100, "greatest() must prevent the offset regressing"

        async with factory() as session:
            await _cleanup(session)
    finally:
        await engine.dispose()
