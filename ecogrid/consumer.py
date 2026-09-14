"""Kafka → PostgreSQL → Redis telemetry consumer.

This is where the Phase 1 at-least-once contract is honoured. The consumer
**must** be idempotent, because the producer guarantees only that every message
arrives at least once — a spool replay can re-emit a window that was already
consumed, and an offset that was not committed before a crash will be redelivered.

Idempotency is achieved with a single statement::

    INSERT ... ON CONFLICT (window_from) DO UPDATE
      SET ... , revision_count = revision_count + 1
      WHERE grid_telemetry.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash

The ``WHERE`` clause is the important part. Without it, every redelivery would
look like a change and ``revision_count`` would count deliveries instead of
revisions — destroying its value as a signal of how often the grid operator
actually revised a window. With it, a redelivered identical payload is a no-op.

Ordering of side effects per message:
  1. upsert into PostgreSQL (the source of truth)
  2. update consumer-offset bookkeeping, in the same transaction
  3. refresh the Redis hot-read cache (best-effort, after the commit)
  4. commit the Kafka offset — deliberately last, so a crash between 1 and 4
     causes redelivery rather than data loss.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from ecogrid.cache import TelemetryCache
from ecogrid.config import PlatformSettings
from ecogrid.db import check_connectivity, create_engine, create_session_factory
from ecogrid.kafka import security_kwargs
from ecogrid.logging_setup import configure_logging
from ecogrid.models import GridTelemetryRow, IngestAudit

# Reuse the Phase 1 contract model verbatim. The consumer therefore validates
# against exactly the schema the worker produced — there is no second definition
# of the contract to drift.
from ingest_grid import GridTelemetry

logger = logging.getLogger("ecogrid.consumer")

#: Outcome labels for a single message.
OUTCOME_INSERTED: Final[str] = "inserted"
OUTCOME_REVISED: Final[str] = "revised"
OUTCOME_DUPLICATE: Final[str] = "duplicate"
OUTCOME_REJECTED: Final[str] = "rejected"


@dataclass
class ConsumerStats:
    """Running counters, also mirrored into ``ingest_audit``."""

    consumed: int = 0
    inserted: int = 0
    revised: int = 0
    duplicates: int = 0
    rejected: int = 0
    dead_lettered: int = 0

    def summary(self) -> str:
        return (
            f"consumed={self.consumed} inserted={self.inserted} revised={self.revised} "
            f"duplicates={self.duplicates} rejected={self.rejected} "
            f"dead_lettered={self.dead_lettered}"
        )


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #


def _row_values(telemetry: GridTelemetry) -> dict[str, Any]:
    """Map the validated contract model onto the table columns."""
    return {
        "window_from": telemetry.window_from,
        "window_to": telemetry.window_to,
        "forecast_intensity": telemetry.forecast_intensity,
        "actual_intensity": telemetry.actual_intensity,
        "carbon_index": telemetry.carbon_index.value,
        "generation_mix": telemetry.generation_mix,
        "renewable_percentage": telemetry.renewable_percentage,
        "low_carbon_percentage": telemetry.low_carbon_percentage,
        "fossil_percentage": telemetry.fossil_percentage,
        "is_forecast_only": telemetry.is_forecast_only,
        "generation_mix_missing": telemetry.generation_mix_missing,
        "schema_version": telemetry.schema_version,
        "source": telemetry.source,
        "payload_hash": telemetry.payload_hash(),
        "ingested_at": telemetry.ingested_at,
    }


@dataclass(frozen=True, slots=True)
class UpsertResult:
    """Outcome of an idempotent write.

    Carries the ledger columns returned by the statement so the caller can build
    a cache entry in the API's shape without a second round-trip. They are ``None``
    for a suppressed duplicate, where no row is updated and nothing changed.
    """

    outcome: str
    revision_count: int | None = None
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None


async def upsert_telemetry(session: AsyncSession, telemetry: GridTelemetry) -> UpsertResult:
    """Idempotent write. Returns an :class:`UpsertResult`."""
    values = _row_values(telemetry)

    base = pg_insert(GridTelemetryRow).values(**values)
    statement = (
        base.on_conflict_do_update(
            index_elements=["window_from"],
            set_={
                # NOTE: first_seen_at is deliberately absent — the first
                # observation of a window must survive every later revision.
                "window_to": base.excluded.window_to,
                "forecast_intensity": base.excluded.forecast_intensity,
                "actual_intensity": base.excluded.actual_intensity,
                "carbon_index": base.excluded.carbon_index,
                "generation_mix": base.excluded.generation_mix,
                "renewable_percentage": base.excluded.renewable_percentage,
                "low_carbon_percentage": base.excluded.low_carbon_percentage,
                "fossil_percentage": base.excluded.fossil_percentage,
                "is_forecast_only": base.excluded.is_forecast_only,
                "generation_mix_missing": base.excluded.generation_mix_missing,
                "schema_version": base.excluded.schema_version,
                "source": base.excluded.source,
                "payload_hash": base.excluded.payload_hash,
                "ingested_at": base.excluded.ingested_at,
                "last_seen_at": func.now(),
                "revision_count": GridTelemetryRow.revision_count + 1,
            },
            # The duplicate-suppression guard. Identical payload -> no row updated.
            where=GridTelemetryRow.payload_hash.is_distinct_from(base.excluded.payload_hash),
        )
        .returning(
            GridTelemetryRow.revision_count,
            GridTelemetryRow.first_seen_at,
            GridTelemetryRow.last_seen_at,
        )
    )

    row = (await session.execute(statement)).first()
    if row is None:
        # WHERE clause filtered the update: byte-identical redelivery.
        return UpsertResult(outcome=OUTCOME_DUPLICATE)

    return UpsertResult(
        # revision_count is 0 only on the initial INSERT; updates set it to old+1.
        outcome=OUTCOME_INSERTED if row[0] == 0 else OUTCOME_REVISED,
        revision_count=row[0],
        first_seen_at=row[1],
        last_seen_at=row[2],
    )


async def update_ingest_audit(
    session: AsyncSession,
    settings: PlatformSettings,
    *,
    partition: int,
    offset: int,
    stats: ConsumerStats,
) -> None:
    """Record consumer progress for one partition.

    ``stats`` must be the *per-partition* counters. Writing a global total here
    would make every partition claim the whole stream, which defeats the point of
    an audit table you are meant to reconcile against the topic.
    """
    statement = pg_insert(IngestAudit).values(
        consumer_group=settings.kafka_consumer_group,
        topic=settings.kafka_topic,
        partition=partition,
        last_offset=offset,
        messages_consumed=stats.consumed,
        messages_rejected=stats.rejected,
        revisions_applied=stats.inserted + stats.revised,
        duplicates_suppressed=stats.duplicates,
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=["consumer_group", "topic", "partition"],
            set_={
                "last_offset": func.greatest(IngestAudit.last_offset, statement.excluded.last_offset),
                "messages_consumed": statement.excluded.messages_consumed,
                "messages_rejected": statement.excluded.messages_rejected,
                "revisions_applied": statement.excluded.revisions_applied,
                "duplicates_suppressed": statement.excluded.duplicates_suppressed,
                "updated_at": func.now(),
            },
        )
    )


# --------------------------------------------------------------------------- #
# Consumer
# --------------------------------------------------------------------------- #


class TelemetryConsumer:
    """Long-running Kafka consumer with a dead-letter path for poison messages."""

    def __init__(self, settings: PlatformSettings) -> None:
        self._settings = settings
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self._cache: TelemetryCache | None = None
        self._redis: Any = None
        self._stop = asyncio.Event()
        self._stats = ConsumerStats()
        #: Per-partition counters for the audit table. The global counters above
        #: are for operator logs; the audit rows must be partition-scoped.
        self._partition_stats: dict[int, ConsumerStats] = {}
        self._uncommitted = 0

    def _stats_for(self, partition: int) -> ConsumerStats:
        return self._partition_stats.setdefault(partition, ConsumerStats())

    # -- lifecycle ---------------------------------------------------------- #

    def request_stop(self, signum: int | None = None) -> None:
        if not self._stop.is_set():
            logger.info(
                "Shutdown requested%s — finishing in-flight batch",
                f" (signal {signal.Signals(signum).name})" if signum else "",
            )
            self._stop.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                signal.signal(sig, lambda s, _f: self.request_stop(s))

    async def start(self) -> None:
        settings = self._settings

        self._engine = create_engine(settings)
        await check_connectivity(self._engine)
        self._session_factory = create_session_factory(self._engine)
        logger.info("PostgreSQL connected | %s", settings.dsn_for_logs)

        from redis.asyncio import Redis

        self._redis = Redis.from_url(
            settings.redis_url,
            max_connections=settings.redis_max_connections,
            decode_responses=True,
        )
        self._cache = TelemetryCache(self._redis, settings)
        try:
            await self._redis.ping()
            logger.info("Redis connected | %s", settings.redis_url)
        except Exception as exc:  # noqa: BLE001 — cache is optional
            logger.warning("Redis unreachable (%s) — running without the hot-read cache", exc)

        self._consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            client_id=settings.kafka_client_id,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            # Offsets are committed by hand, only after PostgreSQL has accepted
            # the write. Auto-commit would risk losing data on a crash.
            enable_auto_commit=False,
            max_poll_records=settings.kafka_max_poll_records,
            session_timeout_ms=settings.kafka_session_timeout_ms,
            **security_kwargs(settings),
        )
        await self._consumer.start()
        assigned = sorted(tp.partition for tp in self._consumer.assignment())
        logger.info(
            "Kafka consumer started | group=%s topic=%s reset=%s assigned_partitions=%s",
            settings.kafka_consumer_group,
            settings.kafka_topic,
            settings.kafka_auto_offset_reset,
            assigned or "none yet",
        )

        self._dlq_producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-dlq",
            acks="all",
            compression_type="gzip",
            **security_kwargs(settings),
        )
        await self._dlq_producer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            try:
                if self._uncommitted:
                    await self._consumer.commit()
                    logger.info("Committed %d outstanding offset(s) on shutdown", self._uncommitted)
            except Exception:  # noqa: BLE001
                logger.warning("Final offset commit failed — the batch will be redelivered", exc_info=True)
            await self._consumer.stop()
        if self._dlq_producer is not None:
            await self._dlq_producer.stop()
        if self._redis is not None:
            await self._redis.aclose()
        if self._engine is not None:
            await self._engine.dispose()
        logger.info("Consumer stopped | %s", self._stats.summary())

    # -- main loop ---------------------------------------------------------- #

    async def run(self) -> None:
        # NOTE: `self._consumer` and `self._session_factory` are created by
        # start(). Do not assert them here — an assert on not-yet-initialised
        # state kills the process before the loop, and before any log line.
        self._install_signal_handlers()
        await self.start()

        settings = self._settings
        last_log = time.monotonic()

        try:
            while not self._stop.is_set():
                try:
                    batches = await self._consumer.getmany(
                        timeout_ms=1000, max_records=settings.kafka_max_poll_records
                    )
                except KafkaError as exc:
                    logger.error("Kafka poll failed: %s — backing off", exc)
                    await asyncio.sleep(settings.kafka_retry_backoff_seconds)
                    continue

                for partition, messages in batches.items():
                    for message in messages:
                        await self._handle_message(partition, message)

                if self._uncommitted:
                    await self._consumer.commit()
                    self._uncommitted = 0

                # Periodic heartbeat so a quiet topic is distinguishable from a
                # wedged consumer.
                if time.monotonic() - last_log >= 60:
                    logger.info("Consumer alive | %s", self._stats.summary())
                    last_log = time.monotonic()
        except Exception:  # noqa: BLE001
            logger.exception("Consumer loop failed — exiting so the supervisor restarts it")
            raise
        finally:
            await self.stop()

    async def _handle_message(self, partition: Any, message: Any) -> None:
        """Process one message. Never raises for a bad payload."""
        self._stats.consumed += 1
        pstats = self._stats_for(partition.partition)
        pstats.consumed += 1

        offset = message.offset
        raw = message.value

        # --- 1. validate against the shared Phase 1 contract ---
        try:
            telemetry = GridTelemetry.model_validate_json(raw)
        except ValidationError as exc:
            self._stats.rejected += 1
            pstats.rejected += 1
            await self._dead_letter(partition.partition, offset, raw, f"schema: {exc}")
            self._uncommitted += 1
            return
        except (json.JSONDecodeError, ValueError) as exc:
            self._stats.rejected += 1
            pstats.rejected += 1
            await self._dead_letter(partition.partition, offset, raw, f"parse: {exc}")
            self._uncommitted += 1
            return

        # --- 2. idempotent write, then offset bookkeeping in one transaction ---
        cache_payload: dict[str, Any] | None = None
        try:
            async with self._session_factory() as session:  # type: ignore[misc]
                result = await upsert_telemetry(session, telemetry)
                if result.outcome == OUTCOME_INSERTED:
                    self._stats.inserted += 1
                    pstats.inserted += 1
                elif result.outcome == OUTCOME_REVISED:
                    self._stats.revised += 1
                    pstats.revised += 1
                else:
                    self._stats.duplicates += 1
                    pstats.duplicates += 1
                    logger.debug(
                        "Duplicate suppressed | window=%s offset=%d",
                        telemetry.kafka_key,
                        offset,
                    )

                await update_ingest_audit(
                    session,
                    self._settings,
                    partition=partition.partition,
                    offset=offset,
                    stats=pstats,
                )
                await session.commit()

            # Build the cache entry in the API's response shape — the contract
            # payload plus the ledger columns. Caching the bare contract payload
            # would make /telemetry/latest fail validation on every hit.
            if result.outcome != OUTCOME_DUPLICATE:
                cache_payload = telemetry.model_dump(mode="json")
                cache_payload["revision_count"] = result.revision_count
                cache_payload["first_seen_at"] = (
                    result.first_seen_at.isoformat() if result.first_seen_at else None
                )
                cache_payload["last_seen_at"] = (
                    result.last_seen_at.isoformat() if result.last_seen_at else None
                )
        except Exception as exc:  # noqa: BLE001
            # The offset is NOT committed, so Kafka redelivers. Combined with the
            # idempotent upsert this is safe and cannot lose the message.
            logger.error(
                "PostgreSQL write failed for offset=%d window=%s: %s — offset left "
                "uncommitted for redelivery",
                offset,
                telemetry.kafka_key,
                exc,
            )
            await asyncio.sleep(self._settings.kafka_retry_backoff_seconds)
            return

        self._uncommitted += 1

        # --- 3. hot-read cache, best-effort after the durable write ---
        if cache_payload is not None and self._cache is not None:
            await self._cache.set_latest_if_newer(cache_payload)

        if self._stats.consumed % 100 == 0:
            logger.info("Progress | %s", self._stats.summary())

    async def _dead_letter(
        self, partition: int, offset: int, raw: bytes, reason: str
    ) -> None:
        """Forward an unparseable message to the DLQ topic.

        The offset still advances. A poison message that cannot ever validate
        must not block its partition forever — that would stall the whole
        pipeline on one bad byte. The DLQ preserves it for forensics.
        """
        logger.error(
            "REJECTED message | partition=%d offset=%d reason=%s payload=%s",
            partition,
            offset,
            reason,
            raw[:2000].decode("utf-8", errors="replace"),
        )
        if self._dlq_producer is None:
            return
        try:
            await self._dlq_producer.send_and_wait(
                f"{self._settings.kafka_topic}.dlq",
                key=f"{partition}:{offset}".encode(),
                value=json.dumps(
                    {
                        "source_topic": self._settings.kafka_topic,
                        "partition": partition,
                        "offset": offset,
                        "reason": reason,
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                        "raw": raw.decode("utf-8", errors="replace"),
                    }
                ).encode("utf-8"),
            )
            self._stats.dead_lettered += 1
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "DLQ forward failed for partition=%d offset=%d: %s — the payload is "
                "recorded in the log above only",
                partition,
                offset,
                exc,
            )


async def run() -> int:
    settings = PlatformSettings()
    configure_logging(settings.log_level)

    consumer = TelemetryConsumer(settings)
    try:
        await consumer.run()
    except Exception:
        # Log the traceback before returning non-zero. Without this the process
        # exits 1 having printed nothing at all, which is indistinguishable from
        # being killed externally.
        logger.critical("Consumer terminated with an unhandled error", exc_info=True)
        return 1
    return 0


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
