"""Plant telemetry consumer — Kafka → PostgreSQL.

Same contract as the Phase 2 grid consumer, because the delivery guarantee is the
same: **at-least-once**. The bridge can republish a window (spool replay), so the
write must be an idempotent upsert on ``(plant_id, window_from)``, and the offset
is committed only after PostgreSQL has accepted it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ecogrid.config import PlatformSettings
from ecogrid.db import check_connectivity, create_engine, create_session_factory
from ecogrid.logging_setup import configure_logging
from ecogrid.models import PlantTelemetryRow
from ecogrid.plant.models import PlantTelemetry

logger = logging.getLogger("ecogrid.plant.consumer")

OUTCOME_INSERTED = "inserted"
OUTCOME_DUPLICATE = "duplicate"
OUTCOME_REVISED = "revised"


@dataclass
class ConsumerStats:
    consumed: int = 0
    inserted: int = 0
    revised: int = 0
    duplicates: int = 0
    rejected: int = 0

    def summary(self) -> str:
        return (
            f"consumed={self.consumed} inserted={self.inserted} revised={self.revised} "
            f"duplicates={self.duplicates} rejected={self.rejected}"
        )


async def upsert_plant(session: AsyncSession, record: PlantTelemetry) -> str:
    """Idempotent upsert. Returns the outcome for stats."""
    values = {
        "plant_id": record.plant_id,
        "window_from": record.window_from,
        "window_to": record.window_to,
        "plant_name": record.plant_name,
        "total_load_mw": record.total_load_mw,
        "flexible_load_mw": record.flexible_load_mw,
        "inflexible_load_mw": record.inflexible_load_mw,
        "process_states": dict(record.process_states),
        "unit": record.unit,
        "is_estimate": record.is_estimate,
        "schema_version": record.schema_version,
        "source": record.source,
        "payload_hash": record.payload_hash,
        "ingested_at": record.ingested_at,
    }

    stmt = pg_insert(PlantTelemetryRow).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["plant_id", "window_from"],
        set_={
            "window_to": stmt.excluded.window_to,
            "plant_name": stmt.excluded.plant_name,
            "total_load_mw": stmt.excluded.total_load_mw,
            "flexible_load_mw": stmt.excluded.flexible_load_mw,
            "inflexible_load_mw": stmt.excluded.inflexible_load_mw,
            "process_states": stmt.excluded.process_states,
            "unit": stmt.excluded.unit,
            "is_estimate": stmt.excluded.is_estimate,
            "payload_hash": stmt.excluded.payload_hash,
            "ingested_at": stmt.excluded.ingested_at,
            "last_seen_at": func.now(),
            "revision_count": PlantTelemetryRow.revision_count + 1,
        },
        where=PlantTelemetryRow.payload_hash.is_distinct_from(stmt.excluded.payload_hash),
    ).returning(PlantTelemetryRow.revision_count)

    result = await session.execute(stmt)
    row = result.first()
    if row is None:
        return OUTCOME_DUPLICATE
    revision = int(row[0] or 0)
    return OUTCOME_INSERTED if revision == 0 else OUTCOME_REVISED


class PlantConsumer:
    """Consumes ``ecogrid.telemetry.plant`` into the plant ledger."""

    def __init__(self, settings: PlatformSettings) -> None:
        self._settings = settings
        self._engine = None
        self._session_factory = None
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq: AIOKafkaProducer | None = None
        self._stats = ConsumerStats()
        self._uncommitted = 0
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._engine = create_engine(self._settings)
        await check_connectivity(self._engine)
        self._session_factory = create_session_factory(self._engine)

        self._consumer = AIOKafkaConsumer(
            self._settings.kafka_plant_topic,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=f"{self._settings.kafka_consumer_group}-plant",
            client_id=f"{self._settings.plant_client_id}-consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await self._consumer.start()

        self._dlq = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            client_id=f"{self._settings.plant_client_id}-dlq",
            acks="all",
            compression_type="gzip",
        )
        await self._dlq.start()

        logger.info(
            "Plant consumer started | topic=%s group=%s",
            self._settings.kafka_plant_topic,
            f"{self._settings.kafka_consumer_group}-plant",
        )

    async def stop(self) -> None:
        if self._consumer is not None:
            try:
                if self._uncommitted:
                    await self._consumer.commit()
            except Exception:  # noqa: BLE001
                logger.warning("Final offset commit failed — batch will be redelivered")
            await self._consumer.stop()
        if self._dlq is not None:
            await self._dlq.stop()
        if self._engine is not None:
            await self._engine.dispose()
        logger.info("Plant consumer stopped | %s", self._stats.summary())

    def request_stop(self, *_: Any) -> None:
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                signal.signal(sig, lambda s, _f: self.request_stop(s))

    async def _dead_letter(self, partition: int, offset: int, raw: bytes, reason: str) -> None:
        logger.error(
            "REJECTED plant message | partition=%d offset=%d reason=%s payload=%s",
            partition,
            offset,
            reason,
            raw[:2000].decode("utf-8", errors="replace"),
        )
        if self._dlq is None:
            return
        try:
            await self._dlq.send_and_wait(
                f"{self._settings.kafka_plant_topic}.dlq",
                key=f"{partition}:{offset}".encode(),
                value=json.dumps(
                    {
                        "source_topic": self._settings.kafka_plant_topic,
                        "partition": partition,
                        "offset": offset,
                        "reason": reason,
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                        "raw": raw.decode("utf-8", errors="replace"),
                    }
                ).encode(),
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("DLQ forward failed for offset=%d: %s", offset, exc)

    async def _handle(self, message: Any) -> None:
        self._stats.consumed += 1
        raw = message.value
        try:
            record = PlantTelemetry.model_validate_json(raw)
        except (ValidationError, ValueError) as exc:
            self._stats.rejected += 1
            await self._dead_letter(message.partition, message.offset, raw, f"schema: {exc}")
            self._uncommitted += 1
            return

        try:
            async with self._session_factory() as session:  # type: ignore[misc]
                outcome = await upsert_plant(session, record)
                await session.commit()
        except Exception as exc:  # noqa: BLE001
            # Offset deliberately NOT committed — Kafka redelivers, and the
            # upsert makes the retry a no-op rather than a duplicate.
            logger.error("Plant write failed for offset=%d: %s — left uncommitted", message.offset, exc)
            await asyncio.sleep(self._settings.kafka_retry_backoff_seconds)
            return

        if outcome == OUTCOME_INSERTED:
            self._stats.inserted += 1
        elif outcome == OUTCOME_REVISED:
            self._stats.revised += 1
        else:
            self._stats.duplicates += 1
        self._uncommitted += 1

    async def run(self) -> int:
        self._install_signal_handlers()
        await self.start()
        try:
            while not self._stop.is_set():
                batches = await self._consumer.getmany(timeout_ms=1000, max_records=200)
                for _partition, messages in batches.items():
                    for message in messages:
                        await self._handle(message)
                if self._uncommitted:
                    await self._consumer.commit()
                    self._uncommitted = 0
        except asyncio.CancelledError:
            logger.info("Plant consumer cancelled")
        except Exception:  # noqa: BLE001
            logger.exception("Plant consumer loop failed — exiting for restart")
            return 1
        finally:
            await self.stop()
        return 0


async def run() -> int:
    settings = PlatformSettings()
    configure_logging(settings.log_level)
    return await PlantConsumer(settings).run()


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
