"""AS400 plant-operations bridge.

Polls a :class:`PlantSource`, validates into :class:`PlantTelemetry`, and publishes
to ``ecogrid.telemetry.plant`` keyed by ``plant_id:window_from``.

Resilience mirrors the Phase 1 worker deliberately: bounded retries, a bounded
JSONL spool for durability, and a spool that is **truncated, never unlinked**
(deleting needs a delete syscall, which some environments guard or forbid).

Delivery is at-least-once, so — exactly as with grid telemetry — consumers must
upsert on the natural key rather than assume single delivery.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ecogrid.config import PlatformSettings
from ecogrid.logging_setup import configure_logging
from ecogrid.plant.models import WINDOW_MINUTES, PlantTelemetry
from ecogrid.plant.sources import PlantSource, build_source

logger = logging.getLogger("ecogrid.plant.bridge")


# --------------------------------------------------------------------------- #
# Spool
# --------------------------------------------------------------------------- #


class PlantSpool:
    """Bounded JSONL buffer so a Kafka outage does not lose plant windows.

    Same contract as the Phase 1 spool: an empty spool is a **zero-byte file**,
    never an unlinked one.
    """

    def __init__(self, path: str | Path, max_records: int = 10_000) -> None:
        self._path = Path(path)
        self._max_records = max_records
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def append(self, records: list[PlantTelemetry]) -> None:
        if not records:
            return
        try:
            with self._path.open("a", encoding="utf-8") as handle:
                for record in records:
                    handle.write(record.model_dump_json())
                    handle.write("\n")
            self._trim()
            logger.warning("Spooled %d plant record(s) to %s", len(records), self._path)
        except OSError as exc:
            logger.error("Failed to spool %d record(s) to %s: %s", len(records), self._path, exc)

    def load(self) -> list[PlantTelemetry]:
        if not self._path.exists():
            return []
        records: list[PlantTelemetry] = []
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                for line_no, line in enumerate(handle, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(PlantTelemetry.model_validate_json(line))
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Discarding corrupt spool line %d: %s", line_no, exc)
        except OSError as exc:
            logger.error("Failed to read spool %s: %s", self._path, exc)
            return []
        return records

    def replace(self, records: list[PlantTelemetry]) -> None:
        """Rewrite the spool. Empty means truncate to zero bytes."""
        try:
            with self._path.open("w", encoding="utf-8") as handle:
                for record in records:
                    handle.write(record.model_dump_json())
                    handle.write("\n")
        except OSError as exc:
            logger.error("Failed to rewrite spool %s: %s", self._path, exc)

    def _trim(self) -> None:
        """Drop the oldest lines past ``max_records``."""
        try:
            if not self._path.exists():
                return
            lines = self._path.read_text(encoding="utf-8").splitlines()
            if len(lines) <= self._max_records:
                return
            kept = lines[-self._max_records :]
            with self._path.open("w", encoding="utf-8") as handle:
                handle.write("\n".join(kept))
                handle.write("\n")
            logger.warning("Spool trimmed to %d record(s)", self._max_records)
        except OSError as exc:
            logger.error("Failed to trim spool %s: %s", self._path, exc)


# --------------------------------------------------------------------------- #
# Bridge
# --------------------------------------------------------------------------- #


@dataclass
class CycleStats:
    """Per-cycle outcome, logged and returned for assertions."""

    published: int = 0
    skipped_unchanged: int = 0
    replayed: int = 0
    failed: int = 0
    remaining_spooled: int = 0

    def summary(self) -> str:
        return (
            f"published={self.published} replayed={self.replayed} "
            f"unchanged={self.skipped_unchanged} failed={self.failed} "
            f"spooled={self.remaining_spooled}"
        )


@dataclass
class PlantBridge:
    """Fetches plant telemetry and publishes it to Kafka."""

    settings: PlatformSettings
    source: PlantSource | None = None
    producer: Any | None = None
    spool: PlantSpool | None = None

    # Runtime state (not constructor args: dataclass + injected deps stay simple).
    _stop: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)
    _last_hash: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _own_producer: bool = field(default=False, init=False, repr=False)
    _started: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        # Resolved in start(); seeded here so run_once() is safe either way.
        self._source = self.source
        self._spool = self.spool

    # -- lifecycle ----------------------------------------------------------- #

    async def _ensure_started(self) -> None:
        """Idempotent startup, so a caller can invoke run_once() directly."""
        if not self._started:
            await self.start()

    async def start(self) -> None:
        self._source = self.source or build_source(self.settings)
        self._spool = self.spool or PlantSpool(self.settings.plant_spool_path)

        if self.producer is None:
            from aiokafka import AIOKafkaProducer

            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.kafka_bootstrap_servers,
                client_id=self.settings.plant_client_id,
                acks="all",
                enable_idempotence=True,
                compression_type="gzip",
            )
            self._own_producer = True
            await self.producer.start()

        self._started = True
        logger.info(
            "Plant bridge started | source=%s topic=%s plants=%s",
            type(self._source).__name__,
            self.settings.kafka_plant_topic,
            self.settings.plant_ids,
        )

    async def stop(self) -> None:
        if self.producer is not None and self._own_producer:
            await self.producer.stop()
        self._started = False
        logger.info("Plant bridge stopped")

    def request_stop(self, *_: Any) -> None:
        logger.info("Shutdown requested — finishing current cycle")
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                signal.signal(sig, lambda s, _f: self.request_stop(s))

    # -- one cycle ---------------------------------------------------------- #

    async def run_once(self, now: datetime | None = None) -> CycleStats:
        """Replay the spool, fetch the current window, publish. Never raises."""
        await self._ensure_started()
        stats = CycleStats()
        window_from, window_to = current_window(now)

        # 1. Replay anything spooled by an earlier failure, newest-effort first.
        pending = self._spool.load()
        if pending:
            still_failing: list[PlantTelemetry] = []
            for record in pending:
                if await self._publish(record):
                    stats.replayed += 1
                    self._last_hash[record.kafka_key] = record.payload_hash
                else:
                    still_failing.append(record)
            self._spool.replace(still_failing)
            stats.remaining_spooled = len(still_failing)
            if still_failing:
                stats.failed += len(still_failing)

        # 2. Fetch and publish the current window.
        try:
            records = await self._source.fetch(window_from, window_to)
        except Exception:  # noqa: BLE001 — a bad read must not kill a 24/7 bridge
            logger.exception("Plant source read failed for window %s", window_from.isoformat())
            return stats

        failures: list[PlantTelemetry] = []
        for record in records:
            if self._last_hash.get(record.kafka_key) == record.payload_hash:
                stats.skipped_unchanged += 1
                continue
            if await self._publish(record):
                stats.published += 1
                self._last_hash[record.kafka_key] = record.payload_hash
            else:
                failures.append(record)

        if failures:
            self._spool.append(failures)
            stats.failed += len(failures)
            stats.remaining_spooled = len(self._spool.load())

        logger.info(
            "Plant cycle %s→%s | %s",
            window_from.isoformat(),
            window_to.isoformat(),
            stats.summary(),
        )
        return stats

    async def _publish(self, record: PlantTelemetry) -> bool:
        """Publish one record with bounded retries. Returns success."""
        max_attempts = 5
        base, ceiling = 1.0, 30.0
        timeout = 15.0

        for attempt in range(1, max_attempts + 1):
            try:
                await asyncio.wait_for(
                    self.producer.send_and_wait(
                        self.settings.kafka_plant_topic,
                        key=record.kafka_key.encode("utf-8"),
                        value=record.to_kafka_value(),
                    ),
                    timeout=timeout,
                )
                return True
            except asyncio.TimeoutError:
                # asyncio.TimeoutError stringifies to "", so name it explicitly.
                logger.warning(
                    "Plant send timed out after %.1fs | topic=%s key=%s (attempt %d/%d)",
                    timeout,
                    self.settings.kafka_plant_topic,
                    record.kafka_key,
                    attempt,
                    max_attempts,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Plant send failed | topic=%s key=%s (attempt %d/%d): %s",
                    self.settings.kafka_plant_topic,
                    record.kafka_key,
                    attempt,
                    max_attempts,
                    exc,
                )
            if attempt < max_attempts:
                await asyncio.sleep(min(base * (2 ** (attempt - 1)), ceiling))
        return False

    # -- main loop ---------------------------------------------------------- #

    async def run(self) -> int:
        self._install_signal_handlers()
        await self.start()
        try:
            while not self._stop.is_set():
                try:
                    await self.run_once()
                except Exception:  # noqa: BLE001 — one bad cycle must not kill the bridge
                    logger.exception("Plant cycle failed — continuing")
                await asyncio.sleep(self.settings.plant_poll_interval_seconds)
        except asyncio.CancelledError:
            logger.info("Plant bridge cancelled — shutting down")
        finally:
            await self.stop()
        return 0


# --------------------------------------------------------------------------- #
# Helpers / entry point
# --------------------------------------------------------------------------- #


def current_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    """Floor ``now`` to the half-hourly settlement window it belongs to."""
    now = now or datetime.now(timezone.utc)
    floored = now.replace(
        minute=(now.minute // WINDOW_MINUTES) * WINDOW_MINUTES, second=0, microsecond=0
    )
    return floored, floored + timedelta(minutes=WINDOW_MINUTES)


async def run() -> int:
    settings = PlatformSettings()
    configure_logging(settings.log_level)
    bridge = PlantBridge(settings)
    try:
        return await bridge.run()
    except Exception:  # noqa: BLE001
        logger.critical("Plant bridge terminated with an unhandled error", exc_info=True)
        return 1


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
