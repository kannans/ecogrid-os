#!/usr/bin/env python3
"""EcoGrid OS — Phase 1: Real-Time Grid Ingestion & Event Backbone.

Polls the public UK Carbon Intensity API (``/intensity`` + ``/generation``),
merges both half-hourly windows into a single validated ``GridTelemetry``
record, and streams it into the Kafka topic ``ecogrid.telemetry.carbon``.

Design guarantees
-----------------
* **Schema-first.** Every byte that reaches Kafka has passed Pydantic
  validation. Malformed upstream payloads are rejected, never forwarded.
* **Merge by window.** ``/intensity`` and ``/generation`` are correlated on
  their ``(from, to)`` interval, not on array position — the two endpoints are
  separate HTTP calls and may return different-length arrays.
* **Resilience.** Exponential backoff with jitter on HTTP transport errors and
  429/5xx; ``Retry-After`` is honoured. Kafka writes are retried, then spooled
  to a local JSONL buffer and replayed on the next cycle so telemetry survives
  broker outages.
* **Idempotent by key.** Each record is keyed by its window start timestamp, so
  downstream consumers can deduplicate or log-compact. Unchanged windows are
  skipped in-process to avoid flooding the topic with identical forecasts.
* **Graceful shutdown.** SIGINT/SIGTERM cancel in-flight work and flush the
  producer before exit.

Usage
-----
    python ingest_grid.py                 # run forever, 300s cadence
    python ingest_grid.py --once          # single cycle then exit
    python ingest_grid.py --once --dry-run  # fetch + validate, do not publish

Environment (all prefixed ``ECOGRID_``, see ``.env.example``)::

    ECOGRID_KAFKA_BOOTSTRAP_SERVERS   default localhost:9092
    ECOGRID_KAFKA_TOPIC               default ecogrid.telemetry.carbon
    ECOGRID_POLL_INTERVAL_SECONDS     default 300
    ECOGRID_LOG_LEVEL                 default INFO
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import random
import signal
import sys
import time
from collections.abc import Mapping, MutableMapping, Sequence
from contextlib import suppress
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Final

import httpx
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError, TopicAlreadyExistsError
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

SCHEMA_VERSION: Final[str] = "1.0.0"
SOURCE_NAME: Final[str] = "api.carbonintensity.org.uk"

#: Fuels that are genuinely renewable (excludes nuclear).
RENEWABLE_FUELS: Final[frozenset[str]] = frozenset({"wind", "solar", "hydro", "biomass"})
#: Fuels that are low-carbon but not strictly renewable.
LOW_CARBON_FUELS: Final[frozenset[str]] = RENEWABLE_FUELS | {"nuclear"}
#: Fossil fuels — the arbitrage target signal.
FOSSIL_FUELS: Final[frozenset[str]] = frozenset({"gas", "coal"})

#: Window-start → payload hash cache size (bounds memory on long-lived pods).
_DEDUPE_CACHE_MAX: Final[int] = 1024

logger = logging.getLogger("ecogrid.ingest")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #


class Settings(BaseSettings):
    """Runtime configuration, resolved from ``ECOGRID_*`` env vars / ``.env``."""

    model_config = SettingsConfigDict(
        env_prefix="ECOGRID_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Kafka ---
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "ecogrid.telemetry.carbon"
    kafka_client_id: str = "ecogrid-grid-ingestor"
    kafka_topic_partitions: int = 3
    kafka_topic_replication_factor: int = 1
    kafka_max_attempts: int = 5
    kafka_backoff_base_seconds: float = 1.0
    kafka_backoff_max_seconds: float = 30.0
    kafka_broker_wait_seconds: float = 90.0
    #: Hard per-attempt ceiling on a single send, enforced with asyncio.wait_for
    #: rather than relying on client internals. aiokafka has no `max_block_ms`
    #: (that is the Java client's name), so a send against a nonexistent topic
    #: otherwise stalls for request_timeout_ms on every attempt — measured at
    #: ~30s per attempt against a live broker, i.e. 150s for 5 attempts inside a
    #: 300s cycle.
    kafka_send_timeout_seconds: float = 15.0
    #: Bounds the underlying produce request itself.
    kafka_request_timeout_ms: int = 15_000

    # --- Upstream API ---
    intensity_url: str = "https://api.carbonintensity.org.uk/intensity"
    generation_url: str = "https://api.carbonintensity.org.uk/generation"
    http_timeout_seconds: float = 15.0
    http_max_attempts: int = 5
    http_backoff_base_seconds: float = 1.0
    http_backoff_max_seconds: float = 60.0

    # --- Scheduler ---
    poll_interval_seconds: int = Field(default=300, ge=30)

    # --- Durability / observability ---
    spool_path: Path = Path("./data/spool/telemetry-spool.jsonl")
    spool_max_records: int = 10_000
    heartbeat_path: Path = Path("./data/heartbeat")

    # --- Behaviour flags ---
    publish_unchanged_windows: bool = False
    log_level: str = "INFO"


# --------------------------------------------------------------------------- #
# Domain model
# --------------------------------------------------------------------------- #


class CarbonIndex(str, Enum):
    """Official carbon-intensity banding. ``UNKNOWN`` covers unrated windows."""

    VERY_LOW = "very low"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very high"
    UNKNOWN = "unknown"

    @property
    def is_clean(self) -> bool:
        """True when the grid is clean enough to soak up discretionary load."""
        return self in {CarbonIndex.VERY_LOW, CarbonIndex.LOW}


class IntensityBlock(BaseModel):
    """The ``intensity`` object inside a ``/intensity`` window."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    forecast: int = Field(ge=0, le=1000, description="gCO2/kWh forecast for the window")
    actual: int | None = Field(
        default=None,
        ge=0,
        le=1000,
        description="gCO2/kWh measured. Null while the window is still forecast-only.",
    )
    index: CarbonIndex = CarbonIndex.UNKNOWN

    @field_validator("index", mode="before")
    @classmethod
    def _normalise_index(cls, value: Any) -> Any:
        """Tolerate casing, underscores, hyphens, and absent/unknown bandings."""
        if value is None:
            return CarbonIndex.UNKNOWN
        if isinstance(value, str):
            normalised = " ".join(value.strip().lower().replace("_", " ").replace("-", " ").split())
            try:
                return CarbonIndex(normalised)
            except ValueError:
                logger.warning("Unrecognised carbon index %r — coercing to 'unknown'", value)
                return CarbonIndex.UNKNOWN
        return value


class FuelMixEntry(BaseModel):
    """A single ``generationmix`` element: ``{"fuel": "wind", "perc": 35.5}``."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    fuel: str = Field(min_length=1)
    perc: float = Field(ge=0.0, le=100.0)

    @field_validator("fuel", mode="before")
    @classmethod
    def _normalise_fuel(cls, value: Any) -> Any:
        return value.strip().lower() if isinstance(value, str) else value


class _Window(BaseModel):
    """Shared half-hourly interval contract for both upstream endpoints.

    The interval is validated at the envelope boundary so a corrupted upstream
    window is rejected at ingestion, never carried into the merge step.
    """

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    window_from: datetime = Field(alias="from")
    window_to: datetime = Field(alias="to")

    @model_validator(mode="after")
    def _check_interval(self) -> _Window:
        if self.window_to <= self.window_from:
            raise ValueError(
                f"window_to ({self.window_to.isoformat()}) must be after "
                f"window_from ({self.window_from.isoformat()})"
            )
        return self


class _IntensityWindow(_Window):
    """One element of ``/intensity`` → ``data[]``."""

    intensity: IntensityBlock


class _GenerationWindow(_Window):
    """One element of ``/generation`` → ``data[]``."""

    generationmix: list[FuelMixEntry] = Field(default_factory=list)


class _Envelope(BaseModel):
    """Base for the upstream response envelopes.

    The public API is not self-consistent: ``/intensity`` returns ``data`` as an
    array of windows, while ``/generation`` called without a date range returns
    a single window *object*. Both shapes are normalised to a list here so the
    rest of the pipeline only ever sees one contract.
    """

    model_config = ConfigDict(extra="ignore")

    @model_validator(mode="before")
    @classmethod
    def _normalise_data(cls, payload: Any) -> Any:
        """Wrap a bare window object into a one-element list."""
        if isinstance(payload, Mapping) and isinstance(payload.get("data"), Mapping):
            return {**payload, "data": [payload["data"]]}
        return payload


class IntensityEnvelope(_Envelope):
    """Top-level ``/intensity`` response envelope."""

    data: list[_IntensityWindow]


class GenerationEnvelope(_Envelope):
    """Top-level ``/generation`` response envelope."""

    data: list[_GenerationWindow]


class GridTelemetry(BaseModel):
    """Unified, validated telemetry record — the event contract on Kafka.

    One record per half-hourly settlement window, correlated across both
    upstream endpoints.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = SCHEMA_VERSION
    source: str = SOURCE_NAME

    window_from: datetime
    window_to: datetime

    forecast_intensity: int = Field(ge=0, le=1000)
    actual_intensity: int | None = Field(default=None, ge=0, le=1000)
    carbon_index: CarbonIndex

    generation_mix: dict[str, float] = Field(default_factory=dict)
    renewable_percentage: float = Field(ge=0.0, le=100.0)
    low_carbon_percentage: float = Field(ge=0.0, le=100.0)
    fossil_percentage: float = Field(ge=0.0, le=100.0)

    #: True when the window has not yet been settled by the grid operator.
    is_forecast_only: bool
    #: True when ``/generation`` had no matching window for this interval.
    generation_mix_missing: bool

    ingested_at: datetime

    @model_validator(mode="after")
    def _check_window(self) -> GridTelemetry:
        if self.window_to <= self.window_from:
            raise ValueError(
                f"window_to ({self.window_to.isoformat()}) must be after "
                f"window_from ({self.window_from.isoformat()})"
            )
        return self

    @property
    def kafka_key(self) -> str:
        """Stable partition/dedupe key: the ISO-8601 window start."""
        return self.window_from.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def effective_intensity(self) -> int:
        """Measured intensity when settled, otherwise the forecast."""
        return self.actual_intensity if self.actual_intensity is not None else self.forecast_intensity

    def payload_hash(self) -> str:
        """Content hash used to suppress republishing unchanged windows."""
        canonical = self.model_dump(mode="json", exclude={"ingested_at"})
        blob = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()

    def to_kafka_value(self) -> bytes:
        """Serialise to the on-wire JSON payload (UTC ISO-8601, sorted keys)."""
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")


# --------------------------------------------------------------------------- #
# Merge / enrichment
# --------------------------------------------------------------------------- #


def _aggregate_mix(mix: Mapping[str, float]) -> tuple[float, float, float]:
    """Return ``(renewable, low_carbon, fossil)`` percentages from a fuel mix."""
    renewable = sum(p for f, p in mix.items() if f in RENEWABLE_FUELS)
    low_carbon = sum(p for f, p in mix.items() if f in LOW_CARBON_FUELS)
    fossil = sum(p for f, p in mix.items() if f in FOSSIL_FUELS)
    return (round(renewable, 3), round(low_carbon, 3), round(fossil, 3))


def merge_telemetry(
    intensity_envelope: IntensityEnvelope,
    generation_envelope: GenerationEnvelope,
    *,
    ingested_at: datetime | None = None,
) -> list[GridTelemetry]:
    """Correlate both endpoints on their ``(from, to)`` interval.

    Windows present in ``/intensity`` but absent from ``/generation`` are still
    emitted (with an empty mix and ``generation_mix_missing=True``) because the
    intensity signal alone is actionable. Windows present only in
    ``/generation`` are dropped — without an intensity reading they carry no
    arbitrage signal.
    """
    observed_at = ingested_at or datetime.now(timezone.utc)

    mix_by_window: dict[tuple[datetime, datetime], list[FuelMixEntry]] = {
        (w.window_from, w.window_to): w.generationmix for w in generation_envelope.data
    }

    records: list[GridTelemetry] = []
    orphaned_generation = 0

    for window in intensity_envelope.data:
        key = (window.window_from, window.window_to)
        entries = mix_by_window.pop(key, None)

        if entries is None:
            orphaned_generation += 1
            mix: dict[str, float] = {}
        else:
            # Collapse duplicate fuel entries defensively (last write wins).
            mix = {entry.fuel: round(float(entry.perc), 3) for entry in entries}

        renewable, low_carbon, fossil = _aggregate_mix(mix)
        if mix and not 99.0 <= sum(mix.values()) <= 101.0:
            logger.warning(
                "Generation mix for %s sums to %.2f%% (expected ~100%%) — upstream drift?",
                window.window_from.isoformat(),
                sum(mix.values()),
            )

        records.append(
            GridTelemetry(
                window_from=window.window_from,
                window_to=window.window_to,
                forecast_intensity=window.intensity.forecast,
                actual_intensity=window.intensity.actual,
                carbon_index=window.intensity.index,
                generation_mix=mix,
                renewable_percentage=renewable,
                low_carbon_percentage=low_carbon,
                fossil_percentage=fossil,
                is_forecast_only=window.intensity.actual is None,
                generation_mix_missing=not mix,
                ingested_at=observed_at,
            )
        )

    if mix_by_window:
        logger.debug(
            "%d generation window(s) had no matching intensity window — skipped", len(mix_by_window)
        )
    if orphaned_generation:
        logger.info(
            "%d intensity window(s) lacked a generation mix; emitted with empty mix",
            orphaned_generation,
        )

    records.sort(key=lambda r: r.window_from)
    return records


# --------------------------------------------------------------------------- #
# Backoff helper
# --------------------------------------------------------------------------- #


def backoff_delay(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter, clamped to ``cap``.

    ``attempt`` is 1-based. Attempt 1 → ~base, attempt 2 → ~2·base, ...
    """
    ceiling = min(base * (2 ** max(attempt - 1, 0)), cap)
    return random.uniform(base * 0.5, ceiling) if ceiling > 0 else 0.0


# --------------------------------------------------------------------------- #
# HTTP client with resilience
# --------------------------------------------------------------------------- #


class GridApiClient:
    """Async client for the UK Carbon Intensity API.

    Retries transport faults, timeouts, HTTP 429 and 5xx with exponential
    backoff. ``Retry-After`` is honoured when the server supplies it. Non-
    retryable 4xx responses fail fast — retrying a malformed request is noise.
    """

    #: Status codes worth retrying.
    _RETRYABLE_STATUS: Final[frozenset[int]] = frozenset({408, 425, 429, 500, 502, 503, 504})

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> GridApiClient:
        read_timeout = self._settings.http_timeout_seconds
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(read_timeout, connect=min(5.0, read_timeout)),
            headers={
                "Accept": "application/json",
                "User-Agent": f"EcoGridOS/{SCHEMA_VERSION} (+phase1-ingestor)",
            },
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
            follow_redirects=True,
            http2=True,
        )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get_json(self, url: str) -> dict[str, Any]:
        if self._client is None:
            raise RuntimeError("GridApiClient used outside of its async context manager")

        cfg = self._settings
        last_error: Exception | None = None

        for attempt in range(1, cfg.http_max_attempts + 1):
            try:
                response = await self._client.get(url)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_error = exc
                if attempt >= cfg.http_max_attempts:
                    break
                delay = backoff_delay(attempt, cfg.http_backoff_base_seconds, cfg.http_backoff_max_seconds)
                logger.warning(
                    "HTTP transport error for %s (attempt %d/%d): %s — retrying in %.1fs",
                    url, attempt, cfg.http_max_attempts, exc, delay,
                )
                await asyncio.sleep(delay)
                continue

            if response.status_code == 200:
                try:
                    payload = response.json()
                except json.JSONDecodeError as exc:
                    last_error = exc
                    if attempt >= cfg.http_max_attempts:
                        break
                    delay = backoff_delay(attempt, cfg.http_backoff_base_seconds, cfg.http_backoff_max_seconds)
                    logger.warning(
                        "Non-JSON body from %s (attempt %d/%d) — retrying in %.1fs",
                        url, attempt, cfg.http_max_attempts, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                if not isinstance(payload, dict):
                    raise ValueError(f"Expected a JSON object from {url}, got {type(payload).__name__}")
                return payload

            if response.status_code in self._RETRYABLE_STATUS:
                last_error = httpx.HTTPStatusError(
                    f"HTTP {response.status_code}", request=response.request, response=response
                )
                if attempt >= cfg.http_max_attempts:
                    break
                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                delay = (
                    retry_after
                    if retry_after is not None
                    else backoff_delay(attempt, cfg.http_backoff_base_seconds, cfg.http_backoff_max_seconds)
                )
                logger.warning(
                    "Upstream returned %d for %s (attempt %d/%d) — retrying in %.1fs",
                    response.status_code, url, attempt, cfg.http_max_attempts, delay,
                )
                await asyncio.sleep(delay)
                continue

            # 4xx (other than the retryable set) — fail fast, this will not heal.
            raise httpx.HTTPStatusError(
                f"Non-retryable HTTP {response.status_code} from {url}",
                request=response.request,
                response=response,
            )

        raise RuntimeError(
            f"Exhausted {cfg.http_max_attempts} attempts for {url}"
        ) from last_error

    async def fetch_intensity(self) -> IntensityEnvelope:
        """GET ``/intensity`` and validate the envelope."""
        payload = await self._get_json(self._settings.intensity_url)
        try:
            return IntensityEnvelope.model_validate(payload)
        except ValidationError as exc:
            raise ValueError(f"/intensity payload failed schema validation: {exc}") from exc

    async def fetch_generation(self) -> GenerationEnvelope:
        """GET ``/generation`` and validate the envelope."""
        payload = await self._get_json(self._settings.generation_url)
        try:
            return GenerationEnvelope.model_validate(payload)
        except ValidationError as exc:
            raise ValueError(f"/generation payload failed schema validation: {exc}") from exc

    async def fetch_both(self) -> tuple[IntensityEnvelope, GenerationEnvelope]:
        """Fetch both endpoints concurrently.

        Uses ``return_exceptions=True`` so one endpoint's failure does not
        cancel the other mid-flight, then re-raises the first failure.
        """
        results = await asyncio.gather(
            self.fetch_intensity(), self.fetch_generation(), return_exceptions=True
        )
        failures = [r for r in results if isinstance(r, BaseException)]
        if failures:
            raise failures[0]  # type: ignore[misc]
        intensity, generation = results  # type: ignore[misc]
        return intensity, generation  # type: ignore[return-value]


def _parse_retry_after(header: str | None) -> float | None:
    """Parse ``Retry-After`` (delta-seconds form). Returns ``None`` if unusable."""
    if not header:
        return None
    try:
        seconds = float(header.strip())
    except ValueError:
        return None
    return max(0.0, min(seconds, 300.0))


# --------------------------------------------------------------------------- #
# Local durability spool
# --------------------------------------------------------------------------- #


class TelemetrySpool:
    """Append-only JSONL buffer that survives Kafka outages.

    Records that exhaust their Kafka retries are appended here and replayed at
    the start of the next cycle. The file is bounded — the oldest records are
    dropped once ``spool_max_records`` is exceeded, so a permanently broken
    broker cannot fill the disk.
    """

    def __init__(self, settings: Settings) -> None:
        self._path = Path(settings.spool_path)
        self._max_records = settings.spool_max_records
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def append(self, records: Sequence[GridTelemetry]) -> None:
        """Persist records as JSONL. Never raises — spooling is best-effort."""
        if not records:
            return
        try:
            with self._path.open("a", encoding="utf-8") as handle:
                for record in records:
                    handle.write(record.to_kafka_value().decode("utf-8"))
                    handle.write("\n")
            self._trim()
            logger.warning("Spooled %d record(s) to %s for replay", len(records), self._path)
        except OSError as exc:
            logger.error("Failed to spool %d record(s) to %s: %s", len(records), self._path, exc)

    def load(self) -> list[GridTelemetry]:
        """Read and validate every spooled record. Corrupt lines are discarded."""
        if not self._path.exists():
            return []
        records: list[GridTelemetry] = []
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                for line_no, line in enumerate(handle, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(GridTelemetry.model_validate_json(line))
                    except ValidationError as exc:
                        logger.warning(
                            "Discarding corrupt spool line %d in %s: %s", line_no, self._path, exc
                        )
        except OSError as exc:
            logger.error("Failed to read spool %s: %s", self._path, exc)
            return []
        return records

    def replace(self, records: Sequence[GridTelemetry]) -> None:
        """Atomically rewrite the spool with the records that still need replay.

        An empty spool is represented as a *zero-byte file*, never by deleting
        the file. Truncation avoids a delete syscall — which some environments
        guard or forbid, and which in testing terminated the worker mid-cycle —
        keeps the inode stable for log shippers and backup tools, and gives
        ``load()`` a single "nothing pending" representation to handle.
        """
        if not records:
            if not self._path.exists():
                return
            try:
                with self._path.open("w", encoding="utf-8"):
                    pass  # truncate in place
            except OSError as exc:
                logger.error("Failed to truncate spool %s: %s", self._path, exc)
            return

        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as handle:
                for record in records:
                    handle.write(record.to_kafka_value().decode("utf-8"))
                    handle.write("\n")
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.error("Failed to rewrite spool %s: %s", self._path, exc)

    def _trim(self) -> None:
        """Drop the oldest lines when the spool exceeds its bound."""
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                lines = handle.readlines()
            if len(lines) <= self._max_records:
                return
            kept = lines[-self._max_records :]
            logger.warning(
                "Spool exceeded %d records — dropping %d oldest line(s)",
                self._max_records, len(lines) - len(kept),
            )
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as handle:
                handle.writelines(kept)
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.error("Failed to trim spool %s: %s", self._path, exc)


# --------------------------------------------------------------------------- #
# Kafka publisher
# --------------------------------------------------------------------------- #


class TelemetryPublisher:
    """Idempotent Kafka producer with bounded retries and a disk spool fallback."""

    def __init__(self, settings: Settings, spool: TelemetrySpool) -> None:
        self._settings = settings
        self._spool = spool
        self._producer: AIOKafkaProducer | None = None
        self._dedupe: MutableMapping[str, str] = {}
        self._published_total = 0
        self._failed_total = 0

    # -- lifecycle ---------------------------------------------------------- #

    async def start(self) -> None:
        """Wait for the broker, provision the topic, and connect the producer."""
        await self._wait_for_broker()
        await self._ensure_topic()

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            client_id=self._settings.kafka_client_id,
            acks="all",
            enable_idempotence=True,
            compression_type="gzip",
            linger_ms=20,
            max_batch_size=32 * 1024,
            request_timeout_ms=self._settings.kafka_request_timeout_ms,
            retry_backoff_ms=500,
        )
        await self._producer.start()
        logger.info(
            "Kafka producer connected | bootstrap=%s topic=%s",
            self._settings.kafka_bootstrap_servers, self._settings.kafka_topic,
        )

    async def stop(self) -> None:
        """Flush pending batches and disconnect."""
        if self._producer is not None:
            with suppress(Exception):
                await self._producer.flush()
            with suppress(Exception):
                await self._producer.stop()
            self._producer = None
            logger.info(
                "Kafka producer closed | published=%d failed=%d",
                self._published_total, self._failed_total,
            )

    async def _wait_for_broker(self) -> None:
        """Poll the admin API until the broker answers or the deadline passes."""
        deadline = time.monotonic() + self._settings.kafka_broker_wait_seconds
        attempt = 0
        while True:
            attempt += 1
            admin = AIOKafkaAdminClient(
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                client_id=f"{self._settings.kafka_client_id}-admin",
                request_timeout_ms=10_000,
            )
            try:
                await admin.start()
                await admin.close()
                logger.info("Kafka broker reachable at %s", self._settings.kafka_bootstrap_servers)
                return
            except Exception as exc:  # noqa: BLE001 — broker may not be up yet
                with suppress(Exception):
                    await admin.close()
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"Kafka broker unreachable at {self._settings.kafka_bootstrap_servers} "
                        f"after {attempt} attempt(s)"
                    ) from exc
                delay = backoff_delay(attempt, 1.0, 10.0)
                logger.info(
                    "Waiting for Kafka broker (%s) — attempt %d, retrying in %.1fs",
                    self._settings.kafka_bootstrap_servers, attempt, delay,
                )
                await asyncio.sleep(delay)

    async def _ensure_topic(self) -> None:
        """Create the telemetry topic if absent. Idempotent and safe to race."""
        admin = AIOKafkaAdminClient(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            client_id=f"{self._settings.kafka_client_id}-admin",
            request_timeout_ms=15_000,
        )
        await admin.start()
        try:
            # Check first rather than relying on TOPIC_ALREADY_EXISTS: not every
            # aiokafka version surfaces that as an exception, which made the
            # success log claim "created" for a topic that already existed.
            existing = await admin.list_topics()
            if self._settings.kafka_topic in existing:
                logger.info("Kafka topic %s already present", self._settings.kafka_topic)
                return

            topic = NewTopic(
                name=self._settings.kafka_topic,
                num_partitions=self._settings.kafka_topic_partitions,
                replication_factor=self._settings.kafka_topic_replication_factor,
                topic_configs={
                    "cleanup.policy": "compact,delete",
                    "retention.ms": str(7 * 24 * 60 * 60 * 1000),
                    "min.insync.replicas": "1",
                },
            )
            try:
                await admin.create_topics([topic])
                logger.info(
                    "Created Kafka topic %s (%d partitions)",
                    self._settings.kafka_topic, self._settings.kafka_topic_partitions,
                )
            except TopicAlreadyExistsError:
                # Lost a benign race with another producer starting up.
                logger.debug("Kafka topic %s already exists", self._settings.kafka_topic)
            except KafkaError as exc:
                logger.warning("Topic provisioning skipped (%s): %s", type(exc).__name__, exc)
        finally:
            with suppress(Exception):
                await admin.close()

    # -- publishing --------------------------------------------------------- #

    async def _send_with_retry(self, key: str, value: bytes, window: str) -> bool:
        """Send one record with bounded exponential backoff. Returns success."""
        if self._producer is None:
            return False

        cfg = self._settings
        for attempt in range(1, cfg.kafka_max_attempts + 1):
            reason: str
            try:
                # Bound the send ourselves: aiokafka exposes no max_block_ms, so
                # an unresolvable topic would otherwise consume request_timeout_ms
                # on every attempt.
                await asyncio.wait_for(
                    self._producer.send_and_wait(
                        cfg.kafka_topic, key=key.encode("utf-8"), value=value
                    ),
                    timeout=cfg.kafka_send_timeout_seconds,
                )
                return True
            except asyncio.TimeoutError:
                # asyncio.TimeoutError stringifies to "", so name it explicitly
                # or the operator sees an empty failure reason.
                reason = (
                    f"send exceeded {cfg.kafka_send_timeout_seconds:g}s "
                    f"(is topic {cfg.kafka_topic!r} reachable?)"
                )
            except KafkaError as exc:
                reason = f"{type(exc).__name__}: {exc}"

            if attempt >= cfg.kafka_max_attempts:
                # Verified against a live broker: a record reported as failed
                # here can still be delivered moments later by the producer's
                # own internal retry (observed during flush() at shutdown).
                # Delivery is therefore at-least-once, never at-most-once —
                # consumers MUST dedupe on the message key.
                logger.error(
                    "Kafka send gave up for window %s after %d attempt(s): %s — "
                    "record queued to the durability spool (delivery is "
                    "at-least-once; the same window may arrive twice)",
                    window, attempt, reason,
                )
                return False

            delay = backoff_delay(attempt, cfg.kafka_backoff_base_seconds, cfg.kafka_backoff_max_seconds)
            logger.warning(
                "Kafka send failed for window %s (attempt %d/%d): %s — retrying in %.1fs",
                window, attempt, cfg.kafka_max_attempts, reason, delay,
            )
            await asyncio.sleep(delay)
        return False

    async def publish(self, records: Sequence[GridTelemetry]) -> tuple[int, int]:
        """Publish records. Returns ``(published, failed)``.

        Unchanged windows are skipped unless ``publish_unchanged_windows`` is
        set. Failures are appended to the durability spool.
        """
        if not records:
            return (0, 0)

        published = 0
        failures: list[GridTelemetry] = []

        for record in records:
            key = record.kafka_key
            digest = record.payload_hash()

            if not self._settings.publish_unchanged_windows and self._dedupe.get(key) == digest:
                logger.debug("Window %s unchanged — skipping publish", key)
                continue

            if await self._send_with_retry(key, record.to_kafka_value(), key):
                published += 1
                self._published_total += 1
                self._dedupe[key] = digest
                self._trim_dedupe()
            else:
                failures.append(record)
                self._failed_total += 1

        if failures:
            self._spool.append(failures)

        return (published, len(failures))

    async def replay_spool(self) -> int:
        """Attempt to publish previously spooled records. Returns replay count."""
        pending = self._spool.load()
        if not pending:
            return 0

        logger.info("Replaying %d spooled record(s) from %s", len(pending), self._spool.path)
        replayed = 0
        still_failing: list[GridTelemetry] = []

        for record in pending:
            key = record.kafka_key
            if await self._send_with_retry(key, record.to_kafka_value(), key):
                replayed += 1
                self._published_total += 1
                self._dedupe[key] = record.payload_hash()
                self._trim_dedupe()
            else:
                still_failing.append(record)

        self._spool.replace(still_failing)
        if replayed:
            logger.info("Replay complete | replayed=%d remaining=%d", replayed, len(still_failing))
        return replayed

    def _trim_dedupe(self) -> None:
        """Bound the dedupe cache (dict preserves insertion order)."""
        overflow = len(self._dedupe) - _DEDUPE_CACHE_MAX
        for _ in range(max(0, overflow)):
            self._dedupe.pop(next(iter(self._dedupe)), None)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


class IngestionWorker:
    """Owns the poll → validate → publish cycle and its lifecycle."""

    def __init__(self, settings: Settings, *, dry_run: bool = False) -> None:
        self._settings = settings
        self._dry_run = dry_run
        self._spool = TelemetrySpool(settings)
        self._publisher = TelemetryPublisher(settings, self._spool)
        self._stop = asyncio.Event()
        self._cycles = 0

    # -- lifecycle ---------------------------------------------------------- #

    def request_stop(self, signum: int | None = None) -> None:
        """Signal handler: request a graceful shutdown."""
        if not self._stop.is_set():
            logger.info(
                "Shutdown requested%s — finishing current cycle",
                f" (signal {signal.Signals(signum).name})" if signum else "",
            )
            self._stop.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig)
            except (NotImplementedError, RuntimeError):  # pragma: no cover — Windows
                signal.signal(sig, lambda s, _f: self.request_stop(s))

    async def run(self, *, once: bool = False) -> None:
        """Run the ingestion loop until stopped (or once, if ``once=True``)."""
        self._install_signal_handlers()
        logger.info(
            "EcoGrid ingestor starting | schema=%s cadence=%ds dry_run=%s",
            SCHEMA_VERSION, self._settings.poll_interval_seconds, self._dry_run,
        )

        if not self._dry_run:
            await self._publisher.start()
            await self._safe_replay()

        try:
            while not self._stop.is_set():
                cycle_started = time.monotonic()
                try:
                    await self._run_cycle()
                except Exception:  # noqa: BLE001
                    # Outer safety net. `_run_cycle` handles the expected faults
                    # (fetch, validation), but an unexpected error in the replay,
                    # publish, or heartbeat stage must not kill a 24/7 worker —
                    # observed in testing, where one such error ended the process
                    # after 2 cycles and left a stale spool behind.
                    logger.exception(
                        "Unhandled error in ingestion cycle — continuing to next cadence"
                    )
                self._cycles += 1

                if once or self._stop.is_set():
                    break

                elapsed = time.monotonic() - cycle_started
                sleep_for = max(0.0, self._settings.poll_interval_seconds - elapsed)
                logger.debug(
                    "Cycle %d took %.1fs — sleeping %.1fs",
                    self._cycles, elapsed, sleep_for,
                )
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=sleep_for)
        finally:
            if not self._dry_run:
                await self._publisher.stop()
            logger.info("EcoGrid ingestor stopped after %d cycle(s)", self._cycles)

    # -- cycle -------------------------------------------------------------- #

    async def _run_cycle(self) -> None:
        """One poll → validate → merge → publish cycle. Never raises."""
        try:
            async with GridApiClient(self._settings) as client:
                intensity_env, generation_env = await client.fetch_both()
        except Exception as exc:  # noqa: BLE001 — the loop must survive any upstream fault
            logger.error("Ingestion cycle failed at fetch stage: %s", exc, exc_info=logger.isEnabledFor(logging.DEBUG))
            return

        try:
            records = merge_telemetry(intensity_env, generation_env)
        except ValidationError as exc:
            logger.error("Merged telemetry failed schema validation: %s", exc)
            return

        if not records:
            logger.warning("Upstream returned no telemetry windows — nothing to publish")
            return

        # Records are sorted ascending by window start, so the newest window is
        # the last element. This is the one operators care about: the live
        # half-hourly settlement window.
        latest = records[-1]
        logger.info(
            "Ingested %d window(s) | index=%s forecast=%d actual=%s mix_missing=%s "
            "renewable=%.1f%% fossil=%.1f%% | latest_window=%s→%s",
            len(records),
            latest.carbon_index.value,
            latest.forecast_intensity,
            latest.actual_intensity if latest.actual_intensity is not None else "pending",
            latest.generation_mix_missing,
            latest.renewable_percentage,
            latest.fossil_percentage,
            latest.window_from.isoformat(),
            latest.window_to.isoformat(),
        )
        if latest.is_forecast_only:
            logger.info(
                "Window %s is forecast-only (actual intensity not yet published) — "
                "carrying forecast=%d forward",
                latest.window_from.isoformat(), latest.forecast_intensity,
            )

        if self._dry_run:
            logger.info(
                "[dry-run] would publish %d record(s) to topic %s | sample=%s",
                len(records), self._settings.kafka_topic,
                latest.to_kafka_value().decode("utf-8"),
            )
        else:
            await self._safe_replay()
            published, failed = await self._publisher.publish(records)
            logger.info(
                "Publish complete | topic=%s published=%d failed=%d",
                self._settings.kafka_topic, published, failed,
            )

        self._touch_heartbeat()

    async def _safe_replay(self) -> None:
        """Replay the spool, tolerating an unavailable broker."""
        try:
            await self._publisher.replay_spool()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Spool replay skipped: %s", exc)

    def _touch_heartbeat(self) -> None:
        """Update the liveness marker consumed by the container healthcheck."""
        try:
            path = Path(self._settings.heartbeat_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        except OSError as exc:
            logger.debug("Heartbeat write failed: %s", exc)


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #


def configure_logging(level: str) -> None:
    """UTC ISO-8601 console logging, tuned for container stdout capture."""
    logging.Formatter.converter = time.gmtime  # type: ignore[assignment]
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)sZ %(levelname)-8s %(name)s :: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # httpx logs every request at INFO; we do our own request accounting.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aiokafka").setLevel(logging.WARNING)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ingest_grid.py",
        description="EcoGrid OS Phase 1 — UK grid telemetry ingestion worker.",
    )
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and validate telemetry but do not connect to Kafka.",
    )
    parser.add_argument("--log-level", default=None, help="Override ECOGRID_LOG_LEVEL.")
    return parser.parse_args(argv)


async def _amain(args: argparse.Namespace) -> int:
    settings = Settings()
    if args.log_level:
        settings.log_level = args.log_level
    configure_logging(settings.log_level)

    worker = IngestionWorker(settings, dry_run=args.dry_run)
    try:
        await worker.run(once=args.once)
    except RuntimeError as exc:
        logger.critical("Fatal startup error: %s", exc)
        return 2
    except Exception:  # noqa: BLE001
        logger.critical("Unhandled worker failure", exc_info=True)
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return asyncio.run(_amain(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
