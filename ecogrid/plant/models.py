"""Plant telemetry contract — the Phase 3 event on Kafka.

Mirrors the Phase 1 ``GridTelemetry`` contract in spirit: one validated record
per (plant, settlement window), hashed for dedupe and keyed for partitioning.

The split between *flexible* and *inflexible* load is the whole point of the
arbitrage engine. Inflexible load (lighting, safety systems, base process heat)
must run whenever it runs; flexible load (batch mills, electrolysers, thermal
storage charging) can be **moved** to a cleaner half-hour. Only the flexible
portion is ever shifted, and this model is where that distinction is declared.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, model_validator

SCHEMA_VERSION = "1.0.0"
SOURCE_NAME = "as400-plant-bridge"

#: Settlement-window length. The UK grid settles every 30 minutes.
WINDOW_MINUTES = 30


class PlantTelemetry(BaseModel):
    """One plant's electrical load for one settlement window."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = SCHEMA_VERSION
    source: str = SOURCE_NAME

    plant_id: str = Field(min_length=1, max_length=64)
    plant_name: str = Field(default="", max_length=128)

    window_from: datetime
    window_to: datetime

    total_load_mw: float = Field(ge=0.0)
    #: Load that may be moved to another window by the optimizer.
    flexible_load_mw: float = Field(ge=0.0)
    #: Load that must run in this window regardless of carbon intensity.
    inflexible_load_mw: float = Field(ge=0.0)

    #: Per-process state snapshot, e.g. ``{"kiln": "idle", "compressor": "running"}``.
    process_states: dict[str, str] = Field(default_factory=dict)
    unit: str = "MW"
    #: True when the figure is an AS400 estimate rather than a metered read.
    is_estimate: bool = False

    ingested_at: datetime

    @model_validator(mode="after")
    def _check_window(self) -> PlantTelemetry:
        if self.window_to <= self.window_from:
            raise ValueError(
                f"window_to ({self.window_to.isoformat()}) must be after "
                f"window_from ({self.window_from.isoformat()})"
            )
        # 1e-6 absorbs float representation error in the MW figures.
        if self.flexible_load_mw + self.inflexible_load_mw > self.total_load_mw + 1e-6:
            raise ValueError(
                "flexible_load_mw + inflexible_load_mw "
                f"({self.flexible_load_mw + self.inflexible_load_mw}) must not exceed "
                f"total_load_mw ({self.total_load_mw})"
            )
        return self

    @property
    def kafka_key(self) -> str:
        """Stable partition/dedupe key: plant id + window start.

        Keying by plant alone would serialise every window onto one partition;
        keying by plant+window keeps one plant's timeline ordered per partition
        while still spreading plants across partitions.
        """
        stamp = self.window_from.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"{self.plant_id}:{stamp}"

    @property
    def payload_hash(self) -> str:
        """Content hash excluding ingestion time — the dedupe guard.

        Two deliveries of the same window must hash identically even though
        ``ingested_at`` differs, otherwise every redelivery looks like a revision.
        """
        content = self.model_dump(mode="json", exclude={"ingested_at"})
        return hashlib.sha256(
            json.dumps(content, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def to_kafka_value(self) -> bytes:
        """UTF-8 JSON, exactly as published."""
        return self.model_dump_json().encode("utf-8")

    @property
    def window_hours(self) -> float:
        """Window length in hours — used to convert MW into kWh."""
        return WINDOW_MINUTES / 60.0
