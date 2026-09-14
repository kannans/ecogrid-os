"""Pluggable sources of plant telemetry.

In production this reads from an AS400 / IBM i. Three implementations exist so the
bridge can be built and verified without mainframe access:

* :class:`SimulatedPlantSource` — deterministic synthetic load (default). Same
  inputs always produce the same output, which is what makes it testable.
* :class:`FilePlantSource` — reads JSON/JSONL batch drops. This is the realistic
  shape of an AS400 overnight export, so it is also the shape most real
  integrations arrive in first.
* :class:`OdbcPlantSource` — the live path. It is a documented seam: it raises
  until a DSN and ``pyodbc`` are supplied, rather than pretending to connect.

Why a seam instead of a real driver: an AS400 connection needs a licensed
ODBC driver, a DSN, and network reachability to a system that does not exist in
this repository. Shipping a fake that silently returns invented numbers would be
worse than shipping a seam that fails loudly.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from ecogrid.config import PlatformSettings
from ecogrid.plant.models import PlantTelemetry

logger = logging.getLogger("ecogrid.plant.sources")


class PlantSource(Protocol):
    """Anything that can produce plant telemetry for one settlement window."""

    async def fetch(
        self, window_from: datetime, window_to: datetime
    ) -> list[PlantTelemetry]:
        """Return the telemetry for ``window_from``→``window_to``."""


class SimulatedPlantSource:
    """Deterministic synthetic plant load.

    Load follows a diurnal curve with a small stable jitter, so the optimizer has
    something non-trivial to work with and tests remain reproducible: fetching the
    same window twice yields byte-identical records.
    """

    def __init__(self, settings: PlatformSettings, plant_ids: list[str] | None = None) -> None:
        self._settings = settings
        self._plant_ids = plant_ids or [
            p.strip() for p in settings.plant_ids.split(",") if p.strip()
        ]

    async def fetch(
        self, window_from: datetime, window_to: datetime
    ) -> list[PlantTelemetry]:
        now = datetime.now(timezone.utc)
        return [self._record(pid, window_from, window_to, now) for pid in self._plant_ids]

    # -- internals ---------------------------------------------------------- #

    def _record(
        self, plant_id: str, window_from: datetime, window_to: datetime, now: datetime
    ) -> PlantTelemetry:
        # Rounding is deliberately avoided: flexible + inflexible must equal the
        # total exactly, or the model validator (1e-6 tolerance) can reject it.
        total = self._settings.plant_base_load_mw * self._load_factor(plant_id, window_from)
        flexible = total * self._settings.plant_flexible_fraction
        inflexible = total - flexible

        return PlantTelemetry(
            plant_id=plant_id,
            plant_name=f"Simulated plant {plant_id}",
            window_from=window_from,
            window_to=window_to,
            total_load_mw=total,
            flexible_load_mw=flexible,
            inflexible_load_mw=inflexible,
            process_states=self._process_states(plant_id, window_from),
            unit="MW",
            # A simulation is an estimate, and saying so matters downstream:
            # dispatchers must never treat it as a metered read.
            is_estimate=True,
            ingested_at=now,
        )

    @staticmethod
    def _load_factor(plant_id: str, window_from: datetime) -> float:
        """Diurnal curve (0.78–1.0) plus a stable ±6% jitter."""
        hour = window_from.astimezone(timezone.utc).hour
        diurnal = 0.78 + 0.22 * math.sin(((hour - 6) / 24.0) * 2 * math.pi)

        digest = hashlib.md5(f"{plant_id}|{window_from.isoformat()}".encode()).hexdigest()
        jitter = (int(digest[:4], 16) / 0xFFFF) - 0.5  # [-0.5, 0.5]
        return max(0.4, diurnal + 0.12 * jitter)

    @staticmethod
    def _process_states(plant_id: str, window_from: datetime) -> dict[str, str]:
        digest = hashlib.md5(f"states|{plant_id}|{window_from.isoformat()}".encode()).hexdigest()
        return {
            "mill": "running" if int(digest[0], 16) % 2 == 0 else "idle",
            "compressor": "running" if int(digest[1], 16) % 2 == 0 else "idle",
        }


class FilePlantSource:
    """Reads AS400-style batch drops from a directory.

    Accepts ``*.json`` (a single object or an array) and ``*.jsonl`` (one JSON
    object per line). Files are read in sorted order and the newest window per
    plant wins, mirroring how an overnight export lands several partial drops.
    """

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)

    async def fetch(
        self, window_from: datetime, window_to: datetime
    ) -> list[PlantTelemetry]:
        if not self._dir.exists():
            logger.warning("AS400 drop directory %s does not exist", self._dir)
            return []

        records: list[PlantTelemetry] = []
        for path in sorted(self._dir.glob("*")):
            if path.suffix.lower() not in {".json", ".jsonl"}:
                continue
            records.extend(self._read_file(path))

        if not records:
            return []

        # Latest window per plant — a drop may contain several revisions.
        newest: dict[str, PlantTelemetry] = {}
        for record in records:
            existing = newest.get(record.plant_id)
            if existing is None or record.window_from > existing.window_from:
                newest[record.plant_id] = record
        return list(newest.values())

    @staticmethod
    def _read_file(path: Path) -> list[PlantTelemetry]:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.error("Cannot read AS400 drop %s: %s", path, exc)
            return []

        out: list[PlantTelemetry] = []
        if path.suffix.lower() == ".jsonl":
            for line_no, line in enumerate(text.splitlines(), start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(PlantTelemetry.model_validate_json(line))
                except Exception as exc:  # noqa: BLE001 — a bad drop must not stop the bridge
                    logger.warning("Skipping bad line %d in %s: %s", line_no, path, exc)
            return out

        try:
            payload: Any = json.loads(text)
        except json.JSONDecodeError as exc:
            logger.error("Skipping malformed JSON drop %s: %s", path, exc)
            return []

        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            try:
                out.append(PlantTelemetry.model_validate(item))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping bad record in %s: %s", path, exc)
        return out


class OdbcPlantSource:
    """Live AS400 / IBM i source — the production integration seam.

    This is intentionally not implemented as a fake. Connecting to an AS400
    requires a licensed ODBC driver (``pyodbc`` + IBM i Access), a DSN, and
    network reachability to the host — none of which exist in this repository.
    Wire it up by supplying ``dsn`` and ``query``; until then :meth:`fetch` fails
    loudly instead of inventing numbers.
    """

    def __init__(self, dsn: str | None = None, query: str | None = None) -> None:
        self._dsn = dsn
        self._query = query or (
            "SELECT PLANT_ID, WINDOW_FROM, WINDOW_TO, TOTAL_MW, FLEX_MW, INFLEX_MW "
            "FROM PLANT.LOAD_HISTORY WHERE WINDOW_FROM = ?"
        )

    @property
    def is_configured(self) -> bool:
        return bool(self._dsn)

    async def fetch(
        self, window_from: datetime, window_to: datetime
    ) -> list[PlantTelemetry]:
        raise NotImplementedError(
            "OdbcPlantSource needs a live AS400 connection. Provide `dsn` and "
            "`query`, install pyodbc and the IBM i Access ODBC driver, then map "
            "the result rows onto PlantTelemetry. "
            f"(configured={self.is_configured}, query={self._query!r})"
        )


def build_source(settings: PlatformSettings) -> PlantSource:
    """Select the source named by ``ECOGRID_PLANT_SOURCE``."""
    kind = (settings.plant_source or "simulated").strip().lower()
    if kind == "simulated":
        return SimulatedPlantSource(settings)
    if kind == "file":
        return FilePlantSource(settings.plant_file_dir)
    if kind == "odbc":
        return OdbcPlantSource()
    raise ValueError(
        f"unknown ECOGRID_PLANT_SOURCE={kind!r} (expected simulated|file|odbc)"
    )
