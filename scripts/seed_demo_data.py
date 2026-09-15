#!/usr/bin/env python3
"""Seed synthetic history onto the event spine, for demos and verification.

Why this exists
---------------
The upstream Carbon Intensity API serves **only the current settlement window**.
There is no backfill endpoint. So a fresh deployment accrues history at one
window per 30 minutes, and the optimizer — which needs at least two windows, and
really wants a full day to find a meaningful spread — is starved for hours.

This publishes synthetic windows **through the real pipeline** (Kafka → consumer
→ PostgreSQL → optimizer), not by writing rows directly. That means it exercises
the same contract, the same validation and the same idempotent upsert as live
data, so it is useful for verification as well as for demos.

Everything it emits is marked ``source: demo-seed`` so it can never be mistaken
for telemetry collected from the grid.

Safety
------
Publishing is opt-in: the default is a **dry run** that prints what it would
send. Pass ``--publish`` to actually write.

    python scripts/seed_demo_data.py                       # dry run
    python scripts/seed_demo_data.py --publish             # 24 grid + plant windows
    python scripts/seed_demo_data.py --publish --windows 48
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ecogrid.plant.models import PlantTelemetry  # noqa: E402
from ingest_grid import CarbonIndex, GridTelemetry  # noqa: E402

SCHEMA_VERSION = "1.0.0"
GRID_SOURCE = "demo-seed"
PLANT_SOURCE = "demo-seed-plant"
WINDOW_MINUTES = 30

#: Intensity -> band. The upstream API supplies the band itself, so this mirrors
#: its observed behaviour (40 and 51 g/kWh both came back "low", 266 "moderate").
#: Demo data only — it is never compared against live banding.
BANDS: list[tuple[float, CarbonIndex]] = [
    (20.0, CarbonIndex.VERY_LOW),
    (100.0, CarbonIndex.LOW),
    (300.0, CarbonIndex.MODERATE),
    (400.0, CarbonIndex.HIGH),
]


def band_for(intensity: float) -> CarbonIndex:
    for upper, index in BANDS:
        if intensity < upper:
            return index
    return CarbonIndex.VERY_HIGH


def _jitter(key: str, span: float) -> float:
    """Deterministic +/- span from a hash, so runs are reproducible."""
    digest = hashlib.md5(key.encode()).hexdigest()
    return ((int(digest[:8], 16) / 0xFFFFFFFF) - 0.5) * 2 * span


def intensity_for(window_from: datetime) -> float:
    """A diurnal carbon curve: clean overnight, dirty at the evening peak.

    Deliberately wide (roughly 35–290 g/kWh) so there is real arbitrage to find —
    a flat series would make the optimizer correctly report 0% saving and teach
    the reader nothing.
    """
    hour = window_from.astimezone(timezone.utc).hour + window_from.minute / 60.0
    # Peak at ~18:00, trough at ~04:00.
    diurnal = 160.0 + 125.0 * math.sin(((hour - 12.0) / 24.0) * 2 * math.pi)
    return max(25.0, min(320.0, diurnal + _jitter(window_from.isoformat(), 18.0)))


def renewable_share(intensity: float) -> float:
    """Cleaner grid => higher renewable share. Keeps the panels self-consistent."""
    return max(5.0, min(88.0, 100.0 - intensity * 0.28))


def build_grid_window(window_from: datetime, now: datetime) -> GridTelemetry:
    window_to = window_from + timedelta(minutes=WINDOW_MINUTES)
    intensity = intensity_for(window_from)
    actual = int(round(intensity))
    # Older windows are settled; the most recent one is still a forecast.
    settled = window_from < now - timedelta(minutes=WINDOW_MINUTES)
    forecast = int(round(intensity + _jitter(f"fc{window_from.isoformat()}", 12.0)))

    renewable = renewable_share(intensity)
    fossil = max(0.0, min(100.0, 100.0 - renewable - 12.0))

    return GridTelemetry(
        schema_version=SCHEMA_VERSION,
        source=GRID_SOURCE,
        window_from=window_from,
        window_to=window_to,
        forecast_intensity=forecast,
        actual_intensity=actual if settled else None,
        carbon_index=band_for(intensity),
        generation_mix={
            "wind": round(renewable * 0.55, 1),
            "solar": round(renewable * 0.20, 1),
            "nuclear": round(renewable * 0.25, 1),
            "gas": round(fossil, 1),
        },
        renewable_percentage=round(renewable, 1),
        low_carbon_percentage=round(min(100.0, renewable + 12.0), 1),
        fossil_percentage=round(fossil, 1),
        is_forecast_only=not settled,
        generation_mix_missing=False,
        ingested_at=now,
    )


def build_plant_window(
    window_from: datetime, now: datetime, plant_id: str = "plant-01"
) -> PlantTelemetry:
    window_to = window_from + timedelta(minutes=WINDOW_MINUTES)
    hour = window_from.astimezone(timezone.utc).hour + window_from.minute / 60.0
    # Plant runs harder in daylight; ~35% of load is movable.
    load = 42.0 + 16.0 * math.sin(((hour - 8.0) / 24.0) * 2 * math.pi)
    # Round the total and the flexible part, then DERIVE the inflexible part from
    # them. Rounding all three independently lets the two parts sum to 0.001 more
    # than the total, which the model's 1e-6 tolerance rejects — the same trap
    # documented in SimulatedPlantSource, and it caught this script on first run.
    total = round(max(20.0, load + _jitter(f"plant{window_from.isoformat()}", 4.0)), 3)
    flexible = round(total * 0.35, 3)

    return PlantTelemetry(
        schema_version=SCHEMA_VERSION,
        source=PLANT_SOURCE,
        plant_id=plant_id,
        plant_name=f"Demo plant {plant_id}",
        window_from=window_from,
        window_to=window_to,
        total_load_mw=total,
        flexible_load_mw=flexible,
        inflexible_load_mw=total - flexible,
        process_states={"mill": "running", "compressor": "idle"},
        unit="MW",
        is_estimate=True,
        ingested_at=now,
    )


def current_window(now: datetime) -> datetime:
    return now.replace(
        minute=(now.minute // WINDOW_MINUTES) * WINDOW_MINUTES, second=0, microsecond=0
    )


async def publish(records: list[tuple[str, str, bytes]], servers: str) -> int:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(
        bootstrap_servers=servers, acks="all", enable_idempotence=True, compression_type="gzip"
    )
    await producer.start()
    sent = 0
    try:
        for topic, key, value in records:
            await producer.send_and_wait(topic, key=key.encode(), value=value)
            sent += 1
    finally:
        await producer.stop()
    return sent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--windows", type=int, default=24, help="half-hourly windows to seed")
    parser.add_argument("--plant", action="store_true", help="also seed plant telemetry")
    parser.add_argument("--plant-id", default="plant-01")
    parser.add_argument("--publish", action="store_true", help="actually write (default: dry run)")
    parser.add_argument("--servers", default="localhost:9092")
    parser.add_argument("--grid-topic", default="ecogrid.telemetry.carbon")
    parser.add_argument("--plant-topic", default="ecogrid.telemetry.plant")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    newest = current_window(now)
    windows = [newest - timedelta(minutes=WINDOW_MINUTES * i) for i in range(args.windows)][::-1]

    records: list[tuple[str, str, bytes]] = []
    for window_from in windows:
        grid = build_grid_window(window_from, now)
        records.append((args.grid_topic, grid.kafka_key, grid.to_kafka_value()))
        if args.plant:
            plant = build_plant_window(window_from, now, args.plant_id)
            records.append((args.plant_topic, plant.kafka_key, plant.to_kafka_value()))

    print(f"window range : {windows[0]:%Y-%m-%d %H:%M}Z .. {windows[-1]:%H:%M}Z")
    print(f"grid windows : {args.windows}")
    print(f"plant windows: {args.windows if args.plant else 0}")
    print(f"records      : {len(records)}")
    intensities = [intensity_for(w) for w in windows]
    print(
        f"intensity    : min {min(intensities):.0f} / max {max(intensities):.0f} gCO2/kWh "
        f"(spread {max(intensities) - min(intensities):.0f})"
    )

    if not args.publish:
        print("\nDRY RUN — nothing sent. Re-run with --publish to write.")
        print("first record:", records[0][1], records[0][2][:110].decode() + "…")
        return 0

    sent = asyncio.run(publish(records, args.servers))
    print(f"\npublished {sent} record(s) to {args.servers}")
    print("The consumer will ingest them; watch: docker compose logs -f consumer")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
