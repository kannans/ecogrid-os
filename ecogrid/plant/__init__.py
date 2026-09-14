"""Phase 3 — AS400 / legacy plant-operations bridge.

Bridges industrial plant load telemetry (historically locked inside an AS400-era
system) onto the Kafka event spine, so the optimizer can correlate *when the grid
is dirty* with *what the plant is doing*.
"""

from ecogrid.plant.bridge import PlantBridge, PlantSpool, run as run_bridge
from ecogrid.plant.consumer import PlantConsumer, upsert_plant
from ecogrid.plant.models import PlantTelemetry
from ecogrid.plant.sources import (
    FilePlantSource,
    OdbcPlantSource,
    PlantSource,
    SimulatedPlantSource,
    build_source,
)

__all__ = [
    "FilePlantSource",
    "OdbcPlantSource",
    "PlantBridge",
    "PlantConsumer",
    "PlantSource",
    "PlantSpool",
    "PlantTelemetry",
    "SimulatedPlantSource",
    "build_source",
    "run_bridge",
    "upsert_plant",
]
