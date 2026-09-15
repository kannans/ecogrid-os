"""Phase 3 — AS400 / legacy plant-operations bridge.

Bridges industrial plant load telemetry (historically locked inside an AS400-era
system) onto the Kafka event spine, so the optimizer can correlate *when the grid
is dirty* with *what the plant is doing*.

Re-exports are resolved lazily (PEP 562). Importing the package eagerly pulled in
the consumer and the bridge, both of which need ``aiokafka`` — so merely doing
``from ecogrid.plant.models import PlantTelemetry`` failed with
``ModuleNotFoundError: No module named 'aiokafka'`` on any interpreter that had
the contract dependency but not the Kafka client. A data contract has no business
requiring a Kafka client; only the parts that talk to a broker do.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — resolved lazily at runtime
    from ecogrid.plant.bridge import PlantBridge, PlantSpool
    from ecogrid.plant.bridge import run as run_bridge
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

#: public name -> (module, attribute name within that module)
_LAZY: dict[str, tuple[str, str]] = {
    "PlantTelemetry": ("ecogrid.plant.models", "PlantTelemetry"),
    "PlantBridge": ("ecogrid.plant.bridge", "PlantBridge"),
    "PlantSpool": ("ecogrid.plant.bridge", "PlantSpool"),
    "run_bridge": ("ecogrid.plant.bridge", "run"),
    "PlantConsumer": ("ecogrid.plant.consumer", "PlantConsumer"),
    "upsert_plant": ("ecogrid.plant.consumer", "upsert_plant"),
    "PlantSource": ("ecogrid.plant.sources", "PlantSource"),
    "SimulatedPlantSource": ("ecogrid.plant.sources", "SimulatedPlantSource"),
    "FilePlantSource": ("ecogrid.plant.sources", "FilePlantSource"),
    "OdbcPlantSource": ("ecogrid.plant.sources", "OdbcPlantSource"),
    "build_source": ("ecogrid.plant.sources", "build_source"),
}


def __getattr__(name: str) -> Any:
    target = _LAZY.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = target
    return getattr(import_module(module_name), attribute)


def __dir__() -> list[str]:
    return sorted(__all__)
