"""Phase 3 — the Databricks optimisation loop.

Turns a carbon-intensity forecast plus plant flexible capacity into a dispatch
schedule, then persists and publishes it.
"""

from ecogrid.optimizer.databricks import DatabricksNotConfigured, DatabricksRunner
from ecogrid.optimizer.loop import (
    DEFAULT_PROCESSES,
    OptimizationLoop,
    build_plan,
    load_processes,
    persist_plan,
    run_once,
)
from ecogrid.optimizer.solver import (
    Decision,
    FlexibleProcess,
    IntensityWindow,
    Plan,
    solve,
)

__all__ = [
    "DEFAULT_PROCESSES",
    "DatabricksNotConfigured",
    "DatabricksRunner",
    "Decision",
    "FlexibleProcess",
    "IntensityWindow",
    "OptimizationLoop",
    "Plan",
    "build_plan",
    "load_processes",
    "persist_plan",
    "run_once",
    "solve",
]
