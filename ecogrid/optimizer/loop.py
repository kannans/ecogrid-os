"""The optimisation loop — grid carbon forecast in, dispatch schedule out.

Each pass:

1. Read the retained grid intensity forecast from PostgreSQL.
2. Read plant flexible capacity from the AS400 bridge's telemetry.
3. Schedule every flexible process into the lowest-carbon feasible block
   (Databricks if configured, otherwise the local solver).
4. Persist the run and its decisions (idempotently — a replayed run does not
   duplicate rows).
5. Publish the schedule to ``ecogrid.decisions.schedule`` for plant control
   systems to act on.

The loop never mutates an existing run: every pass writes a new ``run_id``. The
schedule is therefore an auditable decision history, not mutable state.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from ecogrid.config import PlatformSettings
from ecogrid.db import create_engine, create_session_factory
from ecogrid.kafka import security_kwargs
from ecogrid.logging_setup import configure_logging
from ecogrid.models import (
    GridTelemetryRow,
    OptimizationRunRow,
    PlantTelemetryRow,
    ScheduleDecisionRow,
)
from ecogrid.optimizer.databricks import DatabricksRunner
from ecogrid.optimizer.solver import FlexibleProcess, IntensityWindow, Plan, solve

logger = logging.getLogger("ecogrid.optimizer.loop")

SCHEMA_VERSION = "1.0.0"

#: Used when ``ECOGRID_OPTIMIZER_PROCESSES_JSON`` is not supplied.
DEFAULT_PROCESSES: list[FlexibleProcess] = [
    FlexibleProcess("mill-01", "Batch mill", 12.0, 2),
    FlexibleProcess("electrolyser-01", "Electrolyser", 8.0, 3),
    FlexibleProcess("thermal-store-01", "Thermal store charge", 5.0, 2),
]


# --------------------------------------------------------------------------- #
# Inputs
# --------------------------------------------------------------------------- #


def load_processes(settings: PlatformSettings) -> list[FlexibleProcess]:
    """Flexible-process portfolio, from config or the built-in default."""
    raw = settings.optimizer_processes_json
    if not raw:
        return list(DEFAULT_PROCESSES)
    try:
        items: Any = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error(
            "ECOGRID_OPTIMIZER_PROCESSES_JSON is not valid JSON (%s) — using defaults", exc
        )
        return list(DEFAULT_PROCESSES)

    processes: list[FlexibleProcess] = []
    for item in items if isinstance(items, list) else []:
        try:
            processes.append(
                FlexibleProcess(
                    process_id=str(item["process_id"]),
                    name=str(item.get("name", item["process_id"])),
                    load_mw=float(item["load_mw"]),
                    duration_windows=int(item.get("duration_windows", 1)),
                    earliest_start=int(item.get("earliest_start", 0)),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.error("Skipping malformed process entry %r: %s", item, exc)
    return processes or list(DEFAULT_PROCESSES)


async def load_windows(
    session_factory: async_sessionmaker, horizon: int
) -> list[IntensityWindow]:
    """Newest ``horizon`` grid windows, ascending, with effective intensity."""
    async with session_factory() as session:
        rows = list(
            (
                await session.execute(
                    select(GridTelemetryRow)
                    .order_by(GridTelemetryRow.window_from.desc())
                    .limit(horizon)
                )
            )
            .scalars()
            .all()
        )

    rows.reverse()  # oldest → newest, which is the order a schedule is read in
    return [
        IntensityWindow(
            window_from=row.window_from,
            window_to=row.window_to,
            intensity=float(
                row.actual_intensity
                if row.actual_intensity is not None
                else row.forecast_intensity
            ),
        )
        for row in rows
    ]


async def load_capacity(
    session_factory: async_sessionmaker, windows: list[IntensityWindow]
) -> list[float] | None:
    """Flexible capacity aligned to ``windows`` — one entry per grid window.

    Capacity comes from the AS400 bridge: how much of the plant's load is
    actually movable. Without it the solver schedules as if capacity were
    unlimited, which is why this is treated as an *input*, not an assumption.

    The result **must** be indexed by the grid windows, not by the plant
    windows. Building it from the plant side produced a list as long as the
    plant's history, which is usually shorter than the grid's — and the solver
    then indexed past the end of it. That failure was invisible in two ways: it
    only appeared once plant telemetry existed (before that the function returns
    ``None``), and the optimizer loop swallowed the resulting IndexError and
    carried on, so the API simply returned 500 with no new schedule.
    """
    if not windows:
        return None

    async with session_factory() as session:
        rows = list(
            (
                await session.execute(
                    select(PlantTelemetryRow)
                    .order_by(PlantTelemetryRow.window_from.desc())
                    .limit(max(len(windows) * 4, 8))
                )
            )
            .scalars()
            .all()
        )
    if not rows:
        logger.info("No plant telemetry yet — scheduling without a capacity ceiling")
        return None

    per_window: dict[datetime, float] = {}
    for row in rows:
        per_window[row.window_from] = max(
            per_window.get(row.window_from, 0.0), row.flexible_load_mw
        )

    # A grid window the plant has not reported for inherits the most recent
    # known capacity rather than dropping out of the list.
    fallback = max(per_window.values())
    return [per_window.get(window.window_from, fallback) for window in windows]


# --------------------------------------------------------------------------- #
# Planning
# --------------------------------------------------------------------------- #


async def build_plan(
    session_factory: async_sessionmaker,
    settings: PlatformSettings,
    *,
    runner: DatabricksRunner | None = None,
) -> Plan:
    """Read inputs and produce a plan. Databricks if reachable, else local."""
    horizon = settings.optimizer_horizon_windows
    processes = load_processes(settings)
    windows = await load_windows(session_factory, horizon)

    if len(windows) < 2:
        logger.warning(
            "Only %d grid window(s) available — nothing to optimise yet", len(windows)
        )
        return Plan(
            run_id=uuid.uuid4().hex,
            solver="none",
            horizon_windows=len(windows),
            decisions=[],
            baseline_carbon_kg=0.0,
            optimized_carbon_kg=0.0,
            carbon_saved_kg=0.0,
            unscheduled=[p.process_id for p in processes],
            notes=["insufficient grid telemetry: need at least 2 retained windows"],
        )

    capacity = await load_capacity(session_factory, windows)
    runner = runner or DatabricksRunner(settings)

    plan = await runner.run_remote(windows, processes)
    if plan is None:
        plan = solve(windows, processes, flexible_capacity_mw=capacity)
    return plan


async def persist_plan(
    session_factory: async_sessionmaker, plan: Plan, process_count: int
) -> None:
    """Write the run and its decisions. Replaying a run_id is a no-op upsert."""
    async with session_factory() as session:
        run_stmt = pg_insert(OptimizationRunRow).values(
            run_id=plan.run_id,
            horizon_windows=plan.horizon_windows,
            solver=plan.solver,
            baseline_carbon_kg=plan.baseline_carbon_kg,
            optimized_carbon_kg=plan.optimized_carbon_kg,
            carbon_saved_kg=plan.carbon_saved_kg,
            process_count=process_count,
            decision_count=len(plan.decisions),
            unscheduled=list(plan.unscheduled),
            notes=list(plan.notes),
        )
        await session.execute(
            run_stmt.on_conflict_do_update(
                index_elements=["run_id"],
                set_={
                    "solver": run_stmt.excluded.solver,
                    "baseline_carbon_kg": run_stmt.excluded.baseline_carbon_kg,
                    "optimized_carbon_kg": run_stmt.excluded.optimized_carbon_kg,
                    "carbon_saved_kg": run_stmt.excluded.carbon_saved_kg,
                    "decision_count": run_stmt.excluded.decision_count,
                    "unscheduled": run_stmt.excluded.unscheduled,
                    "notes": run_stmt.excluded.notes,
                },
            )
        )

        if plan.decisions:
            rows = [
                {
                    "run_id": plan.run_id,
                    "process_id": d.process_id,
                    "process_name": d.process_name,
                    "window_from": d.window_from,
                    "window_to": d.window_to,
                    "action": d.action,
                    "load_mw": d.load_mw,
                    "intensity": d.intensity,
                    "carbon_kg": d.carbon_kg,
                    "baseline_carbon_kg": d.baseline_carbon_kg,
                    "carbon_saved_kg": d.carbon_saved_kg,
                    "reason": d.reason,
                }
                for d in plan.decisions
            ]
            dec_stmt = pg_insert(ScheduleDecisionRow).values(rows)
            await session.execute(
                dec_stmt.on_conflict_do_update(
                    constraint="uq_schedule_decision",
                    set_={
                        "action": dec_stmt.excluded.action,
                        "load_mw": dec_stmt.excluded.load_mw,
                        "intensity": dec_stmt.excluded.intensity,
                        "carbon_kg": dec_stmt.excluded.carbon_kg,
                        "baseline_carbon_kg": dec_stmt.excluded.baseline_carbon_kg,
                        "carbon_saved_kg": dec_stmt.excluded.carbon_saved_kg,
                        "reason": dec_stmt.excluded.reason,
                    },
                )
            )
        await session.commit()

    logger.info(
        "Persisted run %s | solver=%s decisions=%d saved=%.1f kg CO2",
        plan.run_id,
        plan.solver,
        len(plan.decisions),
        plan.carbon_saved_kg,
    )


def plan_payload(plan: Plan) -> dict[str, Any]:
    """Compact wire form published to the decisions topic."""
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": plan.run_id,
        "solver": plan.solver,
        "horizon_windows": plan.horizon_windows,
        "baseline_carbon_kg": round(plan.baseline_carbon_kg, 3),
        "optimized_carbon_kg": round(plan.optimized_carbon_kg, 3),
        "carbon_saved_kg": round(plan.carbon_saved_kg, 3),
        "saving_pct": round(plan.saving_pct, 2),
        "scheduled": [
            {
                "process_id": d.process_id,
                "window_from": d.window_from.isoformat(),
                "window_to": d.window_to.isoformat(),
                "load_mw": d.load_mw,
            }
            for d in plan.decisions
            if d.action == "run"
        ],
        "unscheduled": list(plan.unscheduled),
        "published_at": datetime.now(timezone.utc).isoformat(),
    }


async def publish_plan(producer: Any, settings: PlatformSettings, plan: Plan) -> bool:
    """Publish the schedule. Never raises — the DB write is the durable step."""
    if producer is None:
        return False
    payload = plan_payload(plan)
    try:
        await asyncio.wait_for(
            producer.send_and_wait(
                settings.kafka_decisions_topic,
                key=plan.run_id.encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
            ),
            timeout=15.0,
        )
        logger.info("Published schedule %s to %s", plan.run_id, settings.kafka_decisions_topic)
        return True
    except asyncio.TimeoutError:
        logger.warning(
            "Publishing schedule %s timed out (topic=%s)",
            plan.run_id,
            settings.kafka_decisions_topic,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Publishing schedule %s failed: %s", plan.run_id, exc)
    return False


class _PlanResult:
    """Convenience container so callers get plan + publish outcome together."""

    def __init__(self, plan: Plan, published: bool) -> None:
        self.plan = plan
        self.published = published


async def run_once(
    session_factory: async_sessionmaker,
    settings: PlatformSettings,
    *,
    producer: Any = None,
    runner: DatabricksRunner | None = None,
) -> _PlanResult:
    """One optimisation pass: plan → persist → publish."""
    processes = load_processes(settings)
    plan = await build_plan(session_factory, settings, runner=runner)
    if plan.decisions:
        await persist_plan(session_factory, plan, len(processes))
    published = await publish_plan(producer, settings, plan)
    return _PlanResult(plan, published)


# --------------------------------------------------------------------------- #
# Service
# --------------------------------------------------------------------------- #


class OptimizationLoop:
    """Runs :func:`run_once` on a fixed cadence."""

    def __init__(self, settings: PlatformSettings, producer: Any = None) -> None:
        self._settings = settings
        self._producer = producer
        self._own_producer = producer is None
        self._engine = None
        self._session_factory: async_sessionmaker | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._engine = create_engine(self._settings)
        self._session_factory = create_session_factory(self._engine)
        if self._producer is None:
            from aiokafka import AIOKafkaProducer

            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                client_id=self._settings.optimizer_client_id,
                acks="all",
                enable_idempotence=True,
                compression_type="gzip",
                **security_kwargs(self._settings),
            )
            await self._producer.start()
        logger.info(
            "Optimizer started | horizon=%d interval=%ss topic=%s",
            self._settings.optimizer_horizon_windows,
            self._settings.optimizer_interval_seconds,
            self._settings.kafka_decisions_topic,
        )

    async def stop(self) -> None:
        if self._producer is not None and self._own_producer:
            await self._producer.stop()
        if self._engine is not None:
            await self._engine.dispose()
        logger.info("Optimizer stopped")

    def request_stop(self, *_: Any) -> None:
        logger.info("Shutdown requested — finishing current pass")
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                signal.signal(sig, lambda s, _f: self.request_stop(s))

    async def run(self) -> int:
        self._install_signal_handlers()
        await self.start()
        try:
            while not self._stop.is_set():
                try:
                    result = await run_once(
                        self._session_factory, self._settings, producer=self._producer
                    )
                    logger.info(
                        "Optimizer pass | run=%s solver=%s saved=%.1f kg (%.1f%%) published=%s",
                        result.plan.run_id,
                        result.plan.solver,
                        result.plan.carbon_saved_kg,
                        result.plan.saving_pct,
                        result.published,
                    )
                except Exception:  # noqa: BLE001 — one bad pass must not kill the loop
                    logger.exception("Optimizer pass failed — continuing")
                await asyncio.sleep(self._settings.optimizer_interval_seconds)
        finally:
            await self.stop()
        return 0


async def run() -> int:
    settings = PlatformSettings()
    configure_logging(settings.log_level)
    loop = OptimizationLoop(settings)
    try:
        return await loop.run()
    except Exception:  # noqa: BLE001
        logger.critical("Optimizer terminated with an unhandled error", exc_info=True)
        return 1


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
