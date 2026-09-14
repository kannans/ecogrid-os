"""Assemble the context the advisor reasons over.

Everything the model sees is assembled here, from the database, in one place.
That matters for two reasons: the prompt is reproducible from stored state, and
the exact inputs are persisted alongside the advice so a decision can be audited
later ("what did the orchestrator know, and when?").
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from ecogrid.models import GridTelemetryRow, OptimizationRunRow, PlantTelemetryRow

#: How many retained windows the advisor is shown.
CONTEXT_WINDOWS = 48


def _effective_intensity(row: GridTelemetryRow) -> float:
    """Measured when settled, otherwise the forecast."""
    value = row.actual_intensity if row.actual_intensity is not None else row.forecast_intensity
    return float(value)


def _grid_summary(rows: list[GridTelemetryRow]) -> dict[str, Any]:
    if not rows:
        return {
            "window_count": 0,
            "forecast_only_count": 0,
            "avg_actual_intensity": None,
            "avg_renewable_percentage": None,
            "cleanest_window": None,
            "dirtiest_window": None,
        }

    scored = [(row, _effective_intensity(row)) for row in rows]
    settled = [value for row, value in scored if row.actual_intensity is not None]

    cleanest = min(scored, key=lambda pair: pair[1])
    dirtiest = max(scored, key=lambda pair: pair[1])

    return {
        "window_count": len(rows),
        "forecast_only_count": sum(1 for row in rows if row.is_forecast_only),
        # Only *settled* windows inform an average actual intensity; mixing in
        # forecasts would understate or overstate the real grid.
        "avg_actual_intensity": (round(sum(settled) / len(settled), 2) if settled else None),
        "avg_renewable_percentage": round(
            sum(row.renewable_percentage for row in rows) / len(rows), 2
        ),
        "cleanest_window": {
            "window_from": cleanest[0].window_from.isoformat(),
            "intensity": round(cleanest[1], 2),
            "carbon_index": cleanest[0].carbon_index,
            "is_forecast_only": cleanest[0].is_forecast_only,
        },
        "dirtiest_window": {
            "window_from": dirtiest[0].window_from.isoformat(),
            "intensity": round(dirtiest[1], 2),
            "carbon_index": dirtiest[0].carbon_index,
            "is_forecast_only": dirtiest[0].is_forecast_only,
        },
    }


def _plan_summary(run: OptimizationRunRow | None) -> dict[str, Any]:
    if run is None:
        return {}
    saving_pct = (
        round(100.0 * run.carbon_saved_kg / run.baseline_carbon_kg, 2)
        if run.baseline_carbon_kg > 0
        else 0.0
    )
    return {
        "run_id": run.run_id,
        "solver": run.solver,
        "horizon_windows": run.horizon_windows,
        "decision_count": run.decision_count,
        "carbon_saved_kg": round(run.carbon_saved_kg, 3),
        "baseline_carbon_kg": round(run.baseline_carbon_kg, 3),
        "optimized_carbon_kg": round(run.optimized_carbon_kg, 3),
        "saving_pct": saving_pct,
        "unscheduled": list(run.unscheduled or []),
        "notes": list(run.notes or []),
    }


def _plant_summary(rows: list[PlantTelemetryRow]) -> dict[str, Any]:
    if not rows:
        return {}
    newest: dict[str, PlantTelemetryRow] = {}
    for row in rows:
        existing = newest.get(row.plant_id)
        if existing is None or row.window_from > existing.window_from:
            newest[row.plant_id] = row
    latest = list(newest.values())
    return {
        "plant_count": len(latest),
        "total_load_mw": round(sum(r.total_load_mw for r in latest), 3),
        "total_flexible_mw": round(sum(r.flexible_load_mw for r in latest), 3),
        "any_estimated": any(r.is_estimate for r in latest),
    }


async def build_context(
    session_factory: async_sessionmaker, horizon_windows: int = CONTEXT_WINDOWS
) -> dict[str, Any]:
    """Read the platform state an advisor needs. Cheap: a handful of queries."""
    async with session_factory() as session:
        grid_rows = list(
            (
                await session.execute(
                    select(GridTelemetryRow)
                    .order_by(GridTelemetryRow.window_from.desc())
                    .limit(horizon_windows)
                )
            )
            .scalars()
            .all()
        )
        run = (
            await session.scalars(
                select(OptimizationRunRow)
                .order_by(OptimizationRunRow.created_at.desc())
                .limit(1)
            )
        ).first()
        plant_rows = list(
            (
                await session.execute(
                    select(PlantTelemetryRow)
                    .order_by(PlantTelemetryRow.window_from.desc())
                    .limit(20)
                )
            )
            .scalars()
            .all()
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "grid": _grid_summary(grid_rows),
        "plan": _plan_summary(run),
        "plant": _plant_summary(plant_rows),
    }
