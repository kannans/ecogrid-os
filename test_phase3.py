"""Phase 3 tests — plant bridge + optimisation loop.

All of these run offline: no broker, no PostgreSQL, no Databricks. The solver is
pure functions over plain data, which is exactly why it is testable without a
cluster.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from ecogrid.config import PlatformSettings
from ecogrid.optimizer.databricks import DatabricksRunner
from ecogrid.optimizer.loop import load_processes, plan_payload
from ecogrid.optimizer.solver import FlexibleProcess, IntensityWindow, solve
from ecogrid.plant.models import PlantTelemetry
from ecogrid.plant.sources import (
    FilePlantSource,
    SimulatedPlantSource,
    build_source,
)

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def plant_payload(plant_id: str = "plant-01", **overrides: object) -> dict:
    """A valid plant record as a plain dict."""
    payload = {
        "plant_id": plant_id,
        "plant_name": "Test plant",
        "window_from": BASE.isoformat(),
        "window_to": (BASE + timedelta(minutes=30)).isoformat(),
        "total_load_mw": 40.0,
        "flexible_load_mw": 14.0,
        "inflexible_load_mw": 26.0,
        "ingested_at": BASE.isoformat(),
    }
    payload.update(overrides)  # type: ignore[arg-type]
    return payload


def make_windows(intensities: list[float]) -> list[IntensityWindow]:
    """Half-hourly windows starting at BASE with the given intensities."""
    return [
        IntensityWindow(
            window_from=BASE + timedelta(minutes=30 * i),
            window_to=BASE + timedelta(minutes=30 * (i + 1)),
            intensity=float(value),
        )
        for i, value in enumerate(intensities)
    ]


# --------------------------------------------------------------------------- #
# PlantTelemetry contract
# --------------------------------------------------------------------------- #


def test_valid_plant_record_is_accepted() -> None:
    record = PlantTelemetry.model_validate(plant_payload())
    assert record.plant_id == "plant-01"
    assert record.total_load_mw == 40.0


def test_inverted_window_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PlantTelemetry.model_validate(
            plant_payload(
                window_from=(BASE + timedelta(minutes=30)).isoformat(),
                window_to=BASE.isoformat(),
            )
        )


def test_load_split_must_not_exceed_total() -> None:
    with pytest.raises(ValidationError):
        PlantTelemetry.model_validate(
            plant_payload(total_load_mw=10.0, flexible_load_mw=8.0, inflexible_load_mw=8.0)
        )


def test_kafka_key_is_plant_plus_window() -> None:
    record = PlantTelemetry.model_validate(plant_payload(plant_id="plant-07"))
    assert record.kafka_key.startswith("plant-07:")
    assert record.kafka_key.endswith("2026-01-01T00:00:00Z")


def test_payload_hash_ignores_ingestion_time() -> None:
    """Two deliveries of the same window must hash identically."""
    first = PlantTelemetry.model_validate(plant_payload())
    second = PlantTelemetry.model_validate(
        plant_payload(ingested_at=(BASE + timedelta(seconds=90)).isoformat())
    )
    assert first.payload_hash == second.payload_hash


def test_payload_hash_tracks_content() -> None:
    first = PlantTelemetry.model_validate(plant_payload())
    revised = PlantTelemetry.model_validate(plant_payload(flexible_load_mw=20.0, inflexible_load_mw=20.0))
    assert first.payload_hash != revised.payload_hash


# --------------------------------------------------------------------------- #
# Sources
# --------------------------------------------------------------------------- #


async def test_simulated_source_is_deterministic() -> None:
    settings = PlatformSettings(plant_ids="plant-01,plant-02")
    source = SimulatedPlantSource(settings)

    first = await source.fetch(BASE, BASE + timedelta(minutes=30))
    second = await source.fetch(BASE, BASE + timedelta(minutes=30))

    assert [r.plant_id for r in first] == ["plant-01", "plant-02"]
    # ingested_at differs between calls; the content hash must not.
    assert [r.payload_hash for r in first] == [r.payload_hash for r in second]


async def test_simulated_source_respects_load_split() -> None:
    settings = PlatformSettings(plant_base_load_mw=100.0, plant_flexible_fraction=0.4)
    source = SimulatedPlantSource(settings, plant_ids=["plant-01"])
    (record,) = await source.fetch(BASE, BASE + timedelta(minutes=30))

    assert record.flexible_load_mw == pytest.approx(record.total_load_mw * 0.4)
    assert record.flexible_load_mw + record.inflexible_load_mw == pytest.approx(
        record.total_load_mw
    )
    assert record.is_estimate is True


async def test_file_source_reads_json_and_jsonl(tmp_path) -> None:
    (tmp_path / "drop.json").write_text(json.dumps([plant_payload("plant-a")]))
    (tmp_path / "drop.jsonl").write_text(
        json.dumps(plant_payload("plant-b", total_load_mw=10.0, flexible_load_mw=2.0, inflexible_load_mw=8.0))
        + "\n"
    )

    source = FilePlantSource(str(tmp_path))
    records = await source.fetch(BASE, BASE + timedelta(minutes=30))

    assert {r.plant_id for r in records} == {"plant-a", "plant-b"}


async def test_file_source_skips_corrupt_entries(tmp_path) -> None:
    (tmp_path / "drop.jsonl").write_text(
        json.dumps(plant_payload("plant-ok")) + "\n" + "{not json at all}\n"
    )
    source = FilePlantSource(str(tmp_path))
    records = await source.fetch(BASE, BASE + timedelta(minutes=30))
    assert [r.plant_id for r in records] == ["plant-ok"]


async def test_file_source_missing_directory_is_empty(tmp_path) -> None:
    source = FilePlantSource(str(tmp_path / "nope"))
    assert await source.fetch(BASE, BASE + timedelta(minutes=30)) == []


def test_build_source_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unknown ECOGRID_PLANT_SOURCE"):
        build_source(PlatformSettings(plant_source="carrier-pigeon"))


def test_build_source_defaults_to_simulated() -> None:
    assert isinstance(build_source(PlatformSettings()), SimulatedPlantSource)


# --------------------------------------------------------------------------- #
# Solver
# --------------------------------------------------------------------------- #


def test_process_is_scheduled_into_the_cleanest_block() -> None:
    # Dirty early, clean late: the optimiser must push the batch later.
    windows = make_windows([300.0, 300.0, 300.0, 100.0, 100.0, 100.0])
    process = FlexibleProcess("mill", "Batch mill", 10.0, 2)

    plan = solve(windows, [process])

    run_windows = [d.window_from for d in plan.decisions if d.action == "run"]
    expected = [windows[3].window_from, windows[4].window_from]
    assert run_windows == expected

    # 10 MW × 0.5 h × (300+300) baseline vs (100+100) optimised.
    assert plan.baseline_carbon_kg == pytest.approx(3000.0)
    assert plan.optimized_carbon_kg == pytest.approx(1000.0)
    assert plan.carbon_saved_kg == pytest.approx(2000.0)
    assert plan.saving_pct == pytest.approx(66.67, abs=0.01)


def test_every_window_gets_a_decision_per_process() -> None:
    windows = make_windows([200.0, 100.0, 300.0, 400.0])
    plan = solve(windows, [FlexibleProcess("p1", "P1", 5.0, 1)])

    assert len(plan.decisions) == len(windows)
    assert sum(1 for d in plan.decisions if d.action == "run") == 1
    assert sum(1 for d in plan.decisions if d.action == "idle") == len(windows) - 1


def test_carbon_math_per_decision() -> None:
    windows = make_windows([250.0, 250.0, 50.0])
    plan = solve(windows, [FlexibleProcess("p1", "P1", 8.0, 1)])

    run = next(d for d in plan.decisions if d.action == "run")
    assert run.window_from == windows[2].window_from
    # 8 MW × 0.5 h × 50 g/kWh = 200 kg
    assert run.carbon_kg == pytest.approx(200.0)

    idle = next(d for d in plan.decisions if d.action == "idle")
    assert idle.carbon_kg == 0.0
    assert idle.load_mw == 0.0


def test_saving_is_never_negative() -> None:
    """The chosen block is by construction no dirtier than the earliest one."""
    windows = make_windows([10.0, 500.0, 480.0, 20.0, 15.0])
    plan = solve(windows, [FlexibleProcess("p1", "P1", 6.0, 2)])
    assert plan.carbon_saved_kg >= 0
    assert all(d.carbon_saved_kg >= 0 for d in plan.decisions)


def test_capacity_shortfall_leaves_process_unscheduled() -> None:
    windows = make_windows([100.0, 100.0, 100.0])
    process = FlexibleProcess("big", "Too big", 50.0, 1)

    plan = solve(windows, [process], flexible_capacity_mw=[10.0, 10.0, 10.0])

    assert plan.unscheduled == ["big"]
    assert plan.decisions == []
    assert plan.notes and "no contiguous block" in plan.notes[0]


def test_duration_longer_than_horizon_is_unscheduled() -> None:
    windows = make_windows([100.0, 100.0])
    plan = solve(windows, [FlexibleProcess("long", "Long", 5.0, 5)])
    assert plan.unscheduled == ["long"]


def test_second_process_fits_around_the_first() -> None:
    # One clean window only: the heavier process should win it.
    windows = make_windows([400.0, 50.0, 400.0])
    heavy = FlexibleProcess("heavy", "Heavy", 20.0, 1)
    light = FlexibleProcess("light", "Light", 5.0, 1)

    plan = solve(windows, [light, heavy], flexible_capacity_mw=[30.0, 20.0, 30.0])

    winner = next(d for d in plan.decisions if d.process_id == "heavy" and d.action == "run")
    assert winner.window_from == windows[1].window_from
    assert plan.unscheduled == []


# --------------------------------------------------------------------------- #
# Process portfolio + payload
# --------------------------------------------------------------------------- #


def test_default_portfolio_is_used_when_unset() -> None:
    processes = load_processes(PlatformSettings(optimizer_processes_json=None))
    assert len(processes) == 3
    assert all(p.load_mw > 0 for p in processes)


def test_portfolio_can_be_overridden_by_json() -> None:
    raw = json.dumps([{"process_id": "kiln", "name": "Kiln", "load_mw": 9.5, "duration_windows": 4}])
    processes = load_processes(PlatformSettings(optimizer_processes_json=raw))
    assert len(processes) == 1
    assert processes[0].process_id == "kiln"
    assert processes[0].duration_windows == 4


def test_invalid_portfolio_json_falls_back_to_defaults() -> None:
    processes = load_processes(PlatformSettings(optimizer_processes_json="{not json"))
    assert len(processes) == 3


def test_plan_payload_is_json_serialisable() -> None:
    windows = make_windows([300.0, 100.0])
    plan = solve(windows, [FlexibleProcess("p1", "P1", 4.0, 1)])
    encoded = json.dumps(plan_payload(plan))
    decoded = json.loads(encoded)
    assert decoded["run_id"] == plan.run_id
    assert decoded["carbon_saved_kg"] == pytest.approx(plan.carbon_saved_kg)
    assert decoded["scheduled"]


def test_misaligned_capacity_raises_a_clear_error() -> None:
    """Regression for a bug that reached a live deployment.

    Capacity was built from the *plant's* window history rather than the grid's,
    so it was shorter than the horizon whenever the plant had reported fewer
    windows. The solver then indexed past the end of the list, producing an
    IndexError, an HTTP 500, and — because the optimizer loop swallows per-pass
    failures — no new schedule and no obvious cause.
    """
    windows = make_windows([100.0, 200.0, 300.0, 400.0])
    process = FlexibleProcess("p1", "P1", 5.0, 2)

    with pytest.raises(ValueError, match="aligned per grid window"):
        solve(windows, [process], flexible_capacity_mw=[10.0, 10.0])


async def test_databricks_falls_back_when_the_cluster_is_unreachable() -> None:
    """UC-12: configured-but-dead Databricks must degrade, not fail.

    Returning ``None`` is the contract — it is the caller's signal to use the
    local solver. A scheduler that produces nothing because a cluster is down
    would be worse than one that produces a slightly worse schedule.
    """
    settings = PlatformSettings(
        databricks_host="http://127.0.0.1:9",  # deliberately nothing listening
        databricks_token="t",  # noqa: S106
        databricks_job_id="1",
    )
    runner = DatabricksRunner(settings)
    assert runner.is_configured

    plan = await runner.run_remote(
        make_windows([300.0, 100.0]),
        [FlexibleProcess("p1", "P1", 4.0, 1)],
        max_wait_seconds=1,
    )
    assert plan is None


async def test_databricks_is_skipped_entirely_when_unconfigured() -> None:
    runner = DatabricksRunner(PlatformSettings(databricks_host=None))
    assert not runner.is_configured
    assert await runner.run_remote(make_windows([1.0, 2.0]), []) is None


def test_capacity_matching_the_horizon_is_accepted() -> None:
    windows = make_windows([100.0, 200.0, 300.0, 400.0])
    plan = solve(
        windows,
        [FlexibleProcess("p1", "P1", 5.0, 2)],
        flexible_capacity_mw=[10.0] * 4,
    )
    assert plan.unscheduled == []
    assert len(plan.decisions) == 4
