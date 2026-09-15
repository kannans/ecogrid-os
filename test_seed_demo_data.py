"""Seeder tests.

The seeder publishes synthetic telemetry **through the real pipeline**, so its
records must satisfy the *consumer's* models exactly — otherwise the data is
rejected at ingest and the whole exercise is pointless.

The first run of the script was rejected by ``PlantTelemetry`` for a 0.001 MW
rounding overshoot: rounding total, flexible and inflexible independently lets
the two parts sum to slightly more than the whole. That is the same trap already
documented in ``SimulatedPlantSource``, so it is pinned here rather than
rediscovered.
"""

from __future__ import annotations

import pathlib
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "scripts"))

from ecogrid.plant.models import PlantTelemetry  # noqa: E402
from ingest_grid import CarbonIndex, GridTelemetry  # noqa: E402
from seed_demo_data import (  # noqa: E402
    band_for,
    build_grid_window,
    build_plant_window,
    current_window,
    intensity_for,
)


def _windows(count: int) -> list[datetime]:
    now = datetime.now(timezone.utc)
    base = current_window(now)
    return [base - timedelta(minutes=30 * i) for i in range(count)]


def test_grid_records_satisfy_the_consumer_contract() -> None:
    now = datetime.now(timezone.utc)
    for window in _windows(24):
        GridTelemetry.model_validate_json(build_grid_window(window, now).to_kafka_value())


def test_plant_records_satisfy_the_consumer_contract() -> None:
    """Regression: independently rounding the three MW figures overshot the total."""
    now = datetime.now(timezone.utc)
    for window in _windows(48):
        record = build_plant_window(window, now)
        PlantTelemetry.model_validate_json(record.to_kafka_value())
        assert record.flexible_load_mw + record.inflexible_load_mw == pytest.approx(
            record.total_load_mw
        )


def test_band_mapping_matches_observed_upstream_behaviour() -> None:
    """Bands observed from the live API: 40 and 51 both returned 'low'."""
    assert band_for(10) is CarbonIndex.VERY_LOW
    assert band_for(40) is CarbonIndex.LOW
    assert band_for(51) is CarbonIndex.LOW
    assert band_for(266) is CarbonIndex.MODERATE
    assert band_for(350) is CarbonIndex.HIGH


def test_seeded_series_has_a_meaningful_spread() -> None:
    """A flat series makes the optimizer correctly report 0% saving — a useless demo."""
    values = [intensity_for(window) for window in _windows(48)]
    assert max(values) - min(values) > 100


def test_seeded_records_are_labelled_as_demo_data() -> None:
    now = datetime.now(timezone.utc)
    window = current_window(now)
    assert build_grid_window(window, now).source == "demo-seed"
    assert build_plant_window(window, now).source == "demo-seed-plant"


def test_importing_the_plant_model_does_not_require_aiokafka() -> None:
    """Regression: `from ecogrid.plant.models import PlantTelemetry` raised
    `ModuleNotFoundError: No module named 'aiokafka'`.

    The package `__init__` eagerly imported the consumer and the bridge, so
    reaching for a plain data contract dragged in the Kafka client. Run in a
    subprocess, because this process may already have aiokafka in sys.modules
    from other tests.
    """
    import subprocess
    import sys

    repo_root = pathlib.Path(__file__).resolve().parent
    code = (
        "import sys, ecogrid.plant.models as m; "
        "assert 'aiokafka' not in sys.modules, 'package __init__ pulled in aiokafka'; "
        "assert m.PlantTelemetry.__name__ == 'PlantTelemetry'; "
        "print('ok')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, cwd=repo_root
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


def test_lazy_reexports_still_work() -> None:
    """Lazy resolution must not break the convenience re-exports."""
    import ecogrid.plant as plant

    assert plant.PlantTelemetry is not None
    assert plant.build_source is not None
    assert callable(plant.run_bridge)
    with pytest.raises(AttributeError):
        _ = plant.definitely_not_a_real_name


def test_only_the_newest_window_is_forecast_only() -> None:
    """Settled history must look settled, or the advisor will warn about forecasts."""
    now = datetime.now(timezone.utc)
    windows = _windows(6)
    flags = [build_grid_window(w, now).is_forecast_only for w in windows]
    assert flags[0] is True, "the newest window is still in progress"
    assert not any(flags[1:]), "older windows should be settled"
