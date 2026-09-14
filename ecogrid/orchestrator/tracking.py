"""Experiment tracking for the orchestrator.

MLflow is the intended sink — it is what makes the optimizer's behaviour
comparable over time (which horizon, which portfolio, which solver produced the
best saving). But MLflow is a heavy dependency and is meaningless without a
tracking server, so:

* ``ECOGRID_MLFLOW_TRACKING_URI`` set **and** ``mlflow`` importable → MLflow.
* Otherwise → a local JSONL run log.

Neither path is allowed to break the orchestrator. Tracking is observation, not
control flow: a failed metric write must never stop advice from being produced.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from ecogrid.config import PlatformSettings

logger = logging.getLogger("ecogrid.orchestrator.tracking")


class TrackingSink(Protocol):
    """Somewhere to record one optimizer/orchestrator run."""

    def log_run(
        self, run_id: str, params: dict[str, Any], metrics: dict[str, float]
    ) -> None:
        """Record a run. Implementations must not raise."""


class FileTrackingSink:
    """Appends runs as JSONL. Best-effort — failures are logged, not raised."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    @property
    def path(self) -> Path:
        return self._path

    def log_run(
        self, run_id: str, params: dict[str, Any], metrics: dict[str, float]
    ) -> None:
        entry = {
            "run_id": run_id,
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "params": params,
            "metrics": metrics,
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, default=str))
                handle.write("\n")
        except OSError as exc:
            logger.warning("Could not append run %s to %s: %s", run_id, self._path, exc)


class MlflowTrackingSink:
    """Logs params/metrics to MLflow. Wrapped so failures degrade to a warning."""

    def __init__(self, tracking_uri: str, experiment: str) -> None:
        self._tracking_uri = tracking_uri
        self._experiment = experiment

    def log_run(
        self, run_id: str, params: dict[str, Any], metrics: dict[str, float]
    ) -> None:
        try:
            import mlflow  # imported lazily: optional dependency

            mlflow.set_tracking_uri(self._tracking_uri)
            mlflow.set_experiment(self._experiment)
            with mlflow.start_run(run_name=run_id):
                mlflow.log_params(params)
                # MLflow rejects non-finite metric values.
                mlflow.log_metrics(
                    {k: float(v) for k, v in metrics.items() if _is_finite(v)}
                )
        except ImportError:
            logger.warning("mlflow is not installed — run %s was not tracked", run_id)
        except Exception as exc:  # noqa: BLE001 — tracking must never break the run
            logger.warning("MLflow tracking failed for run %s: %s", run_id, exc)


def _is_finite(value: Any) -> bool:
    try:
        return value == value and value not in (float("inf"), float("-inf"))  # noqa: PLR0124
    except (TypeError, ValueError):
        return False


def build_tracker(settings: PlatformSettings) -> TrackingSink:
    """MLflow when configured and installed, otherwise the local JSONL sink."""
    if settings.mlflow_tracking_uri:
        try:
            import mlflow  # noqa: F401

            return MlflowTrackingSink(
                settings.mlflow_tracking_uri, settings.mlflow_experiment
            )
        except ImportError:
            logger.warning(
                "ECOGRID_MLFLOW_TRACKING_URI is set but mlflow is not installed "
                "(%s) — falling back to local JSONL tracking",
                "pip install mlflow",
            )
    return FileTrackingSink(settings.orchestrator_tracking_path)
