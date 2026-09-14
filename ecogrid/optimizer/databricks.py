"""Databricks job submission — the production optimisation path.

The local solver (:mod:`ecogrid.optimizer.solver`) is deliberately small and
greedy. The production job on Databricks can afford to be a real MILP over a much
wider horizon, with price curves, ramp constraints and unit-commitment detail.

This module is the seam between the two:

* If ``ECOGRID_DATABRICKS_HOST`` / ``_TOKEN`` / ``_JOB_ID`` are set, the loop
  submits the job and uses whatever schedule it returns.
* If they are not set — or the job fails, times out, or returns something we
  cannot parse — the loop **falls back to the local solver** and logs it.

That fallback is a deliberate design choice, not laziness: an optimiser that
refuses to produce a schedule because a cluster is unreachable is worse than one
that produces a slightly worse schedule. Degradation is logged so it is visible.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

import httpx
from pydantic import BaseModel, Field

from ecogrid.config import PlatformSettings
from ecogrid.optimizer.solver import Decision, FlexibleProcess, IntensityWindow, Plan

logger = logging.getLogger("ecogrid.optimizer.databricks")

REMOTE_SOLVER = "databricks-job"


class DatabricksNotConfigured(RuntimeError):
    """Raised when the remote path is requested but credentials are absent."""


class RemoteDecision(BaseModel):
    """Shape the Databricks notebook is expected to emit."""

    process_id: str
    process_name: str = ""
    window_from: datetime
    window_to: datetime
    action: str = "run"
    load_mw: float = 0.0
    intensity: float = 0.0
    carbon_kg: float = 0.0
    baseline_carbon_kg: float = 0.0
    reason: str = "remote"


class RemotePlan(BaseModel):
    decisions: list[RemoteDecision] = Field(default_factory=list)
    baseline_carbon_kg: float = 0.0
    optimized_carbon_kg: float = 0.0


class DatabricksRunner:
    """Submits and reads back the Databricks optimisation job."""

    def __init__(self, settings: PlatformSettings) -> None:
        self._settings = settings

    @property
    def is_configured(self) -> bool:
        return bool(
            self._settings.databricks_host
            and self._settings.databricks_token
            and self._settings.databricks_job_id
        )

    def _url(self, path: str) -> str:
        return f"{self._settings.databricks_host.rstrip('/')}{path}"

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._settings.databricks_token}"}

    async def submit(self, payload: dict[str, Any]) -> str:
        """Trigger the job. Returns the Databricks run id."""
        if not self.is_configured:
            raise DatabricksNotConfigured(
                "Databricks is not configured (need ECOGRID_DATABRICKS_HOST, "
                "ECOGRID_DATABRICKS_TOKEN, ECOGRID_DATABRICKS_JOB_ID)"
            )
        body = {
            "job_id": int(self._settings.databricks_job_id),  # type: ignore[arg-type]
            "notebook_params": {"payload": json.dumps(payload)},
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                self._url("/api/2.1/jobs/run-now"), headers=self._headers(), json=body
            )
            response.raise_for_status()
            run_id = response.json().get("run_id")
        if not run_id:
            raise RuntimeError("Databricks run-now returned no run_id")
        return str(run_id)

    async def _wait_for_termination(self, run_id: str, max_wait_seconds: float) -> bool:
        """Poll until the run terminates. Returns True if it finished in time."""
        deadline = max_wait_seconds
        waited = 0.0
        async with httpx.AsyncClient(timeout=30.0) as client:
            while waited < deadline:
                response = await client.get(
                    self._url("/api/2.1/jobs/runs/get"),
                    headers=self._headers(),
                    params={"run_id": run_id},
                )
                response.raise_for_status()
                state = response.json().get("state", {})
                life_cycle = state.get("life_cycle_state")
                if life_cycle in {"TERMINATED", "SKIPPED"}:
                    result = state.get("result_state")
                    if result and result != "SUCCESS":
                        raise RuntimeError(f"Databricks run {run_id} finished {result}")
                    return True
                if life_cycle in {"INTERNAL_ERROR", "CANCELED", "CANCELLED"}:
                    raise RuntimeError(f"Databricks run {run_id} entered {life_cycle}")
                await asyncio.sleep(5.0)
                waited += 5.0
        logger.warning("Databricks run %s did not finish within %.0fs", run_id, max_wait_seconds)
        return False

    async def _fetch_result(self, run_id: str) -> RemotePlan:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                self._url("/api/2.1/jobs/runs/get-output"),
                headers=self._headers(),
                params={"run_id": run_id},
            )
            response.raise_for_status()
            payload = response.json()
        raw = (payload.get("notebook_output") or {}).get("result")
        if not raw:
            raise RuntimeError("Databricks run produced no notebook output")
        return RemotePlan.model_validate_json(raw)

    async def run_remote(
        self,
        windows: list[IntensityWindow],
        processes: list[FlexibleProcess],
        *,
        max_wait_seconds: float = 120.0,
    ) -> Plan | None:
        """Run the optimisation on Databricks.

        Returns ``None`` when the remote path is unavailable or fails, which is
        the signal for the caller to fall back to the local solver.
        """
        if not self.is_configured:
            return None

        payload = {
            "windows": [
                {
                    "window_from": w.window_from.isoformat(),
                    "window_to": w.window_to.isoformat(),
                    "intensity": w.intensity,
                }
                for w in windows
            ],
            "processes": [
                {
                    "process_id": p.process_id,
                    "name": p.name,
                    "load_mw": p.load_mw,
                    "duration_windows": p.duration_windows,
                    "earliest_start": p.earliest_start,
                }
                for p in processes
            ],
        }

        try:
            run_id = await self.submit(payload)
            logger.info("Submitted optimisation to Databricks | run_id=%s", run_id)
            if not await self._wait_for_termination(run_id, max_wait_seconds):
                return None
            remote = await self._fetch_result(run_id)
        except Exception as exc:  # noqa: BLE001 — any remote failure means fall back
            logger.warning(
                "Databricks optimisation failed (%s) — falling back to the local solver", exc
            )
            return None

        decisions = [
            Decision(
                process_id=d.process_id,
                process_name=d.process_name,
                window_from=d.window_from,
                window_to=d.window_to,
                action=d.action,
                load_mw=d.load_mw,
                intensity=d.intensity,
                carbon_kg=d.carbon_kg,
                baseline_carbon_kg=d.baseline_carbon_kg,
                carbon_saved_kg=d.baseline_carbon_kg - d.carbon_kg,
                reason=d.reason or "remote",
            )
            for d in remote.decisions
        ]

        baseline = remote.baseline_carbon_kg or sum(d.baseline_carbon_kg for d in decisions)
        optimized = remote.optimized_carbon_kg or sum(d.carbon_kg for d in decisions)
        logger.info("Databricks optimisation applied | run_id=%s decisions=%d", run_id, len(decisions))
        return Plan(
            run_id=run_id,
            solver=REMOTE_SOLVER,
            horizon_windows=len(windows),
            decisions=decisions,
            baseline_carbon_kg=baseline,
            optimized_carbon_kg=optimized,
            carbon_saved_kg=baseline - optimized,
            unscheduled=[],
            notes=[f"produced by Databricks run {run_id}"],
        )
