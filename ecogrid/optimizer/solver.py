"""Carbon-arbitrage solver — the "Databricks optimization loop" in pure Python.

The idea
--------
Industrial plants have two kinds of load. **Inflexible** load must run whenever it
runs. **Flexible** load — batch mills, electrolysers, thermal stores — can be moved
between settlement windows. Grid carbon intensity swings by 3–5x across a day, so
*when* flexible load runs changes the emissions bill even though the total energy
is identical.

This module answers: given a forecast of half-hourly carbon intensity and a set of
flexible processes, which windows should each process run in?

It is a greedy scheduler, not a MILP:

* Each process needs a **contiguous** block of windows (a batch cannot be paused
  halfway without wrecking the product).
* Blocks are chosen by minimum total intensity, subject to the flexible capacity
  still available in each window.
* Heaviest processes are placed first, because they have the most to gain.

That is deliberately simple enough to read and to test. In production this is the
job Databricks runs (see :mod:`ecogrid.optimizer.databricks`) — the local solver
exists so the platform is fully functional, and verifiable, without a cluster.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

LOCAL_SOLVER = "local-greedy-v1"

#: Used when no plant telemetry is available: no capacity ceiling.
UNLIMITED = float("inf")


@dataclass(frozen=True)
class IntensityWindow:
    """Carbon intensity for one settlement window."""

    window_from: datetime
    window_to: datetime
    #: gCO2/kWh — the *effective* figure (measured when settled, else forecast).
    intensity: float


@dataclass(frozen=True)
class FlexibleProcess:
    """A load that may be moved in time."""

    process_id: str
    name: str
    #: Steady draw while running, in MW.
    load_mw: float
    #: Contiguous windows the process must occupy once started.
    duration_windows: int
    #: Earliest window index it may start at (0 = horizon start).
    earliest_start: int = 0


@dataclass(frozen=True)
class Decision:
    """One process × one window. The full schedule is the set of these."""

    process_id: str
    process_name: str
    window_from: datetime
    window_to: datetime
    #: ``run`` — the process is scheduled here. ``idle`` — it is not.
    action: str
    load_mw: float
    intensity: float
    carbon_kg: float
    baseline_carbon_kg: float
    carbon_saved_kg: float
    reason: str


@dataclass(frozen=True)
class Plan:
    """A complete optimisation run."""

    run_id: str
    solver: str
    horizon_windows: int
    decisions: list[Decision]
    baseline_carbon_kg: float
    optimized_carbon_kg: float
    carbon_saved_kg: float
    #: Processes that could not be placed at all (horizon too short, no capacity).
    unscheduled: list[str]
    notes: list[str]

    @property
    def scheduled_processes(self) -> list[str]:
        return sorted({d.process_id for d in self.decisions if d.action == "run"})

    @property
    def saving_pct(self) -> float:
        """Percentage reduction vs the naive run-as-early-as-possible baseline."""
        if self.baseline_carbon_kg <= 0:
            return 0.0
        return 100.0 * self.carbon_saved_kg / self.baseline_carbon_kg


def _window_hours(window: IntensityWindow) -> float:
    """Window length in hours, used to turn MW into kWh."""
    delta = window.window_to - window.window_from
    return max(delta.total_seconds() / 3600.0, 0.0)


def solve(
    windows: list[IntensityWindow],
    processes: list[FlexibleProcess],
    *,
    flexible_capacity_mw: list[float] | None = None,
    run_id: str | None = None,
    solver: str = LOCAL_SOLVER,
) -> Plan:
    """Schedule every flexible process into the lowest-carbon feasible block.

    ``flexible_capacity_mw`` bounds how much flexible load each window can take
    (from plant telemetry). Omit it to schedule without a capacity ceiling.
    """
    horizon = len(windows)

    # Capacity is indexed by grid window, so a short list would silently walk off
    # the end during placement. Fail with something actionable instead of an
    # IndexError three frames deep — this exact misalignment (capacity built from
    # plant history rather than grid history) reached production once.
    if flexible_capacity_mw is not None and len(flexible_capacity_mw) != horizon:
        raise ValueError(
            f"flexible_capacity_mw has {len(flexible_capacity_mw)} entr(ies) but the "
            f"horizon has {horizon} window(s); capacity must be aligned per grid window"
        )

    capacity = (
        list(flexible_capacity_mw)
        if flexible_capacity_mw is not None
        else [UNLIMITED] * horizon
    )
    remaining = list(capacity)

    decisions: list[Decision] = []
    unscheduled: list[str] = []
    notes: list[str] = []
    baseline_total = 0.0
    optimized_total = 0.0

    # Heaviest first: they capture the largest absolute saving, and lighter
    # processes can still fit around them.
    for proc in sorted(processes, key=lambda p: (-p.load_mw, p.process_id)):
        duration = max(1, int(proc.duration_windows))
        if duration > horizon:
            unscheduled.append(proc.process_id)
            notes.append(
                f"{proc.process_id}: duration {duration} exceeds horizon of {horizon} window(s)"
            )
            continue

        start_lo = max(0, int(proc.earliest_start))
        start_hi = horizon - duration
        if start_lo > start_hi:
            unscheduled.append(proc.process_id)
            notes.append(
                f"{proc.process_id}: earliest_start {start_lo} leaves no room for "
                f"{duration} window(s) in a horizon of {horizon}"
            )
            continue

        # Cheapest feasible contiguous block by total intensity.
        best: tuple[float, int] | None = None
        for start in range(start_lo, start_hi + 1):
            if any(remaining[start + k] < proc.load_mw - 1e-9 for k in range(duration)):
                continue
            total_intensity = sum(windows[start + k].intensity for k in range(duration))
            if best is None or total_intensity < best[0]:
                best = (total_intensity, start)

        if best is None:
            unscheduled.append(proc.process_id)
            notes.append(
                f"{proc.process_id}: no contiguous block with {proc.load_mw} MW free "
                f"for {duration} window(s)"
            )
            continue

        start = best[1]
        # Baseline is the naive schedule: run at the earliest legal moment.
        baseline_start = start_lo
        for k in range(duration):
            remaining[start + k] -= proc.load_mw

        # Totals are computed per BLOCK, then shared evenly across its windows.
        #
        # Attributing savings positionally — this window against the baseline
        # window at the same offset — is wrong: the first window of a clean block
        # can still be dirtier than the first window of an early block, which
        # produces a negative "saving" on an individual row even though the move
        # is a clear win overall. Sharing the block saving keeps every row honest
        # while the row totals still add up exactly to the block totals.
        block_optimized = sum(
            proc.load_mw * _window_hours(windows[start + k]) * windows[start + k].intensity
            for k in range(duration)
        )
        block_baseline = sum(
            proc.load_mw
            * _window_hours(windows[baseline_start + k])
            * windows[baseline_start + k].intensity
            for k in range(duration)
        )
        per_window_baseline = block_baseline / duration
        per_window_saved = (block_baseline - block_optimized) / duration

        for idx, window in enumerate(windows):
            if start <= idx < start + duration:
                hours = _window_hours(window)
                carbon = proc.load_mw * hours * window.intensity
                decisions.append(
                    Decision(
                        process_id=proc.process_id,
                        process_name=proc.name,
                        window_from=window.window_from,
                        window_to=window.window_to,
                        action="run",
                        load_mw=proc.load_mw,
                        intensity=window.intensity,
                        carbon_kg=carbon,
                        baseline_carbon_kg=per_window_baseline,
                        carbon_saved_kg=per_window_saved,
                        reason=f"lowest-carbon contiguous block (start index {start})",
                    )
                )
                optimized_total += carbon
                baseline_total += per_window_baseline
            else:
                decisions.append(
                    Decision(
                        process_id=proc.process_id,
                        process_name=proc.name,
                        window_from=window.window_from,
                        window_to=window.window_to,
                        action="idle",
                        load_mw=0.0,
                        intensity=window.intensity,
                        carbon_kg=0.0,
                        baseline_carbon_kg=0.0,
                        carbon_saved_kg=0.0,
                        reason="not selected — cleaner windows were available",
                    )
                )

    return Plan(
        run_id=run_id or uuid.uuid4().hex,
        solver=solver,
        horizon_windows=horizon,
        decisions=decisions,
        baseline_carbon_kg=baseline_total,
        optimized_carbon_kg=optimized_total,
        carbon_saved_kg=baseline_total - optimized_total,
        unscheduled=unscheduled,
        notes=notes,
    )
