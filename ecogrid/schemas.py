"""Pydantic request/response contracts for the Platform Core API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, computed_field

T = TypeVar("T")


class TelemetryOut(BaseModel):
    """A single telemetry window as served by the API."""

    model_config = ConfigDict(from_attributes=True)

    window_from: datetime
    window_to: datetime

    forecast_intensity: int
    actual_intensity: int | None
    carbon_index: str

    generation_mix: dict[str, Any]
    renewable_percentage: float
    low_carbon_percentage: float
    fossil_percentage: float

    is_forecast_only: bool
    generation_mix_missing: bool

    schema_version: str
    source: str
    ingested_at: datetime

    first_seen_at: datetime
    last_seen_at: datetime
    revision_count: int

    @property
    def effective_intensity(self) -> int:
        """Measured intensity when settled, otherwise the forecast."""
        return self.actual_intensity if self.actual_intensity is not None else self.forecast_intensity


class Page(BaseModel, Generic[T]):
    """Envelope for list responses. Cursor pagination, not offset.

    Offset pagination on an append-only time series produces duplicates and
    skips as new windows arrive; a keyset cursor on ``window_from`` does not.
    """

    items: list[T]
    count: int = Field(description="Number of items in this page")
    next_cursor: datetime | None = Field(
        default=None,
        description="Pass as `before` to fetch the next (older) page. Null when exhausted.",
    )
    total: int | None = Field(
        default=None, description="Total matching rows; only populated on request."
    )


class HealthOut(BaseModel):
    status: str
    version: str
    postgres: str
    redis: str
    consumer_lag_seconds: float | None = Field(
        default=None,
        description="Age of the newest telemetry window seen. Null if nothing has been consumed.",
    )


class PrincipalOut(BaseModel):
    """Introspection endpoint so a caller can verify its own identity and role."""

    name: str
    role: str
    key_prefix: str
    can_read_telemetry: bool
    can_manage_keys: bool


class WindowStats(BaseModel):
    """Aggregate over a time range — the shape the arbitrage dashboard needs."""

    window_count: int
    avg_renewable_percentage: float | None
    avg_fossil_percentage: float | None
    avg_actual_intensity: float | None
    cleanest_window_from: datetime | None
    dirtiest_window_from: datetime | None
    forecast_only_count: int


# --------------------------------------------------------------------------- #
# Phase 3 — plant operations + optimisation
# --------------------------------------------------------------------------- #


class PlantOut(BaseModel):
    """Plant load for one settlement window."""

    model_config = ConfigDict(from_attributes=True)

    plant_id: str
    plant_name: str
    window_from: datetime
    window_to: datetime

    total_load_mw: float
    flexible_load_mw: float
    inflexible_load_mw: float

    process_states: dict[str, Any]
    unit: str
    is_estimate: bool

    ingested_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    revision_count: int


class ScheduleDecisionOut(BaseModel):
    """One process × window decision."""

    model_config = ConfigDict(from_attributes=True)

    run_id: str
    process_id: str
    process_name: str
    window_from: datetime
    window_to: datetime
    action: str
    load_mw: float
    intensity: float
    carbon_kg: float
    baseline_carbon_kg: float
    carbon_saved_kg: float
    reason: str


class OptimizationRunOut(BaseModel):
    """A completed optimisation run."""

    model_config = ConfigDict(from_attributes=True)

    run_id: str
    created_at: datetime
    horizon_windows: int
    solver: str

    baseline_carbon_kg: float
    optimized_carbon_kg: float
    carbon_saved_kg: float

    process_count: int
    decision_count: int
    unscheduled: list[Any] = Field(default_factory=list)
    notes: list[Any] = Field(default_factory=list)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def saving_pct(self) -> float:
        """Percentage carbon reduction vs the naive baseline."""
        if self.baseline_carbon_kg <= 0:
            return 0.0
        return round(100.0 * self.carbon_saved_kg / self.baseline_carbon_kg, 2)


class SchedulePlanOut(BaseModel):
    """The latest run together with its schedule — what a dashboard renders."""

    run: OptimizationRunOut
    count: int
    decisions: list[ScheduleDecisionOut]


class OptimizeRunResponse(BaseModel):
    """Result of triggering an optimisation run."""

    run_id: str
    solver: str
    horizon_windows: int
    process_count: int
    decision_count: int
    baseline_carbon_kg: float
    optimized_carbon_kg: float
    carbon_saved_kg: float
    saving_pct: float
    unscheduled: list[str]
    notes: list[str]
    status: str = Field(description="`ok`, or `skipped` when there was nothing to optimise")


# --------------------------------------------------------------------------- #
# AI Orchestrator
# --------------------------------------------------------------------------- #


class AdviceOut(BaseModel):
    """A stored orchestrator recommendation."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: str | None
    #: ``claude`` or ``heuristic`` — always check this before acting on advice.
    source: str
    headline: str
    rationale: str
    confidence: float
    recommended_actions: list[Any] = Field(default_factory=list)
    risk_flags: list[Any] = Field(default_factory=list)
    created_at: datetime


class AdviceContextOut(BaseModel):
    """The stored inputs an advice row was derived from."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: str | None
    source: str
    context: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class OrchestratorRunResponse(BaseModel):
    """Result of triggering an orchestrator pass."""

    run_id: str | None
    source: str
    headline: str
    rationale: str
    confidence: float
    recommended_actions: list[str]
    risk_flags: list[str]
    published: bool
    status: str = "ok"
