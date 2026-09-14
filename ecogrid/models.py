"""SQLAlchemy models for the Platform Core.

Design notes
------------
* ``grid_telemetry`` is keyed by ``window_from`` (the same key the Phase 1 worker
  puts on the Kafka message). That makes the consumer's write idempotent, which
  is mandatory given the at-least-once delivery contract.
* ``payload_hash`` plus ``revision_count`` let the system distinguish a genuine
  revision (forecast updated, actual settled) from a duplicate redelivery. Only
  genuine revisions bump the counter.
* ``audit_log`` is append-only: it records who did what, and there is no update
  or delete path in the application.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base. ``create_all`` on this metadata is idempotent."""


class GridTelemetryRow(Base):
    """Settled/forecast grid telemetry — one row per half-hourly window.

    ``window_from`` is the natural primary key: it is the Kafka message key, the
    idempotency key, and the partition key. Using it as the PK means the
    consumer's upsert needs no surrogate lookup.
    """

    __tablename__ = "grid_telemetry"

    window_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    window_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    forecast_intensity: Mapped[int] = mapped_column(Integer, nullable=False)
    actual_intensity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    carbon_index: Mapped[str] = mapped_column(String(16), nullable=False)

    generation_mix: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    renewable_percentage: Mapped[float] = mapped_column(Float, nullable=False)
    low_carbon_percentage: Mapped[float] = mapped_column(Float, nullable=False)
    fossil_percentage: Mapped[float] = mapped_column(Float, nullable=False)

    is_forecast_only: Mapped[bool] = mapped_column(Boolean, nullable=False)
    generation_mix_missing: Mapped[bool] = mapped_column(Boolean, nullable=False)

    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    source: Mapped[str] = mapped_column(String(128), nullable=False)

    #: Content hash of the payload, excluding ingestion time. Guards the upsert.
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    #: Timestamp from the producing worker (when it fetched the window).
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # --- Ledger bookkeeping ---
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    #: Number of *content changes* observed, not the number of deliveries.
    revision_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        # Powers "what was the grid doing between X and Y" range scans.
        Index("ix_grid_telemetry_window_from_desc", window_from.desc()),
        Index("ix_grid_telemetry_carbon_index", "carbon_index"),
        Index("ix_grid_telemetry_renewable", "renewable_percentage"),
    )


class IngestAudit(Base):
    """Consumer progress per (group, topic, partition).

    Persisting offsets alongside the data is what makes the ledger
    reconcilable: given a row set, you can prove which topic offsets produced it.
    """

    __tablename__ = "ingest_audit"

    consumer_group: Mapped[str] = mapped_column(String(128), primary_key=True)
    topic: Mapped[str] = mapped_column(String(255), primary_key=True)
    partition: Mapped[int] = mapped_column(Integer, primary_key=True)

    #: Highest offset committed for this partition (inclusive).
    last_offset: Mapped[int] = mapped_column(BigInteger, nullable=False, default=-1)
    messages_consumed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    messages_rejected: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    revisions_applied: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    duplicates_suppressed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (UniqueConstraint("consumer_group", "topic", "partition", name="uq_ingest_audit"),)


class ApiKey(Base):
    """Hashed API credential with a role.

    Only the SHA-256 digest is stored. A leaked database therefore does not leak
    usable credentials, and lookups are a single indexed equality on the digest.
    """

    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    #: SHA-256 hex digest of the presented key.
    key_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    #: Prefix retained for operator identification ("which key is this?").
    key_prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (Index("ix_api_keys_role_active", "role", "is_active"),)


class AuditLog(Base):
    """Append-only record of authenticated actions.

    There is deliberately no update or delete helper for this table anywhere in
    the codebase. Corrections are expressed as new rows.
    """

    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    actor_key_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    actor_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    actor_role: Mapped[str | None] = mapped_column(String(32), nullable=True)

    action: Mapped[str] = mapped_column(String(64), nullable=False)
    resource: Mapped[str] = mapped_column(String(255), nullable=False)
    method: Mapped[str] = mapped_column(String(8), nullable=False)
    path: Mapped[str] = mapped_column(String(512), nullable=False)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False)

    request_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    client_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    detail: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        Index("ix_audit_log_occurred_at_desc", occurred_at.desc()),
        Index("ix_audit_log_actor", "actor_name"),
    )


# --------------------------------------------------------------------------- #
# Phase 3 — plant operations + the optimisation loop
# --------------------------------------------------------------------------- #


class PlantTelemetryRow(Base):
    """Plant electrical load per (plant, window).

    Keyed on ``(plant_id, window_from)`` — the same natural key the bridge puts on
    the Kafka message — so the consumer's upsert is idempotent under the
    at-least-once contract.
    """

    __tablename__ = "plant_telemetry"

    plant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    window_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    window_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    plant_name: Mapped[str] = mapped_column(String(128), nullable=False, default="")

    total_load_mw: Mapped[float] = mapped_column(Float, nullable=False)
    flexible_load_mw: Mapped[float] = mapped_column(Float, nullable=False)
    inflexible_load_mw: Mapped[float] = mapped_column(Float, nullable=False)

    process_states: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    unit: Mapped[str] = mapped_column(String(16), nullable=False, default="MW")
    #: True when the figure is an AS400 estimate rather than a metered read.
    is_estimate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    revision_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("ix_plant_telemetry_window_from_desc", window_from.desc()),
        Index("ix_plant_telemetry_plant", "plant_id"),
    )


class OptimizationRunRow(Base):
    """One execution of the optimisation loop.

    Runs are immutable: each invocation gets a new ``run_id``. That makes the
    schedule a decision record you can audit later — "what did we decide, and
    why?" — rather than mutable state with no history.
    """

    __tablename__ = "optimization_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    horizon_windows: Mapped[int] = mapped_column(Integer, nullable=False)
    solver: Mapped[str] = mapped_column(String(64), nullable=False)

    baseline_carbon_kg: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    optimized_carbon_kg: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    carbon_saved_kg: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    process_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    decision_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    #: Processes that could not be placed, and any solver notes.
    unscheduled: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)
    notes: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, default=list)

    __table_args__ = (Index("ix_optimization_runs_created_at_desc", created_at.desc()),)


class ScheduleDecisionRow(Base):
    """One process × one window decision belonging to an :class:`OptimizationRunRow`."""

    __tablename__ = "schedule_decisions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)

    process_id: Mapped[str] = mapped_column(String(64), nullable=False)
    process_name: Mapped[str] = mapped_column(String(128), nullable=False, default="")

    window_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_to: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    #: ``run`` | ``idle``
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    load_mw: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    intensity: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    carbon_kg: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    baseline_carbon_kg: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    carbon_saved_kg: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        # Re-running the same run_id must not duplicate the schedule.
        UniqueConstraint("run_id", "process_id", "window_from", name="uq_schedule_decision"),
        Index("ix_schedule_decisions_run", "run_id"),
        Index("ix_schedule_decisions_window_from_desc", window_from.desc()),
        Index("ix_schedule_decisions_action", "action"),
    )
