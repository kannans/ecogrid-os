"""Baseline schema — all Phase 1-3 tables.

Revision ID: 0001_baseline
Revises: None

This is the hand-written equivalent of what ``ecogrid.migrate`` creates with
``Base.metadata.create_all``. It exists so that the *next* schema change can be
expressed as a real migration instead of silently drifting: ``create_all`` never
alters an existing column, so without this baseline any future column change
would leave the live database behind.

After this revision, the workflow is:

    alembic revision --autogenerate -m "describe the change"
    alembic upgrade head

``ecogrid.migrate`` remains as the fast bootstrap for a brand-new database
(and still provisions the first admin key), but it is no longer the only
mechanism.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- Phase 2: grid ledger --------------------------------------------- #
    op.create_table(
        "grid_telemetry",
        sa.Column("window_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("forecast_intensity", sa.Integer(), nullable=False),
        sa.Column("actual_intensity", sa.Integer(), nullable=True),
        sa.Column("carbon_index", sa.String(16), nullable=False),
        sa.Column("generation_mix", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("renewable_percentage", sa.Float(), nullable=False),
        sa.Column("low_carbon_percentage", sa.Float(), nullable=False),
        sa.Column("fossil_percentage", sa.Float(), nullable=False),
        sa.Column("is_forecast_only", sa.Boolean(), nullable=False),
        sa.Column("generation_mix_missing", sa.Boolean(), nullable=False),
        sa.Column("schema_version", sa.String(16), nullable=False),
        sa.Column("source", sa.String(128), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("revision_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("window_from"),
    )
    op.create_index(
        "ix_grid_telemetry_window_from_desc",
        "grid_telemetry",
        [sa.text("window_from DESC")],
    )
    op.create_index("ix_grid_telemetry_carbon_index", "grid_telemetry", ["carbon_index"])
    op.create_index("ix_grid_telemetry_renewable", "grid_telemetry", ["renewable_percentage"])

    op.create_table(
        "ingest_audit",
        sa.Column("consumer_group", sa.String(128), nullable=False),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("last_offset", sa.BigInteger(), nullable=False),
        sa.Column("messages_consumed", sa.BigInteger(), nullable=False),
        sa.Column("messages_rejected", sa.BigInteger(), nullable=False),
        sa.Column("revisions_applied", sa.BigInteger(), nullable=False),
        sa.Column("duplicates_suppressed", sa.BigInteger(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("consumer_group", "topic", "partition"),
        sa.UniqueConstraint("consumer_group", "topic", "partition", name="uq_ingest_audit"),
    )

    op.create_table(
        "api_keys",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("key_hash", sa.String(64), nullable=False),
        sa.Column("key_prefix", sa.String(16), nullable=False),
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_reason", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key_hash"),
    )
    op.create_index("ix_api_keys_role_active", "api_keys", ["role", "is_active"])

    op.create_table(
        "audit_log",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("actor_key_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("actor_name", sa.String(128), nullable=True),
        sa.Column("actor_role", sa.String(32), nullable=True),
        sa.Column("action", sa.String(64), nullable=False),
        sa.Column("resource", sa.String(255), nullable=False),
        sa.Column("method", sa.String(8), nullable=False),
        sa.Column("path", sa.String(512), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("request_id", sa.String(64), nullable=True),
        sa.Column("client_ip", sa.String(64), nullable=True),
        sa.Column("detail", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_audit_log_occurred_at_desc", "audit_log", [sa.text("occurred_at DESC")])
    op.create_index("ix_audit_log_actor", "audit_log", ["actor_name"])

    # --- Phase 3: plant operations ---------------------------------------- #
    op.create_table(
        "plant_telemetry",
        sa.Column("plant_id", sa.String(64), nullable=False),
        sa.Column("window_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("plant_name", sa.String(128), nullable=False),
        sa.Column("total_load_mw", sa.Float(), nullable=False),
        sa.Column("flexible_load_mw", sa.Float(), nullable=False),
        sa.Column("inflexible_load_mw", sa.Float(), nullable=False),
        sa.Column("process_states", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("unit", sa.String(16), nullable=False),
        sa.Column("is_estimate", sa.Boolean(), nullable=False),
        sa.Column("schema_version", sa.String(16), nullable=False),
        sa.Column("source", sa.String(128), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("revision_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("plant_id", "window_from"),
    )
    op.create_index(
        "ix_plant_telemetry_window_from_desc",
        "plant_telemetry",
        [sa.text("window_from DESC")],
    )
    op.create_index("ix_plant_telemetry_plant", "plant_telemetry", ["plant_id"])

    # --- Phase 3: optimization -------------------------------------------- #
    op.create_table(
        "optimization_runs",
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("horizon_windows", sa.Integer(), nullable=False),
        sa.Column("solver", sa.String(64), nullable=False),
        sa.Column("baseline_carbon_kg", sa.Float(), nullable=False),
        sa.Column("optimized_carbon_kg", sa.Float(), nullable=False),
        sa.Column("carbon_saved_kg", sa.Float(), nullable=False),
        sa.Column("process_count", sa.Integer(), nullable=False),
        sa.Column("decision_count", sa.Integer(), nullable=False),
        sa.Column("unscheduled", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("notes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index(
        "ix_optimization_runs_created_at_desc",
        "optimization_runs",
        [sa.text("created_at DESC")],
    )

    op.create_table(
        "schedule_decisions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("process_id", sa.String(64), nullable=False),
        sa.Column("process_name", sa.String(128), nullable=False),
        sa.Column("window_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("load_mw", sa.Float(), nullable=False),
        sa.Column("intensity", sa.Float(), nullable=False),
        sa.Column("carbon_kg", sa.Float(), nullable=False),
        sa.Column("baseline_carbon_kg", sa.Float(), nullable=False),
        sa.Column("carbon_saved_kg", sa.Float(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "process_id", "window_from", name="uq_schedule_decision"),
    )
    op.create_index("ix_schedule_decisions_run", "schedule_decisions", ["run_id"])
    op.create_index(
        "ix_schedule_decisions_window_from_desc",
        "schedule_decisions",
        [sa.text("window_from DESC")],
    )
    op.create_index("ix_schedule_decisions_action", "schedule_decisions", ["action"])

    # --- AI Orchestrator --------------------------------------------------- #
    op.create_table(
        "orchestrator_advice",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(64), nullable=True),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("headline", sa.String(280), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("recommended_actions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("risk_flags", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("context", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_orchestrator_advice_created_at_desc",
        "orchestrator_advice",
        [sa.text("created_at DESC")],
    )
    op.create_index("ix_orchestrator_advice_source", "orchestrator_advice", ["source"])


def downgrade() -> None:
    op.drop_table("orchestrator_advice")
    op.drop_table("schedule_decisions")
    op.drop_table("optimization_runs")
    op.drop_table("plant_telemetry")
    op.drop_table("audit_log")
    op.drop_table("api_keys")
    op.drop_table("ingest_audit")
    op.drop_table("grid_telemetry")
