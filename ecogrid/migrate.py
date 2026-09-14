"""Idempotent schema bootstrap and credential provisioning.

Run as a one-shot service before the consumer and API start::

    python -m ecogrid.migrate

``Base.metadata.create_all(checkfirst=True)`` is idempotent, so this is safe to
re-run on every deploy and against an existing volume.

**Deferred:** this is *bootstrap*, not migration. It creates missing tables and
indexes but never alters an existing column. The moment a column changes shape,
Alembic (or equivalent) becomes mandatory — running ``create_all`` against a
changed model would silently leave the live schema behind.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from sqlalchemy import inspect, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from ecogrid.config import PlatformSettings
from ecogrid.db import check_connectivity, create_engine, create_session_factory
from ecogrid.logging_setup import configure_logging
from ecogrid.models import ApiKey, Base
from ecogrid.security import Role, generate_api_key, hash_api_key

logger = logging.getLogger("ecogrid.migrate")


async def apply_schema(engine: AsyncEngine) -> list[str]:
    """Create any missing tables and indexes. Returns the table names present."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with engine.connect() as conn:
        tables = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names())
    return sorted(t for t in tables if not t.startswith("pg_"))


async def bootstrap_admin_key(settings: PlatformSettings, engine: AsyncEngine) -> str | None:
    """Ensure at least one admin credential exists.

    Returns the raw key *only* when one was just created, so the caller can print
    it once. On subsequent runs it returns ``None`` — the key is not recoverable,
    by design.
    """
    session_factory = create_session_factory(engine)

    async with session_factory() as session:
        existing = (
            await session.execute(select(ApiKey).where(ApiKey.role == Role.ADMIN.value))
        ).scalars().first()
        if existing is not None:
            logger.info(
                "Admin credential already present (name=%s prefix=%s) — not regenerating",
                existing.name,
                existing.key_prefix,
            )
            return None

        if settings.bootstrap_admin_key:
            raw, digest, prefix = (
                settings.bootstrap_admin_key,
                hash_api_key(settings.bootstrap_admin_key),
                settings.bootstrap_admin_key[:8],
            )
            source = "ECOGRID_BOOTSTRAP_ADMIN_KEY"
        else:
            raw, digest, prefix = generate_api_key()
            source = "generated"

        session.add(
            ApiKey(name="bootstrap-admin", key_hash=digest, key_prefix=prefix, role=Role.ADMIN.value)
        )
        await session.commit()

    logger.info("Created bootstrap admin credential (source=%s, prefix=%s)", source, prefix)
    return raw


async def _table_counts(engine: AsyncEngine) -> dict[str, int]:
    """Row counts for the tables that matter, for a readable deploy log."""
    counts: dict[str, int] = {}
    for table in ("grid_telemetry", "ingest_audit", "api_keys", "audit_log"):
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text(f"SELECT count(*) FROM {table}"))
                counts[table] = int(result.scalar_one())
        except Exception:  # noqa: BLE001 — a missing table is reported by apply_schema
            counts[table] = -1
    return counts


async def run() -> int:
    settings = PlatformSettings()
    configure_logging(settings.log_level)

    logger.info("Applying schema to %s", settings.dsn_for_logs)
    engine = create_engine(settings)
    try:
        await check_connectivity(engine)
        tables = await apply_schema(engine)
        logger.info("Schema ready | tables=%s", ", ".join(tables))

        new_key = await bootstrap_admin_key(settings, engine)

        counts = await _table_counts(engine)
        logger.info(
            "Row counts | %s",
            " ".join(f"{name}={count}" for name, count in counts.items()),
        )

        if new_key:
            # Printed exactly once. It cannot be recovered later.
            print(
                "\n"
                "==================== BOOTSTRAP ADMIN API KEY ====================\n"
                f"  {new_key}\n"
                "  Shown once and never recoverable — store it in your secret manager.\n"
                "  Use it as:  X-API-Key: <key>\n"
                "=================================================================\n",
                flush=True,
            )
    finally:
        await engine.dispose()
    return 0


def main() -> int:
    try:
        return asyncio.run(run())
    except Exception:  # noqa: BLE001
        logger.critical("Schema bootstrap failed", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
