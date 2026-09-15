"""Alembic environment for EcoGrid OS.

Two things worth noting:

* The DSN comes from :class:`PlatformSettings` (``ECOGRID_POSTGRES_DSN``), never
  from ``alembic.ini`` — one source of truth for credentials.
* Alembic runs **synchronously**. ``PlatformSettings`` normalises the DSN to the
  asyncpg driver for the app, so we strip ``+asyncpg`` back off here.
"""

from __future__ import annotations

import asyncio

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from ecogrid.config import PlatformSettings
from ecogrid.models import Base

config = context.config

settings = PlatformSettings()
# Keep the asyncpg driver. Alembic does not need a sync driver: it can drive an
# async engine via `connection.run_sync()`, which is what this file does below.
#
# An earlier version stripped `+asyncpg` to get a `postgresql://` URL, which
# SQLAlchemy resolves to psycopg2 — a driver this project never installs. The
# result was `ModuleNotFoundError: No module named 'psycopg2'` on every alembic
# command. Adding psycopg2 would have worked too, but it means a second driver
# and a second connection path for no benefit.
config.set_main_option("sqlalchemy.url", settings.postgres_dsn)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Emit SQL to stdout instead of executing it (``alembic upgrade --sql``)."""
    context.configure(
        url=settings.postgres_dsn,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: object) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def _run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        # Bridges Alembic's synchronous migration machinery onto the async
        # connection, so the asyncpg driver is the only one we need.
        await connection.run_sync(_do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(_run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
