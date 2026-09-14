"""Alembic environment for EcoGrid OS.

Two things worth noting:

* The DSN comes from :class:`PlatformSettings` (``ECOGRID_POSTGRES_DSN``), never
  from ``alembic.ini`` — one source of truth for credentials.
* Alembic runs **synchronously**. ``PlatformSettings`` normalises the DSN to the
  asyncpg driver for the app, so we strip ``+asyncpg`` back off here.
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from ecogrid.config import PlatformSettings
from ecogrid.models import Base

config = context.config

settings = PlatformSettings()
sync_url = settings.postgres_dsn.replace("+asyncpg", "")
config.set_main_option("sqlalchemy.url", sync_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Emit SQL to stdout instead of executing it (``alembic upgrade --sql``)."""
    context.configure(
        url=sync_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
