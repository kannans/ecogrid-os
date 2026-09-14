"""Async database engine, session factory, and lifecycle helpers."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ecogrid.config import PlatformSettings

logger = logging.getLogger("ecogrid.db")


def create_engine(settings: PlatformSettings) -> AsyncEngine:
    """Build the async engine with pool settings suited to a long-lived worker."""
    return create_async_engine(
        settings.postgres_dsn,
        echo=settings.db_echo,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_recycle=settings.db_pool_recycle_seconds,
        # Detects connections killed by a database restart or network blip
        # before handing them to a query.
        pool_pre_ping=True,
        connect_args={
            "server_settings": {
                # Bound any single statement so a pathological query cannot pin a
                # connection indefinitely.
                "statement_timeout": str(settings.db_statement_timeout_ms),
                "application_name": "ecogrid-platform-core",
            }
        },
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Session factory. ``expire_on_commit=False`` keeps ORM objects usable after
    commit, which matters because the consumer commits then serialises."""
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@asynccontextmanager
async def session_scope(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Transactional scope: commit on success, roll back on any exception."""
    session = factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def check_connectivity(engine: AsyncEngine) -> None:
    """Fail fast at startup with a clear message rather than on first query."""
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
