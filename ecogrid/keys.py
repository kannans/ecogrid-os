"""API key administration CLI.

The API has no key-management endpoints on purpose: issuing a credential is an
out-of-band administrative action, not something a compromised key should be able
to do by calling the API. Operators run this against the database directly.

    python -m ecogrid.keys create --name dashboard --role viewer
    python -m ecogrid.keys list
    python -m ecogrid.keys revoke --name dashboard --reason "rotated"

A newly created key is printed exactly once. Only its SHA-256 digest is stored,
so it cannot be recovered afterwards.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from datetime import datetime, timezone

from sqlalchemy import select

from ecogrid.config import PlatformSettings
from ecogrid.db import check_connectivity, create_engine, create_session_factory
from ecogrid.logging_setup import configure_logging
from ecogrid.models import ApiKey
from ecogrid.security import Role, generate_api_key, hash_api_key

logger = __import__("logging").getLogger("ecogrid.keys")


async def cmd_create(name: str, role: str, *, raw_key: str | None = None) -> int:
    settings = PlatformSettings()
    engine = create_engine(settings)
    try:
        await check_connectivity(engine)
        factory = create_session_factory(engine)
        async with factory() as session:
            exists = (
                await session.execute(select(ApiKey).where(ApiKey.name == name))
            ).scalar_one_or_none()
            if exists is not None:
                logger.error("A key named %r already exists (prefix=%s)", name, exists.key_prefix)
                return 1

            if raw_key:
                raw, digest, prefix = raw_key, hash_api_key(raw_key), raw_key[:8]
            else:
                raw, digest, prefix = generate_api_key()

            session.add(ApiKey(name=name, key_hash=digest, key_prefix=prefix, role=role))
            await session.commit()
    finally:
        await engine.dispose()

    print(
        f"\nCreated API key\n"
        f"  name   : {name}\n"
        f"  role   : {role}\n"
        f"  prefix : {prefix}\n"
        f"  key    : {raw}\n"
        f"\n  Shown once and never recoverable. Use as:  X-API-Key: {raw}\n",
        flush=True,
    )
    return 0


async def cmd_list() -> int:
    settings = PlatformSettings()
    engine = create_engine(settings)
    try:
        await check_connectivity(engine)
        factory = create_session_factory(engine)
        async with factory() as session:
            rows = list((await session.execute(select(ApiKey).order_by(ApiKey.created_at))).scalars().all())
    finally:
        await engine.dispose()

    if not rows:
        print("No API keys.")
        return 0

    print(f"{'NAME':<22} {'ROLE':<10} {'PREFIX':<10} {'ACTIVE':<7} {'LAST USED':<22}")
    print("-" * 74)
    for row in rows:
        last_used = row.last_used_at.strftime("%Y-%m-%dT%H:%M:%SZ") if row.last_used_at else "never"
        print(
            f"{row.name:<22} {row.role:<10} {row.key_prefix:<10} "
            f"{str(row.is_active):<7} {last_used:<22}"
        )
    return 0


async def cmd_revoke(name: str, reason: str | None) -> int:
    """Revoke a key. The row is retained so the audit trail keeps its referent."""
    settings = PlatformSettings()
    engine = create_engine(settings)
    try:
        await check_connectivity(engine)
        factory = create_session_factory(engine)
        async with factory() as session:
            row = (
                await session.execute(select(ApiKey).where(ApiKey.name == name))
            ).scalar_one_or_none()
            if row is None:
                logger.error("No key named %r", name)
                return 1
            if not row.is_active:
                logger.warning("Key %r is already revoked", name)
                return 0

            row.is_active = False
            row.revoked_at = datetime.now(timezone.utc)
            row.revoked_reason = reason
            await session.commit()
    finally:
        await engine.dispose()

    logger.info("Revoked key %r (prefix=%s)%s", name, row.key_prefix, f" — {reason}" if reason else "")
    return 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ecogrid.keys", description="Manage Platform Core API keys.")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="Issue a new API key")
    create.add_argument("--name", required=True, help="Unique, human-readable key name")
    create.add_argument(
        "--role", required=True, choices=[r.value for r in Role], help="Role granted to the key"
    )
    create.add_argument(
        "--key", default=None, help="Use a fixed key value instead of generating one (testing only)"
    )

    sub.add_parser("list", help="List keys and their roles")

    revoke = sub.add_parser("revoke", help="Revoke a key by name")
    revoke.add_argument("--name", required=True)
    revoke.add_argument("--reason", default=None)

    return parser.parse_args(argv)


async def _dispatch(args: argparse.Namespace) -> int:
    if args.command == "create":
        return await cmd_create(args.name, args.role, raw_key=args.key)
    if args.command == "list":
        return await cmd_list()
    if args.command == "revoke":
        return await cmd_revoke(args.name, args.reason)
    raise SystemExit(f"unknown command {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(PlatformSettings().log_level)
    try:
        return asyncio.run(_dispatch(args))
    except Exception:  # noqa: BLE001
        logger.critical("Key management command failed", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
