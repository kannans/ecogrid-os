"""API-key authentication and role-based access control.

Model
-----
A credential is a 256-bit random token. Only its SHA-256 digest is persisted, so
a database disclosure does not yield usable keys. Lookup is a single indexed
equality on the digest — no scan, no per-row comparison, and therefore no timing
side channel to defend against.

Roles are ranked, and a requirement is expressed as a minimum rank. That keeps
endpoint declarations readable (``require_role(Role.OPERATOR)``) and makes adding
a role a one-line change rather than an edit to every dependency.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecogrid.models import ApiKey

logger = logging.getLogger("ecogrid.security")

#: Bytes of entropy in a generated key (256 bits).
_KEY_BYTES = 32
#: Visible prefix retained for operator identification.
_PREFIX_CHARS = 8


class Role(str, Enum):
    """Ranked roles. Higher rank implies every capability of lower ranks."""

    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"


_ROLE_RANK: dict[Role, int] = {
    Role.VIEWER: 10,
    Role.OPERATOR: 20,
    Role.ADMIN: 30,
}


def role_rank(role: Role | str) -> int:
    """Rank for a role, defaulting to 0 for unknown values.

    Unknown roles rank lowest rather than raising: a row corrupted or written by
    a future version must fail closed (denied), not crash the request.
    """
    if isinstance(role, str):
        try:
            role = Role(role)
        except ValueError:
            logger.warning("Unknown role %r — treating as unprivileged", role)
            return 0
    return _ROLE_RANK.get(role, 0)


class AuthError(Exception):
    """Raised when a credential is missing, malformed, unknown, or revoked."""


@dataclass(frozen=True, slots=True)
class Principal:
    """The authenticated caller."""

    key_id: object
    name: str
    role: Role
    key_prefix: str

    @property
    def rank(self) -> int:
        return role_rank(self.role)

    def has_at_least(self, minimum: Role) -> bool:
        return self.rank >= role_rank(minimum)


def hash_api_key(raw_key: str) -> str:
    """SHA-256 hex digest of a presented key."""
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def generate_api_key() -> tuple[str, str, str]:
    """Create a credential. Returns ``(raw_key, key_hash, key_prefix)``.

    The raw key is returned exactly once, at creation. It is never recoverable
    afterwards because only the digest is stored.
    """
    raw = secrets.token_urlsafe(_KEY_BYTES)
    return raw, hash_api_key(raw), raw[:_PREFIX_CHARS]


async def authenticate(
    session_factory: async_sessionmaker[AsyncSession],
    raw_key: str | None,
) -> Principal:
    """Resolve a raw key to a Principal, or raise :class:`AuthError`."""
    if not raw_key or not raw_key.strip():
        raise AuthError("missing API key")

    digest = hash_api_key(raw_key.strip())

    async with session_factory() as session:
        row = (
            await session.execute(select(ApiKey).where(ApiKey.key_hash == digest))
        ).scalar_one_or_none()

        if row is None:
            raise AuthError("unknown API key")
        if not row.is_active or row.revoked_at is not None:
            raise AuthError("API key revoked")

        # Best-effort usage tracking; never fail an authenticated request over it.
        row.last_used_at = datetime.now(timezone.utc)
        await session.commit()

    return Principal(
        key_id=row.id,
        name=row.name,
        role=Role(row.role) if row.role in {r.value for r in Role} else Role.VIEWER,
        key_prefix=row.key_prefix,
    )
