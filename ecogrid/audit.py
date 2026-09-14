"""Append-only audit logging.

Every authenticated action is recorded. Writes are best-effort: an audit failure
must never fail the business request, but it must be loud, because a silent gap
in the audit trail is worse than a failed read.

There is deliberately no update or delete function here. Corrections are new
rows, which is what makes the trail trustworthy.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ecogrid.models import AuditLog
from ecogrid.security import Principal

logger = logging.getLogger("ecogrid.audit")


async def write_audit_entry(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    action: str,
    resource: str,
    method: str,
    path: str,
    status_code: int,
    principal: Principal | None = None,
    request_id: str | None = None,
    client_ip: str | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    """Persist one audit row. Never raises."""
    entry = AuditLog(
        actor_key_id=principal.key_id if principal else None,
        actor_name=principal.name if principal else None,
        actor_role=principal.role.value if principal else None,
        action=action,
        resource=resource,
        method=method,
        path=path,
        status_code=status_code,
        request_id=request_id,
        client_ip=client_ip,
        detail=detail,
    )
    try:
        async with session_factory() as session:
            session.add(entry)
            await session.commit()
    except Exception:  # noqa: BLE001 — auditing must not break the request
        logger.exception(
            "AUDIT WRITE FAILED action=%s resource=%s actor=%s status=%d — the audit "
            "trail now has a gap for this request",
            action,
            resource,
            principal.name if principal else "<anonymous>",
            status_code,
        )


async def record_failed_auth(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    path: str,
    method: str,
    client_ip: str | None,
    reason: str,
    key_prefix: str | None = None,
) -> None:
    """Record a rejected authentication attempt.

    Anonymous failures are the highest-signal audit events — they are how a
    credential-stuffing attempt becomes visible.
    """
    await write_audit_entry(
        session_factory,
        action="auth.rejected",
        resource="auth",
        method=method,
        path=path,
        status_code=401,
        client_ip=client_ip,
        detail={"reason": reason, "key_prefix": key_prefix},
    )
