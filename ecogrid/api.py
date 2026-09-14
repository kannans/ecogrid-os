"""FastAPI service for the EcoGrid OS Platform Core.

Reads served by this API come from PostgreSQL, with the newest window served from
a Redis hot cache. Every authenticated request is written to the append-only
audit log.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy import Select, and_, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from ecogrid import __version__
from ecogrid.audit import record_failed_auth, write_audit_entry
from ecogrid.cache import TelemetryCache
from ecogrid.config import PlatformSettings
from ecogrid.db import check_connectivity, create_engine, create_session_factory
from ecogrid.logging_setup import configure_logging
from ecogrid.models import (
    AuditLog,
    GridTelemetryRow,
    IngestAudit,
    OptimizationRunRow,
    PlantTelemetryRow,
    ScheduleDecisionRow,
)
from ecogrid.ratelimit import RateLimiter
from ecogrid.schemas import (
    HealthOut,
    OptimizeRunResponse,
    OptimizationRunOut,
    Page,
    PlantOut,
    PrincipalOut,
    ScheduleDecisionOut,
    SchedulePlanOut,
    TelemetryOut,
    WindowStats,
)
from ecogrid.security import AuthError, Principal, Role, authenticate, hash_api_key

logger = logging.getLogger("ecogrid.api")

settings = PlatformSettings()


# --------------------------------------------------------------------------- #
# Lifespan
# --------------------------------------------------------------------------- #


class AppState:
    """Process-wide resources, created once at startup."""

    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    redis: Redis
    cache: TelemetryCache
    limiter: RateLimiter
    postgres_ok: bool = False
    redis_ok: bool = False


state = AppState()


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    configure_logging(settings.log_level)

    state.engine = create_engine(settings)
    state.session_factory = create_session_factory(state.engine)

    try:
        await check_connectivity(state.engine)
        state.postgres_ok = True
        logger.info("PostgreSQL connected | %s", settings.dsn_for_logs)
    except Exception as exc:  # noqa: BLE001
        # Start anyway so /healthz can report the failure; a crash-looping
        # container with no health signal is harder to diagnose.
        logger.critical("PostgreSQL unreachable at startup: %s", exc)

    state.redis = Redis.from_url(
        settings.redis_url, max_connections=settings.redis_max_connections, decode_responses=True
    )
    try:
        await state.redis.ping()
        state.redis_ok = True
        logger.info("Redis connected | %s", settings.redis_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Redis unreachable at startup (%s) — cache and rate limiting degrade", exc)

    state.cache = TelemetryCache(state.redis, settings)
    state.limiter = RateLimiter(state.redis, settings)

    try:
        yield
    finally:
        await state.redis.aclose()
        await state.engine.dispose()
        logger.info("Platform Core shut down")


app = FastAPI(
    title=settings.api_title,
    version=__version__,
    description=(
        "Authenticated read API over the grid telemetry ledger. "
        "Delivery from the ingestion worker is at-least-once; writes into this "
        "service are idempotent on `window_from`."
    ),
    docs_url="/docs" if settings.api_docs_enabled else None,
    redoc_url="/redoc" if settings.api_docs_enabled else None,
    openapi_url="/openapi.json" if settings.api_docs_enabled else None,
    lifespan=lifespan,
)


# --------------------------------------------------------------------------- #
# Dependencies
# --------------------------------------------------------------------------- #


def client_ip(request: Request) -> str:
    """Best-effort client address, honouring one proxy hop."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


async def enforce_rate_limit(request: Request) -> None:
    """Per-caller rate limiting, applied before authentication.

    Limiting *before* auth means a caller presenting garbage credentials is still
    throttled — otherwise a credential-stuffing loop gets unlimited attempts.
    The identity used as the Redis key is a digest, never the raw key: Redis keys
    end up in ``MONITOR`` output, slow logs, and backups, and a credential must
    not leak through any of them.
    """
    raw_key = request.headers.get(settings.api_key_header)
    if not raw_key:
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            raw_key = auth_header[7:].strip()

    identity = (
        f"key:{hash_api_key(raw_key)[:16]}" if raw_key else f"ip:{client_ip(request)}"
    )

    result = await state.limiter.check(identity)
    if not result.allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="rate limit exceeded",
            headers={
                "Retry-After": str(result.reset_after_seconds),
                "X-RateLimit-Limit": str(result.limit),
                "X-RateLimit-Remaining": "0",
            },
        )
    request.state.rate_limit_remaining = result.remaining


async def get_principal(
    request: Request,
    x_api_key: Annotated[str | None, Header(alias=settings.api_key_header)] = None,
    authorization: Annotated[str | None, Header()] = None,
) -> Principal:
    """Authenticate the caller. Raises 401 on any failure."""
    raw_key = x_api_key
    if not raw_key and authorization and authorization.lower().startswith("bearer "):
        raw_key = authorization[7:].strip()

    try:
        principal = await authenticate(state.session_factory, raw_key)
    except AuthError as exc:
        # Anonymous/denied attempts are the highest-signal audit events.
        await record_failed_auth(
            state.session_factory,
            path=request.url.path,
            method=request.method,
            client_ip=client_ip(request),
            reason=str(exc),
            key_prefix=(raw_key[:8] if raw_key else None),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "ApiKey"},
        ) from exc

    request.state.principal = principal
    return principal


def require_role(minimum: Role) -> Any:
    """Dependency factory enforcing a minimum role."""

    async def _dependency(
        request: Request, principal: Annotated[Principal, Depends(get_principal)]
    ) -> Principal:
        if not principal.has_at_least(minimum):
            await write_audit_entry(
                state.session_factory,
                action="access.denied",
                resource=request.url.path,
                method=request.method,
                path=request.url.path,
                status_code=status.HTTP_403_FORBIDDEN,
                principal=principal,
                client_ip=client_ip(request),
                detail={"required_role": minimum.value, "actual_role": principal.role.value},
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"requires role '{minimum.value}' or higher",
            )
        return principal

    return _dependency


ViewerDep = Annotated[Principal, Depends(require_role(Role.VIEWER))]
OperatorDep = Annotated[Principal, Depends(require_role(Role.OPERATOR))]
AdminDep = Annotated[Principal, Depends(require_role(Role.ADMIN))]


async def get_session() -> AsyncIterator[AsyncSession]:
    async with state.session_factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


# --------------------------------------------------------------------------- #
# Versioned router
# --------------------------------------------------------------------------- #

#: Every versioned route is mounted here so rate limiting is applied uniformly —
#: including to routes added later, which is the failure mode of adding the
#: dependency route-by-route.
#:
#: `/healthz` is deliberately NOT on this router: a rate-limited healthcheck
#: would let an orchestrator mark a perfectly healthy container as failed.
api_router = APIRouter(prefix="/api/v1", dependencies=[Depends(enforce_rate_limit)])
# NOTE: `app.include_router(api_router)` is called at the END of this module.
# FastAPI copies the router's routes at include time, so including it here —
# before the @api_router decorators run — would mount an empty router and every
# versioned endpoint would 404.


# --------------------------------------------------------------------------- #
# Audit middleware
# --------------------------------------------------------------------------- #


@app.middleware("http")
async def audit_middleware(request: Request, call_next: Any) -> Response:
    """Record every authenticated request after the response is produced."""
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
    try:
        response = await call_next(request)
    except Exception:
        principal = getattr(request.state, "principal", None)
        await write_audit_entry(
            state.session_factory,
            action="request.error",
            resource=request.url.path,
            method=request.method,
            path=request.url.path,
            status_code=500,
            principal=principal,
            request_id=request_id,
            client_ip=client_ip(request),
        )
        raise

    response.headers["X-Request-ID"] = request_id
    remaining = getattr(request.state, "rate_limit_remaining", None)
    if remaining is not None:
        response.headers["X-RateLimit-Remaining"] = str(remaining)

    principal = getattr(request.state, "principal", None)
    if principal is not None and request.url.path not in {"/healthz"}:
        await write_audit_entry(
            state.session_factory,
            action="read" if request.method == "GET" else "write",
            resource=request.url.path,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            principal=principal,
            request_id=request_id,
            client_ip=client_ip(request),
            detail={"query": str(request.url.query)} if request.url.query else None,
        )
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Uniform error envelope so clients have one shape to parse."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail, "status": exc.status_code, "path": request.url.path},
        headers=exc.headers,
    )


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #


@app.get("/healthz", response_model=HealthOut, tags=["ops"], summary="Liveness and dependency probe")
async def healthz() -> HealthOut:
    """Unauthenticated: container orchestrators cannot present credentials."""
    postgres = "ok"
    redis = "ok"

    try:
        await check_connectivity(state.engine)
    except Exception as exc:  # noqa: BLE001
        postgres = f"error: {type(exc).__name__}"

    try:
        await state.redis.ping()
    except Exception as exc:  # noqa: BLE001
        redis = f"error: {type(exc).__name__}"

    lag: float | None = None
    try:
        async with state.session_factory() as session:
            newest = await session.scalar(select(func.max(GridTelemetryRow.window_to)))
        if newest is not None:
            if newest.tzinfo is None:
                newest = newest.replace(tzinfo=timezone.utc)
            lag = (datetime.now(timezone.utc) - newest).total_seconds()
    except Exception:  # noqa: BLE001 — health must not 500
        lag = None

    healthy = postgres == "ok"
    return HealthOut(
        status="ok" if healthy else "degraded",
        version=__version__,
        postgres=postgres,
        redis=redis,
        consumer_lag_seconds=lag,
    )


@api_router.get("/whoami", response_model=PrincipalOut, tags=["auth"], summary="Introspect the caller")
async def whoami(principal: ViewerDep) -> PrincipalOut:
    return PrincipalOut(
        name=principal.name,
        role=principal.role.value,
        key_prefix=principal.key_prefix,
        can_read_telemetry=principal.has_at_least(Role.VIEWER),
        can_manage_keys=principal.has_at_least(Role.ADMIN),
    )


# NOTE: /telemetry/latest must be declared BEFORE /telemetry/{window_from},
# otherwise FastAPI matches "latest" as a path parameter and returns 422.
@api_router.get(
    "/telemetry/latest",
    response_model=TelemetryOut,
    tags=["telemetry"],
    summary="Newest telemetry window (Redis-cached)",
)
async def latest_telemetry(principal: ViewerDep) -> TelemetryOut:
    cached = await state.cache.get_latest()
    if cached is not None:
        try:
            return TelemetryOut.model_validate(cached)
        except ValidationError:
            # A cache entry written by an older schema must not break the read.
            # Treat it as a miss, evict it, and serve from the source of truth.
            logger.warning(
                "Cached telemetry failed validation (schema drift?) — evicting and "
                "falling back to PostgreSQL",
                exc_info=True,
            )
            await state.cache.invalidate()

    async with state.session_factory() as session:
        row = await session.scalar(
            select(GridTelemetryRow).order_by(GridTelemetryRow.window_from.desc()).limit(1)
        )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no telemetry has been consumed yet",
        )

    payload = TelemetryOut.model_validate(row).model_dump(mode="json")
    # Repopulate the cache so the next caller is served from Redis.
    await state.cache.set_latest_if_newer(payload)
    return TelemetryOut.model_validate(payload)


@api_router.get(
    "/telemetry",
    response_model=Page[TelemetryOut],
    tags=["telemetry"],
    summary="List telemetry windows (keyset paginated, newest first)",
)
async def list_telemetry(
    principal: ViewerDep,
    session: SessionDep,
    from_ts: Annotated[datetime | None, Query(alias="from", description="Inclusive lower bound on window_from")] = None,
    to_ts: Annotated[datetime | None, Query(alias="to", description="Inclusive upper bound on window_to")] = None,
    carbon_index: Annotated[str | None, Query(description="Filter by index band")] = None,
    settled_only: Annotated[bool, Query(description="Exclude forecast-only windows")] = False,
    before: Annotated[datetime | None, Query(description="Cursor: return windows older than this")] = None,
    limit: Annotated[int | None, Query(ge=1, description="Page size")] = None,
    include_total: Annotated[bool, Query(description="Also count all matching rows")] = False,
) -> Page[TelemetryOut]:
    page_size = min(limit or settings.api_default_page_size, settings.api_max_page_size)

    def filtered() -> Select[Any]:
        stmt = select(GridTelemetryRow)
        if from_ts is not None:
            stmt = stmt.where(GridTelemetryRow.window_from >= from_ts)
        if to_ts is not None:
            stmt = stmt.where(GridTelemetryRow.window_to <= to_ts)
        if carbon_index is not None:
            stmt = stmt.where(GridTelemetryRow.carbon_index == carbon_index)
        if settled_only:
            stmt = stmt.where(GridTelemetryRow.is_forecast_only.is_(False))
        return stmt

    stmt = filtered().order_by(GridTelemetryRow.window_from.desc()).limit(page_size)
    if before is not None:
        stmt = stmt.where(GridTelemetryRow.window_from < before)

    rows = list((await session.execute(stmt)).scalars().all())
    items = [TelemetryOut.model_validate(row) for row in rows]

    next_cursor = rows[-1].window_from if len(rows) == page_size else None

    total: int | None = None
    if include_total:
        count_stmt = select(func.count()).select_from(filtered().subquery())
        total = int(await session.scalar(count_stmt) or 0)

    return Page[TelemetryOut](items=items, count=len(items), next_cursor=next_cursor, total=total)


@api_router.get(
    "/telemetry/stats",
    response_model=WindowStats,
    tags=["telemetry"],
    summary="Aggregates over a time range",
)
async def telemetry_stats(
    principal: ViewerDep,
    session: SessionDep,
    hours: Annotated[int, Query(ge=1, le=720, description="Look-back window in hours")] = 24,
) -> WindowStats:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    base = select(GridTelemetryRow).where(GridTelemetryRow.window_from >= since)

    aggregates = (
        await session.execute(
            select(
                func.count(),
                func.avg(GridTelemetryRow.renewable_percentage),
                func.avg(GridTelemetryRow.fossil_percentage),
                func.avg(GridTelemetryRow.actual_intensity),
                func.count().filter(GridTelemetryRow.is_forecast_only.is_(True)),
            ).where(GridTelemetryRow.window_from >= since)
        )
    ).one()

    cleanest = await session.scalar(
        base.order_by(GridTelemetryRow.actual_intensity.asc().nullslast()).limit(1)
    )
    dirtiest = await session.scalar(
        base.order_by(GridTelemetryRow.actual_intensity.desc().nullslast()).limit(1)
    )

    return WindowStats(
        window_count=int(aggregates[0] or 0),
        avg_renewable_percentage=float(aggregates[1]) if aggregates[1] is not None else None,
        avg_fossil_percentage=float(aggregates[2]) if aggregates[2] is not None else None,
        avg_actual_intensity=float(aggregates[3]) if aggregates[3] is not None else None,
        cleanest_window_from=cleanest.window_from if cleanest else None,
        dirtiest_window_from=dirtiest.window_from if dirtiest else None,
        forecast_only_count=int(aggregates[4] or 0),
    )


@api_router.get(
    "/telemetry/{window_from}",
    response_model=TelemetryOut,
    tags=["telemetry"],
    summary="Fetch one window by its start timestamp",
)
async def get_window(principal: ViewerDep, session: SessionDep, window_from: datetime) -> TelemetryOut:
    row = await session.get(GridTelemetryRow, window_from)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no telemetry for window_from={window_from.isoformat()}",
        )
    return TelemetryOut.model_validate(row)


@api_router.get(
    "/audit",
    tags=["ops"],
    summary="Recent audit entries (admin only)",
)
async def recent_audit(
    principal: AdminDep,
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> dict[str, Any]:
    rows = list(
        (
            await session.execute(
                select(AuditLog).order_by(AuditLog.id.desc()).limit(limit)
            )
        )
        .scalars()
        .all()
    )
    return {
        "count": len(rows),
        "items": [
            {
                "id": r.id,
                "occurred_at": r.occurred_at.isoformat(),
                "actor_name": r.actor_name,
                "actor_role": r.actor_role,
                "action": r.action,
                "resource": r.resource,
                "method": r.method,
                "status_code": r.status_code,
                "client_ip": r.client_ip,
                "detail": r.detail,
            }
            for r in rows
        ],
    }


@api_router.get(
    "/ingest-status",
    tags=["ops"],
    summary="Consumer progress per partition",
)
async def ingest_status(principal: ViewerDep, session: SessionDep) -> dict[str, Any]:
    """Exposes the reconciliation surface: what offset the ledger is built from."""
    rows = list((await session.execute(select(IngestAudit))).scalars().all())
    return {
        "consumer_group": settings.kafka_consumer_group,
        "topic": settings.kafka_topic,
        "partitions": [
            {
                "partition": r.partition,
                "last_offset": r.last_offset,
                "messages_consumed": r.messages_consumed,
                "revisions_applied": r.revisions_applied,
                "duplicates_suppressed": r.duplicates_suppressed,
                "messages_rejected": r.messages_rejected,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            }
            for r in rows
        ],
    }


# --------------------------------------------------------------------------- #
# Phase 3 — plant operations + optimisation
# --------------------------------------------------------------------------- #


def _newest_plant_subquery() -> Any:
    """Newest window per plant — one plant may lag another by a window."""
    return (
        select(
            PlantTelemetryRow.plant_id.label("plant_id"),
            func.max(PlantTelemetryRow.window_from).label("newest"),
        ).group_by(PlantTelemetryRow.plant_id)
    ).subquery()


@api_router.get(
    "/plant/latest",
    response_model=list[PlantOut],
    tags=["plant"],
    summary="Newest load reading for every plant",
)
async def latest_plant(principal: ViewerDep, session: SessionDep) -> list[PlantOut]:
    newest = _newest_plant_subquery()
    rows = list(
        (
            await session.execute(
                select(PlantTelemetryRow).join(
                    newest,
                    and_(
                        PlantTelemetryRow.plant_id == newest.c.plant_id,
                        PlantTelemetryRow.window_from == newest.c.newest,
                    ),
                )
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no plant telemetry has been consumed yet",
        )
    return [PlantOut.model_validate(row) for row in rows]


@api_router.get(
    "/plant",
    response_model=Page[PlantOut],
    tags=["plant"],
    summary="Plant load history (keyset paginated, newest first)",
)
async def list_plant(
    principal: ViewerDep,
    session: SessionDep,
    plant_id: Annotated[str | None, Query(description="Filter by plant")] = None,
    before: Annotated[datetime | None, Query(description="Cursor: return windows older than this")] = None,
    limit: Annotated[int | None, Query(ge=1, description="Page size")] = None,
) -> Page[PlantOut]:
    page_size = min(limit or settings.api_default_page_size, settings.api_max_page_size)

    stmt = select(PlantTelemetryRow)
    if plant_id is not None:
        stmt = stmt.where(PlantTelemetryRow.plant_id == plant_id)
    if before is not None:
        stmt = stmt.where(PlantTelemetryRow.window_from < before)

    rows = list(
        (await session.execute(stmt.order_by(PlantTelemetryRow.window_from.desc()).limit(page_size)))
        .scalars()
        .all()
    )
    items = [PlantOut.model_validate(row) for row in rows]
    next_cursor = rows[-1].window_from if len(rows) == page_size else None
    return Page[PlantOut](items=items, count=len(items), next_cursor=next_cursor, total=None)


@api_router.get(
    "/schedule/latest",
    response_model=SchedulePlanOut,
    tags=["schedule"],
    summary="Most recent optimisation run and its dispatch schedule",
)
async def latest_schedule(
    principal: ViewerDep,
    session: SessionDep,
    action: Annotated[str | None, Query(description="Filter decisions: run | idle")] = None,
) -> SchedulePlanOut:
    run = (
        await session.scalars(
            select(OptimizationRunRow).order_by(OptimizationRunRow.created_at.desc()).limit(1)
        )
    ).first()
    if run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no optimisation run has been recorded yet",
        )

    stmt = select(ScheduleDecisionRow).where(ScheduleDecisionRow.run_id == run.run_id)
    if action is not None:
        stmt = stmt.where(ScheduleDecisionRow.action == action)
    rows = list(
        (
            await session.execute(
                stmt.order_by(ScheduleDecisionRow.window_from, ScheduleDecisionRow.process_id)
            )
        )
        .scalars()
        .all()
    )
    return SchedulePlanOut(
        run=OptimizationRunOut.model_validate(run),
        count=len(rows),
        decisions=[ScheduleDecisionOut.model_validate(row) for row in rows],
    )


@api_router.post(
    "/optimize/run",
    response_model=OptimizeRunResponse,
    tags=["schedule"],
    summary="Trigger an optimisation run now (operator or admin)",
)
async def trigger_optimization(principal: OperatorDep) -> OptimizeRunResponse:
    """Run the optimiser on demand and persist the resulting schedule.

    The run is durable immediately. Publishing to ``ecogrid.decisions.schedule``
    is left to the optimizer service, which holds the Kafka producer — the API
    stays a read/trigger surface and owns no producer of its own.
    """
    from ecogrid.optimizer.loop import build_plan, load_processes, persist_plan

    processes = load_processes(settings)
    plan = await build_plan(state.session_factory, settings)
    if plan.decisions:
        await persist_plan(state.session_factory, plan, len(processes))

    return OptimizeRunResponse(
        run_id=plan.run_id,
        solver=plan.solver,
        horizon_windows=plan.horizon_windows,
        process_count=len(processes),
        decision_count=len(plan.decisions),
        baseline_carbon_kg=round(plan.baseline_carbon_kg, 3),
        optimized_carbon_kg=round(plan.optimized_carbon_kg, 3),
        carbon_saved_kg=round(plan.carbon_saved_kg, 3),
        saving_pct=round(plan.saving_pct, 2),
        unscheduled=list(plan.unscheduled),
        notes=list(plan.notes),
        status="ok" if plan.decisions else "skipped",
    )


# Mount the versioned router LAST — see the note at its definition.
app.include_router(api_router)
