"""Platform Core configuration.

Shares the ``ECOGRID_`` environment prefix with the Phase 1 ingestion worker, so
variables common to both (``ECOGRID_KAFKA_BOOTSTRAP_SERVERS``,
``ECOGRID_KAFKA_TOPIC``) are declared once in ``.env`` and resolve identically in
every process.
"""

from __future__ import annotations

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PlatformSettings(BaseSettings):
    """Runtime configuration for the Platform Core (consumer + API)."""

    model_config = SettingsConfigDict(
        env_prefix="ECOGRID_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- PostgreSQL ---
    #: Accepts either ``postgresql://`` or ``postgresql+asyncpg://``; the async
    #: driver is injected automatically (see the validator below).
    postgres_dsn: str = (
        "postgresql+asyncpg://ecogrid:ecogrid_dev_password@localhost:5432/ecogrid"
    )
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=5, ge=0)
    db_pool_recycle_seconds: int = Field(default=1800, ge=0)
    db_statement_timeout_ms: int = Field(default=15_000, ge=0)
    db_echo: bool = False

    # --- Redis ---
    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = Field(default=20, ge=1)
    #: Hot-read TTL for the latest telemetry snapshot.
    telemetry_cache_ttl_seconds: int = Field(default=900, ge=0)
    telemetry_cache_key: str = "ecogrid:telemetry:latest"

    # --- Kafka (consumer) ---
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "ecogrid.telemetry.carbon"
    kafka_consumer_group: str = "ecogrid-platform-core"
    kafka_client_id: str = "ecogrid-telemetry-consumer"
    #: ``earliest`` so a fresh consumer group backfills the retained history.
    kafka_auto_offset_reset: str = "earliest"
    kafka_session_timeout_ms: int = Field(default=30_000, ge=1_000)
    kafka_max_poll_records: int = Field(default=200, ge=1)
    kafka_commit_batch_size: int = Field(default=50, ge=1)
    kafka_retry_backoff_seconds: float = Field(default=5.0, gt=0)

    # --- API ---
    api_title: str = "EcoGrid OS — Platform Core"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_docs_enabled: bool = True
    api_max_page_size: int = Field(default=500, ge=1)
    api_default_page_size: int = Field(default=50, ge=1)

    # --- Auth ---
    #: Header carrying the API credential. `Authorization: Bearer <key>` is also
    #: accepted, so both machine and browser-style clients work unchanged.
    api_key_header: str = "X-API-Key"
    #: Optional fixed bootstrap key. If unset, `migrate` generates one and prints
    #: it once — never logged again thereafter.
    bootstrap_admin_key: str | None = None

    # --- Rate limiting ---
    rate_limit_enabled: bool = True
    rate_limit_requests: int = Field(default=120, ge=1)
    rate_limit_window_seconds: int = Field(default=60, ge=1)

    # --- Logging ---
    log_level: str = "INFO"

    @field_validator("postgres_dsn", mode="before")
    @classmethod
    def _force_async_driver(cls, value: object) -> object:
        """Normalise a plain DSN to the asyncpg driver.

        ``.env`` files written for the Phase 1 worker use the bare
        ``postgresql://`` scheme (which psql and most tooling expect). Rather
        than force operators to remember a second spelling, rewrite it here.
        """
        if isinstance(value, str):
            if value.startswith("postgresql+"):
                return value
            if value.startswith("postgresql://"):
                return value.replace("postgresql://", "postgresql+asyncpg://", 1)
            if value.startswith("postgres://"):
                return value.replace("postgres://", "postgresql+asyncpg://", 1)
        return value

    @property
    def dsn_for_logs(self) -> str:
        """DSN with the password redacted, safe to log."""
        dsn = self.postgres_dsn
        if "@" not in dsn:
            return dsn
        scheme, _, rest = dsn.partition("://")
        credentials, _, host = rest.partition("@")
        user = credentials.split(":", 1)[0]
        return f"{scheme}://{user}:***@{host}"
