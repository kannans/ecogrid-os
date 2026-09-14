"""EcoGrid OS Platform Core (Phase 2).

Consumes the Phase 1 telemetry stream into PostgreSQL, serves it through an
authenticated FastAPI service, and uses Redis for hot reads and rate limiting.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.2.0"
