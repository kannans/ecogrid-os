"""Shared logging configuration.

Every EcoGrid process must emit the same line format, or log aggregation across
the worker, consumer, and API produces timestamps that sort differently and
cannot be correlated. Phase 1 established UTC ISO-8601 on stdout; this module is
the single definition so Phase 2 cannot drift from it.
"""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Sequence

#: UTC ISO-8601, container-friendly, lexicographically sortable.
LOG_FORMAT: str = "%(asctime)sZ %(levelname)-8s %(name)s :: %(message)s"
LOG_DATEFMT: str = "%Y-%m-%dT%H:%M:%S"

#: Third-party loggers that are noisy at INFO and whose output we supersede with
#: our own request/consumer accounting.
DEFAULT_NOISY: tuple[str, ...] = ("httpx", "aiokafka", "aiokafka.consumer.group_coordinator", "uvicorn.access")


def configure_logging(level: str = "INFO", *, noisy: Sequence[str] | None = None) -> None:
    """Configure root logging for a process entrypoint."""
    # Render timestamps in UTC regardless of the host timezone.
    logging.Formatter.converter = time.gmtime  # type: ignore[assignment]

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATEFMT))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    for name in noisy if noisy is not None else DEFAULT_NOISY:
        logging.getLogger(name).setLevel(logging.WARNING)
