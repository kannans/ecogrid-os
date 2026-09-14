"""Shared Kafka client authentication.

Every Kafka client in the platform — grid consumer, plant bridge, plant consumer,
optimizer, orchestrator, and every DLQ producer — must authenticate identically.
Centralising that here means SASL is configured in one place instead of six, and
cannot be quietly forgotten in one of them. A client that omits auth works
perfectly against a local broker and fails only in production, which is the worst
possible place to discover it.

Note on the two hops:

* client → platform   : TLS, terminated by the nginx gateway
* platform → **broker**: this module

They are independent. Turning on the gateway's TLS does **not** authenticate the
platform's own connections to Kafka.
"""

from __future__ import annotations

import logging
from typing import Any

from ecogrid.config import PlatformSettings

logger = logging.getLogger("ecogrid.kafka")

#: Protocols that require SASL credentials to be supplied.
SASL_PROTOCOLS = frozenset({"SASL_PLAINTEXT", "SASL_SSL"})
PLAINTEXT = "PLAINTEXT"


def security_kwargs(settings: PlatformSettings) -> dict[str, Any]:
    """Auth kwargs for ``AIOKafkaProducer`` / ``AIOKafkaConsumer``.

    Returns an empty dict for ``PLAINTEXT`` so the default local stack is
    byte-for-byte unchanged. Raises on a half-configured setup rather than
    connecting without credentials.
    """
    protocol = (settings.kafka_security_protocol or PLAINTEXT).strip().upper()

    if protocol == PLAINTEXT:
        return {}

    if protocol not in SASL_PROTOCOLS:
        raise ValueError(
            f"unsupported ECOGRID_KAFKA_SECURITY_PROTOCOL={protocol!r} "
            f"(expected {PLAINTEXT}, SASL_PLAINTEXT or SASL_SSL)"
        )

    if not settings.kafka_sasl_username or not settings.kafka_sasl_password:
        # Fail loudly and early. Silently falling back to no auth would produce a
        # client that works locally and is rejected by the production broker.
        raise ValueError(
            f"{protocol} requires both ECOGRID_KAFKA_SASL_USERNAME and "
            f"ECOGRID_KAFKA_SASL_PASSWORD to be set"
        )

    logger.info(
        "Kafka SASL enabled | protocol=%s mechanism=%s user=%s",
        protocol,
        settings.kafka_sasl_mechanism,
        settings.kafka_sasl_username,
    )
    return {
        "security_protocol": protocol,
        "sasl_mechanism": settings.kafka_sasl_mechanism,
        "sasl_plain_username": settings.kafka_sasl_username,
        "sasl_plain_password": settings.kafka_sasl_password,
    }
