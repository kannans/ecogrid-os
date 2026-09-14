"""Kafka authentication wiring.

The failure this guards against is subtle and expensive: a client that omits
SASL works perfectly against a local broker and is rejected only in production.
So the helper must be inert by default, complete when enabled, and loud when
half-configured.
"""

from __future__ import annotations

import pytest

from ecogrid.config import PlatformSettings
from ecogrid.kafka import security_kwargs


def test_plaintext_is_the_default_and_adds_nothing() -> None:
    """The default local stack must be byte-for-byte unchanged."""
    assert security_kwargs(PlatformSettings()) == {}
    assert security_kwargs(PlatformSettings(kafka_security_protocol="PLAINTEXT")) == {}


def test_sasl_plaintext_supplies_credentials() -> None:
    kwargs = security_kwargs(
        PlatformSettings(
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="SCRAM-SHA-256",
            kafka_sasl_username="ecogrid",
            kafka_sasl_password="secret",  # noqa: S106
        )
    )
    assert kwargs == {
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "SCRAM-SHA-256",
        "sasl_plain_username": "ecogrid",
        "sasl_plain_password": "secret",
    }


def test_sasl_ssl_is_supported() -> None:
    kwargs = security_kwargs(
        PlatformSettings(
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_username="u",
            kafka_sasl_password="p",  # noqa: S106
        )
    )
    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "PLAIN"


@pytest.mark.parametrize("missing", ["username", "password"])
def test_sasl_without_credentials_raises(missing: str) -> None:
    """Half-configured auth must fail loudly, not connect anonymously."""
    creds = {"kafka_sasl_username": "ecogrid", "kafka_sasl_password": "secret"}
    creds[f"kafka_sasl_{missing}"] = None
    with pytest.raises(ValueError, match="requires both"):
        security_kwargs(
            PlatformSettings(kafka_security_protocol="SASL_PLAINTEXT", **creds)
        )


def test_unsupported_protocol_raises() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        security_kwargs(PlatformSettings(kafka_security_protocol="KERBEROS"))


def test_protocol_is_case_insensitive() -> None:
    kwargs = security_kwargs(
        PlatformSettings(
            kafka_security_protocol="sasl_plaintext",
            kafka_sasl_username="u",
            kafka_sasl_password="p",  # noqa: S106
        )
    )
    assert kwargs["security_protocol"] == "SASL_PLAINTEXT"
