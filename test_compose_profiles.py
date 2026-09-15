"""Compose integrity checks.

Two classes of bug bit this project, both invisible until someone ran the exact
command that tripped them, and both trivially preventable by asserting the
invariant in CI rather than relying on anyone remembering.

1. **Cross-profile dependencies.** A service gated behind a profile that is not
   active makes the whole command fail with `no such service: <name>`. Seen twice:

     * `docker compose --profile phase3 logs plant-bridge` -> no such service: migrate
     * `docker compose --profile gateway up gateway`       -> no such service: api

2. **Inheriting the image's default CMD.** The platform image declares exactly
   one `CMD` (the consumer). Any service that uses it without overriding
   `command` silently runs the wrong process — which is what made the one-shot
   `migrate` container start a consumer and never exit, blocking everything that
   waited on it.

Both are cheap to check and expensive to discover.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

COMPOSE = pathlib.Path(__file__).parent / "docker-compose.yml"
PLATFORM_IMAGE_PREFIX = "ecogrid/platform-core"


def _services() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"]


def _profiles(service: dict) -> set[str]:
    return set(service.get("profiles") or [])


def _dependency_names(service: dict) -> list[str]:
    depends = service.get("depends_on") or {}
    return list(depends) if isinstance(depends, dict) else list(depends)


def test_compose_parses() -> None:
    services = _services()
    assert services, "docker-compose.yml defines no services"


def test_every_dependency_shares_a_profile_with_its_dependant() -> None:
    """Enabling a service's profile must also enable everything it depends on.

    A dependency with no profiles is always in the model and is fine. Otherwise
    the two must share at least one profile, because a service may declare
    several and is enabled when *any* of them is active.
    """
    services = _services()
    problems: list[str] = []

    for name, service in services.items():
        for dependency in _dependency_names(service):
            assert dependency in services, f"{name} depends on unknown service {dependency}"
            dep_profiles = _profiles(services[dependency])
            if not dep_profiles:
                continue  # always enabled
            if not (_profiles(service) & dep_profiles):
                problems.append(
                    f"{name} (profiles={sorted(_profiles(service)) or 'none'}) depends on "
                    f"{dependency} (profiles={sorted(dep_profiles)}): no shared profile, so "
                    f"enabling {name}'s profile fails with 'no such service: {dependency}'"
                )

    assert not problems, "\n".join(problems)


def test_platform_image_services_override_the_default_command() -> None:
    """Only the consumer may rely on the image's default CMD."""
    offenders = [
        name
        for name, service in _services().items()
        if str(service.get("image", "")).startswith(PLATFORM_IMAGE_PREFIX)
        and not service.get("command")
    ]
    assert not offenders, (
        "these services use the platform image without an explicit `command`, so "
        f"they would inherit the consumer CMD: {offenders}"
    )


def test_one_shot_services_do_not_restart() -> None:
    """`migrate` and `certs-init` must exit, or dependants wait forever."""
    services = _services()
    for name in ("migrate", "certs-init"):
        assert services[name].get("restart") == "no", f"{name} must set restart: 'no'"


def test_gateway_profile_closure_is_complete() -> None:
    """Regression: `--profile gateway up gateway` failed with 'no such service: api'.

    Every service the gateway profile enables must have all of its dependencies
    enabled by that same profile — transitively, which is why this walks the
    closure rather than checking only direct dependencies.
    """
    services = _services()
    gateway_profiles = _profiles(services["gateway"])
    assert gateway_profiles, "gateway should be profile-gated"

    enabled = {
        name
        for name, service in services.items()
        if not _profiles(service) or (_profiles(service) & gateway_profiles)
    }
    for name in sorted(enabled):
        for dependency in _dependency_names(services[name]):
            assert dependency in enabled, (
                f"the gateway profile enables {name} but not its dependency "
                f"{dependency} (profiles={sorted(_profiles(services[dependency]))})"
            )


def _command_strings(service: dict) -> list[str]:
    command = service.get("command")
    if command is None:
        return []
    if isinstance(command, str):
        return [command]
    return [str(part) for part in command]


def _compose_files() -> list[pathlib.Path]:
    base = COMPOSE.parent
    return [
        path
        for path in (COMPOSE, base / "docker-compose.ha.yml", base / "docker-compose.sasl.yml")
        if path.exists()
    ]


def test_no_bare_dollar_in_compose_commands() -> None:
    """Compose interpolates `$VAR` in YAML *before* the container shell sees it.

    A bare `$BS` inside an embedded shell script is replaced with an empty
    string — silently yielding `kafka-topics --bootstrap-server ""` and five
    `WARN The "BS" variable is not set` lines. Anything intended for the
    container shell must be escaped as `$$VAR`. `${VAR}` is deliberate Compose
    interpolation and is fine.
    """
    import re

    bare_dollar = re.compile(r"(?<!\$)\$(?![\{$])")
    problems: list[str] = []

    for path in _compose_files():
        services = yaml.safe_load(path.read_text(encoding="utf-8"))["services"]
        for name, service in services.items():
            for command in _command_strings(service):
                for line_no, line in enumerate(command.splitlines(), start=1):
                    # Shell comments are not executed, so `$VAR` in prose is
                    # fine — only real command lines matter.
                    if line.strip().startswith("#"):
                        continue
                    if bare_dollar.search(line):
                        problems.append(f"{path.name}: {name} line {line_no}: {line.strip()[:80]}")

    assert not problems, (
        "bare `$` will be consumed by Compose interpolation (use `$$`):\n"
        + "\n".join(problems)
    )


def test_sasl_listener_name_has_no_underscore() -> None:
    """Confluent splits the JAAS env var on every underscore.

    `KAFKA_LISTENER_NAME_SASL_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG` becomes
    `listener.name.sasl.plaintext.plain.sasl.jaas.config`, which is not a Kafka
    setting — so the JAAS is dropped and the broker starts unhealthy. A
    single-word listener name avoids the ambiguity.
    """
    path = COMPOSE.parent / "docker-compose.sasl.yml"
    if not path.exists():
        pytest.skip("sasl override not present")

    kafka_env = yaml.safe_load(path.read_text(encoding="utf-8"))["services"]["kafka"]["environment"]

    protocols = {
        name: protocol
        for name, protocol in (
            entry.split(":", 1)
            for entry in str(kafka_env["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"]).split(",")
        )
    }
    sasl_listeners = [name for name, protocol in protocols.items() if protocol.startswith("SASL")]
    assert sasl_listeners, "expected at least one SASL listener in the protocol map"

    underscored = [name for name in sasl_listeners if "_" in name]
    assert not underscored, (
        f"SASL listener names must not contain underscores — Confluent splits the "
        f"JAAS env var on every underscore and the config is dropped: {underscored}"
    )

    # And the JAAS env var must actually be present under the mapped name.
    for name in sasl_listeners:
        assert f"KAFKA_LISTENER_NAME_{name.upper()}_PLAIN_SASL_JAAS_CONFIG" in kafka_env, (
            f"no JAAS config found for SASL listener {name!r}"
        )
