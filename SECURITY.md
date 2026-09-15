# Security Policy

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Report privately to the maintainers — use GitHub's *Report a vulnerability*
(Security → Advisories) on this repository, or email the maintainer address on
the repository owner's profile.

Include: what you found, how to reproduce it, the impact you believe it has, and
any suggested fix. We aim to acknowledge within a few working days.

Please do **not** test against systems you do not own. Everything in this
repository is designed to run locally; there is no hosted service to probe.

---

## What this system protects

| Asset | Why it matters |
|---|---|
| **API credentials** | A key grants read access to plant operations data; an admin key grants key management |
| **Audit log integrity** | The record of who did what is the compliance evidence. Its value depends entirely on it being append-only |
| **Plant telemetry** | Reveals operational patterns of an industrial site |
| **Grid/schedule decisions** | Could disclose commercially sensitive operating strategy |

## Trust boundaries

```
  browser ──TLS──▶ [ nginx gateway ] ──▶ [ FastAPI ] ──▶ [ PostgreSQL ]
                          │                   │
                          │                   └──▶ [ Redis ]   (cache, rate limits)
                          │
  AS400 bridge ───────────┴──▶ [ Kafka ] ◀── optimizer / orchestrator
```

Two independent hops, frequently conflated:

1. **client → platform** — protected by TLS at the gateway.
2. **platform → broker** — protected by SASL (`docker-compose.sasl.yml`), which
   is **off by default**.

Enabling the gateway's TLS does **not** authenticate hop 2. They are separate
controls and must be enabled separately.

## Controls in place

| Control | Implementation | Notes |
|---|---|---|
| **API key hashing** | SHA-256 digest; raw value shown once at creation and never stored | A database leak does not yield usable credentials |
| **Ranked RBAC** | `viewer < operator < admin`, enforced by a dependency factory | Denials are audited as `access.denied` |
| **Rate limiting — application** | Per-key fixed window (default 120/min), keyed on a **digest** of the key | Never the raw key: Redis keys surface in `MONITOR`, slow logs and backups |
| **Rate limiting — edge** | nginx per-IP, 20 r/s burst 40 | Defence in depth; independent of the app limiter |
| **Rate limiting order** | Applied **before** authentication | A credential-stuffing loop gets throttled |
| **Fail-open limiter** | Redis outage allows requests through | Deliberate: a cache outage must not become an outage |
| **Append-only audit** | No update or delete helper exists anywhere in the codebase | Corrections are new rows |
| **Unauthenticated healthcheck** | `/healthz` only; not rate limited | Orchestrators cannot present credentials; throttling it would fake failures |
| **TLS termination** | nginx, outside the application process | The app never holds a private key |
| **Non-root containers** | Platform image runs as a dedicated unprivileged UID | |
| **Input validation** | Strict Pydantic models; invalid records rejected, never forwarded | |
| **Secret hygiene** | `.env` gitignored; only `.env.example` tracked; DSNs redacted in logs | Verified: no credentials in git history |
| **SASL support** | All Kafka clients share one auth path; half-configured auth fails loudly | `ecogrid/kafka.py` |
| **No secrets in error responses** | Uniform error envelope; 401/403 details are non-specific | |

## Known gaps and accepted risks

Stated plainly. These are **deliberate** for a local development system and are
**not acceptable** for a production deployment.

| Gap | Risk | Required before production |
|---|---|---|
| **Self-signed TLS certificates** | Clients cannot verify the gateway's identity; MITM is trivial | Replace `./certs` with certificates from a real CA |
| **Kafka is PLAINTEXT by default** | Anything on the Docker network can read/write the event spine | Enable `docker-compose.sasl.yml`, and prefer `SASL_SSL` on any shared network. PLAIN sends credentials in cleartext |
| **Single broker (RF=1)** | Losing the broker loses the event spine | Use `docker-compose.ha.yml` (RF=3, min-ISR 2) |
| **Static API keys, no rotation or expiry** | A leaked key is valid until manually revoked | Add expiry and a rotation policy |
| **No MFA / SSO** | A key is a bearer token with no second factor | Put an identity-aware proxy in front, or integrate an IdP |
| **Coarse authorisation** | Roles are global; there is no per-resource or per-plant scoping | Add resource-scoped policy if multiple sites are ever served |
| **Single-tenant** | One audit boundary for one site | Tenancy isolation is an architecture change, not a config change |
| **No DLQ consumer** | Rejected messages are retained but unread, so failures can go unnoticed | Add a DLQ reader with alerting |
| **Redis unauthenticated** | Anyone on the Docker network can read the cache and tamper with rate-limit counters | Require AUTH and TLS for Redis |
| **PostgreSQL credentials in `.env`** | Fine for local; a secret manager is required otherwise | Move to a secret manager |
| **Rate limiter fails open** | A Redis outage removes application-level throttling | Accept (edge limiter still applies), or fail closed for sensitive routes |

## Hardening checklist

Before exposing this to anything beyond a local machine:

- [ ] Real TLS certificates in `./certs`; remove the self-signed generation
- [ ] Enable SASL (`-f docker-compose.sasl.yml`), and prefer `SASL_SSL`
- [ ] Enable Redis AUTH and TLS
- [ ] Move database credentials out of `.env` into a secret manager
- [ ] Enable the HA topology (`-f docker-compose.ha.yml`)
- [ ] Add key expiry and rotation
- [ ] Put an identity-aware proxy (or IdP) in front of the API
- [ ] Restrict the published host ports — or remove the direct `api` mapping so
      the gateway is the only way in
- [ ] Add a DLQ consumer with alerting
- [ ] Confirm no secrets are committed (grep git history, not just the tree)

## Operational security notes

**Key material is not recoverable.** `ecogrid.keys create` prints the raw key
once. Losing it means revoking and reissuing — by design.

**The bootstrap admin key is printed once** by `migrate` on first provisioning.
It will not be regenerated if an admin already exists. Re-run
`python -m ecogrid.keys create --name <n> --role admin` if it is lost.

**Synthetic demo data is labelled.** `scripts/seed_demo_data.py` writes records
with `source: demo-seed`. They are indistinguishable from real telemetry in
aggregate metrics, so purge them before using any figure for reporting:

```sql
DELETE FROM grid_telemetry  WHERE source = 'demo-seed';
DELETE FROM plant_telemetry WHERE source = 'demo-seed-plant';
```

**The audit log is evidence, not telemetry.** It records denials as well as
successes, and includes the client IP as seen by the API (honouring one proxy
hop). If you put another proxy in front, revisit
`client_ip()` in `ecogrid/api.py`.
