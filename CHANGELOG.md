# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

Nothing yet.

---

## [0.2.0] — 2026-09-15

Initial public release.

Every capability below was executed against a running stack before being
claimed; the evidence is recorded per use case in
[`docs/VERIFICATION.md`](docs/VERIFICATION.md).

### Added

**Grid ingestion and the event backbone**
- Async worker polling the UK Carbon Intensity API for both `/intensity` and
  `/generation` concurrently, correlated on the `(from, to)` window rather than
  by array position.
- Strict Pydantic contract (`GridTelemetry`) shared by producer and consumer,
  with `schema_version` for forward compatibility.
- Kafka publishing keyed by window start, with in-process dedupe, bounded
  retry/backoff, and a durable JSONL spool that replays and truncates on
  recovery.

**Platform Core**
- Idempotent Kafka→PostgreSQL consumer: upsert on `window_from`, with
  `revision_count` incrementing only on genuine content change.
- Per-partition ingest audit ledger, reconcilable against the topic.
- Dead-letter topic with offset advance, so one poison message cannot stall a
  partition.
- Redis hot cache for the newest window, with a PostgreSQL fallback.
- FastAPI read API with API-key authentication (SHA-256 digests, never
  plaintext), ranked RBAC (`viewer < operator < admin`), per-key rate limiting
  applied *before* authentication, and an append-only audit log.

**Plant operations and optimization**
- AS400/legacy plant bridge with three source adapters — `simulated`, `file`
  (batch drop) and an `odbc` seam that fails loudly rather than fabricating
  mainframe data.
- Plant telemetry consumer with the same idempotent semantics as the grid path.
- Carbon-arbitrage optimizer placing each flexible process into the lowest-carbon
  contiguous block that fits, honouring a per-window flexible capacity ceiling,
  and reporting anything it cannot place as `unscheduled` with a reason.
- Databricks job seam with a local greedy solver fallback.

**AI Orchestrator**
- Claude-backed advisor over the optimizer's output, with schema validation
  before storage.
- Deterministic heuristic advisor as a fallback, so advice is never absent when
  the model is unconfigured, unreachable, or returns something unusable.
- Run tracking in MLflow when configured, otherwise a local JSONL sink.

**Operator surface**
- React + TypeScript dashboard: platform health, grid intensity, plant load,
  dispatch schedule and orchestrator advice.
- nginx edge gateway: TLS termination, static hosting, per-IP rate limiting, and
  reverse proxying to the API.

**Tooling and operations**
- Alembic migrations with a baseline covering all eight tables.
- `scripts/seed_demo_data.py` — synthetic history published **through the real
  pipeline** for demos and verification, tagged `source: demo-seed`.
- Optional SASL broker authentication and a 3-broker HA topology (RF=3), both as
  compose overrides.
- 118 offline tests, and 18 numbered verification use cases.

### Fixed

Twelve defects were found during development. **The automated suite caught none
of them** — every one surfaced by executing the system. They are listed with the
symptom that exposed them, because the symptom is the useful part.

| Symptom | Cause | Fix |
|---|---|---|
| `POST /optimize/run` returned HTTP 500 with an empty body | Flexible capacity was built from the *plant's* window history, not the grid's, so the solver indexed past the end of it. Masked twice: it only appeared once plant telemetry existed, and the optimizer loop swallowed the exception | Capacity aligned per grid window; `solve()` now validates the length and raises a descriptive error |
| `no such service: migrate` | Cross-profile `depends_on`: `migrate` was gated behind `platform`, but `phase3` services depend on it | `migrate` declares all four profiles it is depended on by |
| `no such service: api` | The same class again — `gateway` depends on `api`, which was gated behind `platform` | `api` declares `platform` and `gateway`; a test now asserts profile closure |
| Gateway returned **502** on `/api` while `/` still served | nginx resolves a hostname in `proxy_pass` once at startup; rebuilding `api` changed its IP | `resolver` + variable `proxy_pass`, so it re-resolves |
| Every `alembic` command died: `No module named 'psycopg2'` | `env.py` stripped `+asyncpg`, and SQLAlchemy resolves a bare `postgresql://` to psycopg2 | Alembic drives the async engine via `connection.run_sync()`; asyncpg remains the only driver |
| `alembic revision` died: `FileNotFoundError: script.py.mako` | The template was never added, so migrations could be *applied* but never *created* | Template added; the drift check then found and resolved a real redundant constraint |
| `WARN The "BS" variable is not set` (×5) | A bare `$BS` in an embedded shell script is interpolated by **Compose**, not the shell, yielding `--bootstrap-server ""` | Escaped to `$$BS`; a lint now rejects bare `$` in compose commands |
| Broker unhealthy with the `sasl` override | The listener was named `SASL_PLAINTEXT`; Confluent splits the JAAS env var on every underscore, producing an invalid config key | Listener renamed `SASL`; a test asserts SASL listener names are underscore-free |
| `from ecogrid.plant.models import PlantTelemetry` raised `ModuleNotFoundError: aiokafka` | The package `__init__` eagerly imported the consumer, so a data contract required a Kafka client | Lazy re-exports (PEP 562) |
| Dashboard showed a schedule beside advice saying "no run available yet" | The orchestrator runs on its own cadence, so advice can legitimately predate the current run | The panel detects the mismatch and names both runs |
| Dashboard showed "Newest window is -7.9 min old" | Age was computed against `window_to`, and the in-progress window ends in the future | Clamped at zero |
| `PlantTelemetry` rejected seeded records for a 0.001 MW overshoot | Rounding total, flexible and inflexible MW independently lets the parts exceed the whole | Derive the inflexible part from the other two |

### Security

- No credentials in the repository or its history; `.env` is gitignored and only
  `.env.example` is tracked.
- API keys stored as SHA-256 digests; the raw value is shown once at creation.
- Rate-limiter identity is a digest, never the raw key — Redis keys surface in
  `MONITOR`, slow logs and backups.
- Optional SASL for the platform→broker hop, distinct from the gateway's TLS.
- See [`SECURITY.md`](SECURITY.md) for the threat model and the accepted risks.

### Known limitations

- **UC-11** (bridge Kafka-outage spool/replay) is implemented and unit-tested but
  has not been triggered against a live broker.
- The `sasl` and `ha` topology overrides are configuration-verified, not booted.
- The AS400 ODBC driver and Databricks job are seams, not integrations.
- No licence for the upstream data; the Carbon Intensity API's own terms apply to
  data it serves.

[Unreleased]: https://example.invalid/compare/v0.2.0...HEAD
[0.2.0]: https://example.invalid/releases/tag/v0.2.0
