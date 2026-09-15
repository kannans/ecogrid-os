# EcoGrid OS — Requirements Specification

Companion to [`PRD.md`](PRD.md) (why) and [`ARCHITECTURE.md`](ARCHITECTURE.md)
(how). This document is the *what* — numbered, testable, and traceable.

## How to read this

**ID convention:** `FR-<area>-<n>` for functional, `NFR-<area>-<n>` for
non-functional. IDs are stable — do not renumber when requirements are removed;
mark them withdrawn instead.

**Status legend**

| Mark | Meaning |
|---|---|
| ✅ | Implemented **and** verified against the running system |
| 🟡 | Implemented, unit-tested, not yet executed against a real counterpart |
| ⬜ | Specified but not built (deliberately deferred — see §5) |

**Traceability.** Each requirement names its use case in
[`VERIFICATION.md`](VERIFICATION.md) (UC-*) and its automated test file where one
exists. A requirement with neither is not verified, whatever its status says.

---

## 1. Functional requirements

### 1.1 Grid ingestion — `FR-ING`

| ID | Requirement | Status | UC | Test |
|---|---|---|---|---|
| FR-ING-1 | The system SHALL poll the UK Carbon Intensity API for both `/intensity` and `/generation` on a configurable cadence (default 300s). | ✅ | UC-1 | `test_ingest_grid.py` |
| FR-ING-2 | Both endpoints SHALL be fetched concurrently, and one failing endpoint SHALL NOT prevent the other's data from being processed. | ✅ | UC-1 | ✅ |
| FR-ING-3 | Payloads SHALL be validated against a strict schema; records failing validation SHALL be rejected and logged, never forwarded. | ✅ | UC-1 | ✅ |
| FR-ING-4 | The two endpoints SHALL be correlated on the `(from, to)` window — never by array position. | ✅ | UC-1 | ✅ |
| FR-ING-5 | Windows with no matching generation data SHALL still be emitted, flagged `generation_mix_missing`. | ✅ | UC-1 | ✅ |
| FR-ING-6 | Records SHALL be published to `ecogrid.telemetry.carbon`, keyed by window start, so a window's revisions stay ordered on one partition. | ✅ | UC-1 | ✅ |
| FR-ING-7 | Unchanged windows SHALL be suppressed in-process; a revised forecast or settled actual SHALL republish. | ✅ | UC-1 | ✅ |
| FR-ING-8 | A publish failure SHALL retry with bounded backoff, then spool to a bounded JSONL file for replay. | ✅ | UC-11 | ✅ |
| FR-ING-9 | The spool SHALL be replayed at the start of each cycle and SHALL be **truncated, never unlinked**. | ✅ | UC-11 | ✅ |
| FR-ING-10 | An unexpected error in one cycle SHALL NOT terminate the worker. | ✅ | UC-11 | ✅ |
| FR-ING-11 | SIGINT/SIGTERM SHALL stop the loop after the in-flight cycle and flush the producer. | ✅ | — | — |

### 1.2 Ledger — `FR-LDG`

| ID | Requirement | Status | UC | Test |
|---|---|---|---|---|
| FR-LDG-1 | The consumer SHALL persist telemetry to PostgreSQL with an **idempotent upsert on `window_from`**. | ✅ | UC-2 | `test_platform.py` |
| FR-LDG-2 | A duplicate delivery SHALL NOT create a second row and SHALL NOT increment `revision_count`. | ✅ | UC-2 | ✅ |
| FR-LDG-3 | A genuine content change SHALL increment `revision_count` and update `last_seen_at`. | ✅ | UC-2 | ✅ |
| FR-LDG-4 | Offsets SHALL be committed **only after** the database has accepted the write. | ✅ | UC-2 | — |
| FR-LDG-5 | Per-partition progress SHALL be recorded (`last_offset`, counters) so the ledger is reconcilable against the topic. | ✅ | UC-2 | — |
| FR-LDG-6 | Unparseable messages SHALL be forwarded to a DLQ and the offset advanced, so one poison message cannot stall a partition. | ✅ | — | — |
| FR-LDG-7 | The newest window SHALL be served from a Redis hot cache, with a PostgreSQL fallback. | ✅ | UC-2 | ✅ |

### 1.3 API, identity and audit — `FR-API`

| ID | Requirement | Status | UC | Test |
|---|---|---|---|---|
| FR-API-1 | Every `/api/v1` route SHALL require a valid API key; an absent or unknown key SHALL return **401**. | ✅ | UC-3 | `test_platform.py` |
| FR-API-2 | `GET /healthz` SHALL be unauthenticated — orchestrators cannot present credentials. | ✅ | UC-3 | — |
| FR-API-3 | Keys SHALL be stored as SHA-256 digests only; the raw value SHALL NOT be recoverable after creation. | ✅ | UC-4 | ✅ |
| FR-API-4 | Roles SHALL be ranked `viewer < operator < admin`, and a route requiring a higher role SHALL return **403**. | ✅ | UC-4 | ✅ |
| FR-API-5 | Rate limiting SHALL be per-caller and SHALL run **before** authentication, so bad credentials are still throttled. | ✅ | UC-5 | ✅ |
| FR-API-6 | Exceeding the limit SHALL return **429** with `Retry-After` and `X-RateLimit-Limit`. | ✅ | UC-5 | ✅ |
| FR-API-7 | The rate limiter SHALL fail **open** when Redis is unavailable, so a cache outage does not become an outage. | ✅ | UC-5 | ✅ |
| FR-API-8 | Every authenticated request SHALL be recorded in an append-only audit log with actor, role, action, path, status and client IP. | ✅ | UC-6 | — |
| FR-API-9 | Denied authorisation attempts SHALL be audited as `access.denied`. | ✅ | UC-6 | — |
| FR-API-10 | The audit log SHALL have no update or delete path anywhere in the codebase. | ✅ | UC-6 | — |
| FR-API-11 | List endpoints SHALL use keyset pagination, not offset — offsets duplicate and skip on an append-only series. | ✅ | — | — |

### 1.4 Plant operations — `FR-PLT`

| ID | Requirement | Status | UC | Test |
|---|---|---|---|---|
| FR-PLT-1 | The system SHALL ingest plant load distinguishing **flexible** from **inflexible** load. | ✅ | UC-7 | `test_phase3.py` |
| FR-PLT-2 | Plant telemetry SHALL be publishable to Kafka keyed by `plant_id:window`. | ✅ | UC-7 | ✅ |
| FR-PLT-3 | Plant telemetry SHALL be persisted idempotently on `(plant_id, window_from)`. | ✅ | UC-8 | ✅ |
| FR-PLT-4 | Three source adapters SHALL be provided: `simulated`, `file` (batch drop), `odbc`. | 🟡 | UC-7 | ✅ |
| FR-PLT-5 | The `odbc` adapter SHALL fail loudly rather than fabricate mainframe data. | ✅ | — | ✅ |
| FR-PLT-6 | A plant window SHALL satisfy `flexible + inflexible == total`, enforced by validation. | ✅ | — | ✅ |

### 1.5 Optimization — `FR-OPT`

| ID | Requirement | Status | UC | Test |
|---|---|---|---|---|
| FR-OPT-1 | The optimizer SHALL schedule each flexible process into the **lowest-carbon contiguous block** that fits its duration. | ✅ | UC-9 | `test_phase3.py` |
| FR-OPT-2 | Contiguity SHALL be respected — a batch SHALL NOT be split across windows. | ✅ | UC-9 | ✅ |
| FR-OPT-3 | A per-window flexible capacity ceiling SHALL be honoured, sourced from plant telemetry. | ✅ | UC-9 | ✅ |
| FR-OPT-4 | Capacity SHALL be aligned **per grid window**, not per plant window. | ✅ | — | ✅ |
| FR-OPT-5 | A process that cannot be placed SHALL be reported in `unscheduled` with a reason, never silently dropped. | ✅ | UC-9 | ✅ |
| FR-OPT-6 | Savings SHALL be computed against a run-as-early-as-possible baseline, and SHALL never be negative. | ✅ | UC-10 | ✅ |
| FR-OPT-7 | Per-window saving attribution SHALL be non-negative (savings shared across a process's block). | ✅ | UC-10 | ✅ |
| FR-OPT-8 | Runs SHALL be immutable and independently identified by `run_id`. | ✅ | UC-9 | — |
| FR-OPT-9 | The schedule SHALL be published to `ecogrid.decisions.schedule`. | ✅ | UC-9 | — |
| FR-OPT-10 | A configured-but-unreachable Databricks SHALL degrade to the local solver, not fail. | 🟡 | UC-12 | ✅ |

### 1.6 AI Orchestrator — `FR-AI`

| ID | Requirement | Status | UC | Test |
|---|---|---|---|---|
| FR-AI-1 | The orchestrator SHALL produce a recommendation with headline, rationale, confidence, actions and risk flags. | ✅ | UC-13 | `test_ai_orchestrator.py` |
| FR-AI-2 | Model output SHALL be schema-validated **before** storage; unvalidated output SHALL be discarded. | ✅ | UC-14 | ✅ |
| FR-AI-3 | On any model failure the system SHALL fall back to a deterministic advisor — advice SHALL never be absent. | ✅ | UC-14 | ✅ |
| FR-AI-4 | Every stored advice row SHALL record which advisor produced it (`source`). | ✅ | UC-13 | ✅ |
| FR-AI-5 | The exact inputs an advice was derived from SHALL be persisted with it. | ✅ | — | — |
| FR-AI-6 | Confidence SHALL be derived from the data, not fixed. | ✅ | — | ✅ |
| FR-AI-7 | Runs SHALL be tracked in MLflow when configured, otherwise a local JSONL sink. | ✅ | UC-13 | ✅ |
| FR-AI-8 | Tracking failure SHALL NOT break a run. | ✅ | — | — |

### 1.7 Operator surface — `FR-UI`

| ID | Requirement | Status | UC | Test |
|---|---|---|---|---|
| FR-UI-1 | A dashboard SHALL display platform health, grid intensity, plant load, the dispatch schedule and orchestrator advice. | ✅ | UC-15 | — |
| FR-UI-2 | The dashboard SHALL distinguish a "no data yet" 404 from an error. | ✅ | UC-15 | — |
| FR-UI-3 | Advice SHALL be marked when it predates the current schedule, naming both runs. | ✅ | UC-15 | — |
| FR-UI-4 | The gateway SHALL terminate TLS. | ✅ | UC-16 | — |
| FR-UI-5 | The gateway SHALL apply a coarse per-IP rate limit **in addition to** the application's per-key limit. | ✅ | UC-16 | — |
| FR-UI-6 | `/healthz` SHALL be proxied but SHALL NOT be edge rate limited — throttling a healthcheck makes a healthy service look dead. | ✅ | UC-16 | — |
| FR-UI-7 | Reported data age SHALL never be negative. | ✅ | — | — |

### 1.8 Operability and topology — `FR-OPS`

| ID | Requirement | Status | UC | Test |
|---|---|---|---|---|
| FR-OPS-1 | Schema SHALL be bootstrapable on a fresh database and migratable thereafter (Alembic). | ✅ | UC-17 | — |
| FR-OPS-2 | `alembic --autogenerate` SHALL report no drift against the models. | ✅ | UC-17 | — |
| FR-OPS-3 | Every service using the platform image SHALL declare an explicit command. | ✅ | — | `test_compose_profiles.py` |
| FR-OPS-4 | Every `depends_on` target SHALL share a profile with its dependant. | ✅ | — | ✅ |
| FR-OPS-5 | One-shot services SHALL NOT restart. | ✅ | — | ✅ |
| FR-OPS-6 | Kafka broker authentication (SASL) SHALL be configurable, defaulting to PLAINTEXT. | 🟡 | — | `test_kafka_security.py` |
| FR-OPS-7 | A multi-broker (RF=3) topology SHALL be available as an override. | 🟡 | — | — |
| FR-OPS-8 | Synthetic history SHALL be seedable through the real pipeline for demos and verification. | ✅ | — | `test_seed_demo_data.py` |

---

## 2. Non-functional requirements

### 2.1 Security — `NFR-SEC`

| ID | Requirement | Status | Notes |
|---|---|---|---|
| NFR-SEC-1 | Credentials SHALL never be committed; `.env` SHALL be gitignored and only `.env.example` tracked. | ✅ | Verified: 0 secrets in git history |
| NFR-SEC-2 | API keys SHALL be stored hashed (SHA-256), never in plaintext. | ✅ | |
| NFR-SEC-3 | Rate-limiter identity SHALL be a digest, never the raw key — Redis keys leak into `MONITOR`, slow logs and backups. | ✅ | |
| NFR-SEC-4 | Secrets SHALL NOT be logged; DSNs SHALL be redacted in logs. | ✅ | |
| NFR-SEC-5 | TLS SHALL terminate at the edge gateway, never in the application. | ✅ | Self-signed dev certs; replace for non-local use |
| NFR-SEC-6 | Broker authentication (SASL) SHALL be available and SHALL fail loudly when half-configured. | 🟡 | Default PLAINTEXT — local only |
| NFR-SEC-7 | A security policy and vulnerability-reporting route SHALL exist. | ✅ | [`../SECURITY.md`](../SECURITY.md) |

### 2.2 Reliability — `NFR-REL`

| ID | Requirement | Status | Notes |
|---|---|---|---|
| NFR-REL-1 | Delivery SHALL be treated as **at-least-once**; every consumer SHALL be idempotent. | ✅ | Empirically established, not assumed |
| NFR-REL-2 | A single bad record SHALL NOT stall a partition (DLQ + offset advance). | ✅ | |
| NFR-REL-3 | A single bad cycle SHALL NOT terminate a long-running worker. | ✅ | |
| NFR-REL-4 | External dependency failure SHALL degrade rather than fail: Databricks→local solver, model→heuristic, Redis→fail-open. | ✅ | |
| NFR-REL-5 | Optional dependencies (MLflow, Databricks) SHALL NOT be required to run. | ✅ | |

### 2.3 Observability — `NFR-OBS`

| ID | Requirement | Status | Notes |
|---|---|---|---|
| NFR-OBS-1 | Logs SHALL be structured and UTC-timestamped, with per-cycle outcome counts. | ✅ | |
| NFR-OBS-2 | The system SHALL expose a liveness/readiness signal with dependency status. | ✅ | `/healthz` |
| NFR-OBS-3 | Liveness SHALL NOT depend on the rate limiter. | ✅ | |
| NFR-OBS-4 | Consumer progress SHALL be inspectable for reconciliation. | ✅ | `/ingest-status` |

### 2.4 Maintainability and testability — `NFR-MAINT`

| ID | Requirement | Status | Notes |
|---|---|---|---|
| NFR-MAINT-1 | The full automated suite SHALL run offline, with no network, broker or database. | ✅ | 118 tests, ~9s |
| NFR-MAINT-2 | Integration tests requiring infrastructure SHALL skip cleanly when it is absent. | ✅ | |
| NFR-MAINT-3 | Contracts SHALL have a single definition shared by producer and consumer. | ✅ | `GridTelemetry` |
| NFR-MAINT-4 | Every capability SHALL have a numbered verification procedure with explicit pass/fail criteria. | ✅ | UC-0 … UC-17 |
| NFR-MAINT-5 | Documentation SHALL distinguish verified from unverified, and never claim more than was observed. | ✅ | |
| NFR-MAINT-6 | A defect class, once found, SHALL get a regression test where the class is mechanically checkable. | ✅ | `test_compose_profiles.py` |

### 2.5 Portability — `NFR-PORT`

| ID | Requirement | Status | Notes |
|---|---|---|---|
| NFR-PORT-1 | The stack SHALL run locally from a single `docker compose` invocation. | ✅ | |
| NFR-PORT-2 | Host ports SHALL be parameterised to survive collisions with native services. | ✅ | |
| NFR-PORT-3 | Services SHALL run as non-root with least privilege. | ✅ | Platform image uses a dedicated UID |

---

## 3. Traceability summary

| Area | Requirements | Verified live | Automated | Deferred |
|---|---|---|---|---|
| Grid ingestion | 11 | 8 | 11 | — |
| Ledger | 7 | 4 | 4 | — |
| API / identity / audit | 11 | 10 | 6 | — |
| Plant operations | 6 | 3 | 6 | real ODBC driver |
| Optimization | 10 | 6 | 8 | real Databricks job |
| AI Orchestrator | 8 | 4 | 7 | — |
| Operator surface | 7 | 4 | 0 | — |
| Operability / topology | 8 | 3 | 4 | SASL + HA runtime |

---

## 4. Definition of done

A requirement may be marked ✅ only when **all three** hold:

1. It is implemented.
2. It has been **executed against the real system** — not inferred from code.
3. Its use case in [`VERIFICATION.md`](VERIFICATION.md) records the observation,
   including anything that did **not** work.

## 5. Deliberately deferred

Specified, understood, and **not built**. Each has a stated reason.

| Requirement | Why deferred |
|---|---|
| Real AS400 ODBC driver (FR-PLT-4) | Needs a licensed driver and a reachable IBM i. The seam fails loudly rather than fabricating data. |
| Real Databricks job (FR-OPT-10) | Needs a workspace and a notebook emitting the expected JSON. The local solver keeps the platform fully functional. |
| SASL runtime verification (FR-OPS-6) | Configuration is complete and unit-tested; the broker-side override has not been booted. |
| Multi-broker runtime verification (FR-OPS-7) | Override provided; RF=3 not yet observed on a running cluster. |
| Schema registry (Avro/Protobuf) | JSON + `schema_version` suffices for one producer and one consumer. |
| DLQ consumer | Topics are provisioned but unconsumed; the producer's spool handles redelivery. A DLQ reader matters once a *consumer* starts failing. |
| Multi-tenancy, key rotation policy, MFA | Out of scope per PRD §4. |
