# EcoGrid OS

Autonomous industrial energy arbitrage & decarbonization platform.

**Current milestone: Phase 1 — Real-Time Grid Ingestion & The Event Backbone.**

This repository contains the local Dockerized infrastructure stack and the
Python ingestion worker that streams UK National Grid telemetry into Kafka.

---

## 1. Architecture

```text
                  React / TypeScript (Arbitrage Dashboard)
                                 │
                            API Gateway (Rate Limiting, TLS)
                                 │
                   ┌─────────────┴─────────────┐
                   │                           │
             Platform Core               AI Orchestrator
           (Auth, RBAC, Audit)          (Claude + MLflow)
                   │                           │
                   └─────────────┬─────────────┘
                                 │
                          Python / FastAPI
                                 │
            ┌────────────────────┼────────────────────┐
            │                    │                    │
       PostgreSQL              Redis                Kafka
     (State & Audit)       (Session Cache)      (Event Stream)
                                                      │
                                  ┌───────────────────┼───────────────────┐
                                  │                   │                   │
                            AS400 / Legacy      Databricks        Ext. Modules
                         (Plant Operations)   (Optimization)   (UK Carbon Intensity API)
```

### Phase 1 scope (what is built here)

```text
   ┌──────────────────────────────────────────────────────────────────┐
   │  api.carbonintensity.org.uk                                      │
   │    GET /intensity      → forecast, actual, index                 │
   │    GET /generation     → generationmix[] per fuel                │
   └───────────────────────────────┬──────────────────────────────────┘
                                   │  poll every 300s
                                   ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  ingest_grid.py  (async worker)                                  │
   │    1. fetch both endpoints concurrently (httpx, HTTP/2)          │
   │    2. validate → Pydantic envelopes                              │
   │    3. merge on (from, to) window → GridTelemetry                 │
   │    4. publish → Kafka, keyed by window start                     │
   │    ↳ on failure: retry w/ backoff → JSONL spool → replay         │
   └───────────────────────────────┬──────────────────────────────────┘
                                   │
                    topic: ecogrid.telemetry.carbon
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
   PostgreSQL 15              Redis 7                 Apache Kafka (KRaft)
   state & audit              session cache            event spine :9092
```

---

## 2. Quickstart

```bash
# 1. Configure
cp .env.example .env

# 2. Bring up the event backbone (PostgreSQL + Redis + Kafka + topic bootstrap)
docker compose up -d
docker compose logs -f kafka-init        # wait for "provisioning topics... done."

# 3a. Run the worker on the host
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python ingest_grid.py

# 3b. …or run it as a container alongside the stack
docker compose --profile worker up -d --build
docker compose logs -f ingestor
```

Verify events are landing:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic ecogrid.telemetry.carbon \
  --from-beginning --max-messages 3 --property print.key=true
```

Smoke-test without Kafka:

```bash
python ingest_grid.py --once --dry-run
```

Run the offline contract tests (no network, no broker):

```bash
pytest -q
```

---

## 3. Services

| Service | Image | Host port | Purpose | Volume |
|---|---|---|---|---|
| `postgres` | `postgres:15-alpine` | `127.0.0.1:${POSTGRES_HOST_PORT:-5432}` | System state, RBAC, audit ledger | `ecogrid-postgres-data` |
| `redis` | `redis:7-alpine` | `127.0.0.1:${REDIS_HOST_PORT:-6379}` | Session cache, rate-limit counters | `ecogrid-redis-data` |
| `kafka` | `confluentinc/cp-kafka:7.6.1` | `127.0.0.1:${KAFKA_HOST_PORT:-9092}` | Event stream spine (KRaft, no ZooKeeper) | `ecogrid-kafka-data` |
| `kafka-init` | same as `kafka` | — | One-shot idempotent topic provisioner | — |
| `ingestor` | local build | — | Grid telemetry worker (`--profile worker`) | `./data` |

**Host port collisions.** Dev machines frequently already run native
PostgreSQL on 5432 and Redis on 6379, which makes `docker compose up` fail with
`address already in use`. Override the host binding in `.env` without touching
the in-container port:

```bash
POSTGRES_HOST_PORT=15432
REDIS_HOST_PORT=16379
KAFKA_HOST_PORT=9092     # must match ECOGRID_KAFKA_BOOTSTRAP_SERVERS
```

`KAFKA_HOST_PORT` must stay in step with `KAFKA_ADVERTISED_LISTENERS` — if the
published port and the advertised port disagree, clients connect and then
receive unreachable broker metadata on the first metadata fetch.

**Kafka listeners.** KRaft mode runs a single combined broker+controller node
(`KAFKA_PROCESS_ROLES=broker,controller`). Two listeners are advertised:

| Client location | Bootstrap server |
|---|---|
| Another container on `ecogrid-net` | `kafka:29092` |
| Host machine (worker running via venv) | `localhost:9092` |

`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` — topics are provisioned explicitly by
`kafka-init`, so a typo'd topic name fails loudly instead of silently creating
an unconfigured one.

**Healthcheck target.** The Kafka healthcheck probes `kafka:29092`, *not*
`localhost:29092`. The `PLAINTEXT` listener is bound to the `kafka` hostname, so
inside the container `localhost:29092` refuses connections while `kafka:29092`
answers. Probing the wrong one leaves the broker permanently `unhealthy` even
though it is running fine, which blocks `kafka-init` and the worker from
starting.

---

## 4. Event contract

**Topic:** `ecogrid.telemetry.carbon`
**Key:** window start, ISO-8601 UTC (`2026-09-14T17:00:00Z`)
**Value:** UTF-8 JSON, one record per half-hourly settlement window

```json
{
  "schema_version": "1.0.0",
  "source": "api.carbonintensity.org.uk",
  "window_from": "2026-09-14T17:00:00Z",
  "window_to": "2026-09-14T17:30:00Z",
  "forecast_intensity": 266,
  "actual_intensity": 263,
  "carbon_index": "moderate",
  "generation_mix": { "biomass": 6.1, "gas": 42.1, "nuclear": 14.4, "solar": 8.0, "wind": 35.5 },
  "renewable_percentage": 43.5,
  "low_carbon_percentage": 57.9,
  "fossil_percentage": 42.1,
  "is_forecast_only": false,
  "generation_mix_missing": false,
  "ingested_at": "2026-09-14T17:05:12.884Z"
}
```

### Design notes

- **Delivery is at-least-once, not exactly-once.** This was established
  empirically against a live broker, not assumed. A record whose send the worker
  gave up on (and spooled for replay) was still delivered ~9 seconds later by the
  producer's own internal retry during `flush()` at shutdown — and then delivered
  *again* when the spool was replayed. The same window can therefore arrive twice.
  **Consumers must dedupe on the message key** (or `INSERT ... ON CONFLICT
  (window_from) DO UPDATE`). The producer is idempotent (`enable_idempotence`,
  `acks=all`), which eliminates broker-side duplicates within a session, but it
  cannot make the spool-and-replay path exactly-once.
- **Keyed by window start.** Consumers can dedupe or log-compact on the key.
  The topic is `cleanup.policy=compact,delete` with a 7-day retention. Because
  the key is stable, every revision of a window lands on the same partition and
  therefore stays ordered.
- **`actual_intensity` may be `null`.** The grid operator publishes intensity
  forecasts before settlement. `is_forecast_only` flags those windows and
  `effective_intensity` (Python property) resolves to the forecast.
- **Merge is keyed on `(from, to)`, never array position.** `/intensity` and
  `/generation` are separate HTTP calls and may return different-length arrays
  or arrive out of order. Intensity windows with no matching generation window
  are still emitted with `generation_mix_missing=true` — the carbon signal
  alone is actionable.
- **Unchanged windows are suppressed in-process.** The API returns the forward
  24h of half-hourly data, so a naive worker republishes ~48 identical records
  every cycle. The worker hashes each payload and skips no-op republishes;
  a revised forecast or a settled `actual` produces a new hash and is published.
  Set `ECOGRID_PUBLISH_UNCHANGED_WINDOWS=true` to disable suppression. Note this
  cache is per-process: a restart republishes the current window (harmless, and
  deduped by the key).
- **Reject, never forward.** Any payload failing Pydantic validation is logged
  and dropped. Nothing unvalidated reaches Kafka.

---

## 5. Resilience model

| Failure | Handling |
|---|---|
| DNS/connection reset, timeout | Exponential backoff with jitter, `ECOGRID_HTTP_MAX_ATTEMPTS` (default 5), base 1s → cap 60s |
| HTTP 429 / 5xx | Retried; `Retry-After` honoured and clamped to 300s |
| HTTP 4xx (non-retryable) | Fail fast for that cycle — retrying a malformed request is noise |
| Non-JSON / non-object body | Treated as transient, retried, then aborts the cycle |
| One endpoint down | Both are fetched with `return_exceptions=True`; the cycle fails cleanly and the loop survives |
| Kafka broker unavailable at boot | Admin API polled until `ECOGRID_KAFKA_BROKER_WAIT_SECONDS` (default 90s) elapses, then exit code 2 |
| Kafka send failure | Bounded retry per record; on exhaustion the record is appended to a JSONL spool. Delivery is at-least-once, so the same window may arrive twice — consumers dedupe on the key |
| Send that never resolves (unknown topic) | Bounded per attempt by `ECOGRID_KAFKA_SEND_TIMEOUT_SECONDS` (default 15s) via `asyncio.wait_for`. Without this, a typo'd topic name stalls ~30s per attempt — 150s across 5 attempts, inside a 300s cycle |
| Spool overflow | Oldest lines dropped past `ECOGRID_SPOOL_MAX_RECORDS` (default 10 000) — a dead broker cannot fill the disk |
| Corrupt spool line | Discarded with a warning; valid lines still replay |
| Spool drained | **Truncated to zero bytes, never unlinked.** Deleting needs a delete syscall, which some environments guard or forbid — in testing that terminated the worker mid-replay and left a stale spool behind |
| Unexpected error mid-cycle | Caught by an outer safety net, logged with a traceback, loop continues to the next cadence. A single bad cycle must never kill a 24/7 worker |
| SIGINT / SIGTERM | Loop stops after the in-flight cycle, producer flushed, exit 0 |

The spool is replayed at the start of every cycle and again before each
publish, so an outage is self-healing without operator intervention.

### Verified against a live broker

These behaviours were exercised end-to-end, not inferred from the code:

| Check | Result |
|---|---|
| Full stack boots, all healthchecks green | ✅ PostgreSQL / Redis / Kafka all `healthy` |
| `kafka-init` provisions the topic | ✅ 3 partitions, `cleanup.policy=compact,delete`, 7-day retention |
| Live API → Kafka publish | ✅ real window ingested and readable back off the topic |
| Partition keying | ✅ 9 messages, all on partition 0 (same window key) |
| In-process dedupe over 3 cycles at 30s cadence | ✅ cycle 1 `published=1`, cycles 2–3 `published=0`; broker offsets confirm zero duplicates |
| Send failure → spool → replay | ✅ record spooled on failure, drained on recovery, spool truncated to 0 bytes |
| **Kafka killed mid-run, then restored** | ✅ send timed out → retried → spooled → replayed on recovery; worker survived all 5 cycles; no data lost |
| Graceful SIGTERM mid-run | ✅ finished the in-flight cycle, flushed the producer, exit 0 |
| Send bound | ✅ unresolvable topic fails in 12s, not 150s |
| Broker unreachable at boot | ✅ retries with backoff, then exit code 2 |
| Worker runs as a container | ✅ image builds; connects via `kafka:29092`; publishes; container health reaches `healthy` |
| Container heartbeat healthcheck | ✅ heartbeat file written through the `./data` bind mount, probe exits 0 |

> Note on test design: the first mid-run outage test **passed vacuously**. With
> dedupe enabled the window was unchanged, so cycles 2–4 skipped the send
> entirely (`published=0 failed=0`) and the outage was never exercised. The test
> only became meaningful with `ECOGRID_PUBLISH_UNCHANGED_WINDOWS=true`, forcing
> a send every cycle.

### Known gaps

- **No long soak test.** The longest continuous run was 5 cycles at a 30s
  cadence. The 300s production cadence has not been left running for hours, so
  slow leaks (dedupe-cache growth, connection-pool exhaustion, file-descriptor
  drift) remain untested.
- **Single broker, no cluster-level fault injection.** Broker *availability*
  was tested (stop/start mid-run). Broker *degradation* — leader elections,
  ISR shrinkage, disk-full — was not, and would need a multi-node cluster.
- **No consumer yet.** Phase 2 will read this topic; the contract is documented
  but nothing validates it in practice.

**Liveness.** The worker touches `ECOGRID_HEARTBEAT_PATH` after every
successful cycle. The container healthcheck marks it unhealthy after 3 missed
cycles (900s), which is the real signal that ingestion has stalled.

---

## 6. Configuration reference

All worker settings are read from `ECOGRID_*` environment variables (or a
`.env` file), validated by `pydantic-settings`.

| Variable | Default | Notes |
|---|---|---|
| `ECOGRID_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka:29092` inside Compose |
| `ECOGRID_KAFKA_TOPIC` | `ecogrid.telemetry.carbon` | |
| `ECOGRID_KAFKA_TOPIC_PARTITIONS` | `3` | Applied at topic creation only |
| `ECOGRID_KAFKA_MAX_ATTEMPTS` | `5` | Per-record send retries |
| `ECOGRID_KAFKA_BACKOFF_BASE_SECONDS` / `_MAX_SECONDS` | `1.0` / `30.0` | Kafka retry backoff |
| `ECOGRID_KAFKA_BROKER_WAIT_SECONDS` | `90.0` | Boot-time broker wait |
| `ECOGRID_KAFKA_SEND_TIMEOUT_SECONDS` | `15.0` | Hard ceiling per send attempt (enforced with `asyncio.wait_for`) |
| `ECOGRID_KAFKA_REQUEST_TIMEOUT_MS` | `15000` | Underlying produce request timeout |
| `ECOGRID_INTENSITY_URL` | `https://api.carbonintensity.org.uk/intensity` | |
| `ECOGRID_GENERATION_URL` | `https://api.carbonintensity.org.uk/generation` | |
| `ECOGRID_HTTP_TIMEOUT_SECONDS` | `15.0` | Connect timeout capped at 5s |
| `ECOGRID_HTTP_MAX_ATTEMPTS` | `5` | Per-request retries |
| `ECOGRID_HTTP_BACKOFF_BASE_SECONDS` / `_MAX_SECONDS` | `1.0` / `60.0` | HTTP retry backoff |
| `ECOGRID_POLL_INTERVAL_SECONDS` | `300` | Minimum 30; cadence is drift-corrected |
| `ECOGRID_SPOOL_PATH` | `./data/spool/telemetry-spool.jsonl` | Durability buffer |
| `ECOGRID_SPOOL_MAX_RECORDS` | `10000` | Bounded on-disk backlog |
| `ECOGRID_HEARTBEAT_PATH` | `./data/heartbeat` | Liveness marker |
| `ECOGRID_PUBLISH_UNCHANGED_WINDOWS` | `false` | Disable dedupe to republish everything |
| `ECOGRID_LOG_LEVEL` | `INFO` | |

**Why 300 seconds?** The upstream API publishes at half-hourly granularity, so
a 5-minute cadence is ~6× oversampled — fast enough to catch revised forecasts
and settle windows promptly, while staying comfortably inside the public rate
limit.

---

## 7. Operations

```bash
# Tail worker logs (UTC timestamps, index + intensity per cycle)
docker compose logs -f ingestor

# Inspect consumer-group lag (once Phase 2 consumers exist)
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --all-groups --describe

# Describe the telemetry topic
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic ecogrid.telemetry.carbon

# Tear down, keeping data volumes
docker compose down

# Tear down and destroy all state
docker compose down -v
```

---

## 8. Repository layout

```text
ecogrid-os/
├── docker-compose.yml        # PostgreSQL 15 · Redis 7 · Kafka (KRaft) · topic init · worker
├── Dockerfile                # Slim non-root image for the ingestion worker
├── ingest_grid.py            # Phase 1 async ingestion worker (the deliverable)
├── test_ingest_grid.py       # Offline contract + resilience tests
├── requirements.txt          # Pinned dependencies (Python 3.11+)
├── .env.example              # Configuration template
├── data/
│   └── spool/                # Durability buffer (JSONL) + heartbeat marker
└── docs/                     # Extended architecture notes
```

---

## 9. Roadmap

- **Phase 1 (this milestone)** — grid ingestion + event backbone.
- **Phase 2** — Platform Core: FastAPI service, auth/RBAC, audit ledger in
  PostgreSQL, Redis session cache, AI Orchestrator.
- **Phase 3** — AS400 / legacy plant-operations bridge and Databricks
  optimization loop.
