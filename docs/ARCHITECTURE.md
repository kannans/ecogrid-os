# EcoGrid OS — Architecture Notes

Companion to the top-level `README.md`. This document records the *why* behind
Phase 1's boundaries and the interfaces later phases depend on.

---

## 1. Layering

```text
Presentation      React / TypeScript dashboard
                        │
Edge              API Gateway — rate limiting, TLS termination
                        │
Platform          Platform Core            AI Orchestrator
                  auth · RBAC · audit      Claude + MLflow
                        │
Service           Python / FastAPI
                        │
State & Streams   PostgreSQL · Redis · Kafka
                        │
Integration       AS400/legacy · Databricks · Ext. modules (UK Carbon Intensity)
```

Each layer may only call downward. The ingestion worker is the sole writer to
`ecogrid.telemetry.carbon`; nothing else may publish grid telemetry. That
single-writer rule is what makes the audit trail in PostgreSQL reconstructable.

---

## 2. Why Kafka is the spine, not a table

Grid telemetry is an *append-only observation stream*, not mutable state.
Modelling it as rows in PostgreSQL would mean upserting every 5 minutes and
losing the forecast-revision history — which is precisely the signal Phase 3's
optimizer needs (how did the forecast for window *W* evolve as *W* approached?).

Kafka gives:

- **Replay.** A new consumer (Databricks optimization job) can reprocess from
  offset zero without touching the producer.
- **Fan-out.** Plant operations, optimization, and the dashboard each consume
  independently at their own pace.
- **Backpressure absorption.** A slow consumer lags; it does not stall ingestion.
- **Ordering per key.** Records are keyed by window start, so all revisions of
  a given settlement window land on one partition and stay ordered.

PostgreSQL then stores *derived, queryable state* (audit ledger, arbitrage
decisions, RBAC), not the raw stream.

---

## 3. KRaft, not ZooKeeper

Kafka 3.x runs metadata through its own Raft quorum. A single combined
`broker,controller` node is correct for local development and removes a whole
moving part (plus ~1GB of container memory). The compose file declares the
quorum explicitly (`KAFKA_CONTROLLER_QUORUM_VOTERS`) so promoting this to a
3-node production cluster is a matter of adding node IDs, not re-architecting.

Two advertised listeners are deliberate:

| Listener | Advertised as | Consumer |
|---|---|---|
| `PLAINTEXT` | `kafka:29092` | other containers on `ecogrid-net` |
| `PLAINTEXT_HOST` | `localhost:9092` | the worker run from a host venv |

A single listener cannot serve both — a container cannot resolve `localhost`
to the broker, and a host process cannot resolve the service name `kafka`.

---

## 4. The ingestion contract

### Correlation, not positional merge

`/intensity` and `/generation` are two independent HTTP calls. Nothing
guarantees they return the same number of windows, in the same order, or even
the same *shape* (verified: `/intensity` returns `data` as an array, while
`/generation` returns `data` as a single object). Records are therefore
correlated on their `(from, to)` interval, and the envelope layer normalises
both shapes before the merge ever runs.

### Forecast vs. actual

The grid operator publishes a forecast, then settles an actual value once the
window closes. Between those moments `actual` is absent. This is normal, not an
error:

- `actual_intensity: null` + `is_forecast_only: true` → window still open.
- Both populated → window settled; downstream may trust the actual.

Consumers must branch on `is_forecast_only` rather than assuming `actual`
exists. Treating a missing `actual` as a failure would drop roughly half of all
real telemetry.

### Idempotency

Each record carries a stable key (window start) and downstream can dedupe on
it. The producer additionally suppresses no-op republishes in-process: the API
returns the same windows on every poll, so a naive worker would emit ~48
identical records every 5 minutes. Payload hashing means a *revised* forecast or
a newly settled actual still publishes — only byte-identical repeats are
skipped.

The topic uses `cleanup.policy=compact,delete`: compaction retains the latest
record per key for state recovery, while the 7-day retention bound ages out
cold history.

---

## 5. Failure domains

| Domain | Blast radius | Mitigation |
|---|---|---|
| Upstream API slow/down | One cycle skipped | Bounded retries; loop survives; next cycle is 5 min away |
| Upstream returns garbage | One cycle skipped | Pydantic rejects at the boundary; nothing invalid is published |
| Broker down at boot | Worker exits 2 | Compose `depends_on: service_healthy` normally prevents this |
| Broker down mid-run | Records buffered | Per-record retry → JSONL spool → replay on next cycle |
| Disk full | Spool capped | `ECOGRID_SPOOL_MAX_RECORDS` bounds the backlog; oldest dropped |
| Clock skew | Window keys wrong | All timestamps are UTC and taken from the upstream payload, not local time |

Deliberate omission: **no local write-ahead log before Kafka.** The upstream API
is itself the durable source — a missed cycle is recoverable by re-polling,
because the API serves the current window on demand. A WAL would add
complexity for no correctness gain. If a future phase ingests data with no
upstream replay window (e.g. AS400 push events), that calculus inverts and a
WAL becomes mandatory.

---

## 5a. Delivery semantics: at-least-once, and why that is the right choice

Measured, not assumed. During a live run, a record whose send the worker had
already reported as failed — and written to the durability spool — was still
delivered to the topic ~9 seconds later by the producer's own internal retry
during `flush()` at shutdown. Replaying the spool then delivered it a second
time.

This is not a defect to be engineered away; it is the correct trade-off:

| Option | Cost |
|---|---|
| **At-most-once** (drop on failure) | Silent data loss. Unacceptable — a missing settlement window corrupts every downstream arbitrage calculation, and the gap is invisible. |
| **Exactly-once** (transactions) | Requires transactional producers, `read_committed` consumers, and a coordinator round-trip per batch. Correctness burden shifts to every consumer, and throughput drops. |
| **At-least-once** (chosen) | Duplicates are possible. They are trivially handled because every record carries a stable window key. |

The duplicate is *safe* precisely because the record is keyed by window start:
`INSERT ... ON CONFLICT (window_from) DO UPDATE` is idempotent, and the payload
is a pure function of the window's data. So the cost of a duplicate is one
redundant write, while the cost of a drop is a hole in the audit trail.

`enable_idempotence=True` plus `acks=all` eliminates broker-side duplication
within a producer session (retries of the same batch are deduplicated by
sequence number). It does **not** cover the spool-and-replay path, where the
record is re-sent as a genuinely new batch in a later session. Only the consumer
can close that gap, which is why the dedupe obligation is documented as part of
the event contract rather than left implicit.

The same reasoning applies to the in-process dedupe cache: it is an
*optimisation* to avoid flooding the topic with byte-identical republishes, not
a correctness mechanism. Losing it on restart costs a duplicate, never a gap.

---

## 6. Interface for Phase 2

The FastAPI service consumes the stream and must uphold:

1. **Read from Kafka, never from the API directly.** All telemetry enters via
   the worker; a second ingestion path would desynchronise the audit ledger.
2. **Use `window_from` as the idempotency key** when persisting to PostgreSQL —
   `INSERT ... ON CONFLICT (window_from) DO UPDATE` handles replays cleanly.
   This is mandatory, not defensive: delivery is at-least-once (§5a).
3. **Never assume `actual_intensity` is present.** Branch on `is_forecast_only`.
4. **Record the consumer offset** alongside each persisted row so the audit
   ledger can be reconciled against the topic.
5. **Treat `carbon_index` as an enum**, not free text — `unknown` is a valid
   value and must not crash a dashboard.
6. **Be prepared for duplicate keys within a single poll.** A spool replay can
   re-emit a window that was already consumed; upsert semantics make this a
   no-op rather than a constraint violation.

---

## 7. Deferred decisions

| Decision | Status | Why |
|---|---|---|
| Schema registry (Avro/Protobuf) | ⏳ Deferred | JSON + `schema_version` is sufficient for one producer and one consumer. Revisit when a third producer appears. |
| TLS — client → platform | ✅ **Done** | Terminated at the nginx edge gateway (`--profile gateway`), which also serves the dashboard and applies per-IP rate limiting. |
| SASL — client → **broker** | ⏳ Open | The broker is only reachable on the Compose bridge. TLS now covers client→gateway, but **broker-side SASL is still not enabled** and is required before any non-local deployment. Do not treat the gateway's TLS as covering this hop. |
| Multi-broker replication | ⏳ Open | Replication factors are pinned to 1. Raising them requires ≥3 brokers and an HA topology override; planned, not built. |
| Dead-letter topic consumption | ⏳ Open | `ecogrid.telemetry.carbon.dlq` is provisioned but unconsumed. The worker's spool handles producer-side redelivery; a DLQ consumer matters once a *consumer* starts failing. |
