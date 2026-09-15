# EcoGrid OS — Product Requirements Document

| | |
|---|---|
| **Status** | Living document — reflects what is built, not an aspiration |
| **Last updated** | 2026-09-15 |
| **Owners** | Platform engineering |
| **Related** | [`REQUIREMENTS.md`](REQUIREMENTS.md) · [`ARCHITECTURE.md`](ARCHITECTURE.md) · [`VERIFICATION.md`](VERIFICATION.md) |

---

## 1. Problem

Industrial electricity has two costs that move independently, and most plants
only manage one of them.

**The price bill** is managed. Procurement hedges it, finance reports it, and it
appears on a monthly statement.

**The carbon bill** is largely unmanaged, because it is invisible at the point of
consumption. A plant sees one number on its meter. It does not see that the same
kilowatt-hour carries roughly **five times** the carbon at 18:00 as it does at
04:00 — and that a meaningful share of its load does not care *when* it runs.

The result is systematic, uncompensated waste. A batch mill, an electrolyser or a
thermal store consumes the same energy whenever it runs, but the emissions
attributable to that energy swing with the grid. In this repository's own
measured runs, moving flexible load into the cleanest overnight windows avoided
**4,543 kg CO₂e — a 79.5% reduction** — with no change in output.

Three things make this newly tractable:

1. **Grid carbon intensity is now published as a live, machine-readable signal**
   (the UK National Grid Carbon Intensity API), at half-hourly settlement
   granularity.
2. **Industrial load is more flexible than its operators assume.** Load is rarely
   uniform: a substantial fraction is *batch* or *thermal* and can be moved within
   a window without touching the product.
3. **The optimization is arithmetic, not research.** Choosing which half-hour to
   run a batch in is a constrained scheduling problem that a modest solver
   handles well.

## 2. Who this is for

| Persona | What they need | How EcoGrid OS serves them |
|---|---|---|
| **Energy / sustainability manager** | Evidence of decarbonization progress that is auditable, not estimated | Immutable per-window ledger, append-only audit log, quantified kg CO₂e avoided per decision |
| **Plant operations manager** | A dispatch plan that respects physical constraints and does not risk the product | Schedules only *flexible* load, honours contiguous batch duration and capacity ceilings, and refuses to place a batch it cannot fit rather than silently dropping it |
| **Control-room operator** | Something actionable, with the reasoning visible | Dashboard showing the schedule, the intensity it was chosen against, and a plain-language recommendation with its confidence and caveats |
| **Platform / data engineer** | A system that is inspectable, testable and honest about its state | Typed event contract, idempotent consumer, documented seams where integrations are stubbed, verification procedures for every capability |
| **Compliance / audit** | Proof of who did what, when | Append-only audit log with actor, role, action, path and client IP; no update or delete path exists in the codebase |

## 3. Product definition

**EcoGrid OS is a carbon-arbitrage decision system for industrial sites.**

It ingests grid carbon intensity and plant load, computes a dispatch schedule
that places flexible load into the cleanest feasible windows, and publishes that
schedule for plant control systems to act on. It also explains its reasoning, and
degrades to deterministic advice when its language model is unavailable.

### The core loop

```
   grid intensity ──┐
                    ├──▶ optimizer ──▶ schedule ──▶ plant control
   plant load ──────┘         │                        │
                              ▼                        ▼
                        audit ledger            operator dashboard
```

### What makes it different from a generic scheduler

- **It is honest about its inputs.** If the retained forecast is mostly
  unsettled, the advisor says so and lowers its confidence. If a process cannot
  be placed, the run reports it as `unscheduled` with a reason rather than
  quietly omitting it.
- **It never goes silent.** No Anthropic key, a dead Databricks cluster, an
  unreachable broker — in every case the system produces its best available
  answer and records which fallback was used.
- **It is verifiable.** Every claim in the README maps to a numbered use case with
  a pass/fail criterion in [`VERIFICATION.md`](VERIFICATION.md).

## 4. Non-goals

Explicitly **not** in scope. These are the boundary, not a backlog.

| Not doing | Why |
|---|---|
| **Actuating plant equipment** | EcoGrid OS *advises*; it does not control. Writing to a PLC or SCADA setpoint requires safety certification, interlocks and liability we are not taking on. The schedule is published; a human or an existing control system decides. |
| **Energy trading / market bidding** | We optimize carbon, not price. Price curves are a natural extension but a different product with different regulatory exposure. |
| **A full EMS / meter data management system** | We consume load telemetry; we do not replace the historian, the SCADA layer or the billing system. |
| **Sub-second / real-time control** | The domain granularity is the half-hourly settlement window. Latency requirements follow from that. |
| **Multi-tenant SaaS** | This is a single-site deployment with one audit boundary. Tenancy isolation is a different architecture. |
| **Carbon accounting / reporting** | We produce the operational evidence (kg CO₂e avoided per decision). Formal disclosure frameworks (SECR, CSRD) are a downstream consumer, not our output. |

## 5. Success metrics

| Metric | Definition | Target | Current |
|---|---|---|---|
| **Carbon avoided per horizon** | kg CO₂e saved vs a run-as-early-as-possible baseline, per optimization run | > 500 kg on a 24-window horizon | **4,543 kg** (seeded 24-window run) |
| **Reduction ratio** | avoided ÷ baseline | > 20% | **79.5%** |
| **Schedule completeness** | share of the flexible portfolio placed | 100%, or an explicit reason per exclusion | 3/3 placed, 0 unscheduled |
| **Ingestion freshness** | age of the newest retained window | < 2× the poll cadence | ~0 min (current window) |
| **Delivery correctness** | duplicate deliveries producing duplicate ledger rows | 0 | **0** (duplicates suppressed, `revision_count` unmoved) |
| **Advice availability** | orchestrator passes producing advice | 100% | 100% (heuristic fallback) |

> **A note on the carbon metric.** These numbers are only meaningful relative to
> the horizon supplied. On a 4-window horizon the same system reports 47%; on 24
> windows it reports 79.5%. Quoting a saving without its horizon is meaningless,
> and this document will not do it.

## 6. Scope, by phase

Each phase is independently verifiable. Phases 1–3 are built.

| Phase | Delivers | Status |
|---|---|---|
| **1 — Grid ingestion & event backbone** | Async worker polling the Carbon Intensity API; validated contract; Kafka publish with dedupe, spool and replay | ✅ Built, verified live |
| **2 — Platform Core** | Idempotent Kafka→PostgreSQL consumer with audit ledger and DLQ; FastAPI read API with API-key auth, ranked RBAC, rate limiting and append-only audit; Redis hot cache | ✅ Built, verified live |
| **3 — Plant operations & optimization** | AS400/legacy plant bridge (simulated · file · ODBC seam); plant telemetry consumer; carbon-arbitrage optimizer with a Databricks seam and local fallback | ✅ Built, verified live |
| **4 — AI Orchestrator** | Claude-backed judgement over the optimizer's output, with a deterministic heuristic fallback and MLflow/JSONL run tracking | ✅ Built, verified live |
| **5 — Operator surface** | React + TypeScript dashboard; nginx edge gateway with TLS, static hosting and per-IP rate limiting | ✅ Built, verified live |
| **6 — Legacy & cluster integrations** | Real AS400 ODBC driver; real Databricks job; SASL broker auth; multi-broker HA topology | ⚠️ Seams and configuration provided; not exercised against real systems |

## 7. Constraints and assumptions

**Hard constraints**

- The upstream Carbon Intensity API serves **only the current settlement window**
  and offers no backfill. History therefore accrues at one window per 30 minutes
  per running ingestor. `scripts/seed_demo_data.py` exists because this is
  genuinely limiting for demos and verification.
- Kafka delivery is **at-least-once**. Exactly-once is not achievable across the
  spool-and-replay path, so every consumer must be idempotent. This is a design
  invariant, not a defect.
- The AS400 and Databricks integrations have **no reachable counterpart** in this
  repository. They are implemented as seams that fail loudly rather than faking
  data.

**Assumptions**

- A meaningful fraction of plant load is genuinely movable. The optimizer
  consumes this as an *input* (the flexible/inflexible split from plant
  telemetry) rather than assuming it.
- Half-hourly resolution is sufficient for the decisions being made.
- The operator can act on a schedule within the window it applies to.

## 8. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| **Schedules are acted on without review** | A batch moved into an unsuitable window could affect product quality | Advice carries explicit `risk_flags`; the system states when a decision rests on forecast rather than settled data; actuation is out of scope by design |
| **Optimizer output mistaken for a guarantee** | Over-claimed savings | Every run reports baseline, optimized and saved figures, so the arithmetic is checkable; savings are always reported with their horizon |
| **Synthetic demo data mistaken for collected telemetry** | False evidence | Seeded records carry `source: demo-seed`; they can be purged with a single predicate |
| **Upstream API change breaks ingestion** | Data stops flowing | Strict Pydantic validation rejects rather than forwards; a shape regression is already covered by tests (the API returns `data` as an object, not an array) |
| **Model advice drifts or degrades silently** | Bad guidance | Advice is schema-validated before storage; unvalidated output is discarded and the deterministic advisor answers instead; `source` is recorded on every row |

## 9. What "done" means for this project

A phase is done when three things hold:

1. The code is written and the automated suite passes.
2. The behaviour has been **executed against the real system**, not inferred.
3. A numbered use case in [`VERIFICATION.md`](VERIFICATION.md) records what was
   observed, including anything that was *not* verified.

This bar exists because of measured experience on this project: **twelve defects
were found during development and the automated suite caught none of them.**
Every one surfaced by running the system — and two only by looking at a
screenshot of the running UI. A green test suite is a necessary condition, never
a sufficient one.
