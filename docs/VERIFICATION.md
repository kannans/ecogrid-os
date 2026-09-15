# EcoGrid OS — Use-Case Verification

Step-by-step verification procedures, kept **separate from the README** because they
answer a different question. The README explains *what this is and how to run it*;
this document answers *how do I prove it actually works*.

Every use case below gives a goal, the exact commands, and an explicit
**pass criteria** line. Work through them top to bottom — later cases depend on
earlier ones having produced data.

> **Environment note.** The procedures assume a host with Docker. They were written
> against the code in this repository; the automated suite (`pytest -q`, 75 tests)
> was run in an environment without a Docker daemon, so the container-level cases
> below are the ones a maintainer executes on their own machine. Each case states
> whether it is also covered automatically.

---

## 0. Setup (do this once)

```bash
cd ecogrid-os
cp .env.example .env          # then set POSTGRES_HOST_PORT/REDIS_HOST_PORT if 5432/6379 are taken

# Backbone: PostgreSQL + Redis + Kafka + topic bootstrap
docker compose up -d
docker compose logs -f kafka-init     # wait for "[kafka-init] done."

# Platform Core (Phase 2) — migrate → consumer → api
docker compose --profile platform up -d --build
docker compose logs -f migrate        # capture the bootstrap admin key printed ONCE

# Phase 3 — plant bridge, plant consumer, optimizer
docker compose --profile phase3 up -d --build
```

Store the credentials and base URL in your shell:

```bash
export API=http://localhost:8000                 # or 127.0.0.1:${API_HOST_PORT:-8000}
export ADMIN_KEY=<the key printed by migrate>    # X-API-Key value
```

Confirm the containers are up:

```bash
docker compose ps
```

**Pass criteria:** `postgres`, `redis`, `kafka` are `healthy`; `migrate` has
exited `0`; `consumer`, `api`, `plant-bridge`, `plant-consumer`, `optimizer` are
`running`.

> ### ✅ CONFIRMED — Phase 2 (2026-09-15)
>
> Observed on a real Docker host:
>
> ```
> ecogrid-api        … Up (health: starting)   127.0.0.1:8000->8000/tcp
> ecogrid-consumer   … Up (health: starting)   8000/tcp
> ecogrid-kafka      … Up 2 hours (healthy)    127.0.0.1:9092->9092/tcp
> ecogrid-postgres   … Up 2 hours (healthy)    127.0.0.1:15432->5432/tcp
> ecogrid-redis      … Up 2 hours (healthy)    127.0.0.1:16379->6379/tcp
> ```
>
> Notes on reading that output:
> * **`migrate` is absent because it exited 0** — it is a one-shot, not a service.
>   The real proof is that `consumer` and `api` started at all: both declare
>   `depends_on: migrate: condition: service_completed_successfully`, so they could
>   only come up if `migrate` completed. This is what the compose `command` fix
>   bought us.
> * `health: starting` is expected — the api has a 20s `start_period` and the
>   consumer 30s. Re-run `docker compose ps` after ~30s to see them `healthy`.
> * That first run did not include the `phase3` / `ai` / `gateway` services.
>   **A later run brought every profile up together and all ten services reached
>   `healthy`**, so the bring-up half of UC-7 → UC-17 is now satisfied.
>
> **Evidenced by the running stack + dashboard render:** UC-1/UC-2 (grid
> ingestion → consumer → API: 4 windows, avg actual 102 g/kWh), UC-7/UC-8 (plant
> bridge → consumer → API: 1 plant, 38.5 MW total / 13.5 MW flexible), UC-13
> (orchestrator advice rendered, `source: heuristic`) and UC-15 (dashboard).
> **UC-9/UC-10 still want a deliberate re-run** now that more grid windows exist —
> the schedule currently on screen is the startup run, taken when only two
> identical-intensity windows had been retained, which is why it reports 0 kg.

---

## UC-1 — Grid telemetry is ingested and deduplicated (Phase 1)

**Goal:** the worker reads the live UK Carbon Intensity API and publishes one
record per settlement window, without republishing unchanged windows.

```bash
docker compose --profile worker up -d --build
docker compose logs -f ingestor
```

Expect a first cycle ending in `published=1` and subsequent cycles reporting
`published=0` for the same window. Then read the topic back:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic ecogrid.telemetry.carbon \
  --from-beginning --max-messages 3 --property print.key=true
```

**Pass criteria:** messages appear with the window-start key; repeated cycles do
**not** add duplicate records for an unchanged window.
*Automated: yes — `test_ingest_grid.py` (dedupe, spool, replay).*

---

## UC-2 — The consumer's write is idempotent under at-least-once delivery

**Goal:** prove that replaying the same window does not create duplicate ledger
rows or inflate `revision_count`.

```bash
docker compose logs -f consumer     # expect: inserted / duplicates=… / revised=…
curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/telemetry?limit=5" | head -c 600
curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/ingest-status"
```

**Pass criteria:** `ingest-status` shows `messages_consumed` equal to the number
of messages on the topic, with `duplicates_suppressed > 0` after a replay; the
row count in `grid_telemetry` equals the number of *distinct* windows, not the
number of deliveries. `revision_count` increments only when the payload actually
changed.
*Automated: yes — `test_platform.py` (upsert insert → suppress → revise).*

---

## UC-3 — Unauthenticated calls are rejected

```bash
curl -i "$API/api/v1/telemetry/latest"                          # no key
curl -i -H "X-API-Key: total-nonsense" "$API/api/v1/telemetry/latest"
```

**Pass criteria:** both return **401** with `WWW-Authenticate: ApiKey`. `/healthz`
must still return **200** without a key (orchestrators cannot present credentials).

---

## UC-4 — RBAC is enforced (viewer cannot read the audit log)

Create a scoped key, then try an admin-only endpoint with it.

```bash
docker compose --profile platform run --rm --entrypoint python migrate \
  -m ecogrid.keys create --name "dash-viewer" --role viewer
# copy the printed key
export VIEWER_KEY=<printed key>

curl -i -H "X-API-Key: $VIEWER_KEY" "$API/api/v1/whoami"      # expect 200
curl -i -H "X-API-Key: $VIEWER_KEY" "$API/api/v1/audit"       # expect 403
curl -i -H "X-API-Key: $ADMIN_KEY"  "$API/api/v1/audit"       # expect 200
```

**Pass criteria:** viewer gets **200** on `/whoami`, **403** on `/audit`
(`requires role 'admin' or higher`); admin gets **200**. Roles rank
`viewer < operator < admin`.
*Automated: yes — unit tests cover role ranking and the dependency factory.*

---

## UC-5 — Rate limiting returns 429 with retry headers

```bash
for i in $(seq 1 130); do
  curl -s -o /dev/null -w "%{http_code} " -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/whoami"
done; echo
```

**Pass criteria:** the first 120 requests in a 60s window return **200**; the rest
return **429** with `Retry-After` and `X-RateLimit-Limit: 120` headers. Wait 60s
and the counter resets. Rate limiting runs *before* authentication, so bad keys are
throttled too.
*Automated: yes — `test_platform.py` (allows to limit, then denies; fails open).*

---

## UC-6 — Actions are written to the append-only audit log

```bash
curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/audit?limit=20"
```

**Pass criteria:** entries exist for the reads above and for the **403 denial**
from UC-4 (`action=access.denied`) and the **401** from UC-3. There is no update
or delete path for this table anywhere in the codebase.

> ### ✅ CONFIRMED — UC-3 / UC-4 / UC-5 / UC-6 (2026-09-15, live stack)
>
> Exercised with temporary keys created directly in the database and revoked
> immediately afterwards, so no long-lived credential was involved.
>
> | Check | Expected | Observed |
> |---|---|---|
> | `GET /api/v1/telemetry/latest`, no key | 401 | **401** |
> | same, garbage key | 401 | **401** |
> | `GET /healthz`, no key | 200 | **200** |
> | viewer `GET /whoami` | 200 | **200** |
> | viewer `GET /audit` (admin-only) | 403 | **403** |
> | admin `GET /audit` | 200 | **200** |
> | viewer `POST /optimize/run` (operator+) | 403 | **403** |
> | 130 rapid requests, one key | 120×200, then 429 | **120×200, 10×429** |
> | 429 response headers | `Retry-After`, `X-RateLimit-Limit` | **`Retry-After: 15`, `X-RateLimit-Limit: 120`** |
> | audit records the denials | `action=access.denied` | **present** (also `auth.rejected`) |
>
> Two incidental findings worth keeping:
> * The audit log also contains **`request.error`** rows — the middleware recorded
>   the optimizer's HTTP 500s from the capacity bug. Failures leave a trail even
>   when the client only receives an empty body.
> * `POST /optimize/run` returned **429** straight after the 130-request burst.
>   That is the limiter working correctly, not a defect: the same key had just
>   exhausted its window. A fresh key returned **200** and this schedule:
>
> ```
> Batch mill            2026-09-15T08:30:00Z    51.0 g/kWh   saves 612 kg
> Thermal store charge  2026-09-14T17:00:00Z   153.0 g/kWh   saves   0 kg
> Thermal store charge  2026-09-14T17:30:00Z   153.0 g/kWh   saves   0 kg
> ```
>
> The mill sits on the 51 g/kWh window; the thermal store stays on 153 g/kWh
> because the mill already holds 12 MW of the 13.5 MW flexible capacity. When
> testing, beware of reusing a key across a rate-limit burst and a functional
> assertion — the resulting 429 looks like a failure and is not.

---

## UC-7 — The AS400 bridge publishes plant telemetry (Phase 3)

```bash
docker compose logs -f plant-bridge
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic ecogrid.telemetry.plant \
  --from-beginning --max-messages 2 --property print.key=true
```

**Pass criteria:** records appear keyed `plant-01:<window>`. The log shows
`published=1` on the first cycle and `unchanged=1` on subsequent cycles within the
same window. Switch the source to real data with
`ECOGRID_PLANT_SOURCE=file` (drop JSON/JSONL into `./data/as400`) or `odbc`
(live DSN — see `ecogrid/plant/sources.py`).
*Automated: yes — `test_phase3.py` (model validation, simulated + file sources).*

---

## UC-8 — Plant telemetry lands in PostgreSQL

```bash
docker compose logs -f plant-consumer    # expect inserted / duplicates=…
curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/plant/latest"
curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/plant?plant_id=plant-01&limit=5"
```

**Pass criteria:** `/plant/latest` returns **200** with one entry per plant
(`plant_id`, `total_load_mw`, `flexible_load_mw`, `inflexible_load_mw`), and
`flexible + inflexible == total`. Replaying a window leaves `revision_count`
unchanged (duplicate) rather than inserting a second row.

---

## UC-9 — The optimizer produces a dispatch schedule

Trigger a run (requires `operator` or `admin`):

```bash
curl -s -X POST -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/optimize/run"
curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/schedule/latest"
```

**Pass criteria:** `POST /optimize/run` returns **200** with `status=ok`, a
`run_id`, `solver` (`local-greedy-v1`, or `databricks-job` when configured), and
`decision_count > 0`. `/schedule/latest` returns the run plus its decisions, each
with `action` of `run` or `idle` and a `reason`.

Triggering with a **viewer** key must return **403**.

> ### ✅ CONFIRMED — UC-9 + UC-10 (2026-09-15)
>
> `POST /api/v1/optimize/run` returned `200`:
>
> ```json
> {"run_id": "890bc25d8fb748fa9c203a646f85b814", "solver": "local-greedy-v1",
>  "horizon_windows": 4, "process_count": 3, "decision_count": 8,
>  "baseline_carbon_kg": 2601.0, "optimized_carbon_kg": 1377.0,
>  "carbon_saved_kg": 1224.0, "saving_pct": 47.06,
>  "unscheduled": ["electrolyser-01"], "status": "ok"}
> ```
>
> **This is the arbitrage working, and the numbers reconcile exactly.** The batch
> mill (12 MW × 2 windows) moved off the 153 g/kWh windows onto the 51 g/kWh one:
> 12 MW × 0.5 h × (153 − 51) = 612 kg per window, × 2 = **1224 kg CO₂e** — the
> reported saving. Baseline 1836 + 765 = 2601; optimised 612 + 765 = 1377;
> 1224 / 2601 = **47.06%**.
>
> Both "failures" in that output are correct behaviour, not defects:
> * `electrolyser-01` unscheduled — it needs a contiguous **3**-window block with
>   8 MW free, but the mill holds 12 MW of the 13.5 MW capacity in the clean
>   windows, leaving 1.5 MW. The `notes` field states this precisely.
> * The thermal store did not move (its 765 kg appears in *both* totals) — the
>   same capacity constraint. A wider horizon or more plant flexibility would let
>   it shift too.
>
> This also closed a live bug: `load_capacity()` had been building its list from
> the plant's windows rather than the grid's, so the request 500'd once plant
> telemetry existed. See the commit `fix(optimizer): align flexible capacity to
> the grid horizon`.

---

## UC-10 — The schedule is a genuine carbon arbitrage (end-to-end)

This is the case that proves the platform does what it claims: it moves flexible
load into cleaner windows.

```bash
curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/schedule/latest?action=run" \
| python3 -c "
import json,sys
plan=json.load(sys.stdin)
runs=plan['decisions']
print('run decisions :', len(runs))
print('saved kg CO2  :', round(plan['run']['carbon_saved_kg'],1))
print('saving %      :', plan['run']['saving_pct'])
for d in runs:
    print(f\"  {d['process_id']:<20} {d['window_from']}  {d['intensity']:>6.1f} g/kWh  saves {d['carbon_saved_kg']:.1f} kg\")
"
```

**Pass criteria:**
1. `carbon_saved_kg >= 0` — the optimised schedule is never dirtier than the naive
   "run as early as possible" baseline.
2. Every run decision has `carbon_saved_kg >= 0` (savings are shared evenly across
   a process's block, so no individual row is negative).
3. When the retained forecast actually varies, `saving_pct > 0` and the chosen
   windows sit at the low-intensity end. Verify against the grid data:
   `curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/telemetry/stats?hours=24"`.
4. Summing `carbon_kg` over run decisions equals `optimized_carbon_kg`.

*Automated: yes — `test_phase3.py` covers the solver, including the case that
caught the negative-saving attribution bug.*

---

## UC-11 — Resilience: the bridge survives a Kafka outage

```bash
docker compose stop kafka
docker compose logs -f plant-bridge     # sends fail → records are spooled
docker compose start kafka
docker compose logs -f plant-bridge     # next cycle replays the spool
```

**Pass criteria:** while the broker is down the bridge logs failures and spools to
`./data/spool/plant-spool.jsonl`; after restart it logs `replayed=N` and the spool
is **truncated to zero bytes** (never deleted). The bridge stays alive throughout —
one bad cycle must never kill a 24/7 process.
*Automated: partially — the spool contract is tested in `test_ingest_grid.py`;
the live outage is a manual case.*

---

## UC-12 — Databricks path degrades gracefully

Leave `ECOGRID_DATABRICKS_HOST/TOKEN/JOB_ID` unset and confirm the optimizer still
produces schedules using the local solver (`solver=local-greedy-v1`). Then set
them to deliberately wrong values and restart:

```bash
docker compose --profile phase3 up -d --force-recreate optimizer
docker compose logs optimizer | grep -i "falling back"
```

**Pass criteria:** the optimizer logs
`Databricks optimisation failed (…) — falling back to the local solver` and still
writes a run. A scheduler that produces nothing because a cluster is unreachable
would be worse than one that produces a slightly worse schedule.

> ### ✅ PARTIALLY CONFIRMED — UC-12 (2026-09-15)
>
> * **Unconfigured path — verified live.** No Databricks credentials are set in
>   the running stack, and the optimizer produced `solver=local-greedy-v1`. That
>   is the fallback working in production.
> * **Unreachable path — verified by test.** `DatabricksRunner.run_remote()`
>   returns `None` when configured against a dead host, which is the contract
>   that makes the loop fall back. Covered by
>   `test_databricks_falls_back_when_the_cluster_is_unreachable`.
> * **Not yet run end-to-end:** a *real* Databricks job (needs a workspace and a
>   notebook that emits the expected JSON). The seam is exercised; the remote
>   side is not.

---

## UC-13 — The AI Orchestrator produces advice (with or without Claude)

```bash
docker compose --profile ai up -d --build
docker compose logs -f orchestrator

curl -s -X POST -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/orchestrator/run"
curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/orchestrator/latest"
curl -s -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/orchestrator/advice?limit=5"
```

**Pass criteria:** `POST /orchestrator/run` returns **200** with a non-empty
`headline`, `confidence` in `[0,1]`, and a `source` of **`heuristic`** when no
`ECOGRID_ANTHROPIC_API_KEY` is set (or **`claude`** when one is). Advice is
produced either way — the platform never goes silent. Triggering with a **viewer**
key returns **403**.
*Automated: yes — `test_ai_orchestrator.py` covers the heuristic rules and the
fallback paths.*

## UC-14 — The orchestrator degrades gracefully when the model is unreachable

Set a deliberately invalid key and restart:

```bash
ECOGRID_ANTHROPIC_API_KEY=sk-invalid docker compose --profile ai up -d --force-recreate orchestrator
docker compose logs orchestrator | grep -i "falling back"
curl -s -X POST -H "X-API-Key: $ADMIN_KEY" "$API/api/v1/orchestrator/run"
```

**Pass criteria:** the log records a fallback (HTTP error, transport error, or
unparseable output) and the response still carries `source=heuristic`. Unvalidated
model output is never stored — if the reply cannot be parsed or fails schema
validation, the advisor declines and the heuristic answers instead.

## UC-15 — The dashboard renders the platform state

```bash
cd dashboard && npm ci && npm run build && cd ..
docker compose --profile gateway up -d --build
open http://localhost:8080          # or https://localhost:8443
```

Enter the admin key in the header field and press **Load**.

**Pass criteria:** the dashboard loads, and the Platform / Grid intensity / Plant
load / Dispatch schedule / AI Orchestrator panels populate from the API. Pressing
**Run optimizer** and **Ask orchestrator** triggers real runs and refreshes the
view. An invalid key shows the API's error message rather than a blank page.

> ### ✅ CONFIRMED — UC-15 (2026-09-15)
>
> Observed at `http://localhost:8080` with the gateway healthy. All five panels
> rendered live data: Platform `v0.2.0 / OK` (postgres ok, redis ok, newest window
> 0.5 min old); Grid intensity `LOW, 51 g/kWh, 70.0% renewable, avg actual 102,
> 4 windows, 0 forecast-only`; Plant load `1 plant, 38.5 MW total / 13.5 MW
> flexible / 25.0 MW inflexible`; Dispatch schedule `local-greedy-v1, 4 decisions`;
> AI Orchestrator `source: heuristic, confidence 50%`.
>
> Reading the panel values correctly:
> * **`SAVED 0 kg / REDUCTION 0.0%` is the *correct* answer for that run, not a
>   failure.** The schedule shown came from the run at startup, when only **two**
>   grid windows had been retained and both sat at the same intensity (153 g/kWh).
>   With a flat spread there is no arbitrage to capture, so zero saving is right —
>   the solver and the heuristic advisor both said exactly that ("Grid is flat").
> * The orchestrator's risk flags (`1 process(es) could not be scheduled:
>   electrolyser-01`, `No plant telemetry`) are **stale**, from that same early
>   run: a 3-window process cannot fit in a 2-window horizon, and plant telemetry
>   had not been consumed yet. Both clear on the next run once more data exists.
> * This is why re-running the optimizer after the ingestor has accumulated
>   windows is the meaningful check — see UC-9/UC-10.

## UC-16 — Gateway terminates TLS and rate limits at the edge

```bash
curl -k -i https://localhost:8443/healthz              # TLS (self-signed)
curl -i http://localhost:8080/api/v1/whoami -H "X-API-Key: $ADMIN_KEY"

# Sustained burst far above the edge limit of 20 r/s:
for i in $(seq 1 80); do
  curl -s -o /dev/null -w "%{http_code} " -H "X-API-Key: $ADMIN_KEY" \
    http://localhost:8080/api/v1/whoami
done; echo
```

**Pass criteria:** `https://localhost:8443/healthz` returns **200** (the
self-signed certificate is expected — `-k` suppresses the warning; replace
`./certs` with real certificates for anything beyond local use). `/api` is proxied
correctly, and a sustained burst returns **429** from nginx *in addition to* the
API's own 120/min per-key limit. `/healthz` is proxied but **never** rate limited —
throttling a healthcheck would let an orchestrator mark a healthy service as dead.

> ### ✅ CONFIRMED — UC-16 (2026-09-15) — 8/8 checks
>
> | Check | Observed |
> |---|---|
> | `GET https://:8443/healthz` | **200** (TLS terminates) |
> | `GET https://:8443/api/v1/whoami` with a valid key | **200** (TLS + proxy + auth) |
> | `GET http://:8080/api/v1/whoami` | **200** |
> | `GET http://:8080/` | **200**, serves the SPA shell (`<div id="root">`) |
> | 120 concurrent requests through the edge | **45×200, 75×429** |
> | The 429s are the *edge* limiter | `server: nginx/1.27.5`, `content-type: text/html`, **no** `X-RateLimit-Limit` |
> | 60 rapid `GET /healthz` | **all 200** — exempt from edge limiting |
>
> The 429 provenance matters: the app's limiter returns JSON with
> `X-RateLimit-Limit`, while nginx returns an HTML page without it. Seeing the
> nginx signature proves the *edge* is doing the throttling, not the application
> — i.e. defence in depth is genuinely two layers, not one limiter observed twice.
>
> Getting here required fixing the gateway twice: the stale-upstream 502 (nginx
> resolved `api` once at startup) and the cross-profile `no such service: api`.
> Both are recorded in the troubleshooting table above.

## UC-17 — Migrations apply cleanly and match the models

```bash
export ECOGRID_POSTGRES_DSN=postgresql://ecogrid:…@localhost:15432/ecogrid
alembic upgrade head          # on an empty database: creates all tables
alembic current
alembic revision --autogenerate -m "drift check"
```

**Pass criteria:** `alembic upgrade head` creates all eight tables, and a
subsequent `--autogenerate` reports **no changes** — that is the proof the
migration matches `ecogrid/models.py`. If it proposes a diff, the migration and
the models have drifted and the new revision should be reviewed and applied.
`alembic downgrade base` drops them again.

> ### ✅ CONFIRMED — UC-17 (2026-09-15)
>
> Run against a **throwaway database** (`ecogrid_migration_test`, created and
> dropped for the test) so the live database was never touched:
>
> ```
> alembic upgrade head   -> ok
> alembic current        -> 0001_baseline (head)
> table set              -> MATCH: 8 migrated == 8 model tables
> alembic downgrade base -> ok   (round-trip clean)
> ```
>
> This case found a real bug: `env.py` stripped `+asyncpg` to build a
> `postgresql://` URL, which SQLAlchemy resolves to **psycopg2** — a driver this
> project never installs. Every alembic command died with
> `ModuleNotFoundError: No module named 'psycopg2'`. Fixed by driving the async
> engine through `connection.run_sync()`, so asyncpg is the only driver needed.
>
> Not covered here: the column-level `--autogenerate` drift check (it writes a
> revision file). The table-set comparison above catches a missing or extra
> table but would not catch a changed column type. Run
> `alembic revision --autogenerate -m "drift check"` and expect an empty
> upgrade/downgrade body before relying on the baseline as a true superset.

---

## Automated coverage

```bash
pytest -q                         # 92 tests, no network/broker/DB required
pytest -q test_phase3.py          # Phase 3 only: 24 tests
pytest -q test_ai_orchestrator.py # AI Orchestrator only: 17 tests
```

| Area | Automated | Manual |
|---|---|---|
| Grid ingestion, dedupe, spool/replay | ✅ | UC-1, UC-11 |
| Consumer idempotency + audit ledger | ✅ | UC-2 |
| Auth / RBAC / rate limit | ✅ | UC-3, UC-4, UC-5 |
| Plant contract + sources | ✅ | UC-7, UC-8 |
| Solver correctness + carbon maths | ✅ | UC-9, UC-10 |
| Advisor rules + Claude fallback | ✅ | UC-13, UC-14 |
| MLflow / JSONL tracking | ✅ | UC-13 |
| Kafka/DB wiring as containers | ❌ | UC-0, UC-2, UC-7, UC-8 |
| Databricks integration | ❌ (seam only) | UC-12 |
| Dashboard rendering | ❌ (build verified) | UC-15 |
| Gateway TLS + edge rate limiting | ❌ | UC-16 |
| Alembic migrations | ❌ (import verified) | UC-17 |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `address already in use` on 5432/6379 | Native PostgreSQL/Redis already running | Set `POSTGRES_HOST_PORT` / `REDIS_HOST_PORT` in `.env` |
| `no such service: migrate` when logging a single service | Naming a service activates its own profile, but its dependencies must also be in an active profile — `plant-bridge` (phase3) depends on `migrate` | Pass every profile you need in one command. `migrate` declares `platform`, `phase3` **and** `ai` so any of them works |
| Gateway returns **502** on `/api/…` and `/healthz`, but `/` still serves the dashboard | nginx resolved `api` to an IP **once at startup**; recreating the `api` container gave it a new IP, so the gateway kept proxying to the dead one. The static dashboard masks it — it looks like the API is down when it is not | Fixed: the gateway now uses `resolver 127.0.0.11 valid=10s` with the upstream in a variable, so it re-resolves. Recreate the gateway to pick up the change: `docker compose --profile gateway up -d --force-recreate gateway` |
| `WARN The "BS" variable is not set. Defaulting to a blank string.` | A bare `$VAR` inside an embedded shell script is interpolated by **Compose**, not the shell, so it expands to empty — `kafka-topics --bootstrap-server ""` | Escape anything meant for the container shell as `$$VAR`. `${VAR}` stays as deliberate Compose interpolation. Guarded by `test_no_bare_dollar_in_compose_commands` |
| Broker `unhealthy` with the `sasl` override | The SASL listener was named `SASL_PLAINTEXT`. Confluent maps `KAFKA_LISTENER_NAME_<NAME>_…_JAAS_CONFIG` to a dotted key by splitting on **every** underscore, producing the invalid `listener.name.sasl.plaintext.plain.sasl.jaas.config` — the JAAS is dropped and the listener cannot start | Listener renamed to `SASL` (single word). Guarded by `test_sasl_listener_name_has_no_underscore` |
| `Found orphan containers (…)` / services fighting between runs | Commands were issued with **different `-f` file sets** against the same stack — e.g. bringing it up with `-f docker-compose.ha.yml` and then running another command with only the base file | Use the **same** `-f` flags for every command against a given stack, and `--remove-orphans` when switching between them |
| `{"error":"unknown API key"}` (401) | `$ADMIN_KEY` is empty or stale — `migrate` only prints the key the *first* time it provisions one | Mint a fresh one: `docker compose --profile platform run --rm --entrypoint python migrate -m ecogrid.keys create --name ops-admin --role admin` |
| Kafka container stuck `unhealthy` | Healthcheck must probe `kafka:29092`, not `localhost:29092` | Already correct; check `KAFKA_HOST_PORT` matches the advertised listener |
| `migrate` never completes / stack hangs | A service inherited the image's default `consumer` command | Ensure `migrate` has `command: ["python","-m","ecogrid.migrate"]` |
| Consumer container always `unhealthy` | `pgrep` missing in the image | `procps` is installed in `Dockerfile.platform`; rebuild |
| `/plant/latest` returns 404 | No plant telemetry consumed yet | Start `plant-bridge` + `plant-consumer`, wait one cycle |
| `/optimize/run` returns `status=skipped` | Fewer than 2 grid windows retained | Let the ingestor run; check `/api/v1/ingest-status` |
| `unknown ECOGRID_PLANT_SOURCE` | Typo in the source name | Use `simulated`, `file`, or `odbc` |



```
cd /Users/kannans/dev/aiml/deepseek/ecogrid-os

# 1. Full teardown — clears the mixed config AND the HA orphans
docker compose -f docker-compose.yml -f docker-compose.ha.yml down -v --remove-orphans

# 2. Bring up the normal stack (single broker) — all profiles, ONE command
docker compose --profile platform --profile phase3 --profile ai --profile gateway up -d --build

# 3. Fresh admin key (the volume was recreated, so migrate generated a NEW one —
#    your old $ADMIN_KEY is dead)
docker compose logs migrate | grep -A6 "BOOTSTRAP ADMIN"
export API=http://localhost:8000
export ADMIN_KEY=<paste it>

# 4. Re-seed data
docker compose --profile worker up -d
```