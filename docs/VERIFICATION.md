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

---

## Automated coverage

```bash
pytest -q                # 75 tests, no network/broker/DB required
pytest -q test_phase3.py # Phase 3 only: 24 tests
```

| Area | Automated | Manual |
|---|---|---|
| Grid ingestion, dedupe, spool/replay | ✅ | UC-1, UC-11 |
| Consumer idempotency + audit ledger | ✅ | UC-2 |
| Auth / RBAC / rate limit | ✅ | UC-3, UC-4, UC-5 |
| Plant contract + sources | ✅ | UC-7, UC-8 |
| Solver correctness + carbon maths | ✅ | UC-9, UC-10 |
| Kafka/DB wiring as containers | ❌ | UC-0, UC-2, UC-7, UC-8 |
| Databricks integration | ❌ (seam only) | UC-12 |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `address already in use` on 5432/6379 | Native PostgreSQL/Redis already running | Set `POSTGRES_HOST_PORT` / `REDIS_HOST_PORT` in `.env` |
| Kafka container stuck `unhealthy` | Healthcheck must probe `kafka:29092`, not `localhost:29092` | Already correct; check `KAFKA_HOST_PORT` matches the advertised listener |
| `migrate` never completes / stack hangs | A service inherited the image's default `consumer` command | Ensure `migrate` has `command: ["python","-m","ecogrid.migrate"]` |
| Consumer container always `unhealthy` | `pgrep` missing in the image | `procps` is installed in `Dockerfile.platform`; rebuild |
| `/plant/latest` returns 404 | No plant telemetry consumed yet | Start `plant-bridge` + `plant-consumer`, wait one cycle |
| `/optimize/run` returns `status=skipped` | Fewer than 2 grid windows retained | Let the ingestor run; check `/api/v1/ingest-status` |
| `unknown ECOGRID_PLANT_SOURCE` | Typo in the source name | Use `simulated`, `file`, or `odbc` |
