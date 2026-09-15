# Contributing to EcoGrid OS

Thanks for taking a look. This document is short on ceremony and long on one
rule, because that rule is the single most important thing about working in this
repository.

---

## The rule

> **Run it. A passing test suite proves almost nothing here.**

That is not a rhetorical flourish — it is measured. During development of this
project, **twelve defects were found, and the automated suite caught none of
them.** Every one surfaced by executing the system:

| Found by | Examples |
|---|---|
| Running a command | `no such service: migrate`; `no such service: api` — cross-profile `depends_on` |
| Reading a response | `POST /optimize/run` returning a 500 with an empty body |
| Watching a container | Gateway returning 502 while the dashboard still loaded |
| Running the tool | `alembic` requiring `psycopg2`; `alembic` with no `script.py.mako` |
| Looking at the UI | A schedule beside advice saying "no run available yet"; `-7.9 min old` |

So: **before you claim something works, execute it and record what you saw.**

---

## Getting started

```bash
git clone <repo> && cd ecogrid-os
cp .env.example .env          # override POSTGRES_HOST_PORT / REDIS_HOST_PORT if taken

docker compose up -d                                    # backbone
docker compose --profile platform --profile phase3 \
               --profile ai --profile gateway up -d --build
```

Admin key (printed once):

```bash
docker compose logs migrate | grep -A6 "BOOTSTRAP ADMIN"
```

Dashboard needs a build before the gateway can serve it:

```bash
cd dashboard && npm ci && npm run build && cd ..
```

## Running the tests

```bash
pytest -q            # 118 tests, fully offline — no network, broker or database
```

The suite is designed to run anywhere: integration tests that need
infrastructure **skip cleanly** rather than fail. If your change needs a database
to be meaningful, add it to a use case instead — see below.

## Style

- **Python:** `ruff` with `F, E9, B, A, C4, SIM, RET, UP, I, PT`.
- **TypeScript:** `tsc --noEmit` must pass (`npm run build` runs it).
- Prefer explicit over clever. Comment *why*, not *what* — the code already says
  what it does.
- When you work around an environment or framework quirk, say so in a comment.
  Someone will otherwise "fix" it back.

## Commits

Conventional-commit subjects (`fix(compose):`, `feat(optimizer):`,
`docs(verification):`), then a body that explains **the reasoning and the
consequence**, not a restatement of the diff.

If you fixed a bug, the body should say what the failure looked like from the
outside. That is the part that is hard to reconstruct later.

---

## Adding a capability

New behaviour needs three things, not one:

1. **Code**, with tests.
2. **A requirement** in [`docs/REQUIREMENTS.md`](docs/REQUIREMENTS.md) with a
   stable `FR-*` / `NFR-*` ID.
3. **A use case** in [`docs/VERIFICATION.md`](docs/VERIFICATION.md) with exact
   commands and an explicit **pass criteria** line.

A requirement with no use case is not verifiable, and will be marked as such.

## When you find a bug

Ask **"what class of bug is this?"** and fix the class, not just the instance.

This was learned the hard way: a cross-profile `depends_on` failure was fixed for
`migrate` only, and the identical bug reappeared for `api` the next time someone
ran a different profile. It is now a test
(`test_compose_profiles.py::test_every_dependency_shares_a_profile_with_its_dependant`).

If the class is mechanically checkable, add the check. A lint that costs 50 ms is
worth more than a paragraph in a document nobody rereads.

## Updating documentation

Docs in this repository make **falsifiable claims**, and they rot fast.

- [`VERIFICATION.md`](docs/VERIFICATION.md) records what was *observed*. When you
  verify something, update it. When you break something, update it.
- The README's verification block has drifted in **both** directions — once
  claiming 92 tests when there were 118, and once claiming features were
  unverified that had been verified. Understating is as misleading as
  overstating. Re-read it after every round of verification.
- Never mark something ✅ without an observation behind it. "It should work" is
  not a status.

## Things that will get a PR sent back

- A new dependency that is not actually needed (see how `mlflow` and
  `psycopg2` were deliberately *not* added).
- A service using the platform image without an explicit `command` — it will
  silently inherit the consumer CMD.
- A `$VAR` in an embedded shell script inside compose YAML. Compose interpolates
  it before the shell runs; use `$$VAR`. There is a test for this.
- Changing `ecogrid/plant/__init__.py` back to eager imports — a data contract
  must not require a Kafka client.
- A README claim without a corresponding verification entry.

---

## Where things live

| Path | Contains |
|---|---|
| `ingest_grid.py` | Phase 1 worker, and the shared `GridTelemetry` contract |
| `ecogrid/` | Platform Core (API, consumer, models) plus `plant/`, `optimizer/`, `orchestrator/` |
| `dashboard/` | React + TypeScript operator dashboard |
| `docker/nginx/` | Edge gateway configuration |
| `docs/ARCHITECTURE.md` | Design decisions and their rationale |
| `docs/PRD.md` | Why the product exists, and what it deliberately does not do |
| `docs/REQUIREMENTS.md` | Numbered, traceable requirements |
| `docs/VERIFICATION.md` | How to prove each capability works |
| `scripts/` | Operational tooling (demo data seeding) |
| `alembic/` | Schema migrations |

## Questions

Open an issue. If it is a security matter, see [`SECURITY.md`](SECURITY.md)
instead — please do not open a public issue for those.
