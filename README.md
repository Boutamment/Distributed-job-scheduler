# JobForge — a distributed job scheduler (mini-Airflow)

Define workflows as DAGs through a FastAPI API. A pool of workers runs the tasks
and the system stays correct when workers crash, hang, or run twice.
**Postgres is the only dependency**: it is the queue, the state machine, the lock
service and the clock.

```
            ┌──────────┐   PUT /workflows, POST /runs     ┌────────────┐
 client ───▶│ FastAPI  │──────────────────────────────────▶│            │
 dashboard ◀│  (API)   │◀──────── /stats, /runs ───────────│  Postgres  │
            └──────────┘                                   │            │
 ┌─────────────┐  reap · propagate · promote · finalize    │ task_      │
 │ Scheduler×N │◀─────────────────────────────────────────▶│ instances  │
 └─────────────┘  (advisory lock: one active, rest standby)│ (queue +   │
 ┌─────────────┐  claim (SKIP LOCKED) · heartbeat ·        │  state     │
 │  Worker×N   │◀────────── complete / fail ───────────────▶│  machine)  │
 └─────────────┘  (all writes fenced by token)             └────────────┘
```

## Run it

```bash
docker compose up --build          # db + api + 2 schedulers + 3 workers
open http://localhost:8000         # dashboard: "Load demo workflows", then "Run ×10"
```

Without Docker (needs a local Postgres):

```bash
pip install -r requirements-dev.txt
export DATABASE_URL=postgresql://jobforge:jobforge@localhost:5432/jobforge
uvicorn app.api:app &  python -m app.scheduler &  python -m app.worker &
pytest                              # 14 tests: DAG validation + state machine
python chaos.py --runs 30 --duration 30   # kill -9 workers while work runs
```

Try the failure modes live: `docker compose kill -s KILL worker` (or kill one
container) while a `long-task` run is going, and watch the dashboard: the lease
expires, "leases reaped" ticks up, another worker finishes the task.

## API

| Method | Path | Purpose |
|---|---|---|
| PUT | `/workflows` | create/replace a workflow (validated: unknown deps, cycles → 422) |
| POST | `/workflows/{name}/runs` | start a run; `Idempotency-Key` header makes retries safe |
| GET | `/runs`, `/runs/{id}` | run list / per-task state, attempts, errors |
| GET | `/stats` | task counts, live workers, reaped leases, retries, duplicates absorbed |
| POST | `/demo/seed` | install sample workflows |

```json
{ "name": "etl", "tasks": [
  {"key": "extract", "handler": "effect"},
  {"key": "clean",    "handler": "effect", "depends_on": ["extract"]},
  {"key": "load",     "handler": "effect", "depends_on": ["clean"], "max_attempts": 5}
]}
```

Add your own handlers in `app/handlers.py` (`async def handler(ctx) -> dict`).

## Task state machine

```
pending ─▶ ready ─▶ running ─▶ success
              ▲         ├─────▶ ready    failure with attempts left (backoff), or lease expired
              │         └─────▶ failed   attempts exhausted (dead letter)
pending ─▶ upstream_failed               a dependency failed
```

## Failure-mode design (the interesting part)

| Failure | What happens | Where |
|---|---|---|
| **Worker crashes mid-task** | Heartbeats stop, lease expires, the scheduler's `reap_expired` returns the task to `ready`. Another worker picks it up. | `queue.reap_expired` |
| **Two workers claim the same task** | Impossible: claim is one statement using `FOR UPDATE SKIP LOCKED`. | `queue.CLAIM_SQL` |
| **Slow worker, not dead (zombie)** | Its lease is reaped and the task re-claimed. Reaping bumps the **fencing token**; every heartbeat/complete/fail is guarded by `WHERE fencing_token = $token`, so the zombie's writes match 0 rows and are discarded. | `queue.complete/fail/heartbeat` |
| **Task runs twice** (at-least-once) | Side effects use an idempotency key `run_id:task_key` (`ON CONFLICT DO UPDATE`) and are *also* fenced by the token, so a zombie cannot write after being superseded. Effectively-once outcome. | `handlers.record_effect` |
| **Clock skew** | No worker clock is ever compared to anything. Leases, backoff and readiness all use the database's `now()`. | all SQL |
| **Scheduler crashes** | Run ≥2. `pg_try_advisory_xact_lock` makes one active; the lock dies with its transaction, so a standby takes over next tick. No state lives in the scheduler. | `scheduler.tick` |
| **Retry storms** | Exponential backoff with jitter: `min(cap, base·2^(n-1)) · U(0.5, 1)`. | `queue.fail` |
| **Client retries "create run"** | `Idempotency-Key` header → same run returned (HTTP 200, not a second run). | `queue.create_run` |
| **Dependency fails** | Downstream tasks become `upstream_failed` (cascaded), the run is marked `failed`. | `queue.propagate_failures` |

`chaos.py` is the evidence: it `SIGKILL`s random workers and the scheduler while
30 workflows run, then asserts every task succeeded, every effect was applied
exactly once, and no fencing token was ever issued twice.

Sample result (12 kills, 4 workers): **13 leases reaped, 23 retries, 30/30 runs
successful, 120/120 effects applied once.**

## Design decisions and trade-offs

- **Postgres as the queue.** Trades peak throughput (low thousands of tasks/s per
  Postgres) for transactional correctness and one dependency. Right call until
  you need Kafka-scale; the SQL is the part that would move to a dedicated broker.
- **Polling, not push.** Workers poll every ~200 ms when idle. Simple and robust;
  `LISTEN/NOTIFY` would cut latency at the cost of more moving parts.
- **Lease length vs. recovery time.** A short lease recovers faster but risks
  false reaps under GC pauses; fencing makes false reaps *safe*, which is why the
  lease can be aggressive.
- **At-least-once + idempotency**, not "exactly-once": the only honest option in
  a distributed system. A crash *after* a side effect but *before* `complete`
  causes a re-run; the idempotency key makes the repeat harmless.

## Known limitations / next steps

- Handlers live in-process (`HANDLERS` registry); a production system would run
  user code in isolated containers or call out over HTTP.
- No cron-style schedules, task timeouts, priorities, or per-queue concurrency
  limits yet.
- No auth on the API. Add OAuth2/JWT before exposing it.
- Benchmark section is yours to add: run `pgbench`-style load (`Run ×10` in a
  loop, or a k6 script against `/workflows/*/runs`) and record tasks/s versus
  worker count. A real throughput number is what makes this README land.
