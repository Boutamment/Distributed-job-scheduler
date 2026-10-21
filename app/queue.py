"""All state transitions of the task state machine, as SQL.

Every function takes `db`, which may be an asyncpg Pool or Connection.

Correctness rests on three rules:
  1. Time comes from the database (now()), never from a worker's clock.
  2. Claiming uses FOR UPDATE SKIP LOCKED, so two workers never get the same row.
  3. Every worker-side write (heartbeat / complete / fail) is guarded by the
     task's fencing token. A worker whose lease was reaped holds a stale token,
     so its writes match zero rows and are discarded.
"""
from __future__ import annotations

import uuid
from typing import Any, Mapping, Sequence

# --------------------------------------------------------------------------
# Workflows and runs
# --------------------------------------------------------------------------


async def upsert_workflow(db, name: str, tasks: Sequence[Mapping[str, Any]]) -> None:
    await db.execute(
        """
        INSERT INTO workflows (name, definition) VALUES ($1, $2)
        ON CONFLICT (name) DO UPDATE SET definition = EXCLUDED.definition
        """,
        name,
        {"tasks": list(tasks)},
    )


async def create_run(pool, workflow_name: str, idempotency_key: str | None = None):
    """Create a run and all its task instances atomically.

    Returns (run_id, created). With an idempotency_key, a retried request
    returns the original run instead of starting a second one.
    Raises LookupError if the workflow does not exist.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            wf = await conn.fetchrow(
                "SELECT id, definition FROM workflows WHERE name = $1", workflow_name
            )
            if wf is None:
                raise LookupError(workflow_name)

            run_id = await conn.fetchval(
                """
                INSERT INTO runs (workflow_id, workflow_name, idempotency_key)
                VALUES ($1, $2, $3)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id
                """,
                wf["id"],
                workflow_name,
                idempotency_key,
            )
            if run_id is None:  # lost the race / retried request
                existing = await conn.fetchval(
                    "SELECT id FROM runs WHERE idempotency_key = $1", idempotency_key
                )
                return existing, False

            rows = []
            for t in wf["definition"]["tasks"]:
                deps = sorted(set(t.get("depends_on") or []))
                rows.append(
                    (
                        run_id,
                        t["key"],
                        t["handler"],
                        t.get("params") or {},
                        deps,
                        int(t.get("max_attempts", 3)),
                        "pending" if deps else "ready",
                    )
                )
            await conn.executemany(
                """
                INSERT INTO task_instances
                    (run_id, task_key, handler, params, depends_on, max_attempts, state)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                rows,
            )
            return run_id, True


# --------------------------------------------------------------------------
# Worker side: claim / heartbeat / complete / fail
# --------------------------------------------------------------------------

CLAIM_SQL = """
WITH candidate AS (
    SELECT id FROM task_instances
    WHERE state = 'ready' AND next_attempt_at <= now()
    ORDER BY next_attempt_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE task_instances t
SET state = 'running',
    attempt = t.attempt + 1,
    lease_owner = $1,
    lease_expires_at = now() + make_interval(secs => $2::float8),
    fencing_token = nextval('fencing_seq'),
    started_at = COALESCE(t.started_at, now())
FROM candidate
WHERE t.id = candidate.id
RETURNING t.id, t.run_id, t.task_key, t.handler, t.params,
          t.attempt, t.max_attempts, t.fencing_token
"""


async def claim(db, worker_id: str, lease_seconds: float):
    """Atomically take one ready task and start its lease. None if nothing is ready."""
    return await db.fetchrow(CLAIM_SQL, worker_id, float(lease_seconds))


async def heartbeat(db, task_id: uuid.UUID, token: int, lease_seconds: float) -> bool:
    """Extend the lease. False means the lease is gone and the worker must stop."""
    row = await db.fetchrow(
        """
        UPDATE task_instances
        SET lease_expires_at = now() + make_interval(secs => $3::float8)
        WHERE id = $1 AND fencing_token = $2 AND state = 'running'
        RETURNING id
        """,
        task_id,
        token,
        float(lease_seconds),
    )
    return row is not None


async def complete(db, task_id: uuid.UUID, token: int, result: Any) -> bool:
    """Mark success. False means we were fenced out and the result is discarded."""
    row = await db.fetchrow(
        """
        UPDATE task_instances
        SET state = 'success', result = $3, finished_at = now(),
            lease_owner = NULL, lease_expires_at = NULL
        WHERE id = $1 AND fencing_token = $2 AND state = 'running'
        RETURNING id
        """,
        task_id,
        token,
        result,
    )
    return row is not None


async def fail(
    db,
    task_id: uuid.UUID,
    token: int,
    error: str,
    backoff_base: float,
    backoff_cap: float,
) -> str | None:
    """Record a failed attempt.

    Retries with exponential backoff and jitter while attempts remain, otherwise
    the task goes to 'failed' (dead). Returns the new state, or None if fenced out.
    Delay = min(cap, base * 2^(attempt-1)) * uniform(0.5, 1.0): the jitter spreads
    out retries so a burst of failures does not come back as a burst.
    """
    return await db.fetchval(
        """
        UPDATE task_instances
        SET state = CASE WHEN attempt < max_attempts THEN 'ready' ELSE 'failed' END,
            next_attempt_at = CASE WHEN attempt < max_attempts
                THEN now() + make_interval(secs =>
                    LEAST($5::float8, $4::float8 * power(2::float8, (attempt - 1)::float8))
                    * (0.5 + random() * 0.5))
                ELSE next_attempt_at END,
            finished_at = CASE WHEN attempt >= max_attempts THEN now() END,
            error = $3,
            lease_owner = NULL,
            lease_expires_at = NULL
        WHERE id = $1 AND fencing_token = $2 AND state = 'running'
        RETURNING state
        """,
        task_id,
        token,
        error,
        float(backoff_base),
        float(backoff_cap),
    )


# --------------------------------------------------------------------------
# Scheduler side: reap / propagate / promote / finalize
# --------------------------------------------------------------------------


async def reap_expired(db, backoff_base: float, backoff_cap: float) -> int:
    """Take back tasks whose lease expired (worker crashed, hung or was partitioned).

    The fencing token is bumped here, so a zombie that wakes up later is fenced
    immediately, before anyone else has even re-claimed the task.
    """
    rows = await db.fetch(
        """
        UPDATE task_instances
        SET state = CASE WHEN attempt < max_attempts THEN 'ready' ELSE 'failed' END,
            next_attempt_at = now() + make_interval(secs =>
                LEAST($2::float8, $1::float8 * power(2::float8, (attempt - 1)::float8))
                * (0.5 + random() * 0.5)),
            fencing_token = nextval('fencing_seq'),
            reaped_count = reaped_count + 1,
            error = 'lease expired (worker lost)',
            finished_at = CASE WHEN attempt >= max_attempts THEN now() END,
            lease_owner = NULL,
            lease_expires_at = NULL
        WHERE state = 'running' AND lease_expires_at < now()
        RETURNING id
        """,
        float(backoff_base),
        float(backoff_cap),
    )
    return len(rows)


async def propagate_failures(db) -> int:
    """Mark pending tasks whose dependency failed as upstream_failed."""
    rows = await db.fetch(
        """
        UPDATE task_instances t
        SET state = 'upstream_failed', finished_at = now()
        WHERE t.state = 'pending' AND EXISTS (
            SELECT 1 FROM task_instances d
            WHERE d.run_id = t.run_id
              AND d.task_key = ANY (t.depends_on)
              AND d.state IN ('failed', 'upstream_failed'))
        RETURNING t.id
        """
    )
    return len(rows)


async def promote_ready(db) -> int:
    """Move pending tasks to ready once every dependency has succeeded."""
    rows = await db.fetch(
        """
        UPDATE task_instances t
        SET state = 'ready', next_attempt_at = now()
        WHERE t.state = 'pending' AND NOT EXISTS (
            SELECT 1 FROM task_instances d
            WHERE d.run_id = t.run_id
              AND d.task_key = ANY (t.depends_on)
              AND d.state <> 'success')
        RETURNING t.id
        """
    )
    return len(rows)


async def finalize_runs(db) -> int:
    """Close runs that have no unfinished tasks left."""
    rows = await db.fetch(
        """
        UPDATE runs r
        SET status = CASE WHEN EXISTS (
                SELECT 1 FROM task_instances t
                WHERE t.run_id = r.id AND t.state IN ('failed', 'upstream_failed'))
            THEN 'failed' ELSE 'success' END,
            finished_at = now()
        WHERE r.status = 'running' AND NOT EXISTS (
            SELECT 1 FROM task_instances t
            WHERE t.run_id = r.id AND t.state IN ('pending', 'ready', 'running'))
        RETURNING r.id
        """
    )
    return len(rows)
