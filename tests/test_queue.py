"""Integration tests for the state machine. Need a reachable Postgres (DATABASE_URL);
they are skipped otherwise. Each test gets a clean database."""
import asyncio
import os

import pytest

from app import queue as q
from app.db import create_pool, init_schema

URL = os.environ.get("DATABASE_URL", "postgresql://jobforge:jobforge@localhost:5432/jobforge")


def run(coro_fn):
    """Run `coro_fn(pool)` against a freshly truncated database."""
    async def runner():
        try:
            pool = await create_pool(URL, retries=2)
        except RuntimeError:
            pytest.skip("Postgres not reachable")
        try:
            await init_schema(pool)
            await pool.execute("TRUNCATE effects, task_instances, runs, workflows, workers CASCADE")
            return await coro_fn(pool)
        finally:
            await pool.close()
    return asyncio.run(runner())


async def make_run(pool, tasks, key=None):
    await q.upsert_workflow(pool, "wf", tasks)
    return await q.create_run(pool, "wf", key)


def task(key, *deps, max_attempts=3):
    return {"key": key, "handler": "noop", "depends_on": list(deps), "max_attempts": max_attempts}


async def state(pool, run_id, key):
    return await pool.fetchval(
        "SELECT state FROM task_instances WHERE run_id = $1 AND task_key = $2", run_id, key)


def test_dependencies_gate_promotion():
    async def body(pool):
        run_id, _ = await make_run(pool, [task("a"), task("b", "a")])
        assert await state(pool, run_id, "b") == "pending"
        first = await q.claim(pool, "w1", 30)
        assert first["task_key"] == "a"
        assert await q.claim(pool, "w1", 30) is None          # b is not ready yet
        assert await q.complete(pool, first["id"], first["fencing_token"], {})
        await q.promote_ready(pool)
        second = await q.claim(pool, "w1", 30)
        assert second["task_key"] == "b"
    run(body)


def test_concurrent_claims_never_share_a_task():
    async def body(pool):
        await make_run(pool, [task(f"t{i}") for i in range(20)])
        claims = await asyncio.gather(*[q.claim(pool, f"w{i}", 30) for i in range(40)])
        got = [c["id"] for c in claims if c]
        assert len(got) == 20 and len(set(got)) == 20       # 20 tasks, 20 distinct winners
    run(body)


def test_expired_lease_is_reaped_and_zombie_is_fenced():
    async def body(pool):
        run_id, _ = await make_run(pool, [task("a")])
        zombie = await q.claim(pool, "w1", 0.2)              # w1 "crashes": no heartbeats
        await asyncio.sleep(0.4)
        assert await q.reap_expired(pool, 0, 0) == 1
        fresh = await q.claim(pool, "w2", 30)
        assert fresh["id"] == zombie["id"] and fresh["attempt"] == 2
        assert fresh["fencing_token"] > zombie["fencing_token"]
        # the zombie wakes up and tries to report: everything is rejected
        assert not await q.heartbeat(pool, zombie["id"], zombie["fencing_token"], 30)
        assert not await q.complete(pool, zombie["id"], zombie["fencing_token"], {"stale": 1})
        assert await q.fail(pool, zombie["id"], zombie["fencing_token"], "x", 0, 0) is None
        assert await state(pool, run_id, "a") == "running"   # still owned by w2
        assert await q.complete(pool, fresh["id"], fresh["fencing_token"], {"ok": 1})
    run(body)


def test_heartbeat_keeps_lease_alive():
    async def body(pool):
        await make_run(pool, [task("a")])
        c = await q.claim(pool, "w1", 0.5)
        for _ in range(4):
            await asyncio.sleep(0.25)
            assert await q.heartbeat(pool, c["id"], c["fencing_token"], 0.5)
        assert await q.reap_expired(pool, 0, 0) == 0
    run(body)


def test_retry_then_dead_letter():
    async def body(pool):
        run_id, _ = await make_run(pool, [task("a", max_attempts=2)])
        c1 = await q.claim(pool, "w", 30)
        assert await q.fail(pool, c1["id"], c1["fencing_token"], "boom", 0, 0) == "ready"
        c2 = await q.claim(pool, "w", 30)
        assert c2["attempt"] == 2
        assert await q.fail(pool, c2["id"], c2["fencing_token"], "boom", 0, 0) == "failed"
        assert await q.claim(pool, "w", 30) is None
    run(body)


def test_backoff_delays_the_retry():
    async def body(pool):
        await make_run(pool, [task("a")])
        c = await q.claim(pool, "w", 30)
        await q.fail(pool, c["id"], c["fencing_token"], "boom", 5.0, 60.0)   # 2.5-5s delay
        assert await q.claim(pool, "w", 30) is None
    run(body)


def test_failure_propagates_downstream_and_run_fails():
    async def body(pool):
        run_id, _ = await make_run(pool, [task("a", max_attempts=1), task("b", "a"), task("c", "b")])
        c = await q.claim(pool, "w", 30)
        await q.fail(pool, c["id"], c["fencing_token"], "boom", 0, 0)
        while await q.propagate_failures(pool):
            pass
        await q.finalize_runs(pool)
        assert await state(pool, run_id, "b") == "upstream_failed"
        assert await state(pool, run_id, "c") == "upstream_failed"
        assert await pool.fetchval("SELECT status FROM runs WHERE id = $1", run_id) == "failed"
    run(body)


def test_run_trigger_is_idempotent():
    async def body(pool):
        first, created1 = await make_run(pool, [task("a")], key="req-1")
        second, created2 = await q.create_run(pool, "wf", "req-1")
        assert created1 and not created2 and first == second
        assert await pool.fetchval("SELECT count(*) FROM runs") == 1
        assert await pool.fetchval("SELECT count(*) FROM task_instances") == 1
    run(body)
