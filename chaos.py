#!/usr/bin/env python3
"""Chaos test: SIGKILL workers (and the scheduler) at random while workflows run,
then prove that nothing was lost and nothing was applied twice.

Spawns its own scheduler + worker processes against DATABASE_URL, so it needs only
Postgres:    python chaos.py --runs 30 --duration 40

Asserts, after the dust settles:
  1. every run finished and every task reached 'success'      (nothing lost)
  2. exactly one effect row per effect task                    (nothing double-applied)
  3. fencing tokens of every task are unique                   (no two holders at once)
Also reports how many leases were reaped and how many duplicate executions the
idempotency key absorbed, which is the evidence that the failures really happened.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import signal
import subprocess
import sys
import time

from app import queue as q
from app.config import load
from app.db import create_pool, init_schema

WORKFLOW = [
    {"key": "extract", "handler": "effect", "params": {"seconds": 1.0}},
    {"key": "clean", "handler": "effect", "params": {"seconds": 1.5}, "depends_on": ["extract"]},
    {"key": "validate", "handler": "flaky", "params": {"seconds": 1.0, "fail_probability": 0.3},
     "depends_on": ["extract"], "max_attempts": 10},
    {"key": "load", "handler": "effect", "params": {"seconds": 1.0}, "depends_on": ["clean", "validate"]},
]
EFFECT_TASKS = len(WORKFLOW)  # every task above writes one effect row


def spawn(module: str, env: dict) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", module], env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=30)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--duration", type=float, default=40, help="seconds of active chaos")
    ap.add_argument("--timeout", type=float, default=180, help="max seconds to wait for completion")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    rng = random.Random(args.seed)

    env = {
        **os.environ,
        "LEASE_SECONDS": "3", "HEARTBEAT_SECONDS": "1", "BACKOFF_BASE": "0.3",
        "BACKOFF_CAP": "2", "WORKER_CONCURRENCY": "4",
    }
    cfg = load()
    pool = await create_pool(cfg.database_url)
    await init_schema(pool)
    await pool.execute("TRUNCATE effects, task_instances, runs, workflows, workers CASCADE")
    await q.upsert_workflow(pool, "chaos", WORKFLOW)

    scheduler = spawn("app.scheduler", env)
    workers = [spawn("app.worker", env) for _ in range(args.workers)]
    for _ in range(args.runs):
        await q.create_run(pool, "chaos")
    print(f"started {args.runs} runs × {len(WORKFLOW)} tasks, {args.workers} workers")

    kills = restarts = 0
    t0 = time.monotonic()
    try:
        while time.monotonic() - t0 < args.duration:
            await asyncio.sleep(rng.uniform(1.0, 3.0))
            if rng.random() < 0.15:  # occasionally take out the scheduler too
                scheduler.send_signal(signal.SIGKILL); scheduler.wait()
                scheduler = spawn("app.scheduler", env); kills += 1; restarts += 1
                print(f"  t+{time.monotonic()-t0:4.0f}s  SIGKILL scheduler")
                continue
            i = rng.randrange(len(workers))
            workers[i].send_signal(signal.SIGKILL); workers[i].wait()
            kills += 1
            print(f"  t+{time.monotonic()-t0:4.0f}s  SIGKILL worker {i}")
            await asyncio.sleep(rng.uniform(0.0, 1.5))  # sometimes the pool runs short-handed
            workers[i] = spawn("app.worker", env); restarts += 1

        print("chaos over; waiting for all runs to settle...")
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            open_runs = await pool.fetchval("SELECT count(*) FROM runs WHERE status = 'running'")
            if open_runs == 0:
                break
            await asyncio.sleep(1)

        # ------------------------------------------------------------ verdicts
        problems: list[str] = []
        runs = await pool.fetch("SELECT status, count(*) AS n FROM runs GROUP BY status")
        status = {r["status"]: r["n"] for r in runs}
        if status.get("success", 0) != args.runs:
            problems.append(f"runs not all successful: {status}")

        bad = await pool.fetchval("SELECT count(*) FROM task_instances WHERE state <> 'success'")
        if bad:
            problems.append(f"{bad} task(s) not in 'success'")

        effects = await pool.fetchval("SELECT count(*) FROM effects")
        expected = args.runs * EFFECT_TASKS
        if effects != expected:
            problems.append(f"effects: expected {expected}, found {effects}")

        dup_tokens = await pool.fetchval(
            "SELECT count(*) FROM (SELECT fencing_token FROM task_instances "
            "GROUP BY fencing_token HAVING count(*) > 1) x"
        )
        if dup_tokens:
            problems.append(f"{dup_tokens} duplicated fencing token(s)")

        reaped = await pool.fetchval("SELECT coalesce(sum(reaped_count), 0) FROM task_instances")
        extra = await pool.fetchval("SELECT coalesce(sum(executions - 1), 0) FROM effects")
        retries = await pool.fetchval("SELECT coalesce(sum(greatest(attempt - 1, 0)), 0) FROM task_instances")

        print("\n=== chaos report ===")
        print(f"process kills        : {kills}")
        print(f"leases reaped        : {reaped}   (tasks recovered after a worker died)")
        print(f"retries              : {retries}")
        print(f"duplicate executions : {extra}   (ran twice, applied once)")
        print(f"runs successful      : {status.get('success', 0)}/{args.runs}")
        print(f"effects applied      : {effects}/{expected}")
        if problems:
            print("\nFAIL")
            for p in problems:
                print(" -", p)
            return 1
        print("\nPASS: no task lost, no effect applied twice")
        return 0
    finally:
        for p in [scheduler, *workers]:
            if p.poll() is None:
                p.terminate()
        for p in [scheduler, *workers]:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        await pool.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
