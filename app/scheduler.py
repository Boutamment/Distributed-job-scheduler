"""Scheduler: turns task states forward. Stateless; run as many as you like.

Each tick, inside ONE transaction guarded by a Postgres advisory lock:
  1. reap tasks whose lease expired   (crash recovery)
  2. mark dependants of failed tasks  (failure propagation)
  3. promote pending -> ready         (dependency resolution)
  4. close finished runs

The advisory lock makes extra scheduler replicas hot standbys: if the active one
dies, its transaction (and lock) disappear and another takes over on its next tick.
No leader election code, no state kept outside Postgres.
"""
from __future__ import annotations

import asyncio
import logging
import signal

from . import queue as q
from .config import load
from .db import create_pool, init_schema

log = logging.getLogger("scheduler")

SCHEDULER_LOCK_ID = 7001


async def tick(pool, backoff_base: float, backoff_cap: float) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            if not await conn.fetchval(
                "SELECT pg_try_advisory_xact_lock($1)", SCHEDULER_LOCK_ID
            ):
                return {"leader": False}
            reaped = await q.reap_expired(conn, backoff_base, backoff_cap)
            blocked = 0
            while True:  # cascade through the whole chain of dependants
                n = await q.propagate_failures(conn)
                blocked += n
                if n == 0:
                    break
            promoted = await q.promote_ready(conn)
            finished = await q.finalize_runs(conn)
    return {
        "leader": True,
        "reaped": reaped,
        "blocked": blocked,
        "promoted": promoted,
        "finished_runs": finished,
    }


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    cfg = load()
    pool = await create_pool(cfg.database_url, max_size=3)
    await init_schema(pool)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    log.info("scheduler started (interval=%.2fs)", cfg.scheduler_interval)
    while not stop.is_set():
        try:
            stats = await tick(pool, cfg.backoff_base, cfg.backoff_cap)
            if stats.get("reaped"):
                log.warning("reaped %d expired lease(s)", stats["reaped"])
            if stats.get("finished_runs"):
                log.info("finished %d run(s)", stats["finished_runs"])
        except Exception:  # a DB blip must not kill the scheduler
            log.exception("tick failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.scheduler_interval)
        except asyncio.TimeoutError:
            pass
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
