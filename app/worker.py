"""Worker: claims ready tasks, runs them, keeps the lease alive with heartbeats.

If the process is SIGKILLed mid-task nothing special happens here: its heartbeats
stop, the lease expires, and the scheduler hands the task to someone else.
SIGTERM/SIGINT shut down gracefully: stop claiming, finish what is in flight.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import uuid

from . import queue as q
from .config import Config, load
from .db import create_pool, init_schema
from .handlers import HANDLERS, FencedError, TaskContext

log = logging.getLogger("worker")


async def _heartbeat_loop(pool, cfg: Config, task_id, token) -> None:
    """Renew the lease until it is lost. Returns only on loss; cancel it on completion."""
    while True:
        await asyncio.sleep(cfg.heartbeat_seconds)
        try:
            if not await q.heartbeat(pool, task_id, token, cfg.lease_seconds):
                return
        except Exception as exc:  # transient DB error: keep trying until the lease truly expires
            log.warning("heartbeat error: %s", exc)


async def execute(pool, cfg: Config, row) -> None:
    task_id, token = row["id"], row["fencing_token"]
    tag = f"{row['task_key']}#{row['attempt']}"

    handler = HANDLERS.get(row["handler"])
    if handler is None:
        await q.fail(
            pool, task_id, token, f"unknown handler {row['handler']!r}",
            cfg.backoff_base, cfg.backoff_cap,
        )
        return

    ctx = TaskContext(
        pool=pool,
        task_id=task_id,
        run_id=row["run_id"],
        task_key=row["task_key"],
        params=row["params"] or {},
        attempt=row["attempt"],
        fencing_token=token,
    )
    heartbeat = asyncio.create_task(_heartbeat_loop(pool, cfg, task_id, token))
    work = asyncio.create_task(handler(ctx))
    try:
        await asyncio.wait({heartbeat, work}, return_when=asyncio.FIRST_COMPLETED)

        if not work.done():  # the heartbeat loop ended first: we lost the lease
            log.warning("lease lost for %s, abandoning", tag)
            return

        try:
            result = work.result()
        except FencedError as exc:
            log.warning("fenced out: %s", exc)
            return
        except Exception as exc:
            state = await q.fail(
                pool, task_id, token, f"{type(exc).__name__}: {exc}",
                cfg.backoff_base, cfg.backoff_cap,
            )
            log.info("failed %s -> %s (%s)", tag, state, exc)
            return

        if await q.complete(pool, task_id, token, result):
            log.info("done %s", tag)
        else:
            log.warning("result for %s discarded (fenced out)", tag)
    finally:
        heartbeat.cancel()
        work.cancel()


async def _register_loop(pool, worker_id: str, inflight: set) -> None:
    host = socket.gethostname()
    while True:
        try:
            await pool.execute(
                """
                INSERT INTO workers (id, hostname, inflight, last_seen_at)
                VALUES ($1, $2, $3, now())
                ON CONFLICT (id) DO UPDATE SET inflight = $3, last_seen_at = now()
                """,
                worker_id, host, len(inflight),
            )
        except Exception as exc:
            log.warning("worker registration failed: %s", exc)
        await asyncio.sleep(2)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    cfg = load()
    worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:4]}"
    pool = await create_pool(cfg.database_url, max_size=cfg.worker_concurrency * 2 + 4)
    await init_schema(pool)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    inflight: set[asyncio.Task] = set()
    register = asyncio.create_task(_register_loop(pool, worker_id, inflight))
    slots = asyncio.Semaphore(cfg.worker_concurrency)
    log.info("worker %s started (concurrency=%d)", worker_id, cfg.worker_concurrency)

    while not stop.is_set():
        await slots.acquire()
        if stop.is_set():
            slots.release()
            break
        try:
            row = await q.claim(pool, worker_id, cfg.lease_seconds)
        except Exception:
            log.exception("claim failed")
            slots.release()
            await asyncio.sleep(1)
            continue

        if row is None:
            slots.release()
            try:
                await asyncio.wait_for(stop.wait(), timeout=cfg.poll_seconds)
            except asyncio.TimeoutError:
                pass
            continue

        task = asyncio.create_task(execute(pool, cfg, row))
        inflight.add(task)

        def _done(t: asyncio.Task) -> None:
            inflight.discard(t)
            slots.release()

        task.add_done_callback(_done)

    log.info("shutting down, draining %d task(s)", len(inflight))
    if inflight:
        await asyncio.gather(*inflight, return_exceptions=True)
    register.cancel()
    await pool.execute("DELETE FROM workers WHERE id = $1", worker_id)
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
