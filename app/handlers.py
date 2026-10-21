"""Task handlers: the code a worker actually runs for a task.

A handler is `async def handler(ctx) -> JSON-serialisable result`. Raise to fail.
Handlers must be idempotent: the system guarantees at-least-once execution, so
the same task can run twice (slow worker, lost lease). `record_effect` shows the
pattern: an idempotent write keyed by ctx.idempotency_key and fenced by the token.
"""
from __future__ import annotations

import asyncio
import random
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


class FencedError(Exception):
    """This execution lost its lease; its side effects must not be applied."""


@dataclass
class TaskContext:
    pool: Any
    task_id: uuid.UUID
    run_id: uuid.UUID
    task_key: str
    params: dict
    attempt: int
    fencing_token: int

    @property
    def idempotency_key(self) -> str:
        # Stable across retries and re-claims of the same logical task.
        return f"{self.run_id}:{self.task_key}"


async def record_effect(ctx: TaskContext) -> None:
    """Idempotent, fenced side effect.

    One row per idempotency key no matter how many times the task executes
    (`executions` counts the extra runs). The EXISTS clause rejects the write if
    this execution no longer holds the current fencing token.
    """
    result = await ctx.pool.execute(
        """
        INSERT INTO effects (idempotency_key, run_id, task_key, executions)
        SELECT $1::text, $2::uuid, $3::text, 1
        WHERE EXISTS (
            SELECT 1 FROM task_instances
            WHERE id = $4::uuid AND fencing_token = $5::bigint AND state = 'running')
        ON CONFLICT (idempotency_key)
        DO UPDATE SET executions = effects.executions + 1, last_at = now()
        """,
        ctx.idempotency_key,
        ctx.run_id,
        ctx.task_key,
        ctx.task_id,
        ctx.fencing_token,
    )
    if result == "INSERT 0 0":
        raise FencedError(f"{ctx.task_key}: fencing token {ctx.fencing_token} is stale")


async def noop(ctx: TaskContext) -> dict:
    return {"ok": True}


async def sleep(ctx: TaskContext) -> dict:
    seconds = float(ctx.params.get("seconds", 1))
    await asyncio.sleep(seconds)
    return {"slept": seconds}


async def effect(ctx: TaskContext) -> dict:
    """Sleep a bit, then perform the idempotent side effect."""
    await asyncio.sleep(float(ctx.params.get("seconds", 0.5)))
    await record_effect(ctx)
    return {"effect": ctx.idempotency_key}


async def flaky(ctx: TaskContext) -> dict:
    """Like `effect`, but fails randomly. Used to exercise retries and backoff."""
    await asyncio.sleep(float(ctx.params.get("seconds", 0.5)))
    if random.random() < float(ctx.params.get("fail_probability", 0.5)):
        raise RuntimeError("simulated transient failure")
    await record_effect(ctx)
    return {"effect": ctx.idempotency_key}


async def always_fail(ctx: TaskContext) -> dict:
    await asyncio.sleep(float(ctx.params.get("seconds", 0.3)))
    raise RuntimeError("simulated permanent failure")


Handler = Callable[[TaskContext], Awaitable[dict]]

HANDLERS: dict[str, Handler] = {
    "noop": noop,
    "sleep": sleep,
    "effect": effect,
    "flaky": flaky,
    "fail": always_fail,
}
