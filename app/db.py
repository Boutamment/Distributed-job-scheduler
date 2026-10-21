"""Connection pool and schema bootstrap."""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import asyncpg

log = logging.getLogger("db")

SCHEMA = (Path(__file__).with_name("schema.sql")).read_text()
SCHEMA_LOCK_ID = 7000


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Let jsonb columns round-trip as Python objects instead of strings.
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


async def create_pool(
    url: str, min_size: int = 1, max_size: int = 10, retries: int = 30
) -> asyncpg.Pool:
    """Create a pool, retrying while the database is still starting up."""
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return await asyncpg.create_pool(
                url, min_size=min_size, max_size=max_size, init=_init_connection
            )
        except (OSError, asyncpg.PostgresError) as exc:
            last = exc
            log.warning("database not ready (%s), attempt %d/%d", exc, attempt, retries)
            await asyncio.sleep(1)
    raise RuntimeError(f"could not connect to database: {last}")


async def init_schema(pool: asyncpg.Pool) -> None:
    """Apply schema.sql. Idempotent; an advisory lock serialises concurrent starters."""
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1)", SCHEMA_LOCK_ID)
        try:
            await conn.execute(SCHEMA)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", SCHEMA_LOCK_ID)
