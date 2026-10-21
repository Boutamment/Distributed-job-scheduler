"""FastAPI control plane: define workflows, trigger runs, inspect state."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import queue as q
from .config import load
from .dag import DagError, validate
from .db import create_pool, init_schema
from .handlers import HANDLERS

STATIC = Path(__file__).parent / "static"
TERMINAL = ("success", "failed", "upstream_failed")


class TaskSpec(BaseModel):
    key: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    handler: str
    params: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = Field(default=3, ge=1, le=20)


class WorkflowSpec(BaseModel):
    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    tasks: list[TaskSpec] = Field(min_length=1, max_length=500)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = load()
    app.state.cfg = cfg
    app.state.pool = await create_pool(cfg.database_url, max_size=10)
    await init_schema(app.state.pool)
    yield
    await app.state.pool.close()


app = FastAPI(title="JobForge", version="1.0.0", lifespan=lifespan)


@app.get("/", include_in_schema=False)
async def dashboard():
    return FileResponse(STATIC / "index.html")


@app.get("/health")
async def health():
    await app.state.pool.fetchval("SELECT 1")
    return {"status": "ok"}


@app.get("/handlers")
async def list_handlers():
    return sorted(HANDLERS)


# ----------------------------------------------------------------- workflows


@app.put("/workflows", status_code=200)
async def put_workflow(spec: WorkflowSpec):
    """Create or replace a workflow definition. Existing runs are unaffected."""
    unknown = sorted({t.handler for t in spec.tasks} - set(HANDLERS))
    if unknown:
        raise HTTPException(422, f"unknown handler(s): {', '.join(unknown)}")
    tasks = [t.model_dump() for t in spec.tasks]
    try:
        order = validate(tasks)
    except DagError as exc:
        raise HTTPException(422, str(exc))
    await q.upsert_workflow(app.state.pool, spec.name, tasks)
    return {"name": spec.name, "tasks": len(tasks), "order": order}


@app.get("/workflows")
async def get_workflows():
    rows = await app.state.pool.fetch(
        "SELECT name, definition, created_at FROM workflows ORDER BY name"
    )
    return [
        {"name": r["name"], "tasks": r["definition"]["tasks"], "created_at": r["created_at"]}
        for r in rows
    ]


# ---------------------------------------------------------------------- runs


@app.post("/workflows/{name}/runs", status_code=201)
async def trigger_run(
    name: str,
    response: Response,
    idempotency_key: str | None = Header(default=None, max_length=128),
):
    """Start a run. Send an Idempotency-Key header to make retries safe."""
    try:
        run_id, created = await q.create_run(app.state.pool, name, idempotency_key)
    except LookupError:
        raise HTTPException(404, f"workflow '{name}' not found")
    if not created:
        response.status_code = 200  # replay of an earlier request
    return {"run_id": run_id, "created": created}


@app.get("/runs")
async def list_runs(limit: int = 30):
    limit = max(1, min(limit, 200))
    rows = await app.state.pool.fetch(
        """
        SELECT r.id, r.workflow_name, r.status, r.created_at, r.finished_at,
               count(t.*) AS total,
               count(*) FILTER (WHERE t.state = 'success') AS success,
               count(*) FILTER (WHERE t.state IN ('failed', 'upstream_failed')) AS failed,
               count(*) FILTER (WHERE t.state = 'running') AS running
        FROM runs r JOIN task_instances t ON t.run_id = r.id
        GROUP BY r.id ORDER BY r.created_at DESC LIMIT $1
        """,
        limit,
    )
    return [dict(r) for r in rows]


@app.get("/runs/{run_id}")
async def get_run(run_id: str):
    try:
        run = await app.state.pool.fetchrow("SELECT * FROM runs WHERE id = $1::uuid", run_id)
    except Exception:
        raise HTTPException(400, "invalid run id")
    if run is None:
        raise HTTPException(404, "run not found")
    tasks = await app.state.pool.fetch(
        """
        SELECT task_key, handler, depends_on, state, attempt, max_attempts,
               lease_owner, fencing_token, reaped_count, error, result,
               started_at, finished_at
        FROM task_instances WHERE run_id = $1::uuid ORDER BY started_at NULLS LAST, task_key
        """,
        run_id,
    )
    return {**dict(run), "tasks": [dict(t) for t in tasks]}


# --------------------------------------------------------------------- stats


@app.get("/stats")
async def stats():
    pool = app.state.pool
    states = await pool.fetch("SELECT state, count(*) AS n FROM task_instances GROUP BY state")
    runs = await pool.fetch("SELECT status, count(*) AS n FROM runs GROUP BY status")
    workers = await pool.fetch(
        """
        SELECT id, hostname, inflight, extract(epoch FROM now() - last_seen_at) AS age
        FROM workers WHERE last_seen_at > now() - interval '15 seconds' ORDER BY id
        """
    )
    totals = await pool.fetchrow(
        """
        SELECT coalesce(sum(reaped_count), 0) AS reaped,
               coalesce(sum(greatest(attempt - 1, 0)), 0) AS retries
        FROM task_instances
        """
    )
    fx = await pool.fetchrow(
        "SELECT count(*) AS keys, coalesce(sum(executions - 1), 0) AS duplicates FROM effects"
    )
    return {
        "tasks": {r["state"]: r["n"] for r in states},
        "runs": {r["status"]: r["n"] for r in runs},
        "workers": [dict(w) for w in workers],
        "reaped_leases": totals["reaped"],
        "retries": totals["retries"],
        "effects": {"unique": fx["keys"], "duplicate_executions_absorbed": fx["duplicates"]},
    }


@app.post("/demo/seed")
async def seed_demo():
    """Install a few sample workflows so the dashboard has something to run."""
    samples = {
        "etl-diamond": [
            {"key": "extract", "handler": "effect", "params": {"seconds": 1}},
            {"key": "clean", "handler": "effect", "params": {"seconds": 2}, "depends_on": ["extract"]},
            {"key": "validate", "handler": "effect", "params": {"seconds": 1.5}, "depends_on": ["extract"]},
            {"key": "load", "handler": "effect", "params": {"seconds": 1}, "depends_on": ["clean", "validate"]},
        ],
        "flaky-pipeline": [
            {"key": "fetch", "handler": "flaky", "params": {"seconds": 0.5, "fail_probability": 0.6}, "max_attempts": 6},
            {"key": "transform", "handler": "flaky", "params": {"seconds": 0.5, "fail_probability": 0.4}, "depends_on": ["fetch"], "max_attempts": 6},
            {"key": "publish", "handler": "effect", "depends_on": ["transform"]},
        ],
        "doomed": [
            {"key": "prepare", "handler": "effect", "params": {"seconds": 0.5}},
            {"key": "explode", "handler": "fail", "depends_on": ["prepare"], "max_attempts": 2},
            {"key": "never-runs", "handler": "noop", "depends_on": ["explode"]},
        ],
        "long-task": [
            {"key": "slow", "handler": "effect", "params": {"seconds": 20}},
            {"key": "after", "handler": "effect", "depends_on": ["slow"]},
        ],
    }
    for name, tasks in samples.items():
        validate(tasks)
        await q.upsert_workflow(app.state.pool, name, tasks)
    return {"workflows": sorted(samples)}
