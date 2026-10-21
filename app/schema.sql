-- Global, strictly increasing counter used for fencing tokens.
CREATE SEQUENCE IF NOT EXISTS fencing_seq;

CREATE TABLE IF NOT EXISTS workflows (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text UNIQUE NOT NULL,
    definition  jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runs (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id     uuid NOT NULL REFERENCES workflows(id),
    workflow_name   text NOT NULL,
    status          text NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'success', 'failed')),
    -- Lets a client retry "create run" safely without starting it twice.
    idempotency_key text UNIQUE,
    created_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz
);

-- One row per task per run. This table *is* the queue and the state machine:
--   pending -> ready -> running -> success
--                 ^         |-----> ready   (failure with attempts left, or lease expired)
--                 |         '-----> failed  (attempts exhausted)
--   pending -> upstream_failed              (a dependency failed)
CREATE TABLE IF NOT EXISTS task_instances (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id           uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    task_key         text NOT NULL,
    handler          text NOT NULL,
    params           jsonb NOT NULL DEFAULT '{}',
    depends_on       text[] NOT NULL DEFAULT '{}',
    state            text NOT NULL
                     CHECK (state IN ('pending', 'ready', 'running', 'success',
                                      'failed', 'upstream_failed')),
    attempt          int NOT NULL DEFAULT 0,
    max_attempts     int NOT NULL DEFAULT 3,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    lease_owner      text,
    lease_expires_at timestamptz,
    -- Bumped on every claim and every lease reap. Only the holder of the
    -- current token may heartbeat, complete or fail the task.
    fencing_token    bigint NOT NULL DEFAULT 0,
    reaped_count     int NOT NULL DEFAULT 0,
    result           jsonb,
    error            text,
    started_at       timestamptz,
    finished_at      timestamptz,
    UNIQUE (run_id, task_key)
);

CREATE INDEX IF NOT EXISTS task_ready_idx
    ON task_instances (next_attempt_at) WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS task_running_idx
    ON task_instances (lease_expires_at) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS task_run_idx ON task_instances (run_id);

-- Registered workers, for the dashboard only (correctness never reads this).
CREATE TABLE IF NOT EXISTS workers (
    id            text PRIMARY KEY,
    hostname      text NOT NULL,
    inflight      int NOT NULL DEFAULT 0,
    last_seen_at  timestamptz NOT NULL DEFAULT now()
);

-- Demo side effects. The primary key on idempotency_key is what turns
-- at-least-once execution into effectively-once outcomes.
CREATE TABLE IF NOT EXISTS effects (
    idempotency_key text PRIMARY KEY,
    run_id          uuid NOT NULL,
    task_key        text NOT NULL,
    executions      int NOT NULL DEFAULT 1,
    first_at        timestamptz NOT NULL DEFAULT now(),
    last_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS effects_run_idx ON effects (run_id);
