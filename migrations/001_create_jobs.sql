CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    idempotency_key varchar(200) NOT NULL,
    submission_hash char(64) NOT NULL,
    queue varchar(100) NOT NULL,
    priority integer NOT NULL,
    payload jsonb NOT NULL,
    state varchar(32) NOT NULL,
    attempt_count integer NOT NULL,
    maximum_attempts integer NOT NULL,
    available_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,

    CONSTRAINT uq_jobs_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_jobs_state CHECK (state IN ('queued')),
    CONSTRAINT ck_jobs_priority CHECK (priority BETWEEN -1000 AND 1000),
    CONSTRAINT ck_jobs_attempt_count CHECK (attempt_count >= 0),
    CONSTRAINT ck_jobs_maximum_attempts CHECK (
        maximum_attempts BETWEEN 1 AND 100
        AND attempt_count <= maximum_attempts
    )
);

CREATE INDEX ix_jobs_queue_selection
    ON jobs (queue, state, priority DESC, available_at, created_at);
