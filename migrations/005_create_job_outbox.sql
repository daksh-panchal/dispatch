CREATE TABLE job_outbox_events (
    id uuid PRIMARY KEY,
    sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    event_type varchar(100) NOT NULL,
    schema_version integer NOT NULL,
    job_id uuid NOT NULL REFERENCES jobs (id),
    occurred_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    published_at timestamptz,
    publish_attempts integer NOT NULL DEFAULT 0,
    last_publish_attempt_at timestamptz,
    next_attempt_at timestamptz NOT NULL,
    claim_token uuid,
    claim_expires_at timestamptz,
    last_publish_error text,
    CONSTRAINT ck_job_outbox_event_type CHECK (
        event_type IN (
            'JobSubmitted',
            'JobClaimed',
            'JobRetryScheduled',
            'JobSucceeded',
            'JobFailed',
            'JobRecovered',
            'JobCancelled'
        )
    ),
    CONSTRAINT ck_job_outbox_schema_version CHECK (schema_version > 0),
    CONSTRAINT ck_job_outbox_publish_attempts CHECK (publish_attempts >= 0),
    CONSTRAINT ck_job_outbox_claim CHECK (
        (claim_token IS NULL AND claim_expires_at IS NULL)
        OR (claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)
    )
);

CREATE INDEX ix_job_outbox_pending
    ON job_outbox_events (next_attempt_at, sequence)
    WHERE published_at IS NULL;
