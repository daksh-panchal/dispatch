CREATE TABLE workers (
    id uuid PRIMARY KEY,
    name varchar(100) NOT NULL,
    queue varchar(100) NOT NULL,
    last_seen_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

ALTER TABLE jobs
    DROP CONSTRAINT ck_jobs_state,
    ADD COLUMN worker_id uuid REFERENCES workers (id),
    ADD COLUMN lease_token uuid,
    ADD COLUMN lease_expires_at timestamptz,
    ADD CONSTRAINT ck_jobs_state CHECK (state IN ('queued', 'running')),
    ADD CONSTRAINT ck_jobs_lease CHECK (
        (state = 'queued' AND worker_id IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
        OR
        (state = 'running' AND worker_id IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    );

CREATE INDEX ix_jobs_expired_leases
    ON jobs (lease_expires_at)
    WHERE state = 'running';
