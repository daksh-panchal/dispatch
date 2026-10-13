ALTER TABLE jobs
    DROP CONSTRAINT ck_jobs_state,
    DROP CONSTRAINT ck_jobs_lease,
    ADD COLUMN result jsonb,
    ADD COLUMN last_error text,
    ADD COLUMN finished_at timestamptz,
    ADD CONSTRAINT ck_jobs_state CHECK (state IN ('queued', 'running', 'succeeded', 'failed')),
    ADD CONSTRAINT ck_jobs_ownership CHECK (
        (state = 'queued'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND finished_at IS NULL)
        OR
        (state = 'running'
            AND worker_id IS NOT NULL
            AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND finished_at IS NULL)
        OR
        (state = 'succeeded'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND finished_at IS NOT NULL
            AND result IS NOT NULL)
        OR
        (state = 'failed'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND finished_at IS NOT NULL
            AND last_error IS NOT NULL)
    ),
    ADD CONSTRAINT ck_jobs_result CHECK (
        (state = 'succeeded' AND result IS NOT NULL)
        OR (state <> 'succeeded' AND result IS NULL)
    );
