ALTER TABLE jobs
    DROP CONSTRAINT ck_jobs_state,
    DROP CONSTRAINT ck_jobs_ownership,
    ADD COLUMN cancelled_at timestamptz,
    ADD CONSTRAINT ck_jobs_state CHECK (
        state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')
    ),
    ADD CONSTRAINT ck_jobs_ownership CHECK (
        (state = 'queued'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND finished_at IS NULL
            AND cancelled_at IS NULL)
        OR
        (state = 'running'
            AND worker_id IS NOT NULL
            AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND finished_at IS NULL
            AND cancelled_at IS NULL)
        OR
        (state = 'succeeded'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND finished_at IS NOT NULL
            AND cancelled_at IS NULL
            AND result IS NOT NULL)
        OR
        (state = 'failed'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND finished_at IS NOT NULL
            AND cancelled_at IS NULL
            AND last_error IS NOT NULL)
        OR
        (state = 'cancelled'
            AND worker_id IS NULL
            AND lease_token IS NULL
            AND lease_expires_at IS NULL
            AND finished_at IS NOT NULL
            AND cancelled_at IS NOT NULL)
    );
