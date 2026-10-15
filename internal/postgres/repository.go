package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/daksh-panchal/work-flow/internal/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const jobColumns = `
	id, idempotency_key, submission_hash, queue, priority, payload, state,
	attempt_count, maximum_attempts, available_at, worker_id, lease_token,
	lease_expires_at, result, last_error, finished_at, cancelled_at, created_at, updated_at`

type JobRepository struct {
	pool *pgxpool.Pool
}

func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

func (repository *JobRepository) CreateOrGet(
	ctx context.Context,
	candidate job.Job,
) (job.Job, bool, error) {
	row := repository.pool.QueryRow(ctx, `
		INSERT INTO jobs (
			id, idempotency_key, submission_hash, queue, priority, payload, state,
			attempt_count, maximum_attempts, available_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+jobColumns,
		candidate.ID.String(),
		candidate.IdempotencyKey,
		candidate.SubmissionHash,
		candidate.Queue,
		candidate.Priority,
		[]byte(candidate.Payload),
		candidate.State,
		candidate.AttemptCount,
		candidate.MaximumAttempts,
		candidate.AvailableAt,
		candidate.CreatedAt,
		candidate.UpdatedAt,
	)

	stored, err := scanJob(row)
	if err == nil { // Job was created successfully
		return stored, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) { // If error is not "no rows", return the error
		return job.Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	// Job with the same idempotency key already exists, retrieve it
	stored, err = scanJob(repository.pool.QueryRow(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE idempotency_key = $1`,
		candidate.IdempotencyKey,
	))
	if err != nil {
		return job.Job{}, false, fmt.Errorf("read idempotent job: %w", err)
	}
	return stored, false, nil // Return the existing job and indicate it was not created
}

func (repository *JobRepository) Get(ctx context.Context, id uuid.UUID) (job.Job, error) {
	stored, err := scanJob(repository.pool.QueryRow(ctx, `
		SELECT `+jobColumns+`
		FROM jobs
		WHERE id = $1`,
		id.String(),
	))
	if errors.Is(err, pgx.ErrNoRows) { // Job not found in the db
		return job.Job{}, job.ErrNotFound
	}
	if err != nil { // Other errors
		return job.Job{}, fmt.Errorf("get job: %w", err)
	}
	return stored, nil // Return the found job and nil error
}

// FOR UPDATE SKIP LOCKED uses row-level locks to prevent workers from claiming the same job.
func (repository *JobRepository) Claim(
	ctx context.Context,
	params job.ClaimParams,
) (job.Job, bool, error) {
	stored, err := scanJob(repository.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id
			FROM jobs
			WHERE queue = $1
			  AND state = 'queued'
			  AND available_at <= $2
			  AND attempt_count < maximum_attempts
			ORDER BY priority DESC, available_at, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs
		SET state = 'running',
			worker_id = $3,
			lease_token = $4,
			lease_expires_at = $5,
			attempt_count = attempt_count + 1,
			last_error = NULL,
			updated_at = $2
		FROM candidate
		WHERE jobs.id = candidate.id
		RETURNING `+qualifiedJobColumns("jobs"),
		params.Queue,
		params.Now,
		params.WorkerID.String(),
		params.LeaseToken.String(),
		params.LeaseExpiresAt,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, nil
	}
	if err != nil {
		return job.Job{}, false, fmt.Errorf("claim job: %w", err)
	}
	return stored, true, nil
}

func (repository *JobRepository) RenewLease(
	ctx context.Context,
	params job.RenewLeaseParams,
) (job.Job, error) {
	stored, err := scanJob(repository.pool.QueryRow(ctx, `
		UPDATE jobs
		SET lease_expires_at = $1,
			updated_at = $2
		WHERE id = $3
		  AND state = 'running'
		  AND worker_id = $4
		  AND lease_token = $5
		  AND lease_expires_at > $2
		RETURNING `+jobColumns,
		params.LeaseExpiresAt,
		params.Now,
		params.JobID.String(),
		params.WorkerID.String(),
		params.LeaseToken.String(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrLeaseLost
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("renew job lease: %w", err)
	}
	return stored, nil
}

func (repository *JobRepository) Complete(
	ctx context.Context,
	params job.CompleteParams,
) (job.Job, error) {
	stored, err := scanJob(repository.pool.QueryRow(ctx, `
		UPDATE jobs
		SET state = 'succeeded',
			worker_id = NULL,
			lease_token = NULL,
			lease_expires_at = NULL,
			result = $1,
			last_error = NULL,
			finished_at = $2,
			updated_at = $2
		WHERE id = $3
		  AND state = 'running'
		  AND worker_id = $4
		  AND lease_token = $5
		  AND lease_expires_at > $2
		RETURNING `+jobColumns,
		[]byte(params.Result),
		params.Now,
		params.JobID.String(),
		params.WorkerID.String(),
		params.LeaseToken.String(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrLeaseLost
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("complete job: %w", err)
	}
	return stored, nil
}

func (repository *JobRepository) Fail(
	ctx context.Context,
	params job.FailParams,
) (job.Job, error) {
	stored, err := scanJob(repository.pool.QueryRow(ctx, `
		UPDATE jobs
		SET state = CASE
				WHEN attempt_count < maximum_attempts THEN 'queued'
				ELSE 'failed'
			END,
			worker_id = NULL,
			lease_token = NULL,
			lease_expires_at = NULL,
			result = NULL,
			last_error = $1,
			available_at = CASE
				WHEN attempt_count < maximum_attempts THEN $2
				ELSE available_at
			END,
			finished_at = CASE
				WHEN attempt_count < maximum_attempts THEN NULL
				ELSE $3
			END,
			updated_at = $3
		WHERE id = $4
		  AND state = 'running'
		  AND worker_id = $5
		  AND lease_token = $6
		  AND lease_expires_at > $3
		RETURNING `+jobColumns,
		params.Reason,
		params.RetryAt,
		params.Now,
		params.JobID.String(),
		params.WorkerID.String(),
		params.LeaseToken.String(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrLeaseLost
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("fail job: %w", err)
	}
	return stored, nil
}

func (repository *JobRepository) Cancel(
	ctx context.Context,
	params job.CancelParams,
) (job.Job, error) {
	stored, err := scanJob(repository.pool.QueryRow(ctx, `
		UPDATE jobs
		SET state = 'cancelled',
			worker_id = NULL,
			lease_token = NULL,
			lease_expires_at = NULL,
			result = NULL,
			finished_at = $1,
			cancelled_at = $1,
			updated_at = $1
		WHERE id = $2
		  AND state IN ('queued', 'running')
		RETURNING `+jobColumns,
		params.Now,
		params.JobID.String(),
	))
	if err == nil {
		return stored, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, fmt.Errorf("cancel job: %w", err)
	}

	stored, err = repository.Get(ctx, params.JobID)
	if err != nil {
		return job.Job{}, err
	}
	if stored.State == job.StateCancelled {
		return stored, nil
	}
	return job.Job{}, job.ErrCancellationConflict
}

func (repository *JobRepository) RecoverExpired(
	ctx context.Context,
	now time.Time,
	retryBase time.Duration,
	retryMaximum time.Duration,
	batchSize int,
) (job.RecoveryResult, error) {
	var result job.RecoveryResult
	err := repository.pool.QueryRow(ctx, `
		WITH expired AS (
			SELECT id
			FROM jobs
			WHERE state = 'running'
			  AND lease_expires_at <= $1
			ORDER BY lease_expires_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $4
		), recovered AS (
			UPDATE jobs
			SET state = CASE
					WHEN jobs.attempt_count < jobs.maximum_attempts THEN 'queued'
					ELSE 'failed'
				END,
				worker_id = NULL,
				lease_token = NULL,
				lease_expires_at = NULL,
				result = NULL,
				last_error = 'worker lease expired',
				available_at = CASE
					WHEN jobs.attempt_count < jobs.maximum_attempts THEN $1 + (
						LEAST(
							$2::bigint * (1::bigint << LEAST(GREATEST(jobs.attempt_count - 1, 0), 20)),
							$3::bigint
						) * interval '1 millisecond'
					)
					ELSE jobs.available_at
				END,
				finished_at = CASE
					WHEN jobs.attempt_count < jobs.maximum_attempts THEN NULL
					ELSE $1
				END,
				updated_at = $1
			FROM expired
			WHERE jobs.id = expired.id
			RETURNING jobs.state
		)
		SELECT
			COUNT(*) FILTER (WHERE state = 'queued'),
			COUNT(*) FILTER (WHERE state = 'failed')
		FROM recovered`,
		now,
		retryBase.Milliseconds(),
		retryMaximum.Milliseconds(),
		batchSize,
	).Scan(&result.Requeued, &result.Failed)
	if err != nil {
		return job.RecoveryResult{}, fmt.Errorf("recover expired jobs: %w", err)
	}
	return result, nil
}

func qualifiedJobColumns(table string) string {
	return `
		` + table + `.id, ` + table + `.idempotency_key, ` + table + `.submission_hash,
		` + table + `.queue, ` + table + `.priority, ` + table + `.payload, ` + table + `.state,
		` + table + `.attempt_count, ` + table + `.maximum_attempts, ` + table + `.available_at,
		` + table + `.worker_id, ` + table + `.lease_token, ` + table + `.lease_expires_at,
		` + table + `.result, ` + table + `.last_error, ` + table + `.finished_at,
		` + table + `.cancelled_at,
		` + table + `.created_at, ` + table + `.updated_at`
}

func scanJob(row pgx.Row) (job.Job, error) {
	var stored job.Job
	var id string
	var state string
	var workerID pgtype.UUID
	var leaseToken pgtype.UUID
	var leaseExpiresAt pgtype.Timestamptz
	var result []byte
	var lastError pgtype.Text
	var finishedAt pgtype.Timestamptz
	var cancelledAt pgtype.Timestamptz

	err := row.Scan(
		&id,
		&stored.IdempotencyKey,
		&stored.SubmissionHash,
		&stored.Queue,
		&stored.Priority,
		&stored.Payload,
		&state,
		&stored.AttemptCount,
		&stored.MaximumAttempts,
		&stored.AvailableAt,
		&workerID,
		&leaseToken,
		&leaseExpiresAt,
		&result,
		&lastError,
		&finishedAt,
		&cancelledAt,
		&stored.CreatedAt,
		&stored.UpdatedAt,
	)
	if err != nil {
		return job.Job{}, err
	}

	stored.ID, err = uuid.Parse(id)
	if err != nil {
		return job.Job{}, fmt.Errorf("parse stored job ID: %w", err)
	}
	stored.State = job.State(state)
	if workerID.Valid {
		value := uuid.UUID(workerID.Bytes)
		stored.WorkerID = &value
	}
	if leaseToken.Valid {
		value := uuid.UUID(leaseToken.Bytes)
		stored.LeaseToken = &value
	}
	if leaseExpiresAt.Valid {
		value := leaseExpiresAt.Time
		stored.LeaseExpiresAt = &value
	}
	if result != nil {
		stored.Result = append([]byte(nil), result...)
	}
	if lastError.Valid {
		value := lastError.String
		stored.LastError = &value
	}
	if finishedAt.Valid {
		value := finishedAt.Time
		stored.FinishedAt = &value
	}
	if cancelledAt.Valid {
		value := cancelledAt.Time
		stored.CancelledAt = &value
	}
	return stored, nil
}
