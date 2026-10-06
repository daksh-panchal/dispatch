package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/daksh-panchal/work-flow/internal/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const jobColumns = `
	id, idempotency_key, submission_hash, queue, priority, payload, state,
	attempt_count, maximum_attempts, available_at, worker_id, lease_token,
	lease_expires_at, created_at, updated_at`

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

// FOR UPDATE SKIP LOCKED is the advisory lock that allows for atomic job claiming by workers
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

func qualifiedJobColumns(table string) string {
	return `
		` + table + `.id, ` + table + `.idempotency_key, ` + table + `.submission_hash,
		` + table + `.queue, ` + table + `.priority, ` + table + `.payload, ` + table + `.state,
		` + table + `.attempt_count, ` + table + `.maximum_attempts, ` + table + `.available_at,
		` + table + `.worker_id, ` + table + `.lease_token, ` + table + `.lease_expires_at,
		` + table + `.created_at, ` + table + `.updated_at`
}

func scanJob(row pgx.Row) (job.Job, error) {
	var stored job.Job
	var id string
	var state string
	var workerID pgtype.UUID
	var leaseToken pgtype.UUID
	var leaseExpiresAt pgtype.Timestamptz

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
	return stored, nil
}
