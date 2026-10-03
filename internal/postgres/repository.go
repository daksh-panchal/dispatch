package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/daksh-panchal/work-flow/internal/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const jobColumns = `
	id, idempotency_key, submission_hash, queue, priority, payload, state,
	attempt_count, maximum_attempts, available_at, created_at, updated_at`

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

func scanJob(row pgx.Row) (job.Job, error) {
	var stored job.Job
	var id string
	var state string

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
	return stored, nil
}
