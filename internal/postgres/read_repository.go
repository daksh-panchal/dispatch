package postgres

import (
	"context"
	"fmt"

	"github.com/daksh-panchal/dispatch/internal/job"
	"github.com/daksh-panchal/dispatch/internal/readmodel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ReadRepository struct {
	pool *pgxpool.Pool
}

func NewReadRepository(pool *pgxpool.Pool) *ReadRepository {
	return &ReadRepository{pool: pool}
}

func (repository *ReadRepository) ListJobs(
	ctx context.Context,
	params readmodel.JobListParams,
) (readmodel.JobPage, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs WHERE TRUE`
	arguments := make([]any, 0, 4)
	if params.Queue != "" {
		arguments = append(arguments, params.Queue)
		query += fmt.Sprintf(" AND queue = $%d", len(arguments))
	}
	if params.State != "" {
		arguments = append(arguments, params.State)
		query += fmt.Sprintf(" AND state = $%d", len(arguments))
	}
	if params.Cursor != nil {
		arguments = append(arguments, *params.Cursor)
		position := len(arguments)
		query += fmt.Sprintf(
			" AND (created_at, id) < (SELECT created_at, id FROM jobs WHERE id = $%d)",
			position,
		)
	}
	arguments = append(arguments, params.Limit+1)
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(arguments))

	rows, err := repository.pool.Query(ctx, query, arguments...)
	if err != nil {
		return readmodel.JobPage{}, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	items := make([]job.Job, 0, params.Limit+1)
	for rows.Next() {
		stored, err := scanJob(rows)
		if err != nil {
			return readmodel.JobPage{}, fmt.Errorf("scan listed job: %w", err)
		}
		items = append(items, stored)
	}
	if err := rows.Err(); err != nil {
		return readmodel.JobPage{}, fmt.Errorf("read listed jobs: %w", err)
	}

	var next *uuid.UUID
	if len(items) > params.Limit {
		items = items[:params.Limit]
		cursor := items[len(items)-1].ID
		next = &cursor
	}
	return readmodel.JobPage{Items: items, NextCursor: next}, nil
}

func (repository *ReadRepository) ListWorkers(
	ctx context.Context,
	params readmodel.WorkerListParams,
) (readmodel.StoredWorkerPage, error) {
	query := `
		SELECT workers.id, workers.name, workers.queue, workers.last_seen_at,
			workers.created_at, workers.updated_at,
			active_job.id, active_job.lease_expires_at
		FROM workers
		LEFT JOIN LATERAL (
			SELECT id, lease_expires_at
			FROM jobs
			WHERE worker_id = workers.id AND state = 'running'
			ORDER BY lease_expires_at, id
			LIMIT 1
		) AS active_job ON TRUE
		WHERE TRUE`
	arguments := make([]any, 0, 3)
	if params.Queue != "" {
		arguments = append(arguments, params.Queue)
		query += fmt.Sprintf(" AND workers.queue = $%d", len(arguments))
	}
	if params.Cursor != nil {
		arguments = append(arguments, *params.Cursor)
		query += fmt.Sprintf(
			" AND (workers.created_at, workers.id) < (SELECT created_at, id FROM workers WHERE id = $%d)",
			len(arguments),
		)
	}
	arguments = append(arguments, params.Limit+1)
	query += fmt.Sprintf(" ORDER BY workers.created_at DESC, workers.id DESC LIMIT $%d", len(arguments))

	rows, err := repository.pool.Query(ctx, query, arguments...)
	if err != nil {
		return readmodel.StoredWorkerPage{}, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()

	items := make([]readmodel.StoredWorker, 0, params.Limit+1)
	for rows.Next() {
		var item readmodel.StoredWorker
		var workerID string
		var currentJobID pgtype.UUID
		var leaseExpiresAt pgtype.Timestamptz
		if err := rows.Scan(
			&workerID,
			&item.Worker.Name,
			&item.Worker.Queue,
			&item.Worker.LastSeenAt,
			&item.Worker.CreatedAt,
			&item.Worker.UpdatedAt,
			&currentJobID,
			&leaseExpiresAt,
		); err != nil {
			return readmodel.StoredWorkerPage{}, fmt.Errorf("scan listed worker: %w", err)
		}
		parsedWorkerID, err := uuid.Parse(workerID)
		if err != nil {
			return readmodel.StoredWorkerPage{}, fmt.Errorf("parse listed worker ID: %w", err)
		}
		item.Worker.ID = parsedWorkerID
		if currentJobID.Valid {
			value := uuid.UUID(currentJobID.Bytes)
			item.CurrentJobID = &value
		}
		if leaseExpiresAt.Valid {
			value := leaseExpiresAt.Time
			item.LeaseExpiresAt = &value
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return readmodel.StoredWorkerPage{}, fmt.Errorf("read listed workers: %w", err)
	}

	var next *uuid.UUID
	if len(items) > params.Limit {
		items = items[:params.Limit]
		cursor := items[len(items)-1].Worker.ID
		next = &cursor
	}
	return readmodel.StoredWorkerPage{Items: items, NextCursor: next}, nil
}

func (repository *ReadRepository) ListEvents(
	ctx context.Context,
	params readmodel.EventListParams,
) (readmodel.EventPage, error) {
	query := `
		SELECT events.sequence, events.id, events.event_type, events.schema_version,
			events.job_id, events.occurred_at, events.payload
		FROM job_outbox_events AS events
		JOIN jobs ON jobs.id = events.job_id
		WHERE events.sequence > $1`
	arguments := []any{params.AfterSequence}
	if params.Queue != "" {
		arguments = append(arguments, params.Queue)
		query += fmt.Sprintf(" AND jobs.queue = $%d", len(arguments))
	}
	if params.JobID != nil {
		arguments = append(arguments, *params.JobID)
		query += fmt.Sprintf(" AND events.job_id = $%d", len(arguments))
	}
	arguments = append(arguments, params.Limit+1)
	query += fmt.Sprintf(" ORDER BY events.sequence LIMIT $%d", len(arguments))

	rows, err := repository.pool.Query(ctx, query, arguments...)
	if err != nil {
		return readmodel.EventPage{}, fmt.Errorf("list job events: %w", err)
	}
	defer rows.Close()

	items := make([]readmodel.EventRecord, 0, params.Limit+1)
	for rows.Next() {
		var event readmodel.EventRecord
		if err := rows.Scan(
			&event.Sequence,
			&event.ID,
			&event.Type,
			&event.SchemaVersion,
			&event.JobID,
			&event.OccurredAt,
			&event.Payload,
		); err != nil {
			return readmodel.EventPage{}, fmt.Errorf("scan listed job event: %w", err)
		}
		event.Payload = append(event.Payload[:0:0], event.Payload...)
		items = append(items, event)
	}
	if err := rows.Err(); err != nil {
		return readmodel.EventPage{}, fmt.Errorf("read listed job events: %w", err)
	}

	var next *int64
	if len(items) > params.Limit {
		items = items[:params.Limit]
		sequence := items[len(items)-1].Sequence
		next = &sequence
	}
	return readmodel.EventPage{Items: items, NextSequence: next}, nil
}
