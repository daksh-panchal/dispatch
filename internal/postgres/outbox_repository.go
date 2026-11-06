package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/daksh-panchal/dispatch/internal/job"
	"github.com/daksh-panchal/dispatch/internal/outbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (repository *OutboxRepository) ClaimBatch(
	ctx context.Context,
	params outbox.ClaimParams,
) ([]outbox.Event, error) {
	rows, err := repository.pool.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM job_outbox_events
			WHERE published_at IS NULL
			  AND next_attempt_at <= $1
			  AND (claim_expires_at IS NULL OR claim_expires_at <= $1)
			ORDER BY sequence
			FOR UPDATE SKIP LOCKED
			LIMIT $4
		)
		UPDATE job_outbox_events AS events
		SET claim_token = $2,
			claim_expires_at = $3
		FROM candidates
		WHERE events.id = candidates.id
		RETURNING events.id, events.event_type, events.schema_version,
			events.job_id, events.occurred_at, events.payload, events.publish_attempts`,
		params.Now,
		params.Token,
		params.ExpiresAt,
		params.BatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]outbox.Event, 0, params.BatchSize)
	for rows.Next() {
		var event outbox.Event
		if err := rows.Scan(
			&event.ID,
			&event.Type,
			&event.SchemaVersion,
			&event.JobID,
			&event.OccurredAt,
			&event.Payload,
			&event.PublishAttempts,
		); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		event.Payload = append([]byte(nil), event.Payload...)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read outbox events: %w", err)
	}
	return events, nil
}

func (repository *OutboxRepository) MarkPublished(
	ctx context.Context,
	eventID uuid.UUID,
	claimToken uuid.UUID,
	publishedAt time.Time,
) error {
	command, err := repository.pool.Exec(ctx, `
		UPDATE job_outbox_events
		SET published_at = $1,
			publish_attempts = publish_attempts + 1,
			last_publish_attempt_at = $1,
			claim_token = NULL,
			claim_expires_at = NULL,
			last_publish_error = NULL
		WHERE id = $2
		  AND claim_token = $3
		  AND published_at IS NULL`,
		publishedAt,
		eventID,
		claimToken,
	)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	if command.RowsAffected() == 0 {
		return outbox.ErrClaimLost
	}
	return nil
}

func (repository *OutboxRepository) MarkFailed(
	ctx context.Context,
	failure outbox.PublishFailure,
) error {
	command, err := repository.pool.Exec(ctx, `
		UPDATE job_outbox_events
		SET publish_attempts = publish_attempts + 1,
			last_publish_attempt_at = $1,
			next_attempt_at = $2,
			claim_token = NULL,
			claim_expires_at = NULL,
			last_publish_error = $3
		WHERE id = $4
		  AND claim_token = $5
		  AND published_at IS NULL`,
		failure.FailedAt,
		failure.NextAttemptAt,
		failure.Error,
		failure.EventID,
		failure.ClaimToken,
	)
	if err != nil {
		return fmt.Errorf("record outbox publish failure: %w", err)
	}
	if command.RowsAffected() == 0 {
		return outbox.ErrClaimLost
	}
	return nil
}

func insertJobEvent(
	ctx context.Context,
	tx pgx.Tx,
	eventType outbox.EventType,
	stored job.Job,
) error {
	eventID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate outbox event ID: %w", err)
	}
	payload, err := json.Marshal(struct {
		State           job.State  `json:"state"`
		Queue           string     `json:"queue"`
		AttemptCount    int        `json:"attemptCount"`
		MaximumAttempts int        `json:"maximumAttempts"`
		AvailableAt     time.Time  `json:"availableAt"`
		WorkerID        *uuid.UUID `json:"workerId,omitempty"`
		LeaseExpiresAt  *time.Time `json:"leaseExpiresAt,omitempty"`
		LastError       *string    `json:"lastError,omitempty"`
		FinishedAt      *time.Time `json:"finishedAt,omitempty"`
		CancelledAt     *time.Time `json:"cancelledAt,omitempty"`
	}{
		State:           stored.State,
		Queue:           stored.Queue,
		AttemptCount:    stored.AttemptCount,
		MaximumAttempts: stored.MaximumAttempts,
		AvailableAt:     stored.AvailableAt,
		WorkerID:        stored.WorkerID,
		LeaseExpiresAt:  stored.LeaseExpiresAt,
		LastError:       stored.LastError,
		FinishedAt:      stored.FinishedAt,
		CancelledAt:     stored.CancelledAt,
	})
	if err != nil {
		return fmt.Errorf("encode outbox event: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO job_outbox_events (
			id, event_type, schema_version, job_id, occurred_at, payload, next_attempt_at
		)
		VALUES ($1, $2, 1, $3, $4, $5, $4)`,
		eventID,
		eventType,
		stored.ID,
		stored.UpdatedAt,
		payload,
	); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func commitJobEvent(ctx context.Context, tx pgx.Tx, eventType outbox.EventType, stored job.Job) error {
	if err := insertJobEvent(ctx, tx, eventType, stored); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit job event: %w", err)
	}
	return nil
}
