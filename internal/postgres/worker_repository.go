package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/daksh-panchal/work-flow/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkerRepository struct {
	pool *pgxpool.Pool
}

func NewWorkerRepository(pool *pgxpool.Pool) *WorkerRepository {
	return &WorkerRepository{pool: pool}
}

func (repository *WorkerRepository) Create(ctx context.Context, registered worker.Worker) error {
	_, err := repository.pool.Exec(ctx, `
		INSERT INTO workers (id, name, queue, last_seen_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		registered.ID.String(),
		registered.Name,
		registered.Queue,
		registered.LastSeenAt,
		registered.CreatedAt,
		registered.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert worker: %w", err)
	}
	return nil
}

func (repository *WorkerRepository) Heartbeat(
	ctx context.Context,
	id uuid.UUID,
	seenAt time.Time,
) (worker.Worker, error) {
	registered, err := scanWorker(repository.pool.QueryRow(ctx, `
		UPDATE workers
		SET last_seen_at = $1, updated_at = $1
		WHERE id = $2
		RETURNING id, name, queue, last_seen_at, created_at, updated_at`,
		seenAt,
		id.String(),
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return worker.Worker{}, worker.ErrNotFound
	}
	if err != nil {
		return worker.Worker{}, fmt.Errorf("heartbeat worker: %w", err)
	}
	return registered, nil
}

func scanWorker(row pgx.Row) (worker.Worker, error) {
	var registered worker.Worker
	var id string
	if err := row.Scan(
		&id,
		&registered.Name,
		&registered.Queue,
		&registered.LastSeenAt,
		&registered.CreatedAt,
		&registered.UpdatedAt,
	); err != nil {
		return worker.Worker{}, err
	}

	parsed, err := uuid.Parse(id)
	if err != nil {
		return worker.Worker{}, fmt.Errorf("parse stored worker ID: %w", err)
	}
	registered.ID = parsed
	return registered, nil
}
