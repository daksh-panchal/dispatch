package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type StatsRepository struct {
	pool *pgxpool.Pool
}

func NewStatsRepository(pool *pgxpool.Pool) *StatsRepository {
	return &StatsRepository{pool: pool}
}

func (repository *StatsRepository) ReadSchedulerStats(
	ctx context.Context,
) (map[string]int64, int64, int64, int64, error) {
	const query = `
		SELECT
			COUNT(*) FILTER (WHERE state = 'queued'),
			COUNT(*) FILTER (WHERE state = 'running'),
			COUNT(*) FILTER (WHERE state = 'succeeded'),
			COUNT(*) FILTER (WHERE state = 'failed'),
			COUNT(*) FILTER (WHERE state = 'cancelled'),
			COUNT(*) FILTER (
				WHERE state = 'running'
				  AND lease_expires_at > CURRENT_TIMESTAMP
			),
			COUNT(*) FILTER (
				WHERE state = 'running'
				  AND lease_expires_at <= CURRENT_TIMESTAMP
			),
			(SELECT COUNT(*) FROM job_outbox_events WHERE published_at IS NULL)
		FROM jobs`

	var queued int64
	var running int64
	var succeeded int64
	var failed int64
	var cancelled int64
	var activeLeases int64
	var expiredLeases int64
	var pendingOutboxEvents int64
	err := repository.pool.QueryRow(ctx, query).Scan(
		&queued,
		&running,
		&succeeded,
		&failed,
		&cancelled,
		&activeLeases,
		&expiredLeases,
		&pendingOutboxEvents,
	)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	counts := map[string]int64{
		"queued":    queued,
		"running":   running,
		"succeeded": succeeded,
		"failed":    failed,
		"cancelled": cancelled,
	}
	return counts, activeLeases, expiredLeases, pendingOutboxEvents, nil
}
