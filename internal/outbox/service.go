package outbox

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
)

const maxPublishErrorLength = 2000

type PublishResult struct {
	Claimed   int
	Published int
	Failed    int
}

type Service struct {
	store        Store
	publisher    Publisher
	now          func() time.Time
	interval     time.Duration
	claimLease   time.Duration
	retryBase    time.Duration
	retryMaximum time.Duration
	batchSize    int
	logger       *slog.Logger
}

func NewService(
	store Store,
	publisher Publisher,
	now func() time.Time,
	interval time.Duration,
	claimLease time.Duration,
	retryBase time.Duration,
	retryMaximum time.Duration,
	batchSize int,
	logger *slog.Logger,
) *Service {
	return &Service{
		store:        store,
		publisher:    publisher,
		now:          now,
		interval:     interval,
		claimLease:   claimLease,
		retryBase:    retryBase,
		retryMaximum: retryMaximum,
		batchSize:    batchSize,
		logger:       logger,
	}
}

func (service *Service) PublishOnce(ctx context.Context) (PublishResult, error) {
	now := service.now().UTC()
	claimToken := uuid.New()
	events, err := service.store.ClaimBatch(ctx, ClaimParams{
		Token:     claimToken,
		Now:       now,
		ExpiresAt: now.Add(service.claimLease),
		BatchSize: service.batchSize,
	})
	if err != nil {
		return PublishResult{}, err
	}

	result := PublishResult{Claimed: len(events)}
	for _, event := range events {
		if err := service.publisher.Publish(ctx, event); err != nil {
			failure := PublishFailure{
				EventID:       event.ID,
				ClaimToken:    claimToken,
				FailedAt:      now,
				NextAttemptAt: now.Add(service.retryDelay(event.PublishAttempts + 1)),
				Error:         truncateError(err.Error()),
			}
			if markErr := service.store.MarkFailed(ctx, failure); markErr != nil {
				return result, markErr
			}
			result.Failed++
			continue
		}

		if err := service.store.MarkPublished(ctx, event.ID, claimToken, now); err != nil {
			return result, err
		}
		result.Published++
	}
	return result, nil
}

func (service *Service) Run(ctx context.Context) {
	service.publishAvailable(ctx)
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			service.publishAvailable(ctx)
		}
	}
}

func (service *Service) publishAvailable(ctx context.Context) {
	for {
		result, err := service.PublishOnce(ctx)
		if err != nil {
			if ctx.Err() == nil {
				service.logger.Error("publish outbox events", "error", err)
			}
			return
		}
		if result.Published > 0 || result.Failed > 0 {
			service.logger.Info(
				"processed outbox events",
				"published", result.Published,
				"failed", result.Failed,
			)
		}
		if result.Claimed < service.batchSize {
			return
		}
	}
}

func (service *Service) retryDelay(attempt int) time.Duration {
	delay := service.retryBase
	for current := 1; current < attempt && delay < service.retryMaximum; current++ {
		if delay > service.retryMaximum/2 {
			return service.retryMaximum
		}
		delay *= 2
	}
	if delay > service.retryMaximum {
		return service.retryMaximum
	}
	return delay
}

func truncateError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= maxPublishErrorLength {
		return value
	}
	return value[:maxPublishErrorLength]
}
