package outbox

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/daksh-panchal/dispatch/internal/observability"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const maxPublishErrorLength = 2000

type PublishResult struct {
	Claimed   int
	Published int
	Failed    int
}

type Observer interface {
	RecordOutbox(context.Context, int, int, int, time.Duration, error)
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
	observer     Observer
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
	observers ...Observer,
) *Service {
	service := &Service{
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
	if len(observers) > 0 {
		service.observer = observers[0]
	}
	return service
}

func (service *Service) PublishOnce(ctx context.Context) (result PublishResult, err error) {
	startedAt := time.Now()
	ctx, span := observability.Tracer().Start(ctx, "outbox.publish_batch")
	defer func() {
		span.SetAttributes(
			attribute.Int("dispatch.outbox.claimed", result.Claimed),
			attribute.Int("dispatch.outbox.published", result.Published),
			attribute.Int("dispatch.outbox.failed", result.Failed),
		)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "outbox publication failed")
		}
		if service.observer != nil {
			service.observer.RecordOutbox(
				ctx,
				result.Claimed,
				result.Published,
				result.Failed,
				time.Since(startedAt),
				err,
			)
		}
		if err != nil && ctx.Err() == nil {
			service.logger.ErrorContext(ctx, "publish outbox events", "error", err)
		} else if result.Published > 0 || result.Failed > 0 {
			service.logger.InfoContext(
				ctx,
				"processed outbox events",
				"published", result.Published,
				"failed", result.Failed,
			)
		}
		span.End()
	}()

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

	result = PublishResult{Claimed: len(events)}
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
			return
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
