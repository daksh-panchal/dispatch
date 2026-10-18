package recovery

import (
	"context"
	"log/slog"
	"time"

	"github.com/daksh-panchal/work-flow/internal/job"
	"github.com/daksh-panchal/work-flow/internal/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type Repository interface {
	RecoverExpired(
		context.Context,
		time.Time,
		time.Duration,
		time.Duration,
		int,
	) (job.RecoveryResult, error)
}

type Observer interface {
	RecordRecovery(context.Context, int64, int64, time.Duration, error)
}

type Service struct {
	repository   Repository
	now          func() time.Time
	interval     time.Duration
	retryBase    time.Duration
	retryMaximum time.Duration
	batchSize    int
	logger       *slog.Logger
	observer     Observer
}

func NewService(
	repository Repository,
	now func() time.Time,
	interval time.Duration,
	retryBase time.Duration,
	retryMaximum time.Duration,
	batchSize int,
	logger *slog.Logger,
	observers ...Observer,
) *Service {
	service := &Service{
		repository:   repository,
		now:          now,
		interval:     interval,
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

func (service *Service) RecoverOnce(ctx context.Context) (job.RecoveryResult, error) {
	startedAt := time.Now()
	ctx, span := observability.Tracer().Start(ctx, "recovery.recover_expired_jobs")
	defer span.End()

	result, err := service.repository.RecoverExpired(
		ctx,
		service.now().UTC(),
		service.retryBase,
		service.retryMaximum,
		service.batchSize,
	)
	span.SetAttributes(
		attribute.Int64("workflow.jobs.requeued", result.Requeued),
		attribute.Int64("workflow.jobs.failed", result.Failed),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "recovery failed")
	}
	if service.observer != nil {
		service.observer.RecordRecovery(ctx, result.Requeued, result.Failed, time.Since(startedAt), err)
	}
	if err != nil && ctx.Err() == nil {
		service.logger.ErrorContext(ctx, "recover expired jobs", "error", err)
	} else if result.Requeued+result.Failed > 0 {
		service.logger.InfoContext(
			ctx,
			"recovered expired jobs",
			"requeued", result.Requeued,
			"failed", result.Failed,
		)
	}
	return result, err
}

func (service *Service) Run(ctx context.Context) {
	service.recover(ctx)
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			service.recover(ctx)
		}
	}
}

func (service *Service) recover(ctx context.Context) {
	for {
		result, err := service.RecoverOnce(ctx)
		if err != nil {
			return
		}
		total := result.Requeued + result.Failed
		if total < int64(service.batchSize) {
			return
		}
	}
}
