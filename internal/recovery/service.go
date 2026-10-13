package recovery

import (
	"context"
	"log/slog"
	"time"

	"github.com/daksh-panchal/work-flow/internal/job"
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

type Service struct {
	repository   Repository
	now          func() time.Time
	interval     time.Duration
	retryBase    time.Duration
	retryMaximum time.Duration
	batchSize    int
	logger       *slog.Logger
}

func NewService(
	repository Repository,
	now func() time.Time,
	interval time.Duration,
	retryBase time.Duration,
	retryMaximum time.Duration,
	batchSize int,
	logger *slog.Logger,
) *Service {
	return &Service{
		repository:   repository,
		now:          now,
		interval:     interval,
		retryBase:    retryBase,
		retryMaximum: retryMaximum,
		batchSize:    batchSize,
		logger:       logger,
	}
}

func (service *Service) RecoverOnce(ctx context.Context) (job.RecoveryResult, error) {
	return service.repository.RecoverExpired(
		ctx,
		service.now().UTC(),
		service.retryBase,
		service.retryMaximum,
		service.batchSize,
	)
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
			if ctx.Err() == nil {
				service.logger.Error("recover expired jobs", "error", err)
			}
			return
		}
		total := result.Requeued + result.Failed
		if total > 0 {
			service.logger.Info(
				"recovered expired jobs",
				"requeued", result.Requeued,
				"failed", result.Failed,
			)
		}
		if total < int64(service.batchSize) {
			return
		}
	}
}
