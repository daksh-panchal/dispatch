package recovery

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/daksh-panchal/dispatch/internal/job"
)

type cancellationRepository struct {
	called chan struct{}
}

func (repository *cancellationRepository) RecoverExpired(
	context.Context,
	time.Time,
	time.Duration,
	time.Duration,
	int,
) (job.RecoveryResult, error) {
	select {
	case repository.called <- struct{}{}:
	default:
	}
	return job.RecoveryResult{}, nil
}

func TestRunStopsWhenContextIsCancelled(t *testing.T) {
	repository := &cancellationRepository{called: make(chan struct{}, 1)}
	service := NewService(
		repository,
		time.Now,
		time.Hour,
		time.Second,
		time.Minute,
		100,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		service.Run(ctx)
		close(done)
	}()

	select {
	case <-repository.called:
	case <-time.After(time.Second):
		t.Fatal("initial recovery cycle did not run")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery service did not stop after cancellation")
	}
}
