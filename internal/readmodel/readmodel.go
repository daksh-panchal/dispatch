package readmodel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/daksh-panchal/work-flow/internal/job"
	"github.com/daksh-panchal/work-flow/internal/outbox"
	"github.com/daksh-panchal/work-flow/internal/worker"
	"github.com/google/uuid"
)

const (
	defaultPageSize = 50
	maximumPageSize = 100
	maximumQueueLen = 100
)

var ErrInvalidQuery = errors.New("invalid read query")

type JobListParams struct {
	Queue  string
	State  job.State
	Cursor *uuid.UUID
	Limit  int
}

type JobPage struct {
	Items      []job.Job
	NextCursor *uuid.UUID
}

type WorkerListParams struct {
	Queue  string
	Cursor *uuid.UUID
	Limit  int
}

type StoredWorker struct {
	Worker         worker.Worker
	CurrentJobID   *uuid.UUID
	LeaseExpiresAt *time.Time
}

type WorkerStatus string

const (
	WorkerIdle    WorkerStatus = "idle"
	WorkerBusy    WorkerStatus = "busy"
	WorkerOffline WorkerStatus = "offline"
)

type WorkerSnapshot struct {
	Worker         worker.Worker
	Status         WorkerStatus
	CurrentJobID   *uuid.UUID
	LeaseExpiresAt *time.Time
}

type StoredWorkerPage struct {
	Items      []StoredWorker
	NextCursor *uuid.UUID
}

type WorkerPage struct {
	Items      []WorkerSnapshot
	NextCursor *uuid.UUID
}

type EventListParams struct {
	Queue         string
	JobID         *uuid.UUID
	AfterSequence int64
	Limit         int
}

type EventRecord struct {
	Sequence      int64            `json:"sequence"`
	ID            uuid.UUID        `json:"id"`
	Type          outbox.EventType `json:"type"`
	SchemaVersion int              `json:"schemaVersion"`
	JobID         uuid.UUID        `json:"jobId"`
	OccurredAt    time.Time        `json:"occurredAt"`
	Payload       json.RawMessage  `json:"payload"`
}

type EventPage struct {
	Items        []EventRecord
	NextSequence *int64
}

type Repository interface {
	ListJobs(context.Context, JobListParams) (JobPage, error)
	ListWorkers(context.Context, WorkerListParams) (StoredWorkerPage, error)
	ListEvents(context.Context, EventListParams) (EventPage, error)
}

type Service struct {
	repository       Repository
	now              func() time.Time
	workerStaleAfter time.Duration
}

func NewService(repository Repository, now func() time.Time, workerStaleAfter time.Duration) *Service {
	return &Service{repository: repository, now: now, workerStaleAfter: workerStaleAfter}
}

func (service *Service) ListJobs(ctx context.Context, params JobListParams) (JobPage, error) {
	params.Queue = strings.TrimSpace(params.Queue)
	if err := validateQueue(params.Queue); err != nil {
		return JobPage{}, err
	}
	if params.State != "" && !validState(params.State) {
		return JobPage{}, fmt.Errorf("%w: unknown job state %q", ErrInvalidQuery, params.State)
	}
	limit, err := normalizeLimit(params.Limit)
	if err != nil {
		return JobPage{}, err
	}
	params.Limit = limit
	return service.repository.ListJobs(ctx, params)
}

func (service *Service) ListWorkers(ctx context.Context, params WorkerListParams) (WorkerPage, error) {
	params.Queue = strings.TrimSpace(params.Queue)
	if err := validateQueue(params.Queue); err != nil {
		return WorkerPage{}, err
	}
	limit, err := normalizeLimit(params.Limit)
	if err != nil {
		return WorkerPage{}, err
	}
	params.Limit = limit
	stored, err := service.repository.ListWorkers(ctx, params)
	if err != nil {
		return WorkerPage{}, err
	}

	cutoff := service.now().UTC().Add(-service.workerStaleAfter)
	items := make([]WorkerSnapshot, 0, len(stored.Items))
	for _, item := range stored.Items {
		status := WorkerIdle
		if item.Worker.LastSeenAt.Before(cutoff) {
			status = WorkerOffline
		} else if item.CurrentJobID != nil {
			status = WorkerBusy
		}
		items = append(items, WorkerSnapshot{
			Worker:         item.Worker,
			Status:         status,
			CurrentJobID:   item.CurrentJobID,
			LeaseExpiresAt: item.LeaseExpiresAt,
		})
	}
	return WorkerPage{Items: items, NextCursor: stored.NextCursor}, nil
}

func (service *Service) ListEvents(ctx context.Context, params EventListParams) (EventPage, error) {
	params.Queue = strings.TrimSpace(params.Queue)
	if err := validateQueue(params.Queue); err != nil {
		return EventPage{}, err
	}
	if params.AfterSequence < 0 {
		return EventPage{}, fmt.Errorf("%w: afterSequence cannot be negative", ErrInvalidQuery)
	}
	limit, err := normalizeLimit(params.Limit)
	if err != nil {
		return EventPage{}, err
	}
	params.Limit = limit
	return service.repository.ListEvents(ctx, params)
}

func normalizeLimit(limit int) (int, error) {
	switch {
	case limit == 0:
		return defaultPageSize, nil
	case limit < 0 || limit > maximumPageSize:
		return 0, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, maximumPageSize)
	default:
		return limit, nil
	}
}

func validateQueue(queue string) error {
	if len(queue) > maximumQueueLen {
		return fmt.Errorf("%w: queue cannot exceed %d characters", ErrInvalidQuery, maximumQueueLen)
	}
	return nil
}

func validState(state job.State) bool {
	switch state {
	case job.StateQueued, job.StateRunning, job.StateSucceeded, job.StateFailed, job.StateCancelled:
		return true
	default:
		return false
	}
}
