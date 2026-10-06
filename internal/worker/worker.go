package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/daksh-panchal/work-flow/internal/job"
	"github.com/google/uuid"
)

const (
	maxNameLength  = 100
	maxQueueLength = 100
)

var (
	ErrNotFound            = errors.New("worker not found")
	ErrInvalidRegistration = errors.New("invalid worker registration")
)

type Worker struct {
	ID         uuid.UUID
	Name       string
	Queue      string
	LastSeenAt time.Time // timestamp of last heartbeat, claim, or lease renewal
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type Repository interface { // Defines the methods that the worker repository must implement
	Create(context.Context, Worker) error
	Heartbeat(context.Context, uuid.UUID, time.Time) (Worker, error)
}

type JobLeaser interface {
	Claim(context.Context, job.ClaimParams) (job.Job, bool, error)
	RenewLease(context.Context, job.RenewLeaseParams) (job.Job, error)
}

type Service struct {
	workers       Repository
	jobs          JobLeaser
	now           func() time.Time
	leaseDuration time.Duration // how long job ownership lasts before renewal is required
}

func NewService(
	workers Repository,
	jobs JobLeaser,
	now func() time.Time,
	leaseDuration time.Duration,
) *Service {
	return &Service{
		workers:       workers,
		jobs:          jobs,
		now:           now,
		leaseDuration: leaseDuration,
	}
}

func (service *Service) Register(ctx context.Context, name, queue string) (Worker, error) {
	name = strings.TrimSpace(name)
	queue = strings.TrimSpace(queue)

	switch { // validation of worker registration parameters
	case name == "":
		return Worker{}, fmt.Errorf("%w: name is required", ErrInvalidRegistration)
	case len(name) > maxNameLength:
		return Worker{}, fmt.Errorf("%w: name cannot exceed %d characters", ErrInvalidRegistration, maxNameLength)
	case queue == "":
		return Worker{}, fmt.Errorf("%w: queue is required", ErrInvalidRegistration)
	case len(queue) > maxQueueLength:
		return Worker{}, fmt.Errorf("%w: queue cannot exceed %d characters", ErrInvalidRegistration, maxQueueLength)
	}

	id, err := uuid.NewV7() // time-ordered UUID just like job IDs
	if err != nil {
		return Worker{}, fmt.Errorf("generate worker ID: %w", err)
	}
	now := service.now().UTC()
	registered := Worker{
		ID:         id,
		Name:       name,
		Queue:      queue,
		LastSeenAt: now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := service.workers.Create(ctx, registered); err != nil { // Store the worker in repository
		return Worker{}, fmt.Errorf("store worker: %w", err)
	}
	return registered, nil
}

func (service *Service) Heartbeat(ctx context.Context, id uuid.UUID) (Worker, error) {
	seenAt := service.now().UTC()
	registered, err := service.workers.Heartbeat(ctx, id, seenAt) // update worker's last seen timestamp in the repository
	if err != nil {
		return Worker{}, err
	}
	return registered, nil // return the updated worker and nil error if successful
}

func (service *Service) Claim(ctx context.Context, workerID uuid.UUID) (job.Job, bool, error) {
	now := service.now().UTC()
	registered, err := service.workers.Heartbeat(ctx, workerID, now)
	if err != nil {
		return job.Job{}, false, err
	}

	leaseToken, err := uuid.NewV7()
	if err != nil {
		return job.Job{}, false, fmt.Errorf("generate lease token: %w", err)
	}
	claimed, found, err := service.jobs.Claim(ctx, job.ClaimParams{ // Claim with PostgreSQL
		WorkerID:       workerID,
		Queue:          registered.Queue,
		LeaseToken:     leaseToken,
		Now:            now,
		LeaseExpiresAt: now.Add(service.leaseDuration),
	})
	if err != nil {
		return job.Job{}, false, fmt.Errorf("claim job: %w", err)
	}
	return claimed, found, nil
}

func (service *Service) RenewLease(
	ctx context.Context,
	workerID uuid.UUID,
	jobID uuid.UUID,
	leaseToken uuid.UUID,
) (job.Job, error) {
	now := service.now().UTC()
	if _, err := service.workers.Heartbeat(ctx, workerID, now); err != nil {
		return job.Job{}, err
	}

	renewed, err := service.jobs.RenewLease(ctx, job.RenewLeaseParams{
		JobID:          jobID,
		WorkerID:       workerID,
		LeaseToken:     leaseToken,
		Now:            now,
		LeaseExpiresAt: now.Add(service.leaseDuration),
	})
	if err != nil {
		return job.Job{}, err
	}
	return renewed, nil
}
