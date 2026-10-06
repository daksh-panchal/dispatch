package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/daksh-panchal/work-flow/internal/job"
	"github.com/google/uuid"
)

var fixedTime = time.Date(2026, time.October, 4, 0, 0, 0, 0, time.UTC)

func TestRegisterNormalizesWorker(t *testing.T) {
	repository := &memoryWorkerRepository{}
	service := NewService(repository, &memoryJobLeaser{}, func() time.Time { return fixedTime }, 30*time.Second)

	registered, err := service.Register(context.Background(), "  builder-1  ", "  default  ")
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if registered.ID == uuid.Nil || registered.Name != "builder-1" || registered.Queue != "default" {
		t.Fatalf("Register() worker = %+v", registered)
	}
	if registered.LastSeenAt != fixedTime {
		t.Fatalf("Register() last seen = %v, want %v", registered.LastSeenAt, fixedTime)
	}
}

func TestRegisterRejectsMissingFields(t *testing.T) {
	service := NewService(&memoryWorkerRepository{}, &memoryJobLeaser{}, func() time.Time { return fixedTime }, 30*time.Second)
	for _, test := range []struct {
		name  string
		queue string
	}{
		{name: "", queue: "default"},
		{name: "builder-1", queue: ""},
	} {
		if _, err := service.Register(context.Background(), test.name, test.queue); !errors.Is(err, ErrInvalidRegistration) {
			t.Fatalf("Register(%q, %q) error = %v, want ErrInvalidRegistration", test.name, test.queue, err)
		}
	}
}

func TestClaimUsesWorkersQueueAndCreatesLease(t *testing.T) {
	workerID := uuid.Must(uuid.NewV7())
	repository := &memoryWorkerRepository{worker: Worker{ID: workerID, Queue: "builds"}}
	leaser := &memoryJobLeaser{found: true}
	service := NewService(repository, leaser, func() time.Time { return fixedTime }, 30*time.Second)

	_, found, err := service.Claim(context.Background(), workerID)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if !found {
		t.Fatal("Claim() found = false, want true")
	}
	if leaser.claim.Queue != "builds" || leaser.claim.WorkerID != workerID {
		t.Fatalf("Claim() params = %+v", leaser.claim)
	}
	if leaser.claim.LeaseToken == uuid.Nil || leaser.claim.LeaseExpiresAt != fixedTime.Add(30*time.Second) {
		t.Fatalf("Claim() lease = %+v", leaser.claim)
	}
}

type memoryWorkerRepository struct {
	worker Worker
}

func (repository *memoryWorkerRepository) Create(_ context.Context, worker Worker) error {
	repository.worker = worker
	return nil
}

func (repository *memoryWorkerRepository) Heartbeat(_ context.Context, id uuid.UUID, seenAt time.Time) (Worker, error) {
	if repository.worker.ID != id {
		return Worker{}, ErrNotFound
	}
	repository.worker.LastSeenAt = seenAt
	repository.worker.UpdatedAt = seenAt
	return repository.worker, nil
}

type memoryJobLeaser struct {
	claim job.ClaimParams
	found bool
}

func (leaser *memoryJobLeaser) Claim(_ context.Context, params job.ClaimParams) (job.Job, bool, error) {
	leaser.claim = params
	return job.Job{}, leaser.found, nil
}

func (leaser *memoryJobLeaser) RenewLease(_ context.Context, _ job.RenewLeaseParams) (job.Job, error) {
	return job.Job{}, nil
}
