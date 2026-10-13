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
	service := newTestService(repository, &memoryJobStore{})

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
	service := newTestService(&memoryWorkerRepository{}, &memoryJobStore{})
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
	store := &memoryJobStore{found: true}
	service := newTestService(repository, store)

	_, found, err := service.Claim(context.Background(), workerID)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if !found {
		t.Fatal("Claim() found = false, want true")
	}
	if store.claim.Queue != "builds" || store.claim.WorkerID != workerID {
		t.Fatalf("Claim() params = %+v", store.claim)
	}
	if store.claim.LeaseToken == uuid.Nil || store.claim.LeaseExpiresAt != fixedTime.Add(30*time.Second) {
		t.Fatalf("Claim() lease = %+v", store.claim)
	}
}

func TestFailSchedulesExponentialRetry(t *testing.T) {
	workerID := uuid.Must(uuid.NewV7())
	jobID := uuid.Must(uuid.NewV7())
	leaseToken := uuid.Must(uuid.NewV7())
	repository := &memoryWorkerRepository{worker: Worker{ID: workerID}}
	store := &memoryJobStore{stored: job.Job{ID: jobID, AttemptCount: 2}}
	service := newTestService(repository, store)

	_, err := service.Fail(context.Background(), workerID, jobID, leaseToken, " compiler error ")
	if err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	if store.fail.Reason != "compiler error" {
		t.Fatalf("Fail() reason = %q", store.fail.Reason)
	}
	wantRetryAt := fixedTime.Add(10 * time.Second)
	if store.fail.RetryAt != wantRetryAt {
		t.Fatalf("Fail() retry at = %v, want %v", store.fail.RetryAt, wantRetryAt)
	}
}

func TestCompleteRejectsInvalidResultJSON(t *testing.T) {
	service := newTestService(&memoryWorkerRepository{}, &memoryJobStore{})
	_, err := service.Complete(
		context.Background(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
		[]byte("not-json"),
	)
	if !errors.Is(err, job.ErrInvalidOutcome) {
		t.Fatalf("Complete() error = %v, want ErrInvalidOutcome", err)
	}
}

func newTestService(workers Repository, jobs JobStore) *Service {
	return NewService(
		workers,
		jobs,
		func() time.Time { return fixedTime },
		30*time.Second,
		5*time.Second,
		time.Minute,
	)
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

type memoryJobStore struct {
	stored   job.Job
	claim    job.ClaimParams
	fail     job.FailParams
	complete job.CompleteParams
	found    bool
}

func (store *memoryJobStore) Get(_ context.Context, _ uuid.UUID) (job.Job, error) {
	return store.stored, nil
}

func (store *memoryJobStore) Claim(_ context.Context, params job.ClaimParams) (job.Job, bool, error) {
	store.claim = params
	return job.Job{}, store.found, nil
}

func (store *memoryJobStore) RenewLease(_ context.Context, _ job.RenewLeaseParams) (job.Job, error) {
	return job.Job{}, nil
}

func (store *memoryJobStore) Complete(_ context.Context, params job.CompleteParams) (job.Job, error) {
	store.complete = params
	return store.stored, nil
}

func (store *memoryJobStore) Fail(_ context.Context, params job.FailParams) (job.Job, error) {
	store.fail = params
	return store.stored, nil
}
