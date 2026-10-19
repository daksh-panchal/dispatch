package demo

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestManagerRunsJobsThroughSchedulerAPI(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler := newFakeScheduler()
	manager := NewManager(
		ctx,
		scheduler,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithJitter(func(time.Duration) time.Duration { return 10 * time.Millisecond }),
		WithIntervals(2*time.Millisecond, 50*time.Millisecond),
	)

	run, err := manager.Start(context.Background(), StartRunInput{
		JobCount: 4, WorkerCount: 2, WorkDuration: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForRunStatus(t, manager, run.ID, RunCompleted)

	snapshot, err := manager.Snapshot(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snapshot.Jobs) != 4 || len(snapshot.Run.Workers) != 2 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	for _, stored := range snapshot.Jobs {
		if stored.State != "succeeded" {
			t.Fatalf("job %s state = %q, want succeeded", stored.ID, stored.State)
		}
	}
	completed := 0
	for _, worker := range snapshot.Run.Workers {
		completed += worker.Completed
	}
	if completed != 4 {
		t.Fatalf("completed jobs = %d, want 4", completed)
	}
}

func TestKillingWorkerAbandonsCurrentLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduler := newFakeScheduler()
	manager := NewManager(
		ctx,
		scheduler,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithJitter(func(time.Duration) time.Duration { return time.Hour }),
		WithIntervals(2*time.Millisecond, time.Minute),
	)

	run, err := manager.Start(context.Background(), StartRunInput{
		JobCount: 1, WorkerCount: 1, WorkDuration: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	var workerID uuid.UUID
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, _ := manager.Get(run.ID)
		if len(current.Workers) == 1 && current.Workers[0].Status == WorkerWorking {
			workerID = current.Workers[0].ID
			break
		}
		time.Sleep(time.Millisecond)
	}
	if workerID == uuid.Nil {
		t.Fatal("worker did not claim the job")
	}

	killed, err := manager.KillWorker(run.ID, workerID)
	if err != nil {
		t.Fatalf("KillWorker() error = %v", err)
	}
	if killed.Status != RunStalled || killed.Workers[0].Status != WorkerKilled {
		t.Fatalf("killed run = %+v", killed)
	}
	time.Sleep(20 * time.Millisecond)
	jobs, _ := scheduler.ListJobs(context.Background(), run.Queue)
	if len(jobs) != 1 || jobs[0].State != "running" {
		t.Fatalf("jobs after worker kill = %+v, want abandoned running lease", jobs)
	}
}

func TestManagerValidatesRunLimits(t *testing.T) {
	manager := NewManager(
		context.Background(),
		newFakeScheduler(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	_, err := manager.Start(context.Background(), StartRunInput{
		JobCount: 0, WorkerCount: 1, WorkDuration: time.Second,
	})
	if !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("Start() error = %v, want ErrInvalidRun", err)
	}
}

func waitForRunStatus(t *testing.T, manager *Manager, id uuid.UUID, expected RunStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		run, err := manager.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == expected {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	run, _ := manager.Get(id)
	t.Fatalf("run status = %q, want %q", run.Status, expected)
}

type fakeScheduler struct {
	mu      sync.Mutex
	jobs    []Job
	workers map[uuid.UUID]SchedulerWorker
	claims  map[uuid.UUID]Claim
}

func newFakeScheduler() *fakeScheduler {
	return &fakeScheduler{
		workers: make(map[uuid.UUID]SchedulerWorker),
		claims:  make(map[uuid.UUID]Claim),
	}
}

func (scheduler *fakeScheduler) SubmitJob(_ context.Context, input SubmitJobInput) (Job, error) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	stored := Job{
		ID: uuid.New(), Queue: input.Queue, Priority: input.Priority, Payload: input.Payload,
		State: "queued", MaximumAttempts: input.MaximumAttempts,
	}
	scheduler.jobs = append(scheduler.jobs, stored)
	return stored, nil
}

func (scheduler *fakeScheduler) RegisterWorker(_ context.Context, name, queue string) (SchedulerWorker, error) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	registered := SchedulerWorker{ID: uuid.New(), Name: name, Queue: queue, Status: "idle"}
	scheduler.workers[registered.ID] = registered
	return registered, nil
}

func (scheduler *fakeScheduler) Claim(ctx context.Context, workerID uuid.UUID) (*Claim, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	registered := scheduler.workers[workerID]
	for index := range scheduler.jobs {
		if scheduler.jobs[index].Queue != registered.Queue || scheduler.jobs[index].State != "queued" {
			continue
		}
		scheduler.jobs[index].State = "running"
		scheduler.jobs[index].WorkerID = &workerID
		claim := Claim{Job: scheduler.jobs[index], LeaseToken: uuid.New()}
		scheduler.claims[claim.Job.ID] = claim
		return &claim, nil
	}
	return nil, nil
}

func (scheduler *fakeScheduler) RenewLease(
	ctx context.Context,
	_ uuid.UUID,
	_ uuid.UUID,
	_ uuid.UUID,
) error {
	return ctx.Err()
}

func (scheduler *fakeScheduler) Complete(
	ctx context.Context,
	workerID uuid.UUID,
	jobID uuid.UUID,
	leaseToken uuid.UUID,
	_ json.RawMessage,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	claim, exists := scheduler.claims[jobID]
	if !exists || claim.LeaseToken != leaseToken || claim.Job.WorkerID == nil || *claim.Job.WorkerID != workerID {
		return errors.New("lease lost")
	}
	for index := range scheduler.jobs {
		if scheduler.jobs[index].ID == jobID {
			scheduler.jobs[index].State = "succeeded"
			delete(scheduler.claims, jobID)
			return nil
		}
	}
	return errors.New("job not found")
}

func (scheduler *fakeScheduler) ListJobs(_ context.Context, queue string) ([]Job, error) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	var result []Job
	for _, stored := range scheduler.jobs {
		if stored.Queue == queue {
			result = append(result, stored)
		}
	}
	return result, nil
}

func (scheduler *fakeScheduler) ListWorkers(_ context.Context, queue string) ([]SchedulerWorker, error) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	var result []SchedulerWorker
	for _, registered := range scheduler.workers {
		if registered.Queue == queue {
			result = append(result, registered)
		}
	}
	return result, nil
}
