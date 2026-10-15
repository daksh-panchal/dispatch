package job

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

var fixedTime = time.Date(2026, time.October, 2, 0, 0, 0, 0, time.UTC)

func TestNewJobStartsQueued(t *testing.T) {
	created, err := New(validParams())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if created.ID == uuid.Nil {
		t.Fatal("New() generated an empty ID")
	}
	if created.ID.Version() != 7 {
		t.Fatalf("New() ID version = %d, want 7", created.ID.Version())
	}
	if created.State != StateQueued {
		t.Fatalf("New() state = %q, want %q", created.State, StateQueued)
	}
	if created.AttemptCount != 0 {
		t.Fatalf("New() attempt count = %d, want 0", created.AttemptCount)
	}
	if created.AvailableAt != fixedTime || created.CreatedAt != fixedTime || created.UpdatedAt != fixedTime {
		t.Fatal("New() did not apply the supplied clock consistently")
	}
}

func TestNewJobRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		change func(*NewJobParams)
	}{
		{name: "empty idempotency key", change: func(params *NewJobParams) { params.IdempotencyKey = " " }},
		{name: "empty queue", change: func(params *NewJobParams) { params.Queue = " " }},
		{name: "invalid hash", change: func(params *NewJobParams) { params.SubmissionHash = "short" }},
		{name: "invalid payload", change: func(params *NewJobParams) { params.Payload = json.RawMessage("not-json") }},
		{name: "zero attempts", change: func(params *NewJobParams) { params.MaximumAttempts = 0 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := validParams()
			test.change(&params)
			if _, err := New(params); err == nil {
				t.Fatal("New() error = nil, want validation error")
			}
		})
	}
}

func TestSubmitIsIdempotentForEquivalentJSON(t *testing.T) {
	repository := newMemoryRepository()
	service := NewService(repository, func() time.Time { return fixedTime })
	ctx := context.Background()

	first, firstCreated, err := service.Submit(ctx, "build-123", SubmitInput{
		Queue:           "default",
		Priority:        0,
		MaximumAttempts: 3,
		Payload:         json.RawMessage(`{"command":"build","options":{"clean":true}}`),
	})
	if err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}

	second, secondCreated, err := service.Submit(ctx, "build-123", SubmitInput{
		Queue:           "default",
		Priority:        0,
		MaximumAttempts: 3,
		Payload:         json.RawMessage(`{ "options": { "clean": true }, "command": "build" }`),
	})
	if err != nil {
		t.Fatalf("second Submit() error = %v", err)
	}

	if !firstCreated || secondCreated {
		t.Fatalf("created results = (%t, %t), want (true, false)", firstCreated, secondCreated)
	}
	if first.ID != second.ID {
		t.Fatalf("job IDs differ: %s != %s", first.ID, second.ID)
	}
}

func TestSubmitRejectsIdempotencyConflict(t *testing.T) {
	repository := newMemoryRepository()
	service := NewService(repository, func() time.Time { return fixedTime })
	ctx := context.Background()

	_, _, err := service.Submit(ctx, "build-123", validSubmitInput())
	if err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}

	changed := validSubmitInput()
	changed.Priority = 10
	_, _, err = service.Submit(ctx, "build-123", changed)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("second Submit() error = %v, want ErrIdempotencyConflict", err)
	}
}

func TestCancelUsesCurrentTime(t *testing.T) {
	repository := newMemoryRepository()
	service := NewService(repository, func() time.Time { return fixedTime })
	id := uuid.New()
	repository.byID[id] = Job{ID: id, State: StateQueued}

	cancelled, err := service.Cancel(context.Background(), id)
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if cancelled.State != StateCancelled || cancelled.CancelledAt == nil || *cancelled.CancelledAt != fixedTime {
		t.Fatalf("Cancel() job = %+v", cancelled)
	}
}

func validParams() NewJobParams {
	return NewJobParams{
		IdempotencyKey:  "build-123",
		SubmissionHash:  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Queue:           "default",
		Priority:        0,
		Payload:         json.RawMessage(`{"command":"build"}`),
		MaximumAttempts: 3,
		Now:             fixedTime,
	}
}

func validSubmitInput() SubmitInput {
	return SubmitInput{
		Queue:           "default",
		Priority:        0,
		Payload:         json.RawMessage(`{"command":"build"}`),
		MaximumAttempts: 3,
	}
}

type memoryRepository struct {
	mu    sync.Mutex
	byID  map[uuid.UUID]Job
	byKey map[string]uuid.UUID
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{
		byID:  make(map[uuid.UUID]Job),
		byKey: make(map[string]uuid.UUID),
	}
}

func (repository *memoryRepository) CreateOrGet(_ context.Context, candidate Job) (Job, bool, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()

	if id, exists := repository.byKey[candidate.IdempotencyKey]; exists {
		return repository.byID[id], false, nil
	}
	repository.byID[candidate.ID] = candidate
	repository.byKey[candidate.IdempotencyKey] = candidate.ID
	return candidate, true, nil
}

func (repository *memoryRepository) Get(_ context.Context, id uuid.UUID) (Job, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()

	stored, exists := repository.byID[id]
	if !exists {
		return Job{}, ErrNotFound
	}
	return stored, nil
}

func (repository *memoryRepository) Cancel(_ context.Context, params CancelParams) (Job, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()

	stored, exists := repository.byID[params.JobID]
	if !exists {
		return Job{}, ErrNotFound
	}
	if stored.State != StateQueued && stored.State != StateRunning && stored.State != StateCancelled {
		return Job{}, ErrCancellationConflict
	}
	if stored.State != StateCancelled {
		cancelledAt := params.Now
		stored.State = StateCancelled
		stored.CancelledAt = &cancelledAt
		stored.FinishedAt = &cancelledAt
		stored.UpdatedAt = params.Now
		repository.byID[params.JobID] = stored
	}
	return stored, nil
}
