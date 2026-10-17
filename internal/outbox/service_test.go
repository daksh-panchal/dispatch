package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
)

var fixedTime = time.Date(2026, time.October, 15, 0, 0, 0, 0, time.UTC)

func TestPublishOnceMarksSuccessAndSchedulesFailure(t *testing.T) {
	successID := uuid.New()
	failureID := uuid.New()
	store := &memoryStore{events: []Event{
		{ID: successID, Type: EventJobSubmitted},
		{ID: failureID, Type: EventJobClaimed, PublishAttempts: 2},
	}}
	publisher := &memoryPublisher{failureID: failureID}
	service := newTestService(store, publisher)

	result, err := service.PublishOnce(context.Background())
	if err != nil {
		t.Fatalf("PublishOnce() error = %v", err)
	}
	if result.Claimed != 2 || result.Published != 1 || result.Failed != 1 {
		t.Fatalf("PublishOnce() result = %+v", result)
	}
	if len(store.published) != 1 || store.published[0] != successID {
		t.Fatalf("published IDs = %v", store.published)
	}
	if len(store.failures) != 1 {
		t.Fatalf("failures = %v", store.failures)
	}
	failure := store.failures[0]
	if failure.EventID != failureID || failure.NextAttemptAt != fixedTime.Add(20*time.Second) {
		t.Fatalf("failure = %+v", failure)
	}
	if failure.Error != "message-flow unavailable" {
		t.Fatalf("failure error = %q", failure.Error)
	}
	if store.claim.Token == uuid.Nil || store.claim.ExpiresAt != fixedTime.Add(30*time.Second) {
		t.Fatalf("claim = %+v", store.claim)
	}
}

func TestPublishOncePropagatesLostClaim(t *testing.T) {
	store := &memoryStore{
		events:        []Event{{ID: uuid.New(), Type: EventJobSucceeded}},
		markPublished: ErrClaimLost,
	}
	service := newTestService(store, &memoryPublisher{})

	_, err := service.PublishOnce(context.Background())
	if !errors.Is(err, ErrClaimLost) {
		t.Fatalf("PublishOnce() error = %v, want ErrClaimLost", err)
	}
}

func newTestService(store Store, publisher Publisher) *Service {
	return NewService(
		store,
		publisher,
		func() time.Time { return fixedTime },
		time.Second,
		30*time.Second,
		5*time.Second,
		time.Minute,
		100,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

type memoryStore struct {
	events        []Event
	claim         ClaimParams
	published     []uuid.UUID
	failures      []PublishFailure
	markPublished error
}

func (store *memoryStore) ClaimBatch(_ context.Context, params ClaimParams) ([]Event, error) {
	store.claim = params
	return store.events, nil
}

func (store *memoryStore) MarkPublished(
	_ context.Context,
	eventID uuid.UUID,
	_ uuid.UUID,
	_ time.Time,
) error {
	if store.markPublished != nil {
		return store.markPublished
	}
	store.published = append(store.published, eventID)
	return nil
}

func (store *memoryStore) MarkFailed(_ context.Context, failure PublishFailure) error {
	store.failures = append(store.failures, failure)
	return nil
}

type memoryPublisher struct {
	failureID uuid.UUID
}

func (publisher *memoryPublisher) Publish(_ context.Context, event Event) error {
	if event.ID == publisher.failureID {
		return errors.New("message-flow unavailable")
	}
	return nil
}
