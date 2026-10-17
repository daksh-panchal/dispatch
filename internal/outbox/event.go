package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventJobSubmitted      EventType = "JobSubmitted"
	EventJobClaimed        EventType = "JobClaimed"
	EventJobRetryScheduled EventType = "JobRetryScheduled"
	EventJobSucceeded      EventType = "JobSucceeded"
	EventJobFailed         EventType = "JobFailed"
	EventJobRecovered      EventType = "JobRecovered"
	EventJobCancelled      EventType = "JobCancelled"
)

var ErrClaimLost = errors.New("outbox event claim is no longer owned by this publisher")

type Event struct {
	ID              uuid.UUID       `json:"id"`
	Type            EventType       `json:"type"`
	SchemaVersion   int             `json:"schemaVersion"`
	JobID           uuid.UUID       `json:"jobId"`
	OccurredAt      time.Time       `json:"occurredAt"`
	Payload         json.RawMessage `json:"payload"`
	PublishAttempts int             `json:"-"`
}

type ClaimParams struct {
	Token     uuid.UUID
	Now       time.Time
	ExpiresAt time.Time
	BatchSize int
}

type PublishFailure struct {
	EventID       uuid.UUID
	ClaimToken    uuid.UUID
	FailedAt      time.Time
	NextAttemptAt time.Time
	Error         string
}

type Store interface {
	ClaimBatch(ctx context.Context, params ClaimParams) ([]Event, error)
	MarkPublished(ctx context.Context, eventID, claimToken uuid.UUID, publishedAt time.Time) error
	MarkFailed(ctx context.Context, failure PublishFailure) error
}

type Publisher interface {
	Publish(context.Context, Event) error
}
