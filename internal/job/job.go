package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	maxIdempotencyKeyLength = 200
	maxQueueLength          = 100
	maxAttemptsLimit        = 100
	minimumPriority         = -1000
	maximumPriority         = 1000
)

type State string // create string-based type for job state to avoid accidental assignment of random strings

const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

var (
	ErrNotFound             = errors.New("job not found")
	ErrIdempotencyConflict  = errors.New("idempotency key is already used by a different submission")
	ErrInvalidSubmission    = errors.New("invalid job submission")
	ErrLeaseLost            = errors.New("job lease is no longer owned by this worker")
	ErrInvalidOutcome       = errors.New("invalid job outcome")
	ErrCancellationConflict = errors.New("job has already reached a terminal state")
)

type Job struct {
	ID              uuid.UUID
	IdempotencyKey  string
	SubmissionHash  string
	Queue           string
	Priority        int
	Payload         json.RawMessage
	State           State
	AttemptCount    int
	MaximumAttempts int
	AvailableAt     time.Time
	WorkerID        *uuid.UUID
	LeaseToken      *uuid.UUID
	LeaseExpiresAt  *time.Time
	Result          json.RawMessage
	LastError       *string
	FinishedAt      *time.Time
	CancelledAt     *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type ClaimParams struct {
	WorkerID       uuid.UUID
	Queue          string
	LeaseToken     uuid.UUID
	Now            time.Time
	LeaseExpiresAt time.Time
}

type RenewLeaseParams struct {
	JobID          uuid.UUID
	WorkerID       uuid.UUID
	LeaseToken     uuid.UUID
	Now            time.Time
	LeaseExpiresAt time.Time
}

type CompleteParams struct {
	JobID      uuid.UUID
	WorkerID   uuid.UUID
	LeaseToken uuid.UUID
	Result     json.RawMessage
	Now        time.Time
}

type FailParams struct {
	JobID      uuid.UUID
	WorkerID   uuid.UUID
	LeaseToken uuid.UUID
	Reason     string
	Now        time.Time
	RetryAt    time.Time
}

type CancelParams struct {
	JobID uuid.UUID
	Now   time.Time
}

type RecoveryResult struct {
	Requeued int64
	Failed   int64
}

type NewJobParams struct {
	IdempotencyKey  string
	SubmissionHash  string
	Queue           string
	Priority        int
	Payload         json.RawMessage
	MaximumAttempts int
	Now             time.Time
}

func New(params NewJobParams) (Job, error) {
	idempotencyKey := strings.TrimSpace(params.IdempotencyKey)
	queue := strings.TrimSpace(params.Queue)

	switch { // return empty Job and an error if any of the parameters are invalid
	case idempotencyKey == "":
		return Job{}, errors.New("idempotency key is required")
	case len(idempotencyKey) > maxIdempotencyKeyLength:
		return Job{}, fmt.Errorf("idempotency key cannot exceed %d characters", maxIdempotencyKeyLength)
	case queue == "":
		return Job{}, errors.New("queue is required")
	case len(queue) > maxQueueLength:
		return Job{}, fmt.Errorf("queue cannot exceed %d characters", maxQueueLength)
	case len(params.SubmissionHash) != 64:
		return Job{}, errors.New("submission hash must be a SHA-256 hexadecimal string")
	case !json.Valid(params.Payload):
		return Job{}, errors.New("payload must be valid JSON")
	case params.Priority < minimumPriority || params.Priority > maximumPriority:
		return Job{}, fmt.Errorf("priority must be between %d and %d", minimumPriority, maximumPriority)
	case params.MaximumAttempts < 1 || params.MaximumAttempts > maxAttemptsLimit:
		return Job{}, fmt.Errorf("maximumAttempts must be between 1 and %d", maxAttemptsLimit)
	}

	id, err := uuid.NewV7() // UUIDv7 returns time-ordered UUID and an error
	if err != nil {
		return Job{}, fmt.Errorf("generate job ID: %w", err)
	}

	now := params.Now.UTC()
	payload := append(json.RawMessage(nil), params.Payload...) // keep as raw JSON since scheduler does not need the payload content

	return Job{
		ID:              id,
		IdempotencyKey:  idempotencyKey,
		SubmissionHash:  params.SubmissionHash,
		Queue:           queue,
		Priority:        params.Priority,
		Payload:         payload,
		State:           StateQueued,
		AttemptCount:    0,
		MaximumAttempts: params.MaximumAttempts,
		AvailableAt:     now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}
