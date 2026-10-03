package job

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Repository interface {
	CreateOrGet(context.Context, Job) (stored Job, created bool, err error)
	Get(context.Context, uuid.UUID) (Job, error)
}

type SubmitInput struct {
	Queue           string
	Priority        int
	Payload         json.RawMessage
	MaximumAttempts int
}

type Service struct {
	repository Repository
	now        func() time.Time
}

func NewService(repository Repository, now func() time.Time) *Service {
	return &Service{repository: repository, now: now}
}

func (service *Service) Submit(
	ctx context.Context,
	idempotencyKey string,
	input SubmitInput,
) (stored Job, created bool, err error) {
	// remove whitespace and ensure valid JSON for submission hash calculation:
	canonicalPayload, err := canonicalJSON(input.Payload)
	if err != nil {
		return Job{}, false, fmt.Errorf("%w: payload must be valid JSON: %v", ErrInvalidSubmission, err)
	}

	// remove whitespace for submission hash calculation:
	queue := strings.TrimSpace(input.Queue)

	hash, err := submissionHash(queue, input.Priority, input.MaximumAttempts, canonicalPayload)
	if err != nil {
		return Job{}, false, fmt.Errorf("hash submission: %w", err)
	}

	candidate, err := New(NewJobParams{
		IdempotencyKey:  idempotencyKey,
		SubmissionHash:  hash,
		Queue:           queue,
		Priority:        input.Priority,
		Payload:         canonicalPayload,
		MaximumAttempts: input.MaximumAttempts,
		Now:             service.now(),
	})
	if err != nil {
		return Job{}, false, fmt.Errorf("%w: %v", ErrInvalidSubmission, err)
	}

	stored, created, err = service.repository.CreateOrGet(ctx, candidate)
	if err != nil { // if repo fails to store/return job, return error
		return Job{}, false, fmt.Errorf("store job: %w", err)
	}
	if !created && stored.SubmissionHash != candidate.SubmissionHash {
		return Job{}, false, ErrIdempotencyConflict
	} // idempotency conflict: different submission hash on retry for same idem. key

	return stored, created, nil // return stored job, created flag, nil error if successful
}

func (service *Service) Get(ctx context.Context, id uuid.UUID) (Job, error) {
	stored, err := service.repository.Get(ctx, id)
	if err != nil {
		return Job{}, err
	}
	return stored, nil
}

func canonicalJSON(value json.RawMessage) (json.RawMessage, error) {
	if len(value) == 0 {
		return nil, fmt.Errorf("payload is required")
	}

	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()

	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("payload must contain one JSON value")
	}

	canonical, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func submissionHash(
	queue string,
	priority int,
	maximumAttempts int,
	payload json.RawMessage,
) (string, error) {
	encoded, err := json.Marshal(struct {
		Queue           string          `json:"queue"`
		Priority        int             `json:"priority"`
		MaximumAttempts int             `json:"maximumAttempts"`
		Payload         json.RawMessage `json:"payload"`
	}{
		Queue:           queue,
		Priority:        priority,
		MaximumAttempts: maximumAttempts,
		Payload:         payload,
	})
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
