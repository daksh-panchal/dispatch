package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const maxErrorBodyBytes = 64 << 10

type Job struct {
	ID              uuid.UUID       `json:"id"`
	Queue           string          `json:"queue"`
	Priority        int             `json:"priority"`
	Payload         json.RawMessage `json:"payload"`
	State           string          `json:"state"`
	AttemptCount    int             `json:"attemptCount"`
	MaximumAttempts int             `json:"maximumAttempts"`
	WorkerID        *uuid.UUID      `json:"workerId,omitempty"`
	LeaseExpiresAt  *time.Time      `json:"leaseExpiresAt,omitempty"`
	Result          json.RawMessage `json:"result,omitempty"`
	LastError       *string         `json:"lastError,omitempty"`
	CreatedAt       time.Time       `json:"createdAt"`
	UpdatedAt       time.Time       `json:"updatedAt"`
}

type SchedulerWorker struct {
	ID             uuid.UUID  `json:"id"`
	Name           string     `json:"name"`
	Queue          string     `json:"queue"`
	Status         string     `json:"status"`
	CurrentJobID   *uuid.UUID `json:"currentJobId,omitempty"`
	LeaseExpiresAt *time.Time `json:"leaseExpiresAt,omitempty"`
	LastSeenAt     time.Time  `json:"lastSeenAt"`
}

type Claim struct {
	Job        Job       `json:"job"`
	LeaseToken uuid.UUID `json:"leaseToken"`
}

type SubmitJobInput struct {
	IdempotencyKey  string
	Queue           string
	Priority        int
	Payload         json.RawMessage
	MaximumAttempts int
}

type APIError struct {
	Status int
	Detail string
}

func (err *APIError) Error() string {
	if err.Detail == "" {
		return fmt.Sprintf("Dispatch returned HTTP %d", err.Status)
	}
	return fmt.Sprintf("Dispatch returned HTTP %d: %s", err.Status, err.Detail)
}

type HTTPClient struct {
	baseURL      *url.URL
	client       *http.Client
	streamClient *http.Client
}

func NewHTTPClient(baseURL string) (*HTTPClient, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("DISPATCH_API_URL must be an absolute URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("DISPATCH_API_URL must use http or https")
	}
	return &HTTPClient{
		baseURL:      parsed,
		client:       &http.Client{Timeout: 10 * time.Second},
		streamClient: &http.Client{},
	}, nil
}

func (client *HTTPClient) Ready(ctx context.Context) error {
	request, err := client.request(ctx, http.MethodGet, "/health/ready", nil)
	if err != nil {
		return err
	}
	response, err := client.client.Do(request)
	if err != nil {
		return fmt.Errorf("check Dispatch readiness: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return decodeAPIError(response)
	}
	return nil
}

func (client *HTTPClient) SubmitJob(ctx context.Context, input SubmitJobInput) (Job, error) {
	body := struct {
		Queue           string          `json:"queue"`
		Priority        int             `json:"priority"`
		Payload         json.RawMessage `json:"payload"`
		MaximumAttempts int             `json:"maximumAttempts"`
	}{input.Queue, input.Priority, input.Payload, input.MaximumAttempts}
	request, err := client.jsonRequest(ctx, http.MethodPost, "/api/v1/jobs", body)
	if err != nil {
		return Job{}, err
	}
	request.Header.Set("Idempotency-Key", input.IdempotencyKey)
	var stored Job
	if err := client.doJSON(request, &stored, http.StatusOK, http.StatusCreated); err != nil {
		return Job{}, fmt.Errorf("submit demo job: %w", err)
	}
	return stored, nil
}

func (client *HTTPClient) RegisterWorker(ctx context.Context, name, queue string) (SchedulerWorker, error) {
	request, err := client.jsonRequest(ctx, http.MethodPost, "/api/v1/workers", map[string]string{
		"name": name, "queue": queue,
	})
	if err != nil {
		return SchedulerWorker{}, err
	}
	var registered SchedulerWorker
	if err := client.doJSON(request, &registered, http.StatusCreated); err != nil {
		return SchedulerWorker{}, fmt.Errorf("register demo worker: %w", err)
	}
	return registered, nil
}

func (client *HTTPClient) Claim(ctx context.Context, workerID uuid.UUID) (*Claim, error) {
	request, err := client.request(
		ctx,
		http.MethodPost,
		"/api/v1/workers/"+workerID.String()+"/claims",
		nil,
	)
	if err != nil {
		return nil, err
	}
	response, err := client.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("claim demo job: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, decodeAPIError(response)
	}
	var claimed Claim
	if err := json.NewDecoder(response.Body).Decode(&claimed); err != nil {
		return nil, fmt.Errorf("decode claimed job: %w", err)
	}
	return &claimed, nil
}

func (client *HTTPClient) RenewLease(
	ctx context.Context,
	workerID uuid.UUID,
	jobID uuid.UUID,
	leaseToken uuid.UUID,
) error {
	path := fmt.Sprintf("/api/v1/workers/%s/jobs/%s/lease", workerID, jobID)
	request, err := client.jsonRequest(ctx, http.MethodPost, path, map[string]uuid.UUID{
		"leaseToken": leaseToken,
	})
	if err != nil {
		return err
	}
	if err := client.doJSON(request, nil, http.StatusOK); err != nil {
		return fmt.Errorf("renew demo job lease: %w", err)
	}
	return nil
}

func (client *HTTPClient) Complete(
	ctx context.Context,
	workerID uuid.UUID,
	jobID uuid.UUID,
	leaseToken uuid.UUID,
	result json.RawMessage,
) error {
	path := fmt.Sprintf("/api/v1/workers/%s/jobs/%s/complete", workerID, jobID)
	request, err := client.jsonRequest(ctx, http.MethodPost, path, struct {
		LeaseToken uuid.UUID       `json:"leaseToken"`
		Result     json.RawMessage `json:"result"`
	}{leaseToken, result})
	if err != nil {
		return err
	}
	if err := client.doJSON(request, nil, http.StatusOK); err != nil {
		return fmt.Errorf("complete demo job: %w", err)
	}
	return nil
}

func (client *HTTPClient) ListJobs(ctx context.Context, queue string) ([]Job, error) {
	var jobs []Job
	var cursor string
	for {
		query := url.Values{"queue": {queue}, "limit": {"100"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		request, err := client.request(ctx, http.MethodGet, "/api/v1/jobs?"+query.Encode(), nil)
		if err != nil {
			return nil, err
		}
		var page struct {
			Items      []Job      `json:"items"`
			NextCursor *uuid.UUID `json:"nextCursor"`
		}
		if err := client.doJSON(request, &page, http.StatusOK); err != nil {
			return nil, fmt.Errorf("list demo jobs: %w", err)
		}
		jobs = append(jobs, page.Items...)
		if page.NextCursor == nil {
			return jobs, nil
		}
		cursor = page.NextCursor.String()
	}
}

func (client *HTTPClient) ListWorkers(ctx context.Context, queue string) ([]SchedulerWorker, error) {
	var workers []SchedulerWorker
	var cursor string
	for {
		query := url.Values{"queue": {queue}, "limit": {"100"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		request, err := client.request(ctx, http.MethodGet, "/api/v1/workers?"+query.Encode(), nil)
		if err != nil {
			return nil, err
		}
		var page struct {
			Items      []SchedulerWorker `json:"items"`
			NextCursor *uuid.UUID        `json:"nextCursor"`
		}
		if err := client.doJSON(request, &page, http.StatusOK); err != nil {
			return nil, fmt.Errorf("list demo workers: %w", err)
		}
		workers = append(workers, page.Items...)
		if page.NextCursor == nil {
			return workers, nil
		}
		cursor = page.NextCursor.String()
	}
}

func (client *HTTPClient) OpenEventStream(
	ctx context.Context,
	queue string,
	afterSequence int64,
	lastEventID string,
) (*http.Response, error) {
	query := url.Values{"queue": {queue}}
	if lastEventID == "" {
		query.Set("afterSequence", strconv.FormatInt(afterSequence, 10))
	}
	request, err := client.request(ctx, http.MethodGet, "/api/v1/events/stream?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "text/event-stream")
	if lastEventID != "" {
		request.Header.Set("Last-Event-ID", lastEventID)
	}
	response, err := client.streamClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("open Dispatch event stream: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		return nil, decodeAPIError(response)
	}
	return response, nil
}

func (client *HTTPClient) doJSON(request *http.Request, destination any, statuses ...int) error {
	response, err := client.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	accepted := false
	for _, status := range statuses {
		if response.StatusCode == status {
			accepted = true
			break
		}
	}
	if !accepted {
		return decodeAPIError(response)
	}
	if destination == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		return fmt.Errorf("decode Dispatch response: %w", err)
	}
	return nil
}

func (client *HTTPClient) jsonRequest(
	ctx context.Context,
	method string,
	path string,
	value any,
) (*http.Request, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode Dispatch request: %w", err)
	}
	request, err := client.request(ctx, method, path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	return request, nil
}

func (client *HTTPClient) request(
	ctx context.Context,
	method string,
	path string,
	body io.Reader,
) (*http.Request, error) {
	resolved, err := client.baseURL.Parse(path)
	if err != nil {
		return nil, fmt.Errorf("build Dispatch URL: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, method, resolved.String(), body)
	if err != nil {
		return nil, fmt.Errorf("build Dispatch request: %w", err)
	}
	return request, nil
}

func decodeAPIError(response *http.Response) error {
	body, err := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes))
	if err != nil {
		return &APIError{Status: response.StatusCode}
	}
	var problem struct {
		Title  string `json:"title"`
		Detail string `json:"detail"`
	}
	if json.Unmarshal(body, &problem) == nil {
		detail := problem.Detail
		if detail == "" {
			detail = problem.Title
		}
		return &APIError{Status: response.StatusCode, Detail: detail}
	}
	detail := strings.TrimSpace(string(body))
	return &APIError{Status: response.StatusCode, Detail: detail}
}

func IsConflict(err error) bool {
	var apiError *APIError
	return errors.As(err, &apiError) && apiError.Status == http.StatusConflict
}
