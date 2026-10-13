package httpapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/daksh-panchal/work-flow/internal/job"
	"github.com/daksh-panchal/work-flow/internal/worker"
	"github.com/google/uuid"
)

const maxRequestBodyBytes = 1 << 20

//go:embed openapi.json
var openAPIDocument []byte

type Pinger interface {
	Ping(context.Context) error
}

type Handler struct {
	jobs    *job.Service
	workers *worker.Service
	pinger  Pinger
	logger  *slog.Logger
}

func New(jobs *job.Service, workers *worker.Service, pinger Pinger, logger *slog.Logger) http.Handler {
	handler := &Handler{jobs: jobs, workers: workers, pinger: pinger, logger: logger}
	mux := http.NewServeMux() // HTTP router that matches requests to handler functions
	mux.HandleFunc("GET /health/live", handler.live)
	mux.HandleFunc("GET /health/ready", handler.ready)
	mux.HandleFunc("GET /openapi/v1.json", handler.openAPI)
	mux.HandleFunc("POST /api/v1/jobs", handler.submitJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}", handler.getJob)
	mux.HandleFunc("POST /api/v1/workers", handler.registerWorker)
	mux.HandleFunc("POST /api/v1/workers/{workerId}/heartbeat", handler.heartbeatWorker)
	mux.HandleFunc("POST /api/v1/workers/{workerId}/claims", handler.claimJob)
	mux.HandleFunc("POST /api/v1/workers/{workerId}/jobs/{jobId}/lease", handler.renewLease)
	mux.HandleFunc("POST /api/v1/workers/{workerId}/jobs/{jobId}/complete", handler.completeJob)
	mux.HandleFunc("POST /api/v1/workers/{workerId}/jobs/{jobId}/fail", handler.failJob)
	return handler.recoverPanics(mux) // Request fails with 500 Internal Server Error if panic
}

type submitJobRequest struct {
	Queue           string          `json:"queue"`
	Priority        int             `json:"priority"`
	Payload         json.RawMessage `json:"payload"`
	MaximumAttempts int             `json:"maximumAttempts"`
}

type jobResponse struct {
	ID              uuid.UUID       `json:"id"`
	Queue           string          `json:"queue"`
	Priority        int             `json:"priority"`
	Payload         json.RawMessage `json:"payload"`
	State           job.State       `json:"state"`
	AttemptCount    int             `json:"attemptCount"`
	MaximumAttempts int             `json:"maximumAttempts"`
	AvailableAt     time.Time       `json:"availableAt"`
	WorkerID        *uuid.UUID      `json:"workerId,omitempty"`
	LeaseExpiresAt  *time.Time      `json:"leaseExpiresAt,omitempty"`
	Result          json.RawMessage `json:"result,omitempty"`
	LastError       *string         `json:"lastError,omitempty"`
	FinishedAt      *time.Time      `json:"finishedAt,omitempty"`
	CreatedAt       time.Time       `json:"createdAt"`
	UpdatedAt       time.Time       `json:"updatedAt"`
}

type registerWorkerRequest struct {
	Name  string `json:"name"`
	Queue string `json:"queue"`
}

type workerResponse struct {
	ID         uuid.UUID `json:"id"`
	Name       string    `json:"name"`
	Queue      string    `json:"queue"`
	LastSeenAt time.Time `json:"lastSeenAt"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

type claimResponse struct {
	Job        jobResponse `json:"job"`
	LeaseToken uuid.UUID   `json:"leaseToken"`
}

type renewLeaseRequest struct {
	LeaseToken uuid.UUID `json:"leaseToken"`
}

type completeJobRequest struct {
	LeaseToken uuid.UUID       `json:"leaseToken"`
	Result     json.RawMessage `json:"result"`
}

type failJobRequest struct {
	LeaseToken uuid.UUID `json:"leaseToken"`
	Error      string    `json:"error"`
}

type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func (handler *Handler) submitJob(writer http.ResponseWriter, request *http.Request) {
	var body submitJobRequest
	if err := decodeJSON(writer, request, &body); err != nil { // Decode JSON request body
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid request", err.Error())
		return
	}

	stored, created, err := handler.jobs.Submit(
		request.Context(),
		request.Header.Get("Idempotency-Key"), // get idempotency key from request header
		job.SubmitInput{ // Map request body to the SubmitInput struct that service.go expects for job submission
			Queue:           body.Queue,
			Priority:        body.Priority,
			Payload:         body.Payload,
			MaximumAttempts: body.MaximumAttempts,
		},
	)
	if err != nil {
		switch { //Error handling for job submission errors. Return correct HTTP status code and error msg
		case errors.Is(err, job.ErrInvalidSubmission):
			handler.writeProblem(writer, http.StatusBadRequest, "Invalid job submission", err.Error())
		case errors.Is(err, job.ErrIdempotencyConflict):
			handler.writeProblem(writer, http.StatusConflict, "Idempotency conflict", err.Error())
		default:
			handler.logger.Error("submit job", "error", err)
			handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
		}
		return
	}

	status := http.StatusOK // 200 OK if job already exists (identical retry)
	if created {
		status = http.StatusCreated // 201 created if job is newly created
		writer.Header().Set("Location", "/api/v1/jobs/"+stored.ID.String())
	}
	handler.writeJSON(writer, status, responseFromJob(stored))
}

func (handler *Handler) getJob(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(request.PathValue("id")) // Get job ID from URL
	if err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid job ID", "job ID must be a UUID")
		return
	}

	stored, err := handler.jobs.Get(request.Context(), id) // Retrieve job from service layer using getJob method
	if errors.Is(err, job.ErrNotFound) {                   // Job not found error, return 404 Not Found
		handler.writeProblem(writer, http.StatusNotFound, "Job not found", "")
		return
	}
	if err != nil { // Return 500 Internal Server Error for any other errors
		handler.logger.Error("get job", "job_id", id, "error", err)
		handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
		return
	}
	// Write JSON response with job details and 200 OK status if job successfully retrieved
	handler.writeJSON(writer, http.StatusOK, responseFromJob(stored))
}

func (handler *Handler) registerWorker(writer http.ResponseWriter, request *http.Request) {
	var body registerWorkerRequest
	if err := decodeJSON(writer, request, &body); err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid request", err.Error())
		return
	}

	registered, err := handler.workers.Register(request.Context(), body.Name, body.Queue)
	if errors.Is(err, worker.ErrInvalidRegistration) {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid worker registration", err.Error())
		return
	}
	if err != nil {
		handler.logger.Error("register worker", "error", err)
		handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
		return
	}

	handler.writeJSON(writer, http.StatusCreated, responseFromWorker(registered))
}

func (handler *Handler) heartbeatWorker(writer http.ResponseWriter, request *http.Request) {
	workerID, ok := handler.parsePathUUID(writer, request, "workerId", "worker ID")
	if !ok {
		return
	}

	registered, err := handler.workers.Heartbeat(request.Context(), workerID)
	if errors.Is(err, worker.ErrNotFound) {
		handler.writeProblem(writer, http.StatusNotFound, "Worker not found", "")
		return
	}
	if err != nil {
		handler.logger.Error("heartbeat worker", "worker_id", workerID, "error", err)
		handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
		return
	}
	handler.writeJSON(writer, http.StatusOK, responseFromWorker(registered))
}

func (handler *Handler) claimJob(writer http.ResponseWriter, request *http.Request) {
	workerID, ok := handler.parsePathUUID(writer, request, "workerId", "worker ID")
	if !ok {
		return
	}

	claimed, found, err := handler.workers.Claim(request.Context(), workerID)
	if errors.Is(err, worker.ErrNotFound) {
		handler.writeProblem(writer, http.StatusNotFound, "Worker not found", "")
		return
	}
	if err != nil {
		handler.logger.Error("claim job", "worker_id", workerID, "error", err)
		handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
		return
	}
	if !found {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	if claimed.LeaseToken == nil {
		handler.logger.Error("claimed job has no lease token", "job_id", claimed.ID)
		handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
		return
	}

	handler.writeJSON(writer, http.StatusOK, claimResponse{
		Job:        responseFromJob(claimed),
		LeaseToken: *claimed.LeaseToken,
	})
}

func (handler *Handler) renewLease(writer http.ResponseWriter, request *http.Request) {
	workerID, ok := handler.parsePathUUID(writer, request, "workerId", "worker ID")
	if !ok {
		return
	}
	jobID, ok := handler.parsePathUUID(writer, request, "jobId", "job ID")
	if !ok {
		return
	}

	var body renewLeaseRequest
	if err := decodeJSON(writer, request, &body); err != nil || body.LeaseToken == uuid.Nil {
		detail := "leaseToken must be a UUID"
		if err != nil {
			detail = err.Error()
		}
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid lease renewal", detail)
		return
	}

	renewed, err := handler.workers.RenewLease(request.Context(), workerID, jobID, body.LeaseToken)
	switch {
	case errors.Is(err, worker.ErrNotFound):
		handler.writeProblem(writer, http.StatusNotFound, "Worker not found", "")
	case errors.Is(err, job.ErrLeaseLost):
		handler.writeProblem(writer, http.StatusConflict, "Lease lost", err.Error())
	case err != nil:
		handler.logger.Error("renew job lease", "worker_id", workerID, "job_id", jobID, "error", err)
		handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
	default:
		handler.writeJSON(writer, http.StatusOK, responseFromJob(renewed))
	}
}

func (handler *Handler) completeJob(writer http.ResponseWriter, request *http.Request) {
	workerID, jobID, ok := handler.parseWorkerAndJobIDs(writer, request)
	if !ok {
		return
	}

	var body completeJobRequest
	if err := decodeJSON(writer, request, &body); err != nil || body.LeaseToken == uuid.Nil {
		detail := "leaseToken must be a UUID"
		if err != nil {
			detail = err.Error()
		}
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid completion", detail)
		return
	}

	completed, err := handler.workers.Complete(
		request.Context(),
		workerID,
		jobID,
		body.LeaseToken,
		body.Result,
	)
	handler.writeOutcome(writer, "complete job", workerID, jobID, completed, err)
}

func (handler *Handler) failJob(writer http.ResponseWriter, request *http.Request) {
	workerID, jobID, ok := handler.parseWorkerAndJobIDs(writer, request)
	if !ok {
		return
	}

	var body failJobRequest
	if err := decodeJSON(writer, request, &body); err != nil || body.LeaseToken == uuid.Nil {
		detail := "leaseToken must be a UUID"
		if err != nil {
			detail = err.Error()
		}
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid failure", detail)
		return
	}

	failed, err := handler.workers.Fail(
		request.Context(),
		workerID,
		jobID,
		body.LeaseToken,
		body.Error,
	)
	handler.writeOutcome(writer, "fail job", workerID, jobID, failed, err)
}

func (handler *Handler) live(writer http.ResponseWriter, _ *http.Request) { // Confirm HTTP app running
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte("Healthy"))
}

func (handler *Handler) ready(writer http.ResponseWriter, request *http.Request) { // Confirm HTTP app ready to accept requests
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel() // Cancel context after 2 seconds to avoid hanging requests

	if err := handler.pinger.Ping(ctx); err != nil { // Ping db to see if ready
		handler.writeProblem(writer, http.StatusServiceUnavailable, "Not ready", "PostgreSQL is unavailable")
		return
	}
	handler.live(writer, request)
}

func (handler *Handler) openAPI(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(openAPIDocument)
}

func (handler *Handler) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() { // Recover from request panics and return 500 Internal Server Error
			if recovered := recover(); recovered != nil {
				handler.logger.Error("http panic", "panic", recovered)
				handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
			}
		}()
		next.ServeHTTP(writer, request)
	})
}

// Decode JSON to extract request body into destination struct, with error handling
func decodeJSON(writer http.ResponseWriter, request *http.Request, destination any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("request body must contain valid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func responseFromJob(stored job.Job) jobResponse {
	return jobResponse{
		ID:              stored.ID,
		Queue:           stored.Queue,
		Priority:        stored.Priority,
		Payload:         stored.Payload,
		State:           stored.State,
		AttemptCount:    stored.AttemptCount,
		MaximumAttempts: stored.MaximumAttempts,
		AvailableAt:     stored.AvailableAt,
		WorkerID:        stored.WorkerID,
		LeaseExpiresAt:  stored.LeaseExpiresAt,
		Result:          stored.Result,
		LastError:       stored.LastError,
		FinishedAt:      stored.FinishedAt,
		CreatedAt:       stored.CreatedAt,
		UpdatedAt:       stored.UpdatedAt,
	}
}

func (handler *Handler) parseWorkerAndJobIDs(
	writer http.ResponseWriter,
	request *http.Request,
) (uuid.UUID, uuid.UUID, bool) {
	workerID, ok := handler.parsePathUUID(writer, request, "workerId", "worker ID")
	if !ok {
		return uuid.Nil, uuid.Nil, false
	}
	jobID, ok := handler.parsePathUUID(writer, request, "jobId", "job ID")
	if !ok {
		return uuid.Nil, uuid.Nil, false
	}
	return workerID, jobID, true
}

func (handler *Handler) writeOutcome(
	writer http.ResponseWriter,
	operation string,
	workerID uuid.UUID,
	jobID uuid.UUID,
	stored job.Job,
	err error,
) {
	switch {
	case errors.Is(err, worker.ErrNotFound):
		handler.writeProblem(writer, http.StatusNotFound, "Worker not found", "")
	case errors.Is(err, job.ErrNotFound):
		handler.writeProblem(writer, http.StatusNotFound, "Job not found", "")
	case errors.Is(err, job.ErrInvalidOutcome):
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid job outcome", err.Error())
	case errors.Is(err, job.ErrLeaseLost):
		handler.writeProblem(writer, http.StatusConflict, "Lease lost", err.Error())
	case err != nil:
		handler.logger.Error(operation, "worker_id", workerID, "job_id", jobID, "error", err)
		handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
	default:
		handler.writeJSON(writer, http.StatusOK, responseFromJob(stored))
	}
}

func responseFromWorker(registered worker.Worker) workerResponse {
	return workerResponse{
		ID:         registered.ID,
		Name:       registered.Name,
		Queue:      registered.Queue,
		LastSeenAt: registered.LastSeenAt,
		CreatedAt:  registered.CreatedAt,
		UpdatedAt:  registered.UpdatedAt,
	}
}

func (handler *Handler) parsePathUUID(
	writer http.ResponseWriter,
	request *http.Request,
	parameter string,
	label string,
) (uuid.UUID, bool) {
	id, err := uuid.Parse(request.PathValue(parameter))
	if err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid "+label, label+" must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

func (handler *Handler) writeProblem(writer http.ResponseWriter, status int, title, detail string) {
	handler.writeJSON(writer, status, problem{
		Type:   "about:blank",
		Title:  title,
		Status: status,
		Detail: detail,
	})
}

func (handler *Handler) writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		handler.logger.Error("encode HTTP response", "error", err)
	}
}
