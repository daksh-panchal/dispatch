package demoapi

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/daksh-panchal/work-flow/internal/demo"
	"github.com/google/uuid"
)

const maxRequestBodyBytes = 1 << 20

//go:embed openapi.json
var openAPIDocument []byte

type SchedulerGateway interface {
	Ready(context.Context) error
	OpenEventStream(context.Context, string, int64, string) (*http.Response, error)
}

type Handler struct {
	manager   *demo.Manager
	scheduler SchedulerGateway
	logger    *slog.Logger
}

func New(manager *demo.Manager, scheduler SchedulerGateway, logger *slog.Logger) http.Handler {
	handler := &Handler{manager: manager, scheduler: scheduler, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", handler.live)
	mux.HandleFunc("GET /health/ready", handler.ready)
	mux.HandleFunc("GET /openapi/v1.json", handler.openAPI)
	mux.HandleFunc("GET /api/v1/demo/runs", handler.listRuns)
	mux.HandleFunc("POST /api/v1/demo/runs", handler.startRun)
	mux.HandleFunc("GET /api/v1/demo/runs/{runId}", handler.getRun)
	mux.HandleFunc("GET /api/v1/demo/runs/{runId}/snapshot", handler.snapshot)
	mux.HandleFunc("POST /api/v1/demo/runs/{runId}/workers", handler.addWorkers)
	mux.HandleFunc("POST /api/v1/demo/runs/{runId}/workers/{workerId}/kill", handler.killWorker)
	mux.HandleFunc("GET /api/v1/demo/runs/{runId}/events/stream", handler.streamEvents)
	return handler.recoverPanics(handler.logRequests(mux))
}

type startRunRequest struct {
	JobCount       int   `json:"jobCount"`
	WorkerCount    int   `json:"workerCount"`
	WorkDurationMS int64 `json:"workDurationMs"`
}

type addWorkersRequest struct {
	Count int `json:"count"`
}

type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func (handler *Handler) startRun(writer http.ResponseWriter, request *http.Request) {
	var body startRunRequest
	if err := decodeJSON(writer, request, &body); err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid demo run", err.Error())
		return
	}
	run, err := handler.manager.Start(request.Context(), demo.StartRunInput{
		JobCount:     body.JobCount,
		WorkerCount:  body.WorkerCount,
		WorkDuration: time.Duration(body.WorkDurationMS) * time.Millisecond,
	})
	if errors.Is(err, demo.ErrInvalidRun) {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid demo run", err.Error())
		return
	}
	if err != nil {
		handler.logger.ErrorContext(request.Context(), "start demo run", "run_id", run.ID, "error", err)
		handler.writeProblem(writer, http.StatusBadGateway, "Could not start demo run", err.Error())
		return
	}
	writer.Header().Set("Location", "/api/v1/demo/runs/"+run.ID.String())
	handler.writeJSON(writer, http.StatusCreated, run)
}

func (handler *Handler) listRuns(writer http.ResponseWriter, _ *http.Request) {
	handler.writeJSON(writer, http.StatusOK, map[string]any{"items": handler.manager.List()})
}

func (handler *Handler) getRun(writer http.ResponseWriter, request *http.Request) {
	runID, ok := handler.pathUUID(writer, request, "runId", "run ID")
	if !ok {
		return
	}
	run, err := handler.manager.Get(runID)
	if errors.Is(err, demo.ErrRunNotFound) {
		handler.writeProblem(writer, http.StatusNotFound, "Demo run not found", "")
		return
	}
	handler.writeJSON(writer, http.StatusOK, run)
}

func (handler *Handler) snapshot(writer http.ResponseWriter, request *http.Request) {
	runID, ok := handler.pathUUID(writer, request, "runId", "run ID")
	if !ok {
		return
	}
	snapshot, err := handler.manager.Snapshot(request.Context(), runID)
	if errors.Is(err, demo.ErrRunNotFound) {
		handler.writeProblem(writer, http.StatusNotFound, "Demo run not found", "")
		return
	}
	if err != nil {
		handler.logger.ErrorContext(request.Context(), "read demo snapshot", "run_id", runID, "error", err)
		handler.writeProblem(writer, http.StatusBadGateway, "Could not read scheduler state", err.Error())
		return
	}
	handler.writeJSON(writer, http.StatusOK, snapshot)
}

func (handler *Handler) addWorkers(writer http.ResponseWriter, request *http.Request) {
	runID, ok := handler.pathUUID(writer, request, "runId", "run ID")
	if !ok {
		return
	}
	var body addWorkersRequest
	if err := decodeJSON(writer, request, &body); err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid worker request", err.Error())
		return
	}
	run, err := handler.manager.AddWorkers(request.Context(), runID, body.Count)
	switch {
	case errors.Is(err, demo.ErrRunNotFound):
		handler.writeProblem(writer, http.StatusNotFound, "Demo run not found", "")
	case errors.Is(err, demo.ErrInvalidRun):
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid worker request", err.Error())
	case err != nil:
		handler.logger.ErrorContext(request.Context(), "add demo workers", "run_id", runID, "error", err)
		handler.writeProblem(writer, http.StatusBadGateway, "Could not add demo workers", err.Error())
	default:
		handler.writeJSON(writer, http.StatusCreated, run)
	}
}

func (handler *Handler) killWorker(writer http.ResponseWriter, request *http.Request) {
	runID, ok := handler.pathUUID(writer, request, "runId", "run ID")
	if !ok {
		return
	}
	workerID, ok := handler.pathUUID(writer, request, "workerId", "worker ID")
	if !ok {
		return
	}
	run, err := handler.manager.KillWorker(runID, workerID)
	switch {
	case errors.Is(err, demo.ErrRunNotFound):
		handler.writeProblem(writer, http.StatusNotFound, "Demo run not found", "")
	case errors.Is(err, demo.ErrWorkerNotFound):
		handler.writeProblem(writer, http.StatusNotFound, "Demo worker not found", "")
	default:
		handler.writeJSON(writer, http.StatusOK, run)
	}
}

func (handler *Handler) streamEvents(writer http.ResponseWriter, request *http.Request) {
	runID, ok := handler.pathUUID(writer, request, "runId", "run ID")
	if !ok {
		return
	}
	run, err := handler.manager.Get(runID)
	if errors.Is(err, demo.ErrRunNotFound) {
		handler.writeProblem(writer, http.StatusNotFound, "Demo run not found", "")
		return
	}
	afterSequence := int64(0)
	if value := request.URL.Query().Get("afterSequence"); value != "" {
		afterSequence, err = strconv.ParseInt(value, 10, 64)
		if err != nil || afterSequence < 0 {
			handler.writeProblem(writer, http.StatusBadRequest, "Invalid event cursor", "afterSequence must be a non-negative integer")
			return
		}
	}
	response, err := handler.scheduler.OpenEventStream(
		request.Context(),
		run.Queue,
		afterSequence,
		request.Header.Get("Last-Event-ID"),
	)
	if err != nil {
		if request.Context().Err() == nil {
			handler.logger.ErrorContext(request.Context(), "open demo event stream", "run_id", runID, "error", err)
			handler.writeProblem(writer, http.StatusBadGateway, "Could not open scheduler event stream", err.Error())
		}
		return
	}
	defer response.Body.Close()

	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)
	flusher, canFlush := writer.(http.Flusher)
	if canFlush {
		flusher.Flush()
	}
	buffer := make([]byte, 4<<10)
	for {
		read, readErr := response.Body.Read(buffer)
		if read > 0 {
			if _, writeErr := writer.Write(buffer[:read]); writeErr != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && request.Context().Err() == nil {
				handler.logger.DebugContext(request.Context(), "demo event stream closed", "run_id", runID, "error", readErr)
			}
			return
		}
	}
}

func (handler *Handler) live(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte("Healthy"))
}

func (handler *Handler) openAPI(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(openAPIDocument)
}

func (handler *Handler) ready(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	if err := handler.scheduler.Ready(ctx); err != nil {
		handler.writeProblem(writer, http.StatusServiceUnavailable, "Not ready", "Work-Flow is unavailable")
		return
	}
	handler.live(writer, request)
}

func (handler *Handler) pathUUID(
	writer http.ResponseWriter,
	request *http.Request,
	parameter string,
	label string,
) (uuid.UUID, bool) {
	parsed, err := uuid.Parse(request.PathValue(parameter))
	if err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid "+label, label+" must be a UUID")
		return uuid.Nil, false
	}
	return parsed, true
}

func (handler *Handler) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				handler.logger.ErrorContext(request.Context(), "demo API panic", "panic", recovered)
				handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
			}
		}()
		next.ServeHTTP(writer, request)
	})
}

func (handler *Handler) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		next.ServeHTTP(writer, request)
		handler.logger.InfoContext(
			request.Context(),
			"demo HTTP request",
			"method", request.Method,
			"route", request.Pattern,
			"duration_ms", time.Since(started).Milliseconds(),
		)
	})
}

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

func (handler *Handler) writeProblem(writer http.ResponseWriter, status int, title, detail string) {
	handler.writeJSON(writer, status, problem{
		Type: "about:blank", Title: title, Status: status, Detail: detail,
	})
}

func (handler *Handler) writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		handler.logger.Error("encode demo HTTP response", "error", err)
	}
}
