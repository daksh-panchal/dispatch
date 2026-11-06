package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/daksh-panchal/dispatch/internal/job"
	"github.com/daksh-panchal/dispatch/internal/readmodel"
	"github.com/google/uuid"
)

const eventStreamPollInterval = 500 * time.Millisecond

type Reader interface {
	ListJobs(context.Context, readmodel.JobListParams) (readmodel.JobPage, error)
	ListWorkers(context.Context, readmodel.WorkerListParams) (readmodel.WorkerPage, error)
	ListEvents(context.Context, readmodel.EventListParams) (readmodel.EventPage, error)
}

type jobPageResponse struct {
	Items      []jobResponse `json:"items"`
	NextCursor *uuid.UUID    `json:"nextCursor,omitempty"`
}

type workerSnapshotResponse struct {
	ID             uuid.UUID              `json:"id"`
	Name           string                 `json:"name"`
	Queue          string                 `json:"queue"`
	Status         readmodel.WorkerStatus `json:"status"`
	CurrentJobID   *uuid.UUID             `json:"currentJobId,omitempty"`
	LeaseExpiresAt *time.Time             `json:"leaseExpiresAt,omitempty"`
	LastSeenAt     time.Time              `json:"lastSeenAt"`
	CreatedAt      time.Time              `json:"createdAt"`
	UpdatedAt      time.Time              `json:"updatedAt"`
}

type workerPageResponse struct {
	Items      []workerSnapshotResponse `json:"items"`
	NextCursor *uuid.UUID               `json:"nextCursor,omitempty"`
}

type eventPageResponse struct {
	Items        []readmodel.EventRecord `json:"items"`
	NextSequence *int64                  `json:"nextSequence,omitempty"`
}

func (handler *Handler) listJobs(writer http.ResponseWriter, request *http.Request) {
	params, err := parseJobListParams(request)
	if err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid job query", err.Error())
		return
	}
	page, err := handler.reader.ListJobs(request.Context(), params)
	if err != nil {
		handler.writeReadError(writer, request, "list jobs", err)
		return
	}

	items := make([]jobResponse, 0, len(page.Items))
	for _, item := range page.Items {
		items = append(items, responseFromJob(item))
	}
	handler.writeJSON(writer, http.StatusOK, jobPageResponse{Items: items, NextCursor: page.NextCursor})
}

func (handler *Handler) listWorkers(writer http.ResponseWriter, request *http.Request) {
	params, err := parseWorkerListParams(request)
	if err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid worker query", err.Error())
		return
	}
	page, err := handler.reader.ListWorkers(request.Context(), params)
	if err != nil {
		handler.writeReadError(writer, request, "list workers", err)
		return
	}

	items := make([]workerSnapshotResponse, 0, len(page.Items))
	for _, item := range page.Items {
		items = append(items, workerSnapshotResponse{
			ID:             item.Worker.ID,
			Name:           item.Worker.Name,
			Queue:          item.Worker.Queue,
			Status:         item.Status,
			CurrentJobID:   item.CurrentJobID,
			LeaseExpiresAt: item.LeaseExpiresAt,
			LastSeenAt:     item.Worker.LastSeenAt,
			CreatedAt:      item.Worker.CreatedAt,
			UpdatedAt:      item.Worker.UpdatedAt,
		})
	}
	handler.writeJSON(writer, http.StatusOK, workerPageResponse{Items: items, NextCursor: page.NextCursor})
}

func (handler *Handler) listEvents(writer http.ResponseWriter, request *http.Request) {
	params, err := parseEventListParams(request)
	if err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid event query", err.Error())
		return
	}
	page, err := handler.reader.ListEvents(request.Context(), params)
	if err != nil {
		handler.writeReadError(writer, request, "list events", err)
		return
	}
	handler.writeJSON(writer, http.StatusOK, eventPageResponse{
		Items:        page.Items,
		NextSequence: page.NextSequence,
	})
}

func (handler *Handler) streamEvents(writer http.ResponseWriter, request *http.Request) {
	params, err := parseEventListParams(request)
	if err != nil {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid event stream query", err.Error())
		return
	}
	if request.URL.Query().Get("afterSequence") == "" {
		if lastID := request.Header.Get("Last-Event-ID"); lastID != "" {
			params.AfterSequence, err = parseNonNegativeInt64(lastID, "Last-Event-ID")
			if err != nil {
				handler.writeProblem(writer, http.StatusBadRequest, "Invalid event stream cursor", err.Error())
				return
			}
		}
	}
	params.Limit = 100

	flusher, ok := writer.(http.Flusher)
	if !ok {
		handler.writeProblem(writer, http.StatusInternalServerError, "Event stream unavailable", "streaming is not supported")
		return
	}
	controller := http.NewResponseController(writer)
	if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		handler.logger.WarnContext(request.Context(), "disable event stream write deadline", "error", err)
	}

	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()

	poll := time.NewTicker(eventStreamPollInterval)
	defer poll.Stop()
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()

	for {
		page, err := handler.reader.ListEvents(request.Context(), params)
		if err != nil {
			if request.Context().Err() == nil {
				handler.logger.ErrorContext(request.Context(), "stream events", "error", err)
			}
			return
		}
		for _, event := range page.Items {
			encoded, err := json.Marshal(event)
			if err != nil {
				handler.logger.ErrorContext(request.Context(), "encode streamed event", "error", err)
				return
			}
			if _, err := fmt.Fprintf(writer, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Type, encoded); err != nil {
				return
			}
			params.AfterSequence = event.Sequence
		}
		if len(page.Items) > 0 {
			flusher.Flush()
			continue
		}

		select {
		case <-request.Context().Done():
			return
		case <-poll.C:
		case <-heartbeat.C:
			if _, err := fmt.Fprint(writer, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func parseJobListParams(request *http.Request) (readmodel.JobListParams, error) {
	limit, err := parseLimit(request)
	if err != nil {
		return readmodel.JobListParams{}, err
	}
	cursor, err := parseOptionalUUID(request.URL.Query().Get("cursor"), "cursor")
	if err != nil {
		return readmodel.JobListParams{}, err
	}
	return readmodel.JobListParams{
		Queue:  request.URL.Query().Get("queue"),
		State:  job.State(request.URL.Query().Get("state")),
		Cursor: cursor,
		Limit:  limit,
	}, nil
}

func parseWorkerListParams(request *http.Request) (readmodel.WorkerListParams, error) {
	limit, err := parseLimit(request)
	if err != nil {
		return readmodel.WorkerListParams{}, err
	}
	cursor, err := parseOptionalUUID(request.URL.Query().Get("cursor"), "cursor")
	if err != nil {
		return readmodel.WorkerListParams{}, err
	}
	return readmodel.WorkerListParams{
		Queue:  request.URL.Query().Get("queue"),
		Cursor: cursor,
		Limit:  limit,
	}, nil
}

func parseEventListParams(request *http.Request) (readmodel.EventListParams, error) {
	limit, err := parseLimit(request)
	if err != nil {
		return readmodel.EventListParams{}, err
	}
	jobID, err := parseOptionalUUID(request.URL.Query().Get("jobId"), "jobId")
	if err != nil {
		return readmodel.EventListParams{}, err
	}
	afterSequence := int64(0)
	if value := request.URL.Query().Get("afterSequence"); value != "" {
		afterSequence, err = parseNonNegativeInt64(value, "afterSequence")
		if err != nil {
			return readmodel.EventListParams{}, err
		}
	}
	return readmodel.EventListParams{
		Queue:         request.URL.Query().Get("queue"),
		JobID:         jobID,
		AfterSequence: afterSequence,
		Limit:         limit,
	}, nil
}

func parseLimit(request *http.Request) (int, error) {
	value := request.URL.Query().Get("limit")
	if value == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("limit must be a number")
	}
	return limit, nil
}

func parseOptionalUUID(value, name string) (*uuid.UUID, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("%s must be a UUID", name)
	}
	return &parsed, nil
}

func parseNonNegativeInt64(value, name string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return parsed, nil
}

func (handler *Handler) writeReadError(
	writer http.ResponseWriter,
	request *http.Request,
	operation string,
	err error,
) {
	if errors.Is(err, readmodel.ErrInvalidQuery) {
		handler.writeProblem(writer, http.StatusBadRequest, "Invalid query", err.Error())
		return
	}
	handler.logger.ErrorContext(request.Context(), operation, "error", err)
	handler.writeProblem(writer, http.StatusInternalServerError, "Internal server error", "")
}
