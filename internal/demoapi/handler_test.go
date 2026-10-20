package demoapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daksh-panchal/work-flow/internal/demo"
	"github.com/google/uuid"
)

func TestHandlerValidatesRunsAndReturnsNotFound(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := demo.NewManager(context.Background(), nil, logger)
	handler := New(manager, stubGateway{}, logger)

	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/demo/runs",
		strings.NewReader(`{"jobCount":0,"workerCount":1,"workDurationMs":5000}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid run status = %d, want %d", response.Code, http.StatusBadRequest)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/demo/runs/"+uuid.NewString(), nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing run status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestHandlerServesOpenAPIAndReadiness(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := demo.NewManager(context.Background(), nil, logger)
	handler := New(manager, stubGateway{}, logger)

	for _, path := range []string{
		"/",
		"/demo.html",
		"/app.js",
		"/styles.css",
		"/components/job-card.js",
		"/health/ready",
		"/openapi/v1.json",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", path, response.Code, http.StatusOK)
		}
		if path == "/" && !strings.Contains(response.Body.String(), "Work-Flow · Demo Console") {
			t.Fatal("frontend response does not contain the demo console")
		}
	}
}

type stubGateway struct{}

func (stubGateway) Ready(context.Context) error { return nil }

func (stubGateway) OpenEventStream(
	context.Context,
	string,
	int64,
	string,
) (*http.Response, error) {
	return nil, nil
}
