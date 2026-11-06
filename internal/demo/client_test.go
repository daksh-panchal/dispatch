package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestHTTPClientUsesDispatchPublicContract(t *testing.T) {
	workerID := uuid.New()
	jobID := uuid.New()
	leaseToken := uuid.New()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/ready", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /api/v1/jobs", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Idempotency-Key") != "demo:run:job:1" {
			t.Errorf("Idempotency-Key = %q", request.Header.Get("Idempotency-Key"))
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(writer, `{"id":%q,"queue":"demo-run","state":"queued","payload":{}}`, jobID)
	})
	mux.HandleFunc("POST /api/v1/workers", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(writer, `{"id":%q,"name":"worker-1","queue":"demo-run"}`, workerID)
	})
	mux.HandleFunc("POST /api/v1/workers/{workerId}/claims", func(writer http.ResponseWriter, request *http.Request) {
		if request.PathValue("workerId") != workerID.String() {
			t.Errorf("worker ID = %q", request.PathValue("workerId"))
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(
			writer,
			`{"job":{"id":%q,"queue":"demo-run","state":"running","payload":{}},"leaseToken":%q}`,
			jobID,
			leaseToken,
		)
	})
	mux.HandleFunc("POST /api/v1/workers/{workerId}/jobs/{jobId}/complete", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"state":"succeeded"}`))
	})
	mux.HandleFunc("GET /api/v1/jobs", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("queue") != "demo-run" {
			t.Errorf("queue = %q", request.URL.Query().Get("queue"))
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(writer, `{"items":[{"id":%q,"queue":"demo-run","state":"succeeded","payload":{}}]}`, jobID)
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := NewHTTPClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := client.Ready(ctx); err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
	stored, err := client.SubmitJob(ctx, SubmitJobInput{
		IdempotencyKey:  "demo:run:job:1",
		Queue:           "demo-run",
		Payload:         json.RawMessage(`{}`),
		MaximumAttempts: 3,
	})
	if err != nil || stored.ID != jobID {
		t.Fatalf("SubmitJob() = %+v, %v", stored, err)
	}
	registered, err := client.RegisterWorker(ctx, "worker-1", "demo-run")
	if err != nil || registered.ID != workerID {
		t.Fatalf("RegisterWorker() = %+v, %v", registered, err)
	}
	claimed, err := client.Claim(ctx, workerID)
	if err != nil || claimed == nil || claimed.LeaseToken != leaseToken {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	if err := client.Complete(ctx, workerID, jobID, leaseToken, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	jobs, err := client.ListJobs(ctx, "demo-run")
	if err != nil || len(jobs) != 1 || jobs[0].State != "succeeded" {
		t.Fatalf("ListJobs() = %+v, %v", jobs, err)
	}
}

func TestHTTPClientReportsProblemDetails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/problem+json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte(`{"title":"Not ready","detail":"PostgreSQL is unavailable"}`))
	}))
	defer server.Close()
	client, err := NewHTTPClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Ready(context.Background())
	if err == nil || err.Error() != "Dispatch returned HTTP 503: PostgreSQL is unavailable" {
		t.Fatalf("Ready() error = %v", err)
	}
}
