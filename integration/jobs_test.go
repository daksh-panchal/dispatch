package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/daksh-panchal/work-flow/internal/httpapi"
	"github.com/daksh-panchal/work-flow/internal/job"
	workflowpostgres "github.com/daksh-panchal/work-flow/internal/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestJobAPIAgainstPostgreSQL(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(
		ctx,
		"postgres:18.6",
		tcpostgres.WithDatabase("workflow_tests"),
		tcpostgres.WithUsername("workflow"),
		tcpostgres.WithPassword("workflow_tests"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start PostgreSQL container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Errorf("terminate PostgreSQL container: %v", err)
		}
	})

	connectionString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get PostgreSQL connection string: %v", err)
	}
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := workflowpostgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repository := workflowpostgres.NewJobRepository(pool)
	service := job.NewService(repository, time.Now)
	server := httptest.NewServer(httpapi.New(service, pool, logger))
	t.Cleanup(server.Close)

	t.Run("submitted job can be retrieved", func(t *testing.T) {
		key := "integration-" + uuid.NewString()
		created := submitJob(t, server.URL, key, "build", http.StatusCreated)

		response, err := http.Get(server.URL + "/api/v1/jobs/" + created.ID.String())
		if err != nil {
			t.Fatalf("GET job: %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET status = %d, want %d", response.StatusCode, http.StatusOK)
		}

		var retrieved apiJob
		decodeResponse(t, response, &retrieved)
		if retrieved.ID != created.ID || retrieved.State != "queued" {
			t.Fatalf("retrieved job = %+v, want ID %s in queued state", retrieved, created.ID)
		}
		if string(retrieved.Payload) != `{"command":"build"}` {
			t.Fatalf("retrieved payload = %s", retrieved.Payload)
		}
	})

	t.Run("repeated submission returns the original job", func(t *testing.T) {
		key := "integration-" + uuid.NewString()
		first := submitJob(t, server.URL, key, "build", http.StatusCreated)
		second := submitJob(t, server.URL, key, "build", http.StatusOK)
		if first.ID != second.ID {
			t.Fatalf("job IDs differ: %s != %s", first.ID, second.ID)
		}
	})

	t.Run("changed submission conflicts with the original key", func(t *testing.T) {
		key := "integration-" + uuid.NewString()
		_ = submitJob(t, server.URL, key, "build", http.StatusCreated)
		_ = submitJob(t, server.URL, key, "test", http.StatusConflict)
	})

	t.Run("concurrent retries create exactly one job", func(t *testing.T) {
		const requestCount = 8
		key := "integration-" + uuid.NewString()

		type result struct {
			status int
			id     uuid.UUID
			err    error
		}
		results := make(chan result, requestCount)
		var group sync.WaitGroup

		for range requestCount {
			group.Add(1)
			go func() {
				defer group.Done()
				status, stored, err := submitJobResult(server.URL, key, "build")
				results <- result{status: status, id: stored.ID, err: err}
			}()
		}
		group.Wait()
		close(results)

		createdCount := 0
		var expectedID uuid.UUID
		for result := range results {
			if result.err != nil {
				t.Fatalf("concurrent submission: %v", result.err)
			}
			if result.status == http.StatusCreated {
				createdCount++
			} else if result.status != http.StatusOK {
				t.Fatalf("concurrent status = %d, want 200 or 201", result.status)
			}
			if expectedID == uuid.Nil {
				expectedID = result.id
			} else if result.id != expectedID {
				t.Fatalf("concurrent jobs differ: %s != %s", result.id, expectedID)
			}
		}
		if createdCount != 1 {
			t.Fatalf("created responses = %d, want 1", createdCount)
		}
	})
}

type apiJob struct {
	ID      uuid.UUID       `json:"id"`
	Payload json.RawMessage `json:"payload"`
	State   string          `json:"state"`
}

func submitJob(t *testing.T, baseURL, key, command string, expectedStatus int) apiJob {
	t.Helper()
	status, stored, err := submitJobResult(baseURL, key, command)
	if err != nil {
		t.Fatal(err)
	}
	if status != expectedStatus {
		t.Fatalf("POST status = %d, want %d", status, expectedStatus)
	}
	return stored
}

func submitJobResult(baseURL, key, command string) (int, apiJob, error) {
	body, err := json.Marshal(map[string]any{
		"queue":           "default",
		"priority":        0,
		"maximumAttempts": 3,
		"payload": map[string]any{
			"command": command,
		},
	})
	if err != nil {
		return 0, apiJob{}, err
	}

	request, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/jobs", bytes.NewReader(body))
	if err != nil {
		return 0, apiJob{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, apiJob{}, err
	}
	defer response.Body.Close()

	var stored apiJob
	if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated {
		if err := json.NewDecoder(response.Body).Decode(&stored); err != nil {
			return response.StatusCode, apiJob{}, fmt.Errorf("decode job response: %w", err)
		}
	}
	return response.StatusCode, stored, nil
}

func decodeResponse(t *testing.T, response *http.Response, destination any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}
