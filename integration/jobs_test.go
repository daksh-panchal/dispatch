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
	"github.com/daksh-panchal/work-flow/internal/worker"
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
	jobRepository := workflowpostgres.NewJobRepository(pool)
	workerRepository := workflowpostgres.NewWorkerRepository(pool)
	jobService := job.NewService(jobRepository, time.Now)
	workerService := worker.NewService(workerRepository, jobRepository, time.Now, 30*time.Second)
	server := httptest.NewServer(httpapi.New(jobService, workerService, pool, logger))
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

	t.Run("worker registers and heartbeats", func(t *testing.T) {
		queue := "heartbeat-" + uuid.NewString()
		registered := registerWorker(t, server.URL, "builder-1", queue)

		request, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/workers/"+registered.ID.String()+"/heartbeat", nil)
		if err != nil {
			t.Fatalf("create heartbeat request: %v", err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("heartbeat worker: %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("heartbeat status = %d, want %d", response.StatusCode, http.StatusOK)
		}
		var heartbeat apiWorker
		decodeResponse(t, response, &heartbeat)
		if heartbeat.ID != registered.ID || heartbeat.LastSeenAt.Before(registered.LastSeenAt) {
			t.Fatalf("heartbeat worker = %+v, registered = %+v", heartbeat, registered)
		}
	})

	t.Run("worker claims highest priority job and renews its lease", func(t *testing.T) {
		queue := "priority-" + uuid.NewString()
		low := submitJobWithOptions(t, server.URL, "integration-"+uuid.NewString(), queue, "low", 0, http.StatusCreated)
		high := submitJobWithOptions(t, server.URL, "integration-"+uuid.NewString(), queue, "high", 10, http.StatusCreated)
		registered := registerWorker(t, server.URL, "priority-worker", queue)

		status, claim, err := claimJobResult(server.URL, registered.ID)
		if err != nil {
			t.Fatalf("claim job: %v", err)
		}
		if status != http.StatusOK {
			t.Fatalf("claim status = %d, want %d", status, http.StatusOK)
		}
		if claim.Job.ID != high.ID || claim.Job.ID == low.ID {
			t.Fatalf("claimed job = %s, want high-priority job %s", claim.Job.ID, high.ID)
		}
		if claim.Job.State != "running" || claim.Job.AttemptCount != 1 || claim.Job.WorkerID == nil || *claim.Job.WorkerID != registered.ID {
			t.Fatalf("claimed job = %+v", claim.Job)
		}

		status, _, err = renewLeaseResult(server.URL, registered.ID, claim.Job.ID, uuid.New())
		if err != nil {
			t.Fatalf("renew with wrong token: %v", err)
		}
		if status != http.StatusConflict {
			t.Fatalf("wrong-token renewal status = %d, want %d", status, http.StatusConflict)
		}

		originalExpiry := claim.Job.LeaseExpiresAt
		renewed := renewLease(t, server.URL, registered.ID, claim.Job.ID, claim.LeaseToken)
		if originalExpiry == nil || renewed.LeaseExpiresAt == nil || renewed.LeaseExpiresAt.Before(*originalExpiry) {
			t.Fatalf("renewed expiry = %v, original = %v", renewed.LeaseExpiresAt, originalExpiry)
		}
	})

	t.Run("concurrent workers cannot claim the same job", func(t *testing.T) {
		const workerCount = 8
		queue := "claims-" + uuid.NewString()
		expected := submitJobWithOptions(t, server.URL, "integration-"+uuid.NewString(), queue, "only", 0, http.StatusCreated)
		workers := make([]apiWorker, workerCount)
		for index := range workers {
			workers[index] = registerWorker(t, server.URL, fmt.Sprintf("claim-worker-%d", index), queue)
		}

		type result struct {
			status int
			claim  apiClaim
			err    error
		}
		results := make(chan result, workerCount)
		var group sync.WaitGroup
		for _, registered := range workers {
			group.Add(1)
			go func(workerID uuid.UUID) {
				defer group.Done()
				status, claim, err := claimJobResult(server.URL, workerID)
				results <- result{status: status, claim: claim, err: err}
			}(registered.ID)
		}
		group.Wait()
		close(results)

		claimedCount := 0
		for result := range results {
			if result.err != nil {
				t.Fatalf("concurrent claim: %v", result.err)
			}
			switch result.status {
			case http.StatusOK:
				claimedCount++
				if result.claim.Job.ID != expected.ID {
					t.Fatalf("claimed job = %s, want %s", result.claim.Job.ID, expected.ID)
				}
			case http.StatusNoContent:
			default:
				t.Fatalf("claim status = %d, want 200 or 204", result.status)
			}
		}
		if claimedCount != 1 {
			t.Fatalf("successful claims = %d, want 1", claimedCount)
		}
	})
}

type apiJob struct {
	ID             uuid.UUID       `json:"id"`
	Priority       int             `json:"priority"`
	Payload        json.RawMessage `json:"payload"`
	State          string          `json:"state"`
	AttemptCount   int             `json:"attemptCount"`
	WorkerID       *uuid.UUID      `json:"workerId"`
	LeaseExpiresAt *time.Time      `json:"leaseExpiresAt"`
}

type apiWorker struct {
	ID         uuid.UUID `json:"id"`
	Name       string    `json:"name"`
	Queue      string    `json:"queue"`
	LastSeenAt time.Time `json:"lastSeenAt"`
}

type apiClaim struct {
	Job        apiJob    `json:"job"`
	LeaseToken uuid.UUID `json:"leaseToken"`
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
	return submitJobResultWithOptions(baseURL, key, "default", command, 0)
}

func submitJobWithOptions(
	t *testing.T,
	baseURL string,
	key string,
	queue string,
	command string,
	priority int,
	expectedStatus int,
) apiJob {
	t.Helper()
	status, stored, err := submitJobResultWithOptions(baseURL, key, queue, command, priority)
	if err != nil {
		t.Fatal(err)
	}
	if status != expectedStatus {
		t.Fatalf("POST status = %d, want %d", status, expectedStatus)
	}
	return stored
}

func submitJobResultWithOptions(baseURL, key, queue, command string, priority int) (int, apiJob, error) {
	body, err := json.Marshal(map[string]any{
		"queue":           queue,
		"priority":        priority,
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

func registerWorker(t *testing.T, baseURL, name, queue string) apiWorker {
	t.Helper()
	body, err := json.Marshal(map[string]string{"name": name, "queue": queue})
	if err != nil {
		t.Fatalf("encode worker: %v", err)
	}
	response, err := http.Post(baseURL+"/api/v1/workers", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("register status = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	var registered apiWorker
	decodeResponse(t, response, &registered)
	return registered
}

func claimJobResult(baseURL string, workerID uuid.UUID) (int, apiClaim, error) {
	request, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/workers/"+workerID.String()+"/claims", nil)
	if err != nil {
		return 0, apiClaim{}, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, apiClaim{}, err
	}
	defer response.Body.Close()

	var claim apiClaim
	if response.StatusCode == http.StatusOK {
		if err := json.NewDecoder(response.Body).Decode(&claim); err != nil {
			return response.StatusCode, apiClaim{}, fmt.Errorf("decode claim response: %w", err)
		}
	}
	return response.StatusCode, claim, nil
}

func renewLease(t *testing.T, baseURL string, workerID, jobID, leaseToken uuid.UUID) apiJob {
	t.Helper()
	status, renewed, err := renewLeaseResult(baseURL, workerID, jobID, leaseToken)
	if err != nil {
		t.Fatalf("renew lease: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("renew lease status = %d, want %d", status, http.StatusOK)
	}
	return renewed
}

func renewLeaseResult(baseURL string, workerID, jobID, leaseToken uuid.UUID) (int, apiJob, error) {
	body, err := json.Marshal(map[string]uuid.UUID{"leaseToken": leaseToken})
	if err != nil {
		return 0, apiJob{}, err
	}
	request, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/api/v1/workers/"+workerID.String()+"/jobs/"+jobID.String()+"/lease",
		bytes.NewReader(body),
	)
	if err != nil {
		return 0, apiJob{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, apiJob{}, err
	}
	defer response.Body.Close()
	var renewed apiJob
	if response.StatusCode == http.StatusOK {
		if err := json.NewDecoder(response.Body).Decode(&renewed); err != nil {
			return response.StatusCode, apiJob{}, err
		}
	}
	return response.StatusCode, renewed, nil
}

func decodeResponse(t *testing.T, response *http.Response, destination any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}
