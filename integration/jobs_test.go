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
	"github.com/daksh-panchal/work-flow/internal/recovery"
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
	const (
		testLeaseDuration = 250 * time.Millisecond
		testRetryBase     = 25 * time.Millisecond
		testRetryMaximum  = 100 * time.Millisecond
	)
	workerService := worker.NewService(
		workerRepository,
		jobRepository,
		time.Now,
		testLeaseDuration,
		testRetryBase,
		testRetryMaximum,
	)
	recoveryService := recovery.NewService(
		jobRepository,
		time.Now,
		time.Second,
		testRetryBase,
		testRetryMaximum,
		100,
		logger,
	)
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

		status, _, err = completeJobResult(server.URL, registered.ID, claim.Job.ID, claim.LeaseToken, nil)
		if err != nil || status != http.StatusOK {
			t.Fatalf("complete claimed job: status = %d, error = %v", status, err)
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
			workerID uuid.UUID
			status   int
			claim    apiClaim
			err      error
		}
		results := make(chan result, workerCount)
		var group sync.WaitGroup
		for _, registered := range workers {
			group.Add(1)
			go func(workerID uuid.UUID) {
				defer group.Done()
				status, claim, err := claimJobResult(server.URL, workerID)
				results <- result{workerID: workerID, status: status, claim: claim, err: err}
			}(registered.ID)
		}
		group.Wait()
		close(results)

		claimedCount := 0
		var winningWorkerID uuid.UUID
		var winningClaim apiClaim
		for result := range results {
			if result.err != nil {
				t.Fatalf("concurrent claim: %v", result.err)
			}
			switch result.status {
			case http.StatusOK:
				claimedCount++
				winningWorkerID = result.workerID
				winningClaim = result.claim
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

		status, _, err := completeJobResult(server.URL, winningWorkerID, winningClaim.Job.ID, winningClaim.LeaseToken, nil)
		if err != nil || status != http.StatusOK {
			t.Fatalf("complete concurrently claimed job: status = %d, error = %v", status, err)
		}
	})

	t.Run("current worker completes a job and stale completion is rejected", func(t *testing.T) {
		queue := "complete-" + uuid.NewString()
		_ = submitJobWithOptions(t, server.URL, "integration-"+uuid.NewString(), queue, "build", 0, http.StatusCreated)
		registered := registerWorker(t, server.URL, "completion-worker", queue)
		status, claim, err := claimJobResult(server.URL, registered.ID)
		if err != nil || status != http.StatusOK {
			t.Fatalf("claim status = %d, error = %v", status, err)
		}

		status, completed, err := completeJobResult(
			server.URL,
			registered.ID,
			claim.Job.ID,
			claim.LeaseToken,
			map[string]any{"artifact": "build.zip"},
		)
		if err != nil {
			t.Fatalf("complete job: %v", err)
		}
		if status != http.StatusOK || completed.State != "succeeded" || completed.FinishedAt == nil {
			t.Fatalf("completed status = %d, job = %+v", status, completed)
		}
		if string(completed.Result) != `{"artifact":"build.zip"}` || completed.WorkerID != nil || completed.LeaseExpiresAt != nil {
			t.Fatalf("completed job = %+v", completed)
		}

		status, _, err = completeJobResult(
			server.URL,
			registered.ID,
			claim.Job.ID,
			claim.LeaseToken,
			map[string]any{"artifact": "late.zip"},
		)
		if err != nil {
			t.Fatalf("repeat completion: %v", err)
		}
		if status != http.StatusConflict {
			t.Fatalf("repeat completion status = %d, want %d", status, http.StatusConflict)
		}
	})

	t.Run("reported failures retry and eventually become terminal", func(t *testing.T) {
		queue := "failure-" + uuid.NewString()
		_ = submitJobWithPolicy(t, server.URL, "integration-"+uuid.NewString(), queue, "fail", 0, 2, http.StatusCreated)
		registered := registerWorker(t, server.URL, "failure-worker", queue)

		status, firstClaim, err := claimJobResult(server.URL, registered.ID)
		if err != nil || status != http.StatusOK {
			t.Fatalf("first claim status = %d, error = %v", status, err)
		}
		status, retry, err := failJobResult(server.URL, registered.ID, firstClaim.Job.ID, firstClaim.LeaseToken, "compiler error")
		if err != nil {
			t.Fatalf("first failure: %v", err)
		}
		if status != http.StatusOK || retry.State != "queued" || retry.LastError == nil || retry.FinishedAt != nil {
			t.Fatalf("retry status = %d, job = %+v", status, retry)
		}

		waitUntil(t, retry.AvailableAt)
		status, secondClaim, err := claimJobResult(server.URL, registered.ID)
		if err != nil || status != http.StatusOK {
			t.Fatalf("second claim status = %d, error = %v", status, err)
		}
		status, terminal, err := failJobResult(server.URL, registered.ID, secondClaim.Job.ID, secondClaim.LeaseToken, "compiler error again")
		if err != nil {
			t.Fatalf("second failure: %v", err)
		}
		if status != http.StatusOK || terminal.State != "failed" || terminal.AttemptCount != 2 || terminal.FinishedAt == nil {
			t.Fatalf("terminal status = %d, job = %+v", status, terminal)
		}
	})

	t.Run("expired lease is recovered once and stale worker is fenced", func(t *testing.T) {
		queue := "recovery-" + uuid.NewString()
		_ = submitJobWithPolicy(t, server.URL, "integration-"+uuid.NewString(), queue, "recover", 0, 2, http.StatusCreated)
		originalWorker := registerWorker(t, server.URL, "original-worker", queue)
		replacementWorker := registerWorker(t, server.URL, "replacement-worker", queue)
		status, originalClaim, err := claimJobResult(server.URL, originalWorker.ID)
		if err != nil || status != http.StatusOK {
			t.Fatalf("original claim status = %d, error = %v", status, err)
		}
		if originalClaim.Job.LeaseExpiresAt == nil {
			t.Fatal("original claim has no lease expiry")
		}
		waitUntil(t, originalClaim.Job.LeaseExpiresAt.Add(10*time.Millisecond))

		const recoveryCount = 8
		results := make(chan job.RecoveryResult, recoveryCount)
		errorsFound := make(chan error, recoveryCount)
		var group sync.WaitGroup
		for range recoveryCount {
			group.Add(1)
			go func() {
				defer group.Done()
				result, err := recoveryService.RecoverOnce(ctx)
				results <- result
				errorsFound <- err
			}()
		}
		group.Wait()
		close(results)
		close(errorsFound)
		for err := range errorsFound {
			if err != nil {
				t.Fatalf("recover expired job: %v", err)
			}
		}
		var recovered int64
		for result := range results {
			recovered += result.Requeued + result.Failed
		}
		if recovered != 1 {
			t.Fatalf("recovered jobs = %d, want 1", recovered)
		}

		recoveredJob := getJob(t, server.URL, originalClaim.Job.ID)
		if recoveredJob.State != "queued" || recoveredJob.LastError == nil {
			t.Fatalf("recovered job = %+v", recoveredJob)
		}
		waitUntil(t, recoveredJob.AvailableAt)
		status, replacementClaim, err := claimJobResult(server.URL, replacementWorker.ID)
		if err != nil || status != http.StatusOK {
			t.Fatalf("replacement claim status = %d, error = %v", status, err)
		}

		status, _, err = completeJobResult(
			server.URL,
			originalWorker.ID,
			originalClaim.Job.ID,
			originalClaim.LeaseToken,
			nil,
		)
		if err != nil {
			t.Fatalf("stale completion: %v", err)
		}
		if status != http.StatusConflict {
			t.Fatalf("stale completion status = %d, want %d", status, http.StatusConflict)
		}

		status, completed, err := completeJobResult(
			server.URL,
			replacementWorker.ID,
			replacementClaim.Job.ID,
			replacementClaim.LeaseToken,
			nil,
		)
		if err != nil || status != http.StatusOK || completed.State != "succeeded" {
			t.Fatalf("replacement completion status = %d, job = %+v, error = %v", status, completed, err)
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
	AvailableAt    time.Time       `json:"availableAt"`
	Result         json.RawMessage `json:"result"`
	LastError      *string         `json:"lastError"`
	FinishedAt     *time.Time      `json:"finishedAt"`
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
	return submitJobWithPolicy(t, baseURL, key, queue, command, priority, 3, expectedStatus)
}

func submitJobWithPolicy(
	t *testing.T,
	baseURL string,
	key string,
	queue string,
	command string,
	priority int,
	maximumAttempts int,
	expectedStatus int,
) apiJob {
	t.Helper()
	status, stored, err := submitJobResultWithPolicy(baseURL, key, queue, command, priority, maximumAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if status != expectedStatus {
		t.Fatalf("POST status = %d, want %d", status, expectedStatus)
	}
	return stored
}

func submitJobResultWithOptions(baseURL, key, queue, command string, priority int) (int, apiJob, error) {
	return submitJobResultWithPolicy(baseURL, key, queue, command, priority, 3)
}

func submitJobResultWithPolicy(baseURL, key, queue, command string, priority, maximumAttempts int) (int, apiJob, error) {
	body, err := json.Marshal(map[string]any{
		"queue":           queue,
		"priority":        priority,
		"maximumAttempts": maximumAttempts,
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

func completeJobResult(
	baseURL string,
	workerID uuid.UUID,
	jobID uuid.UUID,
	leaseToken uuid.UUID,
	result any,
) (int, apiJob, error) {
	return postOutcomeResult(baseURL, workerID, jobID, "complete", map[string]any{
		"leaseToken": leaseToken,
		"result":     result,
	})
}

func failJobResult(
	baseURL string,
	workerID uuid.UUID,
	jobID uuid.UUID,
	leaseToken uuid.UUID,
	reason string,
) (int, apiJob, error) {
	return postOutcomeResult(baseURL, workerID, jobID, "fail", map[string]any{
		"leaseToken": leaseToken,
		"error":      reason,
	})
}

func postOutcomeResult(
	baseURL string,
	workerID uuid.UUID,
	jobID uuid.UUID,
	action string,
	bodyValue any,
) (int, apiJob, error) {
	body, err := json.Marshal(bodyValue)
	if err != nil {
		return 0, apiJob{}, err
	}
	request, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/api/v1/workers/"+workerID.String()+"/jobs/"+jobID.String()+"/"+action,
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

	var stored apiJob
	if response.StatusCode == http.StatusOK {
		if err := json.NewDecoder(response.Body).Decode(&stored); err != nil {
			return response.StatusCode, apiJob{}, err
		}
	}
	return response.StatusCode, stored, nil
}

func getJob(t *testing.T, baseURL string, id uuid.UUID) apiJob {
	t.Helper()
	response, err := http.Get(baseURL + "/api/v1/jobs/" + id.String())
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("get job status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var stored apiJob
	decodeResponse(t, response, &stored)
	return stored
}

func waitUntil(t *testing.T, instant time.Time) {
	t.Helper()
	if delay := time.Until(instant); delay > 0 {
		time.Sleep(delay)
	}
}

func decodeResponse(t *testing.T, response *http.Response, destination any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}
