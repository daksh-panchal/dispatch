package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/daksh-panchal/dispatch/internal/demo"
	"github.com/daksh-panchal/dispatch/internal/httpapi"
	"github.com/daksh-panchal/dispatch/internal/job"
	"github.com/daksh-panchal/dispatch/internal/outbox"
	dispatchpostgres "github.com/daksh-panchal/dispatch/internal/postgres"
	"github.com/daksh-panchal/dispatch/internal/readmodel"
	"github.com/daksh-panchal/dispatch/internal/recovery"
	"github.com/daksh-panchal/dispatch/internal/worker"
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

	if err := dispatchpostgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	jobRepository := dispatchpostgres.NewJobRepository(pool)
	workerRepository := dispatchpostgres.NewWorkerRepository(pool)
	readRepository := dispatchpostgres.NewReadRepository(pool)
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
	readService := readmodel.NewService(readRepository, time.Now, testLeaseDuration)
	recoveryService := recovery.NewService(
		jobRepository,
		time.Now,
		time.Second,
		testRetryBase,
		testRetryMaximum,
		100,
		logger,
	)
	server := httptest.NewServer(httpapi.New(
		jobService,
		workerService,
		pool,
		logger,
		httpapi.WithReader(readService),
	))
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

	t.Run("operational reads expose jobs workers and durable events", func(t *testing.T) {
		queue := "read-" + uuid.NewString()
		first := submitJobWithOptions(
			t,
			server.URL,
			"integration-"+uuid.NewString(),
			queue,
			"first",
			0,
			http.StatusCreated,
		)
		_ = submitJobWithOptions(
			t,
			server.URL,
			"integration-"+uuid.NewString(),
			queue,
			"second",
			0,
			http.StatusCreated,
		)

		var firstPage apiJobPage
		getJSON(t, server.URL+"/api/v1/jobs?queue="+queue+"&state=queued&limit=1", &firstPage)
		if len(firstPage.Items) != 1 || firstPage.NextCursor == nil {
			t.Fatalf("first job page = %+v, want one item and a cursor", firstPage)
		}
		var secondPage apiJobPage
		getJSON(t, server.URL+"/api/v1/jobs?queue="+queue+"&state=queued&limit=1&cursor="+firstPage.NextCursor.String(), &secondPage)
		if len(secondPage.Items) != 1 || secondPage.Items[0].ID == firstPage.Items[0].ID {
			t.Fatalf("second job page = %+v, want the other job", secondPage)
		}

		registered := registerWorker(t, server.URL, "read-worker", queue)
		status, claimed, err := claimJobResult(server.URL, registered.ID)
		if err != nil || status != http.StatusOK {
			t.Fatalf("claim job: status = %d, error = %v", status, err)
		}
		var workers apiWorkerPage
		getJSON(t, server.URL+"/api/v1/workers?queue="+queue, &workers)
		if len(workers.Items) != 1 || workers.Items[0].Status != "busy" || workers.Items[0].CurrentJobID == nil {
			t.Fatalf("worker page = %+v, want one busy worker", workers)
		}

		var events apiEventPage
		getJSON(t, server.URL+"/api/v1/events?queue="+queue+"&jobId="+claimed.Job.ID.String(), &events)
		if len(events.Items) < 2 {
			t.Fatalf("event page has %d items, want submitted and claimed events", len(events.Items))
		}
		for _, event := range events.Items {
			if event.JobID != claimed.Job.ID || !json.Valid(event.Payload) {
				t.Fatalf("event = %+v, want matching job and JSON payload", event)
			}
		}

		streamCtx, cancelStream := context.WithTimeout(ctx, 3*time.Second)
		defer cancelStream()
		request, err := http.NewRequestWithContext(
			streamCtx,
			http.MethodGet,
			server.URL+"/api/v1/events/stream?jobId="+first.ID.String(),
			nil,
		)
		if err != nil {
			t.Fatalf("create event stream request: %v", err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("open event stream: %v", err)
		}
		foundData := false
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			if len(scanner.Text()) >= 6 && scanner.Text()[:6] == "data: " {
				foundData = true
				break
			}
		}
		_ = response.Body.Close()
		cancelStream()
		if !foundData {
			t.Fatalf("event stream returned no data: %v", scanner.Err())
		}

		status, _, err = completeJobResult(
			server.URL,
			registered.ID,
			claimed.Job.ID,
			claimed.LeaseToken,
			json.RawMessage(`{"artifact":"read-test"}`),
		)
		if err != nil || status != http.StatusOK {
			t.Fatalf("complete read test job: status = %d, error = %v", status, err)
		}
	})

	t.Run("demo orchestrator drives work and survives a worker crash", func(t *testing.T) {
		demoClient, err := demo.NewHTTPClient(server.URL)
		if err != nil {
			t.Fatalf("create demo client: %v", err)
		}
		demoCtx, cancelDemo := context.WithCancel(ctx)
		defer cancelDemo()
		demoManager := demo.NewManager(
			demoCtx,
			demoClient,
			logger,
			demo.WithJitter(func(time.Duration) time.Duration { return 500 * time.Millisecond }),
			demo.WithIntervals(10*time.Millisecond, 50*time.Millisecond),
		)
		run, err := demoManager.Start(ctx, demo.StartRunInput{
			JobCount: 2, WorkerCount: 2, WorkDuration: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("start demo run: %v", err)
		}

		var victim uuid.UUID
		claimDeadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(claimDeadline) {
			current, getErr := demoManager.Get(run.ID)
			if getErr != nil {
				t.Fatalf("read demo run: %v", getErr)
			}
			for _, candidate := range current.Workers {
				if candidate.Status == demo.WorkerWorking {
					victim = candidate.ID
					break
				}
			}
			if victim != uuid.Nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if victim == uuid.Nil {
			t.Fatal("no demo worker claimed a job")
		}
		if _, err := demoManager.KillWorker(run.ID, victim); err != nil {
			t.Fatalf("kill demo worker: %v", err)
		}

		time.Sleep(testLeaseDuration + 50*time.Millisecond)
		recovered, err := recoveryService.RecoverOnce(ctx)
		if err != nil {
			t.Fatalf("recover killed demo worker lease: %v", err)
		}
		if recovered.Requeued != 1 || recovered.Failed != 0 {
			t.Fatalf("recovery result = %+v, want one requeued job", recovered)
		}

		completionDeadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(completionDeadline) {
			current, getErr := demoManager.Get(run.ID)
			if getErr != nil {
				t.Fatalf("read demo run: %v", getErr)
			}
			if current.Status == demo.RunCompleted {
				snapshot, snapshotErr := demoManager.Snapshot(ctx, run.ID)
				if snapshotErr != nil {
					t.Fatalf("read completed demo snapshot: %v", snapshotErr)
				}
				for _, stored := range snapshot.Jobs {
					if stored.State != "succeeded" {
						t.Fatalf("demo job %s state = %q, want succeeded", stored.ID, stored.State)
					}
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("demo run did not complete after lease recovery")
	})

	t.Run("scheduler statistics reflect durable state", func(t *testing.T) {
		_ = submitJob(t, server.URL, "integration-"+uuid.NewString(), "observe", http.StatusCreated)
		statsRepository := dispatchpostgres.NewStatsRepository(pool)
		counts, activeLeases, expiredLeases, pendingEvents, err := statsRepository.ReadSchedulerStats(ctx)
		if err != nil {
			t.Fatalf("read scheduler statistics: %v", err)
		}
		if counts["queued"] < 1 || pendingEvents < 1 {
			t.Fatalf("scheduler statistics = %+v, pending events = %d", counts, pendingEvents)
		}
		if activeLeases < 0 || expiredLeases < 0 {
			t.Fatalf("invalid lease statistics: active = %d, expired = %d", activeLeases, expiredLeases)
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
		assertEventTypes(t, pool, expectedID, []outbox.EventType{outbox.EventJobSubmitted})
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
		assertEventTypes(t, pool, terminal.ID, []outbox.EventType{
			outbox.EventJobSubmitted,
			outbox.EventJobClaimed,
			outbox.EventJobRetryScheduled,
			outbox.EventJobClaimed,
			outbox.EventJobFailed,
		})
	})

	t.Run("queued cancellation is idempotent and prevents claims", func(t *testing.T) {
		queue := "cancel-queued-" + uuid.NewString()
		created := submitJobWithOptions(t, server.URL, "integration-"+uuid.NewString(), queue, "cancel", 0, http.StatusCreated)

		status, cancelled, err := cancelJobResult(server.URL, created.ID)
		if err != nil {
			t.Fatalf("cancel queued job: %v", err)
		}
		if status != http.StatusOK || cancelled.State != "cancelled" || cancelled.CancelledAt == nil || cancelled.FinishedAt == nil {
			t.Fatalf("cancel status = %d, job = %+v", status, cancelled)
		}

		status, repeated, err := cancelJobResult(server.URL, created.ID)
		if err != nil {
			t.Fatalf("repeat cancellation: %v", err)
		}
		if status != http.StatusOK || repeated.CancelledAt == nil || !repeated.CancelledAt.Equal(*cancelled.CancelledAt) {
			t.Fatalf("repeat cancellation status = %d, job = %+v", status, repeated)
		}

		registered := registerWorker(t, server.URL, "cancel-queued-worker", queue)
		status, _, err = claimJobResult(server.URL, registered.ID)
		if err != nil || status != http.StatusNoContent {
			t.Fatalf("claim cancelled job status = %d, error = %v", status, err)
		}
		assertEventTypes(t, pool, created.ID, []outbox.EventType{
			outbox.EventJobSubmitted,
			outbox.EventJobCancelled,
		})
	})

	t.Run("running cancellation invalidates the worker lease", func(t *testing.T) {
		queue := "cancel-running-" + uuid.NewString()
		_ = submitJobWithOptions(t, server.URL, "integration-"+uuid.NewString(), queue, "cancel", 0, http.StatusCreated)
		registered := registerWorker(t, server.URL, "cancel-running-worker", queue)
		status, claim, err := claimJobResult(server.URL, registered.ID)
		if err != nil || status != http.StatusOK {
			t.Fatalf("claim status = %d, error = %v", status, err)
		}

		status, cancelled, err := cancelJobResult(server.URL, claim.Job.ID)
		if err != nil {
			t.Fatalf("cancel running job: %v", err)
		}
		if status != http.StatusOK || cancelled.State != "cancelled" || cancelled.WorkerID != nil || cancelled.LeaseExpiresAt != nil {
			t.Fatalf("cancel status = %d, job = %+v", status, cancelled)
		}

		status, _, err = renewLeaseResult(server.URL, registered.ID, claim.Job.ID, claim.LeaseToken)
		if err != nil || status != http.StatusConflict {
			t.Fatalf("renew cancelled job status = %d, error = %v", status, err)
		}
		status, _, err = completeJobResult(server.URL, registered.ID, claim.Job.ID, claim.LeaseToken, nil)
		if err != nil || status != http.StatusConflict {
			t.Fatalf("complete cancelled job status = %d, error = %v", status, err)
		}
	})

	t.Run("cancellation and completion have exactly one winner", func(t *testing.T) {
		queue := "cancel-race-" + uuid.NewString()
		_ = submitJobWithOptions(t, server.URL, "integration-"+uuid.NewString(), queue, "race", 0, http.StatusCreated)
		registered := registerWorker(t, server.URL, "cancel-race-worker", queue)
		status, claim, err := claimJobResult(server.URL, registered.ID)
		if err != nil || status != http.StatusOK {
			t.Fatalf("claim status = %d, error = %v", status, err)
		}

		type result struct {
			action string
			status int
			err    error
		}
		results := make(chan result, 2)
		go func() {
			status, _, err := cancelJobResult(server.URL, claim.Job.ID)
			results <- result{action: "cancel", status: status, err: err}
		}()
		go func() {
			status, _, err := completeJobResult(server.URL, registered.ID, claim.Job.ID, claim.LeaseToken, nil)
			results <- result{action: "complete", status: status, err: err}
		}()

		statuses := make(map[string]int, 2)
		for range 2 {
			result := <-results
			if result.err != nil {
				t.Fatalf("%s race request: %v", result.action, result.err)
			}
			statuses[result.action] = result.status
		}
		if !((statuses["cancel"] == http.StatusOK && statuses["complete"] == http.StatusConflict) ||
			(statuses["complete"] == http.StatusOK && statuses["cancel"] == http.StatusConflict)) {
			t.Fatalf("race statuses = %+v, want one 200 and one 409", statuses)
		}

		stored := getJob(t, server.URL, claim.Job.ID)
		if stored.State != "cancelled" && stored.State != "succeeded" {
			t.Fatalf("race winner left job in state %q", stored.State)
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
		assertEventTypes(t, pool, completed.ID, []outbox.EventType{
			outbox.EventJobSubmitted,
			outbox.EventJobClaimed,
			outbox.EventJobRecovered,
			outbox.EventJobClaimed,
			outbox.EventJobSucceeded,
		})
	})

	t.Run("concurrent outbox publishers claim each event once", func(t *testing.T) {
		_ = submitJob(t, server.URL, "integration-"+uuid.NewString(), "publish-one", http.StatusCreated)
		_ = submitJob(t, server.URL, "integration-"+uuid.NewString(), "publish-two", http.StatusCreated)

		outboxRepository := dispatchpostgres.NewOutboxRepository(pool)
		now := time.Now().UTC()
		var expected int
		if err := pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM job_outbox_events
			WHERE published_at IS NULL
			  AND next_attempt_at <= $1
			  AND (claim_expires_at IS NULL OR claim_expires_at <= $1)`, now).Scan(&expected); err != nil {
			t.Fatalf("count pending events: %v", err)
		}

		type claimResult struct {
			token  uuid.UUID
			events []outbox.Event
			err    error
		}
		results := make(chan claimResult, 2)
		for range 2 {
			go func() {
				token := uuid.New()
				events, err := outboxRepository.ClaimBatch(ctx, outbox.ClaimParams{
					Token:     token,
					Now:       now,
					ExpiresAt: now.Add(time.Minute),
					BatchSize: 1000,
				})
				results <- claimResult{token: token, events: events, err: err}
			}()
		}

		claimed := make(map[uuid.UUID]uuid.UUID, expected)
		for range 2 {
			result := <-results
			if result.err != nil {
				t.Fatalf("claim outbox events: %v", result.err)
			}
			for _, event := range result.events {
				if _, exists := claimed[event.ID]; exists {
					t.Fatalf("event %s was claimed by two publishers", event.ID)
				}
				claimed[event.ID] = result.token
			}
		}
		if len(claimed) != expected {
			t.Fatalf("claimed events = %d, want %d", len(claimed), expected)
		}

		var publishedID uuid.UUID
		var publishedToken uuid.UUID
		for eventID, token := range claimed {
			publishedID = eventID
			publishedToken = token
			break
		}
		if publishedID == uuid.Nil {
			t.Fatal("no event was available to verify publication")
		}
		if err := outboxRepository.MarkPublished(ctx, publishedID, uuid.New(), now); !errors.Is(err, outbox.ErrClaimLost) {
			t.Fatalf("wrong-token publication error = %v, want ErrClaimLost", err)
		}
		if err := outboxRepository.MarkPublished(ctx, publishedID, publishedToken, now); err != nil {
			t.Fatalf("mark event published: %v", err)
		}
		var attempts int
		var publishedAt *time.Time
		if err := pool.QueryRow(ctx, `
			SELECT publish_attempts, published_at
			FROM job_outbox_events
			WHERE id = $1`, publishedID).Scan(&attempts, &publishedAt); err != nil {
			t.Fatalf("read published event: %v", err)
		}
		if attempts != 1 || publishedAt == nil {
			t.Fatalf("published attempts = %d, published at = %v", attempts, publishedAt)
		}

		var failedID uuid.UUID
		var failedToken uuid.UUID
		for eventID, token := range claimed {
			if eventID != publishedID {
				failedID = eventID
				failedToken = token
				break
			}
		}
		if failedID == uuid.Nil {
			t.Fatal("no second event was available to verify publish failure")
		}
		nextAttemptAt := now.Add(time.Minute)
		if err := outboxRepository.MarkFailed(ctx, outbox.PublishFailure{
			EventID:       failedID,
			ClaimToken:    failedToken,
			FailedAt:      now,
			NextAttemptAt: nextAttemptAt,
			Error:         "message-flow unavailable",
		}); err != nil {
			t.Fatalf("mark event failed: %v", err)
		}
		var failedAttempts int
		var storedNextAttempt time.Time
		var lastError string
		var claimToken *uuid.UUID
		if err := pool.QueryRow(ctx, `
			SELECT publish_attempts, next_attempt_at, last_publish_error, claim_token
			FROM job_outbox_events
			WHERE id = $1`, failedID).Scan(
			&failedAttempts,
			&storedNextAttempt,
			&lastError,
			&claimToken,
		); err != nil {
			t.Fatalf("read failed event: %v", err)
		}
		if failedAttempts != 1 || storedNextAttempt.Sub(nextAttemptAt).Abs() > time.Microsecond || lastError != "message-flow unavailable" || claimToken != nil {
			t.Fatalf(
				"failed attempts = %d, next attempt = %v, error = %q, claim token = %v",
				failedAttempts,
				storedNextAttempt,
				lastError,
				claimToken,
			)
		}
	})

	t.Run("expired outbox claim is delivered again", func(t *testing.T) {
		created := submitJob(t, server.URL, "integration-"+uuid.NewString(), "redeliver", http.StatusCreated)
		outboxRepository := dispatchpostgres.NewOutboxRepository(pool)
		now := time.Now().UTC()
		firstToken := uuid.New()
		first, err := outboxRepository.ClaimBatch(ctx, outbox.ClaimParams{
			Token:     firstToken,
			Now:       now,
			ExpiresAt: now.Add(20 * time.Millisecond),
			BatchSize: 1,
		})
		if err != nil || len(first) != 1 || first[0].JobID != created.ID {
			t.Fatalf("first claim = %+v, error = %v", first, err)
		}

		beforeExpiry, err := outboxRepository.ClaimBatch(ctx, outbox.ClaimParams{
			Token:     uuid.New(),
			Now:       now.Add(10 * time.Millisecond),
			ExpiresAt: now.Add(time.Minute),
			BatchSize: 1,
		})
		if err != nil || len(beforeExpiry) != 0 {
			t.Fatalf("claim before expiry = %+v, error = %v", beforeExpiry, err)
		}

		afterExpiry, err := outboxRepository.ClaimBatch(ctx, outbox.ClaimParams{
			Token:     uuid.New(),
			Now:       now.Add(30 * time.Millisecond),
			ExpiresAt: now.Add(time.Minute),
			BatchSize: 1,
		})
		if err != nil || len(afterExpiry) != 1 || afterExpiry[0].ID != first[0].ID {
			t.Fatalf("claim after expiry = %+v, error = %v", afterExpiry, err)
		}
	})
}

type apiJob struct {
	ID             uuid.UUID       `json:"id"`
	Queue          string          `json:"queue"`
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
	CancelledAt    *time.Time      `json:"cancelledAt"`
}

type apiWorker struct {
	ID         uuid.UUID `json:"id"`
	Name       string    `json:"name"`
	Queue      string    `json:"queue"`
	LastSeenAt time.Time `json:"lastSeenAt"`
}

type apiJobPage struct {
	Items      []apiJob   `json:"items"`
	NextCursor *uuid.UUID `json:"nextCursor"`
}

type apiWorkerSnapshot struct {
	apiWorker
	Status       string     `json:"status"`
	CurrentJobID *uuid.UUID `json:"currentJobId"`
}

type apiWorkerPage struct {
	Items      []apiWorkerSnapshot `json:"items"`
	NextCursor *uuid.UUID          `json:"nextCursor"`
}

type apiEvent struct {
	Sequence int64           `json:"sequence"`
	JobID    uuid.UUID       `json:"jobId"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
}

type apiEventPage struct {
	Items        []apiEvent `json:"items"`
	NextSequence *int64     `json:"nextSequence"`
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

func getJSON(t *testing.T, url string, destination any) {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d", url, response.StatusCode, http.StatusOK)
	}
	decodeResponse(t, response, destination)
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

func cancelJobResult(baseURL string, jobID uuid.UUID) (int, apiJob, error) {
	request, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/jobs/"+jobID.String()+"/cancel", nil)
	if err != nil {
		return 0, apiJob{}, err
	}
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

func assertEventTypes(
	t *testing.T,
	pool *pgxpool.Pool,
	jobID uuid.UUID,
	want []outbox.EventType,
) {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT event_type
		FROM job_outbox_events
		WHERE job_id = $1
		ORDER BY sequence`, jobID)
	if err != nil {
		t.Fatalf("read job events: %v", err)
	}
	defer rows.Close()

	var got []outbox.EventType
	for rows.Next() {
		var eventType outbox.EventType
		if err := rows.Scan(&eventType); err != nil {
			t.Fatalf("scan job event: %v", err)
		}
		got = append(got, eventType)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate job events: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("event types = %v, want %v", got, want)
		}
	}
}

func decodeResponse(t *testing.T, response *http.Response, destination any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}
