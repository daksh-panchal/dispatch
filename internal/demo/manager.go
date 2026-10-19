package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	minimumJobs          = 1
	maximumJobs          = 100
	minimumWorkers       = 1
	maximumWorkers       = 20
	minimumWorkDuration  = 100 * time.Millisecond
	maximumWorkDuration  = 5 * time.Minute
	defaultPollInterval  = 500 * time.Millisecond
	defaultRenewInterval = 5 * time.Second
)

var (
	ErrInvalidRun     = errors.New("invalid demo run")
	ErrRunNotFound    = errors.New("demo run not found")
	ErrWorkerNotFound = errors.New("demo worker not found")
)

type Scheduler interface {
	SubmitJob(context.Context, SubmitJobInput) (Job, error)
	RegisterWorker(context.Context, string, string) (SchedulerWorker, error)
	Claim(context.Context, uuid.UUID) (*Claim, error)
	RenewLease(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error
	Complete(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, json.RawMessage) error
	ListJobs(context.Context, string) ([]Job, error)
	ListWorkers(context.Context, string) ([]SchedulerWorker, error)
}

type RunStatus string

const (
	RunStarting  RunStatus = "starting"
	RunRunning   RunStatus = "running"
	RunCompleted RunStatus = "completed"
	RunStalled   RunStatus = "stalled"
	RunFailed    RunStatus = "failed"
)

type WorkerStatus string

const (
	WorkerIdle    WorkerStatus = "idle"
	WorkerWorking WorkerStatus = "working"
	WorkerKilled  WorkerStatus = "killed"
	WorkerStopped WorkerStatus = "stopped"
)

type StartRunInput struct {
	JobCount     int
	WorkerCount  int
	WorkDuration time.Duration
}

type Run struct {
	ID             uuid.UUID    `json:"id"`
	Queue          string       `json:"queue"`
	Status         RunStatus    `json:"status"`
	JobCount       int          `json:"jobCount"`
	WorkDurationMS int64        `json:"workDurationMs"`
	Workers        []DemoWorker `json:"workers"`
	CreatedAt      time.Time    `json:"createdAt"`
	CompletedAt    *time.Time   `json:"completedAt,omitempty"`
	LastError      string       `json:"lastError,omitempty"`
}

type DemoWorker struct {
	ID           uuid.UUID    `json:"id"`
	Name         string       `json:"name"`
	Status       WorkerStatus `json:"status"`
	CurrentJobID *uuid.UUID   `json:"currentJobId,omitempty"`
	Completed    int          `json:"completedJobs"`
	LastError    string       `json:"lastError,omitempty"`
}

type Snapshot struct {
	Run     Run               `json:"run"`
	Jobs    []Job             `json:"jobs"`
	Workers []SchedulerWorker `json:"workers"`
}

type workerProcess struct {
	view   DemoWorker
	cancel context.CancelFunc
}

type runState struct {
	view       Run
	workers    map[uuid.UUID]*workerProcess
	nextWorker int
}

type Manager struct {
	ctx           context.Context
	scheduler     Scheduler
	logger        *slog.Logger
	now           func() time.Time
	jitter        func(time.Duration) time.Duration
	pollInterval  time.Duration
	renewInterval time.Duration

	mu   sync.RWMutex
	runs map[uuid.UUID]*runState
}

type ManagerOption func(*Manager)

func WithClock(now func() time.Time) ManagerOption {
	return func(manager *Manager) { manager.now = now }
}

func WithJitter(jitter func(time.Duration) time.Duration) ManagerOption {
	return func(manager *Manager) { manager.jitter = jitter }
}

func WithIntervals(poll, renew time.Duration) ManagerOption {
	return func(manager *Manager) {
		manager.pollInterval = poll
		manager.renewInterval = renew
	}
}

func NewManager(
	ctx context.Context,
	scheduler Scheduler,
	logger *slog.Logger,
	options ...ManagerOption,
) *Manager {
	manager := &Manager{
		ctx:           ctx,
		scheduler:     scheduler,
		logger:        logger,
		now:           time.Now,
		jitter:        randomWorkDuration,
		pollInterval:  defaultPollInterval,
		renewInterval: defaultRenewInterval,
		runs:          make(map[uuid.UUID]*runState),
	}
	for _, option := range options {
		option(manager)
	}
	return manager
}

func (manager *Manager) Start(ctx context.Context, input StartRunInput) (Run, error) {
	if err := validateStart(input); err != nil {
		return Run{}, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Run{}, fmt.Errorf("generate demo run ID: %w", err)
	}
	now := manager.now().UTC()
	state := &runState{
		view: Run{
			ID:             id,
			Queue:          "demo-" + id.String(),
			Status:         RunStarting,
			JobCount:       input.JobCount,
			WorkDurationMS: input.WorkDuration.Milliseconds(),
			CreatedAt:      now,
		},
		workers: make(map[uuid.UUID]*workerProcess),
	}
	manager.mu.Lock()
	manager.runs[id] = state
	manager.mu.Unlock()

	for index := range input.JobCount {
		duration := manager.jitter(input.WorkDuration)
		payload, err := json.Marshal(map[string]any{
			"kind":       "demo",
			"runId":      id,
			"jobNumber":  index + 1,
			"durationMs": duration.Milliseconds(),
		})
		if err != nil {
			return manager.failRun(id, fmt.Errorf("encode demo job: %w", err))
		}
		_, err = manager.scheduler.SubmitJob(ctx, SubmitJobInput{
			IdempotencyKey:  fmt.Sprintf("demo:%s:job:%d", id, index+1),
			Queue:           state.view.Queue,
			Priority:        rand.IntN(10),
			Payload:         payload,
			MaximumAttempts: 3,
		})
		if err != nil {
			return manager.failRun(id, fmt.Errorf("submit demo job %d: %w", index+1, err))
		}
	}

	manager.mu.Lock()
	state.view.Status = RunRunning
	manager.mu.Unlock()
	if _, err := manager.AddWorkers(ctx, id, input.WorkerCount); err != nil {
		return manager.failRun(id, err)
	}
	result, _ := manager.Get(id)
	manager.logger.Info("demo run started", "run_id", id, "queue", result.Queue, "jobs", input.JobCount, "workers", input.WorkerCount)
	return result, nil
}

func (manager *Manager) Get(id uuid.UUID) (Run, error) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	state, exists := manager.runs[id]
	if !exists {
		return Run{}, ErrRunNotFound
	}
	return copyRun(state), nil
}

func (manager *Manager) List() []Run {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	runs := make([]Run, 0, len(manager.runs))
	for _, state := range manager.runs {
		runs = append(runs, copyRun(state))
	}
	sort.Slice(runs, func(left, right int) bool {
		return runs[left].CreatedAt.After(runs[right].CreatedAt)
	})
	return runs
}

func (manager *Manager) Snapshot(ctx context.Context, id uuid.UUID) (Snapshot, error) {
	run, err := manager.Get(id)
	if err != nil {
		return Snapshot{}, err
	}
	jobs, err := manager.scheduler.ListJobs(ctx, run.Queue)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read demo jobs: %w", err)
	}
	workers, err := manager.scheduler.ListWorkers(ctx, run.Queue)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read demo workers: %w", err)
	}
	return Snapshot{Run: run, Jobs: jobs, Workers: workers}, nil
}

func (manager *Manager) AddWorkers(ctx context.Context, runID uuid.UUID, count int) (Run, error) {
	if count < minimumWorkers || count > maximumWorkers {
		return Run{}, fmt.Errorf("%w: workerCount must be between %d and %d", ErrInvalidRun, minimumWorkers, maximumWorkers)
	}

	for range count {
		manager.mu.Lock()
		state, exists := manager.runs[runID]
		if !exists {
			manager.mu.Unlock()
			return Run{}, ErrRunNotFound
		}
		if len(state.workers) >= maximumWorkers {
			manager.mu.Unlock()
			return Run{}, fmt.Errorf("%w: a run cannot exceed %d workers", ErrInvalidRun, maximumWorkers)
		}
		if state.view.Status == RunCompleted || state.view.Status == RunFailed {
			manager.mu.Unlock()
			return Run{}, fmt.Errorf("%w: workers cannot be added to a %s run", ErrInvalidRun, state.view.Status)
		}
		state.nextWorker++
		workerNumber := state.nextWorker
		queue := state.view.Queue
		name := fmt.Sprintf("demo-%s-worker-%02d", runID.String()[:8], workerNumber)
		manager.mu.Unlock()

		registered, err := manager.scheduler.RegisterWorker(ctx, name, queue)
		if err != nil {
			return Run{}, fmt.Errorf("register %s: %w", name, err)
		}
		workerCtx, cancel := context.WithCancel(manager.ctx)
		process := &workerProcess{
			view:   DemoWorker{ID: registered.ID, Name: registered.Name, Status: WorkerIdle},
			cancel: cancel,
		}
		manager.mu.Lock()
		state = manager.runs[runID]
		state.workers[registered.ID] = process
		if state.view.Status == RunStalled {
			state.view.Status = RunRunning
		}
		manager.mu.Unlock()
		go manager.runWorker(workerCtx, runID, registered.ID)
	}
	return manager.Get(runID)
}

func (manager *Manager) KillWorker(runID, workerID uuid.UUID) (Run, error) {
	manager.mu.Lock()
	state, exists := manager.runs[runID]
	if !exists {
		manager.mu.Unlock()
		return Run{}, ErrRunNotFound
	}
	process, exists := state.workers[workerID]
	if !exists {
		manager.mu.Unlock()
		return Run{}, ErrWorkerNotFound
	}
	if process.view.Status != WorkerKilled && process.view.Status != WorkerStopped {
		process.view.Status = WorkerKilled
		process.cancel()
	}
	if !hasActiveWorker(state) && (state.view.Status == RunRunning || state.view.Status == RunStarting) {
		state.view.Status = RunStalled
	}
	result := copyRun(state)
	manager.mu.Unlock()
	manager.logger.Info("demo worker killed", "run_id", runID, "worker_id", workerID)
	return result, nil
}

func (manager *Manager) runWorker(ctx context.Context, runID, workerID uuid.UUID) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		claimed, err := manager.scheduler.Claim(ctx, workerID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			manager.recordWorkerError(runID, workerID, err)
			if !waitFor(ctx, manager.pollInterval) {
				return
			}
			continue
		}
		if claimed == nil {
			complete, err := manager.queueComplete(ctx, runID)
			if err != nil {
				manager.recordWorkerError(runID, workerID, err)
			} else if complete {
				manager.markCompleted(runID, workerID)
				return
			} else {
				manager.markIdle(runID, workerID)
			}
			if !waitFor(ctx, manager.pollInterval) {
				return
			}
			continue
		}

		manager.markWorking(runID, workerID, claimed.Job.ID)
		duration := manager.jobDuration(runID, claimed.Job)
		continueWorker, completed := manager.execute(ctx, workerID, *claimed, duration)
		if !continueWorker {
			return
		}
		if completed {
			manager.markJobCompleted(runID, workerID)
		} else {
			manager.markIdle(runID, workerID)
		}
	}
}

func (manager *Manager) execute(
	ctx context.Context,
	workerID uuid.UUID,
	claimed Claim,
	duration time.Duration,
) (bool, bool) {
	work := time.NewTimer(duration)
	defer work.Stop()
	renew := time.NewTicker(manager.renewInterval)
	defer renew.Stop()

	for {
		select {
		case <-ctx.Done():
			return false, false
		case <-renew.C:
			if err := manager.scheduler.RenewLease(ctx, workerID, claimed.Job.ID, claimed.LeaseToken); err != nil {
				if ctx.Err() != nil {
					return false, false
				}
				manager.logger.Warn("demo worker lost lease", "worker_id", workerID, "job_id", claimed.Job.ID, "error", err)
				return true, false
			}
		case <-work.C:
			result, _ := json.Marshal(map[string]any{
				"demo":       true,
				"workerId":   workerID,
				"durationMs": duration.Milliseconds(),
			})
			if err := manager.scheduler.Complete(ctx, workerID, claimed.Job.ID, claimed.LeaseToken, result); err != nil {
				if ctx.Err() != nil {
					return false, false
				}
				manager.logger.Warn("demo worker could not complete job", "worker_id", workerID, "job_id", claimed.Job.ID, "error", err)
				return true, false
			}
			return true, true
		}
	}
}

func (manager *Manager) jobDuration(runID uuid.UUID, stored Job) time.Duration {
	var payload struct {
		DurationMS int64 `json:"durationMs"`
	}
	if json.Unmarshal(stored.Payload, &payload) == nil && payload.DurationMS > 0 {
		return time.Duration(payload.DurationMS) * time.Millisecond
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if state := manager.runs[runID]; state != nil {
		return time.Duration(state.view.WorkDurationMS) * time.Millisecond
	}
	return time.Second
}

func (manager *Manager) queueComplete(ctx context.Context, runID uuid.UUID) (bool, error) {
	manager.mu.RLock()
	state := manager.runs[runID]
	if state == nil {
		manager.mu.RUnlock()
		return false, ErrRunNotFound
	}
	queue, expected := state.view.Queue, state.view.JobCount
	manager.mu.RUnlock()
	jobs, err := manager.scheduler.ListJobs(ctx, queue)
	if err != nil {
		return false, err
	}
	if len(jobs) < expected {
		return false, nil
	}
	for _, stored := range jobs {
		switch stored.State {
		case "succeeded", "failed", "cancelled":
		default:
			return false, nil
		}
	}
	return true, nil
}

func (manager *Manager) markWorking(runID, workerID, jobID uuid.UUID) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if process := manager.worker(runID, workerID); process != nil && process.view.Status != WorkerKilled {
		process.view.Status = WorkerWorking
		process.view.CurrentJobID = &jobID
		process.view.LastError = ""
	}
}

func (manager *Manager) markIdle(runID, workerID uuid.UUID) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if process := manager.worker(runID, workerID); process != nil && process.view.Status != WorkerKilled {
		process.view.Status = WorkerIdle
		process.view.CurrentJobID = nil
	}
}

func (manager *Manager) markJobCompleted(runID, workerID uuid.UUID) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if process := manager.worker(runID, workerID); process != nil && process.view.Status != WorkerKilled {
		process.view.Status = WorkerIdle
		process.view.CurrentJobID = nil
		process.view.Completed++
		process.view.LastError = ""
	}
}

func (manager *Manager) markCompleted(runID, workerID uuid.UUID) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	state := manager.runs[runID]
	if state == nil {
		return
	}
	if process := state.workers[workerID]; process != nil && process.view.Status != WorkerKilled {
		process.view.Status = WorkerStopped
		process.view.CurrentJobID = nil
	}
	if state.view.Status != RunCompleted && state.view.Status != RunFailed {
		now := manager.now().UTC()
		state.view.Status = RunCompleted
		state.view.CompletedAt = &now
		manager.logger.Info("demo run completed", "run_id", runID)
	}
}

func (manager *Manager) recordWorkerError(runID, workerID uuid.UUID, err error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if process := manager.worker(runID, workerID); process != nil && process.view.Status != WorkerKilled {
		process.view.Status = WorkerIdle
		process.view.CurrentJobID = nil
		process.view.LastError = err.Error()
	}
}

func (manager *Manager) worker(runID, workerID uuid.UUID) *workerProcess {
	state := manager.runs[runID]
	if state == nil {
		return nil
	}
	return state.workers[workerID]
}

func (manager *Manager) failRun(id uuid.UUID, cause error) (Run, error) {
	manager.mu.Lock()
	state := manager.runs[id]
	for _, process := range state.workers {
		if process.view.Status != WorkerKilled && process.view.Status != WorkerStopped {
			process.view.Status = WorkerStopped
			process.view.CurrentJobID = nil
			process.cancel()
		}
	}
	state.view.Status = RunFailed
	state.view.LastError = cause.Error()
	result := copyRun(state)
	manager.mu.Unlock()
	return result, cause
}

func validateStart(input StartRunInput) error {
	switch {
	case input.JobCount < minimumJobs || input.JobCount > maximumJobs:
		return fmt.Errorf("%w: jobCount must be between %d and %d", ErrInvalidRun, minimumJobs, maximumJobs)
	case input.WorkerCount < minimumWorkers || input.WorkerCount > maximumWorkers:
		return fmt.Errorf("%w: workerCount must be between %d and %d", ErrInvalidRun, minimumWorkers, maximumWorkers)
	case input.WorkDuration < minimumWorkDuration || input.WorkDuration > maximumWorkDuration:
		return fmt.Errorf("%w: workDurationMs must be between %d and %d", ErrInvalidRun, minimumWorkDuration.Milliseconds(), maximumWorkDuration.Milliseconds())
	default:
		return nil
	}
}

func copyRun(state *runState) Run {
	result := state.view
	result.Workers = make([]DemoWorker, 0, len(state.workers))
	for _, process := range state.workers {
		worker := process.view
		if worker.CurrentJobID != nil {
			jobID := *worker.CurrentJobID
			worker.CurrentJobID = &jobID
		}
		result.Workers = append(result.Workers, worker)
	}
	sort.Slice(result.Workers, func(left, right int) bool {
		return result.Workers[left].Name < result.Workers[right].Name
	})
	return result
}

func hasActiveWorker(state *runState) bool {
	for _, process := range state.workers {
		if process.view.Status != WorkerKilled && process.view.Status != WorkerStopped {
			return true
		}
	}
	return false
}

func waitFor(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func randomWorkDuration(base time.Duration) time.Duration {
	// Each job takes between 80% and 120% of the requested duration so repeated
	// demos have natural variation while exercising the same scheduler rules.
	percentage := 80 + rand.IntN(41)
	return base * time.Duration(percentage) / 100
}
