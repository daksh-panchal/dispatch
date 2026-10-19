package readmodel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/daksh-panchal/work-flow/internal/job"
	"github.com/daksh-panchal/work-flow/internal/worker"
	"github.com/google/uuid"
)

var readModelNow = time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)

func TestListJobsNormalizesAndValidatesQuery(t *testing.T) {
	repository := &fakeRepository{}
	service := NewService(repository, func() time.Time { return readModelNow }, time.Minute)

	if _, err := service.ListJobs(context.Background(), JobListParams{Queue: " builds "}); err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if repository.jobParams.Queue != "builds" || repository.jobParams.Limit != defaultPageSize {
		t.Fatalf("repository params = %+v", repository.jobParams)
	}

	_, err := service.ListJobs(context.Background(), JobListParams{State: job.State("unknown")})
	if !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("invalid state error = %v, want ErrInvalidQuery", err)
	}
	_, err = service.ListJobs(context.Background(), JobListParams{Limit: maximumPageSize + 1})
	if !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("invalid limit error = %v, want ErrInvalidQuery", err)
	}
}

func TestListWorkersDerivesOperationalStatus(t *testing.T) {
	busyJobID := uuid.New()
	repository := &fakeRepository{workerPage: StoredWorkerPage{Items: []StoredWorker{
		{Worker: worker.Worker{ID: uuid.New(), LastSeenAt: readModelNow}},
		{Worker: worker.Worker{ID: uuid.New(), LastSeenAt: readModelNow}, CurrentJobID: &busyJobID},
		{Worker: worker.Worker{ID: uuid.New(), LastSeenAt: readModelNow.Add(-2 * time.Minute)}},
	}}}
	service := NewService(repository, func() time.Time { return readModelNow }, time.Minute)

	page, err := service.ListWorkers(context.Background(), WorkerListParams{})
	if err != nil {
		t.Fatalf("ListWorkers() error = %v", err)
	}
	want := []WorkerStatus{WorkerIdle, WorkerBusy, WorkerOffline}
	for index, expected := range want {
		if page.Items[index].Status != expected {
			t.Fatalf("worker %d status = %q, want %q", index, page.Items[index].Status, expected)
		}
	}
}

func TestListEventsRejectsNegativeCursor(t *testing.T) {
	service := NewService(&fakeRepository{}, func() time.Time { return readModelNow }, time.Minute)
	_, err := service.ListEvents(context.Background(), EventListParams{AfterSequence: -1})
	if !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("ListEvents() error = %v, want ErrInvalidQuery", err)
	}
}

type fakeRepository struct {
	jobParams   JobListParams
	workerPage  StoredWorkerPage
	eventParams EventListParams
}

func (repository *fakeRepository) ListJobs(_ context.Context, params JobListParams) (JobPage, error) {
	repository.jobParams = params
	return JobPage{}, nil
}

func (repository *fakeRepository) ListWorkers(
	_ context.Context,
	_ WorkerListParams,
) (StoredWorkerPage, error) {
	return repository.workerPage, nil
}

func (repository *fakeRepository) ListEvents(_ context.Context, params EventListParams) (EventPage, error) {
	repository.eventParams = params
	return EventPage{}, nil
}
