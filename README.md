# work-flow

`work-flow` is a distributed job scheduler. It
accepts jobs, persists their state, and coordinates workers that claim them for
execution.

Milestone 4 adds cancellation and atomic lifecycle races. Queued and running
jobs can be cancelled without allowing a worker's late result to overwrite the
terminal state.

## Structure

```text
cmd/workflow-api       process entry point and dependency wiring
internal/httpapi       HTTP transport
internal/job           job model and application operations
internal/worker        worker registration, heartbeat, and claim operations
internal/recovery      background recovery of expired worker leases
internal/postgres      PostgreSQL repository and migration runner
migrations             embedded SQL schema migrations
integration            end-to-end tests with a real PostgreSQL container
```

This is one Go module and one deployable API. Package boundaries keep scheduling
rules separate from HTTP and PostgreSQL without introducing separate services.

## Current behaviour

- `POST /api/v1/jobs` creates a queued job and returns `201 Created`.
- Repeating the same request and `Idempotency-Key` returns the original job with
  `200 OK`.
- Reusing a key with different request contents returns `409 Conflict`.
- `GET /api/v1/jobs/{id}` returns persisted state or `404 Not Found`.
- `POST /api/v1/jobs/{id}/cancel` moves a queued or running job to `cancelled`,
  clears any active lease, and returns the existing job when repeated.
- PostgreSQL's unique constraint provides the final concurrency guarantee for
  idempotent submission.
- `POST /api/v1/workers` registers a worker for one queue.
- `POST /api/v1/workers/{workerId}/heartbeat` records that a worker is alive.
- `POST /api/v1/workers/{workerId}/claims` atomically claims the highest-priority
  eligible job, or returns `204 No Content` when the queue is empty.
- A claim moves the job from `queued` to `running`, increments its attempt count,
  and grants the worker a 30-second lease.
- `POST /api/v1/workers/{workerId}/jobs/{jobId}/lease` renews a valid lease.
- `POST /api/v1/workers/{workerId}/jobs/{jobId}/complete` records a successful
  result and moves the job to `succeeded`.
- `POST /api/v1/workers/{workerId}/jobs/{jobId}/fail` records an error. The job
  is delayed and requeued when attempts remain, or moved to `failed` when its
  retry budget is exhausted.
- Retry delays use capped exponential backoff: 5 seconds, 10 seconds, 20
  seconds, and so on, up to 5 minutes.
- A background recovery loop finds expired leases every 2 seconds. It safely
  requeues the job or permanently fails it when no attempts remain.
- Completion, failure, and renewal require the current worker ID, lease token,
  and an unexpired lease. A stale worker receives `409 Conflict`.
- Cancellation, completion, and failure use conditional PostgreSQL updates, so
  concurrent terminal transitions have exactly one winner. Cancelling a job
  that already succeeded or failed returns `409 Conflict`.
- PostgreSQL row locking with `FOR UPDATE SKIP LOCKED` prevents concurrent workers
  from claiming or recovering the same job.

## Requirements

- Go 1.27 or newer
- Docker Desktop

## Run locally

Start PostgreSQL:

```powershell
docker compose up -d
```

Restore modules and start the API:

```powershell
go mod download
go run ./cmd/workflow-api
```

The API automatically applies embedded database migrations and listens on
`http://localhost:5080`.

Useful endpoints:

```text
GET  /health/live
GET  /health/ready
GET  /openapi/v1.json
POST /api/v1/jobs
GET  /api/v1/jobs/{id}
POST /api/v1/jobs/{id}/cancel
POST /api/v1/workers
POST /api/v1/workers/{workerId}/heartbeat
POST /api/v1/workers/{workerId}/claims
POST /api/v1/workers/{workerId}/jobs/{jobId}/lease
POST /api/v1/workers/{workerId}/jobs/{jobId}/complete
POST /api/v1/workers/{workerId}/jobs/{jobId}/fail
```

Example requests are in `requests/work-flow.http`.

## Test

```powershell
go test ./...
```

Unit tests require only Go. Integration tests require Docker because they start
and dispose of an isolated PostgreSQL container through Testcontainers.

## Configuration

Local defaults match `compose.yaml`. Override them with environment variables:

```text
DATABASE_URL=postgres://user:password@host:5432/database?sslmode=require
HTTP_ADDR=127.0.0.1:5080
```

The committed `workflow_dev` password protects only the disposable local Docker
database and must never be reused outside local development.

The local API and PostgreSQL port bind to `127.0.0.1`, so they are reachable
from this computer only, not from other devices on the local network. A future
deployment can deliberately override these bindings behind appropriate network
controls and authentication.

## Later milestones

- Durable scheduler events and Message-Flow integration
- OpenTelemetry observability and operational hardening
