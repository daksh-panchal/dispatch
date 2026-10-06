# work-flow

`work-flow` is a distributed job scheduler. It
accepts jobs, persists their state, and coordinates workers that claim them for
execution. Later milestones will add failure recovery, retries, completion, and
cancellation.

Milestone 2 implements worker registration and exclusive job claiming with
renewable leases. Job execution is deliberately outside this milestone.

## Structure

```text
cmd/workflow-api       process entry point and dependency wiring
internal/httpapi       HTTP transport
internal/job           job model and application operations
internal/worker        worker registration, heartbeat, and claim operations
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
- PostgreSQL's unique constraint provides the final concurrency guarantee for
  idempotent submission.
- `POST /api/v1/workers` registers a worker for one queue.
- `POST /api/v1/workers/{workerId}/heartbeat` records that a worker is alive.
- `POST /api/v1/workers/{workerId}/claims` atomically claims the highest-priority
  eligible job, or returns `204 No Content` when the queue is empty.
- A claim moves the job from `queued` to `running`, increments its attempt count,
  and grants the worker a 30-second lease.
- `POST /api/v1/workers/{workerId}/jobs/{jobId}/lease` renews a valid lease.
- PostgreSQL row locking with `FOR UPDATE SKIP LOCKED` prevents concurrent workers
  from claiming the same job.

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
POST /api/v1/workers
POST /api/v1/workers/{workerId}/heartbeat
POST /api/v1/workers/{workerId}/claims
POST /api/v1/workers/{workerId}/jobs/{jobId}/lease
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
HTTP_ADDR=:5080
```

The committed `workflow_dev` password protects only the disposable local Docker
database and must never be reused outside local development.

## Later milestones

- Expired-lease recovery and worker failure detection
- Retry scheduling and recovery
- Completion and cancellation races
- OpenTelemetry and production hardening
