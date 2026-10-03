# work-flow

`work-flow` is a distributed job scheduler. It
accepts jobs, persists their state, and will eventually coordinate worker
leases, failure recovery, retries, and cancellation.

Milestone 1 implements a durable job inbox in Go: clients can submit a job once
and retrieve its persisted state. Job execution is deliberately outside this
milestone.

## Structure

```text
cmd/workflow-api       process entry point and dependency wiring
internal/httpapi       HTTP transport
internal/job           job model and application operations
internal/postgres      PostgreSQL repository and migration runner
migrations             embedded SQL schema migrations
integration            end-to-end tests with a real PostgreSQL container
```

This is one Go module and one deployable API. Package boundaries keep scheduling
rules separate from HTTP and PostgreSQL without introducing separate services.

## Milestone 1 behaviour

- `POST /api/v1/jobs` creates a queued job and returns `201 Created`.
- Repeating the same request and `Idempotency-Key` returns the original job with
  `200 OK`.
- Reusing a key with different request contents returns `409 Conflict`.
- `GET /api/v1/jobs/{id}` returns persisted state or `404 Not Found`.
- PostgreSQL's unique constraint provides the final concurrency guarantee for
  idempotent submission.

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

- Worker registration and atomic job claiming
- Leases, heartbeats, and worker failure detection
- Retry scheduling and recovery
- Completion and cancellation races
- OpenTelemetry and production hardening
