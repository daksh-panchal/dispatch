# work-flow

`work-flow` is a distributed job scheduler. It
accepts jobs, persists their state, and coordinates workers that claim them for
execution.

Milestone 6 adds OpenTelemetry observability and operational hardening. The
scheduler now produces standard traces and metrics, correlates structured logs,
validates runtime configuration, and shuts down its HTTP and background work
gracefully.

## Structure

```text
cmd/workflow-api       process entry point and dependency wiring
internal/httpapi       HTTP transport
internal/job           job model and application operations
internal/worker        worker registration, heartbeat, and claim operations
internal/recovery      background recovery of expired worker leases
internal/outbox        durable event model and background publishing workflow
internal/observability OpenTelemetry setup, metrics, and log correlation
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
- Job submission, claiming, retry scheduling, recovery, completion, failure, and
  cancellation write versioned events to `job_outbox_events` in the same
  PostgreSQL transaction as their state change.
- Outbox events use short-lived claim leases so multiple publisher instances can
  work concurrently without publishing the same event at the same time.
- Failed publications use capped exponential backoff. An expired publisher claim
  becomes eligible again, providing at-least-once delivery after crashes.
- Event IDs are stable across redelivery so Message-Flow consumers can safely
  deduplicate repeated deliveries.
- HTTP requests and background recovery/outbox work produce OpenTelemetry
  spans. W3C trace context is accepted from upstream services and propagated
  through request contexts.
- Metrics cover HTTP outcomes, recovery and outbox activity, jobs by state,
  active and expired leases, and the pending outbox backlog.
- Logs are emitted as structured JSON and include trace and span IDs whenever
  they are written from an active trace.
- OpenTelemetry uses OTLP over HTTP when an exporter endpoint is configured.
  Without one, Work-Flow runs normally with zero external telemetry dependency.
- Runtime durations and batch sizes are validated at startup so invalid
  operational settings fail fast.
- Shutdown stops new HTTP traffic, cancels the recovery loop, waits for
  background work, closes PostgreSQL, and flushes telemetry within a bounded
  timeout.

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
and dispose of an isolated PostgreSQL container through Testcontainers. The
integration suite verifies ordered lifecycle events, concurrent outbox claims,
claim-token enforcement, redelivery after a publisher claim expires, and the
database statistics used by operational gauges. Unit tests cover telemetry
collection, log correlation, and configuration validation.

## Configuration

Local defaults match `compose.yaml`. Override them with environment variables:

```text
DATABASE_URL=postgres://user:password@host:5432/database?sslmode=require
HTTP_ADDR=127.0.0.1:5080
LEASE_DURATION=30s
RETRY_BASE=5s
RETRY_MAXIMUM=5m
RECOVERY_INTERVAL=2s
RECOVERY_BATCH_SIZE=100
SHUTDOWN_TIMEOUT=10s
WORKFLOW_ENVIRONMENT=development
WORKFLOW_VERSION=dev
```

To export traces and metrics, point Work-Flow at any OTLP/HTTP-compatible
collector. Signal-specific OpenTelemetry environment variables are also
supported by the standard exporters.

```text
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
```

Set `OTEL_SDK_DISABLED=true` to explicitly disable telemetry export. Collector
availability is not part of API readiness: an observability outage must not
prevent jobs from being scheduled.

The committed `workflow_dev` password protects only the disposable local Docker
database and must never be reused outside local development.

The local API and PostgreSQL port bind to `127.0.0.1`, so they are reachable
from this computer only, not from other devices on the local network. A future
deployment can deliberately override these bindings behind appropriate network
controls and authentication.

## Next phase

The six Work-Flow milestones now provide the scheduler foundation. The next
phase connects its existing outbox publisher interface to Message-Flow, uses it
as Build-Flow's execution backbone, and sends this telemetry to Observe-Flow for
storage and visualization.

The outbox publisher is deliberately transport-independent. It is not started
by the API until Message-Flow defines its publishing protocol; its adapter can
then implement the existing `outbox.Publisher` interface without changing job
transactions or delivery guarantees.
