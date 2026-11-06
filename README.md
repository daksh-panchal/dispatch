# Dispatch

Dispatch is a durable distributed job scheduler written in Go. It accepts
jobs, persists their state in PostgreSQL, and coordinates workers that claim and
execute them safely—even when requests are repeated or workers disappear.

This project demonstrates the core reliability
patterns behind job schedulers: durable state, concurrent worker
coordination, failure recovery, lifecycle events, and operational visibility.

## Key features

- Durable job submission with idempotency and conflict detection
- Atomic, priority-aware job claiming across concurrent workers
- Time-limited leases, heartbeats, and stale-worker fencing
- Automatic recovery when a worker crashes or stops renewing its lease
- Bounded retries with exponential backoff
- Safe completion, failure, and cancellation under concurrency
- Transactional outbox events for durable lifecycle history and live streaming
- Structured logs, metrics, and distributed traces through OpenTelemetry
- Paginated operational views and a resumable live event stream
- An interactive failure-recovery demo using the real public API

## Architecture

```mermaid
flowchart LR
    Browser[Demo console] --> Demo[Demo orchestrator]
    Demo -->|public HTTP API| Scheduler[Dispatch scheduler]
    Scheduler --> Database[(PostgreSQL)]
    Scheduler --> Outbox[Durable event outbox]
    Scheduler -. OTLP .-> Collector[Optional telemetry collector]
```

The scheduler owns durable job state, leases, retries, recovery, and lifecycle
events. The demo orchestrator creates simulated workers but uses the same public
API that any external client or worker would use. Neither the browser nor the
demo process accesses PostgreSQL directly.

## Key design decisions

**PostgreSQL is the source of truth.** Job state and ownership survive process
restarts. Unique constraints enforce idempotency, while row-level locking with
`FOR UPDATE SKIP LOCKED` lets workers claim different jobs concurrently without
claiming the same job twice.

**Leases represent temporary ownership.** A worker must renew its lease while a
job is running. If the lease expires, a background recovery loop makes the job
eligible for another attempt. Lease tokens fence off stale workers so that an
old worker cannot overwrite a newer result.

**State changes and events are committed together.** Every important job
transition writes a versioned outbox event in the same transaction. This avoids
the failure window where database state changes but its corresponding lifecycle
event is lost. Those durable events support operational history and the
resumable live stream exposed by Dispatch.

**The scheduler remains transport-independent.** HTTP handlers translate
requests into application operations; PostgreSQL code owns persistence; core
packages own scheduling rules. These are package boundaries within one Go
application, not independently deployed microservices.

**The demo is a real client.** Its simulated workers register, claim, renew,
complete, and fail work through the public API. The UI therefore demonstrates
the actual scheduler rather than a separate scripted version of it.

## Project structure

```text
cmd/          API and demo process entry points
internal/     scheduler, persistence, telemetry, and demo packages
migrations/   embedded PostgreSQL schema migrations
integration/  end-to-end tests against a real PostgreSQL container
requests/     example HTTP requests for manual exploration
```

## Requirements

- Docker Desktop for the one-command demo and integration tests
- Go 1.27 or newer for direct local development

## Quick demo

Start PostgreSQL, the scheduler, and the demo console with one command:

```powershell
docker compose up --build
```

Compose waits for PostgreSQL to become healthy before starting Dispatch, then
waits for Dispatch readiness before starting the demo. When all three services
are healthy, open [http://127.0.0.1:5090](http://127.0.0.1:5090).

For a useful failure-recovery demonstration:

1. Start 8 jobs with 3 workers and an 8-second duration.
2. Kill a worker while it is marked `WORKING`.
3. Watch its lease expire and the job emit `JobRecovered`.
4. Watch a surviving worker claim the recovered job as its next attempt.
5. Confirm that the run still reaches 100% completion.

Compose uses short lease and retry timings so recovery is visible without a
long wait. These are ordinary scheduler settings, not a demo-only recovery
path. Every service is published on `127.0.0.1`, so the demo is accessible only
from the computer running it.

Press `Ctrl+C` to stop a foreground run. Remove the stopped containers and
network with:

```powershell
docker compose down
```

This preserves the PostgreSQL volume, so jobs and events remain available on
the next run. To deliberately reset the local demo to a completely fresh
database, remove the volume instead:

```powershell
docker compose down --volumes
```

This reset command permanently deletes only the disposable local database. The
schema is recreated automatically the next time Compose starts.

To follow logs while running Compose in the background, use:

```powershell
docker compose up --build -d
docker compose logs -f dispatch-api dispatch-demo
```

## Development workflow

To run the Go processes directly during development, start only PostgreSQL:

```powershell
docker compose up -d postgres
```

Then start the scheduler and demo in separate terminals:

```powershell
go run ./cmd/dispatch-api
```

```powershell
go run ./cmd/dispatch-demo
```

The API automatically applies embedded migrations and listens on
`http://127.0.0.1:5080`. The demo listens on `http://127.0.0.1:5090`.
Example API requests are available in `requests/dispatch.http` and
`requests/dispatch-demo.http`.

Each demo run receives a unique ordinary queue. The orchestrator submits jobs,
registers workers, claims work, renews leases, and records outcomes exclusively
through the public Dispatch API. Demo run metadata is held in memory and resets
when the demo process restarts; scheduler jobs, workers, and lifecycle events
remain durable in PostgreSQL.

The complete machine-readable HTTP contract is available from the running API
at [http://127.0.0.1:5080/openapi/v1.json](http://127.0.0.1:5080/openapi/v1.json).

## Test

```powershell
go test ./...
go vet ./...
docker compose config --quiet
```

Unit tests require only Go. Integration tests require Docker because they start
and dispose of an isolated PostgreSQL container through Testcontainers. Together
they cover scheduling rules, concurrent claims, leases, retries, recovery,
outbox delivery, operational reads, telemetry, and the demo workflow.

The end-to-end recovery scenario can also be run on its own:

```powershell
go test ./integration -run "TestJobAPIAgainstPostgreSQL/demo_orchestrator_drives_work_and_survives_a_worker_crash" -v -count=1
```

That test uses real PostgreSQL and the public HTTP API. It starts a demo run,
kills a worker holding a lease, invokes recovery, verifies that exactly one job
is requeued, and waits for every job to succeed.

## Configuration

When the Go processes run directly, they use conservative application defaults.
Override them with environment variables:

```text
DATABASE_URL=postgres://user:password@host:5432/database?sslmode=require
HTTP_ADDR=127.0.0.1:5080
LEASE_DURATION=30s
RETRY_BASE=5s
RETRY_MAXIMUM=5m
RECOVERY_INTERVAL=2s
RECOVERY_BATCH_SIZE=100
SHUTDOWN_TIMEOUT=10s
DISPATCH_ENVIRONMENT=development
DISPATCH_VERSION=dev
DEMO_HTTP_ADDR=127.0.0.1:5090
DISPATCH_API_URL=http://127.0.0.1:5080
DEMO_SHUTDOWN_TIMEOUT=10s
```

Docker Compose supplies container addresses and shorter demo timings. Its ports
and timing settings can be customized without editing `compose.yaml`:

```powershell
Copy-Item .env.example .env
```

The committed `.env.example` documents every Compose override. The resulting
`.env` file is ignored by Git and is optional—the built-in values support the
one-command demo without any setup.

To export traces and metrics, point Dispatch at any OTLP/HTTP-compatible
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

The local API, demo, and PostgreSQL ports bind to `127.0.0.1`, so they are
reachable from this computer only, not from other devices on the local network.
A future deployment can deliberately override these bindings behind appropriate
network controls and authentication.

## Standalone scope

Dispatch's standalone portfolio scope is complete: it provides durable
scheduling, worker coordination, retries, crash recovery, operational reads,
telemetry instrumentation, and a repeatable interactive demonstration.

Clients and workers interact through its documented HTTP
API, PostgreSQL stores scheduler state and lifecycle events, the included demo
visualizes real scheduler behaviour, and optional OpenTelemetry export allows
standard external tooling to collect traces and metrics if desired.
