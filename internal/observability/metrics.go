package observability

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type StatsReader interface {
	ReadSchedulerStats(context.Context) (
		jobCounts map[string]int64,
		activeLeases int64,
		expiredLeases int64,
		pendingOutboxEvents int64,
		err error,
	)
}

type Metrics struct {
	httpRequests       metric.Int64Counter
	httpDuration       metric.Float64Histogram
	recoveryJobs       metric.Int64Counter
	outboxEvents       metric.Int64Counter
	backgroundDuration metric.Float64Histogram
}

func NewMetrics(meter metric.Meter, reader StatsReader, logger *slog.Logger) (*Metrics, error) {
	httpRequests, err := meter.Int64Counter(
		"workflow.http.requests",
		metric.WithDescription("Number of HTTP requests handled by Work-Flow"),
	)
	if err != nil {
		return nil, err
	}
	httpDuration, err := meter.Float64Histogram(
		"workflow.http.request.duration",
		metric.WithDescription("Duration of HTTP requests handled by Work-Flow"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	recoveryJobs, err := meter.Int64Counter(
		"workflow.recovery.jobs",
		metric.WithDescription("Number of expired jobs processed by recovery"),
	)
	if err != nil {
		return nil, err
	}
	outboxEvents, err := meter.Int64Counter(
		"workflow.outbox.events",
		metric.WithDescription("Number of outbox events processed for publication"),
	)
	if err != nil {
		return nil, err
	}
	backgroundDuration, err := meter.Float64Histogram(
		"workflow.background.duration",
		metric.WithDescription("Duration of Work-Flow background operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	metrics := &Metrics{
		httpRequests:       httpRequests,
		httpDuration:       httpDuration,
		recoveryJobs:       recoveryJobs,
		outboxEvents:       outboxEvents,
		backgroundDuration: backgroundDuration,
	}
	if reader != nil {
		if err := metrics.registerSchedulerGauges(meter, reader, logger); err != nil {
			return nil, err
		}
	}
	return metrics, nil
}

func (metrics *Metrics) RecordHTTPRequest(
	ctx context.Context,
	method string,
	route string,
	status int,
	duration time.Duration,
) {
	metrics.httpRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("http.request.method", method),
		attribute.String("http.route", route),
		attribute.Int("http.response.status_code", status),
	))
	metrics.httpDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("http.request.method", method),
		attribute.String("http.route", route),
		attribute.Int("http.response.status_code", status),
	))
}

func (metrics *Metrics) RecordRecovery(
	ctx context.Context,
	requeued int64,
	failed int64,
	duration time.Duration,
	err error,
) {
	if requeued > 0 {
		metrics.recoveryJobs.Add(ctx, requeued, metric.WithAttributes(attribute.String("outcome", "requeued")))
	}
	if failed > 0 {
		metrics.recoveryJobs.Add(ctx, failed, metric.WithAttributes(attribute.String("outcome", "failed")))
	}
	metrics.backgroundDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("workflow.operation", "recovery"),
		attribute.Bool("error", err != nil),
	))
}

func (metrics *Metrics) RecordOutbox(
	ctx context.Context,
	claimed int,
	published int,
	failed int,
	duration time.Duration,
	err error,
) {
	if published > 0 {
		metrics.outboxEvents.Add(ctx, int64(published), metric.WithAttributes(attribute.String("outcome", "published")))
	}
	if failed > 0 {
		metrics.outboxEvents.Add(ctx, int64(failed), metric.WithAttributes(attribute.String("outcome", "failed")))
	}
	if claimed == 0 {
		metrics.outboxEvents.Add(ctx, 0, metric.WithAttributes(attribute.String("outcome", "idle")))
	}
	metrics.backgroundDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("workflow.operation", "outbox.publish"),
		attribute.Bool("error", err != nil),
	))
}

func (metrics *Metrics) registerSchedulerGauges(
	meter metric.Meter,
	reader StatsReader,
	logger *slog.Logger,
) error {
	jobs, err := meter.Int64ObservableGauge(
		"workflow.jobs",
		metric.WithDescription("Current number of jobs by state"),
	)
	if err != nil {
		return err
	}
	activeLeases, err := meter.Int64ObservableGauge(
		"workflow.leases.active",
		metric.WithDescription("Current number of unexpired job leases"),
	)
	if err != nil {
		return err
	}
	expiredLeases, err := meter.Int64ObservableGauge(
		"workflow.leases.expired",
		metric.WithDescription("Current number of expired job leases awaiting recovery"),
	)
	if err != nil {
		return err
	}
	pendingOutbox, err := meter.Int64ObservableGauge(
		"workflow.outbox.pending",
		metric.WithDescription("Current number of unpublished outbox events"),
	)
	if err != nil {
		return err
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		jobCounts, active, expired, pending, readErr := reader.ReadSchedulerStats(ctx)
		if readErr != nil {
			logger.ErrorContext(ctx, "collect scheduler metrics", "error", readErr)
			return nil
		}
		for state, count := range jobCounts {
			observer.ObserveInt64(jobs, count, metric.WithAttributes(attribute.String("job.state", state)))
		}
		observer.ObserveInt64(activeLeases, active)
		observer.ObserveInt64(expiredLeases, expired)
		observer.ObserveInt64(pendingOutbox, pending)
		return nil
	}, jobs, activeLeases, expiredLeases, pendingOutbox)
	return err
}
