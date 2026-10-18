package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

type fixedStatsReader struct {
	called bool
}

func (reader *fixedStatsReader) ReadSchedulerStats(
	context.Context,
) (map[string]int64, int64, int64, int64, error) {
	reader.called = true
	return map[string]int64{"queued": 2, "running": 1}, 1, 0, 3, nil
}

func TestMetricsCollectSchedulerState(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	stats := &fixedStatsReader{}
	metrics, err := NewMetrics(
		provider.Meter("test"),
		stats,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	metrics.RecordHTTPRequest(context.Background(), "POST", "/api/v1/jobs", 201, 25*time.Millisecond)
	metrics.RecordRecovery(context.Background(), 2, 1, 10*time.Millisecond, nil)

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	if !stats.called {
		t.Fatal("scheduler stats reader was not called")
	}

	names := make(map[string]bool)
	for _, scope := range collected.ScopeMetrics {
		for _, measured := range scope.Metrics {
			names[measured.Name] = true
		}
	}
	for _, name := range []string{
		"workflow.http.requests",
		"workflow.http.request.duration",
		"workflow.recovery.jobs",
		"workflow.jobs",
		"workflow.leases.active",
		"workflow.leases.expired",
		"workflow.outbox.pending",
	} {
		if !names[name] {
			t.Errorf("metric %q was not collected", name)
		}
	}
}

func TestSetupCanDisableExport(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")

	telemetry, err := Setup(context.Background(), "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if telemetry.TracesEnabled || telemetry.MetricsEnabled {
		t.Fatalf("disabled telemetry = %+v", telemetry)
	}
	if err := telemetry.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTraceHandlerAddsCorrelationIDs(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(NewTraceHandler(slog.NewJSONHandler(&output, nil)))
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1},
		SpanID:  trace.SpanID{2},
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)

	logger.InfoContext(ctx, "correlated")

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["trace_id"] != spanContext.TraceID().String() || record["span_id"] != spanContext.SpanID().String() {
		t.Fatalf("log correlation fields = %+v", record)
	}
}
