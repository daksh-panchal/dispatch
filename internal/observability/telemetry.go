package observability

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/daksh-panchal/work-flow"

type Telemetry struct {
	TracesEnabled  bool
	MetricsEnabled bool
	shutdown       []func(context.Context) error
}

func Setup(ctx context.Context, serviceVersion, environment string) (*Telemetry, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	configured := &Telemetry{}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true") {
		return configured, nil
	}

	serviceResource, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			attribute.String("service.name", "work-flow"),
			attribute.String("service.version", serviceVersion),
			attribute.String("deployment.environment.name", environment),
		),
	)
	if err != nil {
		return nil, err
	}

	commonEndpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	traceEndpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
	metricEndpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"))

	if commonEndpoint != "" || traceEndpoint != "" {
		exporter, exportErr := otlptracehttp.New(ctx)
		if exportErr != nil {
			return nil, exportErr
		}
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(serviceResource),
		)
		otel.SetTracerProvider(provider)
		configured.TracesEnabled = true
		configured.shutdown = append(configured.shutdown, provider.Shutdown)
	}

	if commonEndpoint != "" || metricEndpoint != "" {
		exporter, exportErr := otlpmetrichttp.New(ctx)
		if exportErr != nil {
			_ = configured.Shutdown(context.Background())
			return nil, exportErr
		}
		provider := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
			sdkmetric.WithResource(serviceResource),
		)
		otel.SetMeterProvider(provider)
		configured.MetricsEnabled = true
		configured.shutdown = append(configured.shutdown, provider.Shutdown)
	}

	return configured, nil
}

func (telemetry *Telemetry) Shutdown(ctx context.Context) error {
	var shutdownErrors []error
	for index := len(telemetry.shutdown) - 1; index >= 0; index-- {
		if err := telemetry.shutdown[index](ctx); err != nil {
			shutdownErrors = append(shutdownErrors, err)
		}
	}
	return errors.Join(shutdownErrors...)
}

func Tracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}

func Meter() metric.Meter {
	return otel.Meter(instrumentationName)
}

type TraceHandler struct {
	slog.Handler
}

func NewTraceHandler(next slog.Handler) *TraceHandler {
	return &TraceHandler{Handler: next}
}

func (handler *TraceHandler) Handle(ctx context.Context, record slog.Record) error {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", spanContext.TraceID().String()),
			slog.String("span_id", spanContext.SpanID().String()),
		)
	}
	return handler.Handler.Handle(ctx, record)
}

func (handler *TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceHandler{Handler: handler.Handler.WithAttrs(attrs)}
}

func (handler *TraceHandler) WithGroup(name string) slog.Handler {
	return &TraceHandler{Handler: handler.Handler.WithGroup(name)}
}
