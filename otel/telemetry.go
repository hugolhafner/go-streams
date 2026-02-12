package otel

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	traceNoop "go.opentelemetry.io/otel/trace/noop"
)

const scopeName = "github.com/hugolhafner/go-streams"

// Telemetry holds all OpenTelemetry instruments for the go-streams library
// When no providers are configured, all instruments are noops with zero overhead
type Telemetry struct {
	Tracer     trace.Tracer
	Propagator propagation.TextMapPropagator

	// Consumer metrics
	MessagesConsumed metric.Int64Counter
	PollDuration     metric.Float64Histogram

	// Processing metrics
	ProcessDuration metric.Float64Histogram

	// Producer metrics
	MessagesProduced metric.Int64Counter
	ProduceDuration  metric.Float64Histogram

	// Error metrics
	Errors              metric.Int64Counter
	ErrorHandlerActions metric.Int64Counter

	// Runner state metrics
	TasksActive metric.Int64UpDownCounter
}

// NewTelemetry creates a Telemetry instance from the given providers.
// all providers are optional and defaulted to noops if nil
func NewTelemetry(tp trace.TracerProvider, mp metric.MeterProvider, prop propagation.TextMapPropagator) (
	*Telemetry, error,
) {
	if tp == nil {
		tp = traceNoop.NewTracerProvider()
	}
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	if prop == nil {
		prop = propagation.TraceContext{}
	}

	tracer := tp.Tracer(scopeName)
	meter := mp.Meter(scopeName)

	messagesConsumed, err := meter.Int64Counter(
		"messaging.consumer.messages",
		metric.WithDescription("Records consumed"),
	)
	if err != nil {
		return nil, err
	}

	pollDuration, err := meter.Float64Histogram(
		"stream.poll.duration",
		metric.WithDescription("Time per Poll() call"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	processDuration, err := meter.Float64Histogram(
		"stream.process.duration",
		metric.WithDescription("End-to-end record processing time"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	messagesProduced, err := meter.Int64Counter(
		"messaging.producer.messages",
		metric.WithDescription("Records produced"),
	)
	if err != nil {
		return nil, err
	}

	produceDuration, err := meter.Float64Histogram(
		"stream.produce.duration",
		metric.WithDescription("Time per Send() call"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	errors, err := meter.Int64Counter(
		"stream.errors",
		metric.WithDescription("Processing errors encountered"),
	)
	if err != nil {
		return nil, err
	}

	errorHandlerActions, err := meter.Int64Counter(
		"stream.error_handler.actions",
		metric.WithDescription("Error handler decisions"),
	)
	if err != nil {
		return nil, err
	}

	tasksActive, err := meter.Int64UpDownCounter(
		"stream.tasks.active",
		metric.WithDescription("Active tasks (partitions)"),
	)
	if err != nil {
		return nil, err
	}

	return &Telemetry{
		Tracer:              tracer,
		Propagator:          prop,
		MessagesConsumed:    messagesConsumed,
		PollDuration:        pollDuration,
		ProcessDuration:     processDuration,
		MessagesProduced:    messagesProduced,
		ProduceDuration:     produceDuration,
		Errors:              errors,
		ErrorHandlerActions: errorHandlerActions,
		TasksActive:         tasksActive,
	}, nil
}

// Noop returns a Telemetry instance with all noop instruments
func Noop() *Telemetry {
	t, _ := NewTelemetry(nil, nil, nil)
	return t
}
