package otel

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
	traceNoop "go.opentelemetry.io/otel/trace/noop"
)

const scopeVersion = "v0.3.0" // x-release-please-version
const scopeName = "github.com/hugolhafner/go-streams"

const (
	unitSecond    = "s"
	unitMessage   = "{message}"
	unitRetry     = "{retry}"
	unitError     = "{error}"
	unitTask      = "{task}"
	unitRebalance = "{rebalance}"
)

// Telemetry holds all OpenTelemetry instruments for the go-streams library
// When no providers are configured, all instruments are noops with zero overhead
type Telemetry struct {
	Tracer     trace.Tracer
	Propagator propagation.TextMapPropagator

	meter metric.Meter

	// Consumer metrics
	MessagesConsumed metric.Int64Counter
	PollDuration     metric.Float64Histogram
	ConsumerLag      metric.Float64Histogram
	PollRecords      metric.Int64Histogram

	// Processing metrics
	ProcessDuration metric.Float64Histogram
	ProcessRetries  metric.Int64Counter

	// Producer metrics
	MessagesProduced metric.Int64Counter
	ProduceDuration  metric.Float64Histogram

	// Error metrics
	Errors metric.Int64Counter

	// Runner state metrics
	TasksActive            metric.Int64UpDownCounter
	RebalanceCount         metric.Int64Counter
	PartitionedQueueDepth  metric.Int64ObservableGauge
	PartitionedPausedDepth metric.Int64ObservableGauge

	// Service graph metrics
	NodeRecords metric.Int64Counter
	NodeLatency metric.Float64Histogram
	EdgeRecords metric.Int64Counter
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

	tracer := tp.Tracer(
		scopeName, trace.WithInstrumentationVersion(scopeVersion),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	meter := mp.Meter(
		scopeName, metric.WithInstrumentationVersion(scopeVersion),
		metric.WithSchemaURL(semconv.SchemaURL),
	)

	messagesConsumed, err := meter.Int64Counter(
		"messaging.consumer.messages",
		metric.WithDescription("Records consumed"),
		metric.WithUnit(unitMessage),
	)
	if err != nil {
		return nil, err
	}

	pollDuration, err := meter.Float64Histogram(
		"stream.poll.duration",
		metric.WithDescription("Time per Poll() call"),
		metric.WithUnit(unitSecond),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, err
	}

	consumerLag, err := meter.Float64Histogram(
		"stream.consumer.lag",
		metric.WithDescription("Time since record was produced"),
		metric.WithUnit(unitSecond),
		metric.WithExplicitBucketBoundaries(0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30, 60, 300),
	)
	if err != nil {
		return nil, err
	}

	pollRecords, err := meter.Int64Histogram(
		"stream.poll.records",
		metric.WithDescription("Records per poll batch"),
		metric.WithUnit(unitMessage),
		metric.WithExplicitBucketBoundaries(0, 1, 5, 10, 25, 50, 100, 250, 500, 1000),
	)
	if err != nil {
		return nil, err
	}

	processDuration, err := meter.Float64Histogram(
		"stream.process.duration",
		metric.WithDescription("End-to-end record processing time"),
		metric.WithUnit(unitSecond),
		metric.WithExplicitBucketBoundaries(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
	)
	if err != nil {
		return nil, err
	}

	processRetries, err := meter.Int64Counter(
		"stream.process.retries",
		metric.WithDescription("Individual retry attempts"),
		metric.WithUnit(unitRetry),
	)
	if err != nil {
		return nil, err
	}

	messagesProduced, err := meter.Int64Counter(
		"messaging.producer.messages",
		metric.WithDescription("Records produced"),
		metric.WithUnit(unitMessage),
	)
	if err != nil {
		return nil, err
	}

	produceDuration, err := meter.Float64Histogram(
		"stream.produce.duration",
		metric.WithDescription("Time per Send() call"),
		metric.WithUnit(unitSecond),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
	)
	if err != nil {
		return nil, err
	}

	errors, err := meter.Int64Counter(
		"stream.errors",
		metric.WithDescription("Processing errors and error handler decisions"),
		metric.WithUnit(unitError),
	)

	if err != nil {
		return nil, err
	}

	tasksActive, err := meter.Int64UpDownCounter(
		"stream.tasks.active",
		metric.WithDescription("Active tasks (partitions)"),
		metric.WithUnit(unitTask),
	)
	if err != nil {
		return nil, err
	}

	rebalanceCount, err := meter.Int64Counter(
		"stream.rebalance.count",
		metric.WithDescription("Rebalance events"),
		metric.WithUnit(unitRebalance),
	)
	if err != nil {
		return nil, err
	}

	nodeRecords, err := meter.Int64Counter(
		"stream.node.records",
		metric.WithDescription("Records processed per node"),
		metric.WithUnit(unitMessage),
	)
	if err != nil {
		return nil, err
	}

	nodeLatency, err := meter.Float64Histogram(
		"stream.node.latency",
		metric.WithDescription("Processing time per node"),
		metric.WithUnit(unitSecond),
		metric.WithExplicitBucketBoundaries(0.00001, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1),
	)
	if err != nil {
		return nil, err
	}

	edgeRecords, err := meter.Int64Counter(
		"stream.edge.records",
		metric.WithDescription("Records flowing between nodes"),
		metric.WithUnit(unitMessage),
	)
	if err != nil {
		return nil, err
	}

	partitionQueueDepth, err := meter.Int64ObservableGauge(
		"stream.partitioned.queue.depth",
		metric.WithDescription("Total records queued across all partition workers"),
		metric.WithUnit(unitMessage),
	)
	if err != nil {
		return nil, err
	}

	partitionPausedDepth, err := meter.Int64ObservableGauge(
		"stream.partitioned.paused.depth",
		metric.WithDescription("Total records queued in paused partitions"),
		metric.WithUnit(unitMessage),
	)
	if err != nil {
		return nil, err
	}

	return &Telemetry{
		Tracer:                 tracer,
		Propagator:             prop,
		meter:                  meter,
		MessagesConsumed:       messagesConsumed,
		PollDuration:           pollDuration,
		ConsumerLag:            consumerLag,
		PollRecords:            pollRecords,
		ProcessDuration:        processDuration,
		ProcessRetries:         processRetries,
		MessagesProduced:       messagesProduced,
		ProduceDuration:        produceDuration,
		Errors:                 errors,
		TasksActive:            tasksActive,
		RebalanceCount:         rebalanceCount,
		PartitionedQueueDepth:  partitionQueueDepth,
		PartitionedPausedDepth: partitionPausedDepth,
		NodeRecords:            nodeRecords,
		NodeLatency:            nodeLatency,
		EdgeRecords:            edgeRecords,
	}, nil
}

// RegisterCallback registers an observable callback for the meter
func (t *Telemetry) RegisterCallback(f metric.Callback, observable metric.Observable) (metric.Registration, error) {
	if t.meter == nil {
		return nil, nil
	}

	return t.meter.RegisterCallback(f, observable)
}

// Noop returns a Telemetry instance with all noop instruments
func Noop() *Telemetry {
	t, _ := NewTelemetry(nil, nil, nil)
	return t
}
