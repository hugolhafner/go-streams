//go:build unit

package runner

import (
	"context"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
	mockkafka "github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/task"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func setupOtelTest(t *testing.T) (*tracetest.InMemoryExporter, *sdkmetric.ManualReader, *streamsotel.Telemetry) {
	t.Helper()

	spanExporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spanExporter),
	)

	metricReader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(metricReader),
	)

	tel, err := streamsotel.NewTelemetry(tp, mp, propagation.TraceContext{})
	require.NoError(t, err)

	t.Cleanup(
		func() {
			_ = tp.Shutdown(context.Background())
			_ = mp.Shutdown(context.Background())
		},
	)

	return spanExporter, metricReader, tel
}

func TestSingleThreaded_OTel_SpanHierarchy(t *testing.T) {
	spanExporter, _, tel := setupOtelTest(t)

	topo := createTestTopology()

	client := mockkafka.NewClient(mockkafka.WithGroupID("test-group"))
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	runnerFactory := NewSingleThreadedRunner()
	rn, err := runnerFactory(topo, factory, client, client, tel)
	require.NoError(t, err)
	r := rn.(*SingleThreaded)

	// Manually subscribe and trigger assign
	err = client.Subscribe(topo.SourceTopics(), r)
	require.NoError(t, err)

	// Process one poll cycle
	err = r.doPoll(context.Background())
	require.NoError(t, err)

	spans := spanExporter.GetSpans()
	require.NotEmpty(t, spans, "Expected spans to be recorded")

	// Collect span names
	spanNames := make(map[string]tracetest.SpanStub)
	for _, s := range spans {
		spanNames[s.Name] = s
	}

	// Verify receive span
	receiveSpan, ok := spanNames["receive"]
	require.True(t, ok, "Expected 'receive' span")
	assertAttribute(t, receiveSpan.Attributes, "messaging.system", "kafka")
	assertAttribute(t, receiveSpan.Attributes, "messaging.operation.type", "receive")

	// Verify process span
	processSpan, ok := spanNames["input process"]
	require.True(t, ok, "Expected 'input process' span")
	assertAttribute(t, processSpan.Attributes, "messaging.system", "kafka")
	assertAttribute(t, processSpan.Attributes, "messaging.operation.type", "process")
	assertAttribute(t, processSpan.Attributes, "messaging.destination.name", "input")
	assertAttribute(t, processSpan.Attributes, "messaging.consumer.group.name", "test-group")
	require.Equal(t, trace.SpanKindConsumer, processSpan.SpanKind)

	// Verify node execute spans exist
	foundProcExecute := false
	for _, s := range spans {
		if s.Name == "proc execute" {
			foundProcExecute = true
			assertAttribute(t, s.Attributes, "stream.node.name", "proc")
			assertAttribute(t, s.Attributes, "stream.node.type", "processor")
		}
	}
	require.True(t, foundProcExecute, "Expected 'proc execute' span")

	// Verify publish span
	publishSpan, ok := spanNames["output publish"]
	require.True(t, ok, "Expected 'output publish' span")
	assertAttribute(t, publishSpan.Attributes, "messaging.system", "kafka")
	assertAttribute(t, publishSpan.Attributes, "messaging.operation.type", "send")
	assertAttribute(t, publishSpan.Attributes, "messaging.destination.name", "output")
	require.Equal(t, trace.SpanKindProducer, publishSpan.SpanKind)
}

func TestSingleThreaded_OTel_Metrics(t *testing.T) {
	_, metricReader, tel := setupOtelTest(t)

	topo := createTestTopology()

	client := mockkafka.NewClient(mockkafka.WithGroupID("test-group"))
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	runnerFactory := NewSingleThreadedRunner()
	rn, err := runnerFactory(topo, factory, client, client, tel)
	require.NoError(t, err)
	r := rn.(*SingleThreaded)

	err = client.Subscribe(topo.SourceTopics(), r)
	require.NoError(t, err)

	err = r.doPoll(context.Background())
	require.NoError(t, err)

	// Collect metrics
	var rm metricdata.ResourceMetrics
	err = metricReader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	metrics := collectMetrics(rm)

	// Verify consumer messages counter
	assertMetricExists(t, metrics, "messaging.consumer.messages")

	// Verify poll duration histogram
	assertMetricExists(t, metrics, "stream.poll.duration")

	// Verify process duration histogram
	assertMetricExists(t, metrics, "stream.process.duration")

	// Verify producer messages counter
	assertMetricExists(t, metrics, "messaging.producer.messages")

	// Verify produce duration histogram
	assertMetricExists(t, metrics, "stream.produce.duration")
}

func TestSingleThreaded_OTel_TasksActiveMetric(t *testing.T) {
	_, metricReader, tel := setupOtelTest(t)

	topo := createTestTopology()

	client := mockkafka.NewClient(mockkafka.WithGroupID("test-group"))
	client.AddRecords("input", 0, mockkafka.SimpleRecord("k1", "v1"))

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	runnerFactory := NewSingleThreadedRunner()
	rn, err := runnerFactory(topo, factory, client, client, tel)
	require.NoError(t, err)
	r := rn.(*SingleThreaded)

	// OnAssigned should increment tasks.active
	r.OnAssigned(context.Background(), []kafka.TopicPartition{{Topic: "input", Partition: 0}})

	var rm metricdata.ResourceMetrics
	err = metricReader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	metrics := collectMetrics(rm)
	assertMetricExists(t, metrics, "stream.tasks.active")
}

func TestSingleThreaded_OTel_ContextPropagation(t *testing.T) {
	spanExporter, _, tel := setupOtelTest(t)

	topo := createTestTopology()

	client := mockkafka.NewClient(mockkafka.WithGroupID("test-group"))

	// Create a record with trace context headers (simulating cross-service propagation)
	rec := mockkafka.Record("k1", "v1").
		WithHeader("traceparent", []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")).
		Build()
	client.AddRecords("input", 0, rec)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	runnerFactory := NewSingleThreadedRunner()
	rn, err := runnerFactory(topo, factory, client, client, tel)
	require.NoError(t, err)
	r := rn.(*SingleThreaded)

	err = client.Subscribe(topo.SourceTopics(), r)
	require.NoError(t, err)

	err = r.doPoll(context.Background())
	require.NoError(t, err)

	// Verify that process span has the parent trace ID from the injected header
	spans := spanExporter.GetSpans()
	for _, s := range spans {
		if s.Name == "input process" {
			require.Equal(
				t, "4bf92f3577b34da6a3ce929d0e0e4736", s.SpanContext.TraceID().String(),
				"Process span should inherit trace ID from record headers",
			)
			break
		}
	}

	// Verify that produced records have trace context headers injected
	produced := client.ProducedRecords()
	require.NotEmpty(t, produced, "Expected produced records")
	foundTraceparent := false
	for _, h := range produced[0].Headers {
		if h.Key == "traceparent" {
			foundTraceparent = true
			require.Contains(
				t, string(h.Value), "4bf92f3577b34da6a3ce929d0e0e4736",
				"Produced record should carry the same trace ID",
			)
		}
	}
	require.True(t, foundTraceparent, "Expected traceparent header on produced record")
}

func TestPartitionedRunner_OTel_BasicSpans(t *testing.T) {
	spanExporter, _, tel := setupOtelTest(t)

	topo := createTestTopology()

	client := mockkafka.NewClient(mockkafka.WithGroupID("test-group"))
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(WithChannelBufferSize(10))
	r, err := runnerFactory(topo, factory, client, client, tel)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- r.Run(ctx)
	}()

	// Wait for spans to be recorded then cancel
	require.Eventually(t, func() bool {
		return len(spanExporter.GetSpans()) > 0
	}, 3*time.Second, 50*time.Millisecond, "spans should be recorded")
	cancel()
	<-done

	spans := spanExporter.GetSpans()

	// Collect unique span names
	spanNames := make(map[string]bool)
	for _, s := range spans {
		spanNames[s.Name] = true
	}

	require.True(t, spanNames["receive"], "Expected 'receive' span")
	require.True(t, spanNames["input process"], "Expected 'input process' span")
	require.True(t, spanNames["output publish"], "Expected 'output publish' span")
}

// Helper functions

func assertAttribute(t *testing.T, attrs []attribute.KeyValue, key, expected string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			require.Equal(
				t, expected, a.Value.AsString(),
				"Attribute %s should be %q", key, expected,
			)
			return
		}
	}
	t.Fatalf("Attribute %q not found in span attributes", key)
}

func collectMetrics(rm metricdata.ResourceMetrics) map[string]bool {
	names := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}
	return names
}

func assertMetricExists(t *testing.T, metrics map[string]bool, name string) {
	t.Helper()
	require.True(t, metrics[name], "Expected metric %q to be recorded", name)
}
