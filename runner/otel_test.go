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
		mockkafka.Record("k1", "v1").WithTimestamp(time.Now().Add(-100*time.Millisecond)).Build(),
		mockkafka.Record("k2", "v2").WithTimestamp(time.Now().Add(-200*time.Millisecond)).Build(),
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

	// Verify new metrics
	assertMetricExists(t, metrics, "stream.poll.records")
	assertMetricExists(t, metrics, "stream.consumer.lag")

	// Verify removed metrics are not present
	assertMetricNotExists(t, metrics, "stream.error_handler.actions")
	assertMetricNotExists(t, metrics, "stream.node.errors")
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

func TestSingleThreaded_OTel_RebalanceCount(t *testing.T) {
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

	partitions := []kafka.TopicPartition{{Topic: "input", Partition: 0}}

	// OnAssigned should record rebalance count
	r.OnAssigned(context.Background(), partitions)

	// OnRevoked should record rebalance count
	r.OnRevoked(context.Background(), partitions)

	var rm metricdata.ResourceMetrics
	err = metricReader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	metrics := collectMetrics(rm)
	assertMetricExists(t, metrics, "stream.rebalance.count")
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
	require.Eventually(
		t, func() bool {
			return len(spanExporter.GetSpans()) > 0
		}, 3*time.Second, 50*time.Millisecond, "spans should be recorded",
	)
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

func TestPartitionedRunner_OTel_QueueDepth(t *testing.T) {
	_, metricReader, tel := setupOtelTest(t)

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

	pr := r.(*PartitionedRunner)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- r.Run(ctx)
	}()

	require.Eventually(
		t, func() bool {
			return len(pr.WorkerQueueDepths()) > 0
		}, 3*time.Second, 50*time.Millisecond, "partition worker should be created",
	)

	var rm metricdata.ResourceMetrics
	err = metricReader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	cancel()
	<-done

	metrics := collectMetrics(rm)
	assertMetricExists(t, metrics, "stream.partitioned.queue.depth")
}

func TestPartitionedRunner_OTel_BackpressureEvents(t *testing.T) {
	_, metricReader, tel := setupOtelTest(t)

	topo := createSlowTopology(5 * time.Millisecond)

	client := mockkafka.NewClient(mockkafka.WithGroupID("test-group"), mockkafka.WithMaxPollRecords(10))
	for i := 0; i < 5; i++ {
		client.AddRecords(
			"input", 0,
			mockkafka.SimpleRecord("k", "v"),
		)
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	// buffer 1 so the poll batch overflows immediately and the partition
	// goes through a full pause/resume cycle
	runnerFactory := NewPartitionedRunner(WithChannelBufferSize(1))
	r, err := runnerFactory(topo, factory, client, client, tel)
	require.NoError(t, err)

	pr := r.(*PartitionedRunner)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- r.Run(ctx)
	}()

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 5 && len(pr.PausedPartitions()) == 0
		}, 3*time.Second, 25*time.Millisecond, "records should process and the partition resume",
	)

	var rm metricdata.ResourceMetrics
	err = metricReader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	cancel()
	<-done

	m, ok := findMetric(rm, "stream.partitioned.backpressure.events")
	require.True(t, ok, "backpressure events metric should be recorded")

	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok, "backpressure events should be a counter")

	events := map[string]int64{}
	for _, dp := range sum.DataPoints {
		if v, found := dp.Attributes.Value(streamsotel.AttrBackpressureEvent); found {
			events[v.AsString()] += dp.Value
		}
	}
	require.Greater(t, events[streamsotel.BackpressureEventPaused], int64(0), "paused events should be recorded")
	require.Greater(t, events[streamsotel.BackpressureEventResumed], int64(0), "resumed events should be recorded")
}

// TestSingleThreaded_OTel_ConsumerLag_ClampsNegative verifies a future record timestamp (producer
// clock skew) does not produce a negative consumer-lag observation.
func TestSingleThreaded_OTel_ConsumerLag_ClampsNegative(t *testing.T) {
	_, metricReader, tel := setupOtelTest(t)

	topo := createTestTopology()

	client := mockkafka.NewClient(mockkafka.WithGroupID("test-group"))
	rec := mockkafka.Record("k1", "v1").
		WithTopic("input").
		WithPartition(0).
		WithOffset(0).
		WithLeaderEpoch(1).
		WithTimestamp(time.Now().Add(5 * time.Second)). // future timestamp -> negative raw lag
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

	var rm metricdata.ResourceMetrics
	err = metricReader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	m, ok := findMetric(rm, "stream.consumer.lag")
	require.True(t, ok, "expected stream.consumer.lag metric")
	hist, ok := m.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "stream.consumer.lag should be a float64 histogram")
	require.NotEmpty(t, hist.DataPoints)
	for _, dp := range hist.DataPoints {
		if minV, hasMin := dp.Min.Value(); hasMin {
			require.GreaterOrEqual(t, minV, 0.0, "consumer lag must be clamped to >= 0")
		}
		require.GreaterOrEqual(t, dp.Sum, 0.0, "consumer lag sum must be >= 0")
	}
}

// TestSingleThreaded_OTel_RebalanceCounted_WhenCreateTasksFails verifies the assigned rebalance is
// counted even when task creation fails, while tasks.active is not incremented.
func TestSingleThreaded_OTel_RebalanceCounted_WhenCreateTasksFails(t *testing.T) {
	_, metricReader, tel := setupOtelTest(t)

	topo := createTestTopology() // only source topic is "input"

	client := mockkafka.NewClient(mockkafka.WithGroupID("test-group"))

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	runnerFactory := NewSingleThreadedRunner()
	rn, err := runnerFactory(topo, factory, client, client, tel)
	require.NoError(t, err)
	r := rn.(*SingleThreaded)

	// "nonexistent" has no source node, so CreateTasks fails and OnAssigned returns early.
	r.OnAssigned(context.Background(), []kafka.TopicPartition{{Topic: "nonexistent", Partition: 0}})

	var rm metricdata.ResourceMetrics
	err = metricReader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	require.Equal(
		t, int64(1),
		sumInt64(t, rm, "stream.rebalance.count", "stream.rebalance.type", "assigned"),
		"assigned rebalance should be counted even when CreateTasks fails",
	)
	require.Equal(
		t, int64(0), sumInt64(t, rm, "stream.tasks.active", "", ""),
		"tasks.active should not increment when CreateTasks fails",
	)
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// sumInt64 totals the Int64 Sum data points for a metric, optionally filtered to data points whose
// attrKey attribute equals attrVal. Returns 0 if the metric was never recorded.
func sumInt64(t *testing.T, rm metricdata.ResourceMetrics, name, attrKey, attrVal string) int64 {
	t.Helper()
	m, ok := findMetric(rm, name)
	if !ok {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok, "metric %q should be Sum[int64]", name)
	var total int64
	for _, dp := range sum.DataPoints {
		if attrKey == "" {
			total += dp.Value
			continue
		}
		if v, found := dp.Attributes.Value(attribute.Key(attrKey)); found && v.AsString() == attrVal {
			total += dp.Value
		}
	}
	return total
}

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

func assertMetricNotExists(t *testing.T, metrics map[string]bool, name string) {
	t.Helper()
	require.False(t, metrics[name], "Expected metric %q to NOT be recorded", name)
}
