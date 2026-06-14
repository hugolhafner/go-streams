//go:build unit

package task_test

import (
	"context"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
	mockkafka "github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func setupOtelTest(t *testing.T) (*sdkmetric.ManualReader, *streamsotel.Telemetry) {
	t.Helper()

	tp := sdktrace.NewTracerProvider()
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

	return metricReader, tel
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	err := reader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	result := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			result[m.Name] = m
		}
	}
	return result
}

func TestTopologyTask_NodeAndEdgeMetrics(t *testing.T) {
	t.Parallel()

	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	reader, tel := setupOtelTest(t)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tsk.Close() })

	// Process a record
	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)
	require.NoError(t, err)

	metrics := collectMetrics(t, reader)

	// Verify stream.node.records was recorded for source, processor, and sink
	nodeRecords, ok := metrics["stream.node.records"]
	require.True(t, ok, "expected stream.node.records metric")
	sum := nodeRecords.Data.(metricdata.Sum[int64])
	require.GreaterOrEqual(t, len(sum.DataPoints), 3, "expected at least 3 data points (source + proc + sink)")

	// Verify stream.node.latency was recorded
	nodeLatency, ok := metrics["stream.node.latency"]
	require.True(t, ok, "expected stream.node.latency metric")
	hist := nodeLatency.Data.(metricdata.Histogram[float64])
	require.GreaterOrEqual(t, len(hist.DataPoints), 3, "expected at least 3 data points (source + proc + sink)")

	// Verify stream.edge.records was recorded
	edgeRecords, ok := metrics["stream.edge.records"]
	require.True(t, ok, "expected stream.edge.records metric")
	edgeSum := edgeRecords.Data.(metricdata.Sum[int64])
	// Edges: source->proc (in processSafe), proc->sink (in context.Forward)
	require.GreaterOrEqual(t, len(edgeSum.DataPoints), 2, "expected at least 2 edge data points")

	// Verify stream.node.errors is NOT present (removed)
	_, hasNodeErrors := metrics["stream.node.errors"]
	require.False(t, hasNodeErrors, "stream.node.errors should not be present")
}

func TestTopologyTask_EdgeMetrics_FanOut(t *testing.T) {
	t.Parallel()

	// Topology: source -> proc -> sink-a, sink-b
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink-a", "output-a",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)
	topo.AddSink(
		"sink-b", "output-b",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	reader, tel := setupOtelTest(t)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tsk.Close() })

	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)
	require.NoError(t, err)

	metrics := collectMetrics(t, reader)

	// Verify edge metrics: source->proc, proc->sink-a, proc->sink-b
	edgeRecords, ok := metrics["stream.edge.records"]
	require.True(t, ok, "expected stream.edge.records metric")
	edgeSum := edgeRecords.Data.(metricdata.Sum[int64])
	require.GreaterOrEqual(t, len(edgeSum.DataPoints), 3, "expected at least 3 edge data points for fan-out")

	// Verify source + 1 proc + 2 sinks = 4 node records
	nodeRecords, ok := metrics["stream.node.records"]
	require.True(t, ok, "expected stream.node.records metric")
	sum := nodeRecords.Data.(metricdata.Sum[int64])
	require.GreaterOrEqual(t, len(sum.DataPoints), 4, "expected at least 4 node data points (source + proc + 2 sinks)")
}

// nodeLatencySeconds returns the recorded stream.node.latency (in seconds) for a single node.
// For a single observation the histogram Sum equals the recorded value.
func nodeLatencySeconds(t *testing.T, metrics map[string]metricdata.Metrics, nodeName string) float64 {
	t.Helper()
	m, ok := metrics["stream.node.latency"]
	require.True(t, ok, "expected stream.node.latency metric")
	hist, ok := m.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "stream.node.latency should be a float64 histogram")
	for _, dp := range hist.DataPoints {
		if v, found := dp.Attributes.Value(attribute.Key("stream.node.name")); found && v.AsString() == nodeName {
			return dp.Sum
		}
	}
	t.Fatalf("no stream.node.latency data point for node %q", nodeName)
	return 0
}

// TestTopologyTask_NodeLatency_ExcludesDownstream verifies that a processor's node latency is its
// exclusive self-time: the recursive downstream subtree it forwards to must be subtracted out.
func TestTopologyTask_NodeLatency_ExcludesDownstream(t *testing.T) {
	t.Parallel()

	const downstreamDelay = 50 * time.Millisecond

	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	// "up" forwards immediately; "down" sleeps before forwarding to the sink.
	var up processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(func(_ context.Context, k, v string) (string, string, error) {
			return k, v, nil
		})
	}
	var down processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(func(_ context.Context, k, v string) (string, string, error) {
			time.Sleep(downstreamDelay)
			return k, v, nil
		})
	}
	topo.AddProcessor("up", up.ToUntyped(), "source")
	topo.AddProcessor("down", down.ToUntyped(), "up")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"down",
	)

	reader, tel := setupOtelTest(t)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tsk.Close() })

	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)
	require.NoError(t, err)

	metrics := collectMetrics(t, reader)

	upLatency := nodeLatencySeconds(t, metrics, "up")
	downLatency := nodeLatencySeconds(t, metrics, "down")

	// "down" actually sleeps, so its self-time reflects that delay.
	require.GreaterOrEqual(t, downLatency, 0.8*downstreamDelay.Seconds(),
		"down node self-time should include its own delay")

	// "up" forwards to "down": its self-time must EXCLUDE the downstream subtree.
	// Before the fix it would have included down's delay (cumulative subtree wall-clock).
	require.Less(t, upLatency, 0.5*downstreamDelay.Seconds(),
		"up node self-time should exclude downstream processing")
}

func TestTopologyTask_SourceNodeMetrics(t *testing.T) {
	t.Parallel()

	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	reader, tel := setupOtelTest(t)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger(), task.WithTelemetry(tel))
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tsk.Close() })

	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)
	require.NoError(t, err)

	metrics := collectMetrics(t, reader)

	// Verify source node emits stream.node.records with stream.node.type = source
	nodeRecords, ok := metrics["stream.node.records"]
	require.True(t, ok, "expected stream.node.records metric")
	sum := nodeRecords.Data.(metricdata.Sum[int64])

	foundSource := false
	for _, dp := range sum.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == "stream.node.type" && attr.Value.AsString() == "source" {
				foundSource = true
			}
		}
	}
	require.True(t, foundSource, "expected source node to emit stream.node.records with stream.node.type=source")

	// Verify stream.node.errors is NOT present
	_, hasNodeErrors := metrics["stream.node.errors"]
	require.False(t, hasNodeErrors, "stream.node.errors should not be present")
}
