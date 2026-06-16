//go:build unit

package topology_test

import (
	"testing"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/hugolhafner/go-streams/topology"
	"github.com/stretchr/testify/require"
)

func TestTopology_Describe_LinearPipeline(t *testing.T) {
	t.Parallel()

	topo := topology.New()
	topo.AddSource(
		"source", "input-topic",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("processor", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output-topic",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"processor",
	)

	desc := topo.Describe()

	require.Len(t, desc.Nodes, 3)
	require.Len(t, desc.Edges, 2)

	// Verify nodes (sorted alphabetically)
	nodeByID := make(map[string]topology.NodeInfo)
	for _, n := range desc.Nodes {
		nodeByID[n.ID] = n
	}

	src := nodeByID["source"]
	require.Equal(t, topology.NodeTypeSource, src.Type)
	require.Equal(t, "input-topic", src.Topic)

	proc := nodeByID["processor"]
	require.Equal(t, topology.NodeTypeProcessor, proc.Type)
	require.Equal(t, "", proc.Topic)

	sink := nodeByID["sink"]
	require.Equal(t, topology.NodeTypeSink, sink.Type)
	require.Equal(t, "output-topic", sink.Topic)

	edgeByID := make(map[string]topology.EdgeInfo)
	for _, e := range desc.Edges {
		edgeByID[e.ID] = e
	}

	require.Contains(t, edgeByID, "source->processor")
	require.Contains(t, edgeByID, "processor->sink")
}

func TestTopology_Describe_FanOut(t *testing.T) {
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

	desc := topo.Describe()

	require.Len(t, desc.Nodes, 4)
	require.Len(t, desc.Edges, 3) // source->proc, proc->sink-a, proc->sink-b

	edgeByID := make(map[string]topology.EdgeInfo)
	for _, e := range desc.Edges {
		edgeByID[e.ID] = e
	}

	require.Contains(t, edgeByID, "source->proc")
	require.Contains(t, edgeByID, "proc->sink-a")
	require.Contains(t, edgeByID, "proc->sink-b")
}

func TestTopology_Describe_Empty(t *testing.T) {
	t.Parallel()

	topo := topology.New()
	desc := topo.Describe()

	require.Empty(t, desc.Nodes)
	require.Empty(t, desc.Edges)
}
