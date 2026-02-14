package task

import (
	"context"
	"fmt"
	"sync"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
	"github.com/hugolhafner/go-streams/topology"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrTaskClosed = fmt.Errorf("task is closed")
)

var _ Task = (*TopologyTask)(nil)

type TopologyTask struct {
	partition  kafka.TopicPartition
	source     topology.SourceNode
	processors map[string]processor.UntypedProcessor
	contexts   map[string]*nodeContext
	sinks      map[string]*sinkHandler
	producer   kafka.Producer
	topology   *topology.Topology

	logger    logger.Logger
	telemetry *streamsotel.Telemetry

	closed bool
	mu     sync.RWMutex
}

func (t *TopologyTask) Partition() kafka.TopicPartition {
	return t.partition
}

func (t *TopologyTask) processSafe(ctx context.Context, rec kafka.ConsumerRecord) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered: %v", r)
		}
	}()

	key, err := t.source.KeySerde().Deserialise(rec.Topic, rec.Key)
	if err != nil {
		return NewSerdeError(fmt.Errorf("deserialize key: %w", err))
	}

	value, err := t.source.ValueSerde().Deserialise(rec.Topic, rec.Value)
	if err != nil {
		return NewSerdeError(fmt.Errorf("deserialize value: %w", err))
	}

	t.logger.Debug(
		"Processing record", "topic", rec.Topic, "partition", rec.Partition, "offset",
		rec.Offset,
	)

	untypedRec := record.NewUntyped(
		key, value, record.Metadata{
			Topic:     rec.Topic,
			Partition: rec.Partition,
			Offset:    rec.Offset,
			Timestamp: rec.Timestamp,
			Headers:   rec.Headers,
		},
	)

	children := t.topology.Children(t.source.Name())
	for _, childName := range children {
		t.logger.Debug("Forwarding record to child node", "node", childName)
		if err := t.processAt(ctx, childName, untypedRec); err != nil {
			if _, ok := AsProductionError(err); ok {
				return err
			}
			if _, ok := AsSerdeError(err); ok {
				return err
			}
			return NewProcessError(err, childName)
		}
	}

	return nil
}

func (t *TopologyTask) Process(ctx context.Context, rec kafka.ConsumerRecord) error {
	t.mu.RLock()
	closed := t.closed
	t.mu.RUnlock()

	if closed {
		return ErrTaskClosed
	}

	return t.processSafe(ctx, rec)
}

func (t *TopologyTask) processAt(ctx context.Context, nodeName string, rec *record.UntypedRecord) error {
	if sink, ok := t.sinks[nodeName]; ok {
		t.logger.Debug("Forwarding record to sink node", "node", nodeName)
		return sink.Process(ctx, rec)
	}

	proc, ok := t.processors[nodeName]
	if !ok {
		t.logger.Error("Unknown node", "node", nodeName)
		return fmt.Errorf("unknown node: %s", nodeName)
	}

	ctx, span := t.telemetry.Tracer.Start(
		ctx, nodeName+" execute",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			streamsotel.AttrNodeName.String(nodeName),
			streamsotel.AttrNodeType.String(streamsotel.NodeTypeProcessor),
		),
	)
	defer span.End()

	t.logger.Debug("Processing record at processor node", "node", nodeName)
	if err := proc.Process(ctx, rec); err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (t *TopologyTask) IsClosed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.closed
}

func (t *TopologyTask) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true

	var lastErr error
	for name, proc := range t.processors {
		if err := proc.Close(); err != nil {
			lastErr = fmt.Errorf("close processor %s: %w", name, err)
		}
	}
	return lastErr
}

func (t *TopologyTask) init() (*TopologyTask, error) {
	for name, node := range t.topology.Nodes() {
		if pn, ok := node.(topology.ProcessorNode); ok {
			t.processors[name] = pn.Supplier()()
		}
	}

	for name := range t.topology.Nodes() {
		children := t.topology.Children(name)
		namedEdges := t.topology.NamedEdges(name)
		t.contexts[name] = &nodeContext{
			task:       t,
			nodeName:   name,
			children:   children,
			namedEdges: namedEdges,
		}
	}

	for name, node := range t.topology.Nodes() {
		if sn, ok := node.(topology.SinkNode); ok {
			t.sinks[name] = &sinkHandler{
				name:      name,
				node:      sn,
				producer:  t.producer,
				telemetry: t.telemetry,
			}
		}
	}

	for name, proc := range t.processors {
		proc.Init(t.contexts[name])
	}

	return t, nil
}
