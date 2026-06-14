package task

import (
	"context"
	"fmt"

	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
	"go.opentelemetry.io/otel/metric"
)

var _ processor.UntypedContext = (*nodeContext)(nil)

type nodeContext struct {
	task       *TopologyTask
	nodeName   string
	children   []string
	namedEdges map[string]string // childName -> actual node name
	telemetry  *streamsotel.Telemetry
}

func (c *nodeContext) Forward(ctx context.Context, rec *record.UntypedRecord) error {
	for _, child := range c.children {
		c.telemetry.EdgeRecords.Add(ctx, 1, metric.WithAttributes(
			streamsotel.AttrEdgeSource.String(c.nodeName),
			streamsotel.AttrEdgeTarget.String(child),
		))
		if err := c.task.processAt(ctx, child, rec); err != nil {
			return fmt.Errorf("forward to %s: %w", child, err)
		}
	}
	return nil
}

func (c *nodeContext) ForwardTo(ctx context.Context, childName string, rec *record.UntypedRecord) error {
	actualName, ok := c.namedEdges[childName]
	if !ok {
		return fmt.Errorf("unknown child name: %s", childName)
	}
	c.telemetry.EdgeRecords.Add(ctx, 1, metric.WithAttributes(
		streamsotel.AttrEdgeSource.String(c.nodeName),
		streamsotel.AttrEdgeTarget.String(actualName),
	))
	return c.task.processAt(ctx, actualName, rec)
}
