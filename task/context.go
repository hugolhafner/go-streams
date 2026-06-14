package task

import (
	"context"
	"fmt"
	"time"

	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var _ processor.UntypedContext = (*nodeContext)(nil)

type nodeContext struct {
	task       *TopologyTask
	nodeName   string
	children   []string
	namedEdges map[string]string // childName -> actual node name
	telemetry  *streamsotel.Telemetry

	// pre-computed to reduce overhead
	selfAttrs      metric.MeasurementOption            // node name + node type
	edgeAttrs      []metric.MeasurementOption          // aligned to children
	namedEdgeAttrs map[string]metric.MeasurementOption // keyed by the public childName
	spanName       string                              // "<node> execute"
	spanAttrs      trace.SpanStartOption               // static processor execute-span attributes

	// wall-clock time of children nodes to track self node time, safe while tasks process single records at a time
	childTime time.Duration
}

func (c *nodeContext) Forward(ctx context.Context, rec *record.UntypedRecord) error {
	for i, child := range c.children {
		c.telemetry.EdgeRecords.Add(ctx, 1, c.edgeAttrs[i])
		childStart := time.Now()
		err := c.task.processAt(ctx, child, rec)
		c.childTime += time.Since(childStart)
		if err != nil {
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
	c.telemetry.EdgeRecords.Add(ctx, 1, c.namedEdgeAttrs[childName])
	childStart := time.Now()
	err := c.task.processAt(ctx, actualName, rec)
	c.childTime += time.Since(childStart)
	return err
}
