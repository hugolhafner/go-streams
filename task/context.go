package task

import (
	"context"
	"fmt"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.UntypedContext = (*nodeContext)(nil)

type nodeContext struct {
	task       *TopologyTask
	nodeName   string
	children   []string
	namedEdges map[string]string // childName -> actual node name
}

func (c *nodeContext) Forward(ctx context.Context, rec *record.UntypedRecord) error {
	for _, child := range c.children {
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
	return c.task.processAt(ctx, actualName, rec)
}
