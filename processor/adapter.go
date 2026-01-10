package processor

import (
	"github.com/hugolhafner/go-streams/record"
	"github.com/hugolhafner/go-streams/topology"
)

type processorAdapter[KIn, VIn, KOut, VOut any] struct {
	typed Processor[KIn, VIn, KOut, VOut]
	ctx   *contextAdapter[KOut, VOut]
}

func (a *processorAdapter[KIn, VIn, KOut, VOut]) Init(ctx topology.ProcessorContext) {
	a.ctx = &contextAdapter[KOut, VOut]{untyped: ctx}
	a.typed.Init(a.ctx)
}

func (a *processorAdapter[KIn, VIn, KOut, VOut]) Process(r any) {
	typed := r.(*record.Record[KIn, VIn])
	a.typed.Process(typed)
}

func (a *processorAdapter[KIn, VIn, KOut, VOut]) Close() error {
	return a.typed.Close()
}

type contextAdapter[K, V any] struct {
	untyped topology.ProcessorContext
}

func (c *contextAdapter[K, V]) Forward(record *record.Record[K, V]) {
	c.untyped.Forward(record)
}

func (c *contextAdapter[K, V]) ForwardTo(childName string, record *record.Record[K, V]) {
	c.untyped.ForwardTo(childName, record)
}
