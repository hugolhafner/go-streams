package processor

import "github.com/hugolhafner/go-streams/record"

var (
	_ UntypedProcessor  = (*untypedProcessorAdapter[any, any, any, any])(nil)
	_ Context[any, any] = (*typedContextAdapter[any, any])(nil)
)

type untypedProcessorAdapter[KIn, VIn, KOut, VOut any] struct {
	typed Processor[KIn, VIn, KOut, VOut]
}

func (a *untypedProcessorAdapter[KIn, VIn, KOut, VOut]) Init(ctx UntypedContext) {
	typedCtx := &typedContextAdapter[KOut, VOut]{untyped: ctx}
	a.typed.Init(typedCtx)
}

func (a *untypedProcessorAdapter[KIn, VIn, KOut, VOut]) Process(r *record.UntypedRecord) error {
	typed := &record.Record[KIn, VIn]{
		Key:      r.Key.(KIn),
		Value:    r.Value.(VIn),
		Metadata: r.Metadata,
	}
	return a.typed.Process(typed)
}

func (a *untypedProcessorAdapter[KIn, VIn, KOut, VOut]) Close() error {
	return a.typed.Close()
}

type typedContextAdapter[K, V any] struct {
	untyped UntypedContext
}

func (c *typedContextAdapter[K, V]) Forward(r *record.Record[K, V]) error {
	return c.untyped.Forward(r.ToUntyped())
}

func (c *typedContextAdapter[K, V]) ForwardTo(childName string, r *record.Record[K, V]) error {
	return c.untyped.ForwardTo(childName, r.ToUntyped())
}
