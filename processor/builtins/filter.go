package builtins

import (
	"context"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*FilterProcessor[any, any])(nil)

type FilterProcessor[K, V any] struct {
	predicate PredicateFunc[K, V]
	ctx       processor.Context[K, V]
}

func NewFilterProcessor[K, V any](predicate PredicateFunc[K, V]) *FilterProcessor[K, V] {
	return &FilterProcessor[K, V]{predicate: predicate}
}

func (p *FilterProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *FilterProcessor[K, V]) Process(ctx context.Context, r *record.Record[K, V]) error {
	if ok, err := p.predicate(ctx, r.Key, r.Value); err != nil {
		return err
	} else if ok {
		return p.ctx.Forward(ctx, r)
	}

	return nil
}

func (p *FilterProcessor[K, V]) Close() error {
	return nil
}
