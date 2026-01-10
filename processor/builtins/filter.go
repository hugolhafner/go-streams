package builtins

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*FilterProcessor[any, any])(nil)

type FilterProcessor[K, V any] struct {
	predicate func(K, V) bool
	ctx       processor.Context[K, V]
}

func NewFilterProcessor[K, V any](predicate func(K, V) bool) *FilterProcessor[K, V] {
	return &FilterProcessor[K, V]{predicate: predicate}
}

func (p *FilterProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *FilterProcessor[K, V]) Process(r *record.Record[K, V]) {
	if p.predicate(r.Key, r.Value) {
		p.ctx.Forward(r)
	}
}

func (p *FilterProcessor[K, V]) Close() error {
	// TODO implement me
	panic("implement me")
}
