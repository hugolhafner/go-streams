package builtins

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*ForEachProcessor[any, any])(nil)

type ForEachProcessor[K, V any] struct {
	action func(K, V)
	ctx    processor.Context[K, V]
}

func NewForEachProcessor[K, V any](action func(K, V)) *ForEachProcessor[K, V] {
	return &ForEachProcessor[K, V]{
		action: action,
	}
}

func (p *ForEachProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *ForEachProcessor[K, V]) Process(r *record.Record[K, V]) {
	p.action(r.Key, r.Value)
}

func (p *ForEachProcessor[K, V]) Close() error {
	return nil
}
