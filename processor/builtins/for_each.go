package builtins

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*ForEachProcessor[any, any])(nil)

type ForEachProcessor[K, V any] struct {
	predicate func(K, V) bool
	ctx       processor.Context[K, V]
}

func NewForEachProcessor[K, V any](action func(K, V)) *ForEachProcessor[K, V] {
	return &ForEachProcessor[K, V]{
		predicate: func(key K, value V) bool {
			action(key, value)
			return true
		},
	}
}

func (p *ForEachProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *ForEachProcessor[K, V]) Process(record *record.Record[K, V]) {
	p.predicate(record.Key, record.Value)
}

func (p *ForEachProcessor[K, V]) Close() error {
	return nil
}
