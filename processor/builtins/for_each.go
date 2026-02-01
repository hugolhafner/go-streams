package builtins

import (
	"context"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*ForEachProcessor[any, any])(nil)

type ForEachFunc[K, V any] func(context.Context, K, V) error

type ForEachProcessor[K, V any] struct {
	action ForEachFunc[K, V]
	ctx    processor.Context[K, V]
}

func NewForEachProcessor[K, V any](action ForEachFunc[K, V]) *ForEachProcessor[K, V] {
	return &ForEachProcessor[K, V]{
		action: action,
	}
}

func (p *ForEachProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *ForEachProcessor[K, V]) Process(ctx context.Context, r *record.Record[K, V]) error {
	return p.action(ctx, r.Key, r.Value)
}

func (p *ForEachProcessor[K, V]) Close() error {
	return nil
}
