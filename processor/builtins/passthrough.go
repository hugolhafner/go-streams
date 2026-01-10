package builtins

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*PassthroughProcessor[any, any])(nil)

type PassthroughProcessor[K, V any] struct {
	ctx processor.Context[K, V]
}

func NewPassthroughProcessor[K, V any]() *PassthroughProcessor[K, V] {
	return &PassthroughProcessor[K, V]{}
}

func (p *PassthroughProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *PassthroughProcessor[K, V]) Process(r *record.Record[K, V]) {
	p.ctx.Forward(r)
}

func (p *PassthroughProcessor[K, V]) Close() error {
	// TODO implement me
	panic("implement me")
}
