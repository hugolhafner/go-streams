package builtins

import (
	"context"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*MapProcessor[any, any, any, any])(nil)

type MapFunc[KIn, VIn, KOut, VOut any] func(context.Context, KIn, VIn) (KOut, VOut, error)

func NewMapProcessor[KIn, VIn, KOut, VOut any](mapper MapFunc[KIn, VIn, KOut, VOut]) *MapProcessor[KIn, VIn, KOut,
	VOut] {
	return &MapProcessor[KIn, VIn, KOut, VOut]{
		mapper: mapper,
	}
}

type MapProcessor[KIn, VIn, KOut, VOut any] struct {
	mapper MapFunc[KIn, VIn, KOut, VOut]
	ctx    processor.Context[KOut, VOut]
}

func (p *MapProcessor[KIn, VIn, KOut, VOut]) Init(ctx processor.Context[KOut, VOut]) {
	p.ctx = ctx
}

func (p *MapProcessor[KIn, VIn, KOut, VOut]) Process(ctx context.Context, r *record.Record[KIn, VIn]) error {
	newK, newV, err := p.mapper(ctx, r.Key, r.Value)
	if err != nil {
		return err
	}

	return p.ctx.Forward(
		ctx,
		&record.Record[KOut, VOut]{
			Key:      newK,
			Value:    newV,
			Metadata: r.Metadata,
		},
	)
}

func (p *MapProcessor[KIn, VIn, KOut, VOut]) Close() error {
	return nil
}
