package builtins

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*MapProcessor[any, any, any, any])(nil)

func NewMapProcessor[KIn, VIn, KOut, VOut any](mapper func(KIn, VIn) (KOut, VOut)) *MapProcessor[KIn, VIn, KOut, VOut] {
	return &MapProcessor[KIn, VIn, KOut, VOut]{
		mapper: mapper,
	}
}

type MapProcessor[KIn, VIn, KOut, VOut any] struct {
	mapper func(KIn, VIn) (KOut, VOut)
	ctx    processor.Context[KOut, VOut]
}

func (p *MapProcessor[KIn, VIn, KOut, VOut]) Init(ctx processor.Context[KOut, VOut]) {
	p.ctx = ctx
}

func (p *MapProcessor[KIn, VIn, KOut, VOut]) Process(r *record.Record[KIn, VIn]) {
	newK, newV := p.mapper(r.Key, r.Value)
	p.ctx.Forward(&record.Record[KOut, VOut]{
		Key:      newK,
		Value:    newV,
		Metadata: r.Metadata,
	})
}

func (p *MapProcessor[KIn, VIn, KOut, VOut]) Close() error {
	return nil
}
