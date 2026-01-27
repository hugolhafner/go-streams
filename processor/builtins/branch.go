package builtins

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*BranchProcessor[any, any])(nil)

type BranchProcessor[K, V any] struct {
	predicates []func(K, V) bool
	branches   []string
	ctx        processor.Context[K, V]
}

func NewBranchProcessor[K, V any](
	predicates []func(K, V) bool,
	branches []string,
) *BranchProcessor[K, V] {
	return &BranchProcessor[K, V]{
		predicates: predicates,
		branches:   branches,
	}
}

func (p *BranchProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *BranchProcessor[K, V]) Process(r *record.Record[K, V]) error {
	for i, pred := range p.predicates {
		if pred(r.Key, r.Value) {
			return p.ctx.ForwardTo(p.branches[i], r)
		}
	}

	return nil
}

func (p *BranchProcessor[K, V]) Close() error {
	return nil
}
