package builtins

import (
	"context"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
)

var _ processor.Processor[any, any, any, any] = (*BranchProcessor[any, any])(nil)

type PredicateFunc[K, V any] func(context.Context, K, V) (bool, error)

type BranchProcessor[K, V any] struct {
	predicates []PredicateFunc[K, V]
	branches   []string
	ctx        processor.Context[K, V]
}

func NewBranchProcessor[K, V any](
	predicates []PredicateFunc[K, V],
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

func (p *BranchProcessor[K, V]) Process(ctx context.Context, r *record.Record[K, V]) error {
	for i, pred := range p.predicates {
		if ok, err := pred(ctx, r.Key, r.Value); err != nil {
			return err
		} else if ok {
			return p.ctx.ForwardTo(ctx, p.branches[i], r)
		}
	}

	return nil
}

func (p *BranchProcessor[K, V]) Close() error {
	return nil
}
