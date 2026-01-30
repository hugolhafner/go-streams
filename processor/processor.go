package processor

import (
	"context"

	"github.com/hugolhafner/go-streams/record"
)

type Processor[KIn, VIn, KOut, VOut any] interface {
	Init(ctx Context[KOut, VOut])
	Process(ctx context.Context, record *record.Record[KIn, VIn]) error
	Close() error
}

type UntypedProcessor interface {
	Init(ctx UntypedContext)
	Process(ctx context.Context, record *record.UntypedRecord) error
	Close() error
}

type UntypedContext interface {
	Forward(ctx context.Context, record *record.UntypedRecord) error
	ForwardTo(ctx context.Context, childName string, record *record.UntypedRecord) error
}

type Context[K, V any] interface {
	Forward(ctx context.Context, record *record.Record[K, V]) error
	ForwardTo(ctx context.Context, childName string, record *record.Record[K, V]) error
}
