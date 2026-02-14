package processor

import (
	"context"

	"github.com/hugolhafner/go-streams/record"
)

// Processor is the interface that all processors must implement. It defines the lifecycle of a processor and how it processes records.
type Processor[KIn, VIn, KOut, VOut any] interface {
	Init(ctx Context[KOut, VOut])
	Process(ctx context.Context, record *record.Record[KIn, VIn]) error
	Close() error
}

// UntypedProcessor is the same as Processor but works with untyped records. It is used internally to allow processors to be used without knowing the types of the records.
type UntypedProcessor interface {
	Init(ctx UntypedContext)
	Process(ctx context.Context, record *record.UntypedRecord) error
	Close() error
}

// UntypedContext is the context passed to the Init method of an UntypedProcessor. It allows the processor to forward records to its children without knowing the types of the records.
type UntypedContext interface {
	Forward(ctx context.Context, record *record.UntypedRecord) error
	ForwardTo(ctx context.Context, childName string, record *record.UntypedRecord) error
}

// Context is the context passed to the Init method of a Processor. It allows the processor to forward records to its children with the correct types.
type Context[K, V any] interface {
	Forward(ctx context.Context, record *record.Record[K, V]) error
	ForwardTo(ctx context.Context, childName string, record *record.Record[K, V]) error
}
