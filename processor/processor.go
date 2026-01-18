package processor

import (
	"github.com/hugolhafner/go-streams/record"
)

type Processor[KIn, VIn, KOut, VOut any] interface {
	Init(ctx Context[KOut, VOut])
	Process(record *record.Record[KIn, VIn]) error
	Close() error
}

type UntypedProcessor interface {
	Init(ctx UntypedContext)
	Process(record *record.UntypedRecord) error
	Close() error
}

type UntypedContext interface {
	Forward(record *record.UntypedRecord) error
	ForwardTo(childName string, record *record.UntypedRecord) error
}

type Context[K, V any] interface {
	Forward(record *record.Record[K, V]) error
	ForwardTo(childName string, record *record.Record[K, V]) error
}
