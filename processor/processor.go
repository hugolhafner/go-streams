package processor

import (
	"github.com/hugolhafner/go-streams/record"
)

type Processor[KIn, VIn, KOut, VOut any] interface {
	Init(ctx Context[KOut, VOut])
	Process(record *record.Record[KIn, VIn])
	Close() error
}

type UntypedProcessor interface {
	Init(ctx UntypedContext)
	Process(record *record.UntypedRecord) error
	Close() error
}

type UntypedContext interface {
	Forward(record *record.UntypedRecord)
	ForwardTo(childName string, record *record.UntypedRecord)
}

type Context[K, V any] interface {
	Forward(record *record.Record[K, V])
	ForwardTo(childName string, record *record.Record[K, V])
}
