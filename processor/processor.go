package processor

import (
	"github.com/hugolhafner/go-streams/record"
)

type Processor[KIn, VIn, KOut, VOut any] interface {
	// Init called during initialisation
	Init(ctx Context[KOut, VOut])

	// Process handles each record
	Process(record *record.Record[KIn, VIn])

	// Close called during shutdown
	Close() error
}
