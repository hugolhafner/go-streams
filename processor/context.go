package processor

import (
	"github.com/hugolhafner/go-streams/record"
)

// Context is passed to processors to allow them to forward records downstream
type Context[K, V any] interface {
	// Forward sends to all downstream processors
	Forward(record *record.Record[K, V])

	// ForwardTo sends to a specific named child
	ForwardTo(childName string, record *record.Record[K, V])
}
